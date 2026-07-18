package feed

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"time"

	"orderbook/book"
	obclock "orderbook/internal/clock"
	"orderbook/internal/pipeline"
	"orderbook/internal/ring"
	"orderbook/internal/syncx"
)

type ReplayMode uint8

const (
	Fast ReplayMode = iota + 1
	Paced
)

type ReplayCfg struct {
	Mode      ReplayMode
	Speed     float64
	TSUnit    time.Duration
	SpinIters int
	Stream    StreamID
}

type Config = ReplayCfg

type SnapshotRequester interface {
	RequestSnapshot(context.Context, ResyncRequest) error
}

type ResyncRequest struct {
	Exchange string
	Symbol   string
	Last     syncx.Cursor
	Received syncx.Cursor
	Reason   syncx.Reason
}

type Stats struct {
	Applied       uint64
	Discarded     uint64
	Invalidated   uint64
	Snapshots     uint64
	Deltas        uint64
	Deletes       uint64
	AbsentDeletes uint64

	Stale                uint64
	Duplicates           uint64
	Gaps                 uint64
	Crossed              uint64
	SnapshotRequests     uint64
	IgnoredWhileDesynced uint64
	LastAcceptedTS       int64
	HighestSeenTS        int64
}

var (
	ErrSnapshotRequired = errors.New("authoritative snapshot required")
	ErrStreamMismatch   = errors.New("record stream does not match configured stream")
)

type SnapshotRequiredError struct {
	State syncx.State
	Last  syncx.Cursor
}

func (e *SnapshotRequiredError) Error() string {
	return fmt.Sprintf("%v at end of input (state=%d, last timestamp=%d)", ErrSnapshotRequired, e.State, e.Last.Timestamp)
}
func (e *SnapshotRequiredError) Unwrap() error { return ErrSnapshotRequired }

func Replay(
	ctx context.Context,
	dec *Decoder,
	bk *book.Book,
	policy syncx.Policy,
	requester SnapshotRequester,
	out *ring.SPSC[pipeline.Event],
	cfg ReplayCfg,
	clk obclock.Clock,
) (Stats, error) {
	var stats Stats
	if out != nil {
		defer out.Close()
	}
	if err := validateReplay(dec, bk, policy, cfg, clk); err != nil {
		return stats, err
	}

	state := syncx.Uninitialized
	var lastAccepted syncx.Cursor
	var notificationID, syncEpoch uint64
	var snapshotRequested bool
	var firstTS, replayStartNS int64
	haveReplayAnchor := false

	requestSnapshot := func(received syncx.Cursor, reason syncx.Reason) error {
		if snapshotRequested {
			return nil
		}
		snapshotRequested = true
		stats.SnapshotRequests++
		if requester == nil {
			return nil
		}
		req := ResyncRequest{Exchange: cfg.Stream.Exchange, Symbol: cfg.Stream.Symbol, Last: lastAccepted, Received: received, Reason: reason}
		if err := requester.RequestSnapshot(ctx, req); err != nil {
			return fmt.Errorf("request snapshot for %s: %w", cfg.Stream, err)
		}
		return nil
	}
	publish := func(ev pipeline.Event) error {
		if out == nil {
			return nil
		}
		if err := out.Publish(ctx, ev, cfg.SpinIters); err != nil {
			return fmt.Errorf("publish event %d: %w", ev.NotificationID, err)
		}
		return nil
	}

	for {
		if err := ctx.Err(); err != nil {
			return stats, err
		}
		rec, err := dec.Next()
		if err == io.EOF {
			if state != syncx.Synchronized {
				return stats, &SnapshotRequiredError{State: state, Last: lastAccepted}
			}
			return stats, nil
		}
		if err != nil {
			return stats, err
		}
		if rec.TS > stats.HighestSeenTS {
			stats.HighestSeenTS = rec.TS
		}
		if rec.Stream != cfg.Stream {
			return stats, fmt.Errorf("line %d: %w: got %s, want %s", rec.Line, ErrStreamMismatch, rec.Stream, cfg.Stream)
		}

		dueNS := int64(0)
		if cfg.Mode == Paced {
			if !haveReplayAnchor {
				firstTS, replayStartNS, haveReplayAnchor = rec.TS, clk.NowNS(), true
			}
			dueNS, err = pacedDueNS(replayStartNS, rec.TS-firstTS, cfg.TSUnit, cfg.Speed)
			if err != nil {
				return stats, fmt.Errorf("line %d pacing: %w", rec.Line, err)
			}
			if err := clk.SleepUntilNS(ctx, dueNS); err != nil {
				return stats, err
			}
		}
		ingressNS := clk.NowNS()
		cursor := syncx.Cursor{Timestamp: rec.TS, FirstUpdateID: rec.FirstUpdateID, FinalUpdateID: rec.FinalUpdateID, HasUpdateID: rec.HasUpdateID}

		if rec.Kind == KindDelta && state != syncx.Synchronized {
			stats.IgnoredWhileDesynced++
			if err := requestSnapshot(cursor, syncx.ReasonMissingCursor); err != nil {
				return stats, err
			}
			continue
		}

		var decision syncx.Decision
		switch rec.Kind {
		case KindSnapshot:
			decision = policy.ClassifySnapshot(cursor)
		case KindDelta:
			decision = policy.ClassifyUpdate(cursor)
		default:
			return stats, fmt.Errorf("line %d: unknown record kind %d", rec.Line, rec.Kind)
		}

		switch decision.Action {
		case syncx.Discard:
			stats.Discarded++
			countReason(&stats, decision.Reason)
			continue
		case syncx.Resync:
			countReason(&stats, decision.Reason)
			if err := desynchronize(ctx, &stats, &state, &notificationID, syncEpoch, policy, bk, publish, clk, rec.TS, dueNS, ingressNS, decision.Reason); err != nil {
				return stats, err
			}
			if err := requestSnapshot(cursor, decision.Reason); err != nil {
				return stats, err
			}
			continue
		case syncx.Apply:
		default:
			return stats, fmt.Errorf("line %d: invalid synchronization action %d", rec.Line, decision.Action)
		}

		var ev pipeline.Event
		switch rec.Kind {
		case KindSnapshot:
			bbo, applyErr := bk.ApplySnapshot(rec.Snap)
			if applyErr != nil {
				reason := syncx.ReasonInvalidSnapshot
				if errors.Is(applyErr, book.ErrCrossedSnapshot) {
					reason = syncx.ReasonCrossed
					stats.Crossed++
				}
				if err := desynchronize(ctx, &stats, &state, &notificationID, syncEpoch, policy, bk, publish, clk, rec.TS, dueNS, ingressNS, reason); err != nil {
					return stats, err
				}
				if err := requestSnapshot(cursor, reason); err != nil {
					return stats, err
				}
				continue
			}
			applyNS := clk.NowNS()
			policy.AcceptSnapshot(cursor)
			lastAccepted, state, snapshotRequested = cursor, syncx.Synchronized, false
			syncEpoch++
			notificationID++
			stats.Applied++
			stats.Snapshots++
			stats.LastAcceptedTS = rec.TS
			ev = appliedEvent(notificationID, syncEpoch, pipeline.SnapshotApplied, bbo, rec.TS, dueNS, ingressNS, applyNS)

		case KindDelta:
			result, applyErr := bk.ApplyDelta(rec.Side, rec.Px, rec.Qty)
			if applyErr != nil {
				if !errors.Is(applyErr, book.ErrCrossedDelta) {
					return stats, fmt.Errorf("line %d apply delta: %w", rec.Line, applyErr)
				}
				stats.Crossed++
				if err := desynchronize(ctx, &stats, &state, &notificationID, syncEpoch, policy, bk, publish, clk, rec.TS, dueNS, ingressNS, syncx.ReasonCrossed); err != nil {
					return stats, err
				}
				if err := requestSnapshot(cursor, syncx.ReasonCrossed); err != nil {
					return stats, err
				}
				continue
			}
			applyNS := clk.NowNS()
			policy.AcceptUpdate(cursor)
			lastAccepted = cursor
			notificationID++
			stats.Applied++
			stats.Deltas++
			stats.LastAcceptedTS = rec.TS
			if rec.Qty == 0 {
				stats.Deletes++
			}
			if result.Kind == book.AbsentDelete {
				stats.AbsentDeletes++
			}
			ev = appliedEvent(notificationID, syncEpoch, pipeline.IncrementalApplied, result.BBO, rec.TS, dueNS, ingressNS, applyNS)
		}
		if err := publish(ev); err != nil {
			return stats, err
		}
	}
}

func validateReplay(dec *Decoder, bk *book.Book, policy syncx.Policy, cfg ReplayCfg, clk obclock.Clock) error {
	if dec == nil || bk == nil || policy == nil || clk == nil {
		return fmt.Errorf("decoder, book, sync policy, and clock are required")
	}
	if cfg.Mode != Fast && cfg.Mode != Paced {
		return fmt.Errorf("invalid replay mode %d", cfg.Mode)
	}
	if cfg.Speed <= 0 || math.IsNaN(cfg.Speed) || math.IsInf(cfg.Speed, 0) {
		return fmt.Errorf("speed must be finite and greater than zero")
	}
	if cfg.TSUnit <= 0 {
		return fmt.Errorf("timestamp unit must be greater than zero")
	}
	if cfg.SpinIters < 0 {
		return fmt.Errorf("spin iterations must be non-negative")
	}
	normalized, err := NormalizeStreamID(cfg.Stream.Exchange, cfg.Stream.Symbol)
	if err != nil {
		return fmt.Errorf("configured stream: %w", err)
	}
	if normalized != cfg.Stream {
		return fmt.Errorf("configured stream must be normalized: got %s, normalized form is %s", cfg.Stream, normalized)
	}
	return nil
}

func pacedDueNS(startNS, timestampDelta int64, unit time.Duration, speed float64) (int64, error) {
	scaled := float64(timestampDelta) * float64(unit) / speed
	if math.IsNaN(scaled) || math.IsInf(scaled, 0) || scaled > math.MaxInt64 || scaled < math.MinInt64 {
		return 0, fmt.Errorf("source timestamp offset overflows monotonic duration")
	}
	offset := int64(scaled)
	if offset > 0 && startNS > math.MaxInt64-offset || offset < 0 && startNS < math.MinInt64-offset {
		return 0, fmt.Errorf("replay due time overflows int64")
	}
	return startNS + offset, nil
}

func appliedEvent(notificationID, syncEpoch uint64, kind pipeline.EventKind, bbo book.BBO, eventTS, dueNS, ingressNS, applyNS int64) pipeline.Event {
	return pipeline.Event{
		NotificationID: notificationID, Version: bbo.Version, SyncEpoch: syncEpoch, Kind: kind,
		State: syncx.Synchronized, Reason: syncx.ReasonNone,
		BidPx: bbo.BidPx, AskPx: bbo.AskPx, BidQty: bbo.BidQty, AskQty: bbo.AskQty, BidOK: bbo.BidOK, AskOK: bbo.AskOK,
		EventTS: eventTS, DueNS: dueNS, IngressNS: ingressNS, ApplyNS: applyNS,
	}
}

func desynchronize(ctx context.Context, stats *Stats, state *syncx.State, notificationID *uint64, syncEpoch uint64, policy syncx.Policy, bk *book.Book, publish func(pipeline.Event) error, clk obclock.Clock, eventTS, dueNS, ingressNS int64, reason syncx.Reason) error {
	if *state == syncx.Desynchronized {
		return nil
	}
	wasSynchronized := *state == syncx.Synchronized
	bk.Invalidate()
	applyNS := clk.NowNS()
	policy.Invalidate()
	*state = syncx.Desynchronized
	if !wasSynchronized {
		return nil
	}
	*notificationID++
	stats.Invalidated++
	ev := pipeline.Event{NotificationID: *notificationID, Version: bk.Version(), SyncEpoch: syncEpoch, Kind: pipeline.BookInvalidated, State: syncx.Desynchronized, Reason: reason, EventTS: eventTS, DueNS: dueNS, IngressNS: ingressNS, ApplyNS: applyNS}
	if err := publish(ev); err != nil {
		return err
	}
	return ctx.Err()
}

func countReason(stats *Stats, reason syncx.Reason) {
	switch reason {
	case syncx.ReasonStale:
		stats.Stale++
	case syncx.ReasonDuplicate:
		stats.Duplicates++
	case syncx.ReasonGap, syncx.ReasonMissingCursor:
		stats.Gaps++
	case syncx.ReasonCrossed:
		stats.Crossed++
	}
}
