package replay

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"time"

	"github.com/anubhav-pandey1/orderbook-constructor/book"
	"github.com/anubhav-pandey1/orderbook-constructor/feed"
)

type Mode uint8

const (
	Fast Mode = iota + 1

	Paced
)

type Clock interface {
	NowNS() int64
	SleepUntilNS(context.Context, int64) error
}

type Handler interface {
	OnEvent(context.Context, Event) error
}

type HandlerFunc func(context.Context, Event) error

func (f HandlerFunc) OnEvent(ctx context.Context, event Event) error {
	return f(ctx, event)
}

type Options struct {
	Mode              Mode
	Speed             float64
	TimestampUnit     time.Duration
	Stream            feed.StreamID
	Policy            Policy
	SnapshotRequester SnapshotRequester
	Clock             Clock
}

type SnapshotRequester interface {
	RequestSnapshot(context.Context, ResyncRequest) error
}

type ResyncRequest struct {
	Exchange string
	Symbol   string
	Last     Cursor
	Received Cursor
	Reason   Reason
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

	ErrStreamMismatch = errors.New("record stream does not match configured stream")
)

type SnapshotRequiredError struct {
	State State
	Last  Cursor
}

func (e *SnapshotRequiredError) Error() string {
	return fmt.Sprintf("%v at end of input (state=%d, last timestamp=%d)", ErrSnapshotRequired, e.State, e.Last.Timestamp)
}

func (e *SnapshotRequiredError) Unwrap() error { return ErrSnapshotRequired }

func Run(ctx context.Context, dec *feed.Decoder, bk *book.Book, handler Handler, opts Options) (Stats, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	var stats Stats
	opts = withDefaults(opts)
	if err := validateOptions(dec, bk, opts); err != nil {
		return stats, err
	}

	state := Uninitialized
	var lastAccepted Cursor
	var notificationID, syncEpoch uint64
	var snapshotRequested bool
	var firstTS, replayStartNS int64
	haveReplayAnchor := false

	requestSnapshot := func(received Cursor, reason Reason) error {
		if snapshotRequested {
			return nil
		}
		snapshotRequested = true
		stats.SnapshotRequests++
		if opts.SnapshotRequester == nil {
			return nil
		}
		req := ResyncRequest{
			Exchange: opts.Stream.Exchange,
			Symbol:   opts.Stream.Symbol,
			Last:     lastAccepted,
			Received: received,
			Reason:   reason,
		}
		if err := opts.SnapshotRequester.RequestSnapshot(ctx, req); err != nil {
			return fmt.Errorf("request snapshot for %s: %w", opts.Stream, err)
		}
		return nil
	}

	publish := func(event Event) error {
		if handler == nil {
			return nil
		}
		if err := handler.OnEvent(ctx, event); err != nil {
			return fmt.Errorf("handle event %d: %w", event.NotificationID, err)
		}
		return nil
	}

	for {
		if err := ctx.Err(); err != nil {
			return stats, err
		}
		rec, err := dec.Next()
		if err == io.EOF {
			if state != Synchronized {
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
		if rec.Stream != opts.Stream {
			return stats, fmt.Errorf("line %d: %w: got %s, want %s", rec.Line, ErrStreamMismatch, rec.Stream, opts.Stream)
		}

		dueNS := int64(0)
		if opts.Mode == Paced {
			if !haveReplayAnchor {
				firstTS, replayStartNS, haveReplayAnchor = rec.TS, opts.Clock.NowNS(), true
			}
			dueNS, err = pacedDueNS(replayStartNS, rec.TS-firstTS, opts.TimestampUnit, opts.Speed)
			if err != nil {
				return stats, fmt.Errorf("line %d pacing: %w", rec.Line, err)
			}
			if err := opts.Clock.SleepUntilNS(ctx, dueNS); err != nil {
				return stats, err
			}
		}
		ingressNS := opts.Clock.NowNS()
		cursor := Cursor{
			Timestamp:     rec.TS,
			FirstUpdateID: rec.FirstUpdateID,
			FinalUpdateID: rec.FinalUpdateID,
			HasUpdateID:   rec.HasUpdateID,
		}

		if rec.Kind == feed.KindDelta && state != Synchronized {
			stats.IgnoredWhileDesynced++
			if err := requestSnapshot(cursor, ReasonMissingCursor); err != nil {
				return stats, err
			}
			continue
		}

		var decision Decision
		switch rec.Kind {
		case feed.KindSnapshot:
			decision = opts.Policy.ClassifySnapshot(cursor)
		case feed.KindDelta:
			decision = opts.Policy.ClassifyUpdate(cursor)
		default:
			return stats, fmt.Errorf("line %d: unknown record kind %d", rec.Line, rec.Kind)
		}

		switch decision.Action {
		case Discard:
			stats.Discarded++
			countReason(&stats, decision.Reason)
			continue
		case Resync:
			countReason(&stats, decision.Reason)
			if err := desynchronize(ctx, &stats, &state, &notificationID, syncEpoch, opts.Policy, bk, publish, opts.Clock, rec.TS, dueNS, ingressNS, decision.Reason); err != nil {
				return stats, err
			}
			if err := requestSnapshot(cursor, decision.Reason); err != nil {
				return stats, err
			}
			continue
		case Apply:
		default:
			return stats, fmt.Errorf("line %d: invalid synchronization action %d", rec.Line, decision.Action)
		}

		var event Event
		switch rec.Kind {
		case feed.KindSnapshot:
			bbo, applyErr := bk.ApplySnapshot(rec.Snap)
			if applyErr != nil {
				reason := ReasonInvalidSnapshot
				if errors.Is(applyErr, book.ErrCrossedSnapshot) {
					reason = ReasonCrossed
					stats.Crossed++
				}
				if err := desynchronize(ctx, &stats, &state, &notificationID, syncEpoch, opts.Policy, bk, publish, opts.Clock, rec.TS, dueNS, ingressNS, reason); err != nil {
					return stats, err
				}
				if err := requestSnapshot(cursor, reason); err != nil {
					return stats, err
				}
				continue
			}
			applyNS := opts.Clock.NowNS()
			opts.Policy.AcceptSnapshot(cursor)
			lastAccepted, state, snapshotRequested = cursor, Synchronized, false
			syncEpoch++
			notificationID++
			stats.Applied++
			stats.Snapshots++
			stats.LastAcceptedTS = rec.TS
			event = appliedEvent(notificationID, syncEpoch, SnapshotApplied, bbo, rec.TS, dueNS, ingressNS, applyNS)

		case feed.KindDelta:
			result, applyErr := bk.ApplyDelta(rec.Side, rec.Px, rec.Qty)
			if applyErr != nil {
				if !errors.Is(applyErr, book.ErrCrossedDelta) {
					return stats, fmt.Errorf("line %d apply delta: %w", rec.Line, applyErr)
				}
				stats.Crossed++
				if err := desynchronize(ctx, &stats, &state, &notificationID, syncEpoch, opts.Policy, bk, publish, opts.Clock, rec.TS, dueNS, ingressNS, ReasonCrossed); err != nil {
					return stats, err
				}
				if err := requestSnapshot(cursor, ReasonCrossed); err != nil {
					return stats, err
				}
				continue
			}
			applyNS := opts.Clock.NowNS()
			opts.Policy.AcceptUpdate(cursor)
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
			event = appliedEvent(notificationID, syncEpoch, IncrementalApplied, result.BBO, rec.TS, dueNS, ingressNS, applyNS)
		}
		if err := publish(event); err != nil {
			return stats, err
		}
	}
}

func withDefaults(opts Options) Options {
	if opts.Mode == 0 {
		opts.Mode = Fast
	}
	if opts.Speed == 0 {
		opts.Speed = 1
	}
	if opts.Policy == nil {
		opts.Policy = NewArrivalOrderPolicy()
	}
	if opts.Clock == nil {
		opts.Clock = realClock{origin: time.Now()}
	}
	return opts
}

func validateOptions(dec *feed.Decoder, bk *book.Book, opts Options) error {
	if dec == nil || bk == nil || opts.Policy == nil || opts.Clock == nil {
		return fmt.Errorf("decoder, book, sync policy, and clock are required")
	}
	if opts.Mode != Fast && opts.Mode != Paced {
		return fmt.Errorf("invalid replay mode %d", opts.Mode)
	}
	if opts.Speed <= 0 || math.IsNaN(opts.Speed) || math.IsInf(opts.Speed, 0) {
		return fmt.Errorf("speed must be finite and greater than zero")
	}
	if opts.Mode == Paced && opts.TimestampUnit <= 0 {
		return fmt.Errorf("timestamp unit must be greater than zero in paced mode")
	}
	normalized, err := feed.NormalizeStreamID(opts.Stream.Exchange, opts.Stream.Symbol)
	if err != nil {
		return fmt.Errorf("configured stream: %w", err)
	}
	if normalized != opts.Stream {
		return fmt.Errorf("configured stream must be normalized: got %s, normalized form is %s", opts.Stream, normalized)
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

func appliedEvent(notificationID, syncEpoch uint64, kind EventKind, bbo book.BBO, eventTS, dueNS, ingressNS, applyNS int64) Event {
	return Event{
		NotificationID: notificationID,
		Version:        bbo.Version,
		SyncEpoch:      syncEpoch,
		Kind:           kind,
		State:          Synchronized,
		Reason:         ReasonNone,
		BidPx:          bbo.BidPx,
		AskPx:          bbo.AskPx,
		BidQty:         bbo.BidQty,
		AskQty:         bbo.AskQty,
		BidOK:          bbo.BidOK,
		AskOK:          bbo.AskOK,
		EventTS:        eventTS,
		DueNS:          dueNS,
		IngressNS:      ingressNS,
		ApplyNS:        applyNS,
	}
}

func desynchronize(ctx context.Context, stats *Stats, state *State, notificationID *uint64, syncEpoch uint64, policy Policy, bk *book.Book, publish func(Event) error, clk Clock, eventTS, dueNS, ingressNS int64, reason Reason) error {
	if *state == Desynchronized {
		return nil
	}
	wasSynchronized := *state == Synchronized
	bk.Invalidate()
	applyNS := clk.NowNS()
	policy.Invalidate()
	*state = Desynchronized
	if !wasSynchronized {
		return nil
	}
	*notificationID++
	stats.Invalidated++
	event := Event{
		NotificationID: *notificationID,
		Version:        bk.Version(),
		SyncEpoch:      syncEpoch,
		Kind:           BookInvalidated,
		State:          Desynchronized,
		Reason:         reason,
		EventTS:        eventTS,
		DueNS:          dueNS,
		IngressNS:      ingressNS,
		ApplyNS:        applyNS,
	}
	if err := publish(event); err != nil {
		return err
	}
	return ctx.Err()
}

func countReason(stats *Stats, reason Reason) {
	switch reason {
	case ReasonStale:
		stats.Stale++
	case ReasonDuplicate:
		stats.Duplicates++
	case ReasonGap, ReasonMissingCursor:
		stats.Gaps++
	case ReasonCrossed:
		stats.Crossed++
	}
}

type realClock struct {
	origin time.Time
}

func (r realClock) NowNS() int64 {
	return time.Since(r.origin).Nanoseconds()
}

func (r realClock) SleepUntilNS(ctx context.Context, target int64) error {
	for {
		now := r.NowNS()
		if now >= target {
			return nil
		}
		timer := time.NewTimer(time.Duration(target - now))
		select {
		case <-timer.C:
		case <-ctx.Done():
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			return ctx.Err()
		}
	}
}
