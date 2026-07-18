package feed

import (
	"context"
	"fmt"
	"io"
	"time"

	"orderbook/book"
)

// Mode selects replay pacing.
type Mode uint8

const (
	Fast  Mode = iota // apply as fast as possible
	Paced             // reproduce timestamp spacing
)

// Clock abstracts time for deterministic tests. Now must carry a monotonic
// reading (real implementation uses time.Now()).
type Clock interface {
	Now() time.Time
	SleepUntil(t time.Time)
}

type RealClock struct{}

func (RealClock) Now() time.Time { return time.Now() }
func (RealClock) SleepUntil(t time.Time) {
	if d := time.Until(t); d > 0 {
		time.Sleep(d)
	}
}

// EventSink receives one BookEvent per accepted row, in order.
type EventSink interface {
	Publish(context.Context, book.BookEvent) error
}

// SinkFunc adapts a func to an EventSink.
type SinkFunc func(book.BookEvent) error

func (f SinkFunc) Publish(_ context.Context, e book.BookEvent) error { return f(e) }

// SyncPolicy is the feed-completeness/ordering seam. This build ships the
// timestamp implementation (monotonicity sanity only — timestamps cannot prove
// completeness). A production UpdateIDPolicy using exchange sequence IDs would
// implement the same interface and additionally trigger resync on a gap.
type SyncPolicy interface {
	Reset(ts int64)
	Check(ts int64) error // non-nil stops replay (strict violation)
	Stats() SyncStats
}

type SyncStats struct {
	Regressions uint64
	Duplicates  uint64
	LargeGaps   uint64
}

// TimestampPolicy enforces monotonicity per book.Policy and records diagnostics.
type TimestampPolicy struct {
	Mode         book.Policy
	GapThreshold int64 // >0 enables the large-gap diagnostic

	last  int64
	have  bool
	stats SyncStats
}

func (p *TimestampPolicy) Reset(ts int64)   { p.last, p.have = ts, true }
func (p *TimestampPolicy) Stats() SyncStats { return p.stats }

func (p *TimestampPolicy) Check(ts int64) error {
	if p.Mode == book.PolicyOff {
		p.last, p.have = ts, true
		return nil
	}
	if !p.have {
		p.last, p.have = ts, true
		return nil
	}
	switch {
	case ts > p.last:
		if p.GapThreshold > 0 && ts-p.last > p.GapThreshold {
			p.stats.LargeGaps++
		}
		p.last = ts
		return nil
	case ts == p.last:
		p.stats.Duplicates++
		if p.Mode == book.PolicyStrict {
			return fmt.Errorf("timestamp not strictly increasing at %d", ts)
		}
		return nil
	default:
		p.stats.Regressions++
		if p.Mode == book.PolicyStrict {
			return fmt.Errorf("timestamp regression: %d <= %d", ts, p.last)
		}
		return nil
	}
}

// Config controls a Replay run.
type Config struct {
	Mode   Mode
	Speed  float64       // paced speed multiplier (>1 = faster than real time)
	TSUnit time.Duration // wall-clock duration of one timestamp unit (paced)
	Sync   SyncPolicy    // nil disables ordering checks
}

// Stats summarizes a completed replay.
type Stats struct {
	Accepted       uint64
	Snapshots      uint64
	Incrementals   uint64
	Deletes        uint64
	CrossedWarn    uint64
	Sync           SyncStats
	LastExchangeTS int64
}

// Replay drives the single writer: decode -> sync-check -> (pace) -> apply ->
// publish. It publishes exactly one event per accepted row and stops on the
// first fatal error.
func Replay(ctx context.Context, dec *Decoder, bk *book.Book, sink EventSink, cfg Config, clk Clock) (Stats, error) {
	var st Stats
	if cfg.Speed <= 0 {
		cfg.Speed = 1
	}
	if cfg.TSUnit <= 0 {
		cfg.TSUnit = time.Millisecond
	}

	var firstTS int64
	var haveFirst bool
	var start time.Time

	for {
		select {
		case <-ctx.Done():
			return st, ctx.Err()
		default:
		}

		rec, err := dec.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return st, err
		}

		if cfg.Sync != nil {
			if err := cfg.Sync.Check(rec.ExchangeTime); err != nil {
				return st, err
			}
		}

		if cfg.Mode == Paced {
			if !haveFirst {
				firstTS, haveFirst, start = rec.ExchangeTime, true, clk.Now()
			}
			delta := time.Duration(float64(rec.ExchangeTime-firstTS) * float64(cfg.TSUnit) / cfg.Speed)
			clk.SleepUntil(start.Add(delta))
		}

		ingress := clk.Now()
		var ev book.BookEvent
		switch rec.Kind {
		case KindSnapshot:
			ev, err = bk.ApplySnapshot(rec.Snapshot, ingress)
			st.Snapshots++
		case KindIncremental:
			ev, err = bk.ApplyIncremental(rec.Incremental, ingress)
			st.Incrementals++
			if rec.Incremental.Qty == 0 {
				st.Deletes++
			}
		default:
			return st, fmt.Errorf("line %d: unknown record kind", rec.Line)
		}
		if err != nil {
			return st, fmt.Errorf("line %d: apply: %w", rec.Line, err)
		}

		st.Accepted++
		st.LastExchangeTS = rec.ExchangeTime

		if sink != nil {
			if err := sink.Publish(ctx, ev); err != nil {
				return st, err
			}
		}
	}

	if cfg.Sync != nil {
		st.Sync = cfg.Sync.Stats()
	}
	st.CrossedWarn = bk.CrossedCount
	return st, nil
}
