// Package strategy consumes book events from the SPSC event ring, optionally
// records pipeline latency, and forwards a structured record to the async
// logger. It is the single consumer of the event ring.
package strategy

import (
	"context"
	"time"

	"orderbook/book"
	"orderbook/internal/asynclog"
	"orderbook/internal/metrics"
	"orderbook/internal/ring"
)

// Strategy turns a book event into a log record. The dummy assignment strategy
// simply reports the best bid/ask.
type Strategy interface {
	OnBookEvent(book.BookEvent) asynclog.LogRecord
}

// BBO is the dummy strategy: read best bid and best ask, emit them.
type BBO struct{}

func (BBO) OnBookEvent(e book.BookEvent) asynclog.LogRecord {
	return asynclog.LogRecord{
		Version:      e.Version,
		ExchangeTime: e.ExchangeTime,
		BidPrice:     e.BestBidPrice,
		BidQty:       e.BestBidQty,
		AskPrice:     e.BestAskPrice,
		AskQty:       e.BestAskQty,
		HasBid:       e.HasBid,
		HasAsk:       e.HasAsk,
	}
}

// Latency holds the two measured spans. Nil disables latency recording.
type Latency struct {
	IngressToRecv *metrics.Histogram
	ApplyToRecv   *metrics.Histogram
}

// Run consumes events until the ring is closed and drained, then closes the
// logger. It records latency (if lat != nil), invokes the strategy, and pushes
// the resulting record to the logger.
func Run(ctx context.Context, in *ring.SPSC[book.BookEvent], lg *asynclog.Logger, s Strategy, lat *Latency, spin int) error {
	for {
		ev, ok, err := in.Pop(ctx, spin)
		if err != nil {
			if lg != nil {
				lg.Close()
			}
			return err
		}
		if !ok { // event ring closed and drained
			if lg != nil {
				lg.Close()
			}
			return nil
		}
		if lat != nil {
			recv := time.Now()
			if !ev.IngressAt.IsZero() {
				lat.IngressToRecv.RecordDuration(recv.Sub(ev.IngressAt))
			}
			if !ev.AppliedAt.IsZero() {
				lat.ApplyToRecv.RecordDuration(recv.Sub(ev.AppliedAt))
			}
		}
		rec := s.OnBookEvent(ev)
		if lg != nil {
			if err := lg.Log(ctx, rec); err != nil {
				lg.Close()
				return err
			}
		}
	}
}
