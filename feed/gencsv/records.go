package gencsv

import (
	"fmt"
	"math/rand"
	"sort"

	"github.com/anubhav-pandey1/orderbook-constructor/book"
	"github.com/anubhav-pandey1/orderbook-constructor/feed"
)

type Generator struct {
	cfg    Config
	stream feed.StreamID
	book   *simBook
	index  int64
}

func NewGenerator(cfg Config) (*Generator, error) {
	if err := validateConfig(cfg); err != nil {
		return nil, err
	}
	stream, err := feed.NormalizeStreamID(cfg.Exchange, cfg.Symbol)
	if err != nil {
		return nil, err
	}
	return &Generator{cfg: cfg, stream: stream, book: newSimBook(cfg.LevelsPerSide, cfg.MaxLevels, rand.New(rand.NewSource(cfg.Seed)))}, nil
}

func (g *Generator) Next() (feed.Record, bool) {
	if g == nil || g.index > g.cfg.Incrementals {
		return feed.Record{}, false
	}
	seq := uint64(g.index + 1)
	rec := feed.Record{Line: int(g.index + 2), Stream: g.stream, TS: g.cfg.StartTS + g.index*g.cfg.TSStep, FirstUpdateID: seq, FinalUpdateID: seq, HasUpdateID: true}
	if g.index == 0 || g.cfg.SnapshotEvery > 0 && g.index%g.cfg.SnapshotEvery == 0 {
		rec.Kind, rec.Snap = feed.KindSnapshot, g.snapshot()
	} else {
		rec.Kind = feed.KindDelta
		side, px, qty := g.book.nextDelta()
		if side == "bid" {
			rec.Side = book.Bid
		} else {
			rec.Side = book.Ask
		}
		rec.Px, rec.Qty = book.Price(px), book.Quantity(qty)
	}
	g.index++
	return rec, true
}

func (g *Generator) snapshot() *book.Snapshot {
	bids := make([]book.Level, 0, len(g.book.bids))
	for px, qty := range g.book.bids {
		bids = append(bids, book.Level{Price: book.Price(px), Qty: book.Quantity(qty)})
	}
	asks := make([]book.Level, 0, len(g.book.asks))
	for px, qty := range g.book.asks {
		asks = append(asks, book.Level{Price: book.Price(px), Qty: book.Quantity(qty)})
	}
	sort.Slice(bids, func(i, j int) bool { return bids[i].Price > bids[j].Price })
	sort.Slice(asks, func(i, j int) bool { return asks[i].Price < asks[j].Price })
	return &book.Snapshot{Bids: bids, Asks: asks}
}

func validateConfig(cfg Config) error {
	switch {
	case cfg.Incrementals < 0:
		return fmt.Errorf("incrementals must be >= 0")
	case cfg.TSStep <= 0:
		return fmt.Errorf("ts-step must be > 0")
	case cfg.LevelsPerSide <= 0:
		return fmt.Errorf("levels-per-side must be > 0")
	case cfg.MaxLevels < cfg.LevelsPerSide:
		return fmt.Errorf("max-levels must be >= levels-per-side")
	case cfg.Exchange == "" || cfg.Symbol == "":
		return fmt.Errorf("exchange and symbol are required")
	}
	return nil
}
