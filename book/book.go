package book

import (
	"errors"
	"sort"
	"time"
)

var (
	ErrNotInitialized  = errors.New("book not initialized: incremental before snapshot")
	ErrCrossedBook     = errors.New("crossed book: best bid >= best ask")
	ErrDuplicatePrice  = errors.New("duplicate price in snapshot")
	ErrInvalidQuantity = errors.New("invalid (non-positive) quantity in snapshot")
	ErrInvalidSide     = errors.New("invalid side")
)

// Config configures a Book instance.
type Config struct {
	CrossedPolicy Policy // Off | Warn | Strict for the crossed-book check
	LevelHint     int    // per-side map/heap preallocation hint
}

// Book is a single-writer aggregated L2 order book for one exchange/symbol.
type Book struct {
	exchange    string
	symbol      string
	initialized bool
	version     uint64
	bids        sideBook
	asks        sideBook
	quotes      quoteCache
	crossed     Policy

	CrossedCount uint64 // number of crossed states observed under Warn policy
}

func New(exchange, symbol string, cfg Config) *Book {
	h := cfg.LevelHint
	if h <= 0 {
		h = 256
	}
	return &Book{
		exchange: exchange,
		symbol:   symbol,
		crossed:  cfg.CrossedPolicy,
		bids:     newSideBook(Bid, h),
		asks:     newSideBook(Ask, h),
	}
}

func (b *Book) Version() uint64    { return b.version }
func (b *Book) Initialized() bool  { return b.initialized }
func (b *Book) Exchange() string   { return b.exchange }
func (b *Book) Symbol() string     { return b.symbol }
func (b *Book) BidLevelCount() int { return len(b.bids.levels) }
func (b *Book) AskLevelCount() int { return len(b.asks.levels) }

// ApplySnapshot builds both sides off-book, validates, and swaps them in as one
// operation. No observer sees a partially built snapshot.
func (b *Book) ApplySnapshot(sn Snapshot, ingress time.Time) (BookEvent, error) {
	nb, err := buildSide(Bid, sn.Bids)
	if err != nil {
		return BookEvent{}, err
	}
	na, err := buildSide(Ask, sn.Asks)
	if err != nil {
		return BookEvent{}, err
	}
	bb, bq, hb := nb.best()
	ab, aq, ha := na.best()
	if hb && ha && bb >= ab {
		switch b.crossed {
		case PolicyStrict:
			return BookEvent{}, ErrCrossedBook
		case PolicyWarn:
			b.CrossedCount++
		}
	}
	b.bids = nb
	b.asks = na
	b.initialized = true
	b.version++
	tob := TopOfBook{BidPrice: bb, BidQty: bq, AskPrice: ab, AskQty: aq, HasBid: hb, HasAsk: ha}
	b.quotes.store(b.version, tob)
	return b.event(EventSnapshot, 0, sn.ExchangeTime, ingress, tob), nil
}

func buildSide(side Side, levels []Level) (sideBook, error) {
	s := newSideBook(side, len(levels)+8)
	for _, lv := range levels {
		if lv.Qty <= 0 {
			return sideBook{}, ErrInvalidQuantity
		}
		if _, dup := s.levels[lv.Price]; dup {
			return sideBook{}, ErrDuplicatePrice
		}
		g := s.nextGen
		s.nextGen++
		s.levels[lv.Price] = level{quantity: lv.Qty, generation: g}
		s.prices.push(heapEntry{price: lv.Price, generation: g})
	}
	return s, nil
}

// ApplyIncremental applies a single per-price-level delta. Qty==0 deletes;
// deleting an absent level is an idempotent no-op that still bumps the version.
func (b *Book) ApplyIncremental(inc Incremental, ingress time.Time) (BookEvent, error) {
	if !b.initialized {
		return BookEvent{}, ErrNotInitialized
	}
	var s *sideBook
	switch inc.Side {
	case Bid:
		s = &b.bids
	case Ask:
		s = &b.asks
	default:
		return BookEvent{}, ErrInvalidSide
	}
	if inc.Qty > 0 {
		s.set(inc.Price, inc.Qty)
	} else {
		s.del(inc.Price)
	}
	s.maybeRebuild()

	bb, bq, hb := b.bids.best()
	ab, aq, ha := b.asks.best()
	if hb && ha && bb >= ab {
		switch b.crossed {
		case PolicyStrict:
			return BookEvent{}, ErrCrossedBook
		case PolicyWarn:
			b.CrossedCount++
		}
	}
	b.version++
	tob := TopOfBook{BidPrice: bb, BidQty: bq, AskPrice: ab, AskQty: aq, HasBid: hb, HasAsk: ha}
	b.quotes.store(b.version, tob)
	return b.event(EventIncremental, inc.Side, inc.ExchangeTime, ingress, tob), nil
}

func (b *Book) event(kind EventKind, side Side, exTime int64, ingress time.Time, tob TopOfBook) BookEvent {
	return BookEvent{
		Version:      b.version,
		Kind:         kind,
		Side:         side,
		ExchangeTime: exTime,
		IngressAt:    ingress,
		AppliedAt:    time.Now(),
		BestBidPrice: tob.BidPrice,
		BestBidQty:   tob.BidQty,
		BestAskPrice: tob.AskPrice,
		BestAskQty:   tob.AskQty,
		HasBid:       tob.HasBid,
		HasAsk:       tob.HasAsk,
	}
}

// BestBidAsk is safe for concurrent callers via the seqlock quote cache.
func (b *Book) BestBidAsk() TopOfBook {
	_, tob := b.quotes.load()
	return tob
}

// DepthSnapshot returns a sorted full-depth view. Writer-owned/quiesced access
// only (it iterates the maps directly).
func (b *Book) DepthSnapshot() Depth {
	d := Depth{
		Bids: make([]Level, 0, len(b.bids.levels)),
		Asks: make([]Level, 0, len(b.asks.levels)),
	}
	for p, lv := range b.bids.levels {
		d.Bids = append(d.Bids, Level{Price: p, Qty: lv.quantity})
	}
	for p, lv := range b.asks.levels {
		d.Asks = append(d.Asks, Level{Price: p, Qty: lv.quantity})
	}
	sort.Slice(d.Bids, func(i, j int) bool { return d.Bids[i].Price > d.Bids[j].Price })
	sort.Slice(d.Asks, func(i, j int) bool { return d.Asks[i].Price < d.Asks[j].Price })
	return d
}
