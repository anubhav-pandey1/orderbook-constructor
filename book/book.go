package book

import (
	"errors"
	"sort"
)

var (
	ErrCrossedSnapshot = errors.New("crossed snapshot")

	ErrCrossedDelta = errors.New("delta crossed book")

	ErrEmptySnapshot = errors.New("snapshot has no levels")

	ErrDuplicatePrice = errors.New("duplicate price in snapshot")

	ErrInvalidQuantity = errors.New("invalid quantity")

	ErrInvalidPrice = errors.New("invalid price")

	ErrInvalidSide = errors.New("invalid side")
)

const defaultLevelHint = 256

type Book struct {
	capHint int
	version uint64
	bids    sideBook
	asks    sideBook
	quotes  quoteCache
}

func New(capHint int) *Book {
	if capHint <= 0 {
		capHint = defaultLevelHint
	}
	b := &Book{
		capHint: capHint,
		bids:    newSideBook(Bid, capHint),
		asks:    newSideBook(Ask, capHint),
	}
	b.quotes.store(BBO{})
	return b
}

func (b *Book) ApplySnapshot(sn *Snapshot) (BBO, error) {
	if sn == nil || len(sn.Bids)+len(sn.Asks) == 0 {
		return BBO{}, ErrEmptySnapshot
	}
	bids, err := buildSide(Bid, sn.Bids, b.capHint)
	if err != nil {
		return BBO{}, err
	}
	asks, err := buildSide(Ask, sn.Asks, b.capHint)
	if err != nil {
		return BBO{}, err
	}
	bidPx, bidQty, bidOK := bids.best()
	askPx, askQty, askOK := asks.best()
	if bidOK && askOK && bidPx >= askPx {
		return BBO{}, ErrCrossedSnapshot
	}
	version := b.version + 1
	bbo := BBO{
		BidPx: bidPx, BidQty: bidQty, BidOK: bidOK,
		AskPx: askPx, AskQty: askQty, AskOK: askOK,
		Version: version,
	}
	b.bids, b.asks = bids, asks
	b.version = version
	b.quotes.store(bbo)
	return bbo, nil
}

func buildSide(side Side, levels []Level, capHint int) (sideBook, error) {
	if len(levels) > capHint {
		capHint = len(levels)
	}
	s := newSideBook(side, capHint)
	for _, candidate := range levels {
		if candidate.Price < 0 {
			return sideBook{}, ErrInvalidPrice
		}
		if candidate.Qty <= 0 {
			return sideBook{}, ErrInvalidQuantity
		}
		if _, exists := s.levels[candidate.Price]; exists {
			return sideBook{}, ErrDuplicatePrice
		}
		s.set(candidate.Price, candidate.Qty)
	}
	return s, nil
}

func (b *Book) ApplyDelta(side Side, px Price, qty Quantity) (DeltaResult, error) {
	if px < 0 {
		return DeltaResult{}, ErrInvalidPrice
	}
	if qty < 0 {
		return DeltaResult{}, ErrInvalidQuantity
	}
	var target *sideBook
	switch side {
	case Bid:
		target = &b.bids
	case Ask:
		target = &b.asks
	default:
		return DeltaResult{}, ErrInvalidSide
	}
	_, exists := target.levels[px]
	kind := deltaKind(exists, qty)
	if qty > 0 {
		if side == Bid {
			askPx, _, askOK := b.asks.best()
			if askOK && px >= askPx {
				return DeltaResult{BBO: b.BBOSnapshot(), Kind: kind}, ErrCrossedDelta
			}
		} else {
			bidPx, _, bidOK := b.bids.best()
			if bidOK && bidPx >= px {
				return DeltaResult{BBO: b.BBOSnapshot(), Kind: kind}, ErrCrossedDelta
			}
		}
		target.set(px, qty)
	} else if exists {
		target.del(px)
	}
	target.maybeRebuild()
	version := b.version + 1
	bbo := b.privateBBO(version)
	if bbo.BidOK && bbo.AskOK && bbo.BidPx >= bbo.AskPx {
		return DeltaResult{BBO: b.BBOSnapshot(), Kind: kind}, ErrCrossedDelta
	}
	b.version = version
	b.quotes.store(bbo)
	return DeltaResult{BBO: bbo, Kind: kind}, nil
}

func deltaKind(exists bool, qty Quantity) DeltaKind {
	if qty == 0 {
		if exists {
			return LevelDeleted
		}
		return AbsentDelete
	}
	if exists {
		return LevelUpdated
	}
	return LevelInserted
}

func (b *Book) privateBBO(version uint64) BBO {
	bidPx, bidQty, bidOK := b.bids.best()
	askPx, askQty, askOK := b.asks.best()
	return BBO{
		BidPx: bidPx, BidQty: bidQty, BidOK: bidOK,
		AskPx: askPx, AskQty: askQty, AskOK: askOK,
		Version: version,
	}
}

func (b *Book) Invalidate() {
	b.bids = newSideBook(Bid, b.capHint)
	b.asks = newSideBook(Ask, b.capHint)
	b.quotes.store(BBO{Version: b.version})
}

func (b *Book) BBOSnapshot() BBO { return b.quotes.load() }

func (b *Book) Version() uint64 { return b.quotes.load().Version }

func (b *Book) DepthSnapshot() Depth {
	depth := Depth{
		Bids: make([]Level, 0, len(b.bids.levels)),
		Asks: make([]Level, 0, len(b.asks.levels)),
	}
	for px, level := range b.bids.levels {
		depth.Bids = append(depth.Bids, Level{Price: px, Qty: level.quantity})
	}
	for px, level := range b.asks.levels {
		depth.Asks = append(depth.Asks, Level{Price: px, Qty: level.quantity})
	}
	sort.Slice(depth.Bids, func(i, j int) bool { return depth.Bids[i].Price > depth.Bids[j].Price })
	sort.Slice(depth.Asks, func(i, j int) bool { return depth.Asks[i].Price < depth.Asks[j].Price })
	return depth
}

func (b *Book) BidLevelCount() int { return len(b.bids.levels) }

func (b *Book) AskLevelCount() int { return len(b.asks.levels) }
