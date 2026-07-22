package book

import (
	"errors"
	"sort"
)

var (
	// ErrCrossedSnapshot reports a snapshot whose best bid is greater than or equal to its best ask.
	ErrCrossedSnapshot = errors.New("crossed snapshot")

	// ErrCrossedDelta reports a delta that would lock or cross the book.
	ErrCrossedDelta = errors.New("delta crossed book")

	// ErrEmptySnapshot reports a nil snapshot or a snapshot with no levels.
	ErrEmptySnapshot = errors.New("snapshot has no levels")

	// ErrDuplicatePrice reports repeated prices on one side of a snapshot.
	ErrDuplicatePrice = errors.New("duplicate price in snapshot")

	// ErrInvalidQuantity reports a negative delta quantity or non-positive snapshot quantity.
	ErrInvalidQuantity = errors.New("invalid quantity")

	// ErrInvalidPrice reports a negative price.
	ErrInvalidPrice = errors.New("invalid price")

	// ErrInvalidSide reports a side other than Bid or Ask.
	ErrInvalidSide = errors.New("invalid side")
)

const defaultLevelHint = 256

// Book stores aggregated level-2 depth for one stream.
//
// Mutating methods are intended for a single writer goroutine. BBOSnapshot and
// Version may be called concurrently with that writer.
type Book struct {
	capHint int
	version uint64
	bids    sideBook
	asks    sideBook
	quotes  quoteCache
}

// New constructs an empty book sized for approximately capHint levels per side.
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

// ApplySnapshot transactionally replaces the book with sn and returns the new BBO.
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

// ApplyDelta applies one level update and returns the resulting BBO.
//
// A zero quantity deletes the level when present and is accepted when absent.
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

// Invalidate clears all levels without advancing Version.
func (b *Book) Invalidate() {
	b.bids = newSideBook(Bid, b.capHint)
	b.asks = newSideBook(Ask, b.capHint)
	b.quotes.store(BBO{Version: b.version})
}

// BBOSnapshot returns the latest published best-bid-offer snapshot.
func (b *Book) BBOSnapshot() BBO { return b.quotes.load() }

// Version returns the latest published book version.
func (b *Book) Version() uint64 { return b.quotes.load().Version }

// DepthSnapshot returns independent sorted copies of both sides.
//
// DepthSnapshot should not be called concurrently with mutating methods.
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

// BidLevelCount returns the number of active bid levels.
func (b *Book) BidLevelCount() int { return len(b.bids.levels) }

// AskLevelCount returns the number of active ask levels.
func (b *Book) AskLevelCount() int { return len(b.asks.levels) }
