package book

import (
	"errors"
	"math"
	"strconv"
)

// Price stores a price as fixed-point ticks using PriceScale.
type Price int64

// Quantity stores an order-book size as fixed-point units using QtyScale.
type Quantity int64

// Ticks is an alias for Price for code that treats prices as integer ticks.
type Ticks = Price

// Qty is an alias for Quantity.
type Qty = Quantity

// Side identifies whether a level belongs to the bid or ask side.
type Side uint8

const (
	// Bid identifies the buy side of the book.
	Bid Side = iota + 1

	// Ask identifies the sell side of the book.
	Ask
)

// String returns "bid", "ask", or "unknown".
func (s Side) String() string {
	switch s {
	case Bid:
		return "bid"
	case Ask:
		return "ask"
	default:
		return "unknown"
	}
}

const (
	// PriceScale is the number of stored price ticks per whole price unit.
	PriceScale int64 = 100

	// QtyScale is the number of stored quantity units per whole size unit.
	QtyScale int64 = 10000

	// PriceFracDigits is the decimal precision accepted by ParsePrice.
	PriceFracDigits = 2

	// QtyFracDigits is the decimal precision accepted by ParseQuantity.
	QtyFracDigits = 4
)

var (
	// ErrEmptyNumber reports an empty fixed-point numeric field.
	ErrEmptyNumber = errors.New("empty numeric field")

	// ErrSyntax reports malformed fixed-point numeric syntax.
	ErrSyntax = errors.New("invalid numeric syntax")

	// ErrPrecision reports more fractional digits than the target type accepts.
	ErrPrecision = errors.New("excess decimal precision")

	// ErrOverflow reports a fixed-point value that cannot fit in int64.
	ErrOverflow = errors.New("numeric overflow")
)

func parseScaled(s string, fracDigits int) (int64, error) {
	if s == "" {
		return 0, ErrEmptyNumber
	}
	dot := -1
	for i := 0; i < len(s); i++ {
		switch c := s[i]; {
		case c == '.':
			if dot >= 0 {
				return 0, ErrSyntax
			}
			dot = i
		case c < '0' || c > '9':
			return 0, ErrSyntax
		}
	}
	if dot == 0 || dot == len(s)-1 {
		return 0, ErrSyntax
	}
	frac := 0
	if dot >= 0 {
		frac = len(s) - dot - 1
		if frac > fracDigits {
			return 0, ErrPrecision
		}
	}
	var value int64
	for i := 0; i < len(s); i++ {
		if i == dot {
			continue
		}
		digit := int64(s[i] - '0')
		if value > (math.MaxInt64-digit)/10 {
			return 0, ErrOverflow
		}
		value = value*10 + digit
	}
	for ; frac < fracDigits; frac++ {
		if value > math.MaxInt64/10 {
			return 0, ErrOverflow
		}
		value *= 10
	}
	return value, nil
}

// ParsePrice parses a non-negative decimal price with PriceFracDigits precision.
func ParsePrice(s string) (Price, error) {
	v, err := parseScaled(s, PriceFracDigits)
	return Price(v), err
}

// ParseQuantity parses a non-negative decimal quantity with QtyFracDigits precision.
func ParseQuantity(s string) (Quantity, error) {
	v, err := parseScaled(s, QtyFracDigits)
	return Quantity(v), err
}

// ParseQty is an alias for ParseQuantity.
func ParseQty(s string) (Quantity, error) { return ParseQuantity(s) }

func appendFixed(dst []byte, value int64, scale uint64, fracDigits int) []byte {
	var magnitude uint64
	if value < 0 {
		dst = append(dst, '-')
		magnitude = uint64(-(value + 1)) + 1
	} else {
		magnitude = uint64(value)
	}
	dst = strconv.AppendUint(dst, magnitude/scale, 10)
	dst = append(dst, '.')
	fraction := magnitude % scale
	divisor := scale / 10
	for i := 0; i < fracDigits; i++ {
		dst = append(dst, byte('0'+fraction/divisor))
		fraction %= divisor
		divisor /= 10
	}
	return dst
}

// AppendText appends the decimal representation of p to dst.
func (p Price) AppendText(dst []byte) []byte {
	return appendFixed(dst, int64(p), uint64(PriceScale), PriceFracDigits)
}

// AppendText appends the decimal representation of q to dst.
func (q Quantity) AppendText(dst []byte) []byte {
	return appendFixed(dst, int64(q), uint64(QtyScale), QtyFracDigits)
}

// String returns the decimal representation of p.
func (p Price) String() string { return string(p.AppendText(nil)) }

// String returns the decimal representation of q.
func (q Quantity) String() string { return string(q.AppendText(nil)) }

// Level represents one aggregated price level.
type Level struct {
	// Price is the fixed-point level price.
	Price Price
	// Qty is the fixed-point aggregated quantity at Price.
	Qty Quantity
}

// Depth contains independent bid and ask snapshots.
type Depth struct {
	// Bids are sorted from best price to worst price.
	Bids []Level
	// Asks are sorted from best price to worst price.
	Asks []Level
}

// BBO is a best-bid-offer snapshot.
type BBO struct {
	// BidPx is the best bid price when BidOK is true.
	BidPx Price
	// AskPx is the best ask price when AskOK is true.
	AskPx Price
	// BidQty is the best bid quantity when BidOK is true.
	BidQty Quantity
	// AskQty is the best ask quantity when AskOK is true.
	AskQty Quantity
	// BidOK reports whether the book has at least one bid.
	BidOK bool
	// AskOK reports whether the book has at least one ask.
	AskOK bool
	// Version is incremented after each accepted snapshot or delta.
	Version uint64
}

// Snapshot replaces the current book with complete bid and ask depth.
type Snapshot struct {
	// Bids contains bid levels in any order.
	Bids []Level
	// Asks contains ask levels in any order.
	Asks []Level
}

// DeltaKind describes how a delta changed a price level.
type DeltaKind uint8

const (
	// LevelInserted means a positive-quantity delta created a new level.
	LevelInserted DeltaKind = iota + 1

	// LevelUpdated means a positive-quantity delta replaced an existing level.
	LevelUpdated

	// LevelDeleted means a zero-quantity delta removed an existing level.
	LevelDeleted

	// AbsentDelete means a zero-quantity delta targeted a missing level.
	AbsentDelete
)

// DeltaResult reports the resulting BBO and operation kind for an accepted delta.
type DeltaResult struct {
	// BBO is the best-bid-offer snapshot after the delta.
	BBO BBO
	// Kind classifies the level-level effect of the delta.
	Kind DeltaKind
}
