package book

import (
	"errors"
	"math"
	"strconv"
)

type Price int64

type Quantity int64

type Ticks = Price

type Qty = Quantity

type Side uint8

const (
	Bid Side = iota + 1

	Ask
)

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
	PriceScale int64 = 100

	QtyScale int64 = 10000

	PriceFracDigits = 2

	QtyFracDigits = 4
)

var (
	ErrEmptyNumber = errors.New("empty numeric field")

	ErrSyntax = errors.New("invalid numeric syntax")

	ErrPrecision = errors.New("excess decimal precision")

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

func ParsePrice(s string) (Price, error) {
	v, err := parseScaled(s, PriceFracDigits)
	return Price(v), err
}

func ParseQuantity(s string) (Quantity, error) {
	v, err := parseScaled(s, QtyFracDigits)
	return Quantity(v), err
}

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

func (p Price) AppendText(dst []byte) []byte {
	return appendFixed(dst, int64(p), uint64(PriceScale), PriceFracDigits)
}

func (q Quantity) AppendText(dst []byte) []byte {
	return appendFixed(dst, int64(q), uint64(QtyScale), QtyFracDigits)
}

func (p Price) String() string { return string(p.AppendText(nil)) }

func (q Quantity) String() string { return string(q.AppendText(nil)) }

type Level struct {
	Price Price
	Qty   Quantity
}

type Depth struct {
	Bids []Level
	Asks []Level
}

type BBO struct {
	BidPx   Price
	AskPx   Price
	BidQty  Quantity
	AskQty  Quantity
	BidOK   bool
	AskOK   bool
	Version uint64
}

type Snapshot struct {
	Bids []Level
	Asks []Level
}

type DeltaKind uint8

const (
	LevelInserted DeltaKind = iota + 1

	LevelUpdated

	LevelDeleted

	AbsentDelete
)

type DeltaResult struct {
	BBO  BBO
	Kind DeltaKind
}
