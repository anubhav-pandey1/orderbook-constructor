package book

import (
	"errors"
	"fmt"
	"math"
	"time"
)

// Price is a fixed-point price in cents (0.01 quote-currency ticks).
type Price int64

// Quantity is a fixed-point size in 1e-4 base-asset units.
type Quantity int64

// Side identifies the book side. Bid=1, Ask=2 (zero value is invalid).
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
	PriceScale      = 100   // price * 100 -> cents
	QtyScale        = 10000 // size  * 10000 -> 1e-4 units
	priceFracDigits = 2
	qtyFracDigits   = 4
)

var (
	ErrEmptyNumber = errors.New("empty numeric field")
	ErrSyntax      = errors.New("invalid numeric syntax")
	ErrPrecision   = errors.New("excess decimal precision")
	ErrOverflow    = errors.New("numeric overflow")
)

// parseScaled converts a non-negative decimal string to an integer scaled by
// 10^fracDigits, exactly and without floating point. It rejects signs,
// exponents, NaN/Inf, empty input, excess precision, and overflow.
func parseScaled(s string, fracDigits int) (int64, error) {
	if len(s) == 0 {
		return 0, ErrEmptyNumber
	}
	var val int64
	dotSeen := false
	digits := false
	frac := 0
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c == '.' {
			if dotSeen {
				return 0, ErrSyntax
			}
			dotSeen = true
			continue
		}
		if c < '0' || c > '9' {
			return 0, ErrSyntax
		}
		if dotSeen {
			if frac == fracDigits {
				return 0, ErrPrecision
			}
			frac++
		}
		d := int64(c - '0')
		if val > (math.MaxInt64-d)/10 {
			return 0, ErrOverflow
		}
		val = val*10 + d
		digits = true
	}
	if !digits {
		return 0, ErrSyntax
	}
	for ; frac < fracDigits; frac++ {
		if val > math.MaxInt64/10 {
			return 0, ErrOverflow
		}
		val *= 10
	}
	return val, nil
}

// ParsePrice parses a decimal price string to scaled Price (2 dp exact).
func ParsePrice(s string) (Price, error) {
	v, err := parseScaled(s, priceFracDigits)
	return Price(v), err
}

// ParseQuantity parses a decimal size string to scaled Quantity (4 dp exact).
func ParseQuantity(s string) (Quantity, error) {
	v, err := parseScaled(s, qtyFracDigits)
	return Quantity(v), err
}

func (p Price) Float() float64    { return float64(p) / PriceScale }
func (q Quantity) Float() float64 { return float64(q) / QtyScale }

func (p Price) String() string    { return fmt.Sprintf("%d.%02d", p/PriceScale, p%PriceScale) }
func (q Quantity) String() string { return fmt.Sprintf("%d.%04d", q/QtyScale, q%QtyScale) }

// Level is a single aggregated price level.
type Level struct {
	Price Price
	Qty   Quantity
}

// Depth is a sorted full-depth view (bids desc, asks asc).
type Depth struct {
	Bids []Level
	Asks []Level
}

// TopOfBook is the best bid/ask snapshot.
type TopOfBook struct {
	BidPrice Price
	BidQty   Quantity
	AskPrice Price
	AskQty   Quantity
	HasBid   bool
	HasAsk   bool
}

// Snapshot is a full both-sides book state.
type Snapshot struct {
	Exchange     string
	Symbol       string
	ExchangeTime int64
	Bids         []Level
	Asks         []Level
}

// Incremental is a single per-price-level delta. Qty==0 deletes the level.
type Incremental struct {
	Side         Side
	Price        Price
	Qty          Quantity
	ExchangeTime int64
}

type EventKind uint8

const (
	EventSnapshot EventKind = iota + 1
	EventIncremental
)

// BookEvent carries the BBO after a successfully applied row, by value.
// IngressAt/AppliedAt retain Go's monotonic clock reading for latency math.
type BookEvent struct {
	Version      uint64
	Kind         EventKind
	Side         Side
	ExchangeTime int64
	IngressAt    time.Time
	AppliedAt    time.Time
	BestBidPrice Price
	BestBidQty   Quantity
	BestAskPrice Price
	BestAskQty   Quantity
	HasBid       bool
	HasAsk       bool
}

// Policy selects validation behavior for crossed-book / timestamp checks.
type Policy uint8

const (
	PolicyOff Policy = iota
	PolicyWarn
	PolicyStrict
)
