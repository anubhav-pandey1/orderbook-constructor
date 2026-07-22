package feed

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"strconv"
	"strings"
	"unicode"

	"github.com/anubhav-pandey1/orderbook-constructor/book"
)

// Kind identifies the decoded feed record type.
type Kind uint8

const (
	// KindSnapshot identifies a full book snapshot record.
	KindSnapshot Kind = iota + 1

	// KindDelta identifies an incremental level update record.
	KindDelta

	// KindIncremental is an alias for KindDelta.
	KindIncremental = KindDelta
)

// StreamID identifies one normalized exchange and symbol stream.
type StreamID struct {
	// Exchange is lowercase and trimmed.
	Exchange string
	// Symbol is uppercase with common separators removed.
	Symbol string
}

// String returns exchange:symbol.
func (s StreamID) String() string { return s.Exchange + ":" + s.Symbol }

// NormalizeStreamID normalizes exchange and symbol into the canonical stream key.
func NormalizeStreamID(exchange, symbol string) (StreamID, error) {
	exchange = strings.ToLower(strings.TrimSpace(exchange))
	symbol = strings.TrimSpace(symbol)
	if exchange == "" || symbol == "" {
		return StreamID{}, fmt.Errorf("stream identity requires non-empty exchange and symbol")
	}
	var b strings.Builder
	b.Grow(len(symbol))
	for _, r := range symbol {
		switch r {
		case '/', '-', '_':
			continue
		default:
			b.WriteRune(unicode.ToUpper(r))
		}
	}
	if b.Len() == 0 {
		return StreamID{}, fmt.Errorf("stream identity has empty normalized symbol")
	}
	return StreamID{Exchange: exchange, Symbol: b.String()}, nil
}

// Record is one decoded snapshot or incremental update.
type Record struct {
	// Kind identifies whether the record is a snapshot or delta.
	Kind Kind
	// Line is the one-based CSV line number of the record.
	Line int
	// Stream identifies the normalized market stream.
	Stream StreamID
	// TS is the non-negative source timestamp as encoded by the feed.
	TS int64

	// Side is set for delta records.
	Side book.Side
	// Px is set for delta records.
	Px book.Price
	// Qty is set for delta records.
	Qty book.Quantity
	// Snap is set for snapshot records.
	Snap *book.Snapshot

	// FirstUpdateID is the first exchange update ID covered by the record when present.
	FirstUpdateID uint64
	// FinalUpdateID is the final exchange update ID covered by the record when present.
	FinalUpdateID uint64
	// HasUpdateID reports whether FirstUpdateID and FinalUpdateID are meaningful.
	HasUpdateID bool
}

var csvColumns = [...]string{"type", "exchange", "symbol", "timestamp", "side", "bids", "asks", "price", "size"}

// Decoder reads CSV records in the library feed schema.
type Decoder struct {
	r          *csv.Reader
	record     int
	headerRead bool
}

// NewDecoder constructs a CSV decoder over rd.
func NewDecoder(rd io.Reader) *Decoder {
	r := csv.NewReader(rd)
	r.FieldsPerRecord = len(csvColumns)
	r.ReuseRecord = true
	return &Decoder{r: r}
}

// Next returns the next record or io.EOF after the final record.
func (d *Decoder) Next() (Record, error) {
	if !d.headerRead {
		fields, err := d.r.Read()
		if err == io.EOF {
			return Record{}, fmt.Errorf("feed header: %w", io.ErrUnexpectedEOF)
		}
		if err != nil {
			return Record{}, fmt.Errorf("feed header: %w", err)
		}
		d.record++
		for i, want := range csvColumns {
			if strings.TrimSpace(fields[i]) != want {
				return Record{}, fmt.Errorf("line 1 column %d: got header %q, want %q", i+1, fields[i], want)
			}
		}
		d.headerRead = true
	}

	fields, err := d.r.Read()
	if err == io.EOF {
		return Record{}, io.EOF
	}
	if err != nil {
		return Record{}, fmt.Errorf("record %d: %w", d.record+1, err)
	}
	d.record++
	return d.parse(fields)
}

func (d *Decoder) parse(f []string) (Record, error) {
	line := d.record
	stream, err := NormalizeStreamID(f[1], f[2])
	if err != nil {
		return Record{}, fmt.Errorf("line %d stream identity: %w", line, err)
	}
	ts, err := strconv.ParseInt(strings.TrimSpace(f[3]), 10, 64)
	if err != nil {
		return Record{}, fmt.Errorf("line %d timestamp %q: %w", line, f[3], err)
	}
	if ts < 0 {
		return Record{}, fmt.Errorf("line %d timestamp must be non-negative", line)
	}

	switch strings.ToLower(strings.TrimSpace(f[0])) {
	case "snapshot":
		if err := requireEmpty(line, f, 4, 7, 8); err != nil {
			return Record{}, err
		}
		bids, err := parseLevels(f[5])
		if err != nil {
			return Record{}, fmt.Errorf("line %d bids: %w", line, err)
		}
		asks, err := parseLevels(f[6])
		if err != nil {
			return Record{}, fmt.Errorf("line %d asks: %w", line, err)
		}
		return Record{Kind: KindSnapshot, Line: line, Stream: stream, TS: ts, Snap: &book.Snapshot{Bids: bids, Asks: asks}}, nil
	case "incremental":
		if err := requireEmpty(line, f, 5, 6); err != nil {
			return Record{}, err
		}
		side, err := parseSide(f[4])
		if err != nil {
			return Record{}, fmt.Errorf("line %d: %w", line, err)
		}
		px, err := book.ParsePrice(strings.TrimSpace(f[7]))
		if err != nil {
			return Record{}, fmt.Errorf("line %d price %q: %w", line, f[7], err)
		}
		qty, err := book.ParseQuantity(strings.TrimSpace(f[8]))
		if err != nil {
			return Record{}, fmt.Errorf("line %d size %q: %w", line, f[8], err)
		}
		return Record{Kind: KindDelta, Line: line, Stream: stream, TS: ts, Side: side, Px: px, Qty: qty}, nil
	default:
		return Record{}, fmt.Errorf("line %d: unknown type %q", line, f[0])
	}
}

func requireEmpty(line int, fields []string, indexes ...int) error {
	for _, index := range indexes {
		if strings.TrimSpace(fields[index]) != "" {
			return fmt.Errorf("line %d: %s must be empty for this record type", line, csvColumns[index])
		}
	}
	return nil
}

func parseSide(s string) (book.Side, error) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "bid":
		return book.Bid, nil
	case "ask":
		return book.Ask, nil
	default:
		return 0, fmt.Errorf("invalid side %q", s)
	}
}

func parseLevels(s string) ([]book.Level, error) {
	dec := json.NewDecoder(strings.NewReader(strings.TrimSpace(s)))
	dec.UseNumber()
	var raw [][]json.Number
	if err := dec.Decode(&raw); err != nil {
		return nil, err
	}
	if err := requireJSONEOF(dec); err != nil {
		return nil, err
	}
	out := make([]book.Level, 0, len(raw))
	for i, pair := range raw {
		if len(pair) != 2 {
			return nil, fmt.Errorf("level %d must contain price and size, got %d values", i, len(pair))
		}
		px, err := book.ParsePrice(pair[0].String())
		if err != nil {
			return nil, fmt.Errorf("level %d price %q: %w", i, pair[0].String(), err)
		}
		qty, err := book.ParseQuantity(pair[1].String())
		if err != nil {
			return nil, fmt.Errorf("level %d size %q: %w", i, pair[1].String(), err)
		}
		out = append(out, book.Level{Price: px, Qty: qty})
	}
	return out, nil
}

func requireJSONEOF(dec *json.Decoder) error {
	var extra any
	err := dec.Decode(&extra)
	if err == io.EOF {
		return nil
	}
	if err == nil {
		return fmt.Errorf("unexpected JSON value after level array")
	}
	return fmt.Errorf("trailing JSON: %w", err)
}
