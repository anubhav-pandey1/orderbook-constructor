package feed

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"strconv"
	"strings"

	"orderbook/book"
)

type RecordKind uint8

const (
	KindSnapshot RecordKind = iota + 1
	KindIncremental
)

// Record is one decoded input row.
type Record struct {
	Kind         RecordKind
	Line         int
	ExchangeTime int64
	Snapshot     book.Snapshot
	Incremental  book.Incremental
}

// Decoder reads the assignment CSV. encoding/csv handles CRLF and quoted JSON
// fields; the header row is skipped automatically.
type Decoder struct {
	r    *csv.Reader
	line int
}

func NewDecoder(rd io.Reader) *Decoder {
	c := csv.NewReader(rd)
	c.FieldsPerRecord = 9
	c.ReuseRecord = true
	return &Decoder{r: c}
}

// Next returns the next record, or io.EOF at end of input.
func (d *Decoder) Next() (Record, error) {
	for {
		fields, err := d.r.Read()
		if err == io.EOF {
			return Record{}, io.EOF
		}
		if err != nil {
			return Record{}, err
		}
		d.line++
		if d.line == 1 && fields[0] == "type" {
			continue // header
		}
		return d.parse(fields)
	}
}

// columns: type,exchange,symbol,timestamp,side,bids,asks,price,size
func (d *Decoder) parse(f []string) (Record, error) {
	ts, err := strconv.ParseInt(strings.TrimSpace(f[3]), 10, 64)
	if err != nil {
		return Record{}, fmt.Errorf("line %d: bad timestamp %q: %w", d.line, f[3], err)
	}
	switch f[0] {
	case "snapshot":
		bids, err := parseLevels(f[5])
		if err != nil {
			return Record{}, fmt.Errorf("line %d bids: %w", d.line, err)
		}
		asks, err := parseLevels(f[6])
		if err != nil {
			return Record{}, fmt.Errorf("line %d asks: %w", d.line, err)
		}
		return Record{
			Kind:         KindSnapshot,
			Line:         d.line,
			ExchangeTime: ts,
			Snapshot:     book.Snapshot{Exchange: f[1], Symbol: f[2], ExchangeTime: ts, Bids: bids, Asks: asks},
		}, nil
	case "incremental":
		side, err := parseSide(f[4])
		if err != nil {
			return Record{}, fmt.Errorf("line %d: %w", d.line, err)
		}
		px, err := book.ParsePrice(strings.TrimSpace(f[7]))
		if err != nil {
			return Record{}, fmt.Errorf("line %d price %q: %w", d.line, f[7], err)
		}
		qty, err := book.ParseQuantity(strings.TrimSpace(f[8]))
		if err != nil {
			return Record{}, fmt.Errorf("line %d size %q: %w", d.line, f[8], err)
		}
		return Record{
			Kind:         KindIncremental,
			Line:         d.line,
			ExchangeTime: ts,
			Incremental:  book.Incremental{Side: side, Price: px, Qty: qty, ExchangeTime: ts},
		}, nil
	default:
		return Record{}, fmt.Errorf("line %d: unknown type %q", d.line, f[0])
	}
}

func parseSide(s string) (book.Side, error) {
	switch strings.TrimSpace(s) {
	case "bid":
		return book.Bid, nil
	case "ask":
		return book.Ask, nil
	}
	return 0, fmt.Errorf("invalid side %q", s)
}

// parseLevels decodes a JSON array of [price, size] pairs using UseNumber so
// the decimal token is converted exactly (never through float64).
func parseLevels(s string) ([]book.Level, error) {
	dec := json.NewDecoder(strings.NewReader(s))
	dec.UseNumber()
	var raw [][]json.Number
	if err := dec.Decode(&raw); err != nil {
		return nil, err
	}
	out := make([]book.Level, 0, len(raw))
	for _, pair := range raw {
		if len(pair) != 2 {
			return nil, fmt.Errorf("level pair must have 2 elements, got %d", len(pair))
		}
		p, err := book.ParsePrice(pair[0].String())
		if err != nil {
			return nil, fmt.Errorf("price %q: %w", pair[0].String(), err)
		}
		q, err := book.ParseQuantity(pair[1].String())
		if err != nil {
			return nil, fmt.Errorf("size %q: %w", pair[1].String(), err)
		}
		out = append(out, book.Level{Price: p, Qty: q})
	}
	return out, nil
}
