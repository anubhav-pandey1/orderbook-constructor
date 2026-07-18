package book_test

import (
	"errors"
	"math"
	"testing"

	"orderbook/book"
)

func TestParsePrice(t *testing.T) {
	cases := []struct {
		in      string
		want    book.Price
		wantErr error
	}{
		{"99999.99", book.Price(9999999), nil},
		{"100", book.Price(10000), nil},
		{"0.01", book.Price(1), nil},
		{"0.0", book.Price(0), nil},
		{"100.123", 0, book.ErrPrecision}, // >2 dp
		{"-1", 0, book.ErrSyntax},
		{"1e3", 0, book.ErrSyntax},
		{"abc", 0, book.ErrSyntax},
		{" ", 0, book.ErrSyntax},
		{"", 0, book.ErrEmptyNumber},
		{"1.2.3", 0, book.ErrSyntax},
	}
	for _, tc := range cases {
		got, err := book.ParsePrice(tc.in)
		if !errors.Is(err, tc.wantErr) {
			t.Errorf("ParsePrice(%q) err = %v, want %v", tc.in, err, tc.wantErr)
			continue
		}
		if tc.wantErr == nil && got != tc.want {
			t.Errorf("ParsePrice(%q) = %d, want %d", tc.in, got, tc.want)
		}
	}
}

func TestFixedPointBoundariesAndAppendText(t *testing.T) {
	p, err := book.ParsePrice("92233720368547758.07")
	if err != nil || int64(p) != math.MaxInt64 {
		t.Fatalf("max price = %d, %v", p, err)
	}
	if _, err = book.ParsePrice("92233720368547758.08"); !errors.Is(err, book.ErrOverflow) {
		t.Fatalf("overflow = %v", err)
	}
	if _, err = book.ParsePrice(".1"); !errors.Is(err, book.ErrSyntax) {
		t.Fatalf("leading dot = %v", err)
	}
	if _, err = book.ParsePrice("1."); !errors.Is(err, book.ErrSyntax) {
		t.Fatalf("trailing dot = %v", err)
	}
	var buf [64]byte
	if got := string(book.Price(9999399).AppendText(buf[:0])); got != "99993.99" {
		t.Fatalf("price text = %q", got)
	}
	if got := string(book.Quantity(5270).AppendText(buf[:0])); got != "0.5270" {
		t.Fatalf("qty text = %q", got)
	}
	var n int
	if allocs := testing.AllocsPerRun(1000, func() { var local [32]byte; n = len(book.Price(9999399).AppendText(local[:0])) }); allocs != 0 || n == 0 {
		t.Fatalf("allocs/len = %v/%d", allocs, n)
	}
}

func TestParseQuantity(t *testing.T) {
	cases := []struct {
		in      string
		want    book.Quantity
		wantErr error
	}{
		{"1.5", book.Quantity(15000), nil},
		{"0.527", book.Quantity(5270), nil},
		{"0.0", book.Quantity(0), nil},
		{"1.23456", 0, book.ErrPrecision}, // >4 dp
		{"-1", 0, book.ErrSyntax},
		{"1e3", 0, book.ErrSyntax},
		{"abc", 0, book.ErrSyntax},
		{" ", 0, book.ErrSyntax},
		{"", 0, book.ErrEmptyNumber},
		{"1.2.3", 0, book.ErrSyntax},
	}
	for _, tc := range cases {
		got, err := book.ParseQuantity(tc.in)
		if !errors.Is(err, tc.wantErr) {
			t.Errorf("ParseQuantity(%q) err = %v, want %v", tc.in, err, tc.wantErr)
			continue
		}
		if tc.wantErr == nil && got != tc.want {
			t.Errorf("ParseQuantity(%q) = %d, want %d", tc.in, got, tc.want)
		}
	}
}

func TestPriceString(t *testing.T) {
	cases := []struct {
		p    book.Price
		want string
	}{
		{book.Price(9999399), "99993.99"},
		{book.Price(9999999), "99999.99"},
		{book.Price(10000), "100.00"},
		{book.Price(1), "0.01"},
		{book.Price(0), "0.00"},
	}
	for _, tc := range cases {
		if got := tc.p.String(); got != tc.want {
			t.Errorf("Price(%d).String() = %q, want %q", int64(tc.p), got, tc.want)
		}
	}
}

func TestQuantityString(t *testing.T) {
	cases := []struct {
		q    book.Quantity
		want string
	}{
		{book.Quantity(21802), "2.1802"},
		{book.Quantity(5270), "0.5270"},
		{book.Quantity(15000), "1.5000"},
		{book.Quantity(0), "0.0000"},
	}
	for _, tc := range cases {
		if got := tc.q.String(); got != tc.want {
			t.Errorf("Quantity(%d).String() = %q, want %q", int64(tc.q), got, tc.want)
		}
	}
}

// TestParseRoundTrip confirms parse then String is stable for representative inputs.
func TestParseRoundTrip(t *testing.T) {
	p, err := book.ParsePrice("99993.99")
	if err != nil {
		t.Fatalf("ParsePrice: %v", err)
	}
	if got := p.String(); got != "99993.99" {
		t.Errorf("round trip price = %q, want 99993.99", got)
	}
	q, err := book.ParseQuantity("2.1802")
	if err != nil {
		t.Fatalf("ParseQuantity: %v", err)
	}
	if got := q.String(); got != "2.1802" {
		t.Errorf("round trip qty = %q, want 2.1802", got)
	}
}
