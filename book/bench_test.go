package book_test

import (
	"testing"
	"time"

	"orderbook/book"
)

// Sinks defeat dead-code elimination of benchmarked results.
var (
	sinkPrice book.Price
	sinkQty   book.Quantity
	sinkEvent book.BookEvent
)

func benchInitBook(b *testing.B, levels int) *book.Book {
	b.Helper()
	bk := book.New("x", "SYM", book.Config{CrossedPolicy: book.PolicyOff, LevelHint: levels + 16})
	bids := make([]book.Level, 0, levels)
	asks := make([]book.Level, 0, levels)
	for i := 0; i < levels; i++ {
		// Bids well below asks so the book is never crossed.
		bids = append(bids, book.Level{Price: book.Price(500000 - int64(i)*10), Qty: book.Quantity(10000 + int64(i))})
		asks = append(asks, book.Level{Price: book.Price(600000 + int64(i)*10), Qty: book.Quantity(10000 + int64(i))})
	}
	if _, err := bk.ApplySnapshot(book.Snapshot{Bids: bids, Asks: asks}, time.Now()); err != nil {
		b.Fatalf("snapshot: %v", err)
	}
	return bk
}

// BenchmarkApplyUpdateExisting replaces the quantity of an already-present level.
// This is the hot path and should be ~0 allocs/op.
func BenchmarkApplyUpdateExisting(b *testing.B) {
	bk := benchInitBook(b, 256)
	price := book.Price(500000) // the best bid, always present
	now := time.Now()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sinkEvent, _ = bk.ApplyIncremental(book.Incremental{
			Side: book.Bid, Price: price, Qty: book.Quantity(10000 + int64(i&0xffff)),
		}, now)
	}
}

// BenchmarkApplyNewLevel inserts a fresh, unique bid price each iteration.
func BenchmarkApplyNewLevel(b *testing.B) {
	bk := benchInitBook(b, 256)
	now := time.Now()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Prices below the starting bid range so they never cross asks and are unique.
		sinkEvent, _ = bk.ApplyIncremental(book.Incremental{
			Side: book.Bid, Price: book.Price(400000 - int64(i)), Qty: book.Quantity(5000),
		}, now)
	}
}

// BenchmarkApplyDelete exercises the delete path of ApplyIncremental (Qty==0).
func BenchmarkApplyDelete(b *testing.B) {
	bk := benchInitBook(b, 256)
	price := book.Price(499000) // a non-best, present bid
	now := time.Now()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// First iteration removes the level; subsequent ones are idempotent no-ops
		// (still a valid, version-bumping delete path).
		sinkEvent, _ = bk.ApplyIncremental(book.Incremental{Side: book.Bid, Price: price, Qty: 0}, now)
	}
}

func BenchmarkParsePrice(b *testing.B) {
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sinkPrice, _ = book.ParsePrice("99993.99")
	}
}

func BenchmarkParseQuantity(b *testing.B) {
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sinkQty, _ = book.ParseQuantity("2.1802")
	}
}
