package book_test

import (
	"fmt"
	"runtime"
	"testing"

	"github.com/anubhav-pandey1/orderbook-constructor/book"
)

var sinkResult book.DeltaResult
var sinkPrice book.Price
var sinkQty book.Quantity

func benchmarkBook(b *testing.B) *book.Book {
	b.Helper()
	bk := book.New(512)
	bids := make([]book.Level, 256)
	asks := make([]book.Level, 256)
	for i := range bids {
		bids[i] = book.Level{Price: book.Price(500000 - int64(i)*10), Qty: 10000}
		asks[i] = book.Level{Price: book.Price(600000 + int64(i)*10), Qty: 10000}
	}
	if _, err := bk.ApplySnapshot(&book.Snapshot{Bids: bids, Asks: asks}); err != nil {
		b.Fatal(err)
	}
	return bk
}

func BenchmarkApplyUpdateExisting(b *testing.B) {
	bk := benchmarkBook(b)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sinkResult, _ = bk.ApplyDelta(book.Bid, 500000, book.Quantity(10000+int64(i&65535)))
	}
}
func BenchmarkApplyNewLevel(b *testing.B) {
	bk := benchmarkBook(b)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {

		sinkResult, _ = bk.ApplyDelta(book.Ask, book.Price(700000+int64(i)), 5000)
	}
}
func BenchmarkApplyAbsentDelete(b *testing.B) {
	bk := benchmarkBook(b)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sinkResult, _ = bk.ApplyDelta(book.Bid, 300000, 0)
	}
}

func BenchmarkApplyActiveDeleteCycle(b *testing.B) {
	bk := benchmarkBook(b)
	const px = book.Price(700000)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = bk.ApplyDelta(book.Ask, px, 5000)
		sinkResult, _ = bk.ApplyDelta(book.Ask, px, 0)
	}
}

func BenchmarkApplyBestDeleteCycle(b *testing.B) {
	bk := benchmarkBook(b)
	const px = book.Price(500000)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sinkResult, _ = bk.ApplyDelta(book.Bid, px, 0)
		_, _ = bk.ApplyDelta(book.Bid, px, 10000)
	}
}

func BenchmarkDeleteReinsertGeneration(b *testing.B) {
	bk := benchmarkBook(b)
	const px = book.Price(499000)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = bk.ApplyDelta(book.Bid, px, 0)
		sinkResult, _ = bk.ApplyDelta(book.Bid, px, 10000)
	}
}

func BenchmarkHeapRebuildChurn(b *testing.B) {
	bk := benchmarkBook(b)
	const px = book.Price(400000)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = bk.ApplyDelta(book.Bid, px, 5000)
		sinkResult, _ = bk.ApplyDelta(book.Bid, px, 0)
	}
}

func BenchmarkBBOSnapshotParallel(b *testing.B) {
	bk := benchmarkBook(b)
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		var result book.BBO
		for pb.Next() {
			result = bk.BBOSnapshot()
		}
		runtime.KeepAlive(result)
	})
}

func BenchmarkApplySnapshot(b *testing.B) {
	for _, levels := range []int{10, 100, 1_000, 10_000} {
		b.Run(fmt.Sprintf("levels_per_side_%d", levels), func(b *testing.B) {
			snapshot := makeBenchmarkSnapshot(levels)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				bk := book.New(levels)
				_, _ = bk.ApplySnapshot(snapshot)
			}
		})
	}
}

func makeBenchmarkSnapshot(levels int) *book.Snapshot {
	bids := make([]book.Level, levels)
	asks := make([]book.Level, levels)
	for i := 0; i < levels; i++ {
		bids[i] = book.Level{Price: book.Price(500000 - i), Qty: 10000}
		asks[i] = book.Level{Price: book.Price(600000 + i), Qty: 10000}
	}
	return &book.Snapshot{Bids: bids, Asks: asks}
}
func BenchmarkParsePrice(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		sinkPrice, _ = book.ParsePrice("99993.99")
	}
}
func BenchmarkParseQuantity(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		sinkQty, _ = book.ParseQuantity("2.1802")
	}
}
