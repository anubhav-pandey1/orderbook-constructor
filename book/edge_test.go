package book_test

import (
	"errors"
	"reflect"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/anubhav-pandey1/orderbook-constructor/book"
)

func TestDepthSnapshotReturnsIndependentSortedCopies(t *testing.T) {
	b := initialized(t)
	depth := b.DepthSnapshot()
	if len(depth.Bids) != 2 || len(depth.Asks) != 2 {
		t.Fatalf("depth=%+v", depth)
	}
	if depth.Bids[0].Price != px(100) || depth.Bids[1].Price != px(99) {
		t.Fatalf("bids not sorted descending: %+v", depth.Bids)
	}
	if depth.Asks[0].Price != px(101) || depth.Asks[1].Price != px(102) {
		t.Fatalf("asks not sorted ascending: %+v", depth.Asks)
	}
	depth.Bids[0] = lv(px(1), qty(1))
	depth.Asks[0] = lv(px(999), qty(1))
	next := b.DepthSnapshot()
	if next.Bids[0].Price != px(100) || next.Asks[0].Price != px(101) {
		t.Fatalf("snapshot mutation leaked into book: %+v", next)
	}
}

func TestCrossedAskDeltaRejectsThenPreservesState(t *testing.T) {
	b := initialized(t)
	beforeBBO := b.BBOSnapshot()
	beforeDepth := b.DepthSnapshot()
	result, err := b.ApplyDelta(book.Ask, px(100), qty(9))
	if !errors.Is(err, book.ErrCrossedDelta) {
		t.Fatalf("err=%v", err)
	}
	if result.Kind != book.LevelInserted || result.BBO != beforeBBO {
		t.Fatalf("result=%+v before=%+v", result, beforeBBO)
	}
	if b.BBOSnapshot() != beforeBBO || !reflect.DeepEqual(b.DepthSnapshot(), beforeDepth) {
		t.Fatal("rejected ask delta changed book state")
	}
}

func TestDeleteLastSideAndRebuildToEmpty(t *testing.T) {
	b := book.New(1)
	if _, err := b.ApplySnapshot(&book.Snapshot{Bids: []book.Level{lv(px(10), qty(1))}}); err != nil {
		t.Fatal(err)
	}
	result, err := b.ApplyDelta(book.Bid, px(10), 0)
	if err != nil {
		t.Fatal(err)
	}
	if result.Kind != book.LevelDeleted || result.BBO.BidOK || result.BBO.AskOK || result.BBO.Version != 2 {
		t.Fatalf("delete result=%+v", result)
	}
	for i := 0; i < 200; i++ {
		_, err = b.ApplyDelta(book.Bid, book.Price(i+1000), 0)
		if err != nil {
			t.Fatal(err)
		}
	}
	if b.BidLevelCount() != 0 || b.AskLevelCount() != 0 || b.BBOSnapshot().Version != 202 {
		t.Fatalf("empty book counts/version=%d/%d/%d", b.BidLevelCount(), b.AskLevelCount(), b.Version())
	}
}

func TestDefaultConstructorAndOneSidedDeltas(t *testing.T) {
	b := book.New(0)
	result, err := b.ApplyDelta(book.Bid, px(10), qty(1))
	if err != nil {
		t.Fatal(err)
	}
	if result.Kind != book.LevelInserted || !result.BBO.BidOK || result.BBO.AskOK || result.BBO.Version != 1 {
		t.Fatalf("bid insert=%+v", result)
	}
	result, err = b.ApplyDelta(book.Ask, px(11), qty(2))
	if err != nil {
		t.Fatal(err)
	}
	if result.Kind != book.LevelInserted || !result.BBO.BidOK || !result.BBO.AskOK || result.BBO.Version != 2 {
		t.Fatalf("ask insert=%+v", result)
	}
}

func TestSideStringContracts(t *testing.T) {
	if book.Bid.String() != "bid" || book.Ask.String() != "ask" || book.Side(99).String() != "unknown" {
		t.Fatalf("side strings: %q %q %q", book.Bid, book.Ask, book.Side(99))
	}
}

func TestExistingLevelDeltaAllocationFree(t *testing.T) {
	b := initialized(t)
	allocs := testing.AllocsPerRun(1000, func() {
		if _, err := b.ApplyDelta(book.Bid, px(100), qty(3)); err != nil {
			t.Fatal(err)
		}
	})
	if allocs != 0 {
		t.Fatalf("existing-level update allocated %v times per run", allocs)
	}
}

func TestConcurrentBBOSnapshotDuringWriterLifecycle(t *testing.T) {
	b := book.New(4)
	if _, err := b.ApplySnapshot(&book.Snapshot{Bids: []book.Level{lv(px(10), qty(1))}, Asks: []book.Level{lv(px(20), qty(1))}}); err != nil {
		t.Fatal(err)
	}
	var stop atomic.Bool
	var failed atomic.Bool
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for !stop.Load() {
				bbo := b.BBOSnapshot()
				if bbo.BidOK && bbo.AskOK && bbo.BidPx >= bbo.AskPx {
					failed.Store(true)
					return
				}
				runtime.Gosched()
			}
		}()
	}
	for i := 0; i < 2000; i++ {
		q := book.Quantity(i+1) * book.Quantity(book.QtyScale)
		if _, err := b.ApplyDelta(book.Bid, px(10), q); err != nil {
			t.Fatal(err)
		}
		if i%200 == 0 {
			b.Invalidate()
			if _, err := b.ApplySnapshot(&book.Snapshot{Bids: []book.Level{lv(px(10), qty(1))}, Asks: []book.Level{lv(px(20), qty(1))}}); err != nil {
				t.Fatal(err)
			}
		}
	}
	stop.Store(true)
	wg.Wait()
	if failed.Load() {
		t.Fatal("observed crossed BBO from atomic snapshot")
	}
	bbo := b.BBOSnapshot()
	if bbo.Version == 0 || !bbo.BidOK || !bbo.AskOK || bbo.BidPx >= bbo.AskPx {
		t.Fatalf("final bbo=%+v", bbo)
	}
}

func TestNegativeFixedPointFormattingDoesNotOverflow(t *testing.T) {
	if got := book.Price(-1).String(); got != "-0.01" {
		t.Fatalf("negative price text=%q", got)
	}
	if got := book.Quantity(-1).String(); got != "-0.0001" {
		t.Fatalf("negative quantity text=%q", got)
	}
}
