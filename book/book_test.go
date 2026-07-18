package book_test

import (
	"errors"
	"testing"
	"time"

	"orderbook/book"
)

func TestSnapshotEstablishesBBOAndDepth(t *testing.T) {
	bk := book.New("x", "SYM", book.Config{CrossedPolicy: book.PolicyStrict})
	sn := book.Snapshot{
		ExchangeTime: 1000,
		Bids:         []book.Level{lv(px(100), qty(1)), lv(px(99), qty(2)), lv(px(98), qty(3))},
		Asks:         []book.Level{lv(px(101), qty(4)), lv(px(102), qty(5))},
	}
	ev, err := bk.ApplySnapshot(sn, time.Now())
	if err != nil {
		t.Fatalf("ApplySnapshot: %v", err)
	}
	if !bk.Initialized() {
		t.Error("book not initialized after snapshot")
	}
	if bk.Version() != 1 {
		t.Errorf("version = %d, want 1", bk.Version())
	}
	if bk.BidLevelCount() != 3 || bk.AskLevelCount() != 2 {
		t.Errorf("levels = %d/%d, want 3/2", bk.BidLevelCount(), bk.AskLevelCount())
	}
	if ev.Kind != book.EventSnapshot || ev.ExchangeTime != 1000 {
		t.Errorf("event = %+v, want snapshot @1000", ev)
	}
	tob := bk.BestBidAsk()
	if !tob.HasBid || !tob.HasAsk {
		t.Fatalf("expected both sides, got %+v", tob)
	}
	if tob.BidPrice != px(100) || tob.BidQty != qty(1) {
		t.Errorf("best bid = %s @ %s, want 100.00 @ 1", tob.BidPrice, tob.BidQty)
	}
	if tob.AskPrice != px(101) || tob.AskQty != qty(4) {
		t.Errorf("best ask = %s @ %s, want 101.00 @ 4", tob.AskPrice, tob.AskQty)
	}
}

func TestSecondSnapshotReplaces(t *testing.T) {
	bk := newInitBook(t) // bids 100/99, asks 101/102
	sn2 := book.Snapshot{
		Bids: []book.Level{lv(px(200), qty(7))},
		Asks: []book.Level{lv(px(201), qty(8))},
	}
	if _, err := bk.ApplySnapshot(sn2, time.Now()); err != nil {
		t.Fatalf("second snapshot: %v", err)
	}
	if bk.BidLevelCount() != 1 || bk.AskLevelCount() != 1 {
		t.Errorf("levels = %d/%d, want 1/1 (snapshot fully replaces)", bk.BidLevelCount(), bk.AskLevelCount())
	}
	tob := bk.BestBidAsk()
	if tob.BidPrice != px(200) || tob.AskPrice != px(201) {
		t.Errorf("BBO = %s/%s, want 200.00/201.00", tob.BidPrice, tob.AskPrice)
	}
}

// TestDeleteThenReinsertGeneration proves the heap's generation stamping:
// after delete-then-reinsert at the same price, the stale heap entry must not
// shadow the fresh level. Exercised purely through the public Book API.
func TestDeleteThenReinsertGeneration(t *testing.T) {
	bk := newInitBook(t) // bids 100/99
	// Delete best bid 100.00 -> best becomes 99.00.
	if _, err := bk.ApplyIncremental(book.Incremental{Side: book.Bid, Price: px(100), Qty: 0}, time.Now()); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if tob := bk.BestBidAsk(); tob.BidPrice != px(99) {
		t.Fatalf("after delete best bid = %s, want 99.00", tob.BidPrice)
	}
	// Reinsert 100.00 with a fresh quantity.
	if _, err := bk.ApplyIncremental(book.Incremental{Side: book.Bid, Price: px(100), Qty: qty(9)}, time.Now()); err != nil {
		t.Fatalf("reinsert: %v", err)
	}
	tob := bk.BestBidAsk()
	if tob.BidPrice != px(100) || tob.BidQty != qty(9) {
		t.Errorf("after reinsert best bid = %s @ %s, want 100.00 @ 9 (generation must pick fresh level)", tob.BidPrice, tob.BidQty)
	}
	if bk.BidLevelCount() != 2 {
		t.Errorf("bid level count = %d, want 2", bk.BidLevelCount())
	}
}

func TestDuplicatePriceSnapshot(t *testing.T) {
	bk := book.New("x", "SYM", book.Config{})
	sn := book.Snapshot{Bids: []book.Level{lv(px(100), qty(1)), lv(px(100), qty(2))}}
	_, err := bk.ApplySnapshot(sn, time.Now())
	if !errors.Is(err, book.ErrDuplicatePrice) {
		t.Errorf("err = %v, want ErrDuplicatePrice", err)
	}
}

func TestInvalidQuantitySnapshot(t *testing.T) {
	bk := book.New("x", "SYM", book.Config{})
	sn := book.Snapshot{Bids: []book.Level{lv(px(100), 0)}}
	_, err := bk.ApplySnapshot(sn, time.Now())
	if !errors.Is(err, book.ErrInvalidQuantity) {
		t.Errorf("err = %v, want ErrInvalidQuantity", err)
	}
}

func TestCrossedSnapshotStrict(t *testing.T) {
	bk := book.New("x", "SYM", book.Config{CrossedPolicy: book.PolicyStrict})
	// bestBid(100) >= bestAsk(99) -> crossed.
	sn := book.Snapshot{
		Bids: []book.Level{lv(px(100), qty(1))},
		Asks: []book.Level{lv(px(99), qty(1))},
	}
	_, err := bk.ApplySnapshot(sn, time.Now())
	if !errors.Is(err, book.ErrCrossedBook) {
		t.Errorf("err = %v, want ErrCrossedBook", err)
	}
	if bk.Initialized() {
		t.Error("book should not be initialized after a rejected crossed snapshot")
	}
}

func TestCrossedSnapshotWarn(t *testing.T) {
	bk := book.New("x", "SYM", book.Config{CrossedPolicy: book.PolicyWarn})
	sn := book.Snapshot{
		Bids: []book.Level{lv(px(100), qty(1))},
		Asks: []book.Level{lv(px(99), qty(1))},
	}
	if _, err := bk.ApplySnapshot(sn, time.Now()); err != nil {
		t.Fatalf("warn policy should apply crossed snapshot, got err %v", err)
	}
	if !bk.Initialized() {
		t.Error("book should be initialized under warn policy")
	}
	if bk.CrossedCount == 0 {
		t.Error("CrossedCount = 0, want > 0 under warn policy")
	}
}

func TestIncrementalBeforeSnapshot(t *testing.T) {
	bk := book.New("x", "SYM", book.Config{})
	_, err := bk.ApplyIncremental(book.Incremental{Side: book.Bid, Price: px(100), Qty: qty(1)}, time.Now())
	if !errors.Is(err, book.ErrNotInitialized) {
		t.Errorf("err = %v, want ErrNotInitialized", err)
	}
}

func TestInvalidSideIncremental(t *testing.T) {
	bk := newInitBook(t)
	_, err := bk.ApplyIncremental(book.Incremental{Side: book.Side(0), Price: px(100), Qty: qty(1)}, time.Now())
	if !errors.Is(err, book.ErrInvalidSide) {
		t.Errorf("err = %v, want ErrInvalidSide", err)
	}
}

func TestVersionMonotonic(t *testing.T) {
	bk := newInitBook(t)
	v := bk.Version()
	for i := 0; i < 5; i++ {
		if _, err := bk.ApplyIncremental(book.Incremental{Side: book.Bid, Price: px(100), Qty: qty(int64(i + 1))}, time.Now()); err != nil {
			t.Fatalf("apply: %v", err)
		}
		if bk.Version() != v+1 {
			t.Fatalf("version = %d, want %d", bk.Version(), v+1)
		}
		v++
	}
}
