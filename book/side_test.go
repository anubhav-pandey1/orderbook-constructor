package book_test

import (
	"testing"
	"time"

	"orderbook/book"
)

// Scaled helpers: prices are cents (x100), quantities are 1e-4 units (x10000).
func px(dollars int64) book.Price                 { return book.Price(dollars * 100) }
func qty(units int64) book.Quantity               { return book.Quantity(units * 10000) }
func lv(p book.Price, q book.Quantity) book.Level { return book.Level{Price: p, Qty: q} }

// newInitBook builds a two-level-per-side book so single-side ops have context.
func newInitBook(t *testing.T) *book.Book {
	t.Helper()
	bk := book.New("x", "SYM", book.Config{CrossedPolicy: book.PolicyStrict})
	sn := book.Snapshot{
		Bids: []book.Level{lv(px(100), qty(1)), lv(px(99), qty(2))},
		Asks: []book.Level{lv(px(101), qty(1)), lv(px(102), qty(2))},
	}
	if _, err := bk.ApplySnapshot(sn, time.Now()); err != nil {
		t.Fatalf("ApplySnapshot: %v", err)
	}
	return bk
}

func TestInsertNewLevel(t *testing.T) {
	bk := newInitBook(t)
	// Insert a new, non-best bid.
	if _, err := bk.ApplyIncremental(book.Incremental{Side: book.Bid, Price: px(98), Qty: qty(3)}, time.Now()); err != nil {
		t.Fatalf("insert: %v", err)
	}
	if got := bk.BidLevelCount(); got != 3 {
		t.Errorf("bid level count = %d, want 3", got)
	}
	if tob := bk.BestBidAsk(); tob.BidPrice != px(100) {
		t.Errorf("best bid = %s, want 100.00", tob.BidPrice)
	}

	// Insert a new best bid.
	if _, err := bk.ApplyIncremental(book.Incremental{Side: book.Bid, Price: px(100) + 50, Qty: qty(4)}, time.Now()); err != nil {
		t.Fatalf("insert best: %v", err)
	}
	tob := bk.BestBidAsk()
	if tob.BidPrice != px(100)+50 {
		t.Errorf("best bid = %s, want 100.50", tob.BidPrice)
	}
	if tob.BidQty != qty(4) {
		t.Errorf("best bid qty = %s, want 4", tob.BidQty)
	}
	if got := bk.BidLevelCount(); got != 4 {
		t.Errorf("bid level count = %d, want 4", got)
	}
}

func TestReplaceExistingQty(t *testing.T) {
	bk := newInitBook(t)
	if _, err := bk.ApplyIncremental(book.Incremental{Side: book.Bid, Price: px(100), Qty: qty(5)}, time.Now()); err != nil {
		t.Fatalf("replace: %v", err)
	}
	if got := bk.BidLevelCount(); got != 2 {
		t.Errorf("bid level count = %d, want 2 (replace must not add a level)", got)
	}
	tob := bk.BestBidAsk()
	if tob.BidPrice != px(100) || tob.BidQty != qty(5) {
		t.Errorf("best bid = %s @ %s, want 100.00 @ 5", tob.BidPrice, tob.BidQty)
	}
}

func TestDeleteLevel(t *testing.T) {
	bk := newInitBook(t)
	// Delete a non-best level (Qty==0 deletes).
	if _, err := bk.ApplyIncremental(book.Incremental{Side: book.Bid, Price: px(99), Qty: 0}, time.Now()); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if got := bk.BidLevelCount(); got != 1 {
		t.Errorf("bid level count = %d, want 1", got)
	}
	if tob := bk.BestBidAsk(); tob.BidPrice != px(100) {
		t.Errorf("best bid = %s, want 100.00", tob.BidPrice)
	}
}

func TestDeleteAbsentLevel(t *testing.T) {
	bk := newInitBook(t)
	v0 := bk.Version()
	cnt0 := bk.BidLevelCount()
	// Deleting a price that is not present: no error, version bumps, count unchanged.
	if _, err := bk.ApplyIncremental(book.Incremental{Side: book.Bid, Price: px(50), Qty: 0}, time.Now()); err != nil {
		t.Fatalf("delete absent: %v", err)
	}
	if got := bk.Version(); got != v0+1 {
		t.Errorf("version = %d, want %d (delete-absent must still bump version)", got, v0+1)
	}
	if got := bk.BidLevelCount(); got != cnt0 {
		t.Errorf("bid level count = %d, want %d (unchanged)", got, cnt0)
	}
}

func TestDeleteBestNextBecomesBest(t *testing.T) {
	bk := newInitBook(t)
	// Delete the current best bid (100.00) -> next-best (99.00) becomes best.
	if _, err := bk.ApplyIncremental(book.Incremental{Side: book.Bid, Price: px(100), Qty: 0}, time.Now()); err != nil {
		t.Fatalf("delete best: %v", err)
	}
	tob := bk.BestBidAsk()
	if tob.BidPrice != px(99) || tob.BidQty != qty(2) {
		t.Errorf("best bid = %s @ %s, want 99.00 @ 2", tob.BidPrice, tob.BidQty)
	}

	// Delete the current best ask (101.00) -> next-best (102.00) becomes best.
	if _, err := bk.ApplyIncremental(book.Incremental{Side: book.Ask, Price: px(101), Qty: 0}, time.Now()); err != nil {
		t.Fatalf("delete best ask: %v", err)
	}
	if tob := bk.BestBidAsk(); tob.AskPrice != px(102) {
		t.Errorf("best ask = %s, want 102.00", tob.AskPrice)
	}
}

func TestOneSidedBook(t *testing.T) {
	// Bid-only snapshot.
	bk := book.New("x", "SYM", book.Config{})
	if _, err := bk.ApplySnapshot(book.Snapshot{Bids: []book.Level{lv(px(100), qty(1))}}, time.Now()); err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	tob := bk.BestBidAsk()
	if !tob.HasBid {
		t.Error("HasBid = false, want true")
	}
	if tob.HasAsk {
		t.Error("HasAsk = true, want false (no asks)")
	}

	// Ask-only snapshot (fully replaces).
	if _, err := bk.ApplySnapshot(book.Snapshot{Asks: []book.Level{lv(px(101), qty(1))}}, time.Now()); err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	tob = bk.BestBidAsk()
	if tob.HasBid {
		t.Error("HasBid = true, want false (bids replaced away)")
	}
	if !tob.HasAsk {
		t.Error("HasAsk = false, want true")
	}
}
