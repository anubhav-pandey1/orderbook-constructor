package book_test

import (
	"errors"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/anubhav-pandey1/orderbook-constructor/book"
)

func px(v int64) book.Price                       { return book.Price(v * book.PriceScale) }
func qty(v int64) book.Quantity                   { return book.Quantity(v * book.QtyScale) }
func lv(p book.Price, q book.Quantity) book.Level { return book.Level{Price: p, Qty: q} }

func initialized(t *testing.T) *book.Book {
	t.Helper()
	b := book.New(16)
	_, err := b.ApplySnapshot(&book.Snapshot{
		Bids: []book.Level{lv(px(99), qty(2)), lv(px(100), qty(1))},
		Asks: []book.Level{lv(px(102), qty(2)), lv(px(101), qty(1))},
	})
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func TestSnapshotReplacementBBOAndDepth(t *testing.T) {
	b := book.New(2)
	bbo, err := b.ApplySnapshot(&book.Snapshot{
		Bids: []book.Level{lv(px(98), qty(3)), lv(px(100), qty(1)), lv(px(99), qty(2))},
		Asks: []book.Level{lv(px(103), qty(3)), lv(px(101), qty(1)), lv(px(102), qty(2))},
	})
	if err != nil {
		t.Fatal(err)
	}
	want := book.BBO{BidPx: px(100), BidQty: qty(1), BidOK: true, AskPx: px(101), AskQty: qty(1), AskOK: true, Version: 1}
	if bbo != want || b.BBOSnapshot() != want {
		t.Fatalf("BBO/cache = %+v/%+v", bbo, b.BBOSnapshot())
	}
	depth := b.DepthSnapshot()
	if depth.Bids[0].Price != px(100) || depth.Asks[0].Price != px(101) {
		t.Fatalf("unsorted depth: %+v", depth)
	}
	bbo, err = b.ApplySnapshot(&book.Snapshot{Bids: []book.Level{lv(px(200), qty(7))}, Asks: []book.Level{lv(px(201), qty(8))}})
	if err != nil {
		t.Fatal(err)
	}
	if bbo.Version != 2 || b.BidLevelCount() != 1 || b.AskLevelCount() != 1 {
		t.Fatalf("replacement = %+v %d/%d", bbo, b.BidLevelCount(), b.AskLevelCount())
	}
}

func TestSnapshotValidationIsTransactional(t *testing.T) {
	b := initialized(t)
	wantBBO, wantDepth := b.BBOSnapshot(), b.DepthSnapshot()
	tests := []struct {
		name string
		sn   *book.Snapshot
		err  error
	}{
		{"nil", nil, book.ErrEmptySnapshot},
		{"empty", &book.Snapshot{}, book.ErrEmptySnapshot},
		{"duplicate", &book.Snapshot{Bids: []book.Level{lv(1, 1), lv(1, 2)}}, book.ErrDuplicatePrice},
		{"quantity", &book.Snapshot{Bids: []book.Level{lv(1, 0)}}, book.ErrInvalidQuantity},
		{"price", &book.Snapshot{Bids: []book.Level{lv(-1, 1)}}, book.ErrInvalidPrice},
		{"locked", &book.Snapshot{Bids: []book.Level{lv(10, 1)}, Asks: []book.Level{lv(10, 1)}}, book.ErrCrossedSnapshot},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := b.ApplySnapshot(tc.sn); !errors.Is(err, tc.err) {
				t.Fatalf("err=%v want %v", err, tc.err)
			}
			if b.BBOSnapshot() != wantBBO || !reflect.DeepEqual(b.DepthSnapshot(), wantDepth) {
				t.Fatal("rejected snapshot changed state")
			}
		})
	}
}

func TestOneSidedSnapshots(t *testing.T) {
	b := book.New(1)
	bbo, err := b.ApplySnapshot(&book.Snapshot{Bids: []book.Level{lv(100, 1)}})
	if err != nil || !bbo.BidOK || bbo.AskOK {
		t.Fatalf("bid-only=%+v %v", bbo, err)
	}
	bbo, err = b.ApplySnapshot(&book.Snapshot{Asks: []book.Level{lv(101, 2)}})
	if err != nil || bbo.BidOK || !bbo.AskOK || bbo.Version != 2 {
		t.Fatalf("ask-only=%+v %v", bbo, err)
	}
}

func TestDeltaKindsVersionsAndGeneration(t *testing.T) {
	b := initialized(t)
	cases := []struct {
		side    book.Side
		p       book.Price
		q       book.Quantity
		kind    book.DeltaKind
		version uint64
	}{
		{book.Bid, px(98), qty(3), book.LevelInserted, 2},
		{book.Bid, px(100), qty(5), book.LevelUpdated, 3},
		{book.Bid, px(100), 0, book.LevelDeleted, 4},
		{book.Bid, px(50), 0, book.AbsentDelete, 5},
		{book.Bid, px(100), qty(9), book.LevelInserted, 6},
	}
	for _, tc := range cases {
		r, err := b.ApplyDelta(tc.side, tc.p, tc.q)
		if err != nil || r.Kind != tc.kind || r.BBO.Version != tc.version {
			t.Fatalf("delta=%+v %v", r, err)
		}
	}
	bbo := b.BBOSnapshot()
	if bbo.BidPx != px(100) || bbo.BidQty != qty(9) || b.Version() != 6 {
		t.Fatalf("reinsert=%+v", bbo)
	}
}

func TestCrossedDeltaRejectsThenInvalidatePreservesVersion(t *testing.T) {
	b := initialized(t)
	before, depth := b.BBOSnapshot(), b.DepthSnapshot()
	r, err := b.ApplyDelta(book.Bid, px(101), qty(9))
	if !errors.Is(err, book.ErrCrossedDelta) || r.Kind != book.LevelInserted || r.BBO != before {
		t.Fatalf("cross=%+v %v", r, err)
	}
	if b.BBOSnapshot() != before || !reflect.DeepEqual(b.DepthSnapshot(), depth) {
		t.Fatal("cross committed")
	}
	b.Invalidate()
	got := b.BBOSnapshot()
	if got.BidOK || got.AskOK || got.Version != before.Version || b.BidLevelCount() != 0 || b.AskLevelCount() != 0 {
		t.Fatalf("invalidate=%+v", got)
	}
}

func TestDeltaValidationNoVersion(t *testing.T) {
	b := initialized(t)
	before := b.BBOSnapshot()
	for _, tc := range []struct {
		s book.Side
		p book.Price
		q book.Quantity
		e error
	}{{0, 1, 1, book.ErrInvalidSide}, {book.Bid, -1, 1, book.ErrInvalidPrice}, {book.Bid, 1, -1, book.ErrInvalidQuantity}} {
		if _, err := b.ApplyDelta(tc.s, tc.p, tc.q); !errors.Is(err, tc.e) {
			t.Fatalf("err=%v", err)
		}
	}
	if b.BBOSnapshot() != before {
		t.Fatal("invalid delta changed cache")
	}
}

func TestQuoteCacheConcurrentReaders(t *testing.T) {
	b := book.New(4)
	if _, err := b.ApplySnapshot(&book.Snapshot{Bids: []book.Level{lv(100, 1)}, Asks: []book.Level{lv(200, 1)}}); err != nil {
		t.Fatal(err)
	}
	var done atomic.Bool
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for !done.Load() {
				x := b.BBOSnapshot()
				if !x.BidOK || !x.AskOK || x.BidPx != 100 || x.AskPx != 200 || uint64(x.BidQty) != x.Version {
					t.Errorf("torn=%+v", x)
					return
				}
			}
		}()
	}
	for v := uint64(2); v <= 10000; v++ {
		if _, err := b.ApplyDelta(book.Bid, 100, book.Quantity(v)); err != nil {
			t.Fatal(err)
		}
	}
	done.Store(true)
	wg.Wait()
}
