package feed_test

import (
	"context"
	"io"
	"os"
	"sort"
	"testing"
	"time"

	"orderbook/book"
	"orderbook/feed"
	"orderbook/internal/pipeline"
	"orderbook/internal/ring"
	"orderbook/internal/syncx"
)

func TestGoldenFixture(t *testing.T) {
	f, err := os.Open("../btc_orderbook_updates.csv")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	out, err := ring.NewSPSC[pipeline.Event](4096)
	if err != nil {
		t.Fatal(err)
	}
	bk := book.New(512)
	stats, err := feed.Replay(context.Background(), feed.NewDecoder(f), bk,
		syncx.NewTimestampPolicy(syncx.TimestampStep, 100), nil, out,
		feed.ReplayCfg{Mode: feed.Fast, Speed: 1, TSUnit: time.Millisecond, SpinIters: 16, Stream: fixtureStream}, &testClock{now: 1000})
	if err != nil {
		t.Fatal(err)
	}
	events := drainEvents(out)
	depth := bk.DepthSnapshot()
	checks := []struct {
		name      string
		got, want int64
	}{
		{"applied", int64(stats.Applied), 2242}, {"version", int64(bk.Version()), 2242}, {"events", int64(len(events)), 2242},
		{"snapshots", int64(stats.Snapshots), 2}, {"deltas", int64(stats.Deltas), 2240}, {"deletes", int64(stats.Deletes), 543},
		{"absent deletes", int64(stats.AbsentDeletes), 5}, {"bids", int64(len(depth.Bids)), 227}, {"asks", int64(len(depth.Asks)), 301},
	}
	for _, c := range checks {
		if c.got != c.want {
			t.Errorf("%s=%d want=%d", c.name, c.got, c.want)
		}
	}
	if stats.Invalidated != 0 || stats.Discarded != 0 || stats.Crossed != 0 || stats.Gaps != 0 {
		t.Errorf("sync stats=%+v", stats)
	}
	for i, ev := range events {
		want := uint64(i + 1)
		if ev.NotificationID != want || ev.Version != want {
			t.Fatalf("event[%d]=%+v", i, ev)
		}
	}
	bbo := bk.BBOSnapshot()
	if !bbo.BidOK || !bbo.AskOK || bbo.BidPx != 9999399 || bbo.BidQty != 21802 || bbo.AskPx != 9999824 || bbo.AskQty != 15550 {
		t.Fatalf("final BBO=%+v", bbo)
	}
}

func TestGoldenFixtureAgainstNaiveReference(t *testing.T) {
	f, err := os.Open("../btc_orderbook_updates.csv")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	dec := feed.NewDecoder(f)
	bk := book.New(512)
	bids := make(map[book.Price]book.Quantity)
	asks := make(map[book.Price]book.Quantity)
	var lastTS int64
	var rows int
	for {
		rec, err := dec.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		if rows > 0 && rec.TS-lastTS != 100 {
			t.Fatalf("row %d timestamp step=%d, want 100", rows+1, rec.TS-lastTS)
		}
		lastTS = rec.TS

		var got book.BBO
		switch rec.Kind {
		case feed.KindSnapshot:
			bids = levelsMap(rec.Snap.Bids)
			asks = levelsMap(rec.Snap.Asks)
			got, err = bk.ApplySnapshot(rec.Snap)
		case feed.KindDelta:
			target := bids
			if rec.Side == book.Ask {
				target = asks
			}
			if rec.Qty == 0 {
				delete(target, rec.Px)
			} else {
				target[rec.Px] = rec.Qty
			}
			var result book.DeltaResult
			result, err = bk.ApplyDelta(rec.Side, rec.Px, rec.Qty)
			got = result.BBO
		default:
			t.Fatalf("row %d unknown kind %d", rows+1, rec.Kind)
		}
		if err != nil {
			t.Fatalf("row %d apply: %v", rows+1, err)
		}
		want := naiveBBO(bids, asks, uint64(rows+1))
		if got != want {
			t.Fatalf("row %d BBO=%+v, naive=%+v", rows+1, got, want)
		}
		rows++
	}
	if rows != 2242 {
		t.Fatalf("rows=%d, want 2242", rows)
	}
}

func levelsMap(levels []book.Level) map[book.Price]book.Quantity {
	out := make(map[book.Price]book.Quantity, len(levels))
	for _, level := range levels {
		out[level.Price] = level.Qty
	}
	return out
}

func naiveBBO(bids, asks map[book.Price]book.Quantity, version uint64) book.BBO {
	bidPrices := make([]book.Price, 0, len(bids))
	for px := range bids {
		bidPrices = append(bidPrices, px)
	}
	askPrices := make([]book.Price, 0, len(asks))
	for px := range asks {
		askPrices = append(askPrices, px)
	}
	sort.Slice(bidPrices, func(i, j int) bool { return bidPrices[i] > bidPrices[j] })
	sort.Slice(askPrices, func(i, j int) bool { return askPrices[i] < askPrices[j] })
	out := book.BBO{Version: version}
	if len(bidPrices) != 0 {
		out.BidPx, out.BidQty, out.BidOK = bidPrices[0], bids[bidPrices[0]], true
	}
	if len(askPrices) != 0 {
		out.AskPx, out.AskQty, out.AskOK = askPrices[0], asks[askPrices[0]], true
	}
	return out
}
