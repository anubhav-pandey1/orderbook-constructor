package feed_test

import (
	"context"
	"os"
	"testing"

	"orderbook/book"
	"orderbook/feed"
)

// TestGoldenFixture replays the supplied CSV through the book and asserts the
// independently-verified oracles (DESIGN §17). Strict crossed-book and strict
// timestamp policies mean the run also proves the fixture is never crossed and
// is strictly increasing (either would return an error).
func TestGoldenFixture(t *testing.T) {
	f, err := os.Open("../btc_orderbook_updates.csv")
	if err != nil {
		t.Fatalf("open fixture: %v", err)
	}
	defer f.Close()

	dec := feed.NewDecoder(f)
	bk := book.New("binance", "BTC/USDT", book.Config{CrossedPolicy: book.PolicyStrict, LevelHint: 512})

	var events int
	var lastVersion uint64
	sink := feed.SinkFunc(func(e book.BookEvent) error {
		events++
		if e.Version != lastVersion+1 {
			t.Errorf("version gap: got %d after %d", e.Version, lastVersion)
		}
		lastVersion = e.Version
		return nil
	})

	sync := &feed.TimestampPolicy{Mode: book.PolicyStrict}
	st, err := feed.Replay(context.Background(), dec, bk, sink, feed.Config{Mode: feed.Fast, Sync: sync}, feed.RealClock{})
	if err != nil {
		t.Fatalf("replay: %v", err)
	}

	checks := []struct {
		name      string
		got, want int64
	}{
		{"accepted", int64(st.Accepted), 2242},
		{"version", int64(bk.Version()), 2242},
		{"events", int64(events), 2242},
		{"snapshots", int64(st.Snapshots), 2},
		{"incrementals", int64(st.Incrementals), 2240},
		{"deletes", int64(st.Deletes), 543},
		{"bid levels", int64(bk.BidLevelCount()), 227},
		{"ask levels", int64(bk.AskLevelCount()), 301},
	}
	for _, c := range checks {
		if c.got != c.want {
			t.Errorf("%s = %d, want %d", c.name, c.got, c.want)
		}
	}

	tob := bk.BestBidAsk()
	if !tob.HasBid || !tob.HasAsk {
		t.Fatalf("expected both sides present, got %+v", tob)
	}
	if tob.BidPrice != 9999399 {
		t.Errorf("best bid = %s, want 99993.99", tob.BidPrice)
	}
	if tob.BidQty != 21802 {
		t.Errorf("best bid qty = %s, want 2.1802", tob.BidQty)
	}
	if tob.AskPrice != 9999824 {
		t.Errorf("best ask = %s, want 99998.24", tob.AskPrice)
	}
	if tob.AskQty != 15550 {
		t.Errorf("best ask qty = %s, want 1.5550", tob.AskQty)
	}
}
