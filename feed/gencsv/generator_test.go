package gencsv_test

import (
	"bytes"
	"context"
	"os"
	"testing"

	"orderbook/book"
	"orderbook/feed"
	"orderbook/feed/gencsv"
)

func TestGeneratedCSVReplay(t *testing.T) {
	cfg := gencsv.DefaultConfig()
	cfg.Incrementals = 5000
	cfg.SnapshotEvery = 2500

	var buf bytes.Buffer
	if err := gencsv.Write(&buf, cfg); err != nil {
		t.Fatalf("write: %v", err)
	}

	dec := feed.NewDecoder(&buf)
	bk := book.New("binance", "BTC/USDT", book.Config{CrossedPolicy: book.PolicyStrict, LevelHint: 512})
	sync := &feed.TimestampPolicy{Mode: book.PolicyStrict}

	st, err := feed.Replay(context.Background(), dec, bk, nil, feed.Config{Mode: feed.Fast, Sync: sync}, feed.RealClock{})
	if err != nil {
		t.Fatalf("replay: %v", err)
	}

	midSnapshots := cfg.Incrementals / cfg.SnapshotEvery
	wantSnapshots := 1 + midSnapshots
	wantIncrementals := cfg.Incrementals - midSnapshots
	wantAccepted := wantSnapshots + wantIncrementals

	if int64(st.Accepted) != wantAccepted {
		t.Fatalf("accepted = %d, want %d", st.Accepted, wantAccepted)
	}
	if st.Snapshots != uint64(wantSnapshots) {
		t.Fatalf("snapshots = %d, want %d", st.Snapshots, wantSnapshots)
	}
	if st.Incrementals != uint64(wantIncrementals) {
		t.Fatalf("incrementals = %d, want %d", st.Incrementals, wantIncrementals)
	}

	tob := bk.BestBidAsk()
	if !tob.HasBid || !tob.HasAsk || tob.BidPrice >= tob.AskPrice {
		t.Fatalf("final book crossed or empty: %+v", tob)
	}
}

func TestGeneratedCSVStressNoCross(t *testing.T) {
	cfg := gencsv.DefaultConfig()
	cfg.Incrementals = 100_000
	cfg.SnapshotEvery = 25_000

	var buf bytes.Buffer
	if err := gencsv.Write(&buf, cfg); err != nil {
		t.Fatalf("write: %v", err)
	}

	dec := feed.NewDecoder(&buf)
	bk := book.New("binance", "BTC/USDT", book.Config{CrossedPolicy: book.PolicyStrict, LevelHint: 512})
	sync := &feed.TimestampPolicy{Mode: book.PolicyStrict}

	if _, err := feed.Replay(context.Background(), dec, bk, nil, feed.Config{Mode: feed.Fast, Sync: sync}, feed.RealClock{}); err != nil {
		t.Fatalf("replay: %v", err)
	}
}

func TestGeneratedCSVFormatSample(t *testing.T) {
	cfg := gencsv.DefaultConfig()
	cfg.Incrementals = 3

	dir := t.TempDir()
	path := dir + "/sample.csv"
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := gencsv.Write(f, cfg); err != nil {
		t.Fatal(err)
	}
	f.Close()

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	text := string(data)
	if len(text) < 200 {
		t.Fatalf("sample too short: %q", text)
	}
	if text[:len("type,exchange,symbol,timestamp,side,bids,asks,price,size\n")] != "type,exchange,symbol,timestamp,side,bids,asks,price,size\n" {
		t.Fatalf("bad header: %q", text[:80])
	}
}
