package gencsv_test

import (
	"bytes"
	"context"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/anubhav-pandey1/orderbook-constructor/book"
	"github.com/anubhav-pandey1/orderbook-constructor/feed"
	"github.com/anubhav-pandey1/orderbook-constructor/feed/gencsv"
	"github.com/anubhav-pandey1/orderbook-constructor/replay"
)

func TestRecordGeneratorDeterministic(t *testing.T) {
	cfg := gencsv.DefaultConfig()
	cfg.Incrementals, cfg.SnapshotEvery = 2_000, 500
	a, err := gencsv.NewGenerator(cfg)
	if err != nil {
		t.Fatal(err)
	}
	b, err := gencsv.NewGenerator(cfg)
	if err != nil {
		t.Fatal(err)
	}
	var n int64
	for {
		left, lok := a.Next()
		right, rok := b.Next()
		if lok != rok {
			t.Fatalf("length differs at %d", n)
		}
		if !lok {
			break
		}
		if !reflect.DeepEqual(left, right) {
			t.Fatalf("record %d differs", n)
		}
		if !left.HasUpdateID || left.FinalUpdateID != uint64(n+1) {
			t.Fatalf("record %d ID=%d", n, left.FinalUpdateID)
		}
		n++
	}
	if n != cfg.Incrementals+1 {
		t.Fatalf("records=%d want=%d", n, cfg.Incrementals+1)
	}
}

func TestGeneratedCSVReplay(t *testing.T) {
	cfg := gencsv.DefaultConfig()
	cfg.Incrementals = 5000
	cfg.SnapshotEvery = 2500

	var buf bytes.Buffer
	if err := gencsv.Write(&buf, cfg); err != nil {
		t.Fatalf("write: %v", err)
	}

	dec := feed.NewDecoder(&buf)
	bk := book.New(512)
	st, err := replay.Run(context.Background(), dec, bk, nil, replay.Options{
		Mode: replay.Fast, Speed: 1, TimestampUnit: time.Millisecond,
		Stream: feed.StreamID{Exchange: "binance", Symbol: "BTCUSDT"},
		Policy: replay.NewTimestampPolicy(replay.TimestampStep, cfg.TSStep),
	})
	if err != nil {
		t.Fatalf("replay: %v", err)
	}

	midSnapshots := cfg.Incrementals / cfg.SnapshotEvery
	wantSnapshots := 1 + midSnapshots
	wantIncrementals := cfg.Incrementals - midSnapshots
	wantAccepted := wantSnapshots + wantIncrementals

	if int64(st.Applied) != wantAccepted {
		t.Fatalf("accepted = %d, want %d", st.Applied, wantAccepted)
	}
	if st.Snapshots != uint64(wantSnapshots) {
		t.Fatalf("snapshots = %d, want %d", st.Snapshots, wantSnapshots)
	}
	if st.Deltas != uint64(wantIncrementals) {
		t.Fatalf("incrementals = %d, want %d", st.Deltas, wantIncrementals)
	}

	bbo := bk.BBOSnapshot()
	if !bbo.BidOK || !bbo.AskOK || bbo.BidPx >= bbo.AskPx {
		t.Fatalf("final book crossed or empty: %+v", bbo)
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
	bk := book.New(512)
	if _, err := replay.Run(context.Background(), dec, bk, nil, replay.Options{
		Mode: replay.Fast, Speed: 1, TimestampUnit: time.Millisecond,
		Stream: feed.StreamID{Exchange: "binance", Symbol: "BTCUSDT"},
		Policy: replay.NewTimestampPolicy(replay.TimestampStep, cfg.TSStep),
	}); err != nil {
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
