package main

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/anubhav-pandey1/orderbook-constructor/book"
	"github.com/anubhav-pandey1/orderbook-constructor/feed"
)

func TestConfigValidateEdges(t *testing.T) {
	valid := config{csvPath: "x", exchange: "binance", symbol: "BTCUSDT", fixtureIters: 1, synthetic: 1, snapshotEvery: 1, syntheticMax: 10, eventRing: 2, logRing: 2, pacedSpeed: 1}
	if err := valid.validate(); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		mutate func(*config)
		want   string
	}{
		{func(c *config) { c.fixtureIters = 0 }, "invalid non-positive"},
		{func(c *config) { c.warmup = -1 }, "invalid non-positive"},
		{func(c *config) { c.synthetic = 0 }, "invalid non-positive"},
		{func(c *config) { c.snapshotEvery = 0 }, "invalid non-positive"},
		{func(c *config) { c.syntheticMax = 9 }, "invalid non-positive"},
		{func(c *config) { c.spin = -1 }, "invalid non-positive"},
		{func(c *config) { c.pacedSpeed = 0 }, "invalid non-positive"},
		{func(c *config) { c.eventRing = 3 }, "event-ring"},
		{func(c *config) { c.logRing = 3 }, "log-ring"},
	} {
		c := valid
		tc.mutate(&c)
		if err := c.validate(); err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Fatalf("config=%+v err=%v want %q", c, err, tc.want)
		}
	}
}

func TestMeasurementHelpers(t *testing.T) {
	m := measurement{name: "x", n: 10, duration: time.Second, mallocs: 20, bytes: 30}
	if m.rate() != 10 || m.allocsPerOp() != 2 || m.bytesPerOp() != 3 {
		t.Fatalf("measurement=%+v rate=%v alloc=%v bytes=%v", m, m.rate(), m.allocsPerOp(), m.bytesPerOp())
	}
	if (measurement{}).allocsPerOp() != 0 || (measurement{}).bytesPerOp() != 0 {
		t.Fatal("zero measurement should have zero per-op values")
	}
	if minInt(1, 2) != 1 || minInt64(3, 2) != 2 || maxUint64(3, 4) != 4 {
		t.Fatal("compat helpers failed")
	}
	if rate(1_500_000) != "1.50M" || rate(1_500) != "1.5k" || rate(9) != "9" {
		t.Fatal("rate formatting failed")
	}
}

func TestDecodeAndApplyErrorPaths(t *testing.T) {
	if _, err := decodeAll([]byte("bad\n")); err == nil || !strings.Contains(err.Error(), "feed header") {
		t.Fatalf("decodeAll err=%v", err)
	}
	records := []feed.Record{
		{Kind: feed.KindSnapshot, Snap: &book.Snapshot{Bids: []book.Level{{Price: 100, Qty: 1}}, Asks: []book.Level{{Price: 101, Qty: 1}}}},
		{Kind: feed.KindDelta, Side: book.Bid, Px: 102, Qty: 1},
	}
	if n, err := applyOnce(records); !errors.Is(err, book.ErrCrossedDelta) || n != 1 {
		t.Fatalf("applyOnce n/err=%d/%v", n, err)
	}
	if n, err := decodeApplyOnce([]byte("bad\n")); err == nil || !strings.Contains(err.Error(), "feed header") || n != 0 {
		t.Fatalf("decodeApplyOnce n/err=%d/%v", n, err)
	}
}

func TestRepeatMeasuredAndBackpressure(t *testing.T) {
	sentinel := errors.New("boom")
	if _, err := repeatMeasured("x", 1, func() (uint64, error) { return 0, sentinel }); !errors.Is(err, sentinel) {
		t.Fatalf("err=%v", err)
	}
	m, err := repeatMeasured("x", 2, func() (uint64, error) { return 3, nil })
	if err != nil {
		t.Fatal(err)
	}
	if m.n != 6 || m.name != "x" {
		t.Fatalf("measurement=%+v", m)
	}
	bp, err := runBackpressure(128, 8, 0)
	if err != nil {
		t.Fatal(err)
	}
	if bp.n != 128 || bp.maxDepth == 0 || bp.maxDepth > 8 {
		t.Fatalf("backpressure=%+v", bp)
	}
}

func TestRunSuiteSmallIntegration(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "feed.csv")
	data := strings.Join([]string{
		"type,exchange,symbol,timestamp,side,bids,asks,price,size",
		`snapshot,binance,BTC/USDT,1700000000000,,"[[100.00,1.0000]]","[[101.00,1.0000]]",,`,
		`incremental,binance,BTC/USDT,1700000000100,bid,,,100.00,2.0000`,
	}, "\n") + "\n"
	if err := os.WriteFile(path, []byte(data), 0o600); err != nil {
		t.Fatal(err)
	}
	report, err := runSuite(config{
		csvPath: path, exchange: "binance", symbol: "BTCUSDT",
		fixtureIters: 1, synthetic: 10, snapshotEvery: 5, syntheticMax: 10,
		eventRing: 16, logRing: 16, spin: 0, pacedSpeed: 1000000,
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"# Benchmark Report", "W2 apply-only fixture mix", "Queueing and backpressure"} {
		if !strings.Contains(report, want) {
			t.Fatalf("report missing %q", want)
		}
	}
}
