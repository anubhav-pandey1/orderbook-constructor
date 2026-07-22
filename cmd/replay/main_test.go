package main

import (
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/anubhav-pandey1/orderbook-constructor/replay"
)

func TestFlagDefaultsMatchSpecification(t *testing.T) {
	cfg, err := parseFlags(nil)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.csvPath != "./testdata/btc_orderbook_updates.csv" || cfg.exchange != "binance" || cfg.symbol != "BTCUSDT" {
		t.Fatalf("unexpected input defaults: %+v", cfg)
	}
	if cfg.replayMode != "fast" || cfg.speed != 1 || cfg.timestampUnit != "auto" {
		t.Fatalf("unexpected replay defaults: %+v", cfg)
	}
	if cfg.syncPolicy != "timestamp" || cfg.timestampMode != "step" || cfg.timestampStep != 100*time.Millisecond {
		t.Fatalf("unexpected synchronization defaults: %+v", cfg)
	}
	if cfg.logSink != "stdout" || cfg.logDelivery != "lossless" || cfg.eventRing != 65536 || cfg.logRing != 65536 || cfg.spin != 128 {
		t.Fatalf("unexpected pipeline defaults: %+v", cfg)
	}
}

func TestParseSyncPolicyConfiguration(t *testing.T) {
	for _, tc := range []struct {
		name, mode, wantErr string
		step, unit          time.Duration
		wantDecision        replay.Decision
	}{
		{"timestamp", "step", "", 100 * time.Millisecond, time.Millisecond, replay.Decision{Action: replay.Apply}},
		{"timestamp", "monotonic", "", 0, time.Millisecond, replay.Decision{Action: replay.Apply}},
		{"timestamp", "step", "exact multiple", 1500 * time.Microsecond, time.Millisecond, replay.Decision{}},
		{"off", "ignored", "", 0, time.Millisecond, replay.Decision{Action: replay.Apply}},
		{"update-id", "step", "requires update-ID fields", time.Millisecond, time.Millisecond, replay.Decision{}},
		{"bogus", "step", "invalid sync policy", time.Millisecond, time.Millisecond, replay.Decision{}},
	} {
		p, err := parseSyncPolicy(tc.name, tc.mode, tc.step, tc.unit)
		if tc.wantErr != "" {
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Errorf("parseSyncPolicy(%q, %q) error=%v, want substring %q", tc.name, tc.mode, err, tc.wantErr)
			}
			continue
		}
		if err != nil {
			t.Errorf("parseSyncPolicy(%q, %q) unexpected error=%v", tc.name, tc.mode, err)
			continue
		}
		p.AcceptSnapshot(replay.Cursor{Timestamp: 1000})
		if got := p.ClassifyUpdate(replay.Cursor{Timestamp: 1100}); got != tc.wantDecision {
			t.Errorf("parseSyncPolicy(%q, %q) decision=%+v want %+v", tc.name, tc.mode, got, tc.wantDecision)
		}
	}
}

func TestDetectTimestampUnit(t *testing.T) {
	const header = "type,exchange,symbol,timestamp,side,bids,asks,price,size\n"
	for _, tc := range []struct {
		name, ts string
		want     time.Duration
	}{
		{"milliseconds", "1700000000000", time.Millisecond},
		{"microseconds", "1700000000000000", time.Microsecond},
		{"nanoseconds", "1700000000000000000", time.Nanosecond},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "feed.csv")
			row := "snapshot,binance,BTC/USDT," + tc.ts + ",,\"[[1.00,1.0000]]\",[],,\n"
			if err := os.WriteFile(path, []byte(header+row), 0o600); err != nil {
				t.Fatal(err)
			}
			got, err := detectTimestampUnit(path)
			if err != nil {
				t.Fatal(err)
			}
			if got != tc.want {
				t.Fatalf("unit=%s, want %s", got, tc.want)
			}
		})
	}
}

func TestDecimalDigitsHandlesInt64Range(t *testing.T) {
	if got := decimalDigits(math.MinInt64); got != 19 {
		t.Fatalf("MinInt64 digits=%d, want 19", got)
	}
	if got := decimalDigits(0); got != 1 {
		t.Fatalf("zero digits=%d, want 1", got)
	}
}

func TestRunDrainsPipelineInOrder(t *testing.T) {
	dir := t.TempDir()
	input := filepath.Join(dir, "feed.csv")
	output := filepath.Join(dir, "events.log")
	writeFixture(t, input,
		"snapshot,binance,BTC/USDT,1700000000000,,\"[[100.00,1.0000]]\",\"[[101.00,2.0000]]\",,\n"+
			"incremental,binance,BTC/USDT,1700000000100,bid,,,100.00,1.5000\n")

	err := run([]string{
		"-csv", input, "-log", "file", "-log-file", output,
		"-event-ring", "2", "-log-ring", "2",
	})
	if err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(output)
	if err != nil {
		t.Fatal(err)
	}
	text := string(data)
	if strings.Count(text, "\n") != 2 {
		t.Fatalf("log records = %q, want two lines", text)
	}
	first, second, _ := strings.Cut(text, "\n")
	if !strings.Contains(first, "notification=1") || !strings.Contains(first, "version=1") || !strings.Contains(first, "kind=1") {
		t.Fatalf("first notification incomplete: %q", first)
	}
	if !strings.Contains(second, "notification=2") || !strings.Contains(second, "version=2") || !strings.Contains(second, "kind=2") || !strings.Contains(second, "bid=100.00x1.5000") {
		t.Fatalf("notifications not ordered: %q", text)
	}
}

func TestFatalDecodeStillDrainsAcceptedEvents(t *testing.T) {
	dir := t.TempDir()
	input := filepath.Join(dir, "feed.csv")
	output := filepath.Join(dir, "events.log")
	writeFixture(t, input,
		"snapshot,binance,BTC/USDT,1700000000000,,\"[[100.00,1.0000]]\",\"[[101.00,2.0000]]\",,\n"+
			"not-a-kind,binance,BTC/USDT,1700000000100,bid,,,100.00,1.5000\n")

	err := run([]string{
		"-csv", input, "-log", "file", "-log-file", output,
		"-event-ring", "2", "-log-ring", "2",
	})
	if err == nil || !strings.Contains(err.Error(), "unknown type") {
		t.Fatalf("malformed row err=%v", err)
	}
	data, readErr := os.ReadFile(output)
	if readErr != nil {
		t.Fatal(readErr)
	}
	text := string(data)
	if strings.Count(text, "\n") != 1 || !strings.Contains(text, "notification=1") || !strings.Contains(text, "version=1") {
		t.Fatalf("accepted notification was not drained: %q", data)
	}
}

func TestLogFileCannotOverwriteInput(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "feed.csv")
	writeFixture(t, path, "snapshot,binance,BTC/USDT,1700000000000,,\"[[100.00,1.0000]]\",\"[[101.00,2.0000]]\",,\n")
	want, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := run([]string{"-csv", path, "-log", "file", "-log-file", path}); err == nil || !strings.Contains(err.Error(), "overwrite") {
		t.Fatalf("same-path overwrite err=%v", err)
	}
	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != string(want) {
		t.Fatal("input changed after rejected log configuration")
	}

	alias := filepath.Join(dir, "feed-alias.csv")
	if err := os.Symlink(path, alias); err != nil {
		t.Skipf("symlink creation is not permitted on this system: %v", err)
	}
	if err := run([]string{"-csv", path, "-log", "file", "-log-file", alias}); err == nil || !strings.Contains(err.Error(), "overwrite") {
		t.Fatalf("symlink overwrite err=%v", err)
	}
}

func writeFixture(t *testing.T, path, rows string) {
	t.Helper()
	const header = "type,exchange,symbol,timestamp,side,bids,asks,price,size\n"
	if err := os.WriteFile(path, []byte(header+rows), 0o600); err != nil {
		t.Fatal(err)
	}
}
