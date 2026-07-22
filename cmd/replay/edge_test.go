package main

import (
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/anubhav-pandey1/orderbook-constructor/internal/logx"
	"github.com/anubhav-pandey1/orderbook-constructor/replay"
)

func TestParseReplayModeAndLogDeliveryErrors(t *testing.T) {
	if mode, err := parseReplayMode("fast"); err != nil || mode != replay.Fast {
		t.Fatalf("fast mode=%v err=%v", mode, err)
	}
	if mode, err := parseReplayMode("paced"); err != nil || mode != replay.Paced {
		t.Fatalf("paced mode=%v err=%v", mode, err)
	}
	if _, err := parseReplayMode("bad"); err == nil || !strings.Contains(err.Error(), "invalid replay mode") {
		t.Fatalf("bad replay mode err=%v", err)
	}
	if delivery, err := parseLogDelivery("lossless"); err != nil || delivery != logx.Lossless {
		t.Fatalf("delivery=%v err=%v", delivery, err)
	}
	if delivery, err := parseLogDelivery("drop"); err != nil || delivery != logx.DropWhenFull {
		t.Fatalf("delivery=%v err=%v", delivery, err)
	}
	if _, err := parseLogDelivery("bad"); err == nil || !strings.Contains(err.Error(), "invalid log delivery") {
		t.Fatalf("bad delivery err=%v", err)
	}
}

func TestParseTimestampUnitErrors(t *testing.T) {
	if _, err := parseTimestampUnit("bad", "ignored"); err == nil || !strings.Contains(err.Error(), "invalid timestamp unit") {
		t.Fatalf("bad timestamp unit err=%v", err)
	}
	path := filepath.Join(t.TempDir(), "feed.csv")
	writeFixture(t, path, "snapshot,binance,BTC/USDT,12345,,\"[[100.00,1.0000]]\",\"[[101.00,1.0000]]\",,\n")
	if _, err := parseTimestampUnit("auto", path); err == nil || !strings.Contains(err.Error(), "cannot infer") {
		t.Fatalf("err=%v", err)
	}
	if _, err := parseTimestampUnit("auto", path+".missing"); err == nil || !strings.Contains(err.Error(), "detect timestamp unit") {
		t.Fatalf("missing file err=%v", err)
	}
}

func TestRunRejectsInvalidRuntimeInputsBeforeMutation(t *testing.T) {
	input := filepath.Join(t.TempDir(), "feed.csv")
	writeFixture(t, input, "snapshot,binance,BTC/USDT,1700000000000,,\"[[100.00,1.0000]]\",\"[[101.00,1.0000]]\",,\n")
	for _, tc := range []struct {
		args []string
		want string
	}{
		{[]string{"-speed", "NaN"}, "speed must be finite"},
		{[]string{"-speed", "+Inf"}, "speed must be finite"},
		{[]string{"-spin", "-1"}, "spin must be non-negative"},
		{[]string{"-gomaxprocs", "-1"}, "gomaxprocs must be non-negative"},
		{[]string{"-csv", input, "-event-ring", "3"}, "event ring"},
		{[]string{"-csv", input, "-log-ring", "3"}, "logger"},
		{[]string{"-csv", input, "-exchange", ""}, "stream identity"},
		{[]string{"extra"}, "unexpected positional arguments"},
	} {
		if err := run(tc.args); err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Fatalf("args=%v err=%v want %q", tc.args, err, tc.want)
		}
	}
}

func TestRunPacedInvalidTimestampStep(t *testing.T) {
	dir := t.TempDir()
	input := filepath.Join(dir, "feed.csv")
	writeFixture(t, input, "snapshot,binance,BTC/USDT,1700000000000,,\"[[100.00,1.0000]]\",\"[[101.00,1.0000]]\",,\n")
	err := run([]string{"-csv", input, "-replay", "paced", "-timestamp-unit", "ms", "-timestamp-step", "1500us", "-log", "discard"})
	if err == nil || !strings.Contains(err.Error(), "exact multiple") {
		t.Fatalf("err=%v", err)
	}
}

func TestRunRejectsLogAliasBeforeCreate(t *testing.T) {
	dir := t.TempDir()
	input := filepath.Join(dir, "feed.csv")
	writeFixture(t, input, "snapshot,binance,BTC/USDT,1700000000000,,\"[[100.00,1.0000]]\",\"[[101.00,1.0000]]\",,\n")
	output := filepath.Join(dir, "missing", "events.log")
	err := run([]string{"-csv", input, "-log", "file", "-log-file", output})
	if err == nil {
		t.Fatal("expected log create error")
	}
	if !strings.Contains(err.Error(), "logger") {
		t.Fatalf("err=%v", err)
	}
}

func TestDecimalDigitsMoreBoundaries(t *testing.T) {
	for _, tc := range []struct {
		v    int64
		want int
	}{
		{9, 1},
		{10, 2},
		{-9, 1},
		{-10, 2},
	} {
		if got := decimalDigits(tc.v); got != tc.want {
			t.Fatalf("digits(%d)=%d want %d", tc.v, got, tc.want)
		}
	}
}

func TestParseSyncPolicyTimestampMonotonic(t *testing.T) {
	p, err := parseSyncPolicy("timestamp", "monotonic", 0, time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	p.AcceptSnapshot(replay.Cursor{Timestamp: 1000})
	if got := p.ClassifyUpdate(replay.Cursor{Timestamp: 1500}); got.Action != replay.Apply {
		t.Fatalf("monotonic policy decision=%+v", got)
	}
	if _, err := parseSyncPolicy("timestamp", "step", 0, time.Millisecond); err == nil || !strings.Contains(err.Error(), "timestamp step") {
		t.Fatalf("zero step err=%v", err)
	}
}
