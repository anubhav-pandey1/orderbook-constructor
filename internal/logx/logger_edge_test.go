package logx

import (
	"bytes"
	"context"
	"errors"
	"os"
	"strings"
	"testing"

	"github.com/anubhav-pandey1/orderbook-constructor/book"
	"github.com/anubhav-pandey1/orderbook-constructor/internal/ring"
	"github.com/anubhav-pandey1/orderbook-constructor/replay"
)

func TestLoggerConfigValidation(t *testing.T) {
	for _, tc := range []struct {
		cfg  Config
		want string
	}{
		{Config{Sink: SinkFile}, "file path required"},
		{Config{Sink: Sink(99)}, "invalid sink"},
		{Config{Sink: SinkDiscard, RingSize: 3}, "capacity"},
		{Config{Sink: SinkDiscard, SpinIters: -1}, "negative configuration"},
		{Config{Sink: SinkDiscard, FlushInterval: -1}, "negative configuration"},
	} {
		if l, err := New(tc.cfg); err == nil || !strings.Contains(err.Error(), tc.want) {
			if l != nil {
				_ = l.Close()
			}
			t.Fatalf("config=%+v err=%v want %q", tc.cfg, err, tc.want)
		}
	}
}

func TestParseSinkAndAppendRecord(t *testing.T) {
	for _, s := range []string{"stdout", "file", "discard"} {
		if _, err := ParseSink(s); err != nil {
			t.Fatalf("ParseSink(%q): %v", s, err)
		}
	}
	if _, err := ParseSink("bad"); err == nil || !strings.Contains(err.Error(), "invalid sink") {
		t.Fatalf("bad sink err=%v", err)
	}
	var buf []byte
	rec := Record{NotificationID: 7, Version: 8, SyncEpoch: 9, Kind: replay.IncrementalApplied, State: replay.Synchronized, Reason: replay.ReasonNone, BidOK: true, AskOK: true, BidPx: book.Price(10000), AskPx: book.Price(10100), BidQty: book.Quantity(20000), AskQty: book.Quantity(30000), EventTS: 1, DueNS: 2, IngressNS: 3, ApplyNS: 4, RecvNS: 5}
	buf = appendRecord(buf, rec)
	text := string(buf)
	want := "notification=7 version=8 epoch=9 kind=2 state=1 reason=0 ts=1 due_ns=2 ingress_ns=3 apply_ns=4 recv_ns=5 bid=100.00x2.0000 ask=101.00x3.0000\n"
	if text != want {
		t.Fatalf("record=%q want %q", text, want)
	}
	allocs := testing.AllocsPerRun(1000, func() {
		local := make([]byte, 0, 384)
		local = appendRecord(local, rec)
		if len(local) == 0 {
			t.Fatal("empty record")
		}
	})
	if allocs != 0 {
		t.Fatalf("appendRecord allocs=%v", allocs)
	}
}

func TestLoggerRunContextCancellation(t *testing.T) {
	l, err := New(Config{Sink: SinkDiscard, RingSize: 2})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- l.Run(ctx) }()
	cancel()
	err = <-done
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err=%v", err)
	}
}

func TestLoggerDropWhenFullMetricsAndClosedLog(t *testing.T) {
	l, err := New(Config{Sink: SinkDiscard, RingSize: 2, Delivery: DropWhenFull})
	if err != nil {
		t.Fatal(err)
	}
	if err := l.Log(context.Background(), Record{NotificationID: 1}); err != nil {
		t.Fatal(err)
	}
	if err := l.Log(context.Background(), Record{NotificationID: 2}); err != nil {
		t.Fatal(err)
	}
	if err := l.Log(context.Background(), Record{NotificationID: 3}); err != nil {
		t.Fatal(err)
	}
	m := l.Metrics()
	if m.Enqueued != 2 || m.Written != 0 || m.Dropped != 1 || m.MaxDepth != 2 || m.WaitCount != 0 {
		t.Fatalf("metrics=%+v", m)
	}
	_ = l.Close()
	if err := l.Log(context.Background(), Record{}); !errors.Is(err, ring.ErrClosed) {
		t.Fatalf("closed log err=%v", err)
	}
}

func TestLoggerFileCreateErrorAndFlushOutput(t *testing.T) {
	dir := t.TempDir()
	if _, err := New(Config{Sink: SinkFile, File: dir}); err == nil || !strings.Contains(err.Error(), dir) {
		t.Fatalf("create directory err=%v want path %q", err, dir)
	}
	path := dir + "/records.log"
	l, err := New(Config{Sink: SinkFile, File: path, RingSize: 2, BatchBytes: 1})
	if err != nil {
		t.Fatal(err)
	}
	if err := l.Log(context.Background(), Record{NotificationID: 1}); err != nil {
		t.Fatal(err)
	}
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	if err := l.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(data, []byte("notification=1 ")) {
		t.Fatalf("log data=%q", data)
	}
}
