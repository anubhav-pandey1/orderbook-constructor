package logx

import (
	"context"
	"errors"
	"orderbook/internal/ring"
	"os"
	"strings"
	"testing"
	"time"
)

func TestLoggerDrainOrderAndDrop(t *testing.T) {
	path := t.TempDir() + "/x.log"
	l, err := New(Config{Sink: SinkFile, File: path, RingSize: 4, BatchBytes: 32})
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- l.Run(context.Background()) }()
	for i := uint64(1); i <= 3; i++ {
		if err := l.Log(context.Background(), Record{NotificationID: i, Version: i, BidOK: true, BidPx: 9999, BidQty: 1}); err != nil {
			t.Fatal(err)
		}
	}
	_ = l.Close()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	data, _ := os.ReadFile(path)
	s := string(data)
	if !(strings.Index(s, "notification=1 ") < strings.Index(s, "notification=2 ") && strings.Index(s, "notification=2 ") < strings.Index(s, "notification=3 ")) {
		t.Fatal(s)
	}
	m := l.Metrics()
	if m.Enqueued != 3 || m.Written != 3 || m.Dropped != 0 {
		t.Fatal(m)
	}
	if err := l.Log(context.Background(), Record{}); !errors.Is(err, ring.ErrClosed) {
		t.Fatal(err)
	}
	d, _ := New(Config{Sink: SinkDiscard, Delivery: DropWhenFull, RingSize: 2})
	d.Log(context.Background(), Record{})
	d.Log(context.Background(), Record{})
	d.Log(context.Background(), Record{})
	if d.Metrics().Dropped != 1 {
		t.Fatal(d.Metrics())
	}
	_ = d.Close()
	if err := d.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestLoggerFlushInterval(t *testing.T) {
	path := t.TempDir() + "/interval.log"
	l, err := New(Config{
		Sink: SinkFile, File: path, RingSize: 4,
		BatchBytes: 1 << 20, FlushInterval: 5 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- l.Run(context.Background()) }()
	if err := l.Log(context.Background(), Record{NotificationID: 1}); err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(time.Second)
	for {
		data, readErr := os.ReadFile(path)
		if readErr != nil {
			t.Fatal(readErr)
		}
		if strings.Contains(string(data), "notification=1 ") {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("record was not flushed on the configured interval")
		}
		time.Sleep(5 * time.Millisecond)
	}
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}
