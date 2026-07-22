package clock

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestRealSleepPastAndCanceledContext(t *testing.T) {
	r := NewReal()
	if err := r.SleepUntilNS(context.Background(), r.NowNS()-1); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := r.SleepUntilNS(ctx, r.NowNS()+int64(time.Second)); !errors.Is(err, context.Canceled) {
		t.Fatalf("err=%v", err)
	}
	if err := r.SleepUntilNS(nil, r.NowNS()-1); err != nil {
		t.Fatal(err)
	}
}

func TestSimCancelNoopAdvanceAndMultipleWaiters(t *testing.T) {
	s := NewSim(10)
	s.Advance(-time.Second)
	if s.NowNS() != 10 {
		t.Fatalf("now=%d", s.NowNS())
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- s.SleepUntilNS(ctx, 20) }()
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("cancel err=%v", err)
	}
	waiters := make(chan error, 2)
	go func() { waiters <- s.SleepUntilNS(nil, 15) }()
	go func() { waiters <- s.SleepUntilNS(context.Background(), 15) }()
	s.Advance(5 * time.Nanosecond)
	for i := 0; i < 2; i++ {
		select {
		case err := <-waiters:
			if err != nil {
				t.Fatal(err)
			}
		case <-time.After(time.Second):
			t.Fatal("waiter did not wake")
		}
	}
}
