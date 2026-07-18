package ring

import (
	"context"
	"errors"
	"testing"
	"time"
	"unsafe"
)

func TestCapacityContract(t *testing.T) {
	for _, n := range []int{-1, 0, 1, 3, 1000} {
		if _, err := NewSPSC[int](n); !errors.Is(err, ErrInvalidCapacity) {
			t.Fatalf("%d: %v", n, err)
		}
	}
	r, err := NewSPSC[int](8)
	if err != nil || r.Cap() != 8 {
		t.Fatalf("new: %v cap=%d", err, r.Cap())
	}
}

func TestFIFOCloseDrainAndClosedPublish(t *testing.T) {
	r, err := NewSPSC[int](4)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 4; i++ {
		if !r.TryPublish(i) {
			t.Fatal("publish")
		}
	}
	if r.TryPublish(5) {
		t.Fatal("published while full")
	}
	_ = r.Close()
	for i := 0; i < 4; i++ {
		v, ok, err := r.ConsumeWait(context.Background(), 1)
		if err != nil || !ok || v != i {
			t.Fatalf("%d: %v %v %v", i, v, ok, err)
		}
	}
	if _, ok, err := r.ConsumeWait(context.Background(), 1); err != nil || ok {
		t.Fatalf("drain: ok=%v err=%v", ok, err)
	}
	if err := r.Publish(context.Background(), 9, 1); !errors.Is(err, ErrClosed) {
		t.Fatalf("publish: %v", err)
	}
}

func TestConcurrentFIFO(t *testing.T) {
	const n = 500000
	r, err := NewSPSC[int](1024)
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		for i := 0; i < n; i++ {
			if err := r.Publish(context.Background(), i, 64); err != nil {
				done <- err
				return
			}
		}
		_ = r.Close()
		done <- nil
	}()
	for i := 0; i < n; i++ {
		v, ok, err := r.ConsumeWait(context.Background(), 64)
		if err != nil || !ok || v != i {
			t.Fatalf("%d: %d %v %v", i, v, ok, err)
		}
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestBlockedPublishCancellationAndClose(t *testing.T) {
	for _, closeRing := range []bool{false, true} {
		r, err := NewSPSC[int](2)
		if err != nil {
			t.Fatal(err)
		}
		r.TryPublish(1)
		r.TryPublish(2)
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() { done <- r.Publish(ctx, 3, 8) }()
		time.Sleep(5 * time.Millisecond)
		if closeRing {
			_ = r.Close()
		} else {
			cancel()
		}
		err = <-done
		cancel()
		if closeRing && !errors.Is(err, ErrClosed) {
			t.Fatalf("close: %v", err)
		}
		if !closeRing && !errors.Is(err, context.Canceled) {
			t.Fatalf("cancel: %v", err)
		}
	}
}

func TestCursorSeparationAndAllocFree(t *testing.T) {
	var r SPSC[int]
	p := unsafe.Offsetof(r.producer) + unsafe.Offsetof(r.producer.published)
	c := unsafe.Offsetof(r.consumer) + unsafe.Offsetof(r.consumer.consumed)
	if c-p < cursorBlockSize {
		t.Fatalf("distance=%d", c-p)
	}
	q, err := NewSPSC[int](8)
	if err != nil {
		t.Fatal(err)
	}
	if a := testing.AllocsPerRun(1000, func() { q.TryPublish(1); q.TryConsume() }); a != 0 {
		t.Fatalf("allocs=%v", a)
	}
}
