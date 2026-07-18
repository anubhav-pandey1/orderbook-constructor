package ring

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestPowerOfTwoRounding(t *testing.T) {
	cases := map[int]int{1: 1, 2: 2, 3: 4, 5: 8, 8: 8, 9: 16, 1000: 1024, 1024: 1024}
	for in, want := range cases {
		if got := New[int](in).Cap(); got != want {
			t.Errorf("New(%d).Cap() = %d, want %d", in, got, want)
		}
	}
}

func TestNewPanicsOnZero(t *testing.T) {
	for _, bad := range []int{0, -1, -100} {
		func() {
			defer func() {
				if recover() == nil {
					t.Errorf("New(%d) did not panic", bad)
				}
			}()
			_ = New[int](bad)
		}()
	}
}

func TestTryPushPopEdges(t *testing.T) {
	r := New[int](2) // cap 2
	if r.Cap() != 2 {
		t.Fatalf("cap = %d, want 2", r.Cap())
	}

	// Empty ring.
	if _, ok := r.TryPop(); ok {
		t.Fatal("TryPop on empty returned ok=true")
	}
	if r.Len() != 0 {
		t.Fatalf("Len on empty = %d, want 0", r.Len())
	}

	// Fill it.
	if !r.TryPush(10) {
		t.Fatal("TryPush #1 failed")
	}
	if !r.TryPush(20) {
		t.Fatal("TryPush #2 failed")
	}
	if r.Len() != 2 {
		t.Fatalf("Len when full = %d, want 2", r.Len())
	}
	// Full now.
	if r.TryPush(30) {
		t.Fatal("TryPush on full returned true")
	}

	// Drain in order.
	if v, ok := r.TryPop(); !ok || v != 10 {
		t.Fatalf("TryPop = (%d,%v), want (10,true)", v, ok)
	}
	// Space again -> wrap.
	if !r.TryPush(30) {
		t.Fatal("TryPush after pop failed")
	}
	if v, ok := r.TryPop(); !ok || v != 20 {
		t.Fatalf("TryPop = (%d,%v), want (20,true)", v, ok)
	}
	if v, ok := r.TryPop(); !ok || v != 30 {
		t.Fatalf("TryPop = (%d,%v), want (30,true)", v, ok)
	}
	if _, ok := r.TryPop(); ok {
		t.Fatal("TryPop on drained returned ok=true")
	}
}

func TestTryPushFailsWhenClosed(t *testing.T) {
	r := New[int](4)
	r.Close()
	if r.TryPush(1) {
		t.Fatal("TryPush on closed returned true")
	}
	if err := r.Push(context.Background(), 1, 4); !errors.Is(err, ErrClosed) {
		t.Fatalf("Push on closed = %v, want ErrClosed", err)
	}
}

// TestConcurrentSequential streams several million sequential integers through
// the ring from a producer goroutine to a consumer goroutine and asserts every
// value arrives exactly once, in order, with none lost.
func TestConcurrentSequential(t *testing.T) {
	const n = 5_000_000
	r := New[int](1024)
	ctx := context.Background()

	errc := make(chan error, 1)
	go func() {
		for i := 0; i < n; i++ {
			if err := r.Push(ctx, i, 64); err != nil {
				errc <- err
				return
			}
		}
		r.Close()
		errc <- nil
	}()

	got := 0
	for {
		v, ok, err := r.Pop(ctx, 64)
		if err != nil {
			t.Fatalf("Pop error at %d: %v", got, err)
		}
		if !ok {
			break // closed and drained
		}
		if v != got {
			t.Fatalf("out of order: got %d, want %d", v, got)
		}
		got++
	}
	if err := <-errc; err != nil {
		t.Fatalf("producer error: %v", err)
	}
	if got != n {
		t.Fatalf("received %d values, want %d", got, n)
	}
}

// TestCloseAndDrain buffers N items, closes, and verifies the consumer drains
// exactly N and then observes closed.
func TestCloseAndDrain(t *testing.T) {
	const n = 1000
	r := New[int](1024) // holds all n at once
	for i := 0; i < n; i++ {
		if !r.TryPush(i) {
			t.Fatalf("TryPush failed at %d (Len=%d)", i, r.Len())
		}
	}
	r.Close()

	ctx := context.Background()
	got := 0
	for {
		v, ok, err := r.Pop(ctx, 8)
		if err != nil {
			t.Fatalf("Pop error at %d: %v", got, err)
		}
		if !ok {
			break
		}
		if v != got {
			t.Fatalf("out of order: got %d, want %d", v, got)
		}
		got++
	}
	if got != n {
		t.Fatalf("drained %d, want %d", got, n)
	}
	// A further Pop on a closed+drained ring keeps reporting closed.
	if v, ok, err := r.Pop(ctx, 8); ok || err != nil {
		t.Fatalf("Pop after drain = (%d,%v,%v), want (_,false,nil)", v, ok, err)
	}
}

// TestCtxCancelUnblocksPop verifies a blocked Pop returns when ctx is cancelled.
func TestCtxCancelUnblocksPop(t *testing.T) {
	r := New[int](4) // empty, never fed
	ctx, cancel := context.WithCancel(context.Background())

	type res struct {
		ok  bool
		err error
	}
	done := make(chan res, 1)
	go func() {
		_, ok, err := r.Pop(ctx, 16)
		done <- res{ok, err}
	}()

	// Give the goroutine time to enter the blocked spin loop, then cancel.
	time.Sleep(20 * time.Millisecond)
	cancel()

	select {
	case got := <-done:
		if got.ok {
			t.Fatal("Pop returned ok=true after cancel")
		}
		if !errors.Is(got.err, context.Canceled) {
			t.Fatalf("Pop err = %v, want context.Canceled", got.err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Pop did not unblock after ctx cancel")
	}
}

// TestCtxCancelUnblocksPush verifies a blocked (full-ring) Push returns when ctx
// is cancelled.
func TestCtxCancelUnblocksPush(t *testing.T) {
	r := New[int](2)
	// Fill it so the next Push blocks.
	r.TryPush(1)
	r.TryPush(2)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- r.Push(ctx, 3, 16)
	}()

	time.Sleep(20 * time.Millisecond)
	cancel()

	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Push err = %v, want context.Canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Push did not unblock after ctx cancel")
	}
}

// TestAllocFree asserts the fast paths perform no heap allocations.
func TestAllocFree(t *testing.T) {
	r := New[int](8)
	if a := testing.AllocsPerRun(1000, func() {
		r.TryPush(1)
		r.TryPop()
	}); a != 0 {
		t.Fatalf("TryPush/TryPop allocated %v/op, want 0", a)
	}
}
