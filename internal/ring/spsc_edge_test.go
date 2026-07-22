package ring

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestConsumeWaitCloseUnblocksEmptyConsumer(t *testing.T) {
	r, err := NewSPSC[int](2)
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		v, ok, err := r.ConsumeWait(nil, -1)
		if err != nil {
			done <- err
			return
		}
		if ok || v != 0 {
			done <- errors.New("unexpected consumed value")
			return
		}
		done <- nil
	}()
	time.Sleep(time.Millisecond)
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("consumer did not unblock after close")
	}
}

func TestPublishNegativeSpinAndNilContext(t *testing.T) {
	r, err := NewSPSC[int](2)
	if err != nil {
		t.Fatal(err)
	}
	if err := r.Publish(nil, 1, -10); err != nil {
		t.Fatal(err)
	}
	if err := r.Publish(nil, 2, -10); err != nil {
		t.Fatal(err)
	}
	if r.TryPublish(3) {
		t.Fatal("published into full ring")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := r.Publish(ctx, 3, -1); !errors.Is(err, context.Canceled) {
		t.Fatalf("publish canceled err=%v", err)
	}
}

func TestCloseIdempotentAndLenClamp(t *testing.T) {
	r, err := NewSPSC[int](4)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 4; i++ {
		if !r.TryPublish(i) {
			t.Fatalf("publish %d failed", i)
		}
	}
	if r.Len() != 4 {
		t.Fatalf("len=%d", r.Len())
	}
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
	if !r.Closed() {
		t.Fatal("ring not closed")
	}
	if r.TryPublish(99) {
		t.Fatal("published after close")
	}
}
