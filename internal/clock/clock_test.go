package clock

import (
	"context"
	"testing"
	"time"
)

func TestSim(t *testing.T) {
	s := NewSim(100)
	done := make(chan error, 1)
	go func() { done <- s.SleepUntilNS(context.Background(), 300) }()
	s.Advance(200 * time.Nanosecond)
	select {
	case err := <-done:
		if err != nil || s.NowNS() != 300 {
			t.Fatalf("sleep err/now=%v/%d, want nil/300", err, s.NowNS())
		}
	case <-time.After(time.Second):
		t.Fatal("simulated sleep did not wake after advance")
	}
}
