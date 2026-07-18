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
	if err := <-done; err != nil || s.NowNS() != 300 {
		t.Fatalf("%v %d", err, s.NowNS())
	}
}
