package clock

import (
	"context"
	"sync"
	"time"
)

type Clock interface {
	NowNS() int64
	SleepUntilNS(context.Context, int64) error
}
type Real struct{ origin time.Time }

func NewReal() *Real         { return &Real{origin: time.Now()} }
func (r *Real) NowNS() int64 { return time.Since(r.origin).Nanoseconds() }
func (r *Real) SleepUntilNS(ctx context.Context, target int64) error {
	if ctx == nil {
		ctx = context.Background()
	}
	d := target - r.NowNS()
	if d <= 0 {
		return nil
	}
	timer := time.NewTimer(time.Duration(d))
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

type Sim struct {
	mu   sync.Mutex
	now  int64
	wake chan struct{}
}

func NewSim(start int64) *Sim { return &Sim{now: start, wake: make(chan struct{})} }
func (s *Sim) NowNS() int64   { s.mu.Lock(); defer s.mu.Unlock(); return s.now }
func (s *Sim) SleepUntilNS(ctx context.Context, target int64) error {
	if ctx == nil {
		ctx = context.Background()
	}
	for {
		s.mu.Lock()
		if s.now >= target {
			s.mu.Unlock()
			return nil
		}
		w := s.wake
		s.mu.Unlock()
		select {
		case <-w:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}
func (s *Sim) Advance(d time.Duration) {
	if d <= 0 {
		return
	}
	s.mu.Lock()
	s.now += d.Nanoseconds()
	close(s.wake)
	s.wake = make(chan struct{})
	s.mu.Unlock()
}
