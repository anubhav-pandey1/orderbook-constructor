package ring

import (
	"context"
	"errors"
	"runtime"
	"sync/atomic"
)

var (
	ErrClosed          = errors.New("ring: closed")
	ErrInvalidCapacity = errors.New("ring: capacity must be a power of two >= 2")
)

const cursorBlockSize = 128

type producerState struct {
	published      atomic.Uint64
	seq            uint64
	cachedConsumed uint64
	_              [cursorBlockSize - 24]byte
}

type consumerState struct {
	consumed        atomic.Uint64
	seq             uint64
	cachedPublished uint64
	_               [cursorBlockSize - 24]byte
}

type SPSC[T any] struct {
	buf      []T
	mask     uint64
	capacity uint64
	producer producerState
	consumer consumerState
	closed   atomic.Bool
}

func NewSPSC[T any](capacity int) (*SPSC[T], error) {
	if capacity < 2 || capacity&(capacity-1) != 0 {
		return nil, ErrInvalidCapacity
	}
	return &SPSC[T]{buf: make([]T, capacity), mask: uint64(capacity - 1), capacity: uint64(capacity)}, nil
}

func (r *SPSC[T]) Cap() int     { return int(r.capacity) }
func (r *SPSC[T]) Closed() bool { return r.closed.Load() }
func (r *SPSC[T]) Len() int {
	p, c := r.producer.published.Load(), r.consumer.consumed.Load()
	if c >= p {
		return 0
	}
	d := p - c
	if d > r.capacity {
		d = r.capacity
	}
	return int(d)
}

func (r *SPSC[T]) TryPublish(v T) bool {
	if r.closed.Load() {
		return false
	}
	i := r.producer.seq
	if i-r.producer.cachedConsumed == r.capacity {
		r.producer.cachedConsumed = r.consumer.consumed.Load()
		if i-r.producer.cachedConsumed == r.capacity {
			return false
		}
	}
	if r.closed.Load() {
		return false
	}
	r.buf[i&r.mask] = v
	r.producer.published.Store(i + 1)
	r.producer.seq = i + 1
	return true
}

func (r *SPSC[T]) Publish(ctx context.Context, v T, spin int) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if spin < 0 {
		spin = 0
	}
	for {
		if r.closed.Load() {
			return ErrClosed
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if r.TryPublish(v) {
			return nil
		}
		for i := 0; i < spin; i++ {
			if r.closed.Load() {
				return ErrClosed
			}
			if err := ctx.Err(); err != nil {
				return err
			}
			if r.TryPublish(v) {
				return nil
			}
		}
		runtime.Gosched()
	}
}

func (r *SPSC[T]) TryConsume() (T, bool) {
	i := r.consumer.seq
	if i == r.consumer.cachedPublished {
		r.consumer.cachedPublished = r.producer.published.Load()
		if i == r.consumer.cachedPublished {
			var zero T
			return zero, false
		}
	}
	v := r.buf[i&r.mask]
	r.consumer.consumed.Store(i + 1)
	r.consumer.seq = i + 1
	return v, true
}

func (r *SPSC[T]) ConsumeWait(ctx context.Context, spin int) (T, bool, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if spin < 0 {
		spin = 0
	}
	var zero T
	for {
		if v, ok := r.TryConsume(); ok {
			return v, true, nil
		}
		if r.closed.Load() && r.consumer.seq == r.producer.published.Load() {
			return zero, false, nil
		}
		if err := ctx.Err(); err != nil {
			return zero, false, err
		}
		for i := 0; i < spin; i++ {
			if v, ok := r.TryConsume(); ok {
				return v, true, nil
			}
			if r.closed.Load() && r.consumer.seq == r.producer.published.Load() {
				return zero, false, nil
			}
			if err := ctx.Err(); err != nil {
				return zero, false, err
			}
		}
		runtime.Gosched()
	}
}

func (r *SPSC[T]) Close() error { r.closed.Store(true); return nil }
