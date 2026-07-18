// Package ring provides a lock-free single-producer/single-consumer (SPSC)
// ring buffer built on the Lamport circular-buffer protocol.
//
// Exactly one goroutine may act as the producer (TryPush/Push) and exactly one
// as the consumer (TryPop/Pop) at any time. Under that discipline the buffer is
// wait-free for the fast path and requires no mutexes.
package ring

import (
	"context"
	"errors"
	"math/bits"
	"runtime"
	"sync/atomic"
)

// ErrClosed is returned by Push once the ring has been closed.
var ErrClosed = errors.New("ring: closed")

// cacheLine is the assumed size of a CPU cache line on the target hardware.
// Cursors are padded to this granularity so that the producer's and consumer's
// hot fields never land on the same line (false sharing).
const cacheLine = 64

// SPSC is a lock-free single-producer/single-consumer ring buffer.
//
// The zero value is not usable; construct one with New. It must be used via a
// pointer (New returns one) and never copied, as it embeds atomics.
type SPSC[T any] struct {
	// Read-mostly configuration, fixed after New. Grouped together on the
	// leading cache line(s). closed is written at most once (by Close).
	buf      []T
	mask     uint64
	capacity uint64
	closed   atomic.Bool
	_        [cacheLine]byte

	// head: total items ever published by the producer. Written by the
	// producer (release), read by the consumer (acquire). Own cache line.
	head atomic.Uint64
	_    [cacheLine - 8]byte

	// tail: total items ever consumed. Written by the consumer (release),
	// read by the producer (acquire). Own cache line.
	tail atomic.Uint64
	_    [cacheLine - 8]byte

	// cachedTail is the producer's private view of tail; refreshed from the
	// atomic only when the ring appears full. Own cache line.
	cachedTail uint64
	_          [cacheLine - 8]byte

	// cachedHead is the consumer's private view of head; refreshed from the
	// atomic only when the ring appears empty. Own cache line.
	cachedHead uint64
	_          [cacheLine - 8]byte
}

// New allocates an SPSC ring with the given capacity rounded UP to the next
// power of two. capacity must be >= 1; New panics otherwise.
func New[T any](capacity int) *SPSC[T] {
	if capacity < 1 {
		panic("ring: capacity must be >= 1")
	}
	c := roundUpPow2(capacity)
	return &SPSC[T]{
		buf:      make([]T, c),
		mask:     uint64(c - 1),
		capacity: uint64(c),
	}
}

// roundUpPow2 returns the smallest power of two >= n (n >= 1).
func roundUpPow2(n int) int {
	if n <= 1 {
		return 1
	}
	return 1 << bits.Len(uint(n-1))
}

// Cap returns the (power-of-two) capacity of the ring.
func (r *SPSC[T]) Cap() int { return int(r.capacity) }

// Len returns the approximate number of buffered items (head-tail). Because it
// reads the two cursors without synchronization relative to each other, the
// result is a snapshot that may be stale the instant it returns.
func (r *SPSC[T]) Len() int {
	h := r.head.Load()
	t := r.tail.Load()
	return int(h - t)
}

// TryPush enqueues v without blocking. It returns false if the ring is full or
// closed. Allocation-free. Producer side only.
func (r *SPSC[T]) TryPush(v T) bool {
	if r.closed.Load() {
		return false
	}
	head := r.head.Load()
	// Fast path: use the cached tail to decide fullness; only touch the
	// (contended) tail atomic when the cached view claims the ring is full.
	if head-r.cachedTail >= r.capacity {
		r.cachedTail = r.tail.Load() // acquire
		if head-r.cachedTail >= r.capacity {
			return false // genuinely full
		}
	}
	r.buf[head&r.mask] = v
	r.head.Store(head + 1) // release: publishes the slot write above
	return true
}

// TryPop dequeues a value without blocking. It returns (zero, false) if the
// ring is empty. Allocation-free. Consumer side only.
func (r *SPSC[T]) TryPop() (T, bool) {
	tail := r.tail.Load()
	// Fast path: use the cached head to decide emptiness; only touch the
	// head atomic when the cached view claims the ring is empty.
	if tail == r.cachedHead {
		r.cachedHead = r.head.Load() // acquire
		if tail == r.cachedHead {
			var zero T
			return zero, false // genuinely empty
		}
	}
	v := r.buf[tail&r.mask]
	r.tail.Store(tail + 1) // release: frees the slot for the producer
	return v, true
}

// Push enqueues v, blocking with backpressure until there is room. It never
// drops. When the ring is full it busy-spins for spin iterations, then yields
// via runtime.Gosched, re-checking ctx and the closed flag each round.
//
// It returns ctx.Err() if ctx is cancelled while blocked, or ErrClosed if the
// ring is closed. On success it returns nil.
func (r *SPSC[T]) Push(ctx context.Context, v T, spin int) error {
	for {
		if r.closed.Load() {
			return ErrClosed
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if r.TryPush(v) {
			return nil
		}
		// Full: busy-spin, then yield the processor.
		for i := 0; i < spin; i++ {
			if r.TryPush(v) {
				return nil
			}
		}
		runtime.Gosched()
	}
}

// Pop dequeues a value, blocking until one is available. It busy-spins for spin
// iterations then yields via runtime.Gosched while empty.
//
// Returns:
//   - (v, true, nil)      a value was dequeued.
//   - (zero, false, nil)  the ring is closed AND drained (empty).
//   - (zero, false, err)  ctx was cancelled while blocked (err == ctx.Err()).
func (r *SPSC[T]) Pop(ctx context.Context, spin int) (T, bool, error) {
	var zero T
	for {
		if v, ok := r.TryPop(); ok {
			return v, true, nil
		}
		// Empty. If closed, drain anything published before/at Close and
		// then report closed. Observing closed==true (an SC atomic) orders
		// after every prior producer head.Store, so one more TryPop sees
		// the final head value.
		if r.closed.Load() {
			if v, ok := r.TryPop(); ok {
				return v, true, nil
			}
			return zero, false, nil
		}
		if err := ctx.Err(); err != nil {
			return zero, false, err
		}
		for i := 0; i < spin; i++ {
			if v, ok := r.TryPop(); ok {
				return v, true, nil
			}
		}
		runtime.Gosched()
	}
}

// Close marks the ring closed. It is idempotent and safe to call from either
// side. A blocked Pop will observe it, drain any remaining items, and then
// return (zero, false, nil); subsequent TryPush/Push calls fail with ErrClosed.
func (r *SPSC[T]) Close() {
	r.closed.Store(true)
}
