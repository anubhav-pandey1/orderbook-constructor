package ring

import (
	"context"
	"testing"
	"time"
)

// BenchmarkSPSC measures end-to-end throughput: a producer goroutine feeds
// b.N sequential integers while the consumer drains them inside the benchmark
// loop. It reports achieved ops/second.
func BenchmarkSPSC(b *testing.B) {
	r := New[int](1 << 12) // 4096 slots
	ctx := context.Background()
	n := b.N

	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < n; i++ {
			if err := r.Push(ctx, i, 128); err != nil {
				return
			}
		}
	}()

	b.ResetTimer()
	t0 := time.Now()
	for i := 0; i < n; i++ {
		v, ok, err := r.Pop(ctx, 128)
		if !ok || err != nil {
			b.Fatalf("Pop failed at %d: ok=%v err=%v", i, ok, err)
		}
		if v != i {
			b.Fatalf("out of order at %d: got %d", i, v)
		}
	}
	dt := time.Since(t0)
	b.StopTimer()
	<-done

	if dt > 0 {
		b.ReportMetric(float64(n)/dt.Seconds(), "ops/s")
	}
}
