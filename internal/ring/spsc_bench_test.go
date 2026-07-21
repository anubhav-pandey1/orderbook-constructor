package ring

import (
	"context"
	"testing"
	"time"
)

var sinkRingInt int

func BenchmarkSPSCTryRoundTrip(b *testing.B) {
	r, err := NewSPSC[int](1024)
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if !r.TryPublish(i) {
			b.Fatal("unexpected full ring")
		}
		var ok bool
		sinkRingInt, ok = r.TryConsume()
		if !ok {
			b.Fatal("unexpected empty ring")
		}
	}
}

func BenchmarkSPSC(b *testing.B) {
	r, err := NewSPSC[int](1 << 12)
	if err != nil {
		b.Fatal(err)
	}
	ctx := context.Background()
	n := b.N

	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < n; i++ {
			if err := r.Publish(ctx, i, 128); err != nil {
				return
			}
		}
	}()

	b.ResetTimer()
	t0 := time.Now()
	for i := 0; i < n; i++ {
		v, ok, err := r.ConsumeWait(ctx, 128)
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
