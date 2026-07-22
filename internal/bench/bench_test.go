package bench

import (
	"testing"
	"time"
)

func TestThroughputAndHistogramAlias(t *testing.T) {
	if got := (Throughput{N: 100, Dur: time.Second}).PerSec(); got != 100 {
		t.Fatalf("rate=%v", got)
	}
	if got := (Throughput{N: 100}).PerSec(); got != 0 {
		t.Fatalf("zero duration rate=%v", got)
	}
	h := NewHist()
	h.Record(10)
	if h.Count() != 1 || h.P(0.5) != 10 {
		t.Fatalf("hist count/p=%d/%d", h.Count(), h.P(0.5))
	}
}
