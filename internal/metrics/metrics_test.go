package metrics

import (
	"math"
	"strings"
	"testing"
	"time"
)

// TestPercentilesUniformRange records the integers 1..10000 ns and asserts
// each percentile is within 2% of the analytic (nearest-rank) value. For a
// contiguous 1..N data set, the nearest-rank value at quantile q is ~q*N.
func TestPercentilesUniformRange(t *testing.T) {
	h := NewHistogram()
	const N = 10000
	for i := int64(1); i <= N; i++ {
		h.Record(i)
	}
	if h.Count() != N {
		t.Fatalf("Count() = %d, want %d", h.Count(), N)
	}
	if got := h.Min(); got != 1 {
		t.Errorf("Min() = %d, want 1", got)
	}
	if got := h.Max(); got != N {
		t.Errorf("Max() = %d, want %d", got, N)
	}

	quantiles := []float64{0.1, 0.25, 0.5, 0.75, 0.9, 0.95, 0.99, 0.999}
	for _, q := range quantiles {
		got := h.Percentile(q)
		want := q * float64(N)
		rel := math.Abs(float64(got)-want) / want
		if rel > 0.02 {
			t.Errorf("Percentile(%g) = %d, want ~%.0f (relative error %.4f > 0.02)",
				q, got, want, rel)
		}
	}
}

// TestUniformConstant records a single constant value many times and asserts
// every percentile resolves to that value.
func TestUniformConstant(t *testing.T) {
	h := NewHistogram()
	const val = int64(500)
	for i := 0; i < 1000; i++ {
		h.Record(val)
	}
	for _, q := range []float64{0, 0.5, 0.9, 0.95, 0.99, 0.999, 1} {
		got := h.Percentile(q)
		rel := math.Abs(float64(got-val)) / float64(val)
		if rel > 0.02 {
			t.Errorf("Percentile(%g) = %d, want ~%d (relative error %.4f > 0.02)",
				q, got, val, rel)
		}
	}
}

// TestEmpty verifies the zero-observation behavior.
func TestEmpty(t *testing.T) {
	h := NewHistogram()
	if got := h.Count(); got != 0 {
		t.Errorf("Count() = %d, want 0", got)
	}
	if got := h.Percentile(0.5); got != 0 {
		t.Errorf("Percentile(0.5) = %d, want 0", got)
	}
	if got := h.Percentile(0.99); got != 0 {
		t.Errorf("Percentile(0.99) = %d, want 0", got)
	}
	if got := h.Min(); got != 0 {
		t.Errorf("Min() = %d, want 0", got)
	}
	if got := h.Max(); got != 0 {
		t.Errorf("Max() = %d, want 0", got)
	}
}

// TestRate checks the throughput helper, including the zero-duration guard.
func TestRate(t *testing.T) {
	if got := Rate(1_000_000, time.Second); got != 1e6 {
		t.Errorf("Rate(1e6, 1s) = %v, want 1e6", got)
	}
	if got := Rate(42, 0); got != 0 {
		t.Errorf("Rate(42, 0) = %v, want 0", got)
	}
	if got := Rate(42, -time.Second); got != 0 {
		t.Errorf("Rate(42, -1s) = %v, want 0", got)
	}
}

// TestSummary verifies the summary line contains the required substrings.
func TestSummary(t *testing.T) {
	h := NewHistogram()
	for i := int64(1); i <= 1000; i++ {
		h.Record(i * 1000) // microsecond-scale values
	}
	s := h.Summary("latency")
	for _, sub := range []string{"latency:", "count=1000", "p50=", "p90=", "p95=", "p99=", "p99.9=", "max="} {
		if !strings.Contains(s, sub) {
			t.Errorf("Summary()=%q missing substring %q", s, sub)
		}
	}
}

// TestRecordDuration verifies RecordDuration records d.Nanoseconds().
func TestRecordDuration(t *testing.T) {
	h := NewHistogram()
	h.RecordDuration(2 * time.Microsecond) // 2000 ns
	if h.Count() != 1 {
		t.Fatalf("Count() = %d, want 1", h.Count())
	}
	got := h.Percentile(0.5)
	want := int64(2000)
	rel := math.Abs(float64(got-want)) / float64(want)
	if rel > 0.02 {
		t.Errorf("Percentile(0.5) = %d, want ~%d (relative error %.4f > 0.02)", got, want, rel)
	}
}

// TestRecordAllocationFree asserts Record performs zero heap allocations.
func TestRecordAllocationFree(t *testing.T) {
	h := NewHistogram()
	allocs := testing.AllocsPerRun(1000, func() {
		h.Record(1234)
	})
	if allocs != 0 {
		t.Errorf("Record allocated %v times per run, want 0", allocs)
	}
}
