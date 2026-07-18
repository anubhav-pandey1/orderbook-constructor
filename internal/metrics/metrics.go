// Package metrics provides a lightweight, allocation-free latency histogram
// for measuring nanosecond-scale operation timings.
//
// The Histogram uses a log-linear bucketing scheme: each power-of-two octave
// of the int64 nanosecond range is divided into a fixed number of linear
// sub-buckets. This yields a bounded relative error (~1/subCount per bucket)
// across the full ~1ns..~60s+ range while keeping Record allocation-free.
package metrics

import (
	"fmt"
	"math"
	"math/bits"
	"time"
)

const (
	// subBits controls the number of linear sub-buckets per power-of-two
	// octave: subCount = 2^subBits. More sub-buckets -> smaller relative
	// error per bucket (~1/subCount) at the cost of more memory.
	subBits     = 6
	subCount    = 1 << subBits // 64 sub-buckets per octave (~1.5% relative width)
	bucketCount = (64 - subBits) * subCount
)

// bucketIndex maps a nanosecond value to its bucket index.
//
// Values below subCount occupy their own exact bucket (linear region).
// Larger values are placed by octave (position of the most-significant bit)
// plus a linear sub-bucket within that octave. Negative values clamp to 0.
func bucketIndex(v int64) int {
	if v < subCount {
		if v < 0 {
			return 0
		}
		return int(v)
	}
	msb := bits.Len64(uint64(v)) - 1
	shift := uint(msb - subBits)
	sub := int((v >> shift) & (subCount - 1))
	base := (msb - subBits + 1) * subCount
	return base + sub
}

// bucketBounds returns the inclusive [lo, hi] nanosecond range covered by
// bucket i.
func bucketBounds(i int) (lo, hi int64) {
	if i < subCount {
		return int64(i), int64(i)
	}
	q := i / subCount   // = msb - subBits + 1
	sub := i % subCount // sub-bucket within the octave
	msb := q + subBits - 1
	shift := uint(msb - subBits)
	lo = int64(subCount+sub) << shift
	hi = (int64(subCount+sub+1) << shift) - 1
	return lo, hi
}

// bucketRep returns a representative (midpoint) nanosecond value for bucket i.
func bucketRep(i int) int64 {
	lo, hi := bucketBounds(i)
	return lo + (hi-lo)/2
}

// Histogram is a log-linear bucketed latency histogram over int64 nanoseconds.
// It is not safe for concurrent use.
type Histogram struct {
	buckets [bucketCount]uint64
	count   uint64
	min     int64
	max     int64
}

// NewHistogram returns an empty Histogram sized to cover the full positive
// int64 nanosecond range.
func NewHistogram() *Histogram {
	return &Histogram{
		min: math.MaxInt64,
		max: math.MinInt64,
	}
}

func (h *Histogram) P(q float64) int64 { return h.Percentile(q) }

// Record adds a single nanosecond observation. It performs no heap allocation.
func (h *Histogram) Record(ns int64) {
	h.buckets[bucketIndex(ns)]++
	h.count++
	if ns < h.min {
		h.min = ns
	}
	if ns > h.max {
		h.max = ns
	}
}

// RecordDuration records d.Nanoseconds().
func (h *Histogram) RecordDuration(d time.Duration) {
	h.Record(d.Nanoseconds())
}

// Count returns the number of recorded observations.
func (h *Histogram) Count() uint64 {
	return h.count
}

// Min returns the smallest recorded value in nanoseconds, or 0 if empty.
func (h *Histogram) Min() int64 {
	if h.count == 0 {
		return 0
	}
	return h.min
}

// Max returns the largest recorded value in nanoseconds, or 0 if empty.
func (h *Histogram) Max() int64 {
	if h.count == 0 {
		return 0
	}
	return h.max
}

// Percentile returns the nanosecond value at quantile q (clamped to [0,1])
// using nearest-rank over cumulative bucket counts. The result is the
// representative value of the containing bucket, clamped to [Min, Max].
// An empty histogram returns 0.
func (h *Histogram) Percentile(q float64) int64 {
	if h.count == 0 {
		return 0
	}
	if q < 0 {
		q = 0
	}
	if q > 1 {
		q = 1
	}
	rank := uint64(math.Ceil(q * float64(h.count)))
	if rank == 0 {
		rank = 1
	}
	var cum uint64
	for i := range h.buckets {
		cum += h.buckets[i]
		if cum >= rank {
			rep := bucketRep(i)
			if rep < h.min {
				rep = h.min
			}
			if rep > h.max {
				rep = h.max
			}
			return rep
		}
	}
	return h.max
}

// Summary returns a single-line human-readable summary of the histogram.
func (h *Histogram) Summary(name string) string {
	return fmt.Sprintf("%s: count=%d p50=%s p90=%s p95=%s p99=%s p99.9=%s max=%s",
		name, h.count,
		formatNS(h.Percentile(0.5)),
		formatNS(h.Percentile(0.9)),
		formatNS(h.Percentile(0.95)),
		formatNS(h.Percentile(0.99)),
		formatNS(h.Percentile(0.999)),
		formatNS(h.Max()),
	)
}

func (h *Histogram) Line(name string) string { return h.Summary(name) }

// formatNS auto-formats a nanosecond value with an appropriate unit
// (ns / µs / ms / s) at 2-3 significant figures.
func formatNS(ns int64) string {
	v := float64(ns)
	switch {
	case ns < 1_000:
		return fmt.Sprintf("%dns", ns)
	case ns < 1_000_000:
		return fmt.Sprintf("%.3gµs", v/1e3)
	case ns < 1_000_000_000:
		return fmt.Sprintf("%.3gms", v/1e6)
	default:
		return fmt.Sprintf("%.3gs", v/1e9)
	}
}

// Rate returns throughput in operations per second: float64(n)/d.Seconds().
// It returns 0 if d <= 0.
func Rate(n uint64, d time.Duration) float64 {
	if d <= 0 {
		return 0
	}
	return float64(n) / d.Seconds()
}
