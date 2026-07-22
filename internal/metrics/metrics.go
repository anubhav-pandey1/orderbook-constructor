package metrics

import (
	"fmt"
	"math"
	"math/bits"
	"time"
)

const (
	subBits     = 6
	subCount    = 1 << subBits
	bucketCount = (64 - subBits) * subCount
)

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

func bucketBounds(i int) (lo, hi int64) {
	if i < subCount {
		return int64(i), int64(i)
	}
	q := i / subCount
	sub := i % subCount
	msb := q + subBits - 1
	shift := uint(msb - subBits)
	lo = int64(subCount+sub) << shift
	hi = (int64(subCount+sub+1) << shift) - 1
	return lo, hi
}

func bucketRep(i int) int64 {
	lo, hi := bucketBounds(i)
	return lo + (hi-lo)/2
}

type Histogram struct {
	buckets [bucketCount]uint64
	count   uint64
	min     int64
	max     int64
}

func NewHistogram() *Histogram {
	return &Histogram{
		min: math.MaxInt64,
		max: math.MinInt64,
	}
}

func (h *Histogram) P(q float64) int64 { return h.Percentile(q) }

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

func (h *Histogram) RecordDuration(d time.Duration) {
	h.Record(d.Nanoseconds())
}

func (h *Histogram) Count() uint64 {
	return h.count
}

func (h *Histogram) Min() int64 {
	if h.count == 0 {
		return 0
	}
	return h.min
}

func (h *Histogram) Max() int64 {
	if h.count == 0 {
		return 0
	}
	return h.max
}

func (h *Histogram) Percentile(q float64) int64 {
	if h.count == 0 {
		return 0
	}
	if q <= 0 {
		return h.min
	}
	if q >= 1 {
		return h.max
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

func formatNS(ns int64) string {
	v := float64(ns)
	switch {
	case ns < 1_000:
		return fmt.Sprintf("%dns", ns)
	case ns < 1_000_000:
		return fmt.Sprintf("%.3gus", v/1e3)
	case ns < 1_000_000_000:
		return fmt.Sprintf("%.3gms", v/1e6)
	default:
		return fmt.Sprintf("%.3gs", v/1e9)
	}
}

func Rate(n uint64, d time.Duration) float64 {
	if d <= 0 {
		return 0
	}
	return float64(n) / d.Seconds()
}
