package bench

import (
	"github.com/anubhav-pandey1/orderbook-constructor/internal/metrics"
	"time"
)

type Hist = metrics.Histogram

func NewHist() *Hist { return metrics.NewHistogram() }

type Throughput struct {
	N   uint64
	Dur time.Duration
}

func (t Throughput) PerSec() float64 { return metrics.Rate(t.N, t.Dur) }
