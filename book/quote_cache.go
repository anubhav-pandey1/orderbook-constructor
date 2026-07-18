package book

import "sync/atomic"

const (
	flagHasBid uint32 = 1 << 0
	flagHasAsk uint32 = 1 << 1
)

// quoteCache is a seqlock exposing a torn-free BBO to out-of-band concurrent
// readers (readers that do not consume the event ring). The single writer
// brackets its multi-field store with an odd/even sequence; readers retry
// while the sequence is odd or changed. All fields are atomic so the race
// detector stays clean.
type quoteCache struct {
	sequence atomic.Uint64
	version  atomic.Uint64
	bidPrice atomic.Int64
	bidQty   atomic.Int64
	askPrice atomic.Int64
	askQty   atomic.Int64
	flags    atomic.Uint32
}

func (qc *quoteCache) store(version uint64, tob TopOfBook) {
	seq := qc.sequence.Load()
	qc.sequence.Store(seq + 1) // odd: write in progress
	qc.version.Store(version)
	qc.bidPrice.Store(int64(tob.BidPrice))
	qc.bidQty.Store(int64(tob.BidQty))
	qc.askPrice.Store(int64(tob.AskPrice))
	qc.askQty.Store(int64(tob.AskQty))
	var f uint32
	if tob.HasBid {
		f |= flagHasBid
	}
	if tob.HasAsk {
		f |= flagHasAsk
	}
	qc.flags.Store(f)
	qc.sequence.Store(seq + 2) // even: consistent
}

func (qc *quoteCache) load() (uint64, TopOfBook) {
	for {
		s1 := qc.sequence.Load()
		if s1&1 != 0 {
			continue
		}
		v := qc.version.Load()
		tob := TopOfBook{
			BidPrice: Price(qc.bidPrice.Load()),
			BidQty:   Quantity(qc.bidQty.Load()),
			AskPrice: Price(qc.askPrice.Load()),
			AskQty:   Quantity(qc.askQty.Load()),
		}
		f := qc.flags.Load()
		if qc.sequence.Load() == s1 {
			tob.HasBid = f&flagHasBid != 0
			tob.HasAsk = f&flagHasAsk != 0
			return v, tob
		}
	}
}
