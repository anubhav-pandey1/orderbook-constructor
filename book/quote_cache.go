package book

import "sync/atomic"

const (
	flagHasBid uint32 = 1 << iota
	flagHasAsk
)

type quoteCache struct {
	sequence atomic.Uint64
	version  atomic.Uint64
	bidPx    atomic.Int64
	bidQty   atomic.Int64
	askPx    atomic.Int64
	askQty   atomic.Int64
	flags    atomic.Uint32
}

func (q *quoteCache) store(bbo BBO) {
	seq := q.sequence.Load()
	q.sequence.Store(seq + 1)
	q.version.Store(bbo.Version)
	q.bidPx.Store(int64(bbo.BidPx))
	q.bidQty.Store(int64(bbo.BidQty))
	q.askPx.Store(int64(bbo.AskPx))
	q.askQty.Store(int64(bbo.AskQty))
	var flags uint32
	if bbo.BidOK {
		flags |= flagHasBid
	}
	if bbo.AskOK {
		flags |= flagHasAsk
	}
	q.flags.Store(flags)
	q.sequence.Store(seq + 2)
}

func (q *quoteCache) load() BBO {
	for {
		before := q.sequence.Load()
		if before&1 != 0 {
			continue
		}
		bbo := BBO{
			Version: q.version.Load(),
			BidPx:   Price(q.bidPx.Load()),
			BidQty:  Quantity(q.bidQty.Load()),
			AskPx:   Price(q.askPx.Load()),
			AskQty:  Quantity(q.askQty.Load()),
		}
		flags := q.flags.Load()
		after := q.sequence.Load()
		if before == after && after&1 == 0 {
			bbo.BidOK = flags&flagHasBid != 0
			bbo.AskOK = flags&flagHasAsk != 0
			return bbo
		}
	}
}
