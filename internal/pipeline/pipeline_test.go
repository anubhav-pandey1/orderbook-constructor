// Package pipeline holds an end-to-end integration test wiring the whole
// system: decoder -> book writer -> event ring -> strategy -> log ring ->
// logger. It verifies ordering, one-event-per-row, and latency sampling, and
// is intended to run under `go test -race`.
package pipeline

import (
	"context"
	"strings"
	"sync"
	"testing"

	"orderbook/book"
	"orderbook/feed"
	"orderbook/internal/asynclog"
	"orderbook/internal/metrics"
	"orderbook/internal/ring"
	"orderbook/internal/strategy"
)

// Mirrors the real fixture's CRLF + quoted-JSON layout.
const miniCSV = "type,exchange,symbol,timestamp,side,bids,asks,price,size\r\n" +
	"snapshot,binance,BTC/USDT,1700000000000,,\"[[100.00, 1.0], [99.99, 2.0]]\",\"[[100.01, 1.0], [100.02, 3.0]]\",,\r\n" +
	"incremental,binance,BTC/USDT,1700000000100,bid,,,100.00,0.0\r\n" +
	"incremental,binance,BTC/USDT,1700000000200,ask,,,100.03,1.5\r\n"

type counter struct {
	inner  strategy.Strategy
	record func(uint64)
}

func (c counter) OnBookEvent(e book.BookEvent) asynclog.LogRecord {
	c.record(e.Version)
	return c.inner.OnBookEvent(e)
}

func TestPipelineEndToEnd(t *testing.T) {
	dec := feed.NewDecoder(strings.NewReader(miniCSV))
	bk := book.New("binance", "BTC/USDT", book.Config{CrossedPolicy: book.PolicyStrict, LevelHint: 16})
	lg, err := asynclog.New(asynclog.Config{Sink: asynclog.SinkDiscard, RingCapacity: 8, Spin: 16})
	if err != nil {
		t.Fatal(err)
	}
	events := ring.New[book.BookEvent](8)
	lat := &strategy.Latency{IngressToRecv: metrics.NewHistogram(), ApplyToRecv: metrics.NewHistogram()}

	var mu sync.Mutex
	var versions []uint64
	strat := counter{inner: strategy.BBO{}, record: func(v uint64) {
		mu.Lock()
		versions = append(versions, v)
		mu.Unlock()
	}}

	ctx := context.Background()
	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); lg.Run(ctx) }()
	go func() { defer wg.Done(); strategy.Run(ctx, events, lg, strat, lat, 16) }()

	pub := feed.SinkFunc(func(e book.BookEvent) error { return events.Push(ctx, e, 16) })
	st, err := feed.Replay(ctx, dec, bk, pub,
		feed.Config{Mode: feed.Fast, Sync: &feed.TimestampPolicy{Mode: book.PolicyStrict}}, feed.RealClock{})
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	events.Close()
	wg.Wait()

	if st.Accepted != 3 {
		t.Errorf("accepted = %d, want 3", st.Accepted)
	}
	if len(versions) != 3 {
		t.Fatalf("strategy saw %d events, want 3", len(versions))
	}
	for i, v := range versions {
		if v != uint64(i+1) {
			t.Errorf("versions[%d] = %d, want %d", i, v, i+1)
		}
	}
	if c := lat.IngressToRecv.Count(); c != 3 {
		t.Errorf("latency samples = %d, want 3", c)
	}

	// After the snapshot (best bid 100.00) the first delta deletes 100.00, so
	// the best bid must fall to 99.99; best ask stays 100.01.
	tob := bk.BestBidAsk()
	if tob.BidPrice != 9999 { // 99.99 * 100
		t.Errorf("final best bid = %s, want 99.99", tob.BidPrice)
	}
	if tob.AskPrice != 10001 { // 100.01 * 100
		t.Errorf("final best ask = %s, want 100.01", tob.AskPrice)
	}
}
