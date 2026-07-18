package feed_test

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"orderbook/book"
	"orderbook/feed"
	"orderbook/internal/pipeline"
	"orderbook/internal/ring"
	"orderbook/internal/syncx"
)

var fixtureStream = feed.StreamID{Exchange: "binance", Symbol: "BTCUSDT"}

type testClock struct {
	now    int64
	sleeps []int64
}

func (c *testClock) NowNS() int64 { return c.now }
func (c *testClock) SleepUntilNS(ctx context.Context, target int64) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	c.sleeps = append(c.sleeps, target)
	if target > c.now {
		c.now = target
	}
	return nil
}

type requestRecorder struct {
	requests []feed.ResyncRequest
	err      error
}

func (r *requestRecorder) RequestSnapshot(_ context.Context, req feed.ResyncRequest) error {
	r.requests = append(r.requests, req)
	return r.err
}

func replayConfig(mode feed.ReplayMode) feed.ReplayCfg {
	return feed.ReplayCfg{Mode: mode, Speed: 1, TSUnit: time.Millisecond, SpinIters: 8, Stream: fixtureStream}
}
func newEventRing(t *testing.T, capacity int) *ring.SPSC[pipeline.Event] {
	t.Helper()
	r, err := ring.NewSPSC[pipeline.Event](capacity)
	if err != nil {
		t.Fatal(err)
	}
	return r
}
func drainEvents(r *ring.SPSC[pipeline.Event]) []pipeline.Event {
	var out []pipeline.Event
	for {
		ev, ok := r.TryConsume()
		if !ok {
			return out
		}
		out = append(out, ev)
	}
}

func TestReplayClassifyApplyAcceptAndRecovery(t *testing.T) {
	data := buildCSV(csvHeader,
		`snapshot,binance,BTC/USDT,1000,,"[[100.00,1.0]]","[[101.00,1.0]]",,`,
		`incremental,binance,BTC/USDT,1100,bid,,,100.00,2.0`,
		`incremental,binance,BTC/USDT,1100,bid,,,100.00,3.0`,
		`incremental,binance,BTC/USDT,1300,ask,,,101.00,2.0`,
		`incremental,binance,BTC/USDT,1400,bid,,,99.00,1.0`,
		`snapshot,binance,BTC/USDT,1500,,"[[99.00,1.0]]","[[102.00,1.0]]",,`,
		`incremental,binance,BTC/USDT,1600,ask,,,102.00,2.0`)
	out, requester, bk := newEventRing(t, 16), &requestRecorder{}, book.New(16)
	stats, err := feed.Replay(context.Background(), feed.NewDecoder(strings.NewReader(data)), bk,
		syncx.NewTimestampPolicy(syncx.TimestampStep, 100), requester, out, replayConfig(feed.Fast), &testClock{now: 10_000})
	if err != nil {
		t.Fatal(err)
	}
	if stats.Applied != 4 || stats.Discarded != 1 || stats.Invalidated != 1 || stats.Snapshots != 2 || stats.Deltas != 2 || stats.Duplicates != 1 || stats.Gaps != 1 || stats.IgnoredWhileDesynced != 1 || stats.SnapshotRequests != 1 {
		t.Fatalf("stats = %+v", stats)
	}
	if len(requester.requests) != 1 {
		t.Fatalf("requests=%d", len(requester.requests))
	}
	req := requester.requests[0]
	if req.Exchange != "binance" || req.Symbol != "BTCUSDT" || req.Last.Timestamp != 1100 || req.Received.Timestamp != 1300 || req.Reason != syncx.ReasonGap {
		t.Fatalf("request=%+v", req)
	}
	events := drainEvents(out)
	if len(events) != 5 {
		t.Fatalf("events=%d", len(events))
	}
	wantVersions := []uint64{1, 2, 2, 3, 4}
	wantEpochs := []uint64{1, 1, 1, 2, 2}
	for i, ev := range events {
		if ev.NotificationID != uint64(i+1) || ev.Version != wantVersions[i] || ev.SyncEpoch != wantEpochs[i] || ev.DueNS != 0 {
			t.Errorf("event[%d]=%+v", i, ev)
		}
	}
	if events[2].Kind != pipeline.BookInvalidated || events[2].State != syncx.Desynchronized || events[2].Reason != syncx.ReasonGap || events[2].BidOK || events[2].AskOK {
		t.Fatalf("invalidation=%+v", events[2])
	}
}

func TestReplayCrossedDeltaDoesNotAdvanceCursor(t *testing.T) {
	data := buildCSV(csvHeader,
		`snapshot,binance,BTC/USDT,1000,,"[[100.00,1.0]]","[[101.00,1.0]]",,`,
		`incremental,binance,BTC/USDT,1100,bid,,,102.00,1.0`,
		`snapshot,binance,BTC/USDT,1050,,"[[99.00,1.0]]","[[102.00,1.0]]",,`)
	out, requester, bk := newEventRing(t, 8), &requestRecorder{}, book.New(8)
	stats, err := feed.Replay(context.Background(), feed.NewDecoder(strings.NewReader(data)), bk,
		syncx.NewTimestampPolicy(syncx.TimestampStep, 100), requester, out, replayConfig(feed.Fast), &testClock{now: 20_000})
	if err != nil {
		t.Fatal(err)
	}
	if stats.Applied != 2 || stats.Invalidated != 1 || stats.Crossed != 1 || stats.SnapshotRequests != 1 || bk.Version() != 2 {
		t.Fatalf("stats=%+v version=%d", stats, bk.Version())
	}
	events := drainEvents(out)
	if len(events) != 3 || events[1].Kind != pipeline.BookInvalidated || events[2].Kind != pipeline.SnapshotApplied {
		t.Fatalf("events=%+v", events)
	}
}

func TestReplayStaleRecoveryEndsDesynchronized(t *testing.T) {
	data := buildCSV(csvHeader,
		`snapshot,binance,BTC/USDT,1000,,"[[100.00,1.0]]","[[101.00,1.0]]",,`,
		`incremental,binance,BTC/USDT,1200,bid,,,99.00,1.0`,
		`snapshot,binance,BTC/USDT,900,,"[[99.00,1.0]]","[[102.00,1.0]]",,`)
	stats, err := feed.Replay(context.Background(), feed.NewDecoder(strings.NewReader(data)), book.New(8),
		syncx.NewTimestampPolicy(syncx.TimestampStep, 100), &requestRecorder{}, newEventRing(t, 8), replayConfig(feed.Fast), &testClock{now: 30_000})
	if !errors.Is(err, feed.ErrSnapshotRequired) {
		t.Fatalf("error=%v", err)
	}
	if stats.Applied != 1 || stats.Invalidated != 1 || stats.Discarded != 1 || stats.Stale != 1 || stats.SnapshotRequests != 1 {
		t.Fatalf("stats=%+v", stats)
	}
}

func TestReplayPacedRebasesMonotonicTime(t *testing.T) {
	data := buildCSV(csvHeader,
		`snapshot,binance,BTC/USDT,1700000000000,,"[[100.00,1.0]]","[[101.00,1.0]]",,`,
		`incremental,binance,BTC/USDT,1700000000100,bid,,,100.00,2.0`)
	out, clk, cfg := newEventRing(t, 4), &testClock{now: 1_000_000_000}, replayConfig(feed.Paced)
	cfg.Speed = 2
	_, err := feed.Replay(context.Background(), feed.NewDecoder(strings.NewReader(data)), book.New(8), syncx.NewTimestampPolicy(syncx.TimestampStep, 100), nil, out, cfg, clk)
	if err != nil {
		t.Fatal(err)
	}
	want := []int64{1_000_000_000, 1_050_000_000}
	if len(clk.sleeps) != 2 || clk.sleeps[0] != want[0] || clk.sleeps[1] != want[1] {
		t.Fatalf("sleeps=%v", clk.sleeps)
	}
	for i, ev := range drainEvents(out) {
		if ev.DueNS != want[i] || ev.IngressNS < ev.DueNS || ev.ApplyNS < ev.IngressNS {
			t.Errorf("timing=%+v", ev)
		}
	}
}

func TestReplayFastAndPacedAreStateEquivalent(t *testing.T) {
	data := buildCSV(csvHeader,
		`snapshot,binance,BTC/USDT,1000,,"[[100.00,1.0]]","[[101.00,1.0]]",,`,
		`incremental,binance,BTC/USDT,1100,bid,,,100.00,2.0`,
		`snapshot,binance,BTC/USDT,1200,,"[[99.00,3.0]]","[[102.00,4.0]]",,`,
		`incremental,binance,BTC/USDT,1300,ask,,,102.00,5.0`)

	run := func(mode feed.ReplayMode) (feed.Stats, book.Depth, []pipeline.Event) {
		t.Helper()
		out := newEventRing(t, 8)
		bk := book.New(8)
		cfg := replayConfig(mode)
		if mode == feed.Paced {
			cfg.Speed = 2
		}
		stats, err := feed.Replay(context.Background(), feed.NewDecoder(strings.NewReader(data)), bk,
			syncx.NewTimestampPolicy(syncx.TimestampStep, 100), nil, out, cfg, &testClock{now: 10_000})
		if err != nil {
			t.Fatal(err)
		}
		return stats, bk.DepthSnapshot(), drainEvents(out)
	}

	fastStats, fastDepth, fastEvents := run(feed.Fast)
	pacedStats, pacedDepth, pacedEvents := run(feed.Paced)
	if !reflect.DeepEqual(fastStats, pacedStats) || !reflect.DeepEqual(fastDepth, pacedDepth) {
		t.Fatalf("mode state differs: fast=%+v/%+v paced=%+v/%+v", fastStats, fastDepth, pacedStats, pacedDepth)
	}
	if len(fastEvents) != len(pacedEvents) {
		t.Fatalf("event counts differ: fast=%d paced=%d", len(fastEvents), len(pacedEvents))
	}
	for i := range fastEvents {
		fastEvents[i].DueNS, fastEvents[i].IngressNS, fastEvents[i].ApplyNS = 0, 0, 0
		pacedEvents[i].DueNS, pacedEvents[i].IngressNS, pacedEvents[i].ApplyNS = 0, 0, 0
		if fastEvents[i] != pacedEvents[i] {
			t.Fatalf("event %d differs: fast=%+v paced=%+v", i, fastEvents[i], pacedEvents[i])
		}
	}
}

func TestReplayStreamMismatchBeforePolicy(t *testing.T) {
	data := buildCSV(csvHeader, `snapshot,kraken,BTC/USD,1000,,"[[100.00,1.0]]","[[101.00,1.0]]",,`)
	p := &countingPolicy{}
	_, err := feed.Replay(context.Background(), feed.NewDecoder(strings.NewReader(data)), book.New(8), p, nil, nil, replayConfig(feed.Fast), &testClock{})
	if !errors.Is(err, feed.ErrStreamMismatch) || p.n != 0 {
		t.Fatalf("err=%v classifications=%d", err, p.n)
	}
}

type countingPolicy struct{ n int }

func (p *countingPolicy) ClassifySnapshot(syncx.Cursor) syncx.Decision {
	p.n++
	return syncx.Decision{Action: syncx.Apply}
}
func (p *countingPolicy) ClassifyUpdate(syncx.Cursor) syncx.Decision {
	p.n++
	return syncx.Decision{Action: syncx.Apply}
}
func (*countingPolicy) AcceptSnapshot(syncx.Cursor) {}
func (*countingPolicy) AcceptUpdate(syncx.Cursor)   {}
func (*countingPolicy) Invalidate()                 {}
