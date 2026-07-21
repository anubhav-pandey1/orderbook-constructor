package feed_test

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/anubhav-pandey1/orderbook-constructor/book"
	"github.com/anubhav-pandey1/orderbook-constructor/feed"
	"github.com/anubhav-pandey1/orderbook-constructor/replay"
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
	requests []replay.ResyncRequest
	err      error
}

func (r *requestRecorder) RequestSnapshot(_ context.Context, req replay.ResyncRequest) error {
	r.requests = append(r.requests, req)
	return r.err
}

type eventCollector struct {
	events []replay.Event
}

func (c *eventCollector) OnEvent(_ context.Context, event replay.Event) error {
	c.events = append(c.events, event)
	return nil
}

func replayOptions(mode replay.Mode, policy replay.Policy, clk replay.Clock) replay.Options {
	return replay.Options{
		Mode:          mode,
		Speed:         1,
		TimestampUnit: time.Millisecond,
		Stream:        fixtureStream,
		Policy:        policy,
		Clock:         clk,
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
	events, requester, bk := &eventCollector{}, &requestRecorder{}, book.New(16)
	opts := replayOptions(replay.Fast, replay.NewTimestampPolicy(replay.TimestampStep, 100), &testClock{now: 10_000})
	opts.SnapshotRequester = requester
	stats, err := replay.Run(context.Background(), feed.NewDecoder(strings.NewReader(data)), bk, events, opts)
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
	if req.Exchange != "binance" || req.Symbol != "BTCUSDT" || req.Last.Timestamp != 1100 || req.Received.Timestamp != 1300 || req.Reason != replay.ReasonGap {
		t.Fatalf("request=%+v", req)
	}
	if len(events.events) != 5 {
		t.Fatalf("events=%d", len(events.events))
	}
	wantVersions := []uint64{1, 2, 2, 3, 4}
	wantEpochs := []uint64{1, 1, 1, 2, 2}
	for i, event := range events.events {
		if event.NotificationID != uint64(i+1) || event.Version != wantVersions[i] || event.SyncEpoch != wantEpochs[i] || event.DueNS != 0 {
			t.Errorf("event[%d]=%+v", i, event)
		}
	}
	if events.events[2].Kind != replay.BookInvalidated || events.events[2].State != replay.Desynchronized || events.events[2].Reason != replay.ReasonGap || events.events[2].BidOK || events.events[2].AskOK {
		t.Fatalf("invalidation=%+v", events.events[2])
	}
}

func TestReplayCrossedDeltaDoesNotAdvanceCursor(t *testing.T) {
	data := buildCSV(csvHeader,
		`snapshot,binance,BTC/USDT,1000,,"[[100.00,1.0]]","[[101.00,1.0]]",,`,
		`incremental,binance,BTC/USDT,1100,bid,,,102.00,1.0`,
		`snapshot,binance,BTC/USDT,1050,,"[[99.00,1.0]]","[[102.00,1.0]]",,`)
	events, requester, bk := &eventCollector{}, &requestRecorder{}, book.New(8)
	opts := replayOptions(replay.Fast, replay.NewTimestampPolicy(replay.TimestampStep, 100), &testClock{now: 20_000})
	opts.SnapshotRequester = requester
	stats, err := replay.Run(context.Background(), feed.NewDecoder(strings.NewReader(data)), bk, events, opts)
	if err != nil {
		t.Fatal(err)
	}
	if stats.Applied != 2 || stats.Invalidated != 1 || stats.Crossed != 1 || stats.SnapshotRequests != 1 || bk.Version() != 2 {
		t.Fatalf("stats=%+v version=%d", stats, bk.Version())
	}
	if len(events.events) != 3 || events.events[1].Kind != replay.BookInvalidated || events.events[2].Kind != replay.SnapshotApplied {
		t.Fatalf("events=%+v", events.events)
	}
}

func TestReplayStaleRecoveryEndsDesynchronized(t *testing.T) {
	data := buildCSV(csvHeader,
		`snapshot,binance,BTC/USDT,1000,,"[[100.00,1.0]]","[[101.00,1.0]]",,`,
		`incremental,binance,BTC/USDT,1200,bid,,,99.00,1.0`,
		`snapshot,binance,BTC/USDT,900,,"[[99.00,1.0]]","[[102.00,1.0]]",,`)
	opts := replayOptions(replay.Fast, replay.NewTimestampPolicy(replay.TimestampStep, 100), &testClock{now: 30_000})
	opts.SnapshotRequester = &requestRecorder{}
	stats, err := replay.Run(context.Background(), feed.NewDecoder(strings.NewReader(data)), book.New(8), &eventCollector{}, opts)
	if !errors.Is(err, replay.ErrSnapshotRequired) {
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
	events, clk := &eventCollector{}, &testClock{now: 1_000_000_000}
	opts := replayOptions(replay.Paced, replay.NewTimestampPolicy(replay.TimestampStep, 100), clk)
	opts.Speed = 2
	_, err := replay.Run(context.Background(), feed.NewDecoder(strings.NewReader(data)), book.New(8), events, opts)
	if err != nil {
		t.Fatal(err)
	}
	want := []int64{1_000_000_000, 1_050_000_000}
	if len(clk.sleeps) != 2 || clk.sleeps[0] != want[0] || clk.sleeps[1] != want[1] {
		t.Fatalf("sleeps=%v", clk.sleeps)
	}
	for i, event := range events.events {
		if event.DueNS != want[i] || event.IngressNS < event.DueNS || event.ApplyNS < event.IngressNS {
			t.Errorf("timing=%+v", event)
		}
	}
}

func TestReplayFastAndPacedAreStateEquivalent(t *testing.T) {
	data := buildCSV(csvHeader,
		`snapshot,binance,BTC/USDT,1000,,"[[100.00,1.0]]","[[101.00,1.0]]",,`,
		`incremental,binance,BTC/USDT,1100,bid,,,100.00,2.0`,
		`snapshot,binance,BTC/USDT,1200,,"[[99.00,3.0]]","[[102.00,4.0]]",,`,
		`incremental,binance,BTC/USDT,1300,ask,,,102.00,5.0`)

	run := func(mode replay.Mode) (replay.Stats, book.Depth, []replay.Event) {
		t.Helper()
		events := &eventCollector{}
		bk := book.New(8)
		opts := replayOptions(mode, replay.NewTimestampPolicy(replay.TimestampStep, 100), &testClock{now: 10_000})
		if mode == replay.Paced {
			opts.Speed = 2
		}
		stats, err := replay.Run(context.Background(), feed.NewDecoder(strings.NewReader(data)), bk, events, opts)
		if err != nil {
			t.Fatal(err)
		}
		return stats, bk.DepthSnapshot(), events.events
	}

	fastStats, fastDepth, fastEvents := run(replay.Fast)
	pacedStats, pacedDepth, pacedEvents := run(replay.Paced)
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
	opts := replayOptions(replay.Fast, p, &testClock{})
	_, err := replay.Run(context.Background(), feed.NewDecoder(strings.NewReader(data)), book.New(8), nil, opts)
	if !errors.Is(err, replay.ErrStreamMismatch) || p.n != 0 {
		t.Fatalf("err=%v classifications=%d", err, p.n)
	}
}

type countingPolicy struct{ n int }

func (p *countingPolicy) ClassifySnapshot(replay.Cursor) replay.Decision {
	p.n++
	return replay.Decision{Action: replay.Apply}
}
func (p *countingPolicy) ClassifyUpdate(replay.Cursor) replay.Decision {
	p.n++
	return replay.Decision{Action: replay.Apply}
}
func (*countingPolicy) AcceptSnapshot(replay.Cursor) {}
func (*countingPolicy) AcceptUpdate(replay.Cursor)   {}
func (*countingPolicy) Invalidate()                  {}
