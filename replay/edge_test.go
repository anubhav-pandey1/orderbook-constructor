package replay

import (
	"context"
	"errors"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/anubhav-pandey1/orderbook-constructor/book"
	"github.com/anubhav-pandey1/orderbook-constructor/feed"
)

const testHeader = "type,exchange,symbol,timestamp,side,bids,asks,price,size"

var testStream = feed.StreamID{Exchange: "binance", Symbol: "BTCUSDT"}

type runTestClock struct {
	now      int64
	sleepErr error
}

func (c *runTestClock) NowNS() int64 { return c.now }
func (c *runTestClock) SleepUntilNS(ctx context.Context, target int64) error {
	if c.sleepErr != nil {
		return c.sleepErr
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if target > c.now {
		c.now = target
	}
	return nil
}

type errHandler struct{ err error }

func (h errHandler) OnEvent(context.Context, Event) error { return h.err }

type errRequester struct{ err error }

func (r errRequester) RequestSnapshot(context.Context, ResyncRequest) error { return r.err }

type actionPolicy struct{ decision Decision }

func (p actionPolicy) ClassifySnapshot(Cursor) Decision { return p.decision }
func (p actionPolicy) ClassifyUpdate(Cursor) Decision   { return p.decision }
func (p actionPolicy) AcceptSnapshot(Cursor)            {}
func (p actionPolicy) AcceptUpdate(Cursor)              {}
func (p actionPolicy) Invalidate()                      {}

func replayCSV(rows ...string) string {
	return strings.Join(append([]string{testHeader}, rows...), "\n") + "\n"
}

func baseOptions(policy Policy) Options {
	return Options{Mode: Fast, Speed: 1, TimestampUnit: time.Millisecond, Stream: testStream, Policy: policy, Clock: &runTestClock{}}
}

func requireReplayErrorContains(t *testing.T, err error, want string) {
	t.Helper()
	if err == nil {
		t.Fatalf("expected error containing %q", want)
	}
	if !strings.Contains(err.Error(), want) {
		t.Fatalf("error=%v, want substring %q", err, want)
	}
}

func TestRunDefaultOptionsAndNilHandler(t *testing.T) {
	data := replayCSV(
		`snapshot,binance,BTC/USDT,1000,,"[[100.00,1.0]]","[[101.00,1.0]]",,`,
		`incremental,binance,BTC/USDT,900,bid,,,100.00,2.0`,
	)
	bk := book.New(4)
	stats, err := Run(nil, feed.NewDecoder(strings.NewReader(data)), bk, nil, Options{Stream: testStream})
	if err != nil {
		t.Fatal(err)
	}
	if stats.Applied != 2 || stats.Snapshots != 1 || stats.Deltas != 1 || stats.LastAcceptedTS != 900 || stats.HighestSeenTS != 1000 {
		t.Fatalf("stats=%+v", stats)
	}
	bbo := bk.BBOSnapshot()
	if bbo.Version != 2 || !bbo.BidOK || !bbo.AskOK || bbo.BidQty != 20000 {
		t.Fatalf("bbo=%+v", bbo)
	}
}

func TestRunOptionValidation(t *testing.T) {
	dec := feed.NewDecoder(strings.NewReader(replayCSV()))
	for _, tc := range []struct {
		name, want string
		dec        *feed.Decoder
		book       *book.Book
		opts       Options
	}{
		{"nil decoder", "decoder, book, sync policy, and clock are required", nil, book.New(1), baseOptions(NewArrivalOrderPolicy())},
		{"nil book", "decoder, book, sync policy, and clock are required", dec, nil, baseOptions(NewArrivalOrderPolicy())},
		{"invalid mode", "invalid replay mode", dec, book.New(1), Options{Mode: Mode(99), Speed: 1, Stream: testStream, Policy: NewArrivalOrderPolicy(), Clock: &runTestClock{}}},
		{"zero stream", "configured stream", dec, book.New(1), Options{Mode: Fast, Speed: 1, Policy: NewArrivalOrderPolicy(), Clock: &runTestClock{}}},
		{"unnormalized stream", "configured stream must be normalized", dec, book.New(1), Options{Mode: Fast, Speed: 1, Stream: feed.StreamID{Exchange: "BINANCE", Symbol: "BTC/USDT"}, Policy: NewArrivalOrderPolicy(), Clock: &runTestClock{}}},
		{"nan speed", "speed must be finite", dec, book.New(1), Options{Mode: Fast, Speed: math.NaN(), Stream: testStream, Policy: NewArrivalOrderPolicy(), Clock: &runTestClock{}}},
		{"inf speed", "speed must be finite", dec, book.New(1), Options{Mode: Fast, Speed: math.Inf(1), Stream: testStream, Policy: NewArrivalOrderPolicy(), Clock: &runTestClock{}}},
		{"paced missing unit", "timestamp unit must be greater", dec, book.New(1), Options{Mode: Paced, Speed: 1, Stream: testStream, Policy: NewArrivalOrderPolicy(), Clock: &runTestClock{}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Run(context.Background(), tc.dec, tc.book, nil, tc.opts)
			requireReplayErrorContains(t, err, tc.want)
		})
	}
}

func TestRunContextCancellationBeforeRead(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := Run(ctx, feed.NewDecoder(strings.NewReader(replayCSV())), book.New(1), nil, baseOptions(NewArrivalOrderPolicy()))
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err=%v", err)
	}
}

func TestRunHandlerErrorStopsWithAppliedStats(t *testing.T) {
	sentinel := errors.New("handler failed")
	data := replayCSV(`snapshot,binance,BTC/USDT,1000,,"[[100.00,1.0]]","[[101.00,1.0]]",,`)
	stats, err := Run(context.Background(), feed.NewDecoder(strings.NewReader(data)), book.New(4), errHandler{sentinel}, baseOptions(NewArrivalOrderPolicy()))
	if !errors.Is(err, sentinel) {
		t.Fatalf("err=%v", err)
	}
	if stats.Applied != 1 || stats.Snapshots != 1 {
		t.Fatalf("stats=%+v", stats)
	}
}

func TestRunSnapshotRequesterErrorStopsAfterInvalidation(t *testing.T) {
	sentinel := errors.New("request failed")
	data := replayCSV(
		`snapshot,binance,BTC/USDT,1000,,"[[100.00,1.0]]","[[101.00,1.0]]",,`,
		`incremental,binance,BTC/USDT,1200,bid,,,99.00,1.0`,
	)
	opts := baseOptions(NewTimestampPolicy(TimestampStep, 100))
	opts.SnapshotRequester = errRequester{sentinel}
	stats, err := Run(context.Background(), feed.NewDecoder(strings.NewReader(data)), book.New(4), nil, opts)
	if !errors.Is(err, sentinel) {
		t.Fatalf("err=%v", err)
	}
	if !strings.Contains(err.Error(), "request snapshot") {
		t.Fatalf("err=%v", err)
	}
	if stats.Applied != 1 || stats.Invalidated != 1 || stats.SnapshotRequests != 1 || stats.Gaps != 1 {
		t.Fatalf("stats=%+v", stats)
	}
}

func TestRunInvalidPolicyAction(t *testing.T) {
	data := replayCSV(`snapshot,binance,BTC/USDT,1000,,"[[100.00,1.0]]","[[101.00,1.0]]",,`)
	_, err := Run(context.Background(), feed.NewDecoder(strings.NewReader(data)), book.New(4), nil, baseOptions(actionPolicy{Decision{Action: Action(99)}}))
	if err == nil || !strings.Contains(err.Error(), "invalid synchronization action") {
		t.Fatalf("err=%v", err)
	}
}

func TestRunEOFWhileUnsynchronized(t *testing.T) {
	_, err := Run(context.Background(), feed.NewDecoder(strings.NewReader(replayCSV())), book.New(1), nil, baseOptions(NewArrivalOrderPolicy()))
	if !errors.Is(err, ErrSnapshotRequired) {
		t.Fatalf("err=%v", err)
	}
	var need *SnapshotRequiredError
	if !errors.As(err, &need) || need.State != Uninitialized {
		t.Fatalf("snapshot error=%+v", err)
	}
}

func TestRunPacedSleepErrorAndOverflow(t *testing.T) {
	sentinel := errors.New("sleep failed")
	data := replayCSV(`snapshot,binance,BTC/USDT,1000,,"[[100.00,1.0]]","[[101.00,1.0]]",,`)
	opts := baseOptions(NewArrivalOrderPolicy())
	opts.Mode = Paced
	opts.Clock = &runTestClock{sleepErr: sentinel}
	_, err := Run(context.Background(), feed.NewDecoder(strings.NewReader(data)), book.New(4), nil, opts)
	if !errors.Is(err, sentinel) {
		t.Fatalf("err=%v", err)
	}
	if _, err = pacedDueNS(math.MaxInt64, 2, time.Second, 1); err == nil || !strings.Contains(err.Error(), "overflows") {
		t.Fatalf("overflow err=%v", err)
	}
}

func TestEventActionable(t *testing.T) {
	for _, tc := range []struct {
		event Event
		want  bool
	}{
		{Event{Kind: SnapshotApplied, State: Synchronized}, true},
		{Event{Kind: IncrementalApplied, State: Synchronized}, true},
		{Event{Kind: BookInvalidated, State: Desynchronized}, false},
		{Event{Kind: SnapshotApplied, State: Desynchronized}, false},
	} {
		if got := tc.event.Actionable(); got != tc.want {
			t.Fatalf("Actionable(%+v)=%v", tc.event, got)
		}
	}
}
