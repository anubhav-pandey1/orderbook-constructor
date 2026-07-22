package strategy

import (
	"context"
	"errors"
	"testing"

	"github.com/anubhav-pandey1/orderbook-constructor/internal/bench"
	"github.com/anubhav-pandey1/orderbook-constructor/internal/clock"
	"github.com/anubhav-pandey1/orderbook-constructor/internal/ring"
	"github.com/anubhav-pandey1/orderbook-constructor/replay"
)

type closeCapture struct {
	closeErr error
	err      error
	closed   bool
	seen     int
}

func (c *closeCapture) OnEvent(replay.Event, int64) { c.seen++ }
func (c *closeCapture) Err() error                  { return c.err }
func (c *closeCapture) Close() error {
	c.closed = true
	return c.closeErr
}

func TestRunWithSpinNilDependencies(t *testing.T) {
	q, _ := ring.NewSPSC[replay.Event](2)
	c := clock.NewSim(0)
	for _, tc := range []struct {
		q *ring.SPSC[replay.Event]
		s Strategy
		c clock.Clock
	}{
		{nil, &NopStrategy{}, c},
		{q, nil, c},
		{q, &NopStrategy{}, nil},
	} {
		if err := RunWithSpin(nil, tc.q, tc.s, tc.c, -1); err == nil || err.Error() != "strategy: nil dependency" {
			t.Fatalf("q=%v strategy=%T clock=%T err=%v", tc.q, tc.s, tc.c, err)
		}
	}
}

func TestRunWithSpinStrategyErrAndCloseJoin(t *testing.T) {
	q, _ := ring.NewSPSC[replay.Event](2)
	q.TryPublish(replay.Event{Version: 1})
	_ = q.Close()
	strategyErr := errors.New("strategy err")
	closeErr := errors.New("close err")
	s := &closeCapture{err: strategyErr, closeErr: closeErr}
	err := RunWithSpin(context.Background(), q, s, clock.NewSim(10), -1)
	if !errors.Is(err, strategyErr) || !errors.Is(err, closeErr) {
		t.Fatalf("err=%v", err)
	}
	if !s.closed || s.seen != 1 {
		t.Fatalf("state=%+v", s)
	}
}

func TestLatencyAndNopNilSafety(t *testing.T) {
	var nilLatency *Latency
	nilLatency.Record(replay.Event{}, 1)
	var nilStrategy *NopStrategy
	nilStrategy.OnEvent(replay.Event{}, 1)
	h := bench.NewHist()
	lat := &Latency{IngressToRecv: h, ApplyToRecv: h, DueToRecv: h, SchedulerLateness: h}
	e := replay.Event{IngressNS: 10, ApplyNS: 20, DueNS: 5}
	lat.Record(e, 25)
	if h.Count() != 4 {
		t.Fatalf("hist count=%d", h.Count())
	}
	if h.Min() != 5 || h.Max() != 20 {
		t.Fatalf("hist min/max=%d/%d", h.Min(), h.Max())
	}
}

func TestLogStrategyNilAndActionable(t *testing.T) {
	s := NewLogStrategy(nil, nil, nil)
	s.OnEvent(replay.Event{Kind: replay.SnapshotApplied, State: replay.Synchronized}, 1)
	if !s.Actionable() || s.Err() != nil {
		t.Fatalf("strategy actionable/err=%v/%v", s.Actionable(), s.Err())
	}
	var nilLog *LogStrategy
	if nilLog.Actionable() || nilLog.Err() != nil || nilLog.Close() != nil {
		t.Fatal("nil log strategy methods should be safe")
	}
}
