package pipeline_test

import (
	"context"
	"orderbook/internal/bench"
	"orderbook/internal/clock"
	"orderbook/internal/logx"
	"orderbook/internal/pipeline"
	"orderbook/internal/ring"
	"orderbook/internal/strategy"
	"orderbook/internal/syncx"
	"testing"
)

func TestEventStrategyLogger(t *testing.T) {
	l, e := logx.New(logx.Config{Sink: logx.SinkDiscard, RingSize: 4})
	if e != nil {
		t.Fatal(e)
	}
	ld := make(chan error, 1)
	go func() { ld <- l.Run(context.Background()) }()
	h := &strategy.Latency{IngressToRecv: bench.NewHist(), ApplyToRecv: bench.NewHist()}
	s := strategy.NewLogStrategy(context.Background(), l, h)
	q, err := ring.NewSPSC[pipeline.Event](4)
	if err != nil {
		t.Fatal(err)
	}
	q.TryPublish(pipeline.Event{NotificationID: 1, Version: 1, Kind: pipeline.SnapshotApplied, State: syncx.Synchronized, BidOK: true, AskOK: true, IngressNS: 100, ApplyNS: 200})
	_ = q.Close()
	if err := strategy.Run(context.Background(), q, s, clock.NewSim(300)); err != nil {
		t.Fatal(err)
	}
	if err := <-ld; err != nil {
		t.Fatal(err)
	}
	if h.IngressToRecv.Count() != 1 || h.ApplyToRecv.Count() != 1 {
		t.Fatal("latency")
	}
	if m := l.Metrics(); m.Enqueued != 1 || m.Written != 1 {
		t.Fatal(m)
	}
}
