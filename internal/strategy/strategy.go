package strategy

import (
	"context"
	"errors"
	"github.com/anubhav-pandey1/orderbook-constructor/internal/bench"
	"github.com/anubhav-pandey1/orderbook-constructor/internal/clock"
	"github.com/anubhav-pandey1/orderbook-constructor/internal/logx"
	"github.com/anubhav-pandey1/orderbook-constructor/internal/ring"
	"github.com/anubhav-pandey1/orderbook-constructor/replay"
)

const defaultSpinIters = 128

type Strategy interface{ OnEvent(replay.Event, int64) }
type Latency struct{ IngressToRecv, ApplyToRecv, DueToRecv, SchedulerLateness *bench.Hist }

func (l *Latency) Record(e replay.Event, recv int64) {
	if l == nil {
		return
	}
	if l.IngressToRecv != nil && recv >= e.IngressNS {
		l.IngressToRecv.Record(recv - e.IngressNS)
	}
	if l.ApplyToRecv != nil && recv >= e.ApplyNS {
		l.ApplyToRecv.Record(recv - e.ApplyNS)
	}
	if e.DueNS != 0 {
		if l.DueToRecv != nil && recv >= e.DueNS {
			l.DueToRecv.Record(recv - e.DueNS)
		}
		if l.SchedulerLateness != nil && e.IngressNS >= e.DueNS {
			l.SchedulerLateness.Record(e.IngressNS - e.DueNS)
		}
	}
}

type NopStrategy struct {
	H       *bench.Hist
	Latency *Latency
}

func (s *NopStrategy) OnEvent(e replay.Event, r int64) {
	if s == nil {
		return
	}
	if s.H != nil && r >= e.IngressNS {
		s.H.Record(r - e.IngressNS)
	}
	s.Latency.Record(e, r)
}

type LogStrategy struct {
	logger     *logx.Logger
	latency    *Latency
	ctx        context.Context
	err        error
	actionable bool
}

func NewLogStrategy(ctx context.Context, l *logx.Logger, h *Latency) *LogStrategy {
	if ctx == nil {
		ctx = context.Background()
	}
	return &LogStrategy{logger: l, latency: h, ctx: ctx}
}
func (s *LogStrategy) OnEvent(e replay.Event, r int64) {
	if s == nil || s.err != nil {
		return
	}
	s.latency.Record(e, r)
	s.actionable = e.Actionable()
	if s.logger == nil {
		return
	}
	s.err = s.logger.Log(s.ctx, logx.Record{NotificationID: e.NotificationID, Version: e.Version, SyncEpoch: e.SyncEpoch, Kind: e.Kind, State: e.State, Reason: e.Reason, BidPx: e.BidPx, AskPx: e.AskPx, BidQty: e.BidQty, AskQty: e.AskQty, BidOK: e.BidOK, AskOK: e.AskOK, EventTS: e.EventTS, DueNS: e.DueNS, IngressNS: e.IngressNS, ApplyNS: e.ApplyNS, RecvNS: r})
}
func (s *LogStrategy) Err() error {
	if s == nil {
		return nil
	}
	return s.err
}
func (s *LogStrategy) Close() error {
	if s == nil || s.logger == nil {
		return nil
	}
	return s.logger.Close()
}
func (s *LogStrategy) Actionable() bool { return s != nil && s.actionable }
func Run(ctx context.Context, in *ring.SPSC[replay.Event], s Strategy, c clock.Clock) error {
	return RunWithSpin(ctx, in, s, c, defaultSpinIters)
}
func RunWithSpin(ctx context.Context, in *ring.SPSC[replay.Event], s Strategy, c clock.Clock, spin int) (err error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if in == nil || s == nil || c == nil {
		return errors.New("strategy: nil dependency")
	}
	defer func() {
		if x, ok := s.(interface{ Close() error }); ok {
			err = errors.Join(err, x.Close())
		}
	}()
	for {
		e, ok, x := in.ConsumeWait(ctx, spin)
		if x != nil {
			return x
		}
		if !ok {
			return nil
		}
		recv := c.NowNS()
		s.OnEvent(e, recv)
		if x, ok := s.(interface{ Err() error }); ok && x.Err() != nil {
			return x.Err()
		}
	}
}
