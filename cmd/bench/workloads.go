package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"runtime"
	"time"

	"orderbook/book"
	"orderbook/feed"
	"orderbook/feed/gencsv"
	benchmetrics "orderbook/internal/bench"
	obclock "orderbook/internal/clock"
	"orderbook/internal/logx"
	"orderbook/internal/pipeline"
	"orderbook/internal/ring"
	"orderbook/internal/strategy"
	"orderbook/internal/syncx"
)

type measurement struct {
	name                                    string
	n                                       uint64
	duration                                time.Duration
	mallocs, bytes, eventDepth              uint64
	log                                     logx.Metrics
	ingress, apply, mutation, due, lateness *benchmetrics.Hist
}

func (m measurement) rate() float64 {
	return (benchmetrics.Throughput{N: m.n, Dur: m.duration}).PerSec()
}
func (m measurement) allocsPerOp() float64 {
	if m.n == 0 {
		return 0
	}
	return float64(m.mallocs) / float64(m.n)
}
func (m measurement) bytesPerOp() float64 {
	if m.n == 0 {
		return 0
	}
	return float64(m.bytes) / float64(m.n)
}

type backpressureResult struct {
	n, waits, maxDepth uint64
	duration           time.Duration
}
type suite struct {
	fixtureRows                                       int
	apply, decodeApply, fixture, synthetic, snapshots measurement
	policyStep, policyOff, policyID, paced            measurement
	backpressure                                      backpressureResult
}

func runSuite(cfg config) (string, error) {
	data, err := os.ReadFile(cfg.csvPath)
	if err != nil {
		return "", err
	}
	records, err := decodeAll(data)
	if err != nil {
		return "", err
	}
	if len(records) == 0 {
		return "", fmt.Errorf("fixture contains no records")
	}
	stream, err := feed.NormalizeStreamID(cfg.exchange, cfg.symbol)
	if err != nil {
		return "", err
	}
	for i := 0; i < cfg.warmup; i++ {
		if _, err = applyOnce(records); err != nil {
			return "", err
		}
		if _, err = decodeApplyOnce(data); err != nil {
			return "", err
		}
	}

	s := suite{fixtureRows: len(records)}
	s.apply, err = repeatMeasured("W2 apply-only fixture mix", cfg.fixtureIters, func() (uint64, error) { return applyOnce(records) })
	if err != nil {
		return "", err
	}
	s.decodeApply, err = repeatMeasured("W3 decode+apply", cfg.fixtureIters, func() (uint64, error) { return decodeApplyOnce(data) })
	if err != nil {
		return "", err
	}
	s.fixture, err = replayFixture(data, stream, cfg, feed.Fast, 1, cfg.fixtureIters, true)
	if err != nil {
		return "", fmt.Errorf("W1: %w", err)
	}

	gc := gencsv.DefaultConfig()
	gc.Incrementals = cfg.synthetic
	gc.MaxLevels = cfg.syntheticMax
	gc.Seed = cfg.seed
	s.synthetic, err = generatedPipeline("W4/W6a delta-only full pipeline", gc, cfg, policyTimestamp)
	if err != nil {
		return "", err
	}
	gc.SnapshotEvery = cfg.snapshotEvery
	s.snapshots, err = generatedPipeline("W6b periodic-snapshot full pipeline", gc, cfg, policyTimestamp)
	if err != nil {
		return "", err
	}

	policyN := cfg.synthetic / 10
	if policyN < 100_000 {
		policyN = min(cfg.synthetic, 100_000)
	}
	gc.Incrementals, gc.SnapshotEvery = policyN, 0
	s.policyStep, err = generatedPipeline("timestamp-step", gc, cfg, policyTimestamp)
	if err != nil {
		return "", err
	}
	s.policyOff, err = generatedPipeline("synchronization-off", gc, cfg, policyArrival)
	if err != nil {
		return "", err
	}
	s.policyID, err = generatedPipeline("update-ID", gc, cfg, policyUpdateID)
	if err != nil {
		return "", err
	}
	s.backpressure, err = runBackpressure(20_000, min(cfg.eventRing, 256), cfg.spin)
	if err != nil {
		return "", err
	}
	s.paced, err = replayFixture(data, stream, cfg, feed.Paced, cfg.pacedSpeed, 1, false)
	if err != nil {
		return "", fmt.Errorf("W7: %w", err)
	}
	return renderReport(cfg, data, s), nil
}

func decodeAll(data []byte) ([]feed.Record, error) {
	d := feed.NewDecoder(bytes.NewReader(data))
	var out []feed.Record
	for {
		r, err := d.Next()
		if err == io.EOF {
			return out, nil
		}
		if err != nil {
			return nil, err
		}
		out = append(out, r)
	}
}
func applyOnce(records []feed.Record) (uint64, error) {
	b := book.New(512)
	for i, r := range records {
		var err error
		if r.Kind == feed.KindSnapshot {
			_, err = b.ApplySnapshot(r.Snap)
		} else {
			_, err = b.ApplyDelta(r.Side, r.Px, r.Qty)
		}
		if err != nil {
			return uint64(i), err
		}
	}
	return uint64(len(records)), nil
}
func decodeApplyOnce(data []byte) (uint64, error) {
	d := feed.NewDecoder(bytes.NewReader(data))
	b := book.New(512)
	var n uint64
	for {
		r, err := d.Next()
		if err == io.EOF {
			return n, nil
		}
		if err != nil {
			return n, err
		}
		if r.Kind == feed.KindSnapshot {
			_, err = b.ApplySnapshot(r.Snap)
		} else {
			_, err = b.ApplyDelta(r.Side, r.Px, r.Qty)
		}
		if err != nil {
			return n, err
		}
		n++
	}
}
func repeatMeasured(name string, iters int, fn func() (uint64, error)) (measurement, error) {
	before := mem()
	start := time.Now()
	var n uint64
	for i := 0; i < iters; i++ {
		x, err := fn()
		if err != nil {
			return measurement{}, err
		}
		n += x
	}
	after := mem()
	return measurement{name: name, n: n, duration: time.Since(start), mallocs: after.Mallocs - before.Mallocs, bytes: after.TotalAlloc - before.TotalAlloc}, nil
}
func mem() runtime.MemStats { var m runtime.MemStats; runtime.ReadMemStats(&m); return m }
func latency() *strategy.Latency {
	return &strategy.Latency{IngressToRecv: benchmetrics.NewHist(), ApplyToRecv: benchmetrics.NewHist(), DueToRecv: benchmetrics.NewHist(), SchedulerLateness: benchmetrics.NewHist()}
}

type observedStrategy struct {
	inner strategy.Strategy
	ring  *ring.SPSC[pipeline.Event]
	max   uint64
}

func (s *observedStrategy) OnEvent(e pipeline.Event, recv int64) {
	s.max = max(s.max, uint64(s.ring.Len()+1))
	s.inner.OnEvent(e, recv)
}

func (s *observedStrategy) Err() error {
	if source, ok := s.inner.(interface{ Err() error }); ok {
		return source.Err()
	}
	return nil
}

func (s *observedStrategy) Close() error {
	if closer, ok := s.inner.(interface{ Close() error }); ok {
		return closer.Close()
	}
	return nil
}

func replayFixture(data []byte, stream feed.StreamID, cfg config, mode feed.ReplayMode, speed float64, iters int, withLogger bool) (measurement, error) {
	lat := latency()
	out := measurement{name: "W1 fixture timestamp-step pipeline", ingress: lat.IngressToRecv, apply: lat.ApplyToRecv, due: lat.DueToRecv, lateness: lat.SchedulerLateness}
	before := mem()
	start := time.Now()
	for range iters {
		clk := obclock.NewReal()
		events, err := ring.NewSPSC[pipeline.Event](cfg.eventRing)
		if err != nil {
			return measurement{}, err
		}
		var inner strategy.Strategy = &strategy.NopStrategy{Latency: lat}
		var logger *logx.Logger
		var loggerDone chan error
		if withLogger {
			logger, err = logx.New(logx.Config{Sink: logx.SinkDiscard, Delivery: logx.Lossless, RingSize: cfg.logRing, SpinIters: cfg.spin})
			if err != nil {
				return measurement{}, err
			}
			loggerDone = make(chan error, 1)
			go func() { loggerDone <- logger.Run(context.Background()) }()
			inner = strategy.NewLogStrategy(context.Background(), logger, lat)
		}
		obs := &observedStrategy{inner: inner, ring: events}
		done := make(chan error, 1)
		go func() { done <- strategy.RunWithSpin(context.Background(), events, obs, clk, cfg.spin) }()
		stats, replayErr := feed.Replay(context.Background(), feed.NewDecoder(bytes.NewReader(data)), book.New(512), syncx.NewTimestampPolicy(syncx.TimestampStep, 100), nil, events, feed.ReplayCfg{Mode: mode, Speed: speed, TSUnit: time.Millisecond, SpinIters: cfg.spin, Stream: stream}, clk)
		strategyErr := <-done
		var loggerErr error
		if loggerDone != nil {
			loggerErr = <-loggerDone
			metrics := logger.Metrics()
			out.log.Enqueued += metrics.Enqueued
			out.log.Written += metrics.Written
			out.log.Dropped += metrics.Dropped
			out.log.WaitCount += metrics.WaitCount
			out.log.MaxDepth = max(out.log.MaxDepth, metrics.MaxDepth)
		}
		if err = errors.Join(replayErr, strategyErr, loggerErr); err != nil {
			return measurement{}, err
		}
		out.n += stats.Applied
		out.eventDepth = max(out.eventDepth, obs.max)
	}
	after := mem()
	out.duration = time.Since(start)
	out.mallocs, out.bytes = after.Mallocs-before.Mallocs, after.TotalAlloc-before.TotalAlloc
	return out, nil
}

type policyKind uint8

const (
	policyTimestamp policyKind = iota + 1
	policyArrival
	policyUpdateID
)

func newPolicy(k policyKind, step int64) syncx.Policy {
	if k == policyTimestamp {
		return syncx.NewTimestampPolicy(syncx.TimestampStep, step)
	}
	if k == policyUpdateID {
		return syncx.NewUpdateIDPolicy()
	}
	return syncx.NewArrivalOrderPolicy()
}

func generatedPipeline(name string, gcfg gencsv.Config, cfg config, pk policyKind) (measurement, error) {
	gen, err := gencsv.NewGenerator(gcfg)
	if err != nil {
		return measurement{}, err
	}
	events, err := ring.NewSPSC[pipeline.Event](cfg.eventRing)
	if err != nil {
		return measurement{}, err
	}
	logger, err := logx.New(logx.Config{Sink: logx.SinkDiscard, Delivery: logx.Lossless, RingSize: cfg.logRing, SpinIters: cfg.spin})
	if err != nil {
		return measurement{}, err
	}
	ctx := context.Background()
	clk := obclock.NewReal()
	lat := latency()
	mut := benchmetrics.NewHist()
	strat := strategy.NewLogStrategy(ctx, logger, lat)
	obs := &observedStrategy{inner: strat, ring: events}
	sdone := make(chan error, 1)
	ldone := make(chan error, 1)
	go func() { ldone <- logger.Run(ctx) }()
	go func() { sdone <- strategy.RunWithSpin(ctx, events, obs, clk, cfg.spin) }()
	policy := newPolicy(pk, gcfg.TSStep)
	bk := book.New(gcfg.MaxLevels)
	var n, notification, epoch uint64
	var producerErr error
	before := mem()
	start := time.Now()
	for {
		rec, ok := gen.Next()
		if !ok {
			break
		}
		ingress := clk.NowNS()
		cursor := syncx.Cursor{Timestamp: rec.TS, FirstUpdateID: rec.FirstUpdateID, FinalUpdateID: rec.FinalUpdateID, HasUpdateID: rec.HasUpdateID}
		decision := policy.ClassifyUpdate(cursor)
		if rec.Kind == feed.KindSnapshot {
			decision = policy.ClassifySnapshot(cursor)
		}
		if decision.Action != syncx.Apply {
			producerErr = fmt.Errorf("%s record %d action=%d reason=%d", name, n, decision.Action, decision.Reason)
			break
		}
		mutationStart := clk.NowNS()
		var bbo book.BBO
		var kind pipeline.EventKind
		if rec.Kind == feed.KindSnapshot {
			bbo, err = bk.ApplySnapshot(rec.Snap)
			kind = pipeline.SnapshotApplied
		} else {
			var d book.DeltaResult
			d, err = bk.ApplyDelta(rec.Side, rec.Px, rec.Qty)
			bbo = d.BBO
			kind = pipeline.IncrementalApplied
		}
		if err != nil {
			producerErr = fmt.Errorf("%s record %d: %w", name, n, err)
			break
		}
		applyNS := clk.NowNS()
		mut.Record(applyNS - mutationStart)
		if rec.Kind == feed.KindSnapshot {
			policy.AcceptSnapshot(cursor)
			epoch++
		} else {
			policy.AcceptUpdate(cursor)
		}
		notification++
		ev := pipeline.Event{NotificationID: notification, Version: bbo.Version, SyncEpoch: epoch, Kind: kind, State: syncx.Synchronized, BidPx: bbo.BidPx, AskPx: bbo.AskPx, BidQty: bbo.BidQty, AskQty: bbo.AskQty, BidOK: bbo.BidOK, AskOK: bbo.AskOK, EventTS: rec.TS, IngressNS: ingress, ApplyNS: applyNS}
		if err = events.Publish(ctx, ev, cfg.spin); err != nil {
			producerErr = err
			break
		}
		n++
	}
	_ = events.Close()
	strategyErr := <-sdone
	loggerErr := <-ldone
	duration := time.Since(start)
	after := mem()
	if err = errors.Join(producerErr, strategyErr, loggerErr, strat.Err()); err != nil {
		return measurement{}, err
	}
	lm := logger.Metrics()
	if lm.Written != n || lm.Dropped != 0 {
		return measurement{}, fmt.Errorf("%s logger delivery=%+v events=%d", name, lm, n)
	}
	return measurement{name: name, n: n, duration: duration, mallocs: after.Mallocs - before.Mallocs, bytes: after.TotalAlloc - before.TotalAlloc, eventDepth: obs.max, log: lm, ingress: lat.IngressToRecv, apply: lat.ApplyToRecv, mutation: mut}, nil
}

func runBackpressure(n, capacity, spin int) (backpressureResult, error) {
	if capacity < 2 {
		capacity = 2
	}
	r, err := ring.NewSPSC[uint64](capacity)
	if err != nil {
		return backpressureResult{}, err
	}
	ctx := context.Background()
	done := make(chan error, 1)
	go func() {
		time.Sleep(time.Millisecond)
		var expected uint64
		for {
			v, ok, e := r.ConsumeWait(ctx, spin)
			if e != nil {
				done <- e
				return
			}
			if !ok {
				break
			}
			if v != expected {
				done <- fmt.Errorf("W5 got %d want %d", v, expected)
				return
			}
			expected++
			if expected&63 == 0 {
				runtime.Gosched()
			}
		}
		if expected != uint64(n) {
			done <- fmt.Errorf("W5 consumed %d want %d", expected, n)
			return
		}
		done <- nil
	}()
	start := time.Now()
	var waits, depth uint64
	for i := 0; i < n; i++ {
		if !r.TryPublish(uint64(i)) {
			waits++
			if err = r.Publish(ctx, uint64(i), spin); err != nil {
				return backpressureResult{}, err
			}
		}
		depth = max(depth, uint64(r.Len()))
	}
	_ = r.Close()
	if err = <-done; err != nil {
		return backpressureResult{}, err
	}
	return backpressureResult{n: uint64(n), waits: waits, maxDepth: depth, duration: time.Since(start)}, nil
}
