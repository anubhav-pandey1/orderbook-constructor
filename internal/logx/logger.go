package logx

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"github.com/anubhav-pandey1/orderbook-constructor/book"
	"github.com/anubhav-pandey1/orderbook-constructor/internal/ring"
	"github.com/anubhav-pandey1/orderbook-constructor/replay"
	"io"
	"os"
	"runtime"
	"strconv"
	"sync/atomic"
	"time"
)

type Record struct {
	NotificationID, Version, SyncEpoch         uint64
	Kind                                       replay.EventKind
	State                                      replay.State
	Reason                                     replay.Reason
	BidPx, AskPx                               book.Price
	BidQty, AskQty                             book.Quantity
	BidOK, AskOK                               bool
	EventTS, DueNS, IngressNS, ApplyNS, RecvNS int64
}
type Sink uint8

const (
	SinkStdout Sink = iota + 1
	SinkFile
	SinkDiscard
)

func ParseSink(s string) (Sink, error) {
	switch s {
	case "stdout":
		return SinkStdout, nil
	case "file":
		return SinkFile, nil
	case "discard":
		return SinkDiscard, nil
	}
	return 0, fmt.Errorf("logx: invalid sink %q", s)
}

type Delivery uint8

const (
	Lossless Delivery = iota + 1
	DropWhenFull
)

type Config struct {
	Sink                            Sink
	File                            string
	Delivery                        Delivery
	RingSize, SpinIters, BatchBytes int
	FlushInterval                   time.Duration
}
type Metrics struct{ Enqueued, Written, Dropped, MaxDepth, WaitCount uint64 }
type Logger struct {
	cfg                                         Config
	in                                          *ring.SPSC[Record]
	w                                           *bufio.Writer
	closer                                      io.Closer
	enqueued, written, dropped, maxDepth, waits atomic.Uint64
}

func New(cfg Config) (*Logger, error) {
	if cfg.Sink == 0 {
		cfg.Sink = SinkStdout
	}
	if cfg.Delivery == 0 {
		cfg.Delivery = Lossless
	}
	if cfg.RingSize == 0 {
		cfg.RingSize = 65536
	}
	if cfg.SpinIters == 0 {
		cfg.SpinIters = 128
	}
	if cfg.SpinIters < 0 || cfg.FlushInterval < 0 {
		return nil, fmt.Errorf("logx: invalid negative configuration")
	}
	if cfg.BatchBytes <= 0 {
		cfg.BatchBytes = 64 << 10
	}
	q, err := ring.NewSPSC[Record](cfg.RingSize)
	if err != nil {
		return nil, err
	}
	var dst io.Writer
	var closer io.Closer
	switch cfg.Sink {
	case SinkStdout:
		dst = os.Stdout
	case SinkDiscard:
		dst = io.Discard
	case SinkFile:
		if cfg.File == "" {
			return nil, fmt.Errorf("logx: file path required")
		}
		f, e := os.Create(cfg.File)
		if e != nil {
			return nil, e
		}
		dst, closer = f, f
	default:
		return nil, fmt.Errorf("logx: invalid sink")
	}
	size := cfg.BatchBytes
	if size < 4096 {
		size = 4096
	}
	return &Logger{cfg: cfg, in: q, w: bufio.NewWriterSize(dst, size), closer: closer}, nil
}

func (l *Logger) Log(ctx context.Context, r Record) error {
	if l.in.Closed() {
		return ring.ErrClosed
	}
	if l.in.TryPublish(r) {
		l.after()
		return nil
	}
	if l.in.Closed() {
		return ring.ErrClosed
	}
	if l.cfg.Delivery == DropWhenFull {
		l.dropped.Add(1)
		return nil
	}
	l.waits.Add(1)
	if err := l.in.Publish(ctx, r, l.cfg.SpinIters); err != nil {
		return err
	}
	l.after()
	return nil
}
func (l *Logger) after() {
	l.enqueued.Add(1)
	d := uint64(l.in.Len())
	for old := l.maxDepth.Load(); d > old; old = l.maxDepth.Load() {
		if l.maxDepth.CompareAndSwap(old, d) {
			break
		}
	}
}
func (l *Logger) Close() error { return l.in.Close() }
func (l *Logger) Metrics() Metrics {
	return Metrics{l.enqueued.Load(), l.written.Load(), l.dropped.Load(), l.maxDepth.Load(), l.waits.Load()}
}
func (l *Logger) Run(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	var ticker *time.Ticker
	var ticks <-chan time.Time
	if l.cfg.FlushInterval > 0 {
		ticker = time.NewTicker(l.cfg.FlushInterval)
		ticks = ticker.C
		defer ticker.Stop()
	}
	line := make([]byte, 0, 384)
	for {
		if r, ok := l.in.TryConsume(); ok {
			line = appendRecord(line[:0], r)
			if _, err := l.w.Write(line); err != nil {
				return l.finish(err)
			}
			l.written.Add(1)
			if l.w.Buffered() >= l.cfg.BatchBytes {
				if err := l.w.Flush(); err != nil {
					return l.finish(err)
				}
			}
			continue
		}
		if l.in.Closed() && l.in.Len() == 0 {
			return l.finish(nil)
		}
		select {
		case <-ctx.Done():
			return l.finish(ctx.Err())
		case <-ticks:
			if err := l.w.Flush(); err != nil {
				return l.finish(err)
			}
		default:
			runtime.Gosched()
		}
	}
}
func (l *Logger) finish(e error) error {
	f := l.w.Flush()
	var c error
	if l.closer != nil {
		c = l.closer.Close()
	}
	return errors.Join(e, f, c)
}
func appendRecord(b []byte, r Record) []byte {
	b = append(b, "notification="...)
	b = strconv.AppendUint(b, r.NotificationID, 10)
	b = append(b, " version="...)
	b = strconv.AppendUint(b, r.Version, 10)
	b = append(b, " epoch="...)
	b = strconv.AppendUint(b, r.SyncEpoch, 10)
	b = append(b, " kind="...)
	b = strconv.AppendUint(b, uint64(r.Kind), 10)
	b = append(b, " state="...)
	b = strconv.AppendUint(b, uint64(r.State), 10)
	b = append(b, " reason="...)
	b = strconv.AppendUint(b, uint64(r.Reason), 10)
	b = append(b, " ts="...)
	b = strconv.AppendInt(b, r.EventTS, 10)
	b = append(b, " due_ns="...)
	b = strconv.AppendInt(b, r.DueNS, 10)
	b = append(b, " ingress_ns="...)
	b = strconv.AppendInt(b, r.IngressNS, 10)
	b = append(b, " apply_ns="...)
	b = strconv.AppendInt(b, r.ApplyNS, 10)
	b = append(b, " recv_ns="...)
	b = strconv.AppendInt(b, r.RecvNS, 10)
	b = append(b, " bid="...)
	if r.BidOK {
		b = r.BidPx.AppendText(b)
		b = append(b, 'x')
		b = r.BidQty.AppendText(b)
	} else {
		b = append(b, '-')
	}
	b = append(b, " ask="...)
	if r.AskOK {
		b = r.AskPx.AppendText(b)
		b = append(b, 'x')
		b = r.AskQty.AppendText(b)
	} else {
		b = append(b, '-')
	}
	return append(b, '\n')
}
