// Package asynclog provides batched, off-hot-path logging. The strategy
// enqueues fixed-size records into an SPSC ring; a logger goroutine drains,
// formats, and batches them through a bufio.Writer to a configurable sink.
package asynclog

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"os"
	"strconv"
	"sync/atomic"

	"orderbook/book"
	"orderbook/internal/ring"
)

type Sink uint8

const (
	SinkStdout Sink = iota
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
	return 0, fmt.Errorf("invalid log sink %q (want stdout|file|discard)", s)
}

// LogRecord is a fixed-size structured record; no text formatting happens on
// the producing (strategy) side.
type LogRecord struct {
	Version      uint64
	ExchangeTime int64
	BidPrice     book.Price
	BidQty       book.Quantity
	AskPrice     book.Price
	AskQty       book.Quantity
	HasBid       bool
	HasAsk       bool
}

type Config struct {
	Sink         Sink
	FilePath     string
	RingCapacity int
	BatchBytes   int // flush the bufio.Writer once it holds at least this many bytes
	Spin         int
}

type Logger struct {
	cfg     Config
	in      *ring.SPSC[LogRecord]
	w       *bufio.Writer
	closer  io.Closer
	written atomic.Uint64
}

func New(cfg Config) (*Logger, error) {
	if cfg.RingCapacity <= 0 {
		cfg.RingCapacity = 4096
	}
	if cfg.BatchBytes <= 0 {
		cfg.BatchBytes = 64 << 10
	}
	if cfg.Spin <= 0 {
		cfg.Spin = 64
	}
	var w io.Writer
	var closer io.Closer
	switch cfg.Sink {
	case SinkStdout:
		w = os.Stdout
	case SinkFile:
		f, err := os.Create(cfg.FilePath)
		if err != nil {
			return nil, err
		}
		w, closer = f, f
	case SinkDiscard:
		w = io.Discard
	default:
		return nil, fmt.Errorf("invalid sink")
	}
	return &Logger{
		cfg:    cfg,
		in:     ring.New[LogRecord](cfg.RingCapacity),
		w:      bufio.NewWriterSize(w, 1<<16),
		closer: closer,
	}, nil
}

// Log enqueues a record with lossless backpressure. Called from the single
// strategy goroutine only.
func (l *Logger) Log(ctx context.Context, rec LogRecord) error {
	return l.in.Push(ctx, rec, l.cfg.Spin)
}

// Close signals that no more records will be enqueued. Run then drains and
// flushes. Idempotent.
func (l *Logger) Close() { l.in.Close() }

func (l *Logger) Written() uint64 { return l.written.Load() }

// Run drains the log ring, formatting and batching to the sink until the ring
// is closed and drained. Runs in its own goroutine.
func (l *Logger) Run(ctx context.Context) error {
	var line []byte
	for {
		rec, ok, err := l.in.Pop(ctx, l.cfg.Spin)
		if err != nil {
			l.w.Flush()
			return err
		}
		if !ok { // closed and drained
			return l.finish()
		}
		line = appendRecord(line[:0], rec)
		if _, err := l.w.Write(line); err != nil {
			return err
		}
		l.written.Add(1)
		if l.w.Buffered() >= l.cfg.BatchBytes {
			if err := l.w.Flush(); err != nil {
				return err
			}
		}
	}
}

func (l *Logger) finish() error {
	err := l.w.Flush()
	if l.closer != nil {
		if cerr := l.closer.Close(); cerr != nil && err == nil {
			err = cerr
		}
	}
	return err
}

func appendRecord(b []byte, r LogRecord) []byte {
	b = append(b, "v="...)
	b = strconv.AppendUint(b, r.Version, 10)
	b = append(b, " ts="...)
	b = strconv.AppendInt(b, r.ExchangeTime, 10)
	b = append(b, " bid="...)
	if r.HasBid {
		b = appendPrice(b, r.BidPrice)
		b = append(b, 'x')
		b = appendQty(b, r.BidQty)
	} else {
		b = append(b, '-')
	}
	b = append(b, " ask="...)
	if r.HasAsk {
		b = appendPrice(b, r.AskPrice)
		b = append(b, 'x')
		b = appendQty(b, r.AskQty)
	} else {
		b = append(b, '-')
	}
	return append(b, '\n')
}

func appendPrice(b []byte, p book.Price) []byte {
	b = strconv.AppendInt(b, int64(p)/book.PriceScale, 10)
	b = append(b, '.')
	frac := int64(p) % book.PriceScale
	if frac < 10 {
		b = append(b, '0')
	}
	return strconv.AppendInt(b, frac, 10)
}

func appendQty(b []byte, q book.Quantity) []byte {
	b = strconv.AppendInt(b, int64(q)/book.QtyScale, 10)
	b = append(b, '.')
	frac := int64(q) % book.QtyScale
	switch {
	case frac < 10:
		b = append(b, "000"...)
	case frac < 100:
		b = append(b, "00"...)
	case frac < 1000:
		b = append(b, '0')
	}
	return strconv.AppendInt(b, frac, 10)
}
