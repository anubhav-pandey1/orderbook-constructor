package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"math"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"syscall"
	"time"

	"github.com/anubhav-pandey1/orderbook-constructor/book"
	"github.com/anubhav-pandey1/orderbook-constructor/feed"
	obclock "github.com/anubhav-pandey1/orderbook-constructor/internal/clock"
	"github.com/anubhav-pandey1/orderbook-constructor/internal/logx"
	"github.com/anubhav-pandey1/orderbook-constructor/internal/ring"
	"github.com/anubhav-pandey1/orderbook-constructor/internal/strategy"
	"github.com/anubhav-pandey1/orderbook-constructor/replay"
)

const defaultRingSize = 65536

type config struct {
	csvPath, exchange, symbol     string
	replayMode, timestampUnit     string
	syncPolicy, timestampMode     string
	logSink, logFile, logDelivery string
	speed                         float64
	timestampStep                 time.Duration
	eventRing, logRing, spin      int
	gomaxprocs                    int
}

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "replay:", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	cfg, err := parseFlags(args)
	if err != nil {
		return err
	}
	if cfg.gomaxprocs < 0 {
		return fmt.Errorf("gomaxprocs must be non-negative")
	}
	if cfg.speed <= 0 || math.IsNaN(cfg.speed) || math.IsInf(cfg.speed, 0) {
		return fmt.Errorf("speed must be finite and greater than zero")
	}
	if cfg.spin < 0 {
		return fmt.Errorf("spin must be non-negative")
	}

	mode, err := parseReplayMode(cfg.replayMode)
	if err != nil {
		return err
	}
	unit, err := parseTimestampUnit(cfg.timestampUnit, cfg.csvPath)
	if err != nil {
		return err
	}
	policy, err := parseSyncPolicy(cfg.syncPolicy, cfg.timestampMode, cfg.timestampStep, unit)
	if err != nil {
		return err
	}
	stream, err := feed.NormalizeStreamID(cfg.exchange, cfg.symbol)
	if err != nil {
		return fmt.Errorf("stream identity: %w", err)
	}
	sink, err := logx.ParseSink(cfg.logSink)
	if err != nil {
		return err
	}
	delivery, err := parseLogDelivery(cfg.logDelivery)
	if err != nil {
		return err
	}
	if sink == logx.SinkFile {
		inputPath, inputErr := filepath.Abs(cfg.csvPath)
		outputPath, outputErr := filepath.Abs(cfg.logFile)
		if inputErr != nil || outputErr != nil {
			return errors.Join(inputErr, outputErr)
		}
		if inputPath == outputPath {
			return fmt.Errorf("log file must not overwrite the input CSV")
		}
	}

	events, err := ring.NewSPSC[replay.Event](cfg.eventRing)
	if err != nil {
		return fmt.Errorf("event ring: %w", err)
	}
	input, err := os.Open(cfg.csvPath)
	if err != nil {
		return fmt.Errorf("open CSV: %w", err)
	}
	defer input.Close()
	if sink == logx.SinkFile {
		inputInfo, inputErr := input.Stat()
		outputInfo, outputErr := os.Stat(cfg.logFile)
		if inputErr != nil {
			return fmt.Errorf("stat input CSV: %w", inputErr)
		}
		if outputErr == nil && os.SameFile(inputInfo, outputInfo) {
			return fmt.Errorf("log file must not overwrite the input CSV")
		}
		if outputErr != nil && !errors.Is(outputErr, os.ErrNotExist) {
			return fmt.Errorf("stat log file: %w", outputErr)
		}
	}
	logger, err := logx.New(logx.Config{
		Sink: sink, File: cfg.logFile, Delivery: delivery,
		RingSize: cfg.logRing, SpinIters: cfg.spin,
	})
	if err != nil {
		return fmt.Errorf("logger: %w", err)
	}
	if cfg.gomaxprocs > 0 {
		runtime.GOMAXPROCS(cfg.gomaxprocs)
	}

	signalCtx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	ctx, cancel := context.WithCancel(signalCtx)
	defer cancel()
	clk := obclock.NewReal()
	bk := book.New(512)
	logStrategy := strategy.NewLogStrategy(ctx, logger, nil)
	loggerDone := make(chan error, 1)
	strategyDone := make(chan error, 1)

	go func() {
		runErr := logger.Run(ctx)
		loggerDone <- runErr
		if runErr != nil {
			cancel()
		}
	}()
	go func() {
		runErr := strategy.RunWithSpin(ctx, events, logStrategy, clk, cfg.spin)
		strategyDone <- runErr
		if runErr != nil {
			cancel()
		}
	}()

	started := time.Now()
	stats, replayErr := replay.Run(ctx, feed.NewDecoder(input), bk, replay.HandlerFunc(func(ctx context.Context, event replay.Event) error {
		return events.Publish(ctx, event, cfg.spin)
	}), replay.Options{
		Mode: mode, Speed: cfg.speed, TimestampUnit: unit, Stream: stream, Policy: policy, Clock: clk,
	})
	_ = events.Close()
	elapsed := time.Since(started)

	strategyErr := <-strategyDone
	loggerErr := <-loggerDone
	if err := errors.Join(replayErr, strategyErr, loggerErr); err != nil {
		return err
	}
	printSummary(stats, bk, logger.Metrics(), elapsed)
	return nil
}

func parseFlags(args []string) (config, error) {
	var cfg config
	fs := flag.NewFlagSet("replay", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	fs.StringVar(&cfg.csvPath, "csv", "./testdata/btc_orderbook_updates.csv", "input CSV path")
	fs.StringVar(&cfg.exchange, "exchange", "binance", "expected exchange")
	fs.StringVar(&cfg.symbol, "symbol", "BTCUSDT", "expected symbol")
	fs.StringVar(&cfg.replayMode, "replay", "fast", "replay mode: fast|paced")
	fs.Float64Var(&cfg.speed, "speed", 1, "paced replay speed multiplier")
	fs.StringVar(&cfg.timestampUnit, "timestamp-unit", "auto", "source timestamp unit: auto|ns|us|ms")
	fs.StringVar(&cfg.syncPolicy, "sync-policy", "timestamp", "synchronization policy: timestamp|update-id|off")
	fs.StringVar(&cfg.timestampMode, "timestamp-mode", "step", "timestamp policy mode: step|monotonic")
	fs.DurationVar(&cfg.timestampStep, "timestamp-step", 100*time.Millisecond, "expected source timestamp step")
	fs.StringVar(&cfg.logSink, "log", "stdout", "log sink: stdout|file|discard")
	fs.StringVar(&cfg.logFile, "log-file", "orderbook.log", "output path for -log=file")
	fs.StringVar(&cfg.logDelivery, "log-delivery", "lossless", "logger delivery: lossless|drop")
	fs.IntVar(&cfg.eventRing, "event-ring", defaultRingSize, "event ring capacity (power of two)")
	fs.IntVar(&cfg.logRing, "log-ring", defaultRingSize, "log ring capacity (power of two)")
	fs.IntVar(&cfg.spin, "spin", 128, "busy-spin iterations before yielding")
	fs.IntVar(&cfg.gomaxprocs, "gomaxprocs", 0, "GOMAXPROCS override (0 keeps default)")
	if err := fs.Parse(args); err != nil {
		return config{}, err
	}
	if fs.NArg() != 0 {
		return config{}, fmt.Errorf("unexpected positional arguments: %v", fs.Args())
	}
	return cfg, nil
}

func parseReplayMode(value string) (replay.Mode, error) {
	switch value {
	case "fast":
		return replay.Fast, nil
	case "paced":
		return replay.Paced, nil
	default:
		return 0, fmt.Errorf("invalid replay mode %q (want fast|paced)", value)
	}
}

func parseTimestampUnit(value, csvPath string) (time.Duration, error) {
	switch value {
	case "ns":
		return time.Nanosecond, nil
	case "us":
		return time.Microsecond, nil
	case "ms":
		return time.Millisecond, nil
	case "auto":
		return detectTimestampUnit(csvPath)
	default:
		return 0, fmt.Errorf("invalid timestamp unit %q (want auto|ns|us|ms)", value)
	}
}

func detectTimestampUnit(path string) (time.Duration, error) {
	input, err := os.Open(path)
	if err != nil {
		return 0, fmt.Errorf("detect timestamp unit: %w", err)
	}
	defer input.Close()
	record, err := feed.NewDecoder(input).Next()
	if err != nil {
		return 0, fmt.Errorf("detect timestamp unit: %w", err)
	}
	switch digits := decimalDigits(record.TS); digits {
	case 13:
		return time.Millisecond, nil
	case 16:
		return time.Microsecond, nil
	case 19:
		return time.Nanosecond, nil
	default:
		return 0, fmt.Errorf("cannot infer timestamp unit from %d-digit value; pass -timestamp-unit explicitly", digits)
	}
}

func decimalDigits(value int64) int {
	var magnitude uint64
	if value < 0 {
		magnitude = uint64(-(value + 1)) + 1
	} else {
		magnitude = uint64(value)
	}
	digits := 1
	for magnitude >= 10 {
		magnitude /= 10
		digits++
	}
	return digits
}

func parseSyncPolicy(name, timestampMode string, stepDuration, unit time.Duration) (replay.Policy, error) {
	if unit <= 0 {
		return nil, fmt.Errorf("timestamp unit must be greater than zero")
	}
	switch name {
	case "off":
		return replay.NewArrivalOrderPolicy(), nil
	case "update-id":
		return nil, fmt.Errorf("sync policy update-id requires update-ID fields, which this CSV format does not contain")
	case "timestamp":
		var mode replay.TimestampMode
		switch timestampMode {
		case "step":
			mode = replay.TimestampStep
			if stepDuration <= 0 {
				return nil, fmt.Errorf("timestamp step must be greater than zero")
			}
			if stepDuration%unit != 0 {
				return nil, fmt.Errorf("timestamp step %s is not an exact multiple of source unit %s", stepDuration, unit)
			}
		case "monotonic":
			mode = replay.TimestampMonotonic
		default:
			return nil, fmt.Errorf("invalid timestamp mode %q (want step|monotonic)", timestampMode)
		}
		return replay.NewTimestampPolicy(mode, int64(stepDuration/unit)), nil
	default:
		return nil, fmt.Errorf("invalid sync policy %q (want timestamp|update-id|off)", name)
	}
}

func parseLogDelivery(value string) (logx.Delivery, error) {
	switch value {
	case "lossless":
		return logx.Lossless, nil
	case "drop":
		return logx.DropWhenFull, nil
	default:
		return 0, fmt.Errorf("invalid log delivery %q (want lossless|drop)", value)
	}
}

func printSummary(stats replay.Stats, bk *book.Book, logs logx.Metrics, elapsed time.Duration) {
	bbo := bk.BBOSnapshot()
	fmt.Fprintln(os.Stderr, "\n--- replay complete ---")
	fmt.Fprintf(os.Stderr, "applied=%d snapshots=%d deltas=%d deletes=%d absent_deletes=%d discarded=%d invalidated=%d\n",
		stats.Applied, stats.Snapshots, stats.Deltas, stats.Deletes, stats.AbsentDeletes, stats.Discarded, stats.Invalidated)
	fmt.Fprintf(os.Stderr, "book version=%d bid_levels=%d ask_levels=%d final_bbo=", bk.Version(), bk.BidLevelCount(), bk.AskLevelCount())
	if bbo.BidOK {
		fmt.Fprintf(os.Stderr, "%s x %s", bbo.BidPx, bbo.BidQty)
	} else {
		fmt.Fprint(os.Stderr, "-")
	}
	fmt.Fprint(os.Stderr, " / ")
	if bbo.AskOK {
		fmt.Fprintf(os.Stderr, "%s x %s", bbo.AskPx, bbo.AskQty)
	} else {
		fmt.Fprint(os.Stderr, "-")
	}
	fmt.Fprintln(os.Stderr)
	fmt.Fprintf(os.Stderr, "sync stale=%d duplicates=%d gaps=%d crossed=%d snapshot_requests=%d ignored_desynced=%d\n",
		stats.Stale, stats.Duplicates, stats.Gaps, stats.Crossed, stats.SnapshotRequests, stats.IgnoredWhileDesynced)
	rate := float64(0)
	if elapsed > 0 {
		rate = float64(stats.Applied) / elapsed.Seconds()
	}
	fmt.Fprintf(os.Stderr, "wall=%s events/s=%.0f logs_enqueued=%d logs_written=%d logs_dropped=%d\n",
		elapsed.Round(time.Microsecond), rate, logs.Enqueued, logs.Written, logs.Dropped)
}
