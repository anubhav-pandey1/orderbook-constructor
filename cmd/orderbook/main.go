// Command orderbook replays the CSV feed through the L2 book and drives the
// strategy over the SPSC pipeline (book writer -> event ring -> strategy ->
// log ring -> logger).
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"runtime"
	"sync"
	"time"

	"orderbook/book"
	"orderbook/feed"
	"orderbook/internal/asynclog"
	"orderbook/internal/ring"
	"orderbook/internal/strategy"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func run() error {
	var (
		csvPath    = flag.String("csv", "btc_orderbook_updates.csv", "input CSV path")
		exchange   = flag.String("exchange", "binance", "exchange")
		symbol     = flag.String("symbol", "BTC/USDT", "symbol")
		replayMode = flag.String("replay", "fast", "replay mode: fast|paced")
		speed      = flag.Float64("speed", 1.0, "paced speed multiplier (>1 = faster than real time)")
		tsUnit     = flag.String("ts-unit", "auto", "timestamp unit: auto|ms|ns")
		tsPolicy   = flag.String("timestamp-policy", "warn", "timestamp policy: off|warn|strict")
		crossed    = flag.String("crossed-policy", "strict", "crossed-book policy: off|warn|strict")
		logSink    = flag.String("log", "stdout", "log sink: stdout|file|discard")
		logFile    = flag.String("log-file", "orderbook.log", "log file (for -log=file)")
		ringCap    = flag.Int("ring", 4096, "event ring capacity")
		logRingCap = flag.Int("log-ring", 4096, "log ring capacity")
		spin       = flag.Int("spin", 128, "backpressure spin count")
		gomaxprocs = flag.Int("gomaxprocs", 0, "GOMAXPROCS (0 = default)")
	)
	flag.Parse()

	if *gomaxprocs > 0 {
		runtime.GOMAXPROCS(*gomaxprocs)
	}

	mode, err := parseMode(*replayMode)
	if err != nil {
		return err
	}
	tsPol, err := parsePolicy(*tsPolicy)
	if err != nil {
		return err
	}
	crPol, err := parsePolicy(*crossed)
	if err != nil {
		return err
	}
	sink, err := asynclog.ParseSink(*logSink)
	if err != nil {
		return err
	}
	unit, err := parseTSUnit(*tsUnit, *csvPath)
	if err != nil {
		return err
	}

	f, err := os.Open(*csvPath)
	if err != nil {
		return err
	}
	defer f.Close()

	dec := feed.NewDecoder(f)
	bk := book.New(*exchange, *symbol, book.Config{CrossedPolicy: crPol, LevelHint: 512})

	lg, err := asynclog.New(asynclog.Config{Sink: sink, FilePath: *logFile, RingCapacity: *logRingCap, Spin: *spin})
	if err != nil {
		return err
	}
	events := ring.New[book.BookEvent](*ringCap)

	ctx := context.Background()
	var wg sync.WaitGroup
	var logErr, stratErr error

	wg.Add(1)
	go func() { defer wg.Done(); logErr = lg.Run(ctx) }()
	wg.Add(1)
	go func() { defer wg.Done(); stratErr = strategy.Run(ctx, events, lg, strategy.BBO{}, nil, *spin) }()

	pub := feed.SinkFunc(func(e book.BookEvent) error { return events.Push(ctx, e, *spin) })
	syncPol := &feed.TimestampPolicy{Mode: tsPol}

	startWall := time.Now()
	st, replayErr := feed.Replay(ctx, dec, bk, pub, feed.Config{Mode: mode, Speed: *speed, TSUnit: unit, Sync: syncPol}, feed.RealClock{})
	elapsed := time.Since(startWall)

	events.Close() // no more events; strategy drains then closes the logger
	wg.Wait()

	if replayErr != nil {
		return replayErr
	}
	if stratErr != nil {
		return stratErr
	}
	if logErr != nil {
		return logErr
	}

	tob := bk.BestBidAsk()
	w := os.Stderr
	fmt.Fprintf(w, "\n--- replay complete ---\n")
	fmt.Fprintf(w, "accepted=%d snapshots=%d incrementals=%d deletes=%d\n", st.Accepted, st.Snapshots, st.Incrementals, st.Deletes)
	fmt.Fprintf(w, "book version=%d  bid levels=%d  ask levels=%d\n", bk.Version(), bk.BidLevelCount(), bk.AskLevelCount())
	fmt.Fprintf(w, "final BBO: bid=%s x %s  ask=%s x %s\n", tob.BidPrice, tob.BidQty, tob.AskPrice, tob.AskQty)
	fmt.Fprintf(w, "ts: regressions=%d duplicates=%d largeGaps=%d  crossedWarn=%d\n",
		st.Sync.Regressions, st.Sync.Duplicates, st.Sync.LargeGaps, st.CrossedWarn)
	fmt.Fprintf(w, "wall=%s  events/s=%.0f  logged=%d\n", elapsed.Round(time.Microsecond), float64(st.Accepted)/elapsed.Seconds(), lg.Written())
	return nil
}

func parseMode(s string) (feed.Mode, error) {
	switch s {
	case "fast":
		return feed.Fast, nil
	case "paced":
		return feed.Paced, nil
	}
	return 0, fmt.Errorf("invalid replay mode %q (want fast|paced)", s)
}

func parsePolicy(s string) (book.Policy, error) {
	switch s {
	case "off":
		return book.PolicyOff, nil
	case "warn":
		return book.PolicyWarn, nil
	case "strict":
		return book.PolicyStrict, nil
	}
	return 0, fmt.Errorf("invalid policy %q (want off|warn|strict)", s)
}

func parseTSUnit(s, csvPath string) (time.Duration, error) {
	switch s {
	case "ms":
		return time.Millisecond, nil
	case "ns":
		return time.Nanosecond, nil
	case "auto":
		return detectTSUnit(csvPath)
	}
	return 0, fmt.Errorf("invalid ts-unit %q (want auto|ms|ns)", s)
}

// detectTSUnit peeks the first data row's timestamp: 13-digit epoch -> ms,
// 16+ digit -> ns, otherwise ambiguous.
func detectTSUnit(path string) (time.Duration, error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, err
	}
	defer f.Close()
	rec, err := feed.NewDecoder(f).Next()
	if err != nil {
		return 0, fmt.Errorf("detect ts-unit: %w", err)
	}
	switch n := digitCount(rec.ExchangeTime); {
	case n <= 13:
		return time.Millisecond, nil
	case n >= 16:
		return time.Nanosecond, nil
	default:
		return 0, fmt.Errorf("ambiguous timestamp magnitude (%d digits); pass -ts-unit=ms|ns", n)
	}
}

func digitCount(v int64) int {
	if v < 0 {
		v = -v
	}
	n := 1
	for v >= 10 {
		v /= 10
		n++
	}
	return n
}
