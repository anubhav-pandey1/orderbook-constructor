// Command bench executes the workloads defined by SPEC.md and writes the
// canonical report. Measured paths never write logs to stdout.
package main

import (
	"flag"
	"fmt"
	"os"
)

type config struct {
	csvPath, exchange, symbol, cpuModel, out string
	fixtureIters, warmup, syntheticMax       int
	synthetic, snapshotEvery, seed           int64
	eventRing, logRing, spin                 int
	pacedSpeed                               float64
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "benchmark:", err)
		os.Exit(1)
	}
}

func run() error {
	cfg := parseFlags()
	if err := cfg.validate(); err != nil {
		return err
	}
	report, err := runSuite(cfg)
	if err != nil {
		return err
	}
	if cfg.out == "" {
		fmt.Print(report)
		return nil
	}
	if err := os.WriteFile(cfg.out, []byte(report), 0o644); err != nil {
		return err
	}
	fmt.Fprintf(os.Stderr, "wrote %s\n", cfg.out)
	return nil
}

func parseFlags() config {
	var c config
	flag.StringVar(&c.csvPath, "csv", "btc_orderbook_updates.csv", "fixture CSV path")
	flag.StringVar(&c.exchange, "exchange", "binance", "exchange")
	flag.StringVar(&c.symbol, "symbol", "BTCUSDT", "normalized symbol")
	flag.StringVar(&c.cpuModel, "cpu-model", "", "CPU model override when OS discovery is unavailable")
	flag.IntVar(&c.fixtureIters, "fixture-iters", 20, "measured fixture repetitions")
	flag.IntVar(&c.warmup, "warmup", 3, "fixture warmups")
	flag.Int64Var(&c.synthetic, "synthetic", 10_000_000, "synthetic positions after initial snapshot")
	flag.Int64Var(&c.snapshotEvery, "snapshot-every", 100_000, "W6b snapshot interval")
	flag.IntVar(&c.syntheticMax, "synthetic-max-levels", 64, "generated levels per side bound")
	flag.Int64Var(&c.seed, "seed", 42, "synthetic seed")
	flag.IntVar(&c.eventRing, "event-ring", 65_536, "event ring capacity")
	flag.IntVar(&c.logRing, "log-ring", 65_536, "log ring capacity")
	flag.IntVar(&c.spin, "spin", 128, "spins before yield")
	flag.Float64Var(&c.pacedSpeed, "paced-speed", 10_000, "W7 speed multiplier")
	flag.StringVar(&c.out, "out", "", "report path (empty is stdout)")
	flag.Parse()
	return c
}

func (c config) validate() error {
	if c.fixtureIters < 1 || c.warmup < 0 || c.synthetic < 1 || c.snapshotEvery < 1 || c.syntheticMax < 10 || c.spin < 0 || c.pacedSpeed <= 0 {
		return fmt.Errorf("invalid non-positive workload setting")
	}
	for name, n := range map[string]int{"event-ring": c.eventRing, "log-ring": c.logRing} {
		if n < 2 || n&(n-1) != 0 {
			return fmt.Errorf("%s must be a power of two >= 2", name)
		}
	}
	return nil
}
