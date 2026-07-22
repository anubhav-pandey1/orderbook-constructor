package main

import (
	"flag"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/anubhav-pandey1/orderbook-constructor/feed/gencsv"
)

func main() {
	if err := run(os.Args[1:], os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "gencsv:", err)
		os.Exit(1)
	}
}

func run(args []string, stdout io.Writer) error {
	cfg := gencsv.DefaultConfig()
	if stdout == nil {
		stdout = io.Discard
	}
	fs := flag.NewFlagSet("gencsv", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	out := fs.String("out", "btc_orderbook_updates_10m.csv", "output CSV path")
	fs.StringVar(&cfg.Exchange, "exchange", cfg.Exchange, "exchange column")
	fs.StringVar(&cfg.Symbol, "symbol", cfg.Symbol, "symbol column")
	fs.Int64Var(&cfg.StartTS, "start-ts", cfg.StartTS, "first snapshot timestamp (epoch ms)")
	fs.Int64Var(&cfg.TSStep, "ts-step", cfg.TSStep, "timestamp increment per row")
	fs.Int64Var(&cfg.Incrementals, "incrementals", cfg.Incrementals, "number of incremental rows")
	fs.IntVar(&cfg.LevelsPerSide, "levels-per-side", cfg.LevelsPerSide, "initial snapshot levels per side")
	fs.IntVar(&cfg.MaxLevels, "max-levels", cfg.MaxLevels, "maximum active levels per side")
	fs.Int64Var(&cfg.SnapshotEvery, "snapshot-every", cfg.SnapshotEvery, "emit a snapshot every N incrementals (0 = initial only)")
	fs.Int64Var(&cfg.Seed, "seed", cfg.Seed, "RNG seed")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 0 {
		return fmt.Errorf("unexpected positional arguments: %v", fs.Args())
	}

	f, err := os.Create(*out)
	if err != nil {
		return fmt.Errorf("create output: %w", err)
	}
	defer f.Close()

	start := time.Now()
	if err := gencsv.Write(f, cfg); err != nil {
		return fmt.Errorf("generate: %w", err)
	}
	if err := f.Sync(); err != nil {
		return fmt.Errorf("sync: %w", err)
	}

	info, err := f.Stat()
	if err != nil {
		return fmt.Errorf("stat: %w", err)
	}

	rows := cfg.Incrementals + 1
	fmt.Fprintf(stdout, "wrote %s\n", *out)
	fmt.Fprintf(stdout, "rows=%d incrementals=%d bytes=%d elapsed=%s\n",
		rows, cfg.Incrementals, info.Size(), time.Since(start).Round(time.Millisecond))
	return nil
}
