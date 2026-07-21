package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/anubhav-pandey1/orderbook-constructor/feed/gencsv"
)

func main() {
	cfg := gencsv.DefaultConfig()
	out := flag.String("out", "btc_orderbook_updates_10m.csv", "output CSV path")
	flag.StringVar(&cfg.Exchange, "exchange", cfg.Exchange, "exchange column")
	flag.StringVar(&cfg.Symbol, "symbol", cfg.Symbol, "symbol column")
	flag.Int64Var(&cfg.StartTS, "start-ts", cfg.StartTS, "first snapshot timestamp (epoch ms)")
	flag.Int64Var(&cfg.TSStep, "ts-step", cfg.TSStep, "timestamp increment per row")
	flag.Int64Var(&cfg.Incrementals, "incrementals", cfg.Incrementals, "number of incremental rows")
	flag.IntVar(&cfg.LevelsPerSide, "levels-per-side", cfg.LevelsPerSide, "initial snapshot levels per side")
	flag.IntVar(&cfg.MaxLevels, "max-levels", cfg.MaxLevels, "maximum active levels per side")
	flag.Int64Var(&cfg.SnapshotEvery, "snapshot-every", cfg.SnapshotEvery, "emit a snapshot every N incrementals (0 = initial only)")
	flag.Int64Var(&cfg.Seed, "seed", cfg.Seed, "RNG seed")
	flag.Parse()

	f, err := os.Create(*out)
	if err != nil {
		log.Fatalf("create output: %v", err)
	}
	defer f.Close()

	start := time.Now()
	if err := gencsv.Write(f, cfg); err != nil {
		log.Fatalf("generate: %v", err)
	}
	if err := f.Sync(); err != nil {
		log.Fatalf("sync: %v", err)
	}

	info, err := f.Stat()
	if err != nil {
		log.Fatalf("stat: %v", err)
	}

	rows := cfg.Incrementals + 1
	if cfg.SnapshotEvery > 0 {
		rows += cfg.Incrementals / cfg.SnapshotEvery
	}
	fmt.Printf("wrote %s\n", *out)
	fmt.Printf("rows=%d incrementals=%d bytes=%d elapsed=%s\n",
		rows, cfg.Incrementals, info.Size(), time.Since(start).Round(time.Millisecond))
}
