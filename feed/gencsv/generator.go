package gencsv

import (
	"bufio"
	"fmt"
	"io"
	"math/rand"
	"sort"
)

const (
	header = "type,exchange,symbol,timestamp,side,bids,asks,price,size\n"

	midTick   = int64(10000000)
	bandTicks = int64(100000)
	minSpread = int64(2)
)

type Config struct {
	Exchange      string
	Symbol        string
	StartTS       int64
	TSStep        int64
	Incrementals  int64
	LevelsPerSide int
	MaxLevels     int
	SnapshotEvery int64
	Seed          int64
}

func DefaultConfig() Config {
	return Config{
		Exchange:      "binance",
		Symbol:        "BTC/USDT",
		StartTS:       1700000000000,
		TSStep:        100,
		Incrementals:  10_000_000,
		LevelsPerSide: 10,
		MaxLevels:     512,
		SnapshotEvery: 0,
		Seed:          42,
	}
}

type simBook struct {
	bids, asks         map[int64]int64
	bidTicks, askTicks []int64
	bidIndex, askIndex map[int64]int
	rng                *rand.Rand
	max                int
}

func newSimBook(levelsPerSide, maxLevels int, rng *rand.Rand) *simBook {
	b := &simBook{
		bids: make(map[int64]int64, maxLevels), asks: make(map[int64]int64, maxLevels),
		bidTicks: make([]int64, 0, maxLevels), askTicks: make([]int64, 0, maxLevels),
		bidIndex: make(map[int64]int, maxLevels), askIndex: make(map[int64]int, maxLevels),
		rng: rng, max: maxLevels,
	}

	bidTicks := []int64{9999999, 9999886, 9999732, 9999254, 9999485, 9998959, 9998207, 9998276, 9998957, 9998608}
	askTicks := []int64{10000001, 10000227, 10000285, 10000495, 10000921, 10000798, 10000759, 10001388, 10000984, 10001618}
	bidQty := []int64{5270, 31404, 20343, 16814, 2099, 41407, 41942, 21042, 22734, 29537}
	askQty := []int64{48224, 22283, 46329, 4391, 7664, 19569, 7061, 22739, 44778, 21118}

	n := levelsPerSide
	if n > len(bidTicks) {
		n = len(bidTicks)
	}
	for i := 0; i < n; i++ {
		b.set("bid", bidTicks[i], bidQty[i])
		b.set("ask", askTicks[i], askQty[i])
	}
	return b
}

func (b *simBook) bestBid() (ticks int64, ok bool) {
	for _, p := range b.bidTicks {
		if !ok || p > ticks {
			ticks, ok = p, true
		}
	}
	return ticks, ok
}

func (b *simBook) bestAsk() (ticks int64, ok bool) {
	for _, p := range b.askTicks {
		if !ok || p < ticks {
			ticks, ok = p, true
		}
	}
	return ticks, ok
}

func (b *simBook) sideMap(side string) map[int64]int64 {
	if side == "ask" {
		return b.asks
	}
	return b.bids
}

func (b *simBook) sideTicks(side string) []int64 {
	if side == "ask" {
		return b.askTicks
	}
	return b.bidTicks
}

func (b *simBook) sideIndex(side string) map[int64]int {
	if side == "ask" {
		return b.askIndex
	}
	return b.bidIndex
}

func (b *simBook) randomLevel(side string) (ticks int64, ok bool) {
	prices := b.sideTicks(side)
	if len(prices) == 0 {
		return 0, false
	}
	return prices[b.rng.Intn(len(prices))], true
}

func (b *simBook) nonBestLevel(side string) (ticks int64, ok bool) {
	bestBid, hasBid := b.bestBid()
	bestAsk, hasAsk := b.bestAsk()
	m := b.sideMap(side)
	if len(m) <= 1 {
		return b.randomLevel(side)
	}
	for i := 0; i < len(m)*4; i++ {
		p, ok := b.randomLevel(side)
		if !ok {
			return 0, false
		}
		if side == "bid" && hasBid && p == bestBid {
			continue
		}
		if side == "ask" && hasAsk && p == bestAsk {
			continue
		}
		return p, true
	}
	return b.randomLevel(side)
}

func (b *simBook) set(side string, ticks, qty int64) {
	m := b.sideMap(side)
	index := b.sideIndex(side)
	if qty == 0 {
		at, exists := index[ticks]
		if !exists {
			return
		}
		prices := b.sideTicks(side)
		last := len(prices) - 1
		moved := prices[last]
		prices[at] = moved
		prices = prices[:last]
		delete(index, ticks)
		if moved != ticks {
			index[moved] = at
		}
		delete(m, ticks)
		if side == "ask" {
			b.askTicks = prices
		} else {
			b.bidTicks = prices
		}
		return
	}
	if _, exists := m[ticks]; !exists {
		if side == "ask" {
			index[ticks] = len(b.askTicks)
			b.askTicks = append(b.askTicks, ticks)
		} else {
			index[ticks] = len(b.bidTicks)
			b.bidTicks = append(b.bidTicks, ticks)
		}
	}
	m[ticks] = qty
}

func (b *simBook) randomQty() int64 {

	base := int64(b.rng.Intn(50000)) + 1
	if b.rng.Intn(4) == 0 {
		base = int64(b.rng.Intn(500000)) + 1000
	}
	return base
}

func bandLo() int64 { return midTick - bandTicks }
func bandHi() int64 { return midTick + bandTicks }

func (b *simBook) evictFarthestNonBest(side string) {
	bestBid, hasBid := b.bestBid()
	bestAsk, hasAsk := b.bestAsk()
	m := b.sideMap(side)
	if len(m) <= 1 {
		return
	}
	var victim int64
	var found bool
	for p := range m {
		if side == "bid" {
			if hasBid && p == bestBid {
				continue
			}
			if !found || p < victim {
				victim, found = p, true
			}
		} else {
			if hasAsk && p == bestAsk {
				continue
			}
			if !found || p > victim {
				victim, found = p, true
			}
		}
	}
	if found {
		b.set(side, victim, 0)
	}
}

func (b *simBook) randomNewBidTick(bestBid, bestAsk int64) (int64, bool) {
	hi := bestBid - 1
	lo := bandLo()
	if lo > hi || bestAsk-bestBid < minSpread {
		return 0, false
	}
	span := hi - lo + 1
	for i := 0; i < 32; i++ {
		t := lo + b.rng.Int63n(span)
		if _, exists := b.bids[t]; exists {
			continue
		}
		if _, exists := b.asks[t]; exists {
			continue
		}
		return t, true
	}
	return 0, false
}

func (b *simBook) randomNewAskTick(bestBid, bestAsk int64) (int64, bool) {
	lo := bestAsk + 1
	hi := bandHi()
	if lo > hi || bestAsk-bestBid < minSpread {
		return 0, false
	}
	span := hi - lo + 1
	for i := 0; i < 32; i++ {
		t := lo + b.rng.Int63n(span)
		if _, exists := b.asks[t]; exists {
			continue
		}
		if _, exists := b.bids[t]; exists {
			continue
		}
		return t, true
	}
	return 0, false
}

func (b *simBook) replaceExisting(side string) (string, int64, int64) {
	ticks, _ := b.randomLevel(side)
	qty := b.randomQty()
	b.set(side, ticks, qty)
	return side, ticks, qty
}

func (b *simBook) nextDelta() (side string, ticks, qty int64) {
	bestBid, hasBid := b.bestBid()
	bestAsk, hasAsk := b.bestAsk()
	if !hasBid || !hasAsk {
		panic("generator book must keep both sides populated")
	}

	op := b.rng.Intn(100)
	switch {
	case op < 35:
		side = "bid"
		if b.rng.Intn(2) == 1 {
			side = "ask"
		}
		ticks, _ = b.randomLevel(side)
		qty = b.randomQty()
		b.set(side, ticks, qty)
	case op < 55:
		side = "bid"
		if b.rng.Intn(2) == 1 {
			side = "ask"
		}
		if len(b.sideMap(side)) <= 1 {
			ticks, _ = b.randomLevel(side)
			qty = b.randomQty()
			b.set(side, ticks, qty)
			return side, ticks, qty
		}
		ticks, _ = b.nonBestLevel(side)
		qty = 0
		b.set(side, ticks, 0)
	case op < 80:
		var ok bool
		if b.rng.Intn(2) == 0 {
			side = "bid"
			if len(b.bids) >= b.max {
				b.evictFarthestNonBest("bid")
			}
			ticks, ok = b.randomNewBidTick(bestBid, bestAsk)
			if !ok {
				return b.replaceExisting(side)
			}
			qty = b.randomQty()
			b.set(side, ticks, qty)
		} else {
			side = "ask"
			if len(b.asks) >= b.max {
				b.evictFarthestNonBest("ask")
			}
			ticks, ok = b.randomNewAskTick(bestBid, bestAsk)
			if !ok {
				return b.replaceExisting(side)
			}
			qty = b.randomQty()
			b.set(side, ticks, qty)
		}
	default:
		if b.rng.Intn(2) == 0 {
			side, ticks = "bid", bestBid
		} else {
			side, ticks = "ask", bestAsk
		}
		if b.rng.Intn(5) == 0 && len(b.sideMap(side)) > 1 {
			b.set(side, ticks, 0)
			return side, ticks, 0
		}
		qty = b.randomQty()
		b.set(side, ticks, qty)
	}

	bestBid, hasBid = b.bestBid()
	bestAsk, hasAsk = b.bestAsk()
	if !hasBid || !hasAsk || bestBid >= bestAsk {
		panic(fmt.Sprintf("crossed book after delta: bid=%v ask=%v", bestBid, bestAsk))
	}
	return side, ticks, qty
}

func formatPrice(ticks int64) string {
	return fmt.Sprintf("%d.%02d", ticks/100, ticks%100)
}

func formatQty(units int64) string {
	return fmt.Sprintf("%d.%04d", units/10000, units%10000)
}

func writeSnapshot(w *bufio.Writer, cfg Config, ts int64, b *simBook) error {
	bids := make([]int64, 0, len(b.bids))
	for p := range b.bids {
		bids = append(bids, p)
	}
	sort.Slice(bids, func(i, j int) bool { return bids[i] > bids[j] })

	asks := make([]int64, 0, len(b.asks))
	for p := range b.asks {
		asks = append(asks, p)
	}
	sort.Slice(asks, func(i, j int) bool { return asks[i] < asks[j] })

	if _, err := fmt.Fprintf(w, "snapshot,%s,%s,%d,,", cfg.Exchange, cfg.Symbol, ts); err != nil {
		return err
	}
	if err := writeLevelJSON(w, bids, b.bids); err != nil {
		return err
	}
	if _, err := io.WriteString(w, ","); err != nil {
		return err
	}
	if err := writeLevelJSON(w, asks, b.asks); err != nil {
		return err
	}
	_, err := io.WriteString(w, ",,\n")
	return err
}

func writeLevelJSON(w *bufio.Writer, prices []int64, levels map[int64]int64) error {
	if _, err := io.WriteString(w, `"[`); err != nil {
		return err
	}
	for i, p := range prices {
		if i > 0 {
			if _, err := io.WriteString(w, ", "); err != nil {
				return err
			}
		}
		if _, err := fmt.Fprintf(w, `[%s, %s]`, formatPrice(p), formatQty(levels[p])); err != nil {
			return err
		}
	}
	_, err := io.WriteString(w, `]"`)
	return err
}

func writeIncremental(w *bufio.Writer, cfg Config, ts int64, side string, ticks, qty int64) error {
	_, err := fmt.Fprintf(w, "incremental,%s,%s,%d,%s,,,%s,%s\n",
		cfg.Exchange, cfg.Symbol, ts, side, formatPrice(ticks), formatQty(qty))
	return err
}

func Write(w io.Writer, cfg Config) error {
	if err := validateConfig(cfg); err != nil {
		return err
	}

	bw := bufio.NewWriterSize(w, 1<<20)
	if _, err := io.WriteString(bw, header); err != nil {
		return err
	}

	rng := rand.New(rand.NewSource(cfg.Seed))
	book := newSimBook(cfg.LevelsPerSide, cfg.MaxLevels, rng)

	ts := cfg.StartTS
	if err := writeSnapshot(bw, cfg, ts, book); err != nil {
		return err
	}

	for i := int64(1); i <= cfg.Incrementals; i++ {
		ts += cfg.TSStep
		if cfg.SnapshotEvery > 0 && i%cfg.SnapshotEvery == 0 {
			if err := writeSnapshot(bw, cfg, ts, book); err != nil {
				return err
			}
			continue
		}
		side, px, qty := book.nextDelta()
		if err := writeIncremental(bw, cfg, ts, side, px, qty); err != nil {
			return err
		}
		if i%1_000_000 == 0 {
			if err := bw.Flush(); err != nil {
				return err
			}
		}
	}
	return bw.Flush()
}
