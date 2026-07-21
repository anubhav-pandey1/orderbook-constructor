package main

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/anubhav-pandey1/orderbook-constructor/book"
	"github.com/anubhav-pandey1/orderbook-constructor/feed"
	"github.com/anubhav-pandey1/orderbook-constructor/replay"
)

func main() {
	const data = `type,exchange,symbol,timestamp,side,bids,asks,price,size
snapshot,binance,BTC/USDT,1700000000000,,"[[100.00,1.0000]]","[[101.00,1.0000]]",,
incremental,binance,BTC/USDT,1700000000100,ask,,,101.00,2.0000
`
	stream, _ := feed.NormalizeStreamID("binance", "BTCUSDT")
	var events int
	stats, err := replay.Run(context.Background(), feed.NewDecoder(strings.NewReader(data)), book.New(16), replay.HandlerFunc(func(_ context.Context, _ replay.Event) error {
		events++
		return nil
	}), replay.Options{
		Mode:          replay.Fast,
		Speed:         1,
		TimestampUnit: time.Millisecond,
		Stream:        stream,
		Policy:        replay.NewTimestampPolicy(replay.TimestampStep, 100),
	})
	if err != nil {
		panic(err)
	}
	fmt.Println(events, stats.Applied)
}
