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
incremental,binance,BTC/USDT,1700000000100,bid,,,100.00,2.0000
`
	stream, _ := feed.NormalizeStreamID("binance", "BTC/USDT")
	bk := book.New(16)
	_, err := replay.Run(context.Background(), feed.NewDecoder(strings.NewReader(data)), bk, replay.HandlerFunc(func(_ context.Context, event replay.Event) error {
		if event.Actionable() {
			fmt.Println(event.Version, event.BidPx, event.AskPx)
		}
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
}
