package main

import (
	"fmt"
	"io"
	"strings"

	"github.com/anubhav-pandey1/orderbook-constructor/feed"
)

func main() {
	const data = `type,exchange,symbol,timestamp,side,bids,asks,price,size
snapshot,binance,BTC/USDT,1700000000000,,"[[100.00,1.0000]]","[[101.00,1.0000]]",,
incremental,binance,BTC/USDT,1700000000100,bid,,,100.00,2.0000
`
	dec := feed.NewDecoder(strings.NewReader(data))
	for {
		record, err := dec.Next()
		if err == io.EOF {
			return
		}
		if err != nil {
			panic(err)
		}
		fmt.Println(record.Line, record.Kind, record.Stream)
	}
}
