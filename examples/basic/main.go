package main

import (
	"fmt"

	"github.com/anubhav-pandey1/orderbook-constructor/book"
)

func main() {
	bid, _ := book.ParsePrice("100.00")
	ask, _ := book.ParsePrice("101.00")
	size, _ := book.ParseQuantity("1.5000")

	bk := book.New(16)
	if _, err := bk.ApplySnapshot(&book.Snapshot{
		Bids: []book.Level{{Price: bid, Qty: size}},
		Asks: []book.Level{{Price: ask, Qty: size}},
	}); err != nil {
		panic(err)
	}

	result, err := bk.ApplyDelta(book.Bid, bid, size+5000)
	if err != nil {
		panic(err)
	}
	fmt.Println(result.BBO.BidPx, result.BBO.BidQty)
}
