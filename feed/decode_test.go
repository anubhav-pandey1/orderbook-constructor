package feed_test

import (
	"io"
	"strings"
	"testing"

	"orderbook/book"
	"orderbook/feed"
)

const csvHeader = "type,exchange,symbol,timestamp,side,bids,asks,price,size"

// buildCSV joins rows with CRLF line endings, which encoding/csv must tolerate.
func buildCSV(rows ...string) string {
	return strings.Join(rows, "\r\n") + "\r\n"
}

func TestDecode(t *testing.T) {
	data := buildCSV(
		csvHeader,
		`snapshot,binance,BTC/USDT,1000,,"[[100.00,1.50],[99.00,2.00]]","[[101.00,1.00],[102.00,3.00]]",,`,
		`incremental,binance,BTC/USDT,1001,bid,,,100.50,2.5`,
		`incremental,binance,BTC/USDT,1002,ask,,,101.00,0`,
	)
	dec := feed.NewDecoder(strings.NewReader(data))

	// Record 1: snapshot (header is skipped; this is source line 2).
	r1, err := dec.Next()
	if err != nil {
		t.Fatalf("Next(1): %v", err)
	}
	if r1.Kind != feed.KindSnapshot {
		t.Fatalf("r1.Kind = %d, want KindSnapshot", r1.Kind)
	}
	if r1.Line != 2 {
		t.Errorf("r1.Line = %d, want 2", r1.Line)
	}
	if r1.ExchangeTime != 1000 {
		t.Errorf("r1.ExchangeTime = %d, want 1000", r1.ExchangeTime)
	}
	sn := r1.Snapshot
	if sn.Exchange != "binance" || sn.Symbol != "BTC/USDT" {
		t.Errorf("snapshot meta = %q/%q, want binance/BTC/USDT", sn.Exchange, sn.Symbol)
	}
	wantBids := []book.Level{{Price: 10000, Qty: 15000}, {Price: 9900, Qty: 20000}}
	wantAsks := []book.Level{{Price: 10100, Qty: 10000}, {Price: 10200, Qty: 30000}}
	if !levelsEqual(sn.Bids, wantBids) {
		t.Errorf("bids = %+v, want %+v", sn.Bids, wantBids)
	}
	if !levelsEqual(sn.Asks, wantAsks) {
		t.Errorf("asks = %+v, want %+v", sn.Asks, wantAsks)
	}

	// Record 2: incremental bid.
	r2, err := dec.Next()
	if err != nil {
		t.Fatalf("Next(2): %v", err)
	}
	if r2.Kind != feed.KindIncremental || r2.Line != 3 {
		t.Errorf("r2 kind/line = %d/%d, want incremental/3", r2.Kind, r2.Line)
	}
	wantInc := book.Incremental{Side: book.Bid, Price: 10050, Qty: 25000, ExchangeTime: 1001}
	if r2.Incremental != wantInc {
		t.Errorf("r2.Incremental = %+v, want %+v", r2.Incremental, wantInc)
	}

	// Record 3: incremental ask with size 0 (delete).
	r3, err := dec.Next()
	if err != nil {
		t.Fatalf("Next(3): %v", err)
	}
	wantDel := book.Incremental{Side: book.Ask, Price: 10100, Qty: 0, ExchangeTime: 1002}
	if r3.Incremental != wantDel {
		t.Errorf("r3.Incremental = %+v, want %+v", r3.Incremental, wantDel)
	}

	// EOF.
	if _, err := dec.Next(); err != io.EOF {
		t.Errorf("Next(EOF) = %v, want io.EOF", err)
	}
}

func TestDecodeBadSide(t *testing.T) {
	data := buildCSV(csvHeader, `incremental,binance,BTC/USDT,1003,BUY,,,100.00,1.0`)
	dec := feed.NewDecoder(strings.NewReader(data))
	if _, err := dec.Next(); err == nil {
		t.Fatal("expected error for bad side, got nil")
	}
}

func TestDecodeBadTimestamp(t *testing.T) {
	data := buildCSV(csvHeader, `incremental,binance,BTC/USDT,notanumber,bid,,,100.00,1.0`)
	dec := feed.NewDecoder(strings.NewReader(data))
	if _, err := dec.Next(); err == nil {
		t.Fatal("expected error for bad timestamp, got nil")
	}
}

func TestDecodeUnknownType(t *testing.T) {
	data := buildCSV(csvHeader, `weird,binance,BTC/USDT,1004,bid,,,100.00,1.0`)
	dec := feed.NewDecoder(strings.NewReader(data))
	if _, err := dec.Next(); err == nil {
		t.Fatal("expected error for unknown type, got nil")
	}
}

func levelsEqual(a, b []book.Level) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
