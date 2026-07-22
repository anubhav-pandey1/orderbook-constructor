package feed_test

import (
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/anubhav-pandey1/orderbook-constructor/book"
	"github.com/anubhav-pandey1/orderbook-constructor/feed"
)

const csvHeader = "type,exchange,symbol,timestamp,side,bids,asks,price,size"

func buildCSV(rows ...string) string { return strings.Join(rows, "\r\n") + "\r\n" }

func TestDecodeCRLFAndNormalizeStream(t *testing.T) {
	data := buildCSV(
		csvHeader,
		`snapshot, Binance , btc/usdt ,1000,,"[[100.00,1.50],[99.00,2.00]]","[[101.00,1.00],[102.00,3.00]]",,`,
		`incremental,binance,BTC-USDT,1100,bid,,,100.50,2.5`,
		`incremental,binance,BTC_USDT,1200,ask,,,101.00,0`,
	)
	dec := feed.NewDecoder(strings.NewReader(data))
	r1, err := dec.Next()
	if err != nil {
		t.Fatal(err)
	}
	wantStream := feed.StreamID{Exchange: "binance", Symbol: "BTCUSDT"}
	if r1.Kind != feed.KindSnapshot || r1.Line != 2 || r1.TS != 1000 || r1.Stream != wantStream || r1.Snap == nil {
		t.Fatalf("snapshot = %+v", r1)
	}
	wantBids := []book.Level{{Price: 10000, Qty: 15000}, {Price: 9900, Qty: 20000}}
	wantAsks := []book.Level{{Price: 10100, Qty: 10000}, {Price: 10200, Qty: 30000}}
	if !levelsEqual(r1.Snap.Bids, wantBids) || !levelsEqual(r1.Snap.Asks, wantAsks) {
		t.Fatalf("snapshot levels = %+v", r1.Snap)
	}
	r2, err := dec.Next()
	if err != nil || r2.Kind != feed.KindDelta || r2.Stream != wantStream || r2.Side != book.Bid || r2.Px != 10050 || r2.Qty != 25000 {
		t.Fatalf("delta = %+v err=%v", r2, err)
	}
	r3, err := dec.Next()
	if err != nil || r3.Side != book.Ask || r3.Px != 10100 || r3.Qty != 0 {
		t.Fatalf("delete = %+v err=%v", r3, err)
	}
	if _, err := dec.Next(); !errors.Is(err, io.EOF) {
		t.Fatalf("EOF = %v", err)
	}
}

func TestNormalizeStreamID(t *testing.T) {
	for _, tc := range []struct {
		exchange, symbol string
		want             feed.StreamID
	}{
		{" Binance ", " btc/usdt ", feed.StreamID{Exchange: "binance", Symbol: "BTCUSDT"}},
		{"KRAKEN", "eth-usd", feed.StreamID{Exchange: "kraken", Symbol: "ETHUSD"}},
		{"coinbase", "btc_usd", feed.StreamID{Exchange: "coinbase", Symbol: "BTCUSD"}},
	} {
		got, err := feed.NormalizeStreamID(tc.exchange, tc.symbol)
		if err != nil || got != tc.want {
			t.Errorf("normalize = %+v,%v want %+v", got, err, tc.want)
		}
	}
	for _, tc := range []struct {
		exchange, symbol, want string
	}{
		{"", "BTCUSDT", "non-empty exchange"},
		{"binance", "", "non-empty exchange"},
		{"binance", "/_-", "empty normalized symbol"},
	} {
		if _, err := feed.NormalizeStreamID(tc.exchange, tc.symbol); err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("NormalizeStreamID(%q, %q) error=%v, want substring %q", tc.exchange, tc.symbol, err, tc.want)
		}
	}
}

func TestDecodeRejectsSchemaViolations(t *testing.T) {
	for _, tc := range []struct{ name, csv, want string }{
		{"bad header", buildCSV("kind,exchange,symbol,timestamp,side,bids,asks,price,size"), "got header"},
		{"bad side", buildCSV(csvHeader, `incremental,binance,BTC/USDT,1003,BUY,,,100.00,1.0`), "invalid side"},
		{"bad timestamp", buildCSV(csvHeader, `incremental,binance,BTC/USDT,x,bid,,,100.00,1.0`), `timestamp "x"`},
		{"negative timestamp", buildCSV(csvHeader, `incremental,binance,BTC/USDT,-1,bid,,,100.00,1.0`), "timestamp must be non-negative"},
		{"unknown type", buildCSV(csvHeader, `weird,binance,BTC/USDT,1004,bid,,,100.00,1.0`), "unknown type"},
		{"snapshot side populated", buildCSV(csvHeader, `snapshot,binance,BTC/USDT,1000,bid,[],[],,`), "side must be empty"},
		{"delta bids populated", buildCSV(csvHeader, `incremental,binance,BTC/USDT,1000,bid,[],,100.0,1.0`), "bids must be empty"},
		{"empty exchange", buildCSV(csvHeader, `incremental,,BTC/USDT,1000,bid,,,100.0,1.0`), "stream identity requires"},
		{"trailing json", buildCSV(csvHeader, `snapshot,binance,BTC/USDT,1000,,"[] true",[],,`), "unexpected JSON value"},
		{"bad arity", buildCSV(csvHeader, `snapshot,binance,BTC/USDT,1000,,"[[100.0]]",[],,`), "must contain price and size"},
		{"exponent", buildCSV(csvHeader, `incremental,binance,BTC/USDT,1000,bid,,,1e2,1.0`), "invalid numeric syntax"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := feed.NewDecoder(strings.NewReader(tc.csv)).Next()
			requireFeedErrorContains(t, err, tc.want)
		})
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
