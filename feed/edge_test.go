package feed_test

import (
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/anubhav-pandey1/orderbook-constructor/book"
	"github.com/anubhav-pandey1/orderbook-constructor/feed"
)

func requireFeedErrorContains(t *testing.T, err error, want string) {
	t.Helper()
	if err == nil {
		t.Fatalf("expected error containing %q", want)
	}
	if !strings.Contains(err.Error(), want) {
		t.Fatalf("error=%v, want substring %q", err, want)
	}
}

func TestDecodeHeaderAndFieldCountErrors(t *testing.T) {
	for _, tc := range []struct {
		name, data, want string
	}{
		{"empty input", "", "feed header"},
		{"short header", "type,exchange\n", "feed header"},
		{"short record", csvHeader + "\nincremental,binance,BTC/USDT,1000,bid\n", "wrong number of fields"},
		{"long record", csvHeader + "\nincremental,binance,BTC/USDT,1000,bid,,,,100.00,1.0\n", "wrong number of fields"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := feed.NewDecoder(strings.NewReader(tc.data)).Next()
			requireFeedErrorContains(t, err, tc.want)
		})
	}
}

func TestDecodeSnapshotEmptyBookDefersValidationToBook(t *testing.T) {
	data := buildCSV(csvHeader, `snapshot,binance,BTC/USDT,1000,,[],[],,`)
	rec, err := feed.NewDecoder(strings.NewReader(data)).Next()
	if err != nil {
		t.Fatal(err)
	}
	if rec.Kind != feed.KindSnapshot || rec.Snap == nil || len(rec.Snap.Bids) != 0 || len(rec.Snap.Asks) != 0 {
		t.Fatalf("record=%+v", rec)
	}
	if _, err := book.New(1).ApplySnapshot(rec.Snap); !errors.Is(err, book.ErrEmptySnapshot) {
		t.Fatalf("apply empty snapshot err=%v", err)
	}
}

func TestDecodeWhitespaceAndEmptyFieldRules(t *testing.T) {
	for _, row := range []string{
		`snapshot,binance,BTC/USDT,1000, ,[],[],,`,
		`incremental,binance,BTC/USDT,1000,bid, , ,100.00,1.0`,
	} {
		rec, err := feed.NewDecoder(strings.NewReader(buildCSV(csvHeader, row))).Next()
		if err != nil {
			t.Fatalf("row %q: %v", row, err)
		}
		if rec.Stream != (feed.StreamID{Exchange: "binance", Symbol: "BTCUSDT"}) {
			t.Fatalf("stream=%+v", rec.Stream)
		}
	}
	for _, tc := range []struct {
		row, want string
	}{
		{`snapshot,binance,BTC/USDT,1000,bid,[],[],,`, "side must be empty"},
		{`incremental,binance,BTC/USDT,1000,bid,[], ,100.00,1.0`, "bids must be empty"},
		{`incremental,binance,BTC/USDT,1000,bid, ,[],100.00,1.0`, "asks must be empty"},
	} {
		_, err := feed.NewDecoder(strings.NewReader(buildCSV(csvHeader, tc.row))).Next()
		requireFeedErrorContains(t, err, tc.want)
	}
}

func TestDecodeLevelNumberFailures(t *testing.T) {
	for _, tc := range []struct {
		row, want string
	}{
		{`snapshot,binance,BTC/USDT,1000,,"[[true,1.0]]",[],,`, "cannot unmarshal bool"},
		{`snapshot,binance,BTC/USDT,1000,,"[[100.001,1.0]]",[],,`, "excess decimal precision"},
		{`snapshot,binance,BTC/USDT,1000,,"[[100.00,-1.0]]",[],,`, "invalid numeric syntax"},
		{`snapshot,binance,BTC/USDT,1000,,"[[100.00,1.00001]]",[],,`, "excess decimal precision"},
		{`snapshot,binance,BTC/USDT,1000,,"{}",[],,`, "cannot unmarshal object"},
	} {
		_, err := feed.NewDecoder(strings.NewReader(buildCSV(csvHeader, tc.row))).Next()
		requireFeedErrorContains(t, err, tc.want)
	}
}

func TestDecodeEOFIsStableAfterEnd(t *testing.T) {
	dec := feed.NewDecoder(strings.NewReader(buildCSV(csvHeader)))
	for i := 0; i < 3; i++ {
		if _, err := dec.Next(); !errors.Is(err, io.EOF) {
			t.Fatalf("EOF[%d]=%v", i, err)
		}
	}
}

func TestRecordAliasesAndStreamString(t *testing.T) {
	if feed.KindIncremental != feed.KindDelta {
		t.Fatalf("alias=%d delta=%d", feed.KindIncremental, feed.KindDelta)
	}
	stream := feed.StreamID{Exchange: "binance", Symbol: "BTCUSDT"}
	if stream.String() != "binance:BTCUSDT" {
		t.Fatalf("stream string=%q", stream)
	}
}
