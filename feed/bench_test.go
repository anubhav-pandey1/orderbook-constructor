package feed_test

import (
	"bytes"
	"io"
	"strconv"
	"strings"
	"testing"

	"github.com/anubhav-pandey1/orderbook-constructor/feed"
)

func buildIncrementalCSV(n int) []byte {
	var b strings.Builder
	b.WriteString(csvHeader)
	b.WriteString("\r\n")
	for i := 0; i < n; i++ {
		side := "bid"
		if i%2 == 1 {
			side = "ask"
		}
		b.WriteString("incremental,binance,BTC/USDT,")
		b.WriteString(strconv.Itoa(1000 + i))
		b.WriteString(",")
		b.WriteString(side)
		b.WriteString(",,,")
		b.WriteString(strconv.Itoa(90000 + i%1000))
		b.WriteString(".50,1.2500\r\n")
	}
	return []byte(b.String())
}

var sinkRecordLine int

func BenchmarkDecodeIncremental(b *testing.B) {
	const rows = 2000
	data := buildIncrementalCSV(rows)
	b.ReportAllocs()
	b.SetBytes(int64(len(data)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		dec := feed.NewDecoder(bytes.NewReader(data))
		for {
			rec, err := dec.Next()
			if err == io.EOF {
				break
			}
			if err != nil {
				b.Fatalf("decode: %v", err)
			}
			sinkRecordLine = rec.Line
		}
	}
}
