package gencsv_test

import (
	"bytes"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/anubhav-pandey1/orderbook-constructor/feed"
	"github.com/anubhav-pandey1/orderbook-constructor/feed/gencsv"
)

type failingWriter struct {
	limit int
	n     int
}

func (w *failingWriter) Write(p []byte) (int, error) {
	if w.n+len(p) > w.limit {
		return 0, io.ErrShortWrite
	}
	w.n += len(p)
	return len(p), nil
}

func TestConfigValidationFailures(t *testing.T) {
	for _, tc := range []struct {
		mutate func(*gencsv.Config)
		want   string
	}{
		{func(c *gencsv.Config) { c.Incrementals = -1 }, "incrementals must be"},
		{func(c *gencsv.Config) { c.TSStep = 0 }, "ts-step must be"},
		{func(c *gencsv.Config) { c.LevelsPerSide = 0 }, "levels-per-side must be"},
		{func(c *gencsv.Config) { c.MaxLevels = c.LevelsPerSide - 1 }, "max-levels must be"},
		{func(c *gencsv.Config) { c.Exchange = "" }, "exchange and symbol"},
		{func(c *gencsv.Config) { c.Symbol = "" }, "exchange and symbol"},
	} {
		cfg := gencsv.DefaultConfig()
		tc.mutate(&cfg)
		if _, err := gencsv.NewGenerator(cfg); err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Fatalf("NewGenerator config=%+v err=%v want %q", cfg, err, tc.want)
		}
		if err := gencsv.Write(io.Discard, cfg); err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Fatalf("Write config=%+v err=%v want %q", cfg, err, tc.want)
		}
	}
}

func TestGeneratorNilExhaustionAndSequence(t *testing.T) {
	var nilGen *gencsv.Generator
	if rec, ok := nilGen.Next(); ok || rec.Kind != 0 {
		t.Fatalf("nil next=%+v %v", rec, ok)
	}
	cfg := gencsv.DefaultConfig()
	cfg.Incrementals = 1
	gen, err := gencsv.NewGenerator(cfg)
	if err != nil {
		t.Fatal(err)
	}
	first, ok := gen.Next()
	if !ok || first.Kind != feed.KindSnapshot || first.FirstUpdateID != 1 || first.FinalUpdateID != 1 || !first.HasUpdateID || first.Snap == nil || len(first.Snap.Bids) == 0 || len(first.Snap.Asks) == 0 {
		t.Fatalf("first=%+v ok=%v", first, ok)
	}
	second, ok := gen.Next()
	if !ok || second.Kind != feed.KindDelta || second.FirstUpdateID != 2 || second.FinalUpdateID != 2 || !second.HasUpdateID || second.Side == 0 || second.Px <= 0 {
		t.Fatalf("second=%+v ok=%v", second, ok)
	}
	if rec, ok := gen.Next(); ok || rec.Kind != 0 {
		t.Fatalf("after exhaustion=%+v %v", rec, ok)
	}
}

func TestWritePropagatesWriterFailure(t *testing.T) {
	cfg := gencsv.DefaultConfig()
	cfg.Incrementals = 3
	err := gencsv.Write(&failingWriter{limit: 4}, cfg)
	if !errors.Is(err, io.ErrShortWrite) {
		t.Fatalf("err=%v", err)
	}
}

func TestGeneratedSnapshotIntervalsReplaceIncrementals(t *testing.T) {
	cfg := gencsv.DefaultConfig()
	cfg.Incrementals = 5
	cfg.SnapshotEvery = 2
	var buf bytes.Buffer
	if err := gencsv.Write(&buf, cfg); err != nil {
		t.Fatal(err)
	}
	dec := feed.NewDecoder(bytes.NewReader(buf.Bytes()))
	var rows, snapshots, deltas int
	for {
		rec, err := dec.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		rows++
		if rec.Kind == feed.KindSnapshot {
			snapshots++
		} else {
			deltas++
		}
	}
	if rows != 6 || snapshots != 3 || deltas != 3 {
		t.Fatalf("rows/snapshots/deltas=%d/%d/%d", rows, snapshots, deltas)
	}
}
