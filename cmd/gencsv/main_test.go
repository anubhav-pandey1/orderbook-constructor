package main

import (
	"bytes"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/anubhav-pandey1/orderbook-constructor/feed"
)

func TestRunWritesDecodableCSVAndReportsRows(t *testing.T) {
	path := filepath.Join(t.TempDir(), "out.csv")
	var stdout bytes.Buffer
	err := run([]string{"-out", path, "-incrementals", "5", "-snapshot-every", "2"}, &stdout)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(stdout.String(), "rows=6 incrementals=5") {
		t.Fatalf("stdout=%q", stdout.String())
	}
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	dec := feed.NewDecoder(f)
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

func TestRunRejectsBadArgumentsAndConfig(t *testing.T) {
	path := filepath.Join(t.TempDir(), "out.csv")
	for _, tc := range []struct {
		args []string
		want string
	}{
		{[]string{"-out", path, "-incrementals", "-1"}, "incrementals must be"},
		{[]string{"-out", path, "-ts-step", "0"}, "ts-step must be"},
		{[]string{"-out", path, "-levels-per-side", "0"}, "levels-per-side must be"},
		{[]string{"-out", path, "extra"}, "unexpected positional arguments"},
		{[]string{"-bad"}, "flag provided but not defined"},
	} {
		if err := run(tc.args, nil); err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Fatalf("args=%v err=%v want %q", tc.args, err, tc.want)
		}
	}
	if err := run([]string{"-out", t.TempDir(), "-incrementals", "1"}, nil); err == nil || !strings.Contains(err.Error(), "create output") {
		t.Fatalf("create output err=%v", err)
	}
}
