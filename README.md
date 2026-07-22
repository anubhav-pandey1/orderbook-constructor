# orderbook-constructor

[![CI](https://github.com/anubhav-pandey1/orderbook-constructor/actions/workflows/ci.yml/badge.svg)](https://github.com/anubhav-pandey1/orderbook-constructor/actions/workflows/ci.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/anubhav-pandey1/orderbook-constructor.svg)](https://pkg.go.dev/github.com/anubhav-pandey1/orderbook-constructor)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

`orderbook-constructor` is a Go library for constructing an aggregated level-2
order book from snapshots and incremental updates. It provides fixed-point book
types, CSV decoding, synchronization policies, and a replay runner with callback
events.

The supported public packages are:

- `book`: fixed-point prices/quantities and the single-writer L2 book.
- `feed`: stream identity normalization and CSV decoding.
- `replay`: decode -> synchronize -> apply -> callback orchestration.

The transport, logger, benchmark, and command implementations remain under
`internal` and are not part of the public compatibility contract.

## Install

```sh
go get github.com/anubhav-pandey1/orderbook-constructor
```

Supported Go versions: Go 1.20 through Go 1.26. The `go.mod` directive is kept at
the oldest version proved by CI.

## Basic Use

```go
bk := book.New(512)
bid, _ := book.ParsePrice("100.00")
ask, _ := book.ParsePrice("101.00")
qty, _ := book.ParseQuantity("1.0000")

bbo, err := bk.ApplySnapshot(&book.Snapshot{
    Bids: []book.Level{{Price: bid, Qty: qty}},
    Asks: []book.Level{{Price: ask, Qty: qty}},
})
if err != nil {
    return err
}
fmt.Println(bbo.BidPx, bbo.AskPx)
```

Replay with a callback:

```go
stream, _ := feed.NormalizeStreamID("binance", "BTC/USDT")
stats, err := replay.Run(ctx, feed.NewDecoder(r), book.New(512), replay.HandlerFunc(
    func(ctx context.Context, event replay.Event) error {
        if event.Actionable() {
            fmt.Println(event.Version, event.BidPx, event.AskPx)
        }
        return nil
    },
), replay.Options{
    Mode:          replay.Fast,
    Speed:         1,
    TimestampUnit: time.Millisecond,
    Stream:        stream,
    Policy:        replay.NewTimestampPolicy(replay.TimestampStep, 100),
})
_ = stats
```

More runnable programs are in `examples/`.

See `docs/ARCHITECTURE.md` for the package boundaries and concurrency model.

## Development

```sh
make build
make test
make vet
go test ./...
go test -race ./...
```

The fixture used by CLI and golden tests lives at
`testdata/btc_orderbook_updates.csv`.

Useful commands:

```sh
go run ./cmd/replay -csv testdata/btc_orderbook_updates.csv -log discard
go run ./cmd/bench -csv testdata/btc_orderbook_updates.csv -fixture-iters 20
```

Install local Git hooks:

```sh
./scripts/install-hooks.sh
pwsh ./scripts/install-hooks.ps1
```

## Compatibility

The public API is limited to `book`, `feed`, `feed/gencsv`, and `replay`.
Packages under `internal` may change without notice.

The book is optimized for a single writer. `Book.BBOSnapshot` is safe for
concurrent readers; direct mutation and depth snapshots should be called by the
writer goroutine or while the book is otherwise quiesced.

## Versioning

Releases use semantic versioning. Pre-1.0 releases may refine the public API.
After `v1.0.0`, patch releases contain fixes, minor releases add
backward-compatible API, and breaking changes require a new major version. A
future `v2` release will use the Go module path suffix `/v2`.

See `CHANGELOG.md` for release notes and migration guidance.
See `docs/RELEASE.md` for the release process and tag policy.
