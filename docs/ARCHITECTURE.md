# Architecture

`orderbook-constructor` is organized around a small public API and internal
supporting infrastructure.

## Public Packages

- `book` owns fixed-point prices, quantities, snapshots, deltas, and the
  single-writer L2 book.
- `feed` owns CSV decoding and stream identity normalization.
- `replay` owns synchronization policy, replay state, event generation, and the
  callback interface.
- `feed/gencsv` generates deterministic synthetic feeds for tests and
  benchmarks.

## Internal Packages

Internal packages provide command and benchmark infrastructure: SPSC ring
transport, async logging, latency histograms, clocks, and strategy runners.
Those packages are not part of the compatibility contract.

## Concurrency Model

The book mutation path is single-writer. Callers may read `Book.BBOSnapshot`
concurrently; other methods should be called by the writer goroutine or while
the book is quiesced.

`replay.Run` is synchronous and callback-based. Callers that need asynchronous
transport can publish `replay.Event` values from the callback into their own
queue or worker system.

## Synchronization

Replay policies classify snapshots and incrementals before mutation. Timestamp
policies are useful for deterministic feeds; update-ID policy should be used
when venue sequence ranges are available. A crossed book or forward gap
invalidates actionable state until a newer valid snapshot is accepted.
