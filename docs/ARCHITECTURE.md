# Architecture

`orderbook-constructor` is a Go library for building and replaying level-2
order books from normalized feed records. The production boundary is deliberately
small: public packages expose fixed-point market data types, CSV feed decoding,
deterministic fixture generation, and replay synchronization. Command packages
and internal packages provide examples, benchmarks, transport, logging, and
strategy infrastructure without expanding the import compatibility surface.

## Goals

- Provide importable Go packages with stable, idiomatic APIs for order-book
  construction and replay.
- Keep the hot mutation path single-writer and allocation-aware.
- Make synchronization decisions explicit before mutating book state.
- Let applications decide how to handle replay events: inline, queued,
  persisted, or forwarded to strategy workers.
- Keep benchmark and command infrastructure available without making it public
  library contract.

## Package Map

| Area | Package | Role | Compatibility |
| --- | --- | --- | --- |
| Public | `book` | Fixed-point prices and quantities, snapshots, deltas, BBO, depth, and the L2 book engine. | Public API |
| Public | `feed` | CSV decoder, feed record model, stream identity normalization. | Public API |
| Public | `replay` | Replay loop, synchronization policies, callbacks, replay events, pacing, resync hooks. | Public API |
| Public | `feed/gencsv` | Deterministic synthetic feed writer and in-memory generator for tests, examples, and benchmarks. | Public API, utility scoped |
| Internal | `internal/ring` | Generic single-producer/single-consumer ring for command pipelines. | Internal only |
| Internal | `internal/logx` | Asynchronous line logger built on the SPSC ring. | Internal only |
| Internal | `internal/strategy` | Strategy runner used by the replay command. | Internal only |
| Internal | `internal/clock` | Real and simulated monotonic clocks. | Internal only |
| Internal | `internal/metrics` | Lightweight histograms and rates. | Internal only |
| Internal | `internal/bench` | Benchmark helper types. | Internal only |
| Commands | `cmd/replay` | CLI replay pipeline over CSV input. | Not import contract |
| Commands | `cmd/gencsv` | CLI fixture generator. | Not import contract |
| Commands | `cmd/bench` | Benchmark harness and reports. | Not import contract |
| Examples | `examples/*` | Import smoke examples and strategy-style consumers. | Documentation |

Only non-`internal` packages are intended to be imported by downstream projects.
The SPSC ring is intentionally not public: its correctness depends on a strict
single-producer/single-consumer ownership model, and exposing it would force a
separate, durable concurrency contract that is wider than the order-book
library itself.

## Runtime Data Flow

The common library flow is:

```text
io.Reader
  -> feed.Decoder
  -> replay.Run
  -> replay.Policy classification
  -> book.Book mutation
  -> replay.Event callback
  -> application handler, queue, logger, strategy, or metrics sink
```

`feed.Decoder` parses the normalized CSV schema into `feed.Record` values.
`replay.Run` checks the configured stream, optionally paces records according to
source timestamps, classifies each cursor through the selected policy, mutates
the `book.Book` only when the policy says the record is applicable, and emits a
`replay.Event` for applied state changes and invalidations.

The command replay pipeline adds internal queues and logger workers around the
same core:

```text
CSV file
  -> feed.Decoder
  -> replay.Run
  -> event SPSC ring
  -> strategy runner
  -> logger SPSC ring
  -> stdout, file, or discard sink
```

That command architecture demonstrates one asynchronous composition, but the
public API does not require callers to use internal rings or strategy runners.

## Public API Boundary

The public import surface is the compatibility contract for releases:

- `book` contains the core market-data representation and mutation semantics.
- `feed` contains the current CSV record source.
- `replay` contains stateful replay orchestration and synchronization policy.
- `feed/gencsv` contains deterministic generated inputs for local testing and
  performance work.

The internal and command packages can change more freely. They should not be
used by external consumers, and the Go module path enforces this for
`internal/*`.

The module declares `go 1.20`. CI proves the minimum supported Go version by
running format, vet, tests, and race tests across Go `1.20.x` through `1.26.x`.
Latest-Go-only jobs run `staticcheck`, `govulncheck`, benchmark smoke tests, and
an external-import smoke test that imports the public packages from a separate
temporary module.

## Book Ownership Model

`book.Book` is a single-writer data structure. All mutating methods and most
read methods must be called from the owner goroutine or while the book is
quiesced. The exception is `Book.BBOSnapshot`, which is implemented through an
atomic quote cache and is safe for concurrent readers when there is only one
writer.

This ownership model keeps the mutation path simple:

- Bids and asks are stored in side-specific maps keyed by fixed-point price.
- Best-price lookup is backed by side-specific heaps.
- Deletes leave stale heap entries behind and cleanup happens lazily.
- Accepted mutations publish the latest BBO to the atomic quote cache.
- Full depth snapshots allocate, copy, and sort current levels.

The book rejects invalid state before it becomes externally visible. Snapshot
application is transactional: candidate bid and ask sides are built and
validated first, then swapped into the book only after validation succeeds.
Delta application validates side, price, quantity, and crossing before
publishing the new BBO.

## Synchronization Model

Replay synchronization is separate from book mutation. A `replay.Policy`
classifies every snapshot or update cursor into one of three actions:

| Action | Meaning |
| --- | --- |
| `Apply` | The record may mutate the book. |
| `Discard` | The record is stale or duplicate and must not mutate the book. |
| `Resync` | The stream is no longer trusted and needs an authoritative snapshot. |

Reasons provide the failure class: stale cursor, duplicate cursor, forward gap,
missing cursor, crossed book, or invalid snapshot.

The replay state machine has three states:

| State | Meaning |
| --- | --- |
| `Uninitialized` | No accepted snapshot has synchronized the book yet. |
| `Synchronized` | The book can produce actionable events. |
| `Desynchronized` | The book was invalidated and awaits a valid newer snapshot. |

Snapshots are the synchronization boundary. Incrementals received before a
snapshot are ignored and can trigger one snapshot request. Gaps, crossed deltas,
and invalid snapshots invalidate actionable state. When replay was previously
synchronized, invalidation emits a `BookInvalidated` event so consumers can stop
acting on the old book epoch.

The currently supplied policies are:

- Arrival order policy: accepts records as they arrive and performs no cursor
  validation.
- Timestamp policy: requires positive timestamps, rejects stale or duplicate
  records, and can require either exact timestamp steps or monotonic increases.
- Update-ID policy: requires update-ID ranges and applies updates that bridge
  the next expected sequence number.

The CSV decoder does not currently populate update-ID fields. `NewUpdateIDPolicy`
is public for callers that can supply `feed.Record` values with cursor metadata,
but the `cmd/replay` CSV command rejects that policy because the current CSV
format has no update-ID columns.

## Event Model

`replay.Run` is synchronous. The handler is invoked on the replay goroutine after
each applied snapshot, applied incremental, or book invalidation. If the handler
returns an error, replay stops and wraps that error with the notification ID.

Events include:

- Notification ID, book version, and sync epoch.
- Event kind and synchronization state.
- Resync reason for invalidations.
- Best bid and best ask fields.
- Source event timestamp, due time, ingress time, and apply time.

`Event.Actionable` is true only for synchronized applied events. Consumers should
treat invalidation events as risk controls, not as trading signals.

Backpressure is delegated to the handler. Inline handlers naturally backpressure
the replay loop. Queueing handlers can choose lossless publish, bounded drop, or
application-specific overflow behavior.

## Failure Model

Input and state errors are fail-fast where continuing would hide data
corruption:

- Invalid CSV header, malformed rows, invalid numbers, unknown record kinds, and
  stream mismatches return errors.
- Invalid replay options return errors before input is consumed.
- Handler and snapshot-requester errors stop replay and preserve context.
- Context cancellation is checked before each record and during pacing.
- EOF while synchronized returns successfully.
- EOF while unsynchronized returns `SnapshotRequiredError` wrapping
  `ErrSnapshotRequired`.

Synchronization failures are handled without applying the offending record:

- Stale and duplicate cursors are discarded.
- Forward gaps and missing cursors trigger resync.
- Crossed deltas invalidate the book.
- Invalid or crossed snapshots trigger resync and snapshot request.

Snapshot requests are coalesced while desynchronized. Replay requests at most
one snapshot until a valid snapshot is accepted again.

## Performance Model

The library favors predictable hot-path behavior:

- Prices and quantities are fixed-point integers, avoiding floating-point drift
  and allocation-heavy decimal types.
- Snapshot application builds replacement sides and swaps them in one operation.
- Delta updates are map updates plus heap maintenance.
- BBO reads use atomics and avoid allocation.
- Full depth snapshots allocate and sort by design.
- Internal queues use power-of-two SPSC rings to avoid locks in the command hot
  path.
- Synthetic feeds and benchmarks are deterministic so performance changes are
  reproducible.

The main concurrency tradeoff is explicit: `Book` is not a multi-writer or
fully thread-safe order book. Applications needing concurrent mutation should
serialize records into one owner goroutine and share results through events,
snapshots, or `BBOSnapshot`.

## Testing And Verification

The test strategy maps to the architecture:

- Public package tests cover parsing, exact book mutation, crossing prevention,
  snapshot transactionality, stream normalization, CSV decoding, policy
  decisions, replay state transitions, event payloads, and error wrapping.
- Internal package tests cover SPSC ring ordering and shutdown, logger delivery
  modes, simulated clock wakeups, strategy close/error handling, histograms, and
  benchmark helper behavior.
- Command tests cover flag parsing, invalid configuration, stream/policy
  constraints, generator row counts, and file safety paths.
- Race-sensitive paths are covered by CI race jobs across the supported Go
  version matrix.

Local hooks and scripts run formatting, vet, tests, and, on pre-push/full modes,
race tests plus optional `staticcheck`, `govulncheck`, and benchmark smoke
checks when those tools are installed.

## Known Tradeoffs

- Public source comments are intentionally minimal because detailed contracts
  live in package docs, examples, tests, and these design documents.
- The public replay runner currently consumes `*feed.Decoder`; a future generic
  record-source interface would make non-CSV ingestion and update-ID replay more
  direct.
- `DepthSnapshot`, `BidLevelCount`, and `AskLevelCount` are not concurrent-read
  APIs; only `BBOSnapshot` is designed for concurrent reads.
- `feed/gencsv` is a deterministic fixture generator, not a market simulator or
  arbitrary CSV writer.
- Internal logger and ring components rely on SPSC ownership. Promoting them to
  public API would require either hardening those contracts or adding separate
  MPSC/MPMC abstractions.
