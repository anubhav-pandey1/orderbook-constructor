# Performance-First L2 Order Book Design

Status: Proposed implementation design

## 1. Objective

Build a Go service that reconstructs an aggregated Level-2 order book from ordered snapshot and incremental events, publishes a consistent best bid and ask after every accepted event, and delivers those publications to a strategy with low and measurable latency.

The design optimizes the path:

```text
decode -> validate -> apply L2 change -> publish quote -> strategy receives
```

Correctness is defined relative to the ordered input stream. The supplied format does not include Binance update IDs, so timestamps are used as a fixture-specific surrogate sequence. This can detect violations of the fixture's exact 100 ms step, but it cannot provide the same completeness guarantee as a venue update ID.

## 2. Scope

### In scope

- One order book per exchange and symbol instance.
- Aggregated L2 price levels: one total quantity at each price.
- Full snapshot replacement.
- Incremental level insertion, quantity replacement, and deletion.
- Constant-time map lookup and amortized constant-time best-price lookup.
- A single book writer and concurrent strategy consumer.
- Atomic SPSC ring-buffer notification.
- Concurrent, consistent best-bid/best-ask reads.
- Fast and timestamp-paced CSV replay.
- Timestamp-derived ordering and gap detection for the supplied synthetic feed.
- Explicit desynchronization, book invalidation, and snapshot recovery.
- Asynchronous, batched logging.
- Core, parsing, ring, and end-to-end benchmarks.

### Non-goals

- L3 order IDs, per-order FIFO queues, or exchange-side matching.
- Market, limit, IOC, FOK, or other order-entry semantics.
- Deriving order-level history from aggregate L2 quantities.
- Network connectivity to Binance.
- Production-grade feed-gap detection without exchange sequence/update IDs.
- Persistence, replication, or recovery after process failure.
- Multiple writers mutating the same book.

## 3. Input observations and assumptions

The supplied CSV contains 2,242 events: two snapshots and 2,240 incrementals. The second snapshot occurs midway through the stream and therefore must replace the current book rather than be treated as an error.

The timestamps are 13-digit values consistent with epoch milliseconds, despite the assignment describing them as nanoseconds. Prices have at most two decimal places and quantities have at most four.

Verified fixture facts:

- All 2,241 adjacent timestamp deltas are uniformly `100`, with no unexpected step.
- Five incremental deletions target levels absent from the current authoritative book.
- Zero accepted snapshot or incremental steps produce `bestBid >= bestAsk`.
- Every row has raw stream identity `binance,BTC/USDT`, normalized to `binance,BTCUSDT`.

An absent-level deletion is treated as a successful idempotent no-op and still produces a book version and strategy notification. The timestamp and crossed-book facts become golden assertions rather than undocumented assumptions.

Assumptions that must be stated in the README:

- Input events are received in their intended application order.
- A snapshot is authoritative for both sides and replaces all previous levels.
- An incremental quantity replaces, rather than adds to, the quantity at its price.
- Quantity zero deletes the price level.
- Negative quantities and excess decimal precision are invalid.
- Incremental timestamps in this fixture must advance by exactly 100 ms.
- A newer valid snapshot is authoritative and re-establishes synchronization even after a detected gap.
- The executable handles one configured exchange/symbol; the `Book` type is reusable for additional instances.

The decoder trims stream components, lowercases the exchange, uppercases the symbol, and removes `/`, `-`, and `_` separators from the symbol. It normalizes every record before synchronization classification and requires it to equal the retained configured identity, which defaults to `binance,BTCUSDT` for this fixture. A mismatch is rejected before book mutation. Snapshot recovery always uses that retained configured identity rather than copying identity from the gap-causing or otherwise discarded record.

Timestamp stepping is deliberately isolated behind a synchronizer interface. Exact timestamp increments are a property of this synthetic assignment stream, not a general market-data guarantee. If the input format later supplies Binance update IDs, an update-ID synchronizer must replace the timestamp-step implementation.

## 4. Architecture

```text
                         writer goroutine
CSV reader / replay  --------------------------+
  decoder -> synchronizer -> L2 Book -> quote  |
                                      |         |
                                      +-> QuoteCache (atomic readers)
                                      |
                                      +-> SPSC event ring
                                                |
                                                v
                                         strategy goroutine
                                                |
                                                +-> SPSC log ring
                                                          |
                                                          v
                                                   logger goroutine
                                                   batched stdout/file
```

The CSV reader, replay pacing, synchronization checks, validation, and book mutation run on one goroutine. This avoids an input queue and any synchronization in the mutation path. The strategy and logger run concurrently on separate goroutines.

The book event ring is single-producer/single-consumer:

- Producer: book/replay goroutine.
- Consumer: strategy goroutine.

The log ring is also SPSC:

- Producer: strategy goroutine.
- Consumer: logger goroutine.

## 5. Domain representation

Floating-point values must not be used as price-level keys.

```go
type Price int64    // number of 0.01 quote-currency ticks
type Quantity int64 // number of 0.0001 base-asset units

type Side uint8

const (
    Bid Side = iota + 1
    Ask
)
```

The parser converts decimal text directly to scaled integers. It does not parse through `float64`. Snapshot JSON is decoded with `json.Decoder.UseNumber`, preserving the decimal token for exact conversion.

The decimal parser rejects:

- Signs where the domain does not allow them.
- More than two price decimals.
- More than four quantity decimals.
- Overflow during scaling.
- NaN, infinity, exponent notation, and empty values.

Fewer decimal digits are padded with zeroes. For example, `100012.1` becomes `10001210` price ticks and `1.5` becomes `15000` quantity units.

## 6. Book structures

```go
type level struct {
    quantity   Quantity
    generation uint64
}

type heapEntry struct {
    price      Price
    generation uint64
}

type sideBook struct {
    side       Side
    levels     map[Price]level
    prices     priceHeap
    nextGen    uint64
    staleCount int
}

type Book struct {
    version uint64
    bids    sideBook
    asks    sideBook
    quotes  quoteCache
}
```

The level map is authoritative. A max-heap indexes bid prices and a min-heap indexes ask prices. Heap entries carry a generation so that deletion followed by reinsertion at the same price cannot make an old heap entry appear current.

Synchronization state, epoch, notification ID, exchange/symbol identity, and policy cursors belong to the producer pipeline rather than the mutation core. This keeps feed-specific sequencing outside `Book`. Entering `Desynchronized` calls `Book.Invalidate`, which clears both sides and publishes an empty quote cache before the strategy can observe another actionable quote.

### 6.1 Set or replace a level

For a positive quantity:

1. If the price exists, replace its quantity. Do not push another heap entry.
2. If it does not exist, allocate a new generation, insert the level, and push one heap entry.

An existing-level update is O(1) and should allocate nothing.

### 6.2 Delete a level

1. Delete the price from the map if present.
2. Increment the stale-entry count if a level was removed.
3. Leave the heap entry in place for lazy cleanup.
4. Treat an absent price as a successful no-op.

Deletion is O(1). Cleaning a deleted best price is amortized across subsequent best-price lookups.

### 6.3 Read the best level

Repeatedly inspect the heap root:

1. Look up the root price in the authoritative map.
2. Return it if the map level exists and its generation equals the heap entry generation.
3. Otherwise pop the stale root and continue.

This handles deletion and delete-then-reinsert correctly.

### 6.4 Heap rebuilding

Lazy deletion can accumulate stale entries. Rebuild a side heap from its active map when:

```text
heap length > 2 * active level count + 64
```

The threshold is a tunable implementation constant and must be benchmarked. Rebuilding is O(n), happens outside the common existing-level update path, and bounds retained stale storage. Every full snapshot naturally constructs fresh heaps.

### 6.5 Complete depth

The maps retain every active price level, satisfying complete L2 reconstruction. A sorted full-depth view is created only on explicit request by copying map entries and sorting them off the update hot path.

Direct concurrent map iteration is prohibited. Full-depth inspection occurs after the writer is quiesced or through a writer-owned control request. Concurrent strategies use the atomic BBO cache or their versioned ring event.

## 7. State transitions

### 7.1 Synchronization state machine

```text
Uninitialized --valid snapshot--> Synchronized
Synchronized  --gap/crossed book--> Desynchronized
Desynchronized --valid snapshot--> Synchronized
```

Incrementals are applied only in `Synchronized`. Incrementals received in either other state are ignored. A transition to `Desynchronized`:

1. Clears both authoritative level maps and price heaps.
2. Publishes an empty atomic quote cache.
3. Increments the notification ID but not the accepted book version.
4. Publishes one `BookInvalidated` event with a reason.
5. Requests a replacement snapshot through the pipeline's `SnapshotRequester`.

The strategy must treat `BookInvalidated` as an immediate stop-trading signal. No crossed or partially invalid quote is published.

### 7.2 Timestamp-derived sequencing

Let `last` be the timestamp of the last accepted event and `step` be the configured fixture cadence, defaulting to 100 ms. For an incremental update:

```text
received == last + step  -> apply
received <= last         -> discard as stale or duplicate
received != last + step  -> declare a gap and desynchronize
```

The final case includes a timestamp beyond the expected next value and a positive but unexpected sub-step value. Both mean that this synthetic stream's sequencing contract has been violated.

Stale or duplicate events are counted and discarded without a book version or strategy event. A gap-causing incremental is not applied. While desynchronized, later incrementals are counted and ignored until a valid snapshot arrives.

This algorithm is intentionally replaceable. Real Binance integration must use first/final update IDs rather than assuming that exchange timestamps advance at a fixed cadence.

### 7.3 Snapshot

1. Validate exchange, symbol, timestamp, both arrays, precision, positive quantities, and duplicate prices. Reject a snapshot only when both sides are empty; one empty side is valid but non-actionable on that side.
2. Discard a snapshot that is not newer than the last accepted authoritative state.
3. Construct new bid and ask `sideBook` values off-book with pre-sized maps and heaps.
4. Determine their best levels and require `bestBid < bestAsk` when both sides exist.
5. If crossed, invalidate the current book and request another snapshot.
6. Otherwise replace both current sides in one writer operation.
7. Set `Synchronized`, increment the sync epoch, and anchor timestamp sequencing to the snapshot timestamp.
8. Increment the accepted book version and notification ID once.
9. Publish the quote cache.
10. Publish exactly one `SnapshotApplied` ring event.

A valid snapshot is authoritative and may recover from an earlier gap even when it does not arrive at exactly `last + step`. No observer can see a partially built snapshot.

A crossed snapshot is treated as a resynchronization symptom, not as an isolated validation error to repair locally. It can indicate a stale or mismatched snapshot, missed deltas, incorrect side handling, or corrupt source data. The engine discards the candidate snapshot, invalidates any previously actionable state, and requests another authoritative snapshot.

### 7.4 Incremental update

1. Require `Synchronized`; otherwise ignore the update while awaiting a snapshot.
2. Validate exchange, symbol, side, price, and quantity.
3. Apply the timestamp-derived sequence rules before mutation.
4. Apply set/replace for positive quantity or deletion for zero.
5. Resolve the current BBO, lazily cleaning stale heap roots.
6. Require `bestBid < bestAsk` when both sides exist.
7. If crossed, immediately clear the book, publish `BookInvalidated`, and request a snapshot. Do not publish the tentative quote.
8. Otherwise increment the accepted version and notification ID, including for an absent-level deletion.
9. Publish the quote cache.
10. Publish exactly one `IncrementalApplied` ring event.

### 7.5 Crossed-book invariant

When both sides have liquidity:

```text
best bid < best ask
```

Equality is invalid as well as a strictly crossed price. A crossed result is treated as loss of synchronization, not as a warning that can remain tradeable. The single writer may tentatively update its private maps before checking, but readers still see the previous atomic quote. It clears the private state and publishes an empty cache before emitting the invalidation event, so no external reader observes the crossed BBO.

## 8. Book event

```go
type BookEvent struct {
    NotificationID uint64
    Version        uint64
    SyncEpoch      uint64
    Kind           EventKind // SnapshotApplied, IncrementalApplied, BookInvalidated
    SyncState      SyncState
    Reason         SyncReason
    ExchangeTime   int64

    DueNS int64 // paced local monotonic target; zero in fast mode

    // Captured when replay releases the decoded update into the ingestion path.
    // This is the local monotonic start of the assignment callback-latency metric.
    IngressNS int64 // monotonic process-relative
    ApplyNS   int64 // monotonic process-relative

    BestBidPrice Price
    BestBidQty   Quantity
    BestAskPrice Price
    BestAskQty   Quantity
    HasBid       bool
    HasAsk       bool
}
```

The injected real clock derives these compact values from `time.Since(processOrigin).Nanoseconds()`, retaining monotonic behavior without copying `time.Time` into every ring slot. `DueNS` holds the source-time-derived target after rebasing into that process clock and is zero for fast replay. `IngressNS` is stamped once, immediately after replay releases a decoded update for processing. Historical exchange time remains separate metadata and is never subtracted from the local latency clock: the CSV contains historical epoch milliseconds, so subtracting it from a current process clock would measure file age rather than engine latency.

The event carries BBO by value. The strategy therefore observes the exact BBO associated with that version even if the writer has already processed later updates.

An invalidation event has `SyncState == Desynchronized`, both `HasBid` and `HasAsk` false, and no actionable price. `NotificationID` is strictly increasing for every ring publication. `Version` counts only accepted snapshots and incrementals, while `SyncEpoch` changes whenever a snapshot establishes a new authoritative book.

## 9. Concurrent BBO reads

Strategies should prefer the BBO in `BookEvent`. Other readers may call a race-free quote cache implemented as a sequence lock whose fields are themselves atomic:

```go
type quoteCache struct {
    sequence atomic.Uint64
    version  atomic.Uint64
    bidPrice atomic.Int64
    bidQty   atomic.Int64
    askPrice atomic.Int64
    askQty   atomic.Int64
    flags    atomic.Uint32
}
```

Writer protocol:

1. Increment `sequence` to an odd value.
2. Store all quote fields atomically.
3. Increment `sequence` to an even value.

Reader protocol:

1. Load `sequence`; retry if odd.
2. Load all fields.
3. Load `sequence` again.
4. Accept only if both sequence values are equal and even.

Using atomic fields avoids Go data races while the sequence guards multi-field consistency. The common strategy path does not need these additional reads because its ring event is already consistent.

## 10. SPSC ring buffer

The ring capacity is a power of two so slot selection is `position & (capacity-1)`.

```go
type Ring[T any] struct {
    slots []T
    mask  uint64

    // Persistent producer/consumer sequences and cached opposite cursors.
    // Shared cursor atomics are at least one cache line apart.
    producer producerState
    consumer consumerState
    closed   atomic.Bool
}
```

Producer protocol:

1. Read the producer-owned sequence.
2. Test it against the cached consumed cursor; refresh the consumer atomic only when locally full.
3. Write the event value into `slots[head & mask]`.
4. Atomically store `head+1`, publishing the completed slot.

Consumer protocol:

1. Read the consumer-owned sequence.
2. Test it against the cached published cursor; refresh the producer atomic only when locally empty.
3. Copy `slots[tail & mask]`.
4. Atomically store `tail+1`, releasing the slot for reuse.

The API exposes allocation-free `TryPublish` and `TryConsume` operations. Blocking wrappers spin for a small configurable count and then call `runtime.Gosched()` until successful, closed, or cancelled. Close is producer-owned and preserves all already published values for consumer drain. The exact cursor layout and close protocol are specified in `SPEC.md`.

The event ring is lossless. When it is full, the book writer experiences backpressure rather than dropping or coalescing updates. Metrics expose ring occupancy, producer waits, and maximum observed depth.

Default event and log capacities are 65,536 entries each. They are configurable with `--event-ring` and `--log-ring`, rejected unless powers of two, and reported together with actual record sizes so memory usage is explicit.

## 11. Strategy and asynchronous logging

The strategy runner owns the receive timestamp so all strategies observe the same callback boundary:

1. Pops the next `BookEvent`.
2. Samples `receiveNS` from the injected monotonic clock immediately after the successful pop and before callback or logging work.
3. Calls `OnBookEvent(event, receiveNS)`.
4. For `SnapshotApplied` and `IncrementalApplied`, the callback reads best bid and ask from the event fields. These values belong to the exact version that triggered the notification. For `BookInvalidated`, it treats both sides as non-actionable.
5. Records release/ingress-to-receive and apply-to-receive latency, plus paced metrics when `DueNS != 0`.
6. Builds a fixed-size structured `LogRecord` without formatting text and pushes it into the logging SPSC ring.

Reading BBO from the value-copied event satisfies the strategy's per-update read requirement without racing a later writer version. Tests may additionally read `BBOSnapshot()` from the atomic quote cache, but may compare it with an event only when the cache version equals the event version; the writer is otherwise allowed to have advanced.

The logger goroutine owns a large `bufio.Writer`. It drains records in batches, formats them, writes them to the configured sink, and flushes on:

- Batch-size threshold.
- Optional flush interval.
- Context cancellation and normal shutdown.

Supported sinks:

- `stdout` for the assignment run.
- File for less terminal-dependent replay.
- `discard` for benchmarks.

The default run mode is lossless. A slow sink can eventually propagate backpressure through the log ring and event ring; it cannot corrupt or reorder the book. Queue depth and wait metrics make this visible.

## 12. State synchronization, resync, and replay

### 12.1 `SyncPolicy` seam

Feed-specific ordering rules live outside the L2 mutation core. The pipeline classifies an event before calling the book, then advances the policy cursor only after the book accepts the event and passes the crossed-book invariant.

```go
type SyncAction uint8

const (
    SyncApply SyncAction = iota + 1
    SyncDiscard
    SyncResync
)

type SyncDecision struct {
    Action SyncAction
    Reason SyncReason
}

type SyncCursor struct {
    Timestamp     int64
    FirstUpdateID uint64
    FinalUpdateID uint64
    HasUpdateID   bool
}

type SyncPolicy interface {
    ClassifySnapshot(SyncCursor) SyncDecision
    ClassifyUpdate(SyncCursor) SyncDecision
    AcceptSnapshot(SyncCursor)
    AcceptUpdate(SyncCursor)
    Invalidate()
}
```

Classification is side-effect free. `AcceptSnapshot` or `AcceptUpdate` commits the cursor only after the corresponding book mutation is valid. `Invalidate` disables incremental continuity while retaining the last accepted watermark used to reject stale recovery snapshots.

The seam supports two concrete policies:

- `UpdateIDPolicy`: production-oriented update-range continuity.
- `TimestampPolicy`: assignment-specific timestamp stepping or monotonic checks.

The selected policy is constructed once at startup. Its classification methods must allocate nothing. The interface dispatch cost is included in end-to-end benchmarks; if it is material, the pipeline may bind the selected concrete classifier once without changing policy semantics.

### 12.2 Classification and recovery table

| Current state | Input and policy decision | Book action | Publication | Next state |
|---|---|---|---|---|
| `Uninitialized` | Valid snapshot / `Apply` | Validate and atomically install | `SnapshotApplied` | `Synchronized` |
| `Uninitialized` | Incremental | Ignore and request/await snapshot | None | `Uninitialized` |
| `Synchronized` | Incremental / `Apply` | Apply level, then check BBO | `IncrementalApplied` | `Synchronized` |
| `Synchronized` | Event / `Discard` | No mutation | None | `Synchronized` |
| `Synchronized` | Event / `Resync` | Clear both sides and quote cache | `BookInvalidated` | `Desynchronized` |
| `Synchronized` | Applied event creates `bestBid >= bestAsk` | Clear both sides and quote cache | `BookInvalidated` | `Desynchronized` |
| `Desynchronized` | Incremental | Ignore while waiting | None | `Desynchronized` |
| `Desynchronized` | Valid snapshot / `Apply` | Atomically install and re-anchor policy | `SnapshotApplied` | `Synchronized` |
| Any | Stale snapshot / `Discard` | No mutation | None | Unchanged |
| Any | Invalid or crossed snapshot / `Resync` | Keep/clear non-actionable state and request again | At most one transition event | `Desynchronized` |

Only a transition from actionable `Synchronized` state publishes `BookInvalidated`; ignored incrementals do not repeatedly emit invalidations or snapshot requests. Recovery is always snapshot-driven.

### 12.3 `UpdateIDPolicy`

This is the preferred policy when a venue supplies a snapshot `lastUpdateID` and incremental ranges `[firstUpdateID, finalUpdateID]`.

Let `last` be the final accepted update ID and `next = last + 1`:

| Condition | Classification | Effect |
|---|---|---|
| `finalUpdateID <= last` | `Discard` | Entire event is stale or duplicated |
| `firstUpdateID <= next && finalUpdateID >= next` | `Apply` | Event covers the required next update; advance to `finalUpdateID` after book acceptance |
| `firstUpdateID > next` | `Resync` | At least one update ID is missing |
| IDs absent or malformed | `Resync` | The selected policy cannot establish continuity |

An authoritative snapshot anchors `last` to its `lastUpdateID`. Venue-specific bootstrap rules—such as buffering WebSocket deltas while fetching the REST snapshot—belong in the feed adapter, but use this same classifier before book mutation.

The supplied CSV has no update-ID fields and therefore cannot use `UpdateIDPolicy`.

### 12.4 `TimestampPolicy`

Exchange timestamps are normally metadata and staleness signals, not sequence numbers. This policy exists because the verified assignment fixture advances by exactly 100 timestamp units for every adjacent event.

Configuration:

```text
--sync-policy=timestamp|update-id|off
--timestamp-mode=step|monotonic
--timestamp-unit=auto|ns|us|ms
--timestamp-step=100ms
```

Step mode, where `last` is the timestamp of the last accepted event:

| Condition | Classification | Effect |
|---|---|---|
| `received <= last` | `Discard` | Stale or duplicated event |
| `received == last + step` | `Apply` | Advance timestamp only after book acceptance |
| `received > last && received != last + step` | `Resync` | Synthetic cadence contract is broken |

Monotonic mode discards `received <= last` and applies any strictly newer event. Large positive gaps are metrics only in monotonic mode. With synchronization off, arrival order is trusted.

A valid newer snapshot re-anchors the timestamp policy even if it does not arrive at exactly `last + step`. Auto unit detection recognizes 13-digit epoch values as milliseconds and 19-digit values as nanoseconds. Ambiguous magnitudes cause a startup error requiring an explicit unit.

`TimestampPolicy` is not presented as a production substitute for exchange update IDs. Real feeds may publish equal timestamps, batch updates, or remain quiet across nominal intervals.

### 12.5 Snapshot request and recovery

On `SyncResync` or a crossed-book symptom:

1. Invalidate and clear the book before publishing any further actionable quote.
2. Publish one `BookInvalidated` event if transitioning from `Synchronized`.
3. Invalidate incremental continuity while retaining the last accepted snapshot-freshness watermark.
4. Call `SnapshotRequester` with exchange, symbol, last accepted cursor, received cursor, and reason.
5. Ignore incrementals until a valid snapshot is installed and accepted by the policy.

A live adapter may retry snapshot requests with bounded backoff while remaining non-actionable. The CSV adapter cannot make a network request, so it scans forward for the next snapshot and fails at EOF if still desynchronized.

### 12.6 Replay timing

```text
--replay=fast|paced
--speed=1.0
```

- `fast`: apply events without intentional delay.
- `paced`: reproduce timestamp spacing using Go's monotonic clock.
- `speed`: divide the original relative delay by the speed factor.

Paced replay calculates each target time from the first event and replay start:

```text
DueNS = replayStartNS + (eventTimestamp - firstTimestamp) * timestampUnit / speed
```

`DueNS` is zero in fast mode. In paced mode the replay waits until `DueNS`, then stamps `IngressNS`; `IngressNS - DueNS` is scheduler lateness and `receiveNS - DueNS` is scheduled-source-to-callback latency. Computing every due time relative to the first event avoids accumulating scheduling error from a sequence of relative sleeps. Replay pacing and synchronization classification both consume exchange timestamps, but they are independent: changing replay speed never changes classification.

## 13. Error handling

Parser and validation errors include the CSV record number, event kind, exchange, symbol, and offending field without dumping an entire large snapshot.

Fatal errors:

- Invalid event type or side.
- Exchange or symbol mismatch.
- Invalid decimal or excess precision.
- Negative quantity.
- Duplicate snapshot price.
- Ring capacity that is zero or not a power of two.
- Failure to obtain a replacement snapshot before EOF in offline replay.

Synchronization events and non-fatal conditions:

- Incremental before the first snapshot: ignore and request/await a snapshot.
- Deleting an absent level.
- Equal or regressing timestamp: discard as stale or duplicate.
- Unexpected timestamp step: invalidate and request a snapshot.
- Crossed book: invalidate and request a snapshot.
- Incremental while desynchronized: ignore while awaiting a snapshot.
- Temporary ring backpressure.

`SnapshotRequester` failures are surfaced to the caller and counted. A live adapter may retry with bounded backoff. The CSV adapter cannot make a network request, so it keeps scanning for the next valid snapshot and returns a desynchronized EOF error if none appears.

## 14. Lifecycle and shutdown

Startup order:

1. Validate configuration and open the input.
2. Start logger goroutine.
3. Start strategy goroutine.
4. Begin replay/book writer loop.

Normal shutdown order:

1. Writer finishes input and marks the event ring closed.
2. Strategy drains all book events and marks the log ring closed.
3. Logger drains all log records and flushes its buffer.
4. Main goroutine joins both consumers and returns their errors.

On fatal writer error, the same drain-and-close sequence is attempted so already accepted events are not silently lost. Context cancellation lets blocking ring operations terminate.

## 15. Proposed Go package layout

```text
orderbook/
  cmd/replay/main.go
  cmd/bench/main.go
  internal/price/
  internal/book/
  internal/syncx/
  internal/ring/
  internal/feed/
  internal/pipeline/
  internal/strategy/
  internal/logx/
  internal/bench/
  internal/clock/
  internal/synth/
  testdata/
  bench/
  Makefile
  README.md
  DESIGN.md
  SPEC.md
  BENCHMARK.md
```

The implementation uses the Go standard library only. `SPEC.md` owns the exact files and package contracts; this layout shows responsibility boundaries.

### 15.1 README content contract

The submission README must let a reviewer run and assess the system without reconstructing intent from this design. It includes:

- Prerequisites and the supported Go version.
- `make build`, `make run`, `make benchmark`, and `make test` examples.
- The default fixture, configured exchange and symbol, and the important replay, synchronization, ring-capacity, and logging flags.
- A plain-language summary of the single-writer pipeline, map-plus-heap book, fixed-point representation, SPSC delivery, and timestamp-policy seam.
- The fixture assumptions in Section 3, especially the distinction between its 100 ms timestamp cadence and production update-ID sequencing.
- The following material trade-offs:

| Choice | Advantages | Costs and limitations |
|---|---|---|
| Single writer plus SPSC rings | Deterministic application order; no mutation-path lock contention; direct latency attribution | A slow downstream consumer creates backpressure; the pipeline is not multi-writer |
| Map plus generation-tagged price heap | Average O(1) existing-level replacement and non-best deletion; O(1) cached BBO reads; complete depth retained | First insertion is O(log n); deleting a best level may clean stale entries; churn can trigger a heap rebuild |
| Fixed-point integers | Exact price keys and deterministic comparisons without floating-point drift | Parsing and formatting are more explicit; scale constants reflect the verified fixture precision |
| BBO copied into each event | Strategy reads the quote for the exact triggering version without a shared-state race | Events are larger than a version-only notification |
| Atomic quote cache | Race-free, consistent BBO reads for independent concurrent readers | Sequence-guarded reads may retry while the writer publishes |
| Asynchronous batched logging | Formatting and system calls stay outside the strategy callback's measured path | Log output can lag the callback and a lossless slow sink eventually propagates backpressure |
| Fixture timestamp-step policy | Detects duplicate, stale, and missing steps in the supplied synthetic stream | It is not valid production Binance sequencing; real integration requires exchange update IDs |
| Lossless delivery | No accepted update is silently dropped before the strategy | Sustained overload stalls ingestion instead of shedding work |

### 15.2 Makefile target contract

| Target | Required behavior |
|---|---|
| `make build` | Compile both `cmd/replay` and `cmd/bench`; generated binaries may be placed under `bin/` |
| `make run` | Replay `btc_orderbook_updates.csv` in `fast` mode with timestamp-step synchronization and BBO logging to stdout |
| `make benchmark` | Run the Go benchmarks and end-to-end latency runner, then create or update the canonical `BENCHMARK.md` report |
| `make test` | Run `go test -race ./...` |

`make run` is deliberately fast by default so the complete assignment fixture finishes promptly. Paced 1x replay remains available through documented runtime arguments, for example `RUN_ARGS="--replay=paced --speed=1"`, or an optional convenience target. Runtime arguments may also override synchronization, ring, and sink defaults without editing the Makefile.

Principal book API:

```go
func New(capHint int) *Book
func (b *Book) ApplySnapshot(*Snapshot) (BBO, error)
func (b *Book) ApplyDelta(Side, Price, Quantity) (DeltaResult, error)
func (b *Book) Invalidate()
func (b *Book) BBOSnapshot() BBO // safe for concurrent callers
func (b *Book) Version() uint64

// Writer-owned or quiesced access only; pipeline control code provides safe
// full-depth requests when the engine is running.
func (b *Book) DepthSnapshot() Depth
```

The application wires concrete components behind small interfaces for testing:

```go
type EventPublisher interface {
    Publish(context.Context, BookEvent) error
}

type SnapshotRequester interface {
    RequestSnapshot(context.Context, ResyncRequest) error
}

type Strategy interface {
    OnBookEvent(BookEvent, receiveNS int64)
}

type LogSink interface {
    WriteBatch([]LogRecord) error
    Flush() error
}
```

The producer pipeline owns `SyncPolicy`, classifies records before invoking these mutation APIs, and constructs publication events. This keeps stale packets out of the strategy ring while ensuring that loss of synchronization is immediately visible to consumers.

## 16. Benchmark design

`make benchmark` runs standard Go benchmarks and an end-to-end latency runner. Every report records CPU, OS, architecture, Go version, `GOMAXPROCS`, ring capacities, validation policy, replay mode, and fixture hash.

### 16.1 Core microbenchmarks

- Existing price quantity replacement.
- New price insertion.
- Non-best deletion.
- Best-price deletion and lazy cleanup.
- Delete/reinsert generation handling.
- Heap rebuild under churn.
- Timestamp-step accept, stale discard, and gap invalidation.
- Snapshot application at 10, 100, 1,000, and 10,000 levels per side.
- Concurrent atomic BBO reads.

Core benchmarks exclude parsing, clocks, event publication, and logging. They report:

- `ns/op`.
- Updates/second via `ReportMetric`.
- `B/op` and allocations/op.

The steady-state existing-level update target is zero allocations per operation.

### 16.2 Component benchmarks

- Decimal parsing.
- CSV incremental decoding.
- Snapshot JSON decoding.
- SPSC `TryPush`/`TryPop` throughput and wraparound.
- SPSC producer/consumer throughput with `GOMAXPROCS=2`.
- Async logger formatting into a discard sink and a temporary file.

### 16.3 End-to-end benchmarks

- Predecoded fixture: apply plus event-ring consumption.
- CSV decode, apply, and no-op strategy.
- CSV decode, apply, strategy, and discard logger.
- Paced replay correctness, excluded from throughput comparison.

Latency runner reports:

- **Assignment callback latency (primary):** local update release/`IngressNS` to `receiveNS`, sampled immediately after the successful ring pop and before callback work. Both endpoints use the same process-relative monotonic clock.
- **Apply-to-callback latency (secondary):** apply completion/`ApplyNS` to `receiveNS`.
- **Apply duration (secondary):** mutation start to `ApplyNS`.
- In paced mode, schedule-target-to-receive latency and scheduler lateness. The target is rebased into the process monotonic domain using `replayStartNS + (eventTimestamp - firstTimestamp) * timestampUnit / speed`; the raw historical epoch is never subtracted from a local clock.
- p50, p90, p95, p99, p99.9, and maximum.
- Event-ring and log-ring maximum occupancy.

Benchmark stdout logging is forbidden. The logger sink is `discard` unless the logger itself is the subject of the benchmark.

Run benchmarks multiple times after warmup. Report raw commands and results; do not claim hardware-independent performance thresholds.

### 16.4 Canonical benchmark report

`make benchmark` creates or updates `BENCHMARK.md`, the human-readable submission report. Raw run output may additionally be retained under `bench/`, but it does not replace the canonical report. At minimum, `BENCHMARK.md` contains:

1. Environment: CPU, OS, architecture, Go version, `GOMAXPROCS`, `GOGC`, `GOMEMLIMIT`, run date or revision, and fixture hash.
2. Configuration: replay and synchronization policies, timestamp unit and step, ring capacities, actual event and log-record sizes, logging sink, and synthetic generator seed and workload size where applicable.
3. Throughput: core apply-path and end-to-end CSV updates/second, plus the separately identified synthetic steady-state workloads.
4. Assignment callback latency: release/ingress-to-callback p50, p90, p95, p99, p99.9, and maximum.
5. Secondary latency: apply-to-callback, apply duration, and paced schedule-target/scheduler-lateness percentiles where applicable.
6. Allocation results and maximum ring occupancy.
7. Exact commands used to reproduce the results and any relevant measurement caveats.

The report must distinguish fixture results from synthetic headline throughput and must not present the historical CSV epoch-to-current-process difference as engine latency.

## 17. Correctness and concurrency tests

### Book tests

- Initial snapshot establishes correct depth and BBO.
- Second snapshot completely replaces old levels.
- Incremental insertion, replacement, and deletion.
- Absent-level deletion is a successful no-op with a new version.
- Delete then reinsert at the same price invalidates stale heap entries.
- Deleting the best level reveals the correct next level.
- Empty bid or ask side returns an explicit absent value.
- Duplicate snapshot price is rejected.
- Exact timestamp-step acceptance.
- Stale and duplicate timestamp discard without mutation or publication.
- Timestamp gap clears the book, publishes invalidation, and awaits a snapshot.
- A valid newer snapshot recovers a desynchronized book and starts a new sync epoch.
- Crossed snapshot or incremental clears the book and publishes no actionable quote.
- Fixed-point parsing, maximum values, and overflow.

### Fixture golden test

After replaying the supplied fixture as milliseconds:

- Adjacent timestamp delta count: 2,241, all exactly 100.
- Absent-level deletions exercised: 5.
- Crossed or locked post-apply steps: 0.
- Accepted event count: 2,242.
- Book version: 2,242.
- Final active bid levels: 227.
- Final active ask levels: 301.
- Final best bid: 99,993.99.
- Final best ask: 99,998.24.
- Strategy event count: 2,242.
- Strategy event versions are strictly ordered without gaps.
- No invalidation event is produced for this gap-free fixture.

### Ring tests

- Empty and full behavior.
- Capacity and power-of-two validation.
- Wraparound across many capacity multiples.
- Producer-to-consumer ordering over millions of values.
- Cancellation while blocked.
- Close and drain semantics.
- Race-detector execution.

### Pipeline tests

- Snapshot publication is one event, not one event per level.
- Strategy sees the BBO belonging to each event version.
- Strategy callback latency uses the release/ingress and receive stamps from the same monotonic clock domain.
- Fast events have `DueNS == 0`; paced events use the expected rebased due time and satisfy `DueNS <= IngressNS <= ApplyNS <= receiveNS`.
- A quote-cache cross-check is performed only when its version equals the event version.
- Raw `binance,BTC/USDT` normalizes to configured identity `binance,BTCUSDT`; mismatches fail before policy or book mutation, and resync requests retain the configured identity.
- Slow strategy causes backpressure without loss or reordering.
- Slow logger causes visible backpressure without data corruption.
- Shutdown drains both rings and flushes logging.
- Fast and paced modes produce identical final books and event sequences.
- Timestamp-step, monotonic, and off synchronization modes.
- A gap triggers exactly one invalidation event and one snapshot request.
- Incrementals are ignored between invalidation and recovery snapshot.

Run the full suite with `go test -race ./...` in addition to the normal test target.

## 18. Acceptance criteria

- `make build`, `make run`, `make benchmark`, and `make test` work from the project root.
- Both snapshots and all valid incrementals are applied in source order.
- Stale timestamped updates are discarded, and unexpected timestamp steps invalidate the book.
- A desynchronized book remains empty and non-actionable until a valid snapshot restores it.
- Any locked or crossed BBO invalidates and clears the book before publication.
- No reader observes a partially applied snapshot or torn BBO.
- The strategy receives one versioned notification for every accepted input event.
- The strategy receives one non-actionable invalidation notification for every transition to `Desynchronized`.
- The golden fixture result matches the expected depth counts and BBO.
- The event ring and logger preserve ordering and do not silently drop data.
- Core existing-level updates are allocation-free after initialization.
- Benchmarks distinguish engine, parsing, notification, and logging costs.
- Primary assignment callback latency is measured from local monotonic update release/ingress to strategy callback entry; apply-to-callback and apply duration are reported as secondary percentiles.
- Paced replay additionally reports rebased monotonic schedule-target latency and scheduler lateness without subtracting the raw historical CSV epoch from a local clock.
- Every record is normalized and checked against the retained configured stream identity before synchronization or mutation, and every resync request uses that retained identity.
- `README.md`, the Makefile targets, and canonical `BENCHMARK.md` satisfy the submission contracts in Sections 15.1, 15.2, and 16.4.
- Timestamp-step gap detection is documented as a synthetic-feed safeguard, not a production substitute for exchange update IDs.
- Tests pass under the Go race detector.

## 19. Architecture decisions

1. **L2 rather than L3:** The feed contains aggregate price quantities and no order identities. L3 structures would add cost without recoverable information.
2. **Fixed-point integers:** Exact keys and predictable operations are more important than decimal-library convenience.
3. **Single writer:** Deterministic ordering and a lock-free mutation path outweigh unsupported multi-writer complexity.
4. **Map plus price heap:** The required hot query is BBO, not arbitrary ordered traversal. This avoids a third-party balanced tree while retaining complete depth in maps.
5. **Generation-tagged lazy deletion:** Deletion stays O(1) without allowing delete/reinsert bugs.
6. **BBO by value in ring events:** Each consumer sees the quote corresponding to the triggering version.
7. **Atomic SPSC rings:** The producer/consumer topology is known and does not require general-purpose channels or MPMC queues.
8. **Lossless backpressure:** The assignment requires every update to reach the strategy; silent dropping is not acceptable.
9. **Asynchronous logging:** Formatting and system calls are isolated from the measured engine-to-strategy path.
10. **Timestamp-step synchronization is fixture-specific:** Exact 100 ms increments provide a useful synthetic sequence for this assignment, while genuine exchange gap detection still requires update IDs.
11. **Fail closed on inconsistency:** A sequence gap or `bestBid >= bestAsk` clears the book, publishes an invalidation signal, and blocks incrementals until snapshot recovery.

## 20. Reference implementations

- Rust matching engine: <https://github.com/anthdm/rust-trading-engine>
- C++ multi-order-type book: <https://github.com/Tzadiko/Orderbook>
- Go order-book package: <https://github.com/danielgatis/go-orderbook>
- Production-oriented Rust engine: <https://github.com/joaquinbejar/OrderBook-rs>
- C++ engine and L2 reconstruction tooling: <https://github.com/mansoor-mamnoon/limit-order-book>
- Binance Spot depth stream: <https://developers.binance.com/docs/binance-spot-api-docs/web-socket-streams>
