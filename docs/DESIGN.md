# Low-Level Design

This document describes the implementation-level design of the public and
internal packages. It is intentionally more concrete than `ARCHITECTURE.md`:
data structures, object ownership, control flow, validation paths, concurrency
assumptions, and memory behavior are called out package by package.

## Design Principles

- Keep the public API small and composable.
- Use fixed-point integers for market data.
- Validate before mutation whenever corrupted state would otherwise become
  visible.
- Make synchronization policy explicit and testable.
- Keep single-writer ownership visible in API usage and examples.
- Use internal packages for command transport and diagnostics without making
  them public compatibility commitments.

## `book`

### Public Types

`Price` and `Quantity` are `int64` fixed-point values.

| Type | Scale | Fraction digits | Example |
| --- | --- | --- | --- |
| `Price` | `100` | `2` | `"101.25"` becomes `10125` |
| `Quantity` | `10000` | `4` | `"3.5000"` becomes `35000` |

`Ticks` aliases `Price`, and `Qty` aliases `Quantity`. They are aliases rather
than new types, so callers can use whichever name reads best without conversion.

`Side` is an enum with `Bid` and `Ask`. Unknown side values are rejected by
mutation paths and print as `"unknown"`.

`Level`, `Snapshot`, `Depth`, `BBO`, `DeltaKind`, and `DeltaResult` are the
value objects used at the book boundary.

### Numeric Parsing

`ParsePrice`, `ParseQuantity`, and `ParseQty` use a shared scaled parser.

Happy paths:

- Unsigned decimal strings are accepted.
- Missing fractional digits are padded to the fixed scale.
- Exact scale precision is accepted.

Sad paths:

- Empty strings return `ErrEmptyNumber`.
- Signs, exponent notation, multiple dots, and non-digits return `ErrSyntax`.
- Too many fractional digits return `ErrPrecision`.
- Values that cannot fit in `int64` return `ErrOverflow`.

Parsing does not trim whitespace. Callers such as `feed.Decoder` trim fields
before parsing.

### Book Object

`Book` owns:

- `capHint`, used when allocating side maps and heaps.
- `version`, incremented for every accepted snapshot or delta.
- `bids` and `asks`, each a `sideBook`.
- `quotes`, an atomic BBO cache.

`New(capHint)` normalizes non-positive hints to the default capacity and creates
empty bid and ask sides. The quote cache starts with an empty BBO.

### Snapshot Application

`ApplySnapshot(*Snapshot)` is transactional.

Flow:

1. Reject nil snapshots and snapshots with no levels on either side.
2. Build a candidate bid side from snapshot bid levels.
3. Build a candidate ask side from snapshot ask levels.
4. Reject negative prices, non-positive quantities, and duplicate prices within
   the same side while building.
5. Compute candidate best bid and best ask.
6. Reject crossed or locked snapshots where best bid is greater than or equal
   to best ask.
7. Increment version, swap both candidate sides into the book, and publish the
   new BBO to the quote cache.

One-sided snapshots are valid as long as at least one level exists and all
levels on the populated side are valid.

Failure is non-mutating. The existing sides, version, and quote cache remain
unchanged when validation fails.

### Delta Application

`ApplyDelta(side, px, qty)` applies one level update.

Validation paths:

- Negative price returns `ErrInvalidPrice`.
- Negative quantity returns `ErrInvalidQuantity`.
- Unknown side returns `ErrInvalidSide`.
- A positive bid at or above the current best ask returns `ErrCrossedDelta`.
- A positive ask at or below the current best bid returns `ErrCrossedDelta`.

Quantity semantics:

- `qty > 0` inserts or updates a level.
- `qty == 0` deletes the level if present.
- `qty == 0` for an absent level is accepted and returns `AbsentDelete`.

Accepted deltas increment the book version, recompute BBO from the side books,
publish it to the quote cache, and return `DeltaResult`.

The returned `DeltaKind` is based on whether the target level existed before the
delta:

| Existing level | Quantity | Kind |
| --- | --- | --- |
| no | positive | `LevelInserted` |
| yes | positive | `LevelUpdated` |
| yes | zero | `LevelDeleted` |
| no | zero | `AbsentDelete` |

### Side Books And Heaps

Each `sideBook` combines a map and heap:

- `levels map[Price]level` stores live quantities and per-level generation.
- `prices priceHeap` stores candidate best prices.
- Bid heaps are max-heaps; ask heaps are min-heaps.
- `nextGen` gives each inserted level a generation token.
- `staleCount` tracks deleted heap entries waiting to be cleaned up.

Updates to an existing price keep the same generation and do not add a heap
entry. Inserts add one heap entry. Deletes remove from the map and leave the
heap entry stale. `best()` peeks the heap and lazily pops stale entries until it
finds a live matching generation.

`maybeRebuild()` compacts a side when the heap grows beyond `2*live + 64`. The
rebuild reuses the heap backing slice, appends live levels, heapifies, and resets
stale count.

### Quote Cache

`quoteCache` stores the latest BBO in atomics and uses a sequence counter:

1. Writer stores an odd sequence value to mark an in-progress update.
2. Writer stores version, prices, quantities, and side-present flags.
3. Writer stores the next even sequence value.
4. Reader retries while the sequence is odd or changes during the read.

This is a seqlock-style design for one writer and many readers. It gives
`BBOSnapshot()` a coherent view without locking. It does not make the whole
`Book` concurrent-safe.

### Concurrency And Undefined Behavior

Supported:

- One goroutine owns mutation.
- Any number of goroutines may call `BBOSnapshot()` concurrently with that
  single writer.
- `DepthSnapshot()` and level-count methods are called by the owner goroutine or
  while writes are stopped.

Unsupported:

- Concurrent calls to `ApplySnapshot`, `ApplyDelta`, or `Invalidate`.
- Concurrent `DepthSnapshot` or level-count reads while mutation is running.
- Multiple writer goroutines without external serialization.

Unsupported usage can produce Go data races and undefined behavior under the Go
memory model.

## `feed`

### Stream Identity

`NormalizeStreamID(exchange, symbol)` normalizes stream identity:

- Exchange is trimmed and lower-cased.
- Symbol is trimmed.
- `/`, `-`, and `_` are removed from the symbol.
- Remaining symbol runes are upper-cased.
- Empty exchange, empty symbol, or empty normalized symbol returns an error.

The normalized `StreamID` is used by replay to reject mixed or unexpected
streams.

### CSV Schema

The decoder expects exactly these columns:

```text
type,exchange,symbol,timestamp,side,bids,asks,price,size
```

`NewDecoder(io.Reader)` creates an `encoding/csv.Reader` with
`FieldsPerRecord = 9` and `ReuseRecord = true`.

The first `Next()` call reads and validates the header. Empty input is reported
as unexpected EOF for the header. Each later call reads one record and returns
`io.EOF` only after all records are consumed.

### Snapshot Rows

Snapshot rows use:

- `type = snapshot`
- `exchange`, `symbol`, `timestamp`
- empty `side`, `price`, and `size`
- `bids` and `asks` as JSON arrays of two-value arrays

Snapshot level JSON is decoded with `json.Decoder.UseNumber`. The decoder
rejects malformed JSON, non-array levels, wrong pair lengths, invalid price or
quantity syntax, and trailing JSON values after the level array.

The decoder can produce an empty snapshot if both JSON arrays are empty. The
book layer rejects that snapshot when applying it.

### Incremental Rows

Incremental rows use:

- `type = incremental`
- `exchange`, `symbol`, `timestamp`
- `side = bid|ask`
- empty `bids` and `asks`
- `price` and `size`

Quantity zero is valid and represents a delete. Negative timestamps are
rejected. Price and quantity parsing errors are wrapped with line and field
context.

### Record Object

`feed.Record` is the interchange object consumed by replay. CSV decoding
populates stream, line, timestamp, snapshot or delta fields. Update-ID cursor
fields exist for future or non-CSV sources, and for generated in-memory records,
but the current CSV decoder does not populate them.

## `replay`

### Core Objects

`Run(ctx, dec, bk, handler, opts)` orchestrates replay. Required dependencies
after defaults are applied:

- `dec *feed.Decoder`
- `bk *book.Book`
- `opts.Policy`
- `opts.Clock`
- normalized `opts.Stream`

Defaults:

- `Mode = Fast`
- `Speed = 1`
- `Policy = NewArrivalOrderPolicy()`
- `Clock = realClock`

`TimestampUnit` is required only in paced mode.

`Handler` is a callback interface. `HandlerFunc` adapts a function to that
interface.

`SnapshotRequester` is optional. It is called when replay detects that an
authoritative snapshot is needed.

### Replay Loop

Per-record flow:

1. Check context cancellation.
2. Decode the next record.
3. Track highest seen timestamp.
4. Reject records whose stream does not equal configured normalized stream.
5. In paced mode, compute due time from source timestamp delta and sleep until
   due time.
6. Capture ingress time.
7. Build a `Cursor` from timestamp and update-ID fields.
8. Ignore deltas while not synchronized and request a snapshot.
9. Ask the policy to classify the snapshot or update cursor.
10. Discard stale or duplicate records without mutating.
11. Desynchronize and request a snapshot on resync decisions.
12. Apply snapshots or deltas to the book.
13. Convert book errors into replay errors, resyncs, or invalidations.
14. Accept the cursor in the policy after successful mutation.
15. Update stats and publish an event.

EOF behavior:

- If state is `Synchronized`, EOF returns successful stats.
- If state is not `Synchronized`, EOF returns `SnapshotRequiredError`.

### Pacing

Paced replay anchors the first input timestamp to the replay clock's current
monotonic nanosecond value. Later record due times are:

```text
startNS + ((recordTS - firstTS) * TimestampUnit / Speed)
```

Overflow, NaN, infinite speed, non-positive speed, and non-positive timestamp
unit in paced mode are rejected.

### State And Events

Replay starts in `Uninitialized`. A successfully applied snapshot transitions to
`Synchronized`, clears pending snapshot-request state, increments sync epoch,
and emits `SnapshotApplied`.

Successfully applied deltas keep the same sync epoch and emit
`IncrementalApplied`.

`desynchronize` invalidates the book, invalidates the policy, and moves state to
`Desynchronized`. If replay was previously synchronized, it emits
`BookInvalidated` and increments invalidation stats. If replay was not yet
synchronized, no invalidation event is emitted because no actionable book epoch
existed.

`Event.Actionable()` returns true for synchronized applied events and false for
invalidation events.

### Snapshot Request Coalescing

Replay maintains a `snapshotRequested` flag. Once a snapshot request has been
issued while desynchronized, further records that need a snapshot do not call the
requester again until a valid snapshot is applied. This avoids a request storm
on long bad tails of input.

The request contains configured exchange and symbol, last accepted cursor, the
received cursor that triggered the request, and the reason.

### Policies

#### Arrival Order

Arrival-order policy always returns `Apply` and stores no state. It is useful
for fixtures that are already known to be clean or for benchmarks where cursor
validation should be excluded.

#### Timestamp

Timestamp policy state:

- `last`: last accepted timestamp.
- `hasLast`: whether any cursor has been accepted.
- `synced`: whether updates may apply.

Snapshot classification:

- Missing or non-positive timestamp returns `Resync/ReasonMissingCursor`.
- Timestamp less than last returns `Discard/ReasonStale`.
- Timestamp equal to last returns `Discard/ReasonDuplicate`.
- Newer timestamp applies.

Update classification:

- Missing timestamp or unsynced policy returns `Resync/ReasonMissingCursor`.
- Stale and duplicate timestamps discard.
- `TimestampMonotonic` applies any newer timestamp.
- `TimestampStep` applies exactly `last + step`.
- Invalid step configuration returns `Resync/ReasonMissingCursor`.
- A newer timestamp that skips the configured step returns
  `Resync/ReasonGap`.

Invalidation clears `synced` but keeps `last`, so old snapshots after a resync
are still discarded.

#### Update ID

Update-ID policy state:

- `last`: last accepted final update ID.
- `hasLast`: whether any cursor has been accepted.
- `synced`: whether updates may apply.

Snapshot classification:

- Missing update ID or zero final update ID returns
  `Resync/ReasonMissingCursor`.
- Final ID less than last returns `Discard/ReasonStale`.
- Final ID equal to last returns `Discard/ReasonDuplicate`.
- Newer final ID applies.

Update classification:

- Missing update IDs, zero first ID, final ID before first ID, or unsynced state
  returns `Resync/ReasonMissingCursor`.
- Final ID less than or equal to last discards as stale or duplicate.
- A range that contains `last + 1` applies, including overlapping ranges.
- A range that starts after `last + 1` returns `Resync/ReasonGap`.

Accepting a snapshot or update sets `last` to `FinalUpdateID` and marks the
policy synced. Invalidation clears only `synced`.

### Stats

`Stats` separates applied record counts from synchronization diagnostics:

- Applied, snapshots, deltas, deletes, and absent deletes count accepted
  mutations.
- Discarded counts stale and duplicate records dropped before mutation.
- Invalidated counts emitted invalidation events.
- Stale, duplicates, gaps, crossed, and snapshot requests classify replay
  health.
- Ignored while desynced counts deltas skipped before synchronization.
- Last accepted and highest seen timestamps make end-state diagnostics explicit.

## `feed/gencsv`

`Config` controls deterministic generation: exchange, symbol, start timestamp,
timestamp step, incremental count, initial levels, maximum levels, snapshot
interval, and RNG seed.

`Write(io.Writer, Config)` writes CSV:

1. Validate config.
2. Write the fixed CSV header.
3. Build an internal simulated book with deterministic initial levels.
4. Write an initial snapshot.
5. For each configured incremental index, either write a periodic snapshot or a
   generated incremental row.
6. Flush every million rows and at the end.

`Generator` exposes the same deterministic source as in-memory `feed.Record`
values. `Next()` returns the initial snapshot at index zero, then
`Incrementals` later records. It populates update-ID cursor fields with a simple
monotonic sequence so non-CSV tests can exercise update-ID policies.

The generator maintains bid and ask maps plus price slices and index maps for
O(1) random level removal. It avoids crossing by selecting new bid prices below
best bid and new ask prices above best ask, and it panics if an internal bug
would cross the simulated book. Public config validation prevents normal
callers from constructing invalid generator state.

## `internal/ring`

`SPSC[T]` is a bounded power-of-two single-producer/single-consumer ring.

Construction:

- Capacity must be a power of two and at least two.
- Invalid capacity returns `ErrInvalidCapacity`.
- The mask is `capacity - 1`.

Producer-owned state:

- `seq` is the next slot to publish.
- `cachedConsumed` avoids loading the consumer cursor on every publish.
- `published` is atomic and visible to the consumer.

Consumer-owned state:

- `seq` is the next slot to consume.
- `cachedPublished` avoids loading the producer cursor on every consume.
- `consumed` is atomic and visible to the producer.

The producer writes the buffer slot before storing the new published cursor. The
consumer reads the published cursor before reading the slot. The single-owner
cursor fields are intentionally non-atomic; using more than one producer or more
than one consumer is outside the contract and creates races.

`TryPublish` and `TryConsume` are non-blocking. `Publish` and `ConsumeWait` loop
until success, close, or context cancellation, spinning for the configured count
before yielding with `runtime.Gosched`.

`Close` is idempotent. Consumers drain already-published items after close and
then return `ok=false`. Slots are not zeroed after consumption, so pointer-heavy
payloads would stay referenced until overwritten; current uses pass value types.

## `internal/logx`

The logger is an asynchronous single-producer/single-consumer pipeline.

`New(Config)` applies defaults, creates an SPSC ring, and chooses a sink:

- stdout
- file created with `os.Create`
- discard

Invalid paths, invalid enum values, negative spin or flush interval, and invalid
ring capacities return errors.

`Log(ctx, Record)` is the producer side:

- If the ring is closed, return `ring.ErrClosed`.
- If publish succeeds immediately, update enqueue and depth metrics.
- In drop mode, full rings increment `Dropped` and return nil.
- In lossless mode, full rings wait through `Publish` and increment wait
  metrics.

`Run(ctx)` is the consumer side:

- Consume records, serialize them into one line, and write to a buffered writer.
- Flush when buffered bytes exceed the configured batch threshold.
- Optionally flush on a ticker.
- On close, drain remaining records before finishing.
- On write, flush, close, or context errors, return joined errors from cleanup.

The logger assumes the command pipeline's single producer. Treating `Log` as
multi-producer would violate the underlying ring contract.

## `internal/strategy`

The strategy package adapts replay events to command-side consumers.

`Strategy` has one method:

```text
OnEvent(replay.Event, recvNS)
```

`RunWithSpin(ctx, ring, strategy, clock, spin)` consumes events from an SPSC
ring, stamps receive time from the clock, calls `OnEvent`, checks an optional
`Err() error` method after each event, and calls an optional `Close() error`
method when the loop exits.

Exit paths:

- Nil dependencies return an error.
- Context cancellation from `ConsumeWait` returns the context error.
- Closed and drained input returns nil, plus any strategy close error.
- Strategy error returns that error, plus any close error.

`NopStrategy` records latency histograms. `LogStrategy` records latency, tracks
whether the last event was actionable, and forwards log records to `logx`.

## `internal/clock`

`Real` wraps a process-relative monotonic origin and sleeps with `time.Timer`.
It treats nil context as `context.Background`.

`Sim` is a deterministic clock for tests. It stores `now` behind a mutex and
uses a wake channel that is closed and replaced on each positive advance.
Sleepers loop until `now >= target`, wake on clock advance, or return context
errors.

`Advance` ignores non-positive durations. This prevents accidental backward
time movement in tests.

## `internal/metrics` And `internal/bench`

`internal/metrics.Histogram` is a lightweight non-concurrent histogram with
log-like buckets. It tracks count, min, max, and percentile estimates. `P(q)` is
an alias for `Percentile(q)`. `Rate(n, duration)` returns zero for non-positive
durations.

`internal/bench.Hist` is a benchmark-facing equivalent used by command
diagnostics and strategy latency tracking.

These types are intended for single-consumer metrics paths. Callers needing
concurrent recording should wrap them externally or replace them with a
concurrent metrics backend.

## Command Design

### `cmd/replay`

The replay command parses flags, validates stream identity, opens the CSV file,
configures logger and event rings, starts logger and strategy goroutines, and
then runs the public replay loop.

Important safety paths:

- Invalid replay mode, timestamp unit, sync policy, timestamp mode, speed, spin,
  and ring sizes fail before replay starts.
- `-sync-policy update-id` fails because the CSV schema lacks update-ID fields.
- Log file output is checked against the input CSV path before logger creation.
- Existing-file identity is checked with `os.SameFile` to catch symlink or hard
  link overwrite risks.

Shutdown flow:

1. `replay.Run` returns success or error.
2. Event ring is closed.
3. Strategy drains events and closes the logger.
4. Logger drains log records and flushes the sink.
5. Errors from replay, strategy, and logger are joined.
6. Successful runs print stats and final BBO to stderr.

### `cmd/gencsv`

The generator command parses flags into `gencsv.Config`, creates the output
file, calls `gencsv.Write`, syncs the file, stats it, and prints row and byte
counts. Rows are reported as `Incrementals + 1`, because the initial snapshot is
always emitted.

### `cmd/bench`

The benchmark command assembles fixture, generated-feed, replay, policy,
backpressure, and pacing workloads. It uses public packages where possible and
internal components where measuring the command pipeline itself is the goal.

## Data Flow Summary

Snapshot data flow:

```text
CSV JSON arrays
  -> []book.Level
  -> book.Snapshot
  -> replay policy snapshot decision
  -> book.ApplySnapshot
  -> atomic BBO cache
  -> replay.Event
```

Incremental data flow:

```text
CSV side, price, size
  -> feed.Record delta fields
  -> replay policy update decision
  -> book.ApplyDelta
  -> sideBook map and heap update
  -> atomic BBO cache
  -> replay.Event
```

Command event data flow:

```text
replay.Event
  -> event SPSC ring
  -> strategy receive timestamp
  -> latency histograms
  -> logx.Record
  -> logger SPSC ring
  -> buffered sink
```

## Race, Deadlock, And Memory Notes

- `Book` avoids internal locks on mutation. Correctness relies on external
  single-writer serialization.
- `quoteCache.load` can spin briefly if a reader observes an in-progress write.
  With one writer, the sequence returns to even quickly. A stalled writer inside
  `store` would make concurrent readers spin until that goroutine resumes.
- `replay.Run` does not start goroutines. Deadlocks in application use usually
  come from a blocking handler whose queue is never drained.
- Internal SPSC rings do not use condition variables. Blocked publish and
  consume paths poll close and context state, spin for a bounded count, then
  yield.
- `cmd/replay` prevents normal shutdown deadlock by closing the event ring after
  replay returns, letting strategy drain, then letting strategy close the logger
  so logger can drain.
- Logger drop mode can lose records by design; lossless mode applies
  backpressure to the producer.
- Consumed ring slots retain their previous values until overwritten.
- Depth snapshots allocate and sort; BBO snapshots do not allocate.
- Histograms are not concurrent-safe.

## Extension Points

Expected extension points are public interfaces and structs:

- Add new replay synchronization policies by implementing `replay.Policy`.
- Add replay consumers by implementing `replay.Handler`.
- Add snapshot request behavior by implementing `replay.SnapshotRequester`.
- Add deterministic fixture scenarios through `feed/gencsv.Config` or
  `Generator`.
- Add alternate command-side strategies by using the public replay callback or
  adapting the internal strategy pattern outside the module.

Likely future library work:

- Introduce a public record-source interface so replay is not tied directly to
  `*feed.Decoder`.
- Add a feed decoder that populates update-ID ranges.
- Consider public transport primitives only if there is a clear use case beyond
  this module's command pipeline.
- Add optional zeroing or pointer-retention controls if pointer payloads become
  common in internal queues.
