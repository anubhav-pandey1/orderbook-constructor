# Order Book System — Implementation Specification

Companion to `DESIGN.md`, which owns scope and rationale. This document defines the Go APIs, synchronization seam, ring-buffer protocol, invariants, package layout, benchmark workloads, and correctness/race test matrix. Signatures are contracts; bodies and field layout sketches are illustrative.

## 0. Topology

```text
main (cmd/replay)
 ├─ goroutine P  producer : decode -> sync classify -> apply -> publish BBO
 ├─ goroutine S  strategy : consume BBO -> record latency -> emit log record
 └─ goroutine L  logger   : consume log record -> format -> batch -> sink

eventRing = ring.SPSC[pipeline.Event]       P -> S
logRing   = ring.SPSC[logx.Record]          S -> L
```

The producer is the only book writer. Values crossing goroutine boundaries are copied into preallocated SPSC slots. Direct concurrent BBO queries use the book's atomic quote cache; mutable level maps and heaps never cross a goroutine boundary.

The mutation sequence is:

```text
decode
  -> SyncPolicy.Classify
       -> Discard: count and continue
       -> Resync: invalidate, publish BookInvalidated, request snapshot
       -> Apply: mutate private book state
  -> require bestBid < bestAsk
  -> SyncPolicy.Accept
  -> publish immutable event
```

The policy cursor advances only after a valid book commit.

## 1. Go APIs

### 1.1 `internal/price`

```go
package price

type Ticks int64 // price in cents
type Qty int64   // size in 0.0001 base-asset units

const (
    PriceFracDigits = 2
    QtyFracDigits   = 4
)

// Exact decimal text to scaled int64. No float conversion.
func ParsePrice(s string) (Ticks, error)
func ParseQty(s string) (Qty, error)

// Append exact fixed-point text to dst without converting through float64.
func (t Ticks) AppendText(dst []byte) []byte
func (q Qty) AppendText(dst []byte) []byte
```

The parser rejects empty input, signs, exponent notation, non-digits, excess fractional digits, and scaled `int64` overflow. Fewer fractional digits are right-padded with zeroes.

`encoding/csv.Reader` returns strings, so the API accepts strings. A custom byte-oriented CSV decoder is permitted only if a parsing benchmark demonstrates that it materially improves the end-to-end path.

### 1.2 `internal/book`

```go
package book

type Side uint8

const (
    Bid Side = iota + 1
    Ask
)

type BBO struct {
    BidPx  price.Ticks
    AskPx  price.Ticks
    BidQty price.Qty
    AskQty price.Qty
    BidOK  bool
    AskOK  bool
    Version uint64
}

type Level struct {
    Px  price.Ticks
    Qty price.Qty
}

type Snapshot struct {
    Bids []Level
    Asks []Level
}

type DeltaKind uint8

const (
    LevelInserted DeltaKind = iota + 1
    LevelUpdated
    LevelDeleted
    AbsentDelete
)

type DeltaResult struct {
    BBO  BBO
    Kind DeltaKind
}

var (
    ErrCrossedSnapshot = errors.New("crossed snapshot")
    ErrCrossedDelta    = errors.New("delta crossed book")
    ErrEmptySnapshot   = errors.New("snapshot has no levels")
)

type Book struct { /* writer-owned maps/heaps + atomic quote cache */ }

func New(capHint int) *Book

// Hot path. qty==0 deletes. Deleting an absent level is an accepted no-op.
// A crossed result is never committed to the quote cache or version. The
// result classifies the operation without requiring another map lookup.
func (b *Book) ApplyDelta(side Side, px price.Ticks, qty price.Qty) (DeltaResult, error)

// Builds both sides off-book, validates them, and swaps the complete state.
// Both sides empty is invalid. One empty side is valid and non-actionable on
// that side. Two populated sides require bestBid < bestAsk.
func (b *Book) ApplySnapshot(sn *Snapshot) (BBO, error)

// Clears maps, heaps, and actionable atomic BBO fields. It preserves the last
// accepted Version and does not increment it.
func (b *Book) Invalidate()

// Race-free, internally sequence-guarded atomic read.
func (b *Book) BBOSnapshot() BBO

// Returns the last accepted mutation version from the atomic quote cache.
func (b *Book) Version() uint64

// Writer-owned or quiesced access only.
func (b *Book) DepthSnapshot() Depth
```

Internal per-side state uses generation-tagged heap membership:

```go
type level struct {
    qty        price.Qty
    generation uint64
}

type heapEntry struct {
    px         price.Ticks
    generation uint64
}

type side struct {
    levels  map[price.Ticks]level
    heap    priceHeap
    nextGen uint64
    stale   int
}
```

An entry is current only when both price and generation match the authoritative map level. This prevents an old heap entry from becoming valid again after delete/reinsert at the same price.

On a tentative crossed delta, `ApplyDelta` leaves the atomic BBO and version unchanged and returns `ErrCrossedDelta`. The producer immediately calls `Invalidate`, transitions synchronization state, and publishes only `BookInvalidated`. No actionable crossed event is emitted.

### 1.3 `internal/book` atomic quote cache

```go
type quoteCache struct {
    sequence atomic.Uint64
    version  atomic.Uint64
    bidPx    atomic.Int64
    bidQty   atomic.Int64
    askPx    atomic.Int64
    askQty   atomic.Int64
    flags    atomic.Uint32
}
```

Writer:

1. Increment `sequence` to odd.
2. Store every field atomically.
3. Increment `sequence` to even.

Reader:

1. Load `sequence`; retry if odd.
2. Load all fields.
3. Load `sequence` again.
4. Accept only equal, even sequence values.

All shared fields are atomic, satisfying the race detector; the sequence makes the multi-field view coherent.

### 1.4 `internal/syncx`

```go
package syncx

type State uint8

const (
    Uninitialized State = iota
    Synchronized
    Desynchronized
)

type Action uint8

const (
    Apply Action = iota + 1
    Discard
    Resync
)

type Reason uint8

const (
    ReasonNone Reason = iota
    ReasonStale
    ReasonDuplicate
    ReasonGap
    ReasonMissingCursor
    ReasonCrossed
    ReasonInvalidSnapshot
)

type Cursor struct {
    Timestamp     int64
    FirstUpdateID uint64
    FinalUpdateID uint64
    HasUpdateID   bool
}

type Decision struct {
    Action Action
    Reason Reason
}

type Policy interface {
    ClassifySnapshot(Cursor) Decision
    ClassifyUpdate(Cursor) Decision
    AcceptSnapshot(Cursor)
    AcceptUpdate(Cursor)
    Invalidate()
}

type TimestampMode uint8

const (
    TimestampStep TimestampMode = iota + 1
    TimestampMonotonic
)

func NewTimestampPolicy(mode TimestampMode, step int64) Policy
func NewUpdateIDPolicy() Policy
func NewArrivalOrderPolicy() Policy
```

Classification is side-effect free and allocation-free. `Accept*` advances the cursor only after book acceptance. `Invalidate` disables incremental continuity but retains the last accepted watermark so stale recovery snapshots remain discardable.

`ClassifySnapshot` discards a snapshot whose authoritative cursor is not newer than that retained watermark and applies a newer valid snapshot regardless of the broken incremental step that caused resynchronization. Timestamp policy compares snapshot timestamps; update-ID policy compares snapshot `lastUpdateID`. Accepting the snapshot re-anchors incremental continuity.

Timestamp step policy:

```text
received <= last                 -> Discard
received == last + configuredStep -> Apply
other newer timestamp            -> Resync
```

Timestamp monotonic policy accepts any strictly newer timestamp. Arrival-order policy always applies valid decoded events and provides no gap detection.

Update-ID policy, where `next = lastFinalUpdateID + 1`:

```text
final <= last                 -> Discard
first <= next && final >= next -> Apply
first > next                  -> Resync
missing/malformed IDs         -> Resync
```

### 1.5 `internal/ring`

```go
package ring

var (
    ErrClosed          = errors.New("ring closed")
    ErrInvalidCapacity = errors.New("capacity must be a power of two >= 2")
)

type SPSC[T any] struct { /* section 2 */ }

func NewSPSC[T any](capacity int) (*SPSC[T], error)

// Producer side; exactly one goroutine may call these methods.
func (r *SPSC[T]) TryPublish(v T) bool
func (r *SPSC[T]) Publish(ctx context.Context, v T, spin int) error
func (r *SPSC[T]) Close() error

// Consumer side; exactly one goroutine may call these methods.
func (r *SPSC[T]) TryConsume() (T, bool)
func (r *SPSC[T]) ConsumeWait(ctx context.Context, spin int) (T, bool, error)

func (r *SPSC[T]) Len() int // approximate metrics only
func (r *SPSC[T]) Closed() bool
```

Capacity is rejected rather than silently rounded. `Close` is producer-owned and idempotent. Publishing after close returns `ErrClosed`. Closing never discards buffered values: the consumer receives `ok=false` only when closed and fully drained.

The hot event and log-record types are pointer-free values. The generic ring does not zero a consumed slot; callers must not use it for long-lived pointer-bearing values without accounting for retention.

### 1.6 `internal/feed`

```go
package feed

type Kind uint8

const (
    KindSnapshot Kind = iota + 1
    KindDelta
)

type StreamID struct {
    Exchange string // canonical lowercase, for example "binance"
    Symbol   string // canonical separator-free uppercase, for example "BTCUSDT"
}

// NormalizeStreamID trims surrounding whitespace, lowercases the exchange,
// uppercases the symbol, removes '/', '-', and '_' from the symbol, and rejects
// an empty component.
func NormalizeStreamID(exchange, symbol string) (StreamID, error)

type Record struct {
    Kind   Kind
    Stream StreamID // decoded and normalized from the CSV exchange/symbol fields
    TS     int64

    Side book.Side
    Px   price.Ticks
    Qty  price.Qty

    Snap *book.Snapshot

    FirstUpdateID uint64
    FinalUpdateID uint64
    HasUpdateID   bool
}

type Decoder struct { /* encoding/csv.Reader, CRLF-safe */ }

func NewDecoder(r io.Reader) *Decoder
func (d *Decoder) Next() (Record, error) // io.EOF at end

type Stats struct {
    Applied      uint64
    Discarded    uint64
    Invalidated  uint64
    Snapshots    uint64
    Deltas       uint64
    Deletes      uint64
    AbsentDeletes uint64

    Stale        uint64
    Duplicates   uint64
    Gaps         uint64
    Crossed      uint64
    SnapshotRequests uint64
    IgnoredWhileDesynced uint64

    LastAcceptedTS int64
    HighestSeenTS  int64
}

type ReplayMode uint8

const (
    Fast ReplayMode = iota + 1
    Paced
)

type ReplayCfg struct {
    Mode      ReplayMode
    Speed     float64
    TSUnit    time.Duration
    SpinIters int
    Stream    StreamID // required normalized identity; every record must match
}

type SnapshotRequester interface {
    RequestSnapshot(context.Context, ResyncRequest) error
}

type ResyncRequest struct {
    Exchange string
    Symbol   string
    Last     syncx.Cursor
    Received syncx.Cursor
    Reason   syncx.Reason
}

func Replay(
    ctx context.Context,
    dec *Decoder,
    bk *book.Book,
    policy syncx.Policy,
    requester SnapshotRequester,
    out *ring.SPSC[pipeline.Event],
    cfg ReplayCfg,
    clk clock.Clock,
) (Stats, error)
```

`Replay` owns synchronization state and notification IDs. It publishes one event per accepted record, no event for stale/discarded records, and one `BookInvalidated` event per transition from `Synchronized` to `Desynchronized`.

The decoder normalizes the exchange and symbol on every row. `Replay` rejects a stream-identity mismatch before synchronization classification or book mutation and uses `ReplayCfg.Stream` as the retained identity for every `ResyncRequest`. Thus a resynchronization request never depends on a discarded record or on mutable decoder storage. For the fixture, raw `binance,BTC/USDT` normalizes to `binance,BTCUSDT`.

The CSV requester scans forward for the next snapshot. If EOF occurs while uninitialized or desynchronized, replay returns a typed snapshot-required error. A future live requester may fetch a REST snapshot with bounded retry while the book remains non-actionable.

`IngressNS` is the local monotonic timestamp at which a decoded record becomes available to the synchronization/application pipeline. Fast replay stamps it immediately before classification. Paced replay first maps the raw source time onto the local monotonic replay timeline, waits for that due time, and then stamps `IngressNS`:

```text
DueNS = replayStartNS + (EventTS - firstEventTS) * TSUnit / Speed
```

`DueNS` is zero in fast mode. In paced mode, `IngressNS - DueNS` is scheduler lateness; `receiveNS - DueNS` is scheduled-source-to-callback latency. Neither calculation subtracts a historical Unix timestamp from a process-relative timestamp.

### 1.7 `internal/pipeline`

```go
package pipeline

type EventKind uint8

const (
    SnapshotApplied EventKind = iota + 1
    IncrementalApplied
    BookInvalidated
)

type Event struct {
    NotificationID uint64
    Version        uint64
    SyncEpoch      uint64
    Kind           EventKind
    State          syncx.State
    Reason         syncx.Reason

    BidPx  price.Ticks
    AskPx  price.Ticks
    BidQty price.Qty
    AskQty price.Qty
    BidOK  bool
    AskOK  bool

    EventTS   int64 // raw source timestamp; metadata and pacing input only
    DueNS     int64 // paced local monotonic target; zero in fast mode
    IngressNS int64 // monotonic process-relative
    ApplyNS   int64 // monotonic process-relative
}
```

`Event` is immutable after publication. `BookInvalidated` has no actionable BBO. `Version` counts accepted book mutations; `NotificationID` counts every publication, including invalidation. `SyncEpoch` increments whenever a valid snapshot establishes a new authoritative state.

The implementation reports `unsafe.Sizeof(pipeline.Event{})` and `unsafe.Sizeof(logx.Record{})` in benchmark metadata. Documentation must not assume a 24-byte event; the sketched event is expected to be substantially larger after alignment.

### 1.8 `internal/strategy`

```go
package strategy

type Strategy interface {
    OnEvent(pipeline.Event, receiveNS int64)
}

type LogStrategy struct { /* logger + latency histograms */ }
func (s *LogStrategy) OnEvent(pipeline.Event, receiveNS int64)

type NopStrategy struct { /* latency histograms only */ }
func (s *NopStrategy) OnEvent(pipeline.Event, receiveNS int64)

func Run(
    ctx context.Context,
    in *ring.SPSC[pipeline.Event],
    strategy Strategy,
    clk clock.Clock,
) error
```

`Run` samples `receiveNS = clk.NowNS()` immediately after a successful ring consume and before callback or logging work, then passes that value to `OnEvent`. The primary assignment latency is `receiveNS - Event.IngressNS`: local monotonic update availability to strategy callback receipt. The strategy also records `receiveNS - Event.ApplyNS`. In paced mode it additionally records `receiveNS - Event.DueNS` and scheduler lateness `Event.IngressNS - Event.DueNS`; those scheduled metrics are not emitted in fast mode.

On every `SnapshotApplied` and `IncrementalApplied` event, the strategy reads and logs `BidPx`, `AskPx`, `BidQty`, `AskQty`, `BidOK`, and `AskOK` from the event. These fields are the version-consistent BBO that triggered the notification; the strategy does not re-query the live book on its hot path. A test may compare the event with `Book.BBOSnapshot()` only when the cache version equals the event version, because the single writer may legitimately have advanced the cache before the strategy consumes an older event.

The strategy must clear or disable actionable strategy state immediately on `BookInvalidated`.

### 1.9 `internal/logx`

```go
package logx

type Record struct {
    NotificationID uint64
    Version        uint64
    SyncEpoch      uint64
    Kind           pipeline.EventKind
    State          syncx.State
    Reason         syncx.Reason

    BidPx  price.Ticks
    AskPx  price.Ticks
    BidQty price.Qty
    AskQty price.Qty
    BidOK  bool
    AskOK  bool

    EventTS   int64
    DueNS     int64
    IngressNS int64
    ApplyNS   int64
    RecvNS    int64
}

type Sink uint8

const (
    SinkStdout Sink = iota + 1
    SinkFile
    SinkDiscard
)

type Delivery uint8

const (
    Lossless Delivery = iota + 1
    DropWhenFull
)

type Config struct {
    Sink          Sink
    File          string
    Delivery      Delivery
    RingSize      int
    SpinIters     int
    BatchBytes    int
    FlushInterval time.Duration
}

type Metrics struct {
    Enqueued  uint64
    Written   uint64
    Dropped   uint64
    MaxDepth  uint64
    WaitCount uint64
}

type Logger struct { /* ring, writer, sink, metrics */ }

func New(cfg Config) (*Logger, error)
func (l *Logger) Log(context.Context, Record) error
func (l *Logger) Run(context.Context) error
func (l *Logger) Close() error
func (l *Logger) Metrics() Metrics
```

Lossless is the assignment default. Drop mode must be explicitly enabled and must expose a nonzero dropped counter; it never applies to the book-to-strategy event ring. `LogStrategy` copies the event timing fields and the callback's exact `receiveNS` sample into the fixed-size log record; histogram recording happens before enqueueing, so logger backpressure is not charged to callback-receipt latency. Formatting uses `price.AppendText` methods and occurs only in the logger goroutine. `Close` is called by the strategy/log producer to close the internal log ring; `Run` owns final drain, sink flush, and sink close.

### 1.10 `internal/bench`

```go
package bench

// Fixed-size log-linear nanosecond buckets. Record allocates nothing.
// The concrete range/resolution and overflow bucket are constants and tested.
type Hist struct { /* fixed arrays + count + max */ }

func NewHist() *Hist
func (h *Hist) Record(ns int64)
func (h *Hist) P(q float64) int64
func (h *Hist) Max() int64
func (h *Hist) Line(name string) string

type Throughput struct {
    N   uint64
    Dur time.Duration
}

func (t Throughput) PerSec() float64
```

Only the owning strategy goroutine mutates a histogram. Cross-goroutine aggregation happens after join, so histogram buckets require no atomics.

### 1.11 `internal/clock`

```go
package clock

type Clock interface {
    NowNS() int64
    SleepUntilNS(context.Context, int64) error
}

type Real struct {
    origin time.Time
}

func NewReal() *Real

// NowNS is monotonic process-relative time, not Unix wall-clock time.
func (r *Real) NowNS() int64 {
    return time.Since(r.origin).Nanoseconds()
}

type Sim struct { /* mutex-protected controllable monotonic time */ }
func NewSim(startNS int64) *Sim
func (s *Sim) Advance(time.Duration)
```

Exchange time remains separately available in `EventTS`. It is used only as source metadata and to derive the paced replay target relative to `firstEventTS`. Latency never subtracts a historical exchange timestamp directly from a local clock, and no `time.Time` is copied into the hot event solely for latency measurement.

### 1.12 CLI flags (`cmd/replay`)

```text
-csv string                    default ./btc_orderbook_updates.csv
-exchange string               default binance; normalized before replay
-symbol string                 default BTCUSDT; normalized before replay
-replay fast|paced             default fast
-speed float                   default 1; paced only
-timestamp-unit auto|ns|us|ms  default auto
-sync-policy timestamp|update-id|off  default timestamp
-timestamp-mode step|monotonic default step
-timestamp-step duration       default 100ms
-log stdout|file|discard       default stdout
-log-file string
-log-delivery lossless|drop    default lossless
-event-ring int                default 65536; must be power of two
-log-ring int                  default 65536; must be power of two
-spin int                      default 128
-gomaxprocs int                default runtime default
```

Selecting update-ID policy for a CSV without update-ID fields is a startup/configuration error. Benchmark commands explicitly set their synchronization and logging modes rather than relying on defaults.

## 2. SPSC ring-buffer memory protocol

The ring is a bounded Lamport-style SPSC queue with persistent cached cursors and separated producer/consumer cursor addresses.

```go
// Conservative separation for common 64-byte lines and Apple Silicon's
// wider cache-line behavior. This is a performance layout assumption, not a
// Go language guarantee.
const cacheLinePad = 128

type producerState struct {
    published      atomic.Uint64 // read by consumer only when locally empty
    seq            uint64        // producer-owned
    cachedConsumed uint64        // producer-owned
    _              [104]byte     // make cursor block 128 bytes
}

type consumerState struct {
    consumed       atomic.Uint64 // read by producer only when locally full
    seq            uint64        // consumer-owned
    cachedPublished uint64       // consumer-owned
    _              [104]byte
}

type SPSC[T any] struct {
    buf  []T
    mask uint64

    producer producerState
    consumer consumerState

    closed atomic.Bool
}
```

The two shared cursor atomics are at least 128 bytes apart, so they cannot occupy the same common 64-byte or 128-byte cache line. Go does not promise cache-line alignment for the containing allocation; all cache-layout claims remain benchmarked target assumptions.

### 2.1 Publish

Producer only:

```text
i = producer.seq

if i - producer.cachedConsumed == capacity:
    producer.cachedConsumed = consumer.consumed.Load()
    if still full:
        TryPublish returns false
        Publish spins, then Gosched, then retries or observes cancellation/close

buf[i & mask] = value
producer.published.Store(i + 1)
producer.seq = i + 1
```

The slot write occurs before the atomic publication store.

### 2.2 Consume

Consumer only:

```text
i = consumer.seq

if i == consumer.cachedPublished:
    consumer.cachedPublished = producer.published.Load()
    if still empty:
        if closed and i == producer.published.Load(): return drained
        TryConsume returns empty
        ConsumeWait spins, then Gosched, then retries or observes cancellation

value = buf[i & mask]
consumer.consumed.Store(i + 1)
consumer.seq = i + 1
```

The publication load synchronizes with the producer store before the slot read. The consumed store releases the slot before reuse.

### 2.3 Close and drain

- Only the producer closes the ring.
- `Close` sets `closed` once and rejects future publication.
- Already published values remain consumable.
- Consumer completion means `closed && consumed == published`.
- Cancellation may stop a caller before drain; normal lifecycle never cancels consumers until upstream rings are drained.
- A producer blocked on full observes context cancellation or close and returns.

### 2.4 Guarantees

- FIFO and lossless in lossless mode.
- `0 <= published-consumed <= capacity` modulo practical `uint64` lifetime.
- No slot overwrite before consumption.
- No slot read before publication.
- No hot-path allocation after construction.
- Cross-core cursor loads occur only when the local cached view reports full/empty.
- Ring depth is approximate and metrics-only.

## 3. State invariants

### 3.1 Book

- **B1:** Every authoritative map level has quantity greater than zero.
- **B2:** No negative price or quantity enters committed state.
- **B3:** The BBO equals the extreme active map key and corresponding quantity; side availability is false iff that side is empty.
- **B4:** Every active map level has at least one generation-matching heap entry.
- **B5:** After stale cleanup, the heap root and cached best describe the same active generation.
- **B6:** If both sides are populated, every committed actionable state satisfies `bestBid < bestAsk`.
- **B7:** A crossed tentative update is never written to the atomic quote cache and triggers invalidation/resync.
- **B8:** Version increases exactly once per accepted snapshot or delta, including absent-level deletion; discarded and invalidating inputs do not increment it.

### 3.2 Heap

- **H1:** Heap order is max-price for bids and min-price for asks.
- **H2:** A heap entry is stale if its price is absent or its generation differs from the map generation.
- **H3:** Rebuild occurs when `len(heap) > 2*len(levels)+64`, unless a benchmark-supported constant replaces it.
- **H4:** Immediately after rebuild, the heap contains exactly one current entry per active level.

### 3.3 Synchronization

- **Y1:** Incrementals mutate the book only in `Synchronized` state and only after policy classification `Apply`.
- **Y2:** Policy cursors advance only after a valid book commit.
- **Y3:** `Discard` causes no book mutation, version, or publication.
- **Y4:** A transition to `Desynchronized` clears the book and atomic BBO before publishing `BookInvalidated`.
- **Y5:** Only a valid authoritative snapshot newer than the retained accepted watermark can enter `Synchronized` from another state.
- **Y6:** At most one invalidation event and snapshot request is emitted for a continuous desynchronized interval.

### 3.4 Ring

- **R1:** `0 <= published-consumed <= capacity`.
- **R2:** Consumer reads slot `i` only after `published > i`.
- **R3:** Producer writes a reused slot only after the prior occupant is consumed.
- **R4:** Close preserves buffered FIFO values and completion occurs only after drain.

### 3.5 Snapshot

- **S1:** Both new sides are completely built and validated before replacing live state.
- **S2:** Both sides empty is invalid; one empty side is valid; two populated sides must be uncrossed.
- **S3:** A crossed or invalid recovery snapshot cannot make the book actionable.

### 3.6 Pipeline

- **P1:** One applied event is published per accepted record.
- **P2:** Stale/discarded records publish nothing.
- **P3:** One non-actionable invalidation event is published per transition to `Desynchronized`.
- **P4:** `NotificationID` increases once per publication; `Version` increases once per accepted mutation.
- **P5:** Event and log records preserve publication order.
- **P6:** Normal shutdown closes and drains P -> S -> L, flushes logs, and leaks no goroutine.
- **P7:** `IngressNS`, `ApplyNS`, and callback `receiveNS` share one local monotonic clock domain with `IngressNS <= ApplyNS <= receiveNS`; in paced mode `DueNS` is in that domain and `DueNS <= IngressNS` after the scheduled wait returns.
- **P8:** Every decoded record matches the configured normalized stream identity before classification; every resynchronization request uses that retained configured identity.
- **P9:** A log record copies `EventTS`, `DueNS`, `IngressNS`, `ApplyNS`, and the callback's exact `receiveNS` without recomputing or changing clock domains.

## 4. Package layout

```text
orderbook/
├── go.mod
├── Makefile
├── README.md
├── DESIGN.md
├── SPEC.md
├── BENCHMARK.md
├── btc_orderbook_updates.csv
├── cmd/
│   ├── replay/main.go
│   └── bench/main.go
├── internal/
│   ├── price/     ticks.go ticks_test.go
│   ├── book/      book.go side.go heap.go snapshot.go quote_cache.go invariants.go
│   ├── syncx/     policy.go timestamp.go update_id.go policy_test.go
│   ├── ring/      spsc.go spsc_test.go
│   ├── feed/      decode.go replay.go requester.go decode_test.go replay_test.go
│   ├── pipeline/  event.go
│   ├── strategy/  strategy.go
│   ├── logx/      logger.go record.go logger_test.go
│   ├── bench/     hist.go throughput.go report.go
│   ├── clock/     clock.go real.go sim.go
│   └── synth/     generator.go generator_test.go
├── testdata/
│   ├── crlf.csv
│   ├── unsorted_snapshot.csv
│   ├── timestamp_gap.csv
│   ├── timestamp_stale.csv
│   ├── crossed.csv
│   └── resync.csv
```

Constraint: standard library only. This keeps `make` dependency-free and makes the measured ring, histogram, parsing, and book behavior self-contained.

### 4.1 Makefile targets

| Target | Contract |
|---|---|
| `make build` | Compile both `cmd/replay` and `cmd/bench`. |
| `make run` | Replay `btc_orderbook_updates.csv` in fast mode with normalized stream `binance,BTCUSDT`, timestamp-step synchronization at `100ms`, lossless delivery, and BBO logging to stdout. |
| `make benchmark` | Run the Go microbenchmarks and end-to-end benchmark runner, then write the canonical submission report to `BENCHMARK.md`. Benchmark logging uses the discard sink. |
| `make test` | Run `go test -race ./...`; recommended in addition to the assignment-required targets. |

The normal `make run` is fast so an evaluator does not wait approximately 224 seconds for a 1x replay of the fixture. Paced 1x behavior remains available through `-replay paced -speed 1` and may have an additional convenience target, but it is not the required default run.

### 4.2 README contract

`README.md` is a required submission artifact and contains:

- Prerequisites, supported Go version, and exact commands for `make build`, `make run`, `make benchmark`, and `make test`.
- Default input and normalized identity: raw fixture values `binance,BTC/USDT` normalize to `binance,BTCUSDT`.
- The important replay, synchronization, ring-capacity, and logger flags.
- The fixture assumptions from `DESIGN.md`, including the fixture-specific 100 ms timestamp step and the fact that production Binance recovery requires update IDs.
- A plain-language design summary covering the single writer, fixed-point representation, map plus generation-tagged heaps, atomic BBO cache, and lossless SPSC delivery.
- A link to the canonical `BENCHMARK.md` and the commands used to reproduce it.

The README pros/cons section includes at least:

| Choice | Pros | Cons |
|---|---|---|
| Single writer plus SPSC rings | Deterministic order, no book lock on the mutation path, explicit latency boundary | A slow strategy applies lossless backpressure; one ring is not a multi-producer transport |
| Map plus generation-tagged heap and cached best | Existing-level replacements are average O(1); cached BBO reads are O(1); new prices are O(log n) | New-level insertion and stale-top repair cost O(log n); churn can trigger an occasional O(n) rebuild |
| Fixed-point integers | Exact keys and quantities with no floating-point drift | Parsing and formatting are more involved; configured precision is feed-specific |
| Version-consistent BBO in each event | Strategy observes the exact quote for every notification without a shared-state race | Value events are larger than a pointer or a version-only notification |
| Asynchronous batched logging | Formatting and sink I/O stay off the strategy callback path | Logs can trail the strategy and lossless mode can propagate sink backpressure |
| Timestamp-step synchronization | Detects stale rows and gaps in this verified fixture | It is not a production substitute for exchange update IDs |
| Lossless event delivery | Never silently drops a book update | A full ring stalls the upstream producer until the consumer catches up |

## 5. Benchmark workloads

The 2,242-event fixture is a correctness and representative-mix latency workload, not a stable headline throughput workload.

| ID | Workload | Purpose | Contract |
|---|---|---|---|
| **W1** | Fixture replay, fast | Correctness and realistic-mix latency | Full decode -> timestamp-step policy -> apply -> ring -> no-op strategy; primary latency is local update availability -> callback receipt; discard logger |
| **W2** | Apply-only microbenchmarks | Isolate book operations | Separate pre-sized cases for existing update, new level, absent delete, active delete, best delete, delete/reinsert, and rebuild |
| **W3** | Decode plus apply | Measure parser cost | Decode from in-memory CSV bytes; report parsing separately from disk I/O |
| **W4** | Full steady-state pipeline | Transport and strategy cost | Streaming synthetic deltas through P -> S -> L(discard), with no retained 10M-record slice |
| **W5** | Burst/backpressure | Prove lossless behavior | Slow strategy/logger deliberately; assert zero event loss and measure occupancy/waits |
| **W6a** | Synthetic delta-only | Headline steady-state throughput | Seeded streaming generator, approximately 10M operations, bounded active levels, controlled update/new/delete mix |
| **W6b** | Synthetic deltas plus snapshots | Snapshot disruption cost | Same seeded stream with periodic snapshots, reported separately from W6a |
| **W7** | Paced replay | Schedule fidelity | Fixture/synthetic stream under simulated clock; source timestamps are rebased onto the local monotonic timeline; report deterministic scheduler lateness and due-time -> callback latency |
| **W8** | Synchronization policy | Quantify seam cost | Run identical stream with timestamp-step and synchronization-off; update-ID policy on generated ID ranges |

The synthetic generator emits one record at a time from a deterministic PRNG. It does not allocate or retain a 10-million-record slice. Deletes select active levels, new levels remain inside a bounded uncrossed band, and snapshot generation is isolated to W6b.

Every report includes:

- CPU model, OS, architecture, and Go version.
- `GOMAXPROCS`, `GOGC`, and `GOMEMLIMIT`.
- Event/log ring capacities and actual record sizes.
- Synchronization policy and timestamp step.
- Logger sink/delivery mode.
- Fixture hash or synthetic seed and operation mix.
- Throughput in updates/second.
- Primary assignment latency, local monotonic update availability (`IngressNS`) to callback receipt, at p50/p90/p95/p99/p99.9/max.
- Apply-to-callback latency at p50/p90/p95/p99/p99.9/max.
- For paced replay only, locally rebased due-time-to-callback latency and scheduler lateness at p50/p90/p95/p99/p99.9/max.
- `ns/op`, `B/op`, and allocations/op for Go benchmarks.
- Ring maximum occupancy and producer wait counts.

Target: zero allocations per steady-state existing-level apply, ring publish/consume, strategy event handling, and histogram record. Snapshot building, CSV decoding, and formatting are reported separately and may allocate.

Raw `EventTS` is never subtracted from a local process-relative timestamp. The primary metric starts at `IngressNS`; paced scheduled metrics start at `DueNS`, which is derived from source-time offsets relative to the local replay start.

### 5.1 Canonical benchmark report (`BENCHMARK.md`)

`make benchmark` generates or updates the human-readable `BENCHMARK.md`. It is the single canonical benchmark artifact in the submission; optional raw command output is supporting data and must not be presented as a second benchmark report.

The report contains, at minimum:

1. Environment: CPU, OS, architecture, Go version, `GOMAXPROCS`, `GOGC`, `GOMEMLIMIT`, date or commit, ring capacities, and actual record sizes.
2. Reproduction: exact build and benchmark commands, fixture hash or synthetic seed, synchronization policy, timestamp unit/step, replay mode, and logger mode.
3. Throughput: core apply operations, decode plus apply, and full-pipeline updates/second; delta-only and periodic-snapshot results remain separate.
4. Primary assignment latency: local monotonic update availability to strategy callback receipt at p50/p90/p95/p99/p99.9/max.
5. Secondary processing latency: apply completion to callback receipt at the same percentiles.
6. Paced replay diagnostics: locally rebased due-time to callback and scheduler lateness at the same percentiles.
7. Allocation and queue data: `ns/op`, `B/op`, allocations/op, ring maximum occupancy, wait counts, and any explicitly enabled logger drops.

Markdown is plain text and needs no charts. Results must identify warmup/repetition methodology and must not claim hardware-independent thresholds.

## 6. Correctness and race test matrix

| ID | Area | Test | Assertion |
|---|---|---|---|
| **C1** | Price | Exact parse table | Price `99999.99 -> 9999999`; qty `0.0001 -> 1`; `1.5 -> 15000`; excess precision, sign, exponent, overflow reject |
| **C2** | Price | Fixed-point formatting | Exact text with required zero padding; no float artifacts; append path allocation-free |
| **C3** | Feed | CRLF and final-field parsing | No trailing carriage return; snapshot JSON and delta fields decode correctly |
| **C4** | Book | Unsorted snapshot | Correct full depth and BBO independent of snapshot order |
| **C5** | Book | Existing/new/delete/absent-delete | Correct map; absent delete succeeds and increments accepted version |
| **C6** | Book | Delete current best | Correct next-best after lazy stale cleanup |
| **C7** | Heap | Delete then reinsert same price | Old generation never becomes current; no duplicate-current ambiguity |
| **C8** | Heap | Rebuild threshold | Rebuild fires and leaves exactly one current entry per level |
| **C9** | Book | Second fixture snapshot | Previous depth fully replaced; sync epoch increments |
| **C10** | Sync | Timestamp step | Next step applies; duplicate/stale discards; unexpected newer step resyncs |
| **C11** | Sync | Timestamp monotonic/off | Monotonic accepts any newer timestamp; off trusts arrival order |
| **C12** | Sync | Update-ID ranges | Stale range discards; overlapping next range applies; forward gap resyncs; missing IDs resync |
| **C13** | Sync | Policy commit timing | Cursor does not advance when book rejects/crosses an otherwise classifiable event |
| **C14** | Resync | Gap transition | Clears book/BBO; emits exactly one invalidation and request; increments no version |
| **C15** | Resync | Continuous desync | Later incrementals ignored; no repeated invalidation or snapshot request |
| **C16** | Resync | Recovery snapshot | Valid newer snapshot re-anchors policy, increments epoch, and resumes apply |
| **C17** | Resync | Stale/invalid recovery snapshot | Remains non-actionable and desynchronized |
| **C18** | Book | Crossed delta/snapshot | Publishes no crossed BBO; clears/inhibits state; reason is crossed; requests snapshot |
| **C19** | Book | Empty snapshot semantics | Both sides empty rejects; one side empty accepts with explicit unavailable BBO side |
| **C20** | Fixture | Audit facts | Exactly five absent deletes, all 2,241 adjacent deltas equal 100, zero crossed/locked steps |
| **C21** | Fixture | Naive reference oracle | Optimized book BBO equals map-plus-sort oracle after every accepted row |
| **C22** | Fixture | Final golden state | 2,242 accepted, 227 bids, 301 asks, bid 99993.99, ask 99998.24, no invalidation |
| **C23** | Pipeline | Version/notification stream | Accepted versions contiguous; notification IDs contiguous; event semantics correct |
| **C24** | Quote cache | Concurrent readers | Coherent BBO/version under writer load and `go test -race` |
| **C25** | Ring | Empty/full/wraparound | FIFO across many wraps; no loss or duplicate |
| **C26** | Ring | Slow consumer | Producer backpressures and resumes; zero loss |
| **C27** | Ring | Close/drain | Buffered values drain; publish-after-close returns `ErrClosed` |
| **C28** | Ring | Cancellation while full | Blocked publisher observes cancellation, returns, then producer closes; no deadlock or overwrite |
| **C29** | Ring | Concurrent race test | Single producer/consumer stress is detector-clean |
| **C30** | Logger | Lossless delivery | Ordered sink content; flush on size, interval, and close; no missing records |
| **C31** | Logger | Explicit drop mode | Never blocks on full; dropped metric exact and visible |
| **C32** | Clock | Paced source-time rebasing | `DueNS` equals replay start plus scaled offset from the first source timestamp; raw epoch time is never subtracted from local monotonic time |
| **C33** | Strategy | Callback receive stamp | Simulated clock proves `receiveNS` is sampled after successful consume and before `OnEvent`; primary/apply histograms and logged `RecvNS` use that exact sample |
| **C34** | Strategy | Event BBO read | Applied events log their version-correlated BBO; cache comparison is required only when cache and event versions match |
| **C35** | Feed | Stream identity | `binance,BTC/USDT` normalizes to `binance,BTCUSDT`; empty identities reject; mismatch rejects before policy/book mutation; resync request retains configured identity |
| **C36** | Pipeline | Fast versus paced | Identical accepted/discarded decisions, versions, BBOs, and final book; timing fields differ by documented mode semantics |
| **C37** | Pipeline | Desynchronized EOF | Typed error; no actionable BBO remains |
| **C38** | Pipeline | Determinism | Same input and simulated clock produce identical events and metrics |
| **C39** | Lifecycle | P -> S -> L shutdown | Rings drain, logger flushes, errors propagate, goroutine count returns to baseline |
| **C40** | Stress | Full synthetic pipeline | Detector-clean, zero event loss, invariant checks pass |
| **C41** | Layout | Record sizes | Report actual `unsafe.Sizeof` values; capacity memory calculation uses measured sizes |

Required commands:

```text
go test ./...
go test -race ./...
go test -bench . -benchmem ./...
```

## Appendix A — exact fixed-point parsing

```text
parse(s, scaleDigits):
    reject empty input
    reject leading sign and exponent notation
    split once on '.' into integer and fractional components
    require a non-empty integer component containing only digits
    require fractional component, when present, to contain only digits
    reject len(fractional) > scaleDigits
    right-pad fractional with zeroes to scaleDigits
    checkedValue = checkedAtoi(integer) * 10^scaleDigits + checkedAtoi(fractional)
    reject int64 overflow
    return checkedValue
```

`ParsePrice` uses two fractional digits; `ParseQty` uses four. Formatting performs the inverse operation directly into a caller-provided byte slice.

## Appendix B — synchronization policy summary

| Policy | Stale/duplicate | Expected next | Forward gap | Completeness claim |
|---|---|---|---|---|
| Arrival order | Apply | Apply | Apply | None |
| Timestamp monotonic | Discard | Apply any newer | Apply and count large gap | Ordering sanity only |
| Timestamp step | Discard | Apply exact step | Resync | Synthetic fixture contract |
| Update ID | Discard final ID <= last | Apply range covering next ID | Resync | Venue continuity when IDs are authentic |

Crossed or locked BBO is an independent resync symptom under every policy.

## Appendix C — lifecycle sequence

Startup:

1. Parse and validate configuration.
2. Open input and output sink.
3. Allocate rings, maps, heaps, and histograms.
4. Start logger goroutine L.
5. Start strategy goroutine S.
6. Run producer P.

Normal shutdown:

1. P reaches EOF, closes event ring, and stops publishing.
2. S drains the event ring, closes the log ring, and exits.
3. L drains the log ring, flushes and closes the sink, and exits.
4. Main joins S and L, combines errors, and emits final metrics.

Fatal decode/configuration errors follow the same downstream close-and-drain order for already accepted records. External cancellation may stop early and is reported separately from a clean drain.
