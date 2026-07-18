# orderbook

An ultra-low-latency, aggregated **Level-2** order book, strategy consumer, and
benchmark suite written in pure-standard-library Go (Go 1.26). It ingests a
Binance BTC/USDT snapshot plus incremental updates from a CSV fixture
(`btc_orderbook_updates.csv`), reconstructs a correct L2 book on a single writer
goroutine, and publishes the best bid/ask to a strategy over a lock-free
single-producer/single-consumer ring. All logging is done asynchronously and in
batches off the hot path, so the measured engine-to-strategy latency reflects
the book itself and not I/O.

---

## Quick start

Requires Go 1.26+. All commands run from the project root.

```sh
make build      # compile all packages, including cmd/replay and cmd/bench
make run        # replay the fixture in fast mode, stream BBO to stdout
make benchmark  # run Go benchmarks + latency runner, write BENCHMARK.md
make test       # go test -race ./... — correctness + race suite
make race       # go test -race ./...  — full suite under the race detector
```

Additional targets: `make fmt` (`go fmt ./...`) and `make vet` (`go vet ./...`).

To pass custom flags, invoke the binaries directly, e.g.:

```sh
go run ./cmd/replay -csv btc_orderbook_updates.csv -replay paced -speed 2.0 -log discard
go run ./cmd/bench -csv btc_orderbook_updates.csv -fixture-iters 100 -out BENCHMARK.md
```

`make run` also accepts overrides through `RUN_ARGS`, for example
`make run RUN_ARGS='-replay paced -speed 2 -log discard'`.

---

## Flag reference

### `cmd/replay` (the run binary)

| Flag | Type / values | Default | Description |
|---|---|---|---|
| `-csv` | string | `btc_orderbook_updates.csv` | Path to the snapshot + incremental CSV fixture. |
| `-exchange` | string | `binance` | Expected exchange; rows that disagree are rejected. |
| `-symbol` | string | `BTCUSDT` | Expected instrument symbol; separators are removed during normalization. |
| `-replay` | `fast` \| `paced` | `fast` | `fast` applies rows back-to-back; `paced` reproduces the original inter-event timing. |
| `-speed` | float | `1.0` | Pacing multiplier for `-replay paced` (2.0 = twice as fast). Ignored in fast mode. |
| `-timestamp-unit` | `auto` \| `ns` \| `us` \| `ms` | `auto` | Interpretation of the timestamp column. Auto recognizes 13/16/19-digit epoch values. |
| `-sync-policy` | `timestamp` \| `update-id` \| `off` | `timestamp` | Continuity policy. `update-id` rejects at startup because the assignment CSV has no update-ID columns. |
| `-timestamp-mode` | `step` \| `monotonic` | `step` | Require the fixture cadence or accept any newer timestamp. |
| `-timestamp-step` | duration | `100ms` | Expected source timestamp step; converted exactly into raw source units. |
| `-log` | `stdout` \| `file` \| `discard` | `stdout` | Log sink. `discard` is used for benchmarking. |
| `-log-file` | string | `orderbook.log` | Output path when `-log file`. |
| `-log-delivery` | `lossless` \| `drop` | `lossless` | Logger backpressure policy. Drop mode is explicit and reports its dropped count. |
| `-event-ring` | int (power of two) | `65536` | Capacity of the lossless writer-to-strategy SPSC ring. |
| `-log-ring` | int (power of two) | `65536` | Capacity of the strategy-to-logger SPSC ring. |
| `-spin` | int | `128` | Busy-spin iterations before a blocked ring op yields via `runtime.Gosched()`. |
| `-gomaxprocs` | int | `0` | Override `GOMAXPROCS`; `0` keeps the Go runtime default. |

### `cmd/bench` (latency + throughput runner)

| Flag | Type / values | Default | Description |
|---|---|---|---|
| `-csv` | string | `btc_orderbook_updates.csv` | Path to the fixture used to drive the benchmarks. |
| `-exchange` | string | `binance` | Expected exchange. |
| `-symbol` | string | `BTCUSDT` | Expected normalized symbol. |
| `-cpu-model` | string | `""` (auto) | CPU-model override when OS discovery is unavailable. |
| `-fixture-iters` | int | `20` | Measured full-fixture replay repetitions. |
| `-warmup` | int | `3` | Fixture warmups discarded before measurement. |
| `-synthetic` | int64 | `10000000` | Synthetic events after the initial snapshot. |
| `-snapshot-every` | int64 | `100000` | W6b periodic-snapshot interval. |
| `-synthetic-max-levels` | int | `64` | Bound on active generated levels per side. |
| `-seed` | int64 | `42` | Deterministic synthetic generator seed. |
| `-event-ring` | int (power of two) | `65536` | Book-event ring capacity used during the run. |
| `-log-ring` | int (power of two) | `65536` | Log ring capacity used during the run. |
| `-spin` | int | `128` | Ring busy-spin iterations before yielding. |
| `-paced-speed` | float | `10000` | Speed multiplier for the W7 paced workload. |
| `-out` | string | `""` (stdout) | Report destination; empty prints to stdout, otherwise the report is written to this path (e.g. `BENCHMARK.md`). |

---

## Design summary

- **Single writer.** All decode, validation, sequencing, and book mutation run on
  one goroutine. There is no lock on the mutation path and application order is
  deterministic.
- **Authoritative map + heap per side.** Each side keeps a `price -> quantity`
  map as the source of truth, plus a max-heap (bids) / min-heap (asks) for the
  best price. Heap entries are **generation-tagged** for lazy deletion, so a
  removed or replaced price cannot resurface as a stale root; BBO lookup is
  amortized O(1).
- **Seqlock quote cache.** A sequence-lock over atomic fields lets independent
  concurrent readers obtain a race-free, internally consistent BBO without
  blocking the writer.
- **BBO carried by value.** Every accepted row emits one versioned event whose
  bid/ask price+size are copied in, so the strategy always sees the BBO belonging
  to exactly the version that woke it, even if the writer has moved on.
- **Lock-free SPSC ring transport.** The writer publishes events to the strategy
  over a single-producer/single-consumer ring; backpressure is spin then
  `runtime.Gosched()`, and delivery is **lossless** (a full ring stalls the
  writer rather than dropping or coalescing updates).
- **Async batched logging off the hot path.** Path is
  strategy -> log ring -> logger goroutine -> `stdout` | `file` | `discard`.
  Formatting and syscalls never sit inside the measured strategy callback.
- **Fixed-point int64.** Price is stored as cents (x100) and size as 1e-4 units
  (x10000); both are exact for every value in this feed, eliminating float drift
  and giving deterministic key comparisons.
- **Snapshots.** A snapshot is built off-book and then atomically replaces both
  sides in a single writer operation, so no reader ever sees a half-built book.
- **Deletes.** `size = 0` deletes a level; a delete targeting an absent level is
  an idempotent no-op (still a successful, versioned step).
- **Versioning.** Exactly one versioned event is published per accepted row.

See **[DESIGN.md](docs/DESIGN.md)** for the full design, state machine, and rationale,
and **[SPEC.md](docs/SPEC.md)** for the implementation contracts and test matrix.

---

## Design choices (pros / cons)

| Choice | Pros | Cons |
|---|---|---|
| **Map + generation-tagged heap** (vs. balanced tree, e.g. red-black/AVL) | O(1) existing-level replace and non-best delete; amortized O(1) BBO; complete depth retained in the map; no third-party dependency | First insert at a new price is O(log n); deleting the best level pays lazy cleanup; heap churn can trigger an O(n) rebuild; not built for full ordered traversal on the hot path |
| **SPSC ring** (vs. Go channels) | Lock-free, allocation-free steady state; tighter, more predictable tail latency; explicit backpressure and occupancy metrics | Only one producer / one consumer; hand-written cursor + close/drain protocol to get right; fixed power-of-two capacity |
| **Async batched logging** (vs. inline logging) | Formatting and syscalls leave the measured path; batching amortizes write cost; pluggable sink incl. `discard` for benchmarks | Log output lags the event; a slow lossless sink eventually backpressures the pipeline; extra ring + goroutine to reason about |
| **Single writer** (vs. multi-writer / locked book) | Deterministic ordering; no mutation-path contention; direct latency attribution | A slow downstream consumer stalls ingestion; does not scale to concurrent writers for one book |
| **Fixed-point int64** (vs. float64 / decimal library) | Exact keys, deterministic comparisons, no rounding drift; cheap integer ops | Parsing/formatting is explicit; scale constants (x100 price, x10000 size) are tied to this feed's verified precision and would need widening for other instruments |

---

## Assumptions & limitations

- **Timestamps are ms-scale, not ns.** The column is nominally labeled
  nanoseconds but holds 13-digit epoch **milliseconds**. They are treated purely
  as event-time / pacing metadata; historical epoch values are never subtracted
  from the local monotonic latency clock (that would measure file age, not engine
  latency).
- **Timestamp-step continuity is fixture-specific.** This verified synthetic
  stream advances by exactly 100 ms, so a stale value is discarded and any
  other forward step triggers fail-closed resynchronization. That is useful for
  this assignment but is not a production completeness guarantee. A real
  adapter would use Binance
  `U`/`u` first/final update IDs to detect gaps and trigger a resync — see the
  `UpdateIDPolicy` seam in [DESIGN.md](docs/DESIGN.md).
- **CRLF line endings.** The fixture uses `\r\n`; the decoder strips the trailing
  carriage return.
- **Single exchange / symbol.** One book instance handles one configured
  normalized exchange/symbol pair (default `binance` / `BTCUSDT`; the fixture's
  raw `BTC/USDT` spelling normalizes to the same identity). The `Book` type itself is
  reusable for additional instruments.
- **Package placement.** The implementation keeps reusable public `book` and
  `feed` packages at the module root, with fixed-point types owned by `book`.
  This differs from the illustrative `internal/price`, `internal/book`, and
  `internal/feed` directory sketch in `SPEC.md`; the APIs and ownership rules
  are unchanged, while external tests can import the engine directly.

---

## Verified golden facts

Measured directly from `btc_orderbook_updates.csv` and asserted by the golden
test:

| Fact | Value |
|---|---|
| Total data rows | **2242** (2 snapshots + 2240 incrementals) |
| Deletions (`size = 0`) | **543** |
| Final active bid levels | **227** |
| Final active ask levels | **301** |
| Final best bid | **99993.99** |
| Final best ask | **99998.24** |
| Adjacent timestamp deltas | all exactly **+100** (strictly increasing) |
| Crossed/locked post-apply steps | **0** (book never crosses) |

---

## Further reading

- **[DESIGN.md](docs/DESIGN.md)** — full design: architecture, book structures, sync
  state machine, ring/seqlock protocols, benchmark design, and architecture
  decisions.
- **[SPEC.md](docs/SPEC.md)** — concrete APIs, invariants, lifecycle, workloads,
  and correctness/race test matrix.
- **[BENCHMARK.md](BENCHMARK.md)** — canonical performance report, regenerated by
  `make benchmark`.
