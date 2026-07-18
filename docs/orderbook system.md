# Technical Assignment: Orderbook System + Strategy Consumer

Language: Any programming language of your choice
Goal: Build a minimal real-time orderbook engine + strategy consumer with basic benchmarks.

## Problem Overview

You are tasked with implementing a lightweight orderbook system capable of:

 - Ingesting snapshot updates
 - Ingesting incremental updates
 - Maintaining a correct, complete limit order book
 - Allowing a strategy module to subscribe to updates
 - Running both concurrently
 - Benchmarking performance and latency

You will be provided a CSV file that simulates a stream of snapshot + incremental updates from:
 - Exchange: Binance
 - Symbol: BTCUSDT

## Input Data Specification

CSV columns:
```
type,exchange,symbol,timestamp,side,bids,asks,price,size
```

For snapshot rows: side,price and size are empty
For incremental rows: bids and asks are empty

The CSV contains two types of rows:

Snapshot update
```
type=snapshot  
exchange  
symbol  
timestamp (ns)  
bids: JSON array of [price, size]  
asks: JSON array of [price, size]
```

Incremental update
```
type=incremental  
exchange  
symbol  
timestamp (ns)  
side (bid or ask)
price  
size   (0 → delete level)
```

## System Requirements

### Orderbook Engine

You shall implement an orderbook that:
 - Applies the initial snapshot
 - Applies incremental updates
 - Merges them into a consistent L2 orderbook
 - Supports best bid / best ask lookup
 - Supports concurrent read/write (single-writer, many-reader is fine)

### Strategy Module

Implement a dummy strategy:
 - Subscribes to orderbook updates for one symbol
 - Every time the orderbook updates, it should:
  - Read the best bid and best ask
  - Log them (logging to stdout is enough)

### Concurrent Streaming

Your system must:

 - Continuously read the CSV file as if it were a live feed
 - Apply updates into the orderbook
 - The strategy should consume updates AS SOON AS the orderbook updates

Use any concurrency mechanism:
threads, async tasks, channels, queues, lock-free, etc.

### Benchmarks

Please include:
 - Throughput benchmark: How many updates/second can your orderbook process?
 - Latency benchmark: Measure timestamp_from_update → strategy_receives_callback


### Makefile

Provide a Makefile with at least:

```
make build

make run

make benchmark
```


## Submission

Your submission should include:

 - Source code
 - README (how to run, design choices, pros/cons)
 - Makefile
 - Benchmark report (simple text file is fine)



