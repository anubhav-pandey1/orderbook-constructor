.PHONY: build run benchmark test race fmt vet

# Optional flags for the replay command, for example:
# make run RUN_ARGS='-replay paced -speed 2 -log discard'
RUN_ARGS ?=

# Compile every package, including both submission binaries.
build:
	go build ./...

# Replay the fixture in fast mode with the assignment's synchronization and
# lossless stdout-logging defaults. RUN_ARGS may override any default.
run:
	go run ./cmd/replay -csv btc_orderbook_updates.csv -exchange binance -symbol BTCUSDT -replay fast -sync-policy timestamp -timestamp-mode step -timestamp-step 100ms -log stdout -log-delivery lossless $(RUN_ARGS)

# Run the Go micro/throughput benchmarks, then the end-to-end latency runner,
# which writes the canonical report to BENCHMARK.md.
benchmark:
	go test -bench . -benchmem -run '^$$' ./...
	go run ./cmd/bench -csv btc_orderbook_updates.csv -out BENCHMARK.md

# Correctness, golden-fixture, integration, and race tests.
test:
	go test -race ./...

# Explicit alias retained for discoverability.
race:
	go test -race ./...

# Format all Go sources in place.
fmt:
	go fmt ./...

# Static analysis.
vet:
	go vet ./...
