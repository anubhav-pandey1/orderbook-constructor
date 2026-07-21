.PHONY: build run benchmark test race fmt vet check hooks

RUN_ARGS ?=

build:
	go build ./...

run:
	go run ./cmd/replay -csv testdata/btc_orderbook_updates.csv -exchange binance -symbol BTCUSDT -replay fast -sync-policy timestamp -timestamp-mode step -timestamp-step 100ms -log stdout -log-delivery lossless $(RUN_ARGS)

benchmark:
	go test -bench . -benchmem -run '^$$' ./...
	go run ./cmd/bench -csv testdata/btc_orderbook_updates.csv -out BENCHMARK.md

test:
	go test -race ./...

race:
	go test -race ./...

fmt:
	go fmt ./...

vet:
	go vet ./...

check:
	test -z "$$(gofmt -l .)"
	go vet ./...
	go test ./...

hooks:
	git config core.hooksPath .githooks
