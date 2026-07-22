.PHONY: build run benchmark test race fmt vet staticcheck vuln check full-check tools hooks

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

staticcheck:
	./.bin/staticcheck ./...

vuln:
	./.bin/govulncheck ./...

check:
	./scripts/check.sh pre-commit

full-check:
	./scripts/check.sh full

tools:
	./scripts/install-tools.sh

hooks:
	git config core.hooksPath .githooks
