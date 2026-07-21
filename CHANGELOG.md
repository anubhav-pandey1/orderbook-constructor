# Changelog

This project follows semantic versioning.

## Unreleased

- Changed the module path to `github.com/anubhav-pandey1/orderbook-constructor`.
- Added the public `replay` package for callback-based replay orchestration.
- Kept `book`, `feed`, `feed/gencsv`, and `replay` as the supported public API.
- Kept ring transport, logging, strategy runner, metrics, and command wiring under
  `internal`.
- Moved the golden fixture to `testdata/btc_orderbook_updates.csv`.
- Added package documentation, examples, CI configuration, and local hook scripts.
