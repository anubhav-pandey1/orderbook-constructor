# Release Policy

`orderbook-constructor` uses semantic versioning for all public module releases.

The supported public API is limited to the `book`, `feed`, `feed/gencsv`, and
`replay` packages. Packages below `internal` and packages below `cmd` are not
part of the compatibility contract.

## Versioning

- `v0.x.x` releases are usable but unstable. Breaking API changes are allowed
  before `v1.0.0`, but they must be documented in `CHANGELOG.md`.
- Patch releases contain bug fixes, documentation fixes, and implementation
  changes that do not intentionally alter public behavior.
- Minor releases add backward-compatible public API.
- `v1.0.0` starts the stable compatibility contract.
- Future major versions at `v2` and later must use Go semantic import
  versioning, for example `github.com/anubhav-pandey1/orderbook-constructor/v2`.

## Go Support

The minimum supported Go version is Go 1.20. CI proves this by running the full
test matrix on Go 1.20 through Go 1.26 with `GOTOOLCHAIN=local`.

The `go.mod` directive stays at `go 1.20` until the public API intentionally
requires a newer language or standard-library feature.

## Release Checklist

1. Confirm `CHANGELOG.md` has a section for the target tag, such as
   `## v0.1.0 - 2026-07-22`.
2. Run `scripts/check.sh full` or `scripts/check.ps1 -Mode full`.
3. Confirm the external import smoke test passes.
4. Merge the release commit to `main`.
5. Tag the exact `main` commit with `git tag v0.1.0`.
6. Push the tag with `git push origin v0.1.0`.
7. Confirm the release workflow creates the GitHub Release.
8. Confirm the Go proxy can resolve the module:

```sh
GOPROXY=proxy.golang.org go list -m github.com/anubhav-pandey1/orderbook-constructor@v0.1.0
```

Do not move, delete, or rewrite a published tag. If a released tag is wrong,
publish a new patch or minor version.
