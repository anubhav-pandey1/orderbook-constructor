#!/usr/bin/env sh
set -eu

mode="${1:-pre-commit}"
root="$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)"
bin="$root/.bin"
export GOTOOLCHAIN="${GOTOOLCHAIN:-local}"

tool() {
  name="$1"
  if [ -x "$bin/$name" ]; then
    printf '%s\n' "$bin/$name"
    return 0
  fi
  if command -v "$name" >/dev/null 2>&1; then
    command -v "$name"
    return 0
  fi
  printf '%s\n' "missing $name; run ./scripts/install-tools.sh" >&2
  return 1
}

fmt_files="$(gofmt -l .)"
if [ -n "$fmt_files" ]; then
  printf '%s\n' "gofmt required:" "$fmt_files"
  exit 1
fi

go vet ./...

"$(tool staticcheck)" ./...

go test ./...

if [ "$mode" = "pre-push" ] || [ "$mode" = "full" ]; then
  go test -race ./...
  "$(tool govulncheck)" ./...
  go test -run '^$' -bench . -benchmem -benchtime=1x ./...
fi
