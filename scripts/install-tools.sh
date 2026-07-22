#!/usr/bin/env sh
set -eu

root="$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)"
mkdir -p "$root/.bin"
GOBIN="$root/.bin" go install honnef.co/go/tools/cmd/staticcheck@v0.7.0
GOBIN="$root/.bin" go install golang.org/x/vuln/cmd/govulncheck@v1.6.0
printf '%s\n' "Installed staticcheck and govulncheck into .bin"
