#!/usr/bin/env sh
set -eu

mode="${1:-pre-commit}"

fmt_files="$(gofmt -l .)"
if [ -n "$fmt_files" ]; then
  printf '%s\n' "gofmt required:" "$fmt_files"
  exit 1
fi

go vet ./...

if command -v staticcheck >/dev/null 2>&1; then
  staticcheck ./...
else
  printf '%s\n' "staticcheck not found; install with: go install honnef.co/go/tools/cmd/staticcheck@latest"
fi

go test ./...

if [ "$mode" = "pre-push" ] || [ "$mode" = "full" ]; then
  go test -race ./...
  if command -v govulncheck >/dev/null 2>&1; then
    govulncheck ./...
  else
    printf '%s\n' "govulncheck not found; install with: go install golang.org/x/vuln/cmd/govulncheck@latest"
  fi
  go test -run '^$' -bench . -benchmem -benchtime=1x ./...
fi
