param(
    [ValidateSet("pre-commit", "pre-push", "full")]
    [string]$Mode = "pre-commit"
)

$ErrorActionPreference = "Stop"

$fmtFiles = gofmt -l .
if ($fmtFiles) {
    Write-Error "gofmt required:`n$($fmtFiles -join "`n")"
}

go vet ./...

if (Get-Command staticcheck -ErrorAction SilentlyContinue) {
    staticcheck ./...
} else {
    Write-Host "staticcheck not found; install with: go install honnef.co/go/tools/cmd/staticcheck@latest"
}

go test ./...

if ($Mode -eq "pre-push" -or $Mode -eq "full") {
    go test -race ./...
    if (Get-Command govulncheck -ErrorAction SilentlyContinue) {
        govulncheck ./...
    } else {
        Write-Host "govulncheck not found; install with: go install golang.org/x/vuln/cmd/govulncheck@latest"
    }
    go test -run '^$' -bench . -benchmem -benchtime=1x ./...
}
