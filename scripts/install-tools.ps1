$ErrorActionPreference = "Stop"

$Root = Resolve-Path (Join-Path $PSScriptRoot "..")
$Bin = Join-Path $Root ".bin"
New-Item -ItemType Directory -Force $Bin | Out-Null

function Invoke-Checked {
    param(
        [Parameter(Mandatory = $true)][string]$FilePath,
        [string[]]$ArgumentList = @()
    )

    & $FilePath @ArgumentList
    if ($LASTEXITCODE -ne 0) {
        throw "$FilePath failed with exit code $LASTEXITCODE"
    }
}

$previous = $env:GOBIN
try {
    $env:GOBIN = $Bin
    Invoke-Checked "go" @("install", "honnef.co/go/tools/cmd/staticcheck@v0.7.0")
    Invoke-Checked "go" @("install", "golang.org/x/vuln/cmd/govulncheck@v1.6.0")
} finally {
    $env:GOBIN = $previous
}

Write-Host "Installed staticcheck and govulncheck into .bin"
