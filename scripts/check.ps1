param(
    [ValidateSet("pre-commit", "pre-push", "full")]
    [string]$Mode = "pre-commit"
)

$ErrorActionPreference = "Stop"

$Root = Resolve-Path (Join-Path $PSScriptRoot "..")
$Bin = Join-Path $Root ".bin"
if (-not $env:GOTOOLCHAIN) {
    $env:GOTOOLCHAIN = "local"
}

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

function Resolve-Tool {
    param([Parameter(Mandatory = $true)][string]$Name)

    $Local = Join-Path $Bin "$Name.exe"
    if (Test-Path $Local) {
        return $Local
    }
    $OnPath = Get-Command $Name -ErrorAction SilentlyContinue
    if ($OnPath) {
        return $OnPath.Source
    }
    throw "missing $Name; run pwsh ./scripts/install-tools.ps1"
}

$fmtFiles = & gofmt -l .
if ($LASTEXITCODE -ne 0) {
    throw "gofmt failed with exit code $LASTEXITCODE"
}
if ($fmtFiles) {
    Write-Error "gofmt required:`n$($fmtFiles -join "`n")"
}

Invoke-Checked "go" @("vet", "./...")

Invoke-Checked (Resolve-Tool "staticcheck") @("./...")

Invoke-Checked "go" @("test", "./...")

if ($Mode -eq "pre-push" -or $Mode -eq "full") {
    Invoke-Checked "go" @("test", "-race", "./...")
    Invoke-Checked (Resolve-Tool "govulncheck") @("./...")
    Invoke-Checked "go" @("test", "-run", "^$", "-bench", ".", "-benchmem", "-benchtime=1x", "./...")
}
