# Builds the browser demo into web/: raftsim.wasm plus the matching
# wasm_exec.js from the local Go install. Serve web/ with any static server.
$ErrorActionPreference = "Stop"

$root = Split-Path -Parent $PSScriptRoot
Push-Location $root
try {
    $oldOS, $oldArch = $env:GOOS, $env:GOARCH
    $env:GOOS = "js"
    $env:GOARCH = "wasm"
    try {
        go build -trimpath -ldflags="-s -w" -o web/raftsim.wasm ./cmd/raftsim
        if ($LASTEXITCODE -ne 0) { throw "go build failed" }
    } finally {
        $env:GOOS, $env:GOARCH = $oldOS, $oldArch
    }

    $goroot = (go env GOROOT).Trim()
    $src = Join-Path $goroot "lib/wasm/wasm_exec.js"
    if (-not (Test-Path $src)) {
        $src = Join-Path $goroot "misc/wasm/wasm_exec.js" # Go 1.23 and older
    }
    Copy-Item $src web/wasm_exec.js -Force

    $kib = [math]::Round((Get-Item web/raftsim.wasm).Length / 1KB)
    Write-Host "web/raftsim.wasm $kib KiB"
} finally {
    Pop-Location
}
