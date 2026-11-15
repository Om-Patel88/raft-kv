# Builds raftkv and kvctl and starts a local three-node cluster on
# 127.0.0.1:8001-8003. Data and logs go under .\data\.
#
#   .\scripts\cluster.ps1          start (or restart) the cluster
#   .\scripts\cluster.ps1 -Stop    stop it
#   .\scripts\cluster.ps1 -Clean   stop it and wipe .\data
param(
    [switch]$Stop,
    [switch]$Clean
)
$ErrorActionPreference = 'Stop'

$root = Split-Path -Parent $PSScriptRoot
Set-Location $root
$bin = Join-Path $root 'bin'
$data = Join-Path $root 'data'
$pidFile = Join-Path $data 'cluster.pids'

function Stop-Cluster {
    if (Test-Path $pidFile) {
        foreach ($p in Get-Content $pidFile) {
            $proc = Get-Process -Id $p -ErrorAction SilentlyContinue
            if ($proc -and $proc.ProcessName -eq 'raftkv') {
                Stop-Process -Id $p -Force
                Write-Host "stopped raftkv (pid $p)"
            }
        }
        Remove-Item $pidFile
    }
}

Stop-Cluster
if ($Clean -and (Test-Path $data)) {
    Remove-Item -Recurse -Force $data
    Write-Host "removed $data"
}
if ($Stop -or $Clean) { return }

Write-Host 'building...'
go build -o $bin ./cmd/raftkv ./cmd/kvctl
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

New-Item -ItemType Directory -Force $data | Out-Null
$peers = 'n1=127.0.0.1:8001,n2=127.0.0.1:8002,n3=127.0.0.1:8003'
$pids = @()
foreach ($i in 1..3) {
    $id = "n$i"
    # Start-Process doesn't quote arguments, so keep them free of spaces:
    # run from the repo root and pass a relative data dir.
    $p = Start-Process -FilePath (Join-Path $bin 'raftkv.exe') -PassThru -WindowStyle Hidden `
        -WorkingDirectory $root `
        -ArgumentList "-id $id -addr 127.0.0.1:800$i -peers $peers -data data/$id" `
        -RedirectStandardError (Join-Path $data "$id.log") -RedirectStandardOutput (Join-Path $data "$id.out")
    $pids += $p.Id
    Write-Host "started $id on 127.0.0.1:800$i (pid $($p.Id), log data\$id.log)"
}
$pids | Set-Content $pidFile

Write-Host @"

try:
  `$env:RAFTKV_SERVERS = '127.0.0.1:8001,127.0.0.1:8002,127.0.0.1:8003'
  .\bin\kvctl.exe put greeting hello
  .\bin\kvctl.exe get greeting
  .\bin\kvctl.exe status

stop with: .\scripts\cluster.ps1 -Stop
"@
