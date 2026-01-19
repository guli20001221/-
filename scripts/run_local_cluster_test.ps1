$ErrorActionPreference = "Stop"

$root = Get-Location
$logDir = Join-Path $root ".tmp"
New-Item -ItemType Directory -Force -Path $logDir | Out-Null

$group = "scores"
$peers = "127.0.0.1:8001,127.0.0.1:8002,127.0.0.1:8003"

$logA = Join-Path $logDir "nodeA.log"
$logAErr = Join-Path $logDir "nodeA.err.log"
$logB = Join-Path $logDir "nodeB.log"
$logBErr = Join-Path $logDir "nodeB.err.log"
$logC = Join-Path $logDir "nodeC.log"
$logCErr = Join-Path $logDir "nodeC.err.log"

try {
  $nodeA = Start-Process -FilePath "go" -ArgumentList @(
    "run", ".\example\test.go",
    "-port", "8001",
    "-node", "A",
    "-peers", $peers,
    "-group", $group
  ) -PassThru -RedirectStandardOutput $logA -RedirectStandardError $logAErr

  $nodeB = Start-Process -FilePath "go" -ArgumentList @(
    "run", ".\example\test.go",
    "-port", "8002",
    "-node", "B",
    "-peers", $peers,
    "-group", $group
  ) -PassThru -RedirectStandardOutput $logB -RedirectStandardError $logBErr

  $nodeC = Start-Process -FilePath "go" -ArgumentList @(
    "run", ".\example\test.go",
    "-port", "8003",
    "-node", "C",
    "-peers", $peers,
    "-group", $group
  ) -PassThru -RedirectStandardOutput $logC -RedirectStandardError $logCErr

  Start-Sleep -Seconds 2

  & go run .\example\test.go -mode client -target "127.0.0.1:8001" -group $group -op set -key k1 -value v_custom
  & go run .\example\test.go -mode client -target "127.0.0.1:8002" -group $group -op get -key k1
  & go run .\example\test.go -mode client -target "127.0.0.1:8003" -group $group -op delete -key k1
  & go run .\example\test.go -mode client -target "127.0.0.1:8001" -group $group -op get -key k1
}
finally {
  if ($nodeA) { Stop-Process -Id $nodeA.Id -Force -ErrorAction SilentlyContinue }
  if ($nodeB) { Stop-Process -Id $nodeB.Id -Force -ErrorAction SilentlyContinue }
  if ($nodeC) { Stop-Process -Id $nodeC.Id -Force -ErrorAction SilentlyContinue }
}
