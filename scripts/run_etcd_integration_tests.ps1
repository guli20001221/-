$ErrorActionPreference = "Stop"

function Get-FreePorts([int]$start, [int]$end, [int]$count) {
  $used = Get-NetTCPConnection -State Listen | Select-Object -ExpandProperty LocalPort | Sort-Object -Unique
  $candidates = ($start..$end) | Where-Object { $_ -notin $used }
  if ($candidates.Count -lt $count) {
    throw "Not enough free ports in range $start-$end"
  }
  return $candidates[0..($count-1)]
}

$root = Get-Location
$logDir = Join-Path $root ".tmp"
New-Item -ItemType Directory -Force -Path $logDir | Out-Null

# pick ports
$etcdPort = (Get-FreePorts 2379 2390 1)[0]
$serverPorts = Get-FreePorts 9000 9100 2
$portA = $serverPorts[0]
$portB = $serverPorts[1]

$container = "etcd-test-$PID"

try {
  # start etcd
  docker run -d --name $container -p ${etcdPort}:2379 quay.io/coreos/etcd:v3.5.0 `
    etcd --advertise-client-urls http://0.0.0.0:2379 `
         --listen-client-urls http://0.0.0.0:2379 | Out-Null

  # wait for etcd
  $healthy = $false
  for ($i = 0; $i -lt 10; $i++) {
    docker exec $container etcdctl endpoint health --endpoints http://127.0.0.1:2379 2>$null | Out-Null
    if ($LASTEXITCODE -eq 0) { $healthy = $true; break }
    Start-Sleep -Seconds 1
  }
  if (-not $healthy) { throw "etcd not healthy" }

  $etcdEndpoint = "127.0.0.1:$etcdPort"
  $service = "GroupCache"
  $group = "scores"

  $log1 = Join-Path $logDir "server1.log"
  $log1Err = Join-Path $logDir "server1.err.log"
  $log2 = Join-Path $logDir "server2.log"
  $log2Err = Join-Path $logDir "server2.err.log"

  $server1 = Start-Process -FilePath "go" -ArgumentList @(
    "run", ".\example\grpc_etcd\main.go",
    "-mode", "server",
    "-addr", "127.0.0.1:$portA",
    "-etcd", $etcdEndpoint,
    "-service", $service,
    "-group", $group
  ) -PassThru -RedirectStandardOutput $log1 -RedirectStandardError $log1Err

  $server2 = Start-Process -FilePath "go" -ArgumentList @(
    "run", ".\example\grpc_etcd\main.go",
    "-mode", "server",
    "-addr", "127.0.0.1:$portB",
    "-etcd", $etcdEndpoint,
    "-service", $service,
    "-group", $group
  ) -PassThru -RedirectStandardOutput $log2 -RedirectStandardError $log2Err

  Start-Sleep -Seconds 2

  # set on node A, get from node B
  & go run .\example\grpc_etcd\main.go -mode client -target "127.0.0.1:$portA" -group $group -op set -key k1 -value v_custom
  & go run .\example\grpc_etcd\main.go -mode client -target "127.0.0.1:$portB" -group $group -op get -key k1
}
finally {
  if ($server1) { Stop-Process -Id $server1.Id -Force -ErrorAction SilentlyContinue }
  if ($server2) { Stop-Process -Id $server2.Id -Force -ErrorAction SilentlyContinue }
  docker rm -f $container 2>$null | Out-Null
}
