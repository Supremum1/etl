param()

$ErrorActionPreference = "Stop"
$Root = Split-Path -Parent (Split-Path -Parent $MyInvocation.MyCommand.Path)
Set-Location $Root

docker compose up -d --wait clickhouse
if ($LASTEXITCODE -ne 0) { throw "Failed to start ClickHouse." }
docker compose exec clickhouse clickhouse-client --query "TRUNCATE TABLE IF EXISTS olap_benchmark.benchmark_runs"
if ($LASTEXITCODE -ne 0) { throw "Failed to clear benchmark history." }
Write-Host "Benchmark history cleared. PostgreSQL and target data were not deleted."
