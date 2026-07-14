param()

$ErrorActionPreference = "Stop"
$Root = Split-Path -Parent (Split-Path -Parent $MyInvocation.MyCommand.Path)
Set-Location $Root

docker compose up -d --wait postgres clickhouse
if ($LASTEXITCODE -ne 0) { throw "Failed to start benchmark databases." }
docker compose ps
