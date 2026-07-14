param(
  [Parameter(Mandatory = $true)]
  [ValidateSet("fact_animals", "med_technic", "turkestan_109_incidents")]
  [string] $Dataset,

  [string] $File,
  [int] $FetchSize = 50000,
  [int] $BatchSize = 50000,
  [int] $LimitRows = 0,
  [ValidateSet("http-tsv", "client-v3")]
  [string] $ClickHouseWriter = "http-tsv",
  [switch] $SkipPrepare,
  [string] $Sheet,
  [string] $Delimiter
)

$ErrorActionPreference = "Stop"
$Root = Split-Path -Parent (Split-Path -Parent $MyInvocation.MyCommand.Path)
. "$Root\scripts\paths.ps1"
Set-Location $Root

docker compose up -d postgres clickhouse
docker compose build java-cli

$ResolvedFile = Resolve-BenchmarkInputFile -Root $Root -File $File

$ArgsList = @(
  "--dataset", $Dataset,
  "--fetch-size", "$FetchSize",
  "--batch-size", "$BatchSize",
  "--clickhouse-writer", $ClickHouseWriter
)
if ($ResolvedFile) { $ArgsList += @("--file", $ResolvedFile.ContainerRelative) }
if ($LimitRows -gt 0 -and -not $SkipPrepare) { $ArgsList += @("--limit-rows", "$LimitRows") }
if ($Sheet) { $ArgsList += @("--sheet", $Sheet) }
if ($Delimiter) { $ArgsList += @("--delimiter", $Delimiter) }
if ($SkipPrepare) { $ArgsList += "--skip-prepare" }

docker compose run --rm java-cli @ArgsList
