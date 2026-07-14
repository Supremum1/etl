param(
  [Parameter(Mandatory = $true)]
  [ValidateSet("fact_animals", "med_technic", "turkestan_109_incidents")]
  [string] $Dataset,

  [string] $File,
  [int] $LimitRows = 0,
  [string] $Sheet,
  [string] $Delimiter
)

$ErrorActionPreference = "Stop"
$Root = Split-Path -Parent (Split-Path -Parent $MyInvocation.MyCommand.Path)
. "$Root\scripts\paths.ps1"
Set-Location $Root

docker compose up -d postgres clickhouse
docker compose build python-cli

$ResolvedFile = Resolve-BenchmarkInputFile -Root $Root -File $File

$ArgsList = @("prepare", "--dataset", $Dataset)
if ($ResolvedFile) { $ArgsList += @("--file", $ResolvedFile.ContainerRelative) }
if ($LimitRows -gt 0) { $ArgsList += @("--limit-rows", "$LimitRows") }
if ($Sheet) { $ArgsList += @("--sheet", $Sheet) }
if ($Delimiter) { $ArgsList += @("--delimiter", $Delimiter) }

docker compose run --rm python-cli @ArgsList
