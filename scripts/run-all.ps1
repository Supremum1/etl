param(
  [Parameter(Mandatory = $true)]
  [ValidateSet("fact_animals", "med_technic", "turkestan_109_incidents")]
  [string] $Dataset,

  [string] $File,
  [int] $FetchSize = 50000,
  [int] $BatchSize = 50000,
  [int] $LimitRows = 0
)

$ErrorActionPreference = "Stop"
$Root = Split-Path -Parent (Split-Path -Parent $MyInvocation.MyCommand.Path)
Set-Location $Root

$PrepareArgs = @{ Dataset = $Dataset }
if ($File) { $PrepareArgs.File = $File }
if ($LimitRows -gt 0) { $PrepareArgs.LimitRows = $LimitRows }
& "$Root\scripts\prepare-source.ps1" @PrepareArgs

& "$Root\scripts\run-python.ps1" -Dataset $Dataset -FetchSize $FetchSize -BatchSize $BatchSize -SkipPrepare
& "$Root\scripts\run-java.ps1" -Dataset $Dataset -FetchSize $FetchSize -BatchSize $BatchSize -SkipPrepare
& "$Root\scripts\run-airflow.ps1" -Dataset $Dataset -FetchSize $FetchSize -BatchSize $BatchSize -SkipPrepare
