param(
  [Parameter(Mandatory = $true)]
  [ValidateSet("fact_animals", "med_technic", "turkestan_109_incidents")]
  [string] $Dataset,

  [string] $File,
  [int] $FetchSize = 50000,
  [int] $BatchSize = 50000,
  [int] $LimitRows = 0,
  [switch] $SkipPrepare,
  [string] $Sheet,
  [string] $Delimiter
)

$ErrorActionPreference = "Stop"
$Root = Split-Path -Parent (Split-Path -Parent $MyInvocation.MyCommand.Path)
. "$Root\scripts\paths.ps1"
Set-Location $Root

docker compose up -d --build postgres clickhouse airflow

$Conf = @{
  dataset = $Dataset
  fetch_size = $FetchSize
  batch_size = $BatchSize
  skip_prepare = [bool]$SkipPrepare
}
$ResolvedFile = Resolve-BenchmarkInputFile -Root $Root -File $File
if ($ResolvedFile) { $Conf.file = $ResolvedFile.ContainerAbsolute }
if ($LimitRows -gt 0 -and -not $SkipPrepare) { $Conf.limit_rows = $LimitRows }
if ($Sheet) { $Conf.sheet = $Sheet }
if ($Delimiter) { $Conf.delimiter = $Delimiter }

$Json = $Conf | ConvertTo-Json -Compress

function Invoke-DockerCapture {
  param(
    [Parameter(Mandatory = $true)]
    [string[]] $Arguments
  )

  $StdoutPath = Join-Path $env:TEMP ("etl-benchmark-docker-stdout-{0}.log" -f ([guid]::NewGuid()))
  $StderrPath = Join-Path $env:TEMP ("etl-benchmark-docker-stderr-{0}.log" -f ([guid]::NewGuid()))
  try {
    $QuotedArgs = $Arguments | ForEach-Object {
      '"' + ($_ -replace '"', '\"') + '"'
    }
    $Command = 'docker ' + ($QuotedArgs -join ' ') + ' 1> "' + $StdoutPath + '" 2> "' + $StderrPath + '"'
    & cmd.exe /d /s /c $Command
    $ExitCode = $LASTEXITCODE
    $Stdout = if (Test-Path -LiteralPath $StdoutPath) { Get-Content -LiteralPath $StdoutPath -Raw } else { "" }
    $Stderr = if (Test-Path -LiteralPath $StderrPath) { Get-Content -LiteralPath $StderrPath -Raw } else { "" }
  } finally {
    Remove-Item -LiteralPath $StdoutPath, $StderrPath -Force -ErrorAction SilentlyContinue
  }

  $CleanStderr = ($Stderr -split "`r?`n" | Where-Object {
    $_ -and
    ($_ -notmatch "FutureWarning: section/key \[core/sql_alchemy_conn\]") -and
    ($_ -notmatch "use\[database/sql_alchemy_conn\]")
  }) -join [Environment]::NewLine
  $Combined = @($Stdout, $CleanStderr) -join [Environment]::NewLine

  return [pscustomobject]@{
    ExitCode = $ExitCode
    Output = $Combined.Trim()
  }
}

$Deadline = (Get-Date).AddMinutes(2)
do {
  $DbCheckResult = Invoke-DockerCapture -Arguments @("compose", "exec", "-T", "airflow", "airflow", "db", "check")
  if ($DbCheckResult.ExitCode -ne 0) {
    if ((Get-Date) -ge $Deadline) {
      Write-Host $DbCheckResult.Output
      throw "Airflow metadata database was not ready within 2 minutes."
    }
    Start-Sleep -Seconds 5
    continue
  }

  $ImportErrorsResult = Invoke-DockerCapture -Arguments @("compose", "exec", "-T", "airflow", "airflow", "dags", "list-import-errors")
  $ImportErrors = $ImportErrorsResult.Output
  if ($ImportErrorsResult.ExitCode -ne 0) {
    if ($ImportErrors -match "initialize the database|metadata database" -and (Get-Date) -lt $Deadline) {
      Start-Sleep -Seconds 5
      continue
    }
    Write-Host $ImportErrors
    throw "Failed to inspect Airflow DAG import errors."
  }
  if ($ImportErrors -and ($ImportErrors -notmatch "No data found") -and ($ImportErrors -match "oltp_olap_benchmark.py|Traceback|Error")) {
    Write-Host $ImportErrors
    throw "Airflow has DAG import errors. See output above."
  }

  $DagListResult = Invoke-DockerCapture -Arguments @("compose", "exec", "-T", "airflow", "airflow", "dags", "list")
  $DagList = $DagListResult.Output
  if ($DagListResult.ExitCode -ne 0) {
    Write-Host $DagList
    throw "Failed to inspect Airflow DAG list."
  }
  if ($DagList -match "oltp_olap_benchmark") {
    break
  }

  if ((Get-Date) -ge $Deadline) {
    Write-Host $DagList
    throw "DAG oltp_olap_benchmark was not registered by Airflow within 2 minutes."
  }
  Start-Sleep -Seconds 5
} while ($true)

if (-not (Test-Path -LiteralPath (Join-Path $Root "work"))) {
  New-Item -ItemType Directory -Force -Path (Join-Path $Root "work") | Out-Null
}
$ConfPath = Join-Path $Root "work\airflow_conf.json"
$TriggerPath = Join-Path $Root "work\airflow_trigger.py"
Set-Content -LiteralPath $ConfPath -Value $Json -NoNewline -Encoding ascii
Set-Content -LiteralPath $TriggerPath -Encoding ascii -Value @'
import subprocess
from pathlib import Path

conf = Path("/benchmark/work/airflow_conf.json").read_text(encoding="ascii")
raise SystemExit(subprocess.call([
    "airflow",
    "dags",
    "trigger",
    "oltp_olap_benchmark",
    "--conf",
    conf,
]))
'@

$TriggerDeadline = (Get-Date).AddMinutes(2)
do {
  $TriggerResult = Invoke-DockerCapture -Arguments @("compose", "exec", "-T", "airflow", "python", "/benchmark/work/airflow_trigger.py")
  if ($TriggerResult.ExitCode -eq 0) {
    Write-Host "Airflow DAG oltp_olap_benchmark triggered for dataset '$Dataset'."
    break
  }
  if ($TriggerResult.Output -match "DagNotFound" -and (Get-Date) -lt $TriggerDeadline) {
    Start-Sleep -Seconds 5
    continue
  }
  Write-Host $TriggerResult.Output
  throw "Failed to trigger Airflow DAG."
} while ($true)
