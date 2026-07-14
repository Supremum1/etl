param()

$ErrorActionPreference = "Stop"
$Root = Split-Path -Parent (Split-Path -Parent $MyInvocation.MyCommand.Path)
Set-Location $Root

docker compose exec clickhouse clickhouse-client --query "
SELECT
    dataset,
    implementation,
    rows,
    round(total_ms, 2) AS total_ms,
    round(prepare_ms, 2) AS prepare_ms,
    round(extract_ms, 2) AS extract_ms,
    round(load_ms, 2) AS load_ms,
    round(rows_per_sec, 2) AS rows_per_sec,
    round(mb_per_sec, 2) AS mb_per_sec,
    batch_size,
    fetch_size,
    batch_count,
    round(peak_rss_mb, 2) AS peak_rss_mb,
    round(cpu_ms, 2) AS cpu_ms,
    measured_at
FROM olap_benchmark.benchmark_runs
ORDER BY measured_at DESC
FORMAT PrettyCompact
"
