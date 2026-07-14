param()

$ErrorActionPreference = "Stop"
$Root = Split-Path -Parent (Split-Path -Parent $MyInvocation.MyCommand.Path)
Set-Location $Root

docker compose exec clickhouse clickhouse-client --query "ALTER TABLE olap_benchmark.benchmark_runs ADD COLUMN IF NOT EXISTS benchmark_version UInt16 DEFAULT 1" | Out-Null
docker compose exec clickhouse clickhouse-client --query "ALTER TABLE olap_benchmark.benchmark_runs ADD COLUMN IF NOT EXISTS benchmark_mode LowCardinality(String) DEFAULT 'legacy'" | Out-Null
foreach ($Column in @("source_verify_ms", "target_setup_ms", "serialize_ms", "overhead_ms")) {
  docker compose exec clickhouse clickhouse-client --query "ALTER TABLE olap_benchmark.benchmark_runs ADD COLUMN IF NOT EXISTS $Column Float64 DEFAULT 0" | Out-Null
}

Write-Host "`n=== Итоговая производительность ==="
docker compose exec clickhouse clickhouse-client --query "
SELECT
    dataset,
    benchmark_mode AS mode,
    implementation,
    rows,
    round(total_ms, 2) AS total_ms,
    round(rows_per_sec, 2) AS rows_per_sec,
    round(mb_per_sec, 2) AS mb_per_sec,
    round(peak_rss_mb, 2) AS peak_rss_mb,
    round(cpu_ms, 2) AS cpu_ms,
    measured_at
FROM olap_benchmark.benchmark_runs
WHERE benchmark_version = 2 AND benchmark_mode IN ('full', 'transfer')
ORDER BY measured_at DESC
LIMIT 20
FORMAT PrettyCompact
"

Write-Host "`n=== Разбор времени по фазам ==="
docker compose exec clickhouse clickhouse-client --query "
SELECT
    dataset,
    benchmark_mode AS mode,
    implementation,
    round(prepare_ms, 2) AS prepare_ms,
    round(source_verify_ms, 2) AS source_verify_ms,
    round(target_setup_ms, 2) AS target_setup_ms,
    round(extract_ms, 2) AS extract_ms,
    round(serialize_ms, 2) AS serialize_ms,
    round(load_ms, 2) AS load_ms,
    round(verify_ms, 2) AS target_verify_ms,
    round(overhead_ms, 2) AS overhead_ms,
    batch_count,
    measured_at
FROM olap_benchmark.benchmark_runs
WHERE benchmark_version = 2 AND benchmark_mode IN ('full', 'transfer')
ORDER BY measured_at DESC
LIMIT 20
FORMAT PrettyCompact
"
