SELECT
    dataset,
    benchmark_mode,
    implementation,
    rows,
    fetch_size,
    batch_size,
    count() AS runs,
    round(avg(total_ms), 2) AS avg_total_ms,
    round(avg(prepare_ms), 2) AS avg_prepare_ms,
    round(avg(source_verify_ms), 2) AS avg_source_verify_ms,
    round(avg(target_setup_ms), 2) AS avg_target_setup_ms,
    round(avg(extract_ms), 2) AS avg_extract_ms,
    round(avg(serialize_ms), 2) AS avg_serialize_ms,
    round(avg(load_ms), 2) AS avg_load_ms,
    round(avg(verify_ms), 2) AS avg_target_verify_ms,
    round(avg(overhead_ms), 2) AS avg_overhead_ms,
    round(avg(rows_per_sec), 2) AS avg_rows_per_sec,
    round(avg(mb_per_sec), 2) AS avg_mb_per_sec,
    round(avg(peak_rss_mb), 2) AS avg_peak_rss_mb,
    round(avg(cpu_ms), 2) AS avg_cpu_ms
FROM olap_benchmark.benchmark_runs
WHERE benchmark_version = 4 AND benchmark_mode IN ('full', 'transfer')
GROUP BY
    dataset,
    benchmark_mode,
    implementation,
    rows,
    fetch_size,
    batch_size
ORDER BY
    dataset,
    benchmark_mode,
    rows,
    avg_total_ms,
    implementation;
