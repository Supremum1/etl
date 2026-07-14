SELECT
    dataset,
    implementation,
    count() AS runs,
    round(avg(rows), 2) AS avg_rows,
    round(avg(total_ms), 2) AS avg_total_ms,
    round(avg(prepare_ms), 2) AS avg_prepare_ms,
    round(avg(extract_ms), 2) AS avg_extract_ms,
    round(avg(load_ms), 2) AS avg_load_ms,
    round(avg(rows_per_sec), 2) AS avg_rows_per_sec,
    round(avg(mb_per_sec), 2) AS avg_mb_per_sec,
    round(avg(peak_rss_mb), 2) AS avg_peak_rss_mb,
    round(avg(cpu_ms), 2) AS avg_cpu_ms
FROM olap_benchmark.benchmark_runs
GROUP BY
    dataset,
    implementation
ORDER BY
    dataset,
    avg_total_ms,
    implementation;
