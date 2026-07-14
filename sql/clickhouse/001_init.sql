CREATE DATABASE IF NOT EXISTS olap_benchmark;

CREATE TABLE IF NOT EXISTS olap_benchmark.benchmark_runs
(
    benchmark_version UInt16 DEFAULT 4,
    benchmark_mode LowCardinality(String),
    run_id UUID,
    measured_at DateTime64(3, 'UTC') DEFAULT now64(3),
    implementation LowCardinality(String),
    dataset LowCardinality(String),
    source_table String,
    target_table String,
    input_file String,
    file_format LowCardinality(String),
    rows UInt64,
    logical_bytes UInt64,
    fetch_size UInt32,
    batch_size UInt32,
    batch_count UInt64,
    prepare_ms Float64,
    source_verify_ms Float64,
    target_setup_ms Float64,
    extract_ms Float64,
    serialize_ms Float64,
    load_ms Float64,
    verify_ms Float64,
    overhead_ms Float64,
    total_ms Float64,
    rows_per_sec Float64,
    mb_per_sec Float64,
    peak_rss_mb Float64,
    cpu_ms Float64,
    extra_json String
)
ENGINE = MergeTree
ORDER BY (dataset, implementation, measured_at, run_id);
