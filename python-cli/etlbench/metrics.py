from __future__ import annotations

import json
import os
import threading
import time
import uuid
from dataclasses import asdict, dataclass, field
from typing import Any

import clickhouse_connect
import psutil

from etlbench.config import ClickHouseConfig


@dataclass
class BenchmarkMetrics:
    benchmark_version: int = 2
    benchmark_mode: str = "full"
    run_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    implementation: str = "python"
    dataset: str = ""
    source_table: str = ""
    target_table: str = ""
    input_file: str = ""
    file_format: str = ""
    rows: int = 0
    logical_bytes: int = 0
    fetch_size: int = 0
    batch_size: int = 0
    batch_count: int = 0
    prepare_ms: float = 0.0
    source_verify_ms: float = 0.0
    target_setup_ms: float = 0.0
    extract_ms: float = 0.0
    serialize_ms: float = 0.0
    load_ms: float = 0.0
    verify_ms: float = 0.0
    overhead_ms: float = 0.0
    total_ms: float = 0.0
    rows_per_sec: float = 0.0
    mb_per_sec: float = 0.0
    peak_rss_mb: float = 0.0
    cpu_ms: float = 0.0
    extra_json: str = "{}"

    def finish_rates(self) -> None:
        seconds = self.total_ms / 1000.0 if self.total_ms else 0.0
        self.rows_per_sec = self.rows / seconds if seconds else 0.0
        self.mb_per_sec = (self.logical_bytes / 1024 / 1024) / seconds if seconds else 0.0

    def set_extra(self, payload: dict[str, Any]) -> None:
        self.extra_json = json.dumps(payload, ensure_ascii=False, sort_keys=True)


class MemorySampler:
    def __init__(self, interval_seconds: float = 0.05) -> None:
        self._process = psutil.Process(os.getpid())
        self._interval_seconds = interval_seconds
        self._stop = threading.Event()
        self.peak_rss = self._process.memory_info().rss
        self._thread = threading.Thread(target=self._sample, daemon=True)

    def __enter__(self) -> "MemorySampler":
        self._thread.start()
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self._stop.set()
        self._thread.join(timeout=1)
        self.peak_rss = max(self.peak_rss, self._process.memory_info().rss)

    def _sample(self) -> None:
        while not self._stop.is_set():
            self.peak_rss = max(self.peak_rss, self._process.memory_info().rss)
            time.sleep(self._interval_seconds)


def now_ms() -> float:
    return time.perf_counter() * 1000.0


def process_cpu_ms() -> float:
    cpu = psutil.Process(os.getpid()).cpu_times()
    return (cpu.user + cpu.system) * 1000.0


def insert_metrics(config: ClickHouseConfig, metrics: BenchmarkMetrics) -> None:
    client = clickhouse_connect.get_client(
        host=config.host,
        port=config.port,
        username=config.user,
        password=config.password,
        database=config.database,
    )
    client.command(f"CREATE DATABASE IF NOT EXISTS {config.database}")
    client.command(
        f"""
        CREATE TABLE IF NOT EXISTS {config.database}.benchmark_runs
        (
            benchmark_version UInt16 DEFAULT 2,
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
        ORDER BY (dataset, implementation, measured_at, run_id)
        """
    )
    client.command(
        f"ALTER TABLE {config.database}.benchmark_runs "
        "ADD COLUMN IF NOT EXISTS benchmark_version UInt16 DEFAULT 1"
    )
    client.command(
        f"ALTER TABLE {config.database}.benchmark_runs "
        "ADD COLUMN IF NOT EXISTS benchmark_mode LowCardinality(String) DEFAULT 'legacy'"
    )
    for column in ("source_verify_ms", "target_setup_ms", "serialize_ms", "overhead_ms"):
        client.command(
            f"ALTER TABLE {config.database}.benchmark_runs "
            f"ADD COLUMN IF NOT EXISTS {column} Float64 DEFAULT 0"
        )
    data = asdict(metrics)
    ordered = [
        "benchmark_version",
        "benchmark_mode",
        "run_id",
        "implementation",
        "dataset",
        "source_table",
        "target_table",
        "input_file",
        "file_format",
        "rows",
        "logical_bytes",
        "fetch_size",
        "batch_size",
        "batch_count",
        "prepare_ms",
        "source_verify_ms",
        "target_setup_ms",
        "extract_ms",
        "serialize_ms",
        "load_ms",
        "verify_ms",
        "overhead_ms",
        "total_ms",
        "rows_per_sec",
        "mb_per_sec",
        "peak_rss_mb",
        "cpu_ms",
        "extra_json",
    ]
    client.insert("benchmark_runs", [[data[name] for name in ordered]], column_names=ordered)
