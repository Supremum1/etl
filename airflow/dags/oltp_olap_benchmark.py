from __future__ import annotations

from datetime import datetime
from pathlib import Path

from airflow.decorators import dag, task

from etlbench.config import ClickHouseConfig, PostgresConfig
from etlbench.identifiers import require_dataset
from etlbench.io_streams import find_default_file
from etlbench.metrics import BenchmarkMetrics, MemorySampler, process_cpu_ms
from etlbench.source_loader import prepare_postgres_source
from etlbench.transfer import transfer_postgres_to_clickhouse


def _conf(context) -> dict:
    return (context["dag_run"].conf or {}) if context.get("dag_run") else {}


@dag(
    dag_id="oltp_olap_benchmark",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["benchmark", "postgres", "clickhouse"],
)
def oltp_olap_benchmark():
    @task
    def prepare(**context) -> dict:
        conf = _conf(context)
        dataset = require_dataset(conf.get("dataset", "turkestan_109_incidents"))
        if bool(conf.get("skip_prepare", False)):
            return {
                "dataset": dataset,
                "input_file": conf.get("file", ""),
                "file_format": "",
                "prepare_ms": 0.0,
                "source_verify_ms": 0.0,
                "prepared_rows": 0,
                "prepare_cpu_ms": 0.0,
                "prepare_peak_rss_mb": 0.0,
                "extra_json": "{}",
            }
        data_dir = Path(conf.get("data_dir", "/benchmark/data/input"))
        input_file = Path(conf["file"]) if conf.get("file") else find_default_file(dataset, data_dir)
        metrics = BenchmarkMetrics(implementation="airflow-python", dataset=dataset)
        cpu_started = process_cpu_ms()
        with MemorySampler() as sampler:
            _, fmt, rows = prepare_postgres_source(
                pg_config=PostgresConfig(),
                dataset=dataset,
                input_file=input_file,
                sheet=conf.get("sheet"),
                delimiter=conf.get("delimiter"),
                truncate=True,
                limit_rows=int(conf["limit_rows"]) if conf.get("limit_rows") else None,
                metrics=metrics,
            )
        return {
            "dataset": dataset,
            "input_file": str(input_file),
            "file_format": fmt,
            "prepare_ms": metrics.prepare_ms,
            "source_verify_ms": metrics.source_verify_ms,
            "prepared_rows": rows,
            "prepare_cpu_ms": process_cpu_ms() - cpu_started,
            "prepare_peak_rss_mb": sampler.peak_rss / 1024 / 1024,
            "extra_json": metrics.extra_json,
        }

    @task
    def transfer(prepared: dict, **context) -> dict:
        conf = _conf(context)
        metrics = BenchmarkMetrics(
            implementation="airflow-python",
            dataset=prepared["dataset"],
            input_file=prepared["input_file"],
            file_format=prepared["file_format"],
            prepare_ms=prepared["prepare_ms"],
            source_verify_ms=prepared["source_verify_ms"],
            rows=prepared["prepared_rows"],
            cpu_ms=prepared["prepare_cpu_ms"],
            peak_rss_mb=prepared["prepare_peak_rss_mb"],
            extra_json=prepared["extra_json"],
        )
        result = transfer_postgres_to_clickhouse(
            pg_config=PostgresConfig(),
            ch_config=ClickHouseConfig(),
            dataset=prepared["dataset"],
            implementation="airflow-python",
            fetch_size=int(conf.get("fetch_size", 50_000)),
            batch_size=int(conf.get("batch_size", 50_000)),
            input_file=Path(prepared["input_file"]) if prepared.get("input_file") else None,
            file_format=prepared["file_format"],
            truncate_target=not bool(conf.get("keep_target", False)),
            inherited_metrics=metrics,
        )
        return {
            "run_id": result.run_id,
            "dataset": result.dataset,
            "rows": result.rows,
            "total_ms": result.total_ms,
            "rows_per_sec": result.rows_per_sec,
        }

    transfer(prepare())


oltp_olap_benchmark()
