from __future__ import annotations

import json
import base64
from contextlib import nullcontext
from pathlib import Path
from typing import Any
from urllib import request
from urllib.parse import urlencode

import psycopg

from etlbench.config import ClickHouseConfig, PostgresConfig
from etlbench.identifiers import quote_ch_ident, quote_pg_ident, require_dataset
from etlbench.metrics import (
    BenchmarkMetrics,
    MemorySampler,
    insert_metrics,
    now_ms,
    process_cpu_ms,
)
from etlbench.source_loader import source_table_name


class ClickHouseHttp:
    def __init__(self, config: ClickHouseConfig) -> None:
        self.database = config.database
        self.url = f"http://{config.host}:{config.port}/?" + urlencode({"database": config.database})
        token = base64.b64encode(f"{config.user}:{config.password}".encode("utf-8")).decode("ascii")
        self.authorization = f"Basic {token}"

    def command(self, sql: str | bytes) -> str:
        data = sql if isinstance(sql, bytes) else sql.encode("utf-8")
        req = request.Request(
            self.url,
            data=data,
            headers={"Authorization": self.authorization},
            method="POST",
        )
        with request.urlopen(req) as response:
            return response.read().decode("utf-8").strip()

    def query_int(self, sql: str) -> int:
        return int(self.command(sql))


def _pg_columns(pg_config: PostgresConfig, table: str) -> list[str]:
    with psycopg.connect(pg_config.dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = 'oltp_source'
                  AND table_name = %s
                  AND column_name <> 'loaded_at'
                ORDER BY ordinal_position
                """,
                (table,),
            )
            columns = [row[0] for row in cur.fetchall()]
    if not columns:
        raise RuntimeError(f"Postgres source table oltp_source.{table} has no columns")
    return columns


def _create_clickhouse_target(
    ch_config: ClickHouseConfig,
    target_table: str,
    columns: list[str],
    truncate: bool,
):
    client = ClickHouseHttp(ch_config)
    client.command(f"CREATE DATABASE IF NOT EXISTS {quote_ch_ident(ch_config.database)}")
    if truncate:
        client.command(f"DROP TABLE IF EXISTS {quote_ch_ident(target_table)}")
    column_ddl = []
    for column in columns:
        if column == "etl_row_num":
            column_ddl.append(f"{quote_ch_ident(column)} UInt64")
        else:
            column_ddl.append(f"{quote_ch_ident(column)} Nullable(String)")
    client.command(
        f"""
        CREATE TABLE IF NOT EXISTS {quote_ch_ident(target_table)}
        (
            {", ".join(column_ddl)}
        )
        ENGINE = MergeTree
        ORDER BY etl_row_num
        """
    )
    return client


def _to_tab_separated(value: Any) -> str:
    if value is None:
        return r"\N"
    return (
        str(value)
        .replace("\\", "\\\\")
        .replace("\t", r"\t")
        .replace("\n", r"\n")
        .replace("\r", r"\r")
    )


def _write_batch(
    client: ClickHouseHttp,
    target_table: str,
    columns: list[str],
    batch: list[tuple[Any, ...]],
    metrics: BenchmarkMetrics,
) -> None:
    serialize_started = now_ms()
    lines = [
        f"INSERT INTO {quote_ch_ident(target_table)} "
        f"({', '.join(quote_ch_ident(column) for column in columns)}) FORMAT TabSeparated"
    ]
    logical_bytes = 0
    for row in batch:
        lines.append("\t".join(_to_tab_separated(value) for value in row))
        for value in row:
            if value is not None:
                logical_bytes += len(str(value).encode("utf-8"))
    body = ("\n".join(lines) + "\n").encode("utf-8")
    metrics.serialize_ms += now_ms() - serialize_started

    load_started = now_ms()
    client.command(body)
    metrics.load_ms += now_ms() - load_started
    metrics.rows += len(batch)
    metrics.batch_count += 1
    metrics.logical_bytes += logical_bytes


def transfer_postgres_to_clickhouse(
    *,
    pg_config: PostgresConfig,
    ch_config: ClickHouseConfig,
    dataset: str,
    implementation: str,
    fetch_size: int = 50_000,
    batch_size: int = 50_000,
    input_file: Path | None = None,
    file_format: str = "",
    truncate_target: bool = True,
    write_metrics: bool = True,
    inherited_metrics: BenchmarkMetrics | None = None,
    cpu_started: float | None = None,
    total_started: float | None = None,
    memory_sampler: MemorySampler | None = None,
) -> BenchmarkMetrics:
    dataset = require_dataset(dataset)
    source_table = source_table_name(dataset)
    target_table = f"{dataset}_olap"
    metrics = inherited_metrics or BenchmarkMetrics()
    metrics.implementation = implementation
    metrics.benchmark_mode = "full" if inherited_metrics and metrics.prepare_ms > 0 else "transfer"
    metrics.dataset = dataset
    metrics.source_table = f"oltp_source.{source_table}"
    metrics.target_table = f"{ch_config.database}.{target_table}"
    metrics.input_file = str(input_file.resolve()) if input_file else metrics.input_file
    metrics.file_format = file_format or metrics.file_format
    metrics.fetch_size = fetch_size
    metrics.batch_size = batch_size
    prepared_rows = metrics.rows
    metrics.rows = 0

    wall_started = total_started if total_started is not None else now_ms()
    process_cpu_started = cpu_started if cpu_started is not None else process_cpu_ms()

    target_setup_started = now_ms()
    columns = _pg_columns(pg_config, source_table)
    client = _create_clickhouse_target(ch_config, target_table, columns, truncate_target)
    metrics.target_setup_ms += now_ms() - target_setup_started
    select_sql = (
        f"SELECT {', '.join(quote_pg_ident(column) for column in columns)} "
        f"FROM oltp_source.{quote_pg_ident(source_table)} "
        f"WHERE etl_row_num > %s ORDER BY etl_row_num LIMIT %s"
    )
    monitor = MemorySampler() if memory_sampler is None else nullcontext(memory_sampler)
    with monitor as sampler:
        with psycopg.connect(pg_config.dsn, autocommit=True) as conn:
            last_id = 0
            with conn.cursor() as cur:
                while True:
                    fetch_started = now_ms()
                    cur.execute(select_sql, (last_id, fetch_size))
                    rows = cur.fetchall()
                    metrics.extract_ms += now_ms() - fetch_started
                    if not rows:
                        break
                    last_id = int(rows[-1][0])
                    for offset in range(0, len(rows), batch_size):
                        batch = rows[offset : offset + batch_size]
                        _write_batch(client, target_table, columns, batch, metrics)

        verify_started = now_ms()
        target_count = client.query_int(f"SELECT count() FROM {quote_ch_ident(target_table)}")
        metrics.verify_ms += now_ms() - verify_started
        if int(target_count) != metrics.rows:
            raise RuntimeError(
                f"Target row count mismatch for {target_table}: expected {metrics.rows}, got {target_count}"
            )

        metrics.peak_rss_mb = sampler.peak_rss / 1024 / 1024

    metrics.total_ms = now_ms() - wall_started
    if total_started is None:
        metrics.total_ms += metrics.prepare_ms + metrics.source_verify_ms
    metrics.cpu_ms += process_cpu_ms() - process_cpu_started
    measured_ms = (
        metrics.prepare_ms
        + metrics.source_verify_ms
        + metrics.target_setup_ms
        + metrics.extract_ms
        + metrics.serialize_ms
        + metrics.load_ms
        + metrics.verify_ms
    )
    metrics.overhead_ms = max(0.0, metrics.total_ms - measured_ms)
    metrics.finish_rates()
    metrics.set_extra(
        {
            "columns": columns,
            "column_count": len(columns),
            "prepared_rows": prepared_rows,
            "target_count": int(target_count),
            "notes": "Simple pipeline: file -> PostgreSQL COPY -> keyset pages -> ClickHouse HTTP TSV",
            **json.loads(metrics.extra_json or "{}"),
        }
    )
    if write_metrics:
        insert_metrics(ch_config, metrics)
    return metrics
