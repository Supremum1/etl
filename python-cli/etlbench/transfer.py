from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import clickhouse_connect
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


def _logical_size(rows: list[tuple[Any, ...]]) -> int:
    total = 0
    for row in rows:
        for value in row:
            if value is None:
                continue
            total += len(str(value).encode("utf-8"))
    return total


def _create_clickhouse_target(
    ch_config: ClickHouseConfig,
    target_table: str,
    columns: list[str],
    truncate: bool,
):
    client = clickhouse_connect.get_client(
        host=ch_config.host,
        port=ch_config.port,
        username=ch_config.user,
        password=ch_config.password,
        database=ch_config.database,
    )
    client.command(f"CREATE DATABASE IF NOT EXISTS {quote_ch_ident(ch_config.database)}")
    if truncate:
        client.command(f"DROP TABLE IF EXISTS {quote_ch_ident(ch_config.database)}.{quote_ch_ident(target_table)}")
    column_ddl = []
    for column in columns:
        if column == "etl_row_num":
            column_ddl.append(f"{quote_ch_ident(column)} UInt64")
        else:
            column_ddl.append(f"{quote_ch_ident(column)} Nullable(String)")
    client.command(
        f"""
        CREATE TABLE IF NOT EXISTS {quote_ch_ident(ch_config.database)}.{quote_ch_ident(target_table)}
        (
            {", ".join(column_ddl)}
        )
        ENGINE = MergeTree
        ORDER BY etl_row_num
        """
    )
    if truncate:
        client.command(f"TRUNCATE TABLE {quote_ch_ident(ch_config.database)}.{quote_ch_ident(target_table)}")
    return client


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
) -> BenchmarkMetrics:
    dataset = require_dataset(dataset)
    source_table = source_table_name(dataset)
    target_table = f"{dataset}_olap"
    metrics = inherited_metrics or BenchmarkMetrics()
    metrics.implementation = implementation
    metrics.dataset = dataset
    metrics.source_table = f"oltp_source.{source_table}"
    metrics.target_table = f"{ch_config.database}.{target_table}"
    metrics.input_file = str(input_file.resolve()) if input_file else metrics.input_file
    metrics.file_format = file_format or metrics.file_format
    metrics.fetch_size = fetch_size
    metrics.batch_size = batch_size
    prepared_rows = metrics.rows
    metrics.rows = 0

    columns = _pg_columns(pg_config, source_table)
    client = _create_clickhouse_target(ch_config, target_table, columns, truncate_target)
    select_sql = (
        f"SELECT {', '.join(quote_pg_ident(column) for column in columns)} "
        f"FROM oltp_source.{quote_pg_ident(source_table)} ORDER BY etl_row_num"
    )
    total_started = now_ms()
    cpu_started = process_cpu_ms()

    with MemorySampler() as sampler:
        with psycopg.connect(pg_config.dsn) as conn:
            with conn.cursor(name="etl_stream") as cur:
                cur.itersize = fetch_size
                query_started = now_ms()
                cur.execute(select_sql)
                metrics.extract_ms += now_ms() - query_started
                while True:
                    fetch_started = now_ms()
                    rows = cur.fetchmany(fetch_size)
                    metrics.extract_ms += now_ms() - fetch_started
                    if not rows:
                        break
                    for offset in range(0, len(rows), batch_size):
                        batch = rows[offset : offset + batch_size]
                        metrics.rows += len(batch)
                        metrics.batch_count += 1
                        metrics.logical_bytes += _logical_size(batch)
                        insert_started = now_ms()
                        client.insert(target_table, batch, column_names=columns)
                        metrics.load_ms += now_ms() - insert_started

        verify_started = now_ms()
        target_count = client.query(f"SELECT count() FROM {quote_ch_ident(target_table)}").result_rows[0][0]
        metrics.verify_ms += now_ms() - verify_started
        if int(target_count) != metrics.rows:
            raise RuntimeError(
                f"Target row count mismatch for {target_table}: expected {metrics.rows}, got {target_count}"
            )

        metrics.peak_rss_mb = sampler.peak_rss / 1024 / 1024

    metrics.total_ms = now_ms() - total_started + metrics.prepare_ms
    metrics.cpu_ms = process_cpu_ms() - cpu_started
    metrics.finish_rates()
    metrics.set_extra(
        {
            "columns": columns,
            "column_count": len(columns),
            "prepared_rows": prepared_rows,
            "target_count": int(target_count),
            "notes": "Postgres server-side cursor -> ClickHouse batched insert",
            **json.loads(metrics.extra_json or "{}"),
        }
    )
    if write_metrics:
        insert_metrics(ch_config, metrics)
    return metrics
