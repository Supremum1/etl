from __future__ import annotations

import json
from contextlib import nullcontext
from pathlib import Path
from typing import Any

import psycopg
from clickhouse_driver import Client

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


def _new_clickhouse_client(config: ClickHouseConfig) -> Client:
    return Client(
        host=config.host,
        port=config.port,
        user=config.user,
        password=config.password,
        database=config.database,
        compression=True,
    )


def _pg_columns(conn: psycopg.Connection, table: str) -> list[str]:
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
        columns = [row[0] for row in cur]
    if not columns:
        raise RuntimeError(f"Postgres source table oltp_source.{table} has no columns")
    return columns


def _create_clickhouse_target(
    client: Client,
    target_table: str,
    columns: list[str],
    truncate: bool,
) -> None:
    if truncate:
        client.execute(f"DROP TABLE IF EXISTS {quote_ch_ident(target_table)}")
    column_ddl = [
        f"{quote_ch_ident(column)} UInt64"
        if column == "etl_row_num"
        else f"{quote_ch_ident(column)} Nullable(String)"
        for column in columns
    ]
    client.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {quote_ch_ident(target_table)}
        (
            {", ".join(column_ddl)}
        )
        ENGINE = MergeTree
        ORDER BY etl_row_num
        """
    )


def _write_batch(
    client: Client,
    target_table: str,
    columns: list[str],
    batch: list[tuple[Any, ...]],
    metrics: BenchmarkMetrics,
) -> None:
    load_started = now_ms()
    insert_sql = (
        f"INSERT INTO {quote_ch_ident(target_table)} "
        f"({', '.join(quote_ch_ident(column) for column in columns)}) VALUES"
    )
    client.execute(insert_sql, batch)
    metrics.load_ms += now_ms() - load_started
    metrics.rows += len(batch)
    metrics.batch_count += 1


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
    monitor = MemorySampler() if memory_sampler is None else nullcontext(memory_sampler)
    client = _new_clickhouse_client(ch_config)

    try:
        with monitor as sampler:
            with psycopg.connect(pg_config.dsn) as conn:
                target_setup_started = now_ms()
                columns = _pg_columns(conn, source_table)
                _create_clickhouse_target(client, target_table, columns, truncate_target)
                metrics.target_setup_ms += now_ms() - target_setup_started

                select_sql = (
                    f"SELECT {', '.join(quote_pg_ident(column) for column in columns)} "
                    f"FROM oltp_source.{quote_pg_ident(source_table)} ORDER BY etl_row_num"
                )
                with conn.cursor(name="oltp_olap_transfer") as cur:
                    cur.execute(select_sql)
                    while True:
                        extract_started = now_ms()
                        rows = cur.fetchmany(fetch_size)
                        metrics.extract_ms += now_ms() - extract_started
                        if not rows:
                            break

                        metrics.logical_bytes += sum(
                            len(str(value).encode("utf-8"))
                            for row in rows
                            for value in row
                            if value is not None
                        )
                        for offset in range(0, len(rows), batch_size):
                            _write_batch(
                                client,
                                target_table,
                                columns,
                                rows[offset : offset + batch_size],
                                metrics,
                            )

            verify_started = now_ms()
            target_count = int(
                client.execute(f"SELECT count() FROM {quote_ch_ident(target_table)}")[0][0]
            )
            metrics.verify_ms += now_ms() - verify_started
            if target_count != metrics.rows:
                raise RuntimeError(
                    f"Target row count mismatch for {target_table}: "
                    f"expected {metrics.rows}, got {target_count}"
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
                "target_count": target_count,
                "clickhouse_client": "clickhouse-driver",
                "clickhouse_transport": "Native protocol over TCP port 9000",
                "notes": "One PostgreSQL server cursor -> ClickHouse Native binary blocks",
                **json.loads(metrics.extra_json or "{}"),
            }
        )
        if write_metrics:
            insert_metrics(ch_config, metrics, client=client)
        return metrics
    finally:
        client.disconnect()
