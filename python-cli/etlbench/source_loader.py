from __future__ import annotations

from pathlib import Path

import psycopg

from etlbench.config import PostgresConfig
from etlbench.identifiers import normalize_headers, quote_pg_ident, require_dataset
from etlbench.io_streams import iter_input
from etlbench.metrics import BenchmarkMetrics, now_ms


def _align_row(row: list[str | None], width: int) -> list[str | None]:
    if len(row) == width:
        return row
    if len(row) > width:
        return row[:width]
    return row + [None] * (width - len(row))


def source_table_name(dataset: str) -> str:
    return require_dataset(dataset)


def prepare_postgres_source(
    *,
    pg_config: PostgresConfig,
    dataset: str,
    input_file: Path,
    sheet: str | None = None,
    delimiter: str | None = None,
    truncate: bool = True,
    limit_rows: int | None = None,
    metrics: BenchmarkMetrics | None = None,
) -> tuple[str, str, int]:
    dataset = require_dataset(dataset)
    table = source_table_name(dataset)
    input_file = input_file.resolve()
    fmt, headers, rows = iter_input(input_file, sheet=sheet, delimiter=delimiter)
    columns = normalize_headers(headers)
    started = now_ms()
    row_count = 0

    with psycopg.connect(pg_config.dsn, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute("CREATE SCHEMA IF NOT EXISTS oltp_source")
            if truncate:
                cur.execute(f"DROP TABLE IF EXISTS oltp_source.{quote_pg_ident(table)}")
            cur.execute(
                f"""
                CREATE TABLE IF NOT EXISTS oltp_source.{quote_pg_ident(table)}
                (
                    etl_row_num BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                    {", ".join(f"{quote_pg_ident(col)} TEXT" for col in columns)},
                    loaded_at TIMESTAMPTZ DEFAULT now()
                )
                """
            )
            if truncate:
                cur.execute(f"TRUNCATE TABLE oltp_source.{quote_pg_ident(table)} RESTART IDENTITY")

            copy_columns = ", ".join(quote_pg_ident(col) for col in columns)
            with cur.copy(
                f"COPY oltp_source.{quote_pg_ident(table)} ({copy_columns}) FROM STDIN"
            ) as copy:
                for row in rows:
                    if limit_rows is not None and row_count >= limit_rows:
                        break
                    copy.write_row(_align_row(row, len(columns)))
                    row_count += 1

    if metrics:
        metrics.dataset = dataset
        metrics.source_table = f"oltp_source.{table}"
        metrics.input_file = str(input_file)
        metrics.file_format = fmt
        metrics.rows = row_count
        metrics.prepare_ms += now_ms() - started
        metrics.set_extra({"source_columns": columns, "limit_rows": limit_rows})

    return table, fmt, row_count
