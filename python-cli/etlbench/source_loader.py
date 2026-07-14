from __future__ import annotations

import csv
from pathlib import Path

import psycopg

from etlbench.config import PostgresConfig
from etlbench.identifiers import normalize_headers, quote_pg_ident, require_dataset
from etlbench.io_streams import detect_file, iter_input
from etlbench.metrics import BenchmarkMetrics, now_ms


def _align_row(row: list[str | None], width: int) -> list[str | None]:
    if len(row) == width:
        return row
    if len(row) > width:
        return row[:width]
    return row + [None] * (width - len(row))


def source_table_name(dataset: str) -> str:
    return require_dataset(dataset)


class NulStrippingReader:
    def __init__(self, handle, chunk_size: int = 1024 * 1024) -> None:
        self.handle = handle
        self.chunk_size = chunk_size

    def read(self, size: int = -1) -> bytes:
        chunk = self.handle.read(self.chunk_size if size is None or size < 0 else size)
        return chunk.replace(b"\x00", b"") if chunk else chunk


def _create_source_table(cur, table: str, columns: list[str], truncate: bool) -> None:
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


def _copy_delimiter_sql(delimiter: str | None) -> str:
    if not delimiter or delimiter == ",":
        return ""
    if len(delimiter) != 1:
        raise ValueError("--delimiter must be a single character")
    if delimiter == "\t":
        return ", DELIMITER E'\\t'"
    escaped = delimiter.replace("\\", "\\\\").replace("'", "''")
    return f", DELIMITER '{escaped}'"


def _read_csv_headers(path: Path, delimiter: str | None) -> list[str]:
    with path.open("r", newline="", encoding="utf-8-sig") as handle:
        reader = csv.reader(handle, delimiter=delimiter or ",")
        return next(reader)


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
    fmt = detect_file(input_file)
    started = now_ms()
    row_count = 0

    with psycopg.connect(pg_config.dsn, autocommit=True) as conn:
        with conn.cursor() as cur:
            if fmt == "csv" and limit_rows is None:
                columns = normalize_headers(_read_csv_headers(input_file, delimiter))
                _create_source_table(cur, table, columns, truncate)
                copy_columns = ", ".join(quote_pg_ident(col) for col in columns)
                copy_sql = (
                    f"COPY oltp_source.{quote_pg_ident(table)} ({copy_columns}) "
                    f"FROM STDIN WITH (FORMAT csv, HEADER true{_copy_delimiter_sql(delimiter)})"
                )
                with input_file.open("rb") as raw:
                    with cur.copy(copy_sql) as copy:
                        reader = NulStrippingReader(raw)
                        while chunk := reader.read():
                            copy.write(chunk)
                prepare_mode = "csv-direct-copy"
            else:
                _, headers, rows = iter_input(input_file, sheet=sheet, delimiter=delimiter)
                columns = normalize_headers(headers)
                _create_source_table(cur, table, columns, truncate)
                copy_columns = ", ".join(quote_pg_ident(col) for col in columns)
                with cur.copy(
                    f"COPY oltp_source.{quote_pg_ident(table)} ({copy_columns}) FROM STDIN"
                ) as copy:
                    for row in rows:
                        if limit_rows is not None and row_count >= limit_rows:
                            break
                        copy.write_row(_align_row(row, len(columns)))
                        row_count += 1
                prepare_mode = "row-copy"

            prepare_ms = now_ms() - started
            verify_started = now_ms()
            cur.execute(f"SELECT count(*) FROM oltp_source.{quote_pg_ident(table)}")
            row_count = int(cur.fetchone()[0])
            source_verify_ms = now_ms() - verify_started

    if metrics:
        metrics.dataset = dataset
        metrics.source_table = f"oltp_source.{table}"
        metrics.input_file = str(input_file)
        metrics.file_format = fmt
        metrics.rows = row_count
        metrics.prepare_ms += prepare_ms
        metrics.source_verify_ms += source_verify_ms
        metrics.set_extra({"source_columns": columns, "limit_rows": limit_rows, "prepare_mode": prepare_mode})

    return table, fmt, row_count
