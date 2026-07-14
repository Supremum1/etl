from __future__ import annotations

import argparse
import json
from dataclasses import asdict
from pathlib import Path

from etlbench.config import ClickHouseConfig, PostgresConfig
from etlbench.identifiers import require_dataset
from etlbench.io_streams import find_default_file
from etlbench.metrics import BenchmarkMetrics
from etlbench.source_loader import prepare_postgres_source
from etlbench.transfer import transfer_postgres_to_clickhouse


def add_common(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--dataset", required=True, help="fact_animals, med_technic, turkestan_109_incidents")
    parser.add_argument("--file", type=Path, help="CSV/XLSX source file. Defaults to data/input/<dataset>.*")
    parser.add_argument("--data-dir", type=Path, default=Path("data/input"))
    parser.add_argument("--sheet", help="XLSX sheet name. Defaults to the first sheet.")
    parser.add_argument("--delimiter", help="CSV delimiter. Defaults to dialect sniffing.")
    parser.add_argument("--fetch-size", type=int, default=50_000)
    parser.add_argument("--batch-size", type=int, default=50_000)
    parser.add_argument("--limit-rows", type=int, help="Prepare only the first N input rows. Useful for smoke tests.")
    parser.add_argument("--keep-target", action="store_true", help="Do not recreate ClickHouse target table.")


def resolve_file(args: argparse.Namespace) -> Path:
    dataset = require_dataset(args.dataset)
    path = args.file if args.file else find_default_file(dataset, args.data_dir)
    if not path.exists():
        raise FileNotFoundError(
            f"Input file not found: {path}. "
            "When running through docker compose, use a path under data/input, "
            "for example data/input/fact_animals.xlsx."
        )
    return path


def command_prepare(args: argparse.Namespace) -> None:
    dataset = require_dataset(args.dataset)
    input_file = resolve_file(args)
    metrics = BenchmarkMetrics(implementation="prepare-postgres", dataset=dataset)
    table, fmt, rows = prepare_postgres_source(
        pg_config=PostgresConfig(),
        dataset=dataset,
        input_file=input_file,
        sheet=args.sheet,
        delimiter=args.delimiter,
        truncate=True,
        limit_rows=args.limit_rows,
        metrics=metrics,
    )
    print(json.dumps({"dataset": dataset, "table": table, "format": fmt, "rows": rows}, ensure_ascii=False))


def command_transfer(args: argparse.Namespace) -> None:
    dataset = require_dataset(args.dataset)
    metrics = transfer_postgres_to_clickhouse(
        pg_config=PostgresConfig(),
        ch_config=ClickHouseConfig(),
        dataset=dataset,
        implementation="python-cli",
        fetch_size=args.fetch_size,
        batch_size=args.batch_size,
        truncate_target=not args.keep_target,
    )
    print(json.dumps(asdict(metrics), ensure_ascii=False, indent=2))


def command_run(args: argparse.Namespace) -> None:
    dataset = require_dataset(args.dataset)
    input_file = resolve_file(args)
    metrics = BenchmarkMetrics(implementation="python-cli", dataset=dataset)
    _, fmt, _ = prepare_postgres_source(
        pg_config=PostgresConfig(),
        dataset=dataset,
        input_file=input_file,
        sheet=args.sheet,
        delimiter=args.delimiter,
        truncate=True,
        limit_rows=args.limit_rows,
        metrics=metrics,
    )
    metrics = transfer_postgres_to_clickhouse(
        pg_config=PostgresConfig(),
        ch_config=ClickHouseConfig(),
        dataset=dataset,
        implementation="python-cli",
        fetch_size=args.fetch_size,
        batch_size=args.batch_size,
        input_file=input_file,
        file_format=fmt,
        truncate_target=not args.keep_target,
        inherited_metrics=metrics,
    )
    print(json.dumps(asdict(metrics), ensure_ascii=False, indent=2))


def main() -> None:
    parser = argparse.ArgumentParser(prog="etlbench", description="OLTP Postgres -> OLAP ClickHouse benchmark")
    subparsers = parser.add_subparsers(dest="command", required=True)

    prepare = subparsers.add_parser("prepare", help="Stream CSV/XLSX into Postgres source table")
    add_common(prepare)
    prepare.set_defaults(func=command_prepare)

    transfer = subparsers.add_parser("transfer", help="Transfer existing Postgres source table into ClickHouse")
    add_common(transfer)
    transfer.set_defaults(func=command_transfer)

    run = subparsers.add_parser("run", help="Prepare source and run Python transfer benchmark")
    add_common(run)
    run.set_defaults(func=command_run)

    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
