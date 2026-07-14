from __future__ import annotations

import re
from collections import Counter

VALID_DATASETS = {"fact_animals", "med_technic", "turkestan_109_incidents"}
RESERVED_COLUMNS = {"etl_row_num", "load_run_id", "loaded_at"}


def normalize_identifier(value: str, fallback: str = "col") -> str:
    value = (value or "").strip().lower()
    value = re.sub(r"[^a-z0-9_]+", "_", value)
    value = re.sub(r"_+", "_", value).strip("_")
    if not value:
        value = fallback
    if value[0].isdigit():
        value = f"{fallback}_{value}"
    if value in RESERVED_COLUMNS:
        value = f"src_{value}"
    return value[:63]


def normalize_headers(headers: list[str]) -> list[str]:
    result: list[str] = []
    seen: Counter[str] = Counter()
    for index, header in enumerate(headers, start=1):
        base = normalize_identifier(str(header or ""), fallback=f"col_{index}")
        seen[base] += 1
        if seen[base] == 1:
            result.append(base)
        else:
            suffix = f"_{seen[base]}"
            result.append(f"{base[:63 - len(suffix)]}{suffix}")
    return result


def quote_pg_ident(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def quote_ch_ident(value: str) -> str:
    return "`" + value.replace("`", "``") + "`"


def require_dataset(dataset: str) -> str:
    normalized = normalize_identifier(dataset, fallback="dataset")
    if normalized not in VALID_DATASETS:
        allowed = ", ".join(sorted(VALID_DATASETS))
        raise ValueError(f"Unknown dataset '{dataset}'. Allowed values: {allowed}")
    return normalized
