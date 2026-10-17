"""
Bronze layer: load raw landing files into Delta tables with COPY INTO.

- COPY INTO is idempotent: files already loaded are skipped, so re-running a day is safe.
- Everything lands as text (CSV) or as-is (JSON): typing and cleaning happen in dbt (silver).
- mergeSchema absorbs new columns (schema drift) instead of failing the load.
- Each row keeps its lineage: source file, file modification time, ingestion time.
- Every COPY INTO targets the files of a few landing days only (PATTERN on the date in the file name).
  Without it, COPY INTO lists the whole folder and, with mergeSchema, reads the header of every CSV
  while compiling: with ~1,300 small POS files it never got past compilation (classic small-files issue).
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import date, timedelta


@dataclass(frozen=True)
class BronzeSource:
    table: str
    path: str          # folder under the landing volume
    file_format: str   # CSV or JSON


SOURCES = [
    BronzeSource("erp_stores", "erp/stores", "CSV"),
    BronzeSource("erp_products", "erp/products", "CSV"),
    BronzeSource("crm_customers", "crm/customers", "JSON"),
    BronzeSource("crm_gdpr_requests", "crm/gdpr_requests", "CSV"),
    BronzeSource("pos_sales", "pos/sales", "CSV"),
    BronzeSource("ecom_orders", "ecom/orders", "JSON"),
    BronzeSource("ecom_returns", "ecom/returns", "CSV"),
    BronzeSource("supply_inventory", "supply/inventory", "CSV"),
    BronzeSource("finance_fx_rates", "finance/fx_rates", "JSON"),
]

FORMAT_OPTIONS = {
    "CSV": "'header' = 'true', 'inferSchema' = 'false', 'mergeSchema' = 'true'",
    "JSON": "'primitivesAsString' = 'true', 'mergeSchema' = 'true'",
}


def catalog_name() -> str:
    return os.getenv("DATABRICKS_CATALOG", "retail")


def setup_statements(catalog: str | None = None) -> list[str]:
    c = catalog or catalog_name()
    return [
        f"CREATE CATALOG IF NOT EXISTS {c}",
        f"CREATE SCHEMA IF NOT EXISTS {c}.landing COMMENT 'Raw files dropped by the source systems'",
        f"CREATE VOLUME IF NOT EXISTS {c}.landing.raw",
        f"CREATE SCHEMA IF NOT EXISTS {c}.bronze COMMENT 'Raw data loaded as-is, one table per feed'",
    ]


def landing_days(start: date, end: date) -> list[date]:
    """Days whose files may have landed between start and end. Starts one day earlier: a POS file
    arriving late is named after its business day (the day before)."""
    return [start - timedelta(days=1) + timedelta(days=i) for i in range((end - start).days + 2)]


def file_pattern(days: list[date]) -> str:
    stamps = ",".join(d.strftime("%Y%m%d") for d in days)
    return f"*_{{{stamps}}}*" if len(days) > 1 else f"*_{stamps}*"


def copy_into_statements(source: BronzeSource, days: list[date], catalog: str | None = None,
                         chunk_size: int = 5) -> list[str]:
    """CREATE the table if needed, then one COPY INTO per chunk of landing days."""
    c = catalog or catalog_name()
    table = f"{c}.bronze.{source.table}"
    chunks = [days[i:i + chunk_size] for i in range(0, len(days), chunk_size)]
    return [f"CREATE TABLE IF NOT EXISTS {table}"] + [
        f"""
COPY INTO {table}
FROM (
    SELECT
        *,
        _metadata.file_path              AS _source_file,
        _metadata.file_modification_time AS _file_modified_at,
        current_timestamp()              AS _ingested_at
    FROM '/Volumes/{c}/landing/raw/{source.path}/'
)
FILEFORMAT = {source.file_format}
PATTERN = '{file_pattern(chunk)}'
FORMAT_OPTIONS ({FORMAT_OPTIONS[source.file_format]})
COPY_OPTIONS ('mergeSchema' = 'true')
""".strip()
        for chunk in chunks
    ]
