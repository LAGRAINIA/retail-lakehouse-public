"""
Run the bronze layer from the command line, without Airflow (handy to iterate):

    python -m include.ingestion setup                                  # catalog, schemas, landing volume
    python -m include.ingestion load --start 2026-07-01 --end 2026-10-16
    python -m include.ingestion counts                                 # rows per bronze table
"""

from __future__ import annotations

import argparse
import os
import time
from datetime import date

from .bronze import SOURCES, catalog_name, copy_into_statements, landing_days, setup_statements


def connect():
    from databricks import sql

    return sql.connect(server_hostname=os.environ["DATABRICKS_HOST"].removeprefix("https://"),
                       http_path=os.environ["DATABRICKS_HTTP_PATH"],
                       access_token=os.environ["DATABRICKS_TOKEN"])


def main() -> None:
    parser = argparse.ArgumentParser(prog="ingestion")
    parser.add_argument("command", choices=["setup", "load", "counts"])
    parser.add_argument("--start", type=date.fromisoformat, default=date.today())
    parser.add_argument("--end", type=date.fromisoformat)
    parser.add_argument("--table", action="append", help="only these bronze tables (repeatable)")
    args = parser.parse_args()
    sources = [s for s in SOURCES if not args.table or s.table in args.table]

    with connect() as conn, conn.cursor() as cur:
        if args.command == "setup":
            for stmt in setup_statements():
                cur.execute(stmt)
                print("ok:", stmt)
        elif args.command == "load":
            days = landing_days(args.start, args.end or args.start)
            for source in sources:
                started, inserted = time.time(), 0
                for stmt in copy_into_statements(source, days):
                    cur.execute(stmt)
                    rows = cur.fetchall()
                    if stmt.startswith("COPY INTO") and rows:
                        inserted += rows[0].num_inserted_rows
                print(f"{source.table:20} +{inserted:>9} rows  ({time.time() - started:.0f}s)", flush=True)
        else:
            for source in sources:
                cur.execute(f"SELECT count(*) FROM {catalog_name()}.bronze.{source.table}")
                print(f"{source.table:20} {cur.fetchone()[0]:>10}")


if __name__ == "__main__":
    main()
