"""
Bronze layer: COPY INTO every raw feed from the landing volume into Delta tables.

- Triggered by the landing Asset (data-aware scheduling). The Asset event says which days landed,
  so each COPY INTO only targets those files (see include/ingestion/bronze.py for why).
- One mapped task per feed (dynamic task mapping), a few at a time to spare the warehouse.
- COPY INTO skips files it already loaded: re-running is safe.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta

from airflow.providers.databricks.operators.databricks_sql import DatabricksSqlOperator
from airflow.sdk import Asset, dag, get_current_context, task

from include.ingestion.bronze import SOURCES, copy_into_statements, landing_days, setup_statements

LANDING = Asset("dbx://retail/landing/raw")
BRONZE = Asset("dbx://retail/bronze")
CONN_ID = "databricks_default"


@dag(
    dag_id="retail_ingest_bronze",
    description="COPY INTO the bronze Delta tables from the landing volume",
    schedule=[LANDING],
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=2)},
    tags=["retail", "bronze", "databricks"],
    doc_md=__doc__,
)
def retail_ingest_bronze():
    setup = DatabricksSqlOperator(
        task_id="create_catalog_objects",
        databricks_conn_id=CONN_ID,
        sql=setup_statements(),
    )

    @task
    def plan_loads() -> list[list[str]]:
        """Landing days from the triggering Asset events (or yesterday and today on a manual run)."""
        context = get_current_context()
        days: set[date] = set()
        for events in context["triggering_asset_events"].values():
            for event in events:
                if event.extra.get("start"):
                    days.update(landing_days(date.fromisoformat(event.extra["start"]),
                                             date.fromisoformat(event.extra["end"])))
        if not days:
            days.update(landing_days(context["data_interval_end"].date(), context["data_interval_end"].date()))
        ordered = sorted(days)
        print(f"Loading {len(ordered)} landing days: {ordered[0]} -> {ordered[-1]}")
        return [copy_into_statements(source, ordered) for source in SOURCES]

    loads = DatabricksSqlOperator.partial(
        task_id="copy_into",
        databricks_conn_id=CONN_ID,
        max_active_tis_per_dagrun=3,
        map_index_template="{{ task.sql[0].split('.')[-1] }}",   # shows the table name in the UI
        outlets=[BRONZE],
    ).expand(sql=plan_loads())

    setup >> loads


retail_ingest_bronze()
