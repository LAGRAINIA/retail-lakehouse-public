"""
Simulated source systems: every day, the POS, e-commerce, ERP, CRM, supply and finance systems drop
their files in the landing volume of Unity Catalog.

- Scheduled run: lands the files of the day that just ended (data interval).
- Manual run with params: land a range of days (history bootstrap) and/or inject an incident.
Files are deterministic per day, so re-running a day re-sends exactly the same files.
"""

from __future__ import annotations

import os
import tempfile
from datetime import date, datetime, timedelta
from pathlib import Path

from airflow.providers.databricks.operators.databricks_sql import DatabricksSqlOperator
from airflow.sdk import Asset, Param, dag, get_current_context, task

from include.ingestion.bronze import setup_statements
from include.retail_generator import config as C
from include.retail_generator.generate import generate_batch
from include.retail_generator.upload import upload_files, volume_root

LANDING = Asset("dbx://retail/landing/raw")

# prod and uat receive a new day every night; dev is fed on demand (RETAIL_SOURCES_SCHEDULE=none)
SCHEDULE = os.getenv("RETAIL_SOURCES_SCHEDULE", "0 1 * * *")


@dag(
    dag_id="retail_source_systems",
    description="Land the daily files of every source system in the Unity Catalog volume",
    schedule=None if SCHEDULE.lower() == "none" else SCHEDULE,
    start_date=datetime.combine(C.EPOCH, datetime.min.time()),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=2)},
    params={
        "start_date": Param(None, type=["null", "string"], format="date",
                            description="First landing day (manual history load). Empty = the scheduled day."),
        "end_date": Param(None, type=["null", "string"], format="date", description="Last landing day."),
        "incidents": Param([], type="array", items={"type": "string", "enum": sorted(C.INCIDENTS)},
                           description="Incidents to inject on these days (see docs/SCENARIOS.md)."),
        "clean": Param(False, type="boolean", description="Disable the built-in data defects."),
    },
    tags=["retail", "sources"],
    doc_md=__doc__,
)
def retail_source_systems():
    # the landing volume must exist before the first upload (a brand-new environment has no catalog yet)
    ensure_landing_zone = DatabricksSqlOperator(
        task_id="ensure_landing_zone",
        databricks_conn_id="databricks_default",
        sql=setup_statements(),
    )

    @task(outlets=[LANDING])
    def land_source_files() -> dict:
        context = get_current_context()
        params = context["params"]
        if params["start_date"]:
            start = date.fromisoformat(params["start_date"])
            end = date.fromisoformat(params["end_date"] or params["start_date"])
        else:
            start = end = context["data_interval_start"].date()

        with tempfile.TemporaryDirectory() as tmp:
            files: list[str] = []
            day = start
            while day <= end:
                files += generate_batch(day, Path(tmp), clean=params["clean"], incidents=tuple(params["incidents"]))
                day += timedelta(days=1)
            uploaded = upload_files(Path(tmp), files, volume_root())

        # the bronze DAG reads these dates from the Asset event to load only these files
        summary = {"start": start.isoformat(), "end": end.isoformat(), "files": uploaded,
                   "incidents": params["incidents"]}
        context["outlet_events"][LANDING].extra = summary
        return summary

    ensure_landing_zone >> land_source_files()


retail_source_systems()
