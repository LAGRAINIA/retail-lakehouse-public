"""
Silver and gold layers with dbt on Databricks, rendered by Cosmos: every dbt model, seed and snapshot
is an Airflow task, followed by its tests (a failing test stops everything downstream).

    dbt_deps -> source_freshness -> dbt (Cosmos task group) -> gdpr_purge_bronze
                                                     \\-> watcher (fails the run if any task failed)
Triggered when the bronze tables have been refreshed (Asset).
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta
from pathlib import Path

from airflow.exceptions import AirflowException
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import Asset, dag, task
from airflow.utils.trigger_rule import TriggerRule
from cosmos import DbtTaskGroup, ExecutionConfig, ProfileConfig, ProjectConfig, RenderConfig
from cosmos.constants import InvocationMode, LoadMode, TestBehavior

BRONZE = Asset("dbx://retail/bronze")

DBT_PROJECT = Path(__file__).parent / "dbt" / "retail"
DBT_EXECUTABLE = os.getenv("DBT_EXECUTABLE", f"{os.environ.get('AIRFLOW_HOME', '/usr/local/airflow')}/dbt_venv/bin/dbt")
DBT_TARGET = os.getenv("DBT_TARGET", "prod")
DBT = f"cd {DBT_PROJECT} && {DBT_EXECUTABLE}"


@dag(
    dag_id="retail_dbt_transform",
    description="dbt build of silver and gold on Databricks (Cosmos)",
    schedule=[BRONZE],
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=2)},
    tags=["retail", "dbt", "cosmos", "databricks"],
    doc_md=__doc__,
)
def retail_dbt_transform():
    dbt_deps = BashOperator(task_id="dbt_deps", bash_command=f"{DBT} deps --profiles-dir .")

    # stops the pipeline when a feed has not been refreshed (error_after in _sources.yml)
    source_freshness = BashOperator(
        task_id="source_freshness",
        bash_command=f"{DBT} source freshness --profiles-dir . --target {DBT_TARGET}",
    )

    dbt = DbtTaskGroup(
        group_id="dbt",
        # deps are installed in the temporary copy Cosmos uses to render and run each task
        project_config=ProjectConfig(DBT_PROJECT, install_dbt_deps=True),
        profile_config=ProfileConfig(
            profile_name="retail",
            target_name=DBT_TARGET,
            profiles_yml_filepath=DBT_PROJECT / "profiles.yml",
        ),
        # dbt runs from its own virtualenv, as a subprocess (it is not installed next to Airflow)
        execution_config=ExecutionConfig(dbt_executable_path=DBT_EXECUTABLE,
                                         invocation_mode=InvocationMode.SUBPROCESS),
        render_config=RenderConfig(
            load_method=LoadMode.DBT_LS,
            test_behavior=TestBehavior.AFTER_EACH,
            should_detach_multiple_parents_tests=True,
            dbt_executable_path=DBT_EXECUTABLE,
            invocation_mode=InvocationMode.SUBPROCESS,
        ),
    )

    # GDPR: erase personal data in the raw layer too, once the new requests have been loaded
    gdpr_purge_bronze = BashOperator(
        task_id="gdpr_purge_bronze",
        bash_command=f"{DBT} run-operation purge_erased_customers --profiles-dir . --target {DBT_TARGET}",
    )

    @task(trigger_rule=TriggerRule.ONE_FAILED, retries=0)
    def watcher():
        """A failure anywhere fails the whole run, even if a final cleanup task succeeds."""
        raise AirflowException("At least one task failed: the run must be marked as failed.")

    dbt_deps >> source_freshness >> dbt >> gdpr_purge_bronze
    [dbt_deps, source_freshness, dbt, gdpr_purge_bronze] >> watcher()


retail_dbt_transform()
