"""DAG integrity: every DAG imports, and the pipeline guarantees we rely on are in place."""

import os
from pathlib import Path

import pytest
from airflow.dag_processing.dagbag import DagBag

DAGS = Path(__file__).resolve().parents[2] / "dags"


@pytest.fixture(scope="module")
def dagbag():
    return DagBag(str(DAGS), include_examples=False)


def test_no_import_errors(dagbag):
    assert dagbag.import_errors == {}


def test_expected_dags(dagbag):
    assert {"retail_source_systems", "retail_ingest_bronze", "retail_dbt_transform"} <= set(dagbag.dag_ids)


def test_every_task_retries(dagbag):
    for dag in dagbag.dags.values():
        for t in dag.tasks:
            if t.task_id != "watcher":
                assert t.retries >= 1, f"{dag.dag_id}.{t.task_id} has no retry"


def test_dbt_failures_fail_the_run(dagbag):
    dag = dagbag.dags["retail_dbt_transform"]
    watcher = dag.get_task("watcher")
    assert watcher.trigger_rule == "one_failed"
    assert not watcher.downstream_task_ids, "the watcher must be a leaf to decide the run state"
