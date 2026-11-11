"""Tests for the orders feed (jobs/orders.py). No Spark required."""

import runpy

import pytest

from jobs.orders import ORDERS
from util.config import Settings

RUN_DATE = "2026-06-28"


def test_orders_is_valid():
    ORDERS.validate()


def test_orders_reads_files_only():
    assert ORDERS.reads_hive() is False
    assert ORDERS.inputs == {"orders": "${RAW_ROOT}/orders/"}


def test_orders_upserts_incrementally():
    (output,) = ORDERS.outputs
    assert output.mode == "merge"
    assert output.format == "delta"
    assert output.merge_keys == ["order_id"]


def test_orders_output_is_partitioned_by_run_date_without_manifest():
    (output,) = ORDERS.outputs

    assert output.partition_by == ["run_date"]
    assert output.manifest is False


def test_orders_sql_selects_the_merge_key():
    """A merge on a column the SQL does not produce would fail at write time."""
    (output,) = ORDERS.outputs
    for key in output.merge_keys:
        assert key in ORDERS.sql


def test_orders_sql_filters_from_run_date():
    rendered = Settings(environment="dev", values={}).resolve(ORDERS.sql, run_date=RUN_DATE)

    assert f"o.ordered_at >= '{RUN_DATE}'" in rendered
    assert "${" not in rendered


@pytest.mark.filterwarnings("ignore::RuntimeWarning")  # runpy re-executes an imported module
def test_orders_script_entry_point_delegates_to_main(monkeypatch):
    calls = []

    def fake_main(job):
        calls.append(job)
        return 0

    monkeypatch.setattr("util.runner.main", fake_main)

    with pytest.raises(SystemExit) as exited:
        runpy.run_module("jobs.orders", run_name="__main__")

    assert exited.value.code == 0
    assert [job.name for job in calls] == ["orders"]
