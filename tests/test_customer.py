"""Tests for the customer feed (jobs/customer.py). No Spark required."""

import runpy

import pytest

from jobs.customer import CUSTOMER
from util.config import Settings

RUN_DATE = "2026-06-28"


def test_customer_is_valid():
    CUSTOMER.validate()


def test_customer_reads_a_hive_table_and_hdfs_files():
    assert CUSTOMER.reads_hive() is True
    assert CUSTOMER.inputs["customers"] == {"table": "${HIVE_DB}.customers"}
    assert "path" in CUSTOMER.inputs["orders"]


def test_customer_orders_input_is_parquet_with_merged_schema():
    orders = CUSTOMER.inputs["orders"]

    assert orders["format"] == "parquet"
    assert orders["options"] == {"mergeSchema": "true"}


def test_customer_writes_parquet_and_csv_to_separate_paths():
    parquet, csv = CUSTOMER.outputs

    assert (parquet.format, csv.format) == ("parquet", "csv")
    assert parquet.path != csv.path
    assert csv.options["header"] == "true"
    # Only the curated copy carries a manifest; the hand-off does not need one.
    assert parquet.manifest is True
    assert csv.manifest is False
    assert parquet.partition_by == csv.partition_by == ["run_date"]


def test_customer_outputs_overwrite_their_partition():
    assert [o.mode for o in CUSTOMER.outputs] == ["overwrite", "overwrite"]


def test_customer_output_compression():
    parquet, csv = CUSTOMER.outputs

    assert parquet.options["compression"] == "snappy"
    assert csv.options["compression"] == "gzip"


def test_customer_sql_renders_run_date():
    rendered = Settings(environment="dev", values={}).resolve(CUSTOMER.sql, run_date=RUN_DATE)

    assert f"'{RUN_DATE}'" in rendered
    assert "${" not in rendered


@pytest.mark.filterwarnings("ignore::RuntimeWarning")  # runpy re-executes an imported module
def test_customer_script_entry_point_delegates_to_main(monkeypatch):
    calls = []

    def fake_main(job):
        calls.append(job)
        return 7

    monkeypatch.setattr("util.runner.main", fake_main)

    with pytest.raises(SystemExit) as exited:
        runpy.run_module("jobs.customer", run_name="__main__")

    assert exited.value.code == 7
    assert [job.name for job in calls] == ["customer"]
