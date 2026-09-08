"""Tests for job declarations and their validation. No Spark required.

`util.runner` imports pyspark only inside functions, so a Job can be declared
and validated without a JVM.
"""

import pytest

from jobs.customer import CUSTOMER
from jobs.orders import ORDERS
from util.config import ConfigError, Settings
from util.runner import Job

ALL_JOBS = [CUSTOMER, ORDERS]


def _job(**overrides):
    base = {
        "name": "t",
        "sql": "SELECT 1",
        "inputs": {"src": "${RAW_ROOT}/src/"},
        "output": "${CURATED_ROOT}/t/",
    }
    return Job(**{**base, **overrides})


@pytest.mark.parametrize("job", ALL_JOBS, ids=lambda j: j.name)
def test_shipped_jobs_are_valid(job):
    job.validate()


@pytest.mark.parametrize("job", ALL_JOBS, ids=lambda j: j.name)
def test_shipped_jobs_resolve_against_every_environment(job):
    """A path or SQL token that no .env file defines must not reach production."""
    for environment in ("dev", "uat", "preprod", "prod"):
        import util.config as config

        settings = config.load(environment)
        params = {"run_date": "2026-06-28", **settings.values}

        settings.resolve(job.output, **params)
        settings.resolve(job.sql, **params)
        for source in job.inputs.values():
            spec = source if isinstance(source, dict) else {"path": source}
            # Whichever kind it is, its template must resolve in this environment
            # — a table name carries ${HIVE_DB} just as a path carries ${RAW_ROOT}.
            settings.resolve(spec.get("path") or spec["table"], **params)


def test_merge_requires_delta():
    with pytest.raises(ConfigError, match="requires format 'delta'"):
        _job(mode="merge", format="parquet", merge_keys=["id"]).validate()


def test_merge_requires_keys():
    with pytest.raises(ConfigError, match="requires merge_keys"):
        _job(mode="merge", format="delta").validate()


def test_unknown_mode_is_rejected():
    with pytest.raises(ConfigError, match="mode must be"):
        _job(mode="upsert").validate()


def test_inputs_are_required():
    with pytest.raises(ConfigError, match="at least one input"):
        _job(inputs={}).validate()


# --- mixed sources: HDFS files and Hive tables ---------------------------


def test_a_table_input_is_accepted():
    _job(inputs={"customers": {"table": "analytics.customers"}}).validate()


def test_files_and_tables_can_be_mixed_in_one_job():
    job = _job(
        inputs={
            "customers": {"table": "${HIVE_DB}.customers"},
            "orders": "hdfs://nn/raw/orders/",
        }
    )
    job.validate()
    assert job.reads_hive() is True


def test_a_file_only_job_does_not_request_hive():
    assert _job(inputs={"orders": "hdfs://nn/raw/orders/"}).reads_hive() is False


def test_a_source_needs_exactly_one_of_path_or_table():
    with pytest.raises(ConfigError, match="exactly one of 'path' or 'table'"):
        _job(inputs={"x": {"path": "/a", "table": "db.t"}}).validate()

    with pytest.raises(ConfigError, match="exactly one of 'path' or 'table'"):
        _job(inputs={"x": {"format": "parquet"}}).validate()


def test_format_and_options_are_rejected_on_a_table():
    """The metastore describes a table; a read format would be silently ignored."""
    with pytest.raises(ConfigError, match="'format' and 'options' do not apply"):
        _job(inputs={"x": {"table": "db.t", "format": "parquet"}}).validate()


def test_customer_reads_a_hive_table_and_hdfs_files():
    assert CUSTOMER.reads_hive() is True
    assert CUSTOMER.inputs["customers"] == {"table": "${HIVE_DB}.customers"}
    assert "path" in CUSTOMER.inputs["orders"]


def test_customer_writes_a_partitioned_manifest():
    assert CUSTOMER.partition_by == ["run_date"]
    assert CUSTOMER.manifest is True


def test_orders_upserts_incrementally():
    assert ORDERS.mode == "merge"
    assert ORDERS.format == "delta"
    assert ORDERS.merge_keys == ["order_id"]


def test_sql_references_only_declared_views():
    """Every view a job's SQL reads from must be registered as an input."""
    for job in ALL_JOBS:
        sql = job.sql.lower()
        for view in job.inputs:
            assert view.lower() in sql, f"{job.name}: input '{view}' unused in SQL"


def test_run_date_reaches_the_sql():
    settings = Settings(environment="dev", values={})
    rendered = settings.resolve(CUSTOMER.sql, run_date="2026-06-28")

    assert "2026-06-28" in rendered
    assert "${run_date}" not in rendered
