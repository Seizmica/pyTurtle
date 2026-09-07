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
            path = source["path"] if isinstance(source, dict) else source
            settings.resolve(path, **params)


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
