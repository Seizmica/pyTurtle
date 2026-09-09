"""Tests for job declarations and their validation. No Spark required.

`util.runner` imports pyspark only inside functions, so a Job can be declared
and validated without a JVM.
"""

import pytest

from jobs.customer import CUSTOMER
from jobs.orders import ORDERS
from util.config import ConfigError, Settings
from util.runner import Job, Output

ALL_JOBS = [CUSTOMER, ORDERS]


def _job(**overrides):
    base = {
        "name": "t",
        "sql": "SELECT 1",
        "inputs": {"src": "${RAW_ROOT}/src/"},
        "outputs": [Output(path="${CURATED_ROOT}/t/")],
    }
    return Job(**{**base, **overrides})


def _out(**overrides):
    return [Output(**{"path": "${CURATED_ROOT}/t/", **overrides})]


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

        for output in job.outputs:
            settings.resolve(output.path, **params)
        settings.resolve(job.sql, **params)
        for source in job.inputs.values():
            spec = source if isinstance(source, dict) else {"path": source}
            # Whichever kind it is, its template must resolve in this environment
            # — a table name carries ${HIVE_DB} just as a path carries ${RAW_ROOT}.
            settings.resolve(spec.get("path") or spec["table"], **params)


def test_merge_requires_delta():
    with pytest.raises(ConfigError, match="requires format 'delta'"):
        _job(outputs=_out(mode="merge", format="parquet", merge_keys=["id"])).validate()


def test_merge_requires_keys():
    with pytest.raises(ConfigError, match="requires merge_keys"):
        _job(outputs=_out(mode="merge", format="delta")).validate()


def test_unknown_mode_is_rejected():
    with pytest.raises(ConfigError, match="mode must be"):
        _job(outputs=_out(mode="upsert")).validate()


def test_inputs_are_required():
    with pytest.raises(ConfigError, match="at least one input"):
        _job(inputs={}).validate()


# --- outputs: parquet, csv, or both --------------------------------------


@pytest.mark.parametrize("fmt", ["parquet", "csv", "delta"])
def test_supported_output_formats(fmt):
    _job(outputs=_out(format=fmt)).validate()


def test_unsupported_output_format_is_rejected():
    with pytest.raises(ConfigError, match="output format must be one of"):
        _job(outputs=_out(format="avro")).validate()


def test_a_job_can_write_parquet_and_csv_together():
    job = _job(
        outputs=[
            Output(path="${CURATED_ROOT}/t/", format="parquet"),
            Output(path="${EXPORT_ROOT}/t/", format="csv", options={"header": "true"}),
        ]
    )
    job.validate()
    assert [o.format for o in job.outputs] == ["parquet", "csv"]


def test_each_output_carries_its_own_path_and_options():
    job = _job(
        outputs=[
            Output(path="/curated/t/", format="parquet", options={"compression": "snappy"}),
            Output(path="/export/t/", format="csv", options={"header": "true", "sep": "|"}),
        ]
    )
    parquet, csv = job.outputs
    assert parquet.path != csv.path
    assert csv.options["sep"] == "|"
    assert "sep" not in parquet.options


def test_outputs_must_have_distinct_paths():
    """Two formats writing to one path would have the second clobber the first."""
    with pytest.raises(ConfigError, match="distinct paths"):
        _job(
            outputs=[
                Output(path="${CURATED_ROOT}/t/", format="parquet"),
                Output(path="${CURATED_ROOT}/t/", format="csv"),
            ]
        ).validate()


def test_at_least_one_output_is_required():
    with pytest.raises(ConfigError, match="at least one output"):
        _job(outputs=[]).validate()


def test_an_output_needs_a_path():
    with pytest.raises(ConfigError, match="needs a path"):
        _job(outputs=_out(path="")).validate()


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


def test_customer_writes_parquet_and_csv_to_separate_paths():
    parquet, csv = CUSTOMER.outputs

    assert (parquet.format, csv.format) == ("parquet", "csv")
    assert parquet.path != csv.path
    assert csv.options["header"] == "true"
    # Only the curated copy carries a manifest; the hand-off does not need one.
    assert parquet.manifest is True
    assert csv.manifest is False
    assert parquet.partition_by == csv.partition_by == ["run_date"]


def test_orders_upserts_incrementally():
    (output,) = ORDERS.outputs
    assert output.mode == "merge"
    assert output.format == "delta"
    assert output.merge_keys == ["order_id"]


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
