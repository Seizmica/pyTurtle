"""End-to-end test of the runner against a local SparkSession.

Requires a JDK. Skipped automatically when pyspark cannot start.
"""

import json
from pathlib import Path

import pytest

pyspark = pytest.importorskip("pyspark")

from pyspark.sql import SparkSession

from util.config import Settings
from util.runner import Job, Output, _key, build_spark, run

RUN_DATE = "2026-06-28"


@pytest.fixture(scope="module")
def spark(tmp_path_factory):
    # pyspark being installed does not mean a JVM is available; without a JDK
    # getOrCreate raises JAVA_GATEWAY_EXITED. Skip rather than error, so
    # `pytest tests` is green on a machine that only runs the non-Spark tests.
    warehouse = tmp_path_factory.mktemp("warehouse")
    try:
        session = (
            SparkSession.builder.appName("etl-lite-test")
            .master("local[1]")
            .config("spark.sql.shuffle.partitions", "1")
            .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
            .config("spark.sql.warehouse.dir", str(warehouse))
            .getOrCreate()
        )
    except Exception as exc:  # noqa: BLE001 - any startup failure means "no JVM here"
        pytest.skip(f"Spark could not start, a JDK is required: {exc}")
    yield session
    session.stop()


@pytest.fixture
def settings(tmp_path, spark):
    """Seed raw inputs and point RAW_ROOT / CURATED_ROOT at a temp dir."""
    raw = tmp_path / "raw"
    spark.createDataFrame(
        [(1, "Ann", "gold"), (2, "Bob", "silver")],
        ["customer_id", "name", "segment"],
    ).write.parquet(str(raw / "customers"))
    spark.createDataFrame(
        [(1, 10.0, RUN_DATE), (1, 5.0, RUN_DATE), (2, 20.0, RUN_DATE)],
        ["customer_id", "amount", "ordered_at"],
    ).write.parquet(str(raw / "orders"))

    return Settings(
        environment="dev",
        values={
            "RAW_ROOT": str(raw),
            "CURATED_ROOT": str(tmp_path / "curated"),
            "EXPORT_ROOT": str(tmp_path / "export"),
            "APP_VERSION": "1.4.2",
            "CHECKPOINT_FORMAT": "%Y%m%d%H%M%S",
        },
    )


def test_build_spark_logs_both_versions(spark, settings, caplog):
    """The client version before the attempt, the cluster's after it.

    A client that does not match the cluster fails inside getOrCreate, so the
    first line has to be emitted before it, where it survives the failure.
    """
    job = Job(
        name="probe",
        sql="SELECT 1",
        inputs={"orders": "${RAW_ROOT}/orders/"},
        outputs=[Output(path="${CURATED_ROOT}/probe/")],
    )

    with caplog.at_level("INFO", logger="etl"):
        # getOrCreate returns the module-scoped session, so this asserts on the
        # logging rather than on a second JVM.
        built = build_spark(job, settings)

    assert built is not None
    messages = [record.getMessage() for record in caplog.records]
    building = [m for m in messages if m.startswith("building session")]
    ready = [m for m in messages if m.startswith("session ready")]

    assert building, messages
    assert f"pyspark={pyspark.__version__}" in building[0]
    assert "hive=False delta=False" in building[0]
    assert ready, messages
    assert f"spark={spark.version}" in ready[0]


def _parquet_out(**overrides):
    return Output(
        **{
            "path": "${CURATED_ROOT}/customer/",
            "format": "parquet",
            "partition_by": ["run_date"],
            "manifest": True,
            **overrides,
        }
    )


def _csv_out(**overrides):
    return Output(
        **{
            "path": "${EXPORT_ROOT}/customer/",
            "format": "csv",
            "partition_by": ["run_date"],
            "options": {"header": "true"},
            **overrides,
        }
    )


def _job(**overrides):
    base = {
        "name": "customer",
        "sql": (
            "SELECT c.customer_id, c.name, SUM(o.amount) AS total_spend, "
            "'${run_date}' AS run_date "
            "FROM customers c JOIN orders o ON c.customer_id = o.customer_id "
            "GROUP BY c.customer_id, c.name"
        ),
        "inputs": {
            "customers": "${RAW_ROOT}/customers/",
            "orders": "${RAW_ROOT}/orders/",
        },
        "outputs": [_parquet_out()],
    }
    return Job(**{**base, **overrides})


def _manifest(settings, run_date=RUN_DATE):
    path = Path(settings.values["CURATED_ROOT"]) / "customer" / f"run_date={run_date}"
    return json.loads((path / "manifest.json").read_text(encoding="utf-8"))


def test_key_normalizes_every_uri_form():
    """Regression: a listing URI and input_file_name() must key alike."""
    assert _key("file:/tmp/part-0.parquet") == _key("file:///tmp/part-0.parquet")
    assert _key("s3a://bucket/k/p.parquet") == "bucket/k/p.parquet"


def test_read_sql_write(settings, spark):
    metrics = run(_job(), settings, RUN_DATE, spark=spark)

    assert metrics["output_rows"] == 2
    assert metrics["input_rows"] == {"customers": 2, "orders": 3}

    written = spark.read.parquet(metrics["targets"][0]).orderBy("customer_id").collect()
    assert [r["total_spend"] for r in written] == [15.0, 20.0]


def test_catalog_table_input_joined_to_file_input(settings, spark):
    """The real shape: one input from the metastore, one from files on HDFS."""
    spark.createDataFrame(
        [(1, "Ann", "gold"), (2, "Bob", "silver")],
        ["customer_id", "name", "segment"],
    ).write.mode("overwrite").saveAsTable("customers_tbl")

    job = _job(
        inputs={
            "customers": {"table": "customers_tbl"},
            "orders": "${RAW_ROOT}/orders/",
        }
    )
    metrics = run(job, settings, RUN_DATE, spark=spark)

    assert metrics["input_rows"] == {"customers": 2, "orders": 3}
    assert metrics["output_rows"] == 2
    written = spark.read.parquet(metrics["targets"][0]).orderBy("customer_id").collect()
    assert [r["total_spend"] for r in written] == [15.0, 20.0]


def test_dry_run_writes_nothing(settings, spark):
    metrics = run(
        _job(outputs=[_parquet_out(), _csv_out()]), settings, RUN_DATE, dry_run=True, spark=spark
    )

    assert metrics["output_rows"] == 2
    assert not Path(settings.values["CURATED_ROOT"]).exists()
    assert not Path(settings.values["EXPORT_ROOT"]).exists()


# --- multiple output formats ---------------------------------------------


def test_parquet_and_csv_written_from_one_run(settings, spark):
    """Both formats land, on their own paths, from a single SQL execution."""
    metrics = run(_job(outputs=[_parquet_out(), _csv_out()]), settings, RUN_DATE, spark=spark)

    parquet_path, csv_path = metrics["targets"]
    assert parquet_path != csv_path

    from_parquet = spark.read.parquet(parquet_path).orderBy("customer_id").collect()
    from_csv = spark.read.option("header", "true").csv(csv_path).orderBy("customer_id").collect()

    assert [r["customer_id"] for r in from_parquet] == [1, 2]
    assert [int(r["customer_id"]) for r in from_csv] == [1, 2]
    assert [float(r["total_spend"]) for r in from_csv] == [15.0, 20.0]


def test_csv_output_is_partitioned_like_parquet(settings, spark):
    run(_job(outputs=[_parquet_out(), _csv_out()]), settings, RUN_DATE, spark=spark)

    csv_partition = Path(settings.values["EXPORT_ROOT"]) / "customer" / f"run_date={RUN_DATE}"
    assert csv_partition.is_dir()
    assert any(p.suffix == ".csv" for p in csv_partition.iterdir())


def test_csv_manifest_counts_exclude_the_header_row(settings, spark):
    """Re-reading a header=true csv without the option would inflate every count."""
    csv_with_manifest = _csv_out(
        path="${EXPORT_ROOT}/customer_manifest/",
        manifest=True,
        options={"header": "true"},
    )
    run(_job(outputs=[csv_with_manifest]), settings, RUN_DATE, spark=spark)

    path = (
        Path(settings.values["EXPORT_ROOT"])
        / "customer_manifest"
        / f"run_date={RUN_DATE}"
        / "manifest.json"
    )
    doc = json.loads(path.read_text(encoding="utf-8"))

    # 2 data rows, not 2 + one header line per shard.
    assert doc["row_count"] == 2


def test_one_output_failing_does_not_silently_skip_the_other(settings, spark):
    """A bad path template fails before anything is written, not midway."""
    job = _job(outputs=[_parquet_out(), _csv_out(path="${NOT_DEFINED}/x/")])

    with pytest.raises(Exception, match="NOT_DEFINED"):
        run(job, settings, RUN_DATE, spark=spark)

    assert not Path(settings.values["CURATED_ROOT"]).exists()


def test_manifest_lands_in_the_partition_with_real_counts(settings, spark):
    run(_job(), settings, RUN_DATE, spark=spark)

    doc = _manifest(settings)
    assert doc["run_date"] == RUN_DATE
    assert doc["app_version"] == "1.4.2"
    assert doc["row_count"] == 2
    assert sum(s["row_count"] for s in doc["shards"]) == 2
    assert [s["index"] for s in doc["shards"]] == list(range(len(doc["shards"])))
    for shard in doc["shards"]:
        assert Path(shard["uri"].replace("file:", "")).stat().st_size == shard["size_bytes"]
    assert list(doc) == [
        "run_date",
        "checkpoint_from",
        "checkpoint_to",
        "row_count",
        "app_version",
        "shards",
    ]


def test_manifest_is_never_read_back_as_data(settings, spark):
    """A rerun must not choke on the manifest.json already in the directory."""
    run(_job(), settings, RUN_DATE, spark=spark)
    run(_job(), settings, RUN_DATE, spark=spark)

    doc = _manifest(settings)
    assert doc["row_count"] == 2
    assert all("manifest.json" not in s["uri"] for s in doc["shards"])


def test_checkpoint_chains_across_runs(settings, spark):
    run(_job(), settings, "2026-06-27", spark=spark)
    first = _manifest(settings, "2026-06-27")
    assert first["checkpoint_from"] is None  # no prior manifest

    run(_job(), settings, RUN_DATE, spark=spark)
    second = _manifest(settings)

    assert second["checkpoint_from"] == first["checkpoint_to"]
    # The earlier partition is left untouched by the later run.
    assert _manifest(settings, "2026-06-27") == first
