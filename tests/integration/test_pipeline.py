"""Integration test running the pipeline against a local SparkSession."""

import hashlib
from pathlib import Path

import pytest

pyspark = pytest.importorskip("pyspark")

from pyspark.sql import SparkSession

from etl_framework.config.schema import (
    AudOutputSpec,
    DataQualitySpec,
    InputSpec,
    JobConfig,
    JobMeta,
    OutputSpec,
    QualityCheck,
    SparkSpec,
)
from etl_framework.core.pipeline import run_pipeline


@pytest.fixture(scope="module")
def spark():
    s = (
        SparkSession.builder.appName("test")
        .master("local[1]")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )
    yield s
    s.stop()


def test_end_to_end(tmp_path, spark):
    # Seed input parquet datasets.
    customers = spark.createDataFrame([(1, "Ann"), (2, "Bob")], ["customer_id", "name"])
    orders = spark.createDataFrame([(1, 10.0), (1, 5.0), (2, 20.0)], ["customer_id", "amount"])
    cust_path = str(tmp_path / "customers")
    ord_path = str(tmp_path / "orders")
    customers.write.parquet(cust_path)
    orders.write.parquet(ord_path)

    sql_file = tmp_path / "q.sql"
    sql_file.write_text(
        "SELECT c.customer_id, c.name, SUM(o.amount) AS total_spend, "
        "'${run_date}' AS run_date FROM customers c "
        "JOIN orders o ON c.customer_id = o.customer_id "
        "GROUP BY c.customer_id, c.name"
    )
    out_path = str(tmp_path / "out")

    cfg = JobConfig(
        job=JobMeta(name="customer_daily"),
        sql_file=str(sql_file),
        sql_params={"run_date": "2026-07-09"},
        inputs=[
            InputSpec(table="customers", path=cust_path, format="parquet"),
            InputSpec(table="orders", path=ord_path, format="parquet"),
        ],
        output=OutputSpec(
            path=out_path,
            format="parquet",
            mode="overwrite",
            expected_schema={"name": "string", "run_date": "string"},
        ),
        spark=SparkSpec(app_name="test", master="local[1]"),
        data_quality=DataQualitySpec(
            enabled=True,
            fail_on_error=True,
            checks=[
                QualityCheck(type="not_null", columns=["customer_id"]),
                QualityCheck(type="unique", columns=["customer_id"]),
                QualityCheck(type="row_count_min", value=1),
                QualityCheck(
                    type="referential",
                    columns=["customer_id"],
                    ref_table="customers",
                    ref_column="customer_id",
                ),
                QualityCheck(
                    type="custom",
                    expression="total_spend >= 0",
                    name="non_negative_spend",
                ),
            ],
        ),
    )

    metrics = run_pipeline(cfg, dry_run=False, spark=spark)
    assert metrics.output_rows == 2
    assert metrics.input_rows == {"customers": 2, "orders": 3}

    result = spark.read.parquet(out_path).orderBy("customer_id").collect()
    assert result[0]["total_spend"] == 15.0
    assert result[1]["total_spend"] == 20.0


def test_aud_checksums_written(tmp_path, spark):
    """The .aud manifest lists a matching MD5 for every written part file."""
    source = spark.createDataFrame([(1, "Ann"), (2, "Bob")], ["customer_id", "name"])
    src_path = str(tmp_path / "src")
    source.write.parquet(src_path)

    sql_file = tmp_path / "q.sql"
    sql_file.write_text("SELECT customer_id, name FROM src")
    out_path = str(tmp_path / "out")

    cfg = JobConfig(
        job=JobMeta(name="aud_job"),
        sql_file=str(sql_file),
        inputs=[InputSpec(table="src", path=src_path, format="parquet")],
        output=OutputSpec(path=out_path, format="parquet", mode="overwrite"),
        aud_output=AudOutputSpec(enabled=True, fail_on_error=True),
        spark=SparkSpec(app_name="test", master="local[1]"),
    )

    metrics = run_pipeline(cfg, dry_run=False, spark=spark)

    aud_file = Path(out_path + ".aud")
    assert metrics.lineage["aud_file"] == str(aud_file)
    entries = [line.split("  ", 1) for line in aud_file.read_text().splitlines()]
    parts = [p for p in Path(out_path).rglob("*.parquet") if p.is_file()]
    assert len(entries) == len(parts) > 0
    for checksum, relative in entries:
        expected = hashlib.md5((Path(out_path) / relative).read_bytes()).hexdigest()
        assert checksum == expected
