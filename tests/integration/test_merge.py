"""Integration test for Delta merge (incremental upsert) mode.

Skips automatically when ``delta-spark`` is not installed. Requires a local
JDK for Spark to start.
"""

import pytest

pytest.importorskip("pyspark")
pytest.importorskip("delta")

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

from etl_framework.config.schema import (
    InputSpec,
    JobConfig,
    JobMeta,
    OutputSpec,
    SparkSpec,
)
from etl_framework.core.pipeline import run_pipeline


@pytest.fixture(scope="module")
def spark(tmp_path_factory):
    warehouse = tmp_path_factory.mktemp("warehouse")
    builder = (
        SparkSession.builder.appName("merge-test")
        .master("local[1]")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.sql.warehouse.dir", str(warehouse))
        .config(
            "spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension",
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
    )
    s = configure_spark_with_delta_pip(builder).getOrCreate()
    yield s
    s.stop()


def _make_cfg(src_path: str, out_path: str, sql_file: str) -> JobConfig:
    return JobConfig(
        job=JobMeta(name="merge_job"),
        sql_file=sql_file,
        inputs=[InputSpec(table="src", path=src_path, format="parquet")],
        output=OutputSpec(
            path=out_path,
            format="delta",
            mode="merge",
            merge_keys=["id"],
        ),
        spark=SparkSpec(app_name="merge-test", master="local[1]"),
    )


def test_merge_upserts(tmp_path, spark):
    src_path = str(tmp_path / "src")
    out_path = str(tmp_path / "out")
    sql_file = tmp_path / "q.sql"
    sql_file.write_text("SELECT id, value FROM src")

    cfg = _make_cfg(src_path, out_path, str(sql_file))

    # Run 1: seed with two rows -> creates the Delta table.
    spark.createDataFrame([(1, "a"), (2, "b")], ["id", "value"]).write.mode("overwrite").parquet(
        src_path
    )
    run_pipeline(cfg, dry_run=False, spark=spark)

    rows = {r["id"]: r["value"] for r in spark.read.format("delta").load(out_path).collect()}
    assert rows == {1: "a", 2: "b"}

    # Run 2: update id=2, insert id=3 -> upsert semantics.
    spark.createDataFrame([(2, "B"), (3, "c")], ["id", "value"]).write.mode("overwrite").parquet(
        src_path
    )
    run_pipeline(cfg, dry_run=False, spark=spark)

    rows = {r["id"]: r["value"] for r in spark.read.format("delta").load(out_path).collect()}
    assert rows == {1: "a", 2: "B", 3: "c"}
