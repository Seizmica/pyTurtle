"""Integration test for the per-partition manifest.json stage on real Spark."""

import json
from pathlib import Path

import pytest

pyspark = pytest.importorskip("pyspark")

from pyspark.sql import SparkSession

from etl_framework.config.schema import (
    InputSpec,
    JobConfig,
    JobMeta,
    ManifestOutputSpec,
    OutputSpec,
    SparkSpec,
)
from etl_framework.core.pipeline import run_pipeline


@pytest.fixture(scope="module")
def spark():
    s = (
        SparkSession.builder.appName("manifest-test")
        .master("local[1]")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .getOrCreate()
    )
    yield s
    s.stop()


def _job(tmp_path, spark, run_date, rows, **manifest_kw):
    """A partitioned job writing ``rows`` for one ``run_date``."""
    src_path = str(tmp_path / f"src-{run_date}")
    spark.createDataFrame(rows, ["customer_id", "run_date"]).write.parquet(src_path)

    sql_file = tmp_path / f"q-{run_date}.sql"
    sql_file.write_text("SELECT customer_id, run_date FROM src")

    return JobConfig(
        job=JobMeta(name="manifest_job"),
        sql_file=str(sql_file),
        sql_params={"run_date": run_date},
        inputs=[InputSpec(table="src", path=src_path, format="parquet")],
        output=OutputSpec(
            path=str(tmp_path / "out"),
            format="parquet",
            mode="overwrite",
            partition_by=["run_date"],
        ),
        manifest_output=ManifestOutputSpec(enabled=True, fail_on_error=True, **manifest_kw),
        spark=SparkSpec(app_name="manifest-test", master="local[1]"),
    )


def _manifest(out_path, run_date):
    path = Path(out_path) / f"run_date={run_date}" / "manifest.json"
    return json.loads(path.read_text(encoding="utf-8"))


def test_manifest_lands_in_partition_with_real_row_counts(tmp_path, spark):
    rows = [(1, "2026-06-28"), (2, "2026-06-28"), (3, "2026-06-28")]
    cfg = _job(tmp_path, spark, "2026-06-28", rows)

    metrics = run_pipeline(cfg, dry_run=False, spark=spark)

    doc = _manifest(cfg.output.path, "2026-06-28")
    assert doc["run_date"] == "2026-06-28"
    assert doc["row_count"] == 3  # per-shard counts sum to the written rows
    assert sum(s["row_count"] for s in doc["shards"]) == 3
    assert doc["shards"], "expected at least one shard"
    assert [s["index"] for s in doc["shards"]] == list(range(len(doc["shards"])))

    # Sizes and URIs match what actually landed on disk.
    for shard in doc["shards"]:
        assert Path(shard["uri"]).stat().st_size == shard["size_bytes"]

    expected = Path(cfg.output.path) / "run_date=2026-06-28" / "manifest.json"
    assert [Path(p) for p in metrics.lineage["manifest_files"]] == [expected]


def test_checkpoint_chain_across_two_runs(tmp_path, spark):
    first = _job(tmp_path, spark, "2026-06-27", [(1, "2026-06-27")], initial_checkpoint=1)
    run_pipeline(first, dry_run=False, spark=spark)
    day_one = _manifest(first.output.path, "2026-06-27")
    assert day_one["checkpoint_from"] == 1  # no prior manifest -> initial

    second = _job(tmp_path, spark, "2026-06-28", [(2, "2026-06-28")], initial_checkpoint=1)
    run_pipeline(second, dry_run=False, spark=spark)
    day_two = _manifest(second.output.path, "2026-06-28")

    # The second run picks up where the first left off.
    assert day_two["checkpoint_from"] == day_one["checkpoint_to"]
    assert day_two["checkpoint_to"] >= day_two["checkpoint_from"]

    # The earlier partition's manifest is left untouched.
    assert _manifest(first.output.path, "2026-06-27") == day_one


def test_manifest_is_not_read_back_as_data(tmp_path, spark):
    """A re-run must not choke on the manifest.json already in the directory."""
    cfg = _job(tmp_path, spark, "2026-06-28", [(1, "2026-06-28"), (2, "2026-06-28")])
    run_pipeline(cfg, dry_run=False, spark=spark)
    run_pipeline(cfg, dry_run=False, spark=spark)  # manifest.json now present on entry

    doc = _manifest(cfg.output.path, "2026-06-28")
    assert doc["row_count"] == 2
    assert all("manifest.json" not in s["uri"] for s in doc["shards"])
