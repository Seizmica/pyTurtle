"""Tests for util/compact.py.

The planning helpers need no Spark. The end-to-end tests run against a local
SparkSession and skip without a JDK, like tests/test_run_local.py.
"""

import gzip
import json
from pathlib import Path

import pytest

from util.compact import (
    FILL,
    Plan,
    infer_codec,
    is_data_file,
    parse_options,
    target_file_count,
)
from util.config import ConfigError

MB = 2**20

# --------------------------------------------------------------------------
# Planning, no Spark
# --------------------------------------------------------------------------


def test_target_file_count_leaves_headroom_under_the_limit():
    assert target_file_count(10 * MB, 50 * MB) == 1
    # 100 MB would fit in exactly two 50 MB files, but FILL keeps each under.
    assert target_file_count(100 * MB, 50 * MB) == 3
    assert target_file_count(int(2 * 50 * MB * FILL), 50 * MB) == 2
    assert target_file_count(0, 50 * MB) == 1


def test_plan_skips_a_directory_that_is_already_sized():
    files = [(f"f{i}", 40 * MB) for i in range(3)]
    assert not Plan("d", files, 50 * MB).needed


def test_plan_merges_many_small_files():
    files = [(f"f{i}", MB) for i in range(200)]
    plan = Plan("d", files, 50 * MB)
    assert plan.needed
    assert plan.target_files == 5


@pytest.mark.parametrize(
    ("name", "fmt", "expected"),
    [
        ("part-00000-abc-c000.snappy.parquet", "parquet", True),
        ("part-00000-abc-c000.csv.gz", "csv", True),
        ("part-00000-abc-c000.csv", "csv", True),
        ("manifest.json", "parquet", False),
        ("_SUCCESS", "csv", False),
        (".part-00000.parquet.crc", "parquet", False),
        ("part-00000-abc-c000.csv", "parquet", False),
    ],
)
def test_is_data_file(name, fmt, expected):
    assert is_data_file(name, fmt) is expected


@pytest.mark.parametrize(
    ("fmt", "names", "expected"),
    [
        ("parquet", ["part-0-abc-c000.snappy.parquet"], "snappy"),
        ("parquet", ["part-0-abc-c000.zstd.parquet"], "zstd"),
        ("parquet", ["part-0-abc-c000.parquet"], "none"),
        ("csv", ["part-0-abc-c000.csv.gz"], "gzip"),
        ("csv", ["part-0-abc-c000.csv"], "none"),
        ("csv", ["a.csv.gz", "b.csv.gz", "c.csv"], "gzip"),
        ("csv", [], None),
    ],
)
def test_infer_codec_keeps_the_existing_compression(fmt, names, expected):
    assert infer_codec(fmt, names) == expected


def test_parse_options():
    assert parse_options(["header=true", "sep=|", "nullValue="]) == {
        "header": "true",
        "sep": "|",
        "nullValue": "",
    }
    assert parse_options(None) == {}
    with pytest.raises(ConfigError, match="KEY=VALUE"):
        parse_options(["header"])


# --------------------------------------------------------------------------
# End to end, local Spark
# --------------------------------------------------------------------------


@pytest.fixture(scope="module")
def spark(tmp_path_factory):
    pytest.importorskip("pyspark")
    from pyspark.sql import SparkSession

    try:
        session = (
            SparkSession.builder.appName("etl-lite-compact-test")
            .master("local[2]")
            .config("spark.sql.shuffle.partitions", "4")
            .config("spark.sql.warehouse.dir", str(tmp_path_factory.mktemp("warehouse")))
            .getOrCreate()
        )
    except Exception as exc:  # noqa: BLE001 - any startup failure means "no JVM here"
        pytest.skip(f"Spark could not start, a JDK is required: {exc}")
    yield session
    session.stop()


def _rows(n=20_000):
    # Varied strings so parquet cannot compress the files down to nothing.
    return [(i, f"customer-{i}-{i * 7919 % 100_003}", float(i % 977)) for i in range(n)]


def _write_small_files(spark, path, fmt="parquet", files=40, **options):
    df = spark.createDataFrame(_rows(), ["customer_id", "name", "total_spend"])
    writer = df.repartition(files).write.format(fmt).mode("overwrite")
    for key, value in options.items():
        writer = writer.option(key, value)
    writer.save(str(path))


def _data(path, suffix):
    return sorted(p for p in Path(path).iterdir() if p.name.endswith(suffix))


def _sorted(df):
    return sorted(tuple(row) for row in df.collect())


def test_parquet_is_merged_under_the_target(spark, tmp_path):
    from util.compact import compact

    partition = tmp_path / "customer" / "run_date=2026-06-28"
    _write_small_files(spark, partition)
    before = _data(partition, ".parquet")
    expected = _sorted(spark.read.parquet(str(partition)))
    total = sum(p.stat().st_size for p in before)
    target = total // 3

    summary = compact(spark, str(tmp_path / "customer"), "parquet", target)

    after = _data(partition, ".parquet")
    assert len(before) == 40
    assert 1 < len(after) <= target_file_count(total, target) + 1
    assert all(p.stat().st_size <= target for p in after)
    assert not set(after) & set(before)
    assert _sorted(spark.read.parquet(str(partition))) == expected
    assert summary["compacted"][0]["rows"] == len(expected)
    assert not (partition / "_compaction").exists()


def test_csv_keeps_its_header_and_compression(spark, tmp_path):
    from util.compact import compact

    directory = tmp_path / "export"
    _write_small_files(spark, directory, "csv", files=12, header="true", compression="gzip")
    expected = _sorted(spark.read.option("header", "true").csv(str(directory)))

    compact(spark, str(directory), "csv", 50 * MB, {"header": "true"})

    after = _data(directory, ".csv.gz")
    assert len(after) == 1
    with gzip.open(after[0], "rt", encoding="utf-8") as handle:
        lines = handle.read().splitlines()
    # One header for the one file, not twelve scattered through the data.
    assert lines.count("customer_id,name,total_spend") == 1
    assert len(lines) == len(expected) + 1
    assert _sorted(spark.read.option("header", "true").csv(str(directory))) == expected


def test_csv_requires_an_explicit_header_option(spark, tmp_path):
    from util.compact import compact

    _write_small_files(spark, tmp_path / "export", "csv", files=3, header="true")
    with pytest.raises(ConfigError, match="header"):
        compact(spark, str(tmp_path / "export"), "csv", 50 * MB)


def test_manifest_follows_the_new_shards(spark, tmp_path):
    from util.compact import compact

    partition = tmp_path / "customer" / "run_date=2026-06-28"
    _write_small_files(spark, partition, files=10)
    manifest = {
        "run_date": "2026-06-28",
        "checkpoint_from": 20260627000000,
        "checkpoint_to": 20260628000000,
        "row_count": 20_000,
        "app_version": "1.4.2",
        "shards": [{"index": 0, "uri": "stale", "size_bytes": 1, "row_count": 1}],
    }
    (partition / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")

    compact(spark, str(partition), "parquet", 50 * MB)

    doc = json.loads((partition / "manifest.json").read_text(encoding="utf-8"))
    files = _data(partition, ".parquet")
    assert [Path(s["uri"].split(":", 1)[-1]).name for s in doc["shards"]] == [p.name for p in files]
    assert [s["size_bytes"] for s in doc["shards"]] == [p.stat().st_size for p in files]
    assert doc["row_count"] == 20_000
    assert doc["checkpoint_from"] == 20260627000000
    assert doc["checkpoint_to"] == 20260628000000
    assert "compacted_at" in doc


def test_a_sized_directory_is_left_untouched(spark, tmp_path):
    from util.compact import compact

    _write_small_files(spark, tmp_path / "t", files=1)
    before = _data(tmp_path / "t", ".parquet")

    summary = compact(spark, str(tmp_path / "t"), "parquet", 50 * MB)

    assert summary["skipped"] == 1
    assert _data(tmp_path / "t", ".parquet") == before


def test_dry_run_changes_nothing(spark, tmp_path):
    from util.compact import compact

    _write_small_files(spark, tmp_path / "t", files=8)
    before = _data(tmp_path / "t", ".parquet")

    summary = compact(spark, str(tmp_path / "t"), "parquet", 50 * MB, dry_run=True)

    assert summary["planned"][0]["files_after"] == 1
    assert _data(tmp_path / "t", ".parquet") == before


def test_an_interrupted_commit_is_finished_by_the_next_run(spark, tmp_path, monkeypatch):
    import util.compact as compact_module

    directory = tmp_path / "t"
    _write_small_files(spark, directory, files=8)
    expected = _sorted(spark.read.parquet(str(directory)))

    def crash(*_args, **_kwargs):
        raise RuntimeError("driver lost")

    # _READY is written, then the swap never starts.
    monkeypatch.setattr(compact_module, "commit", crash)
    with pytest.raises(RuntimeError, match="driver lost"):
        compact_module.compact(spark, str(directory), "parquet", 50 * MB)
    monkeypatch.undo()
    assert (directory / "_compaction" / "_READY").is_file()

    summary = compact_module.compact(spark, str(directory), "parquet", 50 * MB)

    assert len(summary["recovered"]) == 1
    assert len(_data(directory, ".parquet")) == 1
    assert _sorted(spark.read.parquet(str(directory))) == expected
    assert not (directory / "_compaction").exists()


def test_unfinished_staging_is_discarded(spark, tmp_path):
    from util.compact import compact

    directory = tmp_path / "t"
    _write_small_files(spark, directory, files=8)
    expected = _sorted(spark.read.parquet(str(directory)))
    # A staging write that died before _READY: never trusted, never committed.
    _write_small_files(spark, directory / "_compaction" / "data", files=2)

    compact(spark, str(directory), "parquet", 50 * MB)

    assert _sorted(spark.read.parquet(str(directory))) == expected
    assert not (directory / "_compaction").exists()


def test_delta_tables_are_refused(spark, tmp_path):
    from util.compact import compact

    _write_small_files(spark, tmp_path / "orders", files=3)
    (tmp_path / "orders" / "_delta_log").mkdir()

    with pytest.raises(ConfigError, match="OPTIMIZE"):
        compact(spark, str(tmp_path / "orders"), "parquet", 50 * MB)


def test_a_missing_path_fails_loudly(spark, tmp_path):
    from util.compact import compact

    with pytest.raises(ConfigError, match="does not exist"):
        compact(spark, str(tmp_path / "nope"), "parquet", 50 * MB)
