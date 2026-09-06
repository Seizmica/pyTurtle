"""Unit tests for the per-partition `manifest.json` stage."""

import json
from datetime import datetime, timezone

import pytest

from etl_framework.config.schema import ManifestOutputSpec, OutputSpec, validate_config
from etl_framework.stages.manifest_stage import (
    Checkpoints,
    build_manifest,
    partition_dir,
    prepare_checkpoints,
    run_manifest_stage,
    stamp_checkpoint,
)
from etl_framework.utils import fs

RUN_DATE = "2026-06-28"


class _FakeDataFrame:
    """Stands in for the result DataFrame; only partition values are read."""

    def __init__(self, values):
        self._values = values

    def select(self, *cols):
        self._cols = cols
        return self

    def distinct(self):
        return self

    def collect(self):
        return [dict(v) for v in self._values]


def _partitioned_output(tmp_path, dates=(RUN_DATE,)):
    """A directory shaped like a partitioned Spark write."""
    out = tmp_path / "customer"
    for date in dates:
        part = out / f"run_date={date}"
        part.mkdir(parents=True)
        (part / "part-00000.parquet").write_bytes(b"a" * 100)
        (part / "part-00001.parquet").write_bytes(b"b" * 200)
        (part / "_SUCCESS").write_bytes(b"")
    return out


def _spec(**kw):
    return ManifestOutputSpec(enabled=True, row_counts=False, **kw)


def _output(path, **kw):
    kw.setdefault("partition_by", ["run_date"])
    return OutputSpec(path=str(path), format="parquet", **kw)


def test_partition_dir_is_hive_style():
    assert (
        partition_dir("s3://b/customer/", {"run_date": RUN_DATE}, ["run_date"])
        == "s3://b/customer/run_date=2026-06-28"
    )
    assert partition_dir("s3://b/customer/", {}, []) == "s3://b/customer"


def test_stamp_checkpoint_renders_digits():
    moment = datetime(2026, 6, 28, 15, 0, 0, tzinfo=timezone.utc)
    assert stamp_checkpoint(ManifestOutputSpec(), moment) == 20260628150000


def test_manifest_written_into_partition_dir(tmp_path):
    out = _partitioned_output(tmp_path)
    df = _FakeDataFrame([{"run_date": RUN_DATE}])

    paths = run_manifest_stage(
        None, df, _spec(), _output(out), Checkpoints(202606271500000, 202606281500000)
    )

    expected = str(out / f"run_date={RUN_DATE}" / "manifest.json").replace("\\", "/")
    assert [p.replace("\\", "/") for p in paths] == [expected]

    doc = json.loads(open(paths[0], encoding="utf-8").read())
    assert doc["run_date"] == RUN_DATE
    assert doc["checkpoint_from"] == 202606271500000
    assert doc["checkpoint_to"] == 202606281500000
    assert [s["index"] for s in doc["shards"]] == [0, 1]
    assert [s["size_bytes"] for s in doc["shards"]] == [100, 200]
    # Side-cars are excluded from the shard list.
    assert all("_SUCCESS" not in s["uri"] for s in doc["shards"])


def test_manifest_excludes_itself_on_rerun(tmp_path):
    out = _partitioned_output(tmp_path)
    df = _FakeDataFrame([{"run_date": RUN_DATE}])
    args = (None, df, _spec(), _output(out), Checkpoints(1, 2))

    run_manifest_stage(*args)
    paths = run_manifest_stage(*args)  # second run sees its own manifest on disk

    doc = json.loads(open(paths[0], encoding="utf-8").read())
    assert len(doc["shards"]) == 2
    assert all("manifest.json" not in s["uri"] for s in doc["shards"])


def test_relative_uri_style(tmp_path):
    out = _partitioned_output(tmp_path)
    df = _FakeDataFrame([{"run_date": RUN_DATE}])

    paths = run_manifest_stage(
        None, df, _spec(uri_style="relative"), _output(out), Checkpoints(None, 2)
    )
    doc = json.loads(open(paths[0], encoding="utf-8").read())
    assert [s["uri"] for s in doc["shards"]] == ["part-00000.parquet", "part-00001.parquet"]


def test_only_partitions_this_run_wrote_get_a_manifest(tmp_path):
    out = _partitioned_output(tmp_path, dates=("2026-06-27", RUN_DATE))
    df = _FakeDataFrame([{"run_date": RUN_DATE}])  # an append touching one day

    paths = run_manifest_stage(None, df, _spec(), _output(out), Checkpoints(None, 2))

    assert len(paths) == 1
    assert f"run_date={RUN_DATE}" in paths[0].replace("\\", "/")
    assert not (out / "run_date=2026-06-27" / "manifest.json").exists()


def test_unpartitioned_output_gets_one_manifest(tmp_path):
    out = tmp_path / "customer"
    out.mkdir()
    (out / "part-00000.parquet").write_bytes(b"a" * 10)
    df = _FakeDataFrame([])

    paths = run_manifest_stage(
        None,
        df,
        _spec(),
        _output(out, partition_by=[]),
        Checkpoints(None, 2),
        run_date=RUN_DATE,
    )

    assert len(paths) == 1
    doc = json.loads(open(paths[0], encoding="utf-8").read())
    assert doc["run_date"] == RUN_DATE  # falls back to the caller's value


def test_checkpoint_from_reads_previous_partition_manifest(tmp_path):
    out = _partitioned_output(tmp_path, dates=("2026-06-26", "2026-06-27"))
    for date, value in (("2026-06-26", 202606261500000), ("2026-06-27", 202606271500000)):
        (out / f"run_date={date}" / "manifest.json").write_text(
            json.dumps({"checkpoint_to": value}), encoding="utf-8"
        )

    checkpoints = prepare_checkpoints(
        None,
        _spec(),
        _output(out),
        now=datetime(2026, 6, 28, 15, 0, 0, tzinfo=timezone.utc),
    )

    assert checkpoints.checkpoint_from == 202606271500000  # newest prior run
    assert checkpoints.checkpoint_to == 20260628150000


def test_checkpoint_from_falls_back_to_initial_on_first_run(tmp_path):
    out = tmp_path / "empty"
    checkpoints = prepare_checkpoints(None, _spec(initial_checkpoint=1), _output(out))
    assert checkpoints.checkpoint_from == 1


def test_explicit_checkpoints_override_tracking(tmp_path):
    out = _partitioned_output(tmp_path)
    (out / f"run_date={RUN_DATE}" / "manifest.json").write_text(
        json.dumps({"checkpoint_to": 999}), encoding="utf-8"
    )

    checkpoints = prepare_checkpoints(
        None, _spec(checkpoint_from=10, checkpoint_to=20), _output(out)
    )
    assert checkpoints == Checkpoints(10, 20)


def test_lookback_bounds_the_partition_scan(tmp_path):
    dates = [f"2026-06-{d:02d}" for d in range(1, 11)]
    out = _partitioned_output(tmp_path, dates=dates)
    (out / "run_date=2026-06-10" / "manifest.json").write_text(
        json.dumps({"checkpoint_to": 111}), encoding="utf-8"
    )
    (out / "run_date=2026-06-01" / "manifest.json").write_text(
        json.dumps({"checkpoint_to": 222}), encoding="utf-8"
    )

    # Newest-first scan with a lookback of 1 sees only run_date=2026-06-10.
    assert (
        prepare_checkpoints(None, _spec(lookback_partitions=1), _output(out)).checkpoint_from == 111
    )


def test_row_counts_summed_into_total():
    entries = [
        fs.FileEntry("part-0.parquet", "/out/part-0.parquet", 10),
        fs.FileEntry("part-1.parquet", "/out/part-1.parquet", 20),
    ]
    counts = {
        fs.normalize_uri("/out/part-0.parquet"): 20,
        fs.normalize_uri("/out/part-1.parquet"): 5,
    }

    doc = build_manifest(RUN_DATE, Checkpoints(1, 2), "1.1.23", entries, counts, "absolute")

    assert doc["row_count"] == 25
    assert [s["row_count"] for s in doc["shards"]] == [20, 5]
    assert doc["app_version"] == "1.1.23"
    assert list(doc) == [
        "run_date",
        "checkpoint_from",
        "checkpoint_to",
        "row_count",
        "app_version",
        "shards",
    ]


def test_row_count_omitted_when_disabled():
    entries = [fs.FileEntry("part-0.parquet", "/out/part-0.parquet", 10)]
    doc = build_manifest(RUN_DATE, Checkpoints(1, 2), "0.1.0", entries, None, "absolute")
    assert doc["row_count"] is None
    assert "row_count" not in doc["shards"][0]


@pytest.mark.parametrize(
    "listed, spark_reported",
    [
        ("/out/part-0.parquet", "file:///out/part-0.parquet"),
        (r"C:\out\part-0.parquet", "file:///C:/out/part-0.parquet"),
        ("s3a://bucket/k/part-0.parquet", "s3a://bucket/k/part-0.parquet"),
    ],
)
def test_normalize_uri_matches_listing_against_input_file_name(listed, spark_reported):
    """A listed file and the same file as `input_file_name()` reports it must key alike."""
    assert fs.normalize_uri(listed) == fs.normalize_uri(spark_reported)


def test_unreadable_history_does_not_fail_the_run(tmp_path, monkeypatch):
    """The watermark lookup runs before the write, so it must never raise."""
    out = _partitioned_output(tmp_path, dates=("2026-06-27",))

    def boom(*_args, **_kwargs):
        raise PermissionError("s3: access denied")

    monkeypatch.setattr(fs, "read_text", boom)
    checkpoints = prepare_checkpoints(None, _spec(initial_checkpoint=7), _output(out))

    assert checkpoints.checkpoint_from == 7  # falls back instead of propagating


def test_partition_without_a_directory_is_skipped(tmp_path):
    out = _partitioned_output(tmp_path)
    # The result claims a partition the write produced no directory for.
    df = _FakeDataFrame([{"run_date": RUN_DATE}, {"run_date": "2026-06-29"}])

    paths = run_manifest_stage(None, df, _spec(), _output(out), Checkpoints(None, 2))

    assert len(paths) == 1
    assert f"run_date={RUN_DATE}" in paths[0].replace("\\", "/")


def test_config_defaults_to_disabled():
    cfg = validate_config(
        {
            "job": {"name": "t"},
            "sql_file": "q.sql",
            "inputs": [{"table": "a", "path": "/a"}],
            "output": {"path": "/o"},
        }
    )
    assert cfg.manifest_output.enabled is False
    assert cfg.manifest_output.filename == "manifest.json"
    assert cfg.manifest_output.row_counts is True


@pytest.mark.parametrize(
    "override, message",
    [
        ({"filename": "sub/manifest.json"}, "bare file name"),
        ({"checkpoint_format": "run-%Y"}, "digits only"),
        ({"lookback_partitions": -1}, "negative"),
    ],
)
def test_invalid_manifest_config_fails_fast(override, message):
    with pytest.raises(ValueError, match=message):
        validate_config(
            {
                "job": {"name": "t"},
                "sql_file": "q.sql",
                "inputs": [{"table": "a", "path": "/a"}],
                "output": {"path": "/o"},
                "manifest_output": {"enabled": True, **override},
            }
        )
