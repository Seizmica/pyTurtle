"""Unit tests for the `.aud` MD5 checksum manifest stage."""

import hashlib
import json

import pytest

from etl_framework.config.schema import AudOutputSpec, validate_config
from etl_framework.stages.aud_stage import (
    default_aud_path,
    is_local_path,
    render_manifest,
    run_aud_stage,
)


def _output_dir(tmp_path):
    """A directory shaped like a Spark write: part files plus side-cars."""
    out = tmp_path / "out"
    (out / "run_date=2026-07-09").mkdir(parents=True)
    (out / "run_date=2026-07-09" / "part-00000.parquet").write_bytes(b"hello")
    (out / "part-00001.parquet").write_bytes(b"world")
    (out / "_SUCCESS").write_bytes(b"")
    (out / ".part-00001.parquet.crc").write_bytes(b"crc")
    return out


def test_default_aud_path():
    assert default_aud_path("s3://curated/out/") == "s3://curated/out.aud"
    assert default_aud_path("/data/out") == "/data/out.aud"


def test_is_local_path():
    assert is_local_path("/data/out")
    assert is_local_path("file:///data/out")
    assert is_local_path(r"C:\data\out")  # Windows drive letter, not a scheme
    assert not is_local_path("s3a://bucket/out")
    assert not is_local_path("hdfs://nn/out")


def test_manifest_covers_every_output_file(tmp_path):
    out = _output_dir(tmp_path)
    path, count = run_aud_stage(None, AudOutputSpec(enabled=True), str(out))

    assert path == str(out) + ".aud"
    assert count == 2  # side-cars excluded
    lines = open(path, encoding="utf-8").read().splitlines()
    assert lines == [
        f"{hashlib.md5(b'world').hexdigest()}  part-00001.parquet",
        f"{hashlib.md5(b'hello').hexdigest()}  run_date=2026-07-09/part-00000.parquet",
    ]


def test_include_hidden_covers_side_cars(tmp_path):
    out = _output_dir(tmp_path)
    _, count = run_aud_stage(None, AudOutputSpec(enabled=True, include_hidden=True), str(out))
    assert count == 4


def test_json_manifest_and_explicit_path(tmp_path):
    out = _output_dir(tmp_path)
    target = str(tmp_path / "manifests" / "customer_daily.aud")
    path, _ = run_aud_stage(None, AudOutputSpec(enabled=True, path=target, format="json"), str(out))

    assert path == target
    data = json.loads(open(path, encoding="utf-8").read())
    assert data["algorithm"] == "md5"
    assert data["file_count"] == 2
    assert data["files"][0]["md5"] == hashlib.md5(b"world").hexdigest()


def test_single_file_output(tmp_path):
    single = tmp_path / "out.json"
    single.write_bytes(b"hello")
    _, count = run_aud_stage(None, AudOutputSpec(enabled=True), str(single))
    assert count == 1


def test_missing_output_raises(tmp_path):
    with pytest.raises(FileNotFoundError, match="does not exist"):
        run_aud_stage(None, AudOutputSpec(enabled=True), str(tmp_path / "nope"))


def test_render_manifest_text_is_md5sum_compatible():
    assert render_manifest([("a.parquet", "abc123")], "/out", "text") == "abc123  a.parquet\n"


def test_config_defaults_to_disabled():
    cfg = validate_config(
        {
            "job": {"name": "t"},
            "sql_file": "q.sql",
            "inputs": [{"table": "a", "path": "/a"}],
            "output": {"path": "/o"},
        }
    )
    assert cfg.aud_output.enabled is False
    assert cfg.aud_output.format == "text"
