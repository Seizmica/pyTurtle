"""Unit tests for config loading, merging, and interpolation."""

import pytest

from etl_framework.config.env import MissingEnvVar, interpolate
from etl_framework.config.loader import deep_merge, load_config
from etl_framework.config.schema import validate_config


def test_deep_merge_overrides_nested():
    base = {"spark": {"conf": {"a": "1", "b": "2"}}, "x": 1}
    override = {"spark": {"conf": {"b": "9"}}, "y": 2}
    merged = deep_merge(base, override)
    assert merged == {"spark": {"conf": {"a": "1", "b": "9"}}, "x": 1, "y": 2}


def test_interpolate_env_and_default(monkeypatch):
    monkeypatch.setenv("FOO", "bar")
    assert interpolate("${FOO}") == "bar"
    assert interpolate("${MISSING:-def}") == "def"
    assert interpolate({"k": ["${FOO}"]}) == {"k": ["bar"]}


def test_interpolate_missing_raises():
    with pytest.raises(MissingEnvVar):
        interpolate("${DEFINITELY_NOT_SET_VAR_123}")


def test_load_config_layered(tmp_path, monkeypatch):
    monkeypatch.setenv("RUN_DATE", "2026-07-09")
    root = tmp_path / "configs"
    (root / "env").mkdir(parents=True)
    (root / "base.yaml").write_text("spark:\n  app_name: base_app\nlogging:\n  level: INFO\n")
    (root / "env" / "dev.yaml").write_text("spark:\n  master: local[*]\nlogging:\n  level: DEBUG\n")
    job = tmp_path / "job.yaml"
    job.write_text(
        "job:\n  name: t\nsql_file: q.sql\n"
        "sql_params:\n  run_date: '${RUN_DATE}'\n"
        "inputs:\n  - {table: a, path: /a, format: parquet}\n"
        "output:\n  path: /out\n  format: parquet\n"
    )
    cfg = load_config(job, env="dev", config_root=root)
    assert cfg.spark.app_name == "base_app"
    assert cfg.spark.master == "local[*]"
    assert cfg.logging.level == "DEBUG"
    assert cfg.sql_params["run_date"] == "2026-07-09"


def test_ttl_enabled_requires_fields():
    raw = {
        "job": {"name": "t"},
        "sql_file": "q.sql",
        "inputs": [{"table": "a", "path": "/a"}],
        "output": {"path": "/o"},
        "ttl_output": {"enabled": True},
    }
    with pytest.raises(ValueError):
        validate_config(raw)
