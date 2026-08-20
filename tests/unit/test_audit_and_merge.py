"""Unit tests for audit records and merge-mode config validation."""

import json

import pytest

from etl_framework.config.schema import (
    JobConfig,
    JobMeta,
    InputSpec,
    OutputSpec,
    validate_config,
)
from etl_framework.observability.audit import (
    build_audit_record,
    write_audit_record,
)
from etl_framework.observability.metrics import RunMetrics


def _raw(**output):
    return {
        "job": {"name": "t"},
        "sql_file": "q.sql",
        "inputs": [{"table": "a", "path": "/a"}],
        "output": {"path": "/o", **output},
    }


def test_merge_requires_delta_format():
    with pytest.raises(ValueError, match="requires format 'delta'"):
        validate_config(_raw(mode="merge", merge_keys=["id"]))


def test_merge_requires_keys():
    with pytest.raises(ValueError, match="requires 'merge_keys'"):
        validate_config(_raw(mode="merge", format="delta"))


def test_merge_valid():
    cfg = validate_config(_raw(mode="merge", format="delta", merge_keys=["id"]))
    assert cfg.output.merge_keys == ["id"]


def _cfg():
    return JobConfig(
        job=JobMeta(name="job1"),
        sql_file="missing.sql",
        inputs=[InputSpec(table="a", path="/a")],
        output=OutputSpec(path="/o"),
    )


def test_build_audit_record_shape():
    metrics = RunMetrics(run_id="r1", job_name="job1")
    metrics.output_rows = 5
    rec = build_audit_record(_cfg(), metrics)
    assert rec["run_id"] == "r1"
    assert rec["job_name"] == "job1"
    assert rec["config_snapshot"]["output"]["path"] == "/o"
    assert rec["metrics"]["output_rows"] == 5
    assert "git_commit" in rec  # present (value may be None)


def test_write_audit_record(tmp_path):
    cfg = _cfg()
    cfg.audit.path = str(tmp_path / "audit")
    metrics = RunMetrics(run_id="r1", job_name="job1")
    path = write_audit_record(cfg, metrics)
    data = json.loads(open(path, encoding="utf-8").read())
    assert data["run_id"] == "r1"
