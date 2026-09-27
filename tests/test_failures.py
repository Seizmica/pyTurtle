"""Tests for how a failure is reported. No Spark required.

Under --deploy-mode cluster the traceback goes to a container log that may be
unreachable, so two other channels carry the cause out: the exit code names the
stage, and FAILURE_REPORT_DIR gets the traceback itself.
"""

import pytest

from util import runner
from util.config import ConfigError, Settings
from util.runner import (
    EXIT_CONFIG,
    EXIT_READ,
    EXIT_SESSION,
    EXIT_TRANSFORM,
    EXIT_UNKNOWN,
    EXIT_WRITE,
    Job,
    Output,
    StageFailure,
    main,
    write_failure_report,
)

ARGV = ["--env", "dev", "--run-date", "2026-06-28"]


def _job():
    return Job(
        name="t",
        sql="SELECT 1",
        inputs={"src": "${RAW_ROOT}/src/"},
        outputs=[Output(path="${CURATED_ROOT}/t/")],
    )


def _settings(**values):
    return Settings(environment="dev", values=values)


# --- stage tagging -------------------------------------------------------


def test_a_stage_tags_the_failure_with_its_code():
    with pytest.raises(StageFailure) as caught:
        with runner._stage("session", EXIT_SESSION):
            raise RuntimeError("Hive classes are not found")

    assert caught.value.stage == "session"
    assert caught.value.code == EXIT_SESSION
    # The original message survives, since it is the part worth reading.
    assert "Hive classes are not found" in str(caught.value)


def test_a_config_error_passes_through_a_stage_untagged():
    """It already has a clear message and its own exit code."""
    with pytest.raises(ConfigError):
        with runner._stage("session", EXIT_SESSION):
            raise ConfigError("RAW_ROOT is not set")


def test_stages_do_not_nest_their_tags():
    """An inner stage's code must survive an outer one, not be overwritten."""
    with pytest.raises(StageFailure) as caught:
        with runner._stage("write", EXIT_WRITE):
            with runner._stage("read inputs", EXIT_READ):
                raise RuntimeError("path does not exist")

    assert caught.value.code == EXIT_READ


# --- exit codes ----------------------------------------------------------


@pytest.mark.parametrize(
    "stage,code",
    [
        ("session", EXIT_SESSION),
        ("read inputs", EXIT_READ),
        ("transform", EXIT_TRANSFORM),
        ("write s3a://x/", EXIT_WRITE),
    ],
)
def test_main_returns_the_failing_stage_code(monkeypatch, stage, code):
    """The submitter sees this number as "User application exited with N"."""

    def explode(*_args, **_kwargs):
        raise StageFailure(stage, code, RuntimeError("boom"))

    monkeypatch.setattr(runner, "run", explode)
    assert main(_job(), ARGV) == code


def test_main_returns_the_config_code_for_a_config_error(monkeypatch):
    def explode(*_args, **_kwargs):
        raise ConfigError("RAW_ROOT is not set")

    monkeypatch.setattr(runner, "run", explode)
    assert main(_job(), ARGV) == EXIT_CONFIG


def test_main_reports_an_untagged_failure_distinctly(monkeypatch):
    """Something outside any stage must not be mistaken for a stage failure."""

    def explode(*_args, **_kwargs):
        raise RuntimeError("unexpected")

    monkeypatch.setattr(runner, "run", explode)
    assert main(_job(), ARGV) == EXIT_UNKNOWN


def test_main_returns_zero_when_the_run_succeeds(monkeypatch):
    monkeypatch.setattr(runner, "run", lambda *a, **k: {"job": "t"})
    assert main(_job(), ARGV) == 0


# --- the report itself ---------------------------------------------------


def test_no_report_is_written_when_the_directory_is_unset():
    assert write_failure_report(_settings(), _job(), RuntimeError("boom")) is None


def test_the_report_carries_the_traceback(tmp_path, monkeypatch):
    """Without a session, the report goes out through the hdfs client."""
    captured = {}

    def fake_run(argv, **_kwargs):
        captured["argv"] = argv
        captured["body"] = open(argv[-2], encoding="utf-8").read()

        class Completed:
            returncode = 0

        return Completed()

    monkeypatch.setattr(runner.subprocess, "run", fake_run)

    try:
        raise RuntimeError("ClassNotFoundException: DeltaSparkSessionExtension")
    except RuntimeError as exc:
        target = write_failure_report(
            _settings(FAILURE_REPORT_DIR="hdfs:///tmp/etl-failures"), _job(), exc
        )

    assert target is not None
    assert target.startswith("hdfs:///tmp/etl-failures/t-")
    assert captured["argv"][:4] == ["hdfs", "dfs", "-put", "-f"]
    assert "ClassNotFoundException" in captured["body"]
    assert "Traceback" in captured["body"]


def test_a_failed_report_does_not_mask_the_failure(monkeypatch):
    """Reporting is best-effort; it must never raise over the real error."""

    def explode(*_args, **_kwargs):
        raise OSError("hdfs: command not found")

    monkeypatch.setattr(runner.subprocess, "run", explode)

    assert (
        write_failure_report(
            _settings(FAILURE_REPORT_DIR="hdfs:///tmp/etl-failures"),
            _job(),
            RuntimeError("boom"),
        )
        is None
    )
