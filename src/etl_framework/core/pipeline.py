"""Orchestrates the full read -> transform -> write flow."""

from __future__ import annotations

import uuid

from ..config.schema import JobConfig
from ..observability.audit import write_audit_record
from ..observability.logging import configure_logging, log
from ..observability.metrics import RunMetrics
from ..quality.schema_check import validate_schema
from ..quality.validators import run_quality
from ..stages.aud_stage import run_aud_stage
from ..stages.ttl_stage import run_ttl_stage
from ..utils.retry import with_retry
from .reader import register_inputs
from .session import build_spark_session
from .transformer import transform
from .writer import write_output


def run_pipeline(cfg: JobConfig, dry_run: bool = False, spark=None) -> RunMetrics:  # noqa: ANN001
    """Execute a job end to end.

    Flow: build session -> read inputs -> pre-quality -> transform ->
    schema validation -> post-quality -> write -> optional TTL and .aud
    checksum stages -> emit metrics. Any cached result is always released. The Spark session is
    stopped on teardown only when this function created it; a caller-supplied
    ``spark`` is left running for the caller to manage.
    """
    run_id = str(uuid.uuid4())
    logger = configure_logging(run_id, cfg.logging.level)
    metrics = RunMetrics(run_id=run_id, job_name=cfg.job.name)
    log(logger, "INFO", "run.start", job=cfg.job.name, env=cfg.environment, dry_run=dry_run)

    owns_session = spark is None
    result = None
    try:
        if spark is None:
            spark = build_spark_session(cfg.spark, cfg)

        count_rows = cfg.metrics.row_counts

        metrics.start_stage("read")
        metrics.input_rows = register_inputs(spark, cfg.inputs, count_rows)
        metrics.end_stage("read")
        log(logger, "INFO", "read.done", rows=metrics.input_rows)

        metrics.start_stage("transform")
        result = transform(spark, cfg.sql_file, cfg.sql_params)
        result.cache()
        metrics.output_rows = result.count() if count_rows else -1
        metrics.end_stage("transform")
        log(logger, "INFO", "transform.done", rows=metrics.output_rows)

        metrics.start_stage("schema")
        validate_schema(result, cfg.output.expected_schema)
        metrics.end_stage("schema")

        metrics.start_stage("quality")
        for r in run_quality(result, cfg.data_quality, spark):
            log(
                logger, "INFO", "quality.check", type=r.check_type, passed=r.passed, detail=r.detail
            )
        metrics.end_stage("quality")

        metrics.lineage = {
            "sources": [i.path for i in cfg.inputs],
            "target": cfg.output.path,
            "sql_file": cfg.sql_file,
        }

        if dry_run:
            log(logger, "INFO", "dry_run.plan", lineage=metrics.lineage)
            _write_audit(cfg, metrics, logger)
            return metrics

        metrics.start_stage("write")
        with_retry(
            lambda: write_output(result, cfg.output),
            max_attempts=cfg.retries.max_attempts,
            backoff_seconds=cfg.retries.backoff_seconds,
            on_retry=lambda a, e: log(logger, "WARNING", "write.retry", attempt=a, error=str(e)),
        )
        metrics.end_stage("write")
        log(logger, "INFO", "write.done", path=cfg.output.path)

        if cfg.ttl_output.enabled:
            _run_ttl(cfg, result, logger, metrics)

        if cfg.aud_output.enabled:
            _run_aud(cfg, spark, logger, metrics)

        _write_audit(cfg, metrics, logger)
        log(logger, "INFO", "run.done", metrics=metrics.to_dict())
        return metrics
    except Exception as exc:  # noqa: BLE001
        log(logger, "ERROR", "run.failed", error=str(exc))
        raise
    finally:
        if result is not None:
            try:
                result.unpersist()
            except Exception:  # noqa: BLE001 - best-effort cleanup
                pass
        if spark is not None and owns_session:
            spark.stop()


def _write_audit(cfg, metrics, logger) -> None:  # noqa: ANN001
    if not cfg.audit.enabled:
        return
    try:
        path = write_audit_record(cfg, metrics)
        log(logger, "INFO", "audit.written", path=path)
    except Exception as exc:  # noqa: BLE001 - audit must not fail the run
        log(logger, "WARNING", "audit.failed", error=str(exc))


def _run_aud(cfg, spark, logger, metrics) -> None:  # noqa: ANN001
    """Checksum the freshly written output into a `.aud` manifest."""
    metrics.start_stage("aud")
    try:
        path, file_count = run_aud_stage(spark, cfg.aud_output, cfg.output.path)
        metrics.lineage["aud_file"] = path
        log(logger, "INFO", "aud.done", path=path, files=file_count)
    except Exception as exc:  # noqa: BLE001
        if cfg.aud_output.fail_on_error:
            raise
        log(logger, "WARNING", "aud.soft_fail", error=str(exc))
    finally:
        metrics.end_stage("aud")


def _run_ttl(cfg, result, logger, metrics) -> None:  # noqa: ANN001
    metrics.start_stage("ttl")
    try:
        run_ttl_stage(result, cfg.ttl_output)
        log(logger, "INFO", "ttl.done", path=cfg.ttl_output.path)
    except Exception as exc:  # noqa: BLE001
        if cfg.ttl_output.fail_on_error:
            raise
        log(logger, "WARNING", "ttl.soft_fail", error=str(exc))
    finally:
        metrics.end_stage("ttl")
