"""Run configured data quality checks against a DataFrame."""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession

from ..config.schema import DataQualitySpec
from .expectations import CHECKERS, CheckResult


class DataQualityError(RuntimeError):
    """Raised when a check fails and ``fail_on_error`` is set."""


def run_quality(
    df: DataFrame, spec: DataQualitySpec, spark: SparkSession | None = None
) -> list[CheckResult]:
    """Execute all configured checks; raise on failure if configured."""
    if not spec.enabled:
        return []
    spark = spark or df.sparkSession
    results: list[CheckResult] = []
    for check in spec.checks:
        checker = CHECKERS.get(check.type)
        if checker is None:
            raise ValueError(f"Unknown data quality check: {check.type}")
        results.append(checker(df, check, spark))

    failures = [r for r in results if not r.passed]
    if failures and spec.fail_on_error:
        detail = "; ".join(f"{r.check_type}: {r.detail}" for r in failures)
        raise DataQualityError(f"Data quality checks failed: {detail}")
    return results
