"""Individual data quality expectations.

Each checker takes the DataFrame under test, a :class:`QualityCheck`, and the
active SparkSession (needed for referential/custom checks), and returns a
:class:`CheckResult`. Register new checkers in ``CHECKERS``.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Callable

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from ..config.schema import QualityCheck


@dataclass
class CheckResult:
    check_type: str
    passed: bool
    detail: str


def check_not_null(df: DataFrame, check: QualityCheck, spark: SparkSession) -> CheckResult:
    failing = []
    for col in check.columns:
        nulls = df.filter(F.col(col).isNull()).count()
        if nulls > 0:
            failing.append(f"{col}={nulls}")
    return CheckResult(
        "not_null",
        passed=not failing,
        detail="null counts: " + (", ".join(failing) or "none"),
    )


def check_unique(df: DataFrame, check: QualityCheck, spark: SparkSession) -> CheckResult:
    total = df.count()
    distinct = df.select(*check.columns).distinct().count()
    dups = total - distinct
    return CheckResult(
        "unique",
        passed=dups == 0,
        detail=f"duplicate rows on {check.columns}: {dups}",
    )


def check_row_count_min(df: DataFrame, check: QualityCheck, spark: SparkSession) -> CheckResult:
    count = df.count()
    minimum = int(check.value)
    return CheckResult(
        "row_count_min",
        passed=count >= minimum,
        detail=f"row_count={count}, min={minimum}",
    )


def check_referential(df: DataFrame, check: QualityCheck, spark: SparkSession) -> CheckResult:
    """Every value in ``columns[0]`` must exist in ``ref_table.ref_column``.

    ``ref_table`` is a registered input temp view (e.g. a dimension table).
    """
    child_col = check.columns[0]
    parent = spark.table(check.ref_table).select(F.col(check.ref_column).alias("_ref"))
    orphans = (
        df.select(F.col(child_col).alias("_child"))
        .where(F.col("_child").isNotNull())
        .join(parent, F.col("_child") == F.col("_ref"), "left_anti")
        .count()
    )
    return CheckResult(
        "referential",
        passed=orphans == 0,
        detail=(
            f"{child_col} -> {check.ref_table}.{check.ref_column}: " f"{orphans} orphan value(s)"
        ),
    )


def check_custom(df: DataFrame, check: QualityCheck, spark: SparkSession) -> CheckResult:
    """A SQL predicate that must hold for every row.

    ``expression`` is a boolean SQL expression over the result columns; any
    row where it is not true counts as a violation.
    """
    violations = df.where(f"NOT ({check.expression})").count()
    label = check.name or check.expression
    return CheckResult(
        "custom",
        passed=violations == 0,
        detail=f"{label}: {violations} violation(s)",
    )


CHECKERS: dict[str, Callable[[DataFrame, QualityCheck, SparkSession], CheckResult]] = {
    "not_null": check_not_null,
    "unique": check_unique,
    "row_count_min": check_row_count_min,
    "referential": check_referential,
    "custom": check_custom,
}
