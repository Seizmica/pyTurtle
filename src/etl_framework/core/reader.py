"""Multi-format input readers that register temp views."""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession

from ..config.schema import InputSpec


def read_input(spark: SparkSession, spec: InputSpec) -> DataFrame:
    """Read a single input dataset per its format and options."""
    reader = spark.read.format(spec.format)
    for key, value in spec.options.items():
        reader = reader.option(key, value)
    return reader.load(spec.path)


def register_inputs(
    spark: SparkSession, specs: list[InputSpec], count_rows: bool = True
) -> dict[str, int]:
    """Read each input and register it as a temp view named by ``table``.

    Returns a mapping of view name -> row count for metrics. When
    ``count_rows`` is False the extra full scan is skipped and counts are -1.
    """
    counts: dict[str, int] = {}
    for spec in specs:
        df = read_input(spark, spec)
        df.createOrReplaceTempView(spec.table)
        counts[spec.table] = df.count() if count_rows else -1
    return counts
