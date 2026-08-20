"""Load a .sql file and execute it via spark.sql."""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession

from ..utils.io_utils import read_text, substitute_params


def transform(
    spark: SparkSession,
    sql_file: str,
    sql_params: dict[str, object] | None = None,
) -> DataFrame:
    """Load ``sql_file``, substitute params, and run it as a Spark SQL query."""
    sql = read_text(sql_file)
    if sql_params:
        sql = substitute_params(sql, sql_params)
    return spark.sql(sql)
