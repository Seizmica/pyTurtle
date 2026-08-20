"""Multi-format output writers with atomic staged commits."""

from __future__ import annotations

import uuid

from pyspark.sql import DataFrame

from ..config.schema import OutputSpec


def _raw_write(df: DataFrame, spec: OutputSpec, path: str) -> None:
    writer = df.write.format(spec.format).mode(spec.mode)
    if spec.partition_by:
        writer = writer.partitionBy(*spec.partition_by)
    for key, value in spec.options.items():
        writer = writer.option(key, value)
    writer.save(path)


def _hadoop_fs(df: DataFrame, path: str):
    """Return (FileSystem, Path) for ``path`` via the JVM Hadoop API."""
    jvm = df.sparkSession._jvm
    hconf = df.sparkSession._jsc.hadoopConfiguration()
    jpath = jvm.org.apache.hadoop.fs.Path(path)
    fs = jpath.getFileSystem(hconf)
    return fs, jpath, jvm


def _merge_write(df: DataFrame, spec: OutputSpec) -> None:
    """Upsert into a Delta table on ``merge_keys`` (incremental load).

    Creates the target table on first run; thereafter matches on the merge
    keys, updating existing rows and inserting new ones.
    """
    from delta.tables import DeltaTable

    spark = df.sparkSession
    if not DeltaTable.isDeltaTable(spark, spec.path):
        writer = df.write.format("delta").mode("overwrite")
        if spec.partition_by:
            writer = writer.partitionBy(*spec.partition_by)
        writer.save(spec.path)
        return

    target = DeltaTable.forPath(spark, spec.path)
    condition = " AND ".join(f"t.{k} = s.{k}" for k in spec.merge_keys)
    (
        target.alias("t")
        .merge(df.alias("s"), condition)
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )


def write_output(df: DataFrame, spec: OutputSpec) -> None:
    """Write a DataFrame per the configured format, mode, and partitioning.

    For overwrite mode with ``atomic`` enabled, the data is first written to a
    sibling staging directory and only committed to the final path once the
    write fully succeeds — avoiding partially written output on failure.
    Append mode always writes directly (a staged append cannot be merged
    atomically without duplicating existing data). Merge mode performs a Delta
    upsert on ``merge_keys`` for incremental loads.
    """
    if spec.mode == "merge":
        _merge_write(df, spec)
        return

    if not (spec.atomic and spec.mode == "overwrite"):
        _raw_write(df, spec, spec.path)
        return

    staging = f"{spec.path.rstrip('/')}__staging_{uuid.uuid4().hex}"
    fs, final_path, jvm = _hadoop_fs(df, spec.path)
    _, staging_path, _ = _hadoop_fs(df, staging)

    try:
        _raw_write(df, spec, staging)
        # Commit: replace the final path with the freshly staged output.
        if fs.exists(final_path):
            fs.delete(final_path, True)
        if not fs.rename(staging_path, final_path):
            raise RuntimeError(f"Atomic commit failed: could not rename {staging} -> {spec.path}")
    finally:
        if fs.exists(staging_path):
            fs.delete(staging_path, True)
