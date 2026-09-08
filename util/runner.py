"""Minimal Spark job runner: read -> SQL -> write.

A job script declares its own SQL and calls :func:`main`. Everything
environment-specific comes from the ``.env`` file, so the same script runs
unchanged against dev, uat, preprod, and prod.

Optionally writes a ``manifest.json`` into each partition directory the run
produced, listing every shard with its URI, size, and row count, plus the
watermark range the run covers.

All filesystem access goes through the JVM Hadoop API, which handles ``s3a://``
and plain local paths alike — so there is one code path, not two.
"""

from __future__ import annotations

import argparse
import json
import logging
import re
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

from util import config
from util.config import ConfigError, Settings

if TYPE_CHECKING:  # pragma: no cover - typing only
    from pyspark.sql import DataFrame, SparkSession

log = logging.getLogger("etl")

MANIFEST_NAME = "manifest.json"
_LOOKBACK_PARTITIONS = 32
# Leading URI scheme: "file:/", "file:///", "s3a://", or a Windows "C:/".
_SCHEME = re.compile(r"^[A-Za-z][A-Za-z0-9+.-]*:/+")


@dataclass
class Job:
    """One data feed. Declared at the top of its own script."""

    name: str
    sql: str
    # View name -> source. A bare string is a path. A dict is either a file
    # source ({"path", "format", "options"}) or a Hive table ({"table"}).
    inputs: dict[str, Any]
    output: str
    format: str = "parquet"
    # append | overwrite | merge  (merge is delta-only, upsert on merge_keys)
    mode: str = "overwrite"
    partition_by: list[str] = field(default_factory=list)
    merge_keys: list[str] = field(default_factory=list)
    options: dict[str, str] = field(default_factory=dict)
    input_format: str = "parquet"
    # Write a manifest.json into each partition directory this run produced.
    manifest: bool = False

    def validate(self) -> None:
        """Fail before Spark starts, not halfway through a write."""
        if self.mode not in ("append", "overwrite", "merge"):
            raise ConfigError(f"{self.name}: mode must be append, overwrite, or merge")
        if self.mode == "merge":
            if self.format != "delta":
                raise ConfigError(f"{self.name}: mode 'merge' requires format 'delta'")
            if not self.merge_keys:
                raise ConfigError(f"{self.name}: mode 'merge' requires merge_keys")
        if not self.inputs:
            raise ConfigError(f"{self.name}: at least one input is required")

        for view, source in self.inputs.items():
            spec = _spec(source)
            has_path, has_table = "path" in spec, "table" in spec
            if has_path == has_table:
                raise ConfigError(
                    f"{self.name}: input '{view}' needs exactly one of 'path' or 'table', "
                    f"got {sorted(spec) or 'nothing'}"
                )
            if has_table and ("format" in spec or "options" in spec):
                raise ConfigError(
                    f"{self.name}: input '{view}' is a table, so 'format' and 'options' "
                    "do not apply - the metastore describes it"
                )

    def reads_hive(self) -> bool:
        """Whether any input is a metastore table, so the session needs Hive."""
        return any("table" in _spec(source) for source in self.inputs.values())


# --------------------------------------------------------------------------
# Spark
# --------------------------------------------------------------------------


def build_spark(job: Job, settings: Settings) -> SparkSession:
    """Build a session from ``.env``: SPARK_MASTER and any SPARK_CONF.* keys."""
    from pyspark.sql import SparkSession

    app_name = f"{job.name}-{settings.environment}"
    builder = SparkSession.builder.appName(app_name)

    master = settings.get("SPARK_MASTER")
    if master:
        builder = builder.master(master)

    conf = settings.prefixed("SPARK_CONF.")
    if job.format == "delta" or any(_fmt(src, job) == "delta" for src in job.inputs.values()):
        conf.setdefault("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        conf.setdefault(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
    for key, value in conf.items():
        builder = builder.config(key, value)

    if job.reads_hive():
        # The metastore itself is configured by the cluster's hive-site.xml, or
        # by SPARK_CONF.hive.metastore.uris in the .env file.
        builder = builder.enableHiveSupport()

    return builder.getOrCreate()


def _spec(source: Any) -> dict[str, Any]:
    """Normalize an input entry: a bare string is a path."""
    return source if isinstance(source, dict) else {"path": source}


def _fmt(source: Any, job: Job) -> str:
    """Read format for a *file* source. Meaningless for a table."""
    spec = _spec(source)
    if "table" in spec:
        return "hive"
    return spec.get("format", job.input_format)


def load_source(
    spark: SparkSession, job: Job, settings: Settings, source: Any, **params: object
) -> tuple[DataFrame, str]:
    """Load one input. Returns the DataFrame and a loggable description.

    Two kinds of source:

    * ``{"table": "db.name"}`` — read through the metastore with
      ``spark.table``. Partition pruning still happens: the job's SQL filters
      are pushed down through the temp view into the table scan.
    * ``{"path": ...}`` or a bare string — read files with the DataFrameReader.
      Any scheme Hadoop understands works, ``hdfs://`` included.
    """
    spec = _spec(source)

    if "table" in spec:
        table = settings.resolve(spec["table"], **params)
        return spark.table(table), f"table {table}"

    path = settings.resolve(spec["path"], **params)
    reader = spark.read.format(_fmt(source, job))
    for key, value in (spec.get("options") or {}).items():
        reader = reader.option(key, value)
    return reader.load(path), path


def read_inputs(
    spark: SparkSession, job: Job, settings: Settings, **params: object
) -> dict[str, int]:
    """Register every input as a temp view named by its key. Returns row counts."""
    counts: dict[str, int] = {}
    for view, source in job.inputs.items():
        df, described = load_source(spark, job, settings, source, **params)
        df.createOrReplaceTempView(view)
        counts[view] = df.count()
        log.info("read %-16s %-9s rows=%-9s %s", view, _fmt(source, job), counts[view], described)
    return counts


# --------------------------------------------------------------------------
# Write
# --------------------------------------------------------------------------


def write(df: DataFrame, job: Job, output_path: str) -> None:
    """Write per the job's format and mode. ``merge`` upserts into Delta."""
    if job.mode == "merge":
        _merge(df, job, output_path)
        return

    writer = df.write.format(job.format).mode(job.mode)
    if job.partition_by:
        writer = writer.partitionBy(*job.partition_by)
    for key, value in job.options.items():
        writer = writer.option(key, value)
    writer.save(output_path)


def _merge(df: DataFrame, job: Job, output_path: str) -> None:
    """Upsert on ``merge_keys``; create the table on first run."""
    from delta.tables import DeltaTable

    spark = df.sparkSession
    if not DeltaTable.isDeltaTable(spark, output_path):
        writer = df.write.format("delta").mode("overwrite")
        if job.partition_by:
            writer = writer.partitionBy(*job.partition_by)
        writer.save(output_path)
        return

    condition = " AND ".join(f"t.{k} = s.{k}" for k in job.merge_keys)
    (
        DeltaTable.forPath(spark, output_path)
        .alias("t")
        .merge(df.alias("s"), condition)
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )


# --------------------------------------------------------------------------
# Hadoop filesystem helpers (work for s3a://, hdfs://, and local paths alike)
# --------------------------------------------------------------------------


def _fs(spark: SparkSession, path: str):  # noqa: ANN202 - JVM handles
    jvm = spark._jvm
    jpath = jvm.org.apache.hadoop.fs.Path(path)
    return jvm, jpath.getFileSystem(spark._jsc.hadoopConfiguration()), jpath


def _is_hidden(name: str) -> bool:
    """Spark side-cars: ``_SUCCESS``, ``_delta_log/``, ``.crc``."""
    return any(part.startswith((".", "_")) for part in name.split("/") if part)


def _list_files(spark: SparkSession, directory: str) -> list[tuple[str, str, int]]:
    """``(relative, uri, size_bytes)`` for each data file, sorted."""
    _, fs, jpath = _fs(spark, directory)
    if not fs.exists(jpath):
        return []
    root = str(fs.makeQualified(jpath)).rstrip("/")
    out = []
    files = fs.listFiles(jpath, True)
    while files.hasNext():
        status = files.next()
        uri = str(status.getPath())
        relative = uri[len(root) :].lstrip("/") if uri.startswith(root) else uri
        if _is_hidden(relative) or relative == MANIFEST_NAME:
            continue
        out.append((relative, uri, int(status.getLen())))
    return sorted(out)


def _read_text(spark: SparkSession, path: str) -> str | None:
    jvm, fs, jpath = _fs(spark, path)
    if not fs.exists(jpath):
        return None
    stream = fs.open(jpath)
    try:
        return str(jvm.org.apache.commons.io.IOUtils.toString(stream, "UTF-8"))
    finally:
        stream.close()


def _write_text(spark: SparkSession, path: str, text: str) -> None:
    _, fs, jpath = _fs(spark, path)
    stream = fs.create(jpath, True)  # overwrite
    try:
        stream.write(bytearray(text.encode("utf-8")))
    finally:
        stream.close()


def _subdirs(spark: SparkSession, directory: str) -> list[str]:
    _, fs, jpath = _fs(spark, directory)
    if not fs.exists(jpath):
        return []
    return sorted(
        f"{directory.rstrip('/')}/{status.getPath().getName()}"
        for status in fs.listStatus(jpath)
        if status.isDirectory() and not _is_hidden(str(status.getPath().getName()))
    )


# --------------------------------------------------------------------------
# Manifest
# --------------------------------------------------------------------------


def previous_checkpoint(
    spark: SparkSession, job: Job, output_path: str, settings: Settings
) -> int | None:
    """Highest ``checkpoint_to`` in an existing partition's manifest.

    Call this BEFORE the write: an overwrite replaces the very partitions the
    previous watermark would be read from. Best-effort — unreadable history
    means "no previous run", never a failed job.
    """
    override = settings.get("CHECKPOINT_FROM")
    if override:
        return int(override)

    best: int | None = None
    try:
        dirs = _subdirs(spark, output_path) if job.partition_by else [output_path]
        for directory in sorted(dirs, reverse=True)[:_LOOKBACK_PARTITIONS]:
            raw = _read_text(spark, f"{directory.rstrip('/')}/{MANIFEST_NAME}")
            if not raw:
                continue
            value = json.loads(raw).get("checkpoint_to")
            if isinstance(value, int) and (best is None or value > best):
                best = value
    except Exception as exc:  # noqa: BLE001 - history is optional, the run is not
        log.warning("could not read previous checkpoint: %s", exc)
    return best


def write_manifests(
    spark: SparkSession,
    df: DataFrame,
    job: Job,
    output_path: str,
    checkpoint_from: int | None,
    checkpoint_to: int,
    settings: Settings,
    run_date: str | None,
) -> list[str]:
    """Write ``manifest.json`` into each partition directory this run produced."""
    app_version = settings.get("APP_VERSION", "0.0.0")

    if job.partition_by:
        rows = df.select(*job.partition_by).distinct().collect()
        targets = [
            (
                f"{output_path.rstrip('/')}/" + "/".join(f"{c}={row[c]}" for c in job.partition_by),
                str(row[job.partition_by[0]]),
            )
            for row in rows
        ]
    else:
        targets = [(output_path.rstrip("/"), run_date)]

    written = []
    for directory, partition_value in targets:
        files = _list_files(spark, directory)
        if not files:
            log.warning("no files under %s - skipping its manifest", directory)
            continue

        counts = _row_counts(spark, job, output_path, [uri for _, uri, _ in files])
        shards = [
            {
                "index": index,
                "uri": uri,
                "size_bytes": size,
                "row_count": counts.get(_key(uri), 0),
            }
            for index, (_, uri, size) in enumerate(files)
        ]
        document = {
            "run_date": partition_value if job.partition_by else run_date,
            "checkpoint_from": checkpoint_from,
            "checkpoint_to": checkpoint_to,
            "row_count": sum(s["row_count"] for s in shards),
            "app_version": app_version,
            "shards": shards,
        }
        target = f"{directory.rstrip('/')}/{MANIFEST_NAME}"
        _write_text(spark, target, json.dumps(document, indent=2) + "\n")
        written.append(target)
        log.info("manifest %s (%d shards, %d rows)", target, len(shards), document["row_count"])
    return written


def _row_counts(spark: SparkSession, job: Job, output_path: str, uris: list[str]) -> dict[str, int]:
    """Exact rows per file, from one re-read of what was just written.

    Non-Delta formats are read as an explicit file list rather than as a
    directory, so a manifest.json left by an earlier run is never parsed as
    data.
    """
    from pyspark.sql.functions import input_file_name

    reader = spark.read.format(job.format)
    df = reader.load(output_path) if job.format == "delta" else reader.load(uris)
    rows = df.groupBy(input_file_name().alias("_f")).count().collect()
    return {_key(row["_f"]): int(row["count"]) for row in rows}


def _key(uri: str) -> str:
    """Scheme-insensitive key so a listing URI matches ``input_file_name()``.

    Hadoop's ``makeQualified`` reports ``file:/tmp/part-0.parquet`` where
    ``input_file_name()`` reports ``file:///tmp/part-0.parquet``; both reduce to
    the same key, as does ``s3a://bucket/k/part-0.parquet``.
    """
    return _SCHEME.sub("", uri.replace("\\", "/")).strip("/").lower()


# --------------------------------------------------------------------------
# Run
# --------------------------------------------------------------------------


def run(
    job: Job,
    settings: Settings,
    run_date: str,
    dry_run: bool = False,
    spark: SparkSession | None = None,
) -> dict[str, Any]:
    """Execute one job end to end. Returns a small metrics dict."""
    job.validate()
    started = time.perf_counter()
    owns_session = spark is None
    params = {"run_date": run_date, **settings.values}
    output_path = settings.resolve(job.output, **params)

    log.info(
        "start %s env=%s run_date=%s dry_run=%s", job.name, settings.environment, run_date, dry_run
    )
    metrics: dict[str, Any] = {"job": job.name, "environment": settings.environment}

    try:
        spark = spark or build_spark(job, settings)
        metrics["input_rows"] = read_inputs(spark, job, settings, **params)

        result = spark.sql(settings.resolve(job.sql, **params))
        result.cache()
        metrics["output_rows"] = result.count()
        metrics["target"] = output_path
        log.info("transform rows=%s -> %s", metrics["output_rows"], output_path)

        if dry_run:
            log.info("dry run - nothing written")
            result.explain(mode="formatted")
            return metrics

        # Resolved before the write: an overwrite can delete the partitions the
        # previous watermark would be read from.
        checkpoint_to = int(
            datetime.now(timezone.utc).strftime(settings.get("CHECKPOINT_FORMAT", "%Y%m%d%H%M%S"))
        )
        checkpoint_from = (
            previous_checkpoint(spark, job, output_path, settings) if job.manifest else None
        )

        write(result, job, output_path)
        log.info("wrote %s (%s, mode=%s)", output_path, job.format, job.mode)

        if job.manifest:
            metrics["manifests"] = write_manifests(
                spark, result, job, output_path, checkpoint_from, checkpoint_to, settings, run_date
            )

        metrics["seconds"] = round(time.perf_counter() - started, 2)
        log.info("done %s in %ss", job.name, metrics["seconds"])
        return metrics
    finally:
        if spark is not None and owns_session:
            spark.stop()


def main(job: Job, argv: list[str] | None = None) -> int:
    """CLI entry point for a job script."""
    parser = argparse.ArgumentParser(prog=job.name, description=f"Run the {job.name} feed.")
    parser.add_argument("--env", choices=config.ENVIRONMENTS, help="Defaults to $APP_ENV")
    parser.add_argument("--run-date", required=True, metavar="YYYY-MM-DD")
    parser.add_argument("--dry-run", action="store_true", help="Plan only, write nothing")
    parser.add_argument("--log-level", default="INFO")
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=args.log_level.upper(),
        format="%(asctime)s %(levelname)-7s %(message)s",
        datefmt="%H:%M:%S",
    )

    try:
        settings = config.load(args.env)
        run(job, settings, args.run_date, args.dry_run)
    except ConfigError as exc:
        log.error("%s", exc)
        return 2
    return 0


if __name__ == "__main__":  # pragma: no cover
    sys.exit("Run a job script instead, e.g. python -m jobs.customer --env dev --run-date ...")
