"""Optional per-partition ``manifest.json`` stage.

After the primary write, this emits one manifest *inside* each partition
directory the run produced::

    s3a://bucket/customer/run_date=2026-06-28/manifest.json

describing the run date, the watermark range it covers, and every data shard
with its URI, byte size, and row count. Like the TTL and ``.aud`` stages it is
decoupled from the write — the shard list comes from the target path, never
from the DataFrame — and its failure handling is configurable (soft/hard fail).

Watermark tracking is framework-owned: ``checkpoint_from`` is read from the
most recent *prior* partition's manifest and ``checkpoint_to`` is stamped from
the run clock. Both are resolved by :func:`prepare_checkpoints` **before** the
write, because an ``overwrite`` write can replace the very partitions the
previous watermark would have been read from.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

from .. import __version__
from ..config.schema import ManifestOutputSpec, OutputSpec
from ..utils import fs

if TYPE_CHECKING:  # pragma: no cover - typing only, keeps the module import light
    from pyspark.sql import DataFrame, SparkSession


@dataclass(frozen=True)
class Checkpoints:
    """The watermark range a run covers."""

    checkpoint_from: int | None
    checkpoint_to: int


def stamp_checkpoint(spec: ManifestOutputSpec, now: datetime | None = None) -> int:
    """Render the run clock as an integer watermark via ``checkpoint_format``."""
    moment = now or datetime.now(timezone.utc)
    return int(moment.strftime(spec.checkpoint_format))


def prepare_checkpoints(
    spark: SparkSession | None,
    spec: ManifestOutputSpec,
    output: OutputSpec,
    now: datetime | None = None,
) -> Checkpoints:
    """Resolve the run's watermark range. Call this BEFORE the write.

    ``checkpoint_to`` is stamped from the clock; ``checkpoint_from`` is the
    highest ``checkpoint_to`` found in an existing partition's manifest, or
    ``initial_checkpoint`` on the first run. Explicit config values win over
    both, which is the escape hatch for backfills.
    """
    to = spec.checkpoint_to if spec.checkpoint_to is not None else stamp_checkpoint(spec, now)
    if spec.checkpoint_from is not None:
        return Checkpoints(spec.checkpoint_from, to)

    previous = _previous_checkpoint(spark, spec, output)
    return Checkpoints(previous if previous is not None else spec.initial_checkpoint, to)


def _previous_checkpoint(
    spark: SparkSession | None,
    spec: ManifestOutputSpec,
    output: OutputSpec,
) -> int | None:
    """Highest ``checkpoint_to`` among existing partitions' manifests.

    Partition directories are scanned newest-first (name-descending, which is
    chronological for ``run_date=YYYY-MM-DD``) and the scan stops at
    ``lookback_partitions`` so a table with thousands of partitions does not
    turn this into a full listing.

    Best-effort by design: this runs before the write, outside the stage's
    soft-fail handler, so an unreadable history must not fail the job. No prior
    watermark simply means ``initial_checkpoint``.
    """
    best: int | None = None
    try:
        candidates = _partition_dirs_on_disk(spark, spec, output)
        for directory in candidates[: spec.lookback_partitions]:
            raw = fs.read_text(spark, fs.join_uri(directory, spec.filename))
            if not raw:
                continue
            try:
                value = json.loads(raw).get("checkpoint_to")
            except (ValueError, AttributeError):
                continue
            if isinstance(value, int) and (best is None or value > best):
                best = value
    except Exception:  # noqa: BLE001 - a missing or unreadable target is just "no history"
        return best
    return best


def _partition_dirs_on_disk(
    spark: SparkSession | None,
    spec: ManifestOutputSpec,
    output: OutputSpec,
) -> list[str]:
    """Existing partition directories under ``output.path``, newest name first."""
    if not output.partition_by:
        return [output.path] if fs.exists(spark, output.path) else []

    level = [output.path.rstrip("/")]
    for _ in output.partition_by:
        nxt = []
        for parent in level:
            nxt.extend(
                fs.join_uri(parent, name)
                for name in fs.list_dirs(spark, parent)
                if not fs.is_hidden(name)
            )
        level = sorted(nxt, reverse=True)
    return level


def partition_values(df: DataFrame, partition_by: list[str]) -> list[dict[str, str]]:
    """Distinct partition-column values present in the result.

    This is the one thing the manifest needs the DataFrame for: it identifies
    which partitions *this* run wrote, so an ``append`` run does not restamp
    manifests for partitions it never touched.
    """
    if not partition_by:
        return [{}]
    rows = df.select(*partition_by).distinct().collect()
    return [{c: _partition_token(row[c]) for c in partition_by} for row in rows]


def _partition_token(value: Any) -> str:
    """Spark's Hive-style directory token for a partition value."""
    return "__HIVE_DEFAULT_PARTITION__" if value is None else str(value)


def partition_dir(output_path: str, values: dict[str, str], partition_by: list[str]) -> str:
    """Hive-style directory for one partition, e.g. ``<path>/run_date=2026-06-28``."""
    if not partition_by:
        return output_path.rstrip("/")
    return fs.join_uri(output_path, *[f"{c}={values[c]}" for c in partition_by])


def _row_counts_by_file(
    spark: SparkSession,
    output: OutputSpec,
    entries: list[fs.FileEntry],
) -> dict[str, int]:
    """Exact row count per data file, keyed by normalized URI.

    One extra scan of the written output. Delta is read as a table (its files
    are only meaningful through the log); every other format is read as an
    explicit file list, which also keeps a previously written ``manifest.json``
    in the directory from being parsed as data.
    """
    from pyspark.sql.functions import input_file_name

    # Write options (compression, ...) are deliberately not replayed on read.
    if output.format == "delta":
        df = spark.read.format("delta").load(output.path)
    else:
        df = spark.read.format(output.format).load([e.uri for e in entries])

    counts = df.groupBy(input_file_name().alias("_file")).count().collect()
    return {fs.normalize_uri(row["_file"]): int(row["count"]) for row in counts}


def build_manifest(
    run_date: str | None,
    checkpoints: Checkpoints,
    app_version: str,
    entries: list[fs.FileEntry],
    counts: dict[str, int] | None,
    uri_style: str,
) -> dict[str, Any]:
    """Assemble the manifest document for a single partition."""
    shards = []
    for index, entry in enumerate(sorted(entries, key=lambda e: e.relative)):
        shard: dict[str, Any] = {
            "index": index,
            "uri": entry.uri if uri_style == "absolute" else entry.relative,
            "size_bytes": entry.size_bytes,
        }
        if counts is not None:
            shard["row_count"] = counts.get(fs.normalize_uri(entry.uri), 0)
        shards.append(shard)

    total = sum(s["row_count"] for s in shards) if counts is not None else None
    return {
        "run_date": run_date,
        "checkpoint_from": checkpoints.checkpoint_from,
        "checkpoint_to": checkpoints.checkpoint_to,
        "row_count": total,
        "app_version": app_version,
        "shards": shards,
    }


def run_manifest_stage(
    spark: SparkSession | None,
    df: DataFrame,
    spec: ManifestOutputSpec,
    output: OutputSpec,
    checkpoints: Checkpoints,
    run_date: str | None = None,
) -> list[str]:
    """Write one ``manifest.json`` per partition this run produced.

    Returns the manifest paths written.
    """
    app_version = spec.app_version or __version__
    written: list[str] = []

    for values in partition_values(df, output.partition_by):
        directory = partition_dir(output.path, values, output.partition_by)
        # A partition value the write produced no directory for (an empty
        # partition) gets no manifest, rather than failing the rest.
        if not fs.exists(spark, directory):
            continue
        entries = [
            e
            for e in fs.list_files(spark, directory, spec.include_hidden)
            if e.relative != spec.filename
        ]

        counts: dict[str, int] | None = None
        if spec.row_counts:
            counts = _row_counts_by_file(spark, output, entries) if entries else {}

        document = build_manifest(
            # For a partitioned write the directory *is* the run date; fall back
            # to the caller's value when the output is not partitioned.
            run_date=values.get("run_date", run_date),
            checkpoints=checkpoints,
            app_version=app_version,
            entries=entries,
            counts=counts,
            uri_style=spec.uri_style,
        )
        target = fs.join_uri(directory, spec.filename)
        fs.write_text(spark, target, json.dumps(document, indent=2) + "\n")
        written.append(target)

    return written
