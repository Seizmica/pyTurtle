"""Merge a feed's small output files into files of about a target size.

Spark writes one file per task per partition directory, so a job that shuffles
into ``spark.sql.shuffle.partitions=200`` can leave 200 small files in every
``run_date=`` directory. This rewrites each directory's data files into
``ceil(total_size / target)`` files of roughly equal size.

    python -m jobs.compact --env prod --run-date 2026-06-28 \\
        --path '${CURATED_ROOT}/customer/run_date=${run_date}/' --format parquet
    ./submit.sh compact prod 2026-06-28 \\
        --path '${EXPORT_ROOT}/customer/' --format csv --option header=true --dry-run

``--path`` may be one partition directory or a whole table: every directory
under it that directly holds data files is planned on its own.

Each directory is rewritten through a staging area, so a failure at any point
leaves either the original files or a recoverable commit, never a mix:

1. Read the directory's data files and write them to ``<dir>/_compaction/data``.
   Spark readers skip ``_``-prefixed directories, so staging is invisible.
2. Check that the staged files hold exactly as many rows as the originals.
3. Write ``<dir>/_compaction/_READY``, which lists the originals. From here on
   the compaction is committed: a rerun finishes it rather than redoing it.
4. Delete the originals, move the staged files in, refresh ``manifest.json``
   if the directory has one, and remove staging.

Not safe to run while the feed itself is writing the same directory, and
readers that list the directory during step 4 can briefly see it incomplete.
Schedule it after the feed and before consumers.

Delta tables are refused: rewriting their files behind the transaction log
corrupts the table. Use ``OPTIMIZE`` for those.
"""

from __future__ import annotations

import argparse
import json
import logging
import math
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

from util import config
from util.config import ConfigError
from util.runner import (
    EXIT_CONFIG,
    EXIT_SESSION,
    EXIT_UNKNOWN,
    EXIT_WRITE,
    MANIFEST_NAME,
    Job,
    Output,
    StageFailure,
    _fs,
    _is_hidden,
    _key,
    _read_text,
    _row_counts,
    _stage,
    _write_text,
    build_spark,
)

if TYPE_CHECKING:  # pragma: no cover - typing only
    from pyspark.sql import DataFrameReader, SparkSession

log = logging.getLogger("etl")

FORMATS = ("parquet", "csv")
STAGING = "_compaction"
READY = "_READY"
DEFAULT_TARGET_MB = 50
# Plan files somewhat below the limit: the rewritten files vary a little in
# size, and merged data compresses differently from the pieces it came from.
FILL = 0.9
# One rewrite, plus one more if a file still came out over the limit.
MAX_ATTEMPTS = 2

# File-name extension -> the codec name Spark's `compression` option takes.
# Parquet names it before the format (`.snappy.parquet`), csv after (`.csv.gz`).
_CODECS = {
    "parquet": {"snappy": "snappy", "gz": "gzip", "zstd": "zstd", "brotli": "brotli", "lzo": "lzo"},
    "csv": {"gz": "gzip", "bz2": "bzip2", "deflate": "deflate", "lz4": "lz4", "snappy": "snappy"},
}


@dataclass
class Plan:
    """What compacting one directory would do."""

    directory: str
    files: list[tuple[str, int]]  # (uri, size_bytes)
    target_bytes: int

    @property
    def total_bytes(self) -> int:
        return sum(size for _, size in self.files)

    @property
    def target_files(self) -> int:
        return target_file_count(self.total_bytes, self.target_bytes)

    @property
    def needed(self) -> bool:
        """Only when the rewrite would leave fewer files than there are now."""
        return len(self.files) > self.target_files


def target_file_count(total_bytes: int, target_bytes: int) -> int:
    return max(1, math.ceil(total_bytes / (target_bytes * FILL)))


def is_data_file(name: str, fmt: str) -> bool:
    """A Spark output file of ``fmt``; anything else in the directory is left alone."""
    if _is_hidden(name) or name == MANIFEST_NAME:
        return False
    if fmt == "parquet":
        return name.endswith(".parquet")
    return name.endswith(".csv") or ".csv." in name


def infer_codec(fmt: str, names: list[str]) -> str | None:
    """The compression the existing files use, so the rewrite keeps it.

    ``None`` when no name says, which leaves Spark's default in place.
    """

    def codec(name: str) -> str | None:
        if fmt == "parquet":
            stem = name[: -len(".parquet")]
            ext = stem.rsplit(".", 1)[1] if "." in stem else ""
        else:
            ext = name.rsplit(".csv", 1)[1].lstrip(".")
        if not ext:
            return "none"
        return _CODECS[fmt].get(ext)

    found = Counter(codec(name) for name in names)
    if not found:
        return None
    if len(found) > 1:
        log.warning("mixed compression %s - rewriting with the most common", dict(found))
    return found.most_common(1)[0][0]


def parse_options(pairs: list[str] | None) -> dict[str, str]:
    options: dict[str, str] = {}
    for pair in pairs or []:
        key, sep, value = pair.partition("=")
        if not sep or not key:
            raise ConfigError(f"--option expects KEY=VALUE, got {pair!r}")
        options[key.strip()] = value
    return options


# --------------------------------------------------------------------------
# Filesystem
# --------------------------------------------------------------------------


def _data_files(spark: SparkSession, directory: str, fmt: str) -> list[tuple[str, int]]:
    """``(uri, size)`` of the data files directly inside ``directory``, sorted."""
    _, fs, jpath = _fs(spark, directory)
    if not fs.exists(jpath):
        return []
    return sorted(
        (str(status.getPath()), int(status.getLen()))
        for status in fs.listStatus(jpath)
        if status.isFile() and is_data_file(str(status.getPath().getName()), fmt)
    )


def _delete(spark: SparkSession, path: str) -> None:
    _, fs, jpath = _fs(spark, path)
    if fs.exists(jpath) and not fs.delete(jpath, True):
        raise OSError(f"could not delete {path}")


def _qualified(spark: SparkSession, path: str) -> str:
    _, fs, jpath = _fs(spark, path)
    return str(fs.makeQualified(jpath)).rstrip("/")


def _inside_delta_table(spark: SparkSession, path: str) -> bool:
    """Whether ``path`` is a Delta table or sits inside one."""
    jvm, fs, jpath = _fs(spark, path)
    current = fs.makeQualified(jpath)
    while current is not None:
        if fs.exists(jvm.org.apache.hadoop.fs.Path(current, "_delta_log")):
            return True
        current = current.getParent()
    return False


def discover(
    spark: SparkSession, root: str, fmt: str
) -> tuple[dict[str, list[tuple[str, int]]], set[str]]:
    """Data files under ``root`` grouped by directory, and directories with staging.

    Directories inside a Delta table nested under ``root`` are left out.
    """
    _, fs, jpath = _fs(spark, root)
    if not fs.exists(jpath):
        raise ConfigError(f"nothing to compact: {root} does not exist")
    base = _qualified(spark, root)

    groups: dict[str, list[tuple[str, int]]] = {}
    staged: set[str] = set()
    delta_roots: set[str] = set()

    files = fs.listFiles(jpath, True)
    while files.hasNext():
        status = files.next()
        path = status.getPath()
        uri = str(path)
        parts = uri[len(base) :].strip("/").split("/") if uri.startswith(base) else [uri]
        if STAGING in parts:
            staged.add("/".join([base, *parts[: parts.index(STAGING)]]))
            continue
        if "_delta_log" in parts:
            delta_roots.add("/".join([base, *parts[: parts.index("_delta_log")]]))
            continue
        if _is_hidden("/".join(parts)) or not is_data_file(str(path.getName()), fmt):
            continue
        groups.setdefault(str(path.getParent()), []).append((uri, int(status.getLen())))

    for directory in [
        d for d in groups if any(d == r or d.startswith(r + "/") for r in delta_roots)
    ]:
        log.warning("skipping %s: it belongs to a Delta table - use OPTIMIZE", directory)
        del groups[directory]
    return {d: sorted(f) for d, f in groups.items()}, staged


# --------------------------------------------------------------------------
# Compaction
# --------------------------------------------------------------------------


def _reader(spark: SparkSession, fmt: str, options: dict[str, str]) -> DataFrameReader:
    reader = spark.read.format(fmt)
    for key, value in options.items():
        reader = reader.option(key, value)
    return reader


def _rewrite_options(
    fmt: str, options: dict[str, str], files: list[tuple[str, int]]
) -> tuple[dict[str, str], dict[str, str]]:
    """Reader and writer options for one directory."""
    read = dict(options)
    write = dict(options)
    if fmt == "parquet":
        # Files appended by different runs may carry different columns; without
        # this, the rewrite would keep one file's schema and drop the rest.
        read.setdefault("mergeSchema", "true")
    if "compression" not in options:
        codec = infer_codec(fmt, [uri.rsplit("/", 1)[-1] for uri, _ in files])
        if codec:
            write["compression"] = codec
    read.pop("compression", None)
    return read, write


def compact_directory(spark: SparkSession, plan: Plan, fmt: str, options: dict[str, str]) -> dict:
    """Rewrite one directory's files into ``plan.target_files`` files."""
    staging = f"{plan.directory}/{STAGING}"
    data = f"{staging}/data"
    read_options, write_options = _rewrite_options(fmt, options, plan.files)
    originals = [uri for uri, _ in plan.files]

    source = _reader(spark, fmt, read_options).load(originals)
    rows = source.count()

    count = plan.target_files
    for attempt in range(1, MAX_ATTEMPTS + 1):
        # repartition (not coalesce) deals rows out round-robin, so the files
        # come out close to equal in size. An explicit count is never changed
        # by adaptive query execution.
        writer = source.repartition(count).write.format(fmt).mode("overwrite")
        for key, value in write_options.items():
            writer = writer.option(key, value)
        writer.save(data)

        staged = _data_files(spark, data, fmt)
        largest = max((size for _, size in staged), default=0)
        if largest <= plan.target_bytes or attempt == MAX_ATTEMPTS:
            break
        count = math.ceil(count * largest / (plan.target_bytes * FILL))
        log.info("largest file %d bytes is over the limit - rewriting as %d", largest, count)

    if largest > plan.target_bytes:
        log.warning(
            "%s: largest file is %.1f MB after %d attempts, over the %.1f MB target",
            plan.directory,
            largest / 2**20,
            MAX_ATTEMPTS,
            plan.target_bytes / 2**20,
        )

    written = _reader(spark, fmt, read_options).load([uri for uri, _ in staged]).count()
    if written != rows:
        _delete(spark, staging)
        raise RuntimeError(
            f"{plan.directory}: staged files hold {written} rows, originals {rows} - "
            "nothing was replaced"
        )

    ready = {
        "format": fmt,
        "read_options": read_options,
        "rows": rows,
        "originals": originals,
    }
    _write_text(spark, f"{staging}/{READY}", json.dumps(ready, indent=2) + "\n")
    return commit(spark, plan.directory, ready)


def commit(spark: SparkSession, directory: str, ready: dict[str, Any]) -> dict:
    """Swap the staged files in. Safe to repeat: every step skips what is done."""
    staging = f"{directory}/{STAGING}"
    fmt = ready["format"]
    jvm, fs, _ = _fs(spark, directory)
    path = jvm.org.apache.hadoop.fs.Path

    for uri in ready["originals"]:
        original = path(uri)
        if fs.exists(original) and not fs.delete(original, False):
            raise OSError(f"could not delete {uri}")

    for uri, _ in _data_files(spark, f"{staging}/data", fmt):
        source = path(uri)
        target = path(f"{directory}/{source.getName()}")
        if not fs.rename(source, target):
            raise OSError(f"could not move {uri} to {target}")

    files = _data_files(spark, directory, fmt)
    _refresh_manifest(spark, directory, ready, files)
    _delete(spark, staging)

    log.info(
        "compacted %s: %d -> %d files, %d rows",
        directory,
        len(ready["originals"]),
        len(files),
        ready["rows"],
    )
    return {
        "directory": directory,
        "files_before": len(ready["originals"]),
        "files_after": len(files),
        "largest_bytes": max((size for _, size in files), default=0),
        "rows": ready["rows"],
    }


def _refresh_manifest(
    spark: SparkSession, directory: str, ready: dict[str, Any], files: list[tuple[str, int]]
) -> None:
    """Point an existing manifest.json at the new shards; keep everything else."""
    target = f"{directory}/{MANIFEST_NAME}"
    raw = _read_text(spark, target)
    if raw is None:
        return

    reading = Output(path=directory, format=ready["format"], options=ready["read_options"])
    counts = _row_counts(spark, reading, directory, [uri for uri, _ in files])

    document = json.loads(raw)
    document["shards"] = [
        {
            "index": index,
            "uri": uri,
            "size_bytes": size,
            "row_count": counts.get(_key(uri), 0),
        }
        for index, (uri, size) in enumerate(files)
    ]
    document["row_count"] = sum(shard["row_count"] for shard in document["shards"])
    document["compacted_at"] = datetime.now(timezone.utc).isoformat(timespec="seconds")
    _write_text(spark, target, json.dumps(document, indent=2) + "\n")


def recover(spark: SparkSession, directory: str) -> dict | None:
    """Finish a committed compaction a failure interrupted, or discard an unfinished one."""
    staging = f"{directory}/{STAGING}"
    raw = _read_text(spark, f"{staging}/{READY}")
    if raw is None:
        log.warning("discarding an unfinished compaction in %s", staging)
        _delete(spark, staging)
        return None
    log.warning("finishing an interrupted compaction in %s", directory)
    return commit(spark, directory, json.loads(raw))


def compact(
    spark: SparkSession,
    root: str,
    fmt: str,
    target_bytes: int,
    options: dict[str, str] | None = None,
    dry_run: bool = False,
) -> dict[str, Any]:
    """Compact every directory under ``root``. Returns a summary."""
    options = dict(options or {})
    if fmt not in FORMATS:
        raise ConfigError(f"format must be one of {', '.join(FORMATS)}, got {fmt!r}")
    if target_bytes <= 0:
        raise ConfigError("the target size must be positive")
    if fmt == "csv" and "header" not in options:
        # Guessing wrong is silent: header lines read as data would be written
        # back into the middle of the merged files, and the row check passes.
        raise ConfigError("csv needs an explicit --option header=true or header=false")
    if _inside_delta_table(spark, root):
        raise ConfigError(f"{root} is a Delta table - compact it with OPTIMIZE instead")

    groups, staged = discover(spark, root, fmt)
    summary: dict[str, Any] = {"compacted": [], "recovered": [], "planned": [], "skipped": 0}

    if staged:
        for directory in sorted(staged):
            if dry_run:
                log.info("would recover the staging left in %s", directory)
                continue
            result = recover(spark, directory)
            if result:
                summary["recovered"].append(result)
        if not dry_run:
            groups, _ = discover(spark, root, fmt)

    if not groups and not summary["recovered"]:
        raise ConfigError(f"no {fmt} files under {root}")

    for directory, files in sorted(groups.items()):
        plan = Plan(directory, files, target_bytes)
        if not plan.needed:
            summary["skipped"] += 1
            log.info(
                "%s: %d file(s), %.1f MB - already sized",
                directory,
                len(files),
                plan.total_bytes / 2**20,
            )
            continue
        log.info(
            "%s: %d files, %.1f MB -> %d file(s)",
            directory,
            len(files),
            plan.total_bytes / 2**20,
            plan.target_files,
        )
        if dry_run:
            summary["planned"].append(
                {
                    "directory": directory,
                    "files_before": len(files),
                    "files_after": plan.target_files,
                }
            )
            continue
        summary["compacted"].append(compact_directory(spark, plan, fmt, options))

    log.info(
        "compaction done: %d compacted, %d recovered, %d already sized%s",
        len(summary["compacted"]),
        len(summary["recovered"]),
        summary["skipped"],
        f", {len(summary['planned'])} planned (dry run)" if dry_run else "",
    )
    return summary


# --------------------------------------------------------------------------
# CLI
# --------------------------------------------------------------------------


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="compact", description="Merge small output files into files of about --target-mb."
    )
    parser.add_argument("--env", choices=config.ENVIRONMENTS, help="Defaults to $APP_ENV")
    parser.add_argument(
        "--run-date", metavar="YYYY-MM-DD", help="Fills ${run_date} in --path, if it has one"
    )
    parser.add_argument(
        "--path", required=True, help="A partition directory or a whole table; ${TOKEN}s resolve"
    )
    parser.add_argument("--format", required=True, choices=FORMATS)
    parser.add_argument("--target-mb", type=float, default=DEFAULT_TARGET_MB)
    parser.add_argument(
        "--option",
        action="append",
        metavar="KEY=VALUE",
        help="Reader and writer option, e.g. header=true or sep=|. Repeatable. "
        "compression defaults to whatever the existing files use.",
    )
    parser.add_argument("--dry-run", action="store_true", help="Plan only, change nothing")
    parser.add_argument("--log-level", default="INFO")
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=args.log_level.upper(),
        format="%(asctime)s %(levelname)-7s %(message)s",
        datefmt="%H:%M:%S",
    )

    spark = None
    try:
        settings = config.load(args.env)
        params = {**settings.values}
        if args.run_date:
            params["run_date"] = args.run_date
        root = settings.resolve(args.path, **params)
        options = parse_options(args.option)
        target_bytes = int(args.target_mb * 2**20)

        with _stage("session", EXIT_SESSION):
            # build_spark wants a Job only for its name and to know whether the
            # session needs Hive or Delta; this one needs neither.
            spark = build_spark(
                Job(name="compact", sql="", inputs={"files": root}, outputs=[Output(path=root)]),
                settings,
            )
        with _stage(f"compact {root}", EXIT_WRITE):
            compact(spark, root, args.format, target_bytes, options, args.dry_run)
    except ConfigError as exc:
        log.error("%s", exc)
        return EXIT_CONFIG
    except StageFailure as exc:
        log.exception("%s", exc)
        log.error("%s stage failed - exiting %d", exc.stage, exc.code)
        return exc.code
    except Exception:
        log.exception("unhandled failure")
        return EXIT_UNKNOWN
    finally:
        if spark is not None:
            spark.stop()
    return 0
