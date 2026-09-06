"""Filesystem helpers shared by the output stages.

Local targets are handled with :mod:`pathlib`; anything with a remote scheme
(``s3a://``, ``hdfs://``, ...) is listed and read through the JVM Hadoop
FileSystem API, so a stage works against the same paths Spark writes to
without needing a cloud SDK of its own.
"""

from __future__ import annotations

import posixpath
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING
from urllib.parse import urlparse

if TYPE_CHECKING:  # pragma: no cover - typing only, keeps the module import light
    from pyspark.sql import SparkSession


@dataclass(frozen=True)
class FileEntry:
    """One file under a listing root."""

    # POSIX path relative to the listing root, e.g. "run_date=2026-06-28/part-0.parquet".
    relative: str
    # Fully qualified location, e.g. "s3a://bucket/customer/run_date=.../part-0.parquet".
    uri: str
    size_bytes: int


def is_local_path(path: str) -> bool:
    """Return ``True`` for filesystem paths, ``False`` for remote URIs.

    A single-character scheme is a Windows drive letter (``C:\\data``), not a
    URI scheme.
    """
    scheme = urlparse(path).scheme
    return scheme in ("", "file") or len(scheme) == 1


def is_hidden(relative: str) -> bool:
    """Spark side-car files: ``_SUCCESS``, ``_delta_log/``, ``.part.crc``."""
    return any(part.startswith((".", "_")) for part in relative.replace("\\", "/").split("/"))


def local_path_of(path: str) -> Path:
    """Local :class:`Path` for ``path``, accepting a ``file:`` URI."""
    return Path(urlparse(path).path if path.startswith("file:") else path)


def hadoop_fs(spark: SparkSession, path: str):  # noqa: ANN201 - JVM handles
    """Return ``(jvm, FileSystem, Path)`` for ``path`` via the JVM Hadoop API."""
    jvm = spark._jvm
    hconf = spark._jsc.hadoopConfiguration()
    jpath = jvm.org.apache.hadoop.fs.Path(path)
    return jvm, jpath.getFileSystem(hconf), jpath


def join_uri(root: str, *parts: str) -> str:
    """Join URI/path segments with ``/``, tolerating a trailing slash on ``root``."""
    return posixpath.join(root.rstrip("/"), *[p.strip("/") for p in parts if p])


def normalize_uri(uri: str) -> str:
    """Scheme- and separator-insensitive form, for matching URIs across APIs.

    ``input_file_name()`` reports ``file:///C:/out/part-0.parquet`` where a
    local listing reports ``C:\\out\\part-0.parquet``; both normalize to the
    same value so shard sizes and row counts line up.
    """
    text = uri.replace("\\", "/")
    parsed = urlparse(text)
    # A single-character scheme is a Windows drive letter ("C:/out"), not a URI
    # scheme, so the whole string is the path.
    if len(parsed.scheme) <= 1:
        host, path = "", text
    else:
        host, path = parsed.netloc, parsed.path
    # A drive letter arrives from a file: URI as a leading "/C:/"; drop the slash.
    if len(path) > 2 and path[0] == "/" and path[2] == ":":
        path = path[1:]
    return f"{host}/{path.strip('/')}".strip("/").lower()


def exists(spark: SparkSession | None, path: str) -> bool:
    """Whether ``path`` exists, local or remote."""
    if is_local_path(path):
        return local_path_of(path).exists()
    _, fs, jpath = hadoop_fs(spark, path)
    return bool(fs.exists(jpath))


def list_files(
    spark: SparkSession | None,
    root: str,
    include_hidden: bool = False,
) -> list[FileEntry]:
    """Recursively list the files under ``root``, sorted by relative path.

    Raises :class:`FileNotFoundError` when ``root`` does not exist. Spark
    side-car files are skipped unless ``include_hidden`` is set.
    """
    entries = (
        _list_local(root, include_hidden)
        if is_local_path(root)
        else _list_hadoop(spark, root, include_hidden)
    )
    return sorted(entries, key=lambda e: e.relative)


def _list_local(root: str, include_hidden: bool) -> list[FileEntry]:
    base = Path(root)
    if not base.exists():
        raise FileNotFoundError(f"Path does not exist: {root}")
    if base.is_file():
        return [FileEntry(base.name, str(base), base.stat().st_size)]

    entries = []
    for file in base.rglob("*"):
        if not file.is_file():
            continue
        relative = file.relative_to(base).as_posix()
        if not include_hidden and is_hidden(relative):
            continue
        entries.append(FileEntry(relative, str(file), file.stat().st_size))
    return entries


def _list_hadoop(spark: SparkSession, root: str, include_hidden: bool) -> list[FileEntry]:
    _, fs, jpath = hadoop_fs(spark, root)
    if not fs.exists(jpath):
        raise FileNotFoundError(f"Path does not exist: {root}")

    qualified = str(fs.makeQualified(jpath)).rstrip("/")
    entries = []
    files = fs.listFiles(jpath, True)  # recursive
    while files.hasNext():
        status = files.next()
        full = str(status.getPath())
        relative = full[len(qualified) :].lstrip("/") if full.startswith(qualified) else full
        if not include_hidden and is_hidden(relative):
            continue
        entries.append(FileEntry(relative, full, int(status.getLen())))
    return entries


def list_dirs(spark: SparkSession | None, root: str) -> list[str]:
    """Immediate subdirectory names of ``root``; empty when it does not exist."""
    if is_local_path(root):
        base = Path(root)
        if not base.is_dir():
            return []
        return sorted(p.name for p in base.iterdir() if p.is_dir())

    _, fs, jpath = hadoop_fs(spark, root)
    if not fs.exists(jpath):
        return []
    return sorted(
        str(status.getPath().getName()) for status in fs.listStatus(jpath) if status.isDirectory()
    )


def read_text(spark: SparkSession | None, path: str) -> str | None:
    """Read ``path`` as UTF-8 text, or ``None`` when it does not exist."""
    if is_local_path(path):
        local = local_path_of(path)
        return local.read_text(encoding="utf-8") if local.is_file() else None

    jvm, fs, jpath = hadoop_fs(spark, path)
    if not fs.exists(jpath):
        return None
    stream = fs.open(jpath)
    try:
        # commons-io ships with Hadoop; decoding in the JVM keeps this to one call.
        return str(jvm.org.apache.commons.io.IOUtils.toString(stream, "UTF-8"))
    finally:
        stream.close()


def write_text(spark: SparkSession | None, path: str, content: str) -> None:
    """Write ``content`` to ``path`` as UTF-8, creating parents and overwriting."""
    if is_local_path(path):
        local = local_path_of(path)
        local.parent.mkdir(parents=True, exist_ok=True)
        local.write_text(content, encoding="utf-8")
        return

    _, fs, jpath = hadoop_fs(spark, path)
    stream = fs.create(jpath, True)  # overwrite
    try:
        stream.write(bytearray(content.encode("utf-8")))
    finally:
        stream.close()
