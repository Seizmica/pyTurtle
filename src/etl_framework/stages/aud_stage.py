"""Optional ``.aud`` checksum manifest stage.

Emits an MD5 checksum for every file of the primary output so downstream
consumers can verify a delivery arrived intact. Like the TTL stage it is
decoupled from the primary write — it only inspects what was written — and its
failure handling is configurable (soft/hard fail).

Local targets are hashed with :mod:`hashlib`; anything with a remote scheme
(``s3a://``, ``hdfs://``, ...) is listed and hashed through the JVM Hadoop
FileSystem API, so no file contents cross into Python.
"""

from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING
from urllib.parse import urlparse

from ..config.schema import AudOutputSpec

if TYPE_CHECKING:  # pragma: no cover - typing only, keeps the module import light
    from pyspark.sql import SparkSession

_CHUNK = 1024 * 1024


def default_aud_path(output_path: str) -> str:
    """Sibling ``.aud`` manifest for an output path (``/out`` -> ``/out.aud``)."""
    return f"{output_path.rstrip('/')}.aud"


def is_local_path(path: str) -> bool:
    """Return ``True`` for filesystem paths, ``False`` for remote URIs.

    A single-character scheme is a Windows drive letter (``C:\\data``), not a
    URI scheme.
    """
    scheme = urlparse(path).scheme
    return scheme in ("", "file") or len(scheme) == 1


def _is_hidden(relative: str) -> bool:
    """Spark side-car files: ``_SUCCESS``, ``_delta_log/``, ``.part.crc``."""
    return any(part.startswith((".", "_")) for part in relative.replace("\\", "/").split("/"))


def _md5_local(path: Path) -> str:
    digest = hashlib.md5()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(_CHUNK), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _local_checksums(target: str, include_hidden: bool) -> list[tuple[str, str]]:
    root = Path(target)
    if not root.exists():
        raise FileNotFoundError(f"Output path does not exist: {target}")
    if root.is_file():
        return [(root.name, _md5_local(root))]

    entries = []
    for file in root.rglob("*"):
        if not file.is_file():
            continue
        relative = file.relative_to(root).as_posix()
        if not include_hidden and _is_hidden(relative):
            continue
        entries.append((relative, _md5_local(file)))
    return sorted(entries)


def _hadoop_fs(spark: SparkSession, path: str):  # noqa: ANN202 - JVM handles
    jvm = spark._jvm
    hconf = spark._jsc.hadoopConfiguration()
    jpath = jvm.org.apache.hadoop.fs.Path(path)
    return jvm, jpath.getFileSystem(hconf), jpath


def _hadoop_checksums(
    spark: SparkSession, target: str, include_hidden: bool
) -> list[tuple[str, str]]:
    jvm, fs, jpath = _hadoop_fs(spark, target)
    if not fs.exists(jpath):
        raise FileNotFoundError(f"Output path does not exist: {target}")

    root = str(fs.makeQualified(jpath)).rstrip("/")
    entries = []
    files = fs.listFiles(jpath, True)  # recursive
    while files.hasNext():
        file_path = files.next().getPath()
        full = str(file_path)
        relative = full[len(root) :].lstrip("/") if full.startswith(root) else full
        if not include_hidden and _is_hidden(relative):
            continue
        entries.append((relative, _md5_hadoop(jvm, fs, file_path)))
    return sorted(entries)


def _md5_hadoop(jvm, fs, file_path) -> str:  # noqa: ANN001 - JVM handles
    """Digest a remote file in the JVM (commons-codec ships with Spark)."""
    stream = fs.open(file_path)
    try:
        return jvm.org.apache.commons.codec.digest.DigestUtils.md5Hex(stream)
    finally:
        stream.close()


def render_manifest(entries: list[tuple[str, str]], target: str, fmt: str) -> str:
    """Render checksum entries as ``md5sum``-compatible text or JSON."""
    if fmt == "json":
        return json.dumps(
            {
                "target": target,
                "algorithm": "md5",
                "generated_at": datetime.now(timezone.utc).isoformat(),
                "file_count": len(entries),
                "files": [{"path": rel, "md5": checksum} for rel, checksum in entries],
            },
            indent=2,
        )
    # `<checksum>  <path>` — verifiable with `md5sum -c <file>.aud`.
    return "".join(f"{checksum}  {rel}\n" for rel, checksum in entries)


def _write_manifest(spark: SparkSession | None, path: str, content: str) -> None:
    if is_local_path(path):
        local = Path(urlparse(path).path if path.startswith("file:") else path)
        local.parent.mkdir(parents=True, exist_ok=True)
        local.write_text(content, encoding="utf-8")
        return

    _, fs, jpath = _hadoop_fs(spark, path)
    stream = fs.create(jpath, True)  # overwrite
    try:
        stream.write(bytearray(content.encode("utf-8")))
    finally:
        stream.close()


def run_aud_stage(
    spark: SparkSession | None,
    spec: AudOutputSpec,
    output_path: str,
) -> tuple[str, int]:
    """Write the ``.aud`` checksum manifest for ``output_path``.

    Every file under the written output (recursively) gets one MD5 entry;
    Spark side-car files (``_SUCCESS``, ``_delta_log/``, ``.crc``) are skipped
    unless ``spec.include_hidden`` is set.

    Returns the manifest path and the number of files it covers.
    """
    target = spec.path or default_aud_path(output_path)
    if is_local_path(output_path):
        entries = _local_checksums(output_path, spec.include_hidden)
    else:
        entries = _hadoop_checksums(spark, output_path, spec.include_hidden)

    _write_manifest(spark, target, render_manifest(entries, output_path, spec.format))
    return target, len(entries)
