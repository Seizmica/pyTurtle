"""Optional ``.aud`` checksum manifest stage.

Emits an MD5 checksum for every file of the primary output so downstream
consumers can verify a delivery arrived intact. Like the TTL stage it is
decoupled from the primary write — it only inspects what was written — and its
failure handling is configurable (soft/hard fail).

Local targets are hashed with :mod:`hashlib`; anything with a remote scheme
(``s3a://``, ``hdfs://``, ...) is hashed through the JVM Hadoop FileSystem
API, so no file contents cross into Python.
"""

from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from typing import TYPE_CHECKING

from ..config.schema import AudOutputSpec
from ..utils import fs
from ..utils.fs import is_local_path  # noqa: F401 - re-exported for callers/tests

if TYPE_CHECKING:  # pragma: no cover - typing only, keeps the module import light
    from pyspark.sql import SparkSession

_CHUNK = 1024 * 1024


def default_aud_path(output_path: str) -> str:
    """Sibling ``.aud`` manifest for an output path (``/out`` -> ``/out.aud``)."""
    return f"{output_path.rstrip('/')}.aud"


def _md5_local(path: str) -> str:
    digest = hashlib.md5()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(_CHUNK), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _md5_hadoop(spark: SparkSession, uri: str) -> str:
    """Digest a remote file in the JVM (commons-codec ships with Spark)."""
    jvm, filesystem, jpath = fs.hadoop_fs(spark, uri)
    stream = filesystem.open(jpath)
    try:
        return str(jvm.org.apache.commons.codec.digest.DigestUtils.md5Hex(stream))
    finally:
        stream.close()


def _checksums(
    spark: SparkSession | None, target: str, include_hidden: bool
) -> list[tuple[str, str]]:
    """``(relative path, md5)`` for every file under ``target``, sorted."""
    local = fs.is_local_path(target)
    return [
        (entry.relative, _md5_local(entry.uri) if local else _md5_hadoop(spark, entry.uri))
        for entry in fs.list_files(spark, target, include_hidden)
    ]


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
    entries = _checksums(spark, output_path, spec.include_hidden)
    fs.write_text(spark, target, render_manifest(entries, output_path, spec.format))
    return target, len(entries)
