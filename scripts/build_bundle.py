"""Assemble the deployable job bundle.

The wheel carries the engine, but a job also needs its ``configs/`` and
``sql/`` — ``main.py`` takes ``--config`` as a filesystem path and
``transformer.py`` reads ``sql_file`` from disk, so neither is importable from
the wheel. The bundle ships all three together under one version, so "prod is
on 1.2.3" describes the engine *and* the SQL that ran.

Layout::

    etl-framework-1.2.3/
      MANIFEST.json          version, git commit, build time
      requirements.lock      fully pinned runtime dependencies
      wheels/etl_framework-1.2.3-py3-none-any.whl
      configs/
      sql/
      bin/run.sh

Usage::

    python scripts/build_bundle.py --wheel dist/etl_framework-1.2.3-py3-none-any.whl
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import subprocess
import zipfile
from datetime import datetime, timezone
from pathlib import Path

from nexus_config import REPO_ROOT

# Directories copied into the bundle verbatim.
_PAYLOAD_DIRS = ("configs", "sql", "bin")
# {name}-{version}-{python}-{abi}-{platform}.whl — anchored so a non-wheel
# filename fails loudly instead of yielding a nonsense version to publish under.
_WHEEL_RE = re.compile(r"^(?P<name>[^-]+)-(?P<version>[^-]+)-[^-]+-[^-]+-[^-]+\.whl$")


def version_from_wheel(wheel: Path) -> str:
    match = _WHEEL_RE.match(wheel.name)
    if not match:
        raise SystemExit(f"Cannot parse a version out of wheel name: {wheel.name}")
    return match.group("version")


def _git(*args: str) -> str | None:
    """Best-effort git query; ``None`` outside a repo."""
    try:
        out = subprocess.run(
            ["git", *args], capture_output=True, text=True, timeout=10, check=False, cwd=REPO_ROOT
        )
        return out.stdout.strip() or None
    except (OSError, subprocess.SubprocessError):
        return None


def _iter_payload(root: Path) -> list[Path]:
    """Every file under the payload directories, sorted for a stable archive."""
    files: list[Path] = []
    for name in _PAYLOAD_DIRS:
        directory = root / name
        if not directory.is_dir():
            continue
        files.extend(
            p for p in directory.rglob("*") if p.is_file() and "__pycache__" not in p.parts
        )
    return sorted(files)


def build_manifest(version: str, wheel_name: str, payload: list[Path], root: Path) -> dict:
    return {
        "name": "etl-framework",
        "version": version,
        "git_commit": _git("rev-parse", "HEAD"),
        "git_ref": _git("rev-parse", "--abbrev-ref", "HEAD"),
        "built_at": datetime.now(timezone.utc).isoformat(),
        "wheel": f"wheels/{wheel_name}",
        "entry_point": "bin/run.sh",
        "contents": sorted(str(p.relative_to(root).as_posix()) for p in payload),
    }


def build_bundle(
    wheel: Path,
    output_dir: Path,
    lock_file: Path | None = None,
    root: Path = REPO_ROOT,
) -> Path:
    """Write ``<output_dir>/etl-framework-<version>.zip`` and its .sha256."""
    version = version_from_wheel(wheel)
    stem = f"etl-framework-{version}"
    archive = output_dir / f"{stem}.zip"
    output_dir.mkdir(parents=True, exist_ok=True)

    payload = _iter_payload(root)
    if not payload:
        raise SystemExit(f"No payload found under {_PAYLOAD_DIRS} in {root}")

    manifest = build_manifest(version, wheel.name, payload, root)

    with zipfile.ZipFile(archive, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr(f"{stem}/MANIFEST.json", json.dumps(manifest, indent=2) + "\n")
        zf.write(wheel, f"{stem}/wheels/{wheel.name}")
        for file in payload:
            zf.write(file, f"{stem}/{file.relative_to(root).as_posix()}")
        if lock_file and lock_file.is_file():
            zf.write(lock_file, f"{stem}/requirements.lock")
        else:
            print("WARNING: no requirements.lock - bundle is not dependency-pinned")

    digest = hashlib.sha256(archive.read_bytes()).hexdigest()
    (output_dir / f"{stem}.zip.sha256").write_text(f"{digest}  {archive.name}\n", encoding="utf-8")

    print(f"Built {archive} ({archive.stat().st_size:,} bytes)")
    print(f"  version:    {version}")
    print(f"  git commit: {manifest['git_commit']}")
    print(f"  payload:    {len(payload)} files + {wheel.name}")
    print(f"  sha256:     {digest}")
    return archive


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Build the deployable job bundle.")
    parser.add_argument("--wheel", required=True, type=Path, help="Path to the built wheel")
    parser.add_argument("--output-dir", type=Path, default=REPO_ROOT / "dist")
    parser.add_argument(
        "--lock-file",
        type=Path,
        default=REPO_ROOT / "requirements.lock",
        help="Pinned dependency file to embed (skipped when absent)",
    )
    args = parser.parse_args(argv)

    if not args.wheel.is_file():
        raise SystemExit(f"Wheel not found: {args.wheel}")
    build_bundle(args.wheel, args.output_dir, args.lock_file)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
