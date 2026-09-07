"""Publish the wheel and the job bundle to Nexus.

Two destinations, one version:

* the wheel goes to the hosted PyPI repository (the snapshot repository when
  the version is a ``.dev`` build), so consumers can ``pip install``;
* the bundle zip, its ``.sha256``, and the sdist go to the raw repository under
  ``<raw_path>/<version>/``, so a deploy can fetch an exact, immutable release.

Nexus settings come from ``[tool.nexus]`` in ``pyproject.toml``, overridable
with ``NEXUS_*`` environment variables. Credentials are environment-only:
``NEXUS_USERNAME`` / ``NEXUS_PASSWORD``.

Usage::

    python scripts/publish_nexus.py --dist dist            # publish everything
    python scripts/publish_nexus.py --dist dist --dry-run  # show the plan
"""

from __future__ import annotations

import argparse
import base64
import os
import subprocess
import sys
import urllib.error
import urllib.request
from pathlib import Path

import nexus_config
from build_bundle import version_from_wheel

_TIMEOUT = 120


def _put(url: str, payload: bytes, user: str, password: str) -> None:
    """Upload one file to a Nexus raw repository."""
    token = base64.b64encode(f"{user}:{password}".encode()).decode("ascii")
    request = urllib.request.Request(url, data=payload, method="PUT")
    request.add_header("Authorization", f"Basic {token}")
    request.add_header("Content-Type", "application/octet-stream")
    try:
        with urllib.request.urlopen(request, timeout=_TIMEOUT) as response:
            if response.status >= 300:
                raise SystemExit(f"Upload failed ({response.status}): {url}")
    except urllib.error.HTTPError as exc:  # message only — never echo credentials
        raise SystemExit(f"Upload failed ({exc.code} {exc.reason}): {url}") from exc
    except urllib.error.URLError as exc:
        raise SystemExit(f"Upload failed ({exc.reason}): {url}") from exc


def publish_wheel(cfg: nexus_config.NexusConfig, dist: Path, version: str, dry_run: bool) -> None:
    """Upload the wheel and sdist with twine."""
    artifacts = sorted(dist.glob("*.whl")) + sorted(dist.glob("*.tar.gz"))
    if not artifacts:
        raise SystemExit(f"No wheel or sdist found in {dist}")

    target = cfg.pypi_url(version)
    kind = "snapshot" if nexus_config.is_snapshot(version) else "release"
    print(f"[pypi/{kind}] {target}")
    for path in artifacts:
        print(f"  {path.name}")
    if dry_run:
        return

    user, password = nexus_config.credentials()
    result = subprocess.run(
        [sys.executable, "-m", "twine", "upload", "--repository-url", target, *map(str, artifacts)],
        env={**_twine_env(user, password)},
        check=False,
    )
    if result.returncode != 0:
        raise SystemExit(f"twine upload failed with exit code {result.returncode}")


def _twine_env(user: str, password: str) -> dict[str, str]:
    return {**os.environ, "TWINE_USERNAME": user, "TWINE_PASSWORD": password}


def publish_bundle(cfg: nexus_config.NexusConfig, dist: Path, version: str, dry_run: bool) -> None:
    """Upload the bundle zip and its checksum to the raw repository."""
    files = [p for p in sorted(dist.glob(f"etl-framework-{version}.zip*")) if p.is_file()]
    if not files:
        raise SystemExit(f"No bundle found in {dist} for version {version}")

    print(f"[raw] {cfg.raw_url(version, '')}")
    for path in files:
        print(f"  {path.name}")
    if dry_run:
        return

    user, password = nexus_config.credentials()
    for path in files:
        _put(cfg.raw_url(version, path.name), path.read_bytes(), user, password)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Publish artifacts to Nexus.")
    parser.add_argument("--dist", type=Path, default=nexus_config.REPO_ROOT / "dist")
    parser.add_argument("--dry-run", action="store_true", help="Print the plan, upload nothing")
    parser.add_argument("--skip-wheel", action="store_true")
    parser.add_argument("--skip-bundle", action="store_true")
    args = parser.parse_args(argv)

    wheels = sorted(args.dist.glob("*.whl"))
    if not wheels:
        raise SystemExit(f"No wheel found in {args.dist} — run `python -m build` first")
    version = version_from_wheel(wheels[0])

    cfg = nexus_config.load()
    print(f"Publishing etl-framework {version} to {cfg.url}")
    if args.dry_run:
        print("(dry run - nothing will be uploaded)")

    if not args.skip_wheel:
        publish_wheel(cfg, args.dist, version, args.dry_run)
    if not args.skip_bundle:
        publish_bundle(cfg, args.dist, version, args.dry_run)

    if not args.dry_run:
        print(
            f"\nDone. Consumers install with:\n  pip install 'etl-framework=={version}' "
            f"--index-url {cfg.index_url()}"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
