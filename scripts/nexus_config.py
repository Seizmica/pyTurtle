"""Resolve Nexus settings from ``pyproject.toml``, overridable by environment.

The build file is the source of truth (``[tool.nexus]``); every field can be
overridden per-environment with ``NEXUS_*`` variables so CI never has to edit
a checked-in file. Credentials are environment-only and are never read from
``pyproject.toml``.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

try:  # Python 3.11+
    import tomllib
except ModuleNotFoundError:  # pragma: no cover - Python 3.10
    import tomli as tomllib  # type: ignore[no-redef]

REPO_ROOT = Path(__file__).resolve().parent.parent

# Field name -> environment variable that overrides it.
_ENV_OVERRIDES = {
    "url": "NEXUS_URL",
    "pypi_repository": "NEXUS_PYPI_REPOSITORY",
    "pypi_snapshot_repository": "NEXUS_PYPI_SNAPSHOT_REPOSITORY",
    "raw_repository": "NEXUS_RAW_REPOSITORY",
    "raw_path": "NEXUS_RAW_PATH",
}


@dataclass(frozen=True)
class NexusConfig:
    """Where artifacts are published."""

    url: str
    pypi_repository: str
    pypi_snapshot_repository: str
    raw_repository: str
    raw_path: str

    def pypi_url(self, version: str) -> str:
        """Upload URL for the wheel, routed by release vs snapshot version."""
        repo = self.pypi_snapshot_repository if is_snapshot(version) else self.pypi_repository
        return f"{self.url.rstrip('/')}/repository/{repo}/"

    def raw_url(self, version: str, filename: str) -> str:
        """Upload URL for one bundle file, namespaced by version."""
        return (
            f"{self.url.rstrip('/')}/repository/{self.raw_repository}/"
            f"{self.raw_path.strip('/')}/{version}/{filename}"
        )

    def index_url(self) -> str:
        """Index a consumer installs from (a group repo proxying both)."""
        return f"{self.url.rstrip('/')}/repository/{self.pypi_repository}/simple"


def is_snapshot(version: str) -> bool:
    """A PEP 440 developmental release goes to the snapshot repository."""
    return ".dev" in version


def load_pyproject(root: Path | None = None) -> dict[str, Any]:
    path = (root or REPO_ROOT) / "pyproject.toml"
    with path.open("rb") as fh:
        return tomllib.load(fh)


def load(root: Path | None = None) -> NexusConfig:
    """Read ``[tool.nexus]``, then apply ``NEXUS_*`` environment overrides."""
    table = load_pyproject(root).get("tool", {}).get("nexus", {})
    missing = [
        f for f in _ENV_OVERRIDES if f not in table and not os.environ.get(_ENV_OVERRIDES[f])
    ]
    if missing:
        raise SystemExit(
            "Missing Nexus settings: "
            + ", ".join(f"[tool.nexus].{f} (or ${_ENV_OVERRIDES[f]})" for f in missing)
        )
    resolved = {
        field: os.environ.get(env) or table.get(field) for field, env in _ENV_OVERRIDES.items()
    }
    return NexusConfig(**resolved)


def credentials() -> tuple[str, str]:
    """Nexus deploy credentials from the environment. Never from a file."""
    user = os.environ.get("NEXUS_USERNAME")
    password = os.environ.get("NEXUS_PASSWORD")
    if not user or not password:
        raise SystemExit("NEXUS_USERNAME and NEXUS_PASSWORD must be set to publish")
    return user, password
