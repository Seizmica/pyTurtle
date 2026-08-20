"""Layered config loading and merging.

Precedence (later overrides earlier):
    base.yaml -> env/<env>.yaml -> jobs/<job>.yaml -> CLI param overrides
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import yaml

from .env import interpolate
from .schema import JobConfig, validate_config


def _read_file(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    text = path.read_text(encoding="utf-8")
    if path.suffix in (".yaml", ".yml"):
        return yaml.safe_load(text) or {}
    if path.suffix == ".json":
        return json.loads(text) or {}
    raise ValueError(f"Unsupported config format: {path.suffix}")


def deep_merge(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    """Recursively merge ``override`` onto ``base`` (override wins)."""
    result = dict(base)
    for key, value in override.items():
        if key in result and isinstance(result[key], dict) and isinstance(value, dict):
            result[key] = deep_merge(result[key], value)
        else:
            result[key] = value
    return result


def load_config(
    job_config: str | Path,
    env: str | None = None,
    config_root: str | Path = "configs",
    overrides: dict[str, Any] | None = None,
) -> JobConfig:
    """Load, merge, interpolate, and validate a job config.

    Args:
        job_config: Path to the per-job config file.
        env: Environment name selecting ``configs/env/<env>.yaml``.
        config_root: Root dir holding ``base.yaml`` and ``env/``.
        overrides: Highest-precedence dict merged last (e.g. CLI params).
    """
    config_root = Path(config_root)
    merged: dict[str, Any] = {}

    merged = deep_merge(merged, _read_file(config_root / "base.yaml"))
    if env:
        merged = deep_merge(merged, _read_file(config_root / "env" / f"{env}.yaml"))
    merged = deep_merge(merged, _read_file(Path(job_config)))
    if overrides:
        merged = deep_merge(merged, overrides)
    if env and "environment" not in (overrides or {}):
        merged["environment"] = env

    merged = interpolate(merged)
    return validate_config(merged)
