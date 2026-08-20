"""Environment variable interpolation for config values.

Replaces ``${VAR}`` and ``${VAR:-default}`` tokens using ``os.environ``.
Secrets are never hardcoded; they are supplied through the environment.
"""

from __future__ import annotations

import os
import re
from typing import Any

_TOKEN = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)(?::-(.*?))?\}")


class MissingEnvVar(KeyError):
    """Raised when a referenced env var is absent and has no default."""


def _sub_string(value: str) -> str:
    def repl(match: re.Match[str]) -> str:
        name, default = match.group(1), match.group(2)
        if name in os.environ:
            return os.environ[name]
        if default is not None:
            return default
        raise MissingEnvVar(f"Environment variable '{name}' is not set")

    return _TOKEN.sub(repl, value)


def interpolate(obj: Any) -> Any:
    """Recursively interpolate ``${ENV_VAR}`` tokens in a config structure."""
    if isinstance(obj, str):
        return _sub_string(obj)
    if isinstance(obj, dict):
        return {k: interpolate(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [interpolate(v) for v in obj]
    return obj
