"""Environment configuration loaded from a per-environment ``.env`` file.

One file per environment — ``.env.dev``, ``.env.uat``, ``.env.preprod``,
``.env.prod`` — each holding everything that environment needs. Nothing is
inherited between them, so the effective value of a key is always readable in
one place.

The real process environment wins over the file for any key the file declares,
which is how CI and a secret manager inject credentials without editing (or
committing) them.
"""

from __future__ import annotations

import os
import re
from dataclasses import dataclass
from pathlib import Path

ENVIRONMENTS = ("dev", "uat", "preprod", "prod")
REPO_ROOT = Path(__file__).resolve().parent.parent

# ${VAR} and ${VAR:-default}
_TOKEN = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)(?::-(.*?))?\}")
_LINE = re.compile(r"^\s*(?:export\s+)?([A-Za-z_][A-Za-z0-9_.]*)\s*=\s*(.*)$")


class ConfigError(RuntimeError):
    """Unknown environment, missing ``.env`` file, or an unset required key."""


def parse_env_file(path: Path) -> dict[str, str]:
    """Parse ``KEY=value`` lines. Supports ``#`` comments, quotes, ``export``."""
    if not path.is_file():
        raise ConfigError(f"Environment file not found: {path}")

    values: dict[str, str] = {}
    for lineno, raw in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        match = _LINE.match(line)
        if not match:
            raise ConfigError(f"{path.name}:{lineno}: expected KEY=value, got {raw!r}")
        key, value = match.group(1), match.group(2).strip()
        # Strip one layer of matching quotes; an unquoted value keeps any
        # trailing comment out by splitting on " #".
        if len(value) >= 2 and value[0] == value[-1] and value[0] in "\"'":
            value = value[1:-1]
        elif " #" in value:
            value = value.split(" #", 1)[0].strip()
        values[key] = value
    return values


@dataclass(frozen=True)
class Settings:
    """Resolved configuration for one environment."""

    environment: str
    values: dict[str, str]

    def get(self, key: str, default: str | None = None) -> str | None:
        return self.values.get(key, default)

    def require(self, key: str) -> str:
        """Fetch ``key`` or fail loudly — before any Spark work starts."""
        value = self.values.get(key)
        if value is None or value == "":
            raise ConfigError(
                f"{key} is not set for environment '{self.environment}'. "
                f"Add it to .env.{self.environment} or export it."
            )
        return value

    def int(self, key: str, default: int) -> int:
        raw = self.values.get(key)
        if raw is None or raw == "":
            return default
        try:
            return int(raw)
        except ValueError as exc:
            raise ConfigError(f"{key} must be an integer, got {raw!r}") from exc

    def bool(self, key: str, default: bool = False) -> bool:
        raw = self.values.get(key)
        if raw is None or raw == "":
            return default
        return raw.strip().lower() in ("1", "true", "yes", "on")

    def prefixed(self, prefix: str) -> dict[str, str]:
        """Keys under ``prefix``, with the prefix stripped.

        Used for ``SPARK_CONF.spark.sql.shuffle.partitions=200`` style entries,
        so Spark tuning stays in the ``.env`` file rather than in code.
        """
        return {
            key[len(prefix) :]: value
            for key, value in self.values.items()
            if key.startswith(prefix) and len(key) > len(prefix)
        }

    def resolve(self, text: str, **extra: object) -> str:
        """Expand ``${VAR}`` / ``${VAR:-default}`` in ``text``.

        Lookup order: ``extra`` (per-run values such as ``run_date``), then the
        environment file, then the process environment.
        """
        overrides = {k: str(v) for k, v in extra.items()}

        def replace(match: re.Match[str]) -> str:
            name, default = match.group(1), match.group(2)
            for source in (overrides, self.values, os.environ):
                if name in source:
                    return str(source[name])
            if default is not None:
                return default
            raise ConfigError(
                f"'{name}' is referenced but not set for environment "
                f"'{self.environment}' (in: {text!r})"
            )

        return _TOKEN.sub(replace, text)


def env_file(environment: str, root: Path | None = None) -> Path:
    """Locate ``.env.<environment>``.

    An explicit ``root`` wins, then ``ETL_CONFIG_ROOT``, then the repo tree.

    The env var exists for ``spark-submit``: in cluster mode the source tree is
    shipped as a zip on ``sys.path``, so ``REPO_ROOT`` resolves to the archive
    itself rather than a real directory, while ``--files .env.<env>`` delivers
    the file to the container working directory. Setting
    ``spark.yarn.appMasterEnv.ETL_CONFIG_ROOT=.`` points the lookup there.
    """
    if root is None:
        override = os.environ.get("ETL_CONFIG_ROOT")
        root = Path(override) if override else REPO_ROOT
    return root / f".env.{environment}"


def load(environment: str | None = None, root: Path | None = None) -> Settings:
    """Load settings for ``environment``, defaulting to ``$APP_ENV``."""
    environment = environment or os.environ.get("APP_ENV") or ""
    if environment not in ENVIRONMENTS:
        raise ConfigError(
            f"Unknown environment {environment!r}. "
            f"Pass --env or set APP_ENV to one of: {', '.join(ENVIRONMENTS)}"
        )

    values = parse_env_file(env_file(environment, root))
    # The process environment overrides declared keys only — never every
    # variable that happens to be exported in the shell.
    for key in list(values):
        if key in os.environ:
            values[key] = os.environ[key]
    values["APP_ENV"] = environment
    return Settings(environment=environment, values=values)
