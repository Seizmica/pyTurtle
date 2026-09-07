"""Config-driven PySpark ETL framework."""

from importlib.metadata import PackageNotFoundError, version

try:
    # Set at build time by setuptools-scm from the git tag.
    __version__ = version("etl-framework")
except PackageNotFoundError:  # pragma: no cover - running from an uninstalled source tree
    __version__ = "0.0.0+unknown"
