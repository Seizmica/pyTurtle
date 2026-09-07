"""Unit tests for the Nexus publishing configuration and bundle assembly."""

import sys
import zipfile
from pathlib import Path

import pytest

# scripts/ holds CI tooling, not an installed package.
SCRIPTS = Path(__file__).resolve().parents[2] / "scripts"
sys.path.insert(0, str(SCRIPTS))

import build_bundle  # noqa: E402
import nexus_config  # noqa: E402

_ENV_VARS = [
    "NEXUS_URL",
    "NEXUS_PYPI_REPOSITORY",
    "NEXUS_PYPI_SNAPSHOT_REPOSITORY",
    "NEXUS_RAW_REPOSITORY",
    "NEXUS_RAW_PATH",
]


@pytest.fixture(autouse=True)
def _clean_env(monkeypatch):
    """A NEXUS_* var leaking in from the shell must not steer these tests."""
    for name in _ENV_VARS:
        monkeypatch.delenv(name, raising=False)


def test_config_loads_from_pyproject():
    cfg = nexus_config.load()
    assert cfg.url.startswith("http")
    assert cfg.pypi_repository and cfg.raw_repository and cfg.raw_path


def test_environment_overrides_the_build_file(monkeypatch):
    monkeypatch.setenv("NEXUS_URL", "https://nexus.internal")
    monkeypatch.setenv("NEXUS_RAW_REPOSITORY", "raw-prod")

    cfg = nexus_config.load()

    assert cfg.url == "https://nexus.internal"
    assert cfg.raw_repository == "raw-prod"
    # Unset fields still come from pyproject.toml.
    assert cfg.pypi_repository == "pypi-hosted"


@pytest.mark.parametrize(
    "version, snapshot",
    [
        ("1.2.3", False),
        ("0.1.0", False),
        ("1.2.4.dev5", True),
        ("0.0.1.dev2", True),
    ],
)
def test_snapshot_detection(version, snapshot):
    assert nexus_config.is_snapshot(version) is snapshot


def test_releases_and_snapshots_route_to_different_repositories(monkeypatch):
    monkeypatch.setenv("NEXUS_URL", "https://nexus.internal")
    cfg = nexus_config.load()

    assert cfg.pypi_url("1.2.3") == "https://nexus.internal/repository/pypi-hosted/"
    assert cfg.pypi_url("1.2.4.dev5") == "https://nexus.internal/repository/pypi-snapshots/"


def test_raw_url_is_namespaced_by_version(monkeypatch):
    monkeypatch.setenv("NEXUS_URL", "https://nexus.internal/")  # trailing slash tolerated
    cfg = nexus_config.load()

    assert cfg.raw_url("1.2.3", "etl-framework-1.2.3.zip") == (
        "https://nexus.internal/repository/raw-hosted/etl-framework/1.2.3/etl-framework-1.2.3.zip"
    )


@pytest.mark.parametrize(
    "wheel_name, expected",
    [
        ("etl_framework-1.2.3-py3-none-any.whl", "1.2.3"),
        ("etl_framework-0.0.1.dev2-py3-none-any.whl", "0.0.1.dev2"),
    ],
)
def test_version_parsed_from_wheel_name(wheel_name, expected):
    assert build_bundle.version_from_wheel(Path(wheel_name)) == expected


def test_unparseable_wheel_name_fails_loudly():
    with pytest.raises(SystemExit, match="Cannot parse a version"):
        build_bundle.version_from_wheel(Path("not-a-wheel.txt"))


def _fake_repo(tmp_path):
    """A source tree shaped like the real one, minus the engine."""
    (tmp_path / "configs" / "env").mkdir(parents=True)
    (tmp_path / "configs" / "base.yaml").write_text("logging:\n  level: INFO\n", encoding="utf-8")
    (tmp_path / "configs" / "env" / "prod.yaml").write_text("environment: prod\n", encoding="utf-8")
    (tmp_path / "sql").mkdir()
    (tmp_path / "sql" / "job.sql").write_text("SELECT 1", encoding="utf-8")
    (tmp_path / "bin").mkdir()
    (tmp_path / "bin" / "run.sh").write_text("#!/bin/sh\n", encoding="utf-8")
    wheel = tmp_path / "etl_framework-1.2.3-py3-none-any.whl"
    wheel.write_bytes(b"PK\x03\x04 not a real wheel")
    return wheel


def test_bundle_ships_configs_sql_and_wheel_together(tmp_path):
    """The engine wheel alone is not deployable: configs and SQL are read from disk."""
    wheel = _fake_repo(tmp_path)
    lock = tmp_path / "requirements.lock"
    lock.write_text("pyyaml==6.0.1\n", encoding="utf-8")

    archive = build_bundle.build_bundle(wheel, tmp_path / "dist", lock, root=tmp_path)

    assert archive.name == "etl-framework-1.2.3.zip"
    with zipfile.ZipFile(archive) as zf:
        names = zf.namelist()
    for expected in (
        "etl-framework-1.2.3/MANIFEST.json",
        "etl-framework-1.2.3/requirements.lock",
        "etl-framework-1.2.3/wheels/etl_framework-1.2.3-py3-none-any.whl",
        "etl-framework-1.2.3/configs/base.yaml",
        "etl-framework-1.2.3/configs/env/prod.yaml",
        "etl-framework-1.2.3/sql/job.sql",
        "etl-framework-1.2.3/bin/run.sh",
    ):
        assert expected in names


def test_bundle_writes_a_checksum(tmp_path):
    import hashlib

    wheel = _fake_repo(tmp_path)
    archive = build_bundle.build_bundle(wheel, tmp_path / "dist", None, root=tmp_path)

    checksum = (tmp_path / "dist" / "etl-framework-1.2.3.zip.sha256").read_text(encoding="utf-8")
    digest, name = checksum.split()
    assert name == archive.name
    assert digest == hashlib.sha256(archive.read_bytes()).hexdigest()


def test_bundle_manifest_records_the_version(tmp_path):
    import json

    wheel = _fake_repo(tmp_path)
    archive = build_bundle.build_bundle(wheel, tmp_path / "dist", None, root=tmp_path)

    with zipfile.ZipFile(archive) as zf:
        manifest = json.loads(zf.read("etl-framework-1.2.3/MANIFEST.json"))

    assert manifest["version"] == "1.2.3"
    assert manifest["wheel"] == "wheels/etl_framework-1.2.3-py3-none-any.whl"
    assert "sql/job.sql" in manifest["contents"]


def test_empty_payload_fails_rather_than_shipping_an_engine_only_bundle(tmp_path):
    wheel = tmp_path / "etl_framework-1.2.3-py3-none-any.whl"
    wheel.write_bytes(b"PK\x03\x04")

    with pytest.raises(SystemExit, match="No payload found"):
        build_bundle.build_bundle(wheel, tmp_path / "dist", None, root=tmp_path)
