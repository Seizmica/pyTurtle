"""Tests for environment config loading. No Spark required."""

import pytest

from util import config
from util.config import ConfigError, Settings


def _write_env(tmp_path, name, body):
    (tmp_path / f".env.{name}").write_text(body, encoding="utf-8")
    return tmp_path


def test_loads_the_named_environment(tmp_path):
    _write_env(tmp_path, "uat", "RAW_ROOT=s3a://raw-uat\nSHARDS=8\n")

    settings = config.load("uat", root=tmp_path)

    assert settings.environment == "uat"
    assert settings.require("RAW_ROOT") == "s3a://raw-uat"
    assert settings.int("SHARDS", 1) == 8
    assert settings.get("APP_ENV") == "uat"


@pytest.mark.parametrize("environment", ["dev", "uat", "preprod", "prod"])
def test_every_environment_ships_a_file(environment):
    """The four real .env files must exist and parse."""
    settings = config.load(environment)
    assert settings.require("RAW_ROOT")
    assert settings.require("CURATED_ROOT")


def test_unknown_environment_is_rejected(tmp_path):
    with pytest.raises(ConfigError, match="Unknown environment"):
        config.load("staging", root=tmp_path)


def test_missing_env_file_names_the_path(tmp_path):
    with pytest.raises(ConfigError, match="Environment file not found"):
        config.load("prod", root=tmp_path)


def test_process_environment_overrides_declared_keys(tmp_path, monkeypatch):
    _write_env(tmp_path, "prod", "RAW_ROOT=s3a://placeholder\nDB_PASSWORD=notset\n")
    monkeypatch.setenv("DB_PASSWORD", "from-secret-manager")

    settings = config.load("prod", root=tmp_path)

    assert settings.require("DB_PASSWORD") == "from-secret-manager"
    assert settings.require("RAW_ROOT") == "s3a://placeholder"


def test_undeclared_shell_variables_do_not_leak_in(tmp_path, monkeypatch):
    """Only keys the file declares can be overridden - not everything exported."""
    _write_env(tmp_path, "dev", "RAW_ROOT=s3a://raw-dev\n")
    monkeypatch.setenv("PATH", "/should/not/appear")

    settings = config.load("dev", root=tmp_path)

    assert "PATH" not in settings.values


def test_comments_quotes_and_export_are_handled(tmp_path):
    _write_env(
        tmp_path,
        "dev",
        "\n".join(
            [
                "# a comment",
                "",
                "RAW_ROOT=s3a://raw-dev   # trailing comment",
                'CURATED_ROOT="s3a://curated-dev"',
                "export SPARK_MASTER='local[*]'",
                "SPARK_CONF.spark.sql.shuffle.partitions=8",
            ]
        ),
    )

    settings = config.load("dev", root=tmp_path)

    assert settings.require("RAW_ROOT") == "s3a://raw-dev"
    assert settings.require("CURATED_ROOT") == "s3a://curated-dev"
    assert settings.require("SPARK_MASTER") == "local[*]"
    assert settings.prefixed("SPARK_CONF.") == {"spark.sql.shuffle.partitions": "8"}


def test_malformed_line_names_the_line_number(tmp_path):
    _write_env(tmp_path, "dev", "GOOD=1\nthis is not a key=value pair\n")

    with pytest.raises(ConfigError, match=r"\.env\.dev:2"):
        config.load("dev", root=tmp_path)


def test_missing_required_key_names_the_environment():
    settings = Settings(environment="prod", values={})

    with pytest.raises(ConfigError, match=r"RAW_ROOT is not set.*'prod'"):
        settings.require("RAW_ROOT")


def test_resolve_expands_tokens_from_settings_and_run_params():
    settings = Settings(environment="dev", values={"RAW_ROOT": "s3a://raw-dev"})

    assert settings.resolve("${RAW_ROOT}/customers/") == "s3a://raw-dev/customers/"
    assert settings.resolve("WHERE d = '${run_date}'", run_date="2026-06-28") == (
        "WHERE d = '2026-06-28'"
    )
    assert settings.resolve("${MISSING:-fallback}") == "fallback"


def test_resolve_fails_loudly_on_an_unset_token():
    settings = Settings(environment="prod", values={})

    with pytest.raises(ConfigError, match="'CURATED_ROOT' is referenced but not set"):
        settings.resolve("${CURATED_ROOT}/out/")


def test_bool_and_int_coercion():
    settings = Settings(environment="dev", values={"ON": "yes", "OFF": "0", "N": "42"})

    assert settings.bool("ON") is True
    assert settings.bool("OFF") is False
    assert settings.bool("ABSENT", True) is True
    assert settings.int("N", 0) == 42

    with pytest.raises(ConfigError, match="must be an integer"):
        Settings(environment="dev", values={"N": "many"}).int("N", 0)
