"""Tests for the environment-boundary configuration."""

from __future__ import annotations

import pytest

from openorc.config import (
    API_HOST_VAR,
    API_PORT_VAR,
    API_RELOAD_VAR,
    DATABASE_URL_VAR,
    DB_POOL_MAX_VAR,
    DB_POOL_MIN_VAR,
    DB_POOL_TIMEOUT_VAR,
    DEFAULT_API_HOST,
    DEFAULT_API_PORT,
    DEFAULT_DATABASE_URL,
    DEFAULT_DB_POOL_MAX,
    DEFAULT_DB_POOL_MIN,
    DEFAULT_DB_POOL_TIMEOUT,
    DEFAULT_ENVIRONMENT,
    DEFAULT_VALKEY_URL,
    ENVIRONMENT_VAR,
    VALKEY_URL_VAR,
    ConfigurationError,
    Settings,
)


def test_defaults_when_environment_is_empty() -> None:
    settings = Settings.from_env({})

    assert settings.environment == "development"
    assert settings.api_host == "127.0.0.1"
    assert settings.api_port == 3000
    assert settings.api_reload is False
    assert settings.valkey_url == "redis://127.0.0.1:6379/0"


def test_environment_values_override_defaults() -> None:
    settings = Settings.from_env(
        {
            "OPENORC_ENV": "test",
            "OPENORC_API_HOST": "0.0.0.0",
            "OPENORC_API_PORT": "3100",
            "OPENORC_API_RELOAD": "true",
            "VALKEY_URL": "redis://localhost:6380/2",
        }
    )

    assert settings.environment == "test"
    assert settings.api_host == "0.0.0.0"
    assert settings.api_port == 3100
    assert settings.api_reload is True
    assert settings.valkey_url == "redis://localhost:6380/2"


def test_blank_values_fall_back_to_defaults() -> None:
    settings = Settings.from_env({"OPENORC_ENV": "", "OPENORC_API_PORT": "", "VALKEY_URL": ""})

    assert settings.environment == "development"
    assert settings.api_port == 3000
    assert settings.valkey_url == "redis://127.0.0.1:6379/0"


@pytest.mark.parametrize("raw", ["not-a-port", "0", "70000"])
def test_invalid_port_values_are_rejected(raw: str) -> None:
    with pytest.raises(ConfigurationError, match="OPENORC_API_PORT"):
        Settings.from_env({"OPENORC_API_PORT": raw})


@pytest.mark.parametrize("raw", ["maybe", "2"])
def test_invalid_reload_values_are_rejected(raw: str) -> None:
    with pytest.raises(ConfigurationError, match="OPENORC_API_RELOAD"):
        Settings.from_env({"OPENORC_API_RELOAD": raw})


def test_from_env_defaults_to_process_environment(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Hermetic by construction: ambient OPENORC_*/VALKEY_URL values must not
    # affect this test even though from_env() reads the real environment.
    for name in (
        ENVIRONMENT_VAR,
        API_HOST_VAR,
        API_PORT_VAR,
        API_RELOAD_VAR,
        VALKEY_URL_VAR,
        DATABASE_URL_VAR,
        DB_POOL_MIN_VAR,
        DB_POOL_MAX_VAR,
        DB_POOL_TIMEOUT_VAR,
    ):
        monkeypatch.delenv(name, raising=False)

    settings = Settings.from_env()

    assert settings.environment == DEFAULT_ENVIRONMENT
    assert settings.api_host == DEFAULT_API_HOST
    assert settings.api_port == DEFAULT_API_PORT
    assert settings.api_reload is False
    assert settings.valkey_url == DEFAULT_VALKEY_URL
    assert settings.database_url == DEFAULT_DATABASE_URL
    assert settings.db_pool_min == DEFAULT_DB_POOL_MIN
    assert settings.db_pool_max == DEFAULT_DB_POOL_MAX
    assert settings.db_pool_timeout == DEFAULT_DB_POOL_TIMEOUT


@pytest.mark.parametrize("raw", ["http://127.0.0.1:1/0", "not-a-url"])
def test_malformed_valkey_url_is_rejected(raw: str) -> None:
    with pytest.raises(ConfigurationError, match="VALKEY_URL"):
        Settings.from_env({"VALKEY_URL": raw})


def test_database_defaults_when_environment_is_empty() -> None:
    settings = Settings.from_env({})

    assert settings.database_url == DEFAULT_DATABASE_URL
    assert settings.db_pool_min == DEFAULT_DB_POOL_MIN
    assert settings.db_pool_max == DEFAULT_DB_POOL_MAX
    assert settings.db_pool_timeout == DEFAULT_DB_POOL_TIMEOUT


def test_database_values_override_defaults() -> None:
    url = "postgresql://postgres:pw@db.example.supabase.co:5432/postgres"
    settings = Settings.from_env(
        {
            "DATABASE_URL": url,
            "OPENORC_DB_POOL_MIN": "2",
            "OPENORC_DB_POOL_MAX": "8",
            "OPENORC_DB_POOL_TIMEOUT": "12.5",
        }
    )

    assert settings.database_url == url
    assert settings.db_pool_min == 2
    assert settings.db_pool_max == 8
    assert settings.db_pool_timeout == 12.5


def test_postgres_scheme_is_accepted() -> None:
    settings = Settings.from_env({"DATABASE_URL": "postgres://u:p@h.example:5432/db"})

    assert settings.database_url == "postgres://u:p@h.example:5432/db"


@pytest.mark.parametrize(
    "raw",
    ["mysql://u:p@h.example/db", "postgresql+psycopg://u:p@h.example/db", "not-a-url"],
)
def test_malformed_database_url_is_rejected(raw: str) -> None:
    with pytest.raises(ConfigurationError, match="DATABASE_URL"):
        Settings.from_env({"DATABASE_URL": raw})


@pytest.mark.parametrize("raw", ["0", "-1", "two"])
def test_pool_min_below_one_is_rejected(raw: str) -> None:
    with pytest.raises(ConfigurationError, match="OPENORC_DB_POOL_MIN"):
        Settings.from_env({"OPENORC_DB_POOL_MIN": raw})


@pytest.mark.parametrize("raw", ["0", "-1", "two"])
def test_pool_max_below_one_is_rejected(raw: str) -> None:
    with pytest.raises(ConfigurationError, match="OPENORC_DB_POOL_MAX"):
        Settings.from_env({"OPENORC_DB_POOL_MAX": raw})


def test_pool_max_below_pool_min_is_rejected() -> None:
    with pytest.raises(ConfigurationError, match="OPENORC_DB_POOL_MAX"):
        Settings.from_env({"OPENORC_DB_POOL_MIN": "4", "OPENORC_DB_POOL_MAX": "2"})


@pytest.mark.parametrize("raw", ["0", "-5", "later"])
def test_invalid_pool_timeout_is_rejected(raw: str) -> None:
    with pytest.raises(ConfigurationError, match="OPENORC_DB_POOL_TIMEOUT"):
        Settings.from_env({"OPENORC_DB_POOL_TIMEOUT": raw})
