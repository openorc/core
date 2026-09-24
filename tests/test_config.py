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
    OTLP_ENDPOINT_VAR,
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


def test_otlp_endpoint_defaults_to_unconfigured() -> None:
    settings = Settings.from_env({})

    assert settings.otlp_endpoint is None


def test_blank_otlp_endpoint_falls_back_to_unconfigured() -> None:
    settings = Settings.from_env({OTLP_ENDPOINT_VAR: ""})

    assert settings.otlp_endpoint is None


def test_otlp_endpoint_is_read_from_the_environment() -> None:
    settings = Settings.from_env({OTLP_ENDPOINT_VAR: "https://collector.example:4318"})

    assert settings.otlp_endpoint == "https://collector.example:4318"


@pytest.mark.parametrize("raw", ["ftp://collector.example", "https://", "not a url"])
def test_malformed_otlp_endpoint_values_are_rejected(raw: str) -> None:
    with pytest.raises(ConfigurationError, match="OPENORC_OTLP_ENDPOINT"):
        Settings.from_env({OTLP_ENDPOINT_VAR: raw})


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


def test_supabase_auth_settings_default_to_optional_url_and_authenticated_audience() -> None:
    settings = Settings.from_env({})

    assert settings.supabase_url is None
    assert settings.supabase_jwt_audience == "authenticated"


def test_supabase_url_and_audience_are_read_when_supplied() -> None:
    settings = Settings.from_env(
        {
            "SUPABASE_URL": "https://example.supabase.co",
            "OPENORC_SUPABASE_JWT_AUDIENCE": "custom-audience",
        }
    )

    assert settings.supabase_url == "https://example.supabase.co"
    assert settings.supabase_jwt_audience == "custom-audience"


def test_blank_supabase_values_fall_back_to_defaults() -> None:
    settings = Settings.from_env({"SUPABASE_URL": "", "OPENORC_SUPABASE_JWT_AUDIENCE": ""})

    assert settings.supabase_url is None
    assert settings.supabase_jwt_audience == "authenticated"


@pytest.mark.parametrize("raw", ["ftp://example.supabase.co", "not-a-url", "https://"])
def test_malformed_supabase_url_is_rejected(raw: str) -> None:
    with pytest.raises(ConfigurationError, match="SUPABASE_URL"):
        Settings.from_env({"SUPABASE_URL": raw})


def test_missing_supabase_url_fails_production_startup() -> None:
    with pytest.raises(ConfigurationError, match="SUPABASE_URL"):
        Settings.from_env({"OPENORC_ENV": "production"})


def test_missing_supabase_url_is_allowed_outside_production() -> None:
    for environment in ("development", "test", "staging"):
        settings = Settings.from_env({"OPENORC_ENV": environment})
        assert settings.supabase_url is None


def test_supplied_supabase_url_satisfies_the_production_requirement() -> None:
    settings = Settings.from_env(
        {
            "OPENORC_ENV": "production",
            "SUPABASE_URL": "https://prod.supabase.co",
        }
    )

    assert settings.supabase_url == "https://prod.supabase.co"


def test_supabase_secret_key_defaults_to_unset() -> None:
    settings = Settings.from_env({})

    assert settings.supabase_secret_key is None


def test_supabase_secret_key_is_read_when_supplied() -> None:
    settings = Settings.from_env({"SUPABASE_SECRET_KEY": "sb_secret_example"})

    assert settings.supabase_secret_key == "sb_secret_example"


def test_blank_supabase_secret_key_is_rejected() -> None:
    # Unlike ordinary optional settings, a supplied-but-blank secret fails
    # closed (without echoing the value): secrets are never silently unset.
    with pytest.raises(ConfigurationError, match="SUPABASE_SECRET_KEY") as error:
        Settings.from_env({"SUPABASE_SECRET_KEY": ""})

    assert "was supplied blank" in str(error.value)


def test_whitespace_only_supabase_secret_key_is_rejected() -> None:
    with pytest.raises(ConfigurationError, match="was supplied blank"):
        Settings.from_env({"SUPABASE_SECRET_KEY": "   "})


def test_supplied_secret_value_is_preserved_verbatim() -> None:
    settings = Settings.from_env({"SUPABASE_SECRET_KEY": "  sb_secret padded  "})

    # Credential bytes are meaningful: supplied values are never stripped.
    assert settings.supabase_secret_key == "  sb_secret padded  "


def test_the_secret_key_never_appears_in_settings_representation() -> None:
    raw_key = "sb_secret_repr-leak-probe"
    settings = Settings.from_env(
        {
            "SUPABASE_URL": "https://example.supabase.co",
            "SUPABASE_SECRET_KEY": raw_key,
        }
    )

    # The generated dataclass repr/str must never carry the raw credential.
    assert raw_key not in repr(settings)
    assert raw_key not in str(settings)
    # The representation stays useful for non-secret fields.
    assert "https://example.supabase.co" in repr(settings)


def test_supabase_secret_key_is_optional_in_production() -> None:
    # Authentication ownership follows direct consumption: the shared
    # Settings surface boots both API and worker processes, so the
    # administrative credential is never globally required — the component
    # constructing the Auth Admin client enforces its own requirement.
    settings = Settings.from_env(
        {
            "OPENORC_ENV": "production",
            "SUPABASE_URL": "https://prod.supabase.co",
        }
    )

    assert settings.supabase_secret_key is None


# --- GitHub App identity (issue #58) -----------------------------------------


def test_github_app_identity_defaults_to_unconfigured() -> None:
    settings = Settings.from_env({})

    assert settings.github_app_id is None
    assert settings.github_app_private_key is None


def test_github_app_identity_is_read_from_the_environment() -> None:
    settings = Settings.from_env(
        {
            "OPENORC_GITHUB_APP_ID": "12345",
            "OPENORC_GITHUB_APP_PRIVATE_KEY": (
                "-----BEGIN PRIVATE KEY-----\nabc\n-----END PRIVATE KEY-----\n"
            ),
        }
    )

    assert settings.github_app_id == 12345
    assert settings.github_app_private_key == (
        "-----BEGIN PRIVATE KEY-----\nabc\n-----END PRIVATE KEY-----\n"
    )


@pytest.mark.parametrize("raw", ["0", "-5", "1.5", "not-a-number"])
def test_invalid_github_app_id_values_are_rejected(raw: str) -> None:
    with pytest.raises(ConfigurationError):
        Settings.from_env({"OPENORC_GITHUB_APP_ID": raw})


def test_github_app_id_must_be_a_positive_integer() -> None:
    settings = Settings.from_env({"OPENORC_GITHUB_APP_ID": "1"})

    assert settings.github_app_id == 1


def test_blank_github_app_private_key_fails_closed_without_echo() -> None:
    with pytest.raises(ConfigurationError, match="was supplied blank"):
        Settings.from_env({"OPENORC_GITHUB_APP_PRIVATE_KEY": "   "})


def test_github_app_private_key_is_preserved_verbatim() -> None:
    pem = "  -----BEGIN PRIVATE KEY-----  \n"
    settings = Settings.from_env({"OPENORC_GITHUB_APP_PRIVATE_KEY": pem})

    # Key bytes are meaningful: supplied values are never stripped.
    assert settings.github_app_private_key == pem


def test_the_github_app_private_key_never_appears_in_settings_representation() -> None:
    pem = "-----BEGIN PRIVATE KEY-----repr-leak-probe-----END PRIVATE KEY-----"
    settings = Settings.from_env(
        {
            "OPENORC_GITHUB_APP_ID": "12345",
            "OPENORC_GITHUB_APP_PRIVATE_KEY": pem,
        }
    )

    # The generated dataclass repr/str must never carry the raw key material.
    assert pem not in repr(settings)
    assert pem not in str(settings)
    # The representation stays useful for non-secret fields.
    assert "12345" in repr(settings)


def test_github_app_identity_is_optional_in_production() -> None:
    # Authentication ownership follows direct consumption: the shared
    # Settings surface boots both API and worker processes, so the GitHub App
    # identity is never globally required — the component constructing the
    # GitHub App client enforces its own requirement.
    settings = Settings.from_env(
        {
            "OPENORC_ENV": "production",
            "SUPABASE_URL": "https://prod.supabase.co",
        }
    )

    assert settings.github_app_id is None
    assert settings.github_app_private_key is None
