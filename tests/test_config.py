"""Tests for the environment-boundary configuration."""

from __future__ import annotations

import pytest

from openorc.config import ConfigurationError, Settings


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


def test_from_env_defaults_to_process_environment() -> None:
    settings = Settings.from_env()

    assert isinstance(settings.api_port, int)
    assert isinstance(settings.valkey_url, str)
