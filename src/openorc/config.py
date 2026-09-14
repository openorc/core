"""Environment-boundary configuration for OpenOrc process surfaces.

Settings load from the process environment only. Beyond documented local
development defaults there are no hardcoded deployment-specific values, and
neither process surface depends on persistent local filesystem state.
"""

from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass

ENVIRONMENT_VAR = "OPENORC_ENV"
API_HOST_VAR = "OPENORC_API_HOST"
API_PORT_VAR = "OPENORC_API_PORT"
API_RELOAD_VAR = "OPENORC_API_RELOAD"
VALKEY_URL_VAR = "VALKEY_URL"

DEFAULT_ENVIRONMENT = "development"
DEFAULT_API_HOST = "127.0.0.1"
DEFAULT_API_PORT = 3000
DEFAULT_VALKEY_URL = "redis://127.0.0.1:6379/0"

_TRUTHY = frozenset({"1", "true", "yes", "on"})
_FALSY = frozenset({"0", "false", "no", "off"})


class ConfigurationError(Exception):
    """Raised when process configuration is missing or invalid."""


def _read(env: Mapping[str, str], name: str) -> str | None:
    """Read an environment value; blank strings count as unset."""
    value = env.get(name)
    if value is None or value == "":
        return None
    return value


@dataclass(frozen=True, slots=True)
class Settings:
    """Process configuration resolved from the environment boundary."""

    environment: str
    api_host: str
    api_port: int
    api_reload: bool
    valkey_url: str

    @classmethod
    def from_env(cls, env: Mapping[str, str] | None = None) -> Settings:
        """Build settings from a mapping, defaulting to the process environment.

        Blank values fall back to documented defaults. Invalid values raise
        :class:`ConfigurationError` so process surfaces fail fast at startup.
        """
        source: Mapping[str, str] = os.environ if env is None else env

        api_port = DEFAULT_API_PORT
        api_port_raw = _read(source, API_PORT_VAR)
        if api_port_raw is not None:
            try:
                api_port = int(api_port_raw)
            except ValueError as exc:
                raise ConfigurationError(
                    f"{API_PORT_VAR} must be an integer, got {api_port_raw!r}"
                ) from exc
            if not 1 <= api_port <= 65535:
                raise ConfigurationError(
                    f"{API_PORT_VAR} must be between 1 and 65535, got {api_port}"
                )

        api_reload = False
        api_reload_raw = _read(source, API_RELOAD_VAR)
        if api_reload_raw is not None:
            normalized = api_reload_raw.lower()
            if normalized in _TRUTHY:
                api_reload = True
            elif normalized not in _FALSY:
                raise ConfigurationError(
                    f"{API_RELOAD_VAR} must be a boolean-like value, got {api_reload_raw!r}"
                )

        return cls(
            environment=_read(source, ENVIRONMENT_VAR) or DEFAULT_ENVIRONMENT,
            api_host=_read(source, API_HOST_VAR) or DEFAULT_API_HOST,
            api_port=api_port,
            api_reload=api_reload,
            valkey_url=_read(source, VALKEY_URL_VAR) or DEFAULT_VALKEY_URL,
        )
