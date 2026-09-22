"""Environment-boundary configuration for OpenOrc process surfaces.

Settings load from the process environment only. Beyond documented local
development defaults there are no hardcoded deployment-specific values, and
neither process surface depends on persistent local filesystem state.
"""

from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass
from urllib.parse import urlparse

ENVIRONMENT_VAR = "OPENORC_ENV"
API_HOST_VAR = "OPENORC_API_HOST"
API_PORT_VAR = "OPENORC_API_PORT"
API_RELOAD_VAR = "OPENORC_API_RELOAD"
VALKEY_URL_VAR = "VALKEY_URL"
DATABASE_URL_VAR = "DATABASE_URL"
DB_POOL_MIN_VAR = "OPENORC_DB_POOL_MIN"
DB_POOL_MAX_VAR = "OPENORC_DB_POOL_MAX"
DB_POOL_TIMEOUT_VAR = "OPENORC_DB_POOL_TIMEOUT"
SUPABASE_URL_VAR = "SUPABASE_URL"
SUPABASE_JWT_AUDIENCE_VAR = "OPENORC_SUPABASE_JWT_AUDIENCE"
OTLP_ENDPOINT_VAR = "OPENORC_OTLP_ENDPOINT"

DEFAULT_ENVIRONMENT = "development"
PRODUCTION_ENVIRONMENT = "production"
DEFAULT_API_HOST = "127.0.0.1"
DEFAULT_API_PORT = 3000
DEFAULT_VALKEY_URL = "redis://127.0.0.1:6379/0"
# Documented Supabase local-stack endpoint (`supabase start`); the only
# non-environment Postgres default, mirroring DEFAULT_VALKEY_URL.
DEFAULT_DATABASE_URL = "postgresql://postgres:postgres@127.0.0.1:54322/postgres"
# Pool bounds are per process, not deployment-wide: sizing deployments must
# account for process count x pool size.
DEFAULT_DB_POOL_MIN = 1
DEFAULT_DB_POOL_MAX = 10
DEFAULT_DB_POOL_TIMEOUT = 30.0
# Expected authenticated audience of Supabase Auth access tokens. Supabase
# Auth mints user access tokens with audience/role "authenticated".
DEFAULT_SUPABASE_JWT_AUDIENCE = "authenticated"

_TRUTHY = frozenset({"1", "true", "yes", "on"})
_FALSY = frozenset({"0", "false", "no", "off"})

# Schemes accepted for the Redis-compatible queue backend, matching redis-py's
# URL parser. Validating here keeps malformed backend configuration inside the
# configuration error boundary instead of surfacing from deep client code.
_VALKEY_URL_SCHEMES = frozenset({"redis", "rediss", "unix"})

# Schemes accepted for the direct Postgres connection URL (standard libpq
# conninfo URLs). Driver-qualified schemes (e.g. SQLAlchemy-style suffixes)
# are not part of this configuration boundary.
_DATABASE_URL_SCHEMES = frozenset({"postgresql", "postgres"})

# Schemes accepted for the Supabase project URL (the auth issuer/JWKS root).
# https is the production form; http is accepted for the documented local
# Supabase stack and non-production branches.
_SUPABASE_URL_SCHEMES = frozenset({"http", "https"})

# Schemes accepted for the OTLP collector base URL (issue #108). Telemetry
# export is deployment configuration; unconfigured telemetry stays a no-op.
_OTLP_URL_SCHEMES = frozenset({"http", "https"})


class ConfigurationError(Exception):
    """Raised when process configuration is missing or invalid."""


def _read(env: Mapping[str, str], name: str) -> str | None:
    """Read an environment value; blank strings count as unset."""
    value = env.get(name)
    if value is None or value == "":
        return None
    return value


def _read_int(env: Mapping[str, str], name: str, *, minimum: int, default: int) -> int:
    """Read an integer setting enforcing a minimum; blank/unset uses default."""
    raw = _read(env, name)
    if raw is None:
        return default
    try:
        value = int(raw)
    except ValueError as exc:
        raise ConfigurationError(f"{name} must be an integer, got {raw!r}") from exc
    if value < minimum:
        raise ConfigurationError(f"{name} must be at least {minimum}, got {value}")
    return value


def _read_positive_float(env: Mapping[str, str], name: str, *, default: float) -> float:
    """Read a strictly positive numeric setting; blank/unset uses default."""
    raw = _read(env, name)
    if raw is None:
        return default
    try:
        value = float(raw)
    except ValueError as exc:
        raise ConfigurationError(f"{name} must be a number, got {raw!r}") from exc
    if value <= 0:
        raise ConfigurationError(f"{name} must be greater than 0, got {value}")
    return value


@dataclass(frozen=True, slots=True)
class Settings:
    """Process configuration resolved from the environment boundary."""

    environment: str
    api_host: str
    api_port: int
    api_reload: bool
    valkey_url: str
    database_url: str = DEFAULT_DATABASE_URL
    db_pool_min: int = DEFAULT_DB_POOL_MIN
    db_pool_max: int = DEFAULT_DB_POOL_MAX
    db_pool_timeout: float = DEFAULT_DB_POOL_TIMEOUT
    supabase_url: str | None = None
    supabase_jwt_audience: str = DEFAULT_SUPABASE_JWT_AUDIENCE
    otlp_endpoint: str | None = None

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

        valkey_url = _read(source, VALKEY_URL_VAR) or DEFAULT_VALKEY_URL
        scheme = urlparse(valkey_url).scheme.lower()
        if scheme not in _VALKEY_URL_SCHEMES:
            raise ConfigurationError(
                f"{VALKEY_URL_VAR} must use one of the schemes "
                f"{', '.join(sorted(_VALKEY_URL_SCHEMES))}; got scheme {scheme!r}"
            )

        database_url = _read(source, DATABASE_URL_VAR) or DEFAULT_DATABASE_URL
        db_scheme = urlparse(database_url).scheme.lower()
        if db_scheme not in _DATABASE_URL_SCHEMES:
            raise ConfigurationError(
                f"{DATABASE_URL_VAR} must use one of the schemes "
                f"{', '.join(sorted(_DATABASE_URL_SCHEMES))}; got scheme {db_scheme!r}"
            )

        # Pool bounds describe one process's pool, never a deployment-wide cap.
        db_pool_min = _read_int(source, DB_POOL_MIN_VAR, minimum=1, default=DEFAULT_DB_POOL_MIN)
        db_pool_max = _read_int(source, DB_POOL_MAX_VAR, minimum=1, default=DEFAULT_DB_POOL_MAX)
        if db_pool_max < db_pool_min:
            raise ConfigurationError(
                f"{DB_POOL_MAX_VAR} ({db_pool_max}) must be greater than or equal to "
                f"{DB_POOL_MIN_VAR} ({db_pool_min})"
            )
        db_pool_timeout = _read_positive_float(
            source, DB_POOL_TIMEOUT_VAR, default=DEFAULT_DB_POOL_TIMEOUT
        )

        environment = _read(source, ENVIRONMENT_VAR) or DEFAULT_ENVIRONMENT

        # Supabase Auth verification configuration. Malformed supplied values
        # fail in every environment; a missing required project URL fails
        # production startup outright (fail fast at startup). Non-production
        # contexts (local development, tests, tooling) may omit it while they
        # do not exercise authentication.
        supabase_url = _read(source, SUPABASE_URL_VAR)
        if supabase_url is not None:
            url_scheme = urlparse(supabase_url).scheme.lower()
            if url_scheme not in _SUPABASE_URL_SCHEMES or not urlparse(supabase_url).netloc:
                raise ConfigurationError(
                    f"{SUPABASE_URL_VAR} must be an http(s) Supabase project URL, "
                    f"got {supabase_url!r}"
                )
        elif environment == PRODUCTION_ENVIRONMENT:
            raise ConfigurationError(
                f"{SUPABASE_URL_VAR} is required when {ENVIRONMENT_VAR} is "
                f"{PRODUCTION_ENVIRONMENT}: authentication cannot verify project "
                "JWTs without the Supabase Auth issuer/project URL"
            )

        supabase_jwt_audience = (
            _read(source, SUPABASE_JWT_AUDIENCE_VAR) or DEFAULT_SUPABASE_JWT_AUDIENCE
        )

        # Application observability export boundary (issue #108). An unset
        # endpoint means unconfigured telemetry: the process runs without an
        # OpenTelemetry export runtime. Malformed supplied values fail in
        # every environment so misconfiguration never surfaces at runtime.
        otlp_endpoint = _read(source, OTLP_ENDPOINT_VAR)
        if otlp_endpoint is not None:
            parsed = urlparse(otlp_endpoint)
            if parsed.scheme.lower() not in _OTLP_URL_SCHEMES or not parsed.netloc:
                raise ConfigurationError(
                    f"{OTLP_ENDPOINT_VAR} must be an http(s) OTLP collector "
                    f"base URL, got {otlp_endpoint!r}"
                )

        return cls(
            environment=environment,
            api_host=_read(source, API_HOST_VAR) or DEFAULT_API_HOST,
            api_port=api_port,
            api_reload=api_reload,
            valkey_url=valkey_url,
            database_url=database_url,
            db_pool_min=db_pool_min,
            db_pool_max=db_pool_max,
            db_pool_timeout=db_pool_timeout,
            supabase_url=supabase_url,
            supabase_jwt_audience=supabase_jwt_audience,
            otlp_endpoint=otlp_endpoint,
        )
