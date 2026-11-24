"""Environment-boundary configuration for OpenOrc process surfaces.

Settings load from the process environment only. Beyond documented local
development defaults there are no hardcoded deployment-specific values, and
neither process surface depends on persistent local filesystem state.
"""

from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass, field
from enum import StrEnum
from typing import Final
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
# The deployment's Supabase secret API key (issue #97): the non-JWT
# administrative credential for the supported server-side Supabase Auth Admin
# boundary (permanent account deletion). Deliberately OPTIONAL on this shared
# surface: both the API and worker process surfaces boot through
# Settings.from_env, and authentication ownership follows direct consumption —
# no process may receive this administrative credential merely because it is
# required elsewhere. The requirement is enforced where the Auth Admin client
# is constructed, in the component that owns account deletion. The key is
# process bootstrap material only: never browser-visible, never persisted in
# openorc.* tables or Vault, never logged or returned.
SUPABASE_SECRET_KEY_VAR = "SUPABASE_SECRET_KEY"
# The deployed OpenOrc GitHub App identity (issue #58): the App ID and the
# App private key are deployment/bootstrap secret material — never Workspace
# data, never stored in ordinary openorc.* tables, never stored in Supabase
# Vault, never returned through ordinary APIs, never logged or attached to
# telemetry. Deliberately OPTIONAL on this shared surface: both the API and
# worker process surfaces boot through Settings.from_env, and authentication
# ownership follows direct consumption — no process may receive the private
# key merely because it is required elsewhere. The requirement is enforced
# where the GitHub App client/authenticator is constructed, which fails fast
# when either value is absent.
GITHUB_APP_ID_VAR = "OPENORC_GITHUB_APP_ID"
GITHUB_APP_PRIVATE_KEY_VAR = "OPENORC_GITHUB_APP_PRIVATE_KEY"
# The GitHub App webhook secret (issue #61): deployment/bootstrap secret
# material used to verify the SHA-256 signature of inbound GitHub webhook
# deliveries before any payload-derived application effect. Same discipline
# as the App identity above: never Workspace data, never stored in ordinary
# openorc.* tables, never stored in Supabase Vault, never returned through
# ordinary APIs, never logged or attached to telemetry. Deliberately OPTIONAL
# on this shared surface: both the API and worker process surfaces boot
# through Settings.from_env, and authentication ownership follows direct
# consumption — the requirement is enforced where the webhook verification
# boundary is constructed, which fails fast when the value is absent.
GITHUB_WEBHOOK_SECRET_VAR = "OPENORC_GITHUB_WEBHOOK_SECRET"
# The GitHub App user-flow credentials (issue #141): the App's OAuth
# user-to-server client ID and client secret — the deployment/bootstrap
# credentials for the Profile-scoped GitHub App user authorization that will
# represent the accountable human Owner for engineering-record GitHub writes
# in the #142/#143 remediation leaves. Deliberately OPTIONAL on this shared
# surface: both the API and worker process surfaces boot through
# Settings.from_env, and authentication ownership follows direct consumption —
# the requirement is enforced by the later component that consumes these
# credentials, which fails fast when a required value is absent. The client
# secret is secret material with the same discipline as the App private key
# and webhook secret: never Workspace data, never stored in ordinary
# openorc.* tables, never stored in Supabase Vault, never returned through
# ordinary APIs, never logged or attached to telemetry. A supplied-but-blank
# value of either setting is a configuration error raised without echoing
# the value.
GITHUB_APP_USER_FLOW_CLIENT_ID_VAR = "OPENORC_GITHUB_APP_USER_FLOW_CLIENT_ID"
GITHUB_APP_USER_FLOW_CLIENT_SECRET_VAR = "OPENORC_GITHUB_APP_USER_FLOW_CLIENT_SECRET"
OTLP_ENDPOINT_VAR = "OPENORC_OTLP_ENDPOINT"
# Execution-mode selection (issue #86): runtime/deployment environment
# configuration, not a database setting or build variant. ``regular`` is the
# bounded API-local executor default and requires no queue backend;
# ``queue`` is the RQ/Valkey opt-in that requires an explicitly supplied
# VALKEY_URL.
EXECUTION_MODE_VAR = "OPENORC_EXECUTION_MODE"
EXECUTION_MODE_REGULAR = "regular"
EXECUTION_MODE_QUEUE = "queue"
# The queue-mode detached-turn observation registration transport (issue #346):
# the base URL of the serving API/control-plane process that hosts the one
# long-lived detached-turn observation host, plus the deployment-level shared
# secret a queue dispatch worker must present on the private registration
# handshake BEFORE it may admit a detached turn. Both are required (fail fast)
# only in queue mode; regular mode hosts its observer in-process and requires
# neither. The URL is deployment network configuration; the secret is
# deployment/bootstrap secret material with the same discipline as the webhook
# secret: never Workspace data, never stored in ordinary openorc.* tables,
# never stored in Supabase Vault, never returned through ordinary APIs, never
# logged or attached to telemetry. The handshake route is private/authenticated
# and nonpublic; it is never an application command or Owner authority.
QUEUE_OBSERVER_REGISTRATION_URL_VAR = "OPENORC_QUEUE_OBSERVER_REGISTRATION_URL"
QUEUE_OBSERVER_REGISTRATION_SECRET_VAR = "OPENORC_QUEUE_OBSERVER_REGISTRATION_SECRET"
# The single private handshake path both ends of the transport agree on: the
# serving router mounts it and the queue dispatch worker's registrar proxy
# addresses it under the configured base URL. One neutral constant so the two
# process surfaces never drift (and the worker never imports the API package).
QUEUE_OBSERVER_REGISTRATION_PATH: Final[str] = "/internal/observer/registrations"
# Bounded local-executor capacity (issue #86): fixed bounded parallelism
# (active workers) and a separate bounded pending-admission capacity, both
# per serving process. Both running and waiting work must stay bounded.
LOCAL_EXECUTOR_WORKERS_VAR = "OPENORC_LOCAL_EXECUTOR_WORKERS"
LOCAL_EXECUTOR_PENDING_VAR = "OPENORC_LOCAL_EXECUTOR_PENDING"
REVIEWER_NEUTRAL_DIRECTORY_VAR = "OPENORC_REVIEWER_NEUTRAL_DIRECTORY"
# Optional deployment-configured browser-CORS origin allow-list (issue #369).
# HTTP mechanism only — never application authorization policy. Unset or blank
# means CORS is disabled (the default in every environment, production
# included): there is no hardcoded Cloud/frontend origin and no permissive
# default. When supplied, it is an explicit, closed list of absolute http(s)
# origins (no wildcard, no path-bearing values); malformed entries fail
# configuration so misconfiguration never surfaces at runtime. A deployment
# whose edge proxy already owns CORS simply leaves this unset.
CORS_ALLOWED_ORIGINS_VAR = "OPENORC_CORS_ALLOWED_ORIGINS"

DEFAULT_ENVIRONMENT = "development"
PRODUCTION_ENVIRONMENT = "production"
DEFAULT_API_HOST = "127.0.0.1"
DEFAULT_API_PORT = 3000
# Documented Supabase local-stack endpoint (`supabase start`); the only
# non-environment Postgres default.
DEFAULT_DATABASE_URL = "postgresql://postgres:postgres@127.0.0.1:54322/postgres"
# Pool bounds are per process, not deployment-wide: sizing deployments must
# account for process count x pool size.
DEFAULT_DB_POOL_MIN = 1
DEFAULT_DB_POOL_MAX = 10
DEFAULT_DB_POOL_TIMEOUT = 30.0
# Expected authenticated audience of Supabase Auth access tokens. Supabase
# Auth mints user access tokens with audience/role "authenticated".
DEFAULT_SUPABASE_JWT_AUDIENCE = "authenticated"
# Bounded local-executor defaults (issue #86): 2 active workers and 8
# pending-admission slots per serving process. Both bounds are positive by
# construction (minimum 1); supplied values below the minimum are
# configuration errors, never silently clamped.
DEFAULT_LOCAL_EXECUTOR_WORKERS = 2
DEFAULT_LOCAL_EXECUTOR_PENDING = 8


class ExecutionMode(StrEnum):
    """Closed execution-mode vocabulary (issue #86).

    The mode determines only where/how an operation is delivered:
    ``regular`` is the bounded API-local executor (no queue backend, no RQ
    worker), ``queue`` is the RQ/Valkey opt-in. Both modes invoke identical
    application services; neither local memory nor RQ IDs are workflow
    truth.
    """

    REGULAR = "regular"
    QUEUE = "queue"


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

# Schemes accepted for the queue-mode observer registration transport base URL
# (issue #346). Plain http is the same-network reference form; https is the
# production form. Network isolation owns the nonpublic expectation of the
# private handshake route.
_QUEUE_OBSERVER_URL_SCHEMES = frozenset({"http", "https"})

# Schemes accepted for the Supabase project URL (the auth issuer/JWKS root).
# https is the production form; http is accepted for the documented local
# Supabase stack and non-production branches.
_SUPABASE_URL_SCHEMES = frozenset({"http", "https"})

# Schemes accepted for the OTLP collector base URL (issue #108). Telemetry
# export is deployment configuration; unconfigured telemetry stays a no-op.
_OTLP_URL_SCHEMES = frozenset({"http", "https"})

# Schemes accepted for each browser-CORS origin entry (issue #369): absolute
# http(s) origins only.
_CORS_ORIGIN_SCHEMES = frozenset({"http", "https"})


class ConfigurationError(Exception):
    """Raised when process configuration is missing or invalid."""


def _read(env: Mapping[str, str], name: str) -> str | None:
    """Read an environment value; blank strings count as unset."""
    value = env.get(name)
    if value is None or value == "":
        return None
    return value


def _read_secret(env: Mapping[str, str], name: str) -> str | None:
    """Read one secret setting, distinguishing absent from supplied-blank.

    Secrets are process bootstrap material under a strict
    never-log/never-return boundary. Unlike ordinary optional settings, a
    supplied-but-blank or whitespace-only value is a configuration error
    raised WITHOUT echoing the value — a silently-unset secret would only
    fail later, misleadingly, at its point of consumption. A supplied value
    is otherwise preserved verbatim: credential bytes are meaningful and are
    never stripped or normalized.
    """
    value = env.get(name)
    if value is None:
        return None
    if not value.strip():
        raise ConfigurationError(
            f"{name} was supplied blank: provide the secret value or leave the variable unset"
        )
    return value


def _read_identifier(env: Mapping[str, str], name: str) -> str | None:
    """Read one optional non-secret identifier setting; blank fails closed.

    Identity material (never excluded from repr/str) under the same
    supplied-blank discipline as secrets: a supplied-but-whitespace-only
    value is a configuration error raised WITHOUT echoing the value — a
    half-configured deployment is never a valid state, and misconfiguration
    must surface at startup rather than later at the point of consumption. A
    supplied non-empty value is preserved verbatim: identifier bytes are
    meaningful and are never stripped or normalized.
    """
    value = env.get(name)
    if value is None:
        return None
    if not value.strip():
        raise ConfigurationError(
            f"{name} was supplied blank: provide the value or leave the variable unset"
        )
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


def _read_optional_int(env: Mapping[str, str], name: str, *, minimum: int) -> int | None:
    """Read an optional integer setting enforcing a minimum; unset stays unset.

    Unlike the bounded optional settings above, absence is a valid outcome for
    integration identity that only its consuming component requires
    (issue #58). A supplied value must still be a well-formed integer meeting
    the minimum, so misconfiguration never surfaces later at runtime.
    """
    raw = _read(env, name)
    if raw is None:
        return None
    try:
        value = int(raw)
    except ValueError as exc:
        raise ConfigurationError(f"{name} must be an integer, got {raw!r}") from exc
    if value < minimum:
        raise ConfigurationError(f"{name} must be at least {minimum}, got {value}")
    return value


def _parse_cors_allowed_origins(raw: str | None) -> tuple[str, ...]:
    """Parse the optional browser-CORS origin allow-list (issue #369).

    Unset/blank means CORS is disabled. Supplied values are comma-separated
    absolute http(s) origins: trimmed, scheme/host-normalized to lowercase,
    deduplicated preserving first-seen order, and rejected (fail fast) when a
    wildcard, a path/query/fragment/userinfo component, or a malformed
    non-http(s) origin appears. The CORS middleware compares origins exactly,
    so the normalization here keeps configured and browser-sent origins
    comparable.
    """
    if raw is None:
        return ()
    origins: list[str] = []
    seen: set[str] = set()
    for chunk in raw.split(","):
        origin = chunk.strip().rstrip("/")
        if not origin:
            continue
        if origin == "*":
            raise ConfigurationError(
                f"{CORS_ALLOWED_ORIGINS_VAR} does not accept wildcard origins: "
                "list each allowed origin explicitly"
            )
        parsed = urlparse(origin)
        if (
            parsed.scheme.lower() not in _CORS_ORIGIN_SCHEMES
            or not parsed.netloc
            or parsed.path
            or parsed.query
            or parsed.fragment
            or "@" in parsed.netloc
        ):
            raise ConfigurationError(
                f"{CORS_ALLOWED_ORIGINS_VAR} entries must be absolute http(s) "
                f"origins without paths, got {origin!r}"
            )
        normalized = f"{parsed.scheme.lower()}://{parsed.netloc.lower()}"
        if normalized not in seen:
            seen.add(normalized)
            origins.append(normalized)
    return tuple(origins)


@dataclass(frozen=True, slots=True)
class Settings:
    """Process configuration resolved from the environment boundary."""

    environment: str
    api_host: str
    api_port: int
    api_reload: bool
    # Execution-mode selection (issue #86): runtime/deployment environment
    # configuration with regular as the default. regular startup/submission/
    # shutdown never requires or touches a queue backend; queue requires the
    # explicitly supplied VALKEY_URL below.
    execution_mode: ExecutionMode = ExecutionMode.REGULAR
    # The Redis-compatible queue backend URL, required only in queue mode
    # (issue #86): no implicit localhost default is selected, so an unset or
    # blank value is None and queue-mode configuration fails without it.
    valkey_url: str | None = None
    # The queue-mode detached-turn observation registration transport (issue
    # #346): the serving API/control-plane base URL hosting the one observer
    # host and the deployment-level shared secret for its private
    # authenticated registration handshake. Required (fail fast) only in queue
    # mode; regular mode hosts its observer in-process and requires neither.
    # The secret is representation-safe: excluded from the dataclass repr/str,
    # opaque, never logged or returned.
    queue_observer_registration_url: str | None = None
    queue_observer_registration_secret: str | None = field(default=None, repr=False)
    database_url: str = DEFAULT_DATABASE_URL
    db_pool_min: int = DEFAULT_DB_POOL_MIN
    db_pool_max: int = DEFAULT_DB_POOL_MAX
    db_pool_timeout: float = DEFAULT_DB_POOL_TIMEOUT
    supabase_url: str | None = None
    supabase_jwt_audience: str = DEFAULT_SUPABASE_JWT_AUDIENCE
    # Representation-safe: the generated dataclass repr/str must never carry
    # the raw administrative credential (issue #97 secret-handling boundary).
    supabase_secret_key: str | None = field(default=None, repr=False)
    # GitHub App identity (issue #58). The App ID is not secret material but
    # is validated as a positive integer when supplied. The private key is
    # deployment/bootstrap secret material: representation-safe (excluded
    # from the dataclass repr/str), never persisted in openorc.* tables or
    # Vault, never logged or returned.
    github_app_id: int | None = None
    github_app_private_key: str | None = field(default=None, repr=False)
    github_webhook_secret: str | None = field(default=None, repr=False)
    # GitHub App user-flow credentials (issue #141). The client ID is
    # identity material (validated non-empty when supplied, repr-visible).
    # The client secret is deployment/bootstrap secret material:
    # representation-safe (excluded from the dataclass repr/str), never
    # persisted in openorc.* tables or Vault, never logged or returned.
    # Both are consumed only by the later user-to-server authorization
    # component, which fails fast at construction when a required value is
    # absent.
    github_app_user_flow_client_id: str | None = None
    github_app_user_flow_client_secret: str | None = field(default=None, repr=False)
    # Bounded local-executor capacity (issue #86), per serving process:
    # fixed bounded parallelism plus a separate bounded pending-admission
    # capacity so both running and waiting work stay bounded.
    local_executor_workers: int = DEFAULT_LOCAL_EXECUTOR_WORKERS
    local_executor_pending: int = DEFAULT_LOCAL_EXECUTOR_PENDING
    # The existing runtime-side neutral non-repository directory the Reviewer
    # session is constructed in (issue #253): supplied to the planning
    # bootstrap's fresh Reviewer attachment and reconstructed in by the
    # workflow's Reviewer attachments. Deployment configuration, never
    # Workspace state; required (fail-fast) when a Task-workflow operation
    # executes without it.
    reviewer_neutral_directory: str | None = None
    otlp_endpoint: str | None = None
    # Optional browser-CORS origin allow-list (issue #369): HTTP mechanism
    # only. Empty means CORS is disabled (the default); parsed and validated
    # in from_env, never at request time.
    cors_allowed_origins: tuple[str, ...] = ()

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

        # Execution-mode selection (issue #86): only the closed regular/queue
        # vocabulary is accepted; invalid values fail configuration
        # validation so misconfiguration surfaces at startup.
        execution_mode_raw = _read(source, EXECUTION_MODE_VAR)
        execution_mode = ExecutionMode.REGULAR
        if execution_mode_raw is not None:
            normalized = execution_mode_raw.strip().lower()
            if normalized == EXECUTION_MODE_REGULAR:
                execution_mode = ExecutionMode.REGULAR
            elif normalized == EXECUTION_MODE_QUEUE:
                execution_mode = ExecutionMode.QUEUE
            else:
                raise ConfigurationError(
                    f"{EXECUTION_MODE_VAR} must be one of "
                    f"{EXECUTION_MODE_REGULAR!r} or {EXECUTION_MODE_QUEUE!r}; "
                    f"got {execution_mode_raw!r}"
                )

        # The queue backend URL is queue-mode-only (issue #86): no implicit
        # localhost default is selected. Unset/blank stays None; regular mode
        # requires no queue backend. A supplied URL is scheme-validated so
        # misconfiguration surfaces at startup, not deep in client code.
        valkey_url = _read(source, VALKEY_URL_VAR)
        if valkey_url is not None:
            scheme = urlparse(valkey_url).scheme.lower()
            if scheme not in _VALKEY_URL_SCHEMES:
                raise ConfigurationError(
                    f"{VALKEY_URL_VAR} must use one of the schemes "
                    f"{', '.join(sorted(_VALKEY_URL_SCHEMES))}; got scheme {scheme!r}"
                )
        if execution_mode is ExecutionMode.QUEUE and valkey_url is None:
            raise ConfigurationError(
                f"{EXECUTION_MODE_QUEUE!r} execution mode requires an explicitly "
                f"supplied {VALKEY_URL_VAR}: no implicit localhost queue backend "
                "is selected"
            )

        # The queue-mode detached-turn observation registration transport
        # (issue #346): required (fail fast) only in queue mode, mirroring the
        # queue backend requirement. Regular mode hosts its observation host
        # in-process and requires neither value. A supplied URL is
        # scheme-validated so misconfiguration surfaces at startup; the secret
        # is opaque deployment material whose blank value fails closed without
        # echoing it (per _read_secret).
        queue_observer_registration_url = _read(source, QUEUE_OBSERVER_REGISTRATION_URL_VAR)
        if queue_observer_registration_url is not None:
            scheme = urlparse(queue_observer_registration_url).scheme.lower()
            if (
                scheme not in _QUEUE_OBSERVER_URL_SCHEMES
                or not urlparse(queue_observer_registration_url).netloc
            ):
                raise ConfigurationError(
                    f"{QUEUE_OBSERVER_REGISTRATION_URL_VAR} must be an http(s) "
                    f"base URL, got {queue_observer_registration_url!r}"
                )
        queue_observer_registration_secret = _read_secret(
            source, QUEUE_OBSERVER_REGISTRATION_SECRET_VAR
        )
        if execution_mode is ExecutionMode.QUEUE:
            if queue_observer_registration_url is None:
                raise ConfigurationError(
                    f"{EXECUTION_MODE_QUEUE!r} execution mode requires an explicitly "
                    f"supplied {QUEUE_OBSERVER_REGISTRATION_URL_VAR}: the queue "
                    "dispatch worker must attest a live detached-turn observation "
                    "registration before any admission"
                )
            if queue_observer_registration_secret is None:
                raise ConfigurationError(
                    f"{EXECUTION_MODE_QUEUE!r} execution mode requires an explicitly "
                    f"supplied {QUEUE_OBSERVER_REGISTRATION_SECRET_VAR}: the private "
                    "registration handshake is authenticated by the deployment-level "
                    "shared secret"
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

        # Supabase Auth Admin credential (issue #97). Optional on this shared
        # surface in every environment — including production: the
        # administrative credential belongs only to the component that
        # constructs the Auth Admin client, and that component fails fast at
        # construction when its required key is absent. A supplied-but-blank
        # or whitespace-only value is a configuration error (fail closed
        # without echoing the value): secrets are never silently unset. The
        # field is representation-safe (excluded from the dataclass
        # repr/str), never validated for shape here (it is opaque deployment
        # material), and never logged or returned.
        supabase_secret_key = _read_secret(source, SUPABASE_SECRET_KEY_VAR)

        # GitHub App identity (issue #58). Optional on this shared surface in
        # every environment — including production: the App identity belongs
        # only to the component that constructs the GitHub App client, and
        # that component fails fast at construction when either value is
        # absent. The App ID is validated as a positive integer when supplied;
        # a supplied-but-blank private key is a configuration error (fail
        # closed without echoing the value, per _read_secret).
        github_app_id = _read_optional_int(source, GITHUB_APP_ID_VAR, minimum=1)
        github_app_private_key = _read_secret(source, GITHUB_APP_PRIVATE_KEY_VAR)

        # GitHub App webhook secret (issue #61). Optional on this shared
        # surface in every environment — including production: the webhook
        # secret belongs only to the component that constructs the webhook
        # verification boundary, and that component fails fast at
        # construction when the value is absent. A supplied-but-blank value
        # is a configuration error (fail closed without echoing the value,
        # per _read_secret).
        github_webhook_secret = _read_secret(source, GITHUB_WEBHOOK_SECRET_VAR)

        # GitHub App user-flow credentials (issue #141). Optional on this
        # shared surface in every environment — including production: the
        # user-to-server authorization component that consumes them enforces
        # its own requirement at construction. The client ID is identity
        # material: a supplied-but-blank value is a configuration error (fail
        # closed without echoing it, per _read_identifier). The client secret
        # is secret material read through _read_secret: supplied-but-blank
        # fails closed without echoing, and the value is preserved verbatim.
        github_app_user_flow_client_id = _read_identifier(
            source, GITHUB_APP_USER_FLOW_CLIENT_ID_VAR
        )
        github_app_user_flow_client_secret = _read_secret(
            source, GITHUB_APP_USER_FLOW_CLIENT_SECRET_VAR
        )

        # Bounded local-executor capacity (issue #86): positive bounds only;
        # malformed or sub-minimum supplied values fail in every environment
        # so misconfiguration never surfaces at runtime.
        local_executor_workers = _read_int(
            source,
            LOCAL_EXECUTOR_WORKERS_VAR,
            minimum=1,
            default=DEFAULT_LOCAL_EXECUTOR_WORKERS,
        )
        local_executor_pending = _read_int(
            source,
            LOCAL_EXECUTOR_PENDING_VAR,
            minimum=1,
            default=DEFAULT_LOCAL_EXECUTOR_PENDING,
        )

        # The reviewer-neutral directory (issue #253). Optional deployment
        # configuration: the Task-workflow provider that consumes it fails
        # fast at the point of use when it is absent. A supplied-but-blank
        # value is a configuration error, never silently unset.
        reviewer_neutral_directory = _read(source, REVIEWER_NEUTRAL_DIRECTORY_VAR)
        if reviewer_neutral_directory is not None and not reviewer_neutral_directory.strip():
            raise ConfigurationError(
                f"{REVIEWER_NEUTRAL_DIRECTORY_VAR} must be a non-empty runtime-side "
                "directory path when supplied"
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

        # Optional browser-CORS origin allow-list (issue #369). Parsed and
        # validated here so misconfiguration fails startup like every other
        # configuration error; an unset/blank value keeps CORS disabled.
        cors_allowed_origins = _parse_cors_allowed_origins(_read(source, CORS_ALLOWED_ORIGINS_VAR))

        return cls(
            environment=environment,
            api_host=_read(source, API_HOST_VAR) or DEFAULT_API_HOST,
            api_port=api_port,
            api_reload=api_reload,
            execution_mode=execution_mode,
            valkey_url=valkey_url,
            queue_observer_registration_url=queue_observer_registration_url,
            queue_observer_registration_secret=queue_observer_registration_secret,
            database_url=database_url,
            db_pool_min=db_pool_min,
            db_pool_max=db_pool_max,
            db_pool_timeout=db_pool_timeout,
            supabase_url=supabase_url,
            supabase_jwt_audience=supabase_jwt_audience,
            supabase_secret_key=supabase_secret_key,
            github_app_id=github_app_id,
            github_app_private_key=github_app_private_key,
            github_webhook_secret=github_webhook_secret,
            github_app_user_flow_client_id=github_app_user_flow_client_id,
            github_app_user_flow_client_secret=github_app_user_flow_client_secret,
            local_executor_workers=local_executor_workers,
            local_executor_pending=local_executor_pending,
            reviewer_neutral_directory=reviewer_neutral_directory,
            otlp_endpoint=otlp_endpoint,
            cors_allowed_origins=cors_allowed_origins,
        )
