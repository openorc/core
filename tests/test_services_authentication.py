"""Deterministic tests for the authentication application service (issue #52).

The service composes the Supabase access-token verifier with idempotent
Profile resolution. Ordinary tests use a fake verifier (adapter-local
normalized errors) and a scripted fake pool/connection seam mirroring the
existing ownership-test fakes, proving: verify-before-bootstrap ordering,
typed error translation in every direction (token rejection →
``AuthenticationError``, JWKS retrieval failure → the external-operation
vocabulary, deleted-auth-user FK violation → ``AuthenticationError``),
idempotent bootstrap, and safe error content.
"""

from __future__ import annotations

from contextlib import contextmanager
from datetime import UTC, datetime, timedelta, timezone
from typing import Any, cast
from uuid import uuid4

import pytest
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from opentelemetry.trace import StatusCode
from psycopg.errors import ForeignKeyViolation

from openorc.adapters.supabase import (
    SupabaseAccessTokenRejectedError,
    SupabaseAccessTokenVerifier,
    SupabaseJwksOutcomeUnknownError,
    SupabaseJwksUnavailableError,
)
from openorc.domain.identity import AuthenticatedPrincipal
from openorc.observability import OPERATION, injected_tracer_source
from openorc.persistence.pool import DatabasePool
from openorc.services.authentication import AuthenticatedUser, authenticate
from openorc.services.errors import (
    ApplicationError,
    AuthenticationError,
    ExternalOperationFailedError,
    ExternalOperationUncertainError,
)

TOKEN = "bearer-token-value-must-never-leak"
OBSERVED_AT = datetime(2026, 9, 16, 12, 0, 0, tzinfo=timezone(timedelta(hours=2)))
_UTC_NOW = datetime(2026, 9, 16, 10, 0, 0, tzinfo=UTC)


class FakeVerifier:
    """Deterministic verifier double returning a fixed principal."""

    def __init__(self, principal: AuthenticatedPrincipal) -> None:
        self._principal = principal
        self.verified_tokens: list[str] = []

    def verify(self, token: str) -> AuthenticatedPrincipal:
        self.verified_tokens.append(token)
        return self._principal


class RejectingVerifier(FakeVerifier):
    """Fake verifier raising the given normalized error after recording the token."""

    def __init__(self, error: Exception) -> None:
        super().__init__(AuthenticatedPrincipal(user_id=uuid4()))
        self._error = error

    def verify(self, token: str) -> AuthenticatedPrincipal:
        self.verified_tokens.append(token)
        raise self._error


class FakeCursor:
    """Returns one scripted row, like a psycopg cursor."""

    def __init__(self, row: tuple[Any, ...] | None) -> None:
        self._row = row

    def fetchone(self) -> tuple[Any, ...] | None:
        return self._row


class ScriptedConnection:
    """Records executed SQL and plays back scripted results in order."""

    def __init__(self, results: list[tuple[Any, ...] | None | Exception]) -> None:
        self.results = list(results)
        self.executed: list[tuple[str, tuple[Any, ...] | None]] = []

    def execute(self, sql: str, params: tuple[Any, ...] | None = None) -> FakeCursor:
        self.executed.append((sql, params))
        result = self.results.pop(0)
        if isinstance(result, Exception):
            raise result
        return FakeCursor(result)


class FakePool:
    """Emulates psycopg_pool ConnectionPool.connection() semantics."""

    def __init__(self, conn: ScriptedConnection) -> None:
        self._conn = conn

    def connection(self) -> Any:
        @contextmanager
        def managed() -> Any:
            yield self._conn

        return managed()

    def close(self) -> None:
        raise AssertionError("authentication service tests never close pools")


def _principal() -> AuthenticatedPrincipal:
    return AuthenticatedPrincipal(user_id=uuid4())


def test_valid_token_resolves_principal_and_bootstraps_profile() -> None:
    principal = _principal()
    verifier = FakeVerifier(principal)
    conn = ScriptedConnection([(principal.user_id, OBSERVED_AT)])
    pool = cast(DatabasePool, FakePool(conn))

    result = authenticate(pool, cast(SupabaseAccessTokenVerifier, verifier), token=TOKEN)

    assert result == AuthenticatedUser(
        principal=principal,
        profile=type(result.profile)(id=principal.user_id, created_at=_UTC_NOW),
    )
    assert verifier.verified_tokens == [TOKEN]
    sql, params = conn.executed[0]
    assert "insert into openorc.profiles" in sql
    assert "on conflict (id) do nothing" in sql
    assert params == (principal.user_id,)


def test_rejected_token_raises_typed_authentication_error_without_database_work() -> None:
    verifier = RejectingVerifier(
        SupabaseAccessTokenRejectedError("authentication failed: the access token has expired")
    )
    conn = ScriptedConnection([])
    pool = cast(DatabasePool, FakePool(conn))

    with pytest.raises(AuthenticationError, match="not valid"):
        authenticate(pool, cast(SupabaseAccessTokenVerifier, verifier), token=TOKEN)

    assert verifier.verified_tokens == [TOKEN]  # verification ran
    assert conn.executed == []  # bootstrap never started


@pytest.mark.parametrize(
    ("jwks_error", "expected_type"),
    [
        (
            SupabaseJwksUnavailableError("the signing-key source rejected the lookup"),
            ExternalOperationFailedError,
        ),
        (
            SupabaseJwksOutcomeUnknownError("the signing-key source could not be reached"),
            ExternalOperationUncertainError,
        ),
    ],
)
def test_jwks_retrieval_failures_are_never_invalid_credentials(
    jwks_error: Exception, expected_type: type[Exception]
) -> None:
    verifier = RejectingVerifier(jwks_error)
    conn = ScriptedConnection([])
    pool = cast(DatabasePool, FakePool(conn))

    with pytest.raises(expected_type) as excinfo:
        authenticate(pool, cast(SupabaseAccessTokenVerifier, verifier), token=TOKEN)

    assert not isinstance(excinfo.value, AuthenticationError)
    assert isinstance(excinfo.value, ApplicationError)
    assert conn.executed == []  # verification failed before any DB work


def test_deleted_auth_user_fk_violation_is_translated_to_authentication_error() -> None:
    # A still-cryptographically-valid JWT whose Supabase Auth user was
    # permanently deleted: Profile bootstrap fails closed through the
    # sanctioned profiles.id -> auth.users FK and is translated to a typed
    # authentication failure, never a leaked database error.
    verifier = FakeVerifier(_principal())
    conn = ScriptedConnection([ForeignKeyViolation("profiles_id_auth_users_fk")])
    pool = cast(DatabasePool, FakePool(conn))

    with pytest.raises(AuthenticationError, match="no longer exists"):
        authenticate(pool, cast(SupabaseAccessTokenVerifier, verifier), token=TOKEN)


def test_repeated_authentication_of_the_same_user_is_idempotent() -> None:
    principal = _principal()
    verifier = FakeVerifier(principal)
    row = (principal.user_id, OBSERVED_AT)
    conn = ScriptedConnection([row, row])
    pool = cast(DatabasePool, FakePool(conn))

    first = authenticate(pool, cast(SupabaseAccessTokenVerifier, verifier), token=TOKEN)
    second = authenticate(pool, cast(SupabaseAccessTokenVerifier, verifier), token=TOKEN)

    assert first.profile == second.profile
    assert first.principal == second.principal
    assert len(conn.executed) == 2  # one ensure_profile transaction per call


@pytest.mark.parametrize(
    "failure_kind",
    ["rejected_token", "jwks_unavailable", "jwks_unknown", "deleted_account"],
)
def test_service_errors_never_contain_the_bearer_token(failure_kind: str) -> None:
    if failure_kind == "rejected_token":
        verifier: FakeVerifier = RejectingVerifier(
            SupabaseAccessTokenRejectedError("authentication failed: the access token has expired")
        )
        conn = ScriptedConnection([])
    elif failure_kind == "jwks_unavailable":
        verifier = RejectingVerifier(
            SupabaseJwksUnavailableError("the signing-key source rejected the lookup")
        )
        conn = ScriptedConnection([])
    elif failure_kind == "jwks_unknown":
        verifier = RejectingVerifier(
            SupabaseJwksOutcomeUnknownError("the signing-key source could not be reached")
        )
        conn = ScriptedConnection([])
    else:
        verifier = FakeVerifier(_principal())
        conn = ScriptedConnection([ForeignKeyViolation("profiles_id_auth_users_fk")])
    pool = cast(DatabasePool, FakePool(conn))

    with pytest.raises(ApplicationError) as excinfo:
        authenticate(pool, cast(SupabaseAccessTokenVerifier, verifier), token=TOKEN)

    assert TOKEN not in str(excinfo.value)
    assert TOKEN not in repr(excinfo.value)


def test_deleted_account_error_does_not_leak_the_user_uuid() -> None:
    # The failure message stays at the account level: the (safe) user UUID is
    # also not required in the message surface.
    verifier = FakeVerifier(_principal())
    conn = ScriptedConnection([ForeignKeyViolation("profiles_id_auth_users_fk")])
    pool = cast(DatabasePool, FakePool(conn))

    with pytest.raises(AuthenticationError) as excinfo:
        authenticate(pool, cast(SupabaseAccessTokenVerifier, verifier), token=TOKEN)

    assert "authentication failed" in str(excinfo.value)


def _local_provider_with_exporter() -> tuple[TracerProvider, InMemorySpanExporter]:
    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    return provider, exporter


def test_successful_authentication_opens_one_service_span_with_only_safe_attributes() -> None:
    provider, exporter = _local_provider_with_exporter()
    principal = _principal()
    verifier = FakeVerifier(principal)
    pool = cast(DatabasePool, FakePool(ScriptedConnection([(principal.user_id, OBSERVED_AT)])))

    with injected_tracer_source(lambda name: provider.get_tracer(name)):
        authenticate(pool, cast(SupabaseAccessTokenVerifier, verifier), token=TOKEN)

    (exported,) = exporter.get_finished_spans()
    assert exported.name == "authentication.authenticate"
    attributes = exported.attributes
    assert attributes is not None
    # The vocabulary carries the operation only: no identity material and no
    # supported path for the bearer token into telemetry attributes.
    assert set(attributes) == {OPERATION}
    assert attributes[OPERATION] == "authentication.authenticate"
    assert exported.status.status_code is StatusCode.UNSET
    assert all(TOKEN not in str(value) for value in attributes.values())


@pytest.mark.parametrize(
    ("verifier_error", "expected_error"),
    [
        (SupabaseAccessTokenRejectedError("expired token"), AuthenticationError),
        (SupabaseJwksUnavailableError("jwks lookup rejected"), ExternalOperationFailedError),
        (SupabaseJwksOutcomeUnknownError("jwks unreachable"), ExternalOperationUncertainError),
    ],
)
def test_failure_paths_keep_typed_errors_and_export_only_the_classification(
    verifier_error: Exception, expected_error: type[ApplicationError]
) -> None:
    provider, exporter = _local_provider_with_exporter()
    verifier = RejectingVerifier(verifier_error)
    pool = cast(DatabasePool, FakePool(ScriptedConnection([])))

    with (
        injected_tracer_source(lambda name: provider.get_tracer(name)),
        pytest.raises(expected_error),
    ):
        authenticate(pool, cast(SupabaseAccessTokenVerifier, verifier), token=TOKEN)

    (exported,) = exporter.get_finished_spans()
    assert exported.status.status_code is StatusCode.ERROR
    # Safe failure classification only: the exception type name — never the
    # exception text and never any bearer-token material.
    assert exported.status.description == expected_error.__name__
    attributes = exported.attributes or {}
    assert all(TOKEN not in str(value) for value in attributes.values())


def test_deleted_account_path_keeps_the_typed_error_with_telemetry() -> None:
    provider, exporter = _local_provider_with_exporter()
    verifier = FakeVerifier(_principal())
    pool = cast(
        DatabasePool,
        FakePool(ScriptedConnection([ForeignKeyViolation("profiles_id_auth_users_fk")])),
    )

    with (
        injected_tracer_source(lambda name: provider.get_tracer(name)),
        pytest.raises(AuthenticationError),
    ):
        authenticate(pool, cast(SupabaseAccessTokenVerifier, verifier), token=TOKEN)

    (exported,) = exporter.get_finished_spans()
    assert exported.status.status_code is StatusCode.ERROR
    assert exported.status.description == "AuthenticationError"
