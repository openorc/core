"""Deterministic tests for the runtime-control secret persistence boundary (issue #55).

The ordinary suite cannot execute Postgres; these tests use canned results and
a scripted fake connection seam to prove the opaque v1 auth_reference codec's
fail-closed shape and the Vault SQL surface: by-exact-UUID operations only,
decryption only on explicit resolution, decrypt-free existence, the in-place
update that reports a dangling pointer, parameterization, and the safe
name/description metadata. Real Vault behavior (encrypted storage, rotation
preserving the secret UUID, transaction rollback) is proven against a real
Supabase branch by the integration-marked suite.
"""

from __future__ import annotations

import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any, cast

import pytest

from openorc.persistence.pool import DatabasePool
from openorc.persistence.runtime_control_secrets import (
    RuntimeControlSecretReferenceError,
    create_runtime_control_secret,
    delete_runtime_control_secret,
    encode_connection_auth_reference,
    parse_connection_auth_reference,
    read_runtime_control_secret,
    runtime_control_secret_exists,
    update_runtime_control_secret,
)


class FakeCursor:
    """Returns one canned row, like a psycopg cursor."""

    def __init__(self, row: tuple[Any, ...] | None) -> None:
        self._row = row

    def fetchone(self) -> tuple[Any, ...] | None:
        return self._row


class ScriptedConnection:
    """Plays back canned statement results in order, recording executed SQL."""

    def __init__(self, results: list[tuple[Any, ...] | None]) -> None:
        self.results = list(results)
        self.executed: list[tuple[str, tuple[Any, ...] | None]] = []

    def execute(self, sql: str, params: tuple[Any, ...] | None = None) -> FakeCursor:
        self.executed.append((sql, params))
        return FakeCursor(self.results.pop(0))


class FakePool:
    """Emulates psycopg_pool ConnectionPool.connection() semantics."""

    def __init__(self, conn: ScriptedConnection) -> None:
        self._conn = conn

    def connection(self) -> Any:
        @contextmanager
        def managed() -> Iterator[ScriptedConnection]:
            yield self._conn

        return managed()

    def close(self) -> None:
        raise AssertionError("runtime-control secret tests never close pools")


def _pool(conn: ScriptedConnection) -> DatabasePool:
    return cast(DatabasePool, FakePool(conn))


def test_reference_round_trip_is_canonical_v1() -> None:
    secret_id = uuid.uuid4()

    reference = encode_connection_auth_reference(secret_id)

    assert reference == f"openorc:connection-auth:v1:vault:{secret_id}"
    # The reference is the storage locator, never the secret value: parsing
    # resolves exactly the UUID the encoder rendered.
    assert parse_connection_auth_reference(reference) == secret_id


@pytest.mark.parametrize(
    "value",
    [
        "vault://openorc/connection-auth/abc",  # an opaque non-v1 string
        "openorc:connection-auth:v1:vault",  # missing payload
        "openorc:connection-auth:v1:vault:",  # empty payload
        "openorc:connection-auth:v1:vault:not-a-uuid",  # non-UUID payload
        "openorc:connection-auth:v1:vault:12345",  # numeric payload
        "openorc:connection-auth:v1:other:9f1ca49e-27aa-4c4f-a633-f083b9d4f0c9",  # unknown backend
        "openorc:connection-auth:v2:vault:9f1ca49e-27aa-4c4f-a633-f083b9d4f0c9",  # unknown version
        "openorc:connection-auth:v1:vault:9f1ca49e-27aa-4c4f-a633-f083b9d4f0c9:extra",
        "openorc:other:v1:vault:9f1ca49e-27aa-4c4f-a633-f083b9d4f0c9",  # wrong purpose
        "OpenOrc:connection-auth:v1:vault:9f1ca49e-27aa-4c4f-a633-f083b9d4f0c9",  # wrong scheme
    ],
)
def test_reference_rejects_unrecognized_formats_fail_closed(value: str) -> None:
    with pytest.raises(RuntimeControlSecretReferenceError):
        parse_connection_auth_reference(value)


def test_reference_rejects_non_canonical_uuid_payloads() -> None:
    canonical = "9f1ca49e-27aa-4c4f-a633-f083b9d4f0c9"

    for variant in (canonical.upper(), canonical.replace("-", "")):
        with pytest.raises(RuntimeControlSecretReferenceError):
            parse_connection_auth_reference(f"openorc:connection-auth:v1:vault:{variant}")


def test_reference_rejects_non_string_values() -> None:
    for value in (
        None,
        123,
        b"openorc:connection-auth:v1:vault:00000000-0000-0000-0000-000000000000",
    ):
        with pytest.raises(RuntimeControlSecretReferenceError):
            parse_connection_auth_reference(value)  # type: ignore[arg-type]


def test_create_secret_uses_the_vault_function_with_safe_metadata() -> None:
    secret = "s3cr3t-credential-value"
    workspace_id = uuid.uuid4()
    connection_id = uuid.uuid4()
    secret_id = uuid.uuid4()
    conn = ScriptedConnection([(secret_id,)])

    stored = create_runtime_control_secret(
        _pool(conn), secret=secret, connection_id=connection_id, workspace_id=workspace_id
    )

    assert stored == secret_id
    sql, params = conn.executed[0]
    assert sql == "select vault.create_secret(%s, null, %s)"
    assert params is not None
    # The credential value is a bound parameter, never part of the SQL text.
    assert secret not in sql
    assert params[0] == secret
    # NULL name (vault.secrets.name is UNIQUE when non-NULL) and safe
    # identifiers only in the description — never the credential value.
    assert params[1] is not None
    assert str(workspace_id) in params[1]
    assert str(connection_id) in params[1]
    assert secret not in params[1]


def test_read_secret_decrypts_only_on_explicit_resolution() -> None:
    secret_id = uuid.uuid4()
    conn = ScriptedConnection([("s3cr3t-credential-value",)])

    value = read_runtime_control_secret(_pool(conn), secret_id=secret_id)

    assert value == "s3cr3t-credential-value"
    sql, params = conn.executed[0]
    assert sql == "select decrypted_secret from vault.decrypted_secrets where id = %s"
    assert params == (secret_id,)

    # An unknown UUID resolves to None, never a fabricated value.
    missing = ScriptedConnection([None])
    assert read_runtime_control_secret(_pool(missing), secret_id=secret_id) is None


def test_existence_lookup_never_decrypts() -> None:
    secret_id = uuid.uuid4()
    conn = ScriptedConnection([(1,)])

    assert runtime_control_secret_exists(_pool(conn), secret_id=secret_id) is True

    sql, params = conn.executed[0]
    assert sql == "select 1 from vault.secrets where id = %s"
    assert params == (secret_id,)
    assert "decrypted" not in sql

    assert (
        runtime_control_secret_exists(_pool(ScriptedConnection([None])), secret_id=secret_id)
        is False
    )


def test_update_replaces_the_value_in_place_when_the_secret_exists() -> None:
    secret_id = uuid.uuid4()
    conn = ScriptedConnection([(1,), (None,)])

    updated = update_runtime_control_secret(_pool(conn), secret_id=secret_id, secret="n3w-v4lu3")

    assert updated is True
    assert len(conn.executed) == 2
    existence_sql, existence_params = conn.executed[0]
    assert existence_sql == "select 1 from vault.secrets where id = %s"
    assert existence_params == (secret_id,)
    update_sql, update_params = conn.executed[1]
    assert update_sql == "select vault.update_secret(%s, %s)"
    assert update_params == (secret_id, "n3w-v4lu3")
    assert "n3w-v4lu3" not in update_sql


def test_update_fails_closed_on_a_dangling_secret_without_touching_the_vault() -> None:
    secret_id = uuid.uuid4()
    conn = ScriptedConnection([None])

    updated = update_runtime_control_secret(_pool(conn), secret_id=secret_id, secret="n3w-v4lu3")

    assert updated is False
    # Only the existence check ran: vault.update_secret silently no-ops on an
    # unknown UUID, so the dangling pointer must be reported instead.
    assert len(conn.executed) == 1
    assert all("vault.update_secret" not in sql for sql, _ in conn.executed)


def test_delete_removes_one_secret_by_exact_uuid() -> None:
    secret_id = uuid.uuid4()
    removed = ScriptedConnection([(1,)])

    assert delete_runtime_control_secret(_pool(removed), secret_id=secret_id) is True

    sql, params = removed.executed[0]
    assert sql == "delete from vault.secrets where id = %s returning 1"
    assert params == (secret_id,)

    # A second delete of the same exact UUID reports nothing removed.
    assert (
        delete_runtime_control_secret(_pool(ScriptedConnection([None])), secret_id=secret_id)
        is False
    )
