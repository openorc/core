"""Deterministic tests for the exact-ID Vault storage primitive (issue #141).

The ordinary suite cannot execute Postgres; these tests use canned results and
a scripted fake connection seam to prove the shared exact-ID Vault mechanics:
by-exact-UUID operations only, decryption only on explicit resolution,
decrypt-free existence, the in-place update that reports a dangling pointer,
and the targeted delete. The primitive knows no reference format and no
secret purpose — it takes the caller-composed safe description as given.
Real Vault behavior (encrypted storage, rotation preserving the secret UUID,
transaction rollback) is proven against a real Supabase branch by the
integration-marked suite, and the Vault privilege matrix stays with the #55
migration/integration coverage rather than being duplicated here.
"""

from __future__ import annotations

import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any, cast

from openorc.persistence.pool import DatabasePool
from openorc.persistence.vault_exact_id import (
    create_vault_secret,
    delete_vault_secret,
    read_vault_secret,
    update_vault_secret,
    vault_secret_exists,
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
        raise AssertionError("exact-ID Vault primitive tests never close pools")


def _pool(conn: ScriptedConnection) -> DatabasePool:
    return cast(DatabasePool, FakePool(conn))


_SECRET = "s3cr3t-credential-value"
_DESCRIPTION = "openorc connection credential (workspace <ws>, connection <c>)"


def test_create_stores_one_secret_with_a_null_name_and_safe_description() -> None:
    secret_id = uuid.uuid4()
    conn = ScriptedConnection([(secret_id,)])

    created = create_vault_secret(_pool(conn), secret=_SECRET, description=_DESCRIPTION)

    assert created == secret_id
    assert len(conn.executed) == 1
    sql, params = conn.executed[0]
    assert sql == "select vault.create_secret(%s, null, %s)"
    assert params is not None
    # The secret value is a bound parameter, never part of the SQL text; the
    # NULL name is the SQL literal (vault.secrets.name is UNIQUE when
    # non-NULL), and the caller-composed description passes through verbatim
    # without ever carrying the secret value.
    assert _SECRET not in sql
    assert len(params) == 2
    assert params[0] == _SECRET
    assert params[1] == _DESCRIPTION
    assert _SECRET not in params[1]


def test_read_secret_decrypts_only_on_explicit_resolution() -> None:
    secret_id = uuid.uuid4()
    conn = ScriptedConnection([(_SECRET,)])

    value = read_vault_secret(_pool(conn), secret_id=secret_id)

    assert value == _SECRET
    sql, params = conn.executed[0]
    assert sql == "select decrypted_secret from vault.decrypted_secrets where id = %s"
    assert params == (secret_id,)

    # An unknown UUID resolves to None, never a fabricated value.
    missing = ScriptedConnection([None])
    assert read_vault_secret(_pool(missing), secret_id=secret_id) is None


def test_existence_lookup_never_decrypts() -> None:
    secret_id = uuid.uuid4()
    conn = ScriptedConnection([(1,)])

    assert vault_secret_exists(_pool(conn), secret_id=secret_id) is True

    sql, params = conn.executed[0]
    assert sql == "select 1 from vault.secrets where id = %s"
    assert params == (secret_id,)
    assert "decrypted" not in sql

    assert vault_secret_exists(_pool(ScriptedConnection([None])), secret_id=secret_id) is False


def test_update_replaces_the_value_in_place_when_the_secret_exists() -> None:
    secret_id = uuid.uuid4()
    conn = ScriptedConnection([(1,), (None,)])

    updated = update_vault_secret(_pool(conn), secret_id=secret_id, secret="n3w-v4lu3")

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

    updated = update_vault_secret(_pool(conn), secret_id=secret_id, secret="n3w-v4lu3")

    assert updated is False
    # Only the existence check ran: vault.update_secret silently no-ops on an
    # unknown UUID, so the dangling pointer must be reported instead.
    assert len(conn.executed) == 1
    assert all("vault.update_secret" not in sql for sql, _ in conn.executed)


def test_delete_removes_one_secret_by_exact_uuid() -> None:
    secret_id = uuid.uuid4()
    removed = ScriptedConnection([(1,)])

    assert delete_vault_secret(_pool(removed), secret_id=secret_id) is True

    sql, params = removed.executed[0]
    assert sql == "delete from vault.secrets where id = %s returning 1"
    assert params == (secret_id,)

    # A second delete of the same exact UUID reports nothing removed.
    assert delete_vault_secret(_pool(ScriptedConnection([None])), secret_id=secret_id) is False
