"""Deterministic tests for the GitHub-user refresh-secret boundary (issue #142).

The ordinary suite cannot execute Postgres: canned results and a scripted
fake connection seam prove the opaque v1 reference codec (fail-closed parse,
no cross-parse with the Connection-purpose reference) and the purpose
operations over the shared exact-ID Vault primitive, including the safe
description composition. Real Vault behavior stays with the integration
suites rather than being duplicated here.
"""

from __future__ import annotations

import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any, cast

import pytest

from openorc.persistence.github_user_refresh_secrets import (
    GitHubUserRefreshSecretReferenceError,
    create_github_user_refresh_secret,
    encode_github_user_refresh_reference,
    parse_github_user_refresh_reference,
)
from openorc.persistence.pool import DatabasePool
from openorc.persistence.vault_exact_id import (
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
    def __init__(self, conn: ScriptedConnection) -> None:
        self._conn = conn

    def connection(self) -> Any:
        @contextmanager
        def managed() -> Iterator[ScriptedConnection]:
            yield self._conn

        return managed()

    def close(self) -> None:
        raise AssertionError("refresh-secret boundary tests never close pools")


def _pool(conn: ScriptedConnection) -> DatabasePool:
    return cast(DatabasePool, FakePool(conn))


_SECRET = "ghr_s3cr3t-refresh-value"
_SECRET_ID = uuid.uuid4()


def test_reference_codec_round_trips_one_canonical_secret_uuid() -> None:
    reference = encode_github_user_refresh_reference(_SECRET_ID)

    assert reference.startswith("openorc:github-user-refresh:v1:vault:")
    assert parse_github_user_refresh_reference(reference) == _SECRET_ID


@pytest.mark.parametrize(
    "value",
    [
        "",
        "not-a-reference",
        f"openorc:connection-auth:v1:vault:{_SECRET_ID}",  # Connection purpose
        f"openorc:github-user-refresh:v2:vault:{_SECRET_ID}",
        f"openorc:github-user-refresh:v1:vault:{uuid.uuid4()}/extra",
        f"openorc:github-user-refresh:v1:vault:{str(_SECRET_ID).upper()}",
        "openorc:github-user-refresh:v1:vault:not-a-uuid",
    ],
)
def test_reference_parse_rejects_everything_but_the_exact_v1_shape(value: str) -> None:
    with pytest.raises(GitHubUserRefreshSecretReferenceError):
        parse_github_user_refresh_reference(value)


def test_reference_parse_rejects_non_string_values() -> None:
    with pytest.raises(GitHubUserRefreshSecretReferenceError):
        parse_github_user_refresh_reference(None)  # type: ignore[arg-type]


def test_create_wraps_the_shared_primitive_with_the_safe_description() -> None:
    conn = ScriptedConnection([(_SECRET_ID,)])
    profile_id = uuid.uuid4()

    returned = create_github_user_refresh_secret(_pool(conn), secret=_SECRET, profile_id=profile_id)

    assert returned == _SECRET_ID
    ((sql, params),) = conn.executed
    assert "vault.create_secret" in sql
    assert params is not None
    assert params[0] == _SECRET  # the credential value itself
    # The name is a literal NULL in the statement (never collides on
    # re-authorization); the description is safe identifiers only.
    assert "null" in sql
    assert params[1] == f"openorc github user refresh credential (profile {profile_id})"
    assert _SECRET not in params[1]


def test_read_resolves_by_exact_uuid_and_delete_targets_one_secret() -> None:
    conn = ScriptedConnection([(_SECRET,)])  # decrypt-on-read
    resolved = read_vault_secret(_pool(conn), secret_id=_SECRET_ID)
    assert conn.executed[0][1] == (_SECRET_ID,)
    assert (
        "select decrypted_secret from vault.decrypted_secrets where id = %s"
        in (conn.executed[0][0])
    )
    assert resolved == _SECRET

    conn = ScriptedConnection([(1,)])
    assert delete_vault_secret(_pool(conn), secret_id=_SECRET_ID) is True

    conn = ScriptedConnection([(1,)])
    assert vault_secret_exists(_pool(conn), secret_id=_SECRET_ID) is True

    conn = ScriptedConnection([(1,), None])
    # The in-place update checks existence first (decrypt-free), then updates.
    assert update_vault_secret(_pool(conn), secret_id=_SECRET_ID, secret=_SECRET) is True
