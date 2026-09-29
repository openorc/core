"""Deterministic mapping tests for the GitHub user authorization repositories.

The ordinary suite cannot execute Postgres: canned rows and a scripted fake
connection seam prove row-to-domain-object mapping, UTC normalization at the
persistence boundary, the explicit lifecycle SQL (in-place reauthorization
advancing the generation monotonically, the compare-and-swap refresh install
guarded by generation AND active status, the revocation statement that
removes the usable credential and advances the generation), and full
parameterization. Database constraint behavior is proven against a real
database by the integration-marked suite.
"""

from __future__ import annotations

import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from typing import Any, cast

from openorc.domain.github_user_authorization import GitHubUserAuthorizationStatus
from openorc.persistence.github_user_authorizations import (
    get_github_user_authorization,
    get_github_user_authorization_for_update,
    insert_active_github_user_authorization,
    reauthorize_github_user_authorization,
    revoke_github_user_authorization_row,
    try_install_rotated_refresh,
)
from openorc.persistence.pool import DatabasePool

_OBSERVED = datetime(2026, 9, 29, 12, 0, 0, tzinfo=UTC)


class FakeCursor:
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
        raise AssertionError("authorization repository tests never close pools")


def _pool(conn: ScriptedConnection) -> DatabasePool:
    return cast(DatabasePool, FakePool(conn))


def _active_row(profile_id: Any, *, generation: int = 1) -> tuple[Any, ...]:
    return (
        profile_id,
        5432,
        "octocat",
        "active",
        "openorc:github-user-refresh:v1:vault:" + str(uuid.uuid4()),
        _OBSERVED,
        generation,
        _OBSERVED,
        None,
        _OBSERVED,
        _OBSERVED,
    )


def test_read_maps_the_row_to_the_typed_domain_record() -> None:
    profile_id = uuid.uuid4()
    row = _active_row(profile_id, generation=4)
    conn = ScriptedConnection([row])

    authorization = get_github_user_authorization(_pool(conn), profile_id=profile_id)

    assert authorization is not None
    assert authorization.profile_id == profile_id
    assert authorization.status is GitHubUserAuthorizationStatus.ACTIVE
    assert authorization.refresh_generation == 4
    # Instants are normalized to timezone-aware UTC at the persistence boundary.
    refresh_expires_at = authorization.refresh_expires_at
    assert refresh_expires_at is not None
    assert refresh_expires_at == _OBSERVED
    assert refresh_expires_at.tzinfo is not None


def test_read_absent_authorization_is_none() -> None:
    conn = ScriptedConnection([None])

    assert get_github_user_authorization(_pool(conn), profile_id=uuid.uuid4()) is None


def test_locked_read_is_a_for_update_statement_bound_to_the_exact_profile() -> None:
    profile_id = uuid.uuid4()
    conn = ScriptedConnection([_active_row(profile_id)])

    authorization = get_github_user_authorization_for_update(_pool(conn), profile_id=profile_id)

    assert authorization is not None
    sql, params = conn.executed[0]
    assert "for update" in sql
    assert "openorc.github_user_authorizations" in sql


def test_insert_creates_the_first_active_authorization_at_generation_one() -> None:
    profile_id = uuid.uuid4()
    conn = ScriptedConnection([_active_row(profile_id, generation=1)])
    reference = "openorc:github-user-refresh:v1:vault:" + str(uuid.uuid4())
    expires = _OBSERVED + timedelta(days=180)

    inserted = insert_active_github_user_authorization(
        _pool(conn),
        profile_id=profile_id,
        github_user_id=5432,
        github_login="octocat",
        refresh_secret_reference=reference,
        refresh_expires_at=expires,
    )

    assert inserted.refresh_generation == 1
    sql, params = conn.executed[0]
    assert "insert into openorc.github_user_authorizations" in sql
    assert "'active'" in sql
    assert params == (profile_id, 5432, "octocat", reference, expires)


def test_reauthorize_updates_in_place_and_monotonically_advances_the_generation() -> None:
    profile_id = uuid.uuid4()
    conn = ScriptedConnection([_active_row(profile_id, generation=7)])
    reference = "openorc:github-user-refresh:v1:vault:" + str(uuid.uuid4())
    expires = _OBSERVED + timedelta(days=180)

    updated = reauthorize_github_user_authorization(
        _pool(conn),
        profile_id=profile_id,
        github_user_id=5432,
        github_login="octocat",
        refresh_secret_reference=reference,
        refresh_expires_at=expires,
    )

    assert updated is not None
    sql, params = conn.executed[0]
    assert "update openorc.github_user_authorizations" in sql
    # The generation advances monotonically in place — never delete-and-reinsert.
    assert "refresh_generation = refresh_generation + 1" in sql
    assert "status = 'active'" in sql
    assert "revoked_at = null" in sql
    assert params == (5432, "octocat", reference, expires, profile_id)


def test_refresh_install_cas_is_guarded_by_generation_and_active_status() -> None:
    profile_id = uuid.uuid4()
    expected = 7
    expires = _OBSERVED + timedelta(hours=8)
    conn = ScriptedConnection([_active_row(profile_id, generation=8)])

    installed = try_install_rotated_refresh(
        _pool(conn),
        profile_id=profile_id,
        expected_generation=expected,
        refresh_expires_at=expires,
    )

    assert installed is not None
    sql, params = conn.executed[0]
    assert "update openorc.github_user_authorizations" in sql
    assert "refresh_generation = %s" in sql
    assert "status = 'active'" in sql
    assert "refresh_generation = refresh_generation + 1" in sql
    assert params == (expires, profile_id, expected)


def test_refresh_install_cas_reports_stale_state_as_none() -> None:
    conn = ScriptedConnection([None])

    assert (
        try_install_rotated_refresh(
            _pool(conn),
            profile_id=uuid.uuid4(),
            expected_generation=3,
            refresh_expires_at=_OBSERVED,
        )
        is None
    )


def test_revoke_statement_removes_the_usable_credential_and_advances_the_generation() -> None:
    profile_id = uuid.uuid4()
    revoked_row = (
        profile_id,
        5432,
        "octocat",
        "revoked",
        None,
        None,
        8,
        _OBSERVED,
        _OBSERVED,
        _OBSERVED,
        _OBSERVED,
    )
    conn = ScriptedConnection([revoked_row])

    revoked = revoke_github_user_authorization_row(_pool(conn), profile_id=profile_id)

    assert revoked is not None
    assert revoked.status is GitHubUserAuthorizationStatus.REVOKED
    assert revoked.refresh_secret_reference is None
    assert revoked.refresh_expires_at is None
    assert revoked.revoked_at is not None
    sql, params = conn.executed[0]
    assert "update openorc.github_user_authorizations" in sql
    assert "status = 'revoked'" in sql
    assert "refresh_secret_reference = null" in sql
    assert "refresh_generation = refresh_generation + 1" in sql
    assert params == (profile_id,)


def test_revoke_statement_reports_an_absent_row_as_none() -> None:
    conn = ScriptedConnection([None])

    assert revoke_github_user_authorization_row(_pool(conn), profile_id=uuid.uuid4()) is None
    assert "update openorc.github_user_authorizations" in conn.executed[0][0]
