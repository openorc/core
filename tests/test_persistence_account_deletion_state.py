"""Deterministic repository tests for the account-deletion persistence helpers.

The ordinary suite cannot execute Postgres; these tests use a recording
connection fake (mirroring the transaction-boundary fakes) to prove the
deliberate statement shapes and conditional-write guards of the new
persistence surface (issue #97): deterministic ``FOR UPDATE`` locked
enumeration, the Workspace/Profile root locks, and the attempt-scoped
claim/transition/clear/compare-and-swap writes whose conditions make one
invocation unable to clear or move another invocation's protection. Real
lock/cascade/constraint behavior is proven by the integration-marked suite.
"""

from __future__ import annotations

import uuid
from contextlib import contextmanager
from typing import Any, cast

from openorc.persistence import connections as connection_repositories
from openorc.persistence import ownership as ownership_repositories
from openorc.persistence.pool import DatabasePool


class FakeCursor:
    def __init__(self, row: tuple[Any, ...] | None, rows: list[tuple[Any, ...]] | None = None):
        self._row = row
        self._rows = rows

    def fetchone(self) -> tuple[Any, ...] | None:
        return self._row

    def fetchall(self) -> list[tuple[Any, ...]]:
        if self._rows is None:
            return [] if self._row is None else [self._row]
        return self._rows


class FakeConnection:
    """Records executed SQL and returns queued rows in call order."""

    def __init__(self, rows: list[Any] | None = None) -> None:
        self.executed: list[tuple[str, tuple[Any, ...] | None]] = []
        self._rows = list(rows or [])

    def execute(self, sql: str, params: tuple[Any, ...] | None = None) -> FakeCursor:
        self.executed.append((sql, params))
        result = self._rows.pop(0) if self._rows else None
        if isinstance(result, list):
            return FakeCursor(None, result)
        return FakeCursor(result)

    def close(self) -> None:  # pragma: no cover - context manager contract
        pass


class FakePool:
    def __init__(self, conn: FakeConnection) -> None:
        self._conn = conn

    def connection(self) -> Any:
        @contextmanager
        def managed() -> Any:
            yield self._conn

        return managed()

    def close(self) -> None:
        raise AssertionError("helper tests never close pools")


def _pool(conn: FakeConnection) -> DatabasePool:
    return cast(DatabasePool, FakePool(conn))


def test_locked_workspace_enumeration_is_deterministic_and_for_update() -> None:
    workspace_id = uuid.uuid4()
    conn = FakeConnection()

    connection_repositories.list_workspace_connections_for_update(
        _pool(conn), workspace_id=workspace_id
    )

    sql, params = conn.executed[0]
    assert sql == (
        "select id, workspace_id, adapter_type, name, safe_config, session_capacity, "
        "enabled, auth_reference, reported_provider, reported_model, created_at, updated_at "
        "from openorc.connections where workspace_id = %s order by id for update"
    )
    assert params == (workspace_id,)


def test_locked_profile_enumeration_locks_only_connection_rows() -> None:
    owner_profile_id = uuid.uuid4()
    conn = FakeConnection()

    connection_repositories.list_profile_connections_for_update(
        _pool(conn), owner_profile_id=owner_profile_id
    )

    sql, params = conn.executed[0]
    assert "join openorc.workspaces w on c.workspace_id = w.id" in sql
    assert "where w.owner_profile_id = %s" in sql
    assert "order by c.id" in sql
    # Deliberate lock scope: only the Connection rows are locked; the joined
    # Workspace rows are never incidentally locked by the enumeration.
    assert sql.endswith("for update of c")
    assert params == (owner_profile_id,)


def test_workspace_root_lock_is_explicit_for_update() -> None:
    workspace_id = uuid.uuid4()
    conn = FakeConnection()

    ownership_repositories.get_workspace_for_update(_pool(conn), workspace_id)

    sql, params = conn.executed[0]
    assert sql.endswith("from openorc.workspaces where id = %s for update")
    assert params == (workspace_id,)


def test_account_deletion_entry_read_is_the_profile_root_for_update() -> None:
    profile_id = uuid.uuid4()
    conn = FakeConnection()

    ownership_repositories.get_profile_for_account_deletion(
        _pool(conn), profile_id=profile_id, lease_seconds=15.0
    )

    sql, params = conn.executed[0]
    assert "for update" in sql
    # The lease expiry is evaluated against the DATABASE clock inside the
    # same statement — never the application clock.
    assert "make_interval(secs => %s)" in sql
    assert "< now()" in sql
    assert params == (15.0, profile_id)


def test_barrier_read_conflicts_with_the_deletion_root_lock() -> None:
    profile_id = uuid.uuid4()
    conn = FakeConnection()

    exists, state = ownership_repositories.read_account_deletion_state_for_key_share(
        _pool(conn), profile_id=profile_id
    )

    sql, params = conn.executed[0]
    assert sql.endswith("from openorc.profiles where id = %s for key share")
    assert params == (profile_id,)
    assert (exists, state) == (False, None)


def test_attempt_claim_is_conditional_on_the_null_state_tuple() -> None:
    profile_id = uuid.uuid4()
    attempt_id = uuid.uuid4()
    conn = FakeConnection([(1,)])

    claimed = ownership_repositories.claim_account_deletion_attempt(
        _pool(conn), profile_id=profile_id, attempt_id=attempt_id
    )

    assert claimed is True
    sql, params = conn.executed[0]
    assert "set account_deletion_state = 'active'" in sql
    assert "account_deletion_state is null" in sql
    assert "account_deletion_attempt_id is null" in sql
    assert "account_deletion_started_at is null" in sql
    assert params == (attempt_id, profile_id)


def test_attempt_transition_to_uncertain_is_attempt_scoped() -> None:
    profile_id = uuid.uuid4()
    attempt_id = uuid.uuid4()
    conn = FakeConnection([(1,)])

    marked = ownership_repositories.mark_account_deletion_attempt_uncertain(
        _pool(conn), profile_id=profile_id, attempt_id=attempt_id
    )

    assert marked is True
    sql, params = conn.executed[0]
    assert "set account_deletion_state = 'uncertain'" in sql
    assert "account_deletion_state = 'active'" in sql
    assert "account_deletion_attempt_id = %s" in sql
    assert params == (profile_id, attempt_id)


def test_attempt_clear_is_attempt_scoped_and_never_touches_uncertain_state() -> None:
    profile_id = uuid.uuid4()
    attempt_id = uuid.uuid4()
    conn = FakeConnection([(1,)])

    cleared = ownership_repositories.clear_account_deletion_attempt(
        _pool(conn), profile_id=profile_id, attempt_id=attempt_id
    )

    assert cleared is True
    sql, params = conn.executed[0]
    assert "account_deletion_state = null" in sql
    assert "account_deletion_attempt_id = null" in sql
    assert "account_deletion_started_at = null" in sql
    assert "account_deletion_state = 'active'" in sql
    assert params == (profile_id, attempt_id)


def test_uncertain_cas_is_bound_to_the_exact_reconciled_attempt() -> None:
    profile_id = uuid.uuid4()
    reconciled_attempt_id = uuid.uuid4()
    new_attempt_id = uuid.uuid4()
    conn = FakeConnection([(1,)])

    claimed = ownership_repositories.reclaim_uncertain_account_deletion_attempt(
        _pool(conn),
        profile_id=profile_id,
        reconciled_attempt_id=reconciled_attempt_id,
        new_attempt_id=new_attempt_id,
    )

    assert claimed is True
    sql, params = conn.executed[0]
    assert "account_deletion_state = 'uncertain'" in sql
    assert "account_deletion_attempt_id = %s" in sql
    assert params == (new_attempt_id, profile_id, reconciled_attempt_id)


def test_expired_active_cas_requires_the_lease_expiry_in_database_time() -> None:
    profile_id = uuid.uuid4()
    expired_attempt_id = uuid.uuid4()
    new_attempt_id = uuid.uuid4()
    conn = FakeConnection([(1,)])

    claimed = ownership_repositories.reclaim_expired_account_deletion_attempt(
        _pool(conn),
        profile_id=profile_id,
        expired_attempt_id=expired_attempt_id,
        new_attempt_id=new_attempt_id,
        lease_seconds=15.0,
    )

    assert claimed is True
    sql, params = conn.executed[0]
    assert "account_deletion_state = 'active'" in sql
    assert "account_deletion_attempt_id = %s" in sql
    assert "account_deletion_started_at + make_interval(secs => %s) < now()" in sql
    assert params == (new_attempt_id, profile_id, expired_attempt_id, 15.0)
