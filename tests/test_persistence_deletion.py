"""Deterministic repository tests for the destructive deletion operations.

The ordinary suite cannot execute Postgres; these tests use a recording
connection fake (mirroring the transaction-boundary fakes) to prove the
deliberate statement ordering, parameterization, purge-guard branching, and
row-to-domain mapping of the deletion repositories. Database cascade and
constraint behavior is proven against a real database by the
integration-marked suite in ``tests/integration/``.
"""

from __future__ import annotations

import uuid
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from typing import Any, cast

import pytest

from openorc.domain.tasks import TaskDomainError, TaskStatus
from openorc.persistence.deletion import (
    delete_connection,
    delete_project,
    delete_repository,
    delete_workspace,
    disconnect_connection,
    purge_archived_task,
)
from openorc.persistence.pool import DatabasePool


class FakeCursor:
    """Returns the queued row for this call, like a psycopg cursor."""

    def __init__(self, row: tuple[Any, ...] | None) -> None:
        self._row = row

    def fetchone(self) -> tuple[Any, ...] | None:
        return self._row


class FakeConnection:
    """Records executed SQL and returns queued rows in call order."""

    def __init__(self, rows: list[tuple[Any, ...] | None] | None = None) -> None:
        self.executed: list[tuple[str, tuple[Any, ...] | None]] = []
        self._rows = list(rows or [])

    def execute(self, sql: str, params: tuple[Any, ...] | None = None) -> FakeCursor:
        self.executed.append((sql, params))
        row = self._rows.pop(0) if self._rows else None
        return FakeCursor(row)


class FakePool:
    """Emulates psycopg_pool ConnectionPool.connection() semantics."""

    def __init__(self, conn: FakeConnection) -> None:
        self._conn = conn

    def connection(self) -> Any:
        @contextmanager
        def managed() -> Any:
            yield self._conn

        return managed()

    def close(self) -> None:
        raise AssertionError("deletion mapping tests never close pools")


def _observed_at() -> datetime:
    # Deliberately non-UTC offset to prove UTC normalization in mappings.
    return datetime(2026, 9, 19, 12, 0, 0, tzinfo=timezone(timedelta(hours=2)))


def _task_row(archived: bool) -> tuple[Any, ...]:
    archived_at = _observed_at() if archived else None
    return (
        uuid.uuid4(),
        uuid.uuid4(),
        uuid.uuid4(),
        424242,
        7,
        "completed" if archived else "ready_to_plan",
        archived_at,
        None,
        uuid.uuid4(),
        None,
        None,
        _observed_at(),
        _observed_at(),
    )


def test_delete_workspace_deletes_in_deliberate_dependency_order() -> None:
    workspace_id = uuid.uuid4()
    profile_id = uuid.uuid4()
    observed = _observed_at()
    conn = FakeConnection(
        rows=[
            None,
            None,
            None,
            None,
            (workspace_id, profile_id, "platform", observed, observed, 5, ""),
        ]
    )

    workspace = delete_workspace(cast(DatabasePool, FakePool(conn)), workspace_id)

    assert workspace is not None
    assert workspace.id == workspace_id
    assert workspace.owner_profile_id == profile_id
    assert workspace.created_at.utcoffset() == timedelta(0)
    sqls = [sql for sql, _ in conn.executed]
    assert sqls[0].startswith("update openorc.tasks")
    assert "current_plan_revision_id = null" in sqls[0]
    assert "current_owner_gate_id = null" in sqls[0]
    assert sqls[1].startswith("delete from openorc.tasks")
    assert sqls[2].startswith("delete from openorc.workflow_role_bindings")
    assert sqls[3].startswith("delete from openorc.connections")
    assert sqls[4].startswith("delete from openorc.workspaces")
    assert "returning" in sqls[4]
    assert len(sqls) == 5
    assert all(params == (workspace_id,) for _, params in conn.executed)


def test_delete_workspace_returns_none_for_a_missing_workspace() -> None:
    conn = FakeConnection()

    assert delete_workspace(cast(DatabasePool, FakePool(conn)), uuid.uuid4()) is None
    assert [sql for sql, _ in conn.executed][-1].startswith("delete from openorc.workspaces")


def test_delete_project_deletes_its_repository_task_subtree_in_order() -> None:
    project_id = uuid.uuid4()
    workspace_id = uuid.uuid4()
    observed = _observed_at()
    conn = FakeConnection(
        rows=[None, None, None, (project_id, workspace_id, "project", observed, observed)]
    )

    project = delete_project(cast(DatabasePool, FakePool(conn)), project_id)

    assert project is not None and project.id == project_id
    sqls = [sql for sql, _ in conn.executed]
    assert sqls[0].startswith("update openorc.tasks")
    assert "select id from openorc.repositories where project_id = %s" in sqls[0]
    assert sqls[1].startswith("delete from openorc.tasks")
    assert "select id from openorc.repositories where project_id = %s" in sqls[1]
    assert sqls[2].startswith("delete from openorc.repositories")
    assert sqls[3].startswith("delete from openorc.projects")
    assert len(sqls) == 4
    assert all(params == (project_id,) for _, params in conn.executed)


def test_delete_repository_deletes_its_task_subtree_in_order() -> None:
    repository_id = uuid.uuid4()
    project_id = uuid.uuid4()
    workspace_id = uuid.uuid4()
    observed = _observed_at()
    conn = FakeConnection(
        rows=[
            None,
            None,
            (
                repository_id,
                project_id,
                workspace_id,
                987654321,
                "octocat",
                "hello-world",
                "https://github.com/octocat/hello-world",
                False,
                "main",
                observed,
                observed,
            ),
        ]
    )

    repository = delete_repository(cast(DatabasePool, FakePool(conn)), repository_id)

    assert repository is not None and repository.id == repository_id
    assert repository.identity.github_repository_id == 987654321
    sqls = [sql for sql, _ in conn.executed]
    assert sqls[0].startswith("update openorc.tasks")
    assert sqls[1].startswith("delete from openorc.tasks")
    assert sqls[2].startswith("delete from openorc.repositories")
    assert len(sqls) == 3
    assert all(params == (repository_id,) for _, params in conn.executed)


def test_purge_archived_task_rejects_a_current_task_before_any_deletion() -> None:
    conn = FakeConnection(rows=[_task_row(archived=False)])

    with pytest.raises(TaskDomainError):
        purge_archived_task(cast(DatabasePool, FakePool(conn)), uuid.uuid4())

    sqls = [sql for sql, _ in conn.executed]
    assert len(sqls) == 1
    assert sqls[0].startswith("select")
    assert "for update" in sqls[0]


def test_purge_archived_task_clears_pointers_then_deletes_the_archived_task() -> None:
    row = _task_row(archived=True)
    task_id = row[0]
    conn = FakeConnection(rows=[row, None, None])

    purged = purge_archived_task(cast(DatabasePool, FakePool(conn)), task_id)

    assert purged is not None and purged.id == task_id
    assert purged.status is TaskStatus.COMPLETED
    assert purged.archived_at is not None
    assert purged.archived_at.utcoffset() == timedelta(0)
    sqls = [sql for sql, _ in conn.executed]
    assert sqls[0].startswith("select")
    assert "for update" in sqls[0]
    assert sqls[1].startswith("update openorc.tasks")
    assert "current_plan_revision_id = null" in sqls[1]
    assert "current_owner_gate_id = null" in sqls[1]
    assert sqls[2].startswith("delete from openorc.tasks")
    assert len(sqls) == 3
    assert all(params == (task_id,) for _, params in conn.executed)


def test_purge_archived_task_returns_none_for_a_missing_task() -> None:
    conn = FakeConnection()

    assert purge_archived_task(cast(DatabasePool, FakePool(conn)), uuid.uuid4()) is None
    assert len(conn.executed) == 1


def test_delete_connection_forces_the_connection_reference_constraints_immediate() -> None:
    connection_id = uuid.uuid4()
    workspace_id = uuid.uuid4()
    observed = _observed_at()
    row = (
        connection_id,
        workspace_id,
        "cline",
        "hub",
        {},
        1,
        True,
        "auth-ref",
        None,
        None,
        observed,
        observed,
    )
    conn = FakeConnection(rows=[row, None])

    deleted = delete_connection(cast(DatabasePool, FakePool(conn)), connection_id)

    assert deleted is not None and deleted.id == connection_id
    sqls = [sql for sql, _ in conn.executed]
    assert sqls[0].startswith("delete from openorc.connections")
    assert "returning" in sqls[0]
    assert sqls[1].startswith("set constraints")
    assert "openorc.task_agent_sessions_connection_id_workspace_id_fkey" in sqls[1]
    assert "openorc.workflow_role_bindings_connection_id_workspace_id_fkey" in sqls[1]
    assert sqls[1].endswith(" immediate")
    assert len(sqls) == 2


def test_delete_connection_skips_the_constraint_forcing_for_a_missing_row() -> None:
    conn = FakeConnection()

    assert delete_connection(cast(DatabasePool, FakePool(conn)), uuid.uuid4()) is None
    sqls = [sql for sql, _ in conn.executed]
    assert len(sqls) == 1
    assert sqls[0].startswith("delete from openorc.connections")


def test_disconnect_connection_revokes_use_and_drops_the_auth_reference_atomically() -> None:
    connection_id = uuid.uuid4()
    workspace_id = uuid.uuid4()
    observed = _observed_at()
    row = (
        connection_id,
        workspace_id,
        "cline",
        "hub",
        {},
        1,
        False,
        None,
        None,
        None,
        observed,
        observed,
    )
    conn = FakeConnection(rows=[row])

    disconnected = disconnect_connection(cast(DatabasePool, FakePool(conn)), connection_id)

    assert disconnected is not None and disconnected.id == connection_id
    assert disconnected.enabled is False
    assert disconnected.auth_reference is None
    sql, params = conn.executed[0]
    assert sql.startswith("update openorc.connections")
    assert "enabled = false" in sql
    assert "auth_reference = null" in sql
    assert "updated_at = now()" in sql
    assert params == (connection_id,)
