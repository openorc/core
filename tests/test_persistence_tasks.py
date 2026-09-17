"""Deterministic mapping tests for the Task repositories.

The ordinary suite cannot execute Postgres; these tests use canned rows and a
fake pool/connection seam (mirroring the connection/ownership mapping fakes)
to prove row-to-domain-object mapping, UTC normalization at the persistence
boundary, parameterization, and the conditional-mutation contracts:
``update_task_status`` is nonterminal-only and raises before any SQL,
``archive_task`` is the exclusive terminal path stamping ``archived_at``, and
every authoritative mutation is conditional on ``expected_state_token`` while
rotating the token in the same statement. Database constraint behavior is
proven against a real database by the integration-marked suite.
"""

from __future__ import annotations

import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta, timezone
from typing import Any, cast

import pytest

from openorc.domain.tasks import TaskDomainError, TaskStatus
from openorc.persistence.pool import DatabasePool
from openorc.persistence.tasks import (
    archive_task,
    bind_canonical_branch,
    create_task,
    find_current_task_by_branch,
    find_current_task_for_issue,
    get_task,
    list_issue_attempts,
    list_workspace_tasks,
    update_task_status,
)


class FakeCursor:
    """Returns one canned row (and optional canned rows), like a psycopg cursor."""

    def __init__(self, row: tuple[Any, ...] | None, rows: list[tuple[Any, ...]] | None) -> None:
        self._row = row
        self._rows = rows if rows is not None else ([] if row is None else [row])

    def fetchone(self) -> tuple[Any, ...] | None:
        return self._row

    def fetchall(self) -> list[tuple[Any, ...]]:
        return list(self._rows)


class FakeConnection:
    """Records executed SQL and returns canned rows."""

    def __init__(
        self,
        row: tuple[Any, ...] | None = None,
        rows: list[tuple[Any, ...]] | None = None,
    ) -> None:
        self.row = row
        self.rows = rows
        self.executed: list[tuple[str, tuple[Any, ...] | None]] = []

    def execute(self, sql: str, params: tuple[Any, ...] | None = None) -> FakeCursor:
        self.executed.append((sql, params))
        return FakeCursor(self.row, self.rows)


class FakePool:
    """Emulates psycopg_pool ConnectionPool.connection() semantics."""

    def __init__(self, conn: FakeConnection) -> None:
        self._conn = conn

    def connection(self) -> Any:
        @contextmanager
        def managed() -> Iterator[FakeConnection]:
            yield self._conn

        return managed()

    def close(self) -> None:
        raise AssertionError("mapping tests never close pools")


def _observed_at() -> datetime:
    # Deliberately non-UTC offset to prove UTC normalization in mappings.
    return datetime(2026, 9, 17, 9, 30, 0, tzinfo=timezone(timedelta(hours=-5)))


def _utc_observed_at() -> datetime:
    return _observed_at().astimezone(UTC)


def _task_row(**overrides: Any) -> tuple[Any, ...]:
    values: dict[str, Any] = {
        "id": uuid.uuid4(),
        "workspace_id": uuid.uuid4(),
        "repository_id": uuid.uuid4(),
        "github_issue_id": 9001,
        "github_issue_number": 42,
        "status": "ready_to_plan",
        "archived_at": None,
        "canonical_feature_branch": None,
        "state_token": uuid.uuid4(),
        "current_plan_revision_id": None,
        "current_owner_gate_id": None,
        "created_at": _observed_at(),
        "updated_at": _observed_at(),
    }
    values.update(overrides)
    return (
        values["id"],
        values["workspace_id"],
        values["repository_id"],
        values["github_issue_id"],
        values["github_issue_number"],
        values["status"],
        values["archived_at"],
        values["canonical_feature_branch"],
        values["state_token"],
        values["current_plan_revision_id"],
        values["current_owner_gate_id"],
        values["created_at"],
        values["updated_at"],
    )


def test_create_task_maps_row_and_normalizes_utc() -> None:
    # A fresh Task is created with a NULL canonical branch; binding happens
    # once later workflow logic has verified the Producer-created branch.
    row = _task_row(status="planning")
    fake_conn = FakeConnection(row)
    pool = cast(DatabasePool, FakePool(fake_conn))

    created = create_task(
        pool,
        workspace_id=row[1],
        repository_id=row[2],
        github_issue_id=9001,
        github_issue_number=42,
        status=TaskStatus.PLANNING,
    )

    assert created.id == row[0]
    assert created.workspace_id == row[1]
    assert created.repository_id == row[2]
    assert created.github_issue_id == 9001
    assert created.github_issue_number == 42
    assert created.status is TaskStatus.PLANNING
    assert created.archived_at is None
    assert created.canonical_feature_branch is None
    assert created.created_at == _utc_observed_at()
    assert created.created_at.utcoffset() == timedelta(0)

    sql, params = fake_conn.executed[0]
    assert "insert into openorc.tasks" in sql
    # The insert's column list never sets a canonical branch: create-NULL is
    # the contract (the RETURNING projection legitimately reads it back).
    columns = sql.split("(", 1)[1].split(")", 1)[0]
    assert columns == "workspace_id, repository_id, github_issue_id, github_issue_number, status"
    assert params is not None
    assert params == (row[1], row[2], 9001, 42, "planning")
    # Values are parameterized, never interpolated into the SQL text.
    assert "9001" not in sql


def test_create_task_rejects_terminal_status_before_any_sql() -> None:
    fake_conn = FakeConnection()
    pool = cast(DatabasePool, FakePool(fake_conn))

    with pytest.raises(TaskDomainError, match="terminal"):
        create_task(
            pool,
            workspace_id=uuid.uuid4(),
            repository_id=uuid.uuid4(),
            github_issue_id=1,
            github_issue_number=1,
            status=TaskStatus.CANCELLED,
        )
    assert fake_conn.executed == []

    with pytest.raises(TaskDomainError, match="terminal"):
        create_task(
            pool,
            workspace_id=uuid.uuid4(),
            repository_id=uuid.uuid4(),
            github_issue_id=1,
            github_issue_number=1,
            status=TaskStatus.COMPLETED,
        )
    assert fake_conn.executed == []


def test_get_task_maps_row_or_none() -> None:
    row = _task_row()
    found = get_task(cast(DatabasePool, FakePool(FakeConnection(row))), row[0])
    assert found is not None
    assert found.id == row[0]
    assert found.state_token == row[8]
    assert get_task(cast(DatabasePool, FakePool(FakeConnection(None))), row[0]) is None


def test_list_workspace_tasks_builds_one_valid_where_clause() -> None:
    # Construction guard: both paths must produce exactly one WHERE clause
    # (a predicate appended after the workspace condition would emit
    # "... where archived_at is null where workspace_id = %s", which is
    # invalid SQL the fake cannot catch by substring assertions alone).
    rows = [_task_row(), _task_row()]
    fake_conn = FakeConnection(None, rows)
    pool = cast(DatabasePool, FakePool(fake_conn))

    listed = list_workspace_tasks(pool, workspace_id=rows[0][1])
    assert [task.id for task in listed] == [rows[0][0], rows[1][0]]
    sql, params = fake_conn.executed[0]
    assert sql.count(" where ") == 1
    assert "where workspace_id = %s" in sql
    # The active-only filter narrows the workspace condition, never opens a
    # second WHERE clause.
    assert "and archived_at is null" in sql
    assert sql.index("where workspace_id") < sql.index("and archived_at is null")
    assert params == (rows[0][1],)

    all_tasks = list_workspace_tasks(pool, workspace_id=rows[0][1], include_archived=True)
    assert [task.id for task in all_tasks] == [rows[0][0], rows[1][0]]
    sql, _ = fake_conn.executed[1]
    assert sql.count(" where ") == 1
    assert "where workspace_id = %s" in sql
    assert "archived_at is null" not in sql


def test_list_issue_attempts_and_current_resolution_queries() -> None:
    rows = [_task_row(), _task_row(status="cancelled", archived_at=_observed_at())]
    fake_conn = FakeConnection(None, rows)
    pool = cast(DatabasePool, FakePool(fake_conn))

    attempts = list_issue_attempts(pool, repository_id=rows[0][2], github_issue_id=9001)
    assert [task.id for task in attempts] == [rows[0][0], rows[1][0]]
    sql, params = fake_conn.executed[0]
    assert "repository_id = %s and github_issue_id = %s" in sql
    assert params == (rows[0][2], 9001)

    fake_conn = FakeConnection(rows[0])
    current = find_current_task_for_issue(
        cast(DatabasePool, FakePool(fake_conn)),
        repository_id=rows[0][2],
        github_issue_id=9001,
    )
    assert current is not None and current.id == rows[0][0]
    sql, _ = fake_conn.executed[0]
    assert "and archived_at is null" in sql

    fake_conn = FakeConnection(None)
    assert (
        find_current_task_for_issue(
            cast(DatabasePool, FakePool(fake_conn)),
            repository_id=rows[0][2],
            github_issue_id=9001,
        )
        is None
    )


def test_find_current_task_by_branch_uses_ownership_lookup() -> None:
    row = _task_row(canonical_feature_branch="openorc/task-42/prod")
    fake_conn = FakeConnection(row)
    pool = cast(DatabasePool, FakePool(fake_conn))

    owner = find_current_task_by_branch(
        pool, repository_id=row[2], canonical_feature_branch="openorc/task-42/prod"
    )
    assert owner is not None and owner.id == row[0]
    sql, params = fake_conn.executed[0]
    assert "canonical_feature_branch = %s" in sql
    assert "and archived_at is null" in sql
    assert params == (row[2], "openorc/task-42/prod")


def test_update_task_status_is_conditional_and_rotates_the_token() -> None:
    # The canned row mirrors the post-mutation row the database would return:
    # new status, freshly rotated token.
    original_token = uuid.uuid4()
    row = _task_row(status="waiting_for_owner", state_token=uuid.uuid4())
    fake_conn = FakeConnection(row)
    pool = cast(DatabasePool, FakePool(fake_conn))

    updated = update_task_status(
        pool,
        row[0],
        expected_state_token=original_token,
        status=TaskStatus.WAITING_FOR_OWNER,
    )

    assert updated is not None
    assert updated.status is TaskStatus.WAITING_FOR_OWNER
    assert updated.state_token != original_token
    sql, params = fake_conn.executed[0]
    # Conditional on the expected token; rotates the token atomically.
    assert "set status = %s, state_token = gen_random_uuid(), updated_at = now()" in sql
    assert "where id = %s and state_token = %s and archived_at is null" in sql
    assert params == ("waiting_for_owner", row[0], original_token)


def test_update_task_status_returns_none_when_token_is_stale() -> None:
    fake_conn = FakeConnection(None)
    pool = cast(DatabasePool, FakePool(fake_conn))

    assert (
        update_task_status(
            pool,
            uuid.uuid4(),
            expected_state_token=uuid.uuid4(),
            status=TaskStatus.IMPLEMENTING,
        )
        is None
    )
    sql, _ = fake_conn.executed[0]
    assert "state_token = %s" in sql


@pytest.mark.parametrize("terminal", [TaskStatus.CANCELLED, TaskStatus.COMPLETED])
def test_update_task_status_never_accepts_terminal_statuses(terminal: TaskStatus) -> None:
    # Terminal transitions go exclusively through archive_task: a competing
    # path would either violate the CHECK constraint or bypass archival.
    fake_conn = FakeConnection()
    pool = cast(DatabasePool, FakePool(fake_conn))

    with pytest.raises(TaskDomainError, match="archive_task"):
        update_task_status(
            pool,
            uuid.uuid4(),
            expected_state_token=uuid.uuid4(),
            status=terminal,
        )
    assert fake_conn.executed == []


def test_archive_task_stamps_archival_and_rotates_the_token() -> None:
    # The canned row mirrors the post-mutation row: terminal status, archived
    # instant, freshly rotated token.
    original_token = uuid.uuid4()
    row = _task_row(status="cancelled", archived_at=_observed_at(), state_token=uuid.uuid4())
    fake_conn = FakeConnection(row)
    pool = cast(DatabasePool, FakePool(fake_conn))

    archived = archive_task(
        pool,
        row[0],
        expected_state_token=original_token,
        terminal_status=TaskStatus.CANCELLED,
    )

    assert archived is not None
    assert archived.status is TaskStatus.CANCELLED
    assert archived.archived_at == _utc_observed_at()
    assert archived.state_token != original_token
    sql, params = fake_conn.executed[0]
    # One atomic statement: terminal status + archival + token rotation.
    assert (
        "set status = %s, archived_at = now(), "
        "state_token = gen_random_uuid(), updated_at = now()" in sql
    )
    assert "where id = %s and state_token = %s and archived_at is null" in sql
    assert params == ("cancelled", row[0], original_token)


def test_archive_task_supports_completed_outcome() -> None:
    original_token = uuid.uuid4()
    row = _task_row(status="completed", archived_at=_observed_at(), state_token=uuid.uuid4())
    fake_conn = FakeConnection(row)
    pool = cast(DatabasePool, FakePool(fake_conn))

    archived = archive_task(
        pool,
        row[0],
        expected_state_token=original_token,
        terminal_status=TaskStatus.COMPLETED,
    )
    assert archived is not None
    assert archived.status is TaskStatus.COMPLETED
    assert archived.archived_at is not None
    assert fake_conn.executed[0][1] == ("completed", row[0], original_token)


@pytest.mark.parametrize("nonterminal", [TaskStatus.PLANNING, TaskStatus.BLOCKED])
def test_archive_task_requires_a_terminal_status(nonterminal: TaskStatus) -> None:
    fake_conn = FakeConnection()
    pool = cast(DatabasePool, FakePool(fake_conn))

    with pytest.raises(TaskDomainError, match="terminal"):
        archive_task(
            pool,
            uuid.uuid4(),
            expected_state_token=uuid.uuid4(),
            terminal_status=nonterminal,
        )
    assert fake_conn.executed == []


def test_archive_task_returns_none_when_token_is_stale() -> None:
    fake_conn = FakeConnection(None)
    pool = cast(DatabasePool, FakePool(fake_conn))

    assert (
        archive_task(
            pool,
            uuid.uuid4(),
            expected_state_token=uuid.uuid4(),
            terminal_status=TaskStatus.CANCELLED,
        )
        is None
    )


def test_bind_canonical_branch_is_conditional_and_one_time() -> None:
    # The canned row mirrors the post-mutation row: bound branch, rotated token.
    original_token = uuid.uuid4()
    row = _task_row(canonical_feature_branch="openorc/task-42/prod", state_token=uuid.uuid4())
    fake_conn = FakeConnection(row)
    pool = cast(DatabasePool, FakePool(fake_conn))

    bound = bind_canonical_branch(
        pool,
        row[0],
        expected_state_token=original_token,
        canonical_feature_branch="openorc/task-42/prod",
    )

    assert bound is not None
    assert bound.canonical_feature_branch == "openorc/task-42/prod"
    assert bound.state_token != original_token
    sql, params = fake_conn.executed[0]
    assert (
        "set canonical_feature_branch = %s, "
        "state_token = gen_random_uuid(), updated_at = now()" in sql
    )
    # One-time binding: the update only applies while the branch is NULL, so a
    # rebinding attempt is a rejected no-op rather than a competing claim.
    assert "and canonical_feature_branch is null" in sql
    assert "where id = %s and state_token = %s and archived_at is null" in sql
    assert params == ("openorc/task-42/prod", row[0], original_token)


@pytest.mark.parametrize("branch", [None, "", "   ", "\t\n"])
def test_bind_canonical_branch_requires_a_nonblank_branch(branch: object) -> None:
    # One-time binding only: there is no release-by-NULL path — archival is
    # what releases branch ownership for future Tasks.
    fake_conn = FakeConnection()
    pool = cast(DatabasePool, FakePool(fake_conn))

    with pytest.raises(TaskDomainError, match="non-empty"):
        bind_canonical_branch(
            pool,
            uuid.uuid4(),
            expected_state_token=uuid.uuid4(),
            canonical_feature_branch=branch,  # type: ignore[arg-type]
        )
    assert fake_conn.executed == []


def test_bind_canonical_branch_returns_none_when_token_is_stale() -> None:
    fake_conn = FakeConnection(None)
    pool = cast(DatabasePool, FakePool(fake_conn))

    assert (
        bind_canonical_branch(
            pool,
            uuid.uuid4(),
            expected_state_token=uuid.uuid4(),
            canonical_feature_branch="openorc/task-42/prod",
        )
        is None
    )
