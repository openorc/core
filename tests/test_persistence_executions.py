"""Deterministic mapping tests for Execution repositories (issue #24).

The ordinary suite cannot execute Postgres; these tests use canned rows and
a fake pool/connection seam (mirroring the planning/review mapping fakes)
to prove row-to-domain-object mapping, UTC normalization, parameterization,
the producer-role enforcement in the creation transaction (a Reviewer
session never anchors an Execution), and the guarded absorbing transition
SQL (finalized history is never rewritten). Database constraint behavior is
proven against a real database by the integration-marked suite in
``tests/integration/``.
"""

from __future__ import annotations

import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta, timezone
from typing import Any, cast

import pytest

from openorc.domain.executions import ExecutionDomainError, ExecutionStatus
from openorc.persistence import executions as executions_module
from openorc.persistence.executions import (
    create_execution,
    get_execution,
    list_task_executions,
    update_execution_status,
)
from openorc.persistence.pool import DatabasePool


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
    """Records executed SQL and returns canned rows, optionally per statement."""

    def __init__(
        self,
        row: tuple[Any, ...] | None = None,
        rows: list[tuple[Any, ...]] | None = None,
        responses: list[tuple[Any, ...] | None] | None = None,
    ) -> None:
        self.row = row
        self.rows = rows
        self.responses = list(responses) if responses is not None else None
        self.executed: list[tuple[str, tuple[Any, ...] | None]] = []

    def execute(self, sql: str, params: tuple[Any, ...] | None = None) -> FakeCursor:
        self.executed.append((sql, params))
        if self.responses is not None:
            response = self.responses.pop(0)
            return FakeCursor(response, None if response is None else [response])
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
    return datetime(2026, 9, 17, 12, 0, 0, tzinfo=timezone(timedelta(hours=2)))


def _utc_observed_at() -> datetime:
    return _observed_at().astimezone(UTC)


def _execution_row(**overrides: Any) -> tuple[Any, ...]:
    values: dict[str, Any] = {
        "id": uuid.uuid4(),
        "workspace_id": uuid.uuid4(),
        "task_id": uuid.uuid4(),
        "producer_session_id": uuid.uuid4(),
        "execution_number": 3,
        "status": "queued",
        "created_at": _observed_at(),
        "updated_at": _observed_at(),
    }
    values.update(overrides)
    return (
        values["id"],
        values["workspace_id"],
        values["task_id"],
        values["producer_session_id"],
        values["execution_number"],
        values["status"],
        values["created_at"],
        values["updated_at"],
    )


def test_create_execution_inserts_queued_and_maps_the_row() -> None:
    row = _execution_row()
    fake_conn = FakeConnection(responses=[("producer",), row])
    pool = cast(DatabasePool, FakePool(fake_conn))

    execution = create_execution(
        pool,
        workspace_id=row[1],
        task_id=row[2],
        producer_session_id=row[3],
        execution_number=row[4],
    )

    assert execution.id == row[0]
    assert execution.producer_session_id == row[3]
    assert execution.execution_number == 3
    assert execution.status is ExecutionStatus.QUEUED
    assert execution.created_at == _utc_observed_at()
    assert execution.created_at.utcoffset() == timedelta(0)

    # The creation transaction reads the binding's role before the insert.
    assert len(fake_conn.executed) == 2
    role_sql, role_params = fake_conn.executed[0]
    assert "select role from openorc.task_agent_sessions" in role_sql
    assert role_params == (row[3],)
    insert_sql, insert_params = fake_conn.executed[1]
    assert "insert into openorc.executions" in insert_sql
    assert insert_params == (row[1], row[2], row[3], row[4], "queued")


def test_create_execution_rejects_a_reviewer_or_missing_session_before_insert() -> None:
    base: dict[str, Any] = {
        "workspace_id": uuid.uuid4(),
        "task_id": uuid.uuid4(),
        "producer_session_id": uuid.uuid4(),
        "execution_number": 1,
    }
    reviewer_conn = FakeConnection(responses=[("reviewer",)])
    with pytest.raises(ExecutionDomainError):
        create_execution(cast(DatabasePool, FakePool(reviewer_conn)), **base)
    missing_conn = FakeConnection(responses=[None])
    with pytest.raises(ExecutionDomainError):
        create_execution(cast(DatabasePool, FakePool(missing_conn)), **base)
    # The insert never ran: no Execution may be anchored to the session.
    assert len(reviewer_conn.executed) == 1
    assert len(missing_conn.executed) == 1


def test_create_execution_rejects_nonpositive_attempt_numbers() -> None:
    fake_conn = FakeConnection()
    pool = cast(DatabasePool, FakePool(fake_conn))
    for bad in (0, -1, 1.5, "1", True):
        with pytest.raises(ExecutionDomainError):
            create_execution(
                pool,
                workspace_id=uuid.uuid4(),
                task_id=uuid.uuid4(),
                producer_session_id=uuid.uuid4(),
                execution_number=bad,  # type: ignore[arg-type]
            )
    assert fake_conn.executed == []


def test_get_execution_maps_or_returns_none() -> None:
    row = _execution_row(status="running")
    pool = cast(DatabasePool, FakePool(FakeConnection(row)))
    execution = get_execution(pool, execution_id=row[0])
    assert execution is not None
    assert execution.status is ExecutionStatus.RUNNING
    assert execution.updated_at == _utc_observed_at()

    empty_pool = cast(DatabasePool, FakePool(FakeConnection(None)))
    assert get_execution(empty_pool, execution_id=row[0]) is None


def test_list_task_executions_orders_by_attempt_number() -> None:
    first = _execution_row(execution_number=1, status="failed_transient")
    second = _execution_row(task_id=first[2], execution_number=2)
    fake_conn = FakeConnection(rows=[first, second])
    pool = cast(DatabasePool, FakePool(fake_conn))

    executions = list_task_executions(pool, task_id=first[2])

    assert [execution.execution_number for execution in executions] == [1, 2]
    sql, params = fake_conn.executed[0]
    assert "openorc.executions" in sql
    assert "order by execution_number" in sql
    assert params == (first[2],)


def test_update_execution_status_applies_a_guarded_transition() -> None:
    row = _execution_row(status="running")
    updated = _execution_row(id=row[0], status="paused")
    fake_conn = FakeConnection(row=updated)
    pool = cast(DatabasePool, FakePool(fake_conn))

    execution = update_execution_status(
        pool,
        execution_id=row[0],
        expected_status=ExecutionStatus.RUNNING,
        next_status=ExecutionStatus.PAUSED,
    )

    assert execution is not None
    assert execution.status is ExecutionStatus.PAUSED
    sql, params = fake_conn.executed[0]
    assert "update openorc.executions" in sql
    assert "updated_at = now()" in sql
    assert "where id = %s and status = %s" in sql
    assert params == ("paused", row[0], "running")


def test_update_execution_status_missing_or_mismatched_row_is_a_no_op() -> None:
    pool = cast(DatabasePool, FakePool(FakeConnection(None)))
    assert (
        update_execution_status(
            pool,
            execution_id=uuid.uuid4(),
            expected_status=ExecutionStatus.RUNNING,
            next_status=ExecutionStatus.PAUSED,
        )
        is None
    )


def test_update_execution_status_never_rewrites_finalized_history() -> None:
    fake_conn = FakeConnection()
    pool = cast(DatabasePool, FakePool(fake_conn))
    # Transitions out of the four final statuses are rejected before SQL.
    for final_status in (
        ExecutionStatus.SUCCEEDED,
        ExecutionStatus.FAILED_TRANSIENT,
        ExecutionStatus.FAILED_FINAL,
        ExecutionStatus.CANCELLED,
    ):
        with pytest.raises(ExecutionDomainError):
            update_execution_status(
                pool,
                execution_id=uuid.uuid4(),
                expected_status=final_status,
                next_status=ExecutionStatus.RUNNING,
            )
    assert fake_conn.executed == []


def test_update_execution_status_rejects_bad_status_values_and_no_ops() -> None:
    fake_conn = FakeConnection()
    pool = cast(DatabasePool, FakePool(fake_conn))
    with pytest.raises(ExecutionDomainError):
        update_execution_status(
            pool,
            execution_id=uuid.uuid4(),
            expected_status="dispatching",  # type: ignore[arg-type]
            next_status=ExecutionStatus.RUNNING,
        )
    with pytest.raises(ExecutionDomainError):
        update_execution_status(
            pool,
            execution_id=uuid.uuid4(),
            expected_status=ExecutionStatus.RUNNING,
            next_status=ExecutionStatus.RUNNING,
        )
    assert fake_conn.executed == []


def test_the_module_surface_is_history_shaped() -> None:
    assert set(executions_module.__all__) == {
        "create_execution",
        "get_execution",
        "list_task_executions",
        "update_execution_status",
    }
