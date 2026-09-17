"""Deterministic mapping tests for Task agent session repositories.

The ordinary suite cannot execute Postgres; these tests use canned rows and a
fake pool/connection seam (mirroring the transaction-boundary and ownership
mapping fakes) to prove row-to-domain-object mapping, UTC normalization at the
persistence boundary, parameterization, the idempotent establishment SQL and
its reuse path, the conditional initialization/transition SQL shapes, the
Jsonb snapshot adapter, and empty-result handling. Database constraint
behavior is proven against a real database by the integration-marked suite in
``tests/integration/``.
"""

from __future__ import annotations

import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta, timezone
from typing import Any, cast

import pytest
from psycopg.types.json import Jsonb

from openorc.domain.connections import WorkflowRole
from openorc.domain.sessions import TaskAgentSessionDomainError, TaskSessionLifecycleStatus
from openorc.persistence.pool import DatabasePool
from openorc.persistence.sessions import (
    ensure_task_agent_session,
    get_task_agent_session,
    initialize_task_agent_session,
    list_active_task_agent_sessions,
    mark_task_agent_session_ended,
    mark_task_agent_session_lost,
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
    """Records executed SQL and returns canned rows, optionally per statement."""

    def __init__(
        self,
        row: tuple[Any, ...] | None = None,
        rows: list[tuple[Any, ...]] | None = None,
        responses: list[tuple[Any, ...] | None] | None = None,
    ) -> None:
        self.row = row
        self.rows = rows
        # When supplied, each execute() consumes the next canned response —
        # this models multi-statement repositories such as the idempotent
        # establishment path (conflicting insert, then reuse read).
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


def _session_row(**overrides: Any) -> tuple[Any, ...]:
    """Build one canned TaskAgentSession row (domain-valid by default)."""
    values: dict[str, Any] = {
        "id": uuid.uuid4(),
        "workspace_id": uuid.uuid4(),
        "task_id": uuid.uuid4(),
        "role": "producer",
        "connection_id": uuid.uuid4(),
        "external_session_id": None,
        "lifecycle_status": "connecting",
        "initialization_protocol_version": None,
        "effective_config_snapshot": None,
        "reported_provider": None,
        "reported_model": None,
        "reported_runtime_version": None,
        "initialized_at": None,
        "ended_at": None,
        "created_at": _observed_at(),
        "updated_at": _observed_at(),
    }
    values.update(overrides)
    return (
        values["id"],
        values["workspace_id"],
        values["task_id"],
        values["role"],
        values["connection_id"],
        values["external_session_id"],
        values["lifecycle_status"],
        values["initialization_protocol_version"],
        values["effective_config_snapshot"],
        values["reported_provider"],
        values["reported_model"],
        values["reported_runtime_version"],
        values["initialized_at"],
        values["ended_at"],
        values["created_at"],
        values["updated_at"],
    )


def test_ensure_task_agent_session_inserts_and_maps_connecting_row() -> None:
    row = _session_row()
    fake_conn = FakeConnection(row)
    pool = cast(DatabasePool, FakePool(fake_conn))

    session = ensure_task_agent_session(
        pool,
        workspace_id=row[1],
        task_id=row[2],
        role=WorkflowRole.PRODUCER,
        connection_id=row[4],
    )

    assert session.id == row[0]
    assert session.workspace_id == row[1]
    assert session.task_id == row[2]
    assert session.role is WorkflowRole.PRODUCER
    assert session.connection_id == row[4]
    assert session.external_session_id is None
    assert session.lifecycle_status is TaskSessionLifecycleStatus.CONNECTING
    assert session.initialized_at is None
    assert session.ended_at is None
    assert session.created_at == _utc_observed_at()
    assert session.created_at.utcoffset() == timedelta(0)

    sql, params = fake_conn.executed[0]
    assert "openorc.task_agent_sessions" in sql
    assert "on conflict (task_id, role) do nothing" in sql
    assert params is not None
    assert params == (row[1], row[2], "producer", row[4])
    # One statement: the fresh insert returned the new row directly.
    assert len(fake_conn.executed) == 1


def test_ensure_task_agent_session_reuses_the_existing_binding_idempotently() -> None:
    existing = _session_row(role="reviewer")
    fake_conn = FakeConnection(responses=[None, existing])
    pool = cast(DatabasePool, FakePool(fake_conn))

    session = ensure_task_agent_session(
        pool,
        workspace_id=existing[1],
        task_id=existing[2],
        role=WorkflowRole.REVIEWER,
        connection_id=existing[4],
    )

    assert session.id == existing[0]
    assert session.role is WorkflowRole.REVIEWER
    assert session.lifecycle_status is TaskSessionLifecycleStatus.CONNECTING
    # Two statements: the insert conflicted, then the existing row was read
    # back for reuse — no second insert and no replacement row.
    assert len(fake_conn.executed) == 2
    reuse_sql, reuse_params = fake_conn.executed[1]
    assert "select" in reuse_sql
    assert "task_id = %s and role = %s" in reuse_sql
    assert reuse_params == (existing[2], "reviewer")


def test_ensure_task_agent_session_rejects_a_different_connection() -> None:
    existing = _session_row()
    fake_conn = FakeConnection(responses=[None, existing])
    pool = cast(DatabasePool, FakePool(fake_conn))

    with pytest.raises(TaskAgentSessionDomainError):
        ensure_task_agent_session(
            pool,
            workspace_id=existing[1],
            task_id=existing[2],
            role=WorkflowRole.PRODUCER,
            connection_id=uuid.uuid4(),
        )


def test_ensure_task_agent_session_rejects_a_different_workspace() -> None:
    existing = _session_row()
    fake_conn = FakeConnection(responses=[None, existing])
    pool = cast(DatabasePool, FakePool(fake_conn))

    with pytest.raises(TaskAgentSessionDomainError):
        ensure_task_agent_session(
            pool,
            workspace_id=uuid.uuid4(),
            task_id=existing[2],
            role=WorkflowRole.PRODUCER,
            connection_id=existing[4],
        )


def test_ensure_task_agent_session_requires_a_workflow_role() -> None:
    with pytest.raises(TaskAgentSessionDomainError):
        ensure_task_agent_session(
            cast(DatabasePool, FakePool(FakeConnection())),
            workspace_id=uuid.uuid4(),
            task_id=uuid.uuid4(),
            role="producer",  # type: ignore[arg-type]
            connection_id=uuid.uuid4(),
        )


def test_initialize_maps_the_ready_row_and_wraps_the_snapshot_in_jsonb() -> None:
    row = _session_row(
        external_session_id="ext-session-1",
        lifecycle_status="ready",
        initialization_protocol_version="1",
        effective_config_snapshot={"stage": "plan"},
        initialized_at=_observed_at(),
        updated_at=_observed_at(),
    )
    fake_conn = FakeConnection(row)
    pool = cast(DatabasePool, FakePool(fake_conn))

    session = initialize_task_agent_session(
        pool,
        task_id=row[2],
        role=WorkflowRole.PRODUCER,
        external_session_id="ext-session-1",
        initialization_protocol_version="1",
        effective_config_snapshot={"stage": "plan"},
    )

    assert session is not None
    assert session.lifecycle_status is TaskSessionLifecycleStatus.READY
    assert session.external_session_id == "ext-session-1"
    assert session.initialization_protocol_version == "1"
    assert dict(session.effective_config_snapshot) == {"stage": "plan"}  # type: ignore[arg-type]
    assert session.initialized_at == _utc_observed_at()
    assert session.ended_at is None

    sql, params = fake_conn.executed[0]
    assert "openorc.task_agent_sessions" in sql
    assert "lifecycle_status = 'ready'" in sql
    # The only external_session_id writer applies exclusively while the
    # binding is CONNECTING with a NULL identity: no replacement is possible.
    assert "where task_id = %s and role = %s" in sql
    assert "and lifecycle_status = 'connecting' and external_session_id is null" in sql
    assert "initialized_at = now()" in sql
    assert params is not None
    assert params[0] == "ext-session-1"
    assert params[1] == "1"
    # psycopg 3 does not adapt plain mappings to jsonb without an explicit
    # wrapper: the repository must supply the Jsonb adapter itself.
    assert isinstance(params[2], Jsonb)
    assert params[2].obj == {"stage": "plan"}
    assert params[3:6] == (None, None, None)
    assert params[6:8] == (row[2], "producer")


def test_initialize_with_an_empty_snapshot_wraps_jsonb_empty_object() -> None:
    # An empty JSON object is a valid effective configuration snapshot when
    # no concrete configurable values exist; NULL is not — initialization
    # facts move atomically with the bound identity.
    row = _session_row(
        external_session_id="ext-session-2",
        lifecycle_status="ready",
        initialization_protocol_version="1",
        effective_config_snapshot={},
        initialized_at=_observed_at(),
        updated_at=_observed_at(),
    )
    fake_conn = FakeConnection(row)
    pool = cast(DatabasePool, FakePool(fake_conn))

    session = initialize_task_agent_session(
        pool,
        task_id=row[2],
        role=WorkflowRole.REVIEWER,
        external_session_id="ext-session-2",
        initialization_protocol_version="1",
        effective_config_snapshot={},
    )

    assert session is not None
    assert dict(session.effective_config_snapshot) == {}  # type: ignore[arg-type]
    _, params = fake_conn.executed[0]
    assert params is not None
    assert isinstance(params[2], Jsonb)
    assert params[2].obj == {}


def test_initialize_requires_the_initialization_facts() -> None:
    # The initialization facts move atomically: a missing protocol version or
    # snapshot is rejected at the boundary, never stored as a partially
    # initialized READY row. The reported provenance fields stay optional.
    pool = cast(DatabasePool, FakePool(FakeConnection()))
    with pytest.raises(TaskAgentSessionDomainError):
        initialize_task_agent_session(
            pool,
            task_id=uuid.uuid4(),
            role=WorkflowRole.PRODUCER,
            external_session_id="ext-session-1",
            initialization_protocol_version=None,  # type: ignore[arg-type]
            effective_config_snapshot={},
        )
    with pytest.raises(TaskAgentSessionDomainError):
        initialize_task_agent_session(
            pool,
            task_id=uuid.uuid4(),
            role=WorkflowRole.PRODUCER,
            external_session_id="ext-session-1",
            initialization_protocol_version="1",
            effective_config_snapshot=None,  # type: ignore[arg-type]
        )


@pytest.mark.parametrize("bad_identity", ["", "   "])
def test_initialize_rejects_blank_external_session_ids(bad_identity: str) -> None:
    with pytest.raises(TaskAgentSessionDomainError):
        initialize_task_agent_session(
            cast(DatabasePool, FakePool(FakeConnection())),
            task_id=uuid.uuid4(),
            role=WorkflowRole.PRODUCER,
            external_session_id=bad_identity,
            initialization_protocol_version="1",
            effective_config_snapshot={},
        )


def test_initialize_rejects_blank_protocol_versions() -> None:
    with pytest.raises(TaskAgentSessionDomainError):
        initialize_task_agent_session(
            cast(DatabasePool, FakePool(FakeConnection())),
            task_id=uuid.uuid4(),
            role=WorkflowRole.PRODUCER,
            external_session_id="ext-session-1",
            initialization_protocol_version="   ",
            effective_config_snapshot={},
        )


def test_mark_lost_applies_only_from_ready_and_preserves_the_identity() -> None:
    row = _session_row(
        external_session_id="ext-session-1",
        lifecycle_status="lost",
        initialization_protocol_version="1",
        effective_config_snapshot={"stage": "plan"},
        initialized_at=_observed_at(),
        updated_at=_observed_at(),
    )
    fake_conn = FakeConnection(row)
    pool = cast(DatabasePool, FakePool(fake_conn))

    session = mark_task_agent_session_lost(pool, task_id=row[2], role=WorkflowRole.PRODUCER)

    assert session is not None
    assert session.lifecycle_status is TaskSessionLifecycleStatus.LOST
    # LOST is lifecycle state on the SAME binding: the identity is preserved.
    assert session.external_session_id == "ext-session-1"
    assert session.initialized_at == _utc_observed_at()
    assert session.ended_at is None
    sql, params = fake_conn.executed[0]
    assert "set lifecycle_status = 'lost'" in sql
    # CONNECTING is not a bound session that can be lost; LOST is absorbing.
    assert "and lifecycle_status = 'ready'" in sql
    assert params == (row[2], "producer")

    assert (
        mark_task_agent_session_lost(
            cast(DatabasePool, FakePool(FakeConnection(None))),
            task_id=row[2],
            role=WorkflowRole.PRODUCER,
        )
        is None
    )


def test_mark_ended_stamps_the_semantic_timestamp() -> None:
    row = _session_row(
        external_session_id="ext-session-1",
        lifecycle_status="ended",
        initialization_protocol_version="1",
        effective_config_snapshot={"stage": "plan"},
        initialized_at=_observed_at(),
        ended_at=_observed_at(),
        updated_at=_observed_at(),
    )
    fake_conn = FakeConnection(row)
    pool = cast(DatabasePool, FakePool(fake_conn))

    session = mark_task_agent_session_ended(pool, task_id=row[2], role=WorkflowRole.REVIEWER)

    assert session is not None
    assert session.lifecycle_status is TaskSessionLifecycleStatus.ENDED
    assert session.ended_at == _utc_observed_at()
    sql, params = fake_conn.executed[0]
    # ended_at is stamped atomically with the status, from CONNECTING or READY.
    assert "set lifecycle_status = 'ended', ended_at = now()" in sql
    assert "and lifecycle_status in ('connecting', 'ready')" in sql
    assert params == (row[2], "reviewer")

    # ENDED is absorbing: a repeated transition is a rejected no-op.
    assert (
        mark_task_agent_session_ended(
            cast(DatabasePool, FakePool(FakeConnection(None))),
            task_id=row[2],
            role=WorkflowRole.REVIEWER,
        )
        is None
    )


def test_get_task_agent_session_maps_row_or_none() -> None:
    row = _session_row()
    found = get_task_agent_session(
        cast(DatabasePool, FakePool(FakeConnection(row))),
        task_id=row[2],
        role=WorkflowRole.PRODUCER,
    )
    assert found is not None
    assert found.id == row[0]
    assert found.created_at == _utc_observed_at()
    assert (
        get_task_agent_session(
            cast(DatabasePool, FakePool(FakeConnection(None))),
            task_id=row[2],
            role=WorkflowRole.PRODUCER,
        )
        is None
    )


def test_list_active_task_agent_sessions_maps_rows_and_filters_active() -> None:
    rows = [
        _session_row(),
        _session_row(
            role="reviewer",
            external_session_id="ext-session-2",
            lifecycle_status="ready",
            initialization_protocol_version="1",
            effective_config_snapshot={"stage": "plan"},
            initialized_at=_observed_at(),
            updated_at=_observed_at(),
        ),
    ]
    fake_conn = FakeConnection(None, rows)
    listed = list_active_task_agent_sessions(
        cast(DatabasePool, FakePool(fake_conn)), connection_id=rows[0][4]
    )
    assert [session.id for session in listed] == [rows[0][0], rows[1][0]]
    sql, params = fake_conn.executed[0]
    # Connection-scoped occupancy accounting covers exactly the active states:
    # Producer and Reviewer sessions on one Connection both count.
    assert "and lifecycle_status in ('connecting', 'ready')" in sql
    assert params == (rows[0][4],)
    assert (
        list_active_task_agent_sessions(
            cast(DatabasePool, FakePool(FakeConnection(None, []))),
            connection_id=rows[0][4],
        )
        == []
    )
