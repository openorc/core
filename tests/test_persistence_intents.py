"""Deterministic mapping tests for the one-shot intent repositories (issue #228).

The ordinary suite cannot execute Postgres; these tests use canned rows and
a fake pool/connection seam (mirroring the blocks/planning mapping fakes) to
prove row-to-domain-object mapping, UTC normalization, parameterization, the
canonical-JSON outcome write through the explicit ``Jsonb`` adapter, the
forward-only outcome transitions (an unsettled intent advances exactly once
to a terminal record; an in-place annotation keeps it unsettled), and the
strictly unsettled/pending conditional guards. Database constraint behavior
(partial unique index, ``unique (task_id)``) is proven against a real
database by the integration-marked suite in ``tests/integration/``.
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
from openorc.domain.intents import (
    TaskInterruptIntentStatus,
    TaskPullRequestCloseIntentStatus,
)
from openorc.persistence.intents import (
    annotate_unsettled_task_interrupt_intent,
    create_task_interrupt_intent,
    create_task_pull_request_close_intent,
    get_task_interrupt_intent,
    get_task_pull_request_close_intent_for_task,
    list_task_interrupt_intents,
    list_unsettled_task_interrupt_intents,
    record_task_interrupt_intent_outcome,
    record_task_pull_request_close_intent_outcome,
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
    return datetime(2026, 10, 23, 9, 30, 0, tzinfo=timezone(timedelta(hours=-5)))


def _utc_observed_at() -> datetime:
    return _observed_at().astimezone(UTC)


_WORKSPACE_ID = uuid.uuid4()
_TASK_ID = uuid.uuid4()
_SESSION_ID = uuid.uuid4()
_OWNER_PROFILE_ID = uuid.uuid4()
_PR_RECORD_ID = uuid.uuid4()


def _intent_row(**overrides: Any) -> tuple[Any, ...]:
    values: dict[str, Any] = {
        "id": uuid.uuid4(),
        "workspace_id": _WORKSPACE_ID,
        "task_id": _TASK_ID,
        "task_agent_session_id": _SESSION_ID,
        "workflow_role": "producer",
        "external_session_id": "external-session-1",
        "requested_by_profile_id": _OWNER_PROFILE_ID,
        "status": "unsettled",
        "outcome": {},
        "created_at": _observed_at(),
        "updated_at": _observed_at(),
    }
    values.update(overrides)
    return (
        values["id"],
        values["workspace_id"],
        values["task_id"],
        values["task_agent_session_id"],
        values["workflow_role"],
        values["external_session_id"],
        values["requested_by_profile_id"],
        values["status"],
        values["outcome"],
        values["created_at"],
        values["updated_at"],
    )


def _close_intent_row(**overrides: Any) -> tuple[Any, ...]:
    values: dict[str, Any] = {
        "id": uuid.uuid4(),
        "workspace_id": _WORKSPACE_ID,
        "task_id": _TASK_ID,
        "task_pull_request_id": _PR_RECORD_ID,
        "github_pr_id": 900_719_925_474_099,
        "github_pr_number": 7,
        "requested_by_profile_id": _OWNER_PROFILE_ID,
        "status": "pending",
        "outcome": {},
        "created_at": _observed_at(),
        "updated_at": _observed_at(),
    }
    values.update(overrides)
    return (
        values["id"],
        values["workspace_id"],
        values["task_id"],
        values["task_pull_request_id"],
        values["github_pr_id"],
        values["github_pr_number"],
        values["requested_by_profile_id"],
        values["status"],
        values["outcome"],
        values["created_at"],
        values["updated_at"],
    )


def test_create_task_interrupt_intent_inserts_unsettled_and_maps_the_row() -> None:
    row = _intent_row()
    fake_conn = FakeConnection(row)
    pool = cast(DatabasePool, FakePool(fake_conn))

    intent = create_task_interrupt_intent(
        pool,
        workspace_id=_WORKSPACE_ID,
        task_id=_TASK_ID,
        task_agent_session_id=_SESSION_ID,
        workflow_role=WorkflowRole.PRODUCER,
        external_session_id="external-session-1",
        requested_by_profile_id=_OWNER_PROFILE_ID,
    )

    assert intent.status is TaskInterruptIntentStatus.UNSETTLED
    assert intent.workflow_role is WorkflowRole.PRODUCER
    assert intent.external_session_id == "external-session-1"
    assert intent.created_at == _utc_observed_at()
    sql, params = fake_conn.executed[0]
    assert params is not None
    assert "insert into openorc.task_interrupt_intents" in sql
    assert "'unsettled'" in sql
    assert params == (
        _WORKSPACE_ID,
        _TASK_ID,
        _SESSION_ID,
        "producer",
        "external-session-1",
        _OWNER_PROFILE_ID,
    )


def test_interrupt_intent_reads_map_rows_and_normalize_utc() -> None:
    rows = [_intent_row(), _intent_row(status="settled", outcome={"dispatch": "acknowledged"})]
    fake_conn = FakeConnection(None, rows)
    pool = cast(DatabasePool, FakePool(fake_conn))

    listed = list_task_interrupt_intents(pool, task_id=_TASK_ID)

    assert [intent.status for intent in listed] == [
        TaskInterruptIntentStatus.UNSETTLED,
        TaskInterruptIntentStatus.SETTLED,
    ]
    assert listed[1].outcome["dispatch"] == "acknowledged"
    assert listed[0].updated_at == _utc_observed_at()
    sql, _ = fake_conn.executed[0]
    assert "from openorc.task_interrupt_intents where task_id = %s" in sql


def test_list_unsettled_interrupt_intents_filters_the_unsettled_status() -> None:
    rows = [_intent_row()]
    fake_conn = FakeConnection(None, rows)
    pool = cast(DatabasePool, FakePool(fake_conn))

    listed = list_unsettled_task_interrupt_intents(pool, task_id=_TASK_ID)

    assert len(listed) == 1
    sql, _ = fake_conn.executed[0]
    assert "status = 'unsettled'" in sql


def test_get_task_interrupt_intent_maps_or_returns_none() -> None:
    intent_id = uuid.uuid4()
    pool = cast(DatabasePool, FakePool(FakeConnection(_intent_row())))
    assert get_task_interrupt_intent(pool, task_interrupt_intent_id=intent_id) is not None
    empty = cast(DatabasePool, FakePool(FakeConnection(None)))
    assert get_task_interrupt_intent(empty, task_interrupt_intent_id=intent_id) is None


def test_annotate_unsettled_interrupt_intent_updates_outcome_in_place() -> None:
    row = _intent_row(outcome={"dispatch": "acknowledged"})
    fake_conn = FakeConnection(row)
    pool = cast(DatabasePool, FakePool(fake_conn))

    annotated = annotate_unsettled_task_interrupt_intent(
        pool,
        task_interrupt_intent_id=row[0],
        outcome={"dispatch": "acknowledged"},
    )

    assert annotated is not None
    assert annotated.status is TaskInterruptIntentStatus.UNSETTLED
    sql, params = fake_conn.executed[0]
    assert params is not None
    assert "set outcome = %s, updated_at = now()" in sql
    assert "status = 'unsettled'" in sql
    assert isinstance(params[0], Jsonb)


def test_record_interrupt_intent_outcome_advances_unsettled_once_to_terminal() -> None:
    row = _intent_row(status="unknown", outcome={"dispatch": "uncertain"})
    fake_conn = FakeConnection(row)
    pool = cast(DatabasePool, FakePool(fake_conn))

    recorded = record_task_interrupt_intent_outcome(
        pool,
        task_interrupt_intent_id=row[0],
        status=TaskInterruptIntentStatus.UNKNOWN,
        outcome={"dispatch": "uncertain"},
    )

    assert recorded is not None
    assert recorded.status is TaskInterruptIntentStatus.UNKNOWN
    sql, params = fake_conn.executed[0]
    assert params is not None
    assert "set status = %s, outcome = %s" in sql
    assert "where id = %s and status = 'unsettled'" in sql
    assert params[0] == "unknown"
    assert isinstance(params[1], Jsonb)


def test_record_interrupt_intent_outcome_rejects_a_nonterminal_target() -> None:
    fake_conn = FakeConnection(None)
    pool = cast(DatabasePool, FakePool(fake_conn))

    with pytest.raises(ValueError, match="terminal"):
        record_task_interrupt_intent_outcome(
            pool,
            task_interrupt_intent_id=uuid.uuid4(),
            status=TaskInterruptIntentStatus.UNSETTLED,
            outcome={},
        )

    assert fake_conn.executed == []


def test_create_close_intent_inserts_pending_and_maps_the_row() -> None:
    row = _close_intent_row()
    fake_conn = FakeConnection(row)
    pool = cast(DatabasePool, FakePool(fake_conn))

    intent = create_task_pull_request_close_intent(
        pool,
        workspace_id=_WORKSPACE_ID,
        task_id=_TASK_ID,
        task_pull_request_id=_PR_RECORD_ID,
        github_pr_id=900_719_925_474_099,
        github_pr_number=7,
        requested_by_profile_id=_OWNER_PROFILE_ID,
    )

    assert intent.status is TaskPullRequestCloseIntentStatus.PENDING
    assert intent.github_pr_id == 900_719_925_474_099
    assert intent.github_pr_number == 7
    sql, params = fake_conn.executed[0]
    assert params is not None
    assert "insert into openorc.task_pull_request_close_intents" in sql
    assert "'pending'" in sql
    assert params == (
        _WORKSPACE_ID,
        _TASK_ID,
        _PR_RECORD_ID,
        900_719_925_474_099,
        7,
        _OWNER_PROFILE_ID,
    )


def test_get_close_intent_for_task_maps_or_returns_none() -> None:
    pool = cast(DatabasePool, FakePool(FakeConnection(_close_intent_row())))
    intent = get_task_pull_request_close_intent_for_task(pool, task_id=_TASK_ID)

    assert intent is not None
    assert intent.status is TaskPullRequestCloseIntentStatus.PENDING
    empty = cast(DatabasePool, FakePool(FakeConnection(None)))
    assert get_task_pull_request_close_intent_for_task(empty, task_id=_TASK_ID) is None


def test_record_close_intent_outcome_advances_pending_once_to_terminal() -> None:
    row = _close_intent_row(
        status="close_requested", outcome={"confirmed": True, "observed_state": "closed_unmerged"}
    )
    fake_conn = FakeConnection(row)
    pool = cast(DatabasePool, FakePool(fake_conn))

    recorded = record_task_pull_request_close_intent_outcome(
        pool,
        task_pull_request_close_intent_id=row[0],
        status=TaskPullRequestCloseIntentStatus.CLOSE_REQUESTED,
        outcome={"confirmed": True, "observed_state": "closed_unmerged"},
    )

    assert recorded is not None
    assert recorded.status is TaskPullRequestCloseIntentStatus.CLOSE_REQUESTED
    sql, params = fake_conn.executed[0]
    assert params is not None
    assert "where id = %s and status = 'pending'" in sql
    assert params[0] == "close_requested"
    assert isinstance(params[1], Jsonb)


def test_record_close_intent_outcome_rejects_a_nonterminal_target() -> None:
    fake_conn = FakeConnection(None)
    pool = cast(DatabasePool, FakePool(fake_conn))

    with pytest.raises(ValueError, match="terminal"):
        record_task_pull_request_close_intent_outcome(
            pool,
            task_pull_request_close_intent_id=uuid.uuid4(),
            status=TaskPullRequestCloseIntentStatus.PENDING,
            outcome={},
        )

    assert fake_conn.executed == []


def test_intent_commands_reject_malformed_shapes_before_any_sql() -> None:
    fake_conn = FakeConnection(None)
    pool = cast(DatabasePool, FakePool(fake_conn))

    with pytest.raises(ValueError):
        create_task_interrupt_intent(
            pool,
            workspace_id="not-a-uuid",  # type: ignore[arg-type]
            task_id=_TASK_ID,
            task_agent_session_id=_SESSION_ID,
            workflow_role=WorkflowRole.PRODUCER,
            external_session_id="external-session-1",
            requested_by_profile_id=_OWNER_PROFILE_ID,
        )
    with pytest.raises(ValueError):
        create_task_pull_request_close_intent(
            pool,
            workspace_id=_WORKSPACE_ID,
            task_id=_TASK_ID,
            task_pull_request_id=_PR_RECORD_ID,
            github_pr_id=0,
            github_pr_number=7,
            requested_by_profile_id=_OWNER_PROFILE_ID,
        )
    with pytest.raises(ValueError):
        create_task_pull_request_close_intent(
            pool,
            workspace_id=_WORKSPACE_ID,
            task_id=_TASK_ID,
            task_pull_request_id=_PR_RECORD_ID,
            github_pr_id=900_719_925_474_099,
            github_pr_number=-1,
            requested_by_profile_id=_OWNER_PROFILE_ID,
        )

    assert fake_conn.executed == []
