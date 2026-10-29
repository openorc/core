"""Deterministic tests for the E5 implementation-authorization boundary (issue #80).

A scripted SQL-connection seam proves the three database-only E5 seams on
their critical paths:

- both accepted shapes take ONE gate-presentation path: ordinary E3b
  ``ACCEPTED_CLEAR`` (Task ``planning``, no gate) and mediated E3c
  ``SETTLED_ACCEPTED`` (Task ``waiting_for_owner`` with the settled gate's
  pointer cleared) each create exactly one pending exact-plan
  IMPLEMENTATION_AUTHORIZATION gate, install it current, rotate the token,
  move a still-``planning`` Task to ``waiting_for_owner`` (E3c needs no
  status write), and record the safe OPENORC ``owner_gate_created`` and
  ``plan_ready`` facts — with no new Reviewer iteration, no gate rewrite,
  and no loop transition;
- clearance is derived from canonical state only: a trailing unresolved
  attempt, a newer change request (the #219-retired gate state), a moved
  plan, cross-scope evidence, a missing OPEN planning loop, a wrong
  pre-gate state, an existing current gate, or a stale token refuses the
  presentation without any write; after a #219 handoff, a NEW distinct
  Reviewer-accepted revision earns its own NEW gate;
- Owner approval closes the still-OPEN planning loop, stamps the gate
  APPROVED with truthful OWNER provenance, moves the Task to
  ``implementing`` under a rotated token, and records
  ``owner_gate_resolved`` (approved) plus ``implementation_authorized`` —
  a forced late failure leaves no partial facts, and a replay after
  success (rotated token) is stale;
- Owner cancellation resolves the exact pending gate CANCELLED (never
  REJECTED) with OWNER provenance and archives the Task with its safe
  ``task_cancelled`` event in one composition; a cancelled Task can never
  be approved afterwards.

E4 discussion owns its own seam and mutates nothing here; E5 reads no
discussion state, so a discussed-then-approved gate is this same approval
composition. No live GitHub, runtime, or Postgres access; the new SQL
composition/cross-record seams are proven by the integration-marked suite
against a real Postgres.
"""

from __future__ import annotations

import uuid
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from datetime import UTC, datetime
from typing import Any, cast

import pytest

from openorc.domain.gates import OwnerGateStatus, OwnerGateType
from openorc.domain.reviews import ReviewLoopStatus, ReviewOutcome
from openorc.domain.tasks import TaskStatus
from openorc.persistence.pool import DatabasePool
from openorc.services.errors import (
    ConflictError,
    InvalidCommandError,
    NotFoundError,
    StaleOperationError,
)
from openorc.services.task_implementation_authorization import (
    approve_implementation_authorization,
    cancel_implementation_authorization,
    open_implementation_authorization,
)

_OBSERVED = datetime(2026, 10, 20, 12, 0, 0, tzinfo=UTC)
_WORKSPACE_ID = uuid.uuid4()
_OWNER_PROFILE_ID = uuid.uuid4()
_TASK_ID = uuid.uuid4()
_REPOSITORY_ID = uuid.uuid4()
_STATE_TOKEN = uuid.uuid4()
_TOKEN_2 = uuid.uuid4()
_TOKEN_3 = uuid.uuid4()
_REVISION_1_ID = uuid.uuid4()
_REVISION_2_ID = uuid.uuid4()
_LOOP_ID = uuid.uuid4()
_ITERATION_1_ID = uuid.uuid4()
_ITERATION_2_ID = uuid.uuid4()
_ITERATION_3_ID = uuid.uuid4()
_SETTLED_GATE_ID = uuid.uuid4()
_GATE_1_ID = uuid.uuid4()
_BASE_SHA = "c" * 40
_PLAN_TEXT = "# Plan\n\nStep one.\n"

_CannedResult = tuple[Any, ...] | list[tuple[Any, ...]] | None | Exception
_Predicate = Callable[[str, tuple[Any, ...] | None], bool]


def _sql_has(*tokens: str) -> _Predicate:
    def predicate(sql: str, _params: tuple[Any, ...] | None) -> bool:
        return all(token in sql for token in tokens)

    return predicate


def _sql_lacks(predicate: _Predicate, *tokens: str) -> _Predicate:
    def wrapped(sql: str, params: tuple[Any, ...] | None) -> bool:
        return predicate(sql, params) and not any(token in sql for token in tokens)

    return wrapped


class FakeCursor:
    def __init__(self, row: tuple[Any, ...] | None, rows: list[tuple[Any, ...]] | None) -> None:
        self._row = row
        self._rows = rows if rows is not None else ([] if row is None else [row])

    def fetchone(self) -> tuple[Any, ...] | None:
        return self._row

    def fetchall(self) -> list[tuple[Any, ...]]:
        return list(self._rows)


class ScriptedConnection:
    """Routes each execute to the first matching scripted handler.

    Handlers are consumed in registration order unless registered as
    repeating, so same-shaped reads with different scripted outcomes script
    in call order.
    """

    def __init__(self) -> None:
        self.executed: list[tuple[str, tuple[Any, ...] | None]] = []
        self._handlers: list[tuple[_Predicate, _CannedResult, bool]] = []

    def on(self, predicate: _Predicate, result: _CannedResult) -> None:
        self._handlers.append((predicate, result, True))

    def on_repeat(self, predicate: _Predicate, result: _CannedResult) -> None:
        self._handlers.append((predicate, result, False))

    def execute(self, sql: str, params: tuple[Any, ...] | None = None) -> FakeCursor:
        self.executed.append((sql, params))
        lowered = " ".join(sql.split()).lower()
        for index, (predicate, result, once) in enumerate(self._handlers):
            if predicate(lowered, params):
                if once:
                    del self._handlers[index]
                if isinstance(result, Exception):
                    raise result
                if isinstance(result, list):
                    return FakeCursor(None, result)
                if isinstance(result, tuple):
                    return FakeCursor(result, None)
                return FakeCursor(None, None)
        raise AssertionError(f"no scripted handler matched: {sql}")

    @contextmanager
    def transaction(self) -> Iterator[None]:
        # Nested psycopg transaction blocks (SAVEPOINTs under composition).
        yield


class FakePool:
    """Pool fake tracking checked-out connections for the transaction proof."""

    def __init__(self, conn: ScriptedConnection) -> None:
        self._conn = conn
        self.active_connections = 0

    def connection(self) -> Any:
        @contextmanager
        def managed() -> Iterator[ScriptedConnection]:
            self.active_connections += 1
            try:
                yield self._conn
            finally:
                self.active_connections -= 1

        return managed()

    def close(self) -> None:
        raise AssertionError("implementation authorization tests never close pools")


def _pool(conn: ScriptedConnection) -> DatabasePool:
    return cast(DatabasePool, FakePool(conn))


_TASK_READ = _sql_lacks(_sql_has("from openorc.tasks where id"), "for update")
_TASK_READ_LOCKED = _sql_has("from openorc.tasks where id", "for update")
_TASK_GATE_WRITE = _sql_has("update openorc.tasks", "current_owner_gate_id")
_STATUS_UPDATE = _sql_has("update openorc.tasks", "queued_at = case")
_ARCHIVE_WRITE = _sql_has("update openorc.tasks", "archived_at = now()")
_REVISION_READ = _sql_has("from openorc.plan_revisions where id")
_LOOPS_LIST = _sql_has("from openorc.review_loops where task_id")
_INTENTS_LIST = _sql_has("from openorc.task_interrupt_intents")
_EXECUTIONS_LIST = _sql_has("from openorc.executions where task_id")
_RUNTIME_REQUESTS_LIST = _sql_has("from openorc.runtime_requests where task_id")
_LOOP_READ_LOCKED = _sql_lacks(
    _sql_has("from openorc.review_loops where id", "for update"), "select status, purpose"
)
_LOOP_UPDATE = _sql_has("update openorc.review_loops")
_ITERATIONS_LIST = _sql_has("review_iterations where review_loop_id")
_GATES_INSERT = _sql_has("insert into openorc.owner_gates")
_GATE_READ = _sql_has("from openorc.owner_gates where id")
_GATE_UPDATE = _sql_has("update openorc.owner_gates")
_EVENT_INSERT = _sql_has("insert into openorc.workflow_events")


def _ws_row() -> tuple[Any, ...]:
    return (_WORKSPACE_ID, _OWNER_PROFILE_ID, "platform", _OBSERVED, _OBSERVED, 2, "", None)


def _task_row(
    *,
    status: str = "planning",
    state_token: Any = _STATE_TOKEN,
    current_plan_revision_id: Any = _REVISION_1_ID,
    gate_id: Any = None,
    archived: bool = False,
) -> tuple[Any, ...]:
    return (
        _TASK_ID,
        _WORKSPACE_ID,
        _REPOSITORY_ID,
        503,
        42,
        status,
        _OBSERVED if archived else None,
        None,
        state_token,
        current_plan_revision_id,
        gate_id,
        "a" * 64,
        None,
        _OBSERVED,
        _OBSERVED,
        None,
        None,
    )


def _revision_row(
    *, revision_id: Any, number: int, content: str = _PLAN_TEXT, workspace_id: Any = _WORKSPACE_ID
) -> tuple[Any, ...]:
    return (revision_id, workspace_id, _TASK_ID, number, content, _BASE_SHA, _OBSERVED)


def _loop_row(*, status: str = "open", closed_at: Any = None) -> tuple[Any, ...]:
    return (_LOOP_ID, _WORKSPACE_ID, _TASK_ID, "planning", 2, status, closed_at, _OBSERVED)


def _iteration_row(
    *,
    iteration_id: Any,
    number: int,
    plan_revision_id: Any = _REVISION_1_ID,
    outcome: str | None = None,
    summary: str | None = None,
    findings: Any = None,
    decided_at: Any = None,
    workspace_id: Any = _WORKSPACE_ID,
) -> tuple[Any, ...]:
    return (
        iteration_id,
        workspace_id,
        _TASK_ID,
        _LOOP_ID,
        number,
        plan_revision_id,
        None,
        None,
        outcome,
        summary,
        findings,
        decided_at,
        _OBSERVED,
    )


def _gate_row(
    *,
    gate_id: Any = _GATE_1_ID,
    gate_type: str = "implementation_authorization",
    plan_revision_id: Any = _REVISION_1_ID,
    status: str = "pending",
) -> tuple[Any, ...]:
    return (
        gate_id,
        _WORKSPACE_ID,
        _TASK_ID,
        gate_type,
        status,
        plan_revision_id,
        None,
        None,
        None,
        _OBSERVED if status != "pending" else None,
        _OBSERVED,
    )


def _event_row() -> tuple[Any, ...]:
    return (
        uuid.uuid4(),
        _WORKSPACE_ID,
        _TASK_ID,
        "plan_ready",
        "openorc",
        None,
        None,
        None,
        {},
        _OBSERVED,
    )


def _event_types(conn: ScriptedConnection) -> list[str]:
    types: list[str] = []
    for sql, params in conn.executed:
        if _EVENT_INSERT(sql, None):
            assert params is not None
            types.append(cast(str, params[2]))
    return types


def _event_params_of_type(conn: ScriptedConnection, event_type: str) -> list[tuple[Any, ...]]:
    matched: list[tuple[Any, ...]] = []
    for sql, params in conn.executed:
        if _EVENT_INSERT(sql, None) and params is not None and params[2] == event_type:
            matched.append(params)
    return matched


def _count(conn: ScriptedConnection, predicate: _Predicate) -> int:
    return sum(1 for sql, _params in conn.executed if predicate(sql, None))


def _accepted_iteration(
    iteration_id: Any = _ITERATION_1_ID, *, number: int = 1, plan_revision_id: Any = _REVISION_1_ID
) -> tuple[Any, ...]:
    return _iteration_row(
        iteration_id=iteration_id,
        number=number,
        plan_revision_id=plan_revision_id,
        outcome="accepted",
        summary="Looks good",
        findings=[],
        decided_at=_OBSERVED,
    )


def _changes_iteration(
    iteration_id: Any = _ITERATION_2_ID, *, number: int = 2, plan_revision_id: Any = _REVISION_1_ID
) -> tuple[Any, ...]:
    return _iteration_row(
        iteration_id=iteration_id,
        number=number,
        plan_revision_id=plan_revision_id,
        outcome="changes_requested",
        summary="Needs another pass",
        findings=[{"summary": "Clarify the retry loop", "details": "Explain the backoff path."}],
        decided_at=_OBSERVED,
    )


# --- scripted sequences (in service call order) ---------------------------------


def _script_repeating_reads(conn: ScriptedConnection) -> None:
    """Same-shaped reads whose result never changes across the whole flow."""
    conn.on_repeat(_sql_has("from openorc.profiles", "for key share"), (None, None, None))
    conn.on_repeat(_sql_has("from openorc.workspaces where id"), _ws_row())
    conn.on_repeat(_EVENT_INSERT, _event_row())


def _script_presentation(
    conn: ScriptedConnection,
    *,
    task_row: tuple[Any, ...],
    revision_row: tuple[Any, ...],
    iterations: list[tuple[Any, ...]],
    loops: list[tuple[Any, ...]] | None = None,
    gate_row: tuple[Any, ...] | None = None,
    installed_task_row: tuple[Any, ...] | None = None,
    waiting_task_row: tuple[Any, ...] | None = None,
) -> None:
    """The gate-presentation transaction in call order.

    ``waiting_task_row=None`` scripts the E3c shape (the Task is already
    ``waiting_for_owner`` and no status write may happen); any unexpected
    extra statement fails the script with a handler-miss assertion.
    """
    _script_repeating_reads(conn)
    conn.on(_TASK_READ, task_row)  # workspace guard task read
    conn.on(_TASK_READ_LOCKED, task_row)  # get_task_for_update
    conn.on(_TASK_READ, task_row)  # require_current_task
    conn.on(_REVISION_READ, revision_row)  # require_current_plan_revision
    conn.on(_LOOPS_LIST, [_loop_row()] if loops is None else loops)
    conn.on(_LOOP_READ_LOCKED, _loop_row())
    conn.on(_ITERATIONS_LIST, iterations)
    conn.on(_GATES_INSERT, gate_row if gate_row is not None else _gate_row())
    conn.on(
        _TASK_GATE_WRITE,
        installed_task_row
        if installed_task_row is not None
        else _task_row(status="waiting_for_owner", state_token=_TOKEN_2, gate_id=_GATE_1_ID),
    )
    if waiting_task_row is not None:
        conn.on(_STATUS_UPDATE, waiting_task_row)


def _script_approval(
    conn: ScriptedConnection,
    *,
    task_row: tuple[Any, ...] | None = None,
    gate_row: tuple[Any, ...] | None = None,
    revision_row: tuple[Any, ...] | None = None,
    iterations: list[tuple[Any, ...]] | None = None,
    loops: list[tuple[Any, ...]] | None = None,
    fail_loop_close: bool = False,
) -> None:
    """The Owner approval transaction in call order."""
    _script_repeating_reads(conn)
    waiting = (
        task_row
        if task_row is not None
        else _task_row(status="waiting_for_owner", gate_id=_GATE_1_ID)
    )
    gate = gate_row if gate_row is not None else _gate_row()
    conn.on(_TASK_READ, waiting)  # workspace guard task read
    conn.on(_TASK_READ_LOCKED, waiting)  # get_task_for_update
    conn.on(_TASK_READ, waiting)  # require_current_task
    conn.on(_GATE_READ, gate)  # require_current_owner_gate
    conn.on(
        _REVISION_READ,
        revision_row
        if revision_row is not None
        else _revision_row(revision_id=_REVISION_1_ID, number=1),
    )
    conn.on(_LOOPS_LIST, [_loop_row()] if loops is None else loops)
    conn.on(_LOOP_READ_LOCKED, _loop_row())
    conn.on(_ITERATIONS_LIST, iterations if iterations is not None else [_accepted_iteration()])
    conn.on(_GATE_READ, gate)  # the resolution's gate read
    conn.on(_TASK_READ_LOCKED, waiting)  # the resolution's Task row lock
    conn.on(_GATE_UPDATE, _gate_row(gate_id=_GATE_1_ID, status="approved"))
    conn.on(
        _TASK_GATE_WRITE,
        _task_row(status="waiting_for_owner", state_token=_TOKEN_2, gate_id=None),
    )
    if fail_loop_close:
        conn.on(_LOOP_UPDATE, RuntimeError("forced late approval failure"))
    else:
        conn.on(_LOOP_UPDATE, _loop_row(status="closed", closed_at=_OBSERVED))
    conn.on(
        _STATUS_UPDATE,
        _task_row(status="implementing", state_token=_TOKEN_3, gate_id=None),
    )


def _script_cancellation(
    conn: ScriptedConnection,
    *,
    task_row: tuple[Any, ...] | None = None,
    gate_row: tuple[Any, ...] | None = None,
    fail_archive: bool = False,
) -> None:
    """The cancel-at-gate transaction in call order."""
    _script_repeating_reads(conn)
    waiting = (
        task_row
        if task_row is not None
        else _task_row(status="waiting_for_owner", gate_id=_GATE_1_ID)
    )
    gate = gate_row if gate_row is not None else _gate_row()
    conn.on(_TASK_READ, waiting)  # workspace guard task read
    conn.on(_TASK_READ_LOCKED, waiting)  # get_task_for_update
    conn.on(_TASK_READ, waiting)  # require_current_task
    conn.on(_INTENTS_LIST, [])  # the accounted-runtime veto reads
    conn.on(_EXECUTIONS_LIST, [])
    conn.on(_RUNTIME_REQUESTS_LIST, [])
    conn.on(_GATE_READ, gate)  # require_current_owner_gate
    conn.on(_GATE_READ, gate)  # the resolution's gate read
    conn.on(_TASK_READ_LOCKED, waiting)  # the resolution's Task row lock
    conn.on(_GATE_UPDATE, _gate_row(gate_id=_GATE_1_ID, status="cancelled"))
    conn.on(
        _TASK_GATE_WRITE,
        _task_row(status="waiting_for_owner", state_token=_TOKEN_2, gate_id=None),
    )
    conn.on(_LOOPS_LIST, [])  # no OPEN review loop to close at this gate
    if fail_archive:
        conn.on(_ARCHIVE_WRITE, RuntimeError("forced late cancellation failure"))
    else:
        conn.on(
            _ARCHIVE_WRITE,
            _task_row(status="cancelled", state_token=_TOKEN_3, gate_id=None, archived=True),
        )


def _open_call(pool: DatabasePool, *, expected_state_token: Any = _STATE_TOKEN) -> Any:
    return open_implementation_authorization(
        pool,
        profile_id=_OWNER_PROFILE_ID,
        workspace_id=_WORKSPACE_ID,
        task_id=_TASK_ID,
        expected_state_token=expected_state_token,
    )


def _approve_call(pool: DatabasePool, *, expected_state_token: Any = _STATE_TOKEN) -> Any:
    return approve_implementation_authorization(
        pool,
        profile_id=_OWNER_PROFILE_ID,
        workspace_id=_WORKSPACE_ID,
        task_id=_TASK_ID,
        owner_gate_id=_GATE_1_ID,
        expected_state_token=expected_state_token,
    )


def _cancel_call(pool: DatabasePool, *, expected_state_token: Any = _STATE_TOKEN) -> Any:
    return cancel_implementation_authorization(
        pool,
        profile_id=_OWNER_PROFILE_ID,
        workspace_id=_WORKSPACE_ID,
        task_id=_TASK_ID,
        owner_gate_id=_GATE_1_ID,
        expected_state_token=expected_state_token,
    )


# --- gate presentation -----------------------------------------------------------


def test_an_ordinary_acceptance_presents_the_exact_plan_gate() -> None:
    """E3b's ACCEPTED_CLEAR shape: the still-``planning`` Task gains exactly
    one pending exact-plan gate, installed current under a rotated token,
    moved to ``waiting_for_owner``, with the safe OPENORC audit facts."""
    conn = ScriptedConnection()
    _script_presentation(
        conn,
        task_row=_task_row(status="planning"),
        revision_row=_revision_row(revision_id=_REVISION_1_ID, number=1),
        iterations=[_accepted_iteration()],
        installed_task_row=_task_row(status="planning", state_token=_TOKEN_2, gate_id=_GATE_1_ID),
        waiting_task_row=_task_row(
            status="waiting_for_owner", state_token=_TOKEN_3, gate_id=_GATE_1_ID
        ),
    )

    outcome = _open_call(_pool(conn))

    assert outcome.task.status is TaskStatus.WAITING_FOR_OWNER
    assert outcome.task.state_token == _TOKEN_3
    assert outcome.task.current_owner_gate_id == _GATE_1_ID
    assert outcome.owner_gate.status is OwnerGateStatus.PENDING
    assert outcome.owner_gate.gate_type is OwnerGateType.IMPLEMENTATION_AUTHORIZATION
    assert outcome.owner_gate.plan_revision_id == _REVISION_1_ID
    assert outcome.owner_gate.decided_at is None
    assert outcome.plan_revision.id == _REVISION_1_ID
    assert outcome.review_iteration.outcome is ReviewOutcome.ACCEPTED
    assert outcome.review_iteration.plan_revision_id == _REVISION_1_ID
    assert outcome.review_loop.status is ReviewLoopStatus.OPEN
    # One new gate row, installed current; no rewrites, no loop transition.
    assert _count(conn, _GATES_INSERT) == 1
    (inserted_params,) = [params for sql, params in conn.executed if _GATES_INSERT(sql, None)]
    assert inserted_params is not None
    assert inserted_params[2] == "implementation_authorization"
    assert inserted_params[4] == _REVISION_1_ID
    assert inserted_params[5] is None and inserted_params[6] is None
    assert _count(conn, _GATE_UPDATE) == 0
    assert _count(conn, _LOOP_UPDATE) == 0
    assert _count(conn, _ARCHIVE_WRITE) == 0
    assert _count(conn, _STATUS_UPDATE) == 1
    assert _event_types(conn) == ["owner_gate_created", "plan_ready"]
    (gate_event,) = _event_params_of_type(conn, "owner_gate_created")
    assert gate_event[3] == "openorc" and gate_event[4] is None
    assert gate_event[5] == "owner_gate" and gate_event[6] == _GATE_1_ID
    (ready_event,) = _event_params_of_type(conn, "plan_ready")
    assert ready_event[3] == "openorc" and ready_event[4] is None
    assert ready_event[5] == "plan_revision" and ready_event[6] == _REVISION_1_ID
    assert ready_event[7].obj == {}


def test_a_settled_acceptance_presents_the_gate_without_a_status_write() -> None:
    """E3c's SETTLED_ACCEPTED shape: the ``waiting_for_owner`` Task with the
    settled gate's pointer cleared takes the same one presentation path and
    needs no status movement — only the new current gate and its token."""
    conn = ScriptedConnection()
    _script_presentation(
        conn,
        task_row=_task_row(status="waiting_for_owner", gate_id=None),
        revision_row=_revision_row(revision_id=_REVISION_1_ID, number=1),
        iterations=[
            _changes_iteration(_ITERATION_1_ID, number=1),
            _accepted_iteration(_ITERATION_2_ID, number=2),
        ],
    )

    outcome = _open_call(_pool(conn))

    assert outcome.task.status is TaskStatus.WAITING_FOR_OWNER
    assert outcome.task.state_token == _TOKEN_2
    assert outcome.task.current_owner_gate_id == _GATE_1_ID
    assert outcome.review_iteration.id == _ITERATION_2_ID
    assert outcome.review_iteration.outcome is ReviewOutcome.ACCEPTED
    assert outcome.owner_gate.plan_revision_id == _REVISION_1_ID
    assert _count(conn, _STATUS_UPDATE) == 0
    assert _count(conn, _GATES_INSERT) == 1
    assert _event_types(conn) == ["owner_gate_created", "plan_ready"]


def test_a_second_presentation_cannot_duplicate_the_current_gate() -> None:
    """A raced or repeated presentation against a Task that already carries
    a current gate is a conflict that installs nothing."""
    conn = ScriptedConnection()
    _script_repeating_reads(conn)
    waiting = _task_row(status="waiting_for_owner", gate_id=_GATE_1_ID)
    conn.on(_TASK_READ, waiting)
    conn.on(_TASK_READ_LOCKED, waiting)
    conn.on(_TASK_READ, waiting)

    with pytest.raises(ConflictError, match="current owner gate"):
        _open_call(_pool(conn))

    assert _count(conn, _GATES_INSERT) == 0
    assert _event_types(conn) == []


def test_a_stale_presentation_token_applies_nothing() -> None:
    conn = ScriptedConnection()
    _script_repeating_reads(conn)
    moved = _task_row(state_token=_TOKEN_3, gate_id=None)
    conn.on(_TASK_READ, moved)
    conn.on(_TASK_READ_LOCKED, moved)
    conn.on(_TASK_READ, moved)

    with pytest.raises(StaleOperationError):
        _open_call(_pool(conn))

    assert _count(conn, _GATES_INSERT) == 0
    assert _event_types(conn) == []


def test_a_wrong_pregate_state_refuses_presentation() -> None:
    conn = ScriptedConnection()
    _script_repeating_reads(conn)
    implementing = _task_row(status="implementing", gate_id=None)
    conn.on(_TASK_READ, implementing)
    conn.on(_TASK_READ_LOCKED, implementing)
    conn.on(_TASK_READ, implementing)

    with pytest.raises(ConflictError, match="pre-gate planning state"):
        _open_call(_pool(conn))

    assert _count(conn, _GATES_INSERT) == 0
    assert _event_types(conn) == []


def test_a_task_without_a_current_plan_refuses_presentation() -> None:
    conn = ScriptedConnection()
    _script_repeating_reads(conn)
    unplanned = _task_row(status="planning", current_plan_revision_id=None, gate_id=None)
    conn.on(_TASK_READ, unplanned)
    conn.on(_TASK_READ_LOCKED, unplanned)
    conn.on(_TASK_READ, unplanned)

    with pytest.raises(ConflictError, match="no current plan revision"):
        _open_call(_pool(conn))

    assert _count(conn, _GATES_INSERT) == 0


def test_a_retired_gate_change_request_refuses_a_new_presentation() -> None:
    """The #219 handoff state: the latest adjudicating review is the change
    request that retired the former gate — no accepted clearance exists."""
    conn = ScriptedConnection()
    _script_presentation(
        conn,
        task_row=_task_row(status="planning", gate_id=None),
        revision_row=_revision_row(revision_id=_REVISION_1_ID, number=1),
        iterations=[_accepted_iteration(), _changes_iteration()],
    )
    # Never reached: the presentation fails closed on the clearance check.
    conn.on(_GATES_INSERT, AssertionError("no gate may be inserted"))

    with pytest.raises(ConflictError, match="change request"):
        _open_call(_pool(conn))

    assert _count(conn, _GATES_INSERT) == 0
    assert _event_types(conn) == []


def test_a_distinct_new_acceptance_earns_its_own_gate() -> None:
    """After E2 produces revision 2 and the Reviewer accepts it, the NEW
    revision earns its own NEW gate; the retired gate's pending history is
    never rewritten."""
    conn = ScriptedConnection()
    _script_presentation(
        conn,
        task_row=_task_row(
            status="planning", current_plan_revision_id=_REVISION_2_ID, gate_id=None
        ),
        revision_row=_revision_row(revision_id=_REVISION_2_ID, number=2),
        iterations=[
            _accepted_iteration(),
            _changes_iteration(),
            _accepted_iteration(_ITERATION_3_ID, number=3, plan_revision_id=_REVISION_2_ID),
        ],
        gate_row=_gate_row(gate_id=_GATE_1_ID, plan_revision_id=_REVISION_2_ID),
        installed_task_row=_task_row(status="planning", state_token=_TOKEN_2, gate_id=_GATE_1_ID),
        waiting_task_row=_task_row(
            status="waiting_for_owner", state_token=_TOKEN_3, gate_id=_GATE_1_ID
        ),
    )

    outcome = _open_call(_pool(conn))

    assert outcome.owner_gate.plan_revision_id == _REVISION_2_ID
    (inserted_params,) = [params for sql, params in conn.executed if _GATES_INSERT(sql, None)]
    assert inserted_params is not None
    assert inserted_params[4] == _REVISION_2_ID
    assert _count(conn, _GATE_UPDATE) == 0
    assert _event_types(conn) == ["owner_gate_created", "plan_ready"]


def test_an_unresolved_newest_attempt_refuses_presentation() -> None:
    conn = ScriptedConnection()
    _script_presentation(
        conn,
        task_row=_task_row(status="planning", gate_id=None),
        revision_row=_revision_row(revision_id=_REVISION_1_ID, number=1),
        iterations=[
            _accepted_iteration(),
            _iteration_row(iteration_id=_ITERATION_2_ID, number=2),
        ],
    )
    conn.on(_GATES_INSERT, AssertionError("no gate may be inserted"))

    with pytest.raises(ConflictError, match="unresolved"):
        _open_call(_pool(conn))

    assert _count(conn, _GATES_INSERT) == 0


def test_an_acceptance_on_a_moved_plan_refuses_presentation() -> None:
    """An earlier acceptance bound to a superseded revision is stale
    clearance, never reconstructable authority for the current plan."""
    conn = ScriptedConnection()
    _script_presentation(
        conn,
        task_row=_task_row(
            status="planning", current_plan_revision_id=_REVISION_2_ID, gate_id=None
        ),
        revision_row=_revision_row(revision_id=_REVISION_2_ID, number=2),
        iterations=[_accepted_iteration(number=1, plan_revision_id=_REVISION_1_ID)],
    )
    conn.on(_GATES_INSERT, AssertionError("no gate may be inserted"))

    with pytest.raises(StaleOperationError, match="clearance is stale"):
        _open_call(_pool(conn))

    assert _count(conn, _GATES_INSERT) == 0


def test_cross_scope_iteration_history_refuses_presentation() -> None:
    conn = ScriptedConnection()
    _script_presentation(
        conn,
        task_row=_task_row(status="planning", gate_id=None),
        revision_row=_revision_row(revision_id=_REVISION_1_ID, number=1),
        iterations=[
            _iteration_row(
                iteration_id=_ITERATION_1_ID,
                number=1,
                workspace_id=uuid.uuid4(),
                outcome="accepted",
                summary="Looks good",
                findings=[],
                decided_at=_OBSERVED,
            )
        ],
    )
    conn.on(_GATES_INSERT, AssertionError("no gate may be inserted"))

    with pytest.raises(ConflictError, match="cross-scope"):
        _open_call(_pool(conn))

    assert _count(conn, _GATES_INSERT) == 0


def test_a_missing_open_planning_loop_refuses_presentation() -> None:
    conn = ScriptedConnection()
    _script_presentation(
        conn,
        task_row=_task_row(status="planning", gate_id=None),
        revision_row=_revision_row(revision_id=_REVISION_1_ID, number=1),
        iterations=[_accepted_iteration()],
        loops=[],
    )
    conn.on(_GATES_INSERT, AssertionError("no gate may be inserted"))

    with pytest.raises(StaleOperationError, match="no longer open"):
        _open_call(_pool(conn))

    assert _count(conn, _GATES_INSERT) == 0


def test_a_cross_scope_current_plan_refuses_presentation() -> None:
    conn = ScriptedConnection()
    _script_presentation(
        conn,
        task_row=_task_row(status="planning", gate_id=None),
        revision_row=_revision_row(revision_id=_REVISION_1_ID, number=1, workspace_id=uuid.uuid4()),
        iterations=[_accepted_iteration()],
    )
    conn.on(_GATES_INSERT, AssertionError("no gate may be inserted"))

    with pytest.raises(NotFoundError):
        _open_call(_pool(conn))

    assert _count(conn, _GATES_INSERT) == 0


# --- Owner approval ---------------------------------------------------------------


def test_owner_approval_is_the_irreversible_planning_boundary() -> None:
    """The exact pending gate resolves APPROVED with OWNER provenance, the
    still-OPEN planning loop closes once, the Task moves to ``implementing``
    under a rotated token, and the resolution + authorization audit facts
    record in the same composition."""
    conn = ScriptedConnection()
    _script_approval(conn)

    outcome = _approve_call(_pool(conn))

    assert outcome.task.status is TaskStatus.IMPLEMENTING
    assert outcome.task.state_token == _TOKEN_3
    assert outcome.task.current_owner_gate_id is None
    assert outcome.owner_gate.status is OwnerGateStatus.APPROVED
    assert outcome.owner_gate.decided_at == _OBSERVED
    assert outcome.owner_gate.plan_revision_id == _REVISION_1_ID
    assert outcome.plan_revision.id == _REVISION_1_ID
    assert outcome.review_iteration.outcome is ReviewOutcome.ACCEPTED
    assert outcome.review_loop.status is ReviewLoopStatus.CLOSED
    assert outcome.review_loop.closed_at == _OBSERVED
    assert _count(conn, _GATES_INSERT) == 0
    assert _count(conn, _GATE_UPDATE) == 1
    assert _count(conn, _LOOP_UPDATE) == 1
    assert _count(conn, _ARCHIVE_WRITE) == 0
    (gate_update_params,) = [params for sql, params in conn.executed if _GATE_UPDATE(sql, None)]
    assert gate_update_params is not None
    assert gate_update_params[0] == "approved"
    assert _event_types(conn) == ["owner_gate_resolved", "implementation_authorized"]
    (resolved_event,) = _event_params_of_type(conn, "owner_gate_resolved")
    assert resolved_event[3] == "owner" and resolved_event[4] == str(_OWNER_PROFILE_ID)
    assert resolved_event[5] == "owner_gate" and resolved_event[6] == _GATE_1_ID
    assert resolved_event[7].obj == {"outcome": "approved"}
    (authorized_event,) = _event_params_of_type(conn, "implementation_authorized")
    assert authorized_event[3] == "owner" and authorized_event[4] == str(_OWNER_PROFILE_ID)
    assert authorized_event[5] == "plan_revision" and authorized_event[6] == _REVISION_1_ID
    assert authorized_event[7].obj == {}


def test_approval_replay_after_success_is_stale() -> None:
    """A worker crash after a committed approval cannot cause a second one:
    the post-write durable state (rotated token, cleared pointer) makes the
    replay a stale operation that applies nothing."""
    conn = ScriptedConnection()
    _script_repeating_reads(conn)
    authorized = _task_row(status="implementing", state_token=_TOKEN_3, gate_id=None)
    conn.on(_TASK_READ, authorized)
    conn.on(_TASK_READ_LOCKED, authorized)
    conn.on(_TASK_READ, authorized)

    with pytest.raises(StaleOperationError):
        _approve_call(_pool(conn))

    assert _count(conn, _GATE_UPDATE) == 0
    assert _event_types(conn) == []


def test_approval_refuses_a_retired_gate() -> None:
    """A #219-handoff-retired G1 (pointer cleared, gate pending as history)
    is never approvable."""
    conn = ScriptedConnection()
    _script_repeating_reads(conn)
    retired = _task_row(status="waiting_for_owner", gate_id=None)
    conn.on(_TASK_READ, retired)
    conn.on(_TASK_READ_LOCKED, retired)
    conn.on(_TASK_READ, retired)
    conn.on(_GATE_READ, _gate_row())  # the pending gate history still exists

    with pytest.raises(StaleOperationError, match="current gate"):
        _approve_call(_pool(conn))

    assert _count(conn, _GATE_UPDATE) == 0
    assert _event_types(conn) == []


def test_approval_refuses_a_switched_plan() -> None:
    conn = ScriptedConnection()
    _script_approval(
        conn,
        revision_row=_revision_row(revision_id=_REVISION_2_ID, number=2),
    )

    with pytest.raises(StaleOperationError, match="no longer the task's current revision"):
        _approve_call(_pool(conn))

    assert _count(conn, _GATE_UPDATE) == 0
    assert _event_types(conn) == []


def test_approval_refuses_a_closed_loop() -> None:
    conn = ScriptedConnection()
    _script_approval(conn, loops=[])

    with pytest.raises(StaleOperationError, match="no longer open"):
        _approve_call(_pool(conn))

    assert _count(conn, _GATE_UPDATE) == 0
    assert _event_types(conn) == []


def test_approval_refuses_a_newer_change_request() -> None:
    conn = ScriptedConnection()
    _script_approval(conn, iterations=[_accepted_iteration(), _changes_iteration()])

    with pytest.raises(ConflictError, match="change request"):
        _approve_call(_pool(conn))

    assert _count(conn, _GATE_UPDATE) == 0
    assert _event_types(conn) == []


def test_approval_refuses_a_non_implementation_gate() -> None:
    conn = ScriptedConnection()
    _script_approval(
        conn,
        gate_row=_gate_row(gate_id=_GATE_1_ID, gate_type="review_resolution"),
    )

    with pytest.raises(ConflictError, match="not an implementation"):
        _approve_call(_pool(conn))

    assert _count(conn, _GATE_UPDATE) == 0
    assert _event_types(conn) == []


def test_approval_refuses_a_missing_gate() -> None:
    conn = ScriptedConnection()
    _script_repeating_reads(conn)
    waiting = _task_row(status="waiting_for_owner", gate_id=_GATE_1_ID)
    conn.on(_TASK_READ, waiting)
    conn.on(_TASK_READ_LOCKED, waiting)
    conn.on(_TASK_READ, waiting)
    conn.on(_GATE_READ, None)

    with pytest.raises(NotFoundError):
        _approve_call(_pool(conn))

    assert _count(conn, _GATE_UPDATE) == 0


def test_a_forced_late_approval_failure_leaves_no_partial_facts() -> None:
    """A failure after the gate resolution applies nothing durable: no loop
    closure, no status write, and no audit event would commit in a real
    transaction."""
    conn = ScriptedConnection()
    _script_approval(conn, fail_loop_close=True)

    with pytest.raises(RuntimeError, match="forced late approval failure"):
        _approve_call(_pool(conn))

    assert _event_types(conn) == []
    assert _count(conn, _STATUS_UPDATE) == 0
    assert _count(conn, _ARCHIVE_WRITE) == 0


# --- Owner cancel at the pending gate ----------------------------------------------


def test_owner_cancel_resolves_the_gate_and_archives_atomically() -> None:
    """Cancellation is a terminal Task outcome: the exact pending gate
    resolves CANCELLED with truthful OWNER provenance (never REJECTED) and
    the Task archives CANCELLED with its safe OWNER event — one
    composition, no gate insert, no loop transition, no status write."""
    conn = ScriptedConnection()
    _script_cancellation(conn)

    outcome = _cancel_call(_pool(conn))

    assert outcome.task.status is TaskStatus.CANCELLED
    assert outcome.task.archived_at == _OBSERVED
    assert outcome.task.state_token == _TOKEN_3
    assert outcome.task.current_owner_gate_id is None
    assert outcome.owner_gate.status is OwnerGateStatus.CANCELLED
    assert outcome.owner_gate.decided_at == _OBSERVED
    assert _count(conn, _GATES_INSERT) == 0
    assert _count(conn, _GATE_UPDATE) == 1
    assert _count(conn, _ARCHIVE_WRITE) == 1
    assert _count(conn, _STATUS_UPDATE) == 0
    assert _count(conn, _LOOP_UPDATE) == 0
    (gate_update_params,) = [params for sql, params in conn.executed if _GATE_UPDATE(sql, None)]
    assert gate_update_params is not None
    assert gate_update_params[0] == "cancelled"
    assert _event_types(conn) == ["owner_gate_resolved", "task_cancelled"]
    (resolved_event,) = _event_params_of_type(conn, "owner_gate_resolved")
    assert resolved_event[3] == "owner" and resolved_event[4] == str(_OWNER_PROFILE_ID)
    assert resolved_event[6] == _GATE_1_ID
    assert resolved_event[7].obj == {"outcome": "cancelled"}
    (terminal_event,) = _event_params_of_type(conn, "task_cancelled")
    assert terminal_event[3] == "owner" and terminal_event[4] == str(_OWNER_PROFILE_ID)
    assert terminal_event[5] is None and terminal_event[6] is None


def test_approval_after_cancellation_is_stale() -> None:
    """An exact pending gate cannot survive its Task's cancellation: the
    archived terminal Task under a rotated token makes any later approval a
    stale operation that applies nothing."""
    conn = ScriptedConnection()
    _script_repeating_reads(conn)
    cancelled = _task_row(status="cancelled", state_token=_TOKEN_3, gate_id=None, archived=True)
    conn.on(_TASK_READ, cancelled)
    conn.on(_TASK_READ_LOCKED, cancelled)
    conn.on(_TASK_READ, cancelled)

    with pytest.raises(StaleOperationError):
        _approve_call(_pool(conn))

    assert _count(conn, _GATE_UPDATE) == 0
    assert _event_types(conn) == []


def test_a_forced_late_cancellation_failure_leaves_no_partial_facts() -> None:
    conn = ScriptedConnection()
    _script_cancellation(conn, fail_archive=True)

    with pytest.raises(RuntimeError, match="forced late cancellation failure"):
        _cancel_call(_pool(conn))

    # The resolved-gate event rode the failed transaction and would roll
    # back with it in a real database; the archival statement was the
    # scripted failure point, and the terminal event never happened.
    assert _event_types(conn) == ["owner_gate_resolved"]
    assert _event_params_of_type(conn, "task_cancelled") == []
    assert _count(conn, _ARCHIVE_WRITE) == 1


def test_cancel_refuses_a_stale_token() -> None:
    conn = ScriptedConnection()
    _script_repeating_reads(conn)
    moved = _task_row(status="waiting_for_owner", state_token=_TOKEN_3, gate_id=_GATE_1_ID)
    conn.on(_TASK_READ, moved)
    conn.on(_TASK_READ_LOCKED, moved)
    conn.on(_TASK_READ, moved)

    with pytest.raises(StaleOperationError):
        _cancel_call(_pool(conn))

    assert _count(conn, _GATE_UPDATE) == 0
    assert _count(conn, _ARCHIVE_WRITE) == 0
    assert _event_types(conn) == []


def test_cancel_refuses_a_non_implementation_gate() -> None:
    conn = ScriptedConnection()
    _script_cancellation(
        conn,
        gate_row=_gate_row(gate_id=_GATE_1_ID, gate_type="review_resolution"),
    )

    with pytest.raises(ConflictError, match="only the current implementation"):
        _cancel_call(_pool(conn))

    assert _count(conn, _GATE_UPDATE) == 0
    assert _count(conn, _ARCHIVE_WRITE) == 0
    assert _event_types(conn) == []


def test_cancel_refuses_a_gate_that_lost_currency() -> None:
    """A #219-handoff-retired gate (pointer cleared) is not cancellable
    through this seam; it stays pending history untouched."""
    conn = ScriptedConnection()
    _script_repeating_reads(conn)
    retired = _task_row(status="waiting_for_owner", gate_id=None)
    conn.on(_TASK_READ, retired)
    conn.on(_TASK_READ_LOCKED, retired)
    conn.on(_TASK_READ, retired)
    conn.on(_INTENTS_LIST, [])  # the accounted-runtime veto reads
    conn.on(_EXECUTIONS_LIST, [])
    conn.on(_RUNTIME_REQUESTS_LIST, [])
    conn.on(_GATE_READ, _gate_row())

    with pytest.raises(StaleOperationError, match="current gate"):
        _cancel_call(_pool(conn))

    assert _count(conn, _GATE_UPDATE) == 0
    assert _count(conn, _ARCHIVE_WRITE) == 0
    assert _event_types(conn) == []


# --- command validation ------------------------------------------------------------


def test_malformed_commands_fail_before_any_state_is_touched() -> None:
    conn = ScriptedConnection()

    with pytest.raises(InvalidCommandError):
        open_implementation_authorization(
            _pool(conn),
            profile_id="not-a-uuid",  # type: ignore[arg-type]
            workspace_id=_WORKSPACE_ID,
            task_id=_TASK_ID,
            expected_state_token=_STATE_TOKEN,
        )
    with pytest.raises(InvalidCommandError):
        approve_implementation_authorization(
            _pool(conn),
            profile_id=_OWNER_PROFILE_ID,
            workspace_id=_WORKSPACE_ID,
            task_id=_TASK_ID,
            owner_gate_id=None,  # type: ignore[arg-type]
            expected_state_token=_STATE_TOKEN,
        )
    with pytest.raises(InvalidCommandError):
        cancel_implementation_authorization(
            _pool(conn),
            profile_id=_OWNER_PROFILE_ID,
            workspace_id=_WORKSPACE_ID,
            task_id=_TASK_ID,
            owner_gate_id=_GATE_1_ID,
            expected_state_token="nope",  # type: ignore[arg-type]
        )

    assert conn.executed == []
