"""Deterministic tests for the E6 authorized-implementation execution (issue #81).

A scripted SQL-connection seam proves the E6 seams on their critical paths:

- manual-flow fidelity: E5-approved exact-plan evidence permits exactly one
  native ``ImplementInteraction`` realization on the original Producer
  external session ID followed by exactly one send carrying the persisted
  accepted Reviewer ``review_result`` envelope (summary preserved, zero
  findings) with ``expected_family=IMPLEMENTATION_RESULT_FAMILY``, then the
  one-shot finalize persisting the validated canonical result with
  ``succeeded`` and the matched Producer-attributed start/complete audit —
  with the send proven to run with no database transaction open;
- authority/currentness refusals before any effect: stale token, archived
  Task, wrong status, foreign Workspace, non-Owner caller, missing or
  non-READY Producer session, absent/wrong-revision/superseded gate,
  missing CLOSED planning loop, non-accepted latest iteration, accepted
  iteration carrying findings, and any prior attempt history (succeeded,
  unresolved, or failed — never an automatic second dispatch);
- the review-finding regression: authority changing during attachment (R1)
  finalizes the never-dispatched attempt ``failed_final`` with its
  ``execution_failed`` audit and no realization or send; authority changing
  during ACT reconstruction (R2) refuses the send and leaves the attempt
  conservatively running;
- failure/uncertainty: known pre-effect runtime failures finalize
  ``failed_transient`` truthfully; uncertain attachment/realization/send
  outcomes, invalid formal completions, stale finalize subjects, and a
  raced duplicate finalize never resend, never fabricate success, and
  never overwrite history;
- activity: content-free runtime-neutral qualifying pulses (issue #246)
  advance the marker with coalescing; subscription failure, unavailable
  telemetry, and late callbacks after finalization never do. The qualified
  kind vocabulary and exact-session enforcement live behind the Cline
  adapter boundary and are pinned by the adapter suite, not here.

No live GitHub, runtime, or Postgres access; the new SQL
composition/concurrent-claim seams are proven by the integration-marked
suite against a real Postgres.
"""

from __future__ import annotations

import json
import uuid
from collections.abc import Callable, Iterator, Sequence
from contextlib import AbstractContextManager, contextmanager
from datetime import UTC, datetime
from typing import Any, cast

import pytest

from openorc.adapters.agent_runtime.errors import (
    AgentRuntimeError,
    AgentRuntimeTimeoutError,
    AgentRuntimeUnavailableError,
)
from openorc.adapters.agent_runtime.results import AgentSessionCreationRequest
from openorc.domain.connections import WorkflowRole
from openorc.domain.detached_turns import TaskDetachedTurnReceiptStatus
from openorc.domain.executions import ExecutionStatus
from openorc.domain.pending_interactions import PendingTaskRuntimeInteractionOperation
from openorc.persistence.pool import DatabasePool
from openorc.services.errors import (
    ExternalOperationFailedError,
    ExternalOperationUncertainError,
    InvalidCommandError,
    ObservationRegistrationRefusedError,
)
from openorc.services.task_authorized_implementation_execution import (
    dispatch_authorized_implementation_detached,
)
from openorc.services.task_detached_turn_observation import (
    ObservationRegistrationRefusalReason,
)
from tests.fakes.agent_runtime import (
    ControlRealization,
    FakeAgentRuntimeAdapter,
    implementation_result_candidate,
    session_ready_candidate,
)
from tests.fakes.task_rows import task_row

_OBSERVED = datetime(2026, 10, 20, 12, 0, 0, tzinfo=UTC)
_WORKSPACE_ID = uuid.uuid4()
_OTHER_WORKSPACE_ID = uuid.uuid4()
_OWNER_PROFILE_ID = uuid.uuid4()
_TASK_ID = uuid.uuid4()
_REPOSITORY_ID = uuid.uuid4()
_STATE_TOKEN = uuid.uuid4()
_TOKEN_2 = uuid.uuid4()
_REVISION_ID = uuid.uuid4()
_OTHER_REVISION_ID = uuid.uuid4()
_LOOP_ID = uuid.uuid4()
_ITERATION_ID = uuid.uuid4()
_GATE_ID = uuid.uuid4()
_SESSION_ID = uuid.uuid4()
_CONNECTION_ID = uuid.uuid4()
_EXECUTION_ID = uuid.uuid4()
_EXTERNAL_SESSION_ID = "fake-session-1"
_BASE_SHA = "c" * 40
_PLAN_TEXT = "# Plan\n\nStep one.\n"
_SUMMARY = "Looks good"

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
    """Routes each execute to the first matching scripted handler."""

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
        raise AssertionError("authorized execution tests never close pools")


def _pool(conn: ScriptedConnection) -> DatabasePool:
    return cast(DatabasePool, FakePool(conn))


_TASK_READ = _sql_lacks(_sql_has("from openorc.tasks where id"), "for update")
_TASK_READ_LOCKED = _sql_has("from openorc.tasks where id", "for update")
_REVISION_READ = _sql_has("from openorc.plan_revisions where id")
_GATES_LIST = _sql_has("from openorc.owner_gates where task_id")
_LOOPS_LIST = _sql_has("from openorc.review_loops where task_id")
_ITERATIONS_LIST = _sql_has("review_iterations where review_loop_id")
_SESSION_READ = _sql_lacks(
    _sql_has("from openorc.task_agent_sessions where task_id"), "select role"
)
_SESSION_ROLE_READ = _sql_has("select role from openorc.task_agent_sessions")
_EXECUTIONS_LIST = _sql_has("from openorc.executions where task_id")
_EXECUTION_INSERT = _sql_has("insert into openorc.executions")
_EXECUTION_CLAIM = _sql_has("update openorc.executions", "set status = 'running'")
_EXECUTION_COMPLETE = _sql_has("update openorc.executions", "set status = 'succeeded'")
_EXECUTION_ACTIVITY = _sql_lacks(
    _sql_has("update openorc.executions", "last_qualifying_activity_at = now()"), "set status"
)
_EXECUTION_GUARDED_UPDATE = _sql_has("update openorc.executions", "status = %s")
_EVENT_INSERT = _sql_has("insert into openorc.workflow_events")


def _ws_row(*, owner_profile_id: Any = _OWNER_PROFILE_ID) -> tuple[Any, ...]:
    return (_WORKSPACE_ID, owner_profile_id, "platform", _OBSERVED, _OBSERVED, 2, "", None)


def _task_row(
    *,
    status: str = "implementing",
    state_token: Any = _STATE_TOKEN,
    current_plan_revision_id: Any = _REVISION_ID,
    gate_id: Any = None,
    archived: bool = False,
    workspace_id: Any = _WORKSPACE_ID,
) -> tuple[Any, ...]:
    return task_row(
        id=_TASK_ID,
        workspace_id=workspace_id,
        repository_id=_REPOSITORY_ID,
        github_issue_id=503,
        status=status,
        archived_at=_OBSERVED if archived else None,
        state_token=state_token,
        current_plan_revision_id=current_plan_revision_id,
        current_owner_gate_id=gate_id,
        source_requirements_fingerprint="a" * 64,
        created_at=_OBSERVED,
        updated_at=_OBSERVED,
    )


def _revision_row(*, revision_id: Any = _REVISION_ID) -> tuple[Any, ...]:
    return (revision_id, _WORKSPACE_ID, _TASK_ID, 1, _PLAN_TEXT, _BASE_SHA, _OBSERVED)


def _loop_row(
    *, loop_id: Any = _LOOP_ID, status: str = "closed", closed_at: Any = _OBSERVED
) -> tuple[Any, ...]:
    return (loop_id, _WORKSPACE_ID, _TASK_ID, "planning", 2, status, closed_at, _OBSERVED)


_ITERATION_ROW_FINDINGS: list[dict[str, str]] = []


def _iteration_row(
    *,
    iteration_id: Any = _ITERATION_ID,
    number: int = 1,
    plan_revision_id: Any = _REVISION_ID,
    outcome: str | None = "accepted",
    summary: str | None = _SUMMARY,
    findings: Any = None,
    decided_at: Any = _OBSERVED,
) -> tuple[Any, ...]:
    if findings is None:
        findings = _ITERATION_ROW_FINDINGS
    return (
        iteration_id,
        _WORKSPACE_ID,
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
    gate_id: Any = _GATE_ID,
    gate_type: str = "implementation_authorization",
    plan_revision_id: Any = _REVISION_ID,
    status: str = "approved",
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


def _session_row(
    *,
    session_id: Any = _SESSION_ID,
    role: str = "producer",
    lifecycle: str = "ready",
    external_id: Any = _EXTERNAL_SESSION_ID,
) -> tuple[Any, ...]:
    return (
        session_id,
        _WORKSPACE_ID,
        _TASK_ID,
        role,
        _CONNECTION_ID,
        external_id,
        lifecycle,
        {},
        None,
        None,
        None,
        _OBSERVED,
        None,
        _OBSERVED,
        _OBSERVED,
        None,
        None,
    )


def _execution_row(
    *,
    execution_id: Any = _EXECUTION_ID,
    number: int = 1,
    status: str = "queued",
    implementation_result: Any = None,
    last_activity: Any = None,
) -> tuple[Any, ...]:
    return (
        execution_id,
        _WORKSPACE_ID,
        _TASK_ID,
        _SESSION_ID,
        number,
        status,
        _OBSERVED,
        _OBSERVED,
        implementation_result,
        last_activity,
        None,
    )


def _event_row() -> tuple[Any, ...]:
    return (
        uuid.uuid4(),
        _WORKSPACE_ID,
        _TASK_ID,
        "execution_started",
        "producer",
        str(_SESSION_ID),
        "execution",
        _EXECUTION_ID,
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


# --- runtime, attachment, and subscription fakes ---------------------------------


class _ProbedSendAdapter(FakeAgentRuntimeAdapter):
    """Fake runtime proving every send runs with no transaction open."""

    def __init__(self, pool: FakePool) -> None:
        super().__init__()
        self._execution_pool = pool

    def send(  # type: ignore[override]
        self,
        session_id: str,
        message: str,
        *,
        expected_family: str | None = None,
    ) -> Any:  # noqa: ANN401 - mirrors the #67 contract return
        assert self._execution_pool.active_connections == 0, (
            "the service must never hold a database transaction open across the send"
        )
        return super().send(session_id, message, expected_family=expected_family)


class _ScriptedRealizeRuntime(_ProbedSendAdapter):
    """Adds a scripted one-shot realization error to the probe runtime."""

    def __init__(self, pool: FakePool) -> None:
        super().__init__(pool)
        self._realize_error: AgentRuntimeError | None = None

    def queue_realize_error(self, error: AgentRuntimeError) -> None:
        self._realize_error = error

    def realize_control(  # type: ignore[override]
        self,
        session_id: str,
        interaction: Any,  # noqa: ANN401 - mirrors the #67 contract parameter
        *,
        message: str | None = None,
        expected_family: str | None = None,
    ) -> Any:  # noqa: ANN401 - mirrors the #67 contract return
        assert self._execution_pool.active_connections == 0, (
            "the service must never hold a database transaction open across the realization"
        )
        if self._realize_error is not None:
            error, self._realize_error = self._realize_error, None
            raise error
        return super().realize_control(
            session_id, interaction, message=message, expected_family=expected_family
        )


class _ProducerAttachment:
    """Deferred Producer attachment probe: counts acquisitions and disposals."""

    def __init__(self, runtime: FakeAgentRuntimeAdapter) -> None:
        self._runtime = runtime
        self.acquisitions = 0
        self.disposals = 0

    def __call__(self) -> AbstractContextManager[FakeAgentRuntimeAdapter]:
        self.acquisitions += 1
        return self

    def __enter__(self) -> FakeAgentRuntimeAdapter:
        return self._runtime

    def __exit__(self, *_exc: object) -> bool:
        self.disposals += 1
        return False


class _FailingAttachment:
    """Raises its typed error exactly when the attachment is acquired."""

    def __init__(self, error: Exception) -> None:
        self._error = error

    def __call__(self) -> AbstractContextManager[FakeAgentRuntimeAdapter]:
        return self

    def __enter__(self) -> FakeAgentRuntimeAdapter:
        raise self._error

    def __exit__(self, *_exc: object) -> bool:
        return False


class _ScriptedActivitySubscription:
    """Delivers scripted zero-argument qualifying pulses on establishment."""

    def __init__(self, pulses: int = 0) -> None:
        self._pulse_count = pulses
        self.session_ids: list[str] = []
        self.pulses: list[Callable[[], None]] = []
        self.closed = 0

    def __call__(
        self,
        _runtime: Any,  # noqa: ANN401 - the service passes its resolved adapter
        session_id: str,
        pulse: Callable[[], None],
    ) -> AbstractContextManager[None]:
        self.session_ids.append(session_id)
        self.pulses.append(pulse)
        return self

    def __enter__(self) -> None:
        for _ in range(self._pulse_count):
            self.pulses[-1]()
        return None

    def __exit__(self, *_exc: object) -> bool:
        self.closed += 1
        return False

    def deliver_late(self) -> None:
        self.pulses[-1]()


class _RaisingSubscription:
    """A subscription whose establishment always fails (advisory telemetry)."""

    def __call__(
        self,
        _runtime: Any,  # noqa: ANN401
        _session_id: str,
        _pulse: Callable[[], None],
    ) -> AbstractContextManager[None]:
        return self

    def __enter__(self) -> None:
        raise RuntimeError("the activity subscription is unavailable")

    def __exit__(self, *_exc: object) -> bool:
        return False


def _prepared_adapter(
    pool: FakePool,
    *,
    send_outcomes: Sequence[Any] = (),
    realize_error: AgentRuntimeError | None = None,
) -> _ScriptedRealizeRuntime:
    """One fake runtime holding the live Producer session."""
    adapter = _ScriptedRealizeRuntime(pool)
    adapter.queue_creation(session_ready_candidate())
    created = adapter.create_session(
        AgentSessionCreationRequest(
            workspace_id=_WORKSPACE_ID,
            task_id=_TASK_ID,
            role=WorkflowRole.PRODUCER,
            initialization="prepared",
        )
    )
    assert created.external_session_id == _EXTERNAL_SESSION_ID
    for outcome in send_outcomes:
        adapter.queue_send(_EXTERNAL_SESSION_ID, outcome)
    adapter.script_realization(_EXTERNAL_SESSION_ID, ControlRealization.NATIVE_ONLY)
    if realize_error is not None:
        adapter.queue_realize_error(realize_error)
    return adapter


# --- scripted sequences (in service call order) ----------------------------------


def _script_repeating_reads(conn: ScriptedConnection) -> None:
    """Same-shaped reads whose result never changes across the whole flow."""
    conn.on_repeat(_sql_has("from openorc.profiles", "for key share"), (None, None, None))
    conn.on_repeat(_sql_has("from openorc.workspaces where id"), _ws_row())
    conn.on_repeat(_EVENT_INSERT, _event_row())


def _script_subject_reads(
    conn: ScriptedConnection,
    *,
    task_row: tuple[Any, ...] | None = None,
    session_row: tuple[Any, ...] | None = None,
    gates: list[tuple[Any, ...]] | None = None,
    loops: list[tuple[Any, ...]] | None = None,
    iterations: list[tuple[Any, ...]] | None = None,
) -> None:
    """One subject-revalidation read block in service call order (R1/R2/finalize)."""
    current = task_row if task_row is not None else _task_row()
    conn.on(_TASK_READ_LOCKED, current)
    conn.on(_TASK_READ, current)
    conn.on(_REVISION_READ, _revision_row())
    conn.on(_GATES_LIST, gates if gates is not None else [_gate_row()])
    conn.on(_LOOPS_LIST, loops if loops is not None else [_loop_row()])
    conn.on(_ITERATIONS_LIST, iterations if iterations is not None else [_iteration_row()])
    conn.on(_SESSION_READ, session_row if session_row is not None else _session_row())


def _script_admission(
    conn: ScriptedConnection,
    *,
    task_row: tuple[Any, ...] | None = None,
    session_row: tuple[Any, ...] | None = None,
    gates: list[tuple[Any, ...]] | None = None,
    loops: list[tuple[Any, ...]] | None = None,
    iterations: list[tuple[Any, ...]] | None = None,
    executions: list[tuple[Any, ...]] | None = None,
) -> None:
    """The admission/reservation transaction in call order."""
    _script_repeating_reads(conn)
    current = task_row if task_row is not None else _task_row()
    conn.on(_TASK_READ, current)  # workspace guard task read
    conn.on(_TASK_READ_LOCKED, current)  # get_task_for_update
    conn.on(_TASK_READ, current)  # require_current_task
    conn.on(_REVISION_READ, _revision_row())
    conn.on(_GATES_LIST, gates if gates is not None else [_gate_row()])
    conn.on(_LOOPS_LIST, loops if loops is not None else [_loop_row()])
    conn.on(_ITERATIONS_LIST, iterations if iterations is not None else [_iteration_row()])
    conn.on(_SESSION_READ, session_row if session_row is not None else _session_row())
    conn.on(_EXECUTIONS_LIST, executions if executions is not None else [])
    conn.on(_SESSION_ROLE_READ, ("producer",))
    conn.on(_EXECUTION_INSERT, _execution_row())
    conn.on(_EXECUTION_CLAIM, _execution_row(status="running", last_activity=_OBSERVED))


def _script_happy_path(
    conn: ScriptedConnection,
    *,
    complete_row: tuple[Any, ...] | None = None,
    with_activity_writes: bool = False,
    finalize_rejects: bool = False,
) -> None:
    """The full admission + R1 + R2 + finalize flow in call order."""
    _script_admission(conn)
    _script_subject_reads(conn)  # R1
    _script_subject_reads(conn)  # R2
    _script_subject_reads(conn)  # finalize
    if finalize_rejects:
        # A concurrent finalize won the conditional write: this finalize's
        # guarded statement applies no row.
        conn.on(_EXECUTION_COMPLETE, None)
    else:
        conn.on(
            _EXECUTION_COMPLETE,
            complete_row
            if complete_row is not None
            else _execution_row(status="succeeded", last_activity=_OBSERVED),
        )
    if with_activity_writes:
        conn.on_repeat(
            _EXECUTION_ACTIVITY,
            _execution_row(status="running", last_activity=_OBSERVED),
        )


_RESULT_CANDIDATE = implementation_result_candidate(
    branch="feat/issue-81",
    summary="Implemented the handoff",
    changes=["feat: handoff"],
    validation=["tests pass"],
)

_COMPLETE_RESULT_ROW = _execution_row(
    status="succeeded",
    implementation_result=dict(_RESULT_CANDIDATE),
    last_activity=_OBSERVED,
)


def _arrange(
    conn: ScriptedConnection,
    *,
    send_outcomes: Sequence[Any] = (),
    realize_error: AgentRuntimeError | None = None,
) -> tuple[FakePool, _ScriptedRealizeRuntime, _ProducerAttachment]:
    """One scripted case: a single shared pool, runtime, and attachment probe.

    The runtime's send/realization probes assert on THIS pool's checkout
    counter, so the no-transaction-open proof is never vacuous.
    """
    pool = FakePool(conn)
    runtime = _prepared_adapter(pool, send_outcomes=send_outcomes, realize_error=realize_error)
    attachment = _ProducerAttachment(runtime)
    return pool, runtime, attachment


# --- manual-flow fidelity --------------------------------------------------------


# --- activity marker -------------------------------------------------------------


# --- authority/currentness refusals before any effect ----------------------------


# --- authority changing mid-flight (review-finding regressions) -------------------


# --- known pre-effect runtime failures -------------------------------------------


# --- uncertain send outcomes and invalid completions ------------------------------


# --- finalize currentness and duplicate races -------------------------------------


# --- the detached dispatch (issues #325/#333) --------------------------------------

_RECEIPT_ID = uuid.uuid4()
_TURN_REFERENCE = "fake-run-1"

_RECEIPT_INSERT = _sql_has("insert into openorc.task_hub_run_receipts")
_RECEIPT_FOR_UPDATE = _sql_has("from openorc.task_hub_run_receipts", "where id = %s for update")
_RECEIPT_ATTACH = _sql_has("update openorc.task_hub_run_receipts", "set status = 'admitted'")
_RECEIPT_REFUSE = _sql_has("update openorc.task_hub_run_receipts", "set status = 'refused'")
_RECEIPT_SETTLE = _sql_has("update openorc.task_hub_run_receipts", "set status = 'settled'")
_EARLY_COMPLETION_READ = _sql_has("from openorc.task_hub_run_early_completions", "for update")
_EARLY_COMPLETION_DELETE = _sql_has("delete from openorc.task_hub_run_early_completions")
_EXECUTION_READ = _sql_lacks(_sql_has("from openorc.executions where id = %s"), "for update")
_EXECUTION_READ_LOCKED = _sql_has("from openorc.executions", "for update")


def _detached_receipt_row(
    *,
    status: str = "claimed",
    admitted_turn_reference: str | None = None,
    outcome: dict[str, Any] | None = None,
) -> tuple[Any, ...]:
    admitted_at = _OBSERVED if status in ("admitted", "settled") else None
    settled_at = _OBSERVED if status in ("settled", "refused") else None
    return (
        _RECEIPT_ID,
        _WORKSPACE_ID,
        _TASK_ID,
        _SESSION_ID,
        "implementation_execution",
        "execution",
        _EXECUTION_ID,
        "implementation_result",
        admitted_turn_reference,
        status,
        outcome if outcome is not None else {},
        None,
        _OBSERVED,
        admitted_at,
        settled_at,
        _OBSERVED,
        None,
        None,
        None,
        None,
    )


class _RecordingRegistrar:
    """Records the pre-admission observation registrations (issue #323 seam)."""

    def __init__(self, error: Exception | None = None) -> None:
        self._error = error
        self.registrations: list[dict[str, Any]] = []

    def register_session(
        self,
        *,
        workspace_id: Any,
        task_id: Any,
        task_agent_session_id: Any,
        connection_id: Any,
        external_session_id: Any,
    ) -> None:
        self.registrations.append(
            {
                "workspace_id": workspace_id,
                "task_id": task_id,
                "task_agent_session_id": task_agent_session_id,
                "connection_id": connection_id,
                "external_session_id": external_session_id,
            }
        )
        if self._error is not None:
            raise self._error


class _RecordingHandoff:
    """Records the ONE post-commit completion handoff invocations (issue #333)."""

    def __init__(self, error: Exception | None = None) -> None:
        self._error = error
        self.calls: list[dict[str, Any]] = []
        self.interrupt_calls: list[dict[str, Any]] = []

    def handoff_settled_completion(
        self,
        *,
        workspace_id: Any,
        task_id: Any,
        operation: Any,
    ) -> None:
        self.calls.append(
            {"workspace_id": workspace_id, "task_id": task_id, "operation": operation}
        )
        if self._error is not None:
            raise self._error

    def handoff_settled_interrupt(
        self,
        *,
        workspace_id: Any,
        task_id: Any,
        task_agent_session_id: Any,
        external_session_id: str,
    ) -> None:
        self.interrupt_calls.append(
            {
                "workspace_id": workspace_id,
                "task_id": task_id,
                "task_agent_session_id": task_agent_session_id,
                "external_session_id": external_session_id,
            }
        )
        if self._error is not None:
            raise self._error


def _detached_runtime(
    pool: FakePool,
    *,
    admission_error: AgentRuntimeError | None = None,
) -> _ScriptedRealizeRuntime:
    """One fake runtime holding the live Producer session, admission-ready."""
    runtime = _ScriptedRealizeRuntime(pool)
    runtime.queue_creation(session_ready_candidate())
    created = runtime.create_session(
        AgentSessionCreationRequest(
            workspace_id=_WORKSPACE_ID,
            task_id=_TASK_ID,
            role=WorkflowRole.PRODUCER,
            initialization="prepared",
        )
    )
    assert created.external_session_id == _EXTERNAL_SESSION_ID
    runtime.script_realization(_EXTERNAL_SESSION_ID, ControlRealization.NATIVE_ONLY)
    if admission_error is not None:
        runtime.queue_detached_admission(admission_error)
    return runtime


def _arrange_detached(
    conn: ScriptedConnection,
    *,
    admission_error: AgentRuntimeError | None = None,
) -> tuple[FakePool, _ScriptedRealizeRuntime, _ProducerAttachment]:
    pool = FakePool(conn)
    runtime = _detached_runtime(pool, admission_error=admission_error)
    attachment = _ProducerAttachment(runtime)
    return pool, runtime, attachment


def _dispatch_detached_call(
    pool: DatabasePool,
    *,
    producer_runtime_factory: Any,  # noqa: ANN401 - typed at the service boundary
    observation_registrar: Any,  # noqa: ANN401
    completion_handoff: Any,  # noqa: ANN401
    expected_state_token: Any = _STATE_TOKEN,
    profile_id: Any = _OWNER_PROFILE_ID,
) -> Any:  # noqa: ANN401 - the typed outcome
    return dispatch_authorized_implementation_detached(
        pool,
        profile_id=profile_id,
        workspace_id=_WORKSPACE_ID,
        task_id=_TASK_ID,
        expected_state_token=expected_state_token,
        producer_runtime_factory=producer_runtime_factory,
        observation_registrar=observation_registrar,
        completion_handoff=completion_handoff,
    )


def _script_detached_admission(conn: ScriptedConnection) -> None:
    """The reservation + R1 + the receipt commit + R2 + the ack attach.

    The R2 block and the attach are scripted separately by the failure
    tests that must break them; this helper covers the ordinary prefix and
    the happy-path remainder.
    """
    _script_admission(conn)  # the reservation
    _script_subject_reads(conn)  # R1
    conn.on(_RECEIPT_INSERT, _detached_receipt_row())


def _script_detached_attach(conn: ScriptedConnection, *, staged: Any = None) -> None:
    """The acknowledgement attach (claimed → admitted, staged or plain)."""
    conn.on(_RECEIPT_FOR_UPDATE, _detached_receipt_row())
    conn.on(
        _RECEIPT_ATTACH,
        _detached_receipt_row(status="admitted", admitted_turn_reference=_TURN_REFERENCE),
    )
    conn.on(_EARLY_COMPLETION_READ, staged)


def test_the_detached_dispatch_records_registers_admits_and_attaches_exactly_once() -> None:
    """One receipt, one registration, one admission, one attach: the worker
    exits with the attempt conservatively claimed and never sends."""
    conn = ScriptedConnection()
    _script_detached_admission(conn)
    _script_subject_reads(conn)  # R2 passes
    _script_detached_attach(conn)
    conn.on(_TASK_READ, _task_row())
    conn.on(_EXECUTION_READ, _execution_row(status="running", last_activity=_OBSERVED))
    pool, runtime, attachment = _arrange_detached(conn)
    registrar = _RecordingRegistrar()
    handoff = _RecordingHandoff()

    outcome = _dispatch_detached_call(
        cast(DatabasePool, pool),
        producer_runtime_factory=attachment,
        observation_registrar=registrar,
        completion_handoff=handoff,
    )

    # The attempt stayed conservatively running; the receipt is admitted
    # with its acknowledged turn reference.
    assert outcome.execution.status is ExecutionStatus.RUNNING
    assert outcome.receipt.status is TaskDetachedTurnReceiptStatus.ADMITTED
    assert outcome.receipt.admitted_turn_reference == _TURN_REFERENCE
    assert (
        outcome.receipt.operation is PendingTaskRuntimeInteractionOperation.IMPLEMENTATION_EXECUTION
    )
    assert outcome.receipt.original_attempt_id == _EXECUTION_ID
    # One registration on the exact bound producer session, before admission.
    assert registrar.registrations == [
        {
            "workspace_id": _WORKSPACE_ID,
            "task_id": _TASK_ID,
            "task_agent_session_id": _SESSION_ID,
            "connection_id": _CONNECTION_ID,
            "external_session_id": _EXTERNAL_SESSION_ID,
        }
    ]
    (admission,) = runtime.detached_admissions()
    assert admission[0] == _EXTERNAL_SESSION_ID
    # No send, no finalize, no completion audit: the observed completion owns
    # them. The handoff did not fire (nothing settled in-dispatch).
    assert runtime.submitted_sends() == ()
    assert _count(conn, _EXECUTION_COMPLETE) == 0
    assert _count(conn, _EXECUTION_GUARDED_UPDATE) == 0
    assert _event_types(conn) == ["execution_started"]
    assert handoff.calls == []
    # The attachment was acquired and disposed exactly once.
    assert attachment.acquisitions == 1 and attachment.disposals == 1
    assert pool.active_connections == 0


def test_the_detached_dispatch_admits_the_persisted_acceptance_verbatim() -> None:
    """The admitted content is the structural serialization of the persisted
    accepted Reviewer envelope — the same message the blocking send carries."""
    conn = ScriptedConnection()
    _script_detached_admission(conn)
    _script_subject_reads(conn)  # R2
    _script_detached_attach(conn)
    conn.on(_TASK_READ, _task_row())
    conn.on(_EXECUTION_READ, _execution_row(status="running", last_activity=_OBSERVED))
    pool, runtime, _attachment = _arrange_detached(conn)

    _dispatch_detached_call(
        cast(DatabasePool, pool),
        producer_runtime_factory=_ProducerAttachment(runtime),
        observation_registrar=_RecordingRegistrar(),
        completion_handoff=_RecordingHandoff(),
    )

    (admission,) = runtime.detached_admissions()
    assert admission[0] == _EXTERNAL_SESSION_ID
    assert json.loads(admission[1]) == {
        "type": "review_result",
        "schema_version": 1,
        "outcome": "accepted",
        "summary": _SUMMARY,
        "findings": [],
    }


def test_detached_authority_movement_before_admission_is_an_uncertainty() -> None:
    """Authority moving on after realization — the R2 revalidation
    immediately before the admission — is the ordinary dispatch's uncertain
    classification: the attempt stays conservatively running, the receipt
    stays conservatively claimed (never refused), nothing is finalized, and
    nothing is re-dispatched."""
    conn = ScriptedConnection()
    _script_detached_admission(conn)
    _script_subject_reads(conn, task_row=_task_row(state_token=_TOKEN_2))  # R2 stale
    pool, runtime, attachment = _arrange_detached(conn)
    registrar = _RecordingRegistrar()
    handoff = _RecordingHandoff()

    with pytest.raises(ExternalOperationUncertainError):
        _dispatch_detached_call(
            cast(DatabasePool, pool),
            producer_runtime_factory=attachment,
            observation_registrar=registrar,
            completion_handoff=handoff,
        )

    # The registration already happened; the admission never did.
    assert len(registrar.registrations) == 1
    assert runtime.detached_admissions() == ()
    assert runtime.submitted_sends() == ()
    # No refusal, no finalize: the honest unresolved state.
    assert _count(conn, _RECEIPT_REFUSE) == 0
    assert _count(conn, _EXECUTION_GUARDED_UPDATE) == 0
    assert _event_types(conn) == ["execution_started"]
    assert handoff.calls == []


def test_detached_known_realization_failure_refuses_the_receipt_and_finalizes() -> None:
    """A positively proven pre-effect reconstruction failure refuses the
    just-recorded receipt and finalizes the never-dispatched attempt
    ``failed_transient`` — no registration, no admission, no send."""
    conn = ScriptedConnection()
    _script_detached_admission(conn)
    conn.on(_RECEIPT_REFUSE, _detached_receipt_row(status="refused"))
    conn.on(_EXECUTION_GUARDED_UPDATE, _execution_row(status="failed_transient"))
    pool, runtime, attachment = _arrange_detached(conn)
    runtime.queue_realize_error(AgentRuntimeUnavailableError("the runtime is unreachable"))
    registrar = _RecordingRegistrar()
    handoff = _RecordingHandoff()

    with pytest.raises(ExternalOperationFailedError):
        _dispatch_detached_call(
            cast(DatabasePool, pool),
            producer_runtime_factory=attachment,
            observation_registrar=registrar,
            completion_handoff=handoff,
        )

    assert registrar.registrations == []
    assert runtime.detached_admissions() == ()
    assert runtime.submitted_sends() == ()
    assert _count(conn, _RECEIPT_REFUSE) == 1
    (update_params,) = [
        params for sql, params in conn.executed if _EXECUTION_GUARDED_UPDATE(sql, None)
    ]
    assert update_params == ("failed_transient", _EXECUTION_ID, "running")
    assert _event_types(conn) == ["execution_started", "execution_failed"]
    (failed,) = _event_params_of_type(conn, "execution_failed")
    assert failed[7].obj == {"failure_kind": "runtime_pre_effect"}
    assert handoff.calls == []


def test_detached_uncertain_realization_leaves_the_receipt_claimed() -> None:
    """An uncertain reconstruction outcome is never a proven non-effect: the
    receipt stays conservatively claimed and the attempt stays running."""
    conn = ScriptedConnection()
    _script_detached_admission(conn)
    pool, runtime, attachment = _arrange_detached(conn)
    runtime.queue_realize_error(
        AgentRuntimeTimeoutError("the reconstruction exceeded its deadline")
    )
    registrar = _RecordingRegistrar()
    handoff = _RecordingHandoff()

    with pytest.raises(ExternalOperationUncertainError):
        _dispatch_detached_call(
            cast(DatabasePool, pool),
            producer_runtime_factory=attachment,
            observation_registrar=registrar,
            completion_handoff=handoff,
        )

    assert registrar.registrations == []
    assert runtime.detached_admissions() == ()
    assert _count(conn, _RECEIPT_REFUSE) == 0
    assert _count(conn, _EXECUTION_GUARDED_UPDATE) == 0
    assert _event_types(conn) == ["execution_started"]
    assert handoff.calls == []


def test_detached_registration_refusal_refuses_the_receipt_and_finalizes() -> None:
    """A positively-required observation registration refusal is a proven
    pre-effect failure: the receipt is refused and the attempt finalizes
    ``failed_transient`` — never an unobserved admission."""
    conn = ScriptedConnection()
    _script_detached_admission(conn)
    conn.on(_RECEIPT_REFUSE, _detached_receipt_row(status="refused"))
    conn.on(_EXECUTION_GUARDED_UPDATE, _execution_row(status="failed_transient"))
    pool, _runtime, attachment = _arrange_detached(conn)
    registrar = _RecordingRegistrar(
        error=ObservationRegistrationRefusedError(
            "the observer refused the registration",
            reason=ObservationRegistrationRefusalReason.OBSERVATION_REGISTRATION_FAILED.value,
        )
    )
    handoff = _RecordingHandoff()

    with pytest.raises(ObservationRegistrationRefusedError):
        _dispatch_detached_call(
            cast(DatabasePool, pool),
            producer_runtime_factory=attachment,
            observation_registrar=registrar,
            completion_handoff=handoff,
        )

    assert len(registrar.registrations) == 1
    assert _count(conn, _RECEIPT_REFUSE) == 1
    (update_params,) = [
        params for sql, params in conn.executed if _EXECUTION_GUARDED_UPDATE(sql, None)
    ]
    assert update_params == ("failed_transient", _EXECUTION_ID, "running")
    assert _event_types(conn) == ["execution_started", "execution_failed"]
    assert handoff.calls == []


def test_detached_admission_refusal_refuses_the_receipt_and_fails_known() -> None:
    """A positively identified admission refusal is a proven pre-effect
    failure: the receipt is refused and the dispatch fails known."""
    conn = ScriptedConnection()
    _script_detached_admission(conn)
    _script_subject_reads(conn)  # R2 passes
    conn.on(_RECEIPT_REFUSE, _detached_receipt_row(status="refused"))
    conn.on(_EXECUTION_GUARDED_UPDATE, _execution_row(status="failed_transient"))
    pool, runtime, attachment = _arrange_detached(
        conn,
        admission_error=AgentRuntimeUnavailableError("the runtime refused the admission"),
    )
    registrar = _RecordingRegistrar()
    handoff = _RecordingHandoff()

    with pytest.raises(ExternalOperationFailedError):
        _dispatch_detached_call(
            cast(DatabasePool, pool),
            producer_runtime_factory=attachment,
            observation_registrar=registrar,
            completion_handoff=handoff,
        )

    assert len(registrar.registrations) == 1
    assert _count(conn, _RECEIPT_REFUSE) == 1
    assert _count(conn, _EXECUTION_GUARDED_UPDATE) == 1
    assert _event_types(conn) == ["execution_started", "execution_failed"]
    assert handoff.calls == []


def test_detached_uncertain_admission_leaves_the_receipt_claimed() -> None:
    """An uncertain admission outcome is never a proven non-delivery: the
    receipt stays conservatively claimed and nothing is re-dispatched."""
    conn = ScriptedConnection()
    _script_detached_admission(conn)
    _script_subject_reads(conn)  # R2 passes
    pool, _runtime, attachment = _arrange_detached(
        conn,
        admission_error=AgentRuntimeTimeoutError("the admission exceeded its deadline"),
    )
    registrar = _RecordingRegistrar()
    handoff = _RecordingHandoff()

    with pytest.raises(ExternalOperationUncertainError):
        _dispatch_detached_call(
            cast(DatabasePool, pool),
            producer_runtime_factory=attachment,
            observation_registrar=registrar,
            completion_handoff=handoff,
        )

    assert len(registrar.registrations) == 1
    assert _count(conn, _RECEIPT_REFUSE) == 0
    assert _count(conn, _EXECUTION_GUARDED_UPDATE) == 0
    assert _event_types(conn) == ["execution_started"]
    assert handoff.calls == []


_STAGED_ID = uuid.uuid4()


def _staged_early_completion_row() -> tuple[Any, ...]:
    return (
        _STAGED_ID,
        _WORKSPACE_ID,
        _TASK_ID,
        _RECEIPT_ID,
        "run.completed",
        41,
        json.dumps(_RESULT_CANDIDATE),
        False,
        _OBSERVED,
    )


def test_a_detached_staged_early_completion_settles_in_dispatch_and_fires_the_handoff_once() -> (
    None
):
    """A validated result that finished before the acknowledgement was
    persisted is consumed exactly once by the attach, settles the attempt
    through the shared finalize, and fires the ONE post-commit completion
    handoff exactly once — no send, no second settlement, no duplicate."""
    conn = ScriptedConnection()
    _script_detached_admission(conn)
    _script_subject_reads(conn)  # R2 passes
    _script_detached_attach(conn, staged=_staged_early_completion_row())
    # The settlement routing inside the attach: attribution, validation,
    # the shared finalize, the receipt settlement, and the consumption.
    conn.on(_EXECUTION_READ, _execution_row(status="running", last_activity=_OBSERVED))
    conn.on(_EXECUTION_READ_LOCKED, _execution_row(status="running", last_activity=_OBSERVED))
    conn.on(_TASK_READ_LOCKED, _task_row())
    conn.on(_REVISION_READ, _revision_row())
    conn.on(_GATES_LIST, [_gate_row()])
    conn.on(
        _EXECUTION_COMPLETE,
        _execution_row(status="succeeded", implementation_result=dict(_RESULT_CANDIDATE)),
    )
    conn.on(
        _RECEIPT_SETTLE,
        _detached_receipt_row(
            status="settled",
            admitted_turn_reference=_TURN_REFERENCE,
            outcome={"settlement": "validated_result", "consumed_sequence": 41},
        ),
    )
    conn.on(_EARLY_COMPLETION_DELETE, None)
    # The post-dispatch reload reflects the settled durable state.
    conn.on(_TASK_READ, _task_row())
    conn.on(
        _EXECUTION_READ,
        _execution_row(status="succeeded", implementation_result=dict(_RESULT_CANDIDATE)),
    )
    pool, runtime, attachment = _arrange_detached(conn)
    registrar = _RecordingRegistrar()
    handoff = _RecordingHandoff()

    outcome = _dispatch_detached_call(
        cast(DatabasePool, pool),
        producer_runtime_factory=attachment,
        observation_registrar=registrar,
        completion_handoff=handoff,
    )

    # The attempt settled through the one shared finalize; the receipt is
    # settled exactly once.
    assert outcome.execution.status is ExecutionStatus.SUCCEEDED
    assert outcome.receipt.status is TaskDetachedTurnReceiptStatus.SETTLED
    assert runtime.submitted_sends() == ()
    assert _count(conn, _EXECUTION_COMPLETE) == 1
    assert _count(conn, _RECEIPT_SETTLE) == 1
    assert _count(conn, _EARLY_COMPLETION_DELETE) == 1
    assert _event_types(conn) == ["execution_started", "execution_completed"]
    # The ONE post-commit completion handoff fired exactly once with the
    # implementation family.
    assert handoff.calls == [
        {
            "workspace_id": _WORKSPACE_ID,
            "task_id": _TASK_ID,
            "operation": PendingTaskRuntimeInteractionOperation.IMPLEMENTATION_EXECUTION,
        }
    ]


def test_a_staged_interrupted_completion_settles_and_hands_off_the_interrupt_settlement() -> None:
    """The complete staged sequence (issue #342): an Owner-aborted turn's
    positively interrupted terminal arrives before the acknowledgement is
    persisted; the attach consumes it exactly once — settling the receipt
    ``interrupted_turn`` and recording the observation on the exact session's
    single unsettled intent — and the ONE post-commit completion handoff then
    owns the Interrupt settlement. No result is applied to the attempt, no
    failure is reported, and the handoff receives the exact session's
    settlement facts for the adapter's D6b guard release and the intent's
    one-shot recording to ``settled``."""
    _INTERRUPT_INTENTS_LIST = _sql_has("from openorc.task_interrupt_intents")
    _INTERRUPT_INTENT_UPDATE = _sql_has("update openorc.task_interrupt_intents")
    intent_id = uuid.uuid4()
    intent_row = (
        intent_id,
        _WORKSPACE_ID,
        _TASK_ID,
        _SESSION_ID,
        None,
        "producer",
        _EXTERNAL_SESSION_ID,
        _OWNER_PROFILE_ID,
        "unsettled",
        {"dispatch": "acknowledged"},
        _OBSERVED,
        _OBSERVED,
    )
    annotated_row = (*intent_row[:9], {"observed_resolution": "interrupted_turn"}, *intent_row[10:])

    conn = ScriptedConnection()
    _script_detached_admission(conn)
    _script_subject_reads(conn)  # R2 passes
    _script_detached_attach(
        conn,
        staged=(
            _STAGED_ID,
            _WORKSPACE_ID,
            _TASK_ID,
            _RECEIPT_ID,
            "run.failed",
            41,
            None,
            True,
            _OBSERVED,
        ),
    )
    # The interrupted routing inside the attach: the single unsettled intent
    # gains the observation, then the receipt settles truthfully.
    conn.on(_INTERRUPT_INTENTS_LIST, [intent_row])
    conn.on(_INTERRUPT_INTENT_UPDATE, annotated_row)
    conn.on(
        _RECEIPT_SETTLE,
        _detached_receipt_row(
            status="settled",
            admitted_turn_reference=_TURN_REFERENCE,
            outcome={"settlement": "interrupted_turn", "consumed_sequence": 41},
        ),
    )
    conn.on(_EARLY_COMPLETION_DELETE, None)
    # The post-dispatch reload reflects the untouched running attempt.
    conn.on(_TASK_READ, _task_row())
    conn.on(_EXECUTION_READ, _execution_row(status="running", last_activity=_OBSERVED))
    pool, runtime, attachment = _arrange_detached(conn)
    registrar = _RecordingRegistrar()
    handoff = _RecordingHandoff()

    outcome = _dispatch_detached_call(
        cast(DatabasePool, pool),
        producer_runtime_factory=attachment,
        observation_registrar=registrar,
        completion_handoff=handoff,
    )

    # The receipt settled exactly once with the interrupted label and the
    # staged observation was consumed; the attempt itself is untouched.
    assert outcome.receipt.status is TaskDetachedTurnReceiptStatus.SETTLED
    assert outcome.execution.status is ExecutionStatus.RUNNING
    assert runtime.submitted_sends() == ()
    assert _count(conn, _RECEIPT_SETTLE) == 1
    assert _count(conn, _EARLY_COMPLETION_DELETE) == 1
    assert _count(conn, _EXECUTION_COMPLETE) == 0
    assert _event_types(conn) == ["execution_started"]
    # The validated-result continuation never fired; the ONE post-commit
    # handoff routed the Interrupt settlement with the exact session facts.
    assert handoff.calls == []
    assert handoff.interrupt_calls == [
        {
            "workspace_id": _WORKSPACE_ID,
            "task_id": _TASK_ID,
            "task_agent_session_id": _SESSION_ID,
            "external_session_id": _EXTERNAL_SESSION_ID,
        }
    ]
    # The observation annotation applied only while the intent was unsettled.
    annotate_sql, annotate_params = next(
        (sql, params)
        for sql, params in conn.executed
        if "update openorc.task_interrupt_intents" in " ".join(sql.split())
    )
    assert annotate_params is not None
    assert annotate_params[0].obj == {"observed_resolution": "interrupted_turn"}
    assert annotate_params[1] == intent_id


def test_the_detached_dispatch_seams_fail_closed() -> None:
    """A registrar or completion handoff that does not implement its seam is
    rejected before any state is touched."""
    conn = ScriptedConnection()
    pool, _runtime, attachment = _arrange_detached(conn)

    with pytest.raises(InvalidCommandError):
        _dispatch_detached_call(
            cast(DatabasePool, pool),
            producer_runtime_factory=attachment,
            observation_registrar=object(),
            completion_handoff=_RecordingHandoff(),
        )
    with pytest.raises(InvalidCommandError):
        _dispatch_detached_call(
            cast(DatabasePool, pool),
            producer_runtime_factory=attachment,
            observation_registrar=_RecordingRegistrar(),
            completion_handoff=object(),
        )
    # No reservation, no receipt, no runtime interaction.
    assert conn.executed == []
