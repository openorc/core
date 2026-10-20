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
- activity: qualifying exact-session observations advance the marker;
  passive status/usage/idle kinds, other sessions, subscription failure,
  unavailable telemetry, and late callbacks after finalization never do.

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
    AgentSessionNotFoundError,
)
from openorc.adapters.agent_runtime.results import AgentSessionCreationRequest
from openorc.adapters.cline.activity import (
    ClineActivityKind,
    ClineActivityObservation,
    ClineCommentaryActivity,
    ClineFinishActivity,
    ClineIterationActivity,
    ClineNoticeActivity,
    ClineNoticeCategory,
    ClineRuntimeEndedActivity,
    ClineRuntimeErrorActivity,
    ClineRuntimeStatusActivity,
    ClineToolActivity,
    ClineToolCategory,
    ClineTurnFinishReason,
    ClineUsageActivity,
)
from openorc.domain.connections import WorkflowRole
from openorc.domain.executions import ExecutionStatus
from openorc.domain.gates import OwnerGateStatus, OwnerGateType
from openorc.domain.tasks import TaskStatus
from openorc.persistence.pool import DatabasePool
from openorc.protocol.interaction import InteractionKind
from openorc.protocol.models import IMPLEMENTATION_RESULT_FAMILY
from openorc.services.errors import (
    ConflictError,
    ExternalOperationFailedError,
    ExternalOperationUncertainError,
    InvalidCommandError,
    NotFoundError,
    StaleOperationError,
)
from openorc.services.task_authorized_implementation_execution import (
    execute_authorized_implementation,
)
from tests.fakes.agent_runtime import (
    ControlRealization,
    FakeAgentRuntimeAdapter,
    implementation_result_candidate,
    session_ready_candidate,
)

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
    return (_WORKSPACE_ID, owner_profile_id, "platform", _OBSERVED, _OBSERVED, 2, "")


def _task_row(
    *,
    status: str = "implementing",
    state_token: Any = _STATE_TOKEN,
    current_plan_revision_id: Any = _REVISION_ID,
    gate_id: Any = None,
    archived: bool = False,
    workspace_id: Any = _WORKSPACE_ID,
) -> tuple[Any, ...]:
    return (
        _TASK_ID,
        workspace_id,
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
    """Delivers scripted observations to the listener on establishment."""

    def __init__(self, observations: Sequence[ClineActivityObservation] = ()) -> None:
        self._observations = list(observations)
        self.session_ids: list[str] = []
        self.listeners: list[Callable[[ClineActivityObservation], None]] = []
        self.closed = 0

    def __call__(
        self,
        _runtime: Any,  # noqa: ANN401 - the service passes its resolved adapter
        session_id: str,
        listener: Callable[[ClineActivityObservation], None],
    ) -> AbstractContextManager[None]:
        self.session_ids.append(session_id)
        self.listeners.append(listener)
        return self

    def __enter__(self) -> None:
        for observation in self._observations:
            self.listeners[-1](observation)
        return None

    def __exit__(self, *_exc: object) -> bool:
        self.closed += 1
        return False

    def deliver_late(self, observation: ClineActivityObservation) -> None:
        self.listeners[-1](observation)


class _RaisingSubscription:
    """A subscription whose establishment always fails (advisory telemetry)."""

    def __call__(
        self,
        _runtime: Any,  # noqa: ANN401
        _session_id: str,
        _listener: Callable[[ClineActivityObservation], None],
    ) -> AbstractContextManager[None]:
        return self

    def __enter__(self) -> None:
        raise RuntimeError("the activity subscription is unavailable")

    def __exit__(self, *_exc: object) -> bool:
        return False


def _observation(
    kind: ClineActivityKind,
    *,
    session_id: str = _EXTERNAL_SESSION_ID,
    detail: Any = None,  # noqa: ANN401 - typed per the D6c vocabulary
) -> ClineActivityObservation:
    return ClineActivityObservation(session_id=session_id, kind=kind, detail=detail)


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


def _execute_call(
    pool: DatabasePool,
    *,
    producer_runtime_factory: Any,  # noqa: ANN401 - typed at the service boundary
    activity_subscription_factory: Any | None = None,  # noqa: ANN401
    expected_state_token: Any = _STATE_TOKEN,
    profile_id: Any = _OWNER_PROFILE_ID,
    workspace_id: Any = _WORKSPACE_ID,
    task_id: Any = _TASK_ID,
) -> Any:  # noqa: ANN401 - the typed outcome
    return execute_authorized_implementation(
        pool,
        profile_id=profile_id,
        workspace_id=workspace_id,
        task_id=task_id,
        expected_state_token=expected_state_token,
        producer_runtime_factory=producer_runtime_factory,
        activity_subscription_factory=activity_subscription_factory,
    )


_RESULT_CANDIDATE = implementation_result_candidate(
    branch="feat/issue-81",
    summary="Implemented the handoff",
    changes=["feat: handoff"],
    validation=["tests pass"],
)

_TOOL_DETAIL = ClineToolActivity(category=ClineToolCategory.OTHER, tool_name="apply_patch")
_ITERATION_DETAIL = ClineIterationActivity(iteration=2)
_COMMENTARY_DETAIL = ClineCommentaryActivity(text="progress", truncated=False)
_USAGE_DETAIL = ClineUsageActivity(input_tokens=10, output_tokens=5)
_NOTICE_DETAIL = ClineNoticeActivity(category=ClineNoticeCategory.STATUS)
_FINISH_DETAIL = ClineFinishActivity(reason=ClineTurnFinishReason.COMPLETED)
_RUNTIME_ERROR_DETAIL = ClineRuntimeErrorActivity(recoverable=False)
_RUNTIME_STATUS_DETAIL = ClineRuntimeStatusActivity(status="running")
_RUNTIME_ENDED_DETAIL = ClineRuntimeEndedActivity(reason="disposed")

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


def test_the_authorized_handoff_realizes_and_sends_exactly_once_and_finalizes() -> None:
    """E5-approved evidence drives exactly one native realization on the
    original Producer session, exactly one send of the persisted accepted
    envelope, and the one-shot succeeded finalize with matched audit."""
    conn = ScriptedConnection()
    _script_happy_path(conn, complete_row=_COMPLETE_RESULT_ROW)
    pool, runtime, attachment = _arrange(conn, send_outcomes=[_RESULT_CANDIDATE])
    outcome = _execute_call(cast(DatabasePool, pool), producer_runtime_factory=attachment)

    # The typed successful outcome carries the Execution, the validated
    # result, the Task, the Producer binding, and the approved gate.
    assert outcome.execution.status is ExecutionStatus.SUCCEEDED
    assert outcome.execution.execution_number == 1
    assert outcome.execution.implementation_result == dict(_RESULT_CANDIDATE)
    assert outcome.execution.last_qualifying_activity_at == _OBSERVED
    assert outcome.implementation_result.status == "COMPLETED"
    assert outcome.implementation_result.branch == "feat/issue-81"
    assert outcome.task.status is TaskStatus.IMPLEMENTING
    assert outcome.task.state_token == _STATE_TOKEN
    assert outcome.producer_session.id == _SESSION_ID
    assert outcome.owner_gate.status is OwnerGateStatus.APPROVED
    assert outcome.owner_gate.gate_type is OwnerGateType.IMPLEMENTATION_AUTHORIZATION

    # Exactly one native realization on the exact external session ID, with
    # no composed message (native mode realization only), and exactly one
    # formal send carrying the persisted accepted Reviewer envelope.
    (control,) = runtime.realized_controls()
    assert control.external_session_id == _EXTERNAL_SESSION_ID
    assert control.interaction.kind is InteractionKind.IMPLEMENT
    assert control.message is None and control.native_action is True
    (send,) = runtime.submitted_sends()
    assert send.external_session_id == _EXTERNAL_SESSION_ID
    assert send.expected_family == IMPLEMENTATION_RESULT_FAMILY
    assert json.loads(send.message) == {
        "type": "review_result",
        "schema_version": 1,
        "outcome": "accepted",
        "summary": _SUMMARY,
        "findings": [],
    }
    # The single recorded creation request is the harness's fake-session
    # registration; the service itself never allocated a session.
    creations_before = len(runtime.creation_requests())
    assert creations_before == 1
    assert len(runtime.creation_requests()) == creations_before

    # Reservation: exactly one attempt row inserted and claimed; no guarded
    # rewrite of a finalized row anywhere in the flow.
    assert _count(conn, _EXECUTION_INSERT) == 1
    (inserted_params,) = [params for sql, params in conn.executed if _EXECUTION_INSERT(sql, None)]
    assert inserted_params is not None
    assert inserted_params[2] == _SESSION_ID and inserted_params[3] == 1
    assert _count(conn, _EXECUTION_CLAIM) == 1
    assert _count(conn, _EXECUTION_GUARDED_UPDATE) == 0
    (complete_params,) = [params for sql, params in conn.executed if _EXECUTION_COMPLETE(sql, None)]
    assert complete_params is not None
    assert complete_params[0].obj == dict(_RESULT_CANDIDATE)
    assert complete_params[1] == _EXECUTION_ID

    # Matched Producer-attributed start/complete audit in order, and nothing
    # else.
    assert _event_types(conn) == ["execution_started", "execution_completed"]
    (started,) = _event_params_of_type(conn, "execution_started")
    assert started[3] == "producer" and started[4] == str(_SESSION_ID)
    assert started[5] == "execution" and started[6] == _EXECUTION_ID
    (completed,) = _event_params_of_type(conn, "execution_completed")
    assert completed[3] == "producer" and completed[4] == str(_SESSION_ID)
    assert completed[5] == "execution" and completed[6] == _EXECUTION_ID

    # The attachment was acquired and disposed exactly once.
    assert attachment.acquisitions == 1 and attachment.disposals == 1
    assert pool.active_connections == 0


def test_the_send_message_is_the_persisted_acceptance_with_zero_findings() -> None:
    """The delivered envelope preserves the Reviewer's substantive fields
    exactly and carries no instructions, plan body, or extra prompt."""
    conn = ScriptedConnection()
    _script_happy_path(conn, complete_row=_COMPLETE_RESULT_ROW)
    pool, runtime, attachment = _arrange(conn, send_outcomes=[_RESULT_CANDIDATE])
    _execute_call(cast(DatabasePool, pool), producer_runtime_factory=attachment)

    (send,) = runtime.submitted_sends()
    envelope = json.loads(send.message)
    assert envelope["outcome"] == "accepted"
    assert envelope["summary"] == _SUMMARY
    assert envelope["findings"] == []
    assert set(envelope) == {"type", "schema_version", "outcome", "summary", "findings"}


# --- activity marker -------------------------------------------------------------


def test_qualifying_observations_advance_the_marker_and_passive_ones_do_not() -> None:
    """Genuine exact-session activity writes the marker; passive status,
    usage, ended notices, and other-session observations never do."""
    conn = ScriptedConnection()
    _script_happy_path(conn, complete_row=_COMPLETE_RESULT_ROW, with_activity_writes=True)
    pool, runtime, attachment = _arrange(conn, send_outcomes=[_RESULT_CANDIDATE])
    subscription = _ScriptedActivitySubscription(
        [
            _observation(ClineActivityKind.TOOL_STARTED, detail=_TOOL_DETAIL),
            _observation(ClineActivityKind.USAGE_OBSERVED, detail=_USAGE_DETAIL),
            _observation(ClineActivityKind.RUNTIME_STATUS, detail=_RUNTIME_STATUS_DETAIL),
            _observation(ClineActivityKind.TURN_FINISHED, detail=_FINISH_DETAIL),
            _observation(
                ClineActivityKind.TOOL_STARTED,
                session_id="another-session",
                detail=_TOOL_DETAIL,
            ),
        ]
    )
    outcome = _execute_call(
        cast(DatabasePool, pool),
        producer_runtime_factory=attachment,
        activity_subscription_factory=subscription,
    )

    # Only the one qualifying TOOL_STARTED observation produced a write, on
    # the exact bound session.
    assert _count(conn, _EXECUTION_ACTIVITY) == 1
    assert subscription.session_ids == [_EXTERNAL_SESSION_ID]
    assert subscription.closed == 1
    assert outcome.execution.last_qualifying_activity_at == _OBSERVED


def test_the_full_qualifying_activity_vocabulary_writes_and_coalesces() -> None:
    """Thinking, iteration/work polling, commentary, and tool lifecycle
    qualify; notices, runtime errors/status/ended never do; noisy bursts
    coalesce without losing the marker."""
    conn = ScriptedConnection()
    _script_happy_path(conn, complete_row=_COMPLETE_RESULT_ROW, with_activity_writes=True)
    pool, runtime, attachment = _arrange(conn, send_outcomes=[_RESULT_CANDIDATE])
    subscription = _ScriptedActivitySubscription(
        [
            _observation(ClineActivityKind.THINKING_STARTED),
            _observation(ClineActivityKind.ITERATION_STARTED, detail=_ITERATION_DETAIL),
            _observation(ClineActivityKind.COMMENTARY, detail=_COMMENTARY_DETAIL),
            _observation(
                ClineActivityKind.TOOL_FAILED,
                detail=ClineToolActivity(category=ClineToolCategory.OTHER),
            ),
            _observation(ClineActivityKind.ITERATION_ENDED, detail=_ITERATION_DETAIL),
            _observation(ClineActivityKind.THINKING_ENDED),
            _observation(ClineActivityKind.NOTICE, detail=_NOTICE_DETAIL),
            _observation(ClineActivityKind.RUNTIME_ERROR, detail=_RUNTIME_ERROR_DETAIL),
            _observation(ClineActivityKind.RUNTIME_ENDED, detail=_RUNTIME_ENDED_DETAIL),
        ]
    )
    _execute_call(
        cast(DatabasePool, pool),
        producer_runtime_factory=attachment,
        activity_subscription_factory=subscription,
    )

    # Six qualifying observations coalesce to a bounded number of writes,
    # but never zero; the passive kinds wrote nothing.
    assert 1 <= _count(conn, _EXECUTION_ACTIVITY) <= 6


def test_unavailable_or_failed_telemetry_never_gates_the_attempt() -> None:
    """No subscription at all, and a subscription that fails to establish,
    both leave the authorized handoff fully intact."""
    for activity_factory in (None, _RaisingSubscription()):
        conn = ScriptedConnection()
        _script_happy_path(conn, complete_row=_COMPLETE_RESULT_ROW)
        pool, runtime, attachment = _arrange(conn, send_outcomes=[_RESULT_CANDIDATE])
        outcome = _execute_call(
            cast(DatabasePool, pool),
            producer_runtime_factory=attachment,
            activity_subscription_factory=activity_factory,
        )
        assert outcome.execution.status is ExecutionStatus.SUCCEEDED
        assert _count(conn, _EXECUTION_ACTIVITY) == 0


def test_late_callbacks_after_finalize_cannot_write_history() -> None:
    """A late observation delivered after the service returned is advisory:
    its conditional write is a rejected no-op against the finalized row and
    never raises into the attempt."""
    conn = ScriptedConnection()
    _script_happy_path(conn, complete_row=_COMPLETE_RESULT_ROW)
    pool, runtime, attachment = _arrange(conn, send_outcomes=[_RESULT_CANDIDATE])
    subscription = _ScriptedActivitySubscription()
    _execute_call(
        cast(DatabasePool, pool),
        producer_runtime_factory=attachment,
        activity_subscription_factory=subscription,
    )
    events_before = _event_types(conn)

    # The late conditional write applies no row (the status is no longer
    # running); the listener swallows the rejection either way.
    conn.on_repeat(_EXECUTION_ACTIVITY, None)
    subscription.deliver_late(_observation(ClineActivityKind.TOOL_STARTED, detail=_TOOL_DETAIL))

    assert _event_types(conn) == events_before
    assert _count(conn, _EXECUTION_GUARDED_UPDATE) == 0


# --- authority/currentness refusals before any effect ----------------------------


def _refused_before_effects(
    conn: ScriptedConnection,
    error_type: type[Exception],
    **call_kwargs: Any,
) -> None:
    """Run a refusal case and prove nothing effectful happened."""
    pool, runtime, attachment = _arrange(conn)
    call_kwargs.setdefault("producer_runtime_factory", attachment)
    with pytest.raises(error_type):
        _execute_call(cast(DatabasePool, pool), **call_kwargs)
    # No attempt row was ever reserved or claimed, and no event was written.
    assert _count(conn, _EXECUTION_INSERT) == 0
    assert _count(conn, _EXECUTION_CLAIM) == 0
    assert _event_types(conn) == []
    assert attachment.acquisitions == 0
    assert runtime.realized_controls() == () and runtime.submitted_sends() == ()


def test_stale_token_and_archived_tasks_refuse_as_stale_operations() -> None:
    for task_row in (
        _task_row(state_token=_TOKEN_2),
        _task_row(archived=True, status="cancelled"),
    ):
        conn = ScriptedConnection()
        _script_admission(conn, task_row=task_row)
        _refused_before_effects(conn, StaleOperationError)


def test_wrong_status_and_current_gate_pointer_refuse_as_conflicts() -> None:
    for task_row in (
        _task_row(status="waiting_for_owner", gate_id=_GATE_ID),
        _task_row(status="planning"),
        _task_row(current_plan_revision_id=None),
    ):
        conn = ScriptedConnection()
        _script_admission(conn, task_row=task_row)
        _refused_before_effects(conn, ConflictError)


def test_foreign_workspace_and_non_owner_callers_are_uniformly_refused() -> None:
    conn = ScriptedConnection()
    _script_admission(conn, task_row=_task_row(workspace_id=_OTHER_WORKSPACE_ID))
    _refused_before_effects(conn, NotFoundError)

    conn = ScriptedConnection()
    _script_repeating_reads(conn)
    _refused_before_effects(conn, NotFoundError, profile_id=uuid.uuid4())


def test_gate_evidence_refusals_fail_closed() -> None:
    """Reviewer ACCEPTED without Owner approval, superseded pending history,
    and a gate bound to another revision never authorize implementation."""
    # No approved gate at all (Reviewer acceptance alone is never authority).
    conn = ScriptedConnection()
    _script_admission(conn, gates=[])
    _refused_before_effects(conn, ConflictError)

    # The #219-superseded gate stays pending history and is never approvable.
    conn = ScriptedConnection()
    _script_admission(conn, gates=[_gate_row(status="pending")])
    _refused_before_effects(conn, ConflictError)

    # An approved gate bound to a different revision is the wrong subject.
    conn = ScriptedConnection()
    _script_admission(conn, gates=[_gate_row(plan_revision_id=_OTHER_REVISION_ID)])
    _refused_before_effects(conn, ConflictError)


def test_review_evidence_refusals_fail_closed() -> None:
    # No closed planning loop following the approval.
    conn = ScriptedConnection()
    _script_admission(conn, loops=[])
    _refused_before_effects(conn, ConflictError)

    # The latest finalized iteration for the revision is not an acceptance.
    conn = ScriptedConnection()
    _script_admission(
        conn,
        iterations=[
            _iteration_row(
                outcome="changes_requested",
                summary="Needs another pass",
                findings=[{"summary": "Clarify", "details": "More."}],
            )
        ],
    )
    _refused_before_effects(conn, ConflictError)

    # A finalized iteration for another revision does not clear this one.
    conn = ScriptedConnection()
    _script_admission(conn, iterations=[_iteration_row(plan_revision_id=_OTHER_REVISION_ID)])
    _refused_before_effects(conn, ConflictError)


def test_any_prior_attempt_history_refuses_the_initial_dispatch() -> None:
    """Concurrent/replayed starts, earlier successes, unresolved dispatches,
    and older failed attempts alike never earn a second initial turn."""
    for status in ("succeeded", "running", "failed_transient", "cancelled"):
        conn = ScriptedConnection()
        _script_admission(conn, executions=[_execution_row(status=status)])
        _refused_before_effects(conn, ConflictError)


def test_malformed_commands_fail_closed_before_any_io() -> None:
    pool, _runtime, attachment = _arrange(ScriptedConnection())
    for kwargs in (
        {"task_id": "not-a-uuid"},
        {"expected_state_token": "not-a-uuid"},
        {"profile_id": "not-a-uuid"},
        {"workspace_id": "not-a-uuid"},
    ):
        with pytest.raises(InvalidCommandError):
            _execute_call(cast(DatabasePool, pool), producer_runtime_factory=attachment, **kwargs)
    with pytest.raises(InvalidCommandError):
        _execute_call(cast(DatabasePool, pool), producer_runtime_factory="not-callable")
    with pytest.raises(InvalidCommandError):
        _execute_call(
            cast(DatabasePool, pool),
            producer_runtime_factory=attachment,
            activity_subscription_factory="not-callable",
        )


# --- authority changing mid-flight (review-finding regressions) -------------------


def test_authority_change_during_attachment_finalizes_failed_without_effects() -> None:
    """R1 regression: the Task moving on while the attachment is acquired
    refuses the realization, finalizes the never-dispatched attempt
    ``failed_final`` with its safe audit, and never resends."""
    conn = ScriptedConnection()
    _script_admission(conn)
    # R1: the Task was cancelled/archived while the attachment was acquired.
    conn.on(_TASK_READ_LOCKED, _task_row(archived=True, status="cancelled"))
    conn.on(_TASK_READ, _task_row(archived=True, status="cancelled"))
    conn.on(_EXECUTION_GUARDED_UPDATE, _execution_row(status="failed_final"))
    pool, runtime, attachment = _arrange(conn)

    with pytest.raises(StaleOperationError):
        _execute_call(cast(DatabasePool, pool), producer_runtime_factory=attachment)

    assert runtime.realized_controls() == () and runtime.submitted_sends() == ()
    assert attachment.disposals == 1
    assert _count(conn, _EXECUTION_GUARDED_UPDATE) == 1
    (update_params,) = [
        params for sql, params in conn.executed if _EXECUTION_GUARDED_UPDATE(sql, None)
    ]
    assert update_params == ("failed_final", _EXECUTION_ID, "running")
    assert _event_types(conn) == ["execution_started", "execution_failed"]
    (failed,) = _event_params_of_type(conn, "execution_failed")
    assert failed[3] == "producer" and failed[4] == str(_SESSION_ID)
    assert failed[5] == "execution" and failed[6] == _EXECUTION_ID
    assert failed[7].obj == {"failure_kind": "authority_stale"}


def test_authority_change_during_reconstruction_refuses_the_send() -> None:
    """R2 regression: the subject set moving on after the ACT reconstruction
    refuses the send, leaves the attempt conservatively running, and never
    resends."""
    conn = ScriptedConnection()
    _script_admission(conn)
    _script_subject_reads(conn)  # R1 passes
    # R2: the plan moved on during the ACT reconstruction.
    conn.on(_TASK_READ_LOCKED, _task_row(current_plan_revision_id=_OTHER_REVISION_ID))
    conn.on(_TASK_READ, _task_row(current_plan_revision_id=_OTHER_REVISION_ID))
    conn.on(_REVISION_READ, _revision_row())
    pool, runtime, attachment = _arrange(conn)

    with pytest.raises(ExternalOperationUncertainError):
        _execute_call(cast(DatabasePool, pool), producer_runtime_factory=attachment)

    # The realization ran (an effect may have begun), but no message was sent.
    assert len(runtime.realized_controls()) == 1
    assert runtime.submitted_sends() == ()
    # The attempt stays conservatively running: no finalize, no failure audit.
    assert _count(conn, _EXECUTION_GUARDED_UPDATE) == 0
    assert _count(conn, _EXECUTION_COMPLETE) == 0
    assert _event_types(conn) == ["execution_started"]


# --- known pre-effect runtime failures -------------------------------------------


def test_known_attachment_failure_finalizes_failed_transient_with_audit() -> None:
    conn = ScriptedConnection()
    _script_admission(conn)
    conn.on(_EXECUTION_GUARDED_UPDATE, _execution_row(status="failed_transient"))
    pool, _runtime, _attachment = _arrange(conn)

    with pytest.raises(ExternalOperationFailedError):
        _execute_call(
            cast(DatabasePool, pool),
            producer_runtime_factory=_FailingAttachment(
                ExternalOperationFailedError("the cline runtime attachment failed")
            ),
        )

    (update_params,) = [
        params for sql, params in conn.executed if _EXECUTION_GUARDED_UPDATE(sql, None)
    ]
    assert update_params == ("failed_transient", _EXECUTION_ID, "running")
    assert _event_types(conn) == ["execution_started", "execution_failed"]
    (failed,) = _event_params_of_type(conn, "execution_failed")
    assert failed[7].obj == {"failure_kind": "runtime_pre_effect"}


def test_known_realization_failure_finalizes_failed_transient_without_a_send() -> None:
    conn = ScriptedConnection()
    _script_admission(conn)
    _script_subject_reads(conn)  # R1 passes
    conn.on(_EXECUTION_GUARDED_UPDATE, _execution_row(status="failed_transient"))
    pool, runtime, attachment = _arrange(
        conn,
        realize_error=AgentRuntimeUnavailableError("the runtime is unreachable"),
    )

    with pytest.raises(ExternalOperationFailedError):
        _execute_call(cast(DatabasePool, pool), producer_runtime_factory=attachment)

    assert runtime.submitted_sends() == ()
    (update_params,) = [
        params for sql, params in conn.executed if _EXECUTION_GUARDED_UPDATE(sql, None)
    ]
    assert update_params == ("failed_transient", _EXECUTION_ID, "running")
    (failed,) = _event_params_of_type(conn, "execution_failed")
    assert failed[7].obj == {"failure_kind": "runtime_pre_effect"}


def test_uncertain_realization_leaves_the_attempt_unresolved() -> None:
    conn = ScriptedConnection()
    _script_admission(conn)
    _script_subject_reads(conn)  # R1 passes
    pool, runtime, attachment = _arrange(
        conn,
        realize_error=AgentRuntimeTimeoutError("the reconstruction exceeded its deadline"),
    )

    with pytest.raises(ExternalOperationUncertainError):
        _execute_call(cast(DatabasePool, pool), producer_runtime_factory=attachment)

    assert runtime.submitted_sends() == ()
    assert _count(conn, _EXECUTION_GUARDED_UPDATE) == 0
    assert _event_types(conn) == ["execution_started"]


# --- uncertain send outcomes and invalid completions ------------------------------


def test_send_uncertainty_after_possible_dispatch_never_resends_or_finalizes() -> None:
    """Timeout, mid-send session loss, protocol failure, and a wrong-family
    reply all leave the attempt conservatively running with no failure
    audit and no completion event."""
    for outcome in (
        AgentRuntimeTimeoutError("the send exceeded its deadline"),
        AgentSessionNotFoundError("the exact addressed session is gone"),
        "this is not formal JSON at all",
        '{"type": "plan_result", "schema_version": 1, "plan": "wrong family"}',
    ):
        conn = ScriptedConnection()
        _script_happy_path(conn)
        pool, runtime, attachment = _arrange(conn, send_outcomes=[outcome])

        with pytest.raises(ExternalOperationUncertainError):
            _execute_call(cast(DatabasePool, pool), producer_runtime_factory=attachment)

        # The one send was attempted; nothing finalized it afterwards.
        assert len(runtime.submitted_sends()) == 1
        assert _count(conn, _EXECUTION_GUARDED_UPDATE) == 0
        assert _count(conn, _EXECUTION_COMPLETE) == 0
        assert _event_types(conn) == ["execution_started"]


# --- finalize currentness and duplicate races -------------------------------------


def test_a_stale_finalize_subject_never_fabricates_an_outcome() -> None:
    """Authority moving on between the validated result and the finalize
    leaves the attempt conservatively running for recovery."""
    conn = ScriptedConnection()
    _script_admission(conn)
    _script_subject_reads(conn)  # R1
    _script_subject_reads(conn)  # R2
    # Finalize: the token rotated underneath the completed send.
    conn.on(_TASK_READ_LOCKED, _task_row(state_token=_TOKEN_2))
    conn.on(_TASK_READ, _task_row(state_token=_TOKEN_2))
    pool, runtime, attachment = _arrange(conn, send_outcomes=[_RESULT_CANDIDATE])

    with pytest.raises(StaleOperationError):
        _execute_call(cast(DatabasePool, pool), producer_runtime_factory=attachment)

    assert len(runtime.submitted_sends()) == 1
    assert _count(conn, _EXECUTION_COMPLETE) == 0
    assert _count(conn, _EXECUTION_GUARDED_UPDATE) == 0
    assert _event_types(conn) == ["execution_started"]


def test_a_raced_duplicate_finalize_is_an_error_without_extra_events() -> None:
    """A concurrent finalize wins the conditional write; the losing finalize
    is a stale error and records no second completion event."""
    conn = ScriptedConnection()
    _script_happy_path(conn, finalize_rejects=True)
    pool, runtime, attachment = _arrange(conn, send_outcomes=[_RESULT_CANDIDATE])

    with pytest.raises(StaleOperationError):
        _execute_call(cast(DatabasePool, pool), producer_runtime_factory=attachment)

    assert len(runtime.submitted_sends()) == 1
    assert _count(conn, _EXECUTION_COMPLETE) == 1  # attempted, rejected as no-op
    assert _event_types(conn) == ["execution_started"]
