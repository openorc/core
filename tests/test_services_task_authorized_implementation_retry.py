"""Deterministic tests for the F5c authorized implementation retry (issue #263).

A scripted SQL-connection seam proves the retry seam on its critical paths:

- one authorized, due, exact-current retry after a proven pre-effect failure
  reserves Execution #2, binds its F4 attempt set-once, and records the
  Producer-attributed started audit inside the same admission composition,
  then reuses the shared E6 external mechanics: one native ACT realization on
  the original Producer external session ID and exactly one send carrying the
  persisted accepted Reviewer envelope, finalized ``succeeded`` with the
  matched audit — the new row only, and the invocation's attempt evidence
  names the NEW retry Execution identity (never the episode's original);
- the refusal table refuses before any effect: premature wakeup (the
  persisted ``not_before`` enforced at actual admission), unknown/foreign
  authorization, terminally disposed episode, replayed/already-consumed
  authorization, non-latest or unconsumed-sibling chain inconsistency,
  superseded gate/plan/loop/iteration evidence, stale token, archived Task,
  wrong status, current gate pointer, non-READY or changed Producer session
  (chain and binding), non-Owner caller, and predecessor failures that are
  not positively proven pre-effect (running, succeeded, disagreeing or
  missing ``runtime_pre_effect`` audit), with no reservation, claim, binding,
  audit, or adapter acquisition;
- failure discipline identical to E6: a proven pre-effect runtime failure
  finalizes the NEW row ``failed_transient`` with its safe
  ``execution_failed`` audit; an uncertain send leaves the new row
  conservatively ``running``; authority moving on during attachment finalizes
  ``failed_final`` with the authority-stale kind. The consumed authorization
  is never re-driven by a replay.

No live GitHub, runtime, or Postgres access; the new reservation/consumption
seam is proven by the integration-marked suite against a real Postgres.
"""

from __future__ import annotations

import json
import uuid
from collections.abc import Callable, Iterator, Sequence
from contextlib import AbstractContextManager, contextmanager
from datetime import UTC, datetime, timedelta
from typing import Any, Final, cast

import pytest

from openorc.adapters.agent_runtime.errors import (
    AgentRuntimeTimeoutError,
    AgentRuntimeUnavailableError,
)
from openorc.adapters.agent_runtime.results import AgentSessionCreationRequest
from openorc.domain.connections import WorkflowRole
from openorc.domain.executions import ExecutionStatus
from openorc.domain.gates import OwnerGateStatus, OwnerGateType
from openorc.domain.operational_failures import OriginalAttemptKind
from openorc.domain.pending_interactions import PendingTaskRuntimeInteractionOperation
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
from openorc.services.operation_attempt_evidence import InvocationAttemptEvidence
from openorc.services.task_authorized_implementation_retry import (
    execute_authorized_implementation_retry,
)
from tests.fakes.agent_runtime import (
    ControlRealization,
    FakeAgentRuntimeAdapter,
    implementation_result_candidate,
    session_ready_candidate,
)

_OBSERVED = datetime(2026, 10, 20, 12, 0, 0, tzinfo=UTC)
_DUE = _OBSERVED - timedelta(seconds=1)
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
_OTHER_SESSION_ID = uuid.uuid4()
_CONNECTION_ID = uuid.uuid4()
_EPISODE_ID = uuid.uuid4()
_ATTEMPT_ID = uuid.uuid4()
_OTHER_ATTEMPT_ID = uuid.uuid4()
_EXECUTION_ID = uuid.uuid4()
_RETRY_EXECUTION_ID = uuid.uuid4()
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
        raise AssertionError("authorized retry tests never close pools")


def _pool(conn: ScriptedConnection) -> DatabasePool:
    return cast(DatabasePool, FakePool(conn))


# --- SQL predicates (service call order) -----------------------------------------

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
_EXECUTION_FOR_UPDATE = _sql_has("from openorc.executions where id", "for update")
_EXECUTION_INSERT = _sql_has("insert into openorc.executions")
_EXECUTION_CLAIM = _sql_has("update openorc.executions", "set status = 'running'")
_EXECUTION_COMPLETE = _sql_has("update openorc.executions", "set status = 'succeeded'")
_EXECUTION_GUARDED_UPDATE = _sql_has("update openorc.executions", "status = %s")
_ATTEMPT_READ = _sql_lacks(_sql_has("from openorc.operational_failure_attempts where id"), "update")
_EPISODE_READ = _sql_has("from openorc.operational_failure_episodes where id")
_ATTEMPTS_LIST = _sql_has("from openorc.operational_failure_attempts where episode_id")
_BIND_UPDATE = _sql_has(
    "update openorc.operational_failure_attempts", "resulting_execution_id = %s"
)
_EVENTS_BY_SUBJECT = _sql_has(
    "from openorc.workflow_events", "subject_type = %s", "subject_id = %s"
)
_EVENT_INSERT = _sql_has("insert into openorc.workflow_events")


# --- durable row shapes -----------------------------------------------------------


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
        None,
        None,
    )


def _revision_row(*, revision_id: Any = _REVISION_ID) -> tuple[Any, ...]:
    return (revision_id, _WORKSPACE_ID, _TASK_ID, 1, _PLAN_TEXT, _BASE_SHA, _OBSERVED)


def _loop_row(
    *, loop_id: Any = _LOOP_ID, status: str = "closed", closed_at: Any = _OBSERVED
) -> tuple[Any, ...]:
    return (loop_id, _WORKSPACE_ID, _TASK_ID, "planning", 2, status, closed_at, _OBSERVED)


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
        findings = []
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


def _ended_session_row() -> tuple[Any, ...]:
    """A coherent ENDED Producer binding (initialized, then terminated)."""
    return (
        _SESSION_ID,
        _WORKSPACE_ID,
        _TASK_ID,
        "producer",
        _CONNECTION_ID,
        _EXTERNAL_SESSION_ID,
        "ended",
        {},
        None,
        None,
        None,
        _OBSERVED,
        _OBSERVED,
        _OBSERVED,
        _OBSERVED,
        None,
        None,
    )


def _execution_row(
    *,
    execution_id: Any = _EXECUTION_ID,
    session_id: Any = _SESSION_ID,
    number: int = 1,
    status: str = "queued",
    implementation_result: Any = None,
    last_activity: Any = None,
) -> tuple[Any, ...]:
    return (
        execution_id,
        _WORKSPACE_ID,
        _TASK_ID,
        session_id,
        number,
        status,
        _OBSERVED,
        _OBSERVED,
        implementation_result,
        last_activity,
        None,
    )


def _episode_row(
    *,
    episode_id: Any = _EPISODE_ID,
    family: str = "implementation_execution",
    original_kind: str = "execution",
    original_attempt_id: Any = _EXECUTION_ID,
    escalated: Any = None,
) -> tuple[Any, ...]:
    return (
        episode_id,
        _WORKSPACE_ID,
        _TASK_ID,
        family,
        original_kind,
        original_attempt_id,
        {"workspace_id": str(_WORKSPACE_ID), "task_id": str(_TASK_ID)},
        escalated,
        _OBSERVED,
    )


def _attempt_row(
    *,
    attempt_id: Any = _ATTEMPT_ID,
    workspace_id: Any = _WORKSPACE_ID,
    episode_id: Any = _EPISODE_ID,
    ordinal: int = 1,
    source_kind: str = "original",
    source_attempt_id: Any = _EXECUTION_ID,
    not_before: Any = _DUE,
    resulting_execution_id: Any = None,
) -> tuple[Any, ...]:
    return (
        attempt_id,
        workspace_id,
        _TASK_ID,
        episode_id,
        ordinal,
        source_kind,
        source_attempt_id,
        "runtime_unavailable",
        not_before,
        resulting_execution_id,
        _OBSERVED,
    )


def _event_row(
    *,
    event_type: str = "execution_started",
    execution_id: Any = _RETRY_EXECUTION_ID,
    context: Any = None,
) -> tuple[Any, ...]:
    if context is None:
        context = {}
    return (
        uuid.uuid4(),
        _WORKSPACE_ID,
        _TASK_ID,
        event_type,
        "producer",
        str(_SESSION_ID),
        "execution",
        execution_id,
        context,
        _OBSERVED,
    )


def _pre_effect_failed_event_row(*, execution_id: Any = _EXECUTION_ID) -> tuple[Any, ...]:
    return _event_row(
        event_type="execution_failed",
        execution_id=execution_id,
        context={"failure_kind": "runtime_pre_effect"},
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


# --- runtime, attachment fakes ----------------------------------------------------


class _ProbedSendAdapter(FakeAgentRuntimeAdapter):
    """Fake runtime proving every runtime call runs with no transaction open."""

    def __init__(self, pool: FakePool) -> None:
        super().__init__()
        self._execution_pool = pool

    def _probe(self) -> None:
        assert self._execution_pool.active_connections == 0, (
            "the service must never hold a database transaction open across a runtime call"
        )

    def send(  # type: ignore[override]
        self,
        session_id: str,
        message: str,
        *,
        expected_family: str | None = None,
    ) -> Any:  # noqa: ANN401 - mirrors the #67 contract return
        self._probe()
        return super().send(session_id, message, expected_family=expected_family)


class _ScriptedRealizeRuntime(_ProbedSendAdapter):
    """Adds a scripted one-shot realization error to the probe runtime."""

    def __init__(self, pool: FakePool) -> None:
        super().__init__(pool)
        self._realize_error: Exception | None = None

    def queue_realize_error(self, error: Exception) -> None:
        self._realize_error = error

    def realize_control(  # type: ignore[override]
        self,
        session_id: str,
        interaction: Any,  # noqa: ANN401 - mirrors the #67 contract parameter
        *,
        message: str | None = None,
        expected_family: str | None = None,
    ) -> Any:  # noqa: ANN401 - mirrors the #67 contract return
        self._probe()
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


def _prepared_adapter(
    pool: FakePool,
    *,
    send_outcomes: Sequence[Any] = (),
    realize_error: Exception | None = None,
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


def _count(conn: ScriptedConnection, predicate: _Predicate) -> int:
    return sum(1 for sql, _params in conn.executed if predicate(sql, None))


# --- scripted sequences (in service call order) ----------------------------------


def _script_repeating_reads(conn: ScriptedConnection) -> None:
    """Same-shaped reads whose result never changes across the whole flow."""
    conn.on_repeat(_sql_has("from openorc.profiles", "for key share"), (None, None, None))
    conn.on_repeat(_sql_has("from openorc.workspaces where id"), _ws_row())
    conn.on_repeat(_EVENT_INSERT, _event_row())


_UNSET: Final = object()


def _script_subject_reads(
    conn: ScriptedConnection,
    *,
    task_row: Any = _UNSET,
    session_row: Any = _UNSET,
    gates: Any = _UNSET,
    loops: Any = _UNSET,
    iterations: Any = _UNSET,
) -> None:
    """One subject-revalidation read block in service call order (R1/R2/finalize)."""
    current = task_row if task_row is not _UNSET else _task_row()
    conn.on(_TASK_READ_LOCKED, current)
    conn.on(_TASK_READ, current)
    conn.on(_REVISION_READ, _revision_row())
    conn.on(_GATES_LIST, gates if gates is not _UNSET else [_gate_row()])
    conn.on(_LOOPS_LIST, loops if loops is not _UNSET else [_loop_row()])
    conn.on(_ITERATIONS_LIST, iterations if iterations is not _UNSET else [_iteration_row()])
    conn.on(_SESSION_READ, session_row if session_row is not _UNSET else _session_row())


def _script_admission(
    conn: ScriptedConnection,
    *,
    task_row: Any = _UNSET,
    session_row: Any = _UNSET,
    gates: Any = _UNSET,
    loops: Any = _UNSET,
    iterations: Any = _UNSET,
    history: Any = _UNSET,
    attempt: Any = _UNSET,
    episode: Any = _UNSET,
    attempts: Any = _UNSET,
    predecessor: Any = _UNSET,
    corroboration: Any = _UNSET,
    bind_result: Any = _UNSET,
) -> None:
    """The admission/consumption transaction in exact service call order."""
    _script_repeating_reads(conn)
    current = task_row if task_row is not _UNSET else _task_row()
    conn.on(_TASK_READ, current)  # workspace guard task read
    conn.on(_TASK_READ_LOCKED, current)  # get_task_for_update
    conn.on(_TASK_READ, current)  # require_current_task
    conn.on(_ATTEMPT_READ, attempt if attempt is not _UNSET else _attempt_row())
    conn.on(_EPISODE_READ, episode if episode is not _UNSET else _episode_row())
    conn.on(_ATTEMPTS_LIST, attempts if attempts is not _UNSET else [_attempt_row()])
    conn.on(
        _EXECUTION_FOR_UPDATE,
        predecessor if predecessor is not _UNSET else _execution_row(status="failed_transient"),
    )
    conn.on(
        _EXECUTIONS_LIST,
        history if history is not _UNSET else [_execution_row(status="failed_transient")],
    )
    conn.on(_REVISION_READ, _revision_row())  # current-plan guard
    conn.on(_GATES_LIST, gates if gates is not _UNSET else [_gate_row()])
    conn.on(_LOOPS_LIST, loops if loops is not _UNSET else [_loop_row()])
    conn.on(_ITERATIONS_LIST, iterations if iterations is not _UNSET else [_iteration_row()])
    conn.on(_SESSION_READ, session_row if session_row is not _UNSET else _session_row())
    conn.on(
        _EVENTS_BY_SUBJECT,
        corroboration if corroboration is not _UNSET else [_pre_effect_failed_event_row()],
    )
    conn.on(_SESSION_ROLE_READ, ("producer",))
    conn.on(_EXECUTION_INSERT, _execution_row(execution_id=_RETRY_EXECUTION_ID, number=2))
    conn.on(
        _EXECUTION_CLAIM,
        _execution_row(
            execution_id=_RETRY_EXECUTION_ID, number=2, status="running", last_activity=_OBSERVED
        ),
    )
    conn.on(
        _BIND_UPDATE,
        bind_result
        if bind_result is not _UNSET
        else _attempt_row(resulting_execution_id=_RETRY_EXECUTION_ID),
    )


_RESULT_CANDIDATE = implementation_result_candidate(
    branch="feat/issue-81",
    summary="Implemented the handoff",
    changes=["feat: handoff"],
    validation=["tests pass"],
)

_COMPLETE_RESULT_ROW = _execution_row(
    execution_id=_RETRY_EXECUTION_ID,
    number=2,
    status="succeeded",
    implementation_result=dict(_RESULT_CANDIDATE),
    last_activity=_OBSERVED,
)


def _script_success_path(
    conn: ScriptedConnection,
    *,
    complete_row: tuple[Any, ...] | None = None,
) -> None:
    """The full admission + R1 + R2 + finalize flow in call order."""
    _script_admission(conn)
    _script_subject_reads(conn)  # R1
    _script_subject_reads(conn)  # R2
    _script_subject_reads(conn)  # finalize
    conn.on(_EXECUTION_COMPLETE, complete_row if complete_row is not None else _COMPLETE_RESULT_ROW)


def _arrange(
    conn: ScriptedConnection,
    *,
    send_outcomes: Sequence[Any] = (),
    realize_error: Exception | None = None,
) -> tuple[FakePool, _ScriptedRealizeRuntime, _ProducerAttachment]:
    """One scripted case: a single shared pool, runtime, and attachment probe."""
    pool = FakePool(conn)
    runtime = _prepared_adapter(pool, send_outcomes=send_outcomes, realize_error=realize_error)
    attachment = _ProducerAttachment(runtime)
    return pool, runtime, attachment


def _retry_call(
    pool: DatabasePool,
    *,
    producer_runtime_factory: Any,  # noqa: ANN401 - typed at the service boundary
    attempt_evidence: InvocationAttemptEvidence | None = None,
    clock: Callable[[], datetime] | None = None,
    expected_state_token: Any = _STATE_TOKEN,
    profile_id: Any = _OWNER_PROFILE_ID,
    workspace_id: Any = _WORKSPACE_ID,
    task_id: Any = _TASK_ID,
    retry_attempt_id: Any = _ATTEMPT_ID,
) -> Any:  # noqa: ANN401 - the typed outcome
    return execute_authorized_implementation_retry(
        pool,
        profile_id=profile_id,
        workspace_id=workspace_id,
        task_id=task_id,
        expected_state_token=expected_state_token,
        retry_attempt_id=retry_attempt_id,
        producer_runtime_factory=producer_runtime_factory,
        attempt_evidence=attempt_evidence,
        clock=clock,
    )


def _refused_before_effects(
    conn: ScriptedConnection,
    error_type: type[Exception],
    **call_kwargs: Any,
) -> None:
    """Run a refusal case and prove nothing effectful happened."""
    pool, runtime, attachment = _arrange(conn)
    call_kwargs.setdefault("producer_runtime_factory", attachment)
    with pytest.raises(error_type):
        _retry_call(cast(DatabasePool, pool), **call_kwargs)
    # No attempt row was reserved or claimed, no authorization was bound,
    # no audit was written, and the adapter was never acquired.
    assert _count(conn, _EXECUTION_INSERT) == 0
    assert _count(conn, _EXECUTION_CLAIM) == 0
    assert _count(conn, _BIND_UPDATE) == 0
    assert _event_types(conn) == []
    assert attachment.acquisitions == 0
    assert runtime.realized_controls() == () and runtime.submitted_sends() == ()


# --- manual-flow fidelity --------------------------------------------------------


def test_the_authorized_due_retry_reserves_binds_and_completes_exactly_once() -> None:
    """One due authorized retry reserves Execution #2, binds its F4 attempt
    set-once inside the same admission composition with the started audit,
    then realizes and sends exactly once on the original Producer session and
    finalizes only the new row."""
    conn = ScriptedConnection()
    _script_success_path(conn, complete_row=_COMPLETE_RESULT_ROW)
    pool, runtime, attachment = _arrange(conn, send_outcomes=[_RESULT_CANDIDATE])
    evidence = InvocationAttemptEvidence()
    outcome = _retry_call(
        cast(DatabasePool, pool),
        producer_runtime_factory=attachment,
        attempt_evidence=evidence,
        clock=lambda: _OBSERVED,
    )

    # The typed successful outcome carries the NEW Execution (number 2), the
    # validated result, the Task, the Producer binding, and the approved gate.
    assert outcome.execution.id == _RETRY_EXECUTION_ID
    assert outcome.execution.status is ExecutionStatus.SUCCEEDED
    assert outcome.execution.execution_number == 2
    assert outcome.execution.implementation_result == dict(_RESULT_CANDIDATE)
    assert outcome.task.status is TaskStatus.IMPLEMENTING
    assert outcome.task.state_token == _STATE_TOKEN
    assert outcome.producer_session.id == _SESSION_ID
    assert outcome.owner_gate.status is OwnerGateStatus.APPROVED
    assert outcome.owner_gate.gate_type is OwnerGateType.IMPLEMENTATION_AUTHORIZATION

    # Exactly one native realization on the exact external session ID and one
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
    assert len(runtime.creation_requests()) == 1  # the harness's fake registration only

    # Reservation: exactly one attempt row inserted (number 2) and claimed;
    # the F4 authorization was bound set-once to that exact Execution; no
    # guarded rewrite of any finalized row happened anywhere in the flow.
    assert _count(conn, _EXECUTION_INSERT) == 1
    (inserted_params,) = [params for sql, params in conn.executed if _EXECUTION_INSERT(sql, None)]
    assert inserted_params is not None
    assert inserted_params[2] == _SESSION_ID and inserted_params[3] == 2
    assert _count(conn, _EXECUTION_CLAIM) == 1
    assert _count(conn, _EXECUTION_GUARDED_UPDATE) == 0
    (bound_params,) = [params for sql, params in conn.executed if _BIND_UPDATE(sql, None)]
    assert bound_params == (
        _RETRY_EXECUTION_ID,
        _ATTEMPT_ID,
        _TASK_ID,
        _WORKSPACE_ID,
        _RETRY_EXECUTION_ID,
    )
    (complete_params,) = [params for sql, params in conn.executed if _EXECUTION_COMPLETE(sql, None)]
    assert complete_params is not None
    assert complete_params[0].obj == dict(_RESULT_CANDIDATE)
    assert complete_params[1] == _RETRY_EXECUTION_ID

    # Matched Producer-attributed start/complete audit for the NEW row only.
    assert _event_types(conn) == ["execution_started", "execution_completed"]
    (started,) = _event_params_of_type(conn, "execution_started")
    assert started[3] == "producer" and started[4] == str(_SESSION_ID)
    assert started[5] == "execution" and started[6] == _RETRY_EXECUTION_ID
    (completed,) = _event_params_of_type(conn, "execution_completed")
    assert completed[6] == _RETRY_EXECUTION_ID

    # The invocation's own evidence names the NEW retry Execution identity.
    identity = evidence.latest_identity
    assert identity is not None
    assert identity.family is PendingTaskRuntimeInteractionOperation.IMPLEMENTATION_EXECUTION
    assert identity.original_attempt_kind is OriginalAttemptKind.EXECUTION
    assert identity.original_attempt_id == _RETRY_EXECUTION_ID
    assert identity.effect_attempt_id is None
    assert identity.phase == "authorized_implementation_retry"

    assert attachment.acquisitions == 1 and attachment.disposals == 1
    assert pool.active_connections == 0


# --- the refusal table: every refusal precedes any effect ------------------------


def _refusal_cases() -> list[tuple[str, type[Exception], dict[str, Any]]]:
    """One representative table over the distinct guard families."""
    return [
        # The persisted due instant is enforced at actual admission.
        (
            "premature_wakeup",
            StaleOperationError,
            {"attempt": _attempt_row(not_before=_OBSERVED + timedelta(hours=1))},
        ),
        # Unknown and foreign authorizations are uniform NotFound refusals.
        ("unknown_attempt", NotFoundError, {"attempt": None}),
        (
            "foreign_scope_attempt",
            NotFoundError,
            {"attempt": _attempt_row(workspace_id=_OTHER_WORKSPACE_ID)},
        ),
        # A terminally disposed episode can never be executed again.
        ("terminal_episode", ConflictError, {"episode": _episode_row(escalated=uuid.uuid4())}),
        # F4's bound resulting_execution_id is the consumption identity: a
        # replay is a no-effect refusal whatever happened to that Execution.
        (
            "replayed_already_consumed",
            StaleOperationError,
            {"attempt": _attempt_row(resulting_execution_id=uuid.uuid4())},
        ),
        # The chain must be consistent: latest ordinal, no unconsumed sibling.
        (
            "not_the_latest_ordinal",
            ConflictError,
            {
                "attempt": _attempt_row(ordinal=1),
                "attempts": [
                    _attempt_row(ordinal=1),
                    _attempt_row(attempt_id=_OTHER_ATTEMPT_ID, ordinal=2),
                ],
            },
        ),
        (
            "unconsumed_earlier_sibling",
            ConflictError,
            {
                "attempt": _attempt_row(ordinal=2),
                "attempts": [
                    _attempt_row(attempt_id=_OTHER_ATTEMPT_ID, ordinal=1),
                    _attempt_row(ordinal=2),
                ],
            },
        ),
        # The episode must be this Task's implementation-execution family.
        (
            "foreign_episode_family",
            ConflictError,
            {"episode": _episode_row(family="pr_remediation_execution")},
        ),
        # The source chain: the authorization's source must be the episode's
        # original attempt, or a PRIOR_AUTHORIZED source that was executed.
        (
            "source_mismatch",
            ConflictError,
            {"attempt": _attempt_row(source_attempt_id=_OTHER_ATTEMPT_ID)},
        ),
        (
            "prior_authorized_source_never_executed",
            ConflictError,
            {
                "attempt": _attempt_row(
                    ordinal=2, source_kind="prior_authorized", source_attempt_id=_OTHER_ATTEMPT_ID
                ),
                "attempts": [
                    _attempt_row(attempt_id=_OTHER_ATTEMPT_ID, ordinal=1),
                    _attempt_row(ordinal=2, source_kind="prior_authorized"),
                ],
            },
        ),
        # The predecessor must be positively known pre-effect failed.
        (
            "predecessor_still_running",
            ConflictError,
            {"predecessor": _execution_row(status="running")},
        ),
        (
            "predecessor_audit_disagrees",
            ConflictError,
            {
                "corroboration": [
                    _event_row(
                        event_type="execution_failed",
                        execution_id=_EXECUTION_ID,
                        context={"failure_kind": "authority_stale"},
                    )
                ]
            },
        ),
        ("predecessor_audit_missing", ConflictError, {"corroboration": []}),
        # The predecessor must belong to the same admitted Producer session.
        (
            "changed_producer_session",
            ConflictError,
            {"predecessor": _execution_row(session_id=_OTHER_SESSION_ID)},
        ),
        # Currentness/authority refusals re-derive from committed state.
        ("stale_token", StaleOperationError, {"task_row": _task_row(state_token=_TOKEN_2)}),
        (
            "archived_task",
            StaleOperationError,
            {"task_row": _task_row(archived=True, status="cancelled")},
        ),
        ("wrong_status", ConflictError, {"task_row": _task_row(status="planning")}),
        ("current_gate_present", ConflictError, {"task_row": _task_row(gate_id=_GATE_ID)}),
        ("superseded_gate", ConflictError, {"gates": []}),
        (
            "foreign_revision_gate",
            ConflictError,
            {"gates": [_gate_row(plan_revision_id=_OTHER_REVISION_ID)]},
        ),
        ("nonready_session", ConflictError, {"session_row": _ended_session_row()}),
        # History: no unresolved or succeeded implementation attempt may exist.
        (
            "succeeded_history",
            ConflictError,
            {
                "history": [
                    _execution_row(status="failed_transient"),
                    _execution_row(execution_id=_RETRY_EXECUTION_ID, number=2, status="succeeded"),
                ]
            },
        ),
        (
            "unresolved_history",
            ConflictError,
            {
                "history": [
                    _execution_row(status="failed_transient"),
                    _execution_row(execution_id=_RETRY_EXECUTION_ID, number=2, status="running"),
                ]
            },
        ),
    ]


def test_the_representative_refusal_table_refuses_before_any_effect() -> None:
    """Every refusal family leaves no reservation, claim, binding, audit, or
    adapter acquisition."""
    for _name, error_type, script_kwargs in _refusal_cases():
        conn = ScriptedConnection()
        _script_admission(conn, **script_kwargs)
        _refused_before_effects(conn, error_type, clock=lambda: _OBSERVED)


def test_non_owner_callers_and_malformed_commands_fail_closed() -> None:
    # A Profile that does not own the Workspace is uniformly not found.
    conn = ScriptedConnection()
    _script_repeating_reads(conn)
    _refused_before_effects(conn, NotFoundError, profile_id=uuid.uuid4(), clock=lambda: _OBSERVED)

    # Malformed commands are rejected before any state is touched.
    pool, _runtime, attachment = _arrange(ScriptedConnection())
    for kwargs in (
        {"retry_attempt_id": "not-a-uuid"},
        {"expected_state_token": "not-a-uuid"},
        {"profile_id": "not-a-uuid"},
        {"workspace_id": "not-a-uuid"},
        {"task_id": "not-a-uuid"},
    ):
        with pytest.raises(InvalidCommandError):
            _retry_call(
                cast(DatabasePool, pool),
                producer_runtime_factory=attachment,
                **cast("dict[str, Any]", kwargs),
            )
    with pytest.raises(InvalidCommandError):
        _retry_call(cast(DatabasePool, pool), producer_runtime_factory="not-callable")


# --- failure discipline identical to E6 -------------------------------------------


def test_proven_pre_effect_runtime_failure_finalizes_the_new_row_only() -> None:
    """A positively proven pre-effect runtime failure finalizes the NEW
    Execution ``failed_transient`` with its safe ``execution_failed`` audit;
    the predecessor history is never rewritten."""
    conn = ScriptedConnection()
    _script_admission(conn)
    _script_subject_reads(conn)  # R1 (the failure happens during realization)
    conn.on(
        _EXECUTION_GUARDED_UPDATE,
        _execution_row(execution_id=_RETRY_EXECUTION_ID, number=2, status="failed_transient"),
    )
    pool, runtime, attachment = _arrange(
        conn, realize_error=AgentRuntimeUnavailableError("the runtime is unavailable")
    )
    with pytest.raises(ExternalOperationFailedError):
        _retry_call(
            cast(DatabasePool, pool), producer_runtime_factory=attachment, clock=lambda: _OBSERVED
        )

    # The realization was attempted but never completed: no recorded
    # realization and nothing was sent.
    assert runtime.realized_controls() == ()
    assert runtime.submitted_sends() == ()

    # The NEW row finalized failed_transient with the matched audit.
    (guarded,) = [params for sql, params in conn.executed if _EXECUTION_GUARDED_UPDATE(sql, None)]
    assert guarded is not None
    assert guarded[0] == "failed_transient" and guarded[1] == _RETRY_EXECUTION_ID
    assert guarded[2] == "running"
    assert _event_types(conn) == ["execution_started", "execution_failed"]
    (failed,) = _event_params_of_type(conn, "execution_failed")
    assert failed[5] == "execution" and failed[6] == _RETRY_EXECUTION_ID
    assert failed[7].obj == {"failure_kind": "runtime_pre_effect"}
    # The authorization stays bound to the failed Execution (never re-driven).
    assert _count(conn, _BIND_UPDATE) == 1
    assert _count(conn, _EXECUTION_COMPLETE) == 0
    assert attachment.acquisitions == 1 and attachment.disposals == 1


def test_uncertain_retry_outcomes_leave_the_new_execution_running() -> None:
    """An uncertain send outcome is neither success nor known failure: the
    claimed Execution stays conservatively running and nothing resends."""
    conn = ScriptedConnection()
    _script_admission(conn)
    _script_subject_reads(conn)  # R1
    _script_subject_reads(conn)  # R2 (the send outcome is uncertain)
    outcome = AgentRuntimeTimeoutError("the send exceeded its deadline")
    pool, runtime, attachment = _arrange(conn, send_outcomes=[outcome])
    with pytest.raises(ExternalOperationUncertainError):
        _retry_call(
            cast(DatabasePool, pool), producer_runtime_factory=attachment, clock=lambda: _OBSERVED
        )

    assert _count(conn, _EXECUTION_GUARDED_UPDATE) == 0
    assert _count(conn, _EXECUTION_COMPLETE) == 0
    assert _event_types(conn) == ["execution_started"]
    assert attachment.acquisitions == 1 and attachment.disposals == 1


def test_authority_stale_during_attachment_finalizes_failed_final() -> None:
    """Authority moving on during attachment refuses reconstruction, finalizes
    the never-dispatched attempt ``failed_final`` truthfully, and never
    reaches the runtime."""
    conn = ScriptedConnection()
    _script_admission(conn)
    _script_subject_reads(conn, task_row=_task_row(state_token=_TOKEN_2))  # R1 goes stale
    conn.on(
        _EXECUTION_GUARDED_UPDATE,
        _execution_row(execution_id=_RETRY_EXECUTION_ID, number=2, status="failed_final"),
    )
    pool, runtime, attachment = _arrange(conn)
    with pytest.raises(StaleOperationError):
        _retry_call(
            cast(DatabasePool, pool), producer_runtime_factory=attachment, clock=lambda: _OBSERVED
        )

    assert runtime.realized_controls() == () and runtime.submitted_sends() == ()
    (guarded,) = [params for sql, params in conn.executed if _EXECUTION_GUARDED_UPDATE(sql, None)]
    assert guarded is not None
    assert guarded[0] == "failed_final" and guarded[1] == _RETRY_EXECUTION_ID
    assert _event_types(conn) == ["execution_started", "execution_failed"]
    (failed,) = _event_params_of_type(conn, "execution_failed")
    assert failed[7].obj == {"failure_kind": "authority_stale"}


def test_replayed_consumption_after_binding_never_reaches_the_adapter() -> None:
    """Reinvoking a consumed authorization is a no-effect refusal even though
    its Execution may be failed, succeeded, or still running."""
    conn = ScriptedConnection()
    _script_admission(conn, attempt=_attempt_row(resulting_execution_id=_RETRY_EXECUTION_ID))
    _refused_before_effects(conn, StaleOperationError, clock=lambda: _OBSERVED)
