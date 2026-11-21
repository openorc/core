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

from openorc.adapters.agent_runtime.results import AgentSessionCreationRequest
from openorc.domain.connections import WorkflowRole
from openorc.domain.detached_turns import TaskDetachedTurnReceiptStatus
from openorc.domain.executions import ExecutionStatus
from openorc.domain.pending_interactions import PendingTaskRuntimeInteractionOperation
from openorc.persistence.pool import DatabasePool
from openorc.protocol.interaction import InteractionKind
from openorc.services.errors import (
    ConflictError,
    NotFoundError,
    StaleOperationError,
)
from openorc.services.task_authorized_implementation_retry import (
    dispatch_authorized_implementation_retry_detached,
)
from openorc.services.task_detached_turn_observation import (
    ObservationRegistrationRefusalReason,
    ObservationRegistrationRefusedError,
)
from tests.fakes.agent_runtime import (
    ControlRealization,
    FakeAgentRuntimeAdapter,
    implementation_result_candidate,
    session_ready_candidate,
)
from tests.fakes.task_rows import task_row
from tests.test_services_task_authorized_implementation_execution import (  # noqa: I001
    _EARLY_COMPLETION_READ,
    _EXECUTION_READ,
    _RECEIPT_ATTACH,
    _RECEIPT_FOR_UPDATE,
    _RECEIPT_ID,
    _RECEIPT_INSERT,
    _RECEIPT_REFUSE,
    _RecordingHandoff,
    _RecordingRegistrar,
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
    consumed_at: Any = None,
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
        consumed_at,
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


# --- manual-flow fidelity --------------------------------------------------------


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


# --- failure discipline identical to E6 -------------------------------------------


# --- the detached retry dispatch (issues #325/#340) ---------------------------------


def _retry_receipt_row(
    *,
    status: str = "claimed",
    admitted_turn_reference: str | None = None,
) -> tuple[Any, ...]:
    """The detached-turn receipt row for the NEW retry Execution identity."""
    admitted_at = _OBSERVED if status == "admitted" else None
    settled_at = _OBSERVED if status == "refused" else None
    return (
        _RECEIPT_ID,
        _WORKSPACE_ID,
        _TASK_ID,
        _SESSION_ID,
        "implementation_execution",
        "execution",
        _RETRY_EXECUTION_ID,
        "implementation_result",
        admitted_turn_reference,
        status,
        {},
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


def _arrange_retry_detached(
    conn: ScriptedConnection,
) -> tuple[FakePool, FakeAgentRuntimeAdapter, _ProducerAttachment]:
    """One scripted case with an admission-ready detached runtime."""
    pool = FakePool(conn)
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
    attachment = _ProducerAttachment(runtime)
    return pool, runtime, attachment


def _dispatch_retry_detached_call(
    pool: DatabasePool,
    *,
    producer_runtime_factory: Any,  # noqa: ANN401 - typed at the service boundary
    observation_registrar: Any,  # noqa: ANN401
    completion_handoff: Any,  # noqa: ANN401
    clock: Callable[[], datetime] | None = None,
) -> Any:  # noqa: ANN401 - the typed outcome
    return dispatch_authorized_implementation_retry_detached(
        pool,
        profile_id=_OWNER_PROFILE_ID,
        workspace_id=_WORKSPACE_ID,
        task_id=_TASK_ID,
        expected_state_token=_STATE_TOKEN,
        retry_attempt_id=_ATTEMPT_ID,
        producer_runtime_factory=producer_runtime_factory,
        observation_registrar=observation_registrar,
        completion_handoff=completion_handoff,
        clock=clock,
    )


def test_the_detached_retry_dispatch_records_registers_admits_and_attaches_exactly_once() -> None:
    """The authorized retry dispatches as one detached turn (#340): the F4
    authorization is consumed for the NEW retry Execution, the receipt commits
    strictly before the first runtime interaction, the serialized ACT mode is
    reconstructed on the exact original Producer session, the observation is
    registered, the persisted accepted Reviewer envelope is admitted verbatim,
    and the acknowledgement attaches — with no send and no blocking wait."""
    conn = ScriptedConnection()
    _script_admission(conn)
    _script_subject_reads(conn)  # R1
    conn.on(_RECEIPT_INSERT, _retry_receipt_row())
    _script_subject_reads(conn)  # R2
    conn.on(_RECEIPT_FOR_UPDATE, _retry_receipt_row())
    conn.on(
        _RECEIPT_ATTACH,
        _retry_receipt_row(status="admitted", admitted_turn_reference="retry-turn-1"),
    )
    conn.on(_EARLY_COMPLETION_READ, None)
    conn.on(_TASK_READ, _task_row())
    conn.on(_EXECUTION_READ, _execution_row(status="running", last_activity=_OBSERVED))
    pool, runtime, attachment = _arrange_retry_detached(conn)
    registrar = _RecordingRegistrar()
    handoff = _RecordingHandoff()

    outcome = _dispatch_retry_detached_call(
        cast(DatabasePool, pool),
        producer_runtime_factory=attachment,
        observation_registrar=registrar,
        completion_handoff=handoff,
        clock=lambda: _OBSERVED,
    )

    # The attempt stayed conservatively running; the receipt is admitted with
    # its acknowledged turn reference and the NEW retry Execution identity.
    assert outcome.execution.status is ExecutionStatus.RUNNING
    assert outcome.receipt.status is TaskDetachedTurnReceiptStatus.ADMITTED
    assert outcome.receipt.admitted_turn_reference == "retry-turn-1"
    assert (
        outcome.receipt.operation is PendingTaskRuntimeInteractionOperation.IMPLEMENTATION_EXECUTION
    )
    assert outcome.receipt.original_attempt_id == _RETRY_EXECUTION_ID

    # The serialized ACT mode was reconstructed once on the exact original
    # Producer session, and the observation registration preceded the
    # admission on that exact session.
    (control,) = runtime.realized_controls()
    assert control.external_session_id == _EXTERNAL_SESSION_ID
    assert control.interaction.kind is InteractionKind.IMPLEMENT
    assert control.message is None and control.native_action is True
    assert len(registrar.registrations) == 1
    assert registrar.registrations[0]["task_agent_session_id"] == _SESSION_ID
    assert registrar.registrations[0]["external_session_id"] == _EXTERNAL_SESSION_ID

    # The persisted accepted Reviewer envelope was admitted verbatim — the
    # same canonical message the blocking send carries — and no blocking
    # send ever ran. No staged early completion settled inside the dispatch,
    # so the handoff is not invoked here.
    ((admitted_session, admitted_message),) = runtime.detached_admissions()
    assert admitted_session == _EXTERNAL_SESSION_ID
    assert json.loads(admitted_message) == {
        "type": "review_result",
        "schema_version": 1,
        "outcome": "accepted",
        "summary": _SUMMARY,
        "findings": [],
    }
    assert runtime.submitted_sends() == ()
    assert handoff.calls == []
    assert attachment.acquisitions == 1 and attachment.disposals == 1
    # The one-shot F4 consumption: exactly one Execution insert (number 2),
    # one claim, and one set-once authorization binding.
    assert _count(conn, _EXECUTION_INSERT) == 1
    assert _count(conn, _EXECUTION_CLAIM) == 1
    assert _count(conn, _BIND_UPDATE) == 1


def test_detached_retry_registration_refusal_refuses_the_receipt_and_finalizes() -> None:
    """A positively-required observation registration refusal is a proven
    pre-effect failure of the never-dispatched retry: the receipt is refused
    (freeing the session's single-outstanding slot), the NEW retry Execution
    finalizes ``failed_transient``, and nothing is admitted or sent."""
    conn = ScriptedConnection()
    _script_admission(conn)
    _script_subject_reads(conn)  # R1
    conn.on(_RECEIPT_INSERT, _retry_receipt_row())
    conn.on(_RECEIPT_REFUSE, _retry_receipt_row(status="refused"))
    conn.on(
        _EXECUTION_GUARDED_UPDATE,
        _execution_row(execution_id=_RETRY_EXECUTION_ID, number=2, status="failed_transient"),
    )
    pool, runtime, attachment = _arrange_retry_detached(conn)
    registrar = _RecordingRegistrar(
        error=ObservationRegistrationRefusedError(
            "the observer refused the registration",
            reason=ObservationRegistrationRefusalReason.OBSERVATION_REGISTRATION_FAILED.value,
        )
    )
    handoff = _RecordingHandoff()

    with pytest.raises(ObservationRegistrationRefusedError):
        _dispatch_retry_detached_call(
            cast(DatabasePool, pool),
            producer_runtime_factory=attachment,
            observation_registrar=registrar,
            completion_handoff=handoff,
            clock=lambda: _OBSERVED,
        )

    assert len(registrar.registrations) == 1
    assert _count(conn, _RECEIPT_REFUSE) == 1
    (update_params,) = [
        params for sql, params in conn.executed if _EXECUTION_GUARDED_UPDATE(sql, None)
    ]
    assert update_params == ("failed_transient", _RETRY_EXECUTION_ID, "running")
    assert _event_types(conn) == ["execution_started", "execution_failed"]
    assert runtime.detached_admissions() == ()
    assert runtime.submitted_sends() == ()
    assert handoff.calls == []
