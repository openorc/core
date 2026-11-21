"""Deterministic tests for the E3a exact-plan REVIEW dispatch (issue #209).

A scripted SQL-connection seam plus the #69 fake Agent Runtime prove the
E3a contract on its critical path:

- the exact successful E2 handoff (post-update Task token and the persisted
  current PlanRevision) is the only gate for the exact-plan REVIEW — coarse
  ``PLANNING``/READY facts, stale tokens, foreign or superseded plans, and
  malformed continuation inputs fail closed before any send;
- on first review exactly one OPEN ``PLANNING`` loop is established with the
  Workspace's current ``review_iteration_limit`` snapshot; a subsequent
  current plan reuses the loop (stored budget preserved despite a Workspace
  setting edit) and reserves the next iteration number;
- exactly one next-numbered unfinalized ReviewIteration is reserved for the
  exact current plan before the external send, and an unresolved earlier
  attempt (the uncertain-send case) can never be bypassed by a second
  REVIEW;
- the ONE REVIEW send goes to the exact admitted READY Reviewer session
  through the #67 funnel with ``expected_family=review_result``, with the
  model-visible message and typed subject carrying the persisted plan
  content and the durable repository/issue identity, with NO database
  transaction open, and only the validated typed ``ReviewResult`` is
  returned to E3b — E3a records no verdict, no iteration result, and no
  Task mutation;
- the Reviewer attachment is acquired through the deferred session-addressed
  factory strictly after the reservation commit and the attempt-evidence
  recording (issue #268), the exact reserved subjects are re-proven after
  acquisition and immediately before the send, and an attachment failure
  leaves the reserved attempt unresolved while carrying the invocation's
  own F5b handoff identity;
- known failures and uncertain outcomes leave the reserved attempt
  unresolved and are never retried or resent here.

Durable atomicity is proven by the integration-marked suite against a real
Postgres. No live GitHub, runtime, or Postgres access.
"""

from __future__ import annotations

import uuid
from collections.abc import Callable, Iterator, Sequence
from contextlib import AbstractContextManager, contextmanager
from datetime import UTC, datetime
from typing import Any, cast

import pytest

from openorc.adapters.agent_runtime.errors import (
    AgentRuntimeDeliveryUncertainError,
    AgentSessionNotFoundError,
)
from openorc.adapters.agent_runtime.results import AgentSessionCreationRequest
from openorc.domain.connections import WorkflowRole
from openorc.domain.planning import PlanRevision
from openorc.domain.sessions import TaskAgentSession, TaskSessionLifecycleStatus
from openorc.domain.tasks import Task, TaskStatus
from openorc.persistence.pool import DatabasePool
from openorc.protocol.interaction import ReviewInteraction
from openorc.services.errors import (
    ExternalOperationFailedError,
    ExternalOperationRejectedError,
    ExternalOperationUncertainError,
)
from openorc.services.task_detached_turn_observation import (
    ObservationRegistrationRefusalReason,
    ObservationRegistrationRefusedError,
)
from openorc.services.task_plan_production import (
    TaskPlanProductionOutcome,
)
from openorc.services.task_planning_review_dispatch import (
    DispatchedPlanningReview,
    dispatch_planning_review_detached,
)
from tests.fakes.agent_runtime import (
    FakeAgentRuntimeAdapter,
    changes_requested_review_result_candidate,
    session_ready_candidate,
)
from tests.fakes.task_rows import task_row

_OBSERVED = datetime(2026, 10, 19, 12, 0, 0, tzinfo=UTC)
_WORKSPACE_ID = uuid.uuid4()
_OWNER_PROFILE_ID = uuid.uuid4()
_OTHER_PROFILE_ID = uuid.uuid4()
_TASK_ID = uuid.uuid4()
_OTHER_TASK_ID = uuid.uuid4()
_REPOSITORY_ID = uuid.uuid4()
_CONNECTION_ID = uuid.uuid4()
_REVIEWER_SESSION_ID = uuid.uuid4()
_REVIEWER_EXTERNAL_ID = "fake-session-1"
_STATE_TOKEN = uuid.uuid4()
_REVISION_1_ID = uuid.uuid4()
_REVISION_2_ID = uuid.uuid4()
_LOOP_ID = uuid.uuid4()
_OTHER_LOOP_ID = uuid.uuid4()
_ITERATION_1_ID = uuid.uuid4()
_ITERATION_2_ID = uuid.uuid4()
_BASE_SHA = "c" * 40
_GITHUB_REPOSITORY_ID = 987654321
_PLAN_TEXT = "# Plan\n\nStep one.\n"
_REVISED_PLAN_TEXT = "# Plan (revised)\n\nStep one, clarified.\n"
_FULL_NAME = "octocat/hello-world"
_REVIEW_FINDINGS = [
    {"summary": "Clarify scope", "details": "Say which module changes."},
    {"summary": "Add tests", "details": "Cover the dispatch edge."},
]

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


def _role_is(base: _Predicate, role: str) -> _Predicate:
    def predicate(sql: str, params: tuple[Any, ...] | None) -> bool:
        return base(sql, params) and params is not None and len(params) > 1 and params[1] == role

    return predicate


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
        raise AssertionError("planning review dispatch tests never close pools")


def _pool(conn: ScriptedConnection) -> DatabasePool:
    return cast(DatabasePool, FakePool(conn))


_RESULT_UPDATE = _sql_has("update openorc.review_iterations")


def _ws_row(*, limit: int = 5) -> tuple[Any, ...]:
    return (_WORKSPACE_ID, _OWNER_PROFILE_ID, "platform", _OBSERVED, _OBSERVED, limit, "", None)


def _task_row(
    *,
    status: str = "planning",
    state_token: Any = _STATE_TOKEN,
    current_plan_revision_id: Any = _REVISION_1_ID,
    workspace_id: Any = _WORKSPACE_ID,
    archived_at: Any = None,
) -> tuple[Any, ...]:
    return task_row(
        id=_TASK_ID,
        workspace_id=workspace_id,
        repository_id=_REPOSITORY_ID,
        github_issue_id=503,
        status=status,
        archived_at=archived_at,
        state_token=state_token,
        current_plan_revision_id=current_plan_revision_id,
        source_requirements_fingerprint="a" * 64,
        created_at=_OBSERVED,
        updated_at=_OBSERVED,
    )


def _session_row(
    role: str,
    *,
    session_id: Any,
    external_id: str | None,
    lifecycle: str = "ready",
) -> tuple[Any, ...]:
    # Row coherence mirrors the domain CHECK: READY and LOST carry the three
    # initialization facts together; CONNECTING carries none.
    initialized = lifecycle in ("ready", "lost", "ended") and external_id is not None
    return (
        session_id,
        _WORKSPACE_ID,
        _TASK_ID,
        role,
        _CONNECTION_ID,
        external_id,
        lifecycle,
        {"adapter": "cline"} if initialized else None,
        None,
        None,
        None,
        _OBSERVED if initialized else None,
        _OBSERVED if lifecycle == "ended" else None,
        _OBSERVED,
        _OBSERVED,
        None,
        None,
    )


def _revision_row(
    *, revision_id: Any = _REVISION_1_ID, number: int = 1, content: str = _PLAN_TEXT
) -> tuple[Any, ...]:
    return (revision_id, _WORKSPACE_ID, _TASK_ID, number, content, _BASE_SHA, _OBSERVED)


def _repository_row() -> tuple[Any, ...]:
    return (
        _REPOSITORY_ID,
        uuid.uuid4(),
        _WORKSPACE_ID,
        _GITHUB_REPOSITORY_ID,
        "octocat",
        "hello-world",
        "https://github.com/octocat/hello-world",
        False,
        "main",
        _OBSERVED,
        _OBSERVED,
        None,
    )


def _loop_row(
    *,
    loop_id: Any = _LOOP_ID,
    purpose: str = "planning",
    status: str = "open",
    limit: int = 3,
    closed_at: Any = None,
) -> tuple[Any, ...]:
    return (loop_id, _WORKSPACE_ID, _TASK_ID, purpose, limit, status, closed_at, _OBSERVED)


def _iteration_row(
    *,
    iteration_id: Any = _ITERATION_1_ID,
    number: int = 1,
    outcome: str | None = None,
    summary: str | None = None,
    findings: Any = None,
    plan_revision_id: Any = _REVISION_1_ID,
    decided_at: Any = None,
) -> tuple[Any, ...]:
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


_TASK_READ = _sql_lacks(_sql_has("from openorc.tasks where id"), "for update")
_TASK_READ_LOCKED = _sql_has("from openorc.tasks where id", "for update")
_TASK_WRITE = _sql_has("update openorc.tasks")
_REVISION_READ = _sql_has("from openorc.plan_revisions where id")
_SESSION_READ = _sql_has("from openorc.task_agent_sessions")
_REPO_READ = _sql_has("from openorc.repositories where id")
_LOOPS_LIST = _sql_has("from openorc.review_loops where task_id")
_LOOP_READ_LOCKED = _sql_has("from openorc.review_loops where id", "for update")
_LOOP_GUARD_LOCKED = _sql_has("select status, purpose from openorc.review_loops", "for update")
_LOOP_INSERT = _sql_has("insert into openorc.review_loops")
_ITERATIONS_LIST = _sql_has("review_iterations where review_loop_id")
_ITERATION_INSERT = _sql_has("insert into openorc.review_iterations")
_RESULT_UPDATE = _sql_has("update openorc.review_iterations")


def _revision_domain(revision_id: Any, *, number: int, content: str = _PLAN_TEXT) -> PlanRevision:
    return PlanRevision(
        id=revision_id,
        workspace_id=_WORKSPACE_ID,
        task_id=_TASK_ID,
        revision_number=number,
        content=content,
        repository_base_sha=_BASE_SHA,
        created_at=_OBSERVED,
    )


def _task_domain(
    *,
    status: TaskStatus = TaskStatus.PLANNING,
    state_token: Any = _STATE_TOKEN,
    current_plan_revision_id: Any = _REVISION_1_ID,
) -> Task:
    return Task(
        id=_TASK_ID,
        workspace_id=_WORKSPACE_ID,
        repository_id=_REPOSITORY_ID,
        github_issue_id=503,
        github_issue_number=42,
        status=status,
        archived_at=None,
        queued_at=None,
        canonical_feature_branch=None,
        state_token=state_token,
        current_plan_revision_id=current_plan_revision_id,
        current_owner_gate_id=None,
        source_requirements_fingerprint="a" * 64,
        created_at=_OBSERVED,
        updated_at=_OBSERVED,
    )


def _session_domain(
    role: WorkflowRole, *, session_id: Any, external_id: str | None
) -> TaskAgentSession:
    ready = external_id is not None
    return TaskAgentSession(
        id=session_id,
        workspace_id=_WORKSPACE_ID,
        task_id=_TASK_ID,
        role=role,
        connection_id=_CONNECTION_ID,
        external_session_id=external_id,
        lifecycle_status=(
            TaskSessionLifecycleStatus.READY if ready else TaskSessionLifecycleStatus.CONNECTING
        ),
        effective_config_snapshot={"adapter": "cline"} if ready else None,
        reported_provider=None,
        reported_model=None,
        reported_runtime_version=None,
        initialized_at=_OBSERVED if ready else None,
        ended_at=None,
        created_at=_OBSERVED,
        updated_at=_OBSERVED,
    )


def _production_outcome(
    *,
    revision_id: Any = _REVISION_1_ID,
    number: int = 1,
    content: str = _PLAN_TEXT,
    state_token: Any = _STATE_TOKEN,
    current_plan_revision_id: Any = _REVISION_1_ID,
) -> TaskPlanProductionOutcome:
    return TaskPlanProductionOutcome(
        plan_revision=_revision_domain(revision_id, number=number, content=content),
        task=_task_domain(
            state_token=state_token, current_plan_revision_id=current_plan_revision_id
        ),
    )


def _review_candidate() -> dict[str, Any]:
    return changes_requested_review_result_candidate(
        "Needs work",
        [("Clarify scope", "Say which module changes.")],
    )


class _ProbedAdapter(FakeAgentRuntimeAdapter):
    """Fake runtime proving the REVIEW send runs with no transaction open."""

    def __init__(self, pool: FakePool) -> None:
        super().__init__()
        self._dispatch_pool = pool

    def realize_control(
        self,
        session_id: str,
        interaction: ReviewInteraction,
        *,
        message: str | None = None,
        expected_family: str | None = None,
    ) -> Any:
        assert self._dispatch_pool.active_connections == 0, (
            "the service must never hold a database transaction open across the REVIEW send"
        )
        return super().realize_control(
            session_id, interaction, message=message, expected_family=expected_family
        )


def _prepared_reviewer_adapter(pool: FakePool, outcomes: Sequence[Any]) -> _ProbedAdapter:
    """A fake adapter holding one live Reviewer session, as admission established it."""
    adapter = _ProbedAdapter(pool)
    adapter.queue_creation(session_ready_candidate())
    created = adapter.create_session(
        AgentSessionCreationRequest(
            workspace_id=_WORKSPACE_ID,
            task_id=_TASK_ID,
            role=WorkflowRole.REVIEWER,
            initialization="prepared",
        )
    )
    assert created.external_session_id == _REVIEWER_EXTERNAL_ID
    for outcome in outcomes:
        adapter.queue_send(_REVIEWER_EXTERNAL_ID, outcome)
    return adapter


class _ReviewerAttachment:
    """Deferred Reviewer attachment probe: counts acquisitions and disposals."""

    def __init__(self, runtime: FakeAgentRuntimeAdapter, conn: ScriptedConnection) -> None:
        self._runtime = runtime
        self._conn = conn
        self.acquisitions = 0
        self.disposals = 0
        self.bindings: list[Any] = []
        self.statements_at_acquisition: list[tuple[str, tuple[Any, ...] | None]] = []

    def __call__(self, binding: Any) -> AbstractContextManager[FakeAgentRuntimeAdapter]:
        self.bindings.append(binding)
        self.statements_at_acquisition = list(self._conn.executed)
        self.acquisitions += 1
        return self

    def __enter__(self) -> FakeAgentRuntimeAdapter:
        return self._runtime

    def __exit__(self, *_exc: object) -> bool:
        self.disposals += 1
        return False


class _FailingAttachment:
    """Raises its typed error exactly when the attachment is acquired."""

    def __init__(self, error: Exception, conn: ScriptedConnection) -> None:
        self._error = error
        self._conn = conn
        self.bindings: list[Any] = []
        self.statements_at_failure: list[tuple[str, tuple[Any, ...] | None]] = []

    def __call__(self, binding: Any) -> AbstractContextManager[FakeAgentRuntimeAdapter]:
        self.bindings.append(binding)
        self.statements_at_failure = list(self._conn.executed)
        raise self._error


def _script_account_and_workspace(
    conn: ScriptedConnection, *, workspace_row: tuple[Any, ...] | None = None
) -> None:
    # The account-operational barrier row: the Profile exists with no
    # deletion-attempt state (all three columns NULL).
    conn.on(_sql_has("from openorc.profiles", "for key share"), (None, None, None))
    conn.on_repeat(
        _sql_has("from openorc.workspaces where id"),
        workspace_row if workspace_row is not None else _ws_row(),
    )


def _script_entry_prefix(
    conn: ScriptedConnection,
    *,
    task_row: tuple[Any, ...],
    reviewer_row: tuple[Any, ...] | None,
    revision_row: tuple[Any, ...],
    workspace_row: tuple[Any, ...] | None = None,
) -> None:
    """The shared entry-transaction reads, in call order."""
    _script_account_and_workspace(conn, workspace_row=workspace_row)
    conn.on(_TASK_READ, task_row)  # require_workspace_task's task read
    conn.on(_TASK_READ_LOCKED, task_row)  # the dispatch's own Task row lock
    conn.on(_TASK_READ, task_row)  # require_current_task
    conn.on(_REVISION_READ, revision_row)
    conn.on(_role_is(_SESSION_READ, "reviewer"), reviewer_row)
    conn.on(_REPO_READ, _repository_row())


def _script_revalidation(
    conn: ScriptedConnection,
    *,
    task_row: tuple[Any, ...],
    revision_row: tuple[Any, ...],
    reviewer_row: tuple[Any, ...] | None,
) -> None:
    """F5e: the post-attachment currentness proof's ordered reads."""
    conn.on(_TASK_READ_LOCKED, task_row)
    conn.on(_TASK_READ, task_row)
    conn.on(_REVISION_READ, revision_row)
    conn.on(_role_is(_SESSION_READ, "reviewer"), reviewer_row)


def _script_first_review_entry(
    conn: ScriptedConnection,
    *,
    task_row: tuple[Any, ...],
    reviewer_row: tuple[Any, ...] | None,
    revision_row: tuple[Any, ...],
    workspace_row: tuple[Any, ...] | None = None,
    loop_row: tuple[Any, ...] | None = None,
    iteration_row: tuple[Any, ...] | None = None,
    revalidation_task_row: tuple[Any, ...] | None = None,
    revalidation_reviewer_row: tuple[Any, ...] | None = None,
) -> None:
    """The first-review entry composition's canned sequence, in call order.

    No loop exists yet, so the composition establishes the OPEN planning
    loop (with the Workspace snapshot) and then locks/rechecks it before
    reserving iteration 1 for the exact current plan. The post-attachment
    currentness proof's reads follow the reservation (issue #268).
    """
    _script_entry_prefix(
        conn,
        task_row=task_row,
        reviewer_row=reviewer_row,
        revision_row=revision_row,
        workspace_row=workspace_row,
    )
    conn.on(_LOOPS_LIST, [])
    conn.on(_LOOP_INSERT, loop_row if loop_row is not None else _loop_row(limit=5))
    conn.on(_LOOP_READ_LOCKED, loop_row if loop_row is not None else _loop_row(limit=5))
    conn.on(_ITERATIONS_LIST, [])
    conn.on(_LOOP_GUARD_LOCKED, ("open", "planning"))
    conn.on(_ITERATION_INSERT, iteration_row if iteration_row is not None else _iteration_row())
    _script_revalidation(
        conn,
        task_row=revalidation_task_row if revalidation_task_row is not None else task_row,
        revision_row=revision_row,
        reviewer_row=(
            revalidation_reviewer_row if revalidation_reviewer_row is not None else reviewer_row
        ),
    )


def _script_reuse_entry(
    conn: ScriptedConnection,
    *,
    task_row: tuple[Any, ...],
    reviewer_row: tuple[Any, ...] | None,
    revision_row: tuple[Any, ...],
    existing_loops: list[tuple[Any, ...]],
    loop_row: tuple[Any, ...],
    loop_iterations: list[tuple[Any, ...]],
    iteration_row: tuple[Any, ...],
    workspace_row: tuple[Any, ...] | None = None,
    revalidation_task_row: tuple[Any, ...] | None = None,
    revalidation_reviewer_row: tuple[Any, ...] | None = None,
) -> None:
    """The loop-reuse entry composition's canned sequence, in call order."""
    _script_entry_prefix(
        conn,
        task_row=task_row,
        reviewer_row=reviewer_row,
        revision_row=revision_row,
        workspace_row=workspace_row,
    )
    conn.on(_LOOPS_LIST, existing_loops)
    conn.on(_LOOP_READ_LOCKED, loop_row)
    conn.on(_ITERATIONS_LIST, loop_iterations)
    conn.on(_LOOP_GUARD_LOCKED, ("open", "planning"))
    conn.on(_ITERATION_INSERT, iteration_row)
    _script_revalidation(
        conn,
        task_row=revalidation_task_row if revalidation_task_row is not None else task_row,
        revision_row=revision_row,
        reviewer_row=(
            revalidation_reviewer_row if revalidation_reviewer_row is not None else reviewer_row
        ),
    )


def _assert_no_reservation(conn: ScriptedConnection) -> None:
    assert not any(_LOOP_INSERT(sql, None) for sql, _ in conn.executed)
    assert not any(_ITERATION_INSERT(sql, None) for sql, _ in conn.executed)


def _assert_no_verdict_recorded(conn: ScriptedConnection) -> None:
    assert not any(_RESULT_UPDATE(sql, None) for sql, _ in conn.executed)


def _assert_no_task_mutation(conn: ScriptedConnection) -> None:
    assert not any(_TASK_WRITE(sql, None) for sql, _ in conn.executed)


# --- the detached regular-mode REVIEW dispatch (issue #336) --------------------

_RECEIPT_ID = uuid.uuid4()
_TURN_REFERENCE = "fake-run-1"

_RECEIPT_INSERT = _sql_has("insert into openorc.task_hub_run_receipts")
_RECEIPT_FOR_UPDATE = _sql_has("from openorc.task_hub_run_receipts", "where id = %s for update")
_RECEIPT_ATTACH = _sql_has("update openorc.task_hub_run_receipts", "set status = 'admitted'")
_RECEIPT_REFUSE = _sql_has("update openorc.task_hub_run_receipts", "set status = 'refused'")
_EARLY_COMPLETION_READ = _sql_has("from openorc.task_hub_run_early_completions", "for update")


def _detached_receipt_row(
    *,
    status: str = "claimed",
    admitted_turn_reference: str | None = None,
) -> tuple[Any, ...]:
    admitted_at = _OBSERVED if status in ("admitted", "settled") else None
    settled_at = _OBSERVED if status in ("settled", "refused") else None
    if status == "settled":
        outcome: dict[str, Any] = {"settlement": "validated_result"}
    elif status == "refused":
        outcome = {"refusal_kind": "admission_refused"}
    else:
        outcome = {}
    return (
        _RECEIPT_ID,
        _WORKSPACE_ID,
        _TASK_ID,
        _REVIEWER_SESSION_ID,
        "reviewer_review",
        "review_iteration",
        _ITERATION_1_ID,
        "review_result",
        admitted_turn_reference,
        status,
        outcome,
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
    """Records the ONE post-commit completion handoff invocations (issue #336)."""

    def __init__(self) -> None:
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


def _receipt_insert_params(conn: ScriptedConnection) -> tuple[Any, ...]:
    for sql, params in conn.executed:
        if _RECEIPT_INSERT(sql, None):
            assert params is not None
            return params
    raise AssertionError("no detached-turn receipt insert executed")


def _script_attach(conn: ScriptedConnection) -> None:
    """The acknowledgement attach (claimed → admitted, no staged completion)."""
    conn.on(_RECEIPT_FOR_UPDATE, _detached_receipt_row())
    conn.on(
        _RECEIPT_ATTACH,
        _detached_receipt_row(status="admitted", admitted_turn_reference=_TURN_REFERENCE),
    )
    conn.on(_EARLY_COMPLETION_READ, None)


# --- the detached regular-mode REVIEW dispatch (issue #336) --------------------


class _DispatchEnv:
    """The scripted-connection environment one detached dispatch test uses.

    Since #347 the blocking dispatch entry is retired; the detached dispatch
    tests reuse only this fixture's scripted connection.
    """

    def __init__(self) -> None:
        self.conn = ScriptedConnection()
        self.pool = FakePool(self.conn)

    def prepared_reviewer_adapter(self, outcomes: Sequence[Any]) -> Any:
        return _prepared_reviewer_adapter(self.pool, outcomes)


_RECEIPT_ID = uuid.uuid4()
_TURN_REFERENCE = "fake-run-1"

_RECEIPT_INSERT = _sql_has("insert into openorc.task_hub_run_receipts")
_RECEIPT_FOR_UPDATE = _sql_has("from openorc.task_hub_run_receipts", "where id = %s for update")
_RECEIPT_ATTACH = _sql_has("update openorc.task_hub_run_receipts", "set status = 'admitted'")
_RECEIPT_REFUSE = _sql_has("update openorc.task_hub_run_receipts", "set status = 'refused'")
_EARLY_COMPLETION_READ = _sql_has("from openorc.task_hub_run_early_completions", "for update")


def _detached_receipt_row(
    *,
    status: str = "claimed",
    admitted_turn_reference: str | None = None,
) -> tuple[Any, ...]:
    admitted_at = _OBSERVED if status in ("admitted", "settled") else None
    settled_at = _OBSERVED if status in ("settled", "refused") else None
    if status == "settled":
        outcome: dict[str, Any] = {"settlement": "validated_result"}
    elif status == "refused":
        outcome = {"refusal_kind": "admission_refused"}
    else:
        outcome = {}
    return (
        _RECEIPT_ID,
        _WORKSPACE_ID,
        _TASK_ID,
        _REVIEWER_SESSION_ID,
        "reviewer_review",
        "review_iteration",
        _ITERATION_1_ID,
        "review_result",
        admitted_turn_reference,
        status,
        outcome,
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
    """Records the ONE post-commit completion handoff invocations (issue #336)."""

    def __init__(self) -> None:
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


def _receipt_insert_params(conn: ScriptedConnection) -> tuple[Any, ...]:
    for sql, params in conn.executed:
        if _RECEIPT_INSERT(sql, None):
            assert params is not None
            return params
    raise AssertionError("no detached-turn receipt insert executed")


def _script_attach(conn: ScriptedConnection) -> None:
    """The acknowledgement attach (claimed → admitted, no staged completion)."""
    conn.on(_RECEIPT_FOR_UPDATE, _detached_receipt_row())
    conn.on(
        _RECEIPT_ATTACH,
        _detached_receipt_row(status="admitted", admitted_turn_reference=_TURN_REFERENCE),
    )
    conn.on(_EARLY_COMPLETION_READ, None)


def _run_detached(
    env: _DispatchEnv,
    *,
    adapter: FakeAgentRuntimeAdapter,
    registrar: Any,
    handoff: Any,
) -> Any:
    return dispatch_planning_review_detached(
        cast(DatabasePool, env.pool),
        _ReviewerAttachment(adapter, env.conn),
        workspace_id=_WORKSPACE_ID,
        task_id=_TASK_ID,
        profile_id=_OWNER_PROFILE_ID,
        plan_production=_production_outcome(),
        observation_registrar=registrar,  # type: ignore[arg-type]
        completion_handoff=handoff,  # type: ignore[arg-type]
    )


def test_the_detached_review_dispatch_reserves_records_registers_admits_and_attaches() -> None:
    """The regular-mode REVIEW is one admitted detached turn: the reserved
    iteration's ``review_result`` receipt companion commits strictly before
    the positive observation registration and the verbatim admission, and
    the dispatching call returns with no blocking send and no verdict."""
    env = _DispatchEnv()
    _script_first_review_entry(
        env.conn,
        task_row=_task_row(),
        reviewer_row=_session_row(
            "reviewer", session_id=_REVIEWER_SESSION_ID, external_id=_REVIEWER_EXTERNAL_ID
        ),
        revision_row=_revision_row(),
        loop_row=_loop_row(limit=5),
        iteration_row=_iteration_row(
            iteration_id=_ITERATION_1_ID,
            number=1,
            plan_revision_id=_REVISION_1_ID,
        ),
    )
    env.conn.on(_RECEIPT_INSERT, _detached_receipt_row())
    _script_attach(env.conn)
    env.conn.on(_TASK_READ, _task_row())  # the post-dispatch reload
    runtime = env.prepared_reviewer_adapter([])
    registrar = _RecordingRegistrar()
    handoff = _RecordingHandoff()

    outcome = _run_detached(env, adapter=runtime, registrar=registrar, handoff=handoff)

    assert isinstance(outcome, DispatchedPlanningReview)
    assert outcome.review_iteration.id == _ITERATION_1_ID
    assert outcome.review_iteration.outcome is None
    assert outcome.receipt.id == _RECEIPT_ID
    assert outcome.reviewer_session.id == _REVIEWER_SESSION_ID
    assert outcome.task.state_token == _STATE_TOKEN
    # The receipt companion expects the canonical review_result family and
    # roots at the reserved ReviewIteration identity.
    receipt_params = _receipt_insert_params(env.conn)
    assert receipt_params[3] == "reviewer_review"
    assert receipt_params[4] == "review_iteration"
    assert receipt_params[6] == "review_result"
    # One positive observation registration on the exact bound Reviewer
    # session.
    assert registrar.registrations == [
        {
            "workspace_id": _WORKSPACE_ID,
            "task_id": _TASK_ID,
            "task_agent_session_id": _REVIEWER_SESSION_ID,
            "connection_id": _CONNECTION_ID,
            "external_session_id": _REVIEWER_EXTERNAL_ID,
        }
    ]
    # The canonical REVIEW instruction was admitted verbatim and no blocking
    # send ran; E3a recorded no verdict, result, or Task mutation.
    assert len(runtime.detached_admissions()) == 1
    admitted_session, admitted_message = runtime.detached_admissions()[0]
    assert admitted_session == _REVIEWER_EXTERNAL_ID
    assert _FULL_NAME in admitted_message
    assert len(runtime.realized_controls(_REVIEWER_EXTERNAL_ID)) == 0
    assert env.pool.active_connections == 0
    _assert_no_verdict_recorded(env.conn)
    _assert_no_task_mutation(env.conn)


def test_a_refused_observation_registration_refuses_the_receipt_before_any_admission() -> None:
    """A positively refused pre-admission registration is a proven pre-effect
    failure: the just-recorded receipt is marked refused so a never-dispatched
    turn cannot hold the session's single-outstanding slot."""
    env = _DispatchEnv()
    _script_first_review_entry(
        env.conn,
        task_row=_task_row(),
        reviewer_row=_session_row(
            "reviewer", session_id=_REVIEWER_SESSION_ID, external_id=_REVIEWER_EXTERNAL_ID
        ),
        revision_row=_revision_row(),
        loop_row=_loop_row(limit=5),
        iteration_row=_iteration_row(
            iteration_id=_ITERATION_1_ID,
            number=1,
            plan_revision_id=_REVISION_1_ID,
        ),
    )
    env.conn.on(_RECEIPT_INSERT, _detached_receipt_row())
    env.conn.on(_RECEIPT_REFUSE, _detached_receipt_row(status="refused"))
    runtime = env.prepared_reviewer_adapter([])

    with pytest.raises(ExternalOperationFailedError, match="registration was refused"):
        _run_detached(
            env,
            adapter=runtime,
            registrar=_RecordingRegistrar(
                error=ObservationRegistrationRefusedError(
                    "refused",
                    reason=ObservationRegistrationRefusalReason.OBSERVATION_REGISTRATION_FAILED,
                )
            ),
            handoff=_RecordingHandoff(),
        )

    assert any(_RECEIPT_REFUSE(sql, None) for sql, _ in env.conn.executed)
    assert runtime.detached_admissions() == ()


def test_a_refused_admission_refuses_the_receipt_and_never_repeats() -> None:
    """A positively identified admission refusal is proven pre-effect: the
    receipt is marked refused and the canonical instruction never dispatched."""
    env = _DispatchEnv()
    _script_first_review_entry(
        env.conn,
        task_row=_task_row(),
        reviewer_row=_session_row(
            "reviewer", session_id=_REVIEWER_SESSION_ID, external_id=_REVIEWER_EXTERNAL_ID
        ),
        revision_row=_revision_row(),
        loop_row=_loop_row(limit=5),
        iteration_row=_iteration_row(
            iteration_id=_ITERATION_1_ID,
            number=1,
            plan_revision_id=_REVISION_1_ID,
        ),
    )
    env.conn.on(_RECEIPT_INSERT, _detached_receipt_row())
    env.conn.on(_RECEIPT_REFUSE, _detached_receipt_row(status="refused"))
    runtime = env.prepared_reviewer_adapter([])
    runtime.queue_detached_admission(AgentSessionNotFoundError("gone"))

    with pytest.raises(ExternalOperationRejectedError):
        _run_detached(
            env,
            adapter=runtime,
            registrar=_RecordingRegistrar(),
            handoff=_RecordingHandoff(),
        )

    assert any(_RECEIPT_REFUSE(sql, None) for sql, _ in env.conn.executed)


def test_an_uncertain_admission_leaves_the_receipt_claimed_and_never_repeats() -> None:
    """An uncertain admission outcome stays conservatively claimed: the
    receipt is never re-dispatched and the durable state owns recovery."""
    env = _DispatchEnv()
    _script_first_review_entry(
        env.conn,
        task_row=_task_row(),
        reviewer_row=_session_row(
            "reviewer", session_id=_REVIEWER_SESSION_ID, external_id=_REVIEWER_EXTERNAL_ID
        ),
        revision_row=_revision_row(),
        loop_row=_loop_row(limit=5),
        iteration_row=_iteration_row(
            iteration_id=_ITERATION_1_ID,
            number=1,
            plan_revision_id=_REVISION_1_ID,
        ),
    )
    env.conn.on(_RECEIPT_INSERT, _detached_receipt_row())
    runtime = env.prepared_reviewer_adapter([])
    runtime.queue_detached_admission(AgentRuntimeDeliveryUncertainError("the outcome is unknown"))

    with pytest.raises(ExternalOperationUncertainError):
        _run_detached(
            env,
            adapter=runtime,
            registrar=_RecordingRegistrar(),
            handoff=_RecordingHandoff(),
        )

    assert not any(_RECEIPT_REFUSE(sql, None) for sql, _ in env.conn.executed)
