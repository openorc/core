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
- known failures and uncertain outcomes leave the reserved attempt
  unresolved and are never retried or resent here.

Durable atomicity is proven by the integration-marked suite against a real
Postgres. No live GitHub, runtime, or Postgres access.
"""

from __future__ import annotations

import uuid
from collections.abc import Callable, Iterator, Sequence
from contextlib import contextmanager
from datetime import UTC, datetime
from typing import Any, cast

import pytest
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from opentelemetry.trace import StatusCode
from psycopg.errors import UniqueViolation

from openorc.adapters.agent_runtime.errors import (
    AgentRuntimeDeliveryUncertainError,
    AgentRuntimeUnavailableError,
)
from openorc.adapters.agent_runtime.results import AgentSessionCreationRequest
from openorc.domain.connections import WorkflowRole
from openorc.domain.planning import PlanRevision
from openorc.domain.reviews import ReviewLoopPurpose, ReviewLoopStatus
from openorc.domain.sessions import TaskAgentSession, TaskSessionLifecycleStatus
from openorc.domain.tasks import Task, TaskStatus
from openorc.observability import TASK_ID, WORKSPACE_ID, injected_tracer_source
from openorc.persistence.pool import DatabasePool
from openorc.protocol.interaction import PlanReviewSubject, ReviewInteraction
from openorc.protocol.models import ReviewResult
from openorc.services.errors import (
    ConflictError,
    ExternalOperationFailedError,
    ExternalOperationUncertainError,
    InvalidCommandError,
    NotFoundError,
    StaleOperationError,
)
from openorc.services.task_plan_production import TaskPlanProductionOutcome
from openorc.services.task_planning_review_dispatch import (
    TaskPlanningReviewDispatchOutcome,
    dispatch_planning_review,
)
from tests.fakes.agent_runtime import (
    FakeAgentRuntimeAdapter,
    accepted_review_result_candidate,
    changes_requested_review_result_candidate,
    plan_result_candidate,
    session_ready_candidate,
)

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
    return (
        _TASK_ID,
        workspace_id,
        _REPOSITORY_ID,
        503,
        42,
        status,
        archived_at,
        None,
        state_token,
        current_plan_revision_id,
        None,
        "a" * 64,
        None,
        _OBSERVED,
        _OBSERVED,
        None,
        None,
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


def _script_first_review_entry(
    conn: ScriptedConnection,
    *,
    task_row: tuple[Any, ...],
    reviewer_row: tuple[Any, ...] | None,
    revision_row: tuple[Any, ...],
    workspace_row: tuple[Any, ...] | None = None,
    loop_row: tuple[Any, ...] | None = None,
    iteration_row: tuple[Any, ...] | None = None,
) -> None:
    """The first-review entry composition's canned sequence, in call order.

    No loop exists yet, so the composition establishes the OPEN planning
    loop (with the Workspace snapshot) and then locks/rechecks it before
    reserving iteration 1 for the exact current plan.
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


def _assert_no_reservation(conn: ScriptedConnection) -> None:
    assert not any(_LOOP_INSERT(sql, None) for sql, _ in conn.executed)
    assert not any(_ITERATION_INSERT(sql, None) for sql, _ in conn.executed)


def _assert_no_verdict_recorded(conn: ScriptedConnection) -> None:
    assert not any(_RESULT_UPDATE(sql, None) for sql, _ in conn.executed)


def _assert_no_task_mutation(conn: ScriptedConnection) -> None:
    assert not any(_TASK_WRITE(sql, None) for sql, _ in conn.executed)


class _Dispatch:
    """Assembled deterministic fixtures for one review-dispatch invocation."""

    def __init__(self) -> None:
        self.conn = ScriptedConnection()
        self.pool = FakePool(self.conn)

    def prepared_reviewer_adapter(self, outcomes: Sequence[Any]) -> _ProbedAdapter:
        return _prepared_reviewer_adapter(self.pool, outcomes)

    def run(
        self,
        *,
        adapter: FakeAgentRuntimeAdapter | None = None,
        plan_production: Any = None,
        workspace_id: Any = _WORKSPACE_ID,
        task_id: Any = _TASK_ID,
        profile_id: Any = _OWNER_PROFILE_ID,
    ) -> Any:
        return dispatch_planning_review(
            cast(DatabasePool, self.pool),
            (
                adapter
                if adapter is not None
                else self.prepared_reviewer_adapter([_review_candidate()])
            ),
            workspace_id=workspace_id,
            task_id=task_id,
            profile_id=profile_id,
            plan_production=(
                plan_production if plan_production is not None else _production_outcome()
            ),
        )


def test_first_review_creates_one_loop_one_pending_iteration_and_returns_the_typed_result() -> None:
    env = _Dispatch()
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
    adapter = env.prepared_reviewer_adapter([_review_candidate()])

    outcome = env.run(adapter=adapter)

    assert isinstance(outcome, TaskPlanningReviewDispatchOutcome)
    # Exactly one OPEN planning loop with the Workspace's snapshotted limit.
    assert outcome.review_loop.id == _LOOP_ID
    assert outcome.review_loop.purpose is ReviewLoopPurpose.PLANNING
    assert outcome.review_loop.status is ReviewLoopStatus.OPEN
    assert outcome.review_loop.iteration_limit == 5
    # Exactly one pending iteration bound to the exact current plan.
    assert outcome.review_iteration.id == _ITERATION_1_ID
    assert outcome.review_iteration.iteration_number == 1
    assert outcome.review_iteration.plan_revision_id == _REVISION_1_ID
    assert outcome.review_iteration.outcome is None
    assert outcome.review_iteration.summary is None
    assert outcome.review_iteration.findings is None
    assert outcome.review_iteration.decided_at is None
    # The typed Reviewer result is input for E3b, never a verdict or gate.
    assert isinstance(outcome.review_result, ReviewResult)
    assert outcome.review_result.outcome == "CHANGES_REQUESTED"
    assert outcome.review_result.summary == "Needs work"
    assert [(finding.summary, finding.details) for finding in outcome.review_result.findings] == [
        ("Clarify scope", "Say which module changes.")
    ]
    assert outcome.reviewer_session.id == _REVIEWER_SESSION_ID
    assert outcome.reviewer_session.lifecycle_status is TaskSessionLifecycleStatus.READY
    # The send happened with no database transaction open, and E3a recorded
    # no verdict, result, or Task mutation.
    assert env.pool.active_connections == 0
    _assert_no_verdict_recorded(env.conn)
    _assert_no_task_mutation(env.conn)


def test_review_message_and_typed_subject_carry_the_persisted_plan_and_identity() -> None:
    env = _Dispatch()
    _script_first_review_entry(
        env.conn,
        task_row=_task_row(),
        reviewer_row=_session_row(
            "reviewer", session_id=_REVIEWER_SESSION_ID, external_id=_REVIEWER_EXTERNAL_ID
        ),
        revision_row=_revision_row(),
    )
    adapter = env.prepared_reviewer_adapter([_review_candidate()])

    env.run(adapter=adapter)

    (control,) = adapter.realized_controls(_REVIEWER_EXTERNAL_ID)
    assert control.expected_family == "review_result"
    assert control.message is not None
    assert control.message == (
        f"Plan review for {_FULL_NAME} issue #42.\n"
        f"Review plan revision 1 (plan_revision_id {_REVISION_1_ID}).\n\n"
        f"{_PLAN_TEXT}"
    )
    interaction = control.interaction
    assert isinstance(interaction, ReviewInteraction)
    subject = interaction.subject
    assert isinstance(subject, PlanReviewSubject)
    assert subject.plan_revision_id == str(_REVISION_1_ID)
    assert subject.repository_full_name == _FULL_NAME
    assert subject.issue_number == 42
    assert subject.plan == _PLAN_TEXT
    (send,) = adapter.submitted_sends(_REVIEWER_EXTERNAL_ID)
    assert send.formal is True
    assert send.expected_family == "review_result"
    assert send.message == control.message
    # One REVIEW to the exact Reviewer session and nothing else.
    assert len(adapter.realized_controls()) == 1


def test_subsequent_plan_reuses_the_loop_and_next_number_despite_a_workspace_edit() -> None:
    env = _Dispatch()
    _script_reuse_entry(
        env.conn,
        task_row=_task_row(current_plan_revision_id=_REVISION_2_ID),
        reviewer_row=_session_row(
            "reviewer", session_id=_REVIEWER_SESSION_ID, external_id=_REVIEWER_EXTERNAL_ID
        ),
        revision_row=_revision_row(
            revision_id=_REVISION_2_ID, number=2, content=_REVISED_PLAN_TEXT
        ),
        # The Workspace setting was edited after the first review (7), but
        # the loop's stored budget (3) is what the loop keeps using.
        workspace_row=_ws_row(limit=7),
        existing_loops=[_loop_row(limit=3)],
        loop_row=_loop_row(limit=3),
        loop_iterations=[
            _iteration_row(
                iteration_id=_ITERATION_1_ID,
                number=1,
                outcome="changes_requested",
                summary="Needs work",
                findings=_REVIEW_FINDINGS,
                decided_at=_OBSERVED,
                plan_revision_id=_REVISION_1_ID,
            )
        ],
        iteration_row=_iteration_row(
            iteration_id=_ITERATION_2_ID,
            number=2,
            plan_revision_id=_REVISION_2_ID,
        ),
    )
    adapter = env.prepared_reviewer_adapter([_review_candidate()])

    outcome = env.run(
        adapter=adapter,
        plan_production=_production_outcome(
            revision_id=_REVISION_2_ID,
            number=2,
            content=_REVISED_PLAN_TEXT,
            current_plan_revision_id=_REVISION_2_ID,
        ),
    )

    # The same loop is reused — no second loop, stored budget preserved.
    assert outcome.review_loop.id == _LOOP_ID
    assert outcome.review_loop.iteration_limit == 3
    assert not any(_LOOP_INSERT(sql, None) for sql, _ in env.conn.executed)
    # The next iteration number is reserved for the exact current plan.
    assert outcome.review_iteration.iteration_number == 2
    assert outcome.review_iteration.plan_revision_id == _REVISION_2_ID
    assert outcome.review_iteration.outcome is None
    # The subject carries the persisted revised content.
    (control,) = adapter.realized_controls(_REVIEWER_EXTERNAL_ID)
    interaction = control.interaction
    assert isinstance(interaction, ReviewInteraction)
    subject = interaction.subject
    assert isinstance(subject, PlanReviewSubject)
    assert subject.plan_revision_id == str(_REVISION_2_ID)
    assert subject.plan == _REVISED_PLAN_TEXT


def test_stale_plan_revision_refuses_dispatch() -> None:
    env = _Dispatch()
    # The Task's current plan pointer has moved on to revision 2 while the
    # supplied E2 outcome still names revision 1.
    _script_first_review_entry(
        env.conn,
        task_row=_task_row(current_plan_revision_id=_REVISION_2_ID),
        reviewer_row=_session_row(
            "reviewer", session_id=_REVIEWER_SESSION_ID, external_id=_REVIEWER_EXTERNAL_ID
        ),
        revision_row=_revision_row(revision_id=_REVISION_1_ID),
    )
    adapter = env.prepared_reviewer_adapter([_review_candidate()])

    with pytest.raises(StaleOperationError):
        env.run(adapter=adapter)

    _assert_no_reservation(env.conn)
    _assert_no_verdict_recorded(env.conn)
    assert adapter.submitted_sends() == ()


def test_foreign_plan_revision_refuses_dispatch() -> None:
    env = _Dispatch()
    _script_first_review_entry(
        env.conn,
        task_row=_task_row(),
        reviewer_row=_session_row(
            "reviewer", session_id=_REVIEWER_SESSION_ID, external_id=_REVIEWER_EXTERNAL_ID
        ),
        revision_row=_revision_row(),
    )
    foreign = TaskPlanProductionOutcome(
        plan_revision=PlanRevision(
            id=_REVISION_1_ID,
            workspace_id=_WORKSPACE_ID,
            task_id=_OTHER_TASK_ID,
            revision_number=1,
            content=_PLAN_TEXT,
            repository_base_sha=_BASE_SHA,
            created_at=_OBSERVED,
        ),
        task=_task_domain(),
    )
    adapter = env.prepared_reviewer_adapter([_review_candidate()])

    with pytest.raises(InvalidCommandError):
        env.run(adapter=adapter, plan_production=foreign)

    _assert_no_reservation(env.conn)
    assert adapter.submitted_sends() == ()


def test_tampered_outcome_content_refuses_dispatch() -> None:
    env = _Dispatch()
    _script_first_review_entry(
        env.conn,
        task_row=_task_row(),
        reviewer_row=_session_row(
            "reviewer", session_id=_REVIEWER_SESSION_ID, external_id=_REVIEWER_EXTERNAL_ID
        ),
        revision_row=_revision_row(),
    )
    tampered = TaskPlanProductionOutcome(
        plan_revision=_revision_domain(_REVISION_1_ID, number=1, content=_REVISED_PLAN_TEXT),
        task=_task_domain(),
    )
    adapter = env.prepared_reviewer_adapter([_review_candidate()])

    with pytest.raises(ConflictError):
        env.run(adapter=adapter, plan_production=tampered)

    _assert_no_reservation(env.conn)
    assert adapter.submitted_sends() == ()


def test_unready_reviewer_refuses_dispatch() -> None:
    env = _Dispatch()
    _script_first_review_entry(
        env.conn,
        task_row=_task_row(),
        reviewer_row=_session_row(
            "reviewer", session_id=_REVIEWER_SESSION_ID, external_id=None, lifecycle="connecting"
        ),
        revision_row=_revision_row(),
    )
    adapter = env.prepared_reviewer_adapter([_review_candidate()])

    with pytest.raises(ConflictError):
        env.run(adapter=adapter)

    _assert_no_reservation(env.conn)
    assert adapter.submitted_sends() == ()


def test_missing_reviewer_binding_refuses_dispatch() -> None:
    env = _Dispatch()
    _script_first_review_entry(
        env.conn,
        task_row=_task_row(),
        reviewer_row=None,
        revision_row=_revision_row(),
    )
    adapter = env.prepared_reviewer_adapter([_review_candidate()])

    with pytest.raises(NotFoundError):
        env.run(adapter=adapter)

    _assert_no_reservation(env.conn)
    assert adapter.submitted_sends() == ()


def test_archived_task_refuses_dispatch() -> None:
    env = _Dispatch()
    # An archived Task has reached a terminal outcome, so it is no longer
    # current: the dispatch is stale and applies nothing.
    _script_first_review_entry(
        env.conn,
        task_row=_task_row(status="cancelled", archived_at=_OBSERVED),
        reviewer_row=_session_row(
            "reviewer", session_id=_REVIEWER_SESSION_ID, external_id=_REVIEWER_EXTERNAL_ID
        ),
        revision_row=_revision_row(),
    )
    adapter = env.prepared_reviewer_adapter([_review_candidate()])

    with pytest.raises(StaleOperationError):
        env.run(adapter=adapter)

    _assert_no_reservation(env.conn)
    assert adapter.submitted_sends() == ()


def test_stale_task_token_refuses_dispatch() -> None:
    env = _Dispatch()
    # The Task's token rotated after E2's post-update token: the
    # continuation is stale and applies nothing.
    _script_first_review_entry(
        env.conn,
        task_row=_task_row(state_token=uuid.uuid4()),
        reviewer_row=_session_row(
            "reviewer", session_id=_REVIEWER_SESSION_ID, external_id=_REVIEWER_EXTERNAL_ID
        ),
        revision_row=_revision_row(),
    )
    adapter = env.prepared_reviewer_adapter([_review_candidate()])

    with pytest.raises(StaleOperationError):
        env.run(adapter=adapter)

    _assert_no_reservation(env.conn)
    assert adapter.submitted_sends() == ()


def test_task_not_in_planning_refuses_dispatch() -> None:
    env = _Dispatch()
    _script_first_review_entry(
        env.conn,
        task_row=_task_row(status="waiting_for_owner"),
        reviewer_row=_session_row(
            "reviewer", session_id=_REVIEWER_SESSION_ID, external_id=_REVIEWER_EXTERNAL_ID
        ),
        revision_row=_revision_row(),
    )
    adapter = env.prepared_reviewer_adapter([_review_candidate()])

    with pytest.raises(ConflictError):
        env.run(adapter=adapter)

    _assert_no_reservation(env.conn)
    assert adapter.submitted_sends() == ()


def test_non_owner_profile_refuses_dispatch() -> None:
    env = _Dispatch()
    _script_first_review_entry(
        env.conn,
        task_row=_task_row(),
        reviewer_row=_session_row(
            "reviewer", session_id=_REVIEWER_SESSION_ID, external_id=_REVIEWER_EXTERNAL_ID
        ),
        revision_row=_revision_row(),
    )
    adapter = env.prepared_reviewer_adapter([_review_candidate()])

    with pytest.raises(NotFoundError):
        env.run(adapter=adapter, profile_id=_OTHER_PROFILE_ID)

    _assert_no_reservation(env.conn)
    assert adapter.submitted_sends() == ()


def test_closed_planning_loop_refuses_dispatch_without_creating_a_new_one() -> None:
    env = _Dispatch()
    _script_reuse_entry(
        env.conn,
        task_row=_task_row(),
        reviewer_row=_session_row(
            "reviewer", session_id=_REVIEWER_SESSION_ID, external_id=_REVIEWER_EXTERNAL_ID
        ),
        revision_row=_revision_row(),
        existing_loops=[_loop_row(status="closed", closed_at=_OBSERVED)],
        loop_row=_loop_row(status="closed", closed_at=_OBSERVED),
        loop_iterations=[],
        iteration_row=_iteration_row(),
    )
    adapter = env.prepared_reviewer_adapter([_review_candidate()])

    with pytest.raises(ConflictError):
        env.run(adapter=adapter)

    # The closed loop is never silently reopened or replaced with a second
    # loop: no new loop and no iteration can appear.
    _assert_no_reservation(env.conn)
    assert adapter.submitted_sends() == ()


def test_multiple_open_planning_loops_fail_closed() -> None:
    env = _Dispatch()
    _script_reuse_entry(
        env.conn,
        task_row=_task_row(),
        reviewer_row=_session_row(
            "reviewer", session_id=_REVIEWER_SESSION_ID, external_id=_REVIEWER_EXTERNAL_ID
        ),
        revision_row=_revision_row(),
        existing_loops=[_loop_row(), _loop_row(loop_id=_OTHER_LOOP_ID)],
        loop_row=_loop_row(),
        loop_iterations=[],
        iteration_row=_iteration_row(),
    )
    adapter = env.prepared_reviewer_adapter([_review_candidate()])

    with pytest.raises(ConflictError):
        env.run(adapter=adapter)

    _assert_no_reservation(env.conn)
    assert adapter.submitted_sends() == ()


def test_unresolved_older_attempt_blocks_a_new_dispatch() -> None:
    env = _Dispatch()
    # An unfinalized iteration is still pending on the older revision: no
    # new REVIEW for the current plan may bypass it.
    _script_reuse_entry(
        env.conn,
        task_row=_task_row(current_plan_revision_id=_REVISION_2_ID),
        reviewer_row=_session_row(
            "reviewer", session_id=_REVIEWER_SESSION_ID, external_id=_REVIEWER_EXTERNAL_ID
        ),
        revision_row=_revision_row(
            revision_id=_REVISION_2_ID, number=2, content=_REVISED_PLAN_TEXT
        ),
        existing_loops=[_loop_row()],
        loop_row=_loop_row(),
        loop_iterations=[
            _iteration_row(
                iteration_id=_ITERATION_1_ID,
                number=1,
                plan_revision_id=_REVISION_1_ID,
            )
        ],
        iteration_row=_iteration_row(iteration_id=_ITERATION_2_ID, number=2),
    )
    adapter = env.prepared_reviewer_adapter([_review_candidate()])

    with pytest.raises(ConflictError):
        env.run(
            adapter=adapter,
            plan_production=_production_outcome(
                revision_id=_REVISION_2_ID,
                number=2,
                content=_REVISED_PLAN_TEXT,
                current_plan_revision_id=_REVISION_2_ID,
            ),
        )

    _assert_no_reservation(env.conn)
    assert adapter.submitted_sends() == ()


def test_competing_iteration_reservation_is_a_stale_operation() -> None:
    env = _Dispatch()
    _script_entry_prefix(
        env.conn,
        task_row=_task_row(),
        reviewer_row=_session_row(
            "reviewer", session_id=_REVIEWER_SESSION_ID, external_id=_REVIEWER_EXTERNAL_ID
        ),
        revision_row=_revision_row(),
    )
    env.conn.on(_LOOPS_LIST, [])
    env.conn.on(_LOOP_INSERT, _loop_row(limit=5))
    env.conn.on(_LOOP_READ_LOCKED, _loop_row(limit=5))
    env.conn.on(_ITERATIONS_LIST, [])
    env.conn.on(_LOOP_GUARD_LOCKED, ("open", "planning"))
    # The unique per-loop numbering rejected the insert: a competing
    # reservation committed first, so this dispatch is stale.
    env.conn.on(
        _ITERATION_INSERT,
        UniqueViolation("a competing review iteration committed first"),
    )
    adapter = env.prepared_reviewer_adapter([_review_candidate()])

    with pytest.raises(StaleOperationError):
        env.run(adapter=adapter)

    assert adapter.submitted_sends() == ()


def test_known_send_failure_leaves_the_attempt_unresolved() -> None:
    env = _Dispatch()
    _script_first_review_entry(
        env.conn,
        task_row=_task_row(),
        reviewer_row=_session_row(
            "reviewer", session_id=_REVIEWER_SESSION_ID, external_id=_REVIEWER_EXTERNAL_ID
        ),
        revision_row=_revision_row(),
    )
    adapter = env.prepared_reviewer_adapter(
        [AgentRuntimeUnavailableError("the runtime is unreachable")]
    )

    with pytest.raises(ExternalOperationFailedError):
        env.run(adapter=adapter)

    # The attempt stays reserved and unresolved; the known failure is never
    # retried or resent here.
    assert any(_ITERATION_INSERT(sql, None) for sql, _ in env.conn.executed)
    _assert_no_verdict_recorded(env.conn)
    assert len(adapter.submitted_sends(_REVIEWER_EXTERNAL_ID)) == 1


def test_untyped_formal_answer_refuses_the_dispatch() -> None:
    env = _Dispatch()
    _script_first_review_entry(
        env.conn,
        task_row=_task_row(),
        reviewer_row=_session_row(
            "reviewer", session_id=_REVIEWER_SESSION_ID, external_id=_REVIEWER_EXTERNAL_ID
        ),
        revision_row=_revision_row(),
    )
    # A valid-looking formal response of the wrong family is a known
    # non-answer for the REVIEW interaction.
    adapter = env.prepared_reviewer_adapter([plan_result_candidate(_PLAN_TEXT)])

    with pytest.raises(ExternalOperationFailedError):
        env.run(adapter=adapter)

    assert any(_ITERATION_INSERT(sql, None) for sql, _ in env.conn.executed)
    _assert_no_verdict_recorded(env.conn)
    assert len(adapter.submitted_sends(_REVIEWER_EXTERNAL_ID)) == 1


def test_typed_accepted_result_is_returned_for_e3b() -> None:
    env = _Dispatch()
    _script_first_review_entry(
        env.conn,
        task_row=_task_row(),
        reviewer_row=_session_row(
            "reviewer", session_id=_REVIEWER_SESSION_ID, external_id=_REVIEWER_EXTERNAL_ID
        ),
        revision_row=_revision_row(),
    )
    adapter = env.prepared_reviewer_adapter([accepted_review_result_candidate("Looks good")])

    outcome = env.run(adapter=adapter)

    assert isinstance(outcome.review_result, ReviewResult)
    assert outcome.review_result.outcome == "ACCEPTED"
    assert outcome.review_result.summary == "Looks good"
    assert outcome.review_result.findings == ()
    # E3a still records no verdict of its own: the typed result is only the
    # continuation input for the E3b decision finalization.
    _assert_no_verdict_recorded(env.conn)
    _assert_no_task_mutation(env.conn)


def test_uncertain_send_cannot_silently_produce_a_second_review_or_a_verdict() -> None:
    env = _Dispatch()
    _script_first_review_entry(
        env.conn,
        task_row=_task_row(),
        reviewer_row=_session_row(
            "reviewer", session_id=_REVIEWER_SESSION_ID, external_id=_REVIEWER_EXTERNAL_ID
        ),
        revision_row=_revision_row(),
    )
    adapter = env.prepared_reviewer_adapter(
        [AgentRuntimeDeliveryUncertainError("the delivery outcome is unknown")]
    )

    with pytest.raises(ExternalOperationUncertainError):
        env.run(adapter=adapter)

    # The attempt was durably reserved before the send and stays unresolved:
    # no verdict is inferred, and nothing is resent.
    assert any(_ITERATION_INSERT(sql, None) for sql, _ in env.conn.executed)
    _assert_no_verdict_recorded(env.conn)
    _assert_no_task_mutation(env.conn)
    assert len(adapter.submitted_sends(_REVIEWER_EXTERNAL_ID)) == 1

    # A later dispatch against the same loop state is refused: the
    # unresolved attempt cannot be bypassed by a new REVIEW.
    second = _Dispatch()
    _script_reuse_entry(
        second.conn,
        task_row=_task_row(),
        reviewer_row=_session_row(
            "reviewer", session_id=_REVIEWER_SESSION_ID, external_id=_REVIEWER_EXTERNAL_ID
        ),
        revision_row=_revision_row(),
        existing_loops=[_loop_row(limit=5)],
        loop_row=_loop_row(limit=5),
        loop_iterations=[
            _iteration_row(
                iteration_id=_ITERATION_1_ID,
                number=1,
                plan_revision_id=_REVISION_1_ID,
            )
        ],
        iteration_row=_iteration_row(iteration_id=_ITERATION_2_ID, number=2),
    )
    second_adapter = second.prepared_reviewer_adapter([_review_candidate()])

    with pytest.raises(ConflictError):
        second.run(adapter=second_adapter)

    _assert_no_reservation(second.conn)
    assert second_adapter.submitted_sends() == ()


def test_service_span_carries_only_safe_annotations_and_fails_closed_safely() -> None:
    provider = TracerProvider()
    exporter = InMemorySpanExporter()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    env = _Dispatch()
    _script_first_review_entry(
        env.conn,
        task_row=_task_row(),
        reviewer_row=_session_row(
            "reviewer", session_id=_REVIEWER_SESSION_ID, external_id=_REVIEWER_EXTERNAL_ID
        ),
        revision_row=_revision_row(),
    )
    adapter = env.prepared_reviewer_adapter([_review_candidate()])
    with injected_tracer_source(lambda name: provider.get_tracer(name)):
        env.run(adapter=adapter)
    (span,) = [
        candidate
        for candidate in exporter.get_finished_spans()
        if candidate.name == "task_planning_review_dispatch.dispatch_planning_review"
    ]
    attributes = span.attributes or {}
    assert attributes[WORKSPACE_ID] == str(_WORKSPACE_ID)
    assert attributes[TASK_ID] == str(_TASK_ID)
    assert span.status is not None
    assert span.status.status_code is StatusCode.UNSET

    failed_provider = TracerProvider()
    failed_exporter = InMemorySpanExporter()
    failed_provider.add_span_processor(SimpleSpanProcessor(failed_exporter))
    failing = _Dispatch()
    _script_first_review_entry(
        failing.conn,
        task_row=_task_row(),
        reviewer_row=_session_row(
            "reviewer", session_id=_REVIEWER_SESSION_ID, external_id=_REVIEWER_EXTERNAL_ID
        ),
        revision_row=_revision_row(),
    )
    failing_adapter = failing.prepared_reviewer_adapter(
        [AgentRuntimeDeliveryUncertainError("the delivery outcome is unknown")]
    )
    with (
        injected_tracer_source(lambda name: failed_provider.get_tracer(name)),
        pytest.raises(ExternalOperationUncertainError),
    ):
        failing.run(adapter=failing_adapter)
    (failed_span,) = [
        candidate
        for candidate in failed_exporter.get_finished_spans()
        if candidate.name == "task_planning_review_dispatch.dispatch_planning_review"
    ]
    assert failed_span.status is not None
    assert failed_span.status.status_code is StatusCode.ERROR
    # The failure telemetry is a safe classification only: the description
    # is the exception type name, never arbitrary message content.
    assert failed_span.status.description == "ExternalOperationUncertainError"


def test_replaying_a_reviewed_revision_production_outcome_cannot_dispatch_again() -> None:
    """Issue #210: after an in-budget finalization, the still-current token
    cannot launch another ordinary REVIEW of the same plan before E2
    publishes the next revision — protecting an in-flight E2 operation from
    being superseded by a duplicate same-plan iteration."""
    conn = ScriptedConnection()
    fake_pool = FakePool(conn)
    _script_reuse_entry(
        conn,
        task_row=_task_row(),
        reviewer_row=_session_row(
            "reviewer", session_id=_REVIEWER_SESSION_ID, external_id=_REVIEWER_EXTERNAL_ID
        ),
        revision_row=_revision_row(),
        existing_loops=[_loop_row(limit=5)],
        loop_row=_loop_row(limit=5),
        loop_iterations=[
            _iteration_row(
                outcome="changes_requested",
                summary="Needs work",
                findings=_REVIEW_FINDINGS,
                decided_at=_OBSERVED,
            )
        ],
        iteration_row=_iteration_row(),
    )
    adapter = _prepared_reviewer_adapter(fake_pool, [_review_candidate()])

    with pytest.raises(ConflictError):
        dispatch_planning_review(
            _pool(conn),
            adapter,
            profile_id=_OWNER_PROFILE_ID,
            workspace_id=_WORKSPACE_ID,
            task_id=_TASK_ID,
            plan_production=_production_outcome(),
        )

    _assert_no_reservation(conn)
    _assert_no_task_mutation(conn)
    assert len(adapter.realized_controls(_REVIEWER_EXTERNAL_ID)) == 0
