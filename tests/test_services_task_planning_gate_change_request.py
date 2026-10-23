"""Deterministic tests for the E3 implementation-gate change-request handoff (issue #219).

A scripted SQL-connection seam plus the #69 fake Agent Runtime driving the
merged E2 revision production prove the narrow G1 handoff seam on its
critical path:

- a typed formal ``CHANGES_REQUESTED`` at the exact current pending
  plan-subject IMPLEMENTATION_AUTHORIZATION gate appends one NEW same-plan
  next-numbered immutable ReviewIteration, records the REVIEWER
  ``PLAN_REVIEWED`` event, supersedes the gate through the narrow
  conditional persistence primitive — the current-gate pointer clears, the
  Task moves ``waiting_for_owner`` → ``planning`` with a rotated token,
  and the gate row itself stays untouched (still PENDING with no
  ``decided_at`` stamp, never an Owner verdict, never the
  REVIEW_RESOLUTION-only SETTLED status, and no ``OWNER_GATE_RESOLVED``
  event) — and routes ONLY the newly finalized findings to real E2;
- the two direction-only discussion message events ride the same
  transaction when ``record_discussion_message_events`` is set (E4's
  formal-exchange path);
- only a ``CHANGES_REQUESTED`` result is actionable at the implementation
  gate: an ACCEPTED result, a non-``ReviewResult`` input, or a
  missing/invalid E2 dependency fails closed at entry before any
  transaction or durable effect (the no-stranding guarantee);
- every stale, foreign, replayed, closed-loop, non-READY, or superseded
  input fails closed and applies nothing — no E2 send ever follows a
  failed handoff.

The planning ReviewLoop stays OPEN across the whole seam (issue #219); a
later Reviewer ACCEPTED reaches E5 for a NEW implementation gate. No live
GitHub, runtime, or Postgres access; durable atomicity is proven by the
integration-marked suite against a real Postgres.
"""

from __future__ import annotations

import uuid
from collections.abc import Callable, Iterator, Sequence
from contextlib import contextmanager
from datetime import UTC, datetime
from typing import Any, cast

import pytest

from openorc.adapters.agent_runtime.results import AgentSessionCreationRequest
from openorc.adapters.github import (
    GitHubBranchObservation,
    GitHubRepositoryObservation,
)
from openorc.domain.connections import WorkflowRole
from openorc.domain.gates import OwnerGateStatus, OwnerGateType
from openorc.domain.planning import PlanRevision
from openorc.domain.reviews import ReviewLoopStatus, ReviewOutcome
from openorc.domain.tasks import Task, TaskStatus
from openorc.persistence.pool import DatabasePool
from openorc.protocol.models import ReviewFinding, ReviewResult
from openorc.services.errors import (
    ConflictError,
    InvalidCommandError,
    StaleOperationError,
)
from openorc.services.task_plan_production import TaskPlanProductionOutcome
from openorc.services.task_planning_review_resolution import (
    PlanningGateChangeRequestDecision,
    supersede_implementation_authorization_with_changes,
)
from tests.fakes.agent_runtime import (
    FakeAgentRuntimeAdapter,
    plan_result_candidate,
    session_ready_candidate,
)

_OBSERVED = datetime(2026, 10, 19, 12, 0, 0, tzinfo=UTC)
_WORKSPACE_ID = uuid.uuid4()
_OWNER_PROFILE_ID = uuid.uuid4()
_TASK_ID = uuid.uuid4()
_REPOSITORY_ID = uuid.uuid4()
_INSTALLATION_ID = uuid.uuid4()
_EXTERNAL_INSTALLATION_ID = 12345678
_GITHUB_REPOSITORY_ID = 987654321
_CONNECTION_ID = uuid.uuid4()
_PRODUCER_SESSION_ID = uuid.uuid4()
_REVIEWER_SESSION_ID = uuid.uuid4()
_PRODUCER_EXTERNAL_ID = "fake-session-1"
_REVIEWER_EXTERNAL_ID = "fake-session-2"
_STATE_TOKEN = uuid.uuid4()
_TOKEN_2 = uuid.uuid4()
_TOKEN_3 = uuid.uuid4()
_TOKEN_5 = uuid.uuid4()
_REVISION_1_ID = uuid.uuid4()
_REVISION_2_ID = uuid.uuid4()
_LOOP_ID = uuid.uuid4()
_ITERATION_1_ID = uuid.uuid4()
_ITERATION_2_ID = uuid.uuid4()
_GATE_1_ID = uuid.uuid4()
_BASE_SHA = "c" * 40
_PLAN_TEXT = "# Plan\n\nStep one.\n"
_REVISED_PLAN_TEXT = "# Plan (revised)\n\nStep one, clarified.\n"
_NEW_FINDINGS = [{"summary": "Clarify the retry loop", "details": "Explain the backoff path."}]
_DISCUSSION_EVENT_TYPE = "owner_reviewer_discussion_message"

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
        raise AssertionError("gate change request tests never close pools")


def _pool(conn: ScriptedConnection) -> DatabasePool:
    return cast(DatabasePool, FakePool(conn))


_TASK_READ = _sql_lacks(_sql_has("from openorc.tasks where id"), "for update")
_TASK_READ_LOCKED = _sql_has("from openorc.tasks where id", "for update")
_TASK_GATE_WRITE = _sql_has("update openorc.tasks", "current_owner_gate_id")
_STATUS_UPDATE = _sql_has("update openorc.tasks", "queued_at = case")
_POINTER_UPDATE = _sql_has("update openorc.tasks", "current_plan_revision_id")
_REVISION_READ = _sql_has("from openorc.plan_revisions where id")
_REVISIONS_HISTORY = _sql_has("from openorc.plan_revisions where task_id")
_REVISION_INSERT = _sql_has("insert into openorc.plan_revisions")
_SESSION_READ = _sql_has("from openorc.task_agent_sessions")
_REPO_READ = _sql_has("from openorc.repositories where id")
_INSTALLATION_READ = _sql_has("from openorc.github_installations where id")
_LOOPS_LIST = _sql_has("from openorc.review_loops where task_id")
_LOOP_READ = _sql_lacks(_sql_has("from openorc.review_loops where id"), "for update")
_LOOP_READ_LOCKED = _sql_lacks(
    _sql_has("from openorc.review_loops where id", "for update"), "select status, purpose"
)
_LOOP_GUARD_LOCKED = _sql_has("select status, purpose from openorc.review_loops", "for update")
# The supersession primitive's one conditional Task update; E2's pointer
# update carries a different SET list (its RETURNING clause shares column
# names, so the broader task-write predicates are ambiguous).
_SUPERSESSION_WRITE = _sql_has(
    "update openorc.tasks", "current_owner_gate_id = null", "status = %s"
)
_ITERATION_READ = _sql_has("from openorc.review_iterations where id")
_ITERATIONS_LIST = _sql_has("review_iterations where review_loop_id")
_ITERATION_INSERT = _sql_has("insert into openorc.review_iterations")
_RESULT_UPDATE = _sql_has("update openorc.review_iterations")
_GATES_LIST = _sql_has("from openorc.owner_gates where task_id")
_GATE_READ = _sql_has("from openorc.owner_gates where id")
_GATE_INSERT = _sql_has("insert into openorc.owner_gates")
_GATE_UPDATE = _sql_has("update openorc.owner_gates")
_EVENT_INSERT = _sql_has("insert into openorc.workflow_events")


def _ws_row(*, limit: int = 2, owner: Any = _OWNER_PROFILE_ID) -> tuple[Any, ...]:
    return (_WORKSPACE_ID, owner, "platform", _OBSERVED, _OBSERVED, limit, "")


def _task_row(
    *,
    status: str = "planning",
    state_token: Any = _STATE_TOKEN,
    current_plan_revision_id: Any = _REVISION_1_ID,
    gate_id: Any = None,
) -> tuple[Any, ...]:
    return (
        _TASK_ID,
        _WORKSPACE_ID,
        _REPOSITORY_ID,
        503,
        42,
        status,
        None,
        None,
        state_token,
        current_plan_revision_id,
        gate_id,
        "a" * 64,
        None,
        _OBSERVED,
        _OBSERVED,
        None,
    )


def _session_row(
    role: str,
    *,
    session_id: Any,
    external_id: str | None,
) -> tuple[Any, ...]:
    initialized = external_id is not None
    return (
        session_id,
        _WORKSPACE_ID,
        _TASK_ID,
        role,
        _CONNECTION_ID,
        external_id,
        "ready" if initialized else "connecting",
        {"adapter": "cline"} if initialized else None,
        None,
        None,
        None,
        _OBSERVED if initialized else None,
        None,
        _OBSERVED,
        _OBSERVED,
    )


def _revision_row(*, revision_id: Any, number: int, content: str = _PLAN_TEXT) -> tuple[Any, ...]:
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
        _INSTALLATION_ID,
    )


def _installation_row() -> tuple[Any, ...]:
    return (
        _INSTALLATION_ID,
        _WORKSPACE_ID,
        _EXTERNAL_INSTALLATION_ID,
        501,
        "octocat",
        "Organization",
        None,
        _OBSERVED,
        _OBSERVED,
    )


def _loop_row(
    *,
    status: str = "open",
    limit: int = 2,
    closed_at: Any = None,
) -> tuple[Any, ...]:
    return (_LOOP_ID, _WORKSPACE_ID, _TASK_ID, "planning", limit, status, closed_at, _OBSERVED)


def _iteration_row(
    *,
    iteration_id: Any,
    number: int,
    plan_revision_id: Any,
    outcome: str | None = None,
    summary: str | None = None,
    findings: Any = None,
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


def _gate_row(
    *,
    gate_id: Any = _GATE_1_ID,
    gate_type: str = "implementation_authorization",
    plan_revision_id: Any = _REVISION_1_ID,
    status: str = "pending",
    head_sha: str | None = None,
    pr_id: Any = None,
) -> tuple[Any, ...]:
    return (
        gate_id,
        _WORKSPACE_ID,
        _TASK_ID,
        gate_type,
        status,
        plan_revision_id,
        head_sha,
        pr_id,
        _OBSERVED if status != "pending" else None,
        _OBSERVED,
    )


def _event_row() -> tuple[Any, ...]:
    return (
        uuid.uuid4(),
        _WORKSPACE_ID,
        _TASK_ID,
        "plan_reviewed",
        "reviewer",
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
    gate_id: Any = None,
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
        current_owner_gate_id=gate_id,
        source_requirements_fingerprint="a" * 64,
        created_at=_OBSERVED,
        updated_at=_OBSERVED,
    )


def _production_outcome(
    *,
    revision_id: Any = _REVISION_2_ID,
    number: int = 2,
    content: str = _REVISED_PLAN_TEXT,
    state_token: Any = _TOKEN_3,
    current_plan_revision_id: Any = _REVISION_2_ID,
) -> TaskPlanProductionOutcome:
    return TaskPlanProductionOutcome(
        plan_revision=_revision_domain(revision_id, number=number, content=content),
        task=_task_domain(
            state_token=state_token, current_plan_revision_id=current_plan_revision_id
        ),
    )


class _ProbedAdapter(FakeAgentRuntimeAdapter):
    """Fake runtime proving every send runs with no transaction open."""

    def __init__(self, pool: FakePool) -> None:
        super().__init__()
        self._resolution_pool = pool

    def realize_control(
        self,
        session_id: str,
        interaction: Any,
        *,
        message: str | None = None,
        expected_family: str | None = None,
    ) -> Any:
        assert self._resolution_pool.active_connections == 0, (
            "the service must never hold a database transaction open across a send"
        )
        return super().realize_control(
            session_id, interaction, message=message, expected_family=expected_family
        )


def _prepared_adapter(
    pool: FakePool, *, producer_outcomes: Sequence[Any], reviewer_outcomes: Sequence[Any]
) -> _ProbedAdapter:
    """One fake runtime holding the live Producer and Reviewer sessions."""
    adapter = _ProbedAdapter(pool)
    adapter.queue_creation(session_ready_candidate())
    producer = adapter.create_session(
        AgentSessionCreationRequest(
            workspace_id=_WORKSPACE_ID,
            task_id=_TASK_ID,
            role=WorkflowRole.PRODUCER,
            initialization="prepared",
        )
    )
    assert producer.external_session_id == _PRODUCER_EXTERNAL_ID
    adapter.queue_creation(session_ready_candidate())
    reviewer = adapter.create_session(
        AgentSessionCreationRequest(
            workspace_id=_WORKSPACE_ID,
            task_id=_TASK_ID,
            role=WorkflowRole.REVIEWER,
            initialization="prepared",
        )
    )
    assert reviewer.external_session_id == _REVIEWER_EXTERNAL_ID
    for outcome in producer_outcomes:
        adapter.queue_send(_PRODUCER_EXTERNAL_ID, outcome)
    for outcome in reviewer_outcomes:
        adapter.queue_send(_REVIEWER_EXTERNAL_ID, outcome)
    return adapter


class FakeGitHubClient:
    """Scripted GitHub observation fake recording call order; zero calls is
    assertable. Each call also proves no database transaction was open."""

    def __init__(self, pool: FakePool) -> None:
        self._pool = pool
        self.repository_calls: list[tuple[int, int]] = []
        self.branch_calls: list[tuple[int, str, str, str]] = []

    def get_installation_repository(
        self, *, github_installation_id: int, github_repository_id: int
    ) -> GitHubRepositoryObservation:
        assert self._pool.active_connections == 0, (
            "the service must never hold a database transaction open across the GitHub call"
        )
        self.repository_calls.append((github_installation_id, github_repository_id))
        return GitHubRepositoryObservation(
            github_repository_id=_GITHUB_REPOSITORY_ID,
            owner_login="octocat",
            name="hello-world",
            html_url="https://github.com/octocat/hello-world",
            is_private=False,
            default_branch="main",
        )

    def get_repository_branch(
        self,
        *,
        github_installation_id: int,
        owner_login: str,
        repository_name: str,
        branch_name: str,
    ) -> GitHubBranchObservation:
        assert self._pool.active_connections == 0, (
            "the service must never hold a database transaction open across the GitHub call"
        )
        self.branch_calls.append(
            (github_installation_id, owner_login, repository_name, branch_name)
        )
        return GitHubBranchObservation(branch_name=branch_name, head_sha=_BASE_SHA)


# --- scripted sequences (in service call order) ---------------------------------


def _script_repeating_reads(conn: ScriptedConnection) -> None:
    """Same-shaped reads whose result never changes across the whole flow."""
    conn.on_repeat(_sql_has("from openorc.profiles", "for key share"), (None, None, None))
    conn.on_repeat(
        _role_is(_SESSION_READ, "reviewer"),
        _session_row(
            "reviewer", session_id=_REVIEWER_SESSION_ID, external_id=_REVIEWER_EXTERNAL_ID
        ),
    )
    conn.on_repeat(
        _role_is(_SESSION_READ, "producer"),
        _session_row(
            "producer", session_id=_PRODUCER_SESSION_ID, external_id=_PRODUCER_EXTERNAL_ID
        ),
    )
    conn.on_repeat(_REPO_READ, _repository_row())
    conn.on_repeat(_INSTALLATION_READ, _installation_row())
    conn.on_repeat(_LOOP_READ, _loop_row())
    conn.on_repeat(_LOOP_READ_LOCKED, _loop_row())
    conn.on_repeat(_LOOP_GUARD_LOCKED, ("open", "planning"))
    conn.on_repeat(_EVENT_INSERT, _event_row())


def _script_handoff(
    conn: ScriptedConnection,
    *,
    supersession_task_row: tuple[Any, ...] | None = None,
    fail_audit: bool = False,
) -> None:
    """The G1 handoff transaction in call order, plus E2's production.

    ``supersession_task_row`` overrides the supersession primitive's locked
    Task read (for the raced/lost-currency stale case); ``fail_audit`` makes
    the second event insert (the first direction-only discussion fact) fail
    after the ``plan_reviewed`` event committed inside the transaction.
    """
    if fail_audit:
        # Registered before the repeating event handler so the first two
        # event inserts consume them in order.
        conn.on(_EVENT_INSERT, _event_row())
        conn.on(_EVENT_INSERT, RuntimeError("the audit write failed"))
    _script_repeating_reads(conn)
    t_waiting = _task_row(status="waiting_for_owner", state_token=_STATE_TOKEN, gate_id=_GATE_1_ID)
    t_planning = _task_row(status="planning", state_token=_TOKEN_2, gate_id=None)
    rev1 = _revision_row(revision_id=_REVISION_1_ID, number=1)
    it1 = _iteration_row(
        iteration_id=_ITERATION_1_ID,
        number=1,
        plan_revision_id=_REVISION_1_ID,
        outcome="accepted",
        summary="Looks good",
        findings=[],
        decided_at=_OBSERVED,
    )
    it2 = _iteration_row(
        iteration_id=_ITERATION_2_ID,
        number=2,
        plan_revision_id=_REVISION_1_ID,
        outcome="changes_requested",
        summary="Needs another pass",
        findings=_NEW_FINDINGS,
        decided_at=_OBSERVED,
    )
    it2_unfin = _iteration_row(
        iteration_id=_ITERATION_2_ID, number=2, plan_revision_id=_REVISION_1_ID
    )
    gate = _gate_row()

    conn.on_repeat(_sql_has("from openorc.workspaces where id"), _ws_row())
    conn.on(_TASK_READ, t_waiting)  # handoff workspace guard
    conn.on(_TASK_READ_LOCKED, t_waiting)  # handoff Task lock
    conn.on(_TASK_READ, t_waiting)  # handoff currentness
    conn.on_repeat(_GATE_READ, gate)  # the guard + the primitive's re-read
    conn.on_repeat(_REVISION_READ, rev1)
    conn.on_repeat(_LOOPS_LIST, [_loop_row()])
    conn.on(_ITERATIONS_LIST, [it1])  # the allocation's numbering history
    conn.on(_ITERATION_INSERT, it2_unfin)
    conn.on(_RESULT_UPDATE, it2)
    conn.on_repeat(_ITERATIONS_LIST, [it1, it2])
    # The supersession primitive: gate re-read (repeat), Task lock, and the
    # conditional pointer/status/token update.
    conn.on(_TASK_READ_LOCKED, supersession_task_row or t_waiting)
    conn.on(_TASK_GATE_WRITE, t_planning)
    # --- E2 revision production (entry + finalize), in call order. ---
    rev2 = _revision_row(revision_id=_REVISION_2_ID, number=2, content=_REVISED_PLAN_TEXT)
    t_pointer = _task_row(
        status="planning", state_token=_TOKEN_3, current_plan_revision_id=_REVISION_2_ID
    )
    conn.on(_TASK_READ, t_planning)  # E2 workspace guard
    conn.on(_TASK_READ_LOCKED, t_planning)  # E2 Task lock
    conn.on(_TASK_READ, t_planning)  # E2 entry currentness
    conn.on(_ITERATION_READ, it2)
    conn.on(_REVISIONS_HISTORY, [rev1])
    conn.on(_TASK_READ, t_planning)  # E2 finalize currentness
    conn.on(_REVISION_INSERT, rev2)
    conn.on(_POINTER_UPDATE, t_pointer)


def _script_handoff_entry(
    conn: ScriptedConnection,
    *,
    gate: tuple[Any, ...] | None = None,
    task: tuple[Any, ...] | None = None,
) -> None:
    """The entry reads up to (and including) the gate read, for fail-closed cases."""
    conn.on_repeat(_sql_has("from openorc.profiles", "for key share"), (None, None, None))
    conn.on_repeat(_sql_has("from openorc.workspaces where id"), _ws_row())
    t_waiting = task or _task_row(
        status="waiting_for_owner", state_token=_STATE_TOKEN, gate_id=_GATE_1_ID
    )
    conn.on(_TASK_READ, t_waiting)  # workspace guard
    conn.on(_TASK_READ_LOCKED, t_waiting)  # supersession lock
    conn.on(_TASK_READ, t_waiting)  # currentness
    conn.on(_GATE_READ, gate or _gate_row())


def _changes_requested_result() -> ReviewResult:
    return ReviewResult(
        outcome="CHANGES_REQUESTED",
        summary="Needs another pass",
        findings=(
            ReviewFinding(summary="Clarify the retry loop", details="Explain the backoff path."),
        ),
    )


def _accepted_result() -> ReviewResult:
    return ReviewResult(outcome="ACCEPTED", summary="Everything checks out.", findings=())


def _handoff_call(
    pool: DatabasePool,
    adapter: Any,
    github: Any,
    *,
    expected_state_token: Any = _STATE_TOKEN,
    review_result: Any = None,
    record_discussion_message_events: bool = True,
) -> Any:
    return supersede_implementation_authorization_with_changes(
        pool,
        adapter,
        github,
        profile_id=_OWNER_PROFILE_ID,
        workspace_id=_WORKSPACE_ID,
        task_id=_TASK_ID,
        expected_state_token=expected_state_token,
        review_result=_changes_requested_result() if review_result is None else review_result,
        record_discussion_message_events=record_discussion_message_events,
    )


def test_a_change_request_at_the_implementation_gate_hands_planning_back() -> None:
    """The typed formal change request at the exact current pending
    plan-subject IMPLEMENTATION_AUTHORIZATION gate appends one NEW
    same-plan next-numbered iteration, supersedes the gate atomically
    (pointer clear, ``waiting_for_owner`` → ``planning``, token rotation,
    gate row untouched), records the safe audit trail, and routes ONLY the
    newly finalized findings to real E2."""
    conn = ScriptedConnection()
    fake_pool = FakePool(conn)
    pool = _pool(conn)
    _script_handoff(conn)
    adapter = _prepared_adapter(
        fake_pool,
        producer_outcomes=[plan_result_candidate(_REVISED_PLAN_TEXT)],
        reviewer_outcomes=[],
    )
    github = FakeGitHubClient(fake_pool)

    outcome = _handoff_call(pool, adapter, github)

    assert outcome.decision is PlanningGateChangeRequestDecision.PLANNING_CONTINUATION_ROUTED
    assert outcome.review_iteration.id == _ITERATION_2_ID
    assert outcome.review_iteration.outcome is ReviewOutcome.CHANGES_REQUESTED
    assert outcome.review_iteration.findings == _NEW_FINDINGS
    assert outcome.plan_revision.id == _REVISION_1_ID
    assert outcome.review_loop.status is ReviewLoopStatus.OPEN
    assert outcome.task.status is TaskStatus.PLANNING
    assert outcome.task.state_token == _TOKEN_3
    assert outcome.task.current_plan_revision_id == _REVISION_2_ID
    assert outcome.superseded_gate.status is OwnerGateStatus.PENDING
    assert outcome.superseded_gate.gate_type is OwnerGateType.IMPLEMENTATION_AUTHORIZATION
    assert outcome.revision_production is not None
    # The safe audit trail: the REVIEWER plan review, the two direction-only
    # discussion facts, and E2's revision event — never an Owner
    # ``OWNER_GATE_RESOLVED`` verdict for the superseded gate.
    assert _event_types(conn) == [
        "plan_reviewed",
        _DISCUSSION_EVENT_TYPE,
        _DISCUSSION_EVENT_TYPE,
        "plan_revision_revised",
    ]
    assert _event_params_of_type(conn, "owner_gate_resolved") == []
    (owner_event, reviewer_event) = _event_params_of_type(conn, _DISCUSSION_EVENT_TYPE)
    assert owner_event[3] == "owner"
    assert owner_event[4] == str(_OWNER_PROFILE_ID)
    assert owner_event[7].obj == {"direction": "owner_to_reviewer"}
    assert reviewer_event[3] == "reviewer"
    assert reviewer_event[4] == str(_REVIEWER_SESSION_ID)
    assert reviewer_event[7].obj == {"direction": "reviewer_to_owner"}
    # The gate row was never rewritten and no replacement gate was created.
    assert _count(conn, _GATE_UPDATE) == 0
    assert _count(conn, _GATE_INSERT) == 0
    # The Task transition came from the conditional supersession primitive,
    # never from the generic status update and never a gate-row rewrite.
    assert _count(conn, _SUPERSESSION_WRITE) == 1
    assert _count(conn, _GATE_UPDATE) == 0
    assert _count(conn, _STATUS_UPDATE) == 0
    # Exactly the NEW findings routed to E2, with the Producer attachment
    # acquired around the post-commit route and no transaction open across it.
    (revise_send,) = adapter.realized_controls(_PRODUCER_EXTERNAL_ID)
    assert revise_send.message is not None
    assert "Clarify the retry loop" in revise_send.message
    assert len(adapter.realized_controls(_REVIEWER_EXTERNAL_ID)) == 0
    assert github.repository_calls != []
    assert github.branch_calls != []


def test_an_accepted_result_is_never_actionable_at_the_implementation_gate() -> None:
    """Issue #219: only a CHANGES_REQUESTED result is actionable at the
    implementation gate; an ACCEPTED reply is E4's advisory conversation and
    is refused at entry before any read or effect."""
    conn = ScriptedConnection()
    fake_pool = FakePool(conn)
    pool = _pool(conn)
    adapter = _prepared_adapter(fake_pool, producer_outcomes=[], reviewer_outcomes=[])
    github = FakeGitHubClient(fake_pool)

    with pytest.raises(InvalidCommandError):
        _handoff_call(pool, adapter, github, review_result=_accepted_result())

    assert conn.executed == []
    assert len(adapter.realized_controls(_PRODUCER_EXTERNAL_ID)) == 0


def test_a_non_formal_input_is_refused_before_any_effect() -> None:
    """A bare string is never a formal result: the seam demands the validated
    typed protocol model and touches nothing."""
    conn = ScriptedConnection()
    fake_pool = FakePool(conn)
    pool = _pool(conn)
    adapter = _prepared_adapter(fake_pool, producer_outcomes=[], reviewer_outcomes=[])
    github = FakeGitHubClient(fake_pool)

    with pytest.raises(InvalidCommandError):
        _handoff_call(pool, adapter, github, review_result="not a review result")  # type: ignore[arg-type]

    assert conn.executed == []
    assert len(adapter.realized_controls(_PRODUCER_EXTERNAL_ID)) == 0


def test_a_stale_token_supersedes_nothing() -> None:
    """A moved-on Task token never supersedes the gate, records an iteration,
    or routes E2."""
    conn = ScriptedConnection()
    fake_pool = FakePool(conn)
    pool = _pool(conn)
    _script_handoff_entry(conn)
    adapter = _prepared_adapter(fake_pool, producer_outcomes=[], reviewer_outcomes=[])
    github = FakeGitHubClient(fake_pool)

    with pytest.raises(StaleOperationError):
        _handoff_call(pool, adapter, github, expected_state_token=_TOKEN_5)

    assert _count(conn, _ITERATION_INSERT) == 0
    assert _count(conn, _TASK_GATE_WRITE) == 0
    assert _count(conn, _GATE_UPDATE) == 0
    assert _count(conn, _EVENT_INSERT) == 0
    assert len(adapter.realized_controls(_PRODUCER_EXTERNAL_ID)) == 0


def test_a_non_implementation_gate_never_supersedes_through_this_seam() -> None:
    """The Task's current gate must be the exact plan-subject
    IMPLEMENTATION_AUTHORIZATION gate; any other gate type fails closed."""
    conn = ScriptedConnection()
    fake_pool = FakePool(conn)
    pool = _pool(conn)
    _script_handoff_entry(conn, gate=_gate_row(gate_type="review_resolution"))
    adapter = _prepared_adapter(fake_pool, producer_outcomes=[], reviewer_outcomes=[])
    github = FakeGitHubClient(fake_pool)

    with pytest.raises(ConflictError):
        _handoff_call(pool, adapter, github)

    assert _count(conn, _ITERATION_INSERT) == 0
    assert _count(conn, _TASK_GATE_WRITE) == 0
    assert _count(conn, _GATE_UPDATE) == 0
    assert _count(conn, _EVENT_INSERT) == 0
    assert len(adapter.realized_controls(_PRODUCER_EXTERNAL_ID)) == 0


def test_a_planning_task_never_supersedes_a_gate() -> None:
    """The handoff continues from ``waiting_for_owner`` only; a Task that is
    not waiting on its owner gate fails closed before any effect."""
    conn = ScriptedConnection()
    fake_pool = FakePool(conn)
    pool = _pool(conn)
    _script_handoff_entry(conn, task=_task_row(status="planning", gate_id=_GATE_1_ID))
    adapter = _prepared_adapter(fake_pool, producer_outcomes=[], reviewer_outcomes=[])
    github = FakeGitHubClient(fake_pool)

    with pytest.raises(ConflictError):
        _handoff_call(pool, adapter, github)

    assert _count(conn, _ITERATION_INSERT) == 0
    assert _count(conn, _TASK_GATE_WRITE) == 0
    assert _count(conn, _GATE_UPDATE) == 0
    assert _count(conn, _EVENT_INSERT) == 0
    assert len(adapter.realized_controls(_PRODUCER_EXTERNAL_ID)) == 0


def test_a_closed_planning_loop_refuses_the_handoff() -> None:
    """The handoff continues the same single OPEN planning loop; a Task with
    no OPEN planning loop is a stale change request (a closed loop never
    reopens and never gains a second loop)."""
    conn = ScriptedConnection()
    fake_pool = FakePool(conn)
    pool = _pool(conn)
    _script_handoff_entry(conn)
    conn.on_repeat(_REVISION_READ, _revision_row(revision_id=_REVISION_1_ID, number=1))
    conn.on(_LOOPS_LIST, [])
    adapter = _prepared_adapter(fake_pool, producer_outcomes=[], reviewer_outcomes=[])
    github = FakeGitHubClient(fake_pool)

    with pytest.raises(StaleOperationError):
        _handoff_call(pool, adapter, github)

    assert _count(conn, _ITERATION_INSERT) == 0
    assert _count(conn, _TASK_GATE_WRITE) == 0
    assert _count(conn, _GATE_UPDATE) == 0
    assert _count(conn, _EVENT_INSERT) == 0
    assert len(adapter.realized_controls(_PRODUCER_EXTERNAL_ID)) == 0


def test_a_non_ready_reviewer_binding_refuses_the_handoff() -> None:
    """The handoff reuses the exact admitted READY Reviewer session only; a
    CONNECTING binding is a conflict and never recreates or reinitializes
    one."""
    conn = ScriptedConnection()
    fake_pool = FakePool(conn)
    pool = _pool(conn)
    _script_handoff_entry(conn)
    conn.on_repeat(_REVISION_READ, _revision_row(revision_id=_REVISION_1_ID, number=1))
    conn.on_repeat(_LOOPS_LIST, [_loop_row()])
    conn.on_repeat(_LOOP_READ_LOCKED, _loop_row())
    conn.on(
        _role_is(_SESSION_READ, "reviewer"),
        _session_row("reviewer", session_id=_REVIEWER_SESSION_ID, external_id=None),
    )
    adapter = _prepared_adapter(fake_pool, producer_outcomes=[], reviewer_outcomes=[])
    github = FakeGitHubClient(fake_pool)

    with pytest.raises(ConflictError):
        _handoff_call(pool, adapter, github)

    assert _count(conn, _ITERATION_INSERT) == 0
    assert _count(conn, _TASK_GATE_WRITE) == 0
    assert _count(conn, _GATE_UPDATE) == 0
    assert _count(conn, _EVENT_INSERT) == 0
    assert len(adapter.realized_controls(_PRODUCER_EXTERNAL_ID)) == 0


def test_missing_e2_dependencies_are_rejected_before_any_commit() -> None:
    """At this gate the change request always routes to E2, so an unavailable
    Producer-side runtime adapter or GitHub client is an invalid command
    before the supersession transaction — an unavailable dependency can
    never strand an already-superseded gate."""
    conn = ScriptedConnection()
    fake_pool = FakePool(conn)
    pool = _pool(conn)
    adapter = _prepared_adapter(fake_pool, producer_outcomes=[], reviewer_outcomes=[])
    github = FakeGitHubClient(fake_pool)

    with pytest.raises(InvalidCommandError):
        _handoff_call(pool, None, github)  # type: ignore[arg-type]
    with pytest.raises(InvalidCommandError):
        _handoff_call(pool, adapter, None)  # type: ignore[arg-type]

    assert conn.executed == []
    assert len(adapter.realized_controls(_PRODUCER_EXTERNAL_ID)) == 0


def test_a_raced_gate_loss_supersedes_nothing_and_never_routes_e2() -> None:
    """The supersession primitive's conditional guards are the durable
    backstop: when the gate already lost currency between the guards and the
    primitive (a raced Owner approval), the handoff fails closed as stale
    and no E2 production ever follows."""
    conn = ScriptedConnection()
    fake_pool = FakePool(conn)
    pool = _pool(conn)
    _script_handoff(
        conn,
        supersession_task_row=_task_row(
            status="waiting_for_owner", state_token=_STATE_TOKEN, gate_id=None
        ),
    )
    adapter = _prepared_adapter(
        fake_pool,
        producer_outcomes=[plan_result_candidate(_REVISED_PLAN_TEXT)],
        reviewer_outcomes=[],
    )
    github = FakeGitHubClient(fake_pool)

    with pytest.raises(StaleOperationError):
        _handoff_call(pool, adapter, github)

    assert _count(conn, _REVISION_INSERT) == 0
    assert _count(conn, _POINTER_UPDATE) == 0
    assert len(adapter.realized_controls(_PRODUCER_EXTERNAL_ID)) == 0


def test_a_late_audit_failure_never_routes_e2() -> None:
    """A failed audit write inside the handoff transaction propagates after
    the composed transaction rolls back (durable atomicity is proven against
    a real Postgres); no E2 production ever follows a failed handoff."""
    conn = ScriptedConnection()
    fake_pool = FakePool(conn)
    pool = _pool(conn)
    _script_handoff(conn, fail_audit=True)
    adapter = _prepared_adapter(
        fake_pool,
        producer_outcomes=[plan_result_candidate(_REVISED_PLAN_TEXT)],
        reviewer_outcomes=[],
    )
    github = FakeGitHubClient(fake_pool)

    with pytest.raises(RuntimeError, match="audit"):
        _handoff_call(pool, adapter, github)

    # The supersession itself ran before the failing discussion-event write;
    # the composed transaction rolls the whole composition back (proven
    # against a real Postgres) and E2 never ran.
    assert _count(conn, _SUPERSESSION_WRITE) == 1
    assert _count(conn, _REVISION_INSERT) == 0
    assert len(adapter.realized_controls(_PRODUCER_EXTERNAL_ID)) == 0


def test_a_replayed_handoff_after_the_gate_moved_fails_closed() -> None:
    """A replayed change request after a successful handoff — the Task is
    back in planning and the gate is no longer current — applies nothing."""
    conn = ScriptedConnection()
    fake_pool = FakePool(conn)
    pool = _pool(conn)
    _script_handoff_entry(
        conn, task=_task_row(status="planning", state_token=_TOKEN_2, gate_id=None)
    )
    adapter = _prepared_adapter(fake_pool, producer_outcomes=[], reviewer_outcomes=[])
    github = FakeGitHubClient(fake_pool)

    with pytest.raises(ConflictError):
        _handoff_call(pool, adapter, github)

    assert _count(conn, _ITERATION_INSERT) == 0
    assert _count(conn, _TASK_GATE_WRITE) == 0
    assert _count(conn, _GATE_UPDATE) == 0
    assert _count(conn, _EVENT_INSERT) == 0
    assert len(adapter.realized_controls(_PRODUCER_EXTERNAL_ID)) == 0
