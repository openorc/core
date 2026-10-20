"""Deterministic tests for the E3c REVIEW_RESOLUTION settlement seam (issue #211).

A scripted SQL-connection seam plus the #69 fake Agent Runtime driving the
merged E2 revision production prove the narrow settlement seam on its
critical path:

- a typed formal ``CHANGES_REQUESTED`` on the exact current pending
  plan-subject REVIEW_RESOLUTION gate appends one NEW same-plan
  next-numbered iteration, records the REVIEWER ``PLAN_REVIEWED`` event,
  stamps the gate ``SETTLED`` with REVIEWER ``OWNER_GATE_RESOLVED``
  provenance (context carrying the settling ``review_iteration_id`` and
  ``review_outcome``), clears the current-gate pointer, rotates the Task
  token, moves the Task ``waiting_for_owner`` → ``planning``, and routes
  ONLY the newly finalized findings to real E2 — the historic exhausted
  findings are never rerouted;
- a typed formal ACCEPTED settles the gate, closes the loop, and yields
  the accepted evidence without creating IMPLEMENTATION_AUTHORIZATION,
  without an E2 route, and without an Owner APPROVED verdict — the Task
  stays ``waiting_for_owner`` for E5's flow;
- a settled gate keeps Owner mediation sticky through durable gate
  history: the next ordinary ``CHANGES_REQUESTED`` review immediately
  creates a NEW resolution gate without re-representing the spent budget;
- every stale, foreign, replayed, superseded, non-READY, non-formal, or
  out-of-scope input fails closed and applies nothing.

The settlement send surface is deliberately empty (E4 owns recognition
and transport); the REVIEW send, the GitHub observation, and the REVISE
send all run with no database transaction open (the fake pool tracks
checked-out connections). Durable atomicity is proven by the
integration-marked suite against a real Postgres. No live GitHub,
runtime, or Postgres access.
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
    GitHubAppClient,
    GitHubBranchObservation,
    GitHubRepositoryObservation,
)
from openorc.domain.connections import WorkflowRole
from openorc.domain.gates import OwnerGateStatus
from openorc.domain.planning import PlanRevision
from openorc.domain.reviews import ReviewLoopStatus, ReviewOutcome
from openorc.domain.tasks import Task, TaskStatus
from openorc.persistence.pool import DatabasePool
from openorc.protocol.models import ReviewFinding, ReviewResult
from openorc.services.errors import (
    ConflictError,
    InvalidCommandError,
    NotFoundError,
    StaleOperationError,
)
from openorc.services.task_plan_production import TaskPlanProductionOutcome
from openorc.services.task_planning_review_decision import (
    PlanningReviewDecision,
    finalize_planning_review_decision,
)
from openorc.services.task_planning_review_dispatch import dispatch_planning_review
from openorc.services.task_planning_review_resolution import (
    PlanningReviewResolutionDecision,
    settle_planning_review_resolution,
)
from tests.fakes.agent_runtime import (
    FakeAgentRuntimeAdapter,
    changes_requested_review_result_candidate,
    plan_result_candidate,
    session_ready_candidate,
)

_OBSERVED = datetime(2026, 10, 19, 12, 0, 0, tzinfo=UTC)
_WORKSPACE_ID = uuid.uuid4()
_OWNER_PROFILE_ID = uuid.uuid4()
_OTHER_PROFILE_ID = uuid.uuid4()
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
_TOKEN_4 = uuid.uuid4()
_TOKEN_5 = uuid.uuid4()
_REVISION_1_ID = uuid.uuid4()
_REVISION_2_ID = uuid.uuid4()
_REVISION_3_ID = uuid.uuid4()
_LOOP_ID = uuid.uuid4()
_ITERATION_1_ID = uuid.uuid4()
_ITERATION_2_ID = uuid.uuid4()
_ITERATION_3_ID = uuid.uuid4()
_GATE_1_ID = uuid.uuid4()
_GATE_2_ID = uuid.uuid4()
_BASE_SHA = "c" * 40
_PLAN_TEXT = "# Plan\n\nStep one.\n"
_REVISED_PLAN_TEXT = "# Plan (revised)\n\nStep one, clarified.\n"
_FULL_NAME = "octocat/hello-world"
_EXHAUSTED_FINDINGS = [
    {"summary": "Clarify scope", "details": "Say which module changes."},
    {"summary": "Add tests", "details": "Cover the dispatch edge."},
]
_NEW_FINDINGS = [{"summary": "Clarify the retry loop", "details": "Explain the backoff path."}]
_REPOSITORY_BASE = "/srv/repositories"

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
        raise AssertionError("planning review resolution tests never close pools")


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
_LOOP_CLOSE = _sql_has("update openorc.review_loops")
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
    gate_type: str = "review_resolution",
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
    """Same-shaped reads whose result never changes across the whole flow.

    The Workspace, Task-row, gate, revision, and iteration-history reads
    are deliberately absent: they vary per phase and are scripted in call
    order by the scenario builders.
    """
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


def _exhausted_iteration_row() -> tuple[Any, ...]:
    """The immutable historic exhausted iteration the settlement must never reroute."""
    return _iteration_row(
        iteration_id=_ITERATION_1_ID,
        number=1,
        plan_revision_id=_REVISION_1_ID,
        outcome="changes_requested",
        summary="Needs work",
        findings=_EXHAUSTED_FINDINGS,
        decided_at=_OBSERVED,
    )


def _settlement_iteration_row(*, accepted: bool) -> tuple[Any, ...]:
    if accepted:
        return _iteration_row(
            iteration_id=_ITERATION_2_ID,
            number=2,
            plan_revision_id=_REVISION_1_ID,
            outcome="accepted",
            summary="Everything checks out.",
            findings=[],
            decided_at=_OBSERVED,
        )
    return _iteration_row(
        iteration_id=_ITERATION_2_ID,
        number=2,
        plan_revision_id=_REVISION_1_ID,
        outcome="changes_requested",
        summary="Needs another pass",
        findings=_NEW_FINDINGS,
        decided_at=_OBSERVED,
    )


def _script_settlement(conn: ScriptedConnection, *, accepted: bool) -> None:
    """The settlement transaction in call order, plus E2's production when
    the formal result is a change request."""
    _script_repeating_reads(conn)
    t_waiting = _task_row(status="waiting_for_owner", state_token=_STATE_TOKEN, gate_id=_GATE_1_ID)
    t_cleared = _task_row(status="waiting_for_owner", state_token=_TOKEN_2, gate_id=None)
    t_planning = _task_row(status="planning", state_token=_TOKEN_3)
    rev1 = _revision_row(revision_id=_REVISION_1_ID, number=1)
    it1 = _exhausted_iteration_row()
    it2 = _settlement_iteration_row(accepted=accepted)
    it2_unfin = _iteration_row(
        iteration_id=_ITERATION_2_ID, number=2, plan_revision_id=_REVISION_1_ID
    )
    settled_gate = _gate_row(status="settled")

    conn.on_repeat(_sql_has("from openorc.workspaces where id"), _ws_row())
    conn.on(_TASK_READ, t_waiting)  # settlement workspace guard
    conn.on(_TASK_READ_LOCKED, t_waiting)  # settlement Task lock
    conn.on(_TASK_READ, t_waiting)  # settlement currentness
    conn.on_repeat(_GATE_READ, _gate_row())
    conn.on_repeat(_REVISION_READ, rev1)
    conn.on_repeat(_LOOPS_LIST, [_loop_row()])
    conn.on(_ITERATIONS_LIST, [it1])  # the allocation's numbering history
    conn.on(_ITERATION_INSERT, it2_unfin)
    conn.on(_RESULT_UPDATE, it2)
    conn.on_repeat(_ITERATIONS_LIST, [it1, it2])
    conn.on(_TASK_READ_LOCKED, t_waiting)  # the gate settlement's Task lock
    conn.on(_GATE_UPDATE, settled_gate)
    conn.on(_TASK_GATE_WRITE, t_cleared)
    if accepted:
        conn.on(_LOOP_CLOSE, _loop_row(status="closed", closed_at=_OBSERVED))
    else:
        conn.on(_STATUS_UPDATE, t_planning)
        # --- E2 revision production (entry + finalize), in call order. ---
        rev2 = _revision_row(revision_id=_REVISION_2_ID, number=2, content=_REVISED_PLAN_TEXT)
        t_pointer = _task_row(
            status="planning", state_token=_TOKEN_4, current_plan_revision_id=_REVISION_2_ID
        )
        conn.on(_TASK_READ, t_planning)  # E2 workspace guard
        conn.on(_TASK_READ_LOCKED, t_planning)  # E2 Task lock
        conn.on(_TASK_READ, t_planning)  # E2 entry currentness
        conn.on(_ITERATION_READ, it2)
        conn.on(_REVISIONS_HISTORY, [rev1])
        conn.on(_TASK_READ, t_planning)  # E2 finalize currentness
        conn.on(_REVISION_INSERT, rev2)
        conn.on(_POINTER_UPDATE, t_pointer)


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


def _settlement_call(
    pool: DatabasePool,
    adapter: _ProbedAdapter,
    github: Any,
    *,
    expected_state_token: Any = _STATE_TOKEN,
    review_result: Any = None,
) -> Any:
    return settle_planning_review_resolution(
        pool,
        adapter,
        cast(GitHubAppClient, github),
        profile_id=_OWNER_PROFILE_ID,
        workspace_id=_WORKSPACE_ID,
        task_id=_TASK_ID,
        expected_state_token=expected_state_token,
        review_result=_changes_requested_result() if review_result is None else review_result,
    )


def test_a_formal_changes_requested_settles_the_gate_and_routes_new_findings_to_e2() -> None:
    """The typed formal change request settles the exact current gate with
    REVIEWER provenance, appends one new same-plan iteration, and routes
    only the newly finalized findings to real E2."""
    conn = ScriptedConnection()
    fake_pool = FakePool(conn)
    pool = _pool(conn)
    _script_settlement(conn, accepted=False)
    adapter = _prepared_adapter(
        fake_pool,
        producer_outcomes=[plan_result_candidate(_REVISED_PLAN_TEXT)],
        reviewer_outcomes=[],
    )
    github = FakeGitHubClient(fake_pool)

    outcome = _settlement_call(pool, adapter, github)

    assert outcome.decision is PlanningReviewResolutionDecision.CONTINUATION_ROUTED
    assert outcome.review_iteration.id == _ITERATION_2_ID
    assert outcome.review_iteration.iteration_number == 2
    assert outcome.review_iteration.outcome is ReviewOutcome.CHANGES_REQUESTED
    assert outcome.review_iteration.plan_revision_id == _REVISION_1_ID
    assert outcome.plan_revision.id == _REVISION_1_ID
    assert outcome.resolution_gate.status is OwnerGateStatus.SETTLED
    assert outcome.task.status is TaskStatus.PLANNING
    assert outcome.task.state_token == _TOKEN_4
    assert outcome.task.current_plan_revision_id == _REVISION_2_ID
    assert outcome.task.current_owner_gate_id is None
    assert outcome.revision_production is not None
    assert outcome.revision_production.plan_revision.id == _REVISION_2_ID
    assert outcome.revision_production.task.state_token == _TOKEN_4
    # The REVIEWER plan review, the REVIEWER-provenance settled gate
    # resolution identifying the settling result, and E2's revision event.
    assert _event_types(conn) == [
        "plan_reviewed",
        "owner_gate_resolved",
        "plan_revision_revised",
    ]
    (reviewed,) = _event_params_of_type(conn, "plan_reviewed")
    assert reviewed[3] == "reviewer"
    assert reviewed[6] == _ITERATION_2_ID
    (resolved,) = _event_params_of_type(conn, "owner_gate_resolved")
    assert resolved[3] == "reviewer"
    assert resolved[4] == str(_REVIEWER_SESSION_ID)
    assert resolved[5] == "owner_gate"
    assert resolved[6] == _GATE_1_ID
    assert resolved[7].obj == {
        "outcome": "settled",
        "review_iteration_id": str(_ITERATION_2_ID),
        "review_outcome": "changes_requested",
    }
    # Exactly the NEW findings routed to E2; the historic exhausted
    # findings are never rerouted.
    (revise_send,) = adapter.realized_controls(_PRODUCER_EXTERNAL_ID)
    assert revise_send.message is not None
    assert "Clarify the retry loop" in revise_send.message
    assert "Say which module changes." not in revise_send.message
    # The settlement itself never sends REVIEW; E2 produced exactly once.
    assert len(adapter.realized_controls(_REVIEWER_EXTERNAL_ID)) == 0
    assert _count(conn, _RESULT_UPDATE) == 1
    assert _count(conn, _GATE_UPDATE) == 1
    assert _count(conn, _STATUS_UPDATE) == 1
    assert _count(conn, _REVISION_INSERT) == 1
    assert len(github.repository_calls) == 1
    assert len(github.branch_calls) == 1


def test_a_formal_accepted_settles_the_gate_closes_the_loop_and_never_touches_e2() -> None:
    """The typed formal acceptance settles the gate, closes the loop, and
    yields the accepted evidence with no Owner APPROVED verdict, no
    IMPLEMENTATION_AUTHORIZATION, no E2 route, and no status move."""
    conn = ScriptedConnection()
    fake_pool = FakePool(conn)
    pool = _pool(conn)
    _script_settlement(conn, accepted=True)
    adapter = _prepared_adapter(fake_pool, producer_outcomes=[], reviewer_outcomes=[])
    github = FakeGitHubClient(fake_pool)

    outcome = _settlement_call(pool, adapter, github, review_result=_accepted_result())

    assert outcome.decision is PlanningReviewResolutionDecision.SETTLED_ACCEPTED
    assert outcome.review_iteration.id == _ITERATION_2_ID
    assert outcome.review_iteration.outcome is ReviewOutcome.ACCEPTED
    assert outcome.plan_revision.id == _REVISION_1_ID
    assert outcome.review_loop.status is ReviewLoopStatus.CLOSED
    assert outcome.task.status is TaskStatus.WAITING_FOR_OWNER
    assert outcome.task.state_token == _TOKEN_2
    assert outcome.task.current_owner_gate_id is None
    assert outcome.resolution_gate.status is OwnerGateStatus.SETTLED
    assert outcome.revision_production is None
    assert _event_types(conn) == ["plan_reviewed", "owner_gate_resolved"]
    (resolved,) = _event_params_of_type(conn, "owner_gate_resolved")
    assert resolved[3] == "reviewer"
    assert resolved[7].obj == {
        "outcome": "settled",
        "review_iteration_id": str(_ITERATION_2_ID),
        "review_outcome": "accepted",
    }
    assert _count(conn, _STATUS_UPDATE) == 0
    assert _count(conn, _REVISION_INSERT) == 0
    assert _count(conn, _GATE_INSERT) == 0
    assert len(adapter.realized_controls(_PRODUCER_EXTERNAL_ID)) == 0
    assert len(adapter.realized_controls(_REVIEWER_EXTERNAL_ID)) == 0
    assert github.repository_calls == []
    assert github.branch_calls == []


def _script_sticky_followup(conn: ScriptedConnection) -> None:
    """The post-settlement state: the revised plan is current, the settled
    gate is durable history, and one more ordinary review round follows."""
    t_current = _task_row(
        status="planning", state_token=_TOKEN_4, current_plan_revision_id=_REVISION_2_ID
    )
    rev2 = _revision_row(revision_id=_REVISION_2_ID, number=2, content=_REVISED_PLAN_TEXT)
    it1 = _exhausted_iteration_row()
    it2 = _settlement_iteration_row(accepted=False)
    settled_gate = _gate_row(status="settled")
    it3_unfin = _iteration_row(
        iteration_id=_ITERATION_3_ID, number=3, plan_revision_id=_REVISION_2_ID
    )
    it3 = _iteration_row(
        iteration_id=_ITERATION_3_ID,
        number=3,
        plan_revision_id=_REVISION_2_ID,
        outcome="changes_requested",
        summary="Still needs work",
        findings=_NEW_FINDINGS,
        decided_at=_OBSERVED,
    )
    conn.on_repeat(_sql_has("from openorc.workspaces where id"), _ws_row())
    conn.on_repeat(_TASK_READ, t_current)
    conn.on_repeat(_TASK_READ_LOCKED, t_current)
    conn.on_repeat(_REVISION_READ, rev2)
    conn.on_repeat(_LOOPS_LIST, [_loop_row()])
    conn.on(_ITERATIONS_LIST, [it1, it2])  # dispatch's admission guards
    conn.on(_ITERATION_INSERT, it3_unfin)
    conn.on(_ITERATIONS_LIST, [it1, it2, it3_unfin])  # the reserved attempt
    conn.on(_RESULT_UPDATE, it3)
    conn.on(_ITERATIONS_LIST, [it1, it2, it3])  # the budget facts
    conn.on(_GATES_LIST, [settled_gate])  # the settled gate keeps mediation sticky
    conn.on(
        _GATE_INSERT,
        _gate_row(gate_id=_GATE_2_ID, plan_revision_id=_REVISION_2_ID),
    )
    t_waiting = _task_row(
        status="waiting_for_owner",
        state_token=_TOKEN_5,
        current_plan_revision_id=_REVISION_2_ID,
        gate_id=_GATE_2_ID,
    )
    conn.on(_TASK_GATE_WRITE, t_waiting)
    conn.on(_STATUS_UPDATE, t_waiting)


def test_a_settled_gate_keeps_owner_mediation_sticky() -> None:
    """The settled REVIEW_RESOLUTION gate stays in durable gate history: the
    next ordinary ``CHANGES_REQUESTED`` review immediately creates a NEW
    resolution gate without refreshing the spent autonomous budget and
    without a second ``REVIEW_LIMIT_REACHED``."""
    conn = ScriptedConnection()
    fake_pool = FakePool(conn)
    pool = _pool(conn)
    _script_repeating_reads(conn)
    _script_sticky_followup(conn)
    adapter = _prepared_adapter(
        fake_pool,
        producer_outcomes=[],
        reviewer_outcomes=[
            changes_requested_review_result_candidate(
                "Still needs work",
                [("Clarify the retry loop", "Explain the backoff path.")],
            )
        ],
    )
    github = FakeGitHubClient(fake_pool)

    dispatch = dispatch_planning_review(
        pool,
        adapter,
        profile_id=_OWNER_PROFILE_ID,
        workspace_id=_WORKSPACE_ID,
        task_id=_TASK_ID,
        plan_production=_production_outcome(
            revision_id=_REVISION_2_ID,
            number=2,
            content=_REVISED_PLAN_TEXT,
            state_token=_TOKEN_4,
            current_plan_revision_id=_REVISION_2_ID,
        ),
    )
    decision = finalize_planning_review_decision(
        pool,
        adapter,
        cast(GitHubAppClient, github),
        profile_id=_OWNER_PROFILE_ID,
        workspace_id=_WORKSPACE_ID,
        task_id=_TASK_ID,
        expected_state_token=_TOKEN_4,
        dispatch=dispatch,
    )

    assert decision.decision is PlanningReviewDecision.EXHAUSTED
    assert decision.resolution_gate is not None
    assert decision.resolution_gate.id == _GATE_2_ID
    assert decision.resolution_gate.plan_revision_id == _REVISION_2_ID
    assert decision.task.status is TaskStatus.WAITING_FOR_OWNER
    assert decision.task.current_owner_gate_id == _GATE_2_ID
    assert decision.task.state_token == _TOKEN_5
    assert _event_types(conn) == ["plan_reviewed", "owner_gate_created"]
    assert _event_params_of_type(conn, "review_limit_reached") == []
    assert len(adapter.realized_controls(_PRODUCER_EXTERNAL_ID)) == 0


def _script_settlement_entry(conn: ScriptedConnection, *, gate: tuple[Any, ...]) -> None:
    """The entry reads up to (and including) the gate read, for fail-closed cases."""
    conn.on_repeat(_sql_has("from openorc.profiles", "for key share"), (None, None, None))
    conn.on_repeat(_sql_has("from openorc.workspaces where id"), _ws_row())
    t_waiting = _task_row(status="waiting_for_owner", state_token=_STATE_TOKEN, gate_id=_GATE_1_ID)
    conn.on(_TASK_READ, t_waiting)  # workspace guard
    conn.on(_TASK_READ_LOCKED, t_waiting)  # settlement lock
    conn.on(_TASK_READ, t_waiting)  # currentness
    conn.on(_GATE_READ, gate)


def test_a_stale_token_settles_nothing() -> None:
    conn = ScriptedConnection()
    fake_pool = FakePool(conn)
    pool = _pool(conn)
    _script_settlement_entry(conn, gate=_gate_row())
    adapter = _prepared_adapter(fake_pool, producer_outcomes=[], reviewer_outcomes=[])
    github = FakeGitHubClient(fake_pool)

    with pytest.raises(StaleOperationError):
        _settlement_call(pool, adapter, github, expected_state_token=_TOKEN_5)

    assert _count(conn, _RESULT_UPDATE) == 0
    assert _count(conn, _GATE_UPDATE) == 0
    assert _count(conn, _STATUS_UPDATE) == 0
    assert _count(conn, _EVENT_INSERT) == 0
    assert len(adapter.realized_controls(_PRODUCER_EXTERNAL_ID)) == 0


def test_a_replayed_settlement_never_rewrites_the_settled_gate() -> None:
    conn = ScriptedConnection()
    fake_pool = FakePool(conn)
    pool = _pool(conn)
    _script_settlement_entry(conn, gate=_gate_row(status="settled"))
    adapter = _prepared_adapter(fake_pool, producer_outcomes=[], reviewer_outcomes=[])
    github = FakeGitHubClient(fake_pool)

    with pytest.raises(StaleOperationError):
        _settlement_call(pool, adapter, github)

    assert _count(conn, _RESULT_UPDATE) == 0
    assert _count(conn, _GATE_UPDATE) == 0
    assert _count(conn, _STATUS_UPDATE) == 0
    assert _count(conn, _EVENT_INSERT) == 0


def test_a_non_resolution_gate_never_settles_through_this_seam() -> None:
    conn = ScriptedConnection()
    fake_pool = FakePool(conn)
    pool = _pool(conn)
    _script_settlement_entry(conn, gate=_gate_row(gate_type="implementation_authorization"))
    adapter = _prepared_adapter(fake_pool, producer_outcomes=[], reviewer_outcomes=[])
    github = FakeGitHubClient(fake_pool)

    with pytest.raises(ConflictError):
        _settlement_call(pool, adapter, github)

    assert _count(conn, _GATE_UPDATE) == 0
    assert _count(conn, _EVENT_INSERT) == 0


def test_a_pr_subject_resolution_gate_belongs_to_later_pr_review_work() -> None:
    conn = ScriptedConnection()
    fake_pool = FakePool(conn)
    pool = _pool(conn)
    _script_settlement_entry(
        conn,
        gate=_gate_row(plan_revision_id=None, head_sha="e" * 40, pr_id=uuid.uuid4()),
    )
    adapter = _prepared_adapter(fake_pool, producer_outcomes=[], reviewer_outcomes=[])
    github = FakeGitHubClient(fake_pool)

    with pytest.raises(ConflictError):
        _settlement_call(pool, adapter, github)

    assert _count(conn, _GATE_UPDATE) == 0
    assert _count(conn, _EVENT_INSERT) == 0


def test_a_changed_current_plan_revision_is_a_stale_settlement() -> None:
    conn = ScriptedConnection()
    fake_pool = FakePool(conn)
    pool = _pool(conn)
    _script_settlement_entry(conn, gate=_gate_row(plan_revision_id=_REVISION_3_ID))
    adapter = _prepared_adapter(fake_pool, producer_outcomes=[], reviewer_outcomes=[])
    github = FakeGitHubClient(fake_pool)

    with pytest.raises(StaleOperationError):
        _settlement_call(pool, adapter, github)

    assert _count(conn, _RESULT_UPDATE) == 0
    assert _count(conn, _GATE_UPDATE) == 0
    assert _count(conn, _EVENT_INSERT) == 0


def test_a_missing_reviewer_binding_fails_closed() -> None:
    conn = ScriptedConnection()
    fake_pool = FakePool(conn)
    pool = _pool(conn)
    _script_settlement_entry(conn, gate=_gate_row())
    conn.on(_REVISION_READ, _revision_row(revision_id=_REVISION_1_ID, number=1))
    conn.on(_LOOPS_LIST, [_loop_row()])
    conn.on(_LOOP_READ_LOCKED, _loop_row())
    conn.on(_role_is(_SESSION_READ, "reviewer"), None)
    adapter = _prepared_adapter(fake_pool, producer_outcomes=[], reviewer_outcomes=[])
    github = FakeGitHubClient(fake_pool)

    with pytest.raises(NotFoundError):
        _settlement_call(pool, adapter, github)

    assert _count(conn, _RESULT_UPDATE) == 0
    assert _count(conn, _GATE_UPDATE) == 0
    assert _count(conn, _EVENT_INSERT) == 0


def test_a_non_ready_reviewer_binding_fails_closed() -> None:
    conn = ScriptedConnection()
    fake_pool = FakePool(conn)
    pool = _pool(conn)
    _script_settlement_entry(conn, gate=_gate_row())
    conn.on(_REVISION_READ, _revision_row(revision_id=_REVISION_1_ID, number=1))
    conn.on(_LOOPS_LIST, [_loop_row()])
    conn.on(_LOOP_READ_LOCKED, _loop_row())
    conn.on(
        _role_is(_SESSION_READ, "reviewer"),
        _session_row("reviewer", session_id=_REVIEWER_SESSION_ID, external_id=None),
    )
    adapter = _prepared_adapter(fake_pool, producer_outcomes=[], reviewer_outcomes=[])
    github = FakeGitHubClient(fake_pool)

    with pytest.raises(ConflictError):
        _settlement_call(pool, adapter, github)

    assert _count(conn, _RESULT_UPDATE) == 0
    assert _count(conn, _GATE_UPDATE) == 0
    assert _count(conn, _EVENT_INSERT) == 0


def test_a_closed_planning_loop_is_a_stale_settlement() -> None:
    conn = ScriptedConnection()
    fake_pool = FakePool(conn)
    pool = _pool(conn)
    _script_settlement_entry(conn, gate=_gate_row())
    conn.on(_REVISION_READ, _revision_row(revision_id=_REVISION_1_ID, number=1))
    conn.on(_LOOPS_LIST, [])
    adapter = _prepared_adapter(fake_pool, producer_outcomes=[], reviewer_outcomes=[])
    github = FakeGitHubClient(fake_pool)

    with pytest.raises(StaleOperationError):
        _settlement_call(pool, adapter, github)

    assert _count(conn, _RESULT_UPDATE) == 0
    assert _count(conn, _GATE_UPDATE) == 0
    assert _count(conn, _EVENT_INSERT) == 0


def test_a_non_formal_input_never_settles() -> None:
    """E4's formal-result recognition owns recognition: a bare dict or
    string is an invalid command before any read or effect."""
    conn = ScriptedConnection()
    fake_pool = FakePool(conn)
    pool = _pool(conn)
    adapter = _prepared_adapter(fake_pool, producer_outcomes=[], reviewer_outcomes=[])
    github = FakeGitHubClient(fake_pool)

    with pytest.raises(InvalidCommandError):
        _settlement_call(pool, adapter, github, review_result={"outcome": "ACCEPTED"})
    with pytest.raises(InvalidCommandError):
        _settlement_call(pool, adapter, github, review_result="ACCEPTED")

    assert conn.executed == []
    assert len(adapter.realized_controls(_PRODUCER_EXTERNAL_ID)) == 0
    assert len(adapter.realized_controls(_REVIEWER_EXTERNAL_ID)) == 0


def test_malformed_command_identifiers_are_invalid_commands() -> None:
    conn = ScriptedConnection()
    fake_pool = FakePool(conn)
    pool = _pool(conn)
    adapter = _prepared_adapter(fake_pool, producer_outcomes=[], reviewer_outcomes=[])
    github = FakeGitHubClient(fake_pool)

    with pytest.raises(InvalidCommandError):
        _settlement_call(pool, adapter, github, expected_state_token="not-a-uuid")
    with pytest.raises(InvalidCommandError):
        settle_planning_review_resolution(
            pool,
            adapter,
            cast(GitHubAppClient, github),
            profile_id="not-a-uuid",  # type: ignore[arg-type]
            workspace_id=_WORKSPACE_ID,
            task_id=_TASK_ID,
            expected_state_token=_STATE_TOKEN,
            review_result=_changes_requested_result(),
        )

    assert conn.executed == []


def test_a_foreign_workspace_is_not_found() -> None:
    conn = ScriptedConnection()
    fake_pool = FakePool(conn)
    pool = _pool(conn)
    conn.on_repeat(_sql_has("from openorc.profiles", "for key share"), (None, None, None))
    conn.on_repeat(_sql_has("from openorc.workspaces where id"), _ws_row(owner=_OTHER_PROFILE_ID))
    adapter = _prepared_adapter(fake_pool, producer_outcomes=[], reviewer_outcomes=[])
    github = FakeGitHubClient(fake_pool)

    with pytest.raises(NotFoundError):
        _settlement_call(pool, adapter, github)

    # Only the account barrier and the Workspace read ran: the probing
    # oracle stays closed.
    assert len(conn.executed) == 2
    assert _count(conn, _GATE_UPDATE) == 0
    assert _count(conn, _EVENT_INSERT) == 0


class _PartialGitHubClient:
    """An object carrying only one of the two consumed adapter capabilities."""

    def get_installation_repository(
        self, *, github_installation_id: int, github_repository_id: int
    ) -> None:
        return None


def test_an_invalid_dependency_refuses_before_any_effect() -> None:
    """A non-adapter runtime or GitHub dependency is an invalid command
    before any transaction or durable effect — it can never strand an
    already-settled gate by failing inside E2's revision production."""
    conn = ScriptedConnection()
    fake_pool = FakePool(conn)
    pool = _pool(conn)
    adapter = _prepared_adapter(fake_pool, producer_outcomes=[], reviewer_outcomes=[])

    with pytest.raises(InvalidCommandError):
        settle_planning_review_resolution(
            pool,
            adapter,
            cast(GitHubAppClient, object()),  # type: ignore[arg-type]
            profile_id=_OWNER_PROFILE_ID,
            workspace_id=_WORKSPACE_ID,
            task_id=_TASK_ID,
            expected_state_token=_STATE_TOKEN,
            review_result=_changes_requested_result(),
        )
    with pytest.raises(InvalidCommandError):
        settle_planning_review_resolution(
            pool,
            adapter,
            cast(GitHubAppClient, _PartialGitHubClient()),  # type: ignore[arg-type]
            profile_id=_OWNER_PROFILE_ID,
            workspace_id=_WORKSPACE_ID,
            task_id=_TASK_ID,
            expected_state_token=_STATE_TOKEN,
            review_result=_changes_requested_result(),
        )
    with pytest.raises(InvalidCommandError):
        settle_planning_review_resolution(
            pool,
            cast(Any, object()),
            cast(GitHubAppClient, FakeGitHubClient(fake_pool)),
            profile_id=_OWNER_PROFILE_ID,
            workspace_id=_WORKSPACE_ID,
            task_id=_TASK_ID,
            expected_state_token=_STATE_TOKEN,
            review_result=_changes_requested_result(),
        )

    assert conn.executed == []
    assert _count(conn, _RESULT_UPDATE) == 0
    assert _count(conn, _EVENT_INSERT) == 0
    assert len(adapter.realized_controls(_PRODUCER_EXTERNAL_ID)) == 0
    assert len(adapter.realized_controls(_REVIEWER_EXTERNAL_ID)) == 0


def test_the_discussion_audit_flag_records_both_message_facts_in_the_settlement() -> None:
    """With E4's coordination flag, the settlement transaction also commits the
    two direction-only Owner ↔ Reviewer discussion message events bound to the
    settled gate, after the settled-gate resolution event."""
    conn = ScriptedConnection()
    fake_pool = FakePool(conn)
    pool = _pool(conn)
    _script_settlement(conn, accepted=False)
    adapter = _prepared_adapter(
        fake_pool,
        producer_outcomes=[plan_result_candidate(_REVISED_PLAN_TEXT)],
        reviewer_outcomes=[],
    )
    github = FakeGitHubClient(fake_pool)

    outcome = settle_planning_review_resolution(
        pool,
        adapter,
        cast(GitHubAppClient, github),
        profile_id=_OWNER_PROFILE_ID,
        workspace_id=_WORKSPACE_ID,
        task_id=_TASK_ID,
        expected_state_token=_STATE_TOKEN,
        review_result=_changes_requested_result(),
        record_discussion_message_events=True,
    )

    assert outcome.decision is PlanningReviewResolutionDecision.CONTINUATION_ROUTED
    assert _event_types(conn) == [
        "plan_reviewed",
        "owner_gate_resolved",
        "owner_reviewer_discussion_message",
        "owner_reviewer_discussion_message",
        "plan_revision_revised",
    ]
    (owner_event, reviewer_event) = _event_params_of_type(conn, "owner_reviewer_discussion_message")
    assert owner_event[3] == "owner"
    assert owner_event[4] == str(_OWNER_PROFILE_ID)
    assert owner_event[5] == "owner_gate"
    assert owner_event[6] == _GATE_1_ID
    assert owner_event[7].obj == {"direction": "owner_to_reviewer"}
    assert reviewer_event[3] == "reviewer"
    assert reviewer_event[4] == str(_REVIEWER_SESSION_ID)
    assert reviewer_event[5] == "owner_gate"
    assert reviewer_event[6] == _GATE_1_ID
    assert reviewer_event[7].obj == {"direction": "reviewer_to_owner"}
    # The default (flag absent) sequence is unchanged for every other caller.
    plain_conn = ScriptedConnection()
    plain_pool = _pool(plain_conn)
    _script_settlement(plain_conn, accepted=False)
    plain_adapter = _prepared_adapter(
        FakePool(plain_conn),
        producer_outcomes=[plan_result_candidate(_REVISED_PLAN_TEXT)],
        reviewer_outcomes=[],
    )
    settle_planning_review_resolution(
        plain_pool,
        plain_adapter,
        cast(GitHubAppClient, FakeGitHubClient(FakePool(plain_conn))),
        profile_id=_OWNER_PROFILE_ID,
        workspace_id=_WORKSPACE_ID,
        task_id=_TASK_ID,
        expected_state_token=_STATE_TOKEN,
        review_result=_changes_requested_result(),
    )
    assert _event_types(plain_conn) == [
        "plan_reviewed",
        "owner_gate_resolved",
        "plan_revision_revised",
    ]


def test_an_accepted_settlement_needs_no_e2_dependencies() -> None:
    """A formal ACCEPTED never routes to E2: the settlement succeeds without a
    runtime adapter or GitHub client, and E2 is never touched."""
    conn = ScriptedConnection()
    fake_pool = FakePool(conn)
    pool = _pool(conn)
    _script_settlement(conn, accepted=True)
    adapter = _prepared_adapter(fake_pool, producer_outcomes=[], reviewer_outcomes=[])

    outcome = settle_planning_review_resolution(
        pool,
        None,
        None,
        profile_id=_OWNER_PROFILE_ID,
        workspace_id=_WORKSPACE_ID,
        task_id=_TASK_ID,
        expected_state_token=_STATE_TOKEN,
        review_result=_accepted_result(),
    )

    assert outcome.decision is PlanningReviewResolutionDecision.SETTLED_ACCEPTED
    assert outcome.revision_production is None
    assert _count(conn, _REVISION_INSERT) == 0
    assert _count(conn, _STATUS_UPDATE) == 0
    assert len(adapter.realized_controls(_PRODUCER_EXTERNAL_ID)) == 0


def test_a_changes_requested_settlement_refuses_missing_e2_dependencies() -> None:
    """A formal CHANGES_REQUESTED proves its E2 dependencies BEFORE the
    settlement transaction: a missing runtime adapter or GitHub client is an
    invalid command that applies nothing (the no-stranding guarantee)."""
    conn = ScriptedConnection()
    pool = _pool(conn)
    _script_settlement(conn, accepted=False)

    with pytest.raises(InvalidCommandError):
        settle_planning_review_resolution(
            pool,
            None,
            None,
            profile_id=_OWNER_PROFILE_ID,
            workspace_id=_WORKSPACE_ID,
            task_id=_TASK_ID,
            expected_state_token=_STATE_TOKEN,
            review_result=_changes_requested_result(),
        )

    assert conn.executed == []
    assert _count(conn, _RESULT_UPDATE) == 0
    assert _count(conn, _EVENT_INSERT) == 0
