"""Deterministic tests for the E3b planning review decision loop (issue #210).

A scripted SQL-connection seam plus the #69 fake Agent Runtime driving the
real E3a dispatch, E3b decision, and merged E2 revision production prove
the bounded planning loop on its critical path:

- budget 2 (snapshotted on the loop at creation): two ``CHANGES_REQUESTED``
  reviews each finalize their iteration once, record one REVIEWER
  ``PLAN_REVIEWED`` event, and route the exact finalized findings to real
  E2 — two distinct persisted revisions with rotated tokens; the third
  change request creates exactly one exact-plan pending
  REVIEW_RESOLUTION gate, installs it current, moves the Task to
  ``waiting_for_owner`` with a rotated token, keeps the loop OPEN, and
  never routes E2. ``REVIEW_LIMIT_REACHED`` is recorded exactly once (the
  first exhaustion), and no findings are forwarded into gates or events;
- the loop's stored budget governs the decision even when every later
  Workspace read carries a different setting value (the snapshot is
  authoritative);
- one representative ACCEPTED clearance closes the loop, yields the exact
  accepted PlanRevision/iteration evidence, and never touches E2, gates,
  or the Task;
- duplicate and stale finalizations apply nothing: no second result,
  event, gate, or send; a continuation without the exact E3a dispatch
  outcome can never record a verdict; an unresolved REVIEW attempt is
  never treated as ``CHANGES_REQUESTED``;
- durable OwnerGate history makes Owner mediation sticky: with a prior
  REVIEW_RESOLUTION gate recorded, the very next change request
  immediately creates a NEW gate without a second ``REVIEW_LIMIT_REACHED``
  event and without refreshing the autonomous allowance;
- the REVIEW send, the GitHub observation, and the REVISE send all run
  with no database transaction open (the fake pool tracks checked-out
  connections), and E2's uncertain failures propagate typed and are never
  resent here.

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

from openorc.adapters.agent_runtime.errors import AgentRuntimeDeliveryUncertainError
from openorc.adapters.agent_runtime.results import AgentSessionCreationRequest
from openorc.adapters.github import (
    GitHubAppClient,
    GitHubBranchObservation,
    GitHubRepositoryObservation,
)
from openorc.domain.connections import WorkflowRole
from openorc.domain.planning import PlanRevision
from openorc.domain.reviews import ReviewLoopStatus
from openorc.domain.sessions import TaskAgentSession, TaskSessionLifecycleStatus
from openorc.domain.tasks import Task, TaskStatus
from openorc.persistence.pool import DatabasePool
from openorc.protocol.models import ReviewResult
from openorc.services.errors import (
    ExternalOperationUncertainError,
    InvalidCommandError,
    StaleOperationError,
)
from openorc.services.task_plan_production import TaskPlanProductionOutcome
from openorc.services.task_planning_review_decision import (
    PlanningReviewDecision,
    finalize_planning_review_decision,
)
from openorc.services.task_planning_review_dispatch import (
    TaskPlanningReviewDispatchOutcome,
    dispatch_planning_review,
)
from tests.fakes.agent_runtime import (
    FakeAgentRuntimeAdapter,
    accepted_review_result_candidate,
    changes_requested_review_result_candidate,
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
_TOKEN_3A = uuid.uuid4()
_TOKEN_4 = uuid.uuid4()
_REVISION_1_ID = uuid.uuid4()
_REVISION_2_ID = uuid.uuid4()
_REVISION_3_ID = uuid.uuid4()
_LOOP_ID = uuid.uuid4()
_ITERATION_1_ID = uuid.uuid4()
_ITERATION_2_ID = uuid.uuid4()
_ITERATION_3_ID = uuid.uuid4()
_GATE_1_ID = uuid.uuid4()
_GATE_2_ID = uuid.uuid4()
_TOKEN_5 = uuid.uuid4()
_TOKEN_6 = uuid.uuid4()
_BASE_SHA = "c" * 40
_PLAN_TEXT = "# Plan\n\nStep one.\n"
_REVISED_PLAN_TEXT = "# Plan (revised)\n\nStep one, clarified.\n"
_REVISED_PLAN_TEXT_2 = "# Plan (revised twice)\n\nStep one, settled.\n"
_FULL_NAME = "octocat/hello-world"
_REVIEW_FINDINGS = [
    {"summary": "Clarify scope", "details": "Say which module changes."},
    {"summary": "Add tests", "details": "Cover the dispatch edge."},
]
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
        raise AssertionError("planning review decision tests never close pools")


def _pool(conn: ScriptedConnection) -> DatabasePool:
    return cast(DatabasePool, FakePool(conn))


_TASK_READ = _sql_lacks(_sql_has("from openorc.tasks where id"), "for update")
_TASK_READ_LOCKED = _sql_has("from openorc.tasks where id", "for update")
_TASK_WRITE = _sql_has("update openorc.tasks")
_POINTER_UPDATE = _sql_has("update openorc.tasks", "current_plan_revision_id")
_GATE_INSTALL = _sql_has("update openorc.tasks", "current_owner_gate_id")
_STATUS_UPDATE = _sql_has("update openorc.tasks", "queued_at = case")
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
_LOOP_INSERT = _sql_has("insert into openorc.review_loops")
_LOOP_CLOSE = _sql_has("update openorc.review_loops")
_ITERATION_READ = _sql_has("from openorc.review_iterations where id")
_ITERATIONS_LIST = _sql_has("review_iterations where review_loop_id")
_ITERATION_INSERT = _sql_has("insert into openorc.review_iterations")
_RESULT_UPDATE = _sql_has("update openorc.review_iterations")
_GATES_LIST = _sql_has("from openorc.owner_gates where task_id")
_GATE_INSERT = _sql_has("insert into openorc.owner_gates")
_EVENT_INSERT = _sql_has("insert into openorc.workflow_events")


def _ws_row(*, limit: int = 2) -> tuple[Any, ...]:
    return (_WORKSPACE_ID, _OWNER_PROFILE_ID, "platform", _OBSERVED, _OBSERVED, limit, "")


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
    gate_id: Any,
    plan_revision_id: Any,
    status: str = "pending",
) -> tuple[Any, ...]:
    return (
        gate_id,
        _WORKSPACE_ID,
        _TASK_ID,
        "review_resolution",
        status,
        plan_revision_id,
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


def _changes_requested_candidate() -> dict[str, Any]:
    return changes_requested_review_result_candidate(
        "Needs work",
        [("Clarify scope", "Say which module changes."), ("Add tests", "Cover the loop edge.")],
    )


class _ProbedAdapter(FakeAgentRuntimeAdapter):
    """Fake runtime proving every send runs with no transaction open."""

    def __init__(self, pool: FakePool) -> None:
        super().__init__()
        self._decision_pool = pool

    def realize_control(
        self,
        session_id: str,
        interaction: Any,
        *,
        message: str | None = None,
        expected_family: str | None = None,
    ) -> Any:
        assert self._decision_pool.active_connections == 0, (
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


def _script_ws_reads(
    conn: ScriptedConnection, *, first_reads: int, first_limit: int, later_limit: int
) -> None:
    """The account barrier plus ordered Workspace reads.

    The first ``first_reads`` Workspace reads carry ``first_limit`` (the
    configured value the first dispatch snapshots onto the loop); every
    later read carries ``later_limit`` — proving the loop's stored budget,
    never the live Workspace setting, governs the decision.
    """
    conn.on_repeat(_sql_has("from openorc.profiles", "for key share"), (None, None, None))
    for _ in range(first_reads):
        conn.on(_sql_has("from openorc.workspaces where id"), _ws_row(limit=first_limit))
    conn.on_repeat(_sql_has("from openorc.workspaces where id"), _ws_row(limit=later_limit))


def _script_repeating_reads(
    conn: ScriptedConnection,
    *,
    reviewer_row: tuple[Any, ...],
    producer_row: tuple[Any, ...],
    loop_row: tuple[Any, ...],
) -> None:
    """Same-shaped reads whose result never changes across the whole flow.

    The Task-row reads are deliberately absent: they are ordered per round
    because each revision production rotates the Task token.
    """
    conn.on_repeat(_role_is(_SESSION_READ, "reviewer"), reviewer_row)
    conn.on_repeat(_role_is(_SESSION_READ, "producer"), producer_row)
    conn.on_repeat(_REPO_READ, _repository_row())
    conn.on_repeat(_INSTALLATION_READ, _installation_row())
    conn.on_repeat(_LOOP_READ, loop_row)
    conn.on_repeat(_LOOP_READ_LOCKED, loop_row)
    conn.on_repeat(_LOOP_GUARD_LOCKED, ("open", "planning"))


def _script_dispatch_phase(
    conn: ScriptedConnection,
    *,
    revision_row: tuple[Any, ...],
    existing_loops: list[tuple[Any, ...]],
    loop_iterations: list[tuple[Any, ...]],
    loop_insert_row: tuple[Any, ...] | None = None,
    iteration_insert_row: tuple[Any, ...] | None = None,
) -> None:
    """One E3a dispatch phase's ordered reads and reservation, in call order."""
    conn.on(_REVISION_READ, revision_row)
    conn.on(_LOOPS_LIST, existing_loops)
    if loop_insert_row is not None:
        conn.on(_LOOP_INSERT, loop_insert_row)
    conn.on(_ITERATIONS_LIST, loop_iterations)
    if iteration_insert_row is not None:
        conn.on(_ITERATION_INSERT, iteration_insert_row)


def _script_finalize_result(
    conn: ScriptedConnection,
    *,
    finalized_row: tuple[Any, ...],
    iterations_after: list[tuple[Any, ...]],
    gates: list[tuple[Any, ...]],
) -> None:
    """The finalize transaction's shared prefix, in call order.

    Registers the one-shot result recording, the refreshed iteration
    history (the budget facts), the durable gate history, and the
    REVIEWER ``PLAN_REVIEWED`` event. The decision-specific continuation
    is registered by the caller.
    """
    conn.on(_RESULT_UPDATE, finalized_row)
    conn.on(_ITERATIONS_LIST, iterations_after)
    conn.on(_GATES_LIST, gates)
    conn.on(_EVENT_INSERT, _event_row())


def _script_revise_phase(
    conn: ScriptedConnection,
    *,
    prior_revision_row: tuple[Any, ...],
    routed_iteration_row: tuple[Any, ...],
    loop_iterations: list[tuple[Any, ...]],
    history: list[tuple[Any, ...]],
    revision_insert_row: tuple[Any, ...],
    pointer_row: tuple[Any, ...],
) -> None:
    """One merged E2 revision production (entry + finalize), in call order."""
    conn.on(_REVISION_READ, prior_revision_row)
    conn.on(_ITERATION_READ, routed_iteration_row)
    conn.on(_ITERATIONS_LIST, loop_iterations)
    conn.on(_REVISION_READ, prior_revision_row)
    conn.on(_ITERATIONS_LIST, loop_iterations)
    conn.on(_REVISIONS_HISTORY, history)
    conn.on(_REVISION_INSERT, revision_insert_row)
    conn.on(_POINTER_UPDATE, pointer_row)
    conn.on(_EVENT_INSERT, _event_row())


def _cr_row(iteration_id: Any, number: int, plan_revision_id: Any) -> tuple[Any, ...]:
    return _iteration_row(
        iteration_id=iteration_id,
        number=number,
        plan_revision_id=plan_revision_id,
        outcome="changes_requested",
        summary="Needs work",
        findings=_REVIEW_FINDINGS,
        decided_at=_OBSERVED,
    )


def _plan_candidate(text: str) -> dict[str, Any]:
    return {"type": "plan_result", "schema_version": 1, "plan": text}


def test_budget_two_routes_two_revisions_then_creates_the_resolution_gate() -> None:
    """Budget 2: two change requests route two real E2 revisions; the third
    creates one exact-plan REVIEW_RESOLUTION gate, moves the Task to
    ``waiting_for_owner``, and records ``REVIEW_LIMIT_REACHED`` exactly once."""
    conn = ScriptedConnection()
    fake_pool = FakePool(conn)
    pool = _pool(conn)

    # Snapshot discrimination: the first three Workspace reads (the first
    # dispatch, which snapshots the configured limit onto the loop) carry
    # limit 2; every later read carries 9. The decision still exhausts on
    # the loop's stored 2 — the Workspace setting is never consulted.
    _script_ws_reads(conn, first_reads=3, first_limit=2, later_limit=9)
    _script_repeating_reads(
        conn,
        reviewer_row=_session_row(
            "reviewer", session_id=_REVIEWER_SESSION_ID, external_id=_REVIEWER_EXTERNAL_ID
        ),
        producer_row=_session_row(
            "producer", session_id=_PRODUCER_SESSION_ID, external_id=_PRODUCER_EXTERNAL_ID
        ),
        loop_row=_loop_row(limit=2),
    )
    t0 = _task_row()
    t1 = _task_row(state_token=_TOKEN_2, current_plan_revision_id=_REVISION_2_ID)
    t2 = _task_row(state_token=_TOKEN_3, current_plan_revision_id=_REVISION_3_ID)
    rev1 = _revision_row(revision_id=_REVISION_1_ID, number=1, content=_PLAN_TEXT)
    rev2 = _revision_row(revision_id=_REVISION_2_ID, number=2, content=_REVISED_PLAN_TEXT)
    rev3 = _revision_row(revision_id=_REVISION_3_ID, number=3, content=_REVISED_PLAN_TEXT_2)
    it1_unfin = _iteration_row(
        iteration_id=_ITERATION_1_ID, number=1, plan_revision_id=_REVISION_1_ID
    )
    it2_unfin = _iteration_row(
        iteration_id=_ITERATION_2_ID, number=2, plan_revision_id=_REVISION_2_ID
    )
    it3_unfin = _iteration_row(
        iteration_id=_ITERATION_3_ID, number=3, plan_revision_id=_REVISION_3_ID
    )
    it1_cr = _cr_row(_ITERATION_1_ID, 1, _REVISION_1_ID)
    it2_cr = _cr_row(_ITERATION_2_ID, 2, _REVISION_2_ID)
    it3_cr = _cr_row(_ITERATION_3_ID, 3, _REVISION_3_ID)
    loop = _loop_row(limit=2)
    gate = _gate_row(gate_id=_GATE_1_ID, plan_revision_id=_REVISION_3_ID)
    installed = _task_row(
        state_token=_TOKEN_3A, current_plan_revision_id=_REVISION_3_ID, gate_id=_GATE_1_ID
    )
    waiting = _task_row(
        status="waiting_for_owner",
        state_token=_TOKEN_4,
        current_plan_revision_id=_REVISION_3_ID,
        gate_id=_GATE_1_ID,
    )

    # Ordered Task reads: 7 unlocked + 2 locked per routed round at that
    # round's token; 4 + 2 for the exhausting round.
    for _ in range(7):
        conn.on(_TASK_READ, t0)
    for _ in range(2):
        conn.on(_TASK_READ_LOCKED, t0)
    for _ in range(7):
        conn.on(_TASK_READ, t1)
    for _ in range(2):
        conn.on(_TASK_READ_LOCKED, t1)
    for _ in range(4):
        conn.on(_TASK_READ, t2)
    for _ in range(2):
        conn.on(_TASK_READ_LOCKED, t2)
    # Ordered revision reads: dispatch/finalize/revise per routed round.
    for row in (rev1, rev1, rev1, rev1, rev2, rev2, rev2, rev2, rev3, rev3):
        conn.on(_REVISION_READ, row)
    # Ordered iteration-history reads: dispatch, attempt recheck, budget
    # recheck, revise supersession, revise revalidation, per round.
    for rows in (
        [],
        [it1_unfin],
        [it1_cr],
        [it1_cr],
        [it1_cr],
        [it1_cr],
        [it1_cr, it2_unfin],
        [it1_cr, it2_cr],
        [it1_cr, it2_cr],
        [it1_cr, it2_cr],
        [it1_cr, it2_cr],
        [it1_cr, it2_cr, it3_unfin],
        [it1_cr, it2_cr, it3_cr],
    ):
        conn.on(_ITERATIONS_LIST, rows)
    # Ordered gate-history reads: empty until the exhausting decision.
    for _ in range(3):
        conn.on(_GATES_LIST, [])
    # Reserved loop listing: none, then the one snapshot loop.
    conn.on(_LOOPS_LIST, [])
    conn.on(_LOOPS_LIST, [loop])
    conn.on(_LOOPS_LIST, [loop])
    # Event inserts, in commit order.
    for _ in range(7):
        conn.on(_EVENT_INSERT, _event_row())

    adapter = _prepared_adapter(
        fake_pool,
        producer_outcomes=[
            _plan_candidate(_REVISED_PLAN_TEXT),
            _plan_candidate(_REVISED_PLAN_TEXT_2),
        ],
        reviewer_outcomes=[
            _changes_requested_candidate(),
            _changes_requested_candidate(),
            _changes_requested_candidate(),
        ],
    )
    github = FakeGitHubClient(fake_pool)

    # --- round 1: dispatch revision 1, finalize in budget, route to E2. ---
    _script_dispatch_phase(
        conn,
        revision_row=rev1,
        existing_loops=[],
        loop_iterations=[],
        loop_insert_row=loop,
        iteration_insert_row=it1_unfin,
    )
    dispatch1 = dispatch_planning_review(
        pool,
        adapter,
        profile_id=_OWNER_PROFILE_ID,
        workspace_id=_WORKSPACE_ID,
        task_id=_TASK_ID,
        plan_production=_production_outcome(),
    )
    assert dispatch1.review_iteration.id == _ITERATION_1_ID
    assert dispatch1.review_iteration.outcome is None
    _script_finalize_result(conn, finalized_row=it1_cr, iterations_after=[it1_cr], gates=[])
    _script_revise_phase(
        conn,
        prior_revision_row=rev1,
        routed_iteration_row=it1_cr,
        loop_iterations=[it1_cr],
        history=[rev1],
        revision_insert_row=rev2,
        pointer_row=t1,
    )
    decision1 = finalize_planning_review_decision(
        pool,
        adapter,
        cast(GitHubAppClient, github),
        profile_id=_OWNER_PROFILE_ID,
        workspace_id=_WORKSPACE_ID,
        task_id=_TASK_ID,
        expected_state_token=_STATE_TOKEN,
        dispatch=dispatch1,
    )
    assert decision1.decision is PlanningReviewDecision.REVISION_ROUTED
    assert decision1.review_iteration.id == _ITERATION_1_ID
    assert decision1.review_iteration.outcome is not None
    assert decision1.review_iteration.summary == "Needs work"
    assert decision1.review_iteration.findings == _REVIEW_FINDINGS
    assert decision1.review_loop.status is ReviewLoopStatus.OPEN
    assert decision1.resolution_gate is None
    production2 = decision1.revision_production
    assert production2 is not None
    assert production2.plan_revision.id == _REVISION_2_ID
    assert production2.task.state_token == _TOKEN_2

    # --- round 2: dispatch revision 2, finalize in budget, route to E2. ---
    _script_dispatch_phase(
        conn,
        revision_row=rev2,
        existing_loops=[loop],
        loop_iterations=[it1_cr],
        iteration_insert_row=it2_unfin,
    )
    dispatch2 = dispatch_planning_review(
        pool,
        adapter,
        profile_id=_OWNER_PROFILE_ID,
        workspace_id=_WORKSPACE_ID,
        task_id=_TASK_ID,
        plan_production=production2,
    )
    _script_finalize_result(conn, finalized_row=it2_cr, iterations_after=[it1_cr, it2_cr], gates=[])
    _script_revise_phase(
        conn,
        prior_revision_row=rev2,
        routed_iteration_row=it2_cr,
        loop_iterations=[it1_cr, it2_cr],
        history=[rev1, rev2],
        revision_insert_row=rev3,
        pointer_row=t2,
    )
    decision2 = finalize_planning_review_decision(
        pool,
        adapter,
        cast(GitHubAppClient, github),
        profile_id=_OWNER_PROFILE_ID,
        workspace_id=_WORKSPACE_ID,
        task_id=_TASK_ID,
        expected_state_token=_TOKEN_2,
        dispatch=dispatch2,
    )
    assert decision2.decision is PlanningReviewDecision.REVISION_ROUTED
    production3 = decision2.revision_production
    assert production3 is not None
    assert production3.plan_revision.id == _REVISION_3_ID
    assert production3.task.state_token == _TOKEN_3

    # --- round 3: the third change request exhausts the snapshotted budget. ---
    _script_dispatch_phase(
        conn,
        revision_row=rev3,
        existing_loops=[loop],
        loop_iterations=[it1_cr, it2_cr],
        iteration_insert_row=it3_unfin,
    )
    dispatch3 = dispatch_planning_review(
        pool,
        adapter,
        profile_id=_OWNER_PROFILE_ID,
        workspace_id=_WORKSPACE_ID,
        task_id=_TASK_ID,
        plan_production=production3,
    )
    _script_finalize_result(
        conn,
        finalized_row=it3_cr,
        iterations_after=[it1_cr, it2_cr, it3_cr],
        gates=[],
    )
    conn.on(_GATE_INSERT, gate)
    conn.on(_GATE_INSTALL, installed)
    conn.on(_EVENT_INSERT, _event_row())
    conn.on(_STATUS_UPDATE, waiting)
    conn.on(_EVENT_INSERT, _event_row())
    decision3 = finalize_planning_review_decision(
        pool,
        adapter,
        cast(GitHubAppClient, github),
        profile_id=_OWNER_PROFILE_ID,
        workspace_id=_WORKSPACE_ID,
        task_id=_TASK_ID,
        expected_state_token=_TOKEN_3,
        dispatch=dispatch3,
    )
    assert decision3.decision is PlanningReviewDecision.EXHAUSTED
    assert decision3.resolution_gate is not None
    assert decision3.resolution_gate.id == _GATE_1_ID
    assert decision3.resolution_gate.plan_revision_id == _REVISION_3_ID
    assert decision3.task.status is TaskStatus.WAITING_FOR_OWNER
    assert decision3.task.current_owner_gate_id == _GATE_1_ID
    assert decision3.task.state_token == _TOKEN_4
    assert decision3.review_loop.status is ReviewLoopStatus.OPEN
    assert decision3.revision_production is None

    # Exactly the expected audit stream: three plan_reviewed events, two
    # plan_revision_revised events, one owner_gate_created, and exactly
    # ONE review_limit_reached (the first exhaustion).
    assert _event_types(conn) == [
        "plan_reviewed",
        "plan_revision_revised",
        "plan_reviewed",
        "plan_revision_revised",
        "plan_reviewed",
        "owner_gate_created",
        "review_limit_reached",
    ]
    reviewed = _event_params_of_type(conn, "plan_reviewed")
    assert [params[6] for params in reviewed] == [
        _ITERATION_1_ID,
        _ITERATION_2_ID,
        _ITERATION_3_ID,
    ]
    for params in reviewed:
        assert params[3] == "reviewer"
        assert params[4] == str(_REVIEWER_SESSION_ID)
        assert params[5] == "review_iteration"
        assert params[7].obj == {"outcome": "changes_requested"}
    (gate_created,) = _event_params_of_type(conn, "owner_gate_created")
    assert gate_created[3] == "openorc"
    assert gate_created[5] == "owner_gate"
    assert gate_created[6] == _GATE_1_ID
    assert gate_created[7].obj == {}
    (limit_reached,) = _event_params_of_type(conn, "review_limit_reached")
    assert limit_reached[3] == "openorc"
    assert limit_reached[5] == "review_loop"
    assert limit_reached[6] == _LOOP_ID
    assert limit_reached[7].obj == {}

    # The exhausted findings are never forwarded: the gate binds only the
    # exact plan revision (no head SHA, no PR binding, no findings).
    gate_inserts = [(sql, params) for sql, params in conn.executed if _GATE_INSERT(sql, None)]
    assert len(gate_inserts) == 1
    assert gate_inserts[0][1] is not None
    assert gate_inserts[0][1][4] == _REVISION_3_ID
    assert gate_inserts[0][1][5] is None
    assert gate_inserts[0][1][6] is None

    # Exactly two real E2 revisions were produced and routed; the third
    # decision never reached the Producer.
    assert _count(conn, _REVISION_INSERT) == 2
    assert _count(conn, _RESULT_UPDATE) == 3
    assert len(adapter.realized_controls(_REVIEWER_EXTERNAL_ID)) == 3
    assert len(adapter.realized_controls(_PRODUCER_EXTERNAL_ID)) == 2


def _loop_domain(*, limit: int = 2, status: str = "open") -> Any:
    from openorc.domain.reviews import ReviewLoop, ReviewLoopPurpose, ReviewLoopStatus

    return ReviewLoop(
        id=_LOOP_ID,
        workspace_id=_WORKSPACE_ID,
        task_id=_TASK_ID,
        purpose=ReviewLoopPurpose.PLANNING,
        iteration_limit=limit,
        status=ReviewLoopStatus(status),
        closed_at=_OBSERVED if status == "closed" else None,
        created_at=_OBSERVED,
    )


def _iteration_domain(*, iteration_id: Any, number: int, plan_revision_id: Any) -> Any:
    from openorc.domain.reviews import ReviewIteration

    return ReviewIteration(
        id=iteration_id,
        workspace_id=_WORKSPACE_ID,
        task_id=_TASK_ID,
        review_loop_id=_LOOP_ID,
        iteration_number=number,
        plan_revision_id=plan_revision_id,
        task_pull_request_id=None,
        reviewed_head_sha=None,
        outcome=None,
        summary=None,
        findings=None,
        decided_at=None,
        created_at=_OBSERVED,
    )


def _dispatch_outcome(*, iteration_id: Any, number: int, plan_revision_id: Any) -> Any:
    """A hand-built E3a continuation input for refusal-path tests."""
    return TaskPlanningReviewDispatchOutcome(
        review_loop=_loop_domain(),
        review_iteration=_iteration_domain(
            iteration_id=iteration_id, number=number, plan_revision_id=plan_revision_id
        ),
        review_result=ReviewResult(outcome="CHANGES_REQUESTED", summary="Needs work", findings=()),
        reviewer_session=_session_domain(
            WorkflowRole.REVIEWER,
            session_id=_REVIEWER_SESSION_ID,
            external_id=_REVIEWER_EXTERNAL_ID,
        ),
    )


def test_accepted_clearance_closes_the_loop_and_yields_the_plan_evidence() -> None:
    """One representative ACCEPTED clearance: the loop closes, the exact
    accepted PlanRevision/iteration evidence is yielded, and nothing else
    happens — no E2 routing, no gate, no Task mutation."""
    conn = ScriptedConnection()
    fake_pool = FakePool(conn)
    pool = _pool(conn)
    _script_ws_reads(conn, first_reads=0, first_limit=5, later_limit=5)
    _script_repeating_reads(
        conn,
        reviewer_row=_session_row(
            "reviewer", session_id=_REVIEWER_SESSION_ID, external_id=_REVIEWER_EXTERNAL_ID
        ),
        producer_row=_session_row(
            "producer", session_id=_PRODUCER_SESSION_ID, external_id=_PRODUCER_EXTERNAL_ID
        ),
        loop_row=_loop_row(limit=5),
    )
    t0 = _task_row()
    rev1 = _revision_row(revision_id=_REVISION_1_ID, number=1, content=_PLAN_TEXT)
    it1_unfin = _iteration_row(
        iteration_id=_ITERATION_1_ID, number=1, plan_revision_id=_REVISION_1_ID
    )
    it1_acc = _iteration_row(
        iteration_id=_ITERATION_1_ID,
        number=1,
        plan_revision_id=_REVISION_1_ID,
        outcome="accepted",
        summary="Looks good",
        findings=[],
        decided_at=_OBSERVED,
    )
    for _ in range(4):
        conn.on(_TASK_READ, t0)
    for _ in range(2):
        conn.on(_TASK_READ_LOCKED, t0)
    conn.on(_REVISION_READ, rev1)
    conn.on(_REVISION_READ, rev1)
    conn.on(_LOOPS_LIST, [])
    conn.on(_LOOP_INSERT, _loop_row(limit=5))
    conn.on(_ITERATION_INSERT, it1_unfin)
    conn.on(_ITERATIONS_LIST, [])
    conn.on(_ITERATIONS_LIST, [it1_unfin])
    conn.on(_ITERATIONS_LIST, [it1_acc])
    _script_finalize_result(conn, finalized_row=it1_acc, iterations_after=[it1_acc], gates=[])
    conn.on(_LOOP_CLOSE, _loop_row(limit=5, status="closed", closed_at=_OBSERVED))
    adapter = _prepared_adapter(
        fake_pool,
        producer_outcomes=[],
        reviewer_outcomes=[accepted_review_result_candidate("Looks good")],
    )
    github = FakeGitHubClient(fake_pool)

    dispatch = dispatch_planning_review(
        pool,
        adapter,
        profile_id=_OWNER_PROFILE_ID,
        workspace_id=_WORKSPACE_ID,
        task_id=_TASK_ID,
        plan_production=_production_outcome(),
    )
    decision = finalize_planning_review_decision(
        pool,
        adapter,
        cast(GitHubAppClient, github),
        profile_id=_OWNER_PROFILE_ID,
        workspace_id=_WORKSPACE_ID,
        task_id=_TASK_ID,
        expected_state_token=_STATE_TOKEN,
        dispatch=dispatch,
    )

    assert decision.decision is PlanningReviewDecision.ACCEPTED_CLEAR
    assert decision.review_loop.status is ReviewLoopStatus.CLOSED
    assert decision.review_loop.closed_at is not None
    assert decision.plan_revision.id == _REVISION_1_ID
    assert decision.review_iteration.id == _ITERATION_1_ID
    assert decision.review_iteration.outcome is not None
    assert decision.review_iteration.findings == []
    assert decision.task.status is TaskStatus.PLANNING
    assert decision.task.state_token == _STATE_TOKEN
    assert decision.resolution_gate is None
    assert decision.revision_production is None
    assert _event_types(conn) == ["plan_reviewed"]
    (reviewed,) = _event_params_of_type(conn, "plan_reviewed")
    assert reviewed[7].obj == {"outcome": "accepted"}
    assert _count(conn, _REVISION_INSERT) == 0
    assert _count(conn, _POINTER_UPDATE) == 0
    assert _count(conn, _GATE_INSERT) == 0
    assert _count(conn, _TASK_WRITE) == 0
    assert len(adapter.realized_controls(_PRODUCER_EXTERNAL_ID)) == 0
    assert len(adapter.realized_controls(_REVIEWER_EXTERNAL_ID)) == 1


def test_duplicate_finalization_applies_nothing() -> None:
    """Replaying the same successful dispatch outcome after finalization is a
    stale duplicate: no second result, event, gate, loop-close, or send."""
    conn = ScriptedConnection()
    fake_pool = FakePool(conn)
    pool = _pool(conn)
    _script_ws_reads(conn, first_reads=0, first_limit=5, later_limit=5)
    _script_repeating_reads(
        conn,
        reviewer_row=_session_row(
            "reviewer", session_id=_REVIEWER_SESSION_ID, external_id=_REVIEWER_EXTERNAL_ID
        ),
        producer_row=_session_row(
            "producer", session_id=_PRODUCER_SESSION_ID, external_id=_PRODUCER_EXTERNAL_ID
        ),
        loop_row=_loop_row(limit=5),
    )
    t0 = _task_row()
    rev1 = _revision_row(revision_id=_REVISION_1_ID, number=1, content=_PLAN_TEXT)
    it1_unfin = _iteration_row(
        iteration_id=_ITERATION_1_ID, number=1, plan_revision_id=_REVISION_1_ID
    )
    it1_acc = _iteration_row(
        iteration_id=_ITERATION_1_ID,
        number=1,
        plan_revision_id=_REVISION_1_ID,
        outcome="accepted",
        summary="Looks good",
        findings=[],
        decided_at=_OBSERVED,
    )
    for _ in range(6):
        conn.on(_TASK_READ, t0)
    for _ in range(3):
        conn.on(_TASK_READ_LOCKED, t0)
    for _ in range(3):
        conn.on(_REVISION_READ, rev1)
    conn.on(_LOOPS_LIST, [])
    conn.on(_LOOP_INSERT, _loop_row(limit=5))
    conn.on(_ITERATION_INSERT, it1_unfin)
    conn.on(_ITERATIONS_LIST, [])
    conn.on(_ITERATIONS_LIST, [it1_unfin])
    conn.on(_ITERATIONS_LIST, [it1_acc])
    conn.on(_ITERATIONS_LIST, [it1_acc])
    _script_finalize_result(conn, finalized_row=it1_acc, iterations_after=[it1_acc], gates=[])
    conn.on(_LOOP_CLOSE, _loop_row(limit=5, status="closed", closed_at=_OBSERVED))
    adapter = _prepared_adapter(
        fake_pool,
        producer_outcomes=[],
        reviewer_outcomes=[accepted_review_result_candidate("Looks good")],
    )
    github = FakeGitHubClient(fake_pool)

    dispatch = dispatch_planning_review(
        pool,
        adapter,
        profile_id=_OWNER_PROFILE_ID,
        workspace_id=_WORKSPACE_ID,
        task_id=_TASK_ID,
        plan_production=_production_outcome(),
    )
    first = finalize_planning_review_decision(
        pool,
        adapter,
        cast(GitHubAppClient, github),
        profile_id=_OWNER_PROFILE_ID,
        workspace_id=_WORKSPACE_ID,
        task_id=_TASK_ID,
        expected_state_token=_STATE_TOKEN,
        dispatch=dispatch,
    )
    assert first.decision is PlanningReviewDecision.ACCEPTED_CLEAR

    with pytest.raises(StaleOperationError):
        finalize_planning_review_decision(
            pool,
            adapter,
            cast(GitHubAppClient, github),
            profile_id=_OWNER_PROFILE_ID,
            workspace_id=_WORKSPACE_ID,
            task_id=_TASK_ID,
            expected_state_token=_STATE_TOKEN,
            dispatch=dispatch,
        )

    assert _count(conn, _RESULT_UPDATE) == 1
    assert len(_event_params_of_type(conn, "plan_reviewed")) == 1
    assert _count(conn, _GATE_INSERT) == 0
    assert _count(conn, _LOOP_CLOSE) == 1
    assert _count(conn, _TASK_WRITE) == 0
    assert len(adapter.realized_controls(_REVIEWER_EXTERNAL_ID)) == 1
    assert len(adapter.realized_controls(_PRODUCER_EXTERNAL_ID)) == 0


def test_stale_token_finalization_refuses_before_any_effect() -> None:
    """A moved-on Task token never applies a verdict, event, or gate."""
    conn = ScriptedConnection()
    fake_pool = FakePool(conn)
    pool = _pool(conn)
    _script_ws_reads(conn, first_reads=0, first_limit=5, later_limit=5)
    _script_repeating_reads(
        conn,
        reviewer_row=_session_row(
            "reviewer", session_id=_REVIEWER_SESSION_ID, external_id=_REVIEWER_EXTERNAL_ID
        ),
        producer_row=_session_row(
            "producer", session_id=_PRODUCER_SESSION_ID, external_id=_PRODUCER_EXTERNAL_ID
        ),
        loop_row=_loop_row(limit=5),
    )
    t0 = _task_row()
    for _ in range(2):
        conn.on(_TASK_READ, t0)
    conn.on(_TASK_READ_LOCKED, t0)
    adapter = _prepared_adapter(fake_pool, producer_outcomes=[], reviewer_outcomes=[])
    github = FakeGitHubClient(fake_pool)

    with pytest.raises(StaleOperationError):
        finalize_planning_review_decision(
            pool,
            adapter,
            cast(GitHubAppClient, github),
            profile_id=_OWNER_PROFILE_ID,
            workspace_id=_WORKSPACE_ID,
            task_id=_TASK_ID,
            expected_state_token=_TOKEN_5,
            dispatch=_dispatch_outcome(
                iteration_id=_ITERATION_1_ID, number=1, plan_revision_id=_REVISION_1_ID
            ),
        )

    assert _count(conn, _RESULT_UPDATE) == 0
    assert _count(conn, _EVENT_INSERT) == 0
    assert _count(conn, _GATE_INSERT) == 0
    assert _count(conn, _TASK_WRITE) == 0
    assert len(adapter.realized_controls(_PRODUCER_EXTERNAL_ID)) == 0


def test_a_continuation_without_the_dispatch_outcome_cannot_record_a_verdict() -> None:
    """Only the exact E3a continuation input authorizes a decision: a bare
    typed result (or no continuation at all) records nothing — an
    unresolved REVIEW attempt is never treated as CHANGES_REQUESTED."""
    conn = ScriptedConnection()
    fake_pool = FakePool(conn)
    pool = _pool(conn)
    github = FakeGitHubClient(fake_pool)
    adapter = _prepared_adapter(fake_pool, producer_outcomes=[], reviewer_outcomes=[])

    with pytest.raises(InvalidCommandError):
        finalize_planning_review_decision(
            pool,
            adapter,
            cast(GitHubAppClient, github),
            profile_id=_OWNER_PROFILE_ID,
            workspace_id=_WORKSPACE_ID,
            task_id=_TASK_ID,
            expected_state_token=_STATE_TOKEN,
            dispatch=None,  # type: ignore[arg-type]
        )
    with pytest.raises(InvalidCommandError):
        finalize_planning_review_decision(
            pool,
            adapter,
            cast(GitHubAppClient, github),
            profile_id=_OWNER_PROFILE_ID,
            workspace_id=_WORKSPACE_ID,
            task_id=_TASK_ID,
            expected_state_token=_STATE_TOKEN,
            dispatch=cast(
                Any,
                ReviewResult(outcome="CHANGES_REQUESTED", summary="Needs work", findings=()),
            ),
        )

    assert conn.executed == []
    assert len(adapter.realized_controls(_PRODUCER_EXTERNAL_ID)) == 0
    assert len(adapter.realized_controls(_REVIEWER_EXTERNAL_ID)) == 0


def test_owner_mediated_history_immediately_reopens_resolution() -> None:
    """Durable REVIEW_RESOLUTION gate history makes Owner mediation sticky:
    the very next change request — well within the untouched snapshot budget
    — immediately creates a NEW gate, moves to ``waiting_for_owner``, and
    never emits a second ``REVIEW_LIMIT_REACHED``."""
    conn = ScriptedConnection()
    fake_pool = FakePool(conn)
    pool = _pool(conn)
    _script_ws_reads(conn, first_reads=0, first_limit=5, later_limit=5)
    _script_repeating_reads(
        conn,
        reviewer_row=_session_row(
            "reviewer", session_id=_REVIEWER_SESSION_ID, external_id=_REVIEWER_EXTERNAL_ID
        ),
        producer_row=_session_row(
            "producer", session_id=_PRODUCER_SESSION_ID, external_id=_PRODUCER_EXTERNAL_ID
        ),
        loop_row=_loop_row(limit=5),
    )
    t1 = _task_row(state_token=_TOKEN_2, current_plan_revision_id=_REVISION_2_ID)
    rev2 = _revision_row(revision_id=_REVISION_2_ID, number=2, content=_REVISED_PLAN_TEXT)
    it1_cr = _cr_row(_ITERATION_1_ID, 1, _REVISION_1_ID)
    it2_unfin = _iteration_row(
        iteration_id=_ITERATION_2_ID, number=2, plan_revision_id=_REVISION_2_ID
    )
    it2_cr = _cr_row(_ITERATION_2_ID, 2, _REVISION_2_ID)
    settled_gate = _gate_row(gate_id=_GATE_1_ID, plan_revision_id=_REVISION_1_ID, status="approved")
    new_gate = _gate_row(gate_id=_GATE_2_ID, plan_revision_id=_REVISION_2_ID)
    installed = _task_row(
        state_token=_TOKEN_5, current_plan_revision_id=_REVISION_2_ID, gate_id=_GATE_2_ID
    )
    waiting = _task_row(
        status="waiting_for_owner",
        state_token=_TOKEN_6,
        current_plan_revision_id=_REVISION_2_ID,
        gate_id=_GATE_2_ID,
    )
    for _ in range(4):
        conn.on(_TASK_READ, t1)
    for _ in range(2):
        conn.on(_TASK_READ_LOCKED, t1)
    conn.on(_REVISION_READ, rev2)
    conn.on(_REVISION_READ, rev2)
    conn.on(_LOOPS_LIST, [_loop_row(limit=5)])
    conn.on(_ITERATION_INSERT, it2_unfin)
    conn.on(_ITERATIONS_LIST, [it1_cr])
    conn.on(_ITERATIONS_LIST, [it1_cr, it2_unfin])
    conn.on(_ITERATIONS_LIST, [it1_cr, it2_cr])
    conn.on(_GATES_LIST, [settled_gate])
    _script_finalize_result(
        conn, finalized_row=it2_cr, iterations_after=[it1_cr, it2_cr], gates=[settled_gate]
    )
    conn.on(_GATE_INSERT, new_gate)
    conn.on(_GATE_INSTALL, installed)
    conn.on(_EVENT_INSERT, _event_row())
    conn.on(_STATUS_UPDATE, waiting)
    # No review_limit_reached handler is registered: an extra event insert
    # would fail this test loudly instead of passing silently.
    adapter = _prepared_adapter(
        fake_pool, producer_outcomes=[], reviewer_outcomes=[_changes_requested_candidate()]
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
            state_token=_TOKEN_2,
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
        expected_state_token=_TOKEN_2,
        dispatch=dispatch,
    )

    assert decision.decision is PlanningReviewDecision.EXHAUSTED
    assert decision.resolution_gate is not None
    assert decision.resolution_gate.id == _GATE_2_ID
    assert decision.resolution_gate.plan_revision_id == _REVISION_2_ID
    assert decision.task.status is TaskStatus.WAITING_FOR_OWNER
    assert decision.task.current_owner_gate_id == _GATE_2_ID
    assert decision.task.state_token == _TOKEN_6
    assert decision.review_loop.status is ReviewLoopStatus.OPEN
    # The budget was NOT exhausted (two change requests against a snapshot
    # of five): the immediate resolution came from durable gate history.
    assert _event_types(conn) == ["plan_reviewed", "owner_gate_created"]
    assert _event_params_of_type(conn, "review_limit_reached") == []
    (gate_created,) = _event_params_of_type(conn, "owner_gate_created")
    assert gate_created[6] == _GATE_2_ID
    assert len(adapter.realized_controls(_PRODUCER_EXTERNAL_ID)) == 0


def test_uncertain_revision_production_propagates_and_is_never_resent() -> None:
    """An uncertain E2 send propagates typed after the review finalization
    committed: the finalized result and its event stay durable, nothing was
    produced, and E3b never resends."""
    conn = ScriptedConnection()
    fake_pool = FakePool(conn)
    pool = _pool(conn)
    _script_ws_reads(conn, first_reads=0, first_limit=2, later_limit=2)
    _script_repeating_reads(
        conn,
        reviewer_row=_session_row(
            "reviewer", session_id=_REVIEWER_SESSION_ID, external_id=_REVIEWER_EXTERNAL_ID
        ),
        producer_row=_session_row(
            "producer", session_id=_PRODUCER_SESSION_ID, external_id=_PRODUCER_EXTERNAL_ID
        ),
        loop_row=_loop_row(limit=2),
    )
    t0 = _task_row()
    rev1 = _revision_row(revision_id=_REVISION_1_ID, number=1, content=_PLAN_TEXT)
    it1_unfin = _iteration_row(
        iteration_id=_ITERATION_1_ID, number=1, plan_revision_id=_REVISION_1_ID
    )
    it1_cr = _cr_row(_ITERATION_1_ID, 1, _REVISION_1_ID)
    for _ in range(6):
        conn.on(_TASK_READ, t0)
    for _ in range(2):
        conn.on(_TASK_READ_LOCKED, t0)
    for _ in range(4):
        conn.on(_REVISION_READ, rev1)
    conn.on(_LOOPS_LIST, [])
    conn.on(_LOOP_INSERT, _loop_row(limit=2))
    conn.on(_ITERATION_INSERT, it1_unfin)
    conn.on(_ITERATION_READ, it1_cr)
    conn.on(_ITERATIONS_LIST, [])
    conn.on(_ITERATIONS_LIST, [it1_unfin])
    conn.on(_ITERATIONS_LIST, [it1_cr])
    conn.on(_ITERATIONS_LIST, [it1_cr])
    conn.on(_ITERATIONS_LIST, [it1_cr])
    _script_finalize_result(conn, finalized_row=it1_cr, iterations_after=[it1_cr], gates=[])
    adapter = _prepared_adapter(
        fake_pool,
        producer_outcomes=[AgentRuntimeDeliveryUncertainError("the delivery outcome is unknown")],
        reviewer_outcomes=[_changes_requested_candidate()],
    )
    github = FakeGitHubClient(fake_pool)

    dispatch = dispatch_planning_review(
        pool,
        adapter,
        profile_id=_OWNER_PROFILE_ID,
        workspace_id=_WORKSPACE_ID,
        task_id=_TASK_ID,
        plan_production=_production_outcome(),
    )
    with pytest.raises(ExternalOperationUncertainError):
        finalize_planning_review_decision(
            pool,
            adapter,
            cast(GitHubAppClient, github),
            profile_id=_OWNER_PROFILE_ID,
            workspace_id=_WORKSPACE_ID,
            task_id=_TASK_ID,
            expected_state_token=_STATE_TOKEN,
            dispatch=dispatch,
        )

    # The decision transaction's durable facts stand; the uncertain E2 send
    # produced nothing and was attempted exactly once.
    assert _count(conn, _RESULT_UPDATE) == 1
    assert _event_types(conn) == ["plan_reviewed"]
    assert _count(conn, _REVISION_INSERT) == 0
    assert _count(conn, _POINTER_UPDATE) == 0
    assert len(adapter.realized_controls(_PRODUCER_EXTERNAL_ID)) == 1
    assert len(adapter.realized_controls(_REVIEWER_EXTERNAL_ID)) == 1
