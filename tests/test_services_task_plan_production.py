"""Deterministic tests for Producer plan production and PlanRevision creation (issue #77).

A scripted SQL-connection seam plus the #69 fake Agent Runtime and a small
scripted GitHub observation fake prove the E2 contract on its critical path:

- the exact successful E1b handoff (post-claim Task token, both READY binding
  identities) is the only gate for the first PLAN — coarse ``PLANNING``/READY
  facts and malformed preparations fail closed before any read or effect;
- strict phase ordering: the entry transaction closes before the GitHub
  observation, the observation and the ONE canonical PLAN/REVISE send run
  with no database transaction open (the fake pool tracks checked-out
  connections), and the finalize transaction rechecks the exact subject
  before the atomic revision/pointer/event composition;
- initial production stores the exact plan text as revision 1 with the
  branch-observed repository-base SHA, installs the pointer, rotates the
  Task token, and records ``PLAN_REVISION_CREATED``; revision production
  routes the persisted finalized ``CHANGES_REQUESTED`` review verbatim to
  the same Producer session and records ``PLAN_REVISION_REVISED``;
- wrong subjects, non-READY bindings, unfinalized/ACCEPTED/non-planning/
  closed-loop reviews, superseded-on-the-same-plan reviews (issue #207),
  invalid formal results, known failures, uncertain outcomes,
  stale-after-reply subjects, and competing finalizations all refuse the
  consequential write, and uncertainty is never permission to resend — a
  superseding result finalized during the send also publishes nothing.

Durable atomicity is proven by the integration-marked suite against a real
Postgres. No live GitHub, runtime, or Postgres access.
"""

from __future__ import annotations

import json
import uuid
from collections.abc import Callable, Iterator, Sequence
from contextlib import contextmanager
from datetime import UTC, datetime
from typing import Any, cast

import pytest

from openorc.adapters.agent_runtime.errors import (
    AgentRuntimeDeliveryUncertainError,
    AgentRuntimeUnavailableError,
)
from openorc.adapters.agent_runtime.results import AgentSessionCreationRequest
from openorc.adapters.github import (
    GitHubAppClient,
    GitHubBranchObservation,
    GitHubRepositoryObservation,
)
from openorc.domain.connections import WorkflowRole
from openorc.domain.send_claims import (
    TaskRuntimeSendClaim,
    TaskRuntimeSendClaimKind,
    TaskRuntimeSendClaimStatus,
)
from openorc.domain.sessions import TaskAgentSession
from openorc.domain.tasks import Task, TaskStatus
from openorc.persistence.pool import DatabasePool
from openorc.protocol.interaction import PlanInteraction
from openorc.protocol.models import PlanResult
from openorc.services.errors import (
    ExternalOperationFailedError,
    ExternalOperationRejectedError,
    ExternalOperationUncertainError,
    InvalidCommandError,
    ObservationRegistrationRefusedError,
    StaleOperationError,
)
from openorc.services.task_detached_turn_observation import (
    ObservationRegistrationRefusalReason,
)
from openorc.services.task_plan_production import (
    DispatchedPlanProduction,
    TaskPlanProductionOutcome,
    dispatch_initial_plan_detached,
    dispatch_planning_revise_detached,
    settle_observed_plan_result,
)
from openorc.services.task_planning_bootstrap import TaskPlanningPreparationOutcome
from tests.fakes.agent_runtime import (
    FakeAgentRuntimeAdapter,
    session_ready_candidate,
)
from tests.fakes.task_rows import task_row

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
_NEXT_TOKEN = uuid.uuid4()
_ROTATED_TOKEN = uuid.uuid4()
_REVISION_1_ID = uuid.uuid4()
_REVISION_2_ID = uuid.uuid4()
_LOOP_ID = uuid.uuid4()
_ITERATION_ID = uuid.uuid4()
_ITERATION_2_ID = uuid.uuid4()
_BASE_SHA = "c" * 40
_PLAN_TEXT = "# Plan\n\nStep one.\n"
_REVISED_PLAN_TEXT = "# Plan (revised)\n\nStep one, clarified.\n"
_GUIDANCE = "Keep the plan minimal."
_REPOSITORY_BASE = "/srv/repositories"
_FULL_NAME = "octocat/hello-world"
_FINDINGS = [
    {"summary": "Clarify scope", "details": "Say which module changes."},
    {"summary": "Add tests", "details": "Cover the duplicate-send edge."},
]
_REVIEW_RESULT_SERIALIZED = json.dumps(
    {
        "type": "review_result",
        "schema_version": 1,
        "outcome": "CHANGES_REQUESTED",
        "summary": "Needs work",
        "findings": _FINDINGS,
    }
)

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
    repeating, so same-shaped reads with different scripted outcomes (the
    entry task reads versus the post-send finalize recheck) script in call
    order.
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
        raise AssertionError("plan production service tests never close pools")


def _pool(conn: ScriptedConnection) -> DatabasePool:
    return cast(DatabasePool, FakePool(conn))


_TASK_READ = _sql_lacks(_sql_has("from openorc.tasks where id"), "for update")
_TASK_READ_LOCKED = _sql_has("from openorc.tasks where id", "for update")
_POINTER_UPDATE = _sql_has("update openorc.tasks", "current_plan_revision_id")
_EVENT_INSERT = _sql_has("insert into openorc.workflow_events")
_REVISION_INSERT = _sql_has("insert into openorc.plan_revisions")


def _ws_row(*, guidance: str = "") -> tuple[Any, ...]:
    return (_WORKSPACE_ID, _OWNER_PROFILE_ID, "platform", _OBSERVED, _OBSERVED, 5, guidance, None)


def _task_row(
    *,
    status: str = "planning",
    state_token: Any = _STATE_TOKEN,
    current_plan_revision_id: Any = None,
    workspace_id: Any = _WORKSPACE_ID,
) -> tuple[Any, ...]:
    return task_row(
        id=_TASK_ID,
        workspace_id=workspace_id,
        repository_id=_REPOSITORY_ID,
        github_issue_id=503,
        status=status,
        state_token=state_token,
        current_plan_revision_id=current_plan_revision_id,
        source_requirements_fingerprint="a" * 64,
        created_at=_OBSERVED,
        updated_at=_OBSERVED,
    )


def _updated_task_row(*, pointer: Any, token: Any = _NEXT_TOKEN) -> tuple[Any, ...]:
    return _task_row(state_token=token, current_plan_revision_id=pointer)


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


def _session_row(
    role: str,
    *,
    session_id: Any,
    external_id: str | None,
    lifecycle: str = "ready",
) -> tuple[Any, ...]:
    # Row coherence mirrors the domain CHECK: READY and LOST carry the three
    # initialization facts together; CONNECTING carries none; ENDED permits
    # either form (scripted here in the initialized form).
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
    *, revision_id: Any, number: int, content: str = _PLAN_TEXT, sha: str = _BASE_SHA
) -> tuple[Any, ...]:
    return (revision_id, _WORKSPACE_ID, _TASK_ID, number, content, sha, _OBSERVED)


def _loop_row(
    *,
    purpose: str = "planning",
    status: str = "open",
    closed_at: Any = None,
) -> tuple[Any, ...]:
    return (_LOOP_ID, _WORKSPACE_ID, _TASK_ID, purpose, 3, status, closed_at, _OBSERVED)


def _iteration_row(
    *,
    iteration_id: Any = _ITERATION_ID,
    number: int = 1,
    outcome: str | None = "changes_requested",
    summary: str | None = "Needs work",
    findings: Any = _FINDINGS,
    plan_revision_id: Any = _REVISION_1_ID,
    decided_at: Any = _OBSERVED,
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


def _event_row(event_type: str, subject_id: Any) -> tuple[Any, ...]:
    return (
        uuid.uuid4(),
        _WORKSPACE_ID,
        _TASK_ID,
        event_type,
        "producer",
        str(_PRODUCER_SESSION_ID),
        "plan_revision",
        subject_id,
        {},
        _OBSERVED,
    )


_CLAIM_ID = uuid.uuid4()
_CLAIM_AT = datetime(2026, 10, 26, 9, 30, 0, tzinfo=UTC)

_CLAIM_PRECHECK = _sql_has("from openorc.task_runtime_send_claims", "task_id = %s and kind = %s")
_CLAIM_INSERT = _sql_has("insert into openorc.task_runtime_send_claims")
_CLAIM_SETTLE = _sql_has("update openorc.task_runtime_send_claims", "settled_at = now()")


def _claim_row(
    *,
    kind: str = "initial_plan_dispatch",
    status: str = "claimed",
    outcome: Any = None,
    review_iteration_id: Any = None,
    owner_gate_id: Any = None,
    subject_head_sha: Any = None,
    dispatch_repository_base_sha: Any = None,
    settled: bool = False,
    claim_id: Any = _CLAIM_ID,
) -> tuple[Any, ...]:
    settled_at = _CLAIM_AT if settled or status != "claimed" else None
    return (
        claim_id,
        _WORKSPACE_ID,
        _TASK_ID,
        kind,
        _PRODUCER_SESSION_ID,
        review_iteration_id,
        owner_gate_id,
        subject_head_sha,
        dispatch_repository_base_sha,
        status,
        {} if outcome is None else outcome,
        _CLAIM_AT,
        settled_at,
        _CLAIM_AT,
    )


def _script_initial_claim(
    conn: ScriptedConnection,
    *,
    task_row: tuple[Any, ...],
    producer_row: tuple[Any, ...],
    existing_claim: tuple[Any, ...] | None = None,
) -> None:
    """The initial-PLAN claim transaction's canned sequence, in call order.

    The locked Task re-read, the Producer binding reproof, the exact-subject
    pre-check (``None`` means unclaimed), and the committed claim row."""
    conn.on(_TASK_READ_LOCKED, task_row)
    conn.on(_role_is(_sql_has("from openorc.task_agent_sessions"), "producer"), producer_row)
    conn.on(_CLAIM_PRECHECK, existing_claim)
    conn.on(_CLAIM_INSERT, _claim_row())


def _script_revise_claim(
    conn: ScriptedConnection,
    *,
    task_row: tuple[Any, ...],
    producer_row: tuple[Any, ...],
    iteration_row: tuple[Any, ...] | None = None,
    loop_row: tuple[Any, ...] | None = None,
    loop_iterations: list[tuple[Any, ...]] | None = None,
    existing_claim: tuple[Any, ...] | None = None,
) -> None:
    """The planning-REVISE claim transaction's canned sequence, in call order.

    The locked Task re-read, the Producer binding reproof, the locked
    subject reproof (the routed iteration still addressing the current
    revision as its latest-numbered one), the exact-subject pre-check, and
    the committed claim row."""
    conn.on(_TASK_READ_LOCKED, task_row)
    conn.on(_role_is(_sql_has("from openorc.task_agent_sessions"), "producer"), producer_row)
    conn.on(
        _sql_has("from openorc.review_iterations where id"),
        iteration_row if iteration_row is not None else _iteration_row(),
    )
    conn.on(
        _sql_has("from openorc.review_loops where id"),
        loop_row if loop_row is not None else _loop_row(),
    )
    conn.on(
        _sql_has("review_iterations where review_loop_id"),
        loop_iterations if loop_iterations is not None else [_iteration_row()],
    )
    conn.on(_CLAIM_PRECHECK, existing_claim)
    conn.on(
        _CLAIM_INSERT,
        _claim_row(kind="planning_revise_dispatch", review_iteration_id=_ITERATION_ID),
    )


def _task_domain(
    *,
    status: TaskStatus = TaskStatus.PLANNING,
    state_token: Any = _STATE_TOKEN,
    current_plan_revision_id: Any = None,
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
    from openorc.domain.sessions import TaskSessionLifecycleStatus

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


def _preparation(
    *,
    state_token: Any = _STATE_TOKEN,
    producer: TaskAgentSession | None = None,
    reviewer: TaskAgentSession | None = None,
    task: Task | None = None,
) -> TaskPlanningPreparationOutcome:
    return TaskPlanningPreparationOutcome(
        task=task if task is not None else _task_domain(state_token=state_token),
        producer_session=(
            producer
            if producer is not None
            else _session_domain(
                WorkflowRole.PRODUCER,
                session_id=_PRODUCER_SESSION_ID,
                external_id=_PRODUCER_EXTERNAL_ID,
            )
        ),
        reviewer_session=(
            reviewer
            if reviewer is not None
            else _session_domain(
                WorkflowRole.REVIEWER,
                session_id=_REVIEWER_SESSION_ID,
                external_id=_REVIEWER_EXTERNAL_ID,
            )
        ),
        producer_checkout_directory=f"{_REPOSITORY_BASE}/{_TASK_ID}",
    )


class FakeGitHubClient:
    """Scripted GitHub observation fake recording call order; zero calls is
    assertable. Each call also proves no database transaction was open."""

    def __init__(
        self,
        pool: FakePool,
        *,
        observation: GitHubRepositoryObservation | None = None,
        branch_sha: str = _BASE_SHA,
        repository_error: Exception | None = None,
        branch_error: Exception | None = None,
    ) -> None:
        self._pool = pool
        self._observation = observation
        self._branch_sha = branch_sha
        self._repository_error = repository_error
        self._branch_error = branch_error
        self.repository_calls: list[tuple[int, int]] = []
        self.branch_calls: list[tuple[int, str, str, str]] = []

    def get_installation_repository(
        self, *, github_installation_id: int, github_repository_id: int
    ) -> GitHubRepositoryObservation:
        assert self._pool.active_connections == 0, (
            "the service must never hold a database transaction open across the GitHub call"
        )
        self.repository_calls.append((github_installation_id, github_repository_id))
        if self._repository_error is not None:
            raise self._repository_error
        assert self._observation is not None
        return self._observation

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
        if self._branch_error is not None:
            raise self._branch_error
        return GitHubBranchObservation(branch_name=branch_name, head_sha=self._branch_sha)


def _repository_observation(*, default_branch: str | None = "main") -> GitHubRepositoryObservation:
    return GitHubRepositoryObservation(
        github_repository_id=_GITHUB_REPOSITORY_ID,
        owner_login="octocat",
        name="hello-world",
        html_url="https://github.com/octocat/hello-world",
        is_private=False,
        default_branch=default_branch,
    )


def _prepared_producer_adapter(outcomes: Sequence[Any]) -> FakeAgentRuntimeAdapter:
    """A fake adapter holding one live Producer session, as E1b established it."""
    adapter = FakeAgentRuntimeAdapter()
    adapter.queue_creation(session_ready_candidate())
    created = adapter.create_session(
        AgentSessionCreationRequest(
            workspace_id=_WORKSPACE_ID,
            task_id=_TASK_ID,
            role=WorkflowRole.PRODUCER,
            initialization="prepared",
        )
    )
    assert created.external_session_id == _PRODUCER_EXTERNAL_ID
    for outcome in outcomes:
        adapter.queue_send(_PRODUCER_EXTERNAL_ID, outcome)
    return adapter


def _plan_candidate() -> dict[str, Any]:
    return {"type": "plan_result", "schema_version": 1, "plan": _PLAN_TEXT}


def _assert_no_consequential_write(conn: ScriptedConnection) -> None:
    assert not any(_REVISION_INSERT(sql, None) for sql, _ in conn.executed)
    assert not any(_POINTER_UPDATE(sql, None) for sql, _ in conn.executed)
    assert not any(_EVENT_INSERT(sql, None) for sql, _ in conn.executed)


def _revised_plan_candidate() -> dict[str, Any]:
    return {"type": "plan_result", "schema_version": 1, "plan": _REVISED_PLAN_TEXT}


def _production_ready_row() -> tuple[Any, ...]:
    return _session_row(
        "producer", session_id=_PRODUCER_SESSION_ID, external_id=_PRODUCER_EXTERNAL_ID
    )


def _review_ready_row() -> tuple[Any, ...]:
    return _session_row(
        "reviewer", session_id=_REVIEWER_SESSION_ID, external_id=_REVIEWER_EXTERNAL_ID
    )


def _script_account_and_workspace(conn: ScriptedConnection, *, guidance: str = "") -> None:
    # The account-operational barrier row: the Profile exists with no
    # deletion-attempt state (all three columns NULL).
    conn.on(_sql_has("from openorc.profiles", "for key share"), (None, None, None))
    conn.on_repeat(_sql_has("from openorc.workspaces where id"), _ws_row(guidance=guidance))


def _script_route(conn: ScriptedConnection) -> None:
    conn.on(_sql_has("from openorc.repositories where id"), _repository_row())
    conn.on(_sql_has("from openorc.github_installations where id"), _installation_row())


def _script_initial_entry(
    conn: ScriptedConnection,
    *,
    task_row: tuple[Any, ...],
    producer_row: tuple[Any, ...],
    reviewer_row: tuple[Any, ...],
    guidance: str = "",
) -> None:
    """The entry transaction's canned sequence, in call order."""
    _script_account_and_workspace(conn, guidance=guidance)
    conn.on(_TASK_READ, task_row)  # require_workspace_task's task read
    conn.on(_TASK_READ, task_row)  # require_current_task
    conn.on(_role_is(_sql_has("from openorc.task_agent_sessions"), "producer"), producer_row)
    conn.on(_role_is(_sql_has("from openorc.task_agent_sessions"), "reviewer"), reviewer_row)
    _script_route(conn)


def _script_initial_finalize(
    conn: ScriptedConnection,
    *,
    task_row: tuple[Any, ...],
    history: list[tuple[Any, ...]],
    revision_row: tuple[Any, ...],
    pointer_row: tuple[Any, ...],
    producer_row: tuple[Any, ...] | None = None,
    reviewer_row: tuple[Any, ...] | None = None,
) -> None:
    """The finalize transaction's canned sequence, in call order.

    The composition rechecks the exact Task subject and BOTH prepared
    session bindings before any write (session loss does not rotate the
    Task token, so the token alone cannot prove the sessions usable)."""
    conn.on(_TASK_READ, task_row)
    conn.on(
        _role_is(_sql_has("from openorc.task_agent_sessions"), "producer"),
        producer_row if producer_row is not None else _production_ready_row(),
    )
    conn.on(
        _role_is(_sql_has("from openorc.task_agent_sessions"), "reviewer"),
        reviewer_row if reviewer_row is not None else _review_ready_row(),
    )
    conn.on(_sql_has("from openorc.plan_revisions where task_id"), history)
    conn.on(_REVISION_INSERT, revision_row)
    conn.on(_POINTER_UPDATE, pointer_row)
    conn.on(_EVENT_INSERT, _event_row("plan_revision_created", revision_row[0]))
    # The F2 claim settles exactly once with the atomic resolution (issue #87).
    conn.on(_CLAIM_SETTLE, _claim_row(status="settled"))


def _script_revise_entry(
    conn: ScriptedConnection,
    *,
    task_row: tuple[Any, ...],
    revision_row: tuple[Any, ...],
    iteration_row: tuple[Any, ...],
    loop_row: tuple[Any, ...],
    producer_row: tuple[Any, ...],
    loop_iterations: list[tuple[Any, ...]] | None = None,
) -> None:
    """The revision entry transaction's canned sequence, in call order."""
    _script_account_and_workspace(conn)
    conn.on(_TASK_READ, task_row)  # require_workspace_task's task read
    conn.on(_TASK_READ, task_row)  # require_current_task
    conn.on(_sql_has("from openorc.plan_revisions where id"), revision_row)
    conn.on(_sql_has("from openorc.review_iterations where id"), iteration_row)
    conn.on(_sql_has("from openorc.review_loops where id"), loop_row)
    # The #207 supersession guard: the loop's ordered iterations decide
    # whether the named change request is still the latest one on the plan.
    conn.on(
        _sql_has("review_iterations where review_loop_id"),
        loop_iterations if loop_iterations is not None else [iteration_row],
    )
    conn.on(_role_is(_sql_has("from openorc.task_agent_sessions"), "producer"), producer_row)
    _script_route(conn)


def _script_revise_finalize(
    conn: ScriptedConnection,
    *,
    task_row: tuple[Any, ...],
    prior_row: tuple[Any, ...],
    history: list[tuple[Any, ...]],
    revision_row: tuple[Any, ...],
    pointer_row: tuple[Any, ...],
    producer_row: tuple[Any, ...] | None = None,
    loop_row: tuple[Any, ...] | None = None,
    loop_iterations: list[tuple[Any, ...]] | None = None,
) -> None:
    """The revision finalize transaction's canned sequence, in call order.

    The composition rechecks the exact Task subject, the exact prepared
    Producer binding, the current prior revision, and — issue #207 — the
    routed change request's supersession state (the loop row re-read locked,
    then its ordered iterations) before any write."""
    conn.on(_TASK_READ, task_row)
    conn.on(
        _role_is(_sql_has("from openorc.task_agent_sessions"), "producer"),
        producer_row if producer_row is not None else _production_ready_row(),
    )
    conn.on(_sql_has("from openorc.plan_revisions where id"), prior_row)
    conn.on(
        _sql_has("from openorc.review_loops where id", "for update"),
        loop_row if loop_row is not None else _loop_row(),
    )
    conn.on(
        _sql_has("review_iterations where review_loop_id"),
        loop_iterations if loop_iterations is not None else [_iteration_row()],
    )
    conn.on(_sql_has("from openorc.plan_revisions where task_id"), history)
    conn.on(_REVISION_INSERT, revision_row)
    conn.on(_POINTER_UPDATE, pointer_row)
    conn.on(_EVENT_INSERT, _event_row("plan_revision_revised", revision_row[0]))
    # The F2 claim settles exactly once with the atomic resolution (issue #87).
    conn.on(
        _CLAIM_SETTLE,
        _claim_row(
            status="settled",
            kind="planning_revise_dispatch",
            review_iteration_id=_ITERATION_ID,
        ),
    )


_UNSET_PREPARATION = object()


class _Production:
    """The scripted-connection environment one plan-production test uses.

    Since #347 the blocking produce/revise entries are retired; the live
    detached dispatch/settlement tests reuse only this fixture's scripted
    connection and fake GitHub wiring.
    """

    def __init__(
        self,
        *,
        observation: GitHubRepositoryObservation | None = None,
        repository_error: Exception | None = None,
    ) -> None:
        self.conn = ScriptedConnection()
        self.pool = FakePool(self.conn)
        self.github_fake = FakeGitHubClient(
            self.pool,
            observation=observation if observation is not None else _repository_observation(),
            repository_error=repository_error,
        )
        self.github: GitHubAppClient = cast(GitHubAppClient, self.github_fake)


# --- the detached plan-result dispatch and settlement (issues #325/#335) -------


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
    operation: str = "initial_plan_dispatch",
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
        _PRODUCER_SESSION_ID,
        operation,
        "runtime_send_claim",
        _CLAIM_ID,
        "plan_result",
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
    """Records the ONE post-commit completion handoff invocations (issue #335)."""

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


class _ProducerAttachment:
    """The zero-arg producer attachment factory's yielded lease."""

    def __init__(self, adapter: FakeAgentRuntimeAdapter) -> None:
        self._adapter = adapter
        self.acquisitions = 0

    def __call__(self) -> Any:
        @contextmanager
        def lease() -> Iterator[FakeAgentRuntimeAdapter]:
            self.acquisitions += 1
            try:
                yield self._adapter
            finally:
                self.acquisitions -= 1

        return lease()


def _detached_initial_runtime() -> FakeAgentRuntimeAdapter:
    """A fake adapter holding the live prepared Producer session, admission-ready."""
    return _prepared_producer_adapter([])


def _dispatch_initial_call(
    pool: DatabasePool,
    github: GitHubAppClient,
    runtime: FakeAgentRuntimeAdapter,
    *,
    registrar: _RecordingRegistrar,
    handoff: _RecordingHandoff,
    preparation: Any = _UNSET_PREPARATION,
) -> Any:
    return dispatch_initial_plan_detached(
        cast(DatabasePool, pool),
        github,
        workspace_id=_WORKSPACE_ID,
        task_id=_TASK_ID,
        profile_id=_OWNER_PROFILE_ID,
        preparation=(preparation if preparation is not _UNSET_PREPARATION else _preparation()),
        producer_runtime_factory=_ProducerAttachment(runtime),
        observation_registrar=registrar,  # type: ignore[arg-type]
        completion_handoff=handoff,  # type: ignore[arg-type]
    )


def _dispatch_revise_call(
    pool: DatabasePool,
    github: GitHubAppClient,
    runtime: FakeAgentRuntimeAdapter,
    *,
    registrar: _RecordingRegistrar,
    handoff: _RecordingHandoff,
) -> Any:
    return dispatch_planning_revise_detached(
        cast(DatabasePool, pool),
        github,
        workspace_id=_WORKSPACE_ID,
        task_id=_TASK_ID,
        profile_id=_OWNER_PROFILE_ID,
        expected_state_token=_STATE_TOKEN,
        prior_plan_revision_id=_REVISION_1_ID,
        review_iteration_id=_ITERATION_ID,
        producer_runtime_factory=_ProducerAttachment(runtime),
        observation_registrar=registrar,  # type: ignore[arg-type]
        completion_handoff=handoff,  # type: ignore[arg-type]
    )


def _known_nondelivery_update(sql: str, params: tuple[Any, ...] | None) -> bool:
    lowered = " ".join(sql.split()).lower()
    return (
        "update openorc.task_runtime_send_claims" in lowered
        and params is not None
        and len(params) > 0
        and params[0] == "known_nondelivery"
    )


def _script_claim_and_receipt(
    conn: ScriptedConnection,
    *,
    task_row: tuple[Any, ...],
    producer_row: tuple[Any, ...],
    existing_claim: tuple[Any, ...] | None = None,
    claim_row: tuple[Any, ...] | None = None,
    receipt_row: tuple[Any, ...] | None = None,
    iteration_row: tuple[Any, ...] | None = None,
    loop_row: tuple[Any, ...] | None = None,
    loop_iterations: list[tuple[Any, ...]] | None = None,
) -> None:
    """The claim+receipt dispatch transaction's canned sequence, in call order.

    The locked Task re-read, the Producer binding reproof, the per-kind
    subject reproof (the routed iteration for the REVISE kind), the
    exact-subject pre-check (``None`` means unclaimed), the committed claim
    row, and the committed detached-turn receipt companion."""
    conn.on(_TASK_READ_LOCKED, task_row)
    conn.on(_role_is(_sql_has("from openorc.task_agent_sessions"), "producer"), producer_row)
    if iteration_row is not None:
        conn.on(_sql_has("from openorc.review_iterations where id"), iteration_row)
        conn.on(
            _sql_has("from openorc.review_loops where id"),
            loop_row if loop_row is not None else _loop_row(),
        )
        conn.on(
            _sql_has("review_iterations where review_loop_id"),
            loop_iterations if loop_iterations is not None else [iteration_row],
        )
    conn.on(_CLAIM_PRECHECK, existing_claim)
    conn.on(_CLAIM_INSERT, claim_row if claim_row is not None else _claim_row())
    conn.on(_RECEIPT_INSERT, receipt_row if receipt_row is not None else _detached_receipt_row())


def _script_attach(conn: ScriptedConnection, *, staged: Any = None) -> None:
    """The acknowledgement attach (claimed → admitted, staged or plain)."""
    conn.on(_RECEIPT_FOR_UPDATE, _detached_receipt_row())
    conn.on(
        _RECEIPT_ATTACH,
        _detached_receipt_row(status="admitted", admitted_turn_reference=_TURN_REFERENCE),
    )
    conn.on(_EARLY_COMPLETION_READ, staged)


def _claim_insert_params(conn: ScriptedConnection) -> tuple[Any, ...]:
    for sql, params in conn.executed:
        if _CLAIM_INSERT(sql, None):
            assert params is not None
            return params
    raise AssertionError("no send-claim insert executed")


def _receipt_insert_params(conn: ScriptedConnection) -> tuple[Any, ...]:
    for sql, params in conn.executed:
        if _RECEIPT_INSERT(sql, None):
            assert params is not None
            return params
    raise AssertionError("no detached-turn receipt insert executed")


def test_detached_initial_plan_dispatch_claims_records_registers_admits_and_attaches() -> None:
    """The regular-mode initial PLAN is one admitted detached turn: the one-shot
    claim (with its observed base context) and the ``plan_result`` receipt
    commit together strictly before the positive observation registration
    and the verbatim admission, and the dispatching call returns with the
    claim conservatively claimed and no blocking send."""
    env = _Production()
    _script_initial_entry(
        env.conn,
        task_row=_task_row(),
        producer_row=_production_ready_row(),
        reviewer_row=_review_ready_row(),
    )
    _script_claim_and_receipt(env.conn, task_row=_task_row(), producer_row=_production_ready_row())
    _script_attach(env.conn)
    env.conn.on(_TASK_READ, _task_row())  # the post-dispatch reload
    runtime = _detached_initial_runtime()
    registrar = _RecordingRegistrar()
    handoff = _RecordingHandoff()

    outcome = _dispatch_initial_call(
        env.pool, env.github, runtime, registrar=registrar, handoff=handoff
    )

    assert isinstance(outcome, DispatchedPlanProduction)
    assert outcome.send_claim.id == _CLAIM_ID
    assert outcome.receipt.id == _RECEIPT_ID
    assert outcome.producer_session.id == _PRODUCER_SESSION_ID
    # The claim records the dispatch-observed repository-base context.
    claim_params = _claim_insert_params(env.conn)
    assert claim_params[-1] == _BASE_SHA
    # The receipt companion expects the canonical plan_result family and
    # roots at the runtime-send-claim identity.
    receipt_params = _receipt_insert_params(env.conn)
    assert receipt_params[3] == "initial_plan_dispatch"
    assert receipt_params[4] == "runtime_send_claim"
    assert receipt_params[6] == "plan_result"
    # One positive observation registration on the exact bound session.
    assert registrar.registrations == [
        {
            "workspace_id": _WORKSPACE_ID,
            "task_id": _TASK_ID,
            "task_agent_session_id": _PRODUCER_SESSION_ID,
            "connection_id": _CONNECTION_ID,
            "external_session_id": _PRODUCER_EXTERNAL_ID,
        }
    ]
    # The canonical PLAN instruction was admitted verbatim — the exact
    # message the blocking send composes — and no blocking send ran.
    assert runtime.detached_admissions() == (
        (
            _PRODUCER_EXTERNAL_ID,
            PlanInteraction(
                repository_full_name=_FULL_NAME,
                issue_number=42,
                guidance="",
            ).render(),
        ),
    )
    assert runtime.submitted_sends(_PRODUCER_EXTERNAL_ID) == ()
    # Exactly one acknowledged admission attached; no handoff fired (the
    # staged-early path is a separate case).
    assert handoff.calls == []
    assert env.pool.active_connections == 0


def test_detached_initial_plan_registration_refusal_refuses_the_receipt_and_fails_known() -> None:
    """A refused observation registration is a proven pre-effect failure: the
    receipt is refused, the claim records its known non-delivery, and no
    admission is ever attempted."""
    env = _Production()
    _script_initial_entry(
        env.conn,
        task_row=_task_row(),
        producer_row=_production_ready_row(),
        reviewer_row=_review_ready_row(),
    )
    _script_claim_and_receipt(env.conn, task_row=_task_row(), producer_row=_production_ready_row())
    env.conn.on(_RECEIPT_REFUSE, _detached_receipt_row(status="refused"))
    env.conn.on(_known_nondelivery_update, _claim_row(status="known_nondelivery"))
    runtime = _detached_initial_runtime()
    registrar = _RecordingRegistrar(
        error=ObservationRegistrationRefusedError(
            "refused", reason=ObservationRegistrationRefusalReason.OBSERVER_UNAVAILABLE
        )
    )
    handoff = _RecordingHandoff()

    with pytest.raises(ExternalOperationFailedError):
        _dispatch_initial_call(env.pool, env.github, runtime, registrar=registrar, handoff=handoff)

    assert any(_RECEIPT_REFUSE(sql, None) for sql, _ in env.conn.executed)
    assert any(_known_nondelivery_update(sql, params) for sql, params in env.conn.executed)
    # The refused dispatch never admitted a turn.
    assert runtime.detached_admissions() == ()
    assert handoff.calls == []


def test_detached_initial_plan_admission_refusal_refuses_the_receipt_and_fails_known() -> None:
    """A positively identified admission refusal is a proven pre-effect
    failure: the receipt is refused, the claim records its known
    non-delivery, and the typed rejection propagates."""
    env = _Production()
    _script_initial_entry(
        env.conn,
        task_row=_task_row(),
        producer_row=_production_ready_row(),
        reviewer_row=_review_ready_row(),
    )
    _script_claim_and_receipt(env.conn, task_row=_task_row(), producer_row=_production_ready_row())
    env.conn.on(_RECEIPT_REFUSE, _detached_receipt_row(status="refused"))
    env.conn.on(_known_nondelivery_update, _claim_row(status="known_nondelivery"))
    runtime = _detached_initial_runtime()
    runtime.queue_detached_admission(AgentRuntimeUnavailableError("the hub is unreachable"))
    registrar = _RecordingRegistrar()
    handoff = _RecordingHandoff()

    with pytest.raises(ExternalOperationRejectedError):
        _dispatch_initial_call(env.pool, env.github, runtime, registrar=registrar, handoff=handoff)

    assert any(_RECEIPT_REFUSE(sql, None) for sql, _ in env.conn.executed)
    assert any(_known_nondelivery_update(sql, params) for sql, params in env.conn.executed)
    # The registration happened (it precedes the admission) and the
    # observation never settled anything.
    assert len(registrar.registrations) == 1
    assert handoff.calls == []


def test_detached_initial_plan_uncertain_admission_leaves_claim_and_receipt_claimed() -> None:
    """An uncertain admission is never a proven non-delivery: the claim and
    the receipt stay conservatively claimed and the instruction is never
    re-dispatched."""
    env = _Production()
    _script_initial_entry(
        env.conn,
        task_row=_task_row(),
        producer_row=_production_ready_row(),
        reviewer_row=_review_ready_row(),
    )
    _script_claim_and_receipt(env.conn, task_row=_task_row(), producer_row=_production_ready_row())
    runtime = _detached_initial_runtime()
    runtime.queue_detached_admission(
        AgentRuntimeDeliveryUncertainError("the admission outcome is unknown")
    )
    registrar = _RecordingRegistrar()
    handoff = _RecordingHandoff()

    with pytest.raises(ExternalOperationUncertainError):
        _dispatch_initial_call(env.pool, env.github, runtime, registrar=registrar, handoff=handoff)

    assert not any(_RECEIPT_REFUSE(sql, None) for sql, _ in env.conn.executed)
    assert not any(
        _sql_has("update openorc.task_runtime_send_claims", "known_nondelivery")(sql, None)
        for sql, _ in env.conn.executed
    )
    assert handoff.calls == []


def test_a_claimed_initial_plan_subject_never_dispatches_again() -> None:
    """A replayed or raced continuation against an already-claimed subject
    fails closed before any receipt or runtime effect: the one-shot claim is
    never re-dispatched."""
    env = _Production()
    _script_initial_entry(
        env.conn,
        task_row=_task_row(),
        producer_row=_production_ready_row(),
        reviewer_row=_review_ready_row(),
    )
    env.conn.on(_TASK_READ_LOCKED, _task_row())
    env.conn.on(
        _role_is(_sql_has("from openorc.task_agent_sessions"), "producer"),
        _production_ready_row(),
    )
    env.conn.on(_CLAIM_PRECHECK, _claim_row())
    runtime = _detached_initial_runtime()
    registrar = _RecordingRegistrar()
    handoff = _RecordingHandoff()

    with pytest.raises(StaleOperationError):
        _dispatch_initial_call(env.pool, env.github, runtime, registrar=registrar, handoff=handoff)

    assert not any(_RECEIPT_INSERT(sql, None) for sql, _ in env.conn.executed)
    assert runtime.detached_admissions() == ()
    assert registrar.registrations == []


def test_detached_revise_dispatch_routes_the_persisted_findings_verbatim() -> None:
    """The routed REVISE is one admitted detached turn whose instruction is
    the persisted finalized change request rebuilt verbatim from durable
    state — never an in-memory continuation object."""
    env = _Production()
    current_row = _task_row(current_plan_revision_id=_REVISION_1_ID)
    iteration_row = _iteration_row()
    _script_revise_entry(
        env.conn,
        task_row=current_row,
        revision_row=_revision_row(revision_id=_REVISION_1_ID, number=1),
        iteration_row=iteration_row,
        loop_row=_loop_row(),
        producer_row=_production_ready_row(),
    )
    _script_claim_and_receipt(
        env.conn,
        task_row=current_row,
        producer_row=_production_ready_row(),
        claim_row=_claim_row(kind="planning_revise_dispatch", review_iteration_id=_ITERATION_ID),
        receipt_row=_detached_receipt_row(operation="planning_revise_dispatch"),
        iteration_row=iteration_row,
    )
    _script_attach(env.conn)
    env.conn.on(_TASK_READ, current_row)  # the post-dispatch reload
    runtime = _detached_initial_runtime()
    registrar = _RecordingRegistrar()
    handoff = _RecordingHandoff()

    outcome = _dispatch_revise_call(
        env.pool, env.github, runtime, registrar=registrar, handoff=handoff
    )

    assert isinstance(outcome, DispatchedPlanProduction)
    # The receipt companion addresses the exact routed change request.
    receipt_params = _receipt_insert_params(env.conn)
    assert receipt_params[3] == "planning_revise_dispatch"
    assert receipt_params[5] == _CLAIM_ID
    assert receipt_params[6] == "plan_result"
    # The persisted findings were serialized verbatim as the admission.
    assert runtime.detached_admissions() == ((_PRODUCER_EXTERNAL_ID, _REVIEW_RESULT_SERIALIZED),)
    assert runtime.submitted_sends(_PRODUCER_EXTERNAL_ID) == ()
    assert handoff.calls == []


def _settlement_claim(
    *,
    kind: TaskRuntimeSendClaimKind,
    review_iteration_id: Any = None,
    dispatch_repository_base_sha: Any = _BASE_SHA,
) -> TaskRuntimeSendClaim:
    return TaskRuntimeSendClaim(
        id=_CLAIM_ID,
        workspace_id=_WORKSPACE_ID,
        task_id=_TASK_ID,
        kind=kind,
        task_agent_session_id=_PRODUCER_SESSION_ID,
        review_iteration_id=review_iteration_id,
        owner_gate_id=None,
        subject_head_sha=None,
        dispatch_repository_base_sha=dispatch_repository_base_sha,
        status=TaskRuntimeSendClaimStatus.CLAIMED,
        outcome={},
        claimed_at=_CLAIM_AT,
        settled_at=None,
        updated_at=_CLAIM_AT,
    )


def test_settlement_of_the_observed_initial_plan_result_creates_revision_one_atomically() -> None:
    """The owning-service settlement re-derives every subject from durable
    state and reuses the one shared commit composition: the immutable
    revision from the claim's recorded base context, the pointer/token
    rotation, the event, and the send-claim settlement."""
    conn = ScriptedConnection()
    conn.on(_TASK_READ, _task_row())
    conn.on(
        _role_is(_sql_has("from openorc.task_agent_sessions"), "producer"),
        _production_ready_row(),
    )
    conn.on(
        _role_is(_sql_has("from openorc.task_agent_sessions"), "reviewer"),
        _review_ready_row(),
    )
    _script_initial_finalize(
        conn,
        task_row=_task_row(),
        history=[],
        revision_row=_revision_row(revision_id=_REVISION_1_ID, number=1),
        pointer_row=_updated_task_row(pointer=_REVISION_1_ID),
    )
    claim = _settlement_claim(kind=TaskRuntimeSendClaimKind.INITIAL_PLAN_DISPATCH)

    outcome = settle_observed_plan_result(
        cast(DatabasePool, FakePool(conn)),
        workspace_id=_WORKSPACE_ID,
        task_id=_TASK_ID,
        send_claim=claim,
        result=PlanResult(plan=_PLAN_TEXT),
    )

    assert isinstance(outcome, TaskPlanProductionOutcome)
    assert outcome.plan_revision.repository_base_sha == _BASE_SHA
    assert outcome.plan_revision.content == _PLAN_TEXT
    assert outcome.task.state_token == _NEXT_TOKEN
    # The send claim settled exactly once with the atomic resolution.
    assert any(_CLAIM_SETTLE(sql, None) for sql, _ in conn.executed)


def test_settlement_of_a_moved_on_subject_never_publishes_a_revision() -> None:
    """A validated result whose subject moved on (the Task left its coarse
    planning state) can no longer apply: nothing is rewritten and the caller
    settles the receipt as result-not-applied."""
    conn = ScriptedConnection()
    conn.on(_TASK_READ, _task_row(status="implementing"))
    claim = _settlement_claim(kind=TaskRuntimeSendClaimKind.INITIAL_PLAN_DISPATCH)

    outcome = settle_observed_plan_result(
        cast(DatabasePool, FakePool(conn)),
        workspace_id=_WORKSPACE_ID,
        task_id=_TASK_ID,
        send_claim=claim,
        result=PlanResult(plan=_PLAN_TEXT),
    )

    assert outcome is None
    _assert_no_consequential_write(conn)
    assert not any(_CLAIM_SETTLE(sql, None) for sql, _ in conn.executed)


def test_settlement_of_a_claim_without_its_dispatch_base_fails_closed() -> None:
    """A claimed plan dispatch without its recorded repository-base context
    is an impossible durable state for any attributable receipt: the
    settlement fails closed instead of fabricating audit context."""
    conn = ScriptedConnection()
    claim = _settlement_claim(
        kind=TaskRuntimeSendClaimKind.INITIAL_PLAN_DISPATCH, dispatch_repository_base_sha=None
    )

    with pytest.raises(InvalidCommandError):
        settle_observed_plan_result(
            cast(DatabasePool, FakePool(conn)),
            workspace_id=_WORKSPACE_ID,
            task_id=_TASK_ID,
            send_claim=claim,
            result=PlanResult(plan=_PLAN_TEXT),
        )

    _assert_no_consequential_write(conn)
