"""E4 service seam tests: Owner ↔ Reviewer discussion across gates (issue #79).

A scripted SQL-connection seam plus the #69 fake Agent Runtime and deferred
attachment probes driving the real service composition. The suite proves:

- ordinary advisory discussion at every pending gate type (verbatim message,
  ordinary reply, exactly two direction-only audit events, no settlement, no
  session/Producer interaction, and no PR invented for the pre-PR gate);
- rejection before any attachment or send for unauthorized, stale, foreign,
  non-WAITING, archived, and unavailable-Reviewer requests;
- the strict standalone formal-result recognition at REVIEW_RESOLUTION only
  (prose/buried/malformed/other-family JSON stays ordinary; one standalone
  valid ``review_result`` routes to the merged E3c planning settlement —
  representative ACCEPTED and CHANGES_REQUESTED paths — while PR/head
  subjects fail closed until E9);
- atomic coordination of the discussion audit with settlement (a failed
  discussion-event write prevents the E2 continuation and reports no
  success), and that a raced resolution is stale advisory output;
- attachment ordering (acquisition strictly after authorization, disposal
  on every path, lazy Producer attachment on the formal continuation only);
- and — via the send probe in every happy path — that no database
  transaction spans the external send.
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
    AgentRuntimeTimeoutError,
    AgentRuntimeUnavailableError,
)
from openorc.adapters.agent_runtime.results import AgentSessionCreationRequest
from openorc.adapters.github import (
    GitHubBranchObservation,
    GitHubRepositoryObservation,
)
from openorc.domain.connections import WorkflowRole
from openorc.domain.gates import OwnerGateStatus
from openorc.domain.reviews import ReviewOutcome
from openorc.domain.tasks import TaskStatus
from openorc.persistence.pool import DatabasePool
from openorc.services.errors import (
    ConflictError,
    ExternalOperationFailedError,
    ExternalOperationUncertainError,
    InvalidCommandError,
    NotFoundError,
    StaleOperationError,
)
from openorc.services.task_owner_reviewer_discussion import (
    OwnerReviewerDiscussionExchange,
    send_owner_reviewer_discussion_message,
)
from openorc.services.task_planning_review_resolution import (
    PlanningReviewResolutionDecision,
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
_REPOSITORY_ID = uuid.uuid4()
_INSTALLATION_ID = uuid.uuid4()
_EXTERNAL_INSTALLATION_ID = 12345678
_CONNECTION_ID = uuid.uuid4()
_PRODUCER_SESSION_ID = uuid.uuid4()
_REVIEWER_SESSION_ID = uuid.uuid4()
_PRODUCER_EXTERNAL_ID = "fake-session-1"
_REVIEWER_EXTERNAL_ID = "fake-session-2"
_STATE_TOKEN = uuid.uuid4()
_TOKEN_2 = uuid.uuid4()
_TOKEN_3 = uuid.uuid4()
_TOKEN_4 = uuid.uuid4()
_REVISION_1_ID = uuid.uuid4()
_REVISION_2_ID = uuid.uuid4()
_LOOP_ID = uuid.uuid4()
_ITERATION_1_ID = uuid.uuid4()
_ITERATION_2_ID = uuid.uuid4()
_GATE_1_ID = uuid.uuid4()
_GATE_2_ID = uuid.uuid4()
_PR_ID = uuid.uuid4()
_BASE_SHA = "c" * 40
_HEAD_SHA = "b" * 40
_OTHER_SHA = "d" * 40
_PLAN_TEXT = "# Plan\n\nStep one.\n"
_REVISED_PLAN_TEXT = "# Plan (revised)\n\nStep one, clarified.\n"
_EXHAUSTED_FINDINGS = [{"summary": "Clarify scope", "details": "Say which module changes."}]
_NEW_FINDINGS = [{"summary": "Clarify the retry loop", "details": "Explain the backoff path."}]
_OWNER_MESSAGE = "Owner question: is the retry backoff sufficient for the failure mode?"
_ORDINARY_REPLY = "The plan looks structurally sound; the backoff covers the failure mode."

_DISCUSSION_EVENT_TYPE = "owner_reviewer_discussion_message"

_CannedResult = tuple[Any, ...] | list[tuple[Any, ...]] | None | Exception
_Predicate = Callable[[str, tuple[Any, ...] | None], bool]


def _sql_has(*tokens: str) -> _Predicate:
    def predicate(sql: str, _params: tuple[Any, ...] | None) -> bool:
        return all(token in sql for token in tokens)

    return predicate


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

    def connection(self) -> Any:  # noqa: ANN401 - test seam
        @contextmanager
        def managed() -> Iterator[ScriptedConnection]:
            self.active_connections += 1
            try:
                yield self._conn
            finally:
                self.active_connections -= 1

        return managed()

    def close(self) -> None:
        raise AssertionError("owner-reviewer discussion tests never close pools")


def _pool(conn: ScriptedConnection) -> DatabasePool:
    return cast(DatabasePool, FakePool(conn))


def _sql_lacks(predicate: _Predicate, *tokens: str) -> _Predicate:
    def wrapped(sql: str, params: tuple[Any, ...] | None) -> bool:
        return predicate(sql, params) and not any(token in sql for token in tokens)

    return wrapped


_TASK_READ = _sql_lacks(_sql_has("from openorc.tasks where id"), "for update")
_TASK_READ_LOCKED = _sql_has("from openorc.tasks where id", "for update")
_TASK_GATE_WRITE = _sql_has("update openorc.tasks", "current_owner_gate_id")
_STATUS_UPDATE = _sql_has("update openorc.tasks", "queued_at = case")
_POINTER_UPDATE = _sql_has("update openorc.tasks", "current_plan_revision_id")
_REVISION_READ = _sql_has("from openorc.plan_revisions where id")
_REVISIONS_HISTORY = _sql_has("from openorc.plan_revisions where task_id")
_REVISION_INSERT = _sql_has("insert into openorc.plan_revisions")
_SESSION_READ = _sql_has("from openorc.task_agent_sessions")
_PR_READ = _sql_has("from openorc.task_pull_requests where task_id")
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
_GATE_READ = _sql_has("from openorc.owner_gates where id")
_GATE_INSERT = _sql_has("insert into openorc.owner_gates")
_GATE_UPDATE = _sql_has("update openorc.owner_gates")
_EVENT_INSERT = _sql_has("insert into openorc.workflow_events")


def _discussion_event_insert(sql: str, params: tuple[Any, ...] | None) -> bool:
    """Match exactly the discussion-message event inserts (for failure seams)."""
    return _EVENT_INSERT(sql, None) and params is not None and params[2] == _DISCUSSION_EVENT_TYPE


def _ws_row(*, owner: Any = _OWNER_PROFILE_ID) -> tuple[Any, ...]:
    return (_WORKSPACE_ID, owner, "platform", _OBSERVED, _OBSERVED, 2, "")


def _task_row(
    *,
    status: str = "waiting_for_owner",
    state_token: Any = _STATE_TOKEN,
    current_plan_revision_id: Any = _REVISION_1_ID,
    gate_id: Any = _GATE_1_ID,
    archived_at: Any = None,
) -> tuple[Any, ...]:
    return (
        _TASK_ID,
        _WORKSPACE_ID,
        _REPOSITORY_ID,
        503,
        42,
        status,
        archived_at,
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
    lifecycle: str | None = None,
) -> tuple[Any, ...]:
    initialized = external_id is not None
    resolved_lifecycle = (
        lifecycle if lifecycle is not None else ("ready" if initialized else "connecting")
    )
    return (
        session_id,
        _WORKSPACE_ID,
        _TASK_ID,
        role,
        _CONNECTION_ID,
        external_id,
        resolved_lifecycle,
        {"adapter": "cline"} if initialized else None,
        None,
        None,
        None,
        _OBSERVED if initialized else None,
        _OBSERVED if resolved_lifecycle == "ended" else None,
        _OBSERVED,
        _OBSERVED,
    )


def _revision_row(*, revision_id: Any, number: int, content: str = _PLAN_TEXT) -> tuple[Any, ...]:
    return (revision_id, _WORKSPACE_ID, _TASK_ID, number, content, _BASE_SHA, _OBSERVED)


def _pr_row(*, head_sha: str = _HEAD_SHA) -> tuple[Any, ...]:
    return (
        _PR_ID,
        _WORKSPACE_ID,
        _TASK_ID,
        _REPOSITORY_ID,
        555001,
        7,
        "feature/issue-42",
        "main",
        head_sha,
        "open",
        None,
        _OBSERVED,
        _OBSERVED,
    )


def _gate_row(
    *,
    gate_id: Any = _GATE_1_ID,
    gate_type: str = "review_resolution",
    plan_revision_id: Any | None = _REVISION_1_ID,
    status: str = "pending",
    head_sha: str | None = None,
    pr_id: Any | None = None,
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


def _loop_row(*, status: str = "open", limit: int = 2, closed_at: Any = None) -> tuple[Any, ...]:
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


def _exhausted_iteration_row() -> tuple[Any, ...]:
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


def _repository_row() -> tuple[Any, ...]:
    return (
        _REPOSITORY_ID,
        uuid.uuid4(),
        _WORKSPACE_ID,
        987654321,
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


def _assert_no_discussion_text_in_sql(conn: ScriptedConnection, *secrets: str) -> None:
    """No executed SQL ever carries the Owner message or the reply text."""
    for sql, params in conn.executed:
        blob = sql + repr(params)
        for secret in secrets:
            assert secret not in blob


class _ProbedSendAdapter(FakeAgentRuntimeAdapter):
    """Fake runtime proving every send runs with no transaction open."""

    def __init__(self, pool: FakePool) -> None:
        super().__init__()
        self._discussion_pool = pool

    def send(
        self,
        session_id: str,
        message: str,
        *,
        expected_family: str | None = None,
    ) -> Any:  # noqa: ANN401 - mirrors the #67 contract return
        assert self._discussion_pool.active_connections == 0, (
            "the service must never hold a database transaction open across the send"
        )
        return super().send(session_id, message, expected_family=expected_family)


class _ReviewerAttachment:
    """Deferred Reviewer attachment probe: counts acquisitions and disposals."""

    def __init__(
        self,
        runtime: FakeAgentRuntimeAdapter,
        *,
        on_acquire: Callable[[], None] | None = None,
    ) -> None:
        self._runtime = runtime
        self._on_acquire = on_acquire
        self.acquisitions = 0
        self.disposals = 0
        self.bindings: list[Any] = []

    def __call__(self, binding: Any) -> AbstractContextManager[FakeAgentRuntimeAdapter]:
        self.bindings.append(binding)
        self.acquisitions += 1
        if self._on_acquire is not None:
            self._on_acquire()
        return self

    def __enter__(self) -> FakeAgentRuntimeAdapter:
        return self._runtime

    def __exit__(self, *_exc: object) -> bool:
        self.disposals += 1
        return False


class _ProducerAttachment:
    """Deferred Producer attachment probe for the formal continuation path."""

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
    reviewer_outcomes: Sequence[Any],
    producer_outcomes: Sequence[Any] = (),
) -> _ProbedSendAdapter:
    """One fake runtime holding the live Producer and Reviewer sessions."""
    adapter = _ProbedSendAdapter(pool)
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
            github_repository_id=987654321,
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


_REVIEWER_READY_ROW = _session_row(
    "reviewer", session_id=_REVIEWER_SESSION_ID, external_id=_REVIEWER_EXTERNAL_ID
)
_USE_SESSION_REPEAT = object()


def _script_common(
    conn: ScriptedConnection,
    *,
    session_repeats: bool = True,
    ws_row: tuple[Any, ...] | None = None,
) -> None:
    """Invariant reads served across the whole flow, in registration order."""
    conn.on_repeat(_sql_has("from openorc.profiles", "for key share"), (None, None, None))
    conn.on_repeat(_sql_has("from openorc.workspaces where id"), ws_row or _ws_row())
    conn.on_repeat(_EVENT_INSERT, _event_row())
    if session_repeats:
        conn.on_repeat(_role_is(_SESSION_READ, "reviewer"), _REVIEWER_READY_ROW)
        conn.on_repeat(
            _role_is(_SESSION_READ, "producer"),
            _session_row(
                "producer", session_id=_PRODUCER_SESSION_ID, external_id=_PRODUCER_EXTERNAL_ID
            ),
        )


def _currentness_round(
    conn: ScriptedConnection,
    *,
    task_row: tuple[Any, ...],
    gate_row: tuple[Any, ...],
    revision_row: tuple[Any, ...] | None = None,
    pr_row: tuple[Any, ...] | None = None,
    session_row: Any = _USE_SESSION_REPEAT,
    locked_task: bool = False,
) -> None:
    """One ``_revalidate_currentness`` round: task, gate, subject, session.

    ``locked_task`` scripts the post-send round's Task-row ``FOR UPDATE``
    read, which the service takes before rechecking currentness so the
    still-current decision serializes against concurrent Task mutations.
    """
    if locked_task:
        conn.on(_TASK_READ_LOCKED, task_row)
    conn.on(_TASK_READ, task_row)
    conn.on(_GATE_READ, gate_row)
    if revision_row is not None:
        conn.on(_REVISION_READ, revision_row)
    if pr_row is not None:
        conn.on(_PR_READ, pr_row)
    if session_row is not _USE_SESSION_REPEAT:
        conn.on(_role_is(_SESSION_READ, "reviewer"), session_row)


def _phase1(
    conn: ScriptedConnection,
    *,
    task_row: tuple[Any, ...],
    gate_row: tuple[Any, ...],
    revision_row: tuple[Any, ...] | None = None,
    pr_row: tuple[Any, ...] | None = None,
    session_row: Any = _USE_SESSION_REPEAT,
) -> None:
    """Phase 1: the workspace authorization's Task read plus the currentness round."""
    conn.on(_TASK_READ, task_row)
    _currentness_round(
        conn,
        task_row=task_row,
        gate_row=gate_row,
        revision_row=revision_row,
        pr_row=pr_row,
        session_row=session_row,
    )


def _script_discussion_flow(
    conn: ScriptedConnection,
    *,
    task_row: tuple[Any, ...] | None = None,
    gate_row: tuple[Any, ...] | None = None,
    revision_row: tuple[Any, ...] | None = None,
    pr_row: tuple[Any, ...] | None = None,
    session_repeats: bool = True,
    ws_row: tuple[Any, ...] | None = None,
) -> None:
    """Phase 1 + pre-send recheck + post-send recheck over constant state."""
    _script_common(conn, session_repeats=session_repeats, ws_row=ws_row)
    flow_task = task_row if task_row is not None else _task_row()
    flow_gate = gate_row if gate_row is not None else _gate_row()
    for round_index in range(3):
        if round_index == 0:
            _phase1(
                conn,
                task_row=flow_task,
                gate_row=flow_gate,
                revision_row=revision_row,
                pr_row=pr_row,
            )
        else:
            _currentness_round(
                conn,
                task_row=flow_task,
                gate_row=flow_gate,
                revision_row=revision_row,
                pr_row=pr_row,
                locked_task=round_index == 2,
            )


def _script_settlement_flow(conn: ScriptedConnection, *, accepted: bool) -> None:
    """The E4 flow rounds plus the E3c settlement and, when routed, E2."""
    _script_common(conn, session_repeats=True)
    t_waiting = _task_row()
    t_cleared = _task_row(state_token=_TOKEN_2, gate_id=None)
    rev1 = _revision_row(revision_id=_REVISION_1_ID, number=1)
    gate = _gate_row()
    for round_index in range(3):
        if round_index == 0:
            _phase1(conn, task_row=t_waiting, gate_row=gate, revision_row=rev1)
        else:
            _currentness_round(
                conn,
                task_row=t_waiting,
                gate_row=gate,
                revision_row=rev1,
                locked_task=round_index == 2,
            )
    conn.on_repeat(_LOOPS_LIST, [_loop_row()])
    conn.on_repeat(_LOOP_READ, _loop_row())
    conn.on_repeat(_LOOP_READ_LOCKED, _loop_row())
    conn.on_repeat(_LOOP_GUARD_LOCKED, ("open", "planning"))
    conn.on_repeat(_REPO_READ, _repository_row())
    conn.on_repeat(_INSTALLATION_READ, _installation_row())
    it1 = _exhausted_iteration_row()
    it2 = _settlement_iteration_row(accepted=accepted)
    it2_unfin = _iteration_row(
        iteration_id=_ITERATION_2_ID, number=2, plan_revision_id=_REVISION_1_ID
    )
    conn.on(_TASK_READ, t_waiting)  # settlement workspace guard
    conn.on(_TASK_READ_LOCKED, t_waiting)  # settlement Task lock
    conn.on(_TASK_READ, t_waiting)  # settlement currentness
    conn.on_repeat(_GATE_READ, _gate_row())
    conn.on_repeat(_REVISION_READ, rev1)
    conn.on(_ITERATIONS_LIST, [it1])
    conn.on(_ITERATION_INSERT, it2_unfin)
    conn.on(_RESULT_UPDATE, it2)
    conn.on_repeat(_ITERATIONS_LIST, [it1, it2])
    settled_gate = _gate_row(status="settled")
    conn.on(_TASK_READ_LOCKED, t_waiting)  # the gate settlement's Task lock
    conn.on(_GATE_UPDATE, settled_gate)
    conn.on(_TASK_GATE_WRITE, t_cleared)
    if accepted:
        conn.on(_LOOP_CLOSE, _loop_row(status="closed", closed_at=_OBSERVED))
        return
    t_planning = _task_row(status="planning", state_token=_TOKEN_3, gate_id=None)
    conn.on(_STATUS_UPDATE, t_planning)
    rev2 = _revision_row(revision_id=_REVISION_2_ID, number=2, content=_REVISED_PLAN_TEXT)
    t_pointer = _task_row(
        status="planning",
        state_token=_TOKEN_4,
        current_plan_revision_id=_REVISION_2_ID,
        gate_id=None,
    )
    conn.on(_TASK_READ, t_planning)  # E2 workspace guard
    conn.on(_TASK_READ_LOCKED, t_planning)  # E2 Task lock
    conn.on(_TASK_READ, t_planning)  # E2 entry currentness
    conn.on(_ITERATION_READ, it2)
    conn.on(_REVISIONS_HISTORY, [rev1])
    conn.on(_TASK_READ, t_planning)  # E2 finalize currentness
    conn.on(_REVISION_INSERT, rev2)
    conn.on(_POINTER_UPDATE, t_pointer)


def _discussion_call(
    pool: DatabasePool,
    reviewer_attachment: _ReviewerAttachment,
    *,
    owner_gate_id: Any = _GATE_1_ID,
    expected_state_token: Any = _STATE_TOKEN,
    message: str = _OWNER_MESSAGE,
    producer_attachment: _ProducerAttachment | None = None,
    github: Any = None,
) -> Any:  # noqa: ANN401 - returns the typed outcome
    return send_owner_reviewer_discussion_message(
        pool,
        profile_id=_OWNER_PROFILE_ID,
        workspace_id=_WORKSPACE_ID,
        task_id=_TASK_ID,
        owner_gate_id=owner_gate_id,
        expected_state_token=expected_state_token,
        message=message,
        reviewer_runtime_factory=reviewer_attachment,
        producer_runtime_factory=producer_attachment,
        github=github,
    )


_GATE_SPECIFICATIONS = (
    (
        "implementation_authorization",
        {"gate_type": "implementation_authorization", "plan_revision_id": _REVISION_1_ID},
        "revision",
    ),
    (
        "pr_authorization",
        {
            "gate_type": "pr_authorization",
            "plan_revision_id": None,
            "head_sha": _HEAD_SHA,
        },
        "none",
    ),
    (
        "merge_decision",
        {
            "gate_type": "merge_decision",
            "plan_revision_id": None,
            "head_sha": _HEAD_SHA,
            "pr_id": _PR_ID,
        },
        "pr",
    ),
    (
        "review_resolution",
        {"gate_type": "review_resolution", "plan_revision_id": _REVISION_1_ID},
        "revision",
    ),
)


@pytest.mark.parametrize(
    ("gate_name", "gate_overrides", "subject_kind"),
    _GATE_SPECIFICATIONS,
    ids=[spec[0] for spec in _GATE_SPECIFICATIONS],
)
def test_ordinary_discussion_at_each_pending_gate_type(
    gate_name: str, gate_overrides: dict[str, Any], subject_kind: str
) -> None:
    """The Owner message is delivered verbatim to the existing READY Reviewer
    and the ordinary reply returns, audited as two direction-only events bound
    to the exact gate. The pre-PR gate never touches pull requests; no gate
    type settles, creates a session, or reaches the Producer."""
    conn = ScriptedConnection()
    fake_pool = FakePool(conn)
    pool = _pool(conn)
    gate_row = _gate_row(gate_id=_GATE_1_ID, **gate_overrides)
    revision_row = _revision_row(revision_id=_REVISION_1_ID, number=1)
    _script_discussion_flow(
        conn,
        gate_row=gate_row,
        revision_row=revision_row if subject_kind == "revision" else None,
        pr_row=_pr_row() if subject_kind == "pr" else None,
    )
    adapter = _prepared_adapter(fake_pool, reviewer_outcomes=[_ORDINARY_REPLY])
    reviewer_attachment = _ReviewerAttachment(adapter)
    producer_attachment = _ProducerAttachment(adapter)

    outcome = _discussion_call(
        pool,
        reviewer_attachment,
        producer_attachment=producer_attachment,
        github=FakeGitHubClient(fake_pool),
    )

    assert outcome.exchange is OwnerReviewerDiscussionExchange.ORDINARY_REPLY
    assert outcome.reply_text == _ORDINARY_REPLY
    assert outcome.owner_gate.id == _GATE_1_ID
    assert outcome.owner_gate.gate_type.value == gate_name
    assert outcome.reviewer_session.id == _REVIEWER_SESSION_ID
    assert outcome.task.state_token == _STATE_TOKEN
    assert outcome.resolution is None
    # Exactly one verbatim ordinary send to the exact Reviewer session; the
    # fake's two setup creations are the only sessions that ever existed.
    (send,) = adapter.submitted_sends()
    assert send.external_session_id == _REVIEWER_EXTERNAL_ID
    assert send.message == _OWNER_MESSAGE
    assert send.expected_family is None
    assert send.formal is False
    assert adapter.created_session_ids() == (_PRODUCER_EXTERNAL_ID, _REVIEWER_EXTERNAL_ID)
    assert adapter.realized_controls() == ()
    # Two direction-only discussion events, bound to the exact gate.
    assert _event_types(conn) == [
        _DISCUSSION_EVENT_TYPE,
        _DISCUSSION_EVENT_TYPE,
    ]
    (owner_event, reviewer_event) = _event_params_of_type(conn, _DISCUSSION_EVENT_TYPE)
    assert owner_event[3] == "owner"
    assert owner_event[4] == str(_OWNER_PROFILE_ID)
    assert owner_event[5] == "owner_gate"
    assert owner_event[6] == _GATE_1_ID
    assert owner_event[7].obj == {"direction": "owner_to_reviewer"}
    assert reviewer_event[3] == "reviewer"
    assert reviewer_event[4] == str(_REVIEWER_SESSION_ID)
    assert reviewer_event[7].obj == {"direction": "reviewer_to_owner"}
    # No settlement, no gate/Task mutation, no invented PR for the pre-PR gate.
    assert _count(conn, _ITERATION_INSERT) == 0
    assert _count(conn, _GATE_UPDATE) == 0
    assert _count(conn, _GATE_INSERT) == 0
    assert _count(conn, _TASK_GATE_WRITE) == 0
    assert _count(conn, _STATUS_UPDATE) == 0
    if subject_kind == "none":
        assert _count(conn, _PR_READ) == 0
        assert _count(conn, _REVISION_READ) == 0
    # Attachment happened exactly once, after authorization, and was disposed;
    # the Producer attachment is never acquired for an ordinary discussion.
    assert reviewer_attachment.acquisitions == 1
    assert reviewer_attachment.disposals == 1
    assert reviewer_attachment.bindings[0].id == _REVIEWER_SESSION_ID
    assert producer_attachment.acquisitions == 0
    # No discussion text ever reaches durable SQL.
    _assert_no_discussion_text_in_sql(conn, _OWNER_MESSAGE, _ORDINARY_REPLY)


def test_malformed_commands_never_touch_state_or_attachments() -> None:
    """Command-shape failures raise before any SQL, attachment, or send."""
    conn = ScriptedConnection()
    pool = _pool(conn)
    adapter = FakeAgentRuntimeAdapter()
    reviewer_attachment = _ReviewerAttachment(adapter)

    with pytest.raises(InvalidCommandError):
        _discussion_call(pool, reviewer_attachment, message="   ")
    with pytest.raises(InvalidCommandError):
        send_owner_reviewer_discussion_message(
            pool,
            profile_id=cast(Any, "not-a-uuid"),
            workspace_id=_WORKSPACE_ID,
            task_id=_TASK_ID,
            owner_gate_id=_GATE_1_ID,
            expected_state_token=_STATE_TOKEN,
            message=_OWNER_MESSAGE,
            reviewer_runtime_factory=reviewer_attachment,
        )
    with pytest.raises(InvalidCommandError):
        send_owner_reviewer_discussion_message(
            pool,
            profile_id=_OWNER_PROFILE_ID,
            workspace_id=_WORKSPACE_ID,
            task_id=_TASK_ID,
            owner_gate_id=_GATE_1_ID,
            expected_state_token=_STATE_TOKEN,
            message=_OWNER_MESSAGE,
            reviewer_runtime_factory=cast(Any, None),
        )

    assert conn.executed == []
    assert reviewer_attachment.acquisitions == 0
    assert adapter.submitted_sends() == ()


def test_an_unauthorized_owner_is_not_found_before_any_attachment() -> None:
    conn = ScriptedConnection()
    fake_pool = FakePool(conn)
    pool = _pool(conn)
    _script_discussion_flow(conn, ws_row=_ws_row(owner=_OTHER_PROFILE_ID))
    adapter = _prepared_adapter(fake_pool, reviewer_outcomes=[])
    reviewer_attachment = _ReviewerAttachment(adapter)

    with pytest.raises(NotFoundError):
        _discussion_call(pool, reviewer_attachment)

    assert reviewer_attachment.acquisitions == 0
    assert adapter.submitted_sends() == ()
    assert _event_types(conn) == []


def test_a_stale_token_rejects_before_any_attachment() -> None:
    conn = ScriptedConnection()
    pool = _pool(conn)
    adapter = FakeAgentRuntimeAdapter()
    reviewer_attachment = _ReviewerAttachment(adapter)
    _script_common(conn, session_repeats=True)
    stale_task = _task_row(state_token=_TOKEN_2)
    conn.on(_TASK_READ, stale_task)  # the workspace authorization's Task read
    conn.on(_TASK_READ, stale_task)  # the currentness Task read

    with pytest.raises(StaleOperationError):
        _discussion_call(pool, reviewer_attachment)

    assert reviewer_attachment.acquisitions == 0
    assert adapter.submitted_sends() == ()
    assert _event_types(conn) == []


def test_a_non_current_gate_rejects_before_any_attachment() -> None:
    conn = ScriptedConnection()
    pool = _pool(conn)
    adapter = FakeAgentRuntimeAdapter()
    reviewer_attachment = _ReviewerAttachment(adapter)
    _script_common(conn, session_repeats=True)
    _phase1(
        conn,
        task_row=_task_row(gate_id=_GATE_2_ID),
        gate_row=_gate_row(gate_id=_GATE_1_ID),
        revision_row=_revision_row(revision_id=_REVISION_1_ID, number=1),
    )

    with pytest.raises(StaleOperationError):
        _discussion_call(pool, reviewer_attachment)

    assert reviewer_attachment.acquisitions == 0
    assert adapter.submitted_sends() == ()


def test_a_resolved_gate_is_a_stale_discussion() -> None:
    conn = ScriptedConnection()
    pool = _pool(conn)
    adapter = FakeAgentRuntimeAdapter()
    reviewer_attachment = _ReviewerAttachment(adapter)
    _script_common(conn, session_repeats=True)
    _phase1(
        conn,
        task_row=_task_row(),
        gate_row=_gate_row(status="approved"),
        revision_row=_revision_row(revision_id=_REVISION_1_ID, number=1),
    )

    with pytest.raises(StaleOperationError):
        _discussion_call(pool, reviewer_attachment)

    assert reviewer_attachment.acquisitions == 0
    assert adapter.submitted_sends() == ()


def test_a_stale_plan_subject_rejects_before_any_attachment() -> None:
    conn = ScriptedConnection()
    pool = _pool(conn)
    adapter = FakeAgentRuntimeAdapter()
    reviewer_attachment = _ReviewerAttachment(adapter)
    _script_common(conn, session_repeats=True)
    _phase1(
        conn,
        task_row=_task_row(current_plan_revision_id=_REVISION_2_ID),
        gate_row=_gate_row(plan_revision_id=_REVISION_1_ID),
        revision_row=_revision_row(revision_id=_REVISION_1_ID, number=1),
    )

    with pytest.raises(StaleOperationError):
        _discussion_call(pool, reviewer_attachment)

    assert reviewer_attachment.acquisitions == 0
    assert adapter.submitted_sends() == ()


def test_a_changed_pr_head_is_a_stale_discussion() -> None:
    conn = ScriptedConnection()
    pool = _pool(conn)
    adapter = FakeAgentRuntimeAdapter()
    reviewer_attachment = _ReviewerAttachment(adapter)
    _script_common(conn, session_repeats=True)
    _phase1(
        conn,
        task_row=_task_row(),
        gate_row=_gate_row(
            gate_type="merge_decision",
            plan_revision_id=None,
            head_sha=_HEAD_SHA,
            pr_id=_PR_ID,
        ),
        pr_row=_pr_row(head_sha=_OTHER_SHA),
    )

    with pytest.raises(StaleOperationError):
        _discussion_call(pool, reviewer_attachment)

    assert reviewer_attachment.acquisitions == 0
    assert adapter.submitted_sends() == ()
    assert _event_types(conn) == []


def test_a_non_waiting_task_rejects_before_any_attachment() -> None:
    conn = ScriptedConnection()
    pool = _pool(conn)
    adapter = FakeAgentRuntimeAdapter()
    reviewer_attachment = _ReviewerAttachment(adapter)
    _script_common(conn, session_repeats=True)
    _phase1(
        conn,
        task_row=_task_row(status="planning"),
        gate_row=_gate_row(),
        revision_row=_revision_row(revision_id=_REVISION_1_ID, number=1),
    )

    with pytest.raises(ConflictError):
        _discussion_call(pool, reviewer_attachment)

    assert reviewer_attachment.acquisitions == 0
    assert adapter.submitted_sends() == ()


def test_an_archived_task_is_a_stale_discussion() -> None:
    conn = ScriptedConnection()
    pool = _pool(conn)
    adapter = FakeAgentRuntimeAdapter()
    reviewer_attachment = _ReviewerAttachment(adapter)
    _script_common(conn, session_repeats=True)
    _phase1(
        conn,
        task_row=_task_row(status="completed", archived_at=_OBSERVED),
        gate_row=_gate_row(),
        revision_row=_revision_row(revision_id=_REVISION_1_ID, number=1),
    )

    with pytest.raises(StaleOperationError):
        _discussion_call(pool, reviewer_attachment)

    assert reviewer_attachment.acquisitions == 0
    assert adapter.submitted_sends() == ()


@pytest.mark.parametrize("lifecycle", ["connecting", "lost", "ended"])
def test_a_non_ready_reviewer_binding_rejects_before_any_attachment(lifecycle: str) -> None:
    conn = ScriptedConnection()
    pool = _pool(conn)
    adapter = FakeAgentRuntimeAdapter()
    reviewer_attachment = _ReviewerAttachment(adapter)
    _script_common(conn, session_repeats=False)
    _phase1(
        conn,
        task_row=_task_row(),
        gate_row=_gate_row(),
        revision_row=_revision_row(revision_id=_REVISION_1_ID, number=1),
        session_row=_session_row(
            "reviewer",
            session_id=_REVIEWER_SESSION_ID,
            external_id=_REVIEWER_EXTERNAL_ID if lifecycle != "connecting" else None,
            lifecycle=lifecycle,
        ),
    )

    with pytest.raises(ConflictError):
        _discussion_call(pool, reviewer_attachment)

    # No rebinding, replacement session, or Producer routing ever happens.
    assert reviewer_attachment.acquisitions == 0
    assert adapter.creation_requests() == ()
    assert adapter.submitted_sends() == ()
    assert _event_types(conn) == []


def test_a_missing_reviewer_binding_rejects_before_any_attachment() -> None:
    conn = ScriptedConnection()
    pool = _pool(conn)
    adapter = FakeAgentRuntimeAdapter()
    reviewer_attachment = _ReviewerAttachment(adapter)
    _script_common(conn, session_repeats=False)
    _phase1(
        conn,
        task_row=_task_row(),
        gate_row=_gate_row(),
        revision_row=_revision_row(revision_id=_REVISION_1_ID, number=1),
        session_row=None,
    )

    with pytest.raises(NotFoundError):
        _discussion_call(pool, reviewer_attachment)

    assert reviewer_attachment.acquisitions == 0
    assert adapter.creation_requests() == ()
    assert adapter.submitted_sends() == ()


@pytest.mark.parametrize(
    "reply",
    [
        "Here is my review: "
        + json.dumps(accepted_review_result_candidate("Looks good.")),  # buried JSON
        "{not valid json",  # malformed standalone
        json.dumps(plan_result_candidate(_PLAN_TEXT)),  # other family
        json.dumps(
            {"type": "review_result", "schema_version": 2, "outcome": "ACCEPTED"}
        ),  # version
        json.dumps(accepted_review_result_candidate("A"))
        + "\n"
        + json.dumps(accepted_review_result_candidate("B")),  # two objects
        "```json\n{broken\n```",  # malformed fenced
    ],
)
def test_non_formal_replies_at_a_resolution_gate_stay_ordinary(reply: str) -> None:
    """Prose-wrapped, malformed, other-family, unsupported-version, and
    multi-object JSON at a REVIEW_RESOLUTION gate are ordinary conversation:
    two direction-only events, no settlement, no authority effect."""
    conn = ScriptedConnection()
    fake_pool = FakePool(conn)
    pool = _pool(conn)
    _script_discussion_flow(conn, revision_row=_revision_row(revision_id=_REVISION_1_ID, number=1))
    adapter = _prepared_adapter(fake_pool, reviewer_outcomes=[reply])
    reviewer_attachment = _ReviewerAttachment(adapter)

    outcome = _discussion_call(pool, reviewer_attachment)

    assert outcome.exchange is OwnerReviewerDiscussionExchange.ORDINARY_REPLY
    assert outcome.reply_text == reply
    assert _event_types(conn) == [_DISCUSSION_EVENT_TYPE, _DISCUSSION_EVENT_TYPE]
    assert _count(conn, _ITERATION_INSERT) == 0
    assert _count(conn, _GATE_UPDATE) == 0
    assert reviewer_attachment.acquisitions == 1


def test_prose_with_embedded_example_json_at_a_resolution_gate_stays_ordinary() -> None:
    """A resolution-gate reply that merely discusses or exemplifies the formal
    shape is never extracted and never becomes authority."""
    conn = ScriptedConnection()
    fake_pool = FakePool(conn)
    pool = _pool(conn)
    _script_discussion_flow(conn, revision_row=_revision_row(revision_id=_REVISION_1_ID, number=1))
    reply = (
        "For example, you could reply with:\n"
        "```json\n" + json.dumps(accepted_review_result_candidate("Example only")) + "\n```\n"
        "but only if you truly accept."
    )
    adapter = _prepared_adapter(fake_pool, reviewer_outcomes=[reply])
    reviewer_attachment = _ReviewerAttachment(adapter)

    outcome = _discussion_call(pool, reviewer_attachment)

    assert outcome.exchange is OwnerReviewerDiscussionExchange.ORDINARY_REPLY
    assert _event_types(conn) == [_DISCUSSION_EVENT_TYPE, _DISCUSSION_EVENT_TYPE]
    assert _count(conn, _ITERATION_INSERT) == 0


def test_a_fenced_valid_acceptance_settles_atomically_without_e2() -> None:
    """One fenced standalone valid ACCEPTED review_result routes to the merged
    E3c planning settlement; the two discussion message facts commit inside
    the settlement's transaction; the loop closes and E2 is never touched."""
    conn = ScriptedConnection()
    fake_pool = FakePool(conn)
    pool = _pool(conn)
    _script_settlement_flow(conn, accepted=True)
    reply = (
        "```json\n"
        + json.dumps(accepted_review_result_candidate("Everything checks out."))
        + "\n```"
    )
    adapter = _prepared_adapter(fake_pool, reviewer_outcomes=[reply])
    reviewer_attachment = _ReviewerAttachment(adapter)
    producer_attachment = _ProducerAttachment(adapter)

    outcome = _discussion_call(pool, reviewer_attachment, producer_attachment=producer_attachment)

    assert outcome.exchange is OwnerReviewerDiscussionExchange.FORMAL_RESOLUTION_SETTLED
    assert outcome.reply_text is None
    assert outcome.resolution is not None
    assert outcome.resolution.decision is PlanningReviewResolutionDecision.SETTLED_ACCEPTED
    assert outcome.resolution.review_iteration.id == _ITERATION_2_ID
    assert outcome.resolution.review_iteration.outcome is ReviewOutcome.ACCEPTED
    assert outcome.resolution.resolution_gate.status is OwnerGateStatus.SETTLED
    assert outcome.task.state_token == _TOKEN_2
    assert outcome.task.current_owner_gate_id is None
    # The discussion audit rides the settlement transaction: exactly the
    # REVIEWER plan review, the settled gate resolution, and the two
    # direction-only message facts — in one commit, nothing else.
    assert _event_types(conn) == [
        "plan_reviewed",
        "owner_gate_resolved",
        _DISCUSSION_EVENT_TYPE,
        _DISCUSSION_EVENT_TYPE,
    ]
    (owner_event, reviewer_event) = _event_params_of_type(conn, _DISCUSSION_EVENT_TYPE)
    assert owner_event[3] == "owner"
    assert owner_event[4] == str(_OWNER_PROFILE_ID)
    assert owner_event[7].obj == {"direction": "owner_to_reviewer"}
    assert reviewer_event[3] == "reviewer"
    assert reviewer_event[4] == str(_REVIEWER_SESSION_ID)
    assert reviewer_event[7].obj == {"direction": "reviewer_to_owner"}
    # No E2 continuation, no Producer attachment, no GitHub observation.
    assert _count(conn, _REVISION_INSERT) == 0
    assert _count(conn, _STATUS_UPDATE) == 0
    assert _count(conn, _LOOP_CLOSE) == 1
    assert producer_attachment.acquisitions == 0
    assert len(adapter.realized_controls(_REVIEWER_EXTERNAL_ID)) == 0
    assert reviewer_attachment.acquisitions == 1
    assert reviewer_attachment.disposals == 1


def test_a_standalone_change_request_settles_atomically_and_routes_to_e2() -> None:
    """One unfenced standalone valid CHANGES_REQUESTED review_result settles
    the gate with REVIEWER provenance, commits the discussion audit in the
    same transaction, lazily acquires the Producer attachment exactly once,
    and routes only the new findings to E2."""
    conn = ScriptedConnection()
    fake_pool = FakePool(conn)
    pool = _pool(conn)
    _script_settlement_flow(conn, accepted=False)
    reply = json.dumps(
        changes_requested_review_result_candidate(
            "Needs another pass", [("Clarify the retry loop", "Explain the backoff path.")]
        )
    )
    adapter = _prepared_adapter(
        fake_pool,
        reviewer_outcomes=[reply],
        producer_outcomes=[plan_result_candidate(_REVISED_PLAN_TEXT)],
    )
    reviewer_attachment = _ReviewerAttachment(adapter)
    producer_attachment = _ProducerAttachment(adapter)
    github = FakeGitHubClient(fake_pool)

    outcome = _discussion_call(
        pool,
        reviewer_attachment,
        producer_attachment=producer_attachment,
        github=github,
    )

    assert outcome.exchange is OwnerReviewerDiscussionExchange.FORMAL_RESOLUTION_SETTLED
    assert outcome.resolution is not None
    assert outcome.resolution.decision is PlanningReviewResolutionDecision.CONTINUATION_ROUTED
    assert outcome.resolution.review_iteration.outcome is ReviewOutcome.CHANGES_REQUESTED
    assert outcome.resolution.task.status is TaskStatus.PLANNING
    assert outcome.task.state_token == _TOKEN_4
    assert outcome.task.current_plan_revision_id == _REVISION_2_ID
    # Discussion audit + settlement + E2's revision event, in order.
    assert _event_types(conn) == [
        "plan_reviewed",
        "owner_gate_resolved",
        _DISCUSSION_EVENT_TYPE,
        _DISCUSSION_EVENT_TYPE,
        "plan_revision_revised",
    ]
    # Exactly the NEW findings routed to E2; the exhausted history is never
    # rerouted, and the Reviewer session saw no control realization.
    (revise_send,) = adapter.realized_controls(_PRODUCER_EXTERNAL_ID)
    assert revise_send.message is not None
    assert "Clarify the retry loop" in revise_send.message
    assert "Say which module changes." not in revise_send.message
    assert len(adapter.realized_controls(_REVIEWER_EXTERNAL_ID)) == 0
    # The Producer attachment was acquired lazily, exactly once, for the
    # continuation only; the Reviewer attachment exactly once.
    assert producer_attachment.acquisitions == 1
    assert producer_attachment.disposals == 1
    assert reviewer_attachment.acquisitions == 1
    assert _count(conn, _REVISION_INSERT) == 1
    assert _count(conn, _GATE_UPDATE) == 1
    assert len(github.repository_calls) == 1
    assert len(github.branch_calls) == 1


def test_a_pr_head_subject_resolution_result_fails_closed() -> None:
    """A recognized formal result at a PR/head-subject REVIEW_RESOLUTION gate
    belongs to E9's settlement: nothing is applied, nothing is audited, and
    no Producer attachment is acquired."""
    conn = ScriptedConnection()
    fake_pool = FakePool(conn)
    pool = _pool(conn)
    pr_gate = _gate_row(
        gate_type="review_resolution",
        plan_revision_id=None,
        head_sha=_HEAD_SHA,
        pr_id=_PR_ID,
    )
    _script_discussion_flow(conn, gate_row=pr_gate, pr_row=_pr_row())
    reply = json.dumps(accepted_review_result_candidate("Ship it."))
    adapter = _prepared_adapter(fake_pool, reviewer_outcomes=[reply])
    reviewer_attachment = _ReviewerAttachment(adapter)
    producer_attachment = _ProducerAttachment(adapter)

    with pytest.raises(ConflictError):
        _discussion_call(
            pool,
            reviewer_attachment,
            producer_attachment=producer_attachment,
            github=FakeGitHubClient(fake_pool),
        )

    assert _count(conn, _ITERATION_INSERT) == 0
    assert _count(conn, _GATE_UPDATE) == 0
    assert _event_types(conn) == []
    assert producer_attachment.acquisitions == 0
    assert reviewer_attachment.disposals == 1


def test_a_missing_producer_factory_refuses_a_formal_continuation() -> None:
    """A recognized CHANGES_REQUESTED without the deferred Producer attachment
    factory is an invalid command before any settlement or continuation."""
    conn = ScriptedConnection()
    fake_pool = FakePool(conn)
    pool = _pool(conn)
    _script_settlement_flow(conn, accepted=False)
    reply = json.dumps(
        changes_requested_review_result_candidate(
            "Needs another pass", [("Clarify the retry loop", "Explain the backoff path.")]
        )
    )
    adapter = _prepared_adapter(fake_pool, reviewer_outcomes=[reply])
    reviewer_attachment = _ReviewerAttachment(adapter)

    with pytest.raises(InvalidCommandError):
        _discussion_call(pool, reviewer_attachment)

    assert _count(conn, _ITERATION_INSERT) == 0
    assert _event_types(conn) == []


def test_the_gate_resolving_during_attachment_refuses_the_send() -> None:
    """When the Owner resolves the gate while the Reviewer attachment is being
    acquired, the pre-send revalidation refuses: no message is sent, nothing
    is audited, and the attachment is disposed exactly once."""
    conn = ScriptedConnection()
    fake_pool = FakePool(conn)
    pool = _pool(conn)
    _script_common(conn, session_repeats=True)
    _phase1(
        conn,
        task_row=_task_row(),
        gate_row=_gate_row(),
        revision_row=_revision_row(revision_id=_REVISION_1_ID, number=1),
    )
    conn.on(_GATE_UPDATE, _gate_row(status="approved"))  # the Owner resolution
    resolved_task = _task_row(state_token=_TOKEN_2, gate_id=None)
    conn.on(_TASK_READ, resolved_task)  # the post-acquisition revalidation
    adapter = _prepared_adapter(fake_pool, reviewer_outcomes=[])

    def resolve_hook() -> None:
        with fake_pool.connection() as hook_conn:
            hook_conn.execute(
                "update openorc.owner_gates set status = 'approved' where id = %s",
                (str(_GATE_1_ID),),
            )

    reviewer_attachment = _ReviewerAttachment(adapter, on_acquire=resolve_hook)

    with pytest.raises(StaleOperationError):
        _discussion_call(pool, reviewer_attachment)

    assert reviewer_attachment.acquisitions == 1
    assert reviewer_attachment.disposals == 1
    assert adapter.submitted_sends() == ()
    assert _event_types(conn) == []


def test_the_gate_resolving_after_the_send_is_stale_advisory_output() -> None:
    """A reply that arrives after the Owner resolved the gate is stale advisory
    output: the send happened, but nothing is audited and no success is
    reported against the resolved gate."""
    conn = ScriptedConnection()
    fake_pool = FakePool(conn)
    pool = _pool(conn)
    _script_common(conn, session_repeats=True)
    _phase1(
        conn,
        task_row=_task_row(),
        gate_row=_gate_row(),
        revision_row=_revision_row(revision_id=_REVISION_1_ID, number=1),
    )
    _currentness_round(
        conn,
        task_row=_task_row(),
        gate_row=_gate_row(),
        revision_row=_revision_row(revision_id=_REVISION_1_ID, number=1),
    )
    resolved_task = _task_row(state_token=_TOKEN_2, gate_id=None)
    conn.on(_TASK_READ_LOCKED, resolved_task)  # the post-send FOR UPDATE read
    conn.on(_TASK_READ, resolved_task)  # the recheck's currentness read
    adapter = _prepared_adapter(fake_pool, reviewer_outcomes=[_ORDINARY_REPLY])
    reviewer_attachment = _ReviewerAttachment(adapter)

    with pytest.raises(StaleOperationError):
        _discussion_call(pool, reviewer_attachment)

    (send,) = adapter.submitted_sends()
    assert send.message == _OWNER_MESSAGE
    assert _event_types(conn) == []
    assert reviewer_attachment.disposals == 1


def test_an_uncertain_send_records_nothing_and_reports_uncertainty() -> None:
    """An uncertain runtime outcome may already have been delivered: it is
    never resent, never audited, and never presented as success."""
    conn = ScriptedConnection()
    fake_pool = FakePool(conn)
    pool = _pool(conn)
    _script_discussion_flow(conn, revision_row=_revision_row(revision_id=_REVISION_1_ID, number=1))
    adapter = _prepared_adapter(
        fake_pool, reviewer_outcomes=[AgentRuntimeTimeoutError("scripted timeout")]
    )
    reviewer_attachment = _ReviewerAttachment(adapter)

    with pytest.raises(ExternalOperationUncertainError):
        _discussion_call(pool, reviewer_attachment)

    assert len(adapter.submitted_sends()) == 1  # the one send; never resent
    assert _event_types(conn) == []
    assert reviewer_attachment.disposals == 1


def test_a_known_send_failure_records_nothing_and_reports_failure() -> None:
    conn = ScriptedConnection()
    fake_pool = FakePool(conn)
    pool = _pool(conn)
    _script_discussion_flow(conn, revision_row=_revision_row(revision_id=_REVISION_1_ID, number=1))
    adapter = _prepared_adapter(
        fake_pool, reviewer_outcomes=[AgentRuntimeUnavailableError("scripted outage")]
    )
    reviewer_attachment = _ReviewerAttachment(adapter)

    with pytest.raises(ExternalOperationFailedError):
        _discussion_call(pool, reviewer_attachment)

    assert len(adapter.submitted_sends()) == 1
    assert _event_types(conn) == []
    assert reviewer_attachment.disposals == 1


def test_a_failed_ordinary_audit_is_never_presented_as_success() -> None:
    """When the post-send audit insert fails, the error propagates honestly:
    no outcome is returned and no partial audit claims the exchange."""
    conn = ScriptedConnection()
    fake_pool = FakePool(conn)
    pool = _pool(conn)
    conn.on(_discussion_event_insert, RuntimeError("scripted audit failure"))
    _script_discussion_flow(conn, revision_row=_revision_row(revision_id=_REVISION_1_ID, number=1))
    adapter = _prepared_adapter(fake_pool, reviewer_outcomes=[_ORDINARY_REPLY])
    reviewer_attachment = _ReviewerAttachment(adapter)

    with pytest.raises(RuntimeError, match="scripted audit failure"):
        _discussion_call(pool, reviewer_attachment)

    # Exactly one discussion insert was attempted before the failure; the
    # reply was delivered but no success was ever reported.
    assert len(_event_params_of_type(conn, _DISCUSSION_EVENT_TYPE)) == 1
    assert reviewer_attachment.disposals == 1


def test_a_failed_settlement_audit_prevents_the_e2_continuation() -> None:
    """When the settlement's discussion-event write fails, the whole
    settlement rolls back atomically: the E2 continuation never runs, the
    routing never dispatches, and no success is reported."""
    conn = ScriptedConnection()
    fake_pool = FakePool(conn)
    pool = _pool(conn)
    conn.on(_discussion_event_insert, RuntimeError("scripted audit failure"))
    _script_settlement_flow(conn, accepted=False)
    reply = json.dumps(
        changes_requested_review_result_candidate(
            "Needs another pass", [("Clarify the retry loop", "Explain the backoff path.")]
        )
    )
    adapter = _prepared_adapter(
        fake_pool,
        reviewer_outcomes=[reply],
        producer_outcomes=[plan_result_candidate(_REVISED_PLAN_TEXT)],
    )
    reviewer_attachment = _ReviewerAttachment(adapter)
    producer_attachment = _ProducerAttachment(adapter)
    github = FakeGitHubClient(fake_pool)

    with pytest.raises(RuntimeError, match="scripted audit failure"):
        _discussion_call(
            pool,
            reviewer_attachment,
            producer_attachment=producer_attachment,
            github=github,
        )

    # The discussion-event failure aborted the settlement before any
    # continuation: no revision production, no GitHub observation, and the
    # settlement's own events were rolled back with it.
    assert _count(conn, _REVISION_INSERT) == 0
    assert github.repository_calls == []
    assert github.branch_calls == []
    assert producer_attachment.disposals == 1


def test_the_post_send_audit_serializes_against_gate_resolution() -> None:
    """The post-send transaction takes the Task row FOR UPDATE before the
    currentness recheck and the audit inserts — the same lock an Owner gate
    resolution takes — so a resolution that committed before the lock is
    observed as a stale reply with no events, and a concurrent resolution can
    never interleave between the recheck and the audit commit."""
    # A resolution that committed before the post-send lock: the FOR UPDATE
    # read observes the rotated token and cleared pointer; nothing audits.
    conn = ScriptedConnection()
    fake_pool = FakePool(conn)
    pool = _pool(conn)
    _script_common(conn, session_repeats=True)
    rev1 = _revision_row(revision_id=_REVISION_1_ID, number=1)
    _phase1(conn, task_row=_task_row(), gate_row=_gate_row(), revision_row=rev1)
    _currentness_round(conn, task_row=_task_row(), gate_row=_gate_row(), revision_row=rev1)
    resolved_task = _task_row(state_token=_TOKEN_2, gate_id=None)
    conn.on(_TASK_READ_LOCKED, resolved_task)  # the post-send FOR UPDATE read
    conn.on(_TASK_READ, resolved_task)  # the recheck's currentness read
    adapter = _prepared_adapter(fake_pool, reviewer_outcomes=[_ORDINARY_REPLY])
    reviewer_attachment = _ReviewerAttachment(adapter)

    with pytest.raises(StaleOperationError):
        _discussion_call(pool, reviewer_attachment)

    (send,) = adapter.submitted_sends()
    assert send.message == _OWNER_MESSAGE
    assert _event_types(conn) == []
    assert _count(conn, _TASK_READ_LOCKED) == 1

    # Still-current exchange: exactly one Task-row lock, acquired before any
    # audit insert inside the same short transaction.
    happy_conn = ScriptedConnection()
    happy_pool = _pool(happy_conn)
    _script_discussion_flow(happy_conn, revision_row=rev1)
    happy_adapter = _prepared_adapter(FakePool(happy_conn), reviewer_outcomes=[_ORDINARY_REPLY])
    happy_attachment = _ReviewerAttachment(happy_adapter)

    outcome = _discussion_call(happy_pool, happy_attachment)

    assert outcome.exchange is OwnerReviewerDiscussionExchange.ORDINARY_REPLY
    assert _event_types(happy_conn) == [_DISCUSSION_EVENT_TYPE, _DISCUSSION_EVENT_TYPE]
    locked_positions = [
        index
        for index, (sql, _params) in enumerate(happy_conn.executed)
        if _TASK_READ_LOCKED(sql, None)
    ]
    first_event_position = next(
        index
        for index, (sql, _params) in enumerate(happy_conn.executed)
        if _EVENT_INSERT(sql, None)
    )
    assert len(locked_positions) == 1
    assert locked_positions[0] < first_event_position
