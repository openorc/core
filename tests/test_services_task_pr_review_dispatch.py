"""Deterministic tests for the E9 §1 PR-review dispatch seam (issue #84).

A scripted SQL-connection seam, a routing fake GitHub client, and the shared
deterministic fake Agent Runtime prove the E9 §1 orchestration seams on their
critical paths:

- authenticated admission precedes the system-capable reconciliation: a
  foreign/cross-Workspace invocation is rejected with the uniform not-found
  vocabulary BEFORE any GitHub read and without touching the target PR's
  persisted reconciliation snapshot; a malformed command shape is rejected
  before any SQL and before telemetry;
- the initial handoff reserves exactly one OPEN ``PR_REVIEW`` loop (snapshot
  of the Workspace limit) and one next-numbered unfinalized PR/head
  iteration, and the ENTIRE Reviewer message is the canonical structural JSON
  of the persisted ``implementation_result`` — the ``PrReviewSubject`` rides
  only in the typed interaction context;
- a GitHub-observed changed head without corresponding remediation evidence
  refuses with no reservation (stale evidence never dispatches); a duplicate
  ordinary dispatch for an already-reviewed head refuses; durable head
  movement between the reservation and the send is a never-dispatched dead
  letter with the reserved iteration left unfinalized; an uncertain send
  leaves the attempt unresolved and is never resent.

No live GitHub, runtime, or Postgres access.
"""

from __future__ import annotations

import uuid
from collections.abc import Callable, Iterator, Sequence
from contextlib import AbstractContextManager, contextmanager
from datetime import UTC, datetime
from typing import Any, cast

import pytest

from openorc.adapters.agent_runtime.errors import AgentRuntimeUncertainOutcomeError
from openorc.adapters.agent_runtime.results import AgentSessionCreationRequest
from openorc.adapters.github import GitHubPullRequestObservation, GitHubRepositoryObservation
from openorc.domain.connections import WorkflowRole
from openorc.persistence.pool import DatabasePool
from openorc.protocol.interaction import PrReviewSubject, ReviewInteraction
from openorc.services.errors import (
    ConflictError,
    ExternalOperationUncertainError,
    InvalidCommandError,
    NotFoundError,
    StaleOperationError,
)
from openorc.services.task_pr_composition import TaskPrPublished
from openorc.services.task_pr_review_dispatch import (
    TaskPrReviewDispatchOutcome,
    dispatch_pr_review,
    serialize_implementation_result_message,
)
from tests.fakes.agent_runtime import (
    FakeAgentRuntimeAdapter,
    accepted_review_result_candidate,
    session_ready_candidate,
)

_OBSERVED = datetime(2026, 10, 21, 12, 0, 0, tzinfo=UTC)
_ACCOUNT_OPERATIONAL_ROW: tuple[Any, ...] = (None, None, None)
_WORKSPACE_ID = uuid.uuid4()
_OTHER_WORKSPACE_ID = uuid.uuid4()
_OWNER_PROFILE_ID = uuid.uuid4()
_OTHER_PROFILE_ID = uuid.uuid4()
_REPOSITORY_ID = uuid.uuid4()
_TASK_ID = uuid.uuid4()
_PR_ID = uuid.uuid4()
_INSTALLATION_ID = uuid.uuid4()
_EXTERNAL_INSTALLATION_ID = 501
_CONNECTION_ID = uuid.uuid4()
_REVIEWER_SESSION_ID = uuid.uuid4()
_LOOP_ID = uuid.uuid4()
_ITERATION_ID = uuid.uuid4()
_STATE_TOKEN = uuid.uuid4()
_HEAD_SHA = "0123456789abcdef0123456789abcdef01234567"
_NEW_HEAD_SHA = "fedcba9876543210fedcba9876543210fedcba98"
_BRANCH = "openorc/task-42-implementation"
_GITHUB_REPOSITORY_ID = 987654321
_GITHUB_PR_ID = 900_719_925_474_099
_PR_NUMBER = 7
_ISSUE_NUMBER = 42
_REVIEWER_EXTERNAL_ID = "fake-session-1"

_IMPLEMENTATION_PAYLOAD: dict[str, Any] = {
    "type": "implementation_result",
    "schema_version": 1,
    "status": "COMPLETED",
    "branch": _BRANCH,
    "summary": "Implemented the retry loop.",
    "changes": ["feat: retry loop"],
    "validation": ["pytest green"],
}


def _sql_has(*tokens: str) -> Callable[[str, tuple[Any, ...] | None], bool]:
    def predicate(sql: str, _params: tuple[Any, ...] | None) -> bool:
        return all(token in sql for token in tokens)

    return predicate


def _sql_lacks(
    base: Callable[[str, tuple[Any, ...] | None], bool], *tokens: str
) -> Callable[[str, tuple[Any, ...] | None], bool]:
    def wrapped(sql: str, params: tuple[Any, ...] | None) -> bool:
        return base(sql, params) and not any(token in sql for token in tokens)

    return wrapped


class FakeCursor:
    def __init__(self, row: tuple[Any, ...] | None, rows: list[tuple[Any, ...]] | None) -> None:
        self._row = row
        self._rows = rows if rows is not None else ([] if row is None else [row])

    def fetchone(self) -> tuple[Any, ...] | None:
        return self._row

    def fetchall(self) -> list[tuple[Any, ...]]:
        return list(self._rows)


_Predicate = Callable[[str, tuple[Any, ...] | None], bool]
_CannedResult = tuple[Any, ...] | list[tuple[Any, ...]] | Exception | None


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
                return FakeCursor(result, None)
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
        raise AssertionError("composition tests never close pools")


def _pool(conn: ScriptedConnection) -> DatabasePool:
    return cast(DatabasePool, FakePool(conn))


_ACCOUNT_BARRIER = _sql_has("from openorc.profiles", "for key share")
_WORKSPACE_READ = _sql_has("from openorc.workspaces")
_TASK_READ = _sql_lacks(_sql_has("from openorc.tasks where id"), "for update")
_TASK_READ_LOCKED = _sql_has("from openorc.tasks where id", "for update")
_PR_READ = _sql_lacks(_sql_has("from openorc.task_pull_requests"), "for update", "update ")
_PR_READ_LOCKED = _sql_has("from openorc.task_pull_requests", "for update")
_PR_UPDATE = _sql_has("update openorc.task_pull_requests")
_REPO_READ = _sql_lacks(_sql_has("from openorc.repositories where id"), "for update")
_REPO_READ_LOCKED = _sql_has("from openorc.repositories where id", "for update")
_INSTALLATION_READ = _sql_has("from openorc.github_installations where id")
_SESSION_READ = _sql_has("from openorc.task_agent_sessions")
_LOOPS_LIST = _sql_has("from openorc.review_loops where task_id")
_LOOP_INSERT = _sql_has("insert into openorc.review_loops")
_LOOP_READ_LOCKED = _sql_lacks(
    _sql_has("from openorc.review_loops where id", "for update"), "select status, purpose"
)
_LOOP_GUARD_LOCKED = _sql_has("select status, purpose from openorc.review_loops", "for update")
_ITERATIONS_LIST = _sql_has("review_iterations where review_loop_id")
_ITERATION_INSERT = _sql_has("insert into openorc.review_iterations")
_EXECUTIONS_LIST = _sql_has("from openorc.executions where task_id")


def _ws_row(*, owner: Any = _OWNER_PROFILE_ID, limit: int = 2) -> tuple[Any, ...]:
    return (_WORKSPACE_ID, owner, "platform", _OBSERVED, _OBSERVED, limit, "")


def _task_row(
    *,
    status: str = "reviewing",
    state_token: Any = _STATE_TOKEN,
    canonical_branch: Any = _BRANCH,
    workspace_id: Any = _WORKSPACE_ID,
    current_gate_id: Any = None,
) -> tuple[Any, ...]:
    return (
        _TASK_ID,
        workspace_id,
        _REPOSITORY_ID,
        503,
        _ISSUE_NUMBER,
        status,
        None,
        canonical_branch,
        state_token,
        None,
        current_gate_id,
        "a" * 64,
        None,
        _OBSERVED,
        _OBSERVED,
    )


def _pr_row(*, head_sha: str = _HEAD_SHA, task_id: Any = _TASK_ID) -> tuple[Any, ...]:
    return (
        _PR_ID,
        _WORKSPACE_ID,
        task_id,
        _REPOSITORY_ID,
        _GITHUB_PR_ID,
        _PR_NUMBER,
        _BRANCH,
        "main",
        head_sha,
        "open",
        None,
        _OBSERVED,
        _OBSERVED,
    )


def _session_row(role: str, session_id: Any, external_id: str) -> tuple[Any, ...]:
    return (
        session_id,
        _WORKSPACE_ID,
        _TASK_ID,
        role,
        _CONNECTION_ID,
        external_id,
        "ready",
        {"repository_base": "/workspace/task-42"},
        None,
        None,
        None,
        _OBSERVED,
        None,
        _OBSERVED,
        _OBSERVED,
    )


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
        987654321,
        "octocat",
        "Organization",
        None,
        _OBSERVED,
        _OBSERVED,
    )


def _loop_row(*, status: str = "open", limit: int = 2) -> tuple[Any, ...]:
    return (_LOOP_ID, _WORKSPACE_ID, _TASK_ID, "pr_review", limit, status, None, _OBSERVED)


def _iteration_row(
    *,
    iteration_id: Any = _ITERATION_ID,
    number: int = 1,
    pr_id: Any = _PR_ID,
    head_sha: str = _HEAD_SHA,
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
        None,
        pr_id,
        head_sha,
        outcome,
        summary,
        findings,
        decided_at,
        _OBSERVED,
    )


def _execution_row(
    *,
    execution_number: int = 1,
    status: str = "succeeded",
    result_payload: Any = _IMPLEMENTATION_PAYLOAD,
    claimed_iteration_id: Any = None,
) -> tuple[Any, ...]:
    return (
        uuid.uuid4(),
        _WORKSPACE_ID,
        _TASK_ID,
        uuid.uuid4(),
        execution_number,
        status,
        _OBSERVED,
        _OBSERVED,
        result_payload,
        None,
        claimed_iteration_id,
    )


def _observation(*, head_sha: str = _HEAD_SHA) -> GitHubPullRequestObservation:
    return GitHubPullRequestObservation(
        github_pr_id=_GITHUB_PR_ID,
        pull_number=_PR_NUMBER,
        head_ref=_BRANCH,
        head_sha=head_sha,
        base_ref="main",
        state="open",
        merged=False,
        merged_at=None,
    )


class FakeGitHubClient:
    """Scripted GitHub observation fake recording call order; zero calls is
    assertable. Each call also proves no database transaction was open."""

    def __init__(self, pool: FakePool, *, pull_request: GitHubPullRequestObservation) -> None:
        self._pool = pool
        self._pull_request = pull_request
        self.repository_calls: list[tuple[int, int]] = []
        self.pull_request_calls: list[tuple[int, str, str, int]] = []

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

    def get_repository_pull_request(
        self,
        *,
        github_installation_id: int,
        owner_login: str,
        repository_name: str,
        pull_number: int,
    ) -> GitHubPullRequestObservation:
        assert self._pool.active_connections == 0, (
            "the service must never hold a database transaction open across the GitHub call"
        )
        self.pull_request_calls.append(
            (github_installation_id, owner_login, repository_name, pull_number)
        )
        return self._pull_request


class _ReviewerAttachment:
    """Deferred Reviewer attachment probe: counts acquisitions and disposals."""

    def __init__(self, runtime: FakeAgentRuntimeAdapter) -> None:
        self._runtime = runtime
        self.acquisitions = 0
        self.disposals = 0
        self.bindings: list[Any] = []

    def __call__(self, binding: Any) -> AbstractContextManager[FakeAgentRuntimeAdapter]:
        self.bindings.append(binding)
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
    reviewer_outcomes: Sequence[Any] = (),
) -> FakeAgentRuntimeAdapter:
    """One fake runtime holding the live Reviewer session."""
    adapter = FakeAgentRuntimeAdapter()
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
    for outcome in reviewer_outcomes:
        adapter.queue_send(_REVIEWER_EXTERNAL_ID, outcome)
    return adapter


def _task_domain(*, status: str = "reviewing", state_token: Any = _STATE_TOKEN) -> Any:
    from openorc.domain.tasks import Task, TaskStatus

    return Task(
        id=_TASK_ID,
        workspace_id=_WORKSPACE_ID,
        repository_id=_REPOSITORY_ID,
        github_issue_id=503,
        github_issue_number=_ISSUE_NUMBER,
        status=TaskStatus(status),
        archived_at=None,
        canonical_feature_branch=_BRANCH,
        state_token=state_token,
        current_plan_revision_id=None,
        current_owner_gate_id=None,
        source_requirements_fingerprint="a" * 64,
        queued_at=None,
        created_at=_OBSERVED,
        updated_at=_OBSERVED,
    )


def _pr_domain(head_sha: str = _HEAD_SHA) -> Any:
    from openorc.domain.pull_requests import TaskPullRequest, TaskPullRequestState

    return TaskPullRequest(
        id=_PR_ID,
        workspace_id=_WORKSPACE_ID,
        task_id=_TASK_ID,
        repository_id=_REPOSITORY_ID,
        github_pr_id=_GITHUB_PR_ID,
        github_pr_number=_PR_NUMBER,
        head_ref=_BRANCH,
        base_ref="main",
        head_sha=head_sha,
        state=TaskPullRequestState.OPEN,
        merged_at=None,
        created_at=_OBSERVED,
        updated_at=_OBSERVED,
    )


def _published(head_sha: str = _HEAD_SHA) -> TaskPrPublished:
    """E8's typed publication facts for the initial handoff."""
    return TaskPrPublished(task=_task_domain(), pull_request=_pr_domain(head_sha))


def _script_phase_a0(conn: ScriptedConnection) -> None:
    """The authenticated admission transaction: barrier + exact Owner scope."""
    conn.on(_ACCOUNT_BARRIER, _ACCOUNT_OPERATIONAL_ROW)
    conn.on(_WORKSPACE_READ, _ws_row())
    conn.on(_TASK_READ, _task_row())


def _script_reconcile(conn: ScriptedConnection) -> None:
    """B7's phase-1 reads, the phase-3 locked write phase (an identical
    observation is a true durable no-op), and the post-reconciliation reads."""
    conn.on(_TASK_READ, _task_row())
    conn.on(_PR_READ, _pr_row())
    conn.on(_REPO_READ, _repository_row())
    conn.on(_INSTALLATION_READ, _installation_row())
    conn.on(_WORKSPACE_READ, _ws_row())
    conn.on(_ACCOUNT_BARRIER, _ACCOUNT_OPERATIONAL_ROW)
    conn.on(_REPO_READ_LOCKED, _repository_row())
    conn.on(_PR_READ_LOCKED, _pr_row())
    conn.on(_TASK_READ, _task_row())
    conn.on(_REPO_READ, _repository_row())


def _script_reservation(conn: ScriptedConnection, *, iterations: list[tuple[Any, ...]]) -> None:
    """Phase B: the Task-lock re-proofs, the READY Reviewer, the single OPEN
    PR loop (established once, limit snapshot 2), the guards, the reservation."""
    conn.on(_ACCOUNT_BARRIER, _ACCOUNT_OPERATIONAL_ROW)
    conn.on(_WORKSPACE_READ, _ws_row())
    conn.on(_TASK_READ, _task_row())
    conn.on(_TASK_READ_LOCKED, _task_row())
    conn.on(_TASK_READ, _task_row())
    conn.on(_PR_READ, _pr_row())
    conn.on(_SESSION_READ, _session_row("reviewer", _REVIEWER_SESSION_ID, _REVIEWER_EXTERNAL_ID))
    conn.on(_WORKSPACE_READ, _ws_row())
    conn.on(_LOOPS_LIST, [])
    conn.on(_LOOP_INSERT, _loop_row())
    conn.on(_LOOP_READ_LOCKED, _loop_row())
    conn.on_repeat(_ITERATIONS_LIST, iterations)
    conn.on(_EXECUTIONS_LIST, [_execution_row()])
    conn.on(_LOOP_GUARD_LOCKED, ("open", "pr_review"))
    conn.on(_ITERATION_INSERT, _iteration_row())


def _count(conn: ScriptedConnection, predicate: _Predicate) -> int:
    return sum(1 for sql, _params in conn.executed if predicate(sql, None))


def _dispatch_call(
    pool: DatabasePool,
    reviewer_attachment: _ReviewerAttachment,
    github: Any,
    *,
    published: TaskPrPublished | None = None,
    expected_state_token: Any = _STATE_TOKEN,
) -> TaskPrReviewDispatchOutcome:
    return dispatch_pr_review(
        pool,
        reviewer_attachment,
        github,
        profile_id=_OWNER_PROFILE_ID,
        workspace_id=_WORKSPACE_ID,
        task_id=_TASK_ID,
        expected_state_token=expected_state_token,
        published=published if published is not None else _published(),
    )


_REVIEWED_ITERATION_ROW = _iteration_row(
    iteration_id=uuid.uuid4(),
    number=1,
    outcome="changes_requested",
    summary="Needs work",
    findings=[{"summary": "a", "details": "b"}],
    decided_at=_OBSERVED,
)


def test_the_initial_handoff_dispatches_one_loop_iteration_and_the_structural_message() -> None:
    conn = ScriptedConnection()
    fake_pool = FakePool(conn)
    pool = _pool(conn)
    _script_phase_a0(conn)
    _script_reconcile(conn)
    _script_reservation(conn, iterations=[])
    conn.on(_PR_READ, _pr_row())
    adapter = _prepared_adapter(
        fake_pool, reviewer_outcomes=[accepted_review_result_candidate("Ship it.")]
    )
    reviewer_attachment = _ReviewerAttachment(adapter)
    github = FakeGitHubClient(fake_pool, pull_request=_observation())

    outcome = _dispatch_call(pool, reviewer_attachment, github)

    # Exactly one OPEN PR_REVIEW loop with the Workspace limit snapshot and
    # one next-numbered unfinalized PR/head iteration.
    assert outcome.review_loop.id == _LOOP_ID
    assert outcome.review_loop.purpose.value == "pr_review"
    assert outcome.review_loop.iteration_limit == 2
    assert outcome.review_iteration.task_pull_request_id == _PR_ID
    assert outcome.review_iteration.reviewed_head_sha == _HEAD_SHA
    assert outcome.review_iteration.outcome is None
    assert outcome.review_result.summary == "Ship it."
    assert outcome.reconciled_head_sha == _HEAD_SHA
    # The ONE send: an established REVIEW interaction on the exact Reviewer
    # session whose entire message is the structural serialization of the
    # persisted implementation_result; the PR/head identity rides only in the
    # typed interaction subject.
    assert len(adapter.realized_controls()) == 1
    control = adapter.realized_controls()[0]
    assert control.external_session_id == _REVIEWER_EXTERNAL_ID
    assert control.expected_family == "review_result"
    assert control.message == serialize_implementation_result_message(_IMPLEMENTATION_PAYLOAD)
    interaction = control.interaction
    assert isinstance(interaction, ReviewInteraction)
    subject = cast(PrReviewSubject, interaction.subject)
    assert subject.repository_full_name == "octocat/hello-world"
    assert subject.pull_request_number == _PR_NUMBER
    assert subject.head_sha == _HEAD_SHA
    assert subject.issue_number == _ISSUE_NUMBER
    assert _HEAD_SHA not in (control.message or "")
    # GitHub was read exactly once per observation; never inside a
    # transaction (proven by the fake).
    assert len(github.repository_calls) == 1
    assert len(github.pull_request_calls) == 1
    assert reviewer_attachment.disposals == 1


def test_an_unauthorized_caller_causes_zero_github_reads_and_zero_pr_snapshot_mutation() -> None:
    conn = ScriptedConnection()
    fake_pool = FakePool(conn)
    pool = _pool(conn)
    # The authenticated admission transaction proves the exact Owner scope:
    # the addressed Workspace is owned by a different Profile, so the uniform
    # not-found vocabulary fires BEFORE any GitHub read and before any
    # PR-snapshot statement can run.
    conn.on(_ACCOUNT_BARRIER, _ACCOUNT_OPERATIONAL_ROW)
    conn.on(_WORKSPACE_READ, _ws_row(owner=_OTHER_PROFILE_ID))
    adapter = _prepared_adapter(fake_pool)
    reviewer_attachment = _ReviewerAttachment(adapter)
    github = FakeGitHubClient(fake_pool, pull_request=_observation())

    with pytest.raises(NotFoundError):
        _dispatch_call(pool, reviewer_attachment, github)

    assert github.repository_calls == []
    assert github.pull_request_calls == []
    assert len(adapter.realized_controls()) == 0
    assert reviewer_attachment.acquisitions == 0
    # The persisted reconciliation snapshot is untouched: no statement even
    # read the PR row, let alone updated it.
    assert not any("task_pull_requests" in sql for sql, _params in conn.executed)


def test_a_malformed_command_shape_is_rejected_before_any_sql_or_github_call() -> None:
    conn = ScriptedConnection()
    fake_pool = FakePool(conn)
    pool = _pool(conn)
    adapter = _prepared_adapter(fake_pool)
    reviewer_attachment = _ReviewerAttachment(adapter)
    github = FakeGitHubClient(fake_pool, pull_request=_observation())

    with pytest.raises(InvalidCommandError):
        _dispatch_call(
            pool,
            reviewer_attachment,
            github,
            published=cast(TaskPrPublished, object()),
        )
    with pytest.raises(InvalidCommandError):
        _dispatch_call(
            pool,
            reviewer_attachment,
            github,
            expected_state_token="not-a-uuid",
        )

    assert conn.executed == []
    assert github.repository_calls == []
    assert github.pull_request_calls == []
    assert reviewer_attachment.acquisitions == 0


def test_a_head_change_without_remediation_evidence_refuses_with_no_reservation() -> None:
    conn = ScriptedConnection()
    fake_pool = FakePool(conn)
    pool = _pool(conn)
    _script_phase_a0(conn)
    # B7 observes a NEW head: the persisted snapshot advances (the durable
    # reconciliation is truthful), but the dispatch's evidence is still the
    # initial completion — stale evidence never dispatches.
    conn.on(_TASK_READ, _task_row())
    conn.on(_PR_READ, _pr_row())
    conn.on(_REPO_READ, _repository_row())
    conn.on(_INSTALLATION_READ, _installation_row())
    conn.on(_WORKSPACE_READ, _ws_row())
    conn.on(_ACCOUNT_BARRIER, _ACCOUNT_OPERATIONAL_ROW)
    conn.on(_REPO_READ_LOCKED, _repository_row())
    conn.on(_PR_READ_LOCKED, _pr_row())
    conn.on(_PR_UPDATE, _pr_row(head_sha=_NEW_HEAD_SHA))
    conn.on(_TASK_READ, _task_row())
    conn.on(_REPO_READ, _repository_row())
    conn.on(_ACCOUNT_BARRIER, _ACCOUNT_OPERATIONAL_ROW)
    conn.on(_WORKSPACE_READ, _ws_row())
    conn.on(_TASK_READ, _task_row())
    conn.on(_TASK_READ_LOCKED, _task_row())
    conn.on(_TASK_READ, _task_row())
    conn.on(_PR_READ, _pr_row(head_sha=_NEW_HEAD_SHA))
    conn.on(_SESSION_READ, _session_row("reviewer", _REVIEWER_SESSION_ID, _REVIEWER_EXTERNAL_ID))
    conn.on(_WORKSPACE_READ, _ws_row())
    conn.on(_LOOPS_LIST, [])
    conn.on(_LOOP_INSERT, _loop_row())
    conn.on(_LOOP_READ_LOCKED, _loop_row())
    conn.on_repeat(_ITERATIONS_LIST, [])
    conn.on(_EXECUTIONS_LIST, [_execution_row()])
    adapter = _prepared_adapter(fake_pool)
    reviewer_attachment = _ReviewerAttachment(adapter)
    github = FakeGitHubClient(fake_pool, pull_request=_observation(head_sha=_NEW_HEAD_SHA))

    with pytest.raises(StaleOperationError):
        _dispatch_call(pool, reviewer_attachment, github)

    # The reconciliation persisted the GitHub-observed head, but no review
    # iteration was reserved and nothing was sent.
    assert _count(conn, _PR_UPDATE) == 1
    assert _count(conn, _ITERATION_INSERT) == 0
    assert len(adapter.realized_controls()) == 0


def test_pre_send_head_movement_is_a_never_dispatched_dead_letter() -> None:
    conn = ScriptedConnection()
    fake_pool = FakePool(conn)
    pool = _pool(conn)
    _script_phase_a0(conn)
    _script_reconcile(conn)
    _script_reservation(conn, iterations=[])
    # The pre-send durable recheck observes a moved head: fail closed
    # without sending.
    conn.on(_PR_READ, _pr_row(head_sha=_NEW_HEAD_SHA))
    adapter = _prepared_adapter(
        fake_pool, reviewer_outcomes=[accepted_review_result_candidate("Ship it.")]
    )
    reviewer_attachment = _ReviewerAttachment(adapter)
    github = FakeGitHubClient(fake_pool, pull_request=_observation())

    with pytest.raises(StaleOperationError):
        _dispatch_call(pool, reviewer_attachment, github)

    # The reservation committed (the dead letter is bound to a superseded
    # head and stays unfinalized for recovery), and no message was ever sent.
    assert _count(conn, _ITERATION_INSERT) == 1
    assert len(adapter.realized_controls()) == 0


def test_an_uncertain_send_leaves_the_reserved_iteration_unresolved() -> None:
    conn = ScriptedConnection()
    fake_pool = FakePool(conn)
    pool = _pool(conn)
    _script_phase_a0(conn)
    _script_reconcile(conn)
    _script_reservation(conn, iterations=[])
    conn.on(_PR_READ, _pr_row())
    adapter = _prepared_adapter(
        fake_pool,
        reviewer_outcomes=[AgentRuntimeUncertainOutcomeError("the outcome is unknown")],
    )
    reviewer_attachment = _ReviewerAttachment(adapter)
    github = FakeGitHubClient(fake_pool, pull_request=_observation())

    with pytest.raises(ExternalOperationUncertainError):
        _dispatch_call(pool, reviewer_attachment, github)

    # The reserved attempt stays unfinalized; the single possibly-delivered
    # send is never resent.
    assert _count(conn, _ITERATION_INSERT) == 1
    assert len(adapter.realized_controls()) == 1


def test_duplicate_dispatch_for_an_already_reviewed_head_refuses() -> None:
    conn = ScriptedConnection()
    fake_pool = FakePool(conn)
    pool = _pool(conn)
    _script_phase_a0(conn)
    _script_reconcile(conn)
    conn.on(_ACCOUNT_BARRIER, _ACCOUNT_OPERATIONAL_ROW)
    conn.on(_WORKSPACE_READ, _ws_row())
    conn.on(_TASK_READ, _task_row())
    conn.on(_TASK_READ_LOCKED, _task_row())
    conn.on(_TASK_READ, _task_row())
    conn.on(_PR_READ, _pr_row())
    conn.on(_SESSION_READ, _session_row("reviewer", _REVIEWER_SESSION_ID, _REVIEWER_EXTERNAL_ID))
    conn.on(_WORKSPACE_READ, _ws_row())
    conn.on(_LOOPS_LIST, [_loop_row()])
    conn.on(_LOOP_READ_LOCKED, _loop_row())
    conn.on(_ITERATIONS_LIST, [_REVIEWED_ITERATION_ROW])
    adapter = _prepared_adapter(
        fake_pool, reviewer_outcomes=[accepted_review_result_candidate("Ship it.")]
    )
    reviewer_attachment = _ReviewerAttachment(adapter)
    github = FakeGitHubClient(fake_pool, pull_request=_observation())

    with pytest.raises(ConflictError):
        _dispatch_call(pool, reviewer_attachment, github)

    assert _count(conn, _ITERATION_INSERT) == 0
    assert len(adapter.realized_controls()) == 0
