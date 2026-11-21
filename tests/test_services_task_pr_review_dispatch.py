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

from openorc.adapters.agent_runtime.results import AgentSessionCreationRequest
from openorc.adapters.github import GitHubPullRequestObservation, GitHubRepositoryObservation
from openorc.domain.connections import WorkflowRole
from openorc.persistence.pool import DatabasePool
from openorc.services import task_pr_review_dispatch
from openorc.services.task_pr_composition import TaskPrPublished
from tests.fakes.agent_runtime import (
    FakeAgentRuntimeAdapter,
    session_ready_candidate,
)
from tests.fakes.task_rows import task_row

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
    """Pool fake tracking checked-out connections for the transaction proof.

    ``checkouts`` counts every ``connection()`` acquisition, so a focused
    test can prove that a composed transaction's operations share one
    transaction-scoped connection (one checkout) instead of each repository
    call acquiring its own (one checkout per statement)."""

    def __init__(self, conn: ScriptedConnection) -> None:
        self._conn = conn
        self.active_connections = 0
        self.checkouts = 0

    def connection(self) -> Any:
        self.checkouts += 1

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
_ITERATION_READ = _sql_has("from openorc.review_iterations where id")
_ITERATION_INSERT = _sql_has("insert into openorc.review_iterations")
_EXECUTIONS_LIST = _sql_has("from openorc.executions where task_id")
_GATES_LIST = _sql_has("from openorc.owner_gates where task_id")


def _ws_row(*, owner: Any = _OWNER_PROFILE_ID, limit: int = 2) -> tuple[Any, ...]:
    return (_WORKSPACE_ID, owner, "platform", _OBSERVED, _OBSERVED, limit, "", None)


def _task_row(
    *,
    status: str = "reviewing",
    state_token: Any = _STATE_TOKEN,
    canonical_branch: Any = _BRANCH,
    workspace_id: Any = _WORKSPACE_ID,
    current_gate_id: Any = None,
    terminalization_outcome: Any = None,
    terminalization_origin: Any = None,
    terminalization_established_at: Any = None,
    cleanup_send_claimed_at: Any = None,
    cleanup_send_settled_at: Any = None,
) -> tuple[Any, ...]:
    return task_row(
        id=_TASK_ID,
        workspace_id=workspace_id,
        repository_id=_REPOSITORY_ID,
        github_issue_id=503,
        github_issue_number=_ISSUE_NUMBER,
        status=status,
        canonical_feature_branch=canonical_branch,
        state_token=state_token,
        current_owner_gate_id=current_gate_id,
        source_requirements_fingerprint="a" * 64,
        created_at=_OBSERVED,
        updated_at=_OBSERVED,
        terminalization_outcome=terminalization_outcome,
        terminalization_origin=terminalization_origin,
        terminalization_established_at=terminalization_established_at,
        cleanup_send_claimed_at=cleanup_send_claimed_at,
        cleanup_send_settled_at=cleanup_send_settled_at,
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


def _session_row(
    role: str, session_id: Any, external_id: str, *, lifecycle: str = "ready"
) -> tuple[Any, ...]:
    return (
        session_id,
        _WORKSPACE_ID,
        _TASK_ID,
        role,
        _CONNECTION_ID,
        external_id,
        lifecycle,
        {"repository_base": "/workspace/task-42"},
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


def _reviewer_binding_domain() -> Any:
    from openorc.domain.connections import WorkflowRole
    from openorc.domain.sessions import TaskAgentSession, TaskSessionLifecycleStatus

    return TaskAgentSession(
        id=_REVIEWER_SESSION_ID,
        workspace_id=_WORKSPACE_ID,
        task_id=_TASK_ID,
        role=WorkflowRole.REVIEWER,
        connection_id=_CONNECTION_ID,
        external_session_id=_REVIEWER_EXTERNAL_ID,
        lifecycle_status=TaskSessionLifecycleStatus.READY,
        effective_config_snapshot={"repository_base": "/workspace/task-42"},
        reported_provider=None,
        reported_model=None,
        reported_runtime_version=None,
        initialized_at=_OBSERVED,
        ended_at=None,
        created_at=_OBSERVED,
        updated_at=_OBSERVED,
    )


def _approved_pr_authorization_gate_row() -> tuple[Any, ...]:
    """The durable E7 authorization history: the durably APPROVED
    ``PR_AUTHORIZATION`` gate binding exactly the initial authorized head
    (the exact head-SHA subject form, no PR binding)."""
    return (
        uuid.uuid4(),
        _WORKSPACE_ID,
        _TASK_ID,
        "pr_authorization",
        "approved",
        None,
        _HEAD_SHA,
        None,
        None,
        _OBSERVED,
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
    conn.on(_GATES_LIST, [_approved_pr_authorization_gate_row()])
    conn.on(_LOOP_GUARD_LOCKED, ("open", "pr_review"))
    conn.on(_ITERATION_INSERT, _iteration_row())


def _count(conn: ScriptedConnection, predicate: _Predicate) -> int:
    return sum(1 for sql, _params in conn.executed if predicate(sql, None))


_REVIEWED_ITERATION_ROW = _iteration_row(
    iteration_id=uuid.uuid4(),
    number=1,
    outcome="changes_requested",
    summary="Needs work",
    findings=[{"summary": "a", "details": "b"}],
    decided_at=_OBSERVED,
)


def test_the_post_attachment_revalidation_shares_one_transaction_scoped_connection() -> None:
    """Finding 1 regression: the post-attachment currentness proof executes
    its Task-lock, Task/token/status, PR identity/head, and Reviewer-binding
    operations inside ONE composed transaction — the pool is checked out
    exactly once and every statement runs on that same transaction-scoped
    connection, so the Task row lock is held across the whole proof and a
    concurrent Task transition can never interleave between the checks."""
    conn = ScriptedConnection()
    # The focused test passes the counting pool itself, so the checkout
    # counter observes exactly what the helper acquires.
    fake_pool = FakePool(conn)
    pool = cast(DatabasePool, fake_pool)
    conn.on(_TASK_READ_LOCKED, _task_row())
    conn.on(_TASK_READ, _task_row())
    conn.on(_PR_READ, _pr_row())
    conn.on(_SESSION_READ, _session_row("reviewer", _REVIEWER_SESSION_ID, _REVIEWER_EXTERNAL_ID))

    task = task_pr_review_dispatch._revalidate_reserved_subjects(
        pool,
        workspace_id=_WORKSPACE_ID,
        task_id=_TASK_ID,
        expected_state_token=_STATE_TOKEN,
        expected_pull_request_id=_PR_ID,
        reserved_head_sha=_HEAD_SHA,
        reviewer_session=_reviewer_binding_domain(),
    )

    assert task.id == _TASK_ID
    # One composed transaction: exactly one checkout, all four statements on
    # that single connection, and the connection released afterward.
    assert fake_pool.checkouts == 1
    assert len(conn.executed) == 4
    assert fake_pool.active_connections == 0


# --- the detached regular-mode dispatch (issue #338) ----------------------------------

_RECEIPT_ID_DETACHED = uuid.uuid4()
_TURN_REFERENCE_DETACHED = "detached-turn-1"
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
        _RECEIPT_ID_DETACHED,
        _WORKSPACE_ID,
        _TASK_ID,
        _REVIEWER_SESSION_ID,
        "reviewer_review",
        "review_iteration",
        _ITERATION_ID,
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
    """Records the ONE post-commit completion handoff invocations (issue #338)."""

    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []
        self.interrupt_calls: list[dict[str, Any]] = []

    def handoff_settled_completion(
        self,
        *,
        workspace_id: Any,
        task_id: Any,
        operation: Any,
        validated_result: Any = None,
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


def _script_detached_attach(conn: ScriptedConnection) -> None:
    """The acknowledgement attach (claimed → admitted, no staged completion)."""
    conn.on(_RECEIPT_FOR_UPDATE, _detached_receipt_row())
    conn.on(
        _RECEIPT_ATTACH,
        _detached_receipt_row(status="admitted", admitted_turn_reference=_TURN_REFERENCE_DETACHED),
    )
    conn.on(_EARLY_COMPLETION_READ, None)
