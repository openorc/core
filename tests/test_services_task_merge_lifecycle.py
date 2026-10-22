"""Deterministic tests for the E10a merge lifecycle seams (issue #227).

Scripted SQL-connection seams, a stubbed B7 reconciliation, and a stubbed
exact-head merge primitive prove the Owner merge command and the shared
GitHub-confirmed completion on their critical paths:

- the Owner command proves the exact Profile/Workspace/Task authority, the
  exact current pending MERGE_DECISION bound to the canonical PR and its
  Reviewer-accepted head, and the OPEN PR_REVIEW loop before anything
  external; one composed transaction resolves the gate APPROVED and records
  the OWNER OWNER_GATE_RESOLVED + MERGE_REQUESTED audit pair (the one-shot
  claim), and the primitive is called exactly once with the gate's SHA, the
  approving Profile, and the durable claim-revalidation hook;
- a GitHub-confirmed merged PR takes the shared completion path without
  consuming the gate; a GitHub-owned refusal and a definitive pre-delivery
  failure are distinguished by the primitive's machine-readable error
  subtype (only the former records MERGE_REJECTED_BY_GITHUB), and after a
  proven no-merge state a NEW pending exact-head gate requires fresh Owner
  authorization; an expected-head mismatch blocks STALE_OPERATION; an
  uncertain or unconfirmable outcome blocks EXTERNAL_OPERATION_UNCERTAIN
  and the merge request is never repeated;
- the issue-open guard refuses a closed source issue without consuming the
  pending gate and fails closed on unavailable/mismatched reads;
- the shared completion reloads durable merged facts under lock, closes the
  OPEN PR_REVIEW loop, archives COMPLETED with PR_MERGED + TASK_COMPLETED
  exactly once, leaves no actionable current gate, never resurrects archived
  CANCELLED history, and is an idempotent no-op when already completed.

No live GitHub, runtime, or Postgres access; the B7 reconciliation and the
merge primitive are stubbed at their module seams (their own modules own
those matrices).
"""

from __future__ import annotations

from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any, cast
from uuid import uuid4

import pytest

from openorc.adapters.github import (
    GitHubAuthorizationRejectedError,
    GitHubIssueObservation,
    GitHubRepositoryObservation,
)
from openorc.domain.pull_requests import TaskPullRequest, TaskPullRequestState
from openorc.domain.tasks import TaskStatus
from openorc.services import (
    pull_request_merge,
    task_pull_request_reconciliation,
)
from openorc.services.errors import (
    AuthorizationError,
    ConflictError,
    ExternalOperationFailedError,
    ExternalOperationRejectedError,
    ExternalOperationUncertainError,
    InvalidCommandError,
    MergeConfirmedReconciliationPendingError,
    NotFoundError,
    StaleOperationError,
)
from openorc.services.task_merge_lifecycle import (
    TaskMergeCompletionKind,
    TaskMergeOutcomeKind,
    complete_confirmed_task_merge,
    request_task_merge,
)
from openorc.services.task_pull_request_reconciliation import TaskPullRequestReconciliation
from tests.test_services_task_pr_review_decision import (
    _EVENT_INSERT,
    _GATE_INSERT,
    _GATE_READ,
    _GATE_UPDATE,
    _STATUS_UPDATE,
    _TASK_GATE_CLEAR,
    _TASK_GATE_INSTALL,
    _event_returned_row,
    _event_types,
    _gate_row,
)
from tests.test_services_task_pr_review_dispatch import (
    _ACCOUNT_BARRIER,
    _ACCOUNT_OPERATIONAL_ROW,
    _GITHUB_REPOSITORY_ID,
    _HEAD_SHA,
    _INSTALLATION_READ,
    _ISSUE_NUMBER,
    _LOOP_ID,
    _LOOPS_LIST,
    _OWNER_PROFILE_ID,
    _PR_ID,
    _PR_NUMBER,
    _PR_READ,
    _REPO_READ,
    _REPOSITORY_ID,
    _STATE_TOKEN,
    _TASK_ID,
    _TASK_READ,
    _TASK_READ_LOCKED,
    _WORKSPACE_ID,
    _WORKSPACE_READ,
    ScriptedConnection,
    _count,
    _installation_row,
    _loop_row,
    _pool,
    _repository_row,
    _sql_has,
    _task_row,
    _ws_row,
)

_OBSERVED = datetime(2026, 10, 22, 12, 0, 0, tzinfo=UTC)
_MERGE_COMMIT_SHA = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
_NEW_HEAD_SHA = "fedcba9876543210fedcba9876543210fedcba98"
_POST_APPROVAL_TOKEN = uuid4()
_RETRY_TOKEN = uuid4()
_COMPLETED_TOKEN = uuid4()
_BLOCKED_TOKEN = uuid4()
_RETRY_GATE_ID = uuid4()

_BLOCK_INSERT = _sql_has("insert into openorc.task_blocks")
_LOOP_CLOSE = _sql_has("update openorc.review_loops")
_ARCHIVE_UPDATE = _sql_has("update openorc.tasks", "archived_at = now()")


def _guard(*, issues: list[Any], repository: Any = None) -> Any:
    """The issue-open-guard fake, cast to the adapter boundary for the call."""
    return cast(Any, _IssueGuardGithub(issues=issues, repository=repository))


def _token_resolver() -> Any:
    """The unused user-token resolver fake for the stubbed primitive seam."""
    return cast(Any, object())


def _domain_pull_request(
    *,
    state: TaskPullRequestState = TaskPullRequestState.OPEN,
    merged_at: datetime | None = None,
    head_sha: str = _HEAD_SHA,
) -> TaskPullRequest:
    return TaskPullRequest(
        id=_PR_ID,
        workspace_id=_WORKSPACE_ID,
        task_id=_TASK_ID,
        repository_id=_REPOSITORY_ID,
        github_pr_id=555,
        github_pr_number=_PR_NUMBER,
        head_ref="openorc/task-1",
        base_ref="main",
        head_sha=head_sha,
        state=state,
        merged_at=merged_at,
        created_at=_OBSERVED,
        updated_at=_OBSERVED,
    )


def _pr_row_values(
    *,
    state: str = "open",
    merged_at: datetime | None = None,
    head_sha: str = _HEAD_SHA,
) -> tuple[Any, ...]:
    return (
        _PR_ID,
        _WORKSPACE_ID,
        _TASK_ID,
        _REPOSITORY_ID,
        555,
        _PR_NUMBER,
        "openorc/task-1",
        "main",
        head_sha,
        state,
        merged_at,
        _OBSERVED,
        _OBSERVED,
    )


def _archived_task_row(*, state_token: Any) -> tuple[Any, ...]:
    row = list(_task_row(status="completed", state_token=state_token, current_gate_id=None))
    row[6] = _OBSERVED  # archived_at
    return tuple(row)


def _waiting_task_row(
    *, state_token: Any, current_gate_id: Any = _RETRY_GATE_ID
) -> tuple[Any, ...]:
    return _task_row(
        status="waiting_for_owner", state_token=state_token, current_gate_id=current_gate_id
    )


def _reconciled(*, pull_request: TaskPullRequest) -> TaskPullRequestReconciliation:
    return TaskPullRequestReconciliation(
        pull_request=pull_request,
        updated=True,
        head_changed=False,
        base_changed=False,
        state_changed=False,
        previous_head_sha=None,
        previous_base_ref=None,
        previous_state=None,
    )


class _ReconcileStub:
    """Stubbed B7 reconciliation returning scripted outcomes in order."""

    def __init__(self, *results: Any) -> None:
        self._results = list(results)
        self.calls: list[dict[str, Any]] = []

    def __call__(self, pool: Any, github: Any, *, workspace_id: Any, task_id: Any) -> Any:
        self.calls.append({"workspace_id": workspace_id, "task_id": task_id})
        result = self._results.pop(0)
        if isinstance(result, Exception):
            raise result
        return result


class _MergePrimitiveStub:
    """Stubbed exact-head merge primitive capturing the durable claim hook."""

    def __init__(self, *results: Any) -> None:
        self._results = list(results)
        self.calls: list[dict[str, Any]] = []

    def __call__(self, pool: Any, github: Any, user_token_resolver: Any, **kwargs: Any) -> Any:
        self.calls.append(dict(kwargs))
        result = self._results.pop(0)
        if isinstance(result, Exception):
            raise result
        return result


class _IssueGuardGithub:
    """Fake GitHub client for the routed repository + issue-open guard reads."""

    def __init__(self, *, issues: list[Any], repository: Any = None) -> None:
        self._issues = list(issues)
        self._repository = repository
        self.repository_calls: list[tuple[int, int]] = []
        self.issue_calls: list[int] = []

    def get_installation_repository(
        self, *, github_installation_id: int, github_repository_id: int
    ) -> Any:
        self.repository_calls.append((github_installation_id, github_repository_id))
        if isinstance(self._repository, Exception):
            raise self._repository
        return self._repository

    def get_repository_issue(
        self,
        *,
        github_installation_id: int,
        owner_login: str,
        repository_name: str,
        issue_number: int,
    ) -> Any:
        self.issue_calls.append(issue_number)
        result = self._issues.pop(0)
        if isinstance(result, Exception):
            raise result
        return result


def _repository_observation() -> GitHubRepositoryObservation:
    return GitHubRepositoryObservation(
        github_repository_id=_GITHUB_REPOSITORY_ID,
        owner_login="octocat",
        name="hello-world",
        html_url="https://github.com/octocat/hello-world",
        is_private=False,
        default_branch="main",
    )


def _issue_observation(
    *, state: str = "open", issue_id: int = 503, is_pull_request: bool = False
) -> GitHubIssueObservation:
    return GitHubIssueObservation(
        github_issue_id=issue_id,
        issue_number=_ISSUE_NUMBER,
        title="t",
        body=None,
        state=state,
        provider_updated_at=_OBSERVED,
        is_pull_request=is_pull_request,
    )


def _wire(
    monkeypatch: pytest.MonkeyPatch,
    *,
    reconcile: _ReconcileStub,
    merge: _MergePrimitiveStub,
) -> None:
    monkeypatch.setattr(task_pull_request_reconciliation, "reconcile_task_pull_request", reconcile)
    monkeypatch.setattr(pull_request_merge, "request_pull_request_merge", merge)


def _script_phase_a(conn: ScriptedConnection) -> None:
    """The Owner authority proof plus the exact-subject proof of Phase A."""
    conn.on(_ACCOUNT_BARRIER, _ACCOUNT_OPERATIONAL_ROW)
    conn.on(_WORKSPACE_READ, _ws_row())
    conn.on(
        _TASK_READ,
        _task_row(status="waiting_for_owner", current_gate_id=_RETRY_GATE_ID),
    )
    conn.on(
        _TASK_READ,
        _task_row(status="waiting_for_owner", current_gate_id=_RETRY_GATE_ID),
    )
    conn.on(_GATE_READ, _gate_row(gate_id=_RETRY_GATE_ID, gate_type="merge_decision"))
    conn.on(_PR_READ, _pr_row_values())
    conn.on(_LOOPS_LIST, [_loop_row()])


def _script_issue_guard(conn: ScriptedConnection) -> None:
    conn.on(_REPO_READ, _repository_row())
    conn.on(_INSTALLATION_READ, _installation_row())


def _script_phase_d(conn: ScriptedConnection) -> None:
    """Re-lock/revalidation plus the one-shot APPROVED resolution and its audit pair."""
    conn.on(
        _TASK_READ,
        _task_row(status="waiting_for_owner", current_gate_id=_RETRY_GATE_ID),
    )
    conn.on(_GATE_READ, _gate_row(gate_id=_RETRY_GATE_ID, gate_type="merge_decision"))
    conn.on(_PR_READ, _pr_row_values())
    conn.on(_LOOPS_LIST, [_loop_row()])
    conn.on(
        _TASK_READ,
        _task_row(status="waiting_for_owner", current_gate_id=_RETRY_GATE_ID),
    )
    conn.on(_GATE_READ, _gate_row(gate_id=_RETRY_GATE_ID, gate_type="merge_decision"))
    conn.on(_GATE_READ, _gate_row(gate_id=_RETRY_GATE_ID, gate_type="merge_decision"))
    conn.on(
        _TASK_READ_LOCKED,
        _task_row(status="waiting_for_owner", current_gate_id=_RETRY_GATE_ID),
    )
    conn.on(
        _GATE_UPDATE,
        _gate_row(gate_id=_RETRY_GATE_ID, gate_type="merge_decision", status="approved"),
    )
    conn.on(
        _TASK_GATE_CLEAR,
        _task_row(
            status="waiting_for_owner",
            state_token=_POST_APPROVAL_TOKEN,
            current_gate_id=None,
        ),
    )
    conn.on(_EVENT_INSERT, _event_returned_row("owner_gate_resolved"))
    conn.on(_EVENT_INSERT, _event_returned_row("merge_requested"))


def _script_completion(
    conn: ScriptedConnection,
    *,
    task_token: Any = _STATE_TOKEN,
    pr_row: tuple[Any, ...] | None = None,
    loops: list[tuple[Any, ...]] | None = None,
) -> None:
    conn.on(_WORKSPACE_READ, _ws_row())
    conn.on(_ACCOUNT_BARRIER, _ACCOUNT_OPERATIONAL_ROW)
    conn.on(
        _TASK_READ_LOCKED,
        _task_row(status="waiting_for_owner", state_token=task_token, current_gate_id=None),
    )
    conn.on(
        _PR_READ,
        pr_row if pr_row is not None else _pr_row_values(state="closed", merged_at=_OBSERVED),
    )
    conn.on(_LOOPS_LIST, loops if loops is not None else [_loop_row()])
    conn.on(
        _LOOP_CLOSE,
        (_LOOP_ID, _WORKSPACE_ID, _TASK_ID, "pr_review", 2, "closed", _OBSERVED, _OBSERVED),
    )
    conn.on(
        _TASK_READ,
        _task_row(status="waiting_for_owner", state_token=task_token, current_gate_id=None),
    )
    conn.on(_ARCHIVE_UPDATE, _archived_task_row(state_token=_COMPLETED_TOKEN))
    conn.on(_EVENT_INSERT, _event_returned_row("task_completed"))
    conn.on(_EVENT_INSERT, _event_returned_row("pr_merged"))


def _script_block(
    conn: ScriptedConnection, *, token: Any = _STATE_TOKEN, reason: str = "stale_operation"
) -> None:
    conn.on(_ACCOUNT_BARRIER, _ACCOUNT_OPERATIONAL_ROW)
    conn.on(_WORKSPACE_READ, _ws_row())
    conn.on(_TASK_READ, _task_row(status="waiting_for_owner", state_token=token))
    conn.on(_TASK_READ, _task_row(status="waiting_for_owner", state_token=token))
    conn.on(_BLOCK_INSERT, (uuid4(), _WORKSPACE_ID, _TASK_ID, reason, {}, None, _OBSERVED))
    conn.on(
        _STATUS_UPDATE,
        _task_row(status="blocked", state_token=_BLOCKED_TOKEN, current_gate_id=None),
    )
    conn.on(_EVENT_INSERT, _event_returned_row("task_blocked"))


def _script_retry_gate(conn: ScriptedConnection) -> None:
    conn.on(_ACCOUNT_BARRIER, _ACCOUNT_OPERATIONAL_ROW)
    conn.on(_WORKSPACE_READ, _ws_row())
    conn.on(
        _TASK_READ,
        _waiting_task_row(state_token=_POST_APPROVAL_TOKEN, current_gate_id=None),
    )
    conn.on(
        _TASK_READ,
        _waiting_task_row(state_token=_POST_APPROVAL_TOKEN, current_gate_id=None),
    )
    conn.on(
        _GATE_INSERT,
        _gate_row(gate_id=_RETRY_GATE_ID, gate_type="merge_decision", status="pending"),
    )
    conn.on(
        _TASK_READ,
        _waiting_task_row(state_token=_POST_APPROVAL_TOKEN, current_gate_id=None),
    )
    conn.on(_GATE_READ, _gate_row(gate_id=_RETRY_GATE_ID, gate_type="merge_decision"))
    conn.on(
        _TASK_GATE_INSTALL,
        _task_row(
            status="waiting_for_owner",
            state_token=_RETRY_TOKEN,
            current_gate_id=_RETRY_GATE_ID,
        ),
    )
    conn.on(_EVENT_INSERT, _event_returned_row("owner_gate_created"))


def test_malformed_command_is_rejected_before_any_state_or_telemetry() -> None:
    conn = ScriptedConnection()
    pool = _pool(conn)
    with pytest.raises(InvalidCommandError):
        request_task_merge(
            pool,
            _guard(issues=[]),
            _token_resolver(),
            profile_id=_OWNER_PROFILE_ID,
            workspace_id=cast(Any, "not-a-uuid"),
            task_id=_TASK_ID,
            owner_gate_id=_RETRY_GATE_ID,
            expected_state_token=_STATE_TOKEN,
        )
    assert conn.executed == []


def test_foreign_profile_authorization_fails_closed_before_any_github_read(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    conn = ScriptedConnection()
    pool = _pool(conn)
    conn.on(_ACCOUNT_BARRIER, None)
    reconcile = _ReconcileStub()
    merge = _MergePrimitiveStub()
    _wire(monkeypatch, reconcile=reconcile, merge=merge)
    github = _guard(issues=[])

    with pytest.raises(NotFoundError):
        request_task_merge(
            pool,
            github,
            _token_resolver(),
            profile_id=_OWNER_PROFILE_ID,
            workspace_id=_WORKSPACE_ID,
            task_id=_TASK_ID,
            owner_gate_id=_RETRY_GATE_ID,
            expected_state_token=_STATE_TOKEN,
        )

    assert github.issue_calls == []
    assert reconcile.calls == []
    assert merge.calls == []


def test_resolved_gate_is_a_stale_operation_with_no_external_work(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    conn = ScriptedConnection()
    pool = _pool(conn)
    conn.on(_ACCOUNT_BARRIER, _ACCOUNT_OPERATIONAL_ROW)
    conn.on(_WORKSPACE_READ, _ws_row())
    conn.on(_TASK_READ, _task_row(status="waiting_for_owner"))
    conn.on(_TASK_READ, _task_row(status="waiting_for_owner"))
    conn.on(
        _GATE_READ,
        _gate_row(gate_id=_RETRY_GATE_ID, gate_type="merge_decision", status="approved"),
    )
    reconcile = _ReconcileStub()
    merge = _MergePrimitiveStub()
    _wire(monkeypatch, reconcile=reconcile, merge=merge)

    with pytest.raises(StaleOperationError):
        request_task_merge(
            pool,
            _guard(issues=[]),
            _token_resolver(),
            profile_id=_OWNER_PROFILE_ID,
            workspace_id=_WORKSPACE_ID,
            task_id=_TASK_ID,
            owner_gate_id=_RETRY_GATE_ID,
            expected_state_token=_STATE_TOKEN,
        )

    assert reconcile.calls == []
    assert merge.calls == []
    assert _count(conn, _GATE_UPDATE) == 0


def test_gate_bound_to_a_different_pull_request_is_stale(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    conn = ScriptedConnection()
    pool = _pool(conn)
    conn.on(_ACCOUNT_BARRIER, _ACCOUNT_OPERATIONAL_ROW)
    conn.on(_WORKSPACE_READ, _ws_row())
    conn.on(_TASK_READ, _task_row(status="waiting_for_owner"))
    conn.on(_TASK_READ, _task_row(status="waiting_for_owner"))
    conn.on(
        _GATE_READ,
        _gate_row(gate_id=_RETRY_GATE_ID, gate_type="merge_decision", pr_id=uuid4()),
    )
    conn.on(_PR_READ, _pr_row_values())
    reconcile = _ReconcileStub()
    merge = _MergePrimitiveStub()
    _wire(monkeypatch, reconcile=reconcile, merge=merge)

    with pytest.raises(StaleOperationError):
        request_task_merge(
            pool,
            _guard(issues=[]),
            _token_resolver(),
            profile_id=_OWNER_PROFILE_ID,
            workspace_id=_WORKSPACE_ID,
            task_id=_TASK_ID,
            owner_gate_id=_RETRY_GATE_ID,
            expected_state_token=_STATE_TOKEN,
        )

    assert reconcile.calls == []
    assert merge.calls == []


def test_owner_merge_command_completes_the_task_atomically(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    conn = ScriptedConnection()
    pool = _pool(conn)
    _script_phase_a(conn)
    _script_issue_guard(conn)
    _script_phase_d(conn)
    _script_completion(conn, task_token=_POST_APPROVAL_TOKEN)
    reconcile = _ReconcileStub(_reconciled(pull_request=_domain_pull_request()))
    merge = _MergePrimitiveStub(SimpleNamespace(merge_commit_sha=_MERGE_COMMIT_SHA))
    _wire(monkeypatch, reconcile=reconcile, merge=merge)
    github = _guard(issues=[_issue_observation()], repository=_repository_observation())

    outcome = request_task_merge(
        pool,
        github,
        _token_resolver(),
        profile_id=_OWNER_PROFILE_ID,
        workspace_id=_WORKSPACE_ID,
        task_id=_TASK_ID,
        owner_gate_id=_RETRY_GATE_ID,
        expected_state_token=_STATE_TOKEN,
    )

    assert outcome.kind is TaskMergeOutcomeKind.COMPLETED
    assert outcome.task.status is TaskStatus.COMPLETED
    assert outcome.merge_commit_sha == _MERGE_COMMIT_SHA
    # Exactly one merge request with the gate's accepted SHA and the
    # approving Profile, carrying the durable claim-revalidation hook.
    assert len(merge.calls) == 1
    assert merge.calls[0]["profile_id"] == _OWNER_PROFILE_ID
    assert merge.calls[0]["workspace_id"] == _WORKSPACE_ID
    assert merge.calls[0]["task_id"] == _TASK_ID
    assert merge.calls[0]["expected_head_sha"] == _HEAD_SHA
    assert callable(merge.calls[0]["claim_revalidation"])
    assert reconcile.calls == [{"workspace_id": _WORKSPACE_ID, "task_id": _TASK_ID}]
    # The issue-open guard read the stable routed repository + issue ID.
    assert github.issue_calls == [_ISSUE_NUMBER]
    assert _event_types(conn) == [
        "owner_gate_resolved",
        "merge_requested",
        "task_completed",
        "pr_merged",
    ]


def test_confirmed_merged_pr_at_preflight_completes_without_consuming_the_gate(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    conn = ScriptedConnection()
    pool = _pool(conn)
    _script_phase_a(conn)
    _script_completion(conn, task_token=_STATE_TOKEN)
    reconcile = _ReconcileStub(
        _reconciled(
            pull_request=_domain_pull_request(
                state=TaskPullRequestState.CLOSED, merged_at=_OBSERVED
            )
        )
    )
    merge = _MergePrimitiveStub()
    _wire(monkeypatch, reconcile=reconcile, merge=merge)
    github = _guard(issues=[])

    outcome = request_task_merge(
        pool,
        github,
        _token_resolver(),
        profile_id=_OWNER_PROFILE_ID,
        workspace_id=_WORKSPACE_ID,
        task_id=_TASK_ID,
        owner_gate_id=_RETRY_GATE_ID,
        expected_state_token=_STATE_TOKEN,
    )

    assert outcome.kind is TaskMergeOutcomeKind.COMPLETED
    # No issue-open guard read, no merge request, and no gate consumption.
    assert github.issue_calls == []
    assert merge.calls == []
    assert _event_types(conn) == ["task_completed", "pr_merged"]
    assert _count(conn, _GATE_UPDATE) == 0


def test_closed_unmerged_preflight_refuses_without_consuming_the_gate(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    conn = ScriptedConnection()
    pool = _pool(conn)
    _script_phase_a(conn)
    reconcile = _ReconcileStub(
        _reconciled(
            pull_request=_domain_pull_request(state=TaskPullRequestState.CLOSED, merged_at=None)
        )
    )
    merge = _MergePrimitiveStub()
    _wire(monkeypatch, reconcile=reconcile, merge=merge)

    with pytest.raises(ConflictError):
        request_task_merge(
            pool,
            _guard(issues=[]),
            _token_resolver(),
            profile_id=_OWNER_PROFILE_ID,
            workspace_id=_WORKSPACE_ID,
            task_id=_TASK_ID,
            owner_gate_id=_RETRY_GATE_ID,
            expected_state_token=_STATE_TOKEN,
        )

    assert merge.calls == []
    assert _count(conn, _GATE_UPDATE) == 0
    assert _count(conn, _EVENT_INSERT) == 0


def test_changed_head_blocks_stale_operation_where_current(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    conn = ScriptedConnection()
    pool = _pool(conn)
    _script_phase_a(conn)
    _script_block(conn)
    reconcile = _ReconcileStub(
        _reconciled(pull_request=_domain_pull_request(head_sha=_NEW_HEAD_SHA))
    )
    merge = _MergePrimitiveStub()
    _wire(monkeypatch, reconcile=reconcile, merge=merge)

    outcome = request_task_merge(
        pool,
        _guard(issues=[]),
        _token_resolver(),
        profile_id=_OWNER_PROFILE_ID,
        workspace_id=_WORKSPACE_ID,
        task_id=_TASK_ID,
        owner_gate_id=_RETRY_GATE_ID,
        expected_state_token=_STATE_TOKEN,
    )

    assert outcome.kind is TaskMergeOutcomeKind.BLOCKED_STALE_OPERATION
    assert outcome.task.status is TaskStatus.BLOCKED
    assert outcome.task_block is not None
    assert outcome.task_block.reason.value == "stale_operation"
    assert merge.calls == []
    assert _count(conn, _GATE_UPDATE) == 0
    assert _event_types(conn) == ["task_blocked"]


def test_closed_source_issue_refuses_without_consuming_the_gate(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    conn = ScriptedConnection()
    pool = _pool(conn)
    _script_phase_a(conn)
    _script_issue_guard(conn)
    reconcile = _ReconcileStub(_reconciled(pull_request=_domain_pull_request()))
    merge = _MergePrimitiveStub()
    _wire(monkeypatch, reconcile=reconcile, merge=merge)
    github = _guard(
        issues=[_issue_observation(state="closed")], repository=_repository_observation()
    )

    with pytest.raises(ConflictError):
        request_task_merge(
            pool,
            github,
            _token_resolver(),
            profile_id=_OWNER_PROFILE_ID,
            workspace_id=_WORKSPACE_ID,
            task_id=_TASK_ID,
            owner_gate_id=_RETRY_GATE_ID,
            expected_state_token=_STATE_TOKEN,
        )

    # The gate stays pending and reusable: no resolution, no rotation, no
    # merge request, no blocking consequence.
    assert _count(conn, _GATE_UPDATE) == 0
    assert _count(conn, _EVENT_INSERT) == 0
    assert _count(conn, _BLOCK_INSERT) == 0
    assert merge.calls == []


def test_unavailable_issue_read_fails_closed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    conn = ScriptedConnection()
    pool = _pool(conn)
    _script_phase_a(conn)
    _script_issue_guard(conn)
    reconcile = _ReconcileStub(_reconciled(pull_request=_domain_pull_request()))
    merge = _MergePrimitiveStub()
    _wire(monkeypatch, reconcile=reconcile, merge=merge)
    github = _guard(
        issues=[GitHubAuthorizationRejectedError("denied")],
        repository=_repository_observation(),
    )

    with pytest.raises(AuthorizationError):
        request_task_merge(
            pool,
            github,
            _token_resolver(),
            profile_id=_OWNER_PROFILE_ID,
            workspace_id=_WORKSPACE_ID,
            task_id=_TASK_ID,
            owner_gate_id=_RETRY_GATE_ID,
            expected_state_token=_STATE_TOKEN,
        )

    assert _count(conn, _GATE_UPDATE) == 0
    assert _count(conn, _EVENT_INSERT) == 0
    assert merge.calls == []


def test_wrong_stable_issue_identity_fails_closed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    conn = ScriptedConnection()
    pool = _pool(conn)
    _script_phase_a(conn)
    _script_issue_guard(conn)
    reconcile = _ReconcileStub(_reconciled(pull_request=_domain_pull_request()))
    merge = _MergePrimitiveStub()
    _wire(monkeypatch, reconcile=reconcile, merge=merge)
    github = _guard(issues=[_issue_observation(issue_id=999)], repository=_repository_observation())

    with pytest.raises(ConflictError):
        request_task_merge(
            pool,
            github,
            _token_resolver(),
            profile_id=_OWNER_PROFILE_ID,
            workspace_id=_WORKSPACE_ID,
            task_id=_TASK_ID,
            owner_gate_id=_RETRY_GATE_ID,
            expected_state_token=_STATE_TOKEN,
        )

    assert _count(conn, _GATE_UPDATE) == 0
    assert merge.calls == []


def test_pr_addressed_through_the_issues_endpoint_fails_closed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    conn = ScriptedConnection()
    pool = _pool(conn)
    _script_phase_a(conn)
    _script_issue_guard(conn)
    reconcile = _ReconcileStub(_reconciled(pull_request=_domain_pull_request()))
    merge = _MergePrimitiveStub()
    _wire(monkeypatch, reconcile=reconcile, merge=merge)
    github = _guard(
        issues=[_issue_observation(is_pull_request=True)], repository=_repository_observation()
    )

    with pytest.raises(ConflictError):
        request_task_merge(
            pool,
            github,
            _token_resolver(),
            profile_id=_OWNER_PROFILE_ID,
            workspace_id=_WORKSPACE_ID,
            task_id=_TASK_ID,
            owner_gate_id=_RETRY_GATE_ID,
            expected_state_token=_STATE_TOKEN,
        )

    assert _count(conn, _GATE_UPDATE) == 0
    assert merge.calls == []


def test_github_policy_rejection_installs_a_new_exact_head_gate(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    conn = ScriptedConnection()
    pool = _pool(conn)
    _script_phase_a(conn)
    _script_issue_guard(conn)
    _script_phase_d(conn)
    conn.on(_EVENT_INSERT, _event_returned_row("merge_rejected_by_github"))
    _script_retry_gate(conn)
    reconcile = _ReconcileStub(
        _reconciled(pull_request=_domain_pull_request()),
        _reconciled(pull_request=_domain_pull_request()),
    )
    merge = _MergePrimitiveStub(ExternalOperationRejectedError("GitHub refused the merge"))
    _wire(monkeypatch, reconcile=reconcile, merge=merge)

    outcome = request_task_merge(
        pool,
        _guard(issues=[_issue_observation()], repository=_repository_observation()),
        _token_resolver(),
        profile_id=_OWNER_PROFILE_ID,
        workspace_id=_WORKSPACE_ID,
        task_id=_TASK_ID,
        owner_gate_id=_RETRY_GATE_ID,
        expected_state_token=_STATE_TOKEN,
    )

    assert outcome.kind is TaskMergeOutcomeKind.RETRY_GATE_INSTALLED
    assert outcome.retry_gate is not None
    assert outcome.retry_gate.status is None or outcome.retry_gate.status.value == "pending"
    # The GitHub-owned refusal is recorded; exactly one merge request was
    # sent; the fresh pending gate carries the same accepted head.
    assert _event_types(conn) == [
        "owner_gate_resolved",
        "merge_requested",
        "merge_rejected_by_github",
        "owner_gate_created",
    ]
    assert len(merge.calls) == 1
    assert _count(conn, _BLOCK_INSERT) == 0


def test_pre_delivery_failure_records_no_github_refusal_but_installs_the_retry_gate(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    conn = ScriptedConnection()
    pool = _pool(conn)
    _script_phase_a(conn)
    _script_issue_guard(conn)
    _script_phase_d(conn)
    _script_retry_gate(conn)
    reconcile = _ReconcileStub(
        _reconciled(pull_request=_domain_pull_request()),
        _reconciled(pull_request=_domain_pull_request()),
    )
    merge = _MergePrimitiveStub(ExternalOperationFailedError("credential failure"))
    _wire(monkeypatch, reconcile=reconcile, merge=merge)

    outcome = request_task_merge(
        pool,
        _guard(issues=[_issue_observation()], repository=_repository_observation()),
        _token_resolver(),
        profile_id=_OWNER_PROFILE_ID,
        workspace_id=_WORKSPACE_ID,
        task_id=_TASK_ID,
        owner_gate_id=_RETRY_GATE_ID,
        expected_state_token=_STATE_TOKEN,
    )

    assert outcome.kind is TaskMergeOutcomeKind.RETRY_GATE_INSTALLED
    # Proven non-delivery is NOT a GitHub-owned refusal: no
    # MERGE_REJECTED_BY_GITHUB is recorded; the retry gate still requires
    # fresh Owner authorization.
    assert _event_types(conn) == ["owner_gate_resolved", "merge_requested", "owner_gate_created"]
    assert len(merge.calls) == 1


def test_uncertain_outcome_blocks_and_is_never_repeated(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    conn = ScriptedConnection()
    pool = _pool(conn)
    _script_phase_a(conn)
    _script_issue_guard(conn)
    _script_phase_d(conn)
    _script_block(conn, token=_POST_APPROVAL_TOKEN, reason="external_operation_uncertain")
    reconcile = _ReconcileStub(
        _reconciled(pull_request=_domain_pull_request()),
        _reconciled(pull_request=_domain_pull_request()),
    )
    merge = _MergePrimitiveStub(ExternalOperationUncertainError("unknown outcome"))
    _wire(monkeypatch, reconcile=reconcile, merge=merge)

    outcome = request_task_merge(
        pool,
        _guard(issues=[_issue_observation()], repository=_repository_observation()),
        _token_resolver(),
        profile_id=_OWNER_PROFILE_ID,
        workspace_id=_WORKSPACE_ID,
        task_id=_TASK_ID,
        owner_gate_id=_RETRY_GATE_ID,
        expected_state_token=_STATE_TOKEN,
    )

    assert outcome.kind is TaskMergeOutcomeKind.BLOCKED_UNCERTAIN
    assert outcome.task.status is TaskStatus.BLOCKED
    assert outcome.task_block is not None
    assert outcome.task_block.reason.value == "external_operation_uncertain"
    # An uncertain request is never repeated and never minted a retry gate.
    assert len(merge.calls) == 1
    assert _count(conn, _GATE_INSERT) == 0
    assert _event_types(conn) == ["owner_gate_resolved", "merge_requested", "task_blocked"]


def test_confirmed_merge_with_failed_reconciliation_completes_without_repeating(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    conn = ScriptedConnection()
    pool = _pool(conn)
    _script_phase_a(conn)
    _script_issue_guard(conn)
    _script_phase_d(conn)
    _script_completion(conn, task_token=_POST_APPROVAL_TOKEN)
    reconcile = _ReconcileStub(
        _reconciled(pull_request=_domain_pull_request()),
        _reconciled(
            pull_request=_domain_pull_request(
                state=TaskPullRequestState.CLOSED, merged_at=_OBSERVED
            )
        ),
    )
    merge = _MergePrimitiveStub(
        MergeConfirmedReconciliationPendingError(
            "confirmed but unconfirmed reconciliation",
            merge_commit_sha=_MERGE_COMMIT_SHA,
            expected_head_sha=_HEAD_SHA,
        )
    )
    _wire(monkeypatch, reconcile=reconcile, merge=merge)

    outcome = request_task_merge(
        pool,
        _guard(issues=[_issue_observation()], repository=_repository_observation()),
        _token_resolver(),
        profile_id=_OWNER_PROFILE_ID,
        workspace_id=_WORKSPACE_ID,
        task_id=_TASK_ID,
        owner_gate_id=_RETRY_GATE_ID,
        expected_state_token=_STATE_TOKEN,
    )

    assert outcome.kind is TaskMergeOutcomeKind.COMPLETED
    assert outcome.merge_commit_sha == _MERGE_COMMIT_SHA
    # The confirmed merge is never repeated, never misclassified as a
    # rejection, and never minted a retry gate.
    assert len(merge.calls) == 1
    assert _count(conn, _GATE_INSERT) == 0
    assert _count(conn, _BLOCK_INSERT) == 0
    assert "merge_rejected_by_github" not in _event_types(conn)


def test_confirmed_merge_with_unconfirmed_reconciliation_blocks_with_evidence(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    conn = ScriptedConnection()
    pool = _pool(conn)
    _script_phase_a(conn)
    _script_issue_guard(conn)
    _script_phase_d(conn)
    _script_block(conn, token=_POST_APPROVAL_TOKEN, reason="external_operation_uncertain")
    reconcile = _ReconcileStub(
        _reconciled(pull_request=_domain_pull_request()),
        _reconciled(pull_request=_domain_pull_request()),
    )
    merge = _MergePrimitiveStub(
        MergeConfirmedReconciliationPendingError(
            "confirmed merged but its durable reconciliation is unconfirmed",
            merge_commit_sha=_MERGE_COMMIT_SHA,
            expected_head_sha=_HEAD_SHA,
        )
    )
    _wire(monkeypatch, reconcile=reconcile, merge=merge)

    outcome = request_task_merge(
        pool,
        _guard(issues=[_issue_observation()], repository=_repository_observation()),
        _token_resolver(),
        profile_id=_OWNER_PROFILE_ID,
        workspace_id=_WORKSPACE_ID,
        task_id=_TASK_ID,
        owner_gate_id=_RETRY_GATE_ID,
        expected_state_token=_STATE_TOKEN,
    )

    assert outcome.kind is TaskMergeOutcomeKind.BLOCKED_UNCERTAIN
    assert outcome.task.status is TaskStatus.BLOCKED
    assert outcome.task_block is not None
    assert outcome.task_block.reason.value == "external_operation_uncertain"
    # The preserved confirmed-merge evidence rides the durable recovery
    # context, and the confirmed merge is never repeated and never minted a
    # retry gate.
    block_inserts = [
        params for sql, params in conn.executed if params is not None and _BLOCK_INSERT(sql, params)
    ]
    assert len(block_inserts) == 1
    block_context = block_inserts[0][3].obj
    assert block_context["merge_commit_sha"] == _MERGE_COMMIT_SHA
    assert block_context["expected_head_sha"] == _HEAD_SHA
    assert outcome.merge_commit_sha == _MERGE_COMMIT_SHA
    assert len(merge.calls) == 1
    assert _count(conn, _GATE_INSERT) == 0
    assert _event_types(conn) == ["owner_gate_resolved", "merge_requested", "task_blocked"]


def test_stale_claim_pre_send_fails_closed_and_applies_nothing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    conn = ScriptedConnection()
    pool = _pool(conn)
    _script_phase_a(conn)
    _script_issue_guard(conn)
    _script_phase_d(conn)
    reconcile = _ReconcileStub(
        _reconciled(pull_request=_domain_pull_request()),
        _reconciled(pull_request=_domain_pull_request()),
    )
    merge = _MergePrimitiveStub(StaleOperationError("the approved claim is stale"))
    _wire(monkeypatch, reconcile=reconcile, merge=merge)

    with pytest.raises(StaleOperationError):
        request_task_merge(
            pool,
            _guard(issues=[_issue_observation()], repository=_repository_observation()),
            _token_resolver(),
            profile_id=_OWNER_PROFILE_ID,
            workspace_id=_WORKSPACE_ID,
            task_id=_TASK_ID,
            owner_gate_id=_RETRY_GATE_ID,
            expected_state_token=_STATE_TOKEN,
        )

    # The PR remained open at the exact claimed head: the staleness was
    # workflow state, the standard stale semantics apply nothing, and the
    # one-shot claim forbids any second merge request.
    assert len(merge.calls) == 1
    assert _count(conn, _GATE_INSERT) == 0
    assert _count(conn, _BLOCK_INSERT) == 0
    assert _event_types(conn) == ["owner_gate_resolved", "merge_requested"]


def test_head_mismatch_after_send_blocks_stale_operation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    conn = ScriptedConnection()
    pool = _pool(conn)
    _script_phase_a(conn)
    _script_issue_guard(conn)
    _script_phase_d(conn)
    _script_block(conn, token=_POST_APPROVAL_TOKEN)
    reconcile = _ReconcileStub(
        _reconciled(pull_request=_domain_pull_request(head_sha=_NEW_HEAD_SHA))
    )
    merge = _MergePrimitiveStub(StaleOperationError("head mismatch"))
    _wire(monkeypatch, reconcile=reconcile, merge=merge)

    outcome = request_task_merge(
        pool,
        _guard(issues=[_issue_observation()], repository=_repository_observation()),
        _token_resolver(),
        profile_id=_OWNER_PROFILE_ID,
        workspace_id=_WORKSPACE_ID,
        task_id=_TASK_ID,
        owner_gate_id=_RETRY_GATE_ID,
        expected_state_token=_STATE_TOKEN,
    )

    assert outcome.kind is TaskMergeOutcomeKind.BLOCKED_STALE_OPERATION
    assert outcome.task.status is TaskStatus.BLOCKED
    # No Reviewer acceptance transfers to the unreviewed head: no retry gate.
    assert _count(conn, _GATE_INSERT) == 0


def test_completion_is_an_idempotent_noop_after_archival() -> None:
    conn = ScriptedConnection()
    pool = _pool(conn)
    conn.on(_WORKSPACE_READ, _ws_row())
    conn.on(_ACCOUNT_BARRIER, _ACCOUNT_OPERATIONAL_ROW)
    conn.on(_TASK_READ_LOCKED, _archived_task_row(state_token=_COMPLETED_TOKEN))
    conn.on(_PR_READ, _pr_row_values(state="closed", merged_at=_OBSERVED))

    outcome = complete_confirmed_task_merge(pool, workspace_id=_WORKSPACE_ID, task_id=_TASK_ID)

    assert outcome.kind is TaskMergeCompletionKind.ALREADY_COMPLETED
    assert outcome.task.status is TaskStatus.COMPLETED
    # No second archival, loop closure, or event set.
    assert _count(conn, _EVENT_INSERT) == 0
    assert _count(conn, _ARCHIVE_UPDATE) == 0
    assert _count(conn, _LOOP_CLOSE) == 0


def test_completion_never_resurrects_archived_cancelled_history() -> None:
    conn = ScriptedConnection()
    pool = _pool(conn)
    conn.on(_WORKSPACE_READ, _ws_row())
    conn.on(_ACCOUNT_BARRIER, _ACCOUNT_OPERATIONAL_ROW)
    cancelled = list(
        _task_row(status="cancelled", state_token=_COMPLETED_TOKEN, current_gate_id=None)
    )
    cancelled[6] = _OBSERVED
    conn.on(_TASK_READ_LOCKED, tuple(cancelled))

    with pytest.raises(StaleOperationError):
        complete_confirmed_task_merge(pool, workspace_id=_WORKSPACE_ID, task_id=_TASK_ID)

    assert _count(conn, _EVENT_INSERT) == 0
    assert _count(conn, _ARCHIVE_UPDATE) == 0


def test_completion_fails_closed_when_the_canonical_pr_is_not_merged() -> None:
    conn = ScriptedConnection()
    pool = _pool(conn)
    conn.on(_WORKSPACE_READ, _ws_row())
    conn.on(_ACCOUNT_BARRIER, _ACCOUNT_OPERATIONAL_ROW)
    conn.on(_TASK_READ_LOCKED, _task_row(status="reviewing"))
    conn.on(_PR_READ, _pr_row_values(state="open"))

    with pytest.raises(StaleOperationError):
        complete_confirmed_task_merge(pool, workspace_id=_WORKSPACE_ID, task_id=_TASK_ID)

    assert _count(conn, _EVENT_INSERT) == 0
    assert _count(conn, _ARCHIVE_UPDATE) == 0


def test_completion_archives_a_blocked_task_and_leaves_no_actionable_gate() -> None:
    conn = ScriptedConnection()
    pool = _pool(conn)
    conn.on(_WORKSPACE_READ, _ws_row())
    conn.on(_ACCOUNT_BARRIER, _ACCOUNT_OPERATIONAL_ROW)
    conn.on(
        _TASK_READ_LOCKED,
        _task_row(status="blocked", state_token=_BLOCKED_TOKEN, current_gate_id=_RETRY_GATE_ID),
    )
    conn.on(_PR_READ, _pr_row_values(state="closed", merged_at=_OBSERVED))
    conn.on(_LOOPS_LIST, [])
    conn.on(_TASK_READ, _task_row(status="blocked", state_token=_BLOCKED_TOKEN))
    conn.on(_ARCHIVE_UPDATE, _archived_task_row(state_token=_COMPLETED_TOKEN))
    conn.on(_EVENT_INSERT, _event_returned_row("task_completed"))
    conn.on(_EVENT_INSERT, _event_returned_row("pr_merged"))

    outcome = complete_confirmed_task_merge(pool, workspace_id=_WORKSPACE_ID, task_id=_TASK_ID)

    assert outcome.kind is TaskMergeCompletionKind.COMPLETED
    assert outcome.task.status is TaskStatus.COMPLETED
    # A pre-E9 external merge may have no PR-review loop: none is manufactured.
    assert _count(conn, _LOOP_CLOSE) == 0
    assert _event_types(conn) == ["task_completed", "pr_merged"]


def test_claim_revalidation_hook_proves_the_durable_claim(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    conn = ScriptedConnection()
    pool = _pool(conn)
    _script_phase_a(conn)
    _script_issue_guard(conn)
    _script_phase_d(conn)
    reconcile = _ReconcileStub(_reconciled(pull_request=_domain_pull_request()))
    merge = _MergePrimitiveStub(RuntimeError("stop before the primitive body"))
    _wire(monkeypatch, reconcile=reconcile, merge=merge)

    with pytest.raises(RuntimeError):
        request_task_merge(
            pool,
            _guard(issues=[_issue_observation()], repository=_repository_observation()),
            _token_resolver(),
            profile_id=_OWNER_PROFILE_ID,
            workspace_id=_WORKSPACE_ID,
            task_id=_TASK_ID,
            owner_gate_id=_RETRY_GATE_ID,
            expected_state_token=_STATE_TOKEN,
        )

    hook = merge.calls[0]["claim_revalidation"]
    hook_conn = ScriptedConnection()
    hook_pool = _pool(hook_conn)
    hook_conn.on(
        _TASK_READ_LOCKED,
        _waiting_task_row(state_token=_POST_APPROVAL_TOKEN, current_gate_id=None),
    )
    hook_conn.on(
        _GATE_READ,
        _gate_row(gate_id=_RETRY_GATE_ID, gate_type="merge_decision", status="approved"),
    )
    hook_conn.on(_PR_READ, _pr_row_values())

    hook(hook_pool)

    assert len(hook_conn.executed) == 3


def test_claim_revalidation_rejects_a_moved_task_token(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    conn = ScriptedConnection()
    pool = _pool(conn)
    _script_phase_a(conn)
    _script_issue_guard(conn)
    _script_phase_d(conn)
    reconcile = _ReconcileStub(_reconciled(pull_request=_domain_pull_request()))
    merge = _MergePrimitiveStub(RuntimeError("stop before the primitive body"))
    _wire(monkeypatch, reconcile=reconcile, merge=merge)

    with pytest.raises(RuntimeError):
        request_task_merge(
            pool,
            _guard(issues=[_issue_observation()], repository=_repository_observation()),
            _token_resolver(),
            profile_id=_OWNER_PROFILE_ID,
            workspace_id=_WORKSPACE_ID,
            task_id=_TASK_ID,
            owner_gate_id=_RETRY_GATE_ID,
            expected_state_token=_STATE_TOKEN,
        )

    hook = merge.calls[0]["claim_revalidation"]
    hook_conn = ScriptedConnection()
    hook_conn.on(_TASK_READ_LOCKED, _task_row(status="waiting_for_owner"))

    with pytest.raises(StaleOperationError):
        hook(_pool(hook_conn))


def test_claim_revalidation_rejects_a_no_longer_open_canonical_pr(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    conn = ScriptedConnection()
    pool = _pool(conn)
    _script_phase_a(conn)
    _script_issue_guard(conn)
    _script_phase_d(conn)
    reconcile = _ReconcileStub(_reconciled(pull_request=_domain_pull_request()))
    merge = _MergePrimitiveStub(RuntimeError("stop before the primitive body"))
    _wire(monkeypatch, reconcile=reconcile, merge=merge)

    with pytest.raises(RuntimeError):
        request_task_merge(
            pool,
            _guard(issues=[_issue_observation()], repository=_repository_observation()),
            _token_resolver(),
            profile_id=_OWNER_PROFILE_ID,
            workspace_id=_WORKSPACE_ID,
            task_id=_TASK_ID,
            owner_gate_id=_RETRY_GATE_ID,
            expected_state_token=_STATE_TOKEN,
        )

    hook = merge.calls[0]["claim_revalidation"]
    hook_conn = ScriptedConnection()
    hook_conn.on(
        _TASK_READ_LOCKED,
        _waiting_task_row(state_token=_POST_APPROVAL_TOKEN, current_gate_id=None),
    )
    hook_conn.on(
        _GATE_READ,
        _gate_row(gate_id=_RETRY_GATE_ID, gate_type="merge_decision", status="approved"),
    )
    hook_conn.on(_PR_READ, _pr_row_values(state="closed", merged_at=_OBSERVED))

    with pytest.raises(StaleOperationError):
        hook(_pool(hook_conn))
