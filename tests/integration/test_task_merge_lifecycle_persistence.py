"""Durable atomicity of the E10a merge lifecycle seams (issue #227).

The targeted real-Postgres checks the new cross-record merge seams own:

- the shared GitHub-confirmed completion composes ONE atomic transition: the
  OPEN ``PR_REVIEW`` loop closes, the Task archives ``COMPLETED`` (releasing
  the current-issue mapping and clearing the current-gate pointer while the
  gate row stays untouched pending history), and exactly one ``pr_merged``
  (GITHUB) plus one terminal ``task_completed`` (OPENORC) event commit
  together; a forced late audit failure rolls the whole composition back;
  a duplicate completion is the idempotent ``ALREADY_COMPLETED`` no-op with
  no second archival, loop closure, or event set;
- the Owner approval/claim composition survives a post-claim failure: the
  APPROVED gate resolution plus the OWNER ``owner_gate_resolved`` and
  ``merge_requested`` audit pair commit durably even when the merge request
  itself fails afterwards, and the claim is one-shot — a second Owner
  command is a stale operation that cannot issue a second merge request;
- an archived ``CANCELLED`` Task is never resurrected by a later merge of
  its old pull request.

No live GitHub, runtime, or Supabase access; the B7 reconciliation and the
merge primitive are stubbed at their module seams (their own modules own
those matrices). The suite consumes the explicitly supplied non-production
branch database per ``tests/integration/conftest.py``.
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime
from typing import Any, cast

import pytest
from psycopg import Connection

from openorc.adapters.github import GitHubIssueObservation, GitHubRepositoryObservation
from openorc.domain.gates import OwnerGateStatus, OwnerGateType
from openorc.domain.pull_requests import TaskPullRequest, TaskPullRequestState
from openorc.domain.reviews import ReviewLoopPurpose
from openorc.domain.tasks import TaskStatus
from openorc.persistence import gates as gate_repositories
from openorc.persistence import reviews as review_repositories
from openorc.persistence import tasks as task_repositories
from openorc.persistence.pool import DatabasePool
from openorc.services import (
    pull_request_merge,
    task_merge_lifecycle,
    task_pull_request_reconciliation,
)
from openorc.services.errors import StaleOperationError
from openorc.services.task_merge_lifecycle import (
    TaskMergeCompletionKind,
    complete_confirmed_task_merge,
    request_task_merge,
)
from openorc.services.task_pull_request_reconciliation import TaskPullRequestReconciliation
from tests.integration.test_task_planning_gate_change_request_persistence import (
    _insert_installation,
    _insert_profile,
    _insert_repository,
    _insert_task,
    _insert_workspace,
    _SingleConnectionPool,
)

pytestmark = pytest.mark.integration

_HEAD_SHA = "0123456789abcdef0123456789abcdef01234567"
_BRANCH = "openorc/task-42-implementation"
_GITHUB_PR_ID = 900_719_925_474_099
_PR_NUMBER = 77
_GITHUB_ISSUE_ID = 7702
_OBSERVED = datetime(2026, 10, 22, 12, 0, 0, tzinfo=UTC)


def _insert_canonical_pr(
    conn: Connection[Any],
    *,
    workspace_id: uuid.UUID,
    task_id: uuid.UUID,
    repository_id: uuid.UUID,
    merged: bool,
) -> uuid.UUID:
    pr_id = uuid.uuid4()
    if merged:
        conn.execute(
            "insert into openorc.task_pull_requests "
            "(id, workspace_id, task_id, repository_id, github_pr_id, github_pr_number, "
            "head_ref, base_ref, head_sha, state, merged_at) "
            "values (%s, %s, %s, %s, %s, %s, %s, 'main', %s, 'closed', now())",
            (
                pr_id,
                workspace_id,
                task_id,
                repository_id,
                _GITHUB_PR_ID,
                _PR_NUMBER,
                _BRANCH,
                _HEAD_SHA,
            ),
        )
    else:
        conn.execute(
            "insert into openorc.task_pull_requests "
            "(id, workspace_id, task_id, repository_id, github_pr_id, github_pr_number, "
            "head_ref, base_ref, head_sha, state) "
            "values (%s, %s, %s, %s, %s, %s, %s, 'main', %s, 'open')",
            (
                pr_id,
                workspace_id,
                task_id,
                repository_id,
                _GITHUB_PR_ID,
                _PR_NUMBER,
                _BRANCH,
                _HEAD_SHA,
            ),
        )
    return pr_id


def _merge_fixture(conn: Connection[Any], *, merged: bool) -> dict[str, Any]:
    """A ``waiting_for_owner`` Task holding a pending current PR/head
    MERGE_DECISION gate over an OPEN ``PR_REVIEW`` loop and one canonical
    pull request (merged or open per the case)."""
    profile_id = _insert_profile(conn)
    workspace_id = _insert_workspace(conn, profile_id)
    installation_id = _insert_installation(conn, workspace_id=workspace_id)
    repository_id = _insert_repository(
        conn, workspace_id=workspace_id, github_installation_id=installation_id
    )
    task_id = _insert_task(conn, workspace_id=workspace_id, repository_id=repository_id)
    pool = cast(DatabasePool, _SingleConnectionPool(conn))

    pr_id = _insert_canonical_pr(
        conn,
        workspace_id=workspace_id,
        task_id=task_id,
        repository_id=repository_id,
        merged=merged,
    )
    task = task_repositories.get_task(pool, task_id)
    assert task is not None
    bound = task_repositories.bind_canonical_branch(
        pool,
        task_id,
        expected_state_token=task.state_token,
        canonical_feature_branch=_BRANCH,
    )
    assert bound is not None
    reviewing = task_repositories.update_task_status(
        pool,
        task_id,
        expected_state_token=bound.state_token,
        status=TaskStatus.REVIEWING,
    )
    assert reviewing is not None
    loop = review_repositories.create_review_loop(
        pool,
        workspace_id=workspace_id,
        task_id=task_id,
        purpose=ReviewLoopPurpose.PR_REVIEW,
        iteration_limit=2,
    )
    gate = gate_repositories.create_owner_gate(
        pool,
        workspace_id=workspace_id,
        task_id=task_id,
        gate_type=OwnerGateType.MERGE_DECISION,
        subject_head_sha=_HEAD_SHA,
        task_pull_request_id=pr_id,
    )
    installed = task_repositories.set_current_owner_gate(
        pool,
        task_id,
        expected_state_token=reviewing.state_token,
        owner_gate_id=gate.id,
    )
    assert installed is not None
    waiting = task_repositories.update_task_status(
        pool,
        task_id,
        expected_state_token=installed.state_token,
        status=TaskStatus.WAITING_FOR_OWNER,
    )
    assert waiting is not None
    return {
        "pool": pool,
        "conn": conn,
        "workspace_id": workspace_id,
        "repository_id": repository_id,
        "task_id": task_id,
        "profile_id": profile_id,
        "pr_id": pr_id,
        "loop_id": loop.id,
        "gate_id": gate.id,
        "state_token": waiting.state_token,
    }


def _event_counts(conn: Connection[Any], task_id: uuid.UUID) -> dict[str, int]:
    row = conn.execute(
        "select event_type, count(*) from openorc.workflow_events "
        "where task_id = %s group by event_type",
        (task_id,),
    ).fetchall()
    return {event_type: int(count) for event_type, count in row}


def _reconciled_open() -> TaskPullRequestReconciliation:
    return TaskPullRequestReconciliation(
        pull_request=TaskPullRequest(
            id=uuid.uuid4(),
            workspace_id=uuid.uuid4(),
            task_id=uuid.uuid4(),
            repository_id=uuid.uuid4(),
            github_pr_id=_GITHUB_PR_ID,
            github_pr_number=_PR_NUMBER,
            head_ref=_BRANCH,
            base_ref="main",
            head_sha=_HEAD_SHA,
            state=TaskPullRequestState.OPEN,
            merged_at=None,
            created_at=_OBSERVED,
            updated_at=_OBSERVED,
        ),
        updated=False,
        head_changed=False,
        base_changed=False,
        state_changed=False,
        previous_head_sha=None,
        previous_base_ref=None,
        previous_state=None,
    )


def test_the_confirmed_merge_completion_commits_atomically(
    conn: Connection[Any],
) -> None:
    facts = _merge_fixture(conn, merged=True)
    pool = facts["pool"]

    outcome = complete_confirmed_task_merge(
        pool, workspace_id=facts["workspace_id"], task_id=facts["task_id"]
    )

    assert outcome.kind is TaskMergeCompletionKind.COMPLETED
    task = task_repositories.get_task(pool, facts["task_id"])
    assert task is not None
    assert task.status is TaskStatus.COMPLETED
    assert task.archived_at is not None
    assert task.current_owner_gate_id is None
    assert task.state_token != facts["state_token"]
    # The gate row stays untouched pending history with its own provenance.
    gate = gate_repositories.get_owner_gate(pool, owner_gate_id=facts["gate_id"])
    assert gate is not None
    assert gate.status is OwnerGateStatus.PENDING
    assert gate.decided_at is None
    # The OPEN PR_REVIEW loop closed.
    loops = review_repositories.list_task_review_loops(pool, task_id=facts["task_id"], status=None)
    assert [loop.status.value for loop in loops] == ["closed"]
    # The current-issue mapping is released by the archival.
    assert (
        task_repositories.find_current_task_for_issue(
            pool, repository_id=facts["repository_id"], github_issue_id=_GITHUB_ISSUE_ID
        )
        is None
    )
    # Exactly the truthful audit pair, once.
    assert _event_counts(conn, facts["task_id"]) == {
        "pr_merged": 1,
        "task_completed": 1,
    }


def test_a_duplicate_completion_is_an_idempotent_noop(conn: Connection[Any]) -> None:
    facts = _merge_fixture(conn, merged=True)
    pool = facts["pool"]

    first = complete_confirmed_task_merge(
        pool, workspace_id=facts["workspace_id"], task_id=facts["task_id"]
    )
    second = complete_confirmed_task_merge(
        pool, workspace_id=facts["workspace_id"], task_id=facts["task_id"]
    )

    assert first.kind is TaskMergeCompletionKind.COMPLETED
    assert second.kind is TaskMergeCompletionKind.ALREADY_COMPLETED
    assert _event_counts(conn, facts["task_id"]) == {
        "pr_merged": 1,
        "task_completed": 1,
    }


def test_a_late_completion_failure_rolls_the_whole_composition_back(
    conn: Connection[Any], monkeypatch: pytest.MonkeyPatch
) -> None:
    facts = _merge_fixture(conn, merged=True)
    pool = facts["pool"]

    def _boom(*args: Any, **kwargs: Any) -> Any:
        raise RuntimeError("forced late merged-audit failure")

    monkeypatch.setattr(
        task_merge_lifecycle.event_coordination,
        "record_pr_merged_event",
        _boom,
    )

    with pytest.raises(RuntimeError, match="forced late merged-audit failure"):
        complete_confirmed_task_merge(
            pool, workspace_id=facts["workspace_id"], task_id=facts["task_id"]
        )

    # Nothing committed: the Task is still current at its original token
    # with the gate still current, the loop is still OPEN, and no events
    # and no archival happened.
    task = task_repositories.get_task(pool, facts["task_id"])
    assert task is not None
    assert task.status is TaskStatus.WAITING_FOR_OWNER
    assert task.archived_at is None
    assert task.current_owner_gate_id == facts["gate_id"]
    assert task.state_token == facts["state_token"]
    loops = review_repositories.list_task_review_loops(pool, task_id=facts["task_id"], status=None)
    assert [loop.status.value for loop in loops] == ["open"]
    assert _event_counts(conn, facts["task_id"]) == {}


def test_archived_cancelled_history_is_never_resurrected_by_a_later_merge(
    conn: Connection[Any],
) -> None:
    facts = _merge_fixture(conn, merged=True)
    pool = facts["pool"]
    cancelled = task_repositories.archive_task(
        pool,
        facts["task_id"],
        expected_state_token=facts["state_token"],
        terminal_status=TaskStatus.CANCELLED,
    )
    assert cancelled is not None

    with pytest.raises(StaleOperationError):
        complete_confirmed_task_merge(
            pool, workspace_id=facts["workspace_id"], task_id=facts["task_id"]
        )

    task = task_repositories.get_task(pool, facts["task_id"])
    assert task is not None
    assert task.status is TaskStatus.CANCELLED
    assert task.archived_at is not None
    assert _event_counts(conn, facts["task_id"]) == {}


def test_the_owner_approval_claim_survives_a_post_claim_failure_and_is_one_shot(
    conn: Connection[Any], monkeypatch: pytest.MonkeyPatch
) -> None:
    facts = _merge_fixture(conn, merged=False)
    pool = facts["pool"]

    class _StubGitHub:
        def get_installation_repository(self, **_kwargs: Any) -> Any:
            return GitHubRepositoryObservation(
                github_repository_id=987654321,
                owner_login="octocat",
                name="hello-world",
                html_url="https://github.com/octocat/hello-world",
                is_private=False,
                default_branch="main",
            )

        def get_repository_issue(self, **_kwargs: Any) -> Any:
            return GitHubIssueObservation(
                github_issue_id=_GITHUB_ISSUE_ID,
                issue_number=42,
                title="t",
                body=None,
                state="open",
                provider_updated_at=_OBSERVED,
                is_pull_request=False,
            )

    merge_calls: list[dict[str, Any]] = []

    def _stub_reconcile(*args: Any, **kwargs: Any) -> Any:
        return _reconciled_open()

    def _stub_merge(*args: Any, **kwargs: Any) -> Any:
        merge_calls.append(dict(kwargs))
        raise RuntimeError("forced post-claim merge failure")

    monkeypatch.setattr(
        task_pull_request_reconciliation, "reconcile_task_pull_request", _stub_reconcile
    )
    monkeypatch.setattr(pull_request_merge, "request_pull_request_merge", _stub_merge)

    with pytest.raises(RuntimeError, match="forced post-claim merge failure"):
        request_task_merge(
            pool,
            cast(Any, _StubGitHub()),
            cast(Any, object()),
            profile_id=facts["profile_id"],
            workspace_id=facts["workspace_id"],
            task_id=facts["task_id"],
            owner_gate_id=facts["gate_id"],
            expected_state_token=facts["state_token"],
        )

    # The one-shot durable claim committed even though the merge request
    # failed afterwards: the gate is APPROVED history, the pointer cleared,
    # and both OWNER audit facts are durable. The PR review loop stays OPEN.
    gate = gate_repositories.get_owner_gate(pool, owner_gate_id=facts["gate_id"])
    assert gate is not None
    assert gate.status is OwnerGateStatus.APPROVED
    assert gate.decided_at is not None
    task = task_repositories.get_task(pool, facts["task_id"])
    assert task is not None
    assert task.status is TaskStatus.WAITING_FOR_OWNER
    assert task.current_owner_gate_id is None
    assert task.state_token != facts["state_token"]
    loops = review_repositories.list_task_review_loops(pool, task_id=facts["task_id"], status=None)
    assert [loop.status.value for loop in loops] == ["open"]
    counts = _event_counts(conn, facts["task_id"])
    assert counts.get("owner_gate_resolved") == 1
    assert counts.get("merge_requested") == 1
    assert len(merge_calls) == 1

    # The claim is one-shot: a second Owner command cannot pass the exact
    # current pending gate again and cannot issue a second merge request.
    with pytest.raises(StaleOperationError):
        request_task_merge(
            pool,
            cast(Any, _StubGitHub()),
            cast(Any, object()),
            profile_id=facts["profile_id"],
            workspace_id=facts["workspace_id"],
            task_id=facts["task_id"],
            owner_gate_id=facts["gate_id"],
            expected_state_token=task.state_token,
        )

    assert len(merge_calls) == 1
    assert _event_counts(conn, facts["task_id"]) == counts
