"""Durable atomicity of the E10b Owner-control seams (issue #228).

The targeted real-Postgres checks the new Abort/Cancel seams own:

- the Owner Abort composition commits atomically: the
  ``owner_action_required`` TaskBlock with its safe ``owner_abort`` context,
  the ``blocked`` transition under a rotated token, the OWNER
  ``task_blocked`` audit, and the one-shot unsettled exact-session Interrupt
  intent; a forced late audit failure rolls the whole composition back and
  the runtime is never invoked;
- the unified cancellation commits atomically: the exact current pending
  MERGE_DECISION gate resolves CANCELLED with OWNER provenance, the OPEN
  PR_REVIEW loop closes, the Task archives ``CANCELLED`` (releasing the
  current-issue mapping and clearing the current-gate pointer), the OWNER
  ``task_cancelled`` audit commits, and the one-shot PR-close intent for the
  open canonical pull request commits with them; a forced late failure rolls
  the whole composition back, leaving the gate pending, the Task current,
  and no intent row.

No live GitHub, runtime, or Supabase access; the B7 reconciliation, the
credential resolution, the route resolution, and the GitHub close client are
stubbed at their module seams (their own modules own those matrices). The
suite consumes the explicitly supplied non-production branch database per
``tests/integration/conftest.py``.
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any, cast

import pytest
from psycopg import Connection

from openorc.adapters.github import (
    GitHubPullRequestCloseOperation,
    GitHubPullRequestCloseResult,
)
from openorc.domain.connections import WorkflowRole
from openorc.domain.gates import OwnerGateStatus, OwnerGateType
from openorc.domain.intents import (
    TaskInterruptIntentStatus,
    TaskPullRequestCloseIntentStatus,
)
from openorc.domain.pull_requests import TaskPullRequest, TaskPullRequestState
from openorc.domain.reviews import ReviewLoopPurpose
from openorc.domain.tasks import TaskStatus
from openorc.persistence import gates as gate_repositories
from openorc.persistence import intents as intent_repositories
from openorc.persistence import reviews as review_repositories
from openorc.persistence import tasks as task_repositories
from openorc.persistence.blocks import list_task_blocks
from openorc.persistence.pool import DatabasePool
from openorc.services import event_coordination, task_cancellation
from openorc.services.task_abort import abort_task_activity
from openorc.services.task_cancellation import cancel_task
from openorc.services.task_pull_request_reconciliation import TaskPullRequestReconciliation
from tests.integration.test_task_planning_gate_change_request_persistence import (
    _insert_connection,
    _insert_installation,
    _insert_profile,
    _insert_ready_session,
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
_EXTERNAL_SESSION_ID = "external-producer-session"
_OBSERVED = datetime(2026, 10, 23, 12, 0, 0, tzinfo=UTC)


def _insert_canonical_pr(
    conn: Connection[Any],
    *,
    workspace_id: uuid.UUID,
    task_id: uuid.UUID,
    repository_id: uuid.UUID,
) -> uuid.UUID:
    pr_id = uuid.uuid4()
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


class _RecordingClineAdapter:
    """The runtime seam recording the exact post-commit interrupt dispatch."""

    def __init__(self) -> None:
        self.abort_calls: list[str] = []

    def abort_activity(self, session_id: str) -> None:
        self.abort_calls.append(session_id)


class _CloseGithub:
    """The fake GitHub client recording the exact close routing arguments."""

    def __init__(self) -> None:
        self.repository_calls: list[dict[str, int]] = []
        self.close_calls: list[dict[str, Any]] = []

    def get_installation_repository(
        self, *, github_installation_id: int, github_repository_id: int
    ) -> Any:
        self.repository_calls.append(
            {
                "github_installation_id": github_installation_id,
                "github_repository_id": github_repository_id,
            }
        )
        return SimpleNamespace(owner_login="octocat", name="hello-world")

    def close_pull_request(self, **kwargs: Any) -> Any:
        self.close_calls.append(dict(kwargs))
        return GitHubPullRequestCloseResult(
            outcome=GitHubPullRequestCloseOperation.CLOSED, pull_request=None
        )


def _abort_fixture(conn: Connection[Any]) -> dict[str, Any]:
    """An ``implementing`` Task with one READY bound Producer session owning
    its RUNNING implementation Execution — the durable in-flight activity the
    Owner Abort binds its admitted target to."""
    profile_id = _insert_profile(conn)
    workspace_id = _insert_workspace(conn, profile_id)
    installation_id = _insert_installation(conn, workspace_id=workspace_id)
    repository_id = _insert_repository(
        conn, workspace_id=workspace_id, github_installation_id=installation_id
    )
    task_id = _insert_task(conn, workspace_id=workspace_id, repository_id=repository_id)
    connection_id = _insert_connection(conn, workspace_id=workspace_id)
    session_id = _insert_ready_session(
        conn,
        workspace_id=workspace_id,
        task_id=task_id,
        connection_id=connection_id,
        role="producer",
        external_session_id=_EXTERNAL_SESSION_ID,
    )
    pool = cast(DatabasePool, _SingleConnectionPool(conn))
    task = task_repositories.get_task(pool, task_id)
    assert task is not None
    implementing = task_repositories.update_task_status(
        pool,
        task_id,
        expected_state_token=task.state_token,
        status=TaskStatus.IMPLEMENTING,
    )
    assert implementing is not None
    conn.execute(
        "insert into openorc.executions "
        "(workspace_id, task_id, producer_session_id, execution_number, status) "
        "values (%s, %s, %s, 1, 'running')",
        (workspace_id, task_id, session_id),
    )
    return {
        "pool": pool,
        "conn": conn,
        "workspace_id": workspace_id,
        "task_id": task_id,
        "profile_id": profile_id,
        "session_id": session_id,
        "state_token": implementing.state_token,
    }


def _cancel_fixture(conn: Connection[Any]) -> dict[str, Any]:
    """A ``waiting_for_owner`` Task holding a pending current MERGE_DECISION
    gate over an OPEN ``PR_REVIEW`` loop and one OPEN canonical pull request."""
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


def _reconciled(pull_request: TaskPullRequest) -> TaskPullRequestReconciliation:
    return TaskPullRequestReconciliation(
        pull_request=pull_request,
        updated=False,
        head_changed=False,
        base_changed=False,
        state_changed=False,
        previous_head_sha=None,
        previous_base_ref=None,
        previous_state=None,
    )


def _open_pull_request(workspace_id: uuid.UUID, task_id: uuid.UUID) -> TaskPullRequest:
    return TaskPullRequest(
        id=uuid.uuid4(),
        workspace_id=workspace_id,
        task_id=task_id,
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
    )


def _stub_close_machinery(monkeypatch: pytest.MonkeyPatch) -> _CloseGithub:
    """Stub the reconciliation/credential/route seams at the cancel module."""
    monkeypatch.setattr(
        task_cancellation,
        "reconcile_task_pull_request",
        lambda pool, github, *, workspace_id, task_id: _reconciled(
            _open_pull_request(workspace_id, task_id)
        ),
    )

    def _credential(pool: Any, resolver: Any, github: Any, *, profile_id: Any, route: Any) -> Any:
        return object()

    monkeypatch.setattr(task_cancellation, "resolve_owner_write_credential", _credential)
    monkeypatch.setattr(
        task_cancellation, "resolve_owner_write_credential_after_rejection", _credential
    )
    monkeypatch.setattr(
        task_cancellation,
        "resolve_system_repository_installation_route",
        lambda pool, *, workspace_id, repository_id: SimpleNamespace(
            github_installation_id=501,
            repository=SimpleNamespace(identity=SimpleNamespace(github_repository_id=77_000_005)),
        ),
    )
    return _CloseGithub()


def test_the_owner_abort_composition_commits_atomically(conn: Connection[Any]) -> None:
    facts = _abort_fixture(conn)
    pool = facts["pool"]
    adapter = _RecordingClineAdapter()

    outcome = abort_task_activity(
        pool,
        cast(Any, adapter),
        profile_id=facts["profile_id"],
        workspace_id=facts["workspace_id"],
        task_id=facts["task_id"],
        expected_state_token=facts["state_token"],
        target_roles=(WorkflowRole.PRODUCER,),
    )

    assert outcome.kind.value == "interrupted"
    task = task_repositories.get_task(pool, facts["task_id"])
    assert task is not None
    assert task.status is TaskStatus.BLOCKED
    assert task.archived_at is None
    blocks = list_task_blocks(pool, task_id=facts["task_id"])
    assert len(blocks) == 1
    assert blocks[0].reason.value == "owner_action_required"
    assert blocks[0].context["control"] == "owner_abort"
    intents = intent_repositories.list_task_interrupt_intents(pool, task_id=facts["task_id"])
    assert len(intents) == 1
    assert intents[0].status is TaskInterruptIntentStatus.UNSETTLED
    assert intents[0].workflow_role.value == "producer"
    assert intents[0].external_session_id == _EXTERNAL_SESSION_ID
    assert _event_counts(facts["conn"], facts["task_id"]) == {"task_blocked": 1}
    # The exact-session Interrupt dispatched after the commit.
    assert adapter.abort_calls == [_EXTERNAL_SESSION_ID]


def test_a_late_abort_failure_rolls_the_whole_composition_back(
    conn: Connection[Any], monkeypatch: pytest.MonkeyPatch
) -> None:
    facts = _abort_fixture(conn)
    pool = facts["pool"]

    def _boom(*args: Any, **kwargs: Any) -> None:
        raise RuntimeError("forced late block failure")

    monkeypatch.setattr(event_coordination, "record_task_blocked_event", _boom)
    adapter = _RecordingClineAdapter()

    with pytest.raises(RuntimeError, match="forced late block failure"):
        abort_task_activity(
            pool,
            cast(Any, adapter),
            profile_id=facts["profile_id"],
            workspace_id=facts["workspace_id"],
            task_id=facts["task_id"],
            expected_state_token=facts["state_token"],
            target_roles=(WorkflowRole.PRODUCER,),
        )

    task = task_repositories.get_task(pool, facts["task_id"])
    assert task is not None
    assert task.status is TaskStatus.IMPLEMENTING
    assert task.state_token == facts["state_token"]
    assert list_task_blocks(pool, task_id=facts["task_id"]) == []
    assert intent_repositories.list_task_interrupt_intents(pool, task_id=facts["task_id"]) == []
    assert _event_counts(facts["conn"], facts["task_id"]) == {}
    # The runtime is never invoked when the composition rolls back.
    assert adapter.abort_calls == []


def test_the_cancellation_composition_commits_atomically_and_releases_the_issue(
    conn: Connection[Any], monkeypatch: pytest.MonkeyPatch
) -> None:
    facts = _cancel_fixture(conn)
    pool = facts["pool"]
    github = _stub_close_machinery(monkeypatch)

    outcome = cancel_task(
        pool,
        cast(Any, github),
        cast(Any, object()),
        profile_id=facts["profile_id"],
        workspace_id=facts["workspace_id"],
        task_id=facts["task_id"],
        expected_state_token=facts["state_token"],
    )

    assert outcome.kind.value == "cancelled"
    task = task_repositories.get_task(pool, facts["task_id"])
    assert task is not None
    assert task.status is TaskStatus.CANCELLED
    assert task.archived_at is not None
    assert task.queued_at is None
    assert task.current_owner_gate_id is None
    # The exact pending gate resolved CANCELLED with its stamp; the row
    # stays as resolved history.
    gate = gate_repositories.get_owner_gate(pool, owner_gate_id=facts["gate_id"])
    assert gate is not None
    assert gate.status is OwnerGateStatus.CANCELLED
    assert gate.decided_at is not None
    # The OPEN PR_REVIEW loop closed.
    loops = review_repositories.list_task_review_loops(pool, task_id=facts["task_id"], status=None)
    assert len(loops) == 1
    assert loops[0].status.value == "closed"
    assert loops[0].closed_at is not None
    # The durable one-shot PR-close obligation committed with the archival.
    intent = intent_repositories.get_task_pull_request_close_intent_for_task(
        pool, task_id=facts["task_id"]
    )
    assert intent is not None
    assert intent.status is TaskPullRequestCloseIntentStatus.CLOSE_REQUESTED
    assert intent.github_pr_id == _GITHUB_PR_ID
    assert intent.requested_by_profile_id == facts["profile_id"]
    assert _event_counts(facts["conn"], facts["task_id"]) == {
        "owner_gate_resolved": 1,
        "task_cancelled": 1,
    }
    # The close request went out exactly once for the stable PR identity.
    assert len(github.close_calls) == 1
    assert github.close_calls[0]["pull_number"] == _PR_NUMBER
    # Archival released the issue's one-current-Task mapping: a fresh Task
    # for the same GitHub issue can now be created.
    second_task_id = _insert_task(
        conn, workspace_id=facts["workspace_id"], repository_id=facts["repository_id"]
    )
    assert second_task_id is not None


def test_a_late_cancellation_failure_rolls_the_whole_composition_back(
    conn: Connection[Any], monkeypatch: pytest.MonkeyPatch
) -> None:
    facts = _cancel_fixture(conn)
    pool = facts["pool"]

    def _boom(*args: Any, **kwargs: Any) -> None:
        raise RuntimeError("forced late cancellation failure")

    monkeypatch.setattr(event_coordination, "record_task_terminal_event", _boom)
    github = _stub_close_machinery(monkeypatch)

    with pytest.raises(RuntimeError, match="forced late cancellation failure"):
        cancel_task(
            pool,
            cast(Any, github),
            cast(Any, object()),
            profile_id=facts["profile_id"],
            workspace_id=facts["workspace_id"],
            task_id=facts["task_id"],
            expected_state_token=facts["state_token"],
        )

    task = task_repositories.get_task(pool, facts["task_id"])
    assert task is not None
    assert task.status is TaskStatus.WAITING_FOR_OWNER
    assert task.archived_at is None
    assert task.state_token == facts["state_token"]
    gate = gate_repositories.get_owner_gate(pool, owner_gate_id=facts["gate_id"])
    assert gate is not None
    assert gate.status is OwnerGateStatus.PENDING
    assert gate.decided_at is None
    loops = review_repositories.list_task_review_loops(pool, task_id=facts["task_id"], status=None)
    assert loops[0].status.value == "open"
    assert (
        intent_repositories.get_task_pull_request_close_intent_for_task(
            pool, task_id=facts["task_id"]
        )
        is None
    )
    assert _event_counts(facts["conn"], facts["task_id"]) == {}
    # The external close request is never issued for a rolled-back composition.
    assert github.close_calls == []


def test_an_interrupt_intent_cannot_reference_another_tasks_session(
    conn: Connection[Any],
) -> None:
    """The composite subject foreign key binds the Interrupt intent's session
    to the intent's own Task and Workspace: a cross-Task (or cross-Workspace)
    reference is a database-rejected integrity violation, never a silently
    accepted mis-scoped intent."""
    import psycopg.errors

    facts = _abort_fixture(conn)
    # A second current Task in the same Workspace/Repository must carry a
    # different GitHub issue (the one-current-Task-per-issue index).
    other_task_id = uuid.uuid4()
    conn.execute(
        "insert into openorc.tasks "
        "(id, workspace_id, repository_id, github_issue_id, github_issue_number, status, "
        "source_requirements_fingerprint) "
        "values (%s, %s, %s, 7703, 43, 'ready_to_plan', %s)",
        (other_task_id, facts["workspace_id"], facts["repository_id"], "b" * 64),
    )
    connection_row = conn.execute(
        "select connection_id from openorc.task_agent_sessions where task_id = %s",
        (facts["task_id"],),
    ).fetchone()
    assert connection_row is not None
    connection_id = connection_row[0]
    other_session_id = uuid.uuid4()
    conn.execute(
        "insert into openorc.task_agent_sessions "
        "(id, workspace_id, task_id, role, connection_id, external_session_id, "
        "lifecycle_status, effective_config_snapshot, initialized_at) "
        "values (%s, %s, %s, 'producer', %s, 'other-external-session', "
        "'ready', '{}'::jsonb, now())",
        (other_session_id, facts["workspace_id"], other_task_id, connection_id),
    )

    with pytest.raises(psycopg.errors.ForeignKeyViolation):
        conn.execute(
            "insert into openorc.task_interrupt_intents "
            "(workspace_id, task_id, task_agent_session_id, workflow_role, "
            "external_session_id, requested_by_profile_id, status) "
            "values (%s, %s, %s, 'producer', 'other-external-session', %s, 'unsettled')",
            (
                facts["workspace_id"],
                facts["task_id"],
                other_session_id,
                facts["profile_id"],
            ),
        )


def test_a_pr_close_intent_cannot_reference_another_tasks_canonical_pr(
    conn: Connection[Any],
) -> None:
    """The composite subject foreign key binds the PR-close intent's canonical
    record to the intent's own Task and Workspace: a cross-Task (or
    cross-Workspace) canonical PR reference is a database-rejected integrity
    violation."""
    import psycopg.errors

    facts = _cancel_fixture(conn)
    other_profile_id = _insert_profile(conn)
    other_workspace_id = _insert_workspace(conn, other_profile_id)
    other_installation_id = _insert_installation(conn, workspace_id=other_workspace_id)
    other_repository_id = _insert_repository(
        conn, workspace_id=other_workspace_id, github_installation_id=other_installation_id
    )
    other_task_id = _insert_task(
        conn, workspace_id=other_workspace_id, repository_id=other_repository_id
    )
    other_pr_id = _insert_canonical_pr(
        conn,
        workspace_id=other_workspace_id,
        task_id=other_task_id,
        repository_id=other_repository_id,
    )

    with pytest.raises(psycopg.errors.ForeignKeyViolation):
        conn.execute(
            "insert into openorc.task_pull_request_close_intents "
            "(workspace_id, task_id, task_pull_request_id, github_pr_id, "
            "github_pr_number, requested_by_profile_id, status) "
            "values (%s, %s, %s, %s, %s, %s, 'pending')",
            (
                facts["workspace_id"],
                facts["task_id"],
                other_pr_id,
                900_719_925_474_100,
                78,
                facts["profile_id"],
            ),
        )
