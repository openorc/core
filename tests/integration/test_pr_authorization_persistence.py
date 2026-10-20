"""Durable atomicity of the E7 PR-authorization seams (issue #82).

The targeted real-Postgres checks the new seams own: the presentation
composition (the one-time canonical-branch bind, one pending exact-head
``PR_AUTHORIZATION`` gate installed current, the ``implementing →
waiting_for_owner`` transition, token rotation, and the safe OPENORC audit)
commits atomically, and a forced late failure rolls the whole presentation
back — the branch stays unbound, no gate row exists, the Task keeps its
token and status, and no event is written. The reason-specific source
classification: confirmed requirements drift takes the atomic
``github_source_changed`` block path (TaskBlock + ``implementing → blocked``
+ ``task_blocked`` audit, no gate) whose forced late failure also rolls back
completely, while a closed source issue fails closed without fabricating a
block. Owner approval resolves the exact gate APPROVED with the durable
approving Profile and the exact head preserved for E8; Owner cancellation
resolves the gate CANCELLED and archives the Task terminal ``cancelled`` in
one composition.

The deterministic suite proves classification and no-write failures; no
live GitHub, runtime, or Supabase access — the GitHub App client is a local
fake returning fixed observations, and the suite consumes the explicitly
supplied non-production branch database per ``tests/integration/conftest.py``.
"""

from __future__ import annotations

import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime
from typing import Any, cast

import pytest
from psycopg import Connection
from psycopg.types.json import Jsonb

from openorc.adapters.github import (
    GitHubBranchObservation,
    GitHubIssueObservation,
    GitHubRepositoryObservation,
)
from openorc.domain.blocks import TaskBlockReason
from openorc.domain.executions import ExecutionStatus
from openorc.domain.gates import OwnerGateStatus, OwnerGateType
from openorc.domain.github_issues import GitHubIssueState, github_issue_requirements_fingerprint
from openorc.domain.reviews import ReviewLoopPurpose, ReviewLoopStatus, ReviewOutcome
from openorc.domain.tasks import TaskStatus
from openorc.persistence import executions as execution_repositories
from openorc.persistence import gates as gate_repositories
from openorc.persistence import github_issues as issue_repositories
from openorc.persistence import planning as planning_repositories
from openorc.persistence import reviews as review_repositories
from openorc.persistence import tasks as task_repositories
from openorc.persistence.pool import DatabasePool
from openorc.services import event_coordination
from openorc.services.errors import ConflictError
from openorc.services.task_pr_authorization import (
    approve_pr_authorization,
    cancel_pr_authorization,
    open_pr_authorization,
)

pytestmark = pytest.mark.integration

_BASE_SHA = "b" * 40
_HEAD_SHA = "c" * 40
_BRANCH = "openorc/task-42-implementation"
_GITHUB_ISSUE_ID = 7703
_ISSUE_NUMBER = 42
_FINGERPRINT = github_issue_requirements_fingerprint("Found a bug", "Requirements body")
_DRIFTED_FINGERPRINT = github_issue_requirements_fingerprint("Found a bug v2", "Requirements body")
_RESULT_PAYLOAD: dict[str, object] = {
    "type": "implementation_result",
    "schema_version": 1,
    "status": "COMPLETED",
    "branch": _BRANCH,
    "summary": "Implemented the retry loop",
    "changes": ["Added the retry loop"],
    "validation": ["pytest tests/test_retry.py"],
}
_OBSERVED = datetime(2026, 10, 20, 12, 0, 0, tzinfo=UTC)


class _FakeGitHubAppClient:
    """A local fixed-observation client for the durable-composition checks."""

    def __init__(
        self,
        *,
        issue_title: str = "Found a bug",
        issue_state: str = "open",
        github_issue_id: int = _GITHUB_ISSUE_ID,
    ) -> None:
        self._issue_title = issue_title
        self._issue_state = issue_state
        self._github_issue_id = github_issue_id

    def get_installation_repository(
        self, *, github_installation_id: int, github_repository_id: int
    ) -> GitHubRepositoryObservation:
        return GitHubRepositoryObservation(
            github_repository_id=github_repository_id,
            owner_login="octocat",
            name="hello-world",
            html_url="https://github.com/octocat/hello-world",
            is_private=False,
            default_branch="main",
        )

    def get_repository_issue(
        self,
        *,
        github_installation_id: int,
        owner_login: str,
        repository_name: str,
        issue_number: int,
    ) -> GitHubIssueObservation:
        return GitHubIssueObservation(
            github_issue_id=self._github_issue_id,
            issue_number=issue_number,
            title=self._issue_title,
            body="Requirements body",
            state=self._issue_state,
            provider_updated_at=_OBSERVED,
            is_pull_request=False,
        )

    def get_repository_branch(
        self,
        *,
        github_installation_id: int,
        owner_login: str,
        repository_name: str,
        branch_name: str,
    ) -> GitHubBranchObservation:
        assert branch_name == _BRANCH
        return GitHubBranchObservation(branch_name=branch_name, head_sha=_HEAD_SHA)

    def create_pull_request(self, **_kwargs: Any) -> Any:
        raise AssertionError("the E7 seam must not create pull requests")

    def get_repository_pull_request(self, **_kwargs: Any) -> Any:
        raise AssertionError("the E7 seam must not observe pull requests")

    def merge_pull_request(self, **_kwargs: Any) -> Any:
        raise AssertionError("the E7 seam must not request merges")

    def validate_installation_repository_access(self, **_kwargs: Any) -> Any:
        raise AssertionError("the E7 seam must not compose raw #58 validation operations")

    def validate_user_installation_repository_access(self, **_kwargs: Any) -> Any:
        raise AssertionError("the E7 seam must not compose user-write validation")

    def get_commit_check_runs(self, **_kwargs: Any) -> Any:
        raise AssertionError("the E7 seam must not project check runs")

    def get_commit_combined_status(self, **_kwargs: Any) -> Any:
        raise AssertionError("the E7 seam must not project combined status")

    def get_installation_capabilities(self, github_installation_id: int) -> Any:
        raise AssertionError("the E7 seam must not compose raw adapter operations")

    def get_issue_blocked_by(self, **_kwargs: Any) -> Any:
        raise AssertionError("the E7 seam must not observe relationships")

    def get_issue_sub_issues(self, **_kwargs: Any) -> Any:
        raise AssertionError("the E7 seam must not observe relationships")

    def get_issue_parent(self, **_kwargs: Any) -> Any:
        raise AssertionError("the E7 seam must not observe relationships")

    def resolve_related_issue_endpoints(self, **_kwargs: Any) -> Any:
        raise AssertionError("the E7 seam must not resolve related endpoints")


class _SingleConnectionPool:
    """Minimal DatabasePool adapter sharing the test connection and transaction."""

    def __init__(self, connection: Connection[Any]) -> None:
        self._connection = connection

    def connection(self) -> Any:
        @contextmanager
        def managed() -> Iterator[Any]:
            with self._connection.transaction():
                yield self._connection

        return managed()

    def close(self) -> None:
        raise AssertionError("the test fixture owns the connection lifetime")


def _insert_profile(conn: Connection[Any]) -> uuid.UUID:
    profile_id = uuid.uuid4()
    conn.execute("insert into auth.users (id) values (%s)", (profile_id,))
    conn.execute("insert into openorc.profiles (id) values (%s)", (profile_id,))
    return profile_id


def _insert_workspace(conn: Connection[Any], owner_profile_id: uuid.UUID) -> uuid.UUID:
    workspace_id = uuid.uuid4()
    conn.execute(
        "insert into openorc.workspaces (id, owner_profile_id, name) values (%s, %s, %s)",
        (workspace_id, owner_profile_id, "workspace"),
    )
    return workspace_id


def _insert_project(conn: Connection[Any], workspace_id: uuid.UUID) -> uuid.UUID:
    project_id = uuid.uuid4()
    conn.execute(
        "insert into openorc.projects (id, workspace_id, name) values (%s, %s, %s)",
        (project_id, workspace_id, "project"),
    )
    return project_id


def _insert_installation(conn: Connection[Any], *, workspace_id: uuid.UUID) -> uuid.UUID:
    installation_id = uuid.uuid4()
    conn.execute(
        "insert into openorc.github_installations "
        "(id, workspace_id, github_installation_id, github_account_id, "
        "account_login, account_type) "
        "values (%s, %s, %s, %s, %s, %s)",
        (installation_id, workspace_id, 77_000_007, 77_000_008, "octocat", "Organization"),
    )
    return installation_id


def _insert_repository(
    conn: Connection[Any],
    *,
    workspace_id: uuid.UUID,
    github_installation_id: uuid.UUID,
) -> uuid.UUID:
    repository_id = uuid.uuid4()
    conn.execute(
        "insert into openorc.repositories "
        "(id, project_id, workspace_id, github_repository_id, owner_login, name, "
        "html_url, is_private, default_branch, github_installation_id) "
        "values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
        (
            repository_id,
            _insert_project(conn, workspace_id),
            workspace_id,
            77_000_009,
            "octocat",
            "hello-world",
            "https://github.com/octocat/hello-world",
            False,
            "main",
            github_installation_id,
        ),
    )
    return repository_id


def _insert_task(
    conn: Connection[Any],
    *,
    workspace_id: uuid.UUID,
    repository_id: uuid.UUID,
    fingerprint: str,
) -> uuid.UUID:
    task_id = uuid.uuid4()
    conn.execute(
        "insert into openorc.tasks "
        "(id, workspace_id, repository_id, github_issue_id, github_issue_number, status, "
        "source_requirements_fingerprint) "
        "values (%s, %s, %s, %s, %s, 'implementing', %s)",
        (task_id, workspace_id, repository_id, _GITHUB_ISSUE_ID, _ISSUE_NUMBER, fingerprint),
    )
    return task_id


def _insert_connection(conn: Connection[Any], *, workspace_id: uuid.UUID) -> uuid.UUID:
    connection_id = uuid.uuid4()
    conn.execute(
        "insert into openorc.connections (id, workspace_id, adapter_type, name, session_capacity) "
        "values (%s, %s, 'cline', %s, %s)",
        (connection_id, workspace_id, "connection", 2),
    )
    return connection_id


def _insert_producer_session(
    conn: Connection[Any], *, workspace_id: uuid.UUID, task_id: uuid.UUID
) -> uuid.UUID:
    session_id = uuid.uuid4()
    conn.execute(
        "insert into openorc.task_agent_sessions "
        "(id, workspace_id, task_id, role, connection_id, external_session_id, "
        "effective_config_snapshot, lifecycle_status, initialized_at) "
        "values (%s, %s, %s, 'producer', %s, %s, %s, 'ready', now())",
        (
            session_id,
            workspace_id,
            task_id,
            _insert_connection(conn, workspace_id=workspace_id),
            "ext-producer-session",
            Jsonb({"repository_base": "/workspace/task-42"}),
        ),
    )
    return session_id


def _row_count(conn: Connection[Any], sql: str, params: tuple[Any, ...]) -> int:
    return int(conn.execute(sql, params).fetchone()[0])  # type: ignore[index]


def _pr_authorization_fixture(conn: Connection[Any]) -> dict[str, Any]:
    """The presentation-ready durable fixture.

    Ownership chain, the durable issue projection at the Task's baseline
    fingerprint, one current PlanRevision, one CLOSED planning loop whose
    finalized iteration 1 is the Reviewer ACCEPTED clearance, the
    Owner-approved historical IMPLEMENTATION_AUTHORIZATION gate bound to
    that plan, the original READY Producer binding, and the one durably
    finalized successful E6 Execution carrying the validated result — with
    the Task still ``implementing``, unbound, and gate-free.
    """
    profile_id = _insert_profile(conn)
    workspace_id = _insert_workspace(conn, profile_id)
    installation_id = _insert_installation(conn, workspace_id=workspace_id)
    repository_id = _insert_repository(
        conn, workspace_id=workspace_id, github_installation_id=installation_id
    )
    task_id = _insert_task(
        conn, workspace_id=workspace_id, repository_id=repository_id, fingerprint=_FINGERPRINT
    )
    pool = cast(DatabasePool, _SingleConnectionPool(conn))

    projected = issue_repositories.reconcile_github_issue(
        pool,
        workspace_id=workspace_id,
        repository_id=repository_id,
        github_issue_id=_GITHUB_ISSUE_ID,
        issue_number=_ISSUE_NUMBER,
        title="Found a bug",
        body="Requirements body",
        state=GitHubIssueState.OPEN,
        requirements_fingerprint=_FINGERPRINT,
        provider_updated_at=None,
    )
    assert projected.outcome is issue_repositories.GitHubIssueReconcileOutcome.INSERTED

    revision = planning_repositories.create_plan_revision(
        pool,
        workspace_id=workspace_id,
        task_id=task_id,
        revision_number=1,
        content="# Plan",
        repository_base_sha=_BASE_SHA,
    )
    task = task_repositories.get_task(pool, task_id)
    assert task is not None
    pointed = task_repositories.set_current_plan_revision(
        pool,
        task_id,
        expected_state_token=task.state_token,
        plan_revision_id=revision.id,
    )
    assert pointed is not None
    loop = review_repositories.create_review_loop(
        pool,
        workspace_id=workspace_id,
        task_id=task_id,
        purpose=ReviewLoopPurpose.PLANNING,
        iteration_limit=5,
    )
    iteration = review_repositories.create_review_iteration(
        pool,
        workspace_id=workspace_id,
        task_id=task_id,
        review_loop_id=loop.id,
        iteration_number=1,
        plan_revision_id=revision.id,
    )
    assert iteration is not None
    finalized = review_repositories.record_review_iteration_result(
        pool,
        review_iteration_id=iteration.id,
        outcome=ReviewOutcome.ACCEPTED,
        summary="Looks good",
        findings=[],
    )
    assert finalized is not None
    closed = review_repositories.close_review_loop(pool, review_loop_id=loop.id)
    assert closed is not None and closed.status is ReviewLoopStatus.CLOSED

    gate = gate_repositories.create_owner_gate(
        pool,
        workspace_id=workspace_id,
        task_id=task_id,
        gate_type=OwnerGateType.IMPLEMENTATION_AUTHORIZATION,
        plan_revision_id=revision.id,
    )
    installed = task_repositories.set_current_owner_gate(
        pool,
        task_id,
        expected_state_token=pointed.state_token,
        owner_gate_id=gate.id,
    )
    assert installed is not None
    resolution = gate_repositories.resolve_owner_gate(
        pool,
        owner_gate_id=gate.id,
        outcome=OwnerGateStatus.APPROVED,
        expected_task_state_token=installed.state_token,
    )
    assert resolution.outcome is gate_repositories.OwnerGateResolutionOutcome.RESOLVED
    approved_task = resolution.task
    assert approved_task is not None

    producer_session_id = _insert_producer_session(conn, workspace_id=workspace_id, task_id=task_id)
    execution = execution_repositories.create_execution(
        pool,
        workspace_id=workspace_id,
        task_id=task_id,
        producer_session_id=producer_session_id,
        execution_number=1,
    )
    claimed = execution_repositories.claim_execution_running(pool, execution_id=execution.id)
    assert claimed is not None
    succeeded = execution_repositories.complete_execution_with_result(
        pool, execution_id=execution.id, implementation_result=_RESULT_PAYLOAD
    )
    assert succeeded is not None
    assert succeeded.status is ExecutionStatus.SUCCEEDED

    return {
        "pool": pool,
        "profile_id": profile_id,
        "workspace_id": workspace_id,
        "task_id": task_id,
        "state_token": approved_task.state_token,
    }


def _open_call(pool: DatabasePool, facts: dict[str, Any]) -> Any:
    return open_pr_authorization(
        pool,
        _FakeGitHubAppClient(),
        profile_id=facts["profile_id"],
        workspace_id=facts["workspace_id"],
        task_id=facts["task_id"],
        expected_state_token=facts["state_token"],
    )


def _approve_call(pool: DatabasePool, facts: dict[str, Any], *, gate_id: Any) -> Any:
    return approve_pr_authorization(
        pool,
        profile_id=facts["profile_id"],
        workspace_id=facts["workspace_id"],
        task_id=facts["task_id"],
        owner_gate_id=gate_id,
        expected_state_token=facts["state_token"],
    )


def _cancel_call(pool: DatabasePool, facts: dict[str, Any], *, gate_id: Any) -> Any:
    return cancel_pr_authorization(
        pool,
        profile_id=facts["profile_id"],
        workspace_id=facts["workspace_id"],
        task_id=facts["task_id"],
        owner_gate_id=gate_id,
        expected_state_token=facts["state_token"],
    )


def _task_block_rows(conn: Connection[Any], task_id: uuid.UUID) -> list[tuple[Any, ...]]:
    return list(
        conn.execute(
            "select reason, context from openorc.task_blocks "
            "where task_id = %s order by created_at",
            (task_id,),
        ).fetchall()
    )


def _event_types(conn: Connection[Any], task_id: uuid.UUID) -> list[tuple[str, str, Any]]:
    """The Task's audit events, sorted by type for order-independent comparison.

    Every statement of a test runs inside the fixture's one outer
    transaction, so ``created_at`` is constant and event *sequence* is not
    observable here — the deterministic suite owns the write-order proofs.
    """
    rows = conn.execute(
        "select event_type, actor_type, actor_id from openorc.workflow_events where task_id = %s",
        (task_id,),
    ).fetchall()
    return sorted(rows, key=lambda row: row[0])


def test_the_presentation_composition_commits_atomically(conn: Connection[Any]) -> None:
    """One verified bind, one exact-head gate, the transition, the token
    rotation, and the safe OPENORC audit commit together."""
    facts = _pr_authorization_fixture(conn)
    pool = facts["pool"]

    presented = _open_call(pool, facts)

    assert presented.owner_gate.status is OwnerGateStatus.PENDING
    assert presented.owner_gate.gate_type is OwnerGateType.PR_AUTHORIZATION
    assert presented.owner_gate.plan_revision_id is None
    assert presented.owner_gate.task_pull_request_id is None
    assert presented.owner_gate.subject_head_sha == _HEAD_SHA
    task = task_repositories.get_task(pool, facts["task_id"])
    assert task is not None
    assert task.status is TaskStatus.WAITING_FOR_OWNER
    assert task.canonical_feature_branch == _BRANCH
    assert task.current_owner_gate_id == presented.owner_gate.id
    assert task.state_token == presented.task.state_token
    assert task.state_token != facts["state_token"]
    # Exactly one new pending gate of this type and no blocking side records.
    assert (
        _row_count(
            conn,
            "select count(*) from openorc.owner_gates "
            "where task_id = %s and gate_type = 'pr_authorization' and status = 'pending'",
            (facts["task_id"],),
        )
        == 1
    )
    assert _task_block_rows(conn, facts["task_id"]) == []
    assert _event_types(conn, facts["task_id"]) == [("owner_gate_created", "openorc", None)]


def test_a_late_presentation_failure_rolls_everything_back(
    conn: Connection[Any],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A failure after the bind and gate installation applies nothing: the
    branch stays unbound, no gate row exists, the Task keeps its token and
    status, and no event is written."""
    facts = _pr_authorization_fixture(conn)
    pool = facts["pool"]
    token_before = facts["state_token"]

    def _boom(*args: Any, **kwargs: Any) -> None:
        raise RuntimeError("forced late presentation failure")

    monkeypatch.setattr(event_coordination, "record_owner_gate_created_event", _boom)
    with pytest.raises(RuntimeError, match="forced late presentation failure"):
        _open_call(pool, facts)

    task = task_repositories.get_task(pool, facts["task_id"])
    assert task is not None
    assert task.status is TaskStatus.IMPLEMENTING
    assert task.canonical_feature_branch is None
    assert task.current_owner_gate_id is None
    assert task.state_token == token_before
    assert (
        _row_count(
            conn,
            "select count(*) from openorc.owner_gates where task_id = %s",
            (facts["task_id"],),
        )
        == 1  # only the fixture's historical approved implementation gate
    )
    assert _event_types(conn, facts["task_id"]) == []


def test_confirmed_drift_blocks_atomically_and_creates_no_gate(conn: Connection[Any]) -> None:
    """The confirmed fingerprint change takes the one atomic block path: the
    TaskBlock, the implementing → blocked transition, and the task_blocked
    audit commit together — and no PR gate exists afterwards."""
    facts = _pr_authorization_fixture(conn)
    pool = facts["pool"]
    token_before = facts["state_token"]

    with pytest.raises(ConflictError):
        open_pr_authorization(
            pool,
            _FakeGitHubAppClient(issue_title="Found a bug v2"),
            profile_id=facts["profile_id"],
            workspace_id=facts["workspace_id"],
            task_id=facts["task_id"],
            expected_state_token=facts["state_token"],
        )

    task = task_repositories.get_task(pool, facts["task_id"])
    assert task is not None
    assert task.status is TaskStatus.BLOCKED
    assert task.canonical_feature_branch is None
    assert task.current_owner_gate_id is None
    assert task.state_token != token_before
    (block,) = _task_block_rows(conn, facts["task_id"])
    assert block[0] == TaskBlockReason.GITHUB_SOURCE_CHANGED.value
    assert block[1]["expected_fingerprint"] == _FINGERPRINT
    assert block[1]["observed_fingerprint"] == _DRIFTED_FINGERPRINT
    assert (
        _row_count(
            conn,
            "select count(*) from openorc.owner_gates "
            "where task_id = %s and gate_type = 'pr_authorization'",
            (facts["task_id"],),
        )
        == 0
    )
    assert _event_types(conn, facts["task_id"]) == [("task_blocked", "openorc", None)]


def test_a_late_drift_block_failure_rolls_the_block_back(
    conn: Connection[Any],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A failure after the TaskBlock creation rolls the whole drift-block
    composition back: the Task stays implementing with its token and no
    block row or event exists."""
    facts = _pr_authorization_fixture(conn)
    pool = facts["pool"]
    token_before = facts["state_token"]

    def _boom(*args: Any, **kwargs: Any) -> None:
        raise RuntimeError("forced late block failure")

    monkeypatch.setattr(event_coordination, "record_task_blocked_event", _boom)
    with pytest.raises(RuntimeError, match="forced late block failure"):
        open_pr_authorization(
            pool,
            _FakeGitHubAppClient(issue_title="Found a bug v2"),
            profile_id=facts["profile_id"],
            workspace_id=facts["workspace_id"],
            task_id=facts["task_id"],
            expected_state_token=facts["state_token"],
        )

    task = task_repositories.get_task(pool, facts["task_id"])
    assert task is not None
    assert task.status is TaskStatus.IMPLEMENTING
    assert task.state_token == token_before
    assert _task_block_rows(conn, facts["task_id"]) == []
    assert _event_types(conn, facts["task_id"]) == []


def test_owner_approval_persists_the_exact_head_and_approving_profile(
    conn: Connection[Any],
) -> None:
    """The correct Profile's exact-gate approval resolves it OWNER-attributed
    and yields the durable approving Profile plus the exact approved head;
    the Task stays waiting_for_owner with no PR record."""
    facts = _pr_authorization_fixture(conn)
    pool = facts["pool"]
    presented = _open_call(pool, facts)
    gate_id = presented.owner_gate.id
    facts["state_token"] = presented.task.state_token

    approved = _approve_call(pool, facts, gate_id=gate_id)

    assert approved.owner_gate.status is OwnerGateStatus.APPROVED
    assert approved.owner_gate.decided_at is not None
    assert approved.owner_gate.subject_head_sha == _HEAD_SHA
    task = task_repositories.get_task(pool, facts["task_id"])
    assert task is not None
    assert task.status is TaskStatus.WAITING_FOR_OWNER
    assert task.current_owner_gate_id is None
    assert task.state_token == approved.task.state_token
    assert _row_count(conn, "select count(*) from openorc.task_pull_requests", ()) == 0
    events = _event_types(conn, facts["task_id"])
    assert events == [
        ("owner_gate_created", "openorc", None),
        ("owner_gate_resolved", "owner", str(facts["profile_id"])),
    ]
    # E8 recovers the approving Profile durably from the resolution audit.
    resolution_event = conn.execute(
        "select actor_id from openorc.workflow_events "
        "where task_id = %s and event_type = 'owner_gate_resolved'",
        (facts["task_id"],),
    ).fetchone()
    assert resolution_event is not None
    assert resolution_event[0] == str(facts["profile_id"])


def test_owner_cancellation_archives_atomically(conn: Connection[Any]) -> None:
    """Cancellation resolves the exact pending gate CANCELLED (never
    REJECTED) and archives the Task terminal cancelled with its OWNER
    terminal event in one composition."""
    facts = _pr_authorization_fixture(conn)
    pool = facts["pool"]
    presented = _open_call(pool, facts)
    gate_id = presented.owner_gate.id
    facts["state_token"] = presented.task.state_token

    cancelled = _cancel_call(pool, facts, gate_id=gate_id)

    assert cancelled.owner_gate.status is OwnerGateStatus.CANCELLED
    task = task_repositories.get_task(pool, facts["task_id"])
    assert task is not None
    assert task.status is TaskStatus.CANCELLED
    assert task.archived_at is not None
    assert task.canonical_feature_branch == _BRANCH
    events = _event_types(conn, facts["task_id"])
    assert [event[0] for event in events] == [
        "owner_gate_created",
        "owner_gate_resolved",
        "task_cancelled",
    ]
    assert events[1][1] == "owner" and events[1][2] == str(facts["profile_id"])
    assert events[2][1] == "owner" and events[2][2] == str(facts["profile_id"])
