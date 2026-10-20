"""Durable atomicity of the G1 change-request handoff (issue #219).

The one targeted real-Postgres check the new seam owns: the
implementation-gate change-request handoff's multi-record composition — the
NEW finalized same-plan Reviewer change request, the safe REVIEWER audit
events, and the conditional supersession of the exact current pending
IMPLEMENTATION_AUTHORIZATION gate (current-pointer clear,
``waiting_for_owner`` → ``planning``, token rotation, gate row untouched) —
commits atomically, and a forced late failure rolls the whole composition
back, leaving the gate pending and current, the Task untouched, and no
handoff facts behind. The post-commit E2 revision production routes only
the new findings through the real fakes, continuing on the post-supersession
token.

E3c's existing settlement atomicity/rollback coverage and E3b's exhaustion
threshold are not duplicated here. No live GitHub, runtime, or Supabase
access; the suite consumes the explicitly supplied non-production branch
database per ``tests/integration/conftest.py``.
"""

from __future__ import annotations

import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any, cast

import pytest
from psycopg import Connection

from openorc.adapters.agent_runtime.results import AgentSessionCreationRequest
from openorc.adapters.github import (
    GitHubAppClient,
    GitHubBranchObservation,
    GitHubRepositoryObservation,
)
from openorc.domain.connections import WorkflowRole
from openorc.domain.gates import OwnerGateStatus, OwnerGateType
from openorc.domain.reviews import ReviewLoopPurpose, ReviewLoopStatus, ReviewOutcome
from openorc.domain.tasks import TaskStatus
from openorc.persistence import gates as gate_repositories
from openorc.persistence import planning as planning_repositories
from openorc.persistence import reviews as review_repositories
from openorc.persistence import tasks as task_repositories
from openorc.persistence.pool import DatabasePool
from openorc.protocol.models import ReviewFinding, ReviewResult
from openorc.services import event_coordination
from openorc.services.task_planning_review_resolution import (
    PlanningGateChangeRequestDecision,
    supersede_implementation_authorization_with_changes,
)
from tests.fakes.agent_runtime import FakeAgentRuntimeAdapter, session_ready_candidate

pytestmark = pytest.mark.integration

_BASE_SHA = "d" * 40
_PLAN_TEXT = "# Plan\n\nStep one.\n"
_REVISED_PLAN_TEXT = "# Plan (revised)\n\nStep one, clarified.\n"
_EXTERNAL_INSTALLATION_ID = 77_000_004


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


class _FakeGitHubClient:
    """Scripted observation fake: the fresh address and the committed base SHA."""

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

    def get_repository_branch(
        self,
        *,
        github_installation_id: int,
        owner_login: str,
        repository_name: str,
        branch_name: str,
    ) -> GitHubBranchObservation:
        return GitHubBranchObservation(branch_name=branch_name, head_sha=_BASE_SHA)


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
            77_000_005,
            "octocat",
            "hello-world",
            "https://github.com/octocat/hello-world",
            False,
            "main",
            # The production route resolver requires the repository-to-installation
            # relationship: the durable UUID of the same-Workspace installation
            # row, never the external numeric installation ID.
            github_installation_id,
        ),
    )
    return repository_id


def _insert_installation(conn: Connection[Any], *, workspace_id: uuid.UUID) -> uuid.UUID:
    installation_id = uuid.uuid4()
    conn.execute(
        "insert into openorc.github_installations "
        "(id, workspace_id, github_installation_id, github_account_id, "
        "account_login, account_type) "
        "values (%s, %s, %s, %s, %s, %s)",
        (
            installation_id,
            workspace_id,
            _EXTERNAL_INSTALLATION_ID,
            77_000_006,
            "octocat",
            "Organization",
        ),
    )
    return installation_id


def _insert_task(
    conn: Connection[Any], *, workspace_id: uuid.UUID, repository_id: uuid.UUID
) -> uuid.UUID:
    task_id = uuid.uuid4()
    conn.execute(
        "insert into openorc.tasks "
        "(id, workspace_id, repository_id, github_issue_id, github_issue_number, status, "
        "source_requirements_fingerprint) "
        "values (%s, %s, %s, %s, %s, 'planning', %s)",
        (task_id, workspace_id, repository_id, 7702, 42, "a" * 64),
    )
    return task_id


def _insert_connection(conn: Connection[Any], *, workspace_id: uuid.UUID) -> uuid.UUID:
    connection_id = uuid.uuid4()
    conn.execute(
        "insert into openorc.connections (id, workspace_id, adapter_type, name) "
        "values (%s, %s, 'cline', 'primary hub')",
        (connection_id, workspace_id),
    )
    return connection_id


def _insert_ready_session(
    conn: Connection[Any],
    *,
    workspace_id: uuid.UUID,
    task_id: uuid.UUID,
    connection_id: uuid.UUID,
    role: str,
    external_session_id: str,
) -> uuid.UUID:
    session_id = uuid.uuid4()
    conn.execute(
        "insert into openorc.task_agent_sessions "
        "(id, workspace_id, task_id, role, connection_id, external_session_id, "
        "lifecycle_status, effective_config_snapshot, initialized_at) "
        "values (%s, %s, %s, %s, %s, %s, 'ready', '{}'::jsonb, now())",
        (session_id, workspace_id, task_id, role, connection_id, external_session_id),
    )
    return session_id


def _row_count(conn: Connection[Any], sql: str, params: tuple[Any, ...]) -> int:
    return int(conn.execute(sql, params).fetchone()[0])  # type: ignore[index]


def _handoff_fixture(
    conn: Connection[Any],
) -> tuple[DatabasePool, FakeAgentRuntimeAdapter, _FakeGitHubClient, str, dict[str, Any]]:
    """The handoff-ready durable fixture.

    Ownership chain, READY Producer and Reviewer bindings with one live fake
    Producer session, one snapshotted OPEN planning loop whose iteration 1 is
    the Reviewer ACCEPTED clearance, and the exact current pending
    plan-subject IMPLEMENTATION_AUTHORIZATION gate installed on the
    ``waiting_for_owner`` Task it guards.
    """
    profile_id = _insert_profile(conn)
    workspace_id = _insert_workspace(conn, profile_id)
    installation_id = _insert_installation(conn, workspace_id=workspace_id)
    repository_id = _insert_repository(
        conn, workspace_id=workspace_id, github_installation_id=installation_id
    )
    task_id = _insert_task(conn, workspace_id=workspace_id, repository_id=repository_id)
    connection_id = _insert_connection(conn, workspace_id=workspace_id)
    adapter = FakeAgentRuntimeAdapter()
    adapter.queue_creation(session_ready_candidate())
    producer = adapter.create_session(
        AgentSessionCreationRequest(
            workspace_id=uuid.UUID(int=0),
            task_id=uuid.UUID(int=0),
            role=WorkflowRole.PRODUCER,
            initialization="prepared",
        )
    )
    producer_external_id = producer.external_session_id
    assert producer_external_id is not None
    _insert_ready_session(
        conn,
        workspace_id=workspace_id,
        task_id=task_id,
        connection_id=connection_id,
        role="producer",
        external_session_id=producer_external_id,
    )
    _insert_ready_session(
        conn,
        workspace_id=workspace_id,
        task_id=task_id,
        connection_id=connection_id,
        role="reviewer",
        external_session_id="fake-reviewer-1",
    )
    pool = cast(DatabasePool, _SingleConnectionPool(conn))

    revision = planning_repositories.create_plan_revision(
        pool,
        workspace_id=workspace_id,
        task_id=task_id,
        revision_number=1,
        content=_PLAN_TEXT,
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
    clearance = review_repositories.create_review_iteration(
        pool,
        workspace_id=workspace_id,
        task_id=task_id,
        review_loop_id=loop.id,
        iteration_number=1,
        plan_revision_id=revision.id,
    )
    assert clearance is not None
    review_repositories.record_review_iteration_result(
        pool,
        review_iteration_id=clearance.id,
        outcome=ReviewOutcome.ACCEPTED,
        summary="Looks good",
        findings=[],
    )
    gate = gate_repositories.create_owner_gate(
        pool,
        workspace_id=workspace_id,
        task_id=task_id,
        gate_type=OwnerGateType.IMPLEMENTATION_AUTHORIZATION,
        plan_revision_id=revision.id,
    )
    after_install = task_repositories.set_current_owner_gate(
        pool,
        task_id,
        expected_state_token=pointed.state_token,
        owner_gate_id=gate.id,
    )
    assert after_install is not None
    waiting = task_repositories.update_task_status(
        pool,
        task_id,
        expected_state_token=after_install.state_token,
        status=TaskStatus.WAITING_FOR_OWNER,
    )
    assert waiting is not None
    return (
        pool,
        adapter,
        _FakeGitHubClient(),
        producer_external_id,
        {
            "workspace_id": workspace_id,
            "task_id": task_id,
            "profile_id": profile_id,
            "revision_id": revision.id,
            "loop_id": loop.id,
            "gate_id": gate.id,
            "state_token": waiting.state_token,
        },
    )


def _handoff_call(
    pool: DatabasePool,
    adapter: FakeAgentRuntimeAdapter,
    github: _FakeGitHubClient,
    facts: dict[str, Any],
    *,
    record_discussion_message_events: bool = False,
) -> Any:
    return supersede_implementation_authorization_with_changes(
        pool,
        adapter,
        cast(GitHubAppClient, github),
        profile_id=facts["profile_id"],
        workspace_id=facts["workspace_id"],
        task_id=facts["task_id"],
        expected_state_token=facts["state_token"],
        review_result=ReviewResult(
            outcome="CHANGES_REQUESTED",
            summary="Needs another pass",
            findings=(
                ReviewFinding(summary="Clarify the retry loop", details="Explain the backoff."),
            ),
        ),
        record_discussion_message_events=record_discussion_message_events,
    )


def test_the_gate_change_handoff_commits_its_cross_record_composition_atomically(
    conn: Connection[Any],
) -> None:
    """The whole composition commits: one NEW next-numbered same-plan
    Reviewer change request, the safe REVIEWER audit events, the superseded
    pending gate (pointer cleared, Task ``planning`` with a rotated token,
    gate row untouched), and the post-commit E2 route that persists revision
    2 on the post-supersession token — with no ``OWNER_GATE_RESOLVED`` event
    and no gate stamp anywhere."""
    pool, adapter, github, producer_external_id, facts = _handoff_fixture(conn)
    adapter.queue_send(
        producer_external_id,
        {"type": "plan_result", "schema_version": 1, "plan": _REVISED_PLAN_TEXT},
    )

    outcome = _handoff_call(pool, adapter, github, facts)

    assert outcome.decision is PlanningGateChangeRequestDecision.PLANNING_CONTINUATION_ROUTED
    gate = gate_repositories.get_owner_gate(pool, owner_gate_id=facts["gate_id"])
    assert gate is not None
    # The superseded gate is immutable pending history: never stamped.
    assert gate.status is OwnerGateStatus.PENDING
    assert gate.decided_at is None
    task = task_repositories.get_task(pool, facts["task_id"])
    assert task is not None
    assert task.status is TaskStatus.PLANNING
    assert task.current_owner_gate_id is None
    assert task.current_plan_revision_id != facts["revision_id"]
    assert task.state_token != facts["state_token"]
    # E2 continued on the post-supersession token and persisted revision 2.
    assert task.current_plan_revision_id == outcome.revision_production.plan_revision.id
    iterations = review_repositories.list_review_loop_iterations(
        pool, review_loop_id=facts["loop_id"]
    )
    assert [iteration.iteration_number for iteration in iterations] == [1, 2]
    assert iterations[0].outcome is ReviewOutcome.ACCEPTED
    assert iterations[1].outcome is ReviewOutcome.CHANGES_REQUESTED
    assert iterations[1].plan_revision_id == facts["revision_id"]
    assert iterations[1].summary == "Needs another pass"
    loop = review_repositories.get_review_loop(pool, review_loop_id=facts["loop_id"])
    assert loop is not None
    assert loop.status is ReviewLoopStatus.OPEN
    assert loop.closed_at is None
    # The safe audit trail: the REVIEWER plan review on the new iteration,
    # the two direction-only discussion facts, and E2's revision event —
    # and never an Owner gate-resolution verdict.
    assert (
        _row_count(
            conn,
            "select count(*) from openorc.workflow_events where task_id = %s "
            "and event_type = 'plan_reviewed' and actor_type = 'reviewer' "
            "and subject_type = 'review_iteration' and subject_id = %s",
            (facts["task_id"], iterations[1].id),
        )
        == 1
    )
    assert (
        _row_count(
            conn,
            "select count(*) from openorc.workflow_events where task_id = %s "
            "and event_type = 'owner_reviewer_discussion_message'",
            (facts["task_id"],),
        )
        == 2
    )
    assert (
        _row_count(
            conn,
            "select count(*) from openorc.workflow_events where task_id = %s "
            "and event_type = 'owner_gate_resolved'",
            (facts["task_id"],),
        )
        == 0
    )
    assert (
        _row_count(
            conn,
            "select count(*) from openorc.workflow_events where task_id = %s "
            "and event_type = 'plan_revision_revised'",
            (facts["task_id"],),
        )
        == 1
    )


def test_a_late_handoff_failure_rolls_the_whole_composition_back(
    conn: Connection[Any],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A failure after the iteration result and the supersession applies
    nothing: the gate stays pending and current, the Task keeps its token
    and status, the handoff iteration vanishes, the loop stays OPEN, and no
    handoff event was written."""
    pool, adapter, github, _producer_external_id, facts = _handoff_fixture(conn)

    def _boom(*args: Any, **kwargs: Any) -> None:
        raise RuntimeError("forced late handoff failure")

    monkeypatch.setattr(event_coordination, "record_owner_reviewer_discussion_message_event", _boom)
    events_before = _row_count(
        conn,
        "select count(*) from openorc.workflow_events where task_id = %s",
        (facts["task_id"],),
    )

    with pytest.raises(RuntimeError, match="forced late handoff failure"):
        _handoff_call(pool, adapter, github, facts, record_discussion_message_events=True)

    gate = gate_repositories.get_owner_gate(pool, owner_gate_id=facts["gate_id"])
    assert gate is not None
    assert gate.status is OwnerGateStatus.PENDING
    assert gate.decided_at is None
    task = task_repositories.get_task(pool, facts["task_id"])
    assert task is not None
    assert task.status is TaskStatus.WAITING_FOR_OWNER
    assert task.current_owner_gate_id == facts["gate_id"]
    assert task.current_plan_revision_id == facts["revision_id"]
    assert task.state_token == facts["state_token"]
    iterations = review_repositories.list_review_loop_iterations(
        pool, review_loop_id=facts["loop_id"]
    )
    assert [iteration.iteration_number for iteration in iterations] == [1]
    assert iterations[0].outcome is ReviewOutcome.ACCEPTED
    loop = review_repositories.get_review_loop(pool, review_loop_id=facts["loop_id"])
    assert loop is not None
    assert loop.status is ReviewLoopStatus.OPEN
    assert (
        _row_count(
            conn,
            "select count(*) from openorc.workflow_events where task_id = %s",
            (facts["task_id"],),
        )
        == events_before
    )
    assert (
        _row_count(
            conn,
            "select count(*) from openorc.plan_revisions where task_id = %s",
            (facts["task_id"],),
        )
        == 1
    )
