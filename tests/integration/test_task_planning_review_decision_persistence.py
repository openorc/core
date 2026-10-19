"""Durable atomicity of the E3b exhaustion threshold transaction (issue #210).

The one targeted real-Postgres check E3b owns: the exhausting decision's
multi-record composition — the one-shot iteration result recording, the
pending exact-plan REVIEW_RESOLUTION gate, its installation as the Task's
current gate, the ``waiting_for_owner`` transition with a rotated token,
and the three coordinated audit events — commits atomically, and a
mid-transaction failure rolls the whole composition back, leaving the
attempt unfinalized, the Task untouched, and no events behind.

E3c's later cross-record atomicity test covers its own SETTLED settlement
seam and must not duplicate this one (parent #78 validation ownership).
No live GitHub, runtime, or Supabase access; the suite consumes the
explicitly supplied non-production branch database per
``tests/integration/conftest.py``.
"""

from __future__ import annotations

import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any, cast

import pytest
from psycopg import Connection

from openorc.adapters.agent_runtime.contract import AgentRuntimeAdapter
from openorc.adapters.github import GitHubAppClient
from openorc.domain.connections import WorkflowRole
from openorc.domain.gates import OwnerGateStatus, OwnerGateType
from openorc.domain.reviews import ReviewLoopPurpose, ReviewOutcome
from openorc.domain.tasks import TaskStatus
from openorc.persistence import gates as gate_repositories
from openorc.persistence import planning as planning_repositories
from openorc.persistence import reviews as review_repositories
from openorc.persistence import tasks as task_repositories
from openorc.persistence.pool import DatabasePool
from openorc.persistence.sessions import get_task_agent_session
from openorc.protocol.models import ReviewFinding, ReviewResult
from openorc.services.errors import StaleOperationError
from openorc.services.task_planning_review_decision import (
    PlanningReviewDecision,
    TaskPlanningReviewDispatchOutcome,
    finalize_planning_review_decision,
)
from tests.fakes.agent_runtime import FakeAgentRuntimeAdapter

pytestmark = pytest.mark.integration

_BASE_SHA = "d" * 40
_PLAN_TEXT = "# Plan\n\nStep one.\n"
_REVIEW_FINDING = [{"summary": "Clarify scope", "details": "Say which module changes."}]


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


def _insert_repository(conn: Connection[Any], *, workspace_id: uuid.UUID) -> uuid.UUID:
    repository_id = uuid.uuid4()
    conn.execute(
        "insert into openorc.repositories "
        "(id, project_id, workspace_id, github_repository_id, owner_login, name, "
        "html_url, is_private, default_branch) "
        "values (%s, %s, %s, %s, %s, %s, %s, %s, %s)",
        (
            repository_id,
            _insert_project(conn, workspace_id),
            workspace_id,
            77_000_002,
            "octocat",
            "hello-world",
            "https://github.com/octocat/hello-world",
            False,
            "main",
        ),
    )
    return repository_id


def _insert_task(
    conn: Connection[Any], *, workspace_id: uuid.UUID, repository_id: uuid.UUID
) -> uuid.UUID:
    task_id = uuid.uuid4()
    conn.execute(
        "insert into openorc.tasks "
        "(id, workspace_id, repository_id, github_issue_id, github_issue_number, status, "
        "source_requirements_fingerprint) "
        "values (%s, %s, %s, %s, %s, 'planning', %s)",
        (task_id, workspace_id, repository_id, 7701, 42, "a" * 64),
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


def _decision_fixture(
    conn: Connection[Any], *, iteration_limit: int
) -> tuple[DatabasePool, dict[str, Any]]:
    """The E3b-ready durable fixture: ownership chain, READY Reviewer
    binding, one snapshotted OPEN planning loop, one finalized change
    request, and one unfinalized attempt — both addressing the current plan."""
    profile_id = _insert_profile(conn)
    workspace_id = _insert_workspace(conn, profile_id)
    repository_id = _insert_repository(conn, workspace_id=workspace_id)
    task_id = _insert_task(conn, workspace_id=workspace_id, repository_id=repository_id)
    connection_id = _insert_connection(conn, workspace_id=workspace_id)
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
        iteration_limit=iteration_limit,
    )
    settled = review_repositories.create_review_iteration(
        pool,
        workspace_id=workspace_id,
        task_id=task_id,
        review_loop_id=loop.id,
        iteration_number=1,
        plan_revision_id=revision.id,
    )
    assert settled is not None
    review_repositories.record_review_iteration_result(
        pool,
        review_iteration_id=settled.id,
        outcome=ReviewOutcome.CHANGES_REQUESTED,
        summary="First objection",
        findings=_REVIEW_FINDING,
    )
    attempt = review_repositories.create_review_iteration(
        pool,
        workspace_id=workspace_id,
        task_id=task_id,
        review_loop_id=loop.id,
        iteration_number=2,
        plan_revision_id=revision.id,
    )
    assert attempt is not None
    reviewer = get_task_agent_session(pool, task_id=task_id, role=WorkflowRole.REVIEWER)
    assert reviewer is not None
    current_task = task_repositories.get_task(pool, task_id)
    assert current_task is not None
    persisted_loop = review_repositories.get_review_loop(pool, review_loop_id=loop.id)
    assert persisted_loop is not None
    dispatch = TaskPlanningReviewDispatchOutcome(
        review_loop=persisted_loop,
        review_iteration=attempt,
        review_result=ReviewResult(
            outcome="CHANGES_REQUESTED",
            summary="Needs work",
            findings=(ReviewFinding(summary="Clarify scope", details="Say which module."),),
        ),
        reviewer_session=reviewer,
    )
    return pool, {
        "workspace_id": workspace_id,
        "task_id": task_id,
        "profile_id": profile_id,
        "revision_id": revision.id,
        "loop_id": loop.id,
        "attempt_id": attempt.id,
        "state_token": current_task.state_token,
        "dispatch": dispatch,
    }


def _exhausting_decision(pool: DatabasePool, facts: dict[str, Any]) -> Any:
    return finalize_planning_review_decision(
        pool,
        cast(AgentRuntimeAdapter, FakeAgentRuntimeAdapter()),
        cast(GitHubAppClient, object()),
        profile_id=facts["profile_id"],
        workspace_id=facts["workspace_id"],
        task_id=facts["task_id"],
        expected_state_token=facts["state_token"],
        dispatch=facts["dispatch"],
    )


def test_the_exhaustion_threshold_transaction_commits_atomically(
    conn: Connection[Any],
) -> None:
    """One committed composition: finalized iteration, pending exact-plan
    gate installed current, ``waiting_for_owner`` with a rotated token, and
    the three coordinated events — with the loop still OPEN."""
    pool, facts = _decision_fixture(conn, iteration_limit=1)

    decision = _exhausting_decision(pool, facts)

    assert decision.decision is PlanningReviewDecision.EXHAUSTED
    finalized = review_repositories.get_review_iteration(
        pool, review_iteration_id=facts["attempt_id"]
    )
    assert finalized is not None
    assert finalized.outcome is ReviewOutcome.CHANGES_REQUESTED
    assert finalized.decided_at is not None
    task = task_repositories.get_task(pool, facts["task_id"])
    assert task is not None
    assert task.status is TaskStatus.WAITING_FOR_OWNER
    assert task.current_owner_gate_id == decision.resolution_gate.id
    assert task.state_token != facts["state_token"]
    gates = gate_repositories.list_task_owner_gates(pool, task_id=facts["task_id"])
    assert len(gates) == 1
    assert gates[0].gate_type is OwnerGateType.REVIEW_RESOLUTION
    assert gates[0].status is OwnerGateStatus.PENDING
    assert gates[0].plan_revision_id == facts["revision_id"]
    assert gates[0].subject_head_sha is None
    assert gates[0].task_pull_request_id is None
    loop = review_repositories.get_review_loop(pool, review_loop_id=facts["loop_id"])
    assert loop is not None
    assert loop.status.value == "open"

    # Exactly the three coordinated events, with their exact subjects and
    # no findings anywhere.
    assert (
        _row_count(
            conn,
            "select count(*) from openorc.workflow_events where task_id = %s "
            "and event_type = 'plan_reviewed' and actor_type = 'reviewer' "
            "and subject_type = 'review_iteration' and subject_id = %s",
            (facts["task_id"], facts["attempt_id"]),
        )
        == 1
    )
    assert (
        _row_count(
            conn,
            "select count(*) from openorc.workflow_events where task_id = %s "
            "and event_type = 'owner_gate_created' and subject_type = 'owner_gate' "
            "and subject_id = %s",
            (facts["task_id"], decision.resolution_gate.id),
        )
        == 1
    )
    assert (
        _row_count(
            conn,
            "select count(*) from openorc.workflow_events where task_id = %s "
            "and event_type = 'review_limit_reached' and subject_type = 'review_loop' "
            "and subject_id = %s",
            (facts["task_id"], facts["loop_id"]),
        )
        == 1
    )


def test_a_mid_transaction_failure_rolls_the_threshold_composition_back(
    conn: Connection[Any],
) -> None:
    """A failed gate installation rolls back the whole exhausting decision:
    the attempt stays unfinalized, the Task stays planning and untouched,
    no gate appears, and no event was written."""
    pool, facts = _decision_fixture(conn, iteration_limit=1)
    # Force the installation precondition to fail: another pending gate is
    # already the Task's current gate, so the resolution gate can never be
    # installed and the whole composition must roll back.
    blocker = gate_repositories.create_owner_gate(
        pool,
        workspace_id=facts["workspace_id"],
        task_id=facts["task_id"],
        gate_type=OwnerGateType.PR_AUTHORIZATION,
        subject_head_sha="e" * 40,
    )
    blocked = task_repositories.set_current_owner_gate(
        pool,
        facts["task_id"],
        expected_state_token=facts["state_token"],
        owner_gate_id=blocker.id,
    )
    assert blocked is not None
    facts["state_token"] = blocked.state_token

    with pytest.raises(StaleOperationError):
        _exhausting_decision(pool, facts)

    attempt = review_repositories.get_review_iteration(
        pool, review_iteration_id=facts["attempt_id"]
    )
    assert attempt is not None
    assert attempt.outcome is None
    assert attempt.summary is None
    assert attempt.findings is None
    assert attempt.decided_at is None
    task = task_repositories.get_task(pool, facts["task_id"])
    assert task is not None
    assert task.status is TaskStatus.PLANNING
    assert task.current_owner_gate_id == blocker.id
    assert task.state_token == facts["state_token"]
    assert task.current_plan_revision_id == facts["revision_id"]
    gates = gate_repositories.list_task_owner_gates(pool, task_id=facts["task_id"])
    assert [gate.id for gate in gates] == [blocker.id]
    for event_type in ("plan_reviewed", "owner_gate_created", "review_limit_reached"):
        assert (
            _row_count(
                conn,
                "select count(*) from openorc.workflow_events where task_id = %s "
                "and event_type = %s",
                (facts["task_id"], event_type),
            )
            == 0
        )
