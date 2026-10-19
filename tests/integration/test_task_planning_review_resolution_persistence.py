"""Durable atomicity and CHECK behavior of the E3c settlement (issue #211).

The one targeted real-Postgres check E3c owns: the terminal ``settled``
status joins the OwnerGate vocabulary for ``review_resolution`` gates only
(CHECK behavior), and the settlement's multi-record composition — the NEW
finalized settlement iteration, the safe REVIEWER audit events, the
one-shot ``SETTLED`` gate stamp with pointer clear and token rotation, and
the loop closure / ``planning`` return — commits atomically, and a forced
late failure rolls the whole composition back, leaving the gate pending,
the Task untouched, and no settlement facts behind.

E3b's existing cross-record atomicity test covers the exhaustion threshold
and is not duplicated here (parent #78 validation ownership). No live
GitHub, runtime, or Supabase access; the suite consumes the explicitly
supplied non-production branch database per
``tests/integration/conftest.py``.
"""

from __future__ import annotations

import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any, cast

import pytest
from psycopg import Connection
from psycopg.errors import CheckViolation

from openorc.adapters.agent_runtime.contract import AgentRuntimeAdapter
from openorc.adapters.github import GitHubAppClient
from openorc.domain.connections import WorkflowRole
from openorc.domain.gates import OwnerGateStatus, OwnerGateType
from openorc.domain.reviews import ReviewLoopPurpose, ReviewLoopStatus, ReviewOutcome
from openorc.domain.tasks import TaskStatus
from openorc.persistence import gates as gate_repositories
from openorc.persistence import planning as planning_repositories
from openorc.persistence import reviews as review_repositories
from openorc.persistence import tasks as task_repositories
from openorc.persistence.pool import DatabasePool
from openorc.persistence.sessions import get_task_agent_session
from openorc.protocol.models import ReviewFinding, ReviewResult
from openorc.services import event_coordination
from openorc.services.task_planning_review_decision import (
    TaskPlanningReviewDispatchOutcome,
    finalize_planning_review_decision,
)
from openorc.services.task_planning_review_resolution import (
    PlanningReviewResolutionDecision,
    settle_planning_review_resolution,
)
from tests.fakes.agent_runtime import FakeAgentRuntimeAdapter

pytestmark = pytest.mark.integration

_BASE_SHA = "d" * 40
_PLAN_TEXT = "# Plan\n\nStep one.\n"
_REVISED_PLAN_TEXT = "# Plan (revised)\n\nStep one, clarified.\n"
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


class _UnusedGitHubClient:
    """A structurally valid GitHubAppClient stub.

    The exhaustion fixture and the settlement compositions exercised here
    never reach E2's revision production, so neither capability is ever
    called; the stub exists so the service entries accept a real adapter
    dependency, never an arbitrary object.
    """

    def get_installation_repository(
        self, *, github_installation_id: int, github_repository_id: int
    ) -> None:
        raise AssertionError("these settlement compositions never route revision production")

    def get_repository_branch(
        self,
        *,
        github_installation_id: int,
        owner_login: str,
        repository_name: str,
        branch_name: str,
    ) -> None:
        raise AssertionError("these settlement compositions never route revision production")


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


def _settlement_fixture(conn: Connection[Any]) -> tuple[DatabasePool, dict[str, Any]]:
    """The settlement-ready durable fixture.

    Ownership chain, READY Reviewer binding, one snapshotted OPEN planning
    loop, one finalized change request, and one unfinalized attempt — with
    the real E3b exhaustion threshold run to reach the exact pending
    REVIEW_RESOLUTION gate and the ``waiting_for_owner`` Task it guards.
    """
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
        iteration_limit=1,
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
    decision = finalize_planning_review_decision(
        pool,
        cast(AgentRuntimeAdapter, FakeAgentRuntimeAdapter()),
        cast(GitHubAppClient, _UnusedGitHubClient()),
        profile_id=profile_id,
        workspace_id=workspace_id,
        task_id=task_id,
        expected_state_token=current_task.state_token,
        dispatch=dispatch,
    )
    assert decision.resolution_gate is not None
    exhausted = task_repositories.get_task(pool, task_id)
    assert exhausted is not None
    return pool, {
        "workspace_id": workspace_id,
        "task_id": task_id,
        "profile_id": profile_id,
        "revision_id": revision.id,
        "loop_id": loop.id,
        "gate_id": decision.resolution_gate.id,
        "state_token": exhausted.state_token,
    }


def _settling_call(
    pool: DatabasePool,
    facts: dict[str, Any],
    *,
    review_result: ReviewResult,
) -> Any:
    return settle_planning_review_resolution(
        pool,
        cast(AgentRuntimeAdapter, FakeAgentRuntimeAdapter()),
        cast(GitHubAppClient, _UnusedGitHubClient()),
        profile_id=facts["profile_id"],
        workspace_id=facts["workspace_id"],
        task_id=facts["task_id"],
        expected_state_token=facts["state_token"],
        review_result=review_result,
    )


def test_the_settled_status_is_a_review_resolution_only_vocabulary(
    conn: Connection[Any],
) -> None:
    """The CHECK restricts ``settled`` to review_resolution gates, the
    terminal stamp stays non-NULL exactly when the status is not pending,
    and the real settlement path stamps the exact review_resolution gate."""
    pool, facts = _settlement_fixture(conn)
    # A settled status without its terminal stamp violates the decided-at
    # coherence even for a review_resolution gate.
    with pytest.raises(CheckViolation), conn.transaction():
        conn.execute(
            "update openorc.owner_gates set status = 'settled' where id = %s",
            (facts["gate_id"],),
        )
    # A non-review_resolution gate can never carry the settled status at all.
    other = gate_repositories.create_owner_gate(
        pool,
        workspace_id=facts["workspace_id"],
        task_id=facts["task_id"],
        gate_type=OwnerGateType.PR_AUTHORIZATION,
        subject_head_sha="e" * 40,
    )
    with pytest.raises(CheckViolation), conn.transaction():
        conn.execute(
            "update openorc.owner_gates set status = 'settled', decided_at = now() where id = %s",
            (other.id,),
        )
    # The real settlement stamps settled on the exact review_resolution gate.
    outcome = _settling_call(
        pool,
        facts,
        review_result=ReviewResult(
            outcome="ACCEPTED", summary="Everything checks out.", findings=()
        ),
    )
    assert outcome.resolution_gate.status is OwnerGateStatus.SETTLED


def test_the_settled_acceptance_composition_commits_atomically(
    conn: Connection[Any],
) -> None:
    """One committed composition: the NEW finalized settlement iteration,
    the REVIEWER plan-review and settled-gate events with the settling
    result's references, the one-shot ``SETTLED`` stamp with pointer clear
    and token rotation, and the loop closure — the unchanged current
    PlanRevision stays current and the exhausted history stays immutable."""
    pool, facts = _settlement_fixture(conn)

    outcome = _settling_call(
        pool,
        facts,
        review_result=ReviewResult(
            outcome="ACCEPTED", summary="Everything checks out.", findings=()
        ),
    )

    assert outcome.decision is PlanningReviewResolutionDecision.SETTLED_ACCEPTED
    gate = gate_repositories.get_owner_gate(pool, owner_gate_id=facts["gate_id"])
    assert gate is not None
    assert gate.status is OwnerGateStatus.SETTLED
    assert gate.decided_at is not None
    task = task_repositories.get_task(pool, facts["task_id"])
    assert task is not None
    assert task.status is TaskStatus.WAITING_FOR_OWNER
    assert task.current_owner_gate_id is None
    assert task.current_plan_revision_id == facts["revision_id"]
    assert task.state_token != facts["state_token"]
    loop = review_repositories.get_review_loop(pool, review_loop_id=facts["loop_id"])
    assert loop is not None
    assert loop.status is ReviewLoopStatus.CLOSED
    assert loop.closed_at is not None
    iterations = review_repositories.list_review_loop_iterations(
        pool, review_loop_id=facts["loop_id"]
    )
    assert [iteration.iteration_number for iteration in iterations] == [1, 2, 3]
    settlement_iteration = iterations[-1]
    assert settlement_iteration.outcome is ReviewOutcome.ACCEPTED
    assert settlement_iteration.plan_revision_id == facts["revision_id"]
    assert settlement_iteration.decided_at is not None
    # The exhausted history is immutable evidence, never rewritten.
    assert iterations[0].findings == _REVIEW_FINDING
    assert iterations[1].outcome is ReviewOutcome.CHANGES_REQUESTED
    assert (
        _row_count(
            conn,
            "select count(*) from openorc.workflow_events where task_id = %s "
            "and event_type = 'plan_reviewed' and actor_type = 'reviewer' "
            "and subject_type = 'review_iteration' and subject_id = %s",
            (facts["task_id"], settlement_iteration.id),
        )
        == 1
    )
    resolved = conn.execute(
        "select actor_type, context from openorc.workflow_events "
        "where task_id = %s and event_type = 'owner_gate_resolved'",
        (facts["task_id"],),
    ).fetchone()
    assert resolved is not None
    assert resolved[0] == "reviewer"
    assert resolved[1] == {
        "outcome": "settled",
        "review_iteration_id": str(settlement_iteration.id),
        "review_outcome": "accepted",
    }


def test_a_late_settlement_failure_rolls_the_whole_composition_back(
    conn: Connection[Any],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A failure after the iteration result and the gate stamp applies
    nothing: the gate stays pending, the Task keeps its token and current
    gate, the settlement iteration vanishes, the loop stays OPEN, and no
    settlement event was written."""
    pool, facts = _settlement_fixture(conn)

    def _boom(*args: Any, **kwargs: Any) -> None:
        raise RuntimeError("forced late settlement failure")

    monkeypatch.setattr(event_coordination, "record_owner_gate_resolved_event", _boom)
    events_before = _row_count(
        conn,
        "select count(*) from openorc.workflow_events where task_id = %s",
        (facts["task_id"],),
    )

    with pytest.raises(RuntimeError, match="forced late settlement failure"):
        _settling_call(
            pool,
            facts,
            review_result=ReviewResult(
                outcome="CHANGES_REQUESTED",
                summary="Needs another pass",
                findings=(
                    ReviewFinding(summary="Clarify the retry loop", details="Explain the backoff."),
                ),
            ),
        )

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
    assert [iteration.iteration_number for iteration in iterations] == [1, 2]
    assert iterations[1].outcome is ReviewOutcome.CHANGES_REQUESTED
    assert iterations[1].summary == "Needs work"
    loop = review_repositories.get_review_loop(pool, review_loop_id=facts["loop_id"])
    assert loop is not None
    assert loop.status is ReviewLoopStatus.OPEN
    assert loop.closed_at is None
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
            "select count(*) from openorc.workflow_events where task_id = %s "
            "and event_type = 'owner_gate_resolved'",
            (facts["task_id"],),
        )
        == 0
    )
