"""Durable atomicity of the E9 merge-gate retirement and the remediation
claim (issue #84).

The targeted real-Postgres checks the new PR/head multi-record seams own:

- the actionable merge-gate change request's cross-record composition — the
  NEW finalized same-head PR ReviewIteration, the REVIEWER ``pr_reviewed``
  audit, the guarded ``reviewing``-destination supersession of the exact
  current pending MERGE_DECISION gate (pointer clear, ``waiting_for_owner``
  → ``reviewing``, token rotation, gate row untouched pending history), and
  the two direction-only discussion events — commits atomically; a forced
  late audit failure rolls the whole composition back, leaving the gate
  pending AND current, the Task untouched at its original token, and no
  retirement facts behind;
- the ``executions.review_iteration_id`` one-shot claim: the partial unique
  index admits exactly one claimed attempt per finalized ReviewIteration
  (completed, known-failed, and uncertain claims alike), while NULL-linkage
  attempts stay unrestricted.

E3c's settlement atomicity and #219's planning supersession coverage are
reused, not duplicated. No live GitHub, runtime, or Supabase access; the
suite consumes the explicitly supplied non-production branch database per
``tests/integration/conftest.py``.
"""

from __future__ import annotations

import uuid
from contextlib import contextmanager
from typing import Any, cast

import pytest
from psycopg import Connection
from psycopg.errors import UniqueViolation

from openorc.domain.gates import OwnerGateStatus, OwnerGateType
from openorc.domain.reviews import ReviewLoopPurpose, ReviewOutcome
from openorc.domain.tasks import TaskStatus
from openorc.persistence import executions as execution_repositories
from openorc.persistence import gates as gate_repositories
from openorc.persistence import reviews as review_repositories
from openorc.persistence import tasks as task_repositories
from openorc.persistence.executions import ExecutionStatus
from openorc.persistence.pool import DatabasePool
from openorc.protocol.models import ReviewFinding, ReviewResult
from openorc.services import event_coordination
from openorc.services.task_pr_review_decision import (
    PrMergeGateChangeRequestDecision,
)
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
_NEW_HEAD_SHA = "fedcba9876543210fedcba9876543210fedcba98"
_BRANCH = "openorc/task-42-implementation"
_GITHUB_PR_ID = 900_719_925_474_099
_PR_NUMBER = 77


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


def _pr_review_fixture(conn: Connection[Any]) -> dict[str, Any]:
    """The merge-gate fixture: a ``waiting_for_owner`` Task holding a pending
    current PR/head MERGE_DECISION gate over one finalized same-head change
    request in its OPEN PR_REVIEW loop, with both role sessions READY."""
    profile_id = _insert_profile(conn)
    workspace_id = _insert_workspace(conn, profile_id)
    installation_id = _insert_installation(conn, workspace_id=workspace_id)
    repository_id = _insert_repository(
        conn, workspace_id=workspace_id, github_installation_id=installation_id
    )
    task_id = _insert_task(conn, workspace_id=workspace_id, repository_id=repository_id)
    connection_id = _insert_connection(conn, workspace_id=workspace_id)
    _insert_ready_session(
        conn,
        workspace_id=workspace_id,
        task_id=task_id,
        connection_id=connection_id,
        role="producer",
        external_session_id="fake-producer-1",
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

    pr_id = _insert_canonical_pr(
        conn, workspace_id=workspace_id, task_id=task_id, repository_id=repository_id
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
    addressed = review_repositories.create_review_iteration(
        pool,
        workspace_id=workspace_id,
        task_id=task_id,
        review_loop_id=loop.id,
        iteration_number=1,
        task_pull_request_id=pr_id,
        reviewed_head_sha=_HEAD_SHA,
    )
    assert addressed is not None
    review_repositories.record_review_iteration_result(
        pool,
        review_iteration_id=addressed.id,
        outcome=ReviewOutcome.CHANGES_REQUESTED,
        summary="The retry loop leaks the connection.",
        findings=[{"summary": "Fix the leak", "details": "Close it in finally."}],
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
    producer_session = task_repositories.get_task(pool, task_id)
    assert producer_session is not None
    return {
        "pool": pool,
        "conn": conn,
        "workspace_id": workspace_id,
        "task_id": task_id,
        "profile_id": profile_id,
        "pr_id": pr_id,
        "loop_id": loop.id,
        "addressed_iteration_id": addressed.id,
        "gate_id": gate.id,
        "state_token": waiting.state_token,
    }


def _changes_requested_result() -> ReviewResult:
    return ReviewResult(
        outcome="CHANGES_REQUESTED",
        summary="The retry loop leaks the connection.",
        findings=(ReviewFinding(summary="Fix the leak", details="Close it in finally."),),
    )


def _retire_call(pool: DatabasePool, facts: dict[str, Any], **overrides: Any) -> Any:
    from openorc.services.task_pr_review_decision import (
        retire_pr_merge_decision_with_changes as _retire,
    )

    class _UnusedAdapter:
        def send(self, *args: Any, **kwargs: Any) -> Any:  # pragma: no cover - stubbed seam
            raise AssertionError("the remediation routing is stubbed in this suite")

        def realize_control(self, *args: Any, **kwargs: Any) -> Any:  # pragma: no cover
            raise AssertionError("the remediation routing is stubbed in this suite")

    class _UnusedGitHub:
        def get_installation_repository(self, **_kwargs: Any) -> Any:  # pragma: no cover
            raise AssertionError("the remediation routing is stubbed in this suite")

        def get_repository_pull_request(self, **_kwargs: Any) -> Any:  # pragma: no cover
            raise AssertionError("the remediation routing is stubbed in this suite")

    # The remediation routing is stubbed at the module seam in this suite,
    # so the continuation dependencies are structurally present but unused.
    return _retire(
        pool,
        cast(Any, _UnusedAdapter()),
        cast(Any, _UnusedGitHub()),
        cast(Any, lambda binding: contextmanager(lambda: iter([_UnusedAdapter()]))()),
        profile_id=facts["profile_id"],
        workspace_id=facts["workspace_id"],
        task_id=facts["task_id"],
        expected_state_token=facts["state_token"],
        review_result=_changes_requested_result(),
        **overrides,
    )


def test_the_merge_gate_change_request_retirement_commits_atomically(
    conn: Connection[Any], monkeypatch: pytest.MonkeyPatch
) -> None:
    facts = _pr_review_fixture(conn)
    pool = facts["pool"]
    remediation_calls: list[dict[str, Any]] = []

    def _stub_remediation(*args: Any, **kwargs: Any) -> Any:
        remediation_calls.append(kwargs)
        return "routed"

    monkeypatch.setattr(
        "openorc.services.task_pr_review_decision.route_pr_remediation", _stub_remediation
    )

    outcome = _retire_call(pool, facts, record_discussion_message_events=True)

    assert outcome.decision is PrMergeGateChangeRequestDecision.MERGE_GATE_RETIRED
    # The NEW same-head finalized iteration is durable history.
    iterations = review_repositories.list_review_loop_iterations(
        pool, review_loop_id=facts["loop_id"]
    )
    assert [iteration.iteration_number for iteration in iterations] == [1, 2]
    assert iterations[1].outcome is ReviewOutcome.CHANGES_REQUESTED
    assert iterations[1].reviewed_head_sha == _HEAD_SHA
    assert iterations[1].task_pull_request_id == facts["pr_id"]
    # The retired gate stays pending history: no terminal stamp, and the
    # Task's pointer cleared with the token rotated into ``reviewing``.
    retired = gate_repositories.get_owner_gate(pool, owner_gate_id=facts["gate_id"])
    assert retired is not None
    assert retired.status is OwnerGateStatus.PENDING
    assert retired.decided_at is None
    task = task_repositories.get_task(pool, facts["task_id"])
    assert task is not None
    assert task.status is TaskStatus.REVIEWING
    assert task.current_owner_gate_id is None
    assert task.state_token != facts["state_token"]
    # Exactly the safe audit sequence: the REVIEWER pr_reviewed event and the
    # two direction-only discussion events — never a false gate resolution.
    assert len(remediation_calls) == 1
    assert remediation_calls[0]["expected_state_token"] == task.state_token
    row = conn.execute(
        "select event_type, count(*) from openorc.workflow_events "
        "where task_id = %s group by event_type order by event_type",
        (facts["task_id"],),
    ).fetchall()
    types = {event_type: count for event_type, count in row}
    assert types.get("pr_reviewed") == 1
    assert types.get("owner_reviewer_discussion_message") == 2
    assert "owner_gate_resolved" not in types


def test_a_late_retirement_failure_rolls_the_whole_composition_back(
    conn: Connection[Any], monkeypatch: pytest.MonkeyPatch
) -> None:
    facts = _pr_review_fixture(conn)
    pool = facts["pool"]

    def _boom(*args: Any, **kwargs: Any) -> Any:
        raise RuntimeError("forced late discussion audit failure")

    monkeypatch.setattr(
        event_coordination,
        "record_owner_reviewer_discussion_message_event",
        _boom,
    )
    monkeypatch.setattr(
        "openorc.services.task_pr_review_decision.route_pr_remediation",
        lambda *args: (_ for _ in ()).throw(AssertionError("unreachable")),
    )

    with pytest.raises(RuntimeError, match="forced late discussion audit failure"):
        _retire_call(pool, facts, record_discussion_message_events=True)

    # Nothing committed: the gate is still pending AND current, the Task is
    # untouched at its original token, and no retirement facts exist.
    retired = gate_repositories.get_owner_gate(pool, owner_gate_id=facts["gate_id"])
    assert retired is not None
    assert retired.status is OwnerGateStatus.PENDING
    task = task_repositories.get_task(pool, facts["task_id"])
    assert task is not None
    assert task.status is TaskStatus.WAITING_FOR_OWNER
    assert task.current_owner_gate_id == facts["gate_id"]
    assert task.state_token == facts["state_token"]
    iterations = review_repositories.list_review_loop_iterations(
        pool, review_loop_id=facts["loop_id"]
    )
    assert [iteration.iteration_number for iteration in iterations] == [1]
    row = conn.execute(
        "select count(*) from openorc.workflow_events where task_id = %s",
        (facts["task_id"],),
    ).fetchone()
    assert row is not None
    assert int(row[0]) == 0


def test_the_remediation_claim_partial_unique_index_is_one_shot(
    conn: Connection[Any],
) -> None:
    facts = _pr_review_fixture(conn)
    pool = facts["pool"]

    def _producer_session_id() -> uuid.UUID:
        row = conn.execute(
            "select id from openorc.task_agent_sessions where task_id = %s and role = 'producer'",
            (facts["task_id"],),
        ).fetchone()
        assert row is not None
        return cast(uuid.UUID, row[0])

    first = execution_repositories.create_execution(
        pool,
        workspace_id=facts["workspace_id"],
        task_id=facts["task_id"],
        producer_session_id=_producer_session_id(),
        execution_number=2,
        review_iteration_id=facts["addressed_iteration_id"],
    )
    assert first.review_iteration_id == facts["addressed_iteration_id"]
    # A second claim for the same iteration is refused by the partial unique
    # index — completed, known-failed, and uncertain prior claims alike.
    with pytest.raises(UniqueViolation):
        execution_repositories.create_execution(
            pool,
            workspace_id=facts["workspace_id"],
            task_id=facts["task_id"],
            producer_session_id=_producer_session_id(),
            execution_number=3,
            review_iteration_id=facts["addressed_iteration_id"],
        )
    # NULL-linkage attempts (ordinary non-remediation history) stay
    # unrestricted by the partial index.
    unclaimed = execution_repositories.create_execution(
        pool,
        workspace_id=facts["workspace_id"],
        task_id=facts["task_id"],
        producer_session_id=_producer_session_id(),
        execution_number=4,
    )
    assert unclaimed.review_iteration_id is None
    assert (
        execution_repositories.list_task_executions(pool, task_id=facts["task_id"])[-1].status
        is ExecutionStatus.QUEUED
    )
