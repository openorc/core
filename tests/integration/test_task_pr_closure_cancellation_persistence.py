"""Durable atomicity of the E10c trusted PR-closure cancellation (issue #229).

The targeted real-Postgres checks the trusted GitHub-observation cancellation
entrypoint owns:

- the confirmed closed-unmerged cancellation composes ONE atomic transition:
  every OPEN ReviewLoop closes, the Task archives ``CANCELLED`` with the
  canonical ``pr_closed_unmerged`` terminal cause (releasing the current-issue
  mapping and clearing the current-gate pointer while the gate row stays
  untouched pending history), and exactly one terminal ``task_cancelled``
  event with truthful GITHUB provenance commits together; a forced late audit
  failure rolls the whole composition back; a duplicate closure is the
  idempotent ``ALREADY_TERMINAL`` no-op, and archived ``COMPLETED`` history is
  never rewritten;
- the terminal evidence is serialized against concurrent observed-snapshot
  reconciliation: a confirmed merge committed before the evidence decision
  prevents the cancellation (the retryable reconciliation paths route merged
  facts to E10a instead), and an in-flight B7 write phase holding the
  canonical PR row lock blocks the evidence read rather than letting the
  cancellation decide from a stale snapshot.

No live GitHub, runtime, or Supabase access; the suite consumes the explicitly
supplied non-production branch database per ``tests/integration/conftest.py``.
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime
from typing import Any, cast

import pytest
from psycopg import Connection, connect
from psycopg.errors import LockNotAvailable

from openorc.domain.gates import OwnerGateStatus, OwnerGateType
from openorc.domain.pull_requests import TaskPullRequestState
from openorc.domain.reviews import ReviewLoopPurpose
from openorc.domain.tasks import TaskStatus, TaskTerminalCause
from openorc.persistence import gates as gate_repositories
from openorc.persistence import reviews as review_repositories
from openorc.persistence import tasks as task_repositories
from openorc.persistence.pool import DatabasePool
from openorc.persistence.pull_requests import (
    TaskPullRequestReconcileOutcome,
    reconcile_task_pull_request_observed,
)
from openorc.services.errors import StaleOperationError
from openorc.services.task_cancellation import (
    TaskPrClosureOutcomeKind,
    cancel_task_for_confirmed_pr_closure,
)
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


def _insert_closed_unmerged_pr(
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
        "values (%s, %s, %s, %s, %s, %s, %s, 'main', %s, 'closed')",
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


def _closure_fixture(conn: Connection[Any]) -> dict[str, Any]:
    """A ``waiting_for_owner`` Task holding a pending current MERGE_DECISION
    gate over an OPEN ``PR_REVIEW`` loop and one closed-unmerged canonical
    pull request."""
    profile_id = _insert_profile(conn)
    workspace_id = _insert_workspace(conn, profile_id)
    installation_id = _insert_installation(conn, workspace_id=workspace_id)
    repository_id = _insert_repository(
        conn, workspace_id=workspace_id, github_installation_id=installation_id
    )
    task_id = _insert_task(conn, workspace_id=workspace_id, repository_id=repository_id)
    pool = cast(DatabasePool, _SingleConnectionPool(conn))

    pr_id = _insert_closed_unmerged_pr(
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


def test_the_confirmed_pr_closure_cancellation_commits_atomically(
    conn: Connection[Any],
) -> None:
    facts = _closure_fixture(conn)
    pool = facts["pool"]

    outcome = cancel_task_for_confirmed_pr_closure(
        pool, workspace_id=facts["workspace_id"], task_id=facts["task_id"]
    )

    assert outcome.kind is TaskPrClosureOutcomeKind.CANCELLED
    task = task_repositories.get_task(pool, facts["task_id"])
    assert task is not None
    assert task.status is TaskStatus.CANCELLED
    assert task.archived_at is not None
    assert task.terminal_cause is TaskTerminalCause.PR_CLOSED_UNMERGED
    assert task.current_owner_gate_id is None
    assert task.state_token != facts["state_token"]
    # The gate row stays untouched pending history with its own provenance:
    # a pending OwnerGate is never resolved as if its Owner chose Cancel.
    gate = gate_repositories.get_owner_gate(pool, owner_gate_id=facts["gate_id"])
    assert gate is not None
    assert gate.status is OwnerGateStatus.PENDING
    assert gate.decided_at is None
    # Every OPEN ReviewLoop closed.
    loops = review_repositories.list_task_review_loops(pool, task_id=facts["task_id"], status=None)
    assert [loop.status.value for loop in loops] == ["closed"]
    # The current-issue mapping is released by the archival.
    assert (
        task_repositories.find_current_task_for_issue(
            pool, repository_id=facts["repository_id"], github_issue_id=_GITHUB_ISSUE_ID
        )
        is None
    )
    # Exactly one terminal event with truthful GITHUB provenance and no
    # actor identity; no close intent exists (the external closure already
    # happened) and no TaskBlock was manufactured.
    assert _event_counts(conn, facts["task_id"]) == {"task_cancelled": 1}
    event = conn.execute(
        "select actor_type, actor_id from openorc.workflow_events "
        "where task_id = %s and event_type = 'task_cancelled'",
        (facts["task_id"],),
    ).fetchone()
    assert event is not None
    assert event[0] == "github"
    assert event[1] is None
    close_intents = conn.execute(
        "select count(*) from openorc.task_pull_request_close_intents where task_id = %s",
        (facts["task_id"],),
    ).fetchone()
    assert close_intents is not None and int(close_intents[0]) == 0
    blocks = conn.execute(
        "select count(*) from openorc.task_blocks where task_id = %s",
        (facts["task_id"],),
    ).fetchone()
    assert blocks is not None and int(blocks[0]) == 0


def test_a_duplicate_closure_is_an_idempotent_noop(conn: Connection[Any]) -> None:
    facts = _closure_fixture(conn)
    pool = facts["pool"]

    first = cancel_task_for_confirmed_pr_closure(
        pool, workspace_id=facts["workspace_id"], task_id=facts["task_id"]
    )
    second = cancel_task_for_confirmed_pr_closure(
        pool, workspace_id=facts["workspace_id"], task_id=facts["task_id"]
    )

    assert first.kind is TaskPrClosureOutcomeKind.CANCELLED
    assert second.kind is TaskPrClosureOutcomeKind.ALREADY_TERMINAL
    assert _event_counts(conn, facts["task_id"]) == {"task_cancelled": 1}


def test_archived_completed_history_is_never_rewritten_by_a_later_closure(
    conn: Connection[Any],
) -> None:
    facts = _closure_fixture(conn)
    pool = facts["pool"]
    completed = task_repositories.archive_task(
        pool,
        facts["task_id"],
        expected_state_token=facts["state_token"],
        terminal_status=TaskStatus.COMPLETED,
    )
    assert completed is not None

    outcome = cancel_task_for_confirmed_pr_closure(
        pool, workspace_id=facts["workspace_id"], task_id=facts["task_id"]
    )

    assert outcome.kind is TaskPrClosureOutcomeKind.ALREADY_TERMINAL
    task = task_repositories.get_task(pool, facts["task_id"])
    assert task is not None
    assert task.status is TaskStatus.COMPLETED
    assert task.terminal_cause is None
    assert _event_counts(conn, facts["task_id"]) == {}


def test_a_late_audit_failure_rolls_the_whole_closure_back(
    conn: Connection[Any], monkeypatch: pytest.MonkeyPatch
) -> None:
    from openorc.services import task_cancellation

    facts = _closure_fixture(conn)
    pool = facts["pool"]

    def _boom(*args: Any, **kwargs: Any) -> Any:
        raise RuntimeError("forced late cancellation-audit failure")

    monkeypatch.setattr(
        task_cancellation.event_coordination,
        "record_task_terminal_event",
        _boom,
    )

    with pytest.raises(RuntimeError, match="forced late cancellation-audit failure"):
        cancel_task_for_confirmed_pr_closure(
            pool, workspace_id=facts["workspace_id"], task_id=facts["task_id"]
        )

    # Nothing committed: the Task is still current at its original token
    # with the gate still current, the loop is still OPEN, the canonical PR
    # keeps its closed-unmerged state, and no events, archival, or terminal
    # cause happened.
    task = task_repositories.get_task(pool, facts["task_id"])
    assert task is not None
    assert task.status is TaskStatus.WAITING_FOR_OWNER
    assert task.archived_at is None
    assert task.terminal_cause is None
    assert task.current_owner_gate_id == facts["gate_id"]
    assert task.state_token == facts["state_token"]
    loops = review_repositories.list_task_review_loops(pool, task_id=facts["task_id"], status=None)
    assert [loop.status.value for loop in loops] == ["open"]
    assert _event_counts(conn, facts["task_id"]) == {}


def test_a_merge_committed_before_the_evidence_decision_prevents_cancellation(
    conn: Connection[Any],
) -> None:
    facts = _closure_fixture(conn)
    pool = facts["pool"]

    # The concurrent B7 write phase commits the authoritative confirmed-merge
    # facts BEFORE the trusted entrypoint's evidence decision.
    reconcile_result = reconcile_task_pull_request_observed(
        pool,
        task_pull_request_id=facts["pr_id"],
        workspace_id=facts["workspace_id"],
        task_id=facts["task_id"],
        head_ref=_BRANCH,
        base_ref="main",
        head_sha=_HEAD_SHA,
        state=TaskPullRequestState.CLOSED,
        merged_at=_OBSERVED,
    )
    assert reconcile_result.outcome is TaskPullRequestReconcileOutcome.UPDATED

    with pytest.raises(StaleOperationError):
        cancel_task_for_confirmed_pr_closure(
            pool, workspace_id=facts["workspace_id"], task_id=facts["task_id"]
        )

    # Nothing was applied: the Task stays current at its original token with
    # the gate still current, the loop still OPEN, and no events, archival,
    # or terminal cause. The retryable read/reconciliation paths re-classify
    # the merged facts and route them to E10a's completion seam instead.
    task = task_repositories.get_task(pool, facts["task_id"])
    assert task is not None
    assert task.status is TaskStatus.WAITING_FOR_OWNER
    assert task.archived_at is None
    assert task.terminal_cause is None
    assert task.current_owner_gate_id == facts["gate_id"]
    assert task.state_token == facts["state_token"]
    loops = review_repositories.list_task_review_loops(pool, task_id=facts["task_id"], status=None)
    assert [loop.status.value for loop in loops] == ["open"]
    assert _event_counts(conn, facts["task_id"]) == {}


def test_an_in_flight_reconciliation_lock_serializes_the_evidence_decision(
    conn: Connection[Any], database_url: str
) -> None:
    # A second, independent connection plays the concurrent B7 write phase:
    # it inserts, commits, and then holds the canonical PR row lock with an
    # uncommitted confirmed-merge update. The trusted entrypoint's evidence
    # read must BLOCK on that row lock (bounded here with a short
    # lock_timeout) instead of deciding from a snapshot that predates the
    # in-flight reconciliation. The fixture connection inserts nothing of
    # its own: it only observes the writer's committed rows.
    with connect(database_url) as writer:
        profile_id = _insert_profile(writer)
        workspace_id = _insert_workspace(writer, profile_id)
        installation_id = _insert_installation(writer, workspace_id=workspace_id)
        repository_id = _insert_repository(
            writer, workspace_id=workspace_id, github_installation_id=installation_id
        )
        task_id = _insert_task(writer, workspace_id=workspace_id, repository_id=repository_id)
        pr_id = _insert_closed_unmerged_pr(
            writer,
            workspace_id=workspace_id,
            task_id=task_id,
            repository_id=repository_id,
        )
        writer.commit()
        writer.execute(
            "update openorc.task_pull_requests set merged_at = now() where id = %s",
            (pr_id,),
        )
        conn.execute("set lock_timeout = '2s'")

        with pytest.raises(LockNotAvailable):
            cancel_task_for_confirmed_pr_closure(
                cast(DatabasePool, _SingleConnectionPool(conn)),
                workspace_id=workspace_id,
                task_id=task_id,
            )

        # The uncommitted merged update never became visible and nothing was
        # applied; the savepoint-scoped failure leaves the fixture
        # transaction alive.
        writer.rollback()
        pool = cast(DatabasePool, _SingleConnectionPool(conn))
        task = task_repositories.get_task(pool, task_id)
        assert task is not None
        assert task.status is TaskStatus.PLANNING
        assert task.archived_at is None
        assert task.terminal_cause is None
        assert _event_counts(conn, task_id) == {}
        # Restore the suite's rollback isolation: remove the committed
        # writer-owned rows.
        writer.execute("delete from openorc.task_pull_requests where id = %s", (pr_id,))
        writer.execute("delete from openorc.tasks where id = %s", (task_id,))
        writer.execute("delete from openorc.repositories where id = %s", (repository_id,))
        writer.execute("delete from openorc.github_installations where id = %s", (installation_id,))
        writer.execute("delete from openorc.workspaces where id = %s", (workspace_id,))
        writer.execute("delete from openorc.profiles where id = %s", (profile_id,))
        writer.commit()
