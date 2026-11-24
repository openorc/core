"""Integration tests for the composed Task observation loader (issue #376).

These tests run against the explicitly supplied non-production Supabase
branch database (the existing conftest path) and prove the loader's
relevant-population selections on real Postgres: the completed
implementation result the PR_AUTHORIZATION decision presents survives the
attempt history, the outstanding detached-turn receipt evidence survives a
large settled history instead of being displaced by it, and every
explicitly bounded selection carries a durable total of exactly the same
relevant population. They are excluded from the ordinary deterministic
baseline by the repository pytest configuration and run explicitly through
the Owner ``devserver.sh --testdb`` flow; agents never invoke that tooling.
The action guards' own complete repository reads are already proven by the
existing cancellation/abort suites and are not duplicated here.
"""

from __future__ import annotations

import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any, cast

import pytest
from psycopg import connect

from openorc.domain.detached_turns import TaskDetachedTurnReceiptStatus
from openorc.domain.executions import ExecutionStatus
from openorc.domain.gates import OwnerGateType
from openorc.persistence import executions as execution_records
from openorc.persistence import gates as gate_records
from openorc.persistence import tasks as task_records
from openorc.persistence.pool import DatabasePool
from openorc.persistence.task_reads import load_task_observation
from openorc.services import task_reads
from tests.integration.persistence_fixtures import (
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


def _seed_task(conn: Any, *, status: str) -> dict[str, uuid.UUID]:
    """Seed one Workspace/Repository/Task with its producer session."""
    profile_id = _insert_profile(conn)
    workspace_id = _insert_workspace(conn, profile_id)
    installation_id = _insert_installation(conn, workspace_id=workspace_id)
    repository_id = _insert_repository(
        conn, workspace_id=workspace_id, github_installation_id=installation_id
    )
    task_id = _insert_task(conn, workspace_id=workspace_id, repository_id=repository_id)
    connection_id = _insert_connection(conn, workspace_id=workspace_id)
    producer_session_id = _insert_ready_session(
        conn,
        workspace_id=workspace_id,
        task_id=task_id,
        connection_id=connection_id,
        role="producer",
        external_session_id="producer-ext-1",
    )
    conn.execute(
        "update openorc.tasks set status = %s where id = %s",
        (status, task_id),
    )
    return {
        "owner_profile_id": profile_id,
        "workspace_id": workspace_id,
        "repository_id": repository_id,
        "task_id": task_id,
        "connection_id": connection_id,
        "producer_session_id": producer_session_id,
    }


_RESULT: dict[str, object] = {
    "type": "implementation_result",
    "schema_version": 1,
    "status": "COMPLETED",
    "branch": "openorc/issue-42",
    "summary": "Implemented the change.",
    "changes": ["Added the read surface."],
    "validation": ["Deterministic suite green."],
}


def _seed_attempt(
    pool: DatabasePool,
    seeded: dict[str, uuid.UUID],
    *,
    execution_number: int,
    final_status: ExecutionStatus,
    implementation_result: dict[str, object] | None = None,
) -> None:
    """Insert one attempt and settle it to the requested final state."""
    attempt = execution_records.create_execution(
        pool,
        workspace_id=seeded["workspace_id"],
        task_id=seeded["task_id"],
        producer_session_id=seeded["producer_session_id"],
        execution_number=execution_number,
    )
    running = execution_records.claim_execution_running(pool, execution_id=attempt.id)
    assert running is not None
    if final_status is ExecutionStatus.SUCCEEDED:
        assert implementation_result is not None
        completed = execution_records.complete_execution_with_result(
            pool, execution_id=attempt.id, implementation_result=implementation_result
        )
        assert completed is not None
        return
    settled = execution_records.update_execution_status(
        pool,
        execution_id=attempt.id,
        expected_status=ExecutionStatus.RUNNING,
        next_status=final_status,
    )
    assert settled is not None


def test_completed_implementation_result_is_presented_at_pr_authorization(
    conn: Any,
) -> None:
    """The PR_AUTHORIZATION decision presents the completed implementation
    result: the latest attempt is a durably finalized successful completion,
    so it joins the relevant population with a matching total — the earlier
    failed attempt stays history on the paginated endpoint."""
    seeded = _seed_task(conn, status="waiting_for_owner")
    pool = cast(DatabasePool, _SingleConnectionPool(conn))
    task = task_records.get_task(pool, seeded["task_id"])
    assert task is not None
    gate = gate_records.create_owner_gate(
        pool,
        workspace_id=seeded["workspace_id"],
        task_id=seeded["task_id"],
        gate_type=OwnerGateType.PR_AUTHORIZATION,
        subject_head_sha="ab" * 20,
    )
    installed = task_records.set_current_owner_gate(
        pool,
        seeded["task_id"],
        expected_state_token=task.state_token,
        owner_gate_id=gate.id,
    )
    assert installed is not None
    _seed_attempt(
        pool,
        seeded,
        execution_number=1,
        final_status=ExecutionStatus.FAILED_FINAL,
    )
    _seed_attempt(
        pool,
        seeded,
        execution_number=2,
        final_status=ExecutionStatus.SUCCEEDED,
        implementation_result=_RESULT,
    )

    observation = load_task_observation(
        pool, workspace_id=seeded["workspace_id"], task_id=seeded["task_id"]
    )

    assert observation is not None
    assert [attempt.execution_number for attempt in observation.executions] == [2]
    assert observation.executions_total == 1
    latest = observation.executions[0]
    assert latest.status is ExecutionStatus.SUCCEEDED
    assert latest.implementation_result is not None
    assert latest.implementation_result["branch"] == "openorc/issue-42"


def test_open_attempts_stay_relevant_bounded_with_a_matching_total(conn: Any) -> None:
    """The relevant execution population is the open work plus the latest
    durably finalized successful completion: an explicitly bounded window
    selects it newest-first in attempt order with the durable total of the
    same population as the companion count."""
    seeded = _seed_task(conn, status="implementing")
    pool = cast(DatabasePool, _SingleConnectionPool(conn))
    _seed_attempt(pool, seeded, execution_number=1, final_status=ExecutionStatus.RUNNING)
    _seed_attempt(pool, seeded, execution_number=2, final_status=ExecutionStatus.RUNNING)
    _seed_attempt(
        pool,
        seeded,
        execution_number=3,
        final_status=ExecutionStatus.SUCCEEDED,
        implementation_result=_RESULT,
    )

    observation = load_task_observation(
        pool,
        workspace_id=seeded["workspace_id"],
        task_id=seeded["task_id"],
        presentation_bound=2,
    )

    assert observation is not None
    # The bounded window carries the two newest relevant attempts (2 and 3)
    # in attempt order; the oldest open attempt is transparently bounded out.
    assert [attempt.execution_number for attempt in observation.executions] == [2, 3]
    assert observation.executions_total == 3
    assert observation.executions[1].implementation_result is not None


def _insert_settled_receipt(
    conn: Any,
    *,
    workspace_id: uuid.UUID,
    task_id: uuid.UUID,
    session_id: uuid.UUID,
    number: int,
    minutes_ago: int,
) -> None:
    conn.execute(
        "insert into openorc.task_hub_run_receipts "
        "(workspace_id, task_id, task_agent_session_id, operation, original_attempt_kind, "
        "original_attempt_id, expected_formal_family, run_id, status, outcome, "
        "claimed_at, admitted_at, settled_at) "
        "values (%s, %s, %s, 'planning_bootstrap_prepare', 'planning_bootstrap_claim', %s, "
        "'workspace_result', %s, 'settled', '{\"settlement\": \"validated_result\"}'::jsonb, "
        "now() - (%s || ' minutes')::interval, now() - (%s || ' minutes')::interval, "
        "now() - (%s || ' minutes')::interval)",
        (
            workspace_id,
            task_id,
            session_id,
            uuid.uuid4(),
            f"run-{number}",
            minutes_ago,
            minutes_ago - 1,
            minutes_ago - 2,
        ),
    )


def test_outstanding_receipt_evidence_survives_a_large_settled_history(
    conn: Any,
) -> None:
    """A large population of terminal settled receipts never displaces the
    unresolved operational evidence: the loader selects the outstanding
    receipts before the presentation bound, and the companion total counts
    exactly the outstanding population."""
    seeded = _seed_task(conn, status="implementing")
    pool = cast(DatabasePool, _SingleConnectionPool(conn))
    reviewer_session_id = _insert_ready_session(
        conn,
        workspace_id=seeded["workspace_id"],
        task_id=seeded["task_id"],
        connection_id=seeded["connection_id"],
        role="reviewer",
        external_session_id="reviewer-ext-1",
    )
    for number in range(60):
        _insert_settled_receipt(
            conn,
            workspace_id=seeded["workspace_id"],
            task_id=seeded["task_id"],
            session_id=seeded["producer_session_id"],
            number=number,
            minutes_ago=120 - number,
        )
    conn.execute(
        "insert into openorc.task_hub_run_receipts "
        "(workspace_id, task_id, task_agent_session_id, operation, original_attempt_kind, "
        "original_attempt_id, expected_formal_family, status, outcome) "
        "values (%s, %s, %s, 'planning_bootstrap_prepare', 'planning_bootstrap_claim', %s, "
        "'workspace_result', 'claimed', '{}'::jsonb)",
        (seeded["workspace_id"], seeded["task_id"], reviewer_session_id, uuid.uuid4()),
    )

    observation = load_task_observation(
        pool, workspace_id=seeded["workspace_id"], task_id=seeded["task_id"]
    )

    assert observation is not None
    assert observation.outstanding_receipts_total == 1
    assert len(observation.receipts) == 1
    assert observation.receipts[0].status is TaskDetachedTurnReceiptStatus.CLAIMED
    assert observation.receipts[0].task_agent_session_id == reviewer_session_id


class _ReaderConnectionPool:
    """A pool adapter over one standalone reader connection.

    Mirrors the production pool's per-checkout commit discipline (commit on
    clean exit, rollback on error) so the authorized read service's
    pre-composition authorization reads and the composed read-only snapshot
    transaction sequence exactly as they do against a real pool.
    """

    def __init__(self, connection: Any) -> None:
        self._connection = connection

    @contextmanager
    def connection(self) -> Iterator[Any]:
        with self._connection:
            yield self._connection

    def close(self) -> None:
        raise AssertionError("the test owns the reader connection")


def test_authorized_task_read_composes_actions_inside_the_read_only_snapshot(
    migrated_database: str,
) -> None:
    """The issue #376 coherence boundary exercised end to end: one actual
    authorized Task detail read — Workspace ownership, the account barrier,
    the composed repeatable-read snapshot, and the backend-generated action
    projection — against real Postgres. The read-only transaction must never
    carry a locking statement (the account barrier is the non-locking
    read-side predicate), and the composition offers the PR_AUTHORIZATION
    menu from the seeded durable facts."""
    with connect(migrated_database, autocommit=True, prepare_threshold=None) as writer:
        seeded = _seed_task(writer, status="waiting_for_owner")
        pool = cast(DatabasePool, _SingleConnectionPool(writer))
        task = task_records.get_task(pool, seeded["task_id"])
        assert task is not None
        gate = gate_records.create_owner_gate(
            pool,
            workspace_id=seeded["workspace_id"],
            task_id=seeded["task_id"],
            gate_type=OwnerGateType.PR_AUTHORIZATION,
            subject_head_sha="ab" * 20,
        )
        installed = task_records.set_current_owner_gate(
            pool,
            seeded["task_id"],
            expected_state_token=task.state_token,
            owner_gate_id=gate.id,
        )
        assert installed is not None
        _seed_attempt(
            pool,
            seeded,
            execution_number=1,
            final_status=ExecutionStatus.SUCCEEDED,
            implementation_result=_RESULT,
        )

        with connect(migrated_database, prepare_threshold=None) as reader:
            reader_pool = cast(DatabasePool, _ReaderConnectionPool(reader))
            detail = task_reads.get_task_for_owner(
                reader_pool,
                profile_id=seeded["owner_profile_id"],
                workspace_id=seeded["workspace_id"],
                task_id=seeded["task_id"],
            )

        assert detail.summary.current_gate is not None
        # The composed action projection: the PR_AUTHORIZATION menu from the
        # seeded durable facts — cancel eligible (no outstanding runtime
        # work), no Task-level duplicate, nothing else offered.
        assert detail.summary.current_gate.actions == ("approve", "cancel_task")
        assert detail.summary.actions == ()
        assert detail.executions_total == 1
        assert detail.executions[0].result is not None
        assert detail.executions[0].result["branch"] == "openorc/issue-42"
        assert detail.outstanding_receipts == ()
        assert detail.outstanding_receipts_total == 0
        assert detail.pending_runtime_requests == ()
        assert detail.pending_runtime_requests_total == 0
        assert detail.outstanding_intents == ()
        assert detail.outstanding_intents_total == 0

        # A later authorized read observes the committed change: the snapshot
        # transaction released with its composition scope.
        writer.execute(
            "update openorc.tasks set status = 'cancelled' where id = %s",
            (seeded["task_id"],),
        )
        with connect(migrated_database, prepare_threshold=None) as reader:
            reread = task_reads.get_task_for_owner(
                cast(DatabasePool, _ReaderConnectionPool(reader)),
                profile_id=seeded["owner_profile_id"],
                workspace_id=seeded["workspace_id"],
                task_id=seeded["task_id"],
            )
            assert reread.summary.task.status.value == "cancelled"
