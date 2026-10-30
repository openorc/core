"""Durable state of the Level 2 operational-failure disposition (issue #251).

The focused real-Postgres checks the F4 seam owns:

- the episode/attempt uniqueness backstops hold in the database: one episode
  per original durable attempt identity, one ordinal per episode within the
  1..3 budget, and at most one successor per failed attempt across the
  whole history; the closed CHECK vocabularies refuse unknown values;
- a triage retry authorization commits exactly one durable attempt with its
  persisted due time, and a repeated report is a rejected no-op that mints
  no second successor;
- an escalation commits the F3 block/gate/BLOCKED/audit composition and the
  episode's set-once terminal TaskBlock reference together, and a repeated
  report is a no-op that names the exact disposed TaskBlock;
- a consumed authorization binds its resulting Execution exactly once and
  exactly within the same Task/Workspace scope (the composite linkage FK
  refuses a cross-Task effect);
- a settlement that never touches the Task row (the claim's
  ``known_nondelivery`` conditional update) serializes against the triage's
  locked evidence read in both orderings: the locked read blocks behind the
  open settlement, and the settlement blocks behind the triage's lock —
  neither side observes a torn state.

No live GitHub, runtime, or Supabase Auth access. The suite consumes the
explicitly supplied non-production branch database per
``tests/integration/conftest.py``.
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime, timedelta
from typing import Any, cast

import pytest
from psycopg import Connection, connect
from psycopg.errors import (
    CheckViolation,
    ForeignKeyViolation,
    LockNotAvailable,
    UniqueViolation,
)

from openorc.domain.blocks import TaskBlockReason
from openorc.domain.operational_failures import (
    FailureEffectEvidence,
    OperationalFailureCode,
    OperationalFailureSourceKind,
    OriginalAttemptKind,
)
from openorc.domain.pending_interactions import PendingTaskRuntimeInteractionOperation
from openorc.domain.tasks import TaskStatus
from openorc.persistence import blocks as block_repositories
from openorc.persistence import operational_failures as failure_repositories
from openorc.persistence import tasks as task_repositories
from openorc.persistence.pool import DatabasePool
from openorc.services import event_coordination
from openorc.services.task_operational_failure_triage import (
    DispositionKind,
    NoOpReason,
    OperationalFailureOperation,
    OperationalFailureReport,
    triage_operational_failure,
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

_NOW = datetime(2026, 10, 30, 12, 0, 0, tzinfo=UTC)
_HEAD_SHA = "b" * 40


def _event_counts(conn: Connection[Any], task_id: uuid.UUID) -> dict[str, int]:
    rows = conn.execute(
        "select event_type, count(*) from openorc.workflow_events where task_id = %s "
        "group by event_type",
        (task_id,),
    ).fetchall()
    return {str(row[0]): int(row[1]) for row in rows}


def _task_fixture(conn: Connection[Any], *, status: TaskStatus) -> dict[str, Any]:
    profile_id = _insert_profile(conn)
    workspace_id = _insert_workspace(conn, profile_id)
    installation_id = _insert_installation(conn, workspace_id=workspace_id)
    repository_id = _insert_repository(
        conn, workspace_id=workspace_id, github_installation_id=installation_id
    )
    task_id = _insert_task(conn, workspace_id=workspace_id, repository_id=repository_id)
    pool = cast(DatabasePool, _SingleConnectionPool(conn))
    task = task_repositories.get_task(pool, task_id)
    assert task is not None
    moved = task_repositories.update_task_status(
        pool,
        task_id,
        expected_state_token=task.state_token,
        status=status,
    )
    assert moved is not None
    return {
        "pool": pool,
        "conn": conn,
        "workspace_id": workspace_id,
        "task_id": task_id,
        "profile_id": profile_id,
        "state_token": moved.state_token,
        "status": status,
    }


def _execution_fixture(
    conn: Connection[Any], facts: dict[str, Any]
) -> tuple[uuid.UUID, uuid.UUID, uuid.UUID]:
    """One IMPLEMENTING Task with its full authorized execution subject set.

    The current plan revision, the Producer binding, the historical APPROVED
    implementation gate bound to that revision, and one failed_transient
    execution whose execution_failed audit event classifies the positively
    proven pre-effect runtime failure.
    """
    producer_session_id = _insert_ready_session(
        conn,
        workspace_id=facts["workspace_id"],
        task_id=facts["task_id"],
        connection_id=_insert_connection(conn, workspace_id=facts["workspace_id"]),
        role="producer",
        external_session_id="fake-producer-1",
    )
    revision_id = uuid.uuid4()
    conn.execute(
        "insert into openorc.plan_revisions "
        "(id, workspace_id, task_id, revision_number, content, repository_base_sha) "
        "values (%s, %s, %s, 1, '# Plan', %s)",
        (revision_id, facts["workspace_id"], facts["task_id"], "a" * 40),
    )
    conn.execute(
        "update openorc.tasks set current_plan_revision_id = %s where id = %s",
        (revision_id, facts["task_id"]),
    )
    gate_id = uuid.uuid4()
    conn.execute(
        "insert into openorc.owner_gates "
        "(id, workspace_id, task_id, gate_type, status, plan_revision_id, decided_at) "
        "values (%s, %s, %s, 'implementation_authorization', 'approved', %s, now())",
        (gate_id, facts["workspace_id"], facts["task_id"], revision_id),
    )
    execution_id = uuid.uuid4()
    conn.execute(
        "insert into openorc.executions "
        "(id, workspace_id, task_id, producer_session_id, execution_number, status) "
        "values (%s, %s, %s, %s, 1, 'failed_transient')",
        (execution_id, facts["workspace_id"], facts["task_id"], producer_session_id),
    )
    conn.execute(
        "insert into openorc.workflow_events "
        "(id, workspace_id, task_id, event_type, actor_type, subject_type, subject_id, context) "
        "values (%s, %s, %s, 'execution_failed', 'producer', 'execution', %s, %s)",
        (
            uuid.uuid4(),
            facts["workspace_id"],
            facts["task_id"],
            execution_id,
            '{"failure_kind": "runtime_pre_effect"}',
        ),
    )
    return execution_id, producer_session_id, revision_id


def _triage(
    pool: DatabasePool,
    facts: dict[str, Any],
    *,
    execution_id: uuid.UUID,
    status: TaskStatus = TaskStatus.IMPLEMENTING,
    state_token: uuid.UUID | None = None,
) -> Any:
    return triage_operational_failure(
        pool,
        workspace_id=facts["workspace_id"],
        task_id=facts["task_id"],
        expected_state_token=state_token or facts["state_token"],
        expected_status=status,
        expected_current_owner_gate_id=None,
        actor=event_coordination.owner_actor(facts["profile_id"]),
        operation=OperationalFailureOperation(
            family=PendingTaskRuntimeInteractionOperation.IMPLEMENTATION_EXECUTION,
            original_attempt_kind=OriginalAttemptKind.EXECUTION,
            original_attempt_id=execution_id,
            subject={"execution_id": str(execution_id)},
        ),
        report=OperationalFailureReport(
            reporting_attempt_kind=OperationalFailureSourceKind.ORIGINAL,
            reporting_attempt_id=execution_id,
            error_code=OperationalFailureCode.RUNTIME_UNAVAILABLE,
            effect_evidence=FailureEffectEvidence.PRE_EFFECT_NOT_APPLIED,
        ),
        clock=lambda: _NOW,
    )


def test_the_episode_and_attempt_uniqueness_backstops_hold(conn: Connection[Any]) -> None:
    facts = _task_fixture(conn, status=TaskStatus.IMPLEMENTING)
    workspace_id = facts["workspace_id"]
    task_id = facts["task_id"]
    original_id = uuid.uuid4()

    episode = failure_repositories.create_operational_failure_episode(
        facts["pool"],
        workspace_id=workspace_id,
        task_id=task_id,
        operation_family=PendingTaskRuntimeInteractionOperation.IMPLEMENTATION_EXECUTION,
        original_attempt_kind=OriginalAttemptKind.EXECUTION,
        original_attempt_id=original_id,
        subject={"execution_id": str(original_id)},
    )
    with pytest.raises(UniqueViolation), conn.transaction():
        failure_repositories.create_operational_failure_episode(
            facts["pool"],
            workspace_id=workspace_id,
            task_id=task_id,
            operation_family=PendingTaskRuntimeInteractionOperation.IMPLEMENTATION_EXECUTION,
            original_attempt_kind=OriginalAttemptKind.EXECUTION,
            original_attempt_id=original_id,
            subject={"execution_id": str(original_id)},
        )

    attempt = failure_repositories.create_operational_failure_attempt(
        facts["pool"],
        workspace_id=workspace_id,
        task_id=task_id,
        episode_id=episode.id,
        ordinal=1,
        source_kind=OperationalFailureSourceKind.ORIGINAL,
        source_attempt_id=original_id,
        error_code=OperationalFailureCode.RUNTIME_UNAVAILABLE,
        not_before=_NOW + timedelta(seconds=2),
    )
    # At most one successor per failed attempt across the whole history.
    with pytest.raises(UniqueViolation), conn.transaction():
        failure_repositories.create_operational_failure_attempt(
            facts["pool"],
            workspace_id=workspace_id,
            task_id=task_id,
            episode_id=episode.id,
            ordinal=2,
            source_kind=OperationalFailureSourceKind.ORIGINAL,
            source_attempt_id=original_id,
            error_code=OperationalFailureCode.RUNTIME_UNAVAILABLE,
            not_before=_NOW + timedelta(seconds=4),
        )
    # One ordinal per episode.
    with pytest.raises(UniqueViolation), conn.transaction():
        failure_repositories.create_operational_failure_attempt(
            facts["pool"],
            workspace_id=workspace_id,
            task_id=task_id,
            episode_id=episode.id,
            ordinal=1,
            source_kind=OperationalFailureSourceKind.PRIOR_AUTHORIZED,
            source_attempt_id=attempt.id,
            error_code=OperationalFailureCode.RUNTIME_UNAVAILABLE,
            not_before=_NOW + timedelta(seconds=4),
        )
    # The ordinal budget is exactly 1..3.
    with pytest.raises(CheckViolation), conn.transaction():
        failure_repositories.create_operational_failure_attempt(
            facts["pool"],
            workspace_id=workspace_id,
            task_id=task_id,
            episode_id=episode.id,
            ordinal=4,
            source_kind=OperationalFailureSourceKind.PRIOR_AUTHORIZED,
            source_attempt_id=attempt.id,
            error_code=OperationalFailureCode.RUNTIME_UNAVAILABLE,
            not_before=_NOW + timedelta(seconds=8),
        )
    # The closed error-code vocabulary refuses an unknown value.
    with pytest.raises(CheckViolation), conn.transaction():
        conn.execute(
            "insert into openorc.operational_failure_attempts "
            "(workspace_id, task_id, episode_id, ordinal, source_kind, "
            "source_attempt_id, error_code, not_before) "
            "values (%s, %s, %s, 2, 'prior_authorized', %s, 'made_up_code', now())",
            (workspace_id, task_id, episode.id, attempt.id),
        )
    # The terminal disposition is set-once: the conditional mark wins once.
    block_id = uuid.uuid4()
    conn.execute(
        "insert into openorc.task_blocks (id, workspace_id, task_id, reason, context) "
        "values (%s, %s, %s, 'runtime_failure', '{}')",
        (block_id, workspace_id, task_id),
    )
    marked = failure_repositories.mark_episode_escalated(
        facts["pool"],
        episode_id=episode.id,
        task_id=task_id,
        workspace_id=workspace_id,
        escalated_task_block_id=block_id,
    )
    assert marked is not None and marked.escalated_task_block_id == block_id
    assert (
        failure_repositories.mark_episode_escalated(
            facts["pool"],
            episode_id=episode.id,
            task_id=task_id,
            workspace_id=workspace_id,
            escalated_task_block_id=uuid.uuid4(),
        )
        is None
    )


def test_the_triage_authorization_commits_one_durable_attempt(conn: Connection[Any]) -> None:
    facts = _task_fixture(conn, status=TaskStatus.IMPLEMENTING)
    execution_id, _session_id, _revision_id = _execution_fixture(conn, facts)

    result = _triage(facts["pool"], facts, execution_id=execution_id)

    assert result.kind is DispositionKind.RETRY_AUTHORIZED
    assert result.retry_attempt is not None
    assert result.retry_attempt.not_before == _NOW + timedelta(seconds=2)
    episodes = conn.execute(
        "select id, escalated_task_block_id from openorc.operational_failure_episodes "
        "where task_id = %s",
        (facts["task_id"],),
    ).fetchall()
    assert len(episodes) == 1
    assert episodes[0][1] is None
    attempts = conn.execute(
        "select ordinal, source_kind, source_attempt_id, error_code, not_before "
        "from openorc.operational_failure_attempts where episode_id = %s order by ordinal",
        (episodes[0][0],),
    ).fetchall()
    assert attempts == [
        (1, "original", execution_id, "runtime_unavailable", _NOW + timedelta(seconds=2))
    ]

    # A repeated report of the same failure is a rejected no-op that mints
    # no second successor and never resets the budget.
    repeated = _triage(facts["pool"], facts, execution_id=execution_id)
    assert repeated.kind is DispositionKind.NO_OP
    assert repeated.no_op_reason is NoOpReason.ALREADY_DISPOSED
    assert repeated.existing_attempt_id == result.retry_attempt.id
    assert (
        len(
            conn.execute(
                "select 1 from openorc.operational_failure_attempts where episode_id = %s",
                (episodes[0][0],),
            ).fetchall()
        )
        == 1
    )


def test_the_escalation_commits_the_terminal_disposition_atomically(
    conn: Connection[Any],
) -> None:
    facts = _task_fixture(conn, status=TaskStatus.PLANNING)
    claim_id = uuid.uuid4()
    conn.execute(
        "insert into openorc.task_runtime_send_claims "
        "(id, workspace_id, task_id, kind, task_agent_session_id, status) "
        "values (%s, %s, %s, 'initial_plan_dispatch', %s, 'claimed')",
        (
            claim_id,
            facts["workspace_id"],
            facts["task_id"],
            _insert_ready_session(
                conn,
                workspace_id=facts["workspace_id"],
                task_id=facts["task_id"],
                connection_id=_insert_connection(conn, workspace_id=facts["workspace_id"]),
                role="producer",
                external_session_id="fake-producer-2",
            ),
        ),
    )

    def _send_report() -> dict[str, Any]:
        return dict(
            workspace_id=facts["workspace_id"],
            task_id=facts["task_id"],
            actor=event_coordination.owner_actor(facts["profile_id"]),
            operation=OperationalFailureOperation(
                family=PendingTaskRuntimeInteractionOperation.INITIAL_PLAN_DISPATCH,
                original_attempt_kind=OriginalAttemptKind.RUNTIME_SEND_CLAIM,
                original_attempt_id=claim_id,
                subject={"claim_id": str(claim_id)},
            ),
            report=OperationalFailureReport(
                reporting_attempt_kind=OperationalFailureSourceKind.ORIGINAL,
                reporting_attempt_id=claim_id,
                error_code=OperationalFailureCode.RUNTIME_UNAVAILABLE,
                effect_evidence=FailureEffectEvidence.PRE_EFFECT_NOT_APPLIED,
            ),
            clock=lambda: _NOW,
        )

    result = triage_operational_failure(
        facts["pool"],
        expected_state_token=facts["state_token"],
        expected_status=TaskStatus.PLANNING,
        expected_current_owner_gate_id=None,
        **_send_report(),
    )

    assert result.kind is DispositionKind.BLOCKED
    assert result.installation is not None
    blocks = block_repositories.list_task_blocks(facts["pool"], task_id=facts["task_id"])
    assert [block.id for block in blocks] == [result.installation.task_block.id]
    assert blocks[0].reason is TaskBlockReason.CONNECTION_UNAVAILABLE
    task = task_repositories.get_task(facts["pool"], facts["task_id"])
    assert task is not None and task.status is TaskStatus.BLOCKED
    assert _event_counts(conn, facts["task_id"]) == {
        "task_blocked": 1,
        "owner_gate_created": 1,
    }
    # The episode's terminal disposition references the exact F3 TaskBlock.
    episode_row = conn.execute(
        "select escalated_task_block_id from openorc.operational_failure_episodes "
        "where original_attempt_kind = 'runtime_send_claim' and original_attempt_id = %s",
        (claim_id,),
    ).fetchone()
    assert episode_row is not None
    assert episode_row[0] == result.installation.task_block.id

    # A repeated report is a no-op naming the exact disposed TaskBlock, and
    # the blocked Task is never a new F3 installation target.
    repeated = triage_operational_failure(
        facts["pool"],
        expected_state_token=result.installation.task.state_token,
        expected_status=TaskStatus.BLOCKED,
        expected_current_owner_gate_id=result.installation.owner_gate.id,
        **_send_report(),
    )
    assert repeated.kind is DispositionKind.NO_OP
    assert repeated.no_op_reason is NoOpReason.ALREADY_TERMINALLY_DISPOSED
    assert repeated.existing_task_block_id == result.installation.task_block.id
    assert _event_counts(conn, facts["task_id"]) == {
        "task_blocked": 1,
        "owner_gate_created": 1,
    }


def test_the_settlement_serializes_against_the_locked_triage_evidence_read(
    conn: Connection[Any], migrated_database: str
) -> None:
    """The two-ordering settlement-versus-triage race, deterministically.

    The claim's ``known_nondelivery`` settlement never touches the Task row,
    so the triage's evidence read must serialize on the claim row itself.
    With a short ``lock_timeout`` on the second session, a blocked waiter
    raises ``LockNotAvailable`` — the deterministic proof of serialization.
    """
    facts = _task_fixture(conn, status=TaskStatus.PLANNING)
    claim_id = uuid.uuid4()
    session_id = _insert_ready_session(
        conn,
        workspace_id=facts["workspace_id"],
        task_id=facts["task_id"],
        connection_id=_insert_connection(conn, workspace_id=facts["workspace_id"]),
        role="producer",
        external_session_id="fake-producer-3",
    )
    conn.execute(
        "insert into openorc.task_runtime_send_claims "
        "(id, workspace_id, task_id, kind, task_agent_session_id, status) "
        "values (%s, %s, %s, 'initial_plan_dispatch', %s, 'claimed')",
        (claim_id, facts["workspace_id"], facts["task_id"], session_id),
    )
    # The race needs two independent sessions over durable rows: commit the
    # fixture, run both orderings, then remove the committed fixture rows.
    conn.commit()

    settlement_sql = (
        "update openorc.task_runtime_send_claims "
        "set status = 'known_nondelivery', outcome = "
        '\'{"failure_kind": "runtime_pre_effect"}\'::jsonb, '
        "settled_at = now(), updated_at = now() "
        "where id = %s and task_id = %s and workspace_id = %s and status = 'claimed'"
    )
    task_lock_sql = "select state_token from openorc.tasks where id = %s for update"
    claim_lock_sql = "select status from openorc.task_runtime_send_claims where id = %s for update"
    # Autocommit sessions with explicit transaction blocks: the lock_timeout
    # SETs apply session-wide, and each blocked waiter raises
    # LockNotAvailable deterministically instead of hanging the suite.
    with (
        connect(migrated_database, autocommit=True) as settlement,
        connect(migrated_database, autocommit=True) as triage,
    ):
        triage.execute("set lock_timeout = '200ms'")
        settlement.execute("set lock_timeout = '200ms'")
        # Ordering 1: the settlement holds the claim row lock; the triage's
        # locked evidence read waits behind it.
        with settlement.transaction():
            settlement.execute(settlement_sql, (claim_id, facts["task_id"], facts["workspace_id"]))
            with pytest.raises(LockNotAvailable), triage.transaction():
                triage.execute(task_lock_sql, (facts["task_id"],))
                triage.execute(claim_lock_sql, (claim_id,))
        # The settlement commits; the triage's locked re-read observes the
        # settled truth - never a torn claimed read.
        with triage.transaction():
            status_row = triage.execute(claim_lock_sql, (claim_id,)).fetchone()
            assert status_row is not None and status_row[0] == "known_nondelivery"

        # Ordering 2: the triage holds the Task lock and the claim lock; the
        # settlement waits behind them instead of proceeding on a stale read.
        with triage.transaction():
            triage.execute(task_lock_sql, (facts["task_id"],))
            triage.execute(claim_lock_sql, (claim_id,))
            with pytest.raises(LockNotAvailable), settlement.transaction():
                settlement.execute(
                    settlement_sql, (claim_id, facts["task_id"], facts["workspace_id"])
                )

    # The committed fixture rows are removed so the branch database stays
    # clean for the rest of the session.
    conn.execute("delete from openorc.workspaces where id = %s", (facts["workspace_id"],))
    conn.execute("delete from auth.users where id = %s", (facts["profile_id"],))
    conn.commit()


def test_the_retry_binding_is_set_once_and_scope_exact(conn: Connection[Any]) -> None:
    facts = _task_fixture(conn, status=TaskStatus.IMPLEMENTING)
    workspace_id = facts["workspace_id"]
    task_id = facts["task_id"]
    original_id = uuid.uuid4()
    episode = failure_repositories.create_operational_failure_episode(
        facts["pool"],
        workspace_id=workspace_id,
        task_id=task_id,
        operation_family=PendingTaskRuntimeInteractionOperation.IMPLEMENTATION_EXECUTION,
        original_attempt_kind=OriginalAttemptKind.EXECUTION,
        original_attempt_id=original_id,
        subject={"execution_id": str(original_id)},
    )
    attempt = failure_repositories.create_operational_failure_attempt(
        facts["pool"],
        workspace_id=workspace_id,
        task_id=task_id,
        episode_id=episode.id,
        ordinal=1,
        source_kind=OperationalFailureSourceKind.ORIGINAL,
        source_attempt_id=original_id,
        error_code=OperationalFailureCode.RUNTIME_UNAVAILABLE,
        not_before=_NOW + timedelta(seconds=2),
    )
    execution_id = uuid.uuid4()
    conn.execute(
        "insert into openorc.executions "
        "(id, workspace_id, task_id, producer_session_id, execution_number, status) "
        "values (%s, %s, %s, %s, 1, 'queued')",
        (
            execution_id,
            workspace_id,
            task_id,
            _insert_ready_session(
                conn,
                workspace_id=workspace_id,
                task_id=task_id,
                connection_id=_insert_connection(conn, workspace_id=workspace_id),
                role="producer",
                external_session_id="fake-producer-bind",
            ),
        ),
    )
    bound = failure_repositories.bind_retry_attempt_execution(
        facts["pool"],
        attempt_id=attempt.id,
        task_id=task_id,
        workspace_id=workspace_id,
        resulting_execution_id=execution_id,
    )
    assert bound is not None and bound.resulting_execution_id == execution_id
    # Set-once: any later binding attempt is a rejected no-op, whatever the
    # proposed Execution.
    assert (
        failure_repositories.bind_retry_attempt_execution(
            facts["pool"],
            attempt_id=attempt.id,
            task_id=task_id,
            workspace_id=workspace_id,
            resulting_execution_id=execution_id,
        )
        is None
    )
    assert (
        failure_repositories.bind_retry_attempt_execution(
            facts["pool"],
            attempt_id=attempt.id,
            task_id=task_id,
            workspace_id=workspace_id,
            resulting_execution_id=uuid.uuid4(),
        )
        is None
    )
    # The binding is a same-Task/Workspace linkage: a cross-Task Execution
    # reference is refused (the composite FK is deferred, so the pending
    # check is forced inside a savepoint).
    other_task_id = uuid.uuid4()
    conn.execute(
        "insert into openorc.tasks "
        "(id, workspace_id, repository_id, github_issue_id, github_issue_number, status, "
        "source_requirements_fingerprint) "
        "values (%s, %s, %s, 7704, 44, 'planning', %s)",
        (other_task_id, workspace_id, facts["repository_id"], "a" * 64),
    )
    other_execution_id = uuid.uuid4()
    conn.execute(
        "insert into openorc.executions "
        "(id, workspace_id, task_id, producer_session_id, execution_number, status) "
        "values (%s, %s, %s, %s, 1, 'queued')",
        (
            other_execution_id,
            workspace_id,
            other_task_id,
            _insert_ready_session(
                conn,
                workspace_id=workspace_id,
                task_id=other_task_id,
                connection_id=_insert_connection(conn, workspace_id=workspace_id),
                role="producer",
                external_session_id="fake-producer-bind-2",
            ),
        ),
    )
    with pytest.raises(ForeignKeyViolation), conn.transaction():
        failure_repositories.bind_retry_attempt_execution(
            facts["pool"],
            attempt_id=attempt.id,
            task_id=task_id,
            workspace_id=workspace_id,
            resulting_execution_id=other_execution_id,
        )
        conn.execute(
            "set constraints openorc.operational_failure_attempts_resulting_execution_fk immediate"
        )
