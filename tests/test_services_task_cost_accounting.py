"""Deterministic cost-accounting service behavior (issue #240).

The scripted seams prove the read/checkpoint/check contract: the
exact-scope high-water checkpoint, the role-agnostic retained-cost
aggregate, the budget evaluation under the Task row lock, and the atomic
Owner-authorized ``COST_LIMIT`` block composed through the shared E10b
machinery and the F3 exceptional blocking foundation — one pending
``BLOCK_RESOLUTION`` OwnerGate bound to the block — with DB-derived Owner
attribution. Ordinary determinism: no live infrastructure, canned rows
only.
"""

from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, datetime
from decimal import Decimal
from types import SimpleNamespace
from typing import Any, cast
from uuid import uuid4

import pytest

from openorc.domain.blocks import TaskBlockReason
from openorc.domain.executions import ExecutionStatus
from openorc.persistence import executions as execution_records
from openorc.persistence import reviews as review_records
from openorc.persistence.pool import DatabasePool
from openorc.services import task_abort
from openorc.services.errors import InvalidCommandError
from openorc.services.task_cost_accounting import (
    BudgetGateDecisionKind,
    SessionCostObservationKind,
    check_task_budget_before_dispatch,
    observe_session_usage,
)
from tests.test_services_task_pr_review_decision import (
    _event_returned_row,
    _event_types,
    _gate_row,
)
from tests.test_services_task_pr_review_dispatch import (
    _ACCOUNT_BARRIER,
    _ACCOUNT_OPERATIONAL_ROW,
    _CONNECTION_ID,
    _OWNER_PROFILE_ID,
    _TASK_ID,
    _TASK_READ,
    _TASK_READ_LOCKED,
    _WORKSPACE_ID,
    ScriptedConnection,
    _count,
    _pool,
    _session_row,
    _sql_has,
    _task_row,
    _ws_row,
)

_OBSERVED = datetime(2026, 10, 26, 12, 0, 0, tzinfo=UTC)
_SESSION_ID = uuid4()
_EXTERNAL_SESSION_ID = "fake-session-1"
_BUDGET = Decimal("25.00")
_GATE_ID = uuid4()
_PRIOR_GATE_ID = uuid4()
_BLOCK_ID = uuid4()
_ROTATED_TOKEN = uuid4()

_CHECKPOINT = _sql_has(
    "update openorc.task_agent_sessions", "retained_cost_usd = %s", "cost_observed_at = now()"
)
_COST_SUM = _sql_has("select sum(retained_cost_usd)", "from openorc.task_agent_sessions")
_LIST_SESSIONS = _sql_has(
    "from openorc.task_agent_sessions", "where task_id = %s order by created_at, id"
)
_BLOCK_INSERT = _sql_has("insert into openorc.task_blocks")
_TASK_BLOCK_UPDATE = _sql_has("update openorc.tasks", "status = 'blocked'")
_GATE_READ = _sql_has("from openorc.owner_gates where id")
_GATE_INSERT = _sql_has("insert into openorc.owner_gates")
_EVENT_INSERT = _sql_has("insert into openorc.workflow_events")
_INTENT_INSERT = _sql_has("insert into openorc.task_interrupt_intents")


def _budgeted_task_row(status: str = "implementing") -> tuple[Any, ...]:
    row = _task_row(status=status, state_token=uuid4())
    return (*row[:16], _BUDGET, *row[17:])


def _queued_budgeted_task_row() -> tuple[Any, ...]:
    # A domain-valid queued row carries its durable queue-entry timestamp.
    row = _budgeted_task_row(status="queued")
    return (*row[:12], _OBSERVED, *row[13:])


def _blocked_task_row() -> tuple[Any, ...]:
    # The post-installation Task: blocked, rotated token, the exceptional
    # gate installed as the current pointer.
    row = _budgeted_task_row(status="blocked")
    row = (*row[:8], _ROTATED_TOKEN, *row[9:])
    return (*row[:10], _GATE_ID, *row[11:])


def _ready_session_row() -> tuple[Any, ...]:
    return _session_row("producer", _SESSION_ID, _EXTERNAL_SESSION_ID)


def _block_row() -> tuple[Any, ...]:
    # openorc.task_blocks: id, workspace_id, task_id, reason, context,
    # resolved_at, created_at.
    return (
        _BLOCK_ID,
        _WORKSPACE_ID,
        _TASK_ID,
        "cost_limit",
        {"control": "owner_abort"},
        None,
        _OBSERVED,
    )


def _intent_row() -> tuple[Any, ...]:
    # openorc.task_interrupt_intents: id, workspace_id, task_id,
    # task_agent_session_id, task_block_id, workflow_role,
    # external_session_id, requested_by_profile_id, status, outcome,
    # created_at, updated_at.
    return (
        uuid4(),
        _WORKSPACE_ID,
        _TASK_ID,
        _SESSION_ID,
        _BLOCK_ID,
        "producer",
        _EXTERNAL_SESSION_ID,
        _OWNER_PROFILE_ID,
        "unsettled",
        {},
        _OBSERVED,
        _OBSERVED,
    )


def _exceptional_gate_row() -> tuple[Any, ...]:
    row = list(_gate_row(gate_id=_GATE_ID, gate_type="cost_limit", head_sha=None, pr_id=None))
    row[8] = _BLOCK_ID  # the gate's TaskBlock subject
    return tuple(row)


def _wire_budgeted_task(
    conn: ScriptedConnection,
    monkeypatch: pytest.MonkeyPatch,
    *,
    status: str = "implementing",
    known_total: Decimal | None = _BUDGET,
    has_unobserved: bool = True,
    task_row: tuple[Any, ...] | None = None,
    no_task: bool = False,
    checkpoint: Any = "session",
    sessions: list[tuple[Any, ...]] | None = None,
    executions: list[Any] | None = None,
    prior_gate_id: Any | None = None,
) -> None:
    """Script the locked budgeted Task, its checkpoint target, and the aggregate.

    ``no_task`` scripts the locked read returning None (a foreign/absent
    subject); ``checkpoint`` overrides the checkpoint write result (``None``
    means the conditional update matched no row). ``prior_gate_id`` scripts a
    current pending ordinary gate on the Task for the idle supersession case.
    The durable in-flight activity readers are patched at their module seams,
    exactly like the E10b tests: no canned execution/review rows flow through
    SQL mapping.
    """
    row = task_row if task_row is not None else _budgeted_task_row(status=status)
    conn.on_repeat(_TASK_READ_LOCKED, None if no_task else row)
    if checkpoint == "session":
        conn.on_repeat(
            _CHECKPOINT,
            _session_row("producer", _SESSION_ID, _EXTERNAL_SESSION_ID),
        )
    else:
        conn.on_repeat(_CHECKPOINT, checkpoint)
    conn.on_repeat(_COST_SUM, (known_total, has_unobserved))
    conn.on_repeat(_LIST_SESSIONS, sessions if sessions is not None else [_ready_session_row()])
    monkeypatch.setattr(
        execution_records,
        "list_task_executions",
        lambda pool, *, task_id: list(executions or []),
    )
    monkeypatch.setattr(
        review_records,
        "list_task_review_loops",
        lambda pool, *, task_id, status: [],
    )
    # The F3 exceptional blocking composition (when reached): the installer's
    # own locked/plain Task reads re-match the same row, a caller-observed
    # prior current gate is proven when one exists, the pending
    # BLOCK_RESOLUTION gate inserts bound to the block, and the guarded
    # blocked transition returns the post-installation row. Both audit
    # events answer the same insert matcher, in their committed order.
    conn.on_repeat(_TASK_READ, row)
    if prior_gate_id is not None:
        conn.on_repeat(
            _GATE_READ,
            _gate_row(gate_id=prior_gate_id, gate_type="pr_authorization", pr_id=None),
        )
    conn.on_repeat(_BLOCK_INSERT, _block_row())
    conn.on_repeat(_GATE_INSERT, _exceptional_gate_row())
    conn.on_repeat(_TASK_BLOCK_UPDATE, _blocked_task_row())
    conn.on_repeat(_EVENT_INSERT, _event_returned_row("task_blocked"))
    conn.on_repeat(_INTENT_INSERT, _intent_row())
    conn.on_repeat(_ACCOUNT_BARRIER, _ACCOUNT_OPERATIONAL_ROW)
    conn.on_repeat(_sql_has("from openorc.workspaces"), _ws_row(owner=_OWNER_PROFILE_ID))


def _open_execution(*, producer_session_id: Any = _SESSION_ID) -> Any:
    return SimpleNamespace(
        id=uuid4(),
        workspace_id=_WORKSPACE_ID,
        task_id=_TASK_ID,
        producer_session_id=producer_session_id,
        execution_number=1,
        status=ExecutionStatus.RUNNING,
        created_at=_OBSERVED,
        updated_at=_OBSERVED,
        implementation_result=None,
        last_qualifying_activity_at=None,
        review_iteration_id=None,
    )


def _sql_of_kind(conn: ScriptedConnection, predicate: Callable[[Any], bool]) -> list[str]:
    return [sql for sql, _ in conn.executed if predicate(sql)]


def _observe(
    conn: ScriptedConnection,
    *,
    cost: Decimal | None = Decimal("26.00"),
    send_in_flight: bool = False,
) -> Any:
    return observe_session_usage(
        cast(DatabasePool, _pool(conn)),
        workspace_id=_WORKSPACE_ID,
        task_id=_TASK_ID,
        task_agent_session_id=_SESSION_ID,
        connection_id=_CONNECTION_ID,
        aggregate_total_cost=cost,
        send_in_flight=send_in_flight,
    )


def test_checkpoint_below_threshold_retains_and_permits(monkeypatch: pytest.MonkeyPatch) -> None:
    conn = ScriptedConnection()
    _wire_budgeted_task(conn, monkeypatch, known_total=Decimal("20.00"))

    observation = _observe(conn, cost=Decimal("20.50"))

    assert observation.kind is SessionCostObservationKind.CHECKPOINTED
    checkpoint_sqls = _sql_of_kind(conn, lambda sql: "update openorc.task_agent_sessions" in sql)
    assert len(checkpoint_sqls) == 1
    # The exact-scope high-water write: id + full ownership tuple, and a
    # strictly-greater amount only.
    assert "where id = %s and task_id = %s and workspace_id = %s" in checkpoint_sqls[0]
    assert "and connection_id = %s" in checkpoint_sqls[0]
    assert "(retained_cost_usd is null or retained_cost_usd < %s)" in checkpoint_sqls[0]
    # No blocking decision below the threshold: no block insert, no guarded
    # blocked transition, no gate, no audit, no interrupt intents.
    assert not _sql_of_kind(conn, lambda sql: "insert into openorc.task_blocks" in sql)
    assert not _sql_of_kind(conn, lambda sql: _TASK_BLOCK_UPDATE(sql, None))
    assert not _sql_of_kind(conn, lambda sql: _GATE_INSERT(sql, None))
    assert not _sql_of_kind(conn, lambda sql: "insert into openorc.task_interrupt_intents" in sql)


def test_the_retained_cost_aggregate_is_role_agnostic_by_construction(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    conn = ScriptedConnection()
    _wire_budgeted_task(conn, monkeypatch, known_total=Decimal("20.00"))

    _observe(conn, cost=Decimal("20.50"))

    sum_sqls = [sql for sql, _ in conn.executed if "select sum(retained_cost_usd)" in sql]
    assert len(sum_sqls) >= 1
    for sql in sum_sqls:
        # No role predicate and no role branch anywhere: ownership is the
        # Task, not a role list (issue #240). This stays true without
        # touching the v1 WorkflowRole vocabulary.
        lowered = " ".join(sql.split()).lower()
        assert "role" not in lowered
        assert "where task_id = %s" in lowered


def test_threshold_reached_blocks_with_owner_attribution_and_exact_intent(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    conn = ScriptedConnection()
    _wire_budgeted_task(
        conn,
        monkeypatch,
        executions=[_open_execution()],
    )

    observation = _observe(conn, cost=Decimal("26.00"), send_in_flight=True)

    assert observation.kind is SessionCostObservationKind.BLOCKED
    assert observation.task is not None
    assert observation.task.status.value == "blocked"
    assert observation.task_block is not None
    assert observation.task_block.reason is TaskBlockReason.COST_LIMIT
    assert observation.interrupt_dispatches != ()
    intent, external_session_id = observation.interrupt_dispatches[0]
    assert external_session_id == _EXTERNAL_SESSION_ID
    assert intent.requested_by_profile_id == _OWNER_PROFILE_ID
    # The block insert carries the additive reason, and the F3 composition
    # installs its pending Level 3-selected COST_LIMIT gate (F8b, issue
    # #287) bound to that block.
    block_params = next(
        params for sql, params in conn.executed if "insert into openorc.task_blocks" in sql
    )
    assert block_params is not None and block_params[2] == "cost_limit"
    (gate_insert,) = [params for sql, params in conn.executed if _GATE_INSERT(sql, None)]
    assert gate_insert is not None and gate_insert[2] == "cost_limit"
    # The guarded blocked transition and the two OWNER audits commit in the
    # same composition; the new gate becomes the Task's current pointer.
    status_params = next(params for sql, params in conn.executed if _TASK_BLOCK_UPDATE(sql, None))
    assert status_params is not None
    assert status_params[0] == _GATE_ID and status_params[5] == _GATE_ID
    assert status_params[3] == "implementing" and status_params[4] is None
    assert _event_types(conn) == ["task_blocked", "owner_gate_created"]
    # The interrupt intent is created inside the composition with the
    # DB-derived Workspace Owner Profile as the accountable requester.
    intent_params = next(
        params
        for sql, params in conn.executed
        if "insert into openorc.task_interrupt_intents" in sql
    )
    assert intent_params is not None
    assert intent_params[3] == _BLOCK_ID
    assert intent_params[6] == _OWNER_PROFILE_ID
    # The threshold was re-verified under the lock: the aggregate ran for
    # the evaluation and again inside the shared composition.
    assert len(_sql_of_kind(conn, lambda sql: "select sum(retained_cost_usd)" in sql)) == 2


def test_idle_crossing_checkpoints_without_blocking(monkeypatch: pytest.MonkeyPatch) -> None:
    # A threshold crossing observed while NO autonomous model-backed turn of
    # the observing session is in flight — an idle Task, here waiting at its
    # pending ordinary Owner gate — checkpoints its truthful spend and
    # defers enforcement to the next active turn (issue #291, F8f,
    # Owner-approved amendment). No block, no interrupt, and the pending
    # ordinary gate stays exactly where it was.
    conn = ScriptedConnection()
    idle_row = _budgeted_task_row(status="waiting_for_owner")
    idle_row = (*idle_row[:10], _PRIOR_GATE_ID, *idle_row[11:])
    _wire_budgeted_task(
        conn,
        monkeypatch,
        status="waiting_for_owner",
        task_row=idle_row,
        prior_gate_id=_PRIOR_GATE_ID,
        sessions=[_ready_session_row()],
        executions=[],
    )

    observation = _observe(conn, send_in_flight=False)

    assert observation.kind is SessionCostObservationKind.TASK_NOT_CURRENT
    assert observation.task_block is None
    assert observation.interrupt_dispatches == ()
    assert not _sql_of_kind(conn, lambda sql: "insert into openorc.task_blocks" in sql)
    assert not _sql_of_kind(conn, lambda sql: "insert into openorc.task_interrupt_intents" in sql)
    assert not _sql_of_kind(conn, lambda sql: _TASK_BLOCK_UPDATE(sql, None))
    # The truthful checkpoint stands; the pending ordinary gate is untouched.
    assert not _event_types(conn)
    assert _count(conn, _sql_has("update openorc.owner_gates")) == 0


def test_unknown_reading_is_unknown_and_nonblocking(monkeypatch: pytest.MonkeyPatch) -> None:
    # An absent or unusable aggregate reading is unknown truth: nothing is
    # written, nothing is blocked, and no speculative fail-closed gate
    # exists.
    conn = ScriptedConnection()
    _wire_budgeted_task(conn, monkeypatch)

    observation = _observe(conn, cost=None)

    assert observation.kind is SessionCostObservationKind.NOT_OBSERVED
    assert not _sql_of_kind(conn, lambda sql: "update openorc.task_agent_sessions" in sql)
    assert not _sql_of_kind(conn, lambda sql: "insert into openorc.task_blocks" in sql)


def test_foreign_or_not_higher_observation_is_ignored(monkeypatch: pytest.MonkeyPatch) -> None:
    # A stale, foreign (cross-Task/Workspace/Connection), or not-higher
    # reading matches no checkpointable row: truthful no-op, never another
    # Task's accounting, and never a blocking decision.
    conn = ScriptedConnection()
    _wire_budgeted_task(conn, monkeypatch, checkpoint=None)

    observation = _observe(conn)

    assert observation.kind is SessionCostObservationKind.IGNORED_FOREIGN
    assert not _sql_of_kind(conn, lambda sql: "insert into openorc.task_blocks" in sql)
    assert not _sql_of_kind(conn, lambda sql: _TASK_BLOCK_UPDATE(sql, None))
    assert not _sql_of_kind(conn, lambda sql: _GATE_INSERT(sql, None))


def test_budget_disabled_task_checkpoints_without_enforcement(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # NULL authorized budget means no standing spending policy: the
    # truthful checkpoint is retained and no decision is ever invented.
    conn = ScriptedConnection()
    _wire_budgeted_task(
        conn, monkeypatch, task_row=_task_row(status="implementing", state_token=uuid4())
    )

    observation = _observe(conn)

    assert observation.kind is SessionCostObservationKind.BUDGET_DISABLED
    assert len(_sql_of_kind(conn, lambda sql: "update openorc.task_agent_sessions" in sql)) == 1
    assert not _sql_of_kind(conn, lambda sql: "insert into openorc.task_blocks" in sql)


def test_already_blocked_task_never_receives_a_duplicate_block(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    conn = ScriptedConnection()
    _wire_budgeted_task(conn, monkeypatch, task_row=_blocked_task_row())

    observation = _observe(conn)

    assert observation.kind is SessionCostObservationKind.ALREADY_BLOCKED
    # The checkpoint is still a truthful cost fact; the blocking decision
    # is not duplicated.
    assert len(_sql_of_kind(conn, lambda sql: "update openorc.task_agent_sessions" in sql)) == 1
    assert not _sql_of_kind(conn, lambda sql: "insert into openorc.task_blocks" in sql)
    assert not _sql_of_kind(conn, lambda sql: "insert into openorc.task_interrupt_intents" in sql)


def test_task_outside_the_enforcement_phases_is_not_current(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # A queued Task has no active work whose standing authorization could
    # interrupt; the truthful checkpoint stands and no policy applies.
    conn = ScriptedConnection()
    _wire_budgeted_task(conn, monkeypatch, task_row=_queued_budgeted_task_row())

    observation = _observe(conn)

    assert observation.kind is SessionCostObservationKind.TASK_NOT_CURRENT
    assert not _sql_of_kind(conn, lambda sql: "insert into openorc.task_blocks" in sql)


def test_foreign_task_observation_is_refused_before_any_write(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    conn = ScriptedConnection()
    _wire_budgeted_task(conn, monkeypatch, no_task=True)

    observation = _observe(conn)

    assert observation.kind is SessionCostObservationKind.IGNORED_FOREIGN
    assert not _sql_of_kind(conn, lambda sql: "update openorc.task_agent_sessions" in sql)


def test_active_planning_interaction_is_interrupted_on_exhaustion(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # A planning-phase Producer interaction runs as an admitted model-backed
    # interaction OUTSIDE any Execution (plan production, the planning
    # bootstrap): the durable facts alone never prove an active turn there,
    # so the unattested crossing checkpoints without blocking (issue #291,
    # F8f, Owner-approved amendment), while the LIVE observation's attested
    # in-flight turn makes the bound READY Producer session an interrupt
    # target and the crossing interrupts it.
    conn = ScriptedConnection()
    _wire_budgeted_task(conn, monkeypatch, status="planning", sessions=[_ready_session_row()])

    idle_observation = _observe(conn)

    assert idle_observation.kind is SessionCostObservationKind.CHECKPOINTED
    assert idle_observation.task_block is None
    assert not _sql_of_kind(conn, lambda sql: "insert into openorc.task_blocks" in sql)

    live_conn = ScriptedConnection()
    _wire_budgeted_task(live_conn, monkeypatch, status="planning", sessions=[_ready_session_row()])

    observation = _observe(live_conn, send_in_flight=True)

    assert observation.kind is SessionCostObservationKind.BLOCKED
    assert observation.task_block is not None
    assert observation.task_block.reason is TaskBlockReason.COST_LIMIT
    assert len(observation.interrupt_dispatches) == 1
    assert observation.interrupt_dispatches[0][1] == _EXTERNAL_SESSION_ID
    intent_params = next(
        params
        for sql, params in live_conn.executed
        if "insert into openorc.task_interrupt_intents" in sql
    )
    assert intent_params is not None and intent_params[6] == _OWNER_PROFILE_ID


def test_a_late_cost_enforcement_never_supersedes_an_ordinary_owner_gate(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # The representative late-enforcement race (issue #291, F8f): the
    # in-flight marker was sampled during the autonomous turn, but the turn
    # finished and installed an ordinary Owner approval gate BEFORE the cost
    # path acquired the Task lock. Under the lock the Task waits at that
    # gate, so the cost-specific eligibility excludes waiting_for_owner: the
    # attested session can no longer make COST_LIMIT replace the approval
    # boundary. The truthful checkpoint stands, no block or interrupt
    # exists, and the pending ordinary gate stays exactly where it was.
    conn = ScriptedConnection()
    waiting_row = _budgeted_task_row(status="waiting_for_owner")
    waiting_row = (*waiting_row[:10], _PRIOR_GATE_ID, *waiting_row[11:])
    _wire_budgeted_task(
        conn,
        monkeypatch,
        status="waiting_for_owner",
        task_row=waiting_row,
        prior_gate_id=_PRIOR_GATE_ID,
        sessions=[_ready_session_row()],
        executions=[],
    )

    observation = _observe(conn, send_in_flight=True)

    assert observation.kind is SessionCostObservationKind.TASK_NOT_CURRENT
    assert observation.task_block is None
    assert observation.interrupt_dispatches == ()
    assert not _sql_of_kind(conn, lambda sql: "insert into openorc.task_blocks" in sql)
    assert not _sql_of_kind(conn, lambda sql: _GATE_INSERT(sql, None))
    assert not _sql_of_kind(conn, lambda sql: _TASK_BLOCK_UPDATE(sql, None))
    assert not _sql_of_kind(conn, lambda sql: "insert into openorc.task_interrupt_intents" in sql)
    # The checkpoint stands and the Owner approval gate is untouched.
    assert _count(conn, _sql_has("update openorc.task_agent_sessions")) == 1
    assert not _event_types(conn)
    assert _count(conn, _sql_has("update openorc.owner_gates")) == 0


def _assert_barrier_precedes_task_lock(conn: ScriptedConnection) -> None:
    """The Profile FOR KEY SHARE guard composes before the Task FOR UPDATE lock."""
    executed = [sql for sql, _ in conn.executed]
    barrier_index = next(
        index for index, sql in enumerate(executed) if "from openorc.profiles" in sql
    )
    task_lock_index = next(
        index
        for index, sql in enumerate(executed)
        if "from openorc.tasks where id" in sql and "for update" in sql
    )
    assert "for key share" in " ".join(executed[barrier_index].split()).lower()
    assert barrier_index < task_lock_index


def test_the_observation_path_barriers_before_the_task_lock(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Lock-order contract: the account-operational barrier (Profile FOR KEY
    # SHARE) is the first lock acquisition of the Owner-attributed
    # observation transaction, ahead of the Task FOR UPDATE lock — the
    # opposite order would deadlock against Profile-root account deletion.
    conn = ScriptedConnection()
    _wire_budgeted_task(conn, monkeypatch, known_total=Decimal("20.00"))

    observation = _observe(conn, cost=Decimal("20.50"))

    assert observation.kind is SessionCostObservationKind.CHECKPOINTED
    _assert_barrier_precedes_task_lock(conn)


def test_the_pre_dispatch_gate_barriers_before_the_task_lock(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    conn = ScriptedConnection()
    _wire_budgeted_task(conn, monkeypatch, known_total=Decimal("20.00"))

    decision = _gate(conn)

    assert decision.kind is BudgetGateDecisionKind.ALLOW
    _assert_barrier_precedes_task_lock(conn)


def test_the_establish_seam_barriers_before_the_task_lock(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # The shared blocking composition is also safe when driven directly
    # through its own short transaction (the #88 F3a pre-dispatch seam): it
    # resolves the accountable Owner from durable Workspace state and
    # composes the barrier before its own Task lock.
    conn = ScriptedConnection()
    _wire_budgeted_task(conn, monkeypatch, executions=[_open_execution()])

    result = task_abort.establish_owner_authorized_cost_limit_block(
        cast(DatabasePool, _pool(conn)), workspace_id=_WORKSPACE_ID, task_id=_TASK_ID
    )

    assert result.kind is task_abort.CostLimitBlockOutcomeKind.BLOCKED_WITH_INTERRUPTS
    _assert_barrier_precedes_task_lock(conn)
    intent_params = next(
        params
        for sql, params in conn.executed
        if "insert into openorc.task_interrupt_intents" in sql
    )
    assert intent_params is not None and intent_params[3] == _BLOCK_ID
    assert intent_params[6] == _OWNER_PROFILE_ID


def _gate(
    conn: ScriptedConnection,
) -> Any:
    return check_task_budget_before_dispatch(
        cast(DatabasePool, _pool(conn)), workspace_id=_WORKSPACE_ID, task_id=_TASK_ID
    )


def test_pre_dispatch_gate_allows_below_the_threshold(monkeypatch: pytest.MonkeyPatch) -> None:
    conn = ScriptedConnection()
    _wire_budgeted_task(conn, monkeypatch, known_total=Decimal("20.00"))

    decision = _gate(conn)

    assert decision.kind is BudgetGateDecisionKind.ALLOW
    assert decision.observed_known_total_usd == Decimal("20.00")
    assert decision.has_unobserved_sessions is True
    assert not _sql_of_kind(conn, lambda sql: "insert into openorc.task_blocks" in sql)


def test_pre_dispatch_gate_blocks_at_the_threshold_atomically(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    conn = ScriptedConnection()
    _wire_budgeted_task(conn, monkeypatch, executions=[_open_execution()])

    decision = _gate(conn)

    assert decision.kind is BudgetGateDecisionKind.BLOCKED
    assert decision.task_block is not None
    assert decision.task_block.reason is TaskBlockReason.COST_LIMIT
    assert decision.interrupt_dispatches != ()
    # The F3 composition: the guarded blocked transition, the pending
    # Level 3-selected COST_LIMIT gate (F8b, issue #287), and the two
    # OWNER audits.
    status_params = next(params for sql, params in conn.executed if _TASK_BLOCK_UPDATE(sql, None))
    assert status_params is not None and status_params[5] == _GATE_ID
    (gate_insert,) = [params for sql, params in conn.executed if _GATE_INSERT(sql, None)]
    assert gate_insert is not None and gate_insert[2] == "cost_limit"
    assert _event_types(conn) == ["task_blocked", "owner_gate_created"]


def test_pre_dispatch_gate_classifies_disabled_already_blocked_and_stale(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    disabled = ScriptedConnection()
    _wire_budgeted_task(
        disabled, monkeypatch, task_row=_task_row(status="implementing", state_token=uuid4())
    )
    assert _gate(disabled).kind is BudgetGateDecisionKind.BUDGET_DISABLED

    already = ScriptedConnection()
    _wire_budgeted_task(already, monkeypatch, task_row=_blocked_task_row())
    assert _gate(already).kind is BudgetGateDecisionKind.ALREADY_BLOCKED

    stale = ScriptedConnection()
    _wire_budgeted_task(stale, monkeypatch, task_row=_queued_budgeted_task_row())
    assert _gate(stale).kind is BudgetGateDecisionKind.NOT_CURRENT

    foreign = ScriptedConnection()
    _wire_budgeted_task(foreign, monkeypatch, no_task=True)
    assert _gate(foreign).kind is BudgetGateDecisionKind.NOT_CURRENT

    for conn in (disabled, already, stale, foreign):
        assert not _sql_of_kind(conn, lambda sql: "insert into openorc.task_blocks" in sql)


def test_malformed_observations_and_gates_fail_closed_before_any_state() -> None:
    pool = cast(DatabasePool, _pool(ScriptedConnection()))
    bad_ids = [
        dict(workspace_id="not-a-uuid"),
        dict(task_id=42),
        dict(task_agent_session_id=None),
        dict(connection_id="x"),
    ]
    for override in bad_ids:
        kwargs = dict(
            workspace_id=_WORKSPACE_ID,
            task_id=_TASK_ID,
            task_agent_session_id=_SESSION_ID,
            connection_id=_CONNECTION_ID,
            aggregate_total_cost=Decimal("1.00"),
        )
        kwargs.update(override)  # type: ignore[arg-type]
        with pytest.raises(InvalidCommandError):
            observe_session_usage(pool, **kwargs)  # type: ignore[arg-type]
    # A malformed amount (float, bool, negative, string) is rejected before
    # any state is touched — only exact decimals carry money here.
    for bad_amount in (0.5, True, Decimal("-1.00"), "1.00"):
        with pytest.raises(InvalidCommandError):
            observe_session_usage(
                pool,
                workspace_id=_WORKSPACE_ID,
                task_id=_TASK_ID,
                task_agent_session_id=_SESSION_ID,
                connection_id=_CONNECTION_ID,
                aggregate_total_cost=bad_amount,  # type: ignore[arg-type]
            )
    with pytest.raises(InvalidCommandError):
        check_task_budget_before_dispatch(
            pool,
            workspace_id="x",  # type: ignore[arg-type]
            task_id=_TASK_ID,
        )


# --- F8f: the effective cumulative threshold and the fail-closed race (issue #291) ---


def _refreshed_task_row(threshold: Decimal) -> tuple[Any, ...]:
    row = _budgeted_task_row()
    return (*row[:17], threshold)


def test_a_refreshed_threshold_is_the_enforced_allowance(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # After a Continue set the effective cumulative threshold to $22 (the
    # $10 frozen budget measured from the $12 observed spend), a $21.50
    # observation is BELOW the refreshed allowance even though it exceeds
    # the original budget: no block, truthful checkpoint.
    conn = ScriptedConnection()
    _wire_budgeted_task(
        conn,
        monkeypatch,
        task_row=_refreshed_task_row(Decimal("22.00")),
        known_total=Decimal("21.50"),
    )

    below = _observe(conn, cost=Decimal("21.50"), send_in_flight=True)

    assert below.kind is SessionCostObservationKind.CHECKPOINTED
    assert not _sql_of_kind(conn, lambda sql: "insert into openorc.task_blocks" in sql)

    # A $22.50 observation crosses the refreshed allowance mid-turn and
    # blocks: the fresh full Task-budget allowance repeats the rule.
    crossed_conn = ScriptedConnection()
    _wire_budgeted_task(
        crossed_conn,
        monkeypatch,
        task_row=_refreshed_task_row(Decimal("22.00")),
        known_total=Decimal("22.50"),
        executions=[_open_execution()],
    )

    crossed = _observe(crossed_conn, cost=Decimal("22.50"), send_in_flight=True)

    assert crossed.kind is SessionCostObservationKind.BLOCKED
    assert crossed.task_block is not None
    assert crossed.task_block.reason is TaskBlockReason.COST_LIMIT


def test_the_pre_dispatch_gate_honors_the_refreshed_threshold(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # The delivered-but-unwired pre-dispatch gate stays coherent with the
    # effective threshold: below the refreshed allowance it permits.
    conn = ScriptedConnection()
    _wire_budgeted_task(
        conn,
        monkeypatch,
        task_row=_refreshed_task_row(Decimal("22.00")),
        known_total=Decimal("21.50"),
    )

    decision = _gate(conn)

    assert decision.kind is BudgetGateDecisionKind.ALLOW


def test_an_attested_mid_turn_crossing_registers_an_unsettled_interrupt_intent(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # The representative turn-completion race shape: the mid-turn crossing
    # composes the block and registers its Interrupt intent in the INITIAL
    # unsettled state. Only the runtime's terminal interrupted-turn
    # classification settles it (the existing E10b machinery); if the turn
    # completes normally instead, the intent stays unsettled — which is what
    # keeps the Owner Continue safely refused at the existing settlement
    # check while the Owner resolves through the existing rules.
    conn = ScriptedConnection()
    _wire_budgeted_task(
        conn,
        monkeypatch,
        known_total=Decimal("26.00"),
        executions=[_open_execution()],
    )

    observation = _observe(conn, cost=Decimal("26.00"), send_in_flight=True)

    assert observation.kind is SessionCostObservationKind.BLOCKED
    assert observation.interrupt_dispatches != ()
    intent_inserts = [sql for sql, _ in conn.executed if _INTENT_INSERT(sql, None)]
    assert len(intent_inserts) == 1
    assert "'unsettled'" in intent_inserts[0]
