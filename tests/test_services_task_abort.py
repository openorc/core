"""Deterministic tests for the E10b Owner Abort command (issue #228).

Scripted SQL-connection seams and a fake Cline adapter prove the Owner
activity-interruption command on its critical paths:

- the dispatch phase proves the account/Owner/currentness/role guards before
  any Cline I/O, then applies ONE atomic composition — the
  ``owner_action_required`` TaskBlock with its safe ``owner_abort`` context,
  the ``blocked`` transition under the rotated token, the OWNER
  ``task_blocked`` audit, and the one-shot exact-session Interrupt intents —
  and calls ``abort_activity`` exactly once per exact target, strictly after
  the commit, never touching the gate or the session bindings;
- the acknowledged-but-unsettled, definitively failed, and uncertain
  dispatch outcomes record their safe facts on the durable intent (an SDK
  acknowledgement is never quiescence);
- the settlement phase of an already-blocked Task settles an outstanding
  interruption only under positive caller-attested D6b evidence — never
  re-interrupting — and refuses without evidence, surfacing the unresolved
  interruption; a blocked Task with no outstanding interruption is
  inapplicable;
- autonomous-phase re-issue while an interruption is outstanding, stale
  tokens, non-interruptible statuses, lost/ended session bindings, and
  malformed commands apply nothing.

No live runtime or Postgres access.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any, cast
from uuid import uuid4

import pytest

from openorc.adapters.agent_runtime import (
    AgentRuntimeError,
    AgentRuntimeUncertainOutcomeError,
)
from openorc.adapters.cline import ClineSettlementOutcome, ClineSettlementRefusal
from openorc.domain.connections import WorkflowRole
from openorc.domain.intents import TaskInterruptIntentStatus
from openorc.domain.tasks import TaskStatus
from openorc.persistence.pool import DatabasePool
from openorc.services.errors import (
    ConflictError,
    InvalidCommandError,
    StaleOperationError,
)
from openorc.services.task_abort import (
    TaskAbortOutcomeKind,
    TaskInterruptSettlement,
    abort_task_activity,
)
from tests.test_services_task_pr_review_decision import (
    _EVENT_INSERT,
    _event_returned_row,
    _event_types,
)
from tests.test_services_task_pr_review_dispatch import (
    _ACCOUNT_BARRIER,
    _ACCOUNT_OPERATIONAL_ROW,
    _OWNER_PROFILE_ID,
    _SESSION_READ,
    _STATE_TOKEN,
    _TASK_ID,
    _TASK_READ,
    _TASK_READ_LOCKED,
    _WORKSPACE_ID,
    _WORKSPACE_READ,
    ScriptedConnection,
    _count,
    _pool,
    _session_row,
    _sql_has,
    _task_row,
    _ws_row,
)

_OBSERVED = datetime(2026, 10, 23, 12, 0, 0, tzinfo=UTC)
_BLOCKED_TOKEN = uuid4()
_SESSION_ID = uuid4()
_EXTERNAL_SESSION_ID = "fake-session-1"
_INTENT_ID = uuid4()

_INTENTS_LIST = _sql_has("from openorc.task_interrupt_intents")
_INTENT_INSERT = _sql_has("insert into openorc.task_interrupt_intents")
_INTENT_UPDATE = _sql_has("update openorc.task_interrupt_intents")
_SESSION_WRITE = _sql_has("update openorc.task_agent_sessions")
_BLOCK_INSERT = _sql_has("insert into openorc.task_blocks")
_STATUS_UPDATE = _sql_has("update openorc.tasks", "queued_at = case")


def _intent_row(
    *,
    intent_id: Any = _INTENT_ID,
    role: str = "producer",
    external_id: str = _EXTERNAL_SESSION_ID,
    status: str = "unsettled",
    outcome: dict[str, Any] | None = None,
) -> tuple[Any, ...]:
    return (
        intent_id,
        _WORKSPACE_ID,
        _TASK_ID,
        _SESSION_ID,
        role,
        external_id,
        _OWNER_PROFILE_ID,
        status,
        outcome if outcome is not None else {},
        _OBSERVED,
        _OBSERVED,
    )


def _block_row() -> tuple[Any, ...]:
    return (
        uuid4(),
        _WORKSPACE_ID,
        _TASK_ID,
        "owner_action_required",
        {
            "control": "owner_abort",
            "target_roles": ["producer"],
            "task_agent_session_ids": [str(_SESSION_ID)],
        },
        None,
        _OBSERVED,
    )


def _ready_session_row(*, lifecycle: str = "ready") -> tuple[Any, ...]:
    row = _session_row("producer", _SESSION_ID, _EXTERNAL_SESSION_ID, lifecycle=lifecycle)
    if lifecycle == "ended":
        # ENDED requires its semantic stamp set exactly when the status is.
        row = (*row[:12], _OBSERVED, row[13], row[14])
    return row


def _task_row_of(status: str) -> tuple[Any, ...]:
    row = _task_row(status=status, state_token=_STATE_TOKEN)
    if status == "queued":
        # A queued Task carries its durable queue-entry timestamp.
        row = (*row[:12], _OBSERVED, row[13], row[14])
    return row


class _FakeClineAdapter:
    """The adapter seam recording the exact dispatch/settlement calls."""

    def __init__(
        self,
        *,
        abort_results: tuple[Any, ...] = (),
        settle_results: tuple[Any, ...] = (),
    ) -> None:
        self.abort_calls: list[str] = []
        self.settle_calls: list[tuple[str, bool, bool]] = []
        self._abort_results = list(abort_results)
        self._settle_results = list(settle_results)

    def abort_activity(self, session_id: str) -> None:
        self.abort_calls.append(session_id)
        if not self._abort_results:
            return
        result = self._abort_results.pop(0)
        if isinstance(result, Exception):
            raise result

    def settle_abort(
        self, session_id: str, *, runtime_work_settled: bool, operation_accounted_for: bool
    ) -> Any:
        self.settle_calls.append((session_id, runtime_work_settled, operation_accounted_for))
        if not self._settle_results:
            return ClineSettlementOutcome(settled=True)
        result = self._settle_results.pop(0)
        if isinstance(result, Exception):
            raise result
        return result


def _script_dispatch_head(
    conn: ScriptedConnection,
    *,
    status: str = "planning",
    session: tuple[Any, ...] | None = None,
) -> None:
    conn.on_repeat(_ACCOUNT_BARRIER, _ACCOUNT_OPERATIONAL_ROW)
    conn.on_repeat(_WORKSPACE_READ, _ws_row())
    conn.on(_TASK_READ, _task_row(status=status, state_token=_STATE_TOKEN))
    conn.on(_TASK_READ_LOCKED, _task_row(status=status, state_token=_STATE_TOKEN))
    conn.on(_TASK_READ, _task_row(status=status, state_token=_STATE_TOKEN))
    if session is not None:
        conn.on(_SESSION_READ, session)


def _script_dispatch_head_row(conn: ScriptedConnection, *, status: str) -> None:
    row = _task_row_of(status)
    conn.on_repeat(_ACCOUNT_BARRIER, _ACCOUNT_OPERATIONAL_ROW)
    conn.on_repeat(_WORKSPACE_READ, _ws_row())
    conn.on(_TASK_READ, row)
    conn.on(_TASK_READ_LOCKED, row)
    conn.on(_TASK_READ, row)


def _dispatch_call(
    pool: DatabasePool,
    adapter: Any,
    *,
    target_roles: tuple[Any, ...] = (WorkflowRole.PRODUCER,),
    expected_state_token: Any = _STATE_TOKEN,
) -> Any:
    return abort_task_activity(
        pool,
        cast(Any, adapter),
        profile_id=_OWNER_PROFILE_ID,
        workspace_id=_WORKSPACE_ID,
        task_id=_TASK_ID,
        expected_state_token=expected_state_token,
        target_roles=target_roles,
    )


def _settlement_call(
    pool: DatabasePool,
    adapter: Any,
    *,
    settlement: TaskInterruptSettlement | None = None,
    expected_state_token: Any = _BLOCKED_TOKEN,
) -> Any:
    return abort_task_activity(
        pool,
        cast(Any, adapter),
        profile_id=_OWNER_PROFILE_ID,
        workspace_id=_WORKSPACE_ID,
        task_id=_TASK_ID,
        expected_state_token=expected_state_token,
        target_roles=(),
        settlement=settlement,
    )


def test_abort_blocks_the_task_and_interrupts_the_exact_session_after_commit() -> None:
    conn = ScriptedConnection()
    _script_dispatch_head(conn, status="planning", session=_ready_session_row())
    conn.on(_INTENTS_LIST, [])
    conn.on(_BLOCK_INSERT, _block_row())
    conn.on(_STATUS_UPDATE, _task_row(status="blocked", state_token=_BLOCKED_TOKEN))
    conn.on(_EVENT_INSERT, _event_returned_row("task_blocked"))
    conn.on(_INTENT_INSERT, _intent_row())
    conn.on(_INTENT_UPDATE, _intent_row(outcome={"dispatch": "acknowledged"}))
    adapter = _FakeClineAdapter()

    outcome = _dispatch_call(_pool(conn), adapter)

    assert outcome.kind is TaskAbortOutcomeKind.INTERRUPTED
    assert outcome.task.status is TaskStatus.BLOCKED
    assert outcome.task_block is not None
    assert outcome.task_block.reason.value == "owner_action_required"
    assert outcome.task_block.context["control"] == "owner_abort"
    # The runtime was called exactly once, after the commit, for the exact
    # bound external session identity.
    assert adapter.abort_calls == [_EXTERNAL_SESSION_ID]
    assert adapter.settle_calls == []
    # The acknowledged dispatch records its fact in place; the intent stays
    # unsettled (an SDK acknowledgement is never quiescence).
    assert outcome.interrupt_outcomes[0].detail == "acknowledged"
    assert outcome.interrupt_outcomes[0].status is TaskInterruptIntentStatus.UNSETTLED
    assert _event_types(conn) == ["task_blocked"]
    # The session bindings were never rewritten.
    assert _count(conn, _SESSION_WRITE) == 0


def test_abort_at_a_pending_gate_never_touches_the_gate() -> None:
    conn = ScriptedConnection()
    _script_dispatch_head(conn, status="waiting_for_owner", session=_ready_session_row())
    conn.on(_INTENTS_LIST, [])
    conn.on(_BLOCK_INSERT, _block_row())
    conn.on(_STATUS_UPDATE, _task_row(status="blocked", state_token=_BLOCKED_TOKEN))
    conn.on(_EVENT_INSERT, _event_returned_row("task_blocked"))
    conn.on(_INTENT_INSERT, _intent_row())
    conn.on(_INTENT_UPDATE, _intent_row(outcome={"dispatch": "acknowledged"}))
    adapter = _FakeClineAdapter()

    outcome = _dispatch_call(_pool(conn), adapter)

    assert outcome.kind is TaskAbortOutcomeKind.INTERRUPTED
    gate_update = _sql_has("update openorc.owner_gates")
    assert _count(conn, gate_update) == 0
    assert _event_types(conn) == ["task_blocked"]


def test_abort_records_a_definitive_dispatch_failure_on_the_intent() -> None:
    conn = ScriptedConnection()
    _script_dispatch_head(conn, status="planning", session=_ready_session_row())
    conn.on(_INTENTS_LIST, [])
    conn.on(_BLOCK_INSERT, _block_row())
    conn.on(_STATUS_UPDATE, _task_row(status="blocked", state_token=_BLOCKED_TOKEN))
    conn.on(_EVENT_INSERT, _event_returned_row("task_blocked"))
    conn.on(_INTENT_INSERT, _intent_row())
    conn.on(_INTENT_UPDATE, _intent_row(status="failed", outcome={"dispatch": "failed"}))
    adapter = _FakeClineAdapter(abort_results=(AgentRuntimeError("refused"),))

    outcome = _dispatch_call(_pool(conn), adapter)

    assert outcome.kind is TaskAbortOutcomeKind.INTERRUPTED
    assert outcome.interrupt_outcomes[0].detail == "failed"
    assert outcome.interrupt_outcomes[0].status is TaskInterruptIntentStatus.FAILED
    assert outcome.task.status is TaskStatus.BLOCKED


def test_abort_records_an_uncertain_dispatch_on_the_intent() -> None:
    conn = ScriptedConnection()
    _script_dispatch_head(conn, status="planning", session=_ready_session_row())
    conn.on(_INTENTS_LIST, [])
    conn.on(_BLOCK_INSERT, _block_row())
    conn.on(_STATUS_UPDATE, _task_row(status="blocked", state_token=_BLOCKED_TOKEN))
    conn.on(_EVENT_INSERT, _event_returned_row("task_blocked"))
    conn.on(_INTENT_INSERT, _intent_row())
    conn.on(_INTENT_UPDATE, _intent_row(status="unknown", outcome={"dispatch": "uncertain"}))
    adapter = _FakeClineAdapter(
        abort_results=(AgentRuntimeUncertainOutcomeError("lost"),),
    )

    outcome = _dispatch_call(_pool(conn), adapter)

    assert outcome.interrupt_outcomes[0].detail == "uncertain"
    assert outcome.interrupt_outcomes[0].status is TaskInterruptIntentStatus.UNKNOWN


def test_abort_dispatches_each_exact_admitted_target_once() -> None:
    reviewer_session = _session_row("reviewer", uuid4(), "fake-session-2")
    conn = ScriptedConnection()
    _script_dispatch_head(conn, status="reviewing", session=_ready_session_row())
    conn.on(_SESSION_READ, reviewer_session)
    conn.on(_INTENTS_LIST, [])
    conn.on(_BLOCK_INSERT, _block_row())
    conn.on(_STATUS_UPDATE, _task_row(status="blocked", state_token=_BLOCKED_TOKEN))
    conn.on(_EVENT_INSERT, _event_returned_row("task_blocked"))
    conn.on(_INTENT_INSERT, _intent_row())
    conn.on(
        _INTENT_INSERT,
        _intent_row(role="reviewer", external_id="fake-session-2"),
    )
    conn.on(_INTENT_UPDATE, _intent_row(outcome={"dispatch": "acknowledged"}))
    conn.on(
        _INTENT_UPDATE,
        _intent_row(
            role="reviewer", external_id="fake-session-2", outcome={"dispatch": "acknowledged"}
        ),
    )
    adapter = _FakeClineAdapter()

    outcome = _dispatch_call(
        _pool(conn), adapter, target_roles=(WorkflowRole.PRODUCER, WorkflowRole.REVIEWER)
    )

    assert adapter.abort_calls == [_EXTERNAL_SESSION_ID, "fake-session-2"]
    assert len(outcome.interrupt_outcomes) == 2


def _script_settlement_head(conn: ScriptedConnection, *, intents: list[tuple[Any, ...]]) -> None:
    conn.on_repeat(_ACCOUNT_BARRIER, _ACCOUNT_OPERATIONAL_ROW)
    conn.on_repeat(_WORKSPACE_READ, _ws_row())
    conn.on(_TASK_READ, _task_row(status="blocked", state_token=_BLOCKED_TOKEN))
    conn.on(_TASK_READ_LOCKED, _task_row(status="blocked", state_token=_BLOCKED_TOKEN))
    conn.on(_TASK_READ, _task_row(status="blocked", state_token=_BLOCKED_TOKEN))
    conn.on(_INTENTS_LIST, intents)


def test_a_second_abort_settles_under_positive_evidence_without_reinterrupting() -> None:
    conn = ScriptedConnection()
    _script_settlement_head(conn, intents=[_intent_row()])
    conn.on(_INTENT_UPDATE, _intent_row(status="settled", outcome={"settlement": "confirmed"}))
    adapter = _FakeClineAdapter()

    outcome = _settlement_call(
        _pool(conn),
        adapter,
        settlement=TaskInterruptSettlement(runtime_work_settled=True, operation_accounted_for=True),
    )

    assert outcome.kind is TaskAbortOutcomeKind.SETTLED
    # The duplicate Abort command never re-interrupts: no dispatch, and the
    # D6b settlement received both positive preconditions.
    assert adapter.abort_calls == []
    assert adapter.settle_calls == [(_EXTERNAL_SESSION_ID, True, True)]
    assert outcome.interrupt_outcomes[0].status is TaskInterruptIntentStatus.SETTLED
    assert _count(conn, _BLOCK_INSERT) == 0
    assert _count(conn, _STATUS_UPDATE) == 0
    assert _event_types(conn) == []


def test_a_settlement_refusal_keeps_the_interruption_unresolved() -> None:
    conn = ScriptedConnection()
    _script_settlement_head(conn, intents=[_intent_row()])
    conn.on(
        _INTENT_UPDATE,
        _intent_row(outcome={"settlement_refusal": "outstanding_abort"}),
    )
    adapter = _FakeClineAdapter(
        settle_results=(
            ClineSettlementOutcome(settled=False, refusal=ClineSettlementRefusal.OUTSTANDING_ABORT),
        )
    )

    outcome = _settlement_call(
        _pool(conn),
        adapter,
        settlement=TaskInterruptSettlement(runtime_work_settled=True, operation_accounted_for=True),
    )

    assert outcome.kind is TaskAbortOutcomeKind.UNRESOLVED
    assert outcome.interrupt_outcomes[0].status is TaskInterruptIntentStatus.UNSETTLED
    assert outcome.interrupt_outcomes[0].detail == "refused:outstanding_abort"
    # The intent stayed unsettled: the annotation wrote no terminal status.
    annotate_sql = [sql for sql, _ in conn.executed if "set outcome = %s" in sql]
    assert len(annotate_sql) == 1
    assert all("set status = %s" not in sql for sql in annotate_sql)


def test_an_abort_without_evidence_refuses_and_surfaces_the_unresolved_interruption() -> None:
    conn = ScriptedConnection()
    _script_settlement_head(conn, intents=[_intent_row()])
    adapter = _FakeClineAdapter()

    with pytest.raises(ConflictError, match="unresolved"):
        abort_task_activity(
            _pool(conn),
            cast(Any, adapter),
            profile_id=_OWNER_PROFILE_ID,
            workspace_id=_WORKSPACE_ID,
            task_id=_TASK_ID,
            expected_state_token=_BLOCKED_TOKEN,
            target_roles=(WorkflowRole.PRODUCER,),
            settlement=None,
        )

    assert adapter.abort_calls == []
    assert adapter.settle_calls == []
    assert _count(conn, _BLOCK_INSERT) == 0
    assert _count(conn, _STATUS_UPDATE) == 0


def test_a_blocked_task_with_no_outstanding_interruption_is_inapplicable() -> None:
    conn = ScriptedConnection()
    _script_settlement_head(conn, intents=[])
    adapter = _FakeClineAdapter()

    with pytest.raises(ConflictError, match="no owner activity interruption"):
        _settlement_call(
            _pool(conn),
            adapter,
            settlement=TaskInterruptSettlement(
                runtime_work_settled=True, operation_accounted_for=True
            ),
        )

    assert adapter.abort_calls == []
    assert _count(conn, _STATUS_UPDATE) == 0


def test_an_outstanding_interruption_refuses_a_second_dispatch() -> None:
    conn = ScriptedConnection()
    _script_dispatch_head(conn, status="planning", session=_ready_session_row())
    conn.on(_INTENTS_LIST, [_intent_row()])
    adapter = _FakeClineAdapter()

    with pytest.raises(StaleOperationError, match="outstanding"):
        _dispatch_call(_pool(conn), adapter)

    assert adapter.abort_calls == []
    assert _count(conn, _BLOCK_INSERT) == 0
    assert _count(conn, _STATUS_UPDATE) == 0
    assert _event_types(conn) == []


def test_noninterruptible_statuses_are_inapplicable() -> None:
    for status in ("ready_to_plan", "queued"):
        conn = ScriptedConnection()
        _script_dispatch_head_row(conn, status=status)
        adapter = _FakeClineAdapter()

        with pytest.raises(ConflictError):
            _dispatch_call(_pool(conn), adapter)

        assert adapter.abort_calls == []
        assert _count(conn, _BLOCK_INSERT) == 0


def test_a_stale_token_applies_nothing() -> None:
    conn = ScriptedConnection()
    _script_dispatch_head(conn, status="planning", session=_ready_session_row())
    adapter = _FakeClineAdapter()

    with pytest.raises(StaleOperationError):
        _dispatch_call(_pool(conn), adapter, expected_state_token=uuid4())

    assert adapter.abort_calls == []
    assert _count(conn, _BLOCK_INSERT) == 0
    assert _count(conn, _STATUS_UPDATE) == 0
    assert _event_types(conn) == []


def test_a_lost_or_ended_session_binding_cannot_be_interrupted() -> None:
    for lifecycle in ("lost", "ended"):
        conn = ScriptedConnection()
        _script_dispatch_head(
            conn,
            status="planning",
            session=_ready_session_row(lifecycle=lifecycle),
        )
        adapter = _FakeClineAdapter()

        with pytest.raises(ConflictError, match="ready external session"):
            _dispatch_call(_pool(conn), adapter)

        assert adapter.abort_calls == []
        assert _count(conn, _BLOCK_INSERT) == 0


def test_malformed_abort_commands_fail_before_any_state_is_touched() -> None:
    conn = ScriptedConnection()
    adapter = _FakeClineAdapter()
    pool = _pool(conn)

    with pytest.raises(InvalidCommandError):
        abort_task_activity(
            pool,
            cast(Any, adapter),
            profile_id="not-a-uuid",  # type: ignore[arg-type]
            workspace_id=_WORKSPACE_ID,
            task_id=_TASK_ID,
            expected_state_token=_STATE_TOKEN,
            target_roles=(WorkflowRole.PRODUCER,),
        )
    with pytest.raises(InvalidCommandError):
        abort_task_activity(
            pool,
            cast(Any, adapter),
            profile_id=_OWNER_PROFILE_ID,
            workspace_id=_WORKSPACE_ID,
            task_id=_TASK_ID,
            expected_state_token=_STATE_TOKEN,
            target_roles=(WorkflowRole.PRODUCER,),
            settlement=TaskInterruptSettlement(  # type: ignore[arg-type]
                runtime_work_settled=False, operation_accounted_for=True
            ),
        )
    with pytest.raises(InvalidCommandError):
        _dispatch_call(pool, adapter, target_roles=())

    assert adapter.abort_calls == []
    assert conn.executed == []
