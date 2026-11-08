"""Deterministic tests for the E10b Owner Abort command (issue #228).

Scripted SQL-connection seams and a fake Cline adapter prove the Owner
activity-interruption command on its critical paths:

- the dispatch phase proves the account/Owner/currentness/role guards before
  any Cline I/O, then applies ONE atomic composition — the F3 exceptional
  blocking foundation: the ``owner_action_required`` TaskBlock with its safe
  ``owner_abort`` context, the pending ``OWNER_ABORT`` OwnerGate the Level 3
  catalog selects (F8b, issue #287) bound to it, the guarded current-gate
  pointer installation with the ``blocked``
  transition under the rotated token (a superseded pending ordinary gate
  stays pending history, never rewritten), the OWNER ``task_blocked`` /
  ``owner_gate_created`` audits, and the one-shot exact-session Interrupt
  intents — and calls ``abort_activity`` exactly once per exact target,
  strictly after the commit, never touching the session bindings;
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
from types import SimpleNamespace
from typing import Any, cast
from uuid import uuid4

import pytest

from openorc.adapters.agent_runtime import (
    AgentRuntimeError,
    AgentRuntimeUncertainOutcomeError,
)
from openorc.adapters.cline import ClineSettlementOutcome, ClineSettlementRefusal
from openorc.domain.connections import WorkflowRole
from openorc.domain.executions import ExecutionStatus
from openorc.domain.intents import TaskInterruptIntentStatus
from openorc.domain.tasks import TaskStatus
from openorc.persistence import executions as execution_records
from openorc.persistence import intents as intent_records
from openorc.persistence import reviews as review_records
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
    settle_observed_task_interrupt,
)
from tests.test_services_task_pr_review_decision import (
    _EVENT_INSERT,
    _event_returned_row,
    _event_types,
    _gate_row,
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
_GATE_ID = uuid4()
_PRIOR_GATE_ID = uuid4()
_BLOCK_ID = uuid4()

_INTENTS_LIST = _sql_has("from openorc.task_interrupt_intents")
_INTENT_INSERT = _sql_has("insert into openorc.task_interrupt_intents")
_INTENT_UPDATE = _sql_has("update openorc.task_interrupt_intents")
_SESSION_WRITE = _sql_has("update openorc.task_agent_sessions")
_BLOCK_INSERT = _sql_has("insert into openorc.task_blocks")
_TASK_BLOCK_UPDATE = _sql_has("update openorc.tasks", "status = 'blocked'")
_GATE_READ = _sql_has("from openorc.owner_gates where id")
_GATE_INSERT = _sql_has("insert into openorc.owner_gates")
_GATE_UPDATE = _sql_has("update openorc.owner_gates")


def _intent_row(
    *,
    intent_id: Any = _INTENT_ID,
    role: str = "producer",
    external_id: str = _EXTERNAL_SESSION_ID,
    status: str = "unsettled",
    outcome: dict[str, Any] | None = None,
    task_block_id: Any = _BLOCK_ID,
) -> tuple[Any, ...]:
    return (
        intent_id,
        _WORKSPACE_ID,
        _TASK_ID,
        _SESSION_ID,
        task_block_id,
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
        _BLOCK_ID,
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
        row = (*row[:12], _OBSERVED, *row[13:])
    return row


def _wire_activity(
    monkeypatch: pytest.MonkeyPatch,
    *,
    executions: list[Any] | None = None,
    loops: list[Any] | None = None,
    iterations_by_loop: dict[Any, list[Any]] | None = None,
) -> None:
    """Patch the durable in-flight activity readers at their module seams.

    The activity proof reads the Producer's open Executions and the Task's
    OPEN ReviewLoops with unfinalized ReviewIterations. Settlement never
    reads or finalizes them (issue #298): a positively settled interruption
    leaves an open Execution truthfully open.
    """
    monkeypatch.setattr(
        execution_records,
        "list_task_executions",
        lambda pool, *, task_id: list(executions or []),
    )
    monkeypatch.setattr(
        review_records,
        "list_task_review_loops",
        lambda pool, *, task_id, status: list(loops or []),
    )
    monkeypatch.setattr(
        review_records,
        "list_review_loop_iterations",
        lambda pool, *, review_loop_id: list((iterations_by_loop or {}).get(review_loop_id, [])),
    )


def _open_execution(
    *,
    execution_id: Any = None,
    producer_session_id: Any = _SESSION_ID,
    status: ExecutionStatus = ExecutionStatus.RUNNING,
) -> Any:
    return SimpleNamespace(
        id=execution_id if execution_id is not None else uuid4(),
        workspace_id=_WORKSPACE_ID,
        task_id=_TASK_ID,
        producer_session_id=producer_session_id,
        execution_number=1,
        status=status,
        created_at=_OBSERVED,
        updated_at=_OBSERVED,
        implementation_result=None,
        last_qualifying_activity_at=None,
        review_iteration_id=None,
    )


def _open_loop_with_unfinalized_iteration(
    *,
    loop_id: Any = None,
) -> tuple[Any, dict[Any, list[Any]]]:
    loop = SimpleNamespace(
        id=loop_id if loop_id is not None else uuid4(),
        workspace_id=_WORKSPACE_ID,
        task_id=_TASK_ID,
        purpose="planning",
        iteration_limit=2,
        status="open",
        closed_at=None,
        created_at=_OBSERVED,
    )
    iteration = SimpleNamespace(
        id=uuid4(),
        workspace_id=_WORKSPACE_ID,
        task_id=_TASK_ID,
        review_loop_id=loop.id,
        iteration_number=1,
        plan_revision_id=uuid4(),
        task_pull_request_id=None,
        reviewed_head_sha=None,
        outcome=None,
        summary=None,
        findings=[],
        decided_at=None,
        created_at=_OBSERVED,
    )
    return loop, {loop.id: [iteration]}


def _task_row_of(status: str) -> tuple[Any, ...]:
    row = _task_row(status=status, state_token=_STATE_TOKEN)
    if status == "queued":
        # A queued Task carries its durable queue-entry timestamp.
        row = (*row[:12], _OBSERVED, *row[13:])
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
    current_gate_id: Any | None = None,
) -> None:
    conn.on_repeat(_ACCOUNT_BARRIER, _ACCOUNT_OPERATIONAL_ROW)
    conn.on_repeat(_WORKSPACE_READ, _ws_row())
    observed_row = _task_row(
        status=status, state_token=_STATE_TOKEN, current_gate_id=current_gate_id
    )
    conn.on(_TASK_READ, observed_row)
    conn.on(_TASK_READ_LOCKED, observed_row)
    conn.on(_TASK_READ, observed_row)
    if session is not None:
        conn.on(_SESSION_READ, session)


def _script_dispatch_head_row(conn: ScriptedConnection, *, status: str) -> None:
    row = _task_row_of(status)
    conn.on_repeat(_ACCOUNT_BARRIER, _ACCOUNT_OPERATIONAL_ROW)
    conn.on_repeat(_WORKSPACE_READ, _ws_row())
    conn.on(_TASK_READ, row)
    conn.on(_TASK_READ_LOCKED, row)
    conn.on(_TASK_READ, row)


def _exceptional_gate_row() -> tuple[Any, ...]:
    row = list(_gate_row(gate_id=_GATE_ID, gate_type="owner_abort", head_sha=None, pr_id=None))
    row[8] = _BLOCK_ID  # the gate's TaskBlock subject
    return tuple(row)


def _blocked_task_row() -> tuple[Any, ...]:
    # The post-installation Task: blocked, rotated token, the exceptional
    # gate installed as the current pointer.
    return _task_row(status="blocked", state_token=_BLOCKED_TOKEN, current_gate_id=_GATE_ID)


def _script_block_composition(
    conn: ScriptedConnection,
    *,
    status: str,
    prior_gate_id: Any | None = None,
) -> None:
    """Script the F3 exceptional blocking composition after the dispatch head.

    The installer takes its own locked Task read and re-reads the current
    row for classification, proves the caller-observed prior current gate
    when one exists, then inserts the TaskBlock, the pending
    ``OWNER_ABORT`` OwnerGate bound to it, the guarded blocked
    transition, and the two truthful audit events.
    """
    observed_row = _task_row(status=status, state_token=_STATE_TOKEN, current_gate_id=prior_gate_id)
    conn.on(_TASK_READ_LOCKED, observed_row)
    conn.on(_TASK_READ, observed_row)
    if prior_gate_id is not None:
        conn.on(
            _GATE_READ,
            _gate_row(gate_id=prior_gate_id, gate_type="pr_authorization", pr_id=None),
        )
    conn.on(_BLOCK_INSERT, _block_row())
    conn.on(_GATE_INSERT, _exceptional_gate_row())
    conn.on(_TASK_BLOCK_UPDATE, _blocked_task_row())
    conn.on(_EVENT_INSERT, _event_returned_row("task_blocked"))
    conn.on(_EVENT_INSERT, _event_returned_row("owner_gate_created"))


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


def test_abort_blocks_the_task_and_interrupts_the_exact_session_after_commit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    conn = ScriptedConnection()
    _script_dispatch_head(conn, status="implementing", session=_ready_session_row())
    _wire_activity(monkeypatch, executions=[_open_execution()])
    conn.on(_INTENTS_LIST, [])
    _script_block_composition(conn, status="implementing")
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
    # The F3 composition committed both truthful audits and installed the
    # pending OWNER_ABORT gate the Level 3 catalog selects (F8b, issue
    # #287) bound to the block.
    assert _event_types(conn) == ["task_blocked", "owner_gate_created"]
    (gate_insert,) = [params for sql, params in conn.executed if _GATE_INSERT(sql, None)]
    assert gate_insert is not None and gate_insert[2] == "owner_abort"
    assert outcome.task.current_owner_gate_id == _GATE_ID
    # The session bindings were never rewritten.
    assert _count(conn, _SESSION_WRITE) == 0


def test_abort_at_a_pending_gate_supersedes_it_with_the_block_resolution_gate(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # An admitted in-flight interaction at a pending ordinary gate aborts
    # through the F3 composition: the new pending OWNER_ABORT gate
    # becomes the Task's current gate while the superseded ordinary gate
    # row is never rewritten — it stays truthful pending history with no
    # manufactured owner_gate_resolved event.
    conn = ScriptedConnection()
    _script_dispatch_head(
        conn,
        status="waiting_for_owner",
        session=_ready_session_row(),
        current_gate_id=_PRIOR_GATE_ID,
    )
    _wire_activity(
        monkeypatch, executions=[_open_execution(status=ExecutionStatus.PAUSED_FOR_APPROVAL)]
    )
    conn.on(_INTENTS_LIST, [])
    _script_block_composition(conn, status="waiting_for_owner", prior_gate_id=_PRIOR_GATE_ID)
    conn.on(_INTENT_INSERT, _intent_row())
    conn.on(_INTENT_UPDATE, _intent_row(outcome={"dispatch": "acknowledged"}))
    adapter = _FakeClineAdapter()

    outcome = _dispatch_call(_pool(conn), adapter)

    assert outcome.kind is TaskAbortOutcomeKind.INTERRUPTED
    assert outcome.task.current_owner_gate_id == _GATE_ID
    assert _count(conn, _GATE_UPDATE) == 0
    assert _event_types(conn) == ["task_blocked", "owner_gate_created"]
    update_sql, update_params = next(
        (sql, params) for sql, params in conn.executed if _TASK_BLOCK_UPDATE(sql, None)
    )
    assert update_params == (
        _GATE_ID,
        _TASK_ID,
        _STATE_TOKEN,
        "waiting_for_owner",
        _PRIOR_GATE_ID,
        _GATE_ID,
    )


def test_abort_records_a_definitive_dispatch_failure_on_the_intent(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    conn = ScriptedConnection()
    _script_dispatch_head(conn, status="implementing", session=_ready_session_row())
    _wire_activity(monkeypatch, executions=[_open_execution()])
    conn.on(_INTENTS_LIST, [])
    _script_block_composition(conn, status="implementing")
    conn.on(_INTENT_INSERT, _intent_row())
    conn.on(_INTENT_UPDATE, _intent_row(status="failed", outcome={"dispatch": "failed"}))
    adapter = _FakeClineAdapter(abort_results=(AgentRuntimeError("refused"),))

    outcome = _dispatch_call(_pool(conn), adapter)

    assert outcome.kind is TaskAbortOutcomeKind.INTERRUPTED
    assert outcome.interrupt_outcomes[0].detail == "failed"
    assert outcome.interrupt_outcomes[0].status is TaskInterruptIntentStatus.FAILED
    assert outcome.task.status is TaskStatus.BLOCKED


def test_abort_records_an_uncertain_dispatch_on_the_intent(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    conn = ScriptedConnection()
    _script_dispatch_head(conn, status="implementing", session=_ready_session_row())
    _wire_activity(monkeypatch, executions=[_open_execution()])
    conn.on(_INTENTS_LIST, [])
    _script_block_composition(conn, status="implementing")
    conn.on(_INTENT_INSERT, _intent_row())
    conn.on(_INTENT_UPDATE, _intent_row(status="unknown", outcome={"dispatch": "uncertain"}))
    adapter = _FakeClineAdapter(
        abort_results=(AgentRuntimeUncertainOutcomeError("lost"),),
    )

    outcome = _dispatch_call(_pool(conn), adapter)

    assert outcome.interrupt_outcomes[0].detail == "uncertain"
    assert outcome.interrupt_outcomes[0].status is TaskInterruptIntentStatus.UNKNOWN


def test_abort_dispatches_each_exact_admitted_target_once(monkeypatch: pytest.MonkeyPatch) -> None:
    reviewer_session = _session_row("reviewer", uuid4(), "fake-session-2")
    conn = ScriptedConnection()
    _script_dispatch_head(conn, status="reviewing", session=_ready_session_row())
    loop, iterations_by_loop = _open_loop_with_unfinalized_iteration()
    _wire_activity(
        monkeypatch,
        executions=[_open_execution()],
        loops=[loop],
        iterations_by_loop=iterations_by_loop,
    )
    conn.on(_SESSION_READ, reviewer_session)
    conn.on(_INTENTS_LIST, [])
    _script_block_composition(conn, status="reviewing")
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


def test_a_second_abort_settles_under_positive_evidence_without_reinterrupting(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    conn = ScriptedConnection()
    _script_settlement_head(conn, intents=[_intent_row()])
    conn.on(_INTENT_UPDATE, _intent_row(status="settled", outcome={"settlement": "confirmed"}))
    execution = _open_execution()
    finalizations: list[tuple[Any, Any, Any]] = []
    _wire_activity(monkeypatch, executions=[execution])

    def _no_finalization(
        pool: Any, *, execution_id: Any, expected_status: Any, next_status: Any
    ) -> Any:
        finalizations.append((execution_id, expected_status, next_status))
        return None

    monkeypatch.setattr(execution_records, "update_execution_status", _no_finalization)
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
    # Settlement records the Interrupt intent, never a formal workflow
    # outcome (issue #298): the open Execution keeps its truthful status and
    # no guarded finalization transition is even attempted.
    assert finalizations == []
    assert execution.status is ExecutionStatus.RUNNING
    assert _count(conn, _BLOCK_INSERT) == 0
    assert _count(conn, _TASK_BLOCK_UPDATE) == 0
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


def test_a_second_abort_settles_across_a_fresh_adapter_instance() -> None:
    """The verified cross-lease shape for the Owner settlement branch: the
    settling adapter instance holds no local abort latch, so its D6b
    settle_abort refuses NOTHING_TO_SETTLE — the owner-attested positive
    evidence stands and the durable intent still records SETTLED."""
    conn = ScriptedConnection()
    _script_settlement_head(conn, intents=[_intent_row()])
    conn.on(_INTENT_UPDATE, _intent_row(status="settled", outcome={"settlement": "confirmed"}))
    adapter = _FakeClineAdapter(
        settle_results=(
            ClineSettlementOutcome(settled=False, refusal=ClineSettlementRefusal.NOTHING_TO_SETTLE),
        )
    )

    outcome = _settlement_call(
        _pool(conn),
        adapter,
        settlement=TaskInterruptSettlement(runtime_work_settled=True, operation_accounted_for=True),
    )

    assert outcome.kind is TaskAbortOutcomeKind.SETTLED
    assert outcome.interrupt_outcomes[0].status is TaskInterruptIntentStatus.SETTLED
    assert adapter.settle_calls == [(_EXTERNAL_SESSION_ID, True, True)]


def test_the_observed_resolution_settles_after_the_acknowledged_dispatch() -> None:
    """Ordering A (the ordinary happy path): the dispatch acknowledgement is
    already recorded when the interrupted session's terminal interrupted-turn
    resolution arrives; the worker-side seam correlates it with the exact
    unsettled intent and records the durable SETTLED outcome through the
    cross-lease nothing-to-release shape."""
    conn = ScriptedConnection()
    _script_settlement_head(conn, intents=[_intent_row(outcome={"dispatch": "acknowledged"})])
    conn.on(_INTENT_UPDATE, _intent_row(outcome={"observed_resolution": "interrupted_turn"}))
    conn.on(_INTENT_UPDATE, _intent_row(status="settled", outcome={"settlement": "confirmed"}))
    adapter = _FakeClineAdapter(
        settle_results=(
            ClineSettlementOutcome(settled=False, refusal=ClineSettlementRefusal.NOTHING_TO_SETTLE),
        )
    )

    outcome = settle_observed_task_interrupt(
        _pool(conn), cast(Any, adapter), task_id=_TASK_ID, external_session_id=_EXTERNAL_SESSION_ID
    )

    assert outcome is not None
    assert outcome.status is TaskInterruptIntentStatus.SETTLED
    assert outcome.detail == "settled"
    # The D6b discipline received both derived preconditions on the exact
    # session; no dispatch and no new intent were involved.
    assert adapter.settle_calls == [(_EXTERNAL_SESSION_ID, True, True)]
    assert adapter.abort_calls == []
    # The settled record asserts the Interrupt intent alone: no blocking
    # composition, no gate, and no workflow event.
    assert _count(conn, _BLOCK_INSERT) == 0
    assert _event_types(conn) == []


def test_a_late_dispatch_annotation_cannot_undo_the_settlement() -> None:
    """Ordering B: the worker's settlement commits while the dispatch
    acknowledgement annotation is still pending. The annotation applies only
    while the intent is unsettled, so the late write is a rejected no-op and
    the durable SETTLED outcome stands."""
    conn = ScriptedConnection()
    _script_settlement_head(conn, intents=[_intent_row()])
    conn.on(_INTENT_UPDATE, _intent_row(outcome={"observed_resolution": "interrupted_turn"}))
    conn.on(_INTENT_UPDATE, _intent_row(status="settled", outcome={"settlement": "confirmed"}))
    adapter = _FakeClineAdapter()

    outcome = settle_observed_task_interrupt(
        _pool(conn), cast(Any, adapter), task_id=_TASK_ID, external_session_id=_EXTERNAL_SESSION_ID
    )
    assert outcome is not None
    assert outcome.status is TaskInterruptIntentStatus.SETTLED

    # The late acknowledgement annotation: one-shot guarded — a raced
    # terminal outcome is a rejected no-op, never a demotion.
    conn.on(_INTENT_UPDATE, None)
    annotated = intent_records.annotate_unsettled_task_interrupt_intent(
        _pool(conn),
        task_interrupt_intent_id=_INTENT_ID,
        outcome={"dispatch": "acknowledged"},
    )
    assert annotated is None


def test_the_observed_resolution_settles_nothing_without_an_outstanding_intent() -> None:
    """Fail closed: an interrupted-turn resolution on a session with no
    unsettled Interrupt intent (no OpenOrc abort outstanding, or an already
    terminal intent) correlates nothing, settles nothing, and never touches
    the adapter."""
    conn = ScriptedConnection()
    conn.on(_INTENTS_LIST, [])
    adapter = _FakeClineAdapter()

    outcome = settle_observed_task_interrupt(
        _pool(conn), cast(Any, adapter), task_id=_TASK_ID, external_session_id=_EXTERNAL_SESSION_ID
    )

    assert outcome is None
    assert adapter.settle_calls == []
    assert adapter.abort_calls == []


def test_an_adapter_veto_keeps_the_observed_interruption_unresolved() -> None:
    """A genuine same-instance veto (an outstanding abort on the settling
    instance) refuses the settlement: the observation is recorded, and the
    intent stays unsettled with its refusal annotation — never settled over
    a live interruption."""
    conn = ScriptedConnection()
    _script_settlement_head(conn, intents=[_intent_row(outcome={"dispatch": "acknowledged"})])
    conn.on(_INTENT_UPDATE, _intent_row(outcome={"observed_resolution": "interrupted_turn"}))
    conn.on(_INTENT_UPDATE, _intent_row(outcome={"settlement_refusal": "outstanding_abort"}))
    adapter = _FakeClineAdapter(
        settle_results=(
            ClineSettlementOutcome(settled=False, refusal=ClineSettlementRefusal.OUTSTANDING_ABORT),
        )
    )

    outcome = settle_observed_task_interrupt(
        _pool(conn), cast(Any, adapter), task_id=_TASK_ID, external_session_id=_EXTERNAL_SESSION_ID
    )

    assert outcome is not None
    assert outcome.status is TaskInterruptIntentStatus.UNSETTLED
    assert outcome.detail == "refused:outstanding_abort"
    annotate_sql = [sql for sql, _ in conn.executed if "set outcome = %s" in sql]
    assert len(annotate_sql) == 2  # the observation, then the refusal annotation
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
    assert _count(conn, _TASK_BLOCK_UPDATE) == 0


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
    assert _count(conn, _TASK_BLOCK_UPDATE) == 0


def test_an_outstanding_interruption_refuses_a_second_dispatch() -> None:
    conn = ScriptedConnection()
    _script_dispatch_head(conn, status="planning", session=_ready_session_row())
    conn.on(_INTENTS_LIST, [_intent_row()])
    adapter = _FakeClineAdapter()

    with pytest.raises(StaleOperationError, match="outstanding"):
        _dispatch_call(_pool(conn), adapter)

    assert adapter.abort_calls == []
    assert _count(conn, _BLOCK_INSERT) == 0
    assert _count(conn, _TASK_BLOCK_UPDATE) == 0
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
    assert _count(conn, _TASK_BLOCK_UPDATE) == 0
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


def test_abort_eligibility_covers_the_autonomous_planning_phase(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # The autonomous phases are eligible for Owner Abort against their
    # appropriate existing bound runtime session without further durable
    # activity proof; only a WAITING_FOR_OWNER gate must demonstrate it.
    conn = ScriptedConnection()
    _script_dispatch_head(conn, status="planning", session=_ready_session_row())
    conn.on(_INTENTS_LIST, [])
    _script_block_composition(conn, status="planning")
    conn.on(_INTENT_INSERT, _intent_row())
    conn.on(_INTENT_UPDATE, _intent_row(outcome={"dispatch": "acknowledged"}))
    adapter = _FakeClineAdapter()

    outcome = _dispatch_call(_pool(conn), adapter)

    assert outcome.kind is TaskAbortOutcomeKind.INTERRUPTED
    assert adapter.abort_calls == [_EXTERNAL_SESSION_ID]
    assert _event_types(conn) == ["task_blocked", "owner_gate_created"]


def test_settlement_recording_commits_atomically(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The settlement outcome records commit or roll back together: one
    failure inside the recording transaction leaves the durable intents
    unsettled (truthfully re-settleable), and a retried settlement completes
    without ever touching the interrupted Execution (issue #298)."""
    conn = ScriptedConnection()
    second_intent = _intent_row(intent_id=uuid4(), role="reviewer", external_id="fake-session-2")
    _script_settlement_head(conn, intents=[_intent_row(), second_intent])
    adapter = _FakeClineAdapter()

    real_record = intent_records.record_task_interrupt_intent_outcome
    recorded_ids: list[Any] = []

    def _record(pool: Any, *, task_interrupt_intent_id: Any, status: Any, outcome: Any) -> Any:
        recorded_ids.append(task_interrupt_intent_id)
        if len(recorded_ids) == 2:
            raise RuntimeError("forced settlement recording failure")
        return real_record(
            pool,
            task_interrupt_intent_id=task_interrupt_intent_id,
            status=status,
            outcome=outcome,
        )

    monkeypatch.setattr(intent_records, "record_task_interrupt_intent_outcome", _record)
    conn.on(_INTENT_UPDATE, _intent_row(status="settled", outcome={"settlement": "confirmed"}))

    with pytest.raises(RuntimeError, match="forced settlement recording failure"):
        _settlement_call(
            _pool(conn),
            adapter,
            settlement=TaskInterruptSettlement(
                runtime_work_settled=True, operation_accounted_for=True
            ),
        )

    assert len(adapter.settle_calls) == 2

    # The still-unsettled durable state is truthfully re-settleable: a
    # retried settlement completes the recording atomically.
    monkeypatch.setattr(intent_records, "record_task_interrupt_intent_outcome", real_record)
    _script_settlement_head(conn, intents=[_intent_row(), second_intent])
    conn.on(_INTENT_UPDATE, _intent_row(status="settled", outcome={"settlement": "confirmed"}))
    conn.on(
        _INTENT_UPDATE,
        _intent_row(
            intent_id=second_intent[0],
            role="reviewer",
            external_id="fake-session-2",
            status="settled",
            outcome={"settlement": "confirmed"},
        ),
    )

    retry = _settlement_call(
        _pool(conn),
        adapter,
        settlement=TaskInterruptSettlement(runtime_work_settled=True, operation_accounted_for=True),
    )

    assert retry.kind is TaskAbortOutcomeKind.SETTLED
    assert retry.interrupt_outcomes[0].status is TaskInterruptIntentStatus.SETTLED
    assert retry.interrupt_outcomes[1].status is TaskInterruptIntentStatus.SETTLED


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


def test_abort_rejects_an_idle_owner_gate_without_in_flight_activity(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # An ordinary exhausted-loop gate carries no in-flight runtime fact:
    # Abort must not interrupt an idle session over a visible gate.
    for role in (WorkflowRole.PRODUCER, WorkflowRole.REVIEWER):
        conn = ScriptedConnection()
        _script_dispatch_head(conn, status="waiting_for_owner", session=_ready_session_row())
        _wire_activity(monkeypatch)
        conn.on(_INTENTS_LIST, [])
        adapter = _FakeClineAdapter()

        with pytest.raises(ConflictError, match="no durably admitted"):
            _dispatch_call(_pool(conn), adapter, target_roles=(role,))

        assert adapter.abort_calls == []
        assert _count(conn, _BLOCK_INSERT) == 0
        assert _count(conn, _TASK_BLOCK_UPDATE) == 0
        assert _event_types(conn) == []
