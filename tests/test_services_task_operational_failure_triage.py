"""Deterministic tests for the Level 2 operational-failure triage (issue #251).

Scripted SQL-connection seams prove the F4 disposition service on its
critical paths:

- one proven-safe, supported network failure authorizes exactly one durable
  retry with the persisted 2/4/8 s backoff on an injectable clock; the
  fourth failure escalates; duplicate, restarted, or replayed triage returns
  the same existing disposition or a no-op and never re-increments;
- the F2 one-shot safety: an already-claimed automated send (including a
  known non-delivery) is never re-dispatched — its failure escalates through
  the atomic F3 composition, and no attempt row is ever minted for it;
- uncertain, non-network, and unrecognized codes escalate with their stable
  code and a sanitized bounded context; a bare finalized status or caller
  classification alone never authorizes (the execution_failed audit
  corroboration is required);
- stale facts — moved-on token/status/gate identity, settled or superseded
  subjects with an unchanged Task token, missing evidence — are typed no-ops
  that apply nothing;
- the escalation commits one exact TaskBlock + pending gate + BLOCKED
  transition + the two truthful audit events + the episode's set-once
  terminal TaskBlock reference together, and the code-mapped reason (with
  the durably proven session-loss override) lands on the block;
- command shape is validated before any database access.

No live runtime or Postgres access.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import uuid4

import pytest

from openorc.domain.events import WorkflowEventActor
from openorc.domain.operational_failures import (
    FailureEffectEvidence,
    OperationalFailureCode,
    OperationalFailureSourceKind,
    OriginalAttemptKind,
)
from openorc.domain.pending_interactions import PendingTaskRuntimeInteractionOperation
from openorc.domain.tasks import TaskStatus
from openorc.services.errors import InvalidCommandError, NotFoundError
from openorc.services.event_coordination import WorkflowActorContext
from openorc.services.task_operational_failure_triage import (
    DispositionKind,
    NoOpReason,
    OperationalFailureOperation,
    OperationalFailureReport,
    triage_operational_failure,
)
from tests.test_services_task_pr_review_decision import _event_returned_row
from tests.test_services_task_pr_review_dispatch import (
    _OBSERVED,
    _STATE_TOKEN,
    _TASK_ID,
    _WORKSPACE_ID,
    ScriptedConnection,
    _count,
    _pool,
    _session_row,
    _sql_has,
    _sql_lacks,
    _task_row,
)

_NOW = datetime(2026, 10, 30, 12, 0, 0, tzinfo=UTC)
_ROTATED_TOKEN = uuid4()

_WORKSPACE_ID = _WORKSPACE_ID
_TASK_ID = _TASK_ID
_STATE_TOKEN = _STATE_TOKEN
_OBSERVED = _OBSERVED

_REVISION_ID = uuid4()
_IMPL_GATE_ID = uuid4()
_LOOP_ID = uuid4()
_ITERATION_ID = uuid4()
_EXECUTION_ID = uuid4()
_REMEDIATION_EXECUTION_ID = uuid4()
_RETRY_EFFECT_ID = uuid4()
_PRODUCER_SESSION_ID = uuid4()
_CLAIM_ID = uuid4()
_CLAIM_GATE_ID = uuid4()
_EPISODE_ID = uuid4()
_ATTEMPT_1_ID = uuid4()
_ATTEMPT_2_ID = uuid4()
_ATTEMPT_3_ID = uuid4()
_BLOCK_ID = uuid4()
_NEW_GATE_ID = uuid4()

_OPENORC_ACTOR = WorkflowActorContext(WorkflowEventActor.OPENORC, None)

_TASK_LOCKED = _sql_has("from openorc.tasks where id", "for update")
_TASK_PLAIN = _sql_lacks(_sql_has("from openorc.tasks where id"), "for update")
_EPISODE_BY_ORIGINAL = _sql_has(
    "from openorc.operational_failure_episodes where original_attempt_kind"
)
_EPISODE_INSERT = _sql_has("insert into openorc.operational_failure_episodes")
_ATTEMPT_INSERT = _sql_has("insert into openorc.operational_failure_attempts")
_ATTEMPT_BY_SOURCE = _sql_has("from openorc.operational_failure_attempts where source_kind")
_ATTEMPTS_LIST = _sql_has("from openorc.operational_failure_attempts where episode_id")
_EPISODE_ESCALATE = _sql_has(
    "update openorc.operational_failure_episodes", "escalated_task_block_id"
)
_CLAIM_LOCKED = _sql_has("from openorc.task_runtime_send_claims", "for update")
_EXECUTION_LOCKED = _sql_has("from openorc.executions", "for update")
_SESSION_LOCKED = _sql_has("from openorc.task_agent_sessions", "for update")
_GATES_LIST = _sql_has("from openorc.owner_gates where task_id")
_GATE_READ = _sql_has("from openorc.owner_gates where id")
_ITERATION_READ = _sql_has("from openorc.review_iterations where id")
_LOOP_READ = _sql_has("from openorc.review_loops where id")
_ITERATIONS_LIST = _sql_has("from openorc.review_iterations where review_loop_id")
_REVISION_READ = _sql_has("from openorc.plan_revisions where id")
_EVENTS_FIND = _sql_has("from openorc.workflow_events where workspace_id", "subject_type")
_BLOCK_INSERT = _sql_has("insert into openorc.task_blocks")
_GATE_INSERT = _sql_has("insert into openorc.owner_gates")
_TASK_BLOCK_UPDATE = _sql_has("update openorc.tasks", "status = 'blocked'")
_EVENT_INSERT = _sql_has("insert into openorc.workflow_events")
_BOOTSTRAP_EVIDENCE = _sql_has("planning_bootstrap_claim_token")


def _task(
    *,
    status: str = "implementing",
    state_token: Any = _STATE_TOKEN,
    plan_revision: Any = None,
    current_gate: Any = None,
    queued_at: Any = None,
) -> tuple[Any, ...]:
    row = list(_task_row(status=status, state_token=state_token, current_gate_id=current_gate))
    row[9] = plan_revision
    row[12] = queued_at
    return tuple(row)


def _execution(
    *,
    execution_id: Any = _EXECUTION_ID,
    status: str = "failed_transient",
    producer_session_id: Any = _PRODUCER_SESSION_ID,
    review_iteration_id: Any = None,
) -> tuple[Any, ...]:
    return (
        execution_id,
        _WORKSPACE_ID,
        _TASK_ID,
        producer_session_id,
        1,
        status,
        _OBSERVED,
        _OBSERVED,
        None,
        None,
        review_iteration_id,
    )


def _claim_row(
    *,
    claim_id: Any = _CLAIM_ID,
    kind: str = "initial_plan_dispatch",
    status: str = "claimed",
    producer_session_id: Any = _PRODUCER_SESSION_ID,
    review_iteration_id: Any = None,
    owner_gate_id: Any = None,
    subject_head_sha: Any = None,
    outcome: Any = None,
) -> tuple[Any, ...]:
    settled_at = _OBSERVED if status != "claimed" else None
    return (
        claim_id,
        _WORKSPACE_ID,
        _TASK_ID,
        kind,
        producer_session_id,
        review_iteration_id,
        owner_gate_id,
        subject_head_sha,
        status,
        outcome or {},
        _OBSERVED,
        settled_at,
        _OBSERVED,
    )


def _impl_gate_row(*, status: str = "approved") -> tuple[Any, ...]:
    return (
        _IMPL_GATE_ID,
        _WORKSPACE_ID,
        _TASK_ID,
        "implementation_authorization",
        status,
        _REVISION_ID,
        None,
        None,
        None,
        _OBSERVED if status != "pending" else None,
        _OBSERVED,
    )


def _pr_auth_gate_row(*, head_sha: str) -> tuple[Any, ...]:
    return (
        _CLAIM_GATE_ID,
        _WORKSPACE_ID,
        _TASK_ID,
        "pr_authorization",
        "approved",
        None,
        head_sha,
        None,
        None,
        _OBSERVED,
        _OBSERVED,
    )


def _revision_row() -> tuple[Any, ...]:
    return (_REVISION_ID, _WORKSPACE_ID, _TASK_ID, 1, "# Plan", "a" * 40, _OBSERVED)


def _loop_row(*, status: str = "open", purpose: str = "planning") -> tuple[Any, ...]:
    return (_LOOP_ID, _WORKSPACE_ID, _TASK_ID, purpose, 3, status, None, _OBSERVED)


def _planning_iteration_row(*, outcome: str | None = "changes_requested") -> tuple[Any, ...]:
    return (
        _ITERATION_ID,
        _WORKSPACE_ID,
        _TASK_ID,
        _LOOP_ID,
        1,
        _REVISION_ID,
        None,
        None,
        outcome,
        "summary" if outcome else None,
        ["resolve the failing step"] if outcome else None,
        _OBSERVED if outcome else None,
        _OBSERVED,
    )


def _failed_event_row(*, failure_kind: str = "runtime_pre_effect") -> tuple[Any, ...]:
    return (
        uuid4(),
        _WORKSPACE_ID,
        _TASK_ID,
        "execution_failed",
        "producer",
        str(_PRODUCER_SESSION_ID),
        "execution",
        _EXECUTION_ID,
        {"failure_kind": failure_kind},
        _OBSERVED,
    )


def _episode_row(*, escalated: Any = None, episode_id: Any = _EPISODE_ID) -> tuple[Any, ...]:
    return (
        episode_id,
        _WORKSPACE_ID,
        _TASK_ID,
        "implementation_execution",
        "execution",
        _EXECUTION_ID,
        {"execution_id": str(_EXECUTION_ID)},
        escalated,
        _OBSERVED,
    )


def _attempt_row(
    *,
    attempt_id: Any = _ATTEMPT_1_ID,
    ordinal: int = 1,
    source_kind: str = "original",
    source_attempt_id: Any = _EXECUTION_ID,
    not_before: Any = None,
) -> tuple[Any, ...]:
    return (
        attempt_id,
        _WORKSPACE_ID,
        _TASK_ID,
        _EPISODE_ID,
        ordinal,
        source_kind,
        source_attempt_id,
        "runtime_unavailable",
        not_before if not_before is not None else _NOW,
        _OBSERVED,
    )


def _operation(
    *,
    family: PendingTaskRuntimeInteractionOperation = (
        PendingTaskRuntimeInteractionOperation.IMPLEMENTATION_EXECUTION
    ),
    kind: OriginalAttemptKind = OriginalAttemptKind.EXECUTION,
    original_id: Any = _EXECUTION_ID,
) -> OperationalFailureOperation:
    return OperationalFailureOperation(
        family=family,
        original_attempt_kind=kind,
        original_attempt_id=original_id,
        subject={"execution_id": str(original_id)},
    )


def _report(
    *,
    code: OperationalFailureCode = OperationalFailureCode.RUNTIME_UNAVAILABLE,
    evidence: FailureEffectEvidence = FailureEffectEvidence.PRE_EFFECT_NOT_APPLIED,
    reporting_kind: OperationalFailureSourceKind = OperationalFailureSourceKind.ORIGINAL,
    reporting_id: Any = None,
    effect: Any = None,
    diagnostics: Any = None,
) -> OperationalFailureReport:
    return OperationalFailureReport(
        reporting_attempt_kind=reporting_kind,
        reporting_attempt_id=_EXECUTION_ID if reporting_id is None else reporting_id,
        error_code=code,
        effect_evidence=evidence,
        effect_attempt_id=effect,
        diagnostic_context=diagnostics or {},
    )


def _triage(
    conn: ScriptedConnection,
    *,
    status: TaskStatus = TaskStatus.IMPLEMENTING,
    current_gate: Any = None,
    token: Any = _STATE_TOKEN,
    operation: OperationalFailureOperation | None = None,
    report: OperationalFailureReport | None = None,
    now: datetime = _NOW,
    **overrides: Any,
) -> Any:
    base: dict[str, Any] = {
        "workspace_id": _WORKSPACE_ID,
        "task_id": _TASK_ID,
        "expected_state_token": token,
        "expected_status": status,
        "expected_current_owner_gate_id": current_gate,
        "actor": _OPENORC_ACTOR,
        "operation": operation if operation is not None else _operation(),
        "report": report if report is not None else _report(),
        "clock": lambda: now,
    }
    base.update(overrides)
    return triage_operational_failure(_pool(conn), **base)


def _script_execution_retry(
    conn: ScriptedConnection,
    *,
    episode_row: Any | None = None,
    attempts_rows: list[Any] | None = None,
    execution_row: Any | None = None,
    session_row: Any | None = None,
    event_rows: list[Any] | None = None,
    attempt_return: Any | None = None,
) -> None:
    """Script the full locked flow of one authorized execution retry."""
    conn.on(_TASK_LOCKED, _task(status="implementing", plan_revision=_REVISION_ID))
    conn.on(_EPISODE_BY_ORIGINAL, episode_row)
    if attempts_rows is not None:
        conn.on(_ATTEMPTS_LIST, attempts_rows)
    conn.on(_ATTEMPT_BY_SOURCE, None)
    conn.on(
        _EXECUTION_LOCKED,
        execution_row if execution_row is not None else _execution(),
    )
    conn.on(_REVISION_READ, _revision_row())
    conn.on(_GATES_LIST, [_impl_gate_row()])
    conn.on(
        _SESSION_LOCKED,
        session_row
        if session_row is not None
        else _session_row("producer", _PRODUCER_SESSION_ID, "fake-session-0"),
    )
    conn.on(_EVENTS_FIND, event_rows if event_rows is not None else [_failed_event_row()])
    if episode_row is None:
        conn.on(_EPISODE_INSERT, _episode_row())
    conn.on(_ATTEMPT_INSERT, attempt_return if attempt_return is not None else _attempt_row())


def _script_escalation(
    conn: ScriptedConnection,
    *,
    task_row: Any,
    episode_row: Any | None = None,
    attempts_rows: list[Any] | None = None,
    claim_row: Any | None = None,
    execution_row: Any | None = None,
    session_row: Any | None = None,
    event_rows: list[Any] | None = None,
    extra_setup: Any = None,
) -> None:
    """Script the full locked flow of one F3 escalation."""
    conn.on(_TASK_LOCKED, task_row)
    conn.on(_EPISODE_BY_ORIGINAL, episode_row)
    if attempts_rows is not None:
        conn.on(_ATTEMPTS_LIST, attempts_rows)
    conn.on(_ATTEMPT_BY_SOURCE, None)
    if claim_row is not None:
        conn.on(_CLAIM_LOCKED, claim_row)
    if execution_row is not None:
        conn.on(_EXECUTION_LOCKED, execution_row)
    if event_rows is not None:
        conn.on(_EVENTS_FIND, event_rows)
    if session_row is not None:
        conn.on(_SESSION_LOCKED, session_row)
    if extra_setup is not None:
        extra_setup(conn)
    if episode_row is None:
        conn.on(_EPISODE_INSERT, _episode_row())
    # The F3 nested composition re-locks and re-reads the same Task row.
    conn.on(_TASK_LOCKED, task_row)
    conn.on(_TASK_PLAIN, task_row)
    conn.on(_BLOCK_INSERT, (_BLOCK_ID, _WORKSPACE_ID, _TASK_ID, "unknown", {}, None, _OBSERVED))
    conn.on(
        _GATE_INSERT,
        (
            _NEW_GATE_ID,
            _WORKSPACE_ID,
            _TASK_ID,
            "block_resolution",
            "pending",
            None,
            None,
            None,
            _BLOCK_ID,
            None,
            _OBSERVED,
        ),
    )
    conn.on(
        _TASK_BLOCK_UPDATE,
        _task(status="blocked", state_token=_ROTATED_TOKEN, current_gate=_NEW_GATE_ID),
    )
    conn.on(_EVENT_INSERT, _event_returned_row("task_blocked"))
    conn.on(_EVENT_INSERT, _event_returned_row("owner_gate_created"))
    conn.on(_EPISODE_ESCALATE, _episode_row(escalated=_BLOCK_ID))


def test_a_proven_safe_supported_network_failure_authorizes_one_durable_retry() -> None:
    conn = ScriptedConnection()
    _script_execution_retry(
        conn,
        attempt_return=_attempt_row(not_before=_NOW + timedelta(seconds=2)),
    )
    result = _triage(conn)
    assert result.kind is DispositionKind.RETRY_AUTHORIZED
    assert result.retry_attempt is not None
    assert result.retry_attempt.ordinal == 1
    assert result.retry_attempt.not_before == _NOW + timedelta(seconds=2)
    assert result.retry_attempt.error_code is OperationalFailureCode.RUNTIME_UNAVAILABLE
    episode_params = next(params for sql, params in conn.executed if _EPISODE_INSERT(sql, None))
    assert episode_params is not None
    assert episode_params[2] == "implementation_execution"
    assert episode_params[3] == "execution"
    assert episode_params[4] == _EXECUTION_ID
    attempt_params = next(params for sql, params in conn.executed if _ATTEMPT_INSERT(sql, None))
    assert attempt_params is not None
    assert attempt_params[3] == 1
    assert attempt_params[4] == "original"
    assert attempt_params[5] == _EXECUTION_ID
    assert attempt_params[6] == "runtime_unavailable"
    assert attempt_params[7] == _NOW + timedelta(seconds=2)


def test_the_budget_advances_with_4_and_8_second_backoff_and_the_fourth_escalates() -> None:
    # Second authorized attempt: the prior-authorized report carries the
    # fresh effect row its execution produced, and the due time is 4 s.
    conn = ScriptedConnection()
    _script_execution_retry(
        conn,
        episode_row=_episode_row(),
        attempts_rows=[_attempt_row()],
        execution_row=_execution(execution_id=_RETRY_EFFECT_ID),
        event_rows=[_failed_event_row()],
        attempt_return=_attempt_row(
            attempt_id=_ATTEMPT_2_ID,
            ordinal=2,
            source_kind="prior_authorized",
            source_attempt_id=_ATTEMPT_1_ID,
            not_before=_NOW + timedelta(seconds=4),
        ),
    )
    result = _triage(
        conn,
        report=_report(
            reporting_kind=OperationalFailureSourceKind.PRIOR_AUTHORIZED,
            reporting_id=_ATTEMPT_1_ID,
            effect=_RETRY_EFFECT_ID,
        ),
    )
    assert result.kind is DispositionKind.RETRY_AUTHORIZED
    assert result.retry_attempt is not None
    assert result.retry_attempt.ordinal == 2
    assert result.retry_attempt.not_before == _NOW + timedelta(seconds=4)
    attempt_params = next(params for sql, params in conn.executed if _ATTEMPT_INSERT(sql, None))
    assert attempt_params is not None
    assert attempt_params[3] == 2
    assert attempt_params[4] == "prior_authorized"
    assert attempt_params[5] == _ATTEMPT_1_ID
    assert attempt_params[7] == _NOW + timedelta(seconds=4)


def test_the_third_attempt_uses_8_seconds_and_the_fourth_failure_escalates() -> None:
    conn = ScriptedConnection()
    _script_execution_retry(
        conn,
        episode_row=_episode_row(),
        attempts_rows=[
            _attempt_row(),
            _attempt_row(
                attempt_id=_ATTEMPT_2_ID,
                ordinal=2,
                source_kind="prior_authorized",
                source_attempt_id=_ATTEMPT_1_ID,
            ),
        ],
        execution_row=_execution(execution_id=_RETRY_EFFECT_ID),
        attempt_return=_attempt_row(
            attempt_id=_ATTEMPT_3_ID,
            ordinal=3,
            source_kind="prior_authorized",
            source_attempt_id=_ATTEMPT_2_ID,
            not_before=_NOW + timedelta(seconds=8),
        ),
    )
    result = _triage(
        conn,
        report=_report(
            reporting_kind=OperationalFailureSourceKind.PRIOR_AUTHORIZED,
            reporting_id=_ATTEMPT_2_ID,
            effect=_RETRY_EFFECT_ID,
        ),
    )
    assert result.retry_attempt is not None
    assert result.retry_attempt.not_before == _NOW + timedelta(seconds=8)

    # The fourth failure exhausts the budget and escalates through F3.
    conn = ScriptedConnection()
    _script_escalation(
        conn,
        task_row=_task(status="implementing", plan_revision=_REVISION_ID),
        episode_row=_episode_row(),
        attempts_rows=[
            _attempt_row(),
            _attempt_row(
                attempt_id=_ATTEMPT_2_ID,
                ordinal=2,
                source_kind="prior_authorized",
                source_attempt_id=_ATTEMPT_1_ID,
            ),
            _attempt_row(
                attempt_id=_ATTEMPT_3_ID,
                ordinal=3,
                source_kind="prior_authorized",
                source_attempt_id=_ATTEMPT_2_ID,
            ),
        ],
        execution_row=_execution(execution_id=_RETRY_EFFECT_ID),
        event_rows=[_failed_event_row()],
        extra_setup=lambda c: (
            c.on(_REVISION_READ, _revision_row()),
            c.on(_GATES_LIST, [_impl_gate_row()]),
            c.on(_SESSION_LOCKED, _session_row("producer", _PRODUCER_SESSION_ID, "fake-session-0")),
        ),
    )
    result = _triage(
        conn,
        report=_report(
            reporting_kind=OperationalFailureSourceKind.PRIOR_AUTHORIZED,
            reporting_id=_ATTEMPT_3_ID,
            effect=_RETRY_EFFECT_ID,
        ),
    )
    assert result.kind is DispositionKind.BLOCKED
    assert _count(conn, _ATTEMPT_INSERT) == 0
    assert _count(conn, _BLOCK_INSERT) == 1


def test_a_duplicate_report_returns_the_existing_disposition_without_new_effects() -> None:
    conn = ScriptedConnection()
    conn.on(_TASK_LOCKED, _task())
    conn.on(_EPISODE_BY_ORIGINAL, None)
    conn.on(_ATTEMPT_BY_SOURCE, _attempt_row())
    result = _triage(conn)
    assert result.kind is DispositionKind.NO_OP
    assert result.no_op_reason is NoOpReason.ALREADY_DISPOSED
    assert result.existing_attempt_id == _ATTEMPT_1_ID
    assert len(conn.executed) == 3


def test_a_terminally_disposed_episode_is_a_no_op_even_after_recovery() -> None:
    conn = ScriptedConnection()
    conn.on(_TASK_LOCKED, _task())
    conn.on(_EPISODE_BY_ORIGINAL, _episode_row(escalated=_BLOCK_ID))
    result = _triage(conn)
    assert result.kind is DispositionKind.NO_OP
    assert result.no_op_reason is NoOpReason.ALREADY_TERMINALLY_DISPOSED
    assert result.existing_task_block_id == _BLOCK_ID
    assert len(conn.executed) == 2


def test_a_stale_token_is_a_typed_no_op_that_touches_nothing_else() -> None:
    conn = ScriptedConnection()
    conn.on(_TASK_LOCKED, _task(state_token=uuid4()))
    result = _triage(conn)
    assert result.kind is DispositionKind.NO_OP
    assert result.no_op_reason is NoOpReason.STALE
    assert len(conn.executed) == 1


def test_a_moved_on_status_is_a_typed_no_op() -> None:
    conn = ScriptedConnection()
    conn.on(_TASK_LOCKED, _task(status="implementing"))
    result = _triage(conn, status=TaskStatus.PLANNING)
    assert result.kind is DispositionKind.NO_OP
    assert result.no_op_reason is NoOpReason.STALE
    assert len(conn.executed) == 1


def test_an_already_claimed_send_failure_escalates_and_never_mints_a_retry() -> None:
    conn = ScriptedConnection()
    _script_escalation(
        conn,
        task_row=_task(status="planning"),
        claim_row=_claim_row(kind="initial_plan_dispatch"),
        session_row=_session_row("producer", _PRODUCER_SESSION_ID, "fake-session-0"),
    )
    result = _triage(
        conn,
        status=TaskStatus.PLANNING,
        operation=_operation(
            family=PendingTaskRuntimeInteractionOperation.INITIAL_PLAN_DISPATCH,
            kind=OriginalAttemptKind.RUNTIME_SEND_CLAIM,
            original_id=_CLAIM_ID,
        ),
        report=_report(reporting_id=_CLAIM_ID),
    )
    assert result.kind is DispositionKind.BLOCKED
    assert result.installation is not None
    assert result.installation.task_block.id == _BLOCK_ID
    assert _count(conn, _ATTEMPT_INSERT) == 0
    assert _count(conn, _BLOCK_INSERT) == 1
    # The code-mapped reason and the sanitized context land on the block.
    block_params = next(params for sql, params in conn.executed if _BLOCK_INSERT(sql, None))
    assert block_params is not None
    assert block_params[2] == "connection_unavailable"
    context = block_params[3].obj
    assert context["error_code"] == "runtime_unavailable"
    assert context["effect_evidence"] == "pre_effect_not_applied"
    assert context["operation_family"] == "initial_plan_dispatch"
    assert context["original_attempt_kind"] == "runtime_send_claim"
    assert context["original_attempt_id"] == str(_CLAIM_ID)
    assert context["failure_episode_id"] == str(_EPISODE_ID)
    assert "prompt" not in str(context)
    # The episode's terminal disposition carries the exact F3 TaskBlock.
    escalate_params = next(params for sql, params in conn.executed if _EPISODE_ESCALATE(sql, None))
    assert escalate_params is not None
    assert escalate_params[0] == _BLOCK_ID
    assert escalate_params[1] == _EPISODE_ID


def test_a_known_non_delivery_claim_failure_still_escalates_without_a_retry() -> None:
    conn = ScriptedConnection()
    _script_escalation(
        conn,
        task_row=_task(status="planning"),
        claim_row=_claim_row(
            kind="initial_plan_dispatch",
            status="known_nondelivery",
            outcome={"failure_kind": "runtime_pre_effect"},
        ),
        session_row=_session_row("producer", _PRODUCER_SESSION_ID, "fake-session-0"),
    )
    result = _triage(
        conn,
        status=TaskStatus.PLANNING,
        operation=_operation(
            family=PendingTaskRuntimeInteractionOperation.INITIAL_PLAN_DISPATCH,
            kind=OriginalAttemptKind.RUNTIME_SEND_CLAIM,
            original_id=_CLAIM_ID,
        ),
        report=_report(reporting_id=_CLAIM_ID),
    )
    assert result.kind is DispositionKind.BLOCKED
    assert _count(conn, _ATTEMPT_INSERT) == 0


def test_a_settled_claim_makes_the_failure_report_stale() -> None:
    conn = ScriptedConnection()
    conn.on(_TASK_LOCKED, _task(status="planning"))
    conn.on(_EPISODE_BY_ORIGINAL, None)
    conn.on(_ATTEMPT_BY_SOURCE, None)
    conn.on(_CLAIM_LOCKED, _claim_row(kind="initial_plan_dispatch", status="settled"))
    result = _triage(
        conn,
        status=TaskStatus.PLANNING,
        operation=_operation(
            family=PendingTaskRuntimeInteractionOperation.INITIAL_PLAN_DISPATCH,
            kind=OriginalAttemptKind.RUNTIME_SEND_CLAIM,
            original_id=_CLAIM_ID,
        ),
        report=_report(reporting_id=_CLAIM_ID),
    )
    assert result.kind is DispositionKind.NO_OP
    assert result.no_op_reason is NoOpReason.STALE


def test_a_published_plan_revision_supersedes_a_plan_dispatch_report() -> None:
    conn = ScriptedConnection()
    conn.on(_TASK_LOCKED, _task(status="planning", plan_revision=_REVISION_ID))
    conn.on(_EPISODE_BY_ORIGINAL, None)
    conn.on(_ATTEMPT_BY_SOURCE, None)
    conn.on(_CLAIM_LOCKED, _claim_row(kind="initial_plan_dispatch"))
    result = _triage(
        conn,
        status=TaskStatus.PLANNING,
        operation=_operation(
            family=PendingTaskRuntimeInteractionOperation.INITIAL_PLAN_DISPATCH,
            kind=OriginalAttemptKind.RUNTIME_SEND_CLAIM,
            original_id=_CLAIM_ID,
        ),
        report=_report(reporting_id=_CLAIM_ID),
    )
    assert result.kind is DispositionKind.NO_OP
    assert result.no_op_reason is NoOpReason.STALE


def test_uncertain_evidence_escalates_and_never_authorizes_a_retry() -> None:
    conn = ScriptedConnection()
    _script_escalation(
        conn,
        task_row=_task(status="implementing", plan_revision=_REVISION_ID),
        execution_row=_execution(status="running"),
        event_rows=[],
        extra_setup=lambda c: (
            c.on(_REVISION_READ, _revision_row()),
            c.on(_GATES_LIST, [_impl_gate_row()]),
            c.on(_SESSION_LOCKED, _session_row("producer", _PRODUCER_SESSION_ID, "fake-session-0")),
        ),
    )
    result = _triage(
        conn,
        report=_report(
            code=OperationalFailureCode.RUNTIME_TIMEOUT,
            evidence=FailureEffectEvidence.EFFECT_UNCLASSIFIED,
        ),
    )
    assert result.kind is DispositionKind.BLOCKED
    assert _count(conn, _ATTEMPT_INSERT) == 0
    block_params = next(params for sql, params in conn.executed if _BLOCK_INSERT(sql, None))
    assert block_params is not None
    assert block_params[2] == "runtime_failure"


def test_a_bare_finalized_status_without_agreeing_audit_evidence_never_retries() -> None:
    # The execution finalized failed_transient, but the newest execution
    # event disagrees with the claimed pre-effect network classification:
    # the retry fails closed and the current failure escalates.
    conn = ScriptedConnection()
    _script_escalation(
        conn,
        task_row=_task(status="implementing", plan_revision=_REVISION_ID),
        execution_row=_execution(),
        event_rows=[_failed_event_row(failure_kind="authority_stale")],
        extra_setup=lambda c: (
            c.on(_REVISION_READ, _revision_row()),
            c.on(_GATES_LIST, [_impl_gate_row()]),
            c.on(
                _SESSION_LOCKED,
                _session_row("producer", _PRODUCER_SESSION_ID, "fake-session-0"),
            ),
        ),
    )
    result = _triage(conn)
    assert result.kind is DispositionKind.BLOCKED
    assert _count(conn, _ATTEMPT_INSERT) == 0


def test_a_terminal_session_binding_makes_session_loss_the_blocking_reason() -> None:
    conn = ScriptedConnection()
    _script_escalation(
        conn,
        task_row=_task(status="implementing", plan_revision=_REVISION_ID),
        execution_row=_execution(),
        event_rows=[_failed_event_row()],
        extra_setup=lambda c: (
            c.on(_REVISION_READ, _revision_row()),
            c.on(_GATES_LIST, [_impl_gate_row()]),
            c.on(
                _SESSION_LOCKED,
                _session_row("producer", _PRODUCER_SESSION_ID, "fake-session-0", lifecycle="lost"),
            ),
        ),
    )
    result = _triage(conn)
    assert result.kind is DispositionKind.BLOCKED
    block_params = next(params for sql, params in conn.executed if _BLOCK_INSERT(sql, None))
    assert block_params is not None
    assert block_params[2] == "agent_session_lost"


def test_an_unrecognized_code_escalates_with_its_stable_fallback_and_diagnostics() -> None:
    conn = ScriptedConnection()
    _script_escalation(
        conn,
        task_row=_task(status="planning"),
        claim_row=_claim_row(kind="initial_plan_dispatch"),
        session_row=_session_row("producer", _PRODUCER_SESSION_ID, "fake-session-0"),
    )
    result = _triage(
        conn,
        status=TaskStatus.PLANNING,
        operation=_operation(
            family=PendingTaskRuntimeInteractionOperation.INITIAL_PLAN_DISPATCH,
            kind=OriginalAttemptKind.RUNTIME_SEND_CLAIM,
            original_id=_CLAIM_ID,
        ),
        report=_report(
            code=OperationalFailureCode.UNKNOWN,
            reporting_id=_CLAIM_ID,
            diagnostics={"classification": "unrecognized-provider-code"},
        ),
    )
    assert result.kind is DispositionKind.BLOCKED
    block_params = next(params for sql, params in conn.executed if _BLOCK_INSERT(sql, None))
    assert block_params is not None
    assert block_params[2] == "unknown"
    context = block_params[3].obj
    assert context["error_code"] == "unknown"
    assert context["diagnostics"] == {"classification": "unrecognized-provider-code"}


def test_the_one_shot_remediation_claim_escalates_even_on_proven_pre_effect_failure() -> None:
    conn = ScriptedConnection()
    _script_escalation(
        conn,
        task_row=_task(status="implementing", plan_revision=_REVISION_ID),
        execution_row=_execution(review_iteration_id=_ITERATION_ID),
        extra_setup=lambda c: (
            c.on(_ITERATION_READ, _planning_iteration_row()),
            c.on(_LOOP_READ, _loop_row()),
            c.on(_ITERATIONS_LIST, [_planning_iteration_row()]),
            c.on(_REVISION_READ, _revision_row()),
            c.on(_GATES_LIST, [_impl_gate_row()]),
            c.on(_SESSION_LOCKED, _session_row("producer", _PRODUCER_SESSION_ID, "fake-session-0")),
        ),
    )
    result = _triage(
        conn,
        operation=_operation(
            family=PendingTaskRuntimeInteractionOperation.PR_REMEDIATION_EXECUTION,
            original_id=_REMEDIATION_EXECUTION_ID,
        ),
        report=_report(reporting_id=_REMEDIATION_EXECUTION_ID),
    )
    assert result.kind is DispositionKind.BLOCKED
    assert _count(conn, _ATTEMPT_INSERT) == 0


def test_a_current_gate_after_approval_supersedes_the_execution_report() -> None:
    conn = ScriptedConnection()
    conn.on(_TASK_LOCKED, _task(status="implementing", current_gate=uuid4()))
    conn.on(_EPISODE_BY_ORIGINAL, None)
    conn.on(_ATTEMPT_BY_SOURCE, None)
    conn.on(_EXECUTION_LOCKED, _execution())
    result = _triage(conn)
    assert result.kind is DispositionKind.NO_OP
    assert result.no_op_reason is NoOpReason.STALE


def test_a_missing_task_identity_is_the_uniform_not_found_error() -> None:
    conn = ScriptedConnection()
    conn.on(_TASK_LOCKED, None)
    with pytest.raises(NotFoundError):
        _triage(conn)


def test_command_shape_is_validated_before_any_database_access() -> None:
    # A malformed identifier is rejected before any SQL.
    with pytest.raises(InvalidCommandError):
        _triage(ScriptedConnection(), task_id="not-a-uuid")  # type: ignore[arg-type]
    # An incoherent family/identity-kind pair is a malformed command.
    conn = ScriptedConnection()
    with pytest.raises(InvalidCommandError):
        _triage(
            conn,
            operation=_operation(
                family=PendingTaskRuntimeInteractionOperation.INITIAL_PLAN_DISPATCH,
                kind=OriginalAttemptKind.EXECUTION,
            ),
        )
    assert conn.executed == []
    # An original-attempt report must address the operation's original identity.
    conn = ScriptedConnection()
    with pytest.raises(InvalidCommandError):
        _triage(conn, report=_report(reporting_id=uuid4()))
    assert conn.executed == []
    # An authorized-attempt report for a retry-eligible family must carry the
    # durable effect row its execution produced.
    conn = ScriptedConnection()
    with pytest.raises(InvalidCommandError):
        _triage(
            conn,
            report=_report(
                reporting_kind=OperationalFailureSourceKind.PRIOR_AUTHORIZED,
                reporting_id=_ATTEMPT_1_ID,
            ),
        )
    assert conn.executed == []
    # The diagnostic context must be sanitized and bounded.
    conn = ScriptedConnection()
    with pytest.raises(InvalidCommandError):
        _triage(conn, report=_report(diagnostics={"bad": float("nan")}))
    assert conn.executed == []
    # The actor must be a WorkflowActorContext.
    conn = ScriptedConnection()
    with pytest.raises(InvalidCommandError):
        _triage(conn, actor="openorc")  # type: ignore[arg-type]
    assert conn.executed == []


def test_a_bounded_diagnostic_context_survives_and_an_unbounded_one_is_rejected() -> None:
    conn = ScriptedConnection()
    _script_escalation(
        conn,
        task_row=_task(status="planning"),
        claim_row=_claim_row(kind="initial_plan_dispatch"),
        session_row=_session_row("producer", _PRODUCER_SESSION_ID, "fake-session-0"),
    )
    result = _triage(
        conn,
        status=TaskStatus.PLANNING,
        operation=_operation(
            family=PendingTaskRuntimeInteractionOperation.INITIAL_PLAN_DISPATCH,
            kind=OriginalAttemptKind.RUNTIME_SEND_CLAIM,
            original_id=_CLAIM_ID,
        ),
        report=_report(
            reporting_id=_CLAIM_ID,
            diagnostics={"sanitized": "fact", "nested": {"ok": True}},
        ),
    )
    assert result.kind is DispositionKind.BLOCKED
    conn = ScriptedConnection()
    with pytest.raises(InvalidCommandError):
        _triage(
            conn,
            report=_report(diagnostics={"big": "x" * 5000}),
        )
    assert conn.executed == []
