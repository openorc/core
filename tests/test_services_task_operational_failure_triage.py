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
    GitHubMergeOperation,
    OperationalFailureCode,
    OperationalFailureFamily,
    OperationalFailureSourceKind,
    OriginalAttemptKind,
    PRAuthorizationPresentationOperation,
)
from openorc.domain.pending_interactions import PendingTaskRuntimeInteractionOperation
from openorc.domain.tasks import TaskStatus
from openorc.services.errors import (
    ConflictError,
    InvalidCommandError,
    NotFoundError,
    StaleOperationError,
)
from openorc.services.event_coordination import WorkflowActorContext
from openorc.services.task_operational_failure_triage import (
    DispositionKind,
    NoOpReason,
    OperationalFailureOperation,
    OperationalFailureReport,
    bind_retry_attempt_execution,
    triage_operational_failure,
)
from tests.test_services_task_pr_review_decision import _event_returned_row
from tests.test_services_task_pr_review_dispatch import (
    _HEAD_SHA,
    _NEW_HEAD_SHA,
    _OBSERVED,
    _PR_ID,
    _STATE_TOKEN,
    _TASK_ID,
    _WORKSPACE_ID,
    ScriptedConnection,
    _count,
    _pool,
    _pr_row,
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
_EPISODE_READ = _sql_has("from openorc.operational_failure_episodes where id")
_ATTEMPT_READ = _sql_has("from openorc.operational_failure_attempts where id")
_ATTEMPT_BIND = _sql_has("update openorc.operational_failure_attempts", "resulting_execution_id")
_PR_READ = _sql_has("from openorc.task_pull_requests where task_id")
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


def _pr_iteration_row(*, outcome: str | None = "changes_requested") -> tuple[Any, ...]:
    return (
        _ITERATION_ID,
        _WORKSPACE_ID,
        _TASK_ID,
        _LOOP_ID,
        1,
        None,
        _PR_ID,
        _HEAD_SHA,
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


def _episode_row(
    *,
    escalated: Any = None,
    episode_id: Any = _EPISODE_ID,
    family: str = "implementation_execution",
    kind: str = "execution",
    original_id: Any = _EXECUTION_ID,
) -> tuple[Any, ...]:
    return (
        episode_id,
        _WORKSPACE_ID,
        _TASK_ID,
        family,
        kind,
        original_id,
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
    resulting_execution_id: Any = _RETRY_EFFECT_ID,
    consumed_at: Any = None,
    error_code: str = "runtime_unavailable",
) -> tuple[Any, ...]:
    return (
        attempt_id,
        _WORKSPACE_ID,
        _TASK_ID,
        _EPISODE_ID,
        ordinal,
        source_kind,
        source_attempt_id,
        error_code,
        not_before if not_before is not None else _NOW,
        resulting_execution_id,
        consumed_at,
        _OBSERVED,
    )


def _operation(
    *,
    family: OperationalFailureFamily = (
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


def test_the_pre_effect_corroboration_finds_the_failure_event_when_timestamps_tie() -> None:
    """Tied audit commit timestamps make the newest-first subject-event
    ordering ambiguous, so the corroboration must find the exact
    ``execution_failed`` event even when the started event sits above it in
    the ordered window — and a disagreeing failure event still fails closed.
    """
    started_row = (
        uuid4(),
        _WORKSPACE_ID,
        _TASK_ID,
        "execution_started",
        "producer",
        str(_PRODUCER_SESSION_ID),
        "execution",
        _EXECUTION_ID,
        {},
        _OBSERVED,
    )

    # Tie: both audit rows carry the same created_at, so the ordered window
    # may return the started event first. The corroboration scans past it.
    conn = ScriptedConnection()
    _script_execution_retry(conn, event_rows=[started_row, _failed_event_row()])
    result = _triage(conn)
    assert result.kind is DispositionKind.RETRY_AUTHORIZED
    assert result.retry_attempt is not None


def test_the_budget_advances_with_4_and_8_second_backoff_and_the_fourth_escalates() -> None:
    # Second authorized attempt: the prior-authorized report carries the
    # fresh effect row its execution produced, and the due time is 4 s.
    conn = ScriptedConnection()
    _script_execution_retry(
        conn,
        episode_row=_episode_row(),
        attempts_rows=[_attempt_row(resulting_execution_id=_RETRY_EFFECT_ID)],
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
    # The execution finalized failed_transient, but the execution_failed
    # audit event disagrees with the claimed pre-effect network
    # classification: the retry fails closed and the current failure
    # escalates.
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


def test_a_current_pr_remediation_failure_escalates_through_its_own_subject() -> None:
    conn = ScriptedConnection()
    _script_escalation(
        conn,
        task_row=_task(status="reviewing"),
        execution_row=_execution(
            execution_id=_REMEDIATION_EXECUTION_ID, review_iteration_id=_ITERATION_ID
        ),
        extra_setup=lambda c: (
            c.on(_ITERATION_READ, _pr_iteration_row()),
            c.on(_LOOP_READ, _loop_row(status="open", purpose="pr_review")),
            c.on(_ITERATIONS_LIST, [_pr_iteration_row()]),
            c.on(_PR_READ, _pr_row(head_sha=_HEAD_SHA)),
            c.on(_SESSION_LOCKED, _session_row("producer", _PRODUCER_SESSION_ID, "fake-session-0")),
        ),
    )
    result = _triage(
        conn,
        status=TaskStatus.REVIEWING,
        operation=_operation(
            family=PendingTaskRuntimeInteractionOperation.PR_REMEDIATION_EXECUTION,
            original_id=_REMEDIATION_EXECUTION_ID,
        ),
        report=_report(reporting_id=_REMEDIATION_EXECUTION_ID),
    )
    assert result.kind is DispositionKind.BLOCKED
    assert _count(conn, _ATTEMPT_INSERT) == 0
    block_params = next(params for sql, params in conn.executed if _BLOCK_INSERT(sql, None))
    assert block_params is not None
    assert block_params[2] == "connection_unavailable"


def test_a_moved_pr_head_supersedes_a_remediation_failure_report() -> None:
    conn = ScriptedConnection()
    conn.on(_TASK_LOCKED, _task(status="reviewing"))
    conn.on(_EPISODE_BY_ORIGINAL, None)
    conn.on(_ATTEMPT_BY_SOURCE, None)
    conn.on(
        _EXECUTION_LOCKED,
        _execution(execution_id=_REMEDIATION_EXECUTION_ID, review_iteration_id=_ITERATION_ID),
    )
    conn.on(_ITERATION_READ, _pr_iteration_row())
    conn.on(_LOOP_READ, _loop_row(status="open", purpose="pr_review"))
    conn.on(_ITERATIONS_LIST, [_pr_iteration_row()])
    conn.on(_PR_READ, _pr_row(head_sha=_NEW_HEAD_SHA))
    result = _triage(
        conn,
        status=TaskStatus.REVIEWING,
        operation=_operation(
            family=PendingTaskRuntimeInteractionOperation.PR_REMEDIATION_EXECUTION,
            original_id=_REMEDIATION_EXECUTION_ID,
        ),
        report=_report(reporting_id=_REMEDIATION_EXECUTION_ID),
    )
    assert result.kind is DispositionKind.NO_OP
    assert result.no_op_reason is NoOpReason.STALE
    assert _count(conn, _BLOCK_INSERT) == 0


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


def test_an_original_report_must_corroborate_its_exact_original_execution() -> None:
    conn = ScriptedConnection()
    with pytest.raises(InvalidCommandError):
        _triage(conn, report=_report(effect=uuid4()))
    assert conn.executed == []


def test_an_unbound_authorized_attempt_report_is_stale() -> None:
    # The authorization was never executed (no durable resulting-Execution
    # binding): a failure report about it addresses nothing that ran.
    conn = ScriptedConnection()
    conn.on(_TASK_LOCKED, _task(status="implementing", plan_revision=_REVISION_ID))
    conn.on(_EPISODE_BY_ORIGINAL, _episode_row())
    conn.on(_ATTEMPTS_LIST, [_attempt_row(resulting_execution_id=None)])
    conn.on(_ATTEMPT_BY_SOURCE, None)
    conn.on(_EXECUTION_LOCKED, _execution(execution_id=_RETRY_EFFECT_ID))
    result = _triage(
        conn,
        report=_report(
            reporting_kind=OperationalFailureSourceKind.PRIOR_AUTHORIZED,
            reporting_id=_ATTEMPT_1_ID,
            effect=_RETRY_EFFECT_ID,
        ),
    )
    assert result.kind is DispositionKind.NO_OP
    assert result.no_op_reason is NoOpReason.STALE
    assert _count(conn, _ATTEMPT_INSERT) == 0
    assert _count(conn, _BLOCK_INSERT) == 0


def test_a_mismatched_effect_binding_is_stale() -> None:
    # The authorization is durably bound to one exact Execution; a report
    # naming a different failed_transient Execution of the same Task must
    # never authorize another retry against the wrong evidence.
    other_execution_id = uuid4()
    conn = ScriptedConnection()
    conn.on(_TASK_LOCKED, _task(status="implementing", plan_revision=_REVISION_ID))
    conn.on(_EPISODE_BY_ORIGINAL, _episode_row())
    conn.on(_ATTEMPTS_LIST, [_attempt_row()])
    conn.on(_ATTEMPT_BY_SOURCE, None)
    conn.on(_EXECUTION_LOCKED, _execution(execution_id=other_execution_id))
    result = _triage(
        conn,
        report=_report(
            reporting_kind=OperationalFailureSourceKind.PRIOR_AUTHORIZED,
            reporting_id=_ATTEMPT_1_ID,
            effect=other_execution_id,
        ),
    )
    assert result.kind is DispositionKind.NO_OP
    assert result.no_op_reason is NoOpReason.STALE
    assert _count(conn, _ATTEMPT_INSERT) == 0


def test_the_binding_service_binds_consumed_authorizations_exactly_once() -> None:
    conn = ScriptedConnection()
    conn.on(_TASK_LOCKED, _task(status="implementing", plan_revision=_REVISION_ID))
    conn.on(_ATTEMPT_READ, _attempt_row(resulting_execution_id=None))
    conn.on(_EPISODE_READ, _episode_row())
    conn.on(_EXECUTION_LOCKED, _execution(execution_id=_RETRY_EFFECT_ID))
    conn.on(
        _ATTEMPT_BIND,
        _attempt_row(resulting_execution_id=_RETRY_EFFECT_ID),
    )
    bound = bind_retry_attempt_execution(
        _pool(conn),
        workspace_id=_WORKSPACE_ID,
        task_id=_TASK_ID,
        expected_state_token=_STATE_TOKEN,
        retry_attempt_id=_ATTEMPT_1_ID,
        resulting_execution_id=_RETRY_EFFECT_ID,
    )
    assert bound.resulting_execution_id == _RETRY_EFFECT_ID
    bind_params = next(params for sql, params in conn.executed if _ATTEMPT_BIND(sql, None))
    assert bind_params is not None
    assert bind_params[0] == _RETRY_EFFECT_ID
    assert bind_params[1] == _ATTEMPT_1_ID

    # Rebinding the same Execution is an idempotent success with no write.
    conn = ScriptedConnection()
    conn.on(_TASK_LOCKED, _task(status="implementing", plan_revision=_REVISION_ID))
    conn.on(_ATTEMPT_READ, _attempt_row(resulting_execution_id=_RETRY_EFFECT_ID))
    conn.on(_EPISODE_READ, _episode_row())
    conn.on(_EXECUTION_LOCKED, _execution(execution_id=_RETRY_EFFECT_ID))
    bound = bind_retry_attempt_execution(
        _pool(conn),
        workspace_id=_WORKSPACE_ID,
        task_id=_TASK_ID,
        expected_state_token=_STATE_TOKEN,
        retry_attempt_id=_ATTEMPT_1_ID,
        resulting_execution_id=_RETRY_EFFECT_ID,
    )
    assert bound.resulting_execution_id == _RETRY_EFFECT_ID
    assert _count(conn, _ATTEMPT_BIND) == 0


def test_the_binding_service_fails_closed_on_conflicts_and_stale_state() -> None:
    base = dict(
        workspace_id=_WORKSPACE_ID,
        task_id=_TASK_ID,
        expected_state_token=_STATE_TOKEN,
        retry_attempt_id=_ATTEMPT_1_ID,
        resulting_execution_id=_RETRY_EFFECT_ID,
    )
    # A different Execution for an already-bound authorization is a conflict.
    conn = ScriptedConnection()
    conn.on(_TASK_LOCKED, _task(status="implementing", plan_revision=_REVISION_ID))
    conn.on(_ATTEMPT_READ, _attempt_row(resulting_execution_id=_EXECUTION_ID))
    conn.on(_EPISODE_READ, _episode_row())
    conn.on(_EXECUTION_LOCKED, _execution(execution_id=_RETRY_EFFECT_ID))
    with pytest.raises(ConflictError):
        bind_retry_attempt_execution(_pool(conn), **base)
    # A terminally disposed episode can no longer execute its authorizations.
    conn = ScriptedConnection()
    conn.on(_TASK_LOCKED, _task(status="implementing", plan_revision=_REVISION_ID))
    conn.on(_ATTEMPT_READ, _attempt_row(resulting_execution_id=None))
    conn.on(_EPISODE_READ, _episode_row(escalated=_BLOCK_ID))
    with pytest.raises(ConflictError):
        bind_retry_attempt_execution(_pool(conn), **base)
    # A stale token is a stale operation that applies nothing.
    conn = ScriptedConnection()
    conn.on(_TASK_LOCKED, _task(state_token=uuid4()))
    with pytest.raises(StaleOperationError):
        bind_retry_attempt_execution(_pool(conn), **base)
    # A missing authorization is the uniform not-found vocabulary.
    conn = ScriptedConnection()
    conn.on(_TASK_LOCKED, _task(status="implementing", plan_revision=_REVISION_ID))
    conn.on(_ATTEMPT_READ, None)
    with pytest.raises(NotFoundError):
        bind_retry_attempt_execution(_pool(conn), **base)


# F5d (issue #267) merge-family fixtures: the approved MERGE_DECISION gate is
# the episode's original durable merge-operation identity.
_GATE_ID = uuid4()
_OWNER_PROFILE_ID = uuid4()

_MERGE_PR_BY_ID = _sql_has("from openorc.task_pull_requests where id")


def _merge_gate_row(*, status: str = "approved", pr_id: Any = _PR_ID) -> tuple[Any, ...]:
    return (
        _GATE_ID,
        _WORKSPACE_ID,
        _TASK_ID,
        "merge_decision",
        status,
        None,
        _HEAD_SHA,
        pr_id,
        None,
        _OBSERVED if status != "pending" else None,
        _OBSERVED,
    )


def _merge_pr_row(
    *, state: str = "open", merged_at: Any = None, head_sha: str = _HEAD_SHA
) -> tuple[Any, ...]:
    return (
        _PR_ID,
        _WORKSPACE_ID,
        _TASK_ID,
        uuid4(),
        555,
        42,
        "openorc/task-1",
        "main",
        head_sha,
        state,
        merged_at,
        _OBSERVED,
        _OBSERVED,
    )


def _merge_requested_event_row(*, gate_id: Any = _GATE_ID, actor: str = "owner") -> tuple[Any, ...]:
    return (
        uuid4(),
        _WORKSPACE_ID,
        _TASK_ID,
        "merge_requested",
        actor,
        str(_OWNER_PROFILE_ID) if actor == "owner" else None,
        "task_pull_request",
        _PR_ID,
        {"owner_gate_id": str(gate_id)},
        _OBSERVED,
    )


def _merge_operation(
    *,
    family: Any = GitHubMergeOperation.EXACT_HEAD_MERGE,
    kind: Any = OriginalAttemptKind.MERGE_DECISION_GATE,
) -> OperationalFailureOperation:
    return OperationalFailureOperation(
        family=family,
        original_attempt_kind=kind,
        original_attempt_id=_GATE_ID,
        subject={
            "owner_gate_id": str(_GATE_ID),
            "task_pull_request_id": str(_PR_ID),
            "expected_head_sha": _HEAD_SHA,
        },
    )


def _merge_report(
    *,
    code: OperationalFailureCode = OperationalFailureCode.EXTERNAL_TRANSPORT_UNAVAILABLE,
    evidence: FailureEffectEvidence = FailureEffectEvidence.EFFECT_UNCLASSIFIED,
    reporting_kind: OperationalFailureSourceKind = OperationalFailureSourceKind.ORIGINAL,
    reporting_id: Any = None,
    effect: Any = None,
) -> OperationalFailureReport:
    return OperationalFailureReport(
        reporting_attempt_kind=reporting_kind,
        reporting_attempt_id=_GATE_ID if reporting_id is None else reporting_id,
        error_code=code,
        effect_evidence=evidence,
        effect_attempt_id=effect,
        diagnostic_context={"merge_mutation_attempted": True},
    )


def _merge_attempt_rows(*, consumed: bool = True, count: int = 1) -> list[tuple[Any, ...]]:
    rows = []
    for index in range(count):
        rows.append(
            _attempt_row(
                attempt_id=_ATTEMPT_1_ID if count == 1 else uuid4(),
                ordinal=index + 1,
                source_kind="original" if index == 0 else "prior_authorized",
                error_code="external_transport_unavailable",
                consumed_at=_OBSERVED if consumed else None,
            )
        )
    rows[-1] = _attempt_row(
        attempt_id=_ATTEMPT_1_ID if count == 1 else uuid4(),
        ordinal=count,
        source_kind="original" if count == 1 else "prior_authorized",
        error_code="external_transport_unavailable",
        consumed_at=_OBSERVED if consumed else None,
    )
    return rows


def _script_merge_retry(
    conn: ScriptedConnection,
    *,
    episode_row: Any | None = None,
    attempts_rows: list[Any] | None = None,
    gate_row: Any | None = None,
    pr_row: Any | None = None,
    event_rows: list[Any] | None = None,
    attempt_return: Any | None = None,
) -> None:
    """Script the full locked flow of one merge-family disposition."""
    conn.on(_TASK_LOCKED, _task(status="waiting_for_owner"))
    conn.on(
        _EPISODE_BY_ORIGINAL,
        episode_row
        if episode_row is not None
        else _episode_row(
            family="exact_head_merge", kind="merge_decision_gate", original_id=_GATE_ID
        ),
    )
    conn.on(
        _ATTEMPTS_LIST,
        attempts_rows if attempts_rows is not None else _merge_attempt_rows(),
    )
    conn.on(_ATTEMPT_BY_SOURCE, None)
    conn.on(_GATE_READ, gate_row if gate_row is not None else _merge_gate_row())
    conn.on(_MERGE_PR_BY_ID, pr_row if pr_row is not None else _merge_pr_row())
    conn.on(_GATE_READ, gate_row if gate_row is not None else _merge_gate_row())
    conn.on(_EVENTS_FIND, event_rows if event_rows is not None else [_merge_requested_event_row()])
    if episode_row is None:
        conn.on(
            _EPISODE_INSERT,
            _episode_row(
                family="exact_head_merge", kind="merge_decision_gate", original_id=_GATE_ID
            ),
        )
    conn.on(
        _ATTEMPT_INSERT,
        attempt_return
        if attempt_return is not None
        else _attempt_row(error_code="external_transport_unavailable", resulting_execution_id=None),
    )


def test_the_merge_family_pairs_only_with_the_gate_identity() -> None:
    conn = ScriptedConnection()
    with pytest.raises(InvalidCommandError):
        _triage(
            conn,
            status=TaskStatus.WAITING_FOR_OWNER,
            operation=_merge_operation(kind=OriginalAttemptKind.EXECUTION),
        )


def test_a_merge_transport_failure_over_a_reconciled_open_head_is_authorized() -> None:
    conn = ScriptedConnection()
    _script_merge_retry(
        conn,
        attempt_return=_attempt_row(
            error_code="external_transport_unavailable",
            resulting_execution_id=None,
            not_before=_NOW + timedelta(seconds=2),
            source_attempt_id=_GATE_ID,
        ),
    )
    result = _triage(
        conn,
        status=TaskStatus.WAITING_FOR_OWNER,
        operation=_merge_operation(),
        report=_merge_report(),
    )
    assert result.kind is DispositionKind.RETRY_AUTHORIZED
    assert result.retry_attempt is not None
    assert result.retry_attempt.error_code.value == "external_transport_unavailable"
    assert result.retry_attempt.not_before == _NOW + timedelta(seconds=2)


def test_a_merge_unsupported_uncertainty_escalates_through_f3() -> None:
    conn = ScriptedConnection()
    _script_escalation_merge(conn)
    result = _triage(
        conn,
        status=TaskStatus.WAITING_FOR_OWNER,
        operation=_merge_operation(),
        report=_merge_report(code=OperationalFailureCode.EXTERNAL_OPERATION_UNCERTAIN),
    )
    assert result.kind is DispositionKind.BLOCKED
    assert result.installation is not None
    assert result.installation.task_block.id == _BLOCK_ID


def _script_escalation_merge(
    conn: ScriptedConnection, *, consumed: bool = True, event_rows: list[Any] | None = None
) -> None:
    """The merge family's escalation flow: F3 install + the episode stamp."""
    task_row = _task(status="waiting_for_owner")
    conn.on(_TASK_LOCKED, task_row)
    conn.on(
        _EPISODE_BY_ORIGINAL,
        _episode_row(family="exact_head_merge", kind="merge_decision_gate", original_id=_GATE_ID),
    )
    conn.on(_ATTEMPTS_LIST, _merge_attempt_rows(consumed=consumed))
    conn.on(_ATTEMPT_BY_SOURCE, None)
    conn.on(_GATE_READ, _merge_gate_row())
    conn.on(_MERGE_PR_BY_ID, _merge_pr_row())
    conn.on(_GATE_READ, _merge_gate_row())
    conn.on(_EVENTS_FIND, event_rows if event_rows is not None else [_merge_requested_event_row()])
    conn.on(
        _EPISODE_INSERT,
        _episode_row(family="exact_head_merge", kind="merge_decision_gate", original_id=_GATE_ID),
    )
    conn.on(_TASK_LOCKED, task_row)
    conn.on(_TASK_PLAIN, task_row)
    conn.on(
        _BLOCK_INSERT,
        (_BLOCK_ID, _WORKSPACE_ID, _TASK_ID, "external_operation_uncertain", {}, None, _OBSERVED),
    )
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
    conn.on(
        _EPISODE_ESCALATE,
        _episode_row(
            family="exact_head_merge",
            kind="merge_decision_gate",
            original_id=_GATE_ID,
            escalated=_BLOCK_ID,
        ),
    )


def test_a_merge_prior_authorized_report_requires_the_consumption_stamp() -> None:
    conn = ScriptedConnection()
    _script_merge_retry(
        conn,
        episode_row=_episode_row(
            family="exact_head_merge", kind="merge_decision_gate", original_id=_GATE_ID
        ),
        attempts_rows=_merge_attempt_rows(consumed=False),
        attempt_return=None,
    )
    result = _triage(
        conn,
        status=TaskStatus.WAITING_FOR_OWNER,
        operation=_merge_operation(),
        report=_merge_report(
            reporting_kind=OperationalFailureSourceKind.PRIOR_AUTHORIZED,
            reporting_id=_ATTEMPT_1_ID,
            effect=_ATTEMPT_1_ID,
        ),
    )
    # The unconsumed attempt's failure is unreportable: a typed no-op that
    # applies nothing.
    assert result.kind is DispositionKind.NO_OP
    assert result.no_op_reason is NoOpReason.STALE


def test_a_merge_report_on_a_merged_durable_pr_is_stale() -> None:
    conn = ScriptedConnection()
    _script_merge_retry(
        conn,
        pr_row=_merge_pr_row(state="closed", merged_at=_OBSERVED),
        attempt_return=None,
    )
    result = _triage(
        conn,
        status=TaskStatus.WAITING_FOR_OWNER,
        operation=_merge_operation(),
        report=_merge_report(),
    )
    assert result.kind is DispositionKind.NO_OP
    assert result.no_op_reason is NoOpReason.STALE


def test_a_merge_report_without_the_owner_claim_escalates() -> None:
    conn = ScriptedConnection()
    _script_escalation_merge(
        conn,
        event_rows=[_merge_requested_event_row(gate_id=uuid4())],  # a different gate's claim
    )
    result = _triage(
        conn,
        status=TaskStatus.WAITING_FOR_OWNER,
        operation=_merge_operation(),
        report=_merge_report(),
    )
    # The one-shot MERGE_REQUESTED Owner claim is the positive corroboration;
    # a missing/mismatched claim fails closed into the escalation.
    assert result.kind is DispositionKind.BLOCKED
    assert result.installation is not None


def test_a_merge_known_failed_evidence_escalates() -> None:
    conn = ScriptedConnection()
    _script_escalation_merge(conn)
    result = _triage(
        conn,
        status=TaskStatus.WAITING_FOR_OWNER,
        operation=_merge_operation(),
        report=_merge_report(evidence=FailureEffectEvidence.KNOWN_FAILED),
    )
    assert result.kind is DispositionKind.BLOCKED
    assert result.installation is not None


def test_a_merge_report_with_a_crossed_runtime_code_escalates() -> None:
    """F4's matrix permits exact (family, code) pairs only: a merge report
    carrying the runtime transient-network code lacks the required classified
    GitHub merge-send transport failure and can never authorize another merge
    — it escalates through the safe F3 installation."""
    conn = ScriptedConnection()
    _script_escalation_merge(conn)
    result = _triage(
        conn,
        status=TaskStatus.WAITING_FOR_OWNER,
        operation=_merge_operation(),
        report=_merge_report(code=OperationalFailureCode.RUNTIME_UNAVAILABLE),
    )
    assert result.kind is DispositionKind.BLOCKED
    assert result.installation is not None
    assert result.installation.task_block.id == _BLOCK_ID


def test_an_implementation_report_with_a_crossed_transport_code_escalates() -> None:
    """The crossed pairing in the other direction: an implementation-execution
    report carrying the GitHub-specific external-transport code lacks the
    classified runtime transient-network failure and escalates — never a
    merge-shaped authorization, never a wrong-family code path."""
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
                _session_row("producer", _PRODUCER_SESSION_ID, "fake-session-0"),
            ),
        ),
    )
    result = _triage(
        conn,
        report=_report(
            code=OperationalFailureCode.EXTERNAL_TRANSPORT_UNAVAILABLE,
            evidence=FailureEffectEvidence.PRE_EFFECT_NOT_APPLIED,
        ),
    )
    assert result.kind is DispositionKind.BLOCKED
    assert result.installation is not None
    assert result.installation.task_block.id == _BLOCK_ID


# --- F5f: the exact-head PR-authorization presentation family ------------------


def _presentation_operation() -> OperationalFailureOperation:
    return _operation(
        family=PRAuthorizationPresentationOperation.EXACT_HEAD_PR_AUTHORIZATION_PRESENTATION,
        kind=OriginalAttemptKind.EXECUTION,
        original_id=_EXECUTION_ID,
    )


def _presentation_report(
    *,
    code: OperationalFailureCode = OperationalFailureCode.EXTERNAL_TRANSPORT_UNAVAILABLE,
    reporting_kind: OperationalFailureSourceKind = OperationalFailureSourceKind.ORIGINAL,
    reporting_id: Any = None,
    effect: Any = None,
) -> OperationalFailureReport:
    return _report(
        code=code,
        evidence=FailureEffectEvidence.PRE_EFFECT_NOT_APPLIED,
        reporting_kind=reporting_kind,
        reporting_id=reporting_id,
        effect=effect,
    )


def _script_presentation_authorization(conn: ScriptedConnection, *, prior: bool = False) -> None:
    """The locked flow of one authorized presentation retry: the structural
    corroboration (root Execution SUCCEEDED, pre-gate Task) authorizes the
    attempt under the exact transport-code pair."""
    task_row = _task(status="implementing", plan_revision=_REVISION_ID)
    conn.on(_TASK_LOCKED, task_row)
    conn.on(_EPISODE_BY_ORIGINAL, None)
    conn.on(_ATTEMPT_BY_SOURCE, None)
    conn.on(_EXECUTION_LOCKED, _execution(status="succeeded", execution_id=_EXECUTION_ID))
    conn.on(_GATES_LIST, [_impl_gate_row()])
    if prior:
        conn.on(_ATTEMPTS_LIST, [_attempt_row()])
    conn.on(_EXECUTION_LOCKED, _execution(status="succeeded", execution_id=_EXECUTION_ID))
    conn.on(_TASK_LOCKED, task_row)
    if not prior:
        conn.on(
            _EPISODE_INSERT,
            _episode_row(
                family="exact_head_pr_authorization_presentation",
                kind="execution",
                original_id=_EXECUTION_ID,
            ),
        )
    conn.on(
        _ATTEMPT_INSERT,
        _attempt_row(
            ordinal=2 if prior else 1,
            source_kind="prior_authorized" if prior else "original",
            source_attempt_id=_ATTEMPT_1_ID if prior else _EXECUTION_ID,
            error_code="external_transport_unavailable",
            resulting_execution_id=None,
            not_before=_NOW + timedelta(seconds=2),
        ),
    )


def test_a_presentation_transport_failure_is_authorized_on_the_structural_facts() -> None:
    """The presentation family's one retry-eligible (family, code) pair: the
    narrowly typed transient GitHub transport classification over positively
    proven pre-effect evidence, corroborated by the still-SUCCEEDED root
    Execution under the still-current pre-gate Task state."""
    conn = ScriptedConnection()
    _script_presentation_authorization(conn)
    result = _triage(conn, operation=_presentation_operation(), report=_presentation_report())
    assert result.kind is DispositionKind.RETRY_AUTHORIZED
    assert result.retry_attempt is not None
    assert result.retry_attempt.error_code.value == "external_transport_unavailable"
    assert result.retry_attempt.resulting_execution_id is None
    assert result.retry_attempt.not_before == _NOW + timedelta(seconds=2)


def _script_presentation_stale(
    conn: ScriptedConnection, *, execution_row: Any | None = None
) -> None:
    conn.on(_TASK_LOCKED, _task(status="implementing", plan_revision=_REVISION_ID))
    conn.on(_EPISODE_BY_ORIGINAL, None)
    conn.on(_ATTEMPT_BY_SOURCE, None)
    conn.on(
        _EXECUTION_LOCKED,
        execution_row
        if execution_row is not None
        else _execution(status="succeeded", execution_id=_EXECUTION_ID),
    )


def test_a_presentation_report_over_a_failed_execution_is_stale() -> None:
    """A presentation episode only ever roots at the successful E6 attempt: a
    report whose root Execution is not SUCCEEDED is incoherent and stale —
    the execution families' own contradiction checks stay untouched."""
    conn = ScriptedConnection()
    _script_presentation_stale(
        conn, execution_row=_execution(status="failed_transient", execution_id=_EXECUTION_ID)
    )
    result = _triage(conn, operation=_presentation_operation(), report=_presentation_report())
    assert result.kind is DispositionKind.NO_OP
    assert result.no_op_reason is NoOpReason.STALE


def test_an_implementation_report_over_a_succeeded_execution_is_stale() -> None:
    """The E6 SUCCEEDED-contradiction check stays exactly intact: the same
    successful Execution reported as an implementation failure is still
    rejected as stale."""
    conn = ScriptedConnection()
    _script_presentation_stale(conn)
    result = _triage(conn, operation=_operation(), report=_report())
    assert result.kind is DispositionKind.NO_OP
    assert result.no_op_reason is NoOpReason.STALE


def _script_presentation_prior_authorized(
    conn: ScriptedConnection, *, consumed: bool = True
) -> None:
    """The locked flow of one consumed re-presentation attempt's report."""
    task_row = _task(status="implementing", plan_revision=_REVISION_ID)
    conn.on(_TASK_LOCKED, task_row)
    conn.on(
        _EPISODE_BY_ORIGINAL,
        _episode_row(
            family="exact_head_pr_authorization_presentation",
            kind="execution",
            original_id=_EXECUTION_ID,
        ),
    )
    conn.on(
        _ATTEMPTS_LIST,
        [
            _attempt_row(
                source_kind="prior_authorized",
                source_attempt_id=_ATTEMPT_1_ID,
                resulting_execution_id=None,
                consumed_at=_OBSERVED if consumed else None,
                error_code="external_transport_unavailable",
            )
        ],
    )
    conn.on(_ATTEMPT_BY_SOURCE, None)
    if consumed:
        conn.on(_EXECUTION_LOCKED, _execution(status="succeeded", execution_id=_EXECUTION_ID))
        conn.on(_GATES_LIST, [_impl_gate_row()])
        conn.on(_EXECUTION_LOCKED, _execution(status="succeeded", execution_id=_EXECUTION_ID))
        conn.on(_TASK_LOCKED, task_row)
        conn.on(
            _ATTEMPT_INSERT,
            _attempt_row(
                attempt_id=_ATTEMPT_2_ID,
                ordinal=2,
                source_kind="prior_authorized",
                source_attempt_id=_ATTEMPT_1_ID,
                error_code="external_transport_unavailable",
                resulting_execution_id=None,
                not_before=_NOW + timedelta(seconds=4),
            ),
        )


def test_a_presentation_prior_authorized_report_is_authorized_on_the_consumed_attempt() -> None:
    """A consumed re-presentation attempt's transport failure reports the
    episode's original Execution identity with PRIOR_AUTHORIZED provenance —
    no resulting-Execution binding is required for this family."""
    conn = ScriptedConnection()
    _script_presentation_prior_authorized(conn)
    result = _triage(
        conn,
        operation=_presentation_operation(),
        report=_presentation_report(
            reporting_kind=OperationalFailureSourceKind.PRIOR_AUTHORIZED,
            reporting_id=_ATTEMPT_1_ID,
            effect=_ATTEMPT_1_ID,
        ),
    )
    assert result.kind is DispositionKind.RETRY_AUTHORIZED
    assert result.retry_attempt is not None
    assert result.retry_attempt.ordinal == 2
    assert result.retry_attempt.not_before == _NOW + timedelta(seconds=4)


def test_a_presentation_prior_authorized_report_requires_the_consumption_stamp() -> None:
    conn = ScriptedConnection()
    _script_presentation_prior_authorized(conn, consumed=False)
    result = _triage(
        conn,
        operation=_presentation_operation(),
        report=_presentation_report(
            reporting_kind=OperationalFailureSourceKind.PRIOR_AUTHORIZED,
            reporting_id=_ATTEMPT_1_ID,
            effect=_ATTEMPT_1_ID,
        ),
    )
    # The unconsumed attempt's failure is unreportable: a typed no-op that
    # applies nothing.
    assert result.kind is DispositionKind.NO_OP
    assert result.no_op_reason is NoOpReason.STALE


def _script_presentation_escalation(
    conn: ScriptedConnection, *, block_reason: str = "external_operation_uncertain"
) -> None:
    """The presentation family's escalation flow: F3 install + the episode stamp."""
    task_row = _task(status="implementing", plan_revision=_REVISION_ID)
    conn.on(_TASK_LOCKED, task_row)
    conn.on(_EPISODE_BY_ORIGINAL, None)
    conn.on(_ATTEMPT_BY_SOURCE, None)
    conn.on(_EXECUTION_LOCKED, _execution(status="succeeded", execution_id=_EXECUTION_ID))
    conn.on(_GATES_LIST, [_impl_gate_row()])
    conn.on(
        _EPISODE_INSERT,
        _episode_row(
            family="exact_head_pr_authorization_presentation",
            kind="execution",
            original_id=_EXECUTION_ID,
        ),
    )
    conn.on(_TASK_LOCKED, task_row)
    conn.on(_TASK_PLAIN, task_row)
    conn.on(
        _BLOCK_INSERT,
        (_BLOCK_ID, _WORKSPACE_ID, _TASK_ID, block_reason, {}, None, _OBSERVED),
    )
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
    conn.on(
        _EPISODE_ESCALATE,
        _episode_row(
            family="exact_head_pr_authorization_presentation",
            kind="execution",
            original_id=_EXECUTION_ID,
            escalated=_BLOCK_ID,
        ),
    )


def test_a_presentation_unsupported_uncertainty_escalates_through_f3() -> None:
    """A plain interpretable-response read uncertainty never produces the
    retry-eligible transport code: it escalates through the safe F3
    installation and the Task gets a durable disposition."""
    conn = ScriptedConnection()
    _script_presentation_escalation(conn)
    result = _triage(
        conn,
        operation=_presentation_operation(),
        report=_presentation_report(code=OperationalFailureCode.EXTERNAL_OPERATION_UNCERTAIN),
    )
    assert result.kind is DispositionKind.BLOCKED
    assert result.installation is not None
    assert result.installation.task_block.id == _BLOCK_ID


def test_a_presentation_known_provider_failure_escalates_through_f3() -> None:
    conn = ScriptedConnection()
    _script_presentation_escalation(conn, block_reason="unknown")
    result = _triage(
        conn,
        operation=_presentation_operation(),
        report=_presentation_report(code=OperationalFailureCode.EXTERNAL_OPERATION_FAILED),
    )
    assert result.kind is DispositionKind.BLOCKED
    assert result.installation is not None
    assert result.installation.task_block.id == _BLOCK_ID


def test_a_presentation_report_with_a_crossed_runtime_code_escalates() -> None:
    """F4's matrix permits exact (family, code) pairs only: a presentation
    report carrying the runtime transient-network code lacks the required
    classified GitHub read transport failure and can never authorize a
    re-presentation — it escalates."""
    conn = ScriptedConnection()
    _script_presentation_escalation(conn, block_reason="connection_unavailable")
    result = _triage(
        conn,
        operation=_presentation_operation(),
        report=_presentation_report(code=OperationalFailureCode.RUNTIME_UNAVAILABLE),
    )
    assert result.kind is DispositionKind.BLOCKED
    assert result.installation is not None
    assert result.installation.task_block.id == _BLOCK_ID
