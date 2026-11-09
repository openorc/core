"""Focused F8f Owner Continue tests (issue #291).

The authorization boundary's guard/refusal behavior, the atomic composition
committing the refreshed effective spending threshold with the restored Task
authority, the immutable grant audit carried on the resolved gate's event,
and the safety refusals (unknown spend, missing budget, zero-target context,
unsettled interruption, stale state, duplicate one-shot authorization).
Database constraint behavior is proven by the integration-marked suite.
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any, cast

import pytest
from psycopg.errors import UniqueViolation

from openorc.domain.blocks import TaskBlock, TaskBlockReason
from openorc.domain.connections import WorkflowRole
from openorc.domain.executions import Execution, ExecutionStatus
from openorc.domain.gates import OwnerGate, OwnerGateStatus, OwnerGateType
from openorc.domain.intents import (
    TaskInterruptIntent,
    TaskInterruptIntentStatus,
    TaskResumeIntent,
    TaskResumeIntentStatus,
)
from openorc.domain.sessions import TaskAgentSession, TaskSessionLifecycleStatus
from openorc.domain.tasks import Task, TaskStatus
from openorc.persistence.gates import OwnerGateResolution, OwnerGateResolutionOutcome
from openorc.persistence.pool import DatabasePool
from openorc.services import task_owner_cost_continue as continue_module
from openorc.services import task_owner_resume as resume_module
from openorc.services.errors import ConflictError
from openorc.services.task_owner_cost_continue import continue_task_after_cost_limit

_WORKSPACE_ID = uuid.uuid4()
_TASK_ID = uuid.uuid4()
_SESSION_ID = uuid.uuid4()
_EXTERNAL_SESSION_ID = "cline-session-1"
_BLOCK_ID = uuid.uuid4()
_GATE_ID = uuid.uuid4()
_OWNER_PROFILE_ID = uuid.uuid4()
_STATE_TOKEN = uuid.uuid4()
_EXECUTION_ID = uuid.uuid4()

_BUDGET = Decimal("10.00")
_KNOWN_SPEND = Decimal("12.00")
_REFRESHED_THRESHOLD = _KNOWN_SPEND + _BUDGET

_NOW = datetime(2026, 11, 9, 12, 0, 0, tzinfo=UTC)
_POOL = cast(DatabasePool, None)


def _task(
    *,
    status: TaskStatus = TaskStatus.BLOCKED,
    token: Any = _STATE_TOKEN,
    budget: Decimal | None = _BUDGET,
) -> Task:
    return Task(
        id=_TASK_ID,
        workspace_id=_WORKSPACE_ID,
        repository_id=uuid.uuid4(),
        github_issue_id=123,
        github_issue_number=7,
        status=status,
        archived_at=None,
        queued_at=None,
        canonical_feature_branch=None,
        state_token=token,
        current_plan_revision_id=None,
        current_owner_gate_id=None,
        source_requirements_fingerprint="a" * 64,
        created_at=_NOW,
        updated_at=_NOW,
        authorized_task_budget_usd=budget,
    )


def _session() -> TaskAgentSession:
    return TaskAgentSession(
        id=_SESSION_ID,
        workspace_id=_WORKSPACE_ID,
        task_id=_TASK_ID,
        role=WorkflowRole.PRODUCER,
        connection_id=uuid.uuid4(),
        external_session_id=_EXTERNAL_SESSION_ID,
        lifecycle_status=TaskSessionLifecycleStatus.READY,
        effective_config_snapshot={"repository_base": "/repo/base"},
        reported_provider=None,
        reported_model=None,
        reported_runtime_version=None,
        initialized_at=_NOW,
        ended_at=None,
        created_at=_NOW,
        updated_at=_NOW,
    )


def _gate(gate_type: OwnerGateType = OwnerGateType.COST_LIMIT) -> OwnerGate:
    return OwnerGate(
        id=_GATE_ID,
        workspace_id=_WORKSPACE_ID,
        task_id=_TASK_ID,
        gate_type=gate_type,
        status=OwnerGateStatus.PENDING,
        plan_revision_id=None,
        subject_head_sha=None,
        task_pull_request_id=None,
        task_block_id=_BLOCK_ID,
        decided_at=None,
        created_at=_NOW,
    )


def _block(context: dict[str, Any] | None = None) -> TaskBlock:
    return TaskBlock(
        id=_BLOCK_ID,
        workspace_id=_WORKSPACE_ID,
        task_id=_TASK_ID,
        reason=TaskBlockReason.COST_LIMIT,
        context=context
        or {
            "control": "owner_abort",
            "reason_provenance": "owner_authorized_task_budget",
            "observed_cost_usd": str(_KNOWN_SPEND),
            "effective_threshold_usd": str(_BUDGET),
            "authorized_budget_usd": str(_BUDGET),
            "aggregate_includes_unobserved_sessions": False,
            "target_roles": [WorkflowRole.PRODUCER.value],
            "task_agent_session_ids": [str(_SESSION_ID)],
        },
        resolved_at=None,
        created_at=_NOW,
    )


def _settled_intent() -> TaskInterruptIntent:
    return TaskInterruptIntent(
        id=uuid.uuid4(),
        workspace_id=_WORKSPACE_ID,
        task_id=_TASK_ID,
        task_agent_session_id=_SESSION_ID,
        task_block_id=_BLOCK_ID,
        workflow_role=WorkflowRole.PRODUCER,
        external_session_id=_EXTERNAL_SESSION_ID,
        requested_by_profile_id=_OWNER_PROFILE_ID,
        status=TaskInterruptIntentStatus.SETTLED,
        outcome={},
        created_at=_NOW,
        updated_at=_NOW,
    )


def _running_execution() -> Execution:
    return Execution(
        id=_EXECUTION_ID,
        workspace_id=_WORKSPACE_ID,
        task_id=_TASK_ID,
        producer_session_id=_SESSION_ID,
        execution_number=1,
        status=ExecutionStatus.RUNNING,
        created_at=_NOW,
        updated_at=_NOW,
    )


def _resume_record() -> TaskResumeIntent:
    return TaskResumeIntent(
        id=uuid.uuid4(),
        workspace_id=_WORKSPACE_ID,
        task_id=_TASK_ID,
        task_agent_session_id=_SESSION_ID,
        task_block_id=_BLOCK_ID,
        workflow_role=WorkflowRole.PRODUCER,
        external_session_id=_EXTERNAL_SESSION_ID,
        requested_by_profile_id=_OWNER_PROFILE_ID,
        status=TaskResumeIntentStatus.AUTHORIZED,
        routing={
            "control": "owner_resume",
            "operation": "implementation_execution",
            "expected_response_family": "implementation_result",
            "subject_kind": "execution",
            "subject_id": str(_EXECUTION_ID),
            "restored_status": TaskStatus.IMPLEMENTING.value,
            "authorized_state_token": str(_STATE_TOKEN),
        },
        outcome={},
        created_at=_NOW,
        updated_at=_NOW,
    )


class _RecordingCalls:
    def __init__(self) -> None:
        self.resolve: dict[str, Any] | None = None
        self.restored: dict[str, Any] | None = None
        self.created: dict[str, Any] | None = None
        self.event: dict[str, Any] | None = None


def _costs(known_total_usd: Any) -> Any:
    return type(
        "Costs", (), {"known_total_usd": known_total_usd, "has_unobserved_sessions": False}
    )()


def _wire_authorization(monkeypatch: pytest.MonkeyPatch, calls: _RecordingCalls) -> None:
    from contextlib import contextmanager

    @contextmanager
    def _fake_tx(pool):
        yield pool

    monkeypatch.setattr(continue_module, "composed_transaction", _fake_tx)
    task = _task()
    monkeypatch.setattr(
        resume_module, "require_account_operational", lambda pool, *, profile_id: None
    )
    monkeypatch.setattr(
        resume_module,
        "require_workspace_task",
        lambda pool, *, profile_id, workspace_id, task_id: None,
    )
    monkeypatch.setattr(
        resume_module.task_records,
        "get_task_for_update",
        lambda pool, task_id: task if task_id == _TASK_ID else None,
    )
    monkeypatch.setattr(
        resume_module,
        "require_current_task",
        lambda pool, *, workspace_id, task_id, expected_state_token: task,
    )
    monkeypatch.setattr(
        resume_module,
        "require_current_owner_gate",
        lambda pool, *, task, owner_gate_id: _gate(),
    )
    monkeypatch.setattr(
        resume_module,
        "require_owner_gate_unresolved_block_subject",
        lambda pool, *, task, gate: _block(),
    )
    monkeypatch.setattr(
        resume_module.session_records,
        "get_task_agent_session",
        lambda pool, *, task_id, role: _session(),
    )
    monkeypatch.setattr(
        resume_module,
        "list_task_interrupt_intents",
        lambda pool, *, task_id: [_settled_intent()],
    )
    monkeypatch.setattr(
        resume_module.execution_records,
        "list_task_executions",
        lambda pool, *, task_id: [_running_execution()],
    )
    monkeypatch.setattr(
        resume_module.send_claim_records,
        "list_task_runtime_send_claims",
        lambda pool, *, task_id: [],
    )
    monkeypatch.setattr(
        resume_module,
        "get_task_planning_bootstrap_evidence",
        lambda pool, *, task_id: None,
    )
    monkeypatch.setattr(
        resume_module.review_records,
        "list_task_review_loops",
        lambda pool, *, task_id, status: [],
    )

    def _resolve(pool, *, owner_gate_id, outcome, expected_task_state_token):
        calls.resolve = {
            "owner_gate_id": owner_gate_id,
            "outcome": outcome,
            "expected_task_state_token": expected_task_state_token,
        }
        restored_task = _task(status=TaskStatus.IMPLEMENTING, token=uuid.uuid4())
        return OwnerGateResolution(
            outcome=OwnerGateResolutionOutcome.RESOLVED,
            gate=_gate(),
            task=restored_task,
        )

    monkeypatch.setattr(continue_module, "resolve_gate_records", _resolve)
    monkeypatch.setattr(
        continue_module,
        "require_resolution_applied",
        lambda resolution: (resolution.gate, resolution.task),
    )

    def _restore(
        pool, task_id, *, expected_state_token, restored_status, effective_cost_threshold_usd=None
    ):
        calls.restored = {
            "expected_state_token": expected_state_token,
            "restored_status": restored_status,
            "effective_cost_threshold_usd": effective_cost_threshold_usd,
        }
        return _task(status=restored_status, token=uuid.uuid4())

    monkeypatch.setattr(continue_module.task_records, "restore_blocked_task_status", _restore)
    monkeypatch.setattr(
        continue_module.block_records,
        "resolve_task_block",
        lambda pool, *, task_block_id: _block(),
    )
    monkeypatch.setattr(
        continue_module.session_records,
        "sum_task_retained_session_costs",
        lambda pool, *, task_id: _costs(_KNOWN_SPEND),
    )

    def _event(pool, **kwargs):
        calls.event = dict(kwargs)

    monkeypatch.setattr(continue_module, "record_owner_gate_resolved_event", _event)

    def _create(pool, **kwargs):
        calls.created = dict(kwargs)
        return _resume_record()

    monkeypatch.setattr(continue_module, "create_task_resume_intent", _create)


def _continue() -> Any:
    return continue_task_after_cost_limit(
        _POOL,
        profile_id=_OWNER_PROFILE_ID,
        workspace_id=_WORKSPACE_ID,
        task_id=_TASK_ID,
        owner_gate_id=_GATE_ID,
        expected_state_token=_STATE_TOKEN,
    )


def test_the_continue_resolves_the_episode_commits_the_refreshed_threshold_and_audits_the_grant(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = _RecordingCalls()
    _wire_authorization(monkeypatch, calls)

    outcome = _continue()

    assert calls.resolve is not None
    assert calls.resolve["outcome"] is OwnerGateStatus.APPROVED
    assert calls.resolve["expected_task_state_token"] == _STATE_TOKEN
    # The refreshed allowance committed IN the guarded restore statement:
    # latest retained known spend + the frozen original budget.
    assert calls.restored is not None
    assert calls.restored["restored_status"] is TaskStatus.IMPLEMENTING
    assert calls.restored["effective_cost_threshold_usd"] == _REFRESHED_THRESHOLD
    assert outcome.effective_cost_threshold_usd == _REFRESHED_THRESHOLD
    assert outcome.task.status is TaskStatus.IMPLEMENTING
    # The immutable grant audit rides the resolved gate's OWNER event.
    assert calls.event is not None
    assert calls.event["outcome"] is OwnerGateStatus.APPROVED
    grant = calls.event["cost_allowance_refresh"]
    assert grant.observed_cost_usd == _KNOWN_SPEND
    assert grant.authorized_budget_usd == _BUDGET
    assert grant.effective_threshold_usd == _REFRESHED_THRESHOLD
    # The one-shot session-resume record addresses the exact interrupted
    # session binding with the post-restore token.
    assert calls.created is not None
    assert calls.created["task_agent_session_id"] == _SESSION_ID
    assert calls.created["task_block_id"] == _BLOCK_ID
    assert calls.created["external_session_id"] == _EXTERNAL_SESSION_ID
    assert outcome.resume.status is TaskResumeIntentStatus.AUTHORIZED


def test_a_non_cost_limit_gate_refuses_the_continue(monkeypatch: pytest.MonkeyPatch) -> None:
    calls = _RecordingCalls()
    _wire_authorization(monkeypatch, calls)
    monkeypatch.setattr(
        resume_module,
        "require_current_owner_gate",
        lambda pool, *, task, owner_gate_id: _gate(OwnerGateType.OWNER_ABORT),
    )

    with pytest.raises(ConflictError, match="cost_limit"):
        _continue()

    assert calls.restored is None and calls.created is None


def test_an_unknown_latest_spend_refuses_the_continue_without_any_grant(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = _RecordingCalls()
    _wire_authorization(monkeypatch, calls)
    monkeypatch.setattr(
        continue_module.session_records,
        "sum_task_retained_session_costs",
        lambda pool, *, task_id: _costs(None),
    )

    with pytest.raises(ConflictError, match="unknown"):
        _continue()

    assert calls.resolve is None and calls.restored is None and calls.created is None


def test_a_missing_authorized_budget_refuses_the_continue(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = _RecordingCalls()
    _wire_authorization(monkeypatch, calls)
    budgetless = _task(budget=None)
    monkeypatch.setattr(
        resume_module,
        "require_current_task",
        lambda pool, *, workspace_id, task_id, expected_state_token: budgetless,
    )

    with pytest.raises(ConflictError, match="no authorized task budget"):
        _continue()

    assert calls.restored is None and calls.created is None


def test_a_zero_target_block_context_refuses_the_continue(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = _RecordingCalls()
    _wire_authorization(monkeypatch, calls)
    empty_context = dict(_block().context)
    empty_context["target_roles"] = []
    empty_context["task_agent_session_ids"] = []
    monkeypatch.setattr(
        resume_module,
        "require_owner_gate_unresolved_block_subject",
        lambda pool, *, task, gate: _block(context=empty_context),
    )

    with pytest.raises(ConflictError, match="incoherent target facts"):
        _continue()

    assert calls.restored is None and calls.created is None


def test_an_unsettled_interruption_refuses_the_continue(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = _RecordingCalls()
    _wire_authorization(monkeypatch, calls)
    unsettled = _settled_intent()
    object.__setattr__(unsettled, "status", TaskInterruptIntentStatus.UNSETTLED)
    monkeypatch.setattr(
        resume_module,
        "list_task_interrupt_intents",
        lambda pool, *, task_id: [unsettled],
    )

    with pytest.raises(ConflictError, match="not positively settled"):
        _continue()

    assert calls.restored is None and calls.created is None


def test_a_stale_restore_refuses_the_continue(monkeypatch: pytest.MonkeyPatch) -> None:
    calls = _RecordingCalls()
    _wire_authorization(monkeypatch, calls)
    monkeypatch.setattr(
        continue_module.task_records,
        "restore_blocked_task_status",
        lambda pool, task_id, **kwargs: None,
    )

    with pytest.raises(ConflictError, match="could not be restored"):
        _continue()

    assert calls.created is None


def test_a_duplicate_one_shot_authorization_refuses_the_continue(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = _RecordingCalls()
    _wire_authorization(monkeypatch, calls)

    def _duplicate(pool, **kwargs):
        raise UniqueViolation("dup")

    monkeypatch.setattr(continue_module, "create_task_resume_intent", _duplicate)

    with pytest.raises(ConflictError, match="one-shot"):
        _continue()

    # Nothing is dispatched and no partial grant survives the rollback: the
    # recorded restore belongs to the rolled-back composition only.
    assert calls.created is None
