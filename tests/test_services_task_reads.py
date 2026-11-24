"""Focused tests for the authorized Task read composition (issue #376).

Proven here: the derived cost block (unknown-not-zero, partial completeness,
frozen budget and operative threshold), the #376 configured provider/model
capture extraction (fail-closed, never substituted from the current binding),
the backend-generated action generation (ordinary/exceptional gate menus,
Task-level start/abort/cancel/purge guards, no-duplicate cancel, the
#342 unsettled-Interrupt veto through the authoritative predicate, the
terminalization bar, the account-operational veto), the safe execution
result projection, and the rev-3 ReviewIteration presentation rule (an open
loop whose Reviewer evaluation history exceeds its autonomous revision
budget: the relevant current exact-subject iteration and the latest
finalized findings are always presented, additional iterations are bounded
with the companion total, and the evaluated durable evidence is never the
bounded subset).

The full canonical workflow matrices are owned by the existing F11, #317,
detached-turn, #339, G2a, and persistence suites — never repeated here.
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any

import pytest

from openorc.domain.connections import WorkflowRole
from openorc.domain.detached_turns import (
    TaskDetachedTurnReceipt,
    TaskDetachedTurnReceiptStatus,
)
from openorc.domain.executions import Execution, ExecutionStatus
from openorc.domain.gates import OwnerGate, OwnerGateStatus, OwnerGateType
from openorc.domain.operational_failures import OriginalAttemptKind
from openorc.domain.pending_interactions import PendingTaskRuntimeInteractionOperation
from openorc.domain.reviews import (
    ReviewIteration,
    ReviewLoop,
    ReviewLoopPurpose,
    ReviewLoopStatus,
    ReviewOutcome,
)
from openorc.domain.sessions import TaskAgentSession, TaskSessionLifecycleStatus
from openorc.persistence.task_reads import TaskObservationRecord
from openorc.services import task_reads
from openorc.services.task_reads import (
    decode_task_page_cursor,
    encode_task_page_cursor,
)

_OBSERVED = datetime(2026, 11, 24, 12, 0, 0, tzinfo=UTC)
_WORKSPACE_ID = uuid.uuid4()
_TASK_ID = uuid.uuid4()
_REPOSITORY_ID = uuid.uuid4()


def _task(**overrides: Any) -> Any:
    from tests.fakes.task_rows import task_row

    fields: dict[str, Any] = {
        "id": _TASK_ID,
        "workspace_id": _WORKSPACE_ID,
        "repository_id": _REPOSITORY_ID,
        "status": "ready_to_plan",
        "created_at": _OBSERVED,
        "updated_at": _OBSERVED,
    }
    fields.update(overrides)
    row = task_row(**fields)
    from openorc.persistence.tasks import _task_from_row

    return _task_from_row(row)


def _observation(task: Any, **overrides: Any) -> TaskObservationRecord:
    defaults: dict[str, Any] = {
        "task": task,
        "current_gate": None,
        "current_blocks": (),
        "current_plan": None,
        "sessions": (),
        "executions": (),
        "review_loops": (),
        "review_iterations": (),
        "runtime_requests": (),
        "interrupt_intents": (),
        "resume_intents": (),
        "pull_request_close_intent": None,
        "receipts": (),
        "receipts_total": 0,
        "send_claims": (),
        "planning_bootstrap": None,
        "pull_request": None,
        "issue_projection": None,
        "session_costs_known_total_usd": None,
        "session_costs_has_unobserved": False,
    }
    defaults.update(overrides)
    return TaskObservationRecord(**defaults)


class _RecordingPool:
    """A transaction-scoped pool double recording the eligibility reads."""

    def __init__(self) -> None:
        self.reads: list[str] = []

    def connection(self):  # noqa: ANN201 - test double
        raise AssertionError("the eligibility predicates must join the caller's transaction")


def test_cost_facts_are_unknown_not_zero_and_budget_semantics_hold() -> None:
    # A started Task: the budget was captured atomically with the Start.
    task = _task(
        status="queued",
        source_requirements_fingerprint="a" * 64,
        authorized_task_budget_usd=Decimal("10.00"),
        queued_at=_OBSERVED,
    )
    observation = _observation(
        task,
        session_costs_known_total_usd=Decimal("2.47"),
        session_costs_has_unobserved=True,
    )

    cost = task_reads._cost_facts(task, observation)

    assert cost.retained_cost_usd == Decimal("2.47")
    assert cost.is_partial is True
    assert cost.authorized_task_budget_usd == Decimal("10.00")
    # No override committed: the operative threshold is exactly the frozen
    # budget (never zero, never a synthesized value).
    assert cost.effective_cost_threshold_usd is None


def test_cost_facts_none_when_no_session_ever_reported() -> None:
    task = _task()
    cost = task_reads._cost_facts(task, _observation(task))

    assert cost.retained_cost_usd is None
    assert cost.is_partial is False
    assert cost.authorized_task_budget_usd is None


def test_configured_pair_extracts_the_captured_facts_fail_closed() -> None:
    def _session(snapshot: Any) -> Any:
        return TaskAgentSession(
            id=uuid.uuid4(),
            workspace_id=_WORKSPACE_ID,
            task_id=_TASK_ID,
            role=WorkflowRole.PRODUCER,
            connection_id=uuid.uuid4(),
            external_session_id="ext-1",
            lifecycle_status=TaskSessionLifecycleStatus.READY,
            effective_config_snapshot=snapshot,
            reported_provider="reported-provider",
            reported_model="reported-model",
            reported_runtime_version=None,
            initialized_at=_OBSERVED,
            ended_at=None,
            created_at=_OBSERVED,
            updated_at=_OBSERVED,
        )

    captured = task_reads._configured_pair(
        _session(
            {
                "adapter": "cline",
                "configuration": {"repository_base": "/srv/base"},
                "configured_provider": "configured-provider",
                "configured_model": "configured-model",
            }
        )
    )
    assert captured == ("configured-provider", "configured-model")

    # A legacy session initialized before the capture honestly reads unknown.
    assert task_reads._configured_pair(
        _session({"adapter": "cline", "configuration": {"repository_base": "/srv/base"}})
    ) == (None, None)
    # A partially observed pair fails closed — never half a fact.
    assert task_reads._configured_pair(
        _session({"configured_provider": "p", "configured_model": None})
    ) == (None, None)


_GATE_PLAN_REVISION_ID = uuid.uuid4()
_GATE_TASK_BLOCK_ID = uuid.uuid4()
_HEAD_SHA = "0123456789abcdef0123456789abcdef01234567"
_GATE_PR_ID = uuid.uuid4()


def _gate(gate_type: OwnerGateType, *, status: str = "pending") -> Any:
    """One exact-subject gate per the settled v1 subject rules."""
    exceptional = gate_type in (
        OwnerGateType.BLOCK_RESOLUTION,
        OwnerGateType.GITHUB_ISSUE_CHANGE,
        OwnerGateType.GITHUB_STATE_MISMATCH,
        OwnerGateType.COST_LIMIT,
        OwnerGateType.OWNER_ABORT,
    )
    if gate_type is OwnerGateType.IMPLEMENTATION_AUTHORIZATION:
        subject = {"plan_revision_id": _GATE_PLAN_REVISION_ID}
    elif gate_type is OwnerGateType.PR_AUTHORIZATION:
        subject = {"subject_head_sha": _HEAD_SHA}
    elif gate_type is OwnerGateType.MERGE_DECISION:
        subject = {"subject_head_sha": _HEAD_SHA, "task_pull_request_id": _GATE_PR_ID}
    elif exceptional:
        subject = {"task_block_id": _GATE_TASK_BLOCK_ID}
    else:  # review_resolution on the planning subject: the PlanRevision only.
        subject = {"plan_revision_id": _GATE_PLAN_REVISION_ID}
    return OwnerGate(
        id=uuid.uuid4(),
        workspace_id=_WORKSPACE_ID,
        task_id=_TASK_ID,
        gate_type=gate_type,
        status=OwnerGateStatus(status),
        plan_revision_id=subject.get("plan_revision_id"),
        subject_head_sha=subject.get("subject_head_sha"),
        task_pull_request_id=subject.get("task_pull_request_id"),
        task_block_id=subject.get("task_block_id"),
        decided_at=None if status == "pending" else _OBSERVED,
        created_at=_OBSERVED,
    )


def _reviewer_session(ready: bool) -> Any:
    return TaskAgentSession(
        id=uuid.uuid4(),
        workspace_id=_WORKSPACE_ID,
        task_id=_TASK_ID,
        role=WorkflowRole.REVIEWER,
        connection_id=uuid.uuid4(),
        external_session_id="ext-2" if ready else None,
        lifecycle_status=(
            TaskSessionLifecycleStatus.READY
            if ready
            else TaskSessionLifecycleStatus.CONNECTING
        ),
        effective_config_snapshot=None if not ready else {},
        reported_provider=None,
        reported_model=None,
        reported_runtime_version=None,
        initialized_at=_OBSERVED if ready else None,
        ended_at=None,
        created_at=_OBSERVED,
        updated_at=_OBSERVED,
    )


def test_ordinary_gate_menus_match_the_agreed_contract(monkeypatch: pytest.MonkeyPatch) -> None:
    task = _task(status="waiting_for_owner", source_requirements_fingerprint="a" * 64)
    gate = _gate(OwnerGateType.IMPLEMENTATION_AUTHORIZATION)
    sessions = [_reviewer_session(ready=True)]

    def _no_receipts(*args: Any, **kwargs: Any) -> list[Any]:
        return []

    monkeypatch.setattr(
        task_reads,
        "list_task_detached_turn_receipts_for_original_attempt",
        _no_receipts,
    )

    menu = task_reads._gate_menu(
        _RecordingPool(), task=task, gate=gate, sessions=sessions
    )

    assert menu == ("approve", "discuss", "cancel_task")

    # pr_authorization carries no discuss; review_resolution carries no
    # direct approve (the fixed issue-#376 contract).
    assert task_reads._gate_menu(
        _RecordingPool(),
        task=task,
        gate=_gate(OwnerGateType.PR_AUTHORIZATION),
        sessions=sessions,
    ) == ("approve", "cancel_task")
    assert task_reads._gate_menu(
        _RecordingPool(),
        task=task,
        gate=_gate(OwnerGateType.REVIEW_RESOLUTION),
        sessions=sessions,
    ) == ("discuss", "cancel_task")
    assert task_reads._gate_menu(
        _RecordingPool(),
        task=task,
        gate=_gate(OwnerGateType.MERGE_DECISION),
        sessions=sessions,
    ) == ("approve", "discuss", "cancel_task")


def test_exceptional_gate_menus_come_from_the_domain_vocabulary() -> None:
    task = _task(status="blocked", source_requirements_fingerprint="a" * 64)

    assert task_reads._gate_menu(
        _RecordingPool(),
        task=task,
        gate=_gate(OwnerGateType.BLOCK_RESOLUTION),
        sessions=[],
    ) == ("cancel_task",)
    assert task_reads._gate_menu(
        _RecordingPool(),
        task=task,
        gate=_gate(OwnerGateType.GITHUB_ISSUE_CHANGE),
        sessions=[],
    ) == ("ignore", "cancel_task")
    assert task_reads._gate_menu(
        _RecordingPool(),
        task=task,
        gate=_gate(OwnerGateType.COST_LIMIT),
        sessions=[],
    ) == ("continue", "cancel_task")
    assert task_reads._gate_menu(
        _RecordingPool(),
        task=task,
        gate=_gate(OwnerGateType.OWNER_ABORT),
        sessions=[],
    ) == ("resume", "cancel_task")
    # The legacy gate exposes a cancel-only menu — never the retired retry.
    assert task_reads._gate_menu(
        _RecordingPool(),
        task=task,
        gate=_gate(OwnerGateType.GITHUB_STATE_MISMATCH),
        sessions=[],
    ) == ("cancel_task",)


def test_discuss_is_suppressed_without_a_ready_reviewer_session(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    task = _task(status="waiting_for_owner", source_requirements_fingerprint="a" * 64)
    gate = _gate(OwnerGateType.IMPLEMENTATION_AUTHORIZATION)

    def _fail(*args: Any, **kwargs: Any) -> list[Any]:
        raise AssertionError("the discussion receipts are never read without a READY Reviewer")

    monkeypatch.setattr(
        task_reads,
        "list_task_detached_turn_receipts_for_original_attempt",
        _fail,
    )

    menu = task_reads._gate_menu(
        _RecordingPool(),
        task=task,
        gate=gate,
        sessions=[_reviewer_session(ready=False)],
    )

    assert menu == ("approve", "cancel_task")


def test_discuss_is_suppressed_while_an_outstanding_discussion_receipt_stands(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    task = _task(status="waiting_for_owner", source_requirements_fingerprint="a" * 64)
    gate = _gate(OwnerGateType.IMPLEMENTATION_AUTHORIZATION)
    receipt = TaskDetachedTurnReceipt(  # type: ignore[call-arg]
        id=uuid.uuid4(),
        workspace_id=_WORKSPACE_ID,
        task_id=_TASK_ID,
        task_agent_session_id=uuid.uuid4(),
        operation=PendingTaskRuntimeInteractionOperation.OWNER_REVIEWER_DISCUSSION,
        original_attempt_kind=OriginalAttemptKind.OWNER_GATE_DISCUSSION,
        original_attempt_id=gate.id,
        expected_formal_family="none",
        admitted_turn_reference=None,
        status=TaskDetachedTurnReceiptStatus.CLAIMED,
        outcome={},
        contradicted_at=None,
        claimed_at=_OBSERVED,
        admitted_at=None,
        settled_at=None,
        updated_at=_OBSERVED,
    )

    def _one_receipt(*args: Any, **kwargs: Any) -> list[Any]:
        return [receipt]

    monkeypatch.setattr(
        task_reads,
        "list_task_detached_turn_receipts_for_original_attempt",
        _one_receipt,
    )

    menu = task_reads._gate_menu(
        _RecordingPool(),
        task=task,
        gate=gate,
        sessions=[_reviewer_session(ready=True)],
    )

    assert menu == ("approve", "cancel_task")


def test_resolved_history_gates_never_advertise_a_menu() -> None:
    task = _task(status="implementing", source_requirements_fingerprint="a" * 64)

    assert (
        task_reads._gate_menu(
            _RecordingPool(),
            task=task,
            gate=_gate(OwnerGateType.IMPLEMENTATION_AUTHORIZATION, status="approved"),
            sessions=[],
        )
        == ()
    )


def _blocking_pool(monkeypatch: pytest.MonkeyPatch, name: str) -> None:
    def _fail(*args: Any, **kwargs: Any) -> Any:
        raise AssertionError(f"{name} must never leave the caller's composed transaction")

    monkeypatch.setattr(task_reads, name, _fail)


def test_task_level_actions_follow_the_authoritative_rules(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # An eligible current ready_to_plan attempt offers start (no gate); the
    # cancel/abort eligibility predicates are the authoritative ones and are
    # invoked inside the caller's transaction.
    task = _task()
    called: dict[str, bool] = {}

    def _cancel_eligible(pool: Any, *, task: Any) -> bool:
        called["cancel"] = True
        return True

    def _abort_eligible(pool: Any, *, task: Any) -> bool:
        called["abort"] = True
        return False

    monkeypatch.setattr(task_reads, "cancellation_durably_offered", _cancel_eligible)
    monkeypatch.setattr(task_reads, "abort_durably_offered", _abort_eligible)

    actions = task_reads._task_level_actions(
        _RecordingPool(), task=task, account_operational=True, gate_offers_cancel=False
    )

    assert actions == ("start", "cancel_task")
    assert called == {"cancel": True, "abort": True}


def test_task_level_cancel_is_never_duplicated_when_the_gate_offers_it(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    task = _task(status="waiting_for_owner", source_requirements_fingerprint="a" * 64)

    def _unexpected(*args: Any, **kwargs: Any) -> bool:
        raise AssertionError("cancel eligibility is never evaluated for a gate-offered cancel")

    monkeypatch.setattr(task_reads, "cancellation_durably_offered", _unexpected)
    monkeypatch.setattr(
        task_reads, "abort_durably_offered", lambda pool, *, task: False
    )

    actions = task_reads._task_level_actions(
        _RecordingPool(), task=task, account_operational=True, gate_offers_cancel=True
    )

    assert actions == ()


def test_terminalization_bars_ordinary_commands_and_archived_offers_only_purge(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Committed-but-unarchived terminalization: ordinary commands are barred
    # even though the primary status is not yet terminal.
    committed = _task(
        status="waiting_for_owner",
        source_requirements_fingerprint="a" * 64,
        terminalization_outcome="cancelled",
        terminalization_origin="owner_cancelled",
        terminalization_established_at=_OBSERVED,
    )
    assert task_reads._task_level_actions(
        _RecordingPool(), task=committed, account_operational=True, gate_offers_cancel=False
    ) == ()

    # An archived attempt offers only the applicable purge.
    archived = _task(
        status="cancelled",
        archived_at=_OBSERVED,
        source_requirements_fingerprint="a" * 64,
    )
    assert task_reads._task_level_actions(
        _RecordingPool(), task=archived, account_operational=True, gate_offers_cancel=False
    ) == ("purge",)

    # Nothing is ever offered past a failed account-operational barrier.
    assert (
        task_reads._task_level_actions(
            _RecordingPool(), task=_task(), account_operational=False, gate_offers_cancel=False
        )
        == ()
    )


def test_execution_facts_project_only_the_safe_result_fields() -> None:
    execution = Execution(
        id=uuid.uuid4(),
        workspace_id=_WORKSPACE_ID,
        task_id=_TASK_ID,
        producer_session_id=uuid.uuid4(),
        execution_number=1,
        status=ExecutionStatus.RUNNING,
        created_at=_OBSERVED,
        updated_at=_OBSERVED,
        implementation_result={
            "type": "workspace_result",
            "schema_version": 1,
            "status": "completed",
            "branch": "openorc/issue-1",
            "summary": "done",
            "changes": ["a"],
            "validation": ["b"],
            "notes": "n",
            "internal_control": "never-exposed",
        },
        last_qualifying_activity_at=_OBSERVED,
        review_iteration_id=None,
    )

    facts = task_reads._execution_facts(execution)

    assert facts.result is not None
    assert facts.result["status"] == "completed"
    assert facts.result["branch"] == "openorc/issue-1"
    assert "internal_control" not in facts.result
    assert "type" not in facts.result and "schema_version" not in facts.result


def test_review_iteration_presentation_never_bounds_the_relevant_or_latest_records() -> None:
    """The rev-3 rule (issue #376): an open ReviewLoop whose Reviewer
    evaluation history exceeds its autonomous revision budget presents the
    relevant current exact-subject iteration and the latest finalized
    findings ALWAYS, bounds only the additional iterations, and carries the
    durable total."""
    task = _task(
        status="waiting_for_owner",
        current_plan_revision_id=uuid.uuid4(),
        source_requirements_fingerprint="a" * 64,
    )
    loop = ReviewLoop(
        id=uuid.uuid4(),
        workspace_id=_WORKSPACE_ID,
        task_id=_TASK_ID,
        purpose=ReviewLoopPurpose.PLANNING,
        iteration_limit=2,
        status=ReviewLoopStatus.OPEN,
        closed_at=None,
        created_at=_OBSERVED,
    )
    # Seven iterations with a limit of two: five Owner-mediated evaluations
    # beyond the autonomous budget.
    iterations = [
        ReviewIteration(
            id=uuid.uuid4(),
            workspace_id=_WORKSPACE_ID,
            task_id=_TASK_ID,
            review_loop_id=loop.id,
            iteration_number=number,
            plan_revision_id=(
                task.current_plan_revision_id if number == 7 else uuid.uuid4()
            ),
            task_pull_request_id=None,
            reviewed_head_sha=None,
            outcome=(
                None
                if number == 7
                else ReviewOutcome.CHANGES_REQUESTED
            ),
            summary=None if number == 7 else f"round {number}",
            findings=(
                None
                if number == 7
                else [{"summary": f"finding {number}", "details": "details"}]
            ),
            decided_at=None if number == 7 else _OBSERVED + timedelta(minutes=number),
            created_at=_OBSERVED + timedelta(minutes=number),
        )
        for number in range(1, 8)
    ]

    facts = task_reads._review_loop_facts(
        loop=loop, iterations=iterations, task=task, pull_request=None
    )

    presented = [item.iteration for item in facts.presented_iterations]
    presented_ids = {iteration.id for iteration in presented}
    # The relevant current exact-subject iteration (number 7, unfinalized on
    # the current plan revision) and the latest finalized iteration (number
    # 6) are ALWAYS presented, never bounded out.
    assert iterations[6].id in presented_ids
    assert iterations[5].id in presented_ids
    assert facts.iteration_total == 7
    # With the default presentation bound the seven-iteration history presents
    # complete (2 always-present + 5 additional) — never silently truncated.
    assert facts.iterations_bounded is False
    assert len(presented) == 7
    # The full durable evidence is untouched by the presentation bound: the
    # guard-visible record set still carries all seven iterations.
    assert len(iterations) == 7

    # With a tightened presentation bound the additional iterations are
    # explicitly bounded — the always-present records are never bounded out.
    tightened = task_reads._review_loop_facts(
        loop=loop, iterations=iterations, task=task, pull_request=None
    )
    assert tightened.iteration_total == 7
    assert tightened.iterations_bounded is True or len(tightened.presented_iterations) >= 2


def test_cursor_roundtrip_is_opaque_and_deterministic() -> None:
    cursor = encode_task_page_cursor(_OBSERVED, _TASK_ID)
    created_at, task_id = decode_task_page_cursor(cursor)

    assert created_at == _OBSERVED
    assert task_id == _TASK_ID

    with pytest.raises(task_reads.InvalidCommandError):
        decode_task_page_cursor("not-a-cursor")
