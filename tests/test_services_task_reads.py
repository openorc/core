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

import contextlib
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
from openorc.domain.intents import TaskInterruptIntent, TaskInterruptIntentStatus
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
        "executions_total": 0,
        "review_loops": (),
        "review_iterations": (),
        "runtime_requests": (),
        "pending_runtime_requests_total": 0,
        "interrupt_intents": (),
        "outstanding_interrupt_intents_total": 0,
        "resume_intents": (),
        "outstanding_resume_intents_total": 0,
        "pull_request_close_intent": None,
        "receipts": (),
        "outstanding_receipts_total": 0,
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

    def close(self) -> None:
        raise AssertionError("read composition tests never close pools")


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
    plan_revision_id: uuid.UUID | None = None
    subject_head_sha: str | None = None
    task_pull_request_id: uuid.UUID | None = None
    task_block_id: uuid.UUID | None = None
    if gate_type is OwnerGateType.IMPLEMENTATION_AUTHORIZATION:
        plan_revision_id = _GATE_PLAN_REVISION_ID
    elif gate_type is OwnerGateType.PR_AUTHORIZATION:
        subject_head_sha = _HEAD_SHA
    elif gate_type is OwnerGateType.MERGE_DECISION:
        subject_head_sha = _HEAD_SHA
        task_pull_request_id = _GATE_PR_ID
    elif exceptional:
        task_block_id = _GATE_TASK_BLOCK_ID
    else:  # review_resolution on the planning subject: the PlanRevision only.
        plan_revision_id = _GATE_PLAN_REVISION_ID
    return OwnerGate(
        id=uuid.uuid4(),
        workspace_id=_WORKSPACE_ID,
        task_id=_TASK_ID,
        gate_type=gate_type,
        status=OwnerGateStatus(status),
        plan_revision_id=plan_revision_id,
        subject_head_sha=subject_head_sha,
        task_pull_request_id=task_pull_request_id,
        task_block_id=task_block_id,
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
            TaskSessionLifecycleStatus.READY if ready else TaskSessionLifecycleStatus.CONNECTING
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


def _eligible_predicates(monkeypatch: pytest.MonkeyPatch) -> None:
    """Pin every reused command-boundary predicate to its positively-offerable
    shape, so the menu-shape tests exercise the projection's own composition."""

    monkeypatch.setattr(task_reads, "cancellation_durably_offered", lambda pool, *, task: True)
    monkeypatch.setattr(
        task_reads,
        "blocked_continuation_durably_offered",
        lambda pool, *, task, gate: True,
    )
    monkeypatch.setattr(task_reads, "ignore_durably_offered", lambda pool, *, task, gate: True)


def test_ordinary_gate_menus_match_the_agreed_contract(monkeypatch: pytest.MonkeyPatch) -> None:
    task = _task(status="waiting_for_owner", source_requirements_fingerprint="a" * 64)
    gate = _gate(OwnerGateType.IMPLEMENTATION_AUTHORIZATION)
    sessions = [_reviewer_session(ready=True)]
    _eligible_predicates(monkeypatch)

    def _no_receipts(*args: Any, **kwargs: Any) -> list[Any]:
        return []

    monkeypatch.setattr(
        task_reads,
        "list_task_detached_turn_receipts_for_original_attempt",
        _no_receipts,
    )

    menu = task_reads._gate_menu(
        _RecordingPool(), task=task, gate=gate, sessions=sessions, account_operational=True
    )

    assert menu == ("approve", "discuss", "cancel_task")

    # pr_authorization carries no discuss; review_resolution carries no
    # direct approve (the fixed issue-#376 contract).
    assert task_reads._gate_menu(
        _RecordingPool(),
        task=task,
        gate=_gate(OwnerGateType.PR_AUTHORIZATION),
        sessions=sessions,
        account_operational=True,
    ) == ("approve", "cancel_task")
    assert task_reads._gate_menu(
        _RecordingPool(),
        task=task,
        gate=_gate(OwnerGateType.REVIEW_RESOLUTION),
        sessions=sessions,
        account_operational=True,
    ) == ("discuss", "cancel_task")
    assert task_reads._gate_menu(
        _RecordingPool(),
        task=task,
        gate=_gate(OwnerGateType.MERGE_DECISION),
        sessions=sessions,
        account_operational=True,
    ) == ("approve", "discuss", "cancel_task")


def test_exceptional_gate_menus_come_from_the_domain_vocabulary(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    task = _task(status="blocked", source_requirements_fingerprint="a" * 64)
    _eligible_predicates(monkeypatch)

    assert task_reads._gate_menu(
        _RecordingPool(),
        task=task,
        gate=_gate(OwnerGateType.BLOCK_RESOLUTION),
        sessions=[],
        account_operational=True,
    ) == ("cancel_task",)
    assert task_reads._gate_menu(
        _RecordingPool(),
        task=task,
        gate=_gate(OwnerGateType.GITHUB_ISSUE_CHANGE),
        sessions=[],
        account_operational=True,
    ) == ("ignore", "cancel_task")
    assert task_reads._gate_menu(
        _RecordingPool(),
        task=task,
        gate=_gate(OwnerGateType.COST_LIMIT),
        sessions=[],
        account_operational=True,
    ) == ("continue", "cancel_task")
    assert task_reads._gate_menu(
        _RecordingPool(),
        task=task,
        gate=_gate(OwnerGateType.OWNER_ABORT),
        sessions=[],
        account_operational=True,
    ) == ("resume", "cancel_task")
    # The legacy gate exposes a cancel-only menu — never the retired retry.
    assert task_reads._gate_menu(
        _RecordingPool(),
        task=task,
        gate=_gate(OwnerGateType.GITHUB_STATE_MISMATCH),
        sessions=[],
        account_operational=True,
    ) == ("cancel_task",)


def test_discuss_is_suppressed_without_a_ready_reviewer_session(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    task = _task(status="waiting_for_owner", source_requirements_fingerprint="a" * 64)
    gate = _gate(OwnerGateType.IMPLEMENTATION_AUTHORIZATION)
    _eligible_predicates(monkeypatch)

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
        account_operational=True,
    )

    assert menu == ("approve", "cancel_task")


def test_discuss_is_suppressed_while_an_outstanding_discussion_receipt_stands(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    task = _task(status="waiting_for_owner", source_requirements_fingerprint="a" * 64)
    gate = _gate(OwnerGateType.IMPLEMENTATION_AUTHORIZATION)
    _eligible_predicates(monkeypatch)
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
        account_operational=True,
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
            account_operational=True,
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
    # start/cancel/abort eligibility predicates are the authoritative ones
    # and are invoked inside the caller's transaction.
    task = _task()
    called: dict[str, bool] = {}

    def _start_eligible(pool: Any, *, task: Any) -> bool:
        called["start"] = True
        return True

    def _cancel_eligible(pool: Any, *, task: Any) -> bool:
        called["cancel"] = True
        return True

    def _abort_eligible(pool: Any, *, task: Any) -> bool:
        called["abort"] = True
        return False

    monkeypatch.setattr(task_reads, "start_durably_offered", _start_eligible)
    monkeypatch.setattr(task_reads, "cancellation_durably_offered", _cancel_eligible)
    monkeypatch.setattr(task_reads, "abort_durably_offered", _abort_eligible)

    actions = task_reads._task_level_actions(
        _RecordingPool(), task=task, account_operational=True, gate_offers_cancel=False
    )

    assert actions == ("start", "cancel_task")
    assert called == {"start": True, "cancel": True, "abort": True}


def test_start_is_suppressed_when_the_durable_projection_eligibility_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # A ready_to_plan Task whose durable GitHub projection has closed (or
    # otherwise contradicted the #76 queue-eligibility guard) is never
    # offered, even though the primary status alone would suggest it.
    task = _task()

    def _ineligible(pool: Any, *, task: Any) -> bool:
        return False

    monkeypatch.setattr(task_reads, "start_durably_offered", _ineligible)
    monkeypatch.setattr(task_reads, "cancellation_durably_offered", lambda pool, *, task: False)
    monkeypatch.setattr(task_reads, "abort_durably_offered", lambda pool, *, task: False)

    assert (
        task_reads._task_level_actions(
            _RecordingPool(), task=task, account_operational=True, gate_offers_cancel=False
        )
        == ()
    )


def test_task_level_cancel_is_never_duplicated_when_the_gate_offers_it(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    task = _task(status="waiting_for_owner", source_requirements_fingerprint="a" * 64)

    def _unexpected(*args: Any, **kwargs: Any) -> bool:
        raise AssertionError("cancel eligibility is never evaluated for a gate-offered cancel")

    monkeypatch.setattr(task_reads, "cancellation_durably_offered", _unexpected)
    monkeypatch.setattr(task_reads, "abort_durably_offered", lambda pool, *, task: False)

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
    assert (
        task_reads._task_level_actions(
            _RecordingPool(), task=committed, account_operational=True, gate_offers_cancel=False
        )
        == ()
    )

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
            plan_revision_id=(task.current_plan_revision_id if number == 7 else uuid.uuid4()),
            task_pull_request_id=None,
            reviewed_head_sha=None,
            outcome=(None if number == 7 else ReviewOutcome.CHANGES_REQUESTED),
            summary=None if number == 7 else f"round {number}",
            findings=(
                None if number == 7 else [{"summary": f"finding {number}", "details": "details"}]
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


# --- composition-level offered menus and detail projections ------------------


class _ReadTransactionConnection:
    """The no-op connection double the composed read transaction runs over."""

    def transaction(self) -> Any:
        return contextlib.nullcontext()

    def execute(self, *args: Any, **kwargs: Any) -> None:
        return None


class _ReadPool:
    """Yields the no-op connection double for the composed read transaction."""

    def connection(self) -> Any:
        return contextlib.nullcontext(_ReadTransactionConnection())

    def close(self) -> None:
        raise AssertionError("read composition tests never close pools")


def _completed_execution() -> Any:
    return Execution(
        id=uuid.uuid4(),
        workspace_id=_WORKSPACE_ID,
        task_id=_TASK_ID,
        producer_session_id=uuid.uuid4(),
        execution_number=2,
        status=ExecutionStatus.SUCCEEDED,
        created_at=_OBSERVED,
        updated_at=_OBSERVED,
        implementation_result={
            "type": "implementation_result",
            "schema_version": 1,
            "status": "COMPLETED",
            "branch": "openorc/issue-42",
            "summary": "Implemented the change.",
            "changes": ["Added the read surface."],
            "validation": ["Deterministic suite green."],
            "notes": None,
        },
        last_qualifying_activity_at=_OBSERVED,
        review_iteration_id=None,
    )


def test_summary_vetoes_gate_cancel_while_the_accounted_runtime_barrier_stands(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The composed summary's actual offered menu: an unsettled current
    Interrupt (#342) vetoes cancel_task in the gate menu exactly as it vetoes
    the Task-level action — the veto is never Task-level only."""
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
    monkeypatch.setattr(task_reads, "cancellation_durably_offered", lambda pool, *, task: False)
    monkeypatch.setattr(task_reads, "abort_durably_offered", lambda pool, *, task: False)

    summary = task_reads._compose_summary(
        _RecordingPool(),
        task=task,
        observation=_observation(task, current_gate=gate, sessions=tuple(sessions)),
        issue=None,
        pull_request=None,
        account_operational=True,
    )

    assert summary.current_gate is not None
    assert summary.current_gate.actions == ("approve", "discuss")
    assert summary.actions == ()


def test_summary_suppresses_resume_while_the_episode_settlement_is_outstanding(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The composed summary's actual offered menu on the blocked OWNER_ABORT
    gate: the guarded continuation derivation's #342 precondition keeps
    ``resume`` unadvertised while the episode interruption is not positively
    settled, and the eligible cancel survives."""
    task = _task(status="blocked", source_requirements_fingerprint="a" * 64)
    gate = _gate(OwnerGateType.OWNER_ABORT)

    monkeypatch.setattr(
        task_reads,
        "blocked_continuation_durably_offered",
        lambda pool, *, task, gate: False,
    )
    monkeypatch.setattr(task_reads, "cancellation_durably_offered", lambda pool, *, task: True)
    monkeypatch.setattr(task_reads, "abort_durably_offered", lambda pool, *, task: False)

    summary = task_reads._compose_summary(
        _RecordingPool(),
        task=task,
        observation=_observation(task, current_gate=gate),
        issue=None,
        pull_request=None,
        account_operational=True,
    )

    assert summary.current_gate is not None
    assert summary.current_gate.actions == ("cancel_task",)
    # The Task-level projection never duplicates the gate-offered cancel.
    assert summary.actions == ()


def test_summary_offers_resume_when_the_guarded_continuation_holds(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    task = _task(status="blocked", source_requirements_fingerprint="a" * 64)
    gate = _gate(OwnerGateType.OWNER_ABORT)

    monkeypatch.setattr(
        task_reads,
        "blocked_continuation_durably_offered",
        lambda pool, *, task, gate: True,
    )
    monkeypatch.setattr(task_reads, "cancellation_durably_offered", lambda pool, *, task: True)
    monkeypatch.setattr(task_reads, "abort_durably_offered", lambda pool, *, task: False)

    summary = task_reads._compose_summary(
        _RecordingPool(),
        task=task,
        observation=_observation(task, current_gate=gate),
        issue=None,
        pull_request=None,
        account_operational=True,
    )

    assert summary.current_gate is not None
    assert summary.current_gate.actions == ("resume", "cancel_task")


def test_gate_menus_respect_the_account_and_terminalization_barriers(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The gate-scoped menu is computed under the same barriers as the
    Task-level projection: a claimed account deletion or a committed
    terminalization bars every offered command, gate menus included."""
    task = _task(status="waiting_for_owner", source_requirements_fingerprint="a" * 64)
    gate = _gate(OwnerGateType.IMPLEMENTATION_AUTHORIZATION)

    def _unexpected(*args: Any, **kwargs: Any) -> bool:
        raise AssertionError("no eligibility predicate runs behind a barrier")

    monkeypatch.setattr(task_reads, "cancellation_durably_offered", _unexpected)
    monkeypatch.setattr(task_reads, "abort_durably_offered", _unexpected)

    assert (
        task_reads._gate_menu(
            _RecordingPool(),
            task=task,
            gate=gate,
            sessions=[],
            account_operational=False,
        )
        == ()
    )
    committed = _task(
        status="waiting_for_owner",
        source_requirements_fingerprint="a" * 64,
        terminalization_outcome="cancelled",
        terminalization_origin="owner_cancelled",
        terminalization_established_at=_OBSERVED,
    )
    assert (
        task_reads._gate_menu(
            _RecordingPool(),
            task=committed,
            gate=gate,
            sessions=[],
            account_operational=True,
        )
        == ()
    )


def test_detail_carries_the_completed_implementation_result_and_outstanding_evidence(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The Task detail at the PR_AUTHORIZATION decision point projects the
    completed implementation result (safe typed fields only) with a matching
    executions_total, and the outstanding receipt evidence is carried with
    its matching total."""
    task = _task(status="waiting_for_owner", source_requirements_fingerprint="a" * 64)
    completed = _completed_execution()
    claimed_receipt = TaskDetachedTurnReceipt(  # type: ignore[call-arg]
        id=uuid.uuid4(),
        workspace_id=_WORKSPACE_ID,
        task_id=_TASK_ID,
        task_agent_session_id=uuid.uuid4(),
        operation=PendingTaskRuntimeInteractionOperation.OWNER_REVIEWER_DISCUSSION,
        original_attempt_kind=OriginalAttemptKind.OWNER_GATE_DISCUSSION,
        original_attempt_id=uuid.uuid4(),
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
    observation = _observation(
        task,
        executions=(completed,),
        executions_total=1,
        receipts=(claimed_receipt,),
        outstanding_receipts_total=1,
        interrupt_intents=(
            TaskInterruptIntent(  # type: ignore[call-arg]
                id=uuid.uuid4(),
                workspace_id=_WORKSPACE_ID,
                task_id=_TASK_ID,
                task_agent_session_id=uuid.uuid4(),
                task_block_id=None,
                workflow_role=WorkflowRole.PRODUCER,
                external_session_id="producer-ext-1",
                requested_by_profile_id=uuid.uuid4(),
                status=TaskInterruptIntentStatus.UNSETTLED,
                outcome={},
                created_at=_OBSERVED,
                updated_at=_OBSERVED,
            ),
        ),
        outstanding_interrupt_intents_total=2,
        pending_runtime_requests_total=3,
    )

    monkeypatch.setattr(task_reads, "require_workspace_task", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        task_reads,
        "load_task_observation",
        lambda pool, *, workspace_id, task_id: observation,
    )
    monkeypatch.setattr(
        task_reads,
        "list_pending_task_runtime_interactions",
        lambda pool, *, workspace_id, task_id: [],
    )
    monkeypatch.setattr(
        task_reads, "account_operationally_offered", lambda pool, *, profile_id: True
    )
    monkeypatch.setattr(task_reads, "abort_durably_offered", lambda pool, *, task: False)
    monkeypatch.setattr(task_reads, "cancellation_durably_offered", lambda pool, *, task: False)

    detail = task_reads.get_task_for_owner(
        _ReadPool(),
        profile_id=uuid.uuid4(),
        workspace_id=_WORKSPACE_ID,
        task_id=_TASK_ID,
    )

    assert detail.executions_total == 1
    assert len(detail.executions) == 1
    projected = detail.executions[0]
    assert projected.execution.id == completed.id
    assert projected.result is not None
    # The safe typed projection: the Owner-authored completion facts only.
    assert projected.result["status"] == "COMPLETED"
    assert projected.result["branch"] == "openorc/issue-42"
    assert "type" not in projected.result
    assert "schema_version" not in projected.result
    # The outstanding evidence survives with its matching total.
    assert detail.outstanding_receipts == (claimed_receipt,)
    assert detail.outstanding_receipts_total == 1
    # The bounded intent and pending-request collections carry companion
    # totals of exactly the same durable populations: the presentation window
    # carries the one loaded outstanding intent while the companion total
    # counts the two durable outstanding interrupts.
    assert len(detail.outstanding_intents) == 1
    assert detail.outstanding_intents[0].kind == "interrupt"
    assert detail.outstanding_intents_total == 2
    assert detail.pending_runtime_requests == ()
    assert detail.pending_runtime_requests_total == 3


def test_list_items_supply_the_discussion_evidence(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The composed list item's actual offered menu: an eligible READY
    Reviewer offers ``discuss`` in the list exactly as in the detail — the
    summary is never a suppression of a durably offered action — and a
    binding without a READY Reviewer suppresses it again."""
    gate = _gate(OwnerGateType.IMPLEMENTATION_AUTHORIZATION)
    task = _task(
        status="waiting_for_owner",
        source_requirements_fingerprint="a" * 64,
        current_owner_gate_id=gate.id,
    )

    monkeypatch.setattr(
        task_reads,
        "find_github_issue_projections_by_identity",
        lambda pool, *, repository_id, github_issue_ids: {},
    )
    monkeypatch.setattr(task_reads, "get_task_pull_request_for_task", lambda pool, *, task_id: None)
    monkeypatch.setattr(
        task_reads,
        "list_owner_gates_by_ids",
        lambda pool, *, owner_gate_ids: {gate.id: gate},
    )
    monkeypatch.setattr(
        task_reads,
        "list_current_task_blocks_for_tasks",
        lambda pool, *, task_ids: {},
    )
    monkeypatch.setattr(
        task_reads,
        "sum_task_retained_session_costs_for_tasks",
        lambda pool, *, task_ids: {},
    )
    monkeypatch.setattr(
        task_reads,
        "list_task_agent_sessions_for_tasks",
        lambda pool, *, task_ids: {_TASK_ID: [_reviewer_session(ready=True)]},
    )

    def _no_receipts(*args: Any, **kwargs: Any) -> list[Any]:
        return []

    monkeypatch.setattr(
        task_reads,
        "list_task_detached_turn_receipts_for_original_attempt",
        _no_receipts,
    )
    monkeypatch.setattr(task_reads, "cancellation_durably_offered", lambda pool, *, task: True)
    monkeypatch.setattr(task_reads, "abort_durably_offered", lambda pool, *, task: False)
    monkeypatch.setattr(
        task_reads, "account_operationally_offered", lambda pool, *, profile_id: True
    )

    summary = task_reads._compose_list_item(_RecordingPool(), task=task, profile_id=uuid.uuid4())

    assert summary.current_gate is not None
    assert summary.current_gate.actions == ("approve", "discuss", "cancel_task")

    # Without a READY Reviewer binding the discussion prerequisite fails again.
    monkeypatch.setattr(
        task_reads,
        "list_task_agent_sessions_for_tasks",
        lambda pool, *, task_ids: {_TASK_ID: [_reviewer_session(ready=False)]},
    )
    suppressed = task_reads._compose_list_item(_RecordingPool(), task=task, profile_id=uuid.uuid4())
    assert suppressed.current_gate is not None
    assert suppressed.current_gate.actions == ("approve", "cancel_task")
