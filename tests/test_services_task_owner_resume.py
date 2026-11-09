"""Focused F8g Owner Resume tests (issue #292).

The authorization boundary's guard/refusal behavior and atomic composition,
the continuation's one-shot claim and proven pre-dispatch supersession, the
dispatch outcome classifications (definite non-delivery, interrupted turn,
uncertainty), and the representative implementation routing. Database
constraint behavior is proven by the integration-marked suite.
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime
from typing import Any, cast

import pytest

from openorc.adapters.agent_runtime.errors import (
    AgentRuntimeInterruptedTurnError,
    AgentRuntimeUncertainOutcomeError,
    AgentSessionNotFoundError,
)
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
from openorc.services import task_owner_resume as resume_module
from openorc.services.errors import ConflictError, StaleOperationError
from openorc.services.task_owner_resume import (
    SessionResumeFollowUp,
    SessionResumeOutcomeKind,
    dispatch_authorized_task_resume,
    resume_task_after_abort,
)

_WORKSPACE_ID = uuid.uuid4()
_TASK_ID = uuid.uuid4()
_SESSION_ID = uuid.uuid4()
_EXTERNAL_SESSION_ID = "cline-session-1"
_BLOCK_ID = uuid.uuid4()
_GATE_ID = uuid.uuid4()
_OWNER_PROFILE_ID = uuid.uuid4()
_STATE_TOKEN = uuid.uuid4()
_EXECUTION_ID = uuid.uuid4()

_NOW = datetime(2026, 11, 9, 12, 0, 0, tzinfo=UTC)
_POOL = cast(DatabasePool, None)


def _task(*, status: TaskStatus = TaskStatus.BLOCKED, token: Any = _STATE_TOKEN) -> Task:
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


def _gate() -> OwnerGate:
    return OwnerGate(
        id=_GATE_ID,
        workspace_id=_WORKSPACE_ID,
        task_id=_TASK_ID,
        gate_type=OwnerGateType.OWNER_ABORT,
        status=OwnerGateStatus.PENDING,
        plan_revision_id=None,
        subject_head_sha=None,
        task_pull_request_id=None,
        task_block_id=_BLOCK_ID,
        decided_at=None,
        created_at=_NOW,
    )


def _block() -> TaskBlock:
    return TaskBlock(
        id=_BLOCK_ID,
        workspace_id=_WORKSPACE_ID,
        task_id=_TASK_ID,
        reason=TaskBlockReason.OWNER_ACTION_REQUIRED,
        context={
            "control": "owner_abort",
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


def _resume_record(status: TaskResumeIntentStatus) -> TaskResumeIntent:
    return TaskResumeIntent(
        id=uuid.uuid4(),
        workspace_id=_WORKSPACE_ID,
        task_id=_TASK_ID,
        task_agent_session_id=_SESSION_ID,
        task_block_id=_BLOCK_ID,
        workflow_role=WorkflowRole.PRODUCER,
        external_session_id=_EXTERNAL_SESSION_ID,
        requested_by_profile_id=_OWNER_PROFILE_ID,
        status=status,
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
        self.superseded: dict[str, Any] | None = None
        self.claimed: dict[str, Any] | None = None
        self.outcomes: list[dict[str, Any]] = []
        self.record_created_record: TaskResumeIntent | None = None
        self.dispatch_factory: Any = None


def _wire_authorization(monkeypatch: pytest.MonkeyPatch, calls: _RecordingCalls) -> None:
    from contextlib import contextmanager

    @contextmanager
    def _fake_tx(pool):
        yield pool

    monkeypatch.setattr(resume_module, "composed_transaction", _fake_tx)
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

    monkeypatch.setattr(resume_module, "resolve_gate_records", _resolve)
    monkeypatch.setattr(
        resume_module,
        "require_resolution_applied",
        lambda resolution: (resolution.gate, resolution.task),
    )

    def _restore(pool, task_id, *, expected_state_token, restored_status):
        calls.restored = {
            "expected_state_token": expected_state_token,
            "restored_status": restored_status,
        }
        return _task(status=restored_status, token=uuid.uuid4())

    monkeypatch.setattr(resume_module.task_records, "restore_blocked_task_status", _restore)
    monkeypatch.setattr(
        resume_module.block_records,
        "resolve_task_block",
        lambda pool, *, task_block_id: _block(),
    )
    monkeypatch.setattr(
        resume_module.event_coordination,
        "record_owner_gate_resolved_event",
        lambda pool, **kwargs: None,
    )

    def _create(pool, **kwargs):
        calls.created = dict(kwargs)
        record = _resume_record(TaskResumeIntentStatus.AUTHORIZED)
        calls.record_created_record = record
        return record

    monkeypatch.setattr(resume_module, "create_task_resume_intent", _create)


def test_the_authorization_resolves_the_episode_and_commits_the_record(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = _RecordingCalls()
    _wire_authorization(monkeypatch, calls)
    outcome = resume_task_after_abort(
        _POOL,
        profile_id=_OWNER_PROFILE_ID,
        workspace_id=_WORKSPACE_ID,
        task_id=_TASK_ID,
        owner_gate_id=_GATE_ID,
        expected_state_token=_STATE_TOKEN,
    )
    assert outcome.task.status is TaskStatus.IMPLEMENTING
    assert outcome.resolved_owner_gate.id == _GATE_ID
    assert outcome.resolved_task_block.id == _BLOCK_ID
    assert calls.resolve is not None
    assert calls.resolve["outcome"] is OwnerGateStatus.APPROVED
    assert calls.resolve["expected_task_state_token"] == _STATE_TOKEN
    assert calls.restored is not None
    assert calls.restored["restored_status"] is TaskStatus.IMPLEMENTING
    assert calls.created is not None
    assert calls.created["task_agent_session_id"] == _SESSION_ID
    assert calls.created["task_block_id"] == _BLOCK_ID
    routing = calls.created["routing"]
    assert routing["operation"] == "implementation_execution"
    assert routing["expected_response_family"] == "implementation_result"
    assert routing["subject_kind"] == "execution"
    assert uuid.UUID(routing["subject_id"]) == _EXECUTION_ID
    assert routing["restored_status"] == TaskStatus.IMPLEMENTING.value
    assert uuid.UUID(routing["authorized_state_token"]) == outcome.task.state_token


def test_a_non_blocked_task_refuses_the_resume_action(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = _RecordingCalls()
    _wire_authorization(monkeypatch, calls)
    monkeypatch.setattr(
        resume_module,
        "require_current_task",
        lambda pool, *, workspace_id, task_id, expected_state_token: _task(
            status=TaskStatus.PLANNING
        ),
    )
    with pytest.raises(ConflictError, match="not blocked"):
        resume_task_after_abort(
            _POOL,
            profile_id=_OWNER_PROFILE_ID,
            workspace_id=_WORKSPACE_ID,
            task_id=_TASK_ID,
            owner_gate_id=_GATE_ID,
            expected_state_token=_STATE_TOKEN,
        )
    assert calls.resolve is None
    assert calls.created is None


def test_a_non_owner_abort_gate_refuses_the_resume_action(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = _RecordingCalls()
    _wire_authorization(monkeypatch, calls)
    gate = _gate()
    object.__setattr__(gate, "gate_type", OwnerGateType.COST_LIMIT)
    monkeypatch.setattr(
        resume_module,
        "require_current_owner_gate",
        lambda pool, *, task, owner_gate_id: gate,
    )
    with pytest.raises(ConflictError, match="owner_abort"):
        resume_task_after_abort(
            _POOL,
            profile_id=_OWNER_PROFILE_ID,
            workspace_id=_WORKSPACE_ID,
            task_id=_TASK_ID,
            owner_gate_id=_GATE_ID,
            expected_state_token=_STATE_TOKEN,
        )
    assert calls.resolve is None


def test_an_unsettled_episode_interruption_refuses_the_resume_action(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = _RecordingCalls()
    _wire_authorization(monkeypatch, calls)
    intent = _settled_intent()
    object.__setattr__(intent, "status", TaskInterruptIntentStatus.UNSETTLED)
    monkeypatch.setattr(
        resume_module,
        "list_task_interrupt_intents",
        lambda pool, *, task_id: [intent],
    )
    with pytest.raises(ConflictError, match="not positively settled"):
        resume_task_after_abort(
            _POOL,
            profile_id=_OWNER_PROFILE_ID,
            workspace_id=_WORKSPACE_ID,
            task_id=_TASK_ID,
            owner_gate_id=_GATE_ID,
            expected_state_token=_STATE_TOKEN,
        )
    assert calls.resolve is None


def test_an_unclassifiable_subject_refuses_the_resume_action(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = _RecordingCalls()
    _wire_authorization(monkeypatch, calls)
    monkeypatch.setattr(
        resume_module.execution_records,
        "list_task_executions",
        lambda pool, *, task_id: [],
    )
    with pytest.raises(ConflictError, match="no durably identifiable"):
        resume_task_after_abort(
            _POOL,
            profile_id=_OWNER_PROFILE_ID,
            workspace_id=_WORKSPACE_ID,
            task_id=_TASK_ID,
            owner_gate_id=_GATE_ID,
            expected_state_token=_STATE_TOKEN,
        )
    assert calls.resolve is None


def test_a_stale_token_refuses_the_resume_action(monkeypatch: pytest.MonkeyPatch) -> None:
    calls = _RecordingCalls()
    _wire_authorization(monkeypatch, calls)
    monkeypatch.setattr(
        resume_module,
        "require_current_task",
        lambda pool, *, workspace_id, task_id, expected_state_token: (_ for _ in ()).throw(
            StaleOperationError("stale")
        ),
    )
    with pytest.raises(StaleOperationError):
        resume_task_after_abort(
            _POOL,
            profile_id=_OWNER_PROFILE_ID,
            workspace_id=_WORKSPACE_ID,
            task_id=_TASK_ID,
            owner_gate_id=_GATE_ID,
            expected_state_token=_STATE_TOKEN,
        )
    assert calls.resolve is None


class _FakeAdapter:
    def __init__(self, outcome: str) -> None:
        self._outcome = outcome
        self.calls: list[tuple[str, str | None]] = []

    def resume_activity(self, session_id: str, *, expected_family: str | None = None):
        self.calls.append((session_id, expected_family))
        if self._outcome == "non_delivered":
            raise AgentSessionNotFoundError("the addressed session is not live")
        if self._outcome == "interrupted":
            raise AgentRuntimeInterruptedTurnError("the turn was interrupted")
        if self._outcome == "uncertain":
            raise AgentRuntimeUncertainOutcomeError("unknown")
        from openorc.protocol.models import ImplementationResult

        return ImplementationResult(
            status="succeeded",
            branch="repo/feature",
            summary="s",
            changes=(),
            validation=(),
            notes=None,
        )


def _wire_dispatch(
    monkeypatch: pytest.MonkeyPatch,
    calls: _RecordingCalls,
    *,
    record: TaskResumeIntent,
    task: Task,
    adapter: _FakeAdapter,
) -> None:
    from contextlib import contextmanager

    @contextmanager
    def _fake_tx(pool):
        yield pool

    monkeypatch.setattr(resume_module, "composed_transaction", _fake_tx)
    monkeypatch.setattr(
        resume_module.task_records,
        "get_task_for_update",
        lambda pool, task_id: task if task_id == _TASK_ID else None,
    )
    monkeypatch.setattr(
        resume_module,
        "get_task_resume_intent",
        lambda pool, *, task_resume_intent_id: record,
    )

    def _claim(pool, *, task_resume_intent_id):
        calls.claimed = {"task_resume_intent_id": task_resume_intent_id}
        claimed = _resume_record(TaskResumeIntentStatus.DISPATCHED)
        object.__setattr__(claimed, "id", record.id)
        return claimed

    monkeypatch.setattr(resume_module, "claim_task_resume_dispatch", _claim)

    def _supersede(pool, *, task_resume_intent_id, outcome):
        calls.superseded = {
            "task_resume_intent_id": task_resume_intent_id,
            "outcome": dict(outcome),
        }
        superseded = _resume_record(TaskResumeIntentStatus.SUPERSEDED)
        object.__setattr__(superseded, "id", record.id)
        return superseded

    monkeypatch.setattr(resume_module, "supersede_task_resume_intent", _supersede)

    def _outcome(pool, *, task_resume_intent_id, status, outcome):
        calls.outcomes.append(
            {
                "task_resume_intent_id": task_resume_intent_id,
                "status": status,
                "outcome": dict(outcome),
            }
        )
        recorded = _resume_record(status)
        object.__setattr__(recorded, "id", task_resume_intent_id)
        return recorded

    monkeypatch.setattr(resume_module, "record_task_resume_intent_outcome", _outcome)

    class _CM:
        def __enter__(self):
            return adapter

        def __exit__(self, *args: Any) -> None:
            return None

    monkeypatch.setattr(
        resume_module,
        "producer_runtime_factory",
        lambda: _CM(),
        raising=False,
    )

    def _factory():
        return _CM()

    calls.dispatch_factory = _factory


def test_a_pre_dispatch_supersession_records_proven_non_dispatch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = _RecordingCalls()
    record = _resume_record(TaskResumeIntentStatus.AUTHORIZED)
    moved_token = uuid.uuid4()
    adapter = _FakeAdapter("completed")
    _wire_dispatch(
        monkeypatch,
        calls,
        record=record,
        task=_task(status=TaskStatus.BLOCKED, token=moved_token),
        adapter=adapter,
    )
    outcome = dispatch_authorized_task_resume(
        _POOL,
        workspace_id=_WORKSPACE_ID,
        task_id=_TASK_ID,
        task_resume_intent_id=record.id,
        profile_id=_OWNER_PROFILE_ID,
        expected_state_token=_STATE_TOKEN,
        producer_runtime_factory=calls.dispatch_factory,
        reviewer_runtime_factory=cast(Any, None),
        github=cast(Any, None),
        bootstrap_composition=cast(Any, None),
    )
    assert outcome.kind is SessionResumeOutcomeKind.SUPERSEDED
    assert outcome.resume.status is TaskResumeIntentStatus.SUPERSEDED
    assert calls.superseded is not None
    assert calls.superseded["outcome"] == {"supersession": "pre_dispatch"}
    assert adapter.calls == []


def test_a_dispatched_record_refuses_the_one_shot_dispatch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = _RecordingCalls()
    record = _resume_record(TaskResumeIntentStatus.DISPATCHED)
    adapter = _FakeAdapter("completed")
    _wire_dispatch(
        monkeypatch,
        calls,
        record=record,
        task=_task(status=TaskStatus.IMPLEMENTING),
        adapter=adapter,
    )
    with pytest.raises(ConflictError, match="one-shot dispatch"):
        dispatch_authorized_task_resume(
            _POOL,
            workspace_id=_WORKSPACE_ID,
            task_id=_TASK_ID,
            task_resume_intent_id=record.id,
            profile_id=_OWNER_PROFILE_ID,
            expected_state_token=_STATE_TOKEN,
            producer_runtime_factory=calls.dispatch_factory,
            reviewer_runtime_factory=cast(Any, None),
            github=cast(Any, None),
            bootstrap_composition=cast(Any, None),
        )
    assert adapter.calls == []


def test_a_definite_pre_effect_refusal_records_non_delivery(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = _RecordingCalls()
    record = _resume_record(TaskResumeIntentStatus.AUTHORIZED)
    adapter = _FakeAdapter("non_delivered")
    _wire_dispatch(
        monkeypatch,
        calls,
        record=record,
        task=_task(status=TaskStatus.IMPLEMENTING),
        adapter=adapter,
    )
    with pytest.raises(Exception, match="pre-effect non-delivery"):
        dispatch_authorized_task_resume(
            _POOL,
            workspace_id=_WORKSPACE_ID,
            task_id=_TASK_ID,
            task_resume_intent_id=record.id,
            profile_id=_OWNER_PROFILE_ID,
            expected_state_token=_STATE_TOKEN,
            producer_runtime_factory=calls.dispatch_factory,
            reviewer_runtime_factory=cast(Any, None),
            github=cast(Any, None),
            bootstrap_composition=cast(Any, None),
        )
    assert adapter.calls == [(_EXTERNAL_SESSION_ID, "implementation_result")]
    assert calls.outcomes
    assert calls.outcomes[-1]["status"] is TaskResumeIntentStatus.NON_DELIVERED


def test_an_interrupted_resumed_turn_records_the_interrupted_outcome(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = _RecordingCalls()
    record = _resume_record(TaskResumeIntentStatus.AUTHORIZED)
    adapter = _FakeAdapter("interrupted")
    _wire_dispatch(
        monkeypatch,
        calls,
        record=record,
        task=_task(status=TaskStatus.IMPLEMENTING),
        adapter=adapter,
    )
    settled: list[dict[str, Any]] = []
    monkeypatch.setattr(
        resume_module,
        "settle_observed_task_interrupt",
        lambda pool, runtime, *, task_id, external_session_id: settled.append(
            {"task_id": task_id, "external_session_id": external_session_id}
        ),
    )
    outcome = dispatch_authorized_task_resume(
        _POOL,
        workspace_id=_WORKSPACE_ID,
        task_id=_TASK_ID,
        task_resume_intent_id=record.id,
        profile_id=_OWNER_PROFILE_ID,
        expected_state_token=_STATE_TOKEN,
        producer_runtime_factory=calls.dispatch_factory,
        reviewer_runtime_factory=cast(Any, None),
        github=cast(Any, None),
        bootstrap_composition=cast(Any, None),
    )
    assert outcome.kind is SessionResumeOutcomeKind.INTERRUPTED
    assert outcome.resume.status is TaskResumeIntentStatus.INTERRUPTED
    assert settled == [{"task_id": _TASK_ID, "external_session_id": _EXTERNAL_SESSION_ID}]


def test_an_uncertain_dispatch_keeps_the_record_conservatively_dispatched(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = _RecordingCalls()
    record = _resume_record(TaskResumeIntentStatus.AUTHORIZED)
    adapter = _FakeAdapter("uncertain")
    _wire_dispatch(
        monkeypatch,
        calls,
        record=record,
        task=_task(status=TaskStatus.IMPLEMENTING),
        adapter=adapter,
    )
    with pytest.raises(Exception, match="unknown"):
        dispatch_authorized_task_resume(
            _POOL,
            workspace_id=_WORKSPACE_ID,
            task_id=_TASK_ID,
            task_resume_intent_id=record.id,
            profile_id=_OWNER_PROFILE_ID,
            expected_state_token=_STATE_TOKEN,
            producer_runtime_factory=calls.dispatch_factory,
            reviewer_runtime_factory=cast(Any, None),
            github=cast(Any, None),
            bootstrap_composition=cast(Any, None),
        )
    assert calls.outcomes == []


def _review_iteration() -> Any:
    from openorc.domain.reviews import ReviewIteration

    return ReviewIteration(
        id=uuid.uuid4(),
        workspace_id=_WORKSPACE_ID,
        task_id=_TASK_ID,
        review_loop_id=uuid.uuid4(),
        iteration_number=1,
        plan_revision_id=uuid.uuid4(),
        task_pull_request_id=None,
        reviewed_head_sha=None,
        outcome=None,
        summary=None,
        findings=None,
        decided_at=None,
        created_at=_NOW,
    )


def _review_loop(purpose: Any) -> Any:
    from openorc.domain.reviews import ReviewLoop, ReviewLoopStatus

    return ReviewLoop(
        id=uuid.uuid4(),
        workspace_id=_WORKSPACE_ID,
        task_id=_TASK_ID,
        purpose=purpose,
        iteration_limit=5,
        status=ReviewLoopStatus.OPEN,
        closed_at=None,
        created_at=_NOW,
    )


def _wire_review_route(
    monkeypatch: pytest.MonkeyPatch,
    *,
    loop: Any,
    iteration: Any,
    decision: Any,
) -> dict[str, Any]:
    from contextlib import contextmanager

    captured: dict[str, Any] = {"runtimes": [], "presentations": []}

    @contextmanager
    def _fake_tx(pool):
        yield pool

    monkeypatch.setattr(resume_module, "composed_transaction", _fake_tx)
    monkeypatch.setattr(
        resume_module.review_records,
        "get_review_iteration",
        lambda pool, *, review_iteration_id: iteration,
    )
    monkeypatch.setattr(
        resume_module.review_records,
        "get_review_loop",
        lambda pool, *, review_loop_id: loop,
    )
    monkeypatch.setattr(
        resume_module.session_records,
        "get_task_agent_session",
        lambda pool, *, task_id, role: _session(),
    )

    class _ProducerAdapter:
        """Distinguishable producer attachment identity."""

        marker = "producer-checkout-adapter"

    @contextmanager
    def _fake_attach(pool, **kwargs):
        yield type("Attached", (), {"adapter": _ProducerAdapter()})()

    monkeypatch.setattr(resume_module, "attach_cline_runtime", _fake_attach)
    monkeypatch.setattr(
        resume_module,
        "producer_checkout_directory",
        lambda pool, *, workspace_id, task_id: f"/repo/base/{task_id}",
    )

    def _fake_decision(
        pool,
        runtime,
        github,
        *,
        profile_id,
        workspace_id,
        task_id,
        expected_state_token,
        dispatch,
        task_resume_intent_id=None,
    ):
        captured["runtimes"].append(runtime)
        captured["settle_id"] = task_resume_intent_id
        return decision

    monkeypatch.setattr(resume_module, "finalize_planning_review_decision", _fake_decision)
    monkeypatch.setattr(
        resume_module,
        "open_implementation_authorization",
        lambda pool, github, **kwargs: captured["presentations"].append(kwargs),
    )
    return captured


def test_an_accepted_planning_review_presents_the_implementation_gate(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from openorc.domain.planning import PlanRevision
    from openorc.domain.reviews import ReviewLoopPurpose
    from openorc.protocol.models import ReviewResult
    from openorc.services.task_planning_review_decision import (
        PlanningReviewDecision,
        TaskPlanningReviewDecisionOutcome,
    )

    loop = _review_loop(ReviewLoopPurpose.PLANNING)
    iteration = _review_iteration()
    object.__setattr__(iteration, "review_loop_id", loop.id)
    accepted_task = _task(status=TaskStatus.PLANNING, token=uuid.uuid4())
    decision = TaskPlanningReviewDecisionOutcome(
        decision=PlanningReviewDecision.ACCEPTED_CLEAR,
        review_iteration=iteration,
        plan_revision=PlanRevision(
            id=uuid.uuid4(),
            workspace_id=_WORKSPACE_ID,
            task_id=_TASK_ID,
            revision_number=1,
            content="plan",
            repository_base_sha="a" * 64,
            created_at=_NOW,
        ),
        review_loop=loop,
        task=accepted_task,
        revision_production=None,
        resolution_gate=None,
    )
    captured = _wire_review_route(monkeypatch, loop=loop, iteration=iteration, decision=decision)
    record = _resume_record(TaskResumeIntentStatus.DISPATCHED)
    object.__setattr__(
        record,
        "routing",
        {
            "control": "owner_resume",
            "operation": "reviewer_review",
            "expected_response_family": "review_result",
            "subject_kind": "review_iteration",
            "subject_id": str(iteration.id),
            "restored_status": TaskStatus.PLANNING.value,
            "authorized_state_token": str(_STATE_TOKEN),
        },
    )
    outcome = _route_resumed_review_result_entry(
        monkeypatch,
        record=record,
        subject_id=iteration.id,
        response=ReviewResult(outcome="ACCEPTED", summary="s", findings=()),
    )
    assert outcome.kind is SessionResumeOutcomeKind.COMPLETED
    assert outcome.follow_up is SessionResumeFollowUp.NONE
    assert captured["presentations"], "the accepted review presents its gate"
    assert captured["presentations"][0]["expected_state_token"] == accepted_task.state_token
    assert captured["settle_id"] == record.id


def _route_resumed_review_result_entry(
    monkeypatch: pytest.MonkeyPatch, *, record: Any, subject_id: Any, response: Any
) -> Any:
    return resume_module._route_resumed_review_result(
        _POOL,
        cast(Any, None),
        producer_runtime_factory=cast(Any, object()),
        reviewer_runtime_factory=cast(Any, object()),
        record=record,
        subject_id=subject_id,
        authorized_token=_STATE_TOKEN,
        response=response,
        profile_id=_OWNER_PROFILE_ID,
    )


def test_a_revision_routed_planning_review_hands_the_next_review_dispatch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from openorc.domain.planning import PlanRevision
    from openorc.domain.reviews import ReviewLoopPurpose
    from openorc.protocol.models import ReviewResult
    from openorc.services.task_plan_production import TaskPlanProductionOutcome
    from openorc.services.task_planning_review_decision import (
        PlanningReviewDecision,
        TaskPlanningReviewDecisionOutcome,
    )

    loop = _review_loop(ReviewLoopPurpose.PLANNING)
    iteration = _review_iteration()
    object.__setattr__(iteration, "review_loop_id", loop.id)
    routed_task = _task(status=TaskStatus.PLANNING, token=uuid.uuid4())
    routed_production = TaskPlanProductionOutcome(
        plan_revision=PlanRevision(
            id=uuid.uuid4(),
            workspace_id=_WORKSPACE_ID,
            task_id=_TASK_ID,
            revision_number=2,
            content="plan-2",
            repository_base_sha="a" * 64,
            created_at=_NOW,
        ),
        task=routed_task,
    )
    decision = TaskPlanningReviewDecisionOutcome(
        decision=PlanningReviewDecision.REVISION_ROUTED,
        review_iteration=iteration,
        plan_revision=routed_production.plan_revision,
        review_loop=loop,
        task=routed_task,
        revision_production=routed_production,
        resolution_gate=None,
    )
    captured = _wire_review_route(monkeypatch, loop=loop, iteration=iteration, decision=decision)
    record = _resume_record(TaskResumeIntentStatus.DISPATCHED)
    object.__setattr__(
        record,
        "routing",
        {
            "control": "owner_resume",
            "operation": "reviewer_review",
            "expected_response_family": "review_result",
            "subject_kind": "review_iteration",
            "subject_id": str(iteration.id),
            "restored_status": TaskStatus.PLANNING.value,
            "authorized_state_token": str(_STATE_TOKEN),
        },
    )
    outcome = _route_resumed_review_result_entry(
        monkeypatch,
        record=record,
        subject_id=iteration.id,
        response=ReviewResult(outcome="CHANGES_REQUESTED", summary="s", findings=()),
    )
    assert outcome.follow_up is SessionResumeFollowUp.PLANNING_REVIEW_DISPATCH
    assert outcome.follow_up_state_token == routed_task.state_token
    # The decision's runtime argument is the PRODUCER-checkout attachment,
    # never the Reviewer attachment the resumed turn itself used.
    assert captured["runtimes"][0].marker == "producer-checkout-adapter"
    assert captured["settle_id"] == record.id


def test_the_resumed_initial_plan_hands_the_next_review_dispatch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from openorc.domain.planning import PlanRevision
    from openorc.protocol.models import PlanResult
    from openorc.services.task_plan_production import TaskPlanProductionOutcome

    produced_task = _task(status=TaskStatus.PLANNING, token=uuid.uuid4())
    production = TaskPlanProductionOutcome(
        plan_revision=PlanRevision(
            id=uuid.uuid4(),
            workspace_id=_WORKSPACE_ID,
            task_id=_TASK_ID,
            revision_number=1,
            content="plan",
            repository_base_sha="a" * 64,
            created_at=_NOW,
        ),
        task=produced_task,
    )
    monkeypatch.setattr(
        resume_module,
        "complete_resumed_initial_plan_result",
        lambda *args, **kwargs: production,
    )
    record = _resume_record(TaskResumeIntentStatus.DISPATCHED)
    outcome = resume_module._route_resumed_result(
        _POOL,
        cast(Any, None),
        bootstrap_composition=None,
        producer_runtime_factory=None,
        reviewer_runtime_factory=None,
        record=record,
        operation=resume_module.PendingTaskRuntimeInteractionOperation.INITIAL_PLAN_DISPATCH,
        subject_id=uuid.uuid4(),
        authorized_token=_STATE_TOKEN,
        response=PlanResult(plan="plan"),
        attempt_evidence=None,
    )
    assert outcome.follow_up is SessionResumeFollowUp.PLANNING_REVIEW_DISPATCH
    assert outcome.follow_up_state_token == produced_task.state_token


def test_review_result_settlement_rolls_back_with_the_finalization(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The F8g atomicity seam (issue #292): the record settles or nothing.

    The planning decision's finalization transaction records the original
    review result and settles the resume record together; a settlement
    failure rolls the finalization back and never reaches the external
    revision consequence.
    """
    from openorc.domain.reviews import ReviewLoopPurpose
    from openorc.protocol.models import ReviewResult
    from openorc.services import (
        task_planning_review_decision as decision_module,
    )
    from openorc.services.task_planning_review_decision import (
        TaskPlanningReviewDispatchOutcome,
    )

    loop = _review_loop(ReviewLoopPurpose.PLANNING)
    iteration = _review_iteration()
    object.__setattr__(iteration, "review_loop_id", loop.id)
    finalized = _review_iteration()
    object.__setattr__(finalized, "id", iteration.id)
    object.__setattr__(finalized, "review_loop_id", loop.id)
    finalized_task = _task(status=TaskStatus.PLANNING, token=uuid.uuid4())

    from contextlib import contextmanager as _cm

    @_cm
    def _fake_decision_tx(pool):
        yield pool

    monkeypatch.setattr(decision_module, "composed_transaction", _fake_decision_tx)
    monkeypatch.setattr(decision_module, "_require_runtime", lambda runtime: runtime)
    monkeypatch.setattr(decision_module, "_require_github", lambda github: github)
    monkeypatch.setattr(
        decision_module, "require_account_operational", lambda pool, *, profile_id: None
    )
    monkeypatch.setattr(
        decision_module,
        "require_workspace_task",
        lambda pool, *, profile_id, workspace_id, task_id: None,
    )
    monkeypatch.setattr(
        decision_module.task_records,
        "get_task_for_update",
        lambda pool, task_id: finalized_task,
    )
    monkeypatch.setattr(
        decision_module,
        "require_current_task",
        lambda pool, *, workspace_id, task_id, expected_state_token: finalized_task,
    )
    monkeypatch.setattr(decision_module, "_require_decision_subject", lambda task, attempt: None)
    monkeypatch.setattr(
        decision_module,
        "require_current_plan_revision",
        lambda pool, *, task, plan_revision_id: object(),
    )
    monkeypatch.setattr(
        decision_module, "_require_decision_loop", lambda pool, *, workspace_id, attempt: loop
    )
    monkeypatch.setattr(
        decision_module,
        "_require_reserved_unfinalized_attempt",
        lambda pool, *, loop, attempt, revision: iteration,
    )
    monkeypatch.setattr(
        decision_module,
        "_require_decision_reviewer",
        lambda pool, *, workspace_id, task_id, expected_session: _session(),
    )
    monkeypatch.setattr(
        decision_module.review_records,
        "record_review_iteration_result",
        lambda pool, **kwargs: finalized,
    )

    settle_calls: list[dict[str, Any]] = []

    def _failing_settle(transaction_pool, *, task_resume_intent_id):
        settle_calls.append({"task_resume_intent_id": task_resume_intent_id})
        raise StaleOperationError(
            "the session-resume record is no longer awaiting its settled outcome"
        )

    monkeypatch.setattr(decision_module, "settle_resumed_task_record", _failing_settle)
    external_calls: list[dict[str, Any]] = []
    monkeypatch.setattr(
        decision_module,
        "revise_task_plan",
        lambda pool, *args, **kwargs: external_calls.append(dict(kwargs)),
    )

    record = _resume_record(TaskResumeIntentStatus.DISPATCHED)
    dispatch = TaskPlanningReviewDispatchOutcome(
        review_loop=loop,
        review_iteration=iteration,
        review_result=ReviewResult(outcome="CHANGES_REQUESTED", summary="s", findings=()),
        reviewer_session=_session(),
    )
    with pytest.raises(StaleOperationError, match="no longer awaiting"):
        decision_module.finalize_planning_review_decision(
            _POOL,
            cast(Any, None),
            cast(Any, None),
            profile_id=_OWNER_PROFILE_ID,
            workspace_id=_WORKSPACE_ID,
            task_id=_TASK_ID,
            expected_state_token=_STATE_TOKEN,
            dispatch=dispatch,
            task_resume_intent_id=record.id,
        )
    assert settle_calls == [{"task_resume_intent_id": record.id}]
    assert external_calls == []


def test_the_exhausted_planning_review_settles_inside_the_finalization(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The settlement composes inside the finalization transaction, not after."""
    from openorc.domain.reviews import ReviewLoopPurpose
    from openorc.protocol.models import ReviewResult
    from openorc.services import (
        task_planning_review_decision as decision_module,
    )
    from openorc.services.task_planning_review_decision import (
        TaskPlanningReviewDispatchOutcome,
    )

    loop = _review_loop(ReviewLoopPurpose.PLANNING)
    iteration = _review_iteration()
    object.__setattr__(iteration, "review_loop_id", loop.id)
    finalized = _review_iteration()
    object.__setattr__(finalized, "id", iteration.id)
    object.__setattr__(finalized, "review_loop_id", loop.id)
    exhausted_task = _task(status=TaskStatus.PLANNING, token=uuid.uuid4())

    settle_depths: list[bool] = []
    tx_depth: list[int] = [0]

    from contextlib import contextmanager

    @contextmanager
    def _tracking_tx(pool):
        tx_depth[0] += 1
        try:
            yield pool
        finally:
            tx_depth[0] -= 1

    monkeypatch.setattr(decision_module, "composed_transaction", _tracking_tx)

    def _settling_recorder(transaction_pool, **kwargs):
        settle_depths.append(tx_depth[0] > 0)
        return finalized

    monkeypatch.setattr(
        decision_module.review_records,
        "record_review_iteration_result",
        _settling_recorder,
    )
    monkeypatch.setattr(
        decision_module,
        "settle_resumed_task_record",
        lambda transaction_pool, *, task_resume_intent_id: settle_depths.append(tx_depth[0] > 0),
    )
    monkeypatch.setattr(decision_module, "_require_runtime", lambda runtime: runtime)
    monkeypatch.setattr(decision_module, "_require_github", lambda github: github)
    monkeypatch.setattr(
        decision_module, "require_account_operational", lambda pool, *, profile_id: None
    )
    monkeypatch.setattr(
        decision_module,
        "require_workspace_task",
        lambda pool, *, profile_id, workspace_id, task_id: None,
    )
    monkeypatch.setattr(
        decision_module.task_records,
        "get_task_for_update",
        lambda pool, task_id: exhausted_task,
    )
    monkeypatch.setattr(
        decision_module,
        "require_current_task",
        lambda pool, *, workspace_id, task_id, expected_state_token: exhausted_task,
    )
    monkeypatch.setattr(decision_module, "_require_decision_subject", lambda task, attempt: None)
    monkeypatch.setattr(
        decision_module,
        "require_current_plan_revision",
        lambda pool, *, task, plan_revision_id: object(),
    )
    monkeypatch.setattr(
        decision_module,
        "_require_decision_loop",
        lambda pool, *, workspace_id, attempt: loop,
    )
    monkeypatch.setattr(
        decision_module,
        "_require_reserved_unfinalized_attempt",
        lambda pool, *, loop, attempt, revision: iteration,
    )
    monkeypatch.setattr(
        decision_module,
        "_require_decision_reviewer",
        lambda pool, *, workspace_id, task_id, expected_session: _session(),
    )

    record = _resume_record(TaskResumeIntentStatus.DISPATCHED)
    # The mapped-recording fake's finalized row carries no outcome, so the
    # decision derivation refuses after the recorded/settled pair — the
    # depth assertion is about the composition boundary, not the outcome.
    with pytest.raises(ConflictError):
        decision_module.finalize_planning_review_decision(
            _POOL,
            cast(Any, None),
            cast(Any, None),
            profile_id=_OWNER_PROFILE_ID,
            workspace_id=_WORKSPACE_ID,
            task_id=_TASK_ID,
            expected_state_token=_STATE_TOKEN,
            dispatch=TaskPlanningReviewDispatchOutcome(
                review_loop=loop,
                review_iteration=iteration,
                review_result=ReviewResult(outcome="CHANGES_REQUESTED", summary="s", findings=()),
                reviewer_session=_session(),
            ),
            task_resume_intent_id=record.id,
        )
    assert len(settle_depths) == 2
    assert all(settle_depths), "the result recording and the settlement share the transaction"
