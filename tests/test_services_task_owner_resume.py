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
