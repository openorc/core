"""Tests for the F5b Level 2 operational-failure handoff coordinator (#262).

Deterministic and fake-backed: the coordinator's persistence reads are
substituted with recording fakes, the Level 2 triage service is injected as a
recording double, and Level 1 is a scripted closure that records into the
collector before failing. No live Postgres, Valkey, GitHub, or Cline is
touched; F4's own persistence, policy, taxonomy, and race suites are reused
rather than re-run.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any
from uuid import UUID

import pytest

from openorc.adapters.agent_runtime.errors import (
    AgentRuntimeConfigurationRejectedError,
    AgentRuntimeDeliveryUncertainError,
    AgentRuntimeProtocolFailureError,
    AgentRuntimeTimeoutError,
    AgentRuntimeTransportError,
    AgentRuntimeUnavailableError,
    AgentSessionNotFoundError,
)
from openorc.domain.events import WorkflowEventType
from openorc.domain.executions import ExecutionStatus
from openorc.domain.operational_failures import (
    FailureEffectEvidence,
    OperationalFailureCode,
    OriginalAttemptKind,
    canonical_failure_subject,
)
from openorc.domain.pending_interactions import PendingTaskRuntimeInteractionOperation
from openorc.domain.send_claims import TaskRuntimeSendClaimKind, TaskRuntimeSendClaimStatus
from openorc.domain.tasks import TaskPlanningBootstrapStage, TaskStatus
from openorc.services import task_workflow_operation_coordinator as coordinator
from openorc.services.errors import (
    ConflictError,
    ExternalOperationFailedError,
    ExternalOperationUncertainError,
    StaleOperationError,
)
from openorc.services.operation_attempt_evidence import InvocationAttemptEvidence
from openorc.services.task_operational_failure_triage import (
    DispositionKind,
    NoOpReason,
    OperationalFailureDisposition,
)
from openorc.services.task_workflow_operation_coordinator import (
    _classify_failure_code,
    coordinate_task_workflow_operation,
)

_WORKSPACE_ID = UUID("0f1c2d3e-4a5b-6c7d-8e9f-000000000001")
_TASK_ID = UUID("0f1c2d3e-4a5b-6c7d-8e9f-000000000002")
_PROFILE_ID = UUID("0f1c2d3e-4a5b-6c7d-8e9f-000000000003")
_PAYLOAD_TOKEN = UUID("0f1c2d3e-4a5b-6c7d-8e9f-000000000004")
_POST_CLAIM_TOKEN = UUID("0f1c2d3e-4a5b-6c7d-8e9f-000000000005")
_CLAIM_TOKEN = UUID("0f1c2d3e-4a5b-6c7d-8e9f-000000000006")
_CLAIM_ID = UUID("0f1c2d3e-4a5b-6c7d-8e9f-000000000007")
_ITERATION_ID = UUID("0f1c2d3e-4a5b-6c7d-8e9f-000000000008")
_EXECUTION_ID = UUID("0f1c2d3e-4a5b-6c7d-8e9f-000000000009")
_ORIGINAL_EXECUTION_ID = UUID("0f1c2d3e-4a5b-6c7d-8e9f-00000000000a")
_ATTEMPT_ID = UUID("0f1c2d3e-4a5b-6c7d-8e9f-00000000000b")
_EPISODE_ID = UUID("0f1c2d3e-4a5b-6c7d-8e9f-00000000000c")
_PLAN_REVISION_ID = UUID("0f1c2d3e-4a5b-6c7d-8e9f-00000000000d")


def _runtime_failure(inner: Exception, outer: type[Exception]) -> Exception:
    """Build an application error with the normalized runtime cause chained."""
    try:
        try:
            raise inner
        except Exception as cause:
            raise outer("the operation failed operationally") from cause
    except Exception as error:  # noqa: BLE001 - test scaffolding
        return error


def _unavailable_failure() -> Exception:
    return _runtime_failure(
        AgentRuntimeUnavailableError("unavailable before any effect"),
        ExternalOperationFailedError,
    )


def _timeout_failure() -> Exception:
    return _runtime_failure(
        AgentRuntimeTimeoutError("deadline exceeded"),
        ExternalOperationUncertainError,
    )


@dataclass
class RecordingTriage:
    """Recording triage double standing in for the F4 disposition service."""

    calls: list[dict[str, Any]] = field(default_factory=list)
    result: OperationalFailureDisposition | None = None
    error: Exception | None = None

    def __call__(self, pool: Any, **kwargs: Any) -> OperationalFailureDisposition:
        self.calls.append(kwargs)
        if self.error is not None:
            raise self.error
        assert self.result is not None
        return self.result

    @property
    def call_count(self) -> int:
        return len(self.calls)


def _blocked_disposition() -> OperationalFailureDisposition:
    return OperationalFailureDisposition(kind=DispositionKind.BLOCKED)


def _stale_no_op_disposition() -> OperationalFailureDisposition:
    return OperationalFailureDisposition(kind=DispositionKind.NO_OP, no_op_reason=NoOpReason.STALE)


def _retry_authorized_disposition() -> OperationalFailureDisposition:
    return OperationalFailureDisposition(kind=DispositionKind.RETRY_AUTHORIZED)


def _patch_persistence(
    monkeypatch: pytest.MonkeyPatch,
    *,
    bootstrap_evidence: Any = None,
    with_bootstrap: bool = False,
    claim: Any = None,
    with_claim: bool = False,
    iteration: Any = None,
    with_iteration: bool = False,
    execution: Any = None,
    with_execution: bool = False,
    bound_attempt: Any = None,
    episode: Any = None,
    events: list[Any] | None = None,
) -> None:
    """Substitute the coordinator's focused durable reads with fakes."""
    if with_bootstrap:
        monkeypatch.setattr(
            coordinator.task_records,
            "get_task_planning_bootstrap_evidence",
            lambda _pool, *, task_id: bootstrap_evidence,
        )
    if with_claim:
        monkeypatch.setattr(
            coordinator.send_claim_records,
            "get_task_runtime_send_claim",
            lambda _pool, *, claim_id: claim,
        )
    if with_iteration:
        monkeypatch.setattr(
            coordinator.review_records,
            "get_review_iteration",
            lambda _pool, *, review_iteration_id: iteration,
        )
    if with_execution:
        monkeypatch.setattr(
            coordinator.execution_records,
            "get_execution",
            lambda _pool, *, execution_id: execution,
        )
    monkeypatch.setattr(
        coordinator.failure_records,
        "find_operational_failure_attempt_by_resulting_execution",
        lambda _pool, *, resulting_execution_id: bound_attempt,
    )
    monkeypatch.setattr(
        coordinator.failure_records,
        "get_operational_failure_episode",
        lambda _pool, *, episode_id: episode,
    )
    monkeypatch.setattr(
        coordinator.event_records,
        "find_events_by_subject",
        lambda _pool, **_kwargs: list(events or []),
    )


def _coordinate(
    *,
    level_one: Any,
    triage: RecordingTriage,
    expected_status: TaskStatus = TaskStatus.IMPLEMENTING,
) -> None:
    coordinate_task_workflow_operation(
        object(),  # type: ignore[arg-type]
        expected_status=expected_status,
        workspace_id=_WORKSPACE_ID,
        task_id=_TASK_ID,
        profile_id=_PROFILE_ID,
        expected_state_token=_PAYLOAD_TOKEN,
        invoke_level_one=level_one,
        triage=triage,
    )


def _fake_claim(**overrides: Any) -> Any:
    facts: dict[str, Any] = {
        "id": _CLAIM_ID,
        "workspace_id": _WORKSPACE_ID,
        "task_id": _TASK_ID,
        "kind": TaskRuntimeSendClaimKind.INITIAL_PLAN_DISPATCH,
        "status": TaskRuntimeSendClaimStatus.KNOWN_NON_DELIVERY,
        "outcome": {"failure_kind": "runtime_pre_effect"},
        "review_iteration_id": None,
        "owner_gate_id": None,
        "subject_head_sha": None,
    }
    facts.update(overrides)
    from types import SimpleNamespace

    return SimpleNamespace(**facts)


def _fake_iteration(**overrides: Any) -> Any:
    from types import SimpleNamespace

    facts: dict[str, Any] = {
        "id": _ITERATION_ID,
        "workspace_id": _WORKSPACE_ID,
        "task_id": _TASK_ID,
        "outcome": None,
        "plan_revision_id": _PLAN_REVISION_ID,
        "task_pull_request_id": None,
    }
    facts.update(overrides)
    return SimpleNamespace(**facts)


def _fake_execution(**overrides: Any) -> Any:
    from types import SimpleNamespace

    facts: dict[str, Any] = {
        "id": _EXECUTION_ID,
        "workspace_id": _WORKSPACE_ID,
        "task_id": _TASK_ID,
        "status": ExecutionStatus.FAILED_TRANSIENT,
        "review_iteration_id": None,
    }
    facts.update(overrides)
    return SimpleNamespace(**facts)


def _fake_failed_event() -> Any:
    from types import SimpleNamespace

    return SimpleNamespace(
        event_type=WorkflowEventType.EXECUTION_FAILED,
        context={"failure_kind": "runtime_pre_effect"},
    )


def _fake_attempt() -> Any:
    from types import SimpleNamespace

    return SimpleNamespace(
        id=_ATTEMPT_ID,
        workspace_id=_WORKSPACE_ID,
        task_id=_TASK_ID,
        episode_id=_EPISODE_ID,
        resulting_execution_id=_EXECUTION_ID,
    )


def _fake_episode() -> Any:
    from types import SimpleNamespace

    return SimpleNamespace(
        id=_EPISODE_ID,
        workspace_id=_WORKSPACE_ID,
        task_id=_TASK_ID,
        operation_family=PendingTaskRuntimeInteractionOperation.IMPLEMENTATION_EXECUTION,
        original_attempt_kind=OriginalAttemptKind.EXECUTION,
        original_attempt_id=_ORIGINAL_EXECUTION_ID,
        subject={"workspace_id": str(_WORKSPACE_ID), "task_id": str(_TASK_ID)},
    )


# --- classification matrix (type-only, no text parsing) -----------------------


def test_the_runtime_taxonomy_classifies_into_the_closed_code_vocabulary() -> None:
    cases = [
        (
            AgentRuntimeConfigurationRejectedError,
            OperationalFailureCode.RUNTIME_CONFIGURATION_REJECTED,
        ),
        (AgentRuntimeUnavailableError, OperationalFailureCode.RUNTIME_UNAVAILABLE),
        (AgentRuntimeTimeoutError, OperationalFailureCode.RUNTIME_TIMEOUT),
        (AgentRuntimeDeliveryUncertainError, OperationalFailureCode.RUNTIME_DELIVERY_UNCERTAIN),
        (AgentSessionNotFoundError, OperationalFailureCode.AGENT_SESSION_LOST),
        (AgentRuntimeTransportError, OperationalFailureCode.RUNTIME_TRANSPORT),
        (AgentRuntimeProtocolFailureError, OperationalFailureCode.RUNTIME_PROTOCOL),
    ]
    for runtime_type, expected_code in cases:
        error = _runtime_failure(runtime_type("safe"), ExternalOperationFailedError)
        assert _classify_failure_code(error) is expected_code


def test_the_application_types_and_the_unknown_fallback_classify() -> None:
    assert (
        _classify_failure_code(ExternalOperationFailedError("known failure"))
        is OperationalFailureCode.EXTERNAL_OPERATION_FAILED
    )
    assert _classify_failure_code(StaleOperationError("moved on")) is OperationalFailureCode.UNKNOWN
    assert _classify_failure_code(TypeError("unexpected")) is OperationalFailureCode.UNKNOWN


# --- invocation-local correlation ---------------------------------------------


def test_a_success_is_a_transparent_passthrough_for_both_representative_roles(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    triage = RecordingTriage()
    invocations: list[InvocationAttemptEvidence] = []

    def level_one(evidence: InvocationAttemptEvidence) -> None:
        invocations.append(evidence)

    _coordinate(level_one=level_one, triage=triage)
    _coordinate(
        level_one=level_one,
        triage=triage,
        expected_status=TaskStatus.PLANNING,
    )

    assert triage.call_count == 0
    assert len(invocations) == 2
    assert invocations[0].current_task_token is _PAYLOAD_TOKEN
    assert invocations[0].latest_identity is None


def test_a_pre_claim_failure_with_another_invocations_outstanding_iteration_is_never_triaged(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # The review finding's exact scenario: a duplicate invocation fails at
    # attachment, before it produced any durable identity, while an earlier
    # invocation's unfinalized ReviewIteration is still durably outstanding.
    triage = RecordingTriage(result=_blocked_disposition())
    _patch_persistence(monkeypatch, iteration=_fake_iteration(), with_iteration=True)
    error = _unavailable_failure()

    def level_one(_evidence: InvocationAttemptEvidence) -> None:
        raise error

    with pytest.raises(ExternalOperationFailedError) as excinfo:
        _coordinate(
            level_one=level_one,
            triage=triage,
            expected_status=TaskStatus.PLANNING,
        )

    assert triage.call_count == 0
    assert excinfo.value is error


def test_workflow_semantic_errors_propagate_without_triage(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    triage = RecordingTriage(result=_blocked_disposition())
    _patch_persistence(monkeypatch, execution=_fake_execution(), with_execution=True)
    error = StaleOperationError("the operation addressed state that has moved on")

    def level_one(evidence: InvocationAttemptEvidence) -> None:
        evidence.record_attempt(
            family=PendingTaskRuntimeInteractionOperation.IMPLEMENTATION_EXECUTION,
            original_attempt_kind=OriginalAttemptKind.EXECUTION,
            original_attempt_id=_EXECUTION_ID,
            phase="authorized_implementation_execution",
        )
        raise error

    with pytest.raises(StaleOperationError) as excinfo:
        _coordinate(level_one=level_one, triage=triage)

    assert triage.call_count == 0
    assert excinfo.value is error


# --- bootstrap currentness and phase-specific families -------------------------


def _bootstrap_prepare_level_one(evidence: InvocationAttemptEvidence) -> None:
    evidence.record_task_token(_POST_CLAIM_TOKEN)
    evidence.record_attempt(
        family=PendingTaskRuntimeInteractionOperation.PLANNING_BOOTSTRAP_PREPARE,
        original_attempt_kind=OriginalAttemptKind.PLANNING_BOOTSTRAP_CLAIM,
        original_attempt_id=_CLAIM_TOKEN,
        phase="planning_bootstrap_prepare",
    )
    raise _unavailable_failure()


def test_a_prepare_phase_failure_reaches_triage_with_the_post_claim_token(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    triage = RecordingTriage(result=_blocked_disposition())
    _patch_persistence(
        monkeypatch,
        bootstrap_evidence=None,
        with_bootstrap=True,
    )
    # The stage evidence is read through the same helper; substitute the
    # unset-stage row this invocation's claim produced.
    from types import SimpleNamespace

    monkeypatch.setattr(
        coordinator.task_records,
        "get_task_planning_bootstrap_evidence",
        lambda _pool, *, task_id: SimpleNamespace(claim_token=_CLAIM_TOKEN, stage=None),
    )
    error = _unavailable_failure()

    def level_one(evidence: InvocationAttemptEvidence) -> None:
        evidence.record_task_token(_POST_CLAIM_TOKEN)
        evidence.record_attempt(
            family=PendingTaskRuntimeInteractionOperation.PLANNING_BOOTSTRAP_PREPARE,
            original_attempt_kind=OriginalAttemptKind.PLANNING_BOOTSTRAP_CLAIM,
            original_attempt_id=_CLAIM_TOKEN,
            phase="planning_bootstrap_prepare",
        )
        raise error

    with pytest.raises(ExternalOperationFailedError) as excinfo:
        _coordinate(
            level_one=level_one,
            triage=triage,
            expected_status=TaskStatus.PLANNING,
        )

    assert triage.call_count == 1
    call = triage.calls[0]
    assert call["expected_state_token"] is _POST_CLAIM_TOKEN
    assert call["expected_status"] is TaskStatus.PLANNING
    assert call["expected_current_owner_gate_id"] is None
    operation = call["operation"]
    assert operation.family is PendingTaskRuntimeInteractionOperation.PLANNING_BOOTSTRAP_PREPARE
    assert operation.original_attempt_kind is OriginalAttemptKind.PLANNING_BOOTSTRAP_CLAIM
    assert operation.original_attempt_id is _CLAIM_TOKEN
    # The constructed subject satisfies F4's own canonicalization contract.
    canonical_failure_subject(dict(operation.subject))
    report = call["report"]
    assert report.error_code is OperationalFailureCode.RUNTIME_UNAVAILABLE
    assert report.effect_evidence is FailureEffectEvidence.PRE_EFFECT_NOT_APPLIED
    assert excinfo.value is error
    assert excinfo.value is error


def test_a_transition_phase_failure_reports_the_transition_family(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    triage = RecordingTriage(result=_blocked_disposition())
    from types import SimpleNamespace

    monkeypatch.setattr(
        coordinator.task_records,
        "get_task_planning_bootstrap_evidence",
        lambda _pool, *, task_id: SimpleNamespace(
            claim_token=_CLAIM_TOKEN, stage=TaskPlanningBootstrapStage.PREPARED
        ),
    )
    error = _unavailable_failure()

    def level_one(evidence: InvocationAttemptEvidence) -> None:
        evidence.record_task_token(_POST_CLAIM_TOKEN)
        evidence.record_attempt(
            family=PendingTaskRuntimeInteractionOperation.PLANNING_BOOTSTRAP_PREPARE,
            original_attempt_kind=OriginalAttemptKind.PLANNING_BOOTSTRAP_CLAIM,
            original_attempt_id=_CLAIM_TOKEN,
            phase="planning_bootstrap_prepare",
        )
        evidence.record_attempt(
            family=PendingTaskRuntimeInteractionOperation.PLANNING_BOOTSTRAP_TRANSITION,
            original_attempt_kind=OriginalAttemptKind.PLANNING_BOOTSTRAP_CLAIM,
            original_attempt_id=_CLAIM_TOKEN,
            phase="planning_bootstrap_transition",
        )
        raise error

    with pytest.raises(ExternalOperationFailedError) as excinfo:
        _coordinate(level_one=level_one, triage=triage, expected_status=TaskStatus.PLANNING)

    assert triage.call_count == 1
    operation = triage.calls[0]["operation"]
    assert operation.family is PendingTaskRuntimeInteractionOperation.PLANNING_BOOTSTRAP_TRANSITION
    assert operation.original_attempt_id is _CLAIM_TOKEN
    assert triage.calls[0]["expected_state_token"] is _POST_CLAIM_TOKEN
    assert excinfo.value is error


def test_an_initial_plan_dispatch_failure_after_bootstrap_reports_the_send_claim(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    triage = RecordingTriage(result=_blocked_disposition())
    _patch_persistence(monkeypatch, claim=_fake_claim(), with_claim=True)
    error = _unavailable_failure()

    def level_one(evidence: InvocationAttemptEvidence) -> None:
        evidence.record_task_token(_POST_CLAIM_TOKEN)
        evidence.record_attempt(
            family=PendingTaskRuntimeInteractionOperation.PLANNING_BOOTSTRAP_PREPARE,
            original_attempt_kind=OriginalAttemptKind.PLANNING_BOOTSTRAP_CLAIM,
            original_attempt_id=_CLAIM_TOKEN,
            phase="planning_bootstrap_prepare",
        )
        evidence.record_attempt(
            family=PendingTaskRuntimeInteractionOperation.INITIAL_PLAN_DISPATCH,
            original_attempt_kind=OriginalAttemptKind.RUNTIME_SEND_CLAIM,
            original_attempt_id=_CLAIM_ID,
            phase="initial_plan_dispatch",
        )
        raise error

    with pytest.raises(ExternalOperationFailedError) as excinfo:
        _coordinate(level_one=level_one, triage=triage, expected_status=TaskStatus.PLANNING)

    assert triage.call_count == 1
    operation = triage.calls[0]["operation"]
    assert operation.family is PendingTaskRuntimeInteractionOperation.INITIAL_PLAN_DISPATCH
    assert operation.original_attempt_id is _CLAIM_ID
    # The constructed subject satisfies F4's own canonicalization contract.
    canonical_failure_subject(dict(operation.subject))
    assert operation.subject["workspace_id"] == str(_WORKSPACE_ID)
    assert triage.calls[0]["expected_state_token"] is _POST_CLAIM_TOKEN
    report = triage.calls[0]["report"]
    assert report.effect_evidence is FailureEffectEvidence.PRE_EFFECT_NOT_APPLIED
    assert excinfo.value is error


def test_an_uncertain_send_outcome_stays_effect_unclassified(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    triage = RecordingTriage(result=_blocked_disposition())
    _patch_persistence(
        monkeypatch,
        claim=_fake_claim(status=TaskRuntimeSendClaimStatus.CLAIMED),
        with_claim=True,
    )
    error = _timeout_failure()

    def level_one(evidence: InvocationAttemptEvidence) -> None:
        evidence.record_attempt(
            family=PendingTaskRuntimeInteractionOperation.INITIAL_PLAN_DISPATCH,
            original_attempt_kind=OriginalAttemptKind.RUNTIME_SEND_CLAIM,
            original_attempt_id=_CLAIM_ID,
            phase="initial_plan_dispatch",
        )
        raise error

    with pytest.raises(Exception) as excinfo:  # noqa: PT011 - exact type asserted below
        _coordinate(level_one=level_one, triage=triage, expected_status=TaskStatus.PLANNING)

    assert triage.call_count == 1
    report = triage.calls[0]["report"]
    assert report.error_code is OperationalFailureCode.RUNTIME_TIMEOUT
    assert report.effect_evidence is FailureEffectEvidence.EFFECT_UNCLASSIFIED
    assert excinfo.value is error


def test_a_settled_claim_fails_closed_without_triage(monkeypatch: pytest.MonkeyPatch) -> None:
    triage = RecordingTriage(result=_blocked_disposition())
    _patch_persistence(
        monkeypatch,
        claim=_fake_claim(status=TaskRuntimeSendClaimStatus.SETTLED),
        with_claim=True,
    )
    error = _unavailable_failure()

    def level_one(evidence: InvocationAttemptEvidence) -> None:
        evidence.record_attempt(
            family=PendingTaskRuntimeInteractionOperation.INITIAL_PLAN_DISPATCH,
            original_attempt_kind=OriginalAttemptKind.RUNTIME_SEND_CLAIM,
            original_attempt_id=_CLAIM_ID,
            phase="initial_plan_dispatch",
        )
        raise error

    with pytest.raises(ExternalOperationFailedError) as excinfo:
        _coordinate(level_one=level_one, triage=triage, expected_status=TaskStatus.PLANNING)

    assert triage.call_count == 0
    assert excinfo.value is error


def test_f4s_blocked_disposition_is_authoritative_and_nothing_further_applies(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    triage = RecordingTriage(result=_blocked_disposition())
    _patch_persistence(monkeypatch, execution=_fake_execution(), with_execution=True)
    invocations: list[int] = []
    error = _unavailable_failure()

    def level_one(evidence: InvocationAttemptEvidence) -> None:
        invocations.append(1)
        evidence.record_attempt(
            family=PendingTaskRuntimeInteractionOperation.IMPLEMENTATION_EXECUTION,
            original_attempt_kind=OriginalAttemptKind.EXECUTION,
            original_attempt_id=_EXECUTION_ID,
            phase="authorized_implementation_execution",
        )
        raise error

    with pytest.raises(ExternalOperationFailedError) as excinfo:
        _coordinate(level_one=level_one, triage=triage)

    assert triage.call_count == 1
    assert len(invocations) == 1  # Level 1 is never replayed by the coordinator.
    assert excinfo.value is error


def test_a_stale_no_op_disposition_is_authoritative(monkeypatch: pytest.MonkeyPatch) -> None:
    triage = RecordingTriage(result=_stale_no_op_disposition())
    _patch_persistence(monkeypatch, execution=_fake_execution(), with_execution=True)
    error = _unavailable_failure()

    def level_one(evidence: InvocationAttemptEvidence) -> None:
        evidence.record_attempt(
            family=PendingTaskRuntimeInteractionOperation.IMPLEMENTATION_EXECUTION,
            original_attempt_kind=OriginalAttemptKind.EXECUTION,
            original_attempt_id=_EXECUTION_ID,
            phase="authorized_implementation_execution",
        )
        raise error

    with pytest.raises(ExternalOperationFailedError) as excinfo:
        _coordinate(level_one=level_one, triage=triage)

    assert triage.call_count == 1  # No second triage, no extra block.
    assert excinfo.value is error


def test_an_authorized_retry_is_recorded_but_never_consumed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    triage = RecordingTriage(result=_retry_authorized_disposition())
    _patch_persistence(monkeypatch, execution=_fake_execution(), with_execution=True)
    invocations: list[int] = []
    error = _unavailable_failure()

    def level_one(evidence: InvocationAttemptEvidence) -> None:
        invocations.append(1)
        evidence.record_attempt(
            family=PendingTaskRuntimeInteractionOperation.IMPLEMENTATION_EXECUTION,
            original_attempt_kind=OriginalAttemptKind.EXECUTION,
            original_attempt_id=_EXECUTION_ID,
            phase="authorized_implementation_execution",
        )
        raise error

    with pytest.raises(ExternalOperationFailedError) as excinfo:
        _coordinate(level_one=level_one, triage=triage)

    # F5c owns the additional implementation attempt; the coordinator records
    # the disposition and never consumes the authorization or replays Level 1.
    assert triage.call_count == 1
    assert len(invocations) == 1
    assert excinfo.value is error


def test_a_triage_failure_preserves_the_original_error(monkeypatch: pytest.MonkeyPatch) -> None:
    triage = RecordingTriage(error=ConflictError("the triage composition failed"))
    _patch_persistence(monkeypatch, execution=_fake_execution(), with_execution=True)
    error = _unavailable_failure()

    def level_one(evidence: InvocationAttemptEvidence) -> None:
        evidence.record_attempt(
            family=PendingTaskRuntimeInteractionOperation.IMPLEMENTATION_EXECUTION,
            original_attempt_kind=OriginalAttemptKind.EXECUTION,
            original_attempt_id=_EXECUTION_ID,
            phase="authorized_implementation_execution",
        )
        raise error

    with pytest.raises(ExternalOperationFailedError) as excinfo:
        _coordinate(level_one=level_one, triage=triage)

    assert excinfo.value is error


def test_a_prior_authorized_retry_execution_reports_the_episodes_original_identity(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    triage = RecordingTriage(result=_blocked_disposition())
    _patch_persistence(
        monkeypatch,
        execution=_fake_execution(),
        with_execution=True,
        bound_attempt=_fake_attempt(),
        episode=_fake_episode(),
        events=[_fake_failed_event()],
    )
    error = _unavailable_failure()

    def level_one(evidence: InvocationAttemptEvidence) -> None:
        evidence.record_attempt(
            family=PendingTaskRuntimeInteractionOperation.IMPLEMENTATION_EXECUTION,
            original_attempt_kind=OriginalAttemptKind.EXECUTION,
            original_attempt_id=_EXECUTION_ID,
            phase="authorized_implementation_execution",
        )
        raise error

    with pytest.raises(ExternalOperationFailedError) as excinfo:
        _coordinate(level_one=level_one, triage=triage)

    assert triage.call_count == 1
    operation = triage.calls[0]["operation"]
    # The episode's original Execution identity is retained; the reported
    # attempt is the durably authorized one and the effect row is the exact
    # bound retry Execution.
    assert operation.original_attempt_id is _ORIGINAL_EXECUTION_ID
    report = triage.calls[0]["report"]
    assert report.reporting_attempt_kind.value == "prior_authorized"
    assert report.reporting_attempt_id is _ATTEMPT_ID
    assert report.effect_attempt_id is _EXECUTION_ID
    assert report.effect_evidence is FailureEffectEvidence.PRE_EFFECT_NOT_APPLIED
    assert excinfo.value is error


def test_an_unbound_execution_failure_reports_the_original_identity(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    triage = RecordingTriage(result=_blocked_disposition())
    _patch_persistence(
        monkeypatch,
        execution=_fake_execution(),
        with_execution=True,
        events=[_fake_failed_event()],
    )
    error = _unavailable_failure()

    def level_one(evidence: InvocationAttemptEvidence) -> None:
        evidence.record_attempt(
            family=PendingTaskRuntimeInteractionOperation.IMPLEMENTATION_EXECUTION,
            original_attempt_kind=OriginalAttemptKind.EXECUTION,
            original_attempt_id=_EXECUTION_ID,
            phase="authorized_implementation_execution",
        )
        raise error

    with pytest.raises(ExternalOperationFailedError) as excinfo:
        _coordinate(level_one=level_one, triage=triage)

    assert triage.call_count == 1
    operation = triage.calls[0]["operation"]
    assert operation.original_attempt_id is _EXECUTION_ID
    # The constructed subject satisfies F4's own canonicalization contract.
    canonical_failure_subject(dict(operation.subject))
    assert operation.subject["task_id"] == str(_TASK_ID)
    report = triage.calls[0]["report"]
    assert report.reporting_attempt_kind.value == "original"
    assert report.effect_attempt_id is None
    assert report.effect_evidence is FailureEffectEvidence.PRE_EFFECT_NOT_APPLIED
    assert excinfo.value is error


def test_a_running_execution_stays_effect_unclassified(monkeypatch: pytest.MonkeyPatch) -> None:
    triage = RecordingTriage(result=_blocked_disposition())
    _patch_persistence(
        monkeypatch,
        execution=_fake_execution(status=ExecutionStatus.RUNNING),
        with_execution=True,
    )
    error = _timeout_failure()

    def level_one(evidence: InvocationAttemptEvidence) -> None:
        evidence.record_attempt(
            family=PendingTaskRuntimeInteractionOperation.IMPLEMENTATION_EXECUTION,
            original_attempt_kind=OriginalAttemptKind.EXECUTION,
            original_attempt_id=_EXECUTION_ID,
            phase="authorized_implementation_execution",
        )
        raise error

    with pytest.raises(Exception) as excinfo:  # noqa: PT011 - exact type asserted below
        _coordinate(level_one=level_one, triage=triage)

    assert triage.call_count == 1
    report = triage.calls[0]["report"]
    assert report.effect_evidence is FailureEffectEvidence.EFFECT_UNCLASSIFIED
    assert excinfo.value is error


def test_a_known_failed_execution_without_pre_effect_proof_stays_known_failed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    triage = RecordingTriage(result=_blocked_disposition())
    _patch_persistence(
        monkeypatch,
        execution=_fake_execution(),
        with_execution=True,
        events=[],  # The corroborating runtime-pre-effect event is absent.
    )
    error = _unavailable_failure()

    def level_one(evidence: InvocationAttemptEvidence) -> None:
        evidence.record_attempt(
            family=PendingTaskRuntimeInteractionOperation.IMPLEMENTATION_EXECUTION,
            original_attempt_kind=OriginalAttemptKind.EXECUTION,
            original_attempt_id=_EXECUTION_ID,
            phase="authorized_implementation_execution",
        )
        raise error

    with pytest.raises(ExternalOperationFailedError) as excinfo:
        _coordinate(level_one=level_one, triage=triage)

    assert triage.call_count == 1
    report = triage.calls[0]["report"]
    assert report.effect_evidence is FailureEffectEvidence.KNOWN_FAILED
    assert excinfo.value is error
    assert triage.call_count == 1
    report = triage.calls[0]["report"]
    assert report.effect_evidence is FailureEffectEvidence.KNOWN_FAILED
    assert excinfo.value is error


def test_a_review_iteration_failure_reports_the_iteration_identity(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    triage = RecordingTriage(result=_blocked_disposition())
    _patch_persistence(monkeypatch, iteration=_fake_iteration(), with_iteration=True)
    error = _unavailable_failure()

    def level_one(evidence: InvocationAttemptEvidence) -> None:
        evidence.record_attempt(
            family=PendingTaskRuntimeInteractionOperation.REVIEWER_REVIEW,
            original_attempt_kind=OriginalAttemptKind.REVIEW_ITERATION,
            original_attempt_id=_ITERATION_ID,
            phase="planning_review_dispatch",
        )
        raise error

    with pytest.raises(ExternalOperationFailedError) as excinfo:
        _coordinate(level_one=level_one, triage=triage, expected_status=TaskStatus.PLANNING)

    assert triage.call_count == 1
    operation = triage.calls[0]["operation"]
    assert operation.family is PendingTaskRuntimeInteractionOperation.REVIEWER_REVIEW
    assert operation.original_attempt_kind is OriginalAttemptKind.REVIEW_ITERATION
    assert operation.original_attempt_id is _ITERATION_ID
    # The constructed subject satisfies F4's own canonicalization contract.
    canonical_failure_subject(dict(operation.subject))
    assert operation.subject["review_iteration_id"] == str(_ITERATION_ID)
    assert triage.calls[0]["expected_status"] is TaskStatus.PLANNING
    report = triage.calls[0]["report"]
    assert report.error_code is OperationalFailureCode.RUNTIME_UNAVAILABLE
    assert report.effect_evidence is FailureEffectEvidence.EFFECT_UNCLASSIFIED
    assert excinfo.value is error


def test_an_evidence_resolution_failure_never_replaces_the_original_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    triage = RecordingTriage(result=_blocked_disposition())
    _patch_persistence(monkeypatch, execution=_fake_execution(), with_execution=True)

    def failing_read(_pool: Any, *, execution_id: UUID) -> None:
        raise RuntimeError("the evidence read failed")

    monkeypatch.setattr(coordinator.execution_records, "get_execution", failing_read)
    error = _unavailable_failure()

    def level_one(evidence: InvocationAttemptEvidence) -> None:
        evidence.record_attempt(
            family=PendingTaskRuntimeInteractionOperation.IMPLEMENTATION_EXECUTION,
            original_attempt_kind=OriginalAttemptKind.EXECUTION,
            original_attempt_id=_EXECUTION_ID,
            phase="authorized_implementation_execution",
        )
        raise error

    with pytest.raises(ExternalOperationFailedError) as excinfo:
        _coordinate(level_one=level_one, triage=triage)

    # The original Level 1 exception object propagates unchanged and F4 is
    # never called without verified evidence.
    assert excinfo.value is error
    assert triage.call_count == 0
