"""Deterministic domain tests for the detached-turn receipt models
(issues #322/#348).

The closed forward-only status vocabulary with its exact per-status
coherence, the canonical safe outcome payload, and the runtime-neutral
evidence and attribution vocabulary the detached-turn boundary consumes —
the transport-independent validation the persistence boundary and services
rely on.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

import pytest

from openorc.domain.detached_turns import (
    REFUSAL_OUTCOME_KIND_KEY,
    SETTLEMENT_OBSERVED_FAILURE,
    SETTLEMENT_OUTCOME_LABEL_KEY,
    SETTLEMENT_OUTCOME_SEQUENCE_KEY,
    SETTLEMENT_RESULT_NOT_APPLIED,
    SETTLEMENT_VALIDATED_RESULT,
    DetachedTurnObservationKind,
    NativeTurnEvidence,
    ObservedTurnEvidence,
    TaskDetachedTurnEarlyCompletion,
    TaskDetachedTurnReceipt,
    TaskDetachedTurnReceiptStatus,
    TurnActivityAttestation,
    TurnAttribution,
    TurnEvidenceFamily,
    TurnFreshness,
    TurnStatusEvidenceVerdict,
    TurnTerminalClassification,
    TurnTerminalOutcome,
    task_detached_turn_early_completion_field_names,
    task_detached_turn_receipt_field_names,
)
from openorc.domain.operational_failures import OriginalAttemptKind
from openorc.domain.pending_interactions import PendingTaskRuntimeInteractionOperation

_OBSERVED = datetime(2026, 11, 15, 9, 0, 0, tzinfo=UTC)


def _receipt(
    *,
    status: TaskDetachedTurnReceiptStatus = TaskDetachedTurnReceiptStatus.CLAIMED,
    admitted_turn_reference: object = None,
    admitted_at: object = None,
    settled_at: object = None,
    outcome: Any = None,
    contradicted_at: object = None,
    expected_formal_family: str = "implementation_result",
    operation: Any = None,
    original_attempt_kind: Any = None,
    claimed_at: object = None,
) -> TaskDetachedTurnReceipt:
    return TaskDetachedTurnReceipt(
        id=uuid4(),
        workspace_id=uuid4(),
        task_id=uuid4(),
        task_agent_session_id=uuid4(),
        operation=(
            operation
            if operation is not None
            else PendingTaskRuntimeInteractionOperation.IMPLEMENTATION_EXECUTION
        ),
        original_attempt_kind=(
            original_attempt_kind
            if original_attempt_kind is not None
            else OriginalAttemptKind.EXECUTION
        ),
        original_attempt_id=uuid4(),
        expected_formal_family=expected_formal_family,  # type: ignore[arg-type]
        admitted_turn_reference=admitted_turn_reference,  # type: ignore[arg-type]
        status=status,
        outcome=outcome if outcome is not None else {},
        contradicted_at=contradicted_at,  # type: ignore[arg-type]
        claimed_at=claimed_at if claimed_at is not None else _OBSERVED,  # type: ignore[arg-type]
        admitted_at=admitted_at,  # type: ignore[arg-type]
        settled_at=settled_at,  # type: ignore[arg-type]
        updated_at=_OBSERVED,
    )


def test_claimed_receipt_is_the_valid_default() -> None:
    receipt = _receipt()
    assert receipt.status is TaskDetachedTurnReceiptStatus.CLAIMED
    assert receipt.admitted_turn_reference is None
    assert receipt.admitted_at is None
    assert receipt.settled_at is None
    assert receipt.outcome == {}
    assert receipt.created_external_session_id is None
    assert receipt.effective_config_snapshot is None
    assert receipt.created_session_provenance is None
    assert task_detached_turn_receipt_field_names() == frozenset(
        {
            "id",
            "workspace_id",
            "task_id",
            "task_agent_session_id",
            "operation",
            "original_attempt_kind",
            "original_attempt_id",
            "expected_formal_family",
            "admitted_turn_reference",
            "status",
            "outcome",
            "contradicted_at",
            "claimed_at",
            "admitted_at",
            "settled_at",
            "updated_at",
            "created_external_session_id",
            "effective_config_snapshot",
            "created_session_provenance",
            "admission_attempted_at",
        }
    )


def test_a_claimed_receipt_carries_no_admission_or_outcome() -> None:
    with pytest.raises(ValueError, match="claimed"):
        _receipt(admitted_turn_reference="turn-1")
    with pytest.raises(ValueError, match="claimed"):
        _receipt(admitted_at=_OBSERVED)
    with pytest.raises(ValueError, match="claimed"):
        _receipt(settled_at=_OBSERVED)
    with pytest.raises(ValueError, match="claimed"):
        _receipt(outcome={SETTLEMENT_OUTCOME_LABEL_KEY: SETTLEMENT_VALIDATED_RESULT})


def test_an_admitted_receipt_carries_its_acknowledged_identity() -> None:
    receipt = _receipt(
        status=TaskDetachedTurnReceiptStatus.ADMITTED,
        admitted_turn_reference="turn-1",
        admitted_at=_OBSERVED,
    )
    assert receipt.admitted_turn_reference == "turn-1"
    with pytest.raises(ValueError, match="admitted"):
        _receipt(status=TaskDetachedTurnReceiptStatus.ADMITTED, admitted_turn_reference="turn-1")
    with pytest.raises(ValueError, match="admitted"):
        _receipt(status=TaskDetachedTurnReceiptStatus.ADMITTED, admitted_at=_OBSERVED)
    with pytest.raises(ValueError, match="admitted"):
        _receipt(
            status=TaskDetachedTurnReceiptStatus.ADMITTED,
            admitted_turn_reference="turn-1",
            admitted_at=_OBSERVED,
            settled_at=_OBSERVED,
        )
    with pytest.raises(ValueError, match="admitted"):
        _receipt(
            status=TaskDetachedTurnReceiptStatus.ADMITTED,
            admitted_turn_reference="turn-1",
            admitted_at=_OBSERVED,
            outcome={SETTLEMENT_OUTCOME_LABEL_KEY: SETTLEMENT_VALIDATED_RESULT},
        )


def test_a_settled_receipt_carries_its_label_and_instants() -> None:
    receipt = _receipt(
        status=TaskDetachedTurnReceiptStatus.SETTLED,
        admitted_turn_reference="turn-1",
        admitted_at=_OBSERVED,
        settled_at=_OBSERVED,
        outcome={
            SETTLEMENT_OUTCOME_LABEL_KEY: SETTLEMENT_OBSERVED_FAILURE,
            SETTLEMENT_OUTCOME_SEQUENCE_KEY: 41,
        },
    )
    assert receipt.outcome[SETTLEMENT_OUTCOME_LABEL_KEY] == SETTLEMENT_OBSERVED_FAILURE
    for missing in (
        {"consumed": 1},
        {SETTLEMENT_OUTCOME_SEQUENCE_KEY: 41},
        {SETTLEMENT_OUTCOME_LABEL_KEY: ""},
    ):
        with pytest.raises(ValueError, match="settled"):
            _receipt(
                status=TaskDetachedTurnReceiptStatus.SETTLED,
                admitted_turn_reference="turn-1",
                admitted_at=_OBSERVED,
                settled_at=_OBSERVED,
                outcome=missing,
            )
    with pytest.raises(ValueError, match="settled"):
        _receipt(
            status=TaskDetachedTurnReceiptStatus.SETTLED,
            admitted_at=_OBSERVED,
            settled_at=_OBSERVED,
            outcome={SETTLEMENT_OUTCOME_LABEL_KEY: SETTLEMENT_VALIDATED_RESULT},
        )


def test_a_refused_receipt_never_ran_and_carries_its_refusal_kind() -> None:
    receipt = _receipt(
        status=TaskDetachedTurnReceiptStatus.REFUSED,
        settled_at=_OBSERVED,
        outcome={REFUSAL_OUTCOME_KIND_KEY: "hub_refused"},
    )
    assert receipt.admitted_turn_reference is None
    with pytest.raises(ValueError, match="refused"):
        _receipt(
            status=TaskDetachedTurnReceiptStatus.REFUSED,
            admitted_turn_reference="turn-1",
            settled_at=_OBSERVED,
            outcome={REFUSAL_OUTCOME_KIND_KEY: "hub_refused"},
        )
    with pytest.raises(ValueError, match="refused"):
        _receipt(status=TaskDetachedTurnReceiptStatus.REFUSED, settled_at=_OBSERVED, outcome={})


def test_the_outcome_payload_is_canonical_safe_json() -> None:
    receipt = _receipt(
        status=TaskDetachedTurnReceiptStatus.SETTLED,
        admitted_turn_reference="turn-1",
        admitted_at=_OBSERVED,
        settled_at=_OBSERVED,
        outcome={
            SETTLEMENT_OUTCOME_LABEL_KEY: SETTLEMENT_RESULT_NOT_APPLIED,
            SETTLEMENT_OUTCOME_SEQUENCE_KEY: 7,
        },
    )
    assert dict(receipt.outcome) == {
        SETTLEMENT_OUTCOME_LABEL_KEY: SETTLEMENT_RESULT_NOT_APPLIED,
        SETTLEMENT_OUTCOME_SEQUENCE_KEY: 7,
    }
    with pytest.raises(ValueError):
        _receipt(
            status=TaskDetachedTurnReceiptStatus.SETTLED,
            admitted_turn_reference="turn-1",
            admitted_at=_OBSERVED,
            settled_at=_OBSERVED,
            outcome={
                SETTLEMENT_OUTCOME_LABEL_KEY: SETTLEMENT_VALIDATED_RESULT,
                "x": float("nan"),
            },
        )


def test_identity_and_instant_validation_fails_closed() -> None:
    with pytest.raises(ValueError, match="operation"):
        _receipt(operation="implementation_execution")
    with pytest.raises(ValueError, match="original_attempt_kind"):
        _receipt(original_attempt_kind="execution")
    with pytest.raises(ValueError, match="expected_formal_family"):
        _receipt(expected_formal_family=" ")
    with pytest.raises(ValueError, match="admitted_turn_reference"):
        _receipt(
            status=TaskDetachedTurnReceiptStatus.ADMITTED,
            admitted_turn_reference=" ",
            admitted_at=_OBSERVED,
        )
    with pytest.raises(ValueError, match="timezone-aware"):
        _receipt(claimed_at=datetime(2026, 11, 15, 9, 0, 0))


def test_only_an_admitted_receipt_carries_the_contradiction_marker() -> None:
    receipt = _receipt(
        status=TaskDetachedTurnReceiptStatus.ADMITTED,
        admitted_turn_reference="turn-1",
        admitted_at=_OBSERVED,
        contradicted_at=_OBSERVED,
    )
    assert receipt.contradicted_at == _OBSERVED
    with pytest.raises(ValueError, match="contradiction marker"):
        _receipt(
            status=TaskDetachedTurnReceiptStatus.CLAIMED,
            contradicted_at=_OBSERVED,
        )
    with pytest.raises(ValueError, match="contradiction marker"):
        _receipt(
            status=TaskDetachedTurnReceiptStatus.SETTLED,
            admitted_turn_reference="turn-1",
            admitted_at=_OBSERVED,
            settled_at=_OBSERVED,
            outcome={SETTLEMENT_OUTCOME_LABEL_KEY: SETTLEMENT_VALIDATED_RESULT},
            contradicted_at=_OBSERVED,
        )


def _carrier() -> NativeTurnEvidence:
    return _SimpleCarrier()


class _SimpleCarrier(NativeTurnEvidence):
    """A minimal concrete carrier proving subclassing works."""

    __slots__ = ()


def test_the_evidence_envelope_validates_its_shape() -> None:
    evidence = ObservedTurnEvidence(
        external_session_id="ext-1",
        family=TurnEvidenceFamily.TURN_TERMINAL,
        native_evidence=_carrier(),
    )
    assert evidence.family is TurnEvidenceFamily.TURN_TERMINAL
    with pytest.raises(ValueError, match="external_session_id"):
        ObservedTurnEvidence(
            external_session_id=" ",
            family=TurnEvidenceFamily.TURN_START,
            native_evidence=_carrier(),
        )
    with pytest.raises(ValueError, match="family"):
        ObservedTurnEvidence(
            external_session_id="ext-1",
            family="turn_terminal",  # type: ignore[arg-type]
            native_evidence=_carrier(),
        )
    with pytest.raises(ValueError, match="native_evidence"):
        ObservedTurnEvidence(
            external_session_id="ext-1",
            family=TurnEvidenceFamily.TURN_START,
            native_evidence=object(),  # type: ignore[arg-type]
        )


def test_the_terminal_outcome_and_classification_validate_their_shapes() -> None:
    outcome = TurnTerminalOutcome(
        kind=DetachedTurnObservationKind.COMPLETED,
        result_candidate='{"type": "implementation_result"}',
    )
    assert outcome.kind is DetachedTurnObservationKind.COMPLETED
    classification = TurnTerminalClassification(
        freshness=TurnFreshness.FRESH,
        outcome=TurnTerminalOutcome(kind=DetachedTurnObservationKind.FAILED, result_candidate=None),
        ordering_marker=41,
    )
    assert classification.freshness is TurnFreshness.FRESH
    assert classification.ordering_marker == 41
    unordered = TurnTerminalClassification(
        freshness=TurnFreshness.UNORDERED,
        outcome=outcome,
        ordering_marker=None,
    )
    assert unordered.ordering_marker is None
    with pytest.raises(ValueError, match="kind"):
        TurnTerminalOutcome(
            kind="run.completed",  # type: ignore[arg-type]
            result_candidate=None,
        )
    with pytest.raises(ValueError, match="result_candidate"):
        TurnTerminalOutcome(
            kind=DetachedTurnObservationKind.COMPLETED,
            result_candidate=" ",
        )
    with pytest.raises(ValueError, match="freshness"):
        TurnTerminalClassification(
            freshness="fresh",  # type: ignore[arg-type]
            outcome=outcome,
            ordering_marker=None,
        )
    with pytest.raises(ValueError, match="outcome"):
        TurnTerminalClassification(
            freshness=TurnFreshness.STALE,
            outcome="completed",  # type: ignore[arg-type]
            ordering_marker=None,
        )
    with pytest.raises(ValueError, match="ordering_marker"):
        TurnTerminalClassification(
            freshness=TurnFreshness.FRESH,
            outcome=outcome,
            ordering_marker=1.5,  # type: ignore[arg-type]
        )


def test_the_neutral_verdict_vocabularies_are_closed() -> None:
    assert {member.value for member in TurnAttribution} == {
        "attributed",
        "contradicted",
        "unattributable",
    }
    assert {member.value for member in TurnFreshness} == {
        "fresh",
        "stale",
        "unordered",
    }
    assert {member.value for member in TurnStatusEvidenceVerdict} == {
        "failure_evidence",
        "completion_without_result_evidence",
        "unresolved_evidence",
    }
    assert {member.value for member in TurnEvidenceFamily} == {
        "turn_start",
        "turn_terminal",
        "turn_activity",
        "turn_usage",
    }
    assert {member.value for member in TurnActivityAttestation} == {
        "active",
        "unattested",
    }
    assert {member.value for member in DetachedTurnObservationKind} == {
        "completed",
        "failed",
    }


def test_the_staged_early_completion_validates_its_shape() -> None:
    completion = TaskDetachedTurnEarlyCompletion(
        id=uuid4(),
        workspace_id=uuid4(),
        task_id=uuid4(),
        task_detached_turn_receipt_id=uuid4(),
        kind=DetachedTurnObservationKind.FAILED,
        ordering_marker=None,
        result_candidate=None,
        staged_at=_OBSERVED,
    )
    assert completion.result_candidate is None
    assert task_detached_turn_early_completion_field_names() == frozenset(
        {
            "id",
            "workspace_id",
            "task_id",
            "task_detached_turn_receipt_id",
            "kind",
            "ordering_marker",
            "result_candidate",
            "staged_at",
        }
    )
    with pytest.raises(ValueError, match="kind"):
        TaskDetachedTurnEarlyCompletion(
            id=uuid4(),
            workspace_id=uuid4(),
            task_id=uuid4(),
            task_detached_turn_receipt_id=uuid4(),
            kind="run.completed",  # type: ignore[arg-type]
            ordering_marker=None,
            result_candidate=None,
            staged_at=_OBSERVED,
        )
    with pytest.raises(ValueError, match="staged_at"):
        TaskDetachedTurnEarlyCompletion(
            id=uuid4(),
            workspace_id=uuid4(),
            task_id=uuid4(),
            task_detached_turn_receipt_id=uuid4(),
            kind=DetachedTurnObservationKind.COMPLETED,
            ordering_marker=None,
            result_candidate=None,
            staged_at=datetime(2026, 11, 15, 9, 0, 0),
        )
