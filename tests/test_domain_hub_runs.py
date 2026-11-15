"""Deterministic domain tests for the Hub-run receipt models (issue #322).

The closed forward-only status vocabulary with its exact per-status
coherence, the canonical safe outcome payload, and the runtime-neutral
normalized observation values the settlement seam consumes — the
transport-independent validation the persistence boundary and services rely
on.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

import pytest

from openorc.domain.hub_runs import (
    REFUSAL_OUTCOME_KIND_KEY,
    SETTLEMENT_OBSERVED_FAILURE,
    SETTLEMENT_OUTCOME_LABEL_KEY,
    SETTLEMENT_OUTCOME_SEQUENCE_KEY,
    SETTLEMENT_RESULT_NOT_APPLIED,
    SETTLEMENT_VALIDATED_RESULT,
    HubRunObservationKind,
    ObservedHubRunCompletion,
    ObservedHubRunStatus,
    TaskHubRunEarlyCompletion,
    TaskHubRunReceipt,
    TaskHubRunReceiptStatus,
    task_hub_run_early_completion_field_names,
    task_hub_run_receipt_field_names,
)
from openorc.domain.operational_failures import OriginalAttemptKind
from openorc.domain.pending_interactions import PendingTaskRuntimeInteractionOperation

_OBSERVED = datetime(2026, 11, 15, 9, 0, 0, tzinfo=UTC)


def _receipt(
    *,
    status: TaskHubRunReceiptStatus = TaskHubRunReceiptStatus.CLAIMED,
    run_id: object = None,
    admitted_at: object = None,
    settled_at: object = None,
    outcome: Any = None,
    expected_formal_family: str = "implementation_result",
    operation: Any = None,
    original_attempt_kind: Any = None,
    claimed_at: object = None,
) -> TaskHubRunReceipt:
    return TaskHubRunReceipt(
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
        run_id=run_id,  # type: ignore[arg-type]
        status=status,
        outcome=outcome if outcome is not None else {},
        claimed_at=claimed_at if claimed_at is not None else _OBSERVED,  # type: ignore[arg-type]
        admitted_at=admitted_at,  # type: ignore[arg-type]
        settled_at=settled_at,  # type: ignore[arg-type]
        updated_at=_OBSERVED,
    )


def test_claimed_receipt_is_the_valid_default() -> None:
    receipt = _receipt()
    assert receipt.status is TaskHubRunReceiptStatus.CLAIMED
    assert receipt.run_id is None
    assert receipt.admitted_at is None
    assert receipt.settled_at is None
    assert receipt.outcome == {}
    assert task_hub_run_receipt_field_names() == frozenset(
        {
            "id",
            "workspace_id",
            "task_id",
            "task_agent_session_id",
            "operation",
            "original_attempt_kind",
            "original_attempt_id",
            "expected_formal_family",
            "run_id",
            "status",
            "outcome",
            "claimed_at",
            "admitted_at",
            "settled_at",
            "updated_at",
        }
    )


def test_a_claimed_receipt_carries_no_admission_or_outcome() -> None:
    with pytest.raises(ValueError, match="claimed"):
        _receipt(run_id="run-1")
    with pytest.raises(ValueError, match="claimed"):
        _receipt(admitted_at=_OBSERVED)
    with pytest.raises(ValueError, match="claimed"):
        _receipt(settled_at=_OBSERVED)
    with pytest.raises(ValueError, match="claimed"):
        _receipt(outcome={SETTLEMENT_OUTCOME_LABEL_KEY: SETTLEMENT_VALIDATED_RESULT})


def test_an_admitted_receipt_carries_its_acknowledged_identity() -> None:
    receipt = _receipt(
        status=TaskHubRunReceiptStatus.ADMITTED,
        run_id="run-1",
        admitted_at=_OBSERVED,
    )
    assert receipt.run_id == "run-1"
    with pytest.raises(ValueError, match="admitted"):
        _receipt(status=TaskHubRunReceiptStatus.ADMITTED, run_id="run-1")
    with pytest.raises(ValueError, match="admitted"):
        _receipt(status=TaskHubRunReceiptStatus.ADMITTED, admitted_at=_OBSERVED)
    with pytest.raises(ValueError, match="admitted"):
        _receipt(
            status=TaskHubRunReceiptStatus.ADMITTED,
            run_id="run-1",
            admitted_at=_OBSERVED,
            settled_at=_OBSERVED,
        )
    with pytest.raises(ValueError, match="admitted"):
        _receipt(
            status=TaskHubRunReceiptStatus.ADMITTED,
            run_id="run-1",
            admitted_at=_OBSERVED,
            outcome={SETTLEMENT_OUTCOME_LABEL_KEY: SETTLEMENT_VALIDATED_RESULT},
        )


def test_a_settled_receipt_carries_its_label_and_instants() -> None:
    receipt = _receipt(
        status=TaskHubRunReceiptStatus.SETTLED,
        run_id="run-1",
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
                status=TaskHubRunReceiptStatus.SETTLED,
                run_id="run-1",
                admitted_at=_OBSERVED,
                settled_at=_OBSERVED,
                outcome=missing,
            )
    with pytest.raises(ValueError, match="settled"):
        _receipt(
            status=TaskHubRunReceiptStatus.SETTLED,
            admitted_at=_OBSERVED,
            settled_at=_OBSERVED,
            outcome={SETTLEMENT_OUTCOME_LABEL_KEY: SETTLEMENT_VALIDATED_RESULT},
        )


def test_a_refused_receipt_never_ran_and_carries_its_refusal_kind() -> None:
    receipt = _receipt(
        status=TaskHubRunReceiptStatus.REFUSED,
        settled_at=_OBSERVED,
        outcome={REFUSAL_OUTCOME_KIND_KEY: "hub_refused"},
    )
    assert receipt.run_id is None
    with pytest.raises(ValueError, match="refused"):
        _receipt(
            status=TaskHubRunReceiptStatus.REFUSED,
            run_id="run-1",
            settled_at=_OBSERVED,
            outcome={REFUSAL_OUTCOME_KIND_KEY: "hub_refused"},
        )
    with pytest.raises(ValueError, match="refused"):
        _receipt(status=TaskHubRunReceiptStatus.REFUSED, settled_at=_OBSERVED, outcome={})


def test_the_outcome_payload_is_canonical_safe_json() -> None:
    receipt = _receipt(
        status=TaskHubRunReceiptStatus.SETTLED,
        run_id="run-1",
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
            status=TaskHubRunReceiptStatus.SETTLED,
            run_id="run-1",
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
    with pytest.raises(ValueError, match="run_id"):
        _receipt(status=TaskHubRunReceiptStatus.ADMITTED, run_id=" ", admitted_at=_OBSERVED)
    with pytest.raises(ValueError, match="timezone-aware"):
        _receipt(claimed_at=datetime(2026, 11, 15, 9, 0, 0))


def test_the_observation_values_validate_their_shapes() -> None:
    completion = ObservedHubRunCompletion(
        external_session_id="ext-1",
        kind=HubRunObservationKind.RUN_COMPLETED,
        sequence=41,
        result_candidate='{"type": "implementation_result"}',
    )
    assert completion.kind is HubRunObservationKind.RUN_COMPLETED
    with pytest.raises(ValueError, match="external_session_id"):
        ObservedHubRunCompletion(
            external_session_id=" ",
            kind=HubRunObservationKind.RUN_COMPLETED,
            sequence=None,
            result_candidate=None,
        )
    with pytest.raises(ValueError, match="kind"):
        ObservedHubRunCompletion(
            external_session_id="ext-1",
            kind="run.completed",  # type: ignore[arg-type]
            sequence=None,
            result_candidate=None,
        )
    with pytest.raises(ValueError, match="sequence"):
        ObservedHubRunCompletion(
            external_session_id="ext-1",
            kind=HubRunObservationKind.RUN_FAILED,
            sequence=1.5,  # type: ignore[arg-type]
            result_candidate=None,
        )
    with pytest.raises(ValueError, match="result_candidate"):
        ObservedHubRunCompletion(
            external_session_id="ext-1",
            kind=HubRunObservationKind.RUN_COMPLETED,
            sequence=None,
            result_candidate=" ",
        )
    status = ObservedHubRunStatus(run_id="run-1", status="failed")
    assert status.status == "failed"
    with pytest.raises(ValueError, match="run_id"):
        ObservedHubRunStatus(run_id="", status="failed")
    with pytest.raises(ValueError, match="status"):
        ObservedHubRunStatus(run_id="run-1", status="  ")


def test_the_staged_early_completion_validates_its_shape() -> None:
    completion = TaskHubRunEarlyCompletion(
        id=uuid4(),
        workspace_id=uuid4(),
        task_id=uuid4(),
        task_hub_run_receipt_id=uuid4(),
        kind=HubRunObservationKind.RUN_FAILED,
        sequence=None,
        result_candidate=None,
        staged_at=_OBSERVED,
    )
    assert completion.result_candidate is None
    assert task_hub_run_early_completion_field_names() == frozenset(
        {
            "id",
            "workspace_id",
            "task_id",
            "task_hub_run_receipt_id",
            "kind",
            "sequence",
            "result_candidate",
            "staged_at",
        }
    )
    with pytest.raises(ValueError, match="kind"):
        TaskHubRunEarlyCompletion(
            id=uuid4(),
            workspace_id=uuid4(),
            task_id=uuid4(),
            task_hub_run_receipt_id=uuid4(),
            kind="run.completed",  # type: ignore[arg-type]
            sequence=None,
            result_candidate=None,
            staged_at=_OBSERVED,
        )
    with pytest.raises(ValueError, match="staged_at"):
        TaskHubRunEarlyCompletion(
            id=uuid4(),
            workspace_id=uuid4(),
            task_id=uuid4(),
            task_hub_run_receipt_id=uuid4(),
            kind=HubRunObservationKind.RUN_COMPLETED,
            sequence=None,
            result_candidate=None,
            staged_at=datetime(2026, 11, 15, 9, 0, 0),
        )
