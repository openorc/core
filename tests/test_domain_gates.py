"""Domain tests for OwnerGate (issue #24).

Ordinary deterministic tests: no database, no network. These prove the
human-authority invariants later persistence and workflow-service behavior
inherit: the settled four-type and four-status vocabularies, the per-type
exact-subject binding, resolution coherence (one terminal stamp), and the
exact field set (no Task status, no optimistic-concurrency token, and no
updated_at — a resolved gate is immutable history with no update path).
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

import pytest

from openorc.domain.gates import (
    OwnerGate,
    OwnerGateDomainError,
    OwnerGateStatus,
    OwnerGateType,
    owner_gate_field_names,
)

_CREATED_AT = datetime(2026, 9, 17, 10, 0, 0, tzinfo=UTC)
_DECIDED_AT = datetime(2026, 9, 17, 11, 0, 0, tzinfo=UTC)


def _gate(**overrides: Any) -> OwnerGate:
    """Build one valid pending gate with optional field overrides."""
    values: dict[str, Any] = {
        "id": uuid4(),
        "workspace_id": uuid4(),
        "task_id": uuid4(),
        "gate_type": OwnerGateType.IMPLEMENTATION_AUTHORIZATION,
        "status": OwnerGateStatus.PENDING,
        "plan_revision_id": uuid4(),
        "subject_head_sha": None,
        "decided_at": None,
        "created_at": _CREATED_AT,
    }
    values.update(overrides)
    return OwnerGate(**values)


def test_gate_type_vocabulary_is_exactly_the_four_settled_decisions() -> None:
    assert {member.value for member in OwnerGateType} == {
        "implementation_authorization",
        "pr_authorization",
        "merge_decision",
        "review_resolution",
    }


def test_gate_status_vocabulary_is_exactly_the_four_settled_statuses() -> None:
    assert {member.value for member in OwnerGateStatus} == {
        "pending",
        "approved",
        "rejected",
        "cancelled",
    }


def test_gate_rejects_non_vocabulary_type_and_status() -> None:
    with pytest.raises(OwnerGateDomainError):
        _gate(gate_type="chat_decision")  # type: ignore[arg-type]
    with pytest.raises(OwnerGateDomainError):
        _gate(status="deferred")  # type: ignore[arg-type]


def test_implementation_authorization_binds_exactly_the_plan_revision() -> None:
    revision_id = uuid4()
    gate = _gate(gate_type=OwnerGateType.IMPLEMENTATION_AUTHORIZATION, plan_revision_id=revision_id)
    assert gate.plan_revision_id == revision_id
    assert gate.subject_head_sha is None
    # A head SHA instead of the plan revision is the wrong subject.
    with pytest.raises(OwnerGateDomainError):
        _gate(gate_type=OwnerGateType.IMPLEMENTATION_AUTHORIZATION, plan_revision_id=None)
    with pytest.raises(OwnerGateDomainError):
        _gate(
            gate_type=OwnerGateType.IMPLEMENTATION_AUTHORIZATION,
            subject_head_sha="0123456789abcdef",
        )


def test_pr_authorization_and_merge_decision_bind_exactly_the_head_sha() -> None:
    for gate_type in (OwnerGateType.PR_AUTHORIZATION, OwnerGateType.MERGE_DECISION):
        gate = _gate(
            gate_type=gate_type,
            plan_revision_id=None,
            subject_head_sha="0123456789abcdef0123456789abcdef01234567",
        )
        assert gate.subject_head_sha == "0123456789abcdef0123456789abcdef01234567"
        with pytest.raises(OwnerGateDomainError):
            _gate(gate_type=gate_type, plan_revision_id=uuid4(), subject_head_sha=None)
        with pytest.raises(OwnerGateDomainError):
            _gate(gate_type=gate_type, plan_revision_id=None, subject_head_sha="")


def test_review_resolution_binds_exactly_one_subject() -> None:
    # Either the exhausted planning PlanRevision or the PR/head review
    # subject — never both, never neither.
    resolved_plan = _gate(
        gate_type=OwnerGateType.REVIEW_RESOLUTION,
        plan_revision_id=uuid4(),
        subject_head_sha=None,
    )
    assert resolved_plan.plan_revision_id is not None
    resolved_head = _gate(
        gate_type=OwnerGateType.REVIEW_RESOLUTION,
        plan_revision_id=None,
        subject_head_sha="0123456789abcdef0123456789abcdef01234567",
    )
    assert resolved_head.subject_head_sha is not None
    with pytest.raises(OwnerGateDomainError):
        _gate(
            gate_type=OwnerGateType.REVIEW_RESOLUTION,
            plan_revision_id=uuid4(),
            subject_head_sha="0123456789abcdef",
        )
    with pytest.raises(OwnerGateDomainError):
        _gate(
            gate_type=OwnerGateType.REVIEW_RESOLUTION,
            plan_revision_id=None,
            subject_head_sha=None,
        )


def test_gate_requires_uuid_identity_fields() -> None:
    with pytest.raises(OwnerGateDomainError):
        _gate(id="not-a-uuid")  # type: ignore[arg-type]
    with pytest.raises(OwnerGateDomainError):
        _gate(workspace_id="not-a-uuid")  # type: ignore[arg-type]
    with pytest.raises(OwnerGateDomainError):
        _gate(task_id="not-a-uuid")  # type: ignore[arg-type]
    with pytest.raises(OwnerGateDomainError):
        _gate(plan_revision_id="not-a-uuid")  # type: ignore[arg-type]


def test_pending_gate_carries_no_decided_at_stamp() -> None:
    with pytest.raises(OwnerGateDomainError):
        _gate(decided_at=_DECIDED_AT)


def test_resolved_gate_carries_its_decided_at_stamp() -> None:
    gate = _gate(
        status=OwnerGateStatus.APPROVED,
        plan_revision_id=uuid4(),
        subject_head_sha=None,
        decided_at=_DECIDED_AT,
    )
    assert gate.is_resolved and not gate.is_pending
    assert gate.decided_at == _DECIDED_AT
    for status in (OwnerGateStatus.REJECTED, OwnerGateStatus.CANCELLED):
        with pytest.raises(OwnerGateDomainError):
            _gate(status=status, plan_revision_id=uuid4(), decided_at=None)


def test_resolved_gate_is_frozen_immutability() -> None:
    gate = _gate(
        status=OwnerGateStatus.APPROVED,
        plan_revision_id=uuid4(),
        decided_at=_DECIDED_AT,
    )
    with pytest.raises(AttributeError):
        gate.status = OwnerGateStatus.REJECTED  # type: ignore[misc]


def test_gate_subject_head_sha_must_be_nonempty_when_set() -> None:
    for bad in ("", "   ", 42):
        with pytest.raises(OwnerGateDomainError):
            _gate(
                gate_type=OwnerGateType.PR_AUTHORIZATION,
                plan_revision_id=None,
                subject_head_sha=bad,
            )


def test_field_set_carries_only_authority_record_facts() -> None:
    # The exact field set is asserted so future drift cannot appear silently:
    # no Task status, no optimistic-concurrency token, no updated_at (a
    # resolved gate is immutable history with no update path), and no
    # duplicated gate data on the Task.
    assert owner_gate_field_names() == frozenset(
        {
            "id",
            "workspace_id",
            "task_id",
            "gate_type",
            "status",
            "plan_revision_id",
            "subject_head_sha",
            "decided_at",
            "created_at",
        }
    )
