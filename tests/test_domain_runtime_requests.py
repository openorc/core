"""Domain tests for RuntimeRequest (issue #24).

Ordinary deterministic tests: no database, no network. These prove the
scoped-runtime-approval invariants later persistence and workflow-service
behavior inherit: the single settled kind, the exact Task/Producer-session/
external-identifier correlation, the typed resolution control, terminal
coherence, and the exact field set (no free-form response document).
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

import pytest

from openorc.domain.runtime_requests import (
    RuntimeRequest,
    RuntimeRequestDomainError,
    RuntimeRequestKind,
    RuntimeRequestResolution,
    RuntimeRequestStatus,
    runtime_request_field_names,
)

_NOW = datetime(2026, 9, 17, 10, 0, 0, tzinfo=UTC)
_DECIDED_AT = datetime(2026, 9, 17, 11, 0, 0, tzinfo=UTC)


def _request(**overrides: Any) -> RuntimeRequest:
    values: dict[str, Any] = {
        "id": uuid4(),
        "workspace_id": uuid4(),
        "task_id": uuid4(),
        "producer_session_id": uuid4(),
        "kind": RuntimeRequestKind.ACTION_APPROVAL,
        "external_approval_id": "approval-1",
        "status": RuntimeRequestStatus.PENDING,
        "resolution": None,
        "decided_at": None,
        "created_at": _NOW,
    }
    values.update(overrides)
    return RuntimeRequest(**values)


def test_kind_vocabulary_is_exactly_action_approval() -> None:
    assert {member.value for member in RuntimeRequestKind} == {"action_approval"}


def test_status_vocabulary_is_exactly_the_settled_lifecycle() -> None:
    assert {member.value for member in RuntimeRequestStatus} == {
        "pending",
        "resolved",
        "expired",
        "cancelled",
    }


def test_resolution_vocabulary_is_exactly_the_typed_control() -> None:
    assert {member.value for member in RuntimeRequestResolution} == {
        "approved",
        "rejected",
    }


def test_request_rejects_non_vocabulary_kind_and_status() -> None:
    with pytest.raises(RuntimeRequestDomainError):
        _request(kind="owner_question")  # type: ignore[arg-type]
    with pytest.raises(RuntimeRequestDomainError):
        _request(status="awaiting_owner")  # type: ignore[arg-type]


def test_external_approval_id_is_required_nonempty() -> None:
    for bad in ("", "   ", None, 42):
        with pytest.raises(RuntimeRequestDomainError):
            _request(external_approval_id=bad)  # type: ignore[misc]


def test_request_requires_uuid_identity_fields() -> None:
    for name in ("id", "workspace_id", "task_id", "producer_session_id"):
        with pytest.raises(RuntimeRequestDomainError):
            _request(**{name: "not-a-uuid"})


def test_pending_request_carries_neither_resolution_nor_stamp() -> None:
    with pytest.raises(RuntimeRequestDomainError):
        _request(resolution=RuntimeRequestResolution.APPROVED)
    with pytest.raises(RuntimeRequestDomainError):
        _request(decided_at=_DECIDED_AT)


def test_resolved_request_carries_its_typed_control_and_stamp() -> None:
    request = _request(
        status=RuntimeRequestStatus.RESOLVED,
        resolution=RuntimeRequestResolution.APPROVED,
        decided_at=_DECIDED_AT,
    )
    assert request.is_pending is False
    assert request.resolution is RuntimeRequestResolution.APPROVED
    assert request.decided_at == _DECIDED_AT
    # A resolved request without its control or stamp is incoherent.
    with pytest.raises(RuntimeRequestDomainError):
        _request(status=RuntimeRequestStatus.RESOLVED, resolution=None)
    with pytest.raises(RuntimeRequestDomainError):
        _request(status=RuntimeRequestStatus.RESOLVED, decided_at=None)


def test_expired_and_cancelled_requests_carry_the_stamp_not_a_control() -> None:
    # Expiry and cancellation are terminal historical facts carrying their
    # semantic stamp, never a typed resolution control.
    for status in (RuntimeRequestStatus.EXPIRED, RuntimeRequestStatus.CANCELLED):
        stamped = _request(status=status, decided_at=_DECIDED_AT)
        assert stamped.is_pending is False
        assert stamped.resolution is None
        assert stamped.decided_at == _DECIDED_AT
        with pytest.raises(RuntimeRequestDomainError):
            _request(
                status=status,
                resolution=RuntimeRequestResolution.REJECTED,
                decided_at=_DECIDED_AT,
            )
        with pytest.raises(RuntimeRequestDomainError):
            _request(status=status)  # unstamped terminal form is incoherent


def test_request_is_frozen() -> None:
    request = _request()
    with pytest.raises(AttributeError):
        request.status = RuntimeRequestStatus.RESOLVED  # type: ignore[misc]


def test_field_set_carries_only_correlation_and_control_facts() -> None:
    # The exact field set is asserted so future drift cannot appear silently:
    # the request is a typed correlated control to one exact pending request,
    # never a general Owner-to-Producer communication channel with a
    # free-form response document.
    assert runtime_request_field_names() == frozenset(
        {
            "id",
            "workspace_id",
            "task_id",
            "producer_session_id",
            "kind",
            "external_approval_id",
            "status",
            "resolution",
            "decided_at",
            "created_at",
        }
    )
