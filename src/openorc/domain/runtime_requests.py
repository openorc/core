"""RuntimeRequest domain models.

A RuntimeRequest is one durable record of one exact scoped
Producer/runtime-originated consequential-action approval (Phase 1, issue
#24).

- The only v1 semantic kind is ``action_approval``: a scoped approval for
  one consequential runtime action. There is deliberately no generic
  Owner-decision, question, chat, or free-form request kind — those would
  be a general Owner-to-Producer channel, which the conversational
  topology (Owner ↔ Reviewer ↔ Producer) does not have.
- A request is tied to the exact Task, the exact Producer TaskAgentSession
  (the ``producer`` role; Reviewer sessions do not participate), and the
  exact external approval/action identifier that created it.
- The correlation identity ``(producer_session_id, external_approval_id)``
  is unique across all history, not merely while pending: one exact
  external request is one RuntimeRequest row forever, and resolving it
  never frees the external identity for a second historical row in the
  same Producer session.
- The lifecycle is exactly PENDING, RESOLVED, EXPIRED, and CANCELLED.
  Resolved, expired, and cancelled requests are immutable historical
  records with no rewrite path.
- The Owner response is a typed correlated control: ``approved`` or
  ``rejected``, set exactly when the status is RESOLVED and stamped with
  ``closed_at``. It is a control to that exact pending request — never a
  general communication channel or an arbitrary response document.

This module carries transport-independent validation only. It performs no
runtime approval protocol integration and no workflow orchestration.
"""

from __future__ import annotations

from dataclasses import dataclass, fields
from datetime import datetime
from enum import StrEnum
from uuid import UUID

__all__ = [
    "RuntimeRequest",
    "RuntimeRequestDomainError",
    "RuntimeRequestKind",
    "RuntimeRequestResolution",
    "RuntimeRequestStatus",
    "runtime_request_field_names",
]


class RuntimeRequestDomainError(Exception):
    """Raised when a RuntimeRequest domain invariant is violated."""


class RuntimeRequestKind(StrEnum):
    """The settled v1 RuntimeRequest kind vocabulary."""

    ACTION_APPROVAL = "action_approval"


class RuntimeRequestStatus(StrEnum):
    """The settled v1 RuntimeRequest lifecycle vocabulary.

    ``PENDING`` is the only nonterminal status; resolved, expired, and
    cancelled are terminal and immutable once stamped with ``closed_at``.
    """

    PENDING = "pending"
    RESOLVED = "resolved"
    EXPIRED = "expired"
    CANCELLED = "cancelled"


class RuntimeRequestResolution(StrEnum):
    """The typed correlated Owner control for one resolved request."""

    APPROVED = "approved"
    REJECTED = "rejected"


def _require_uuid(value: object, name: str) -> None:
    if not isinstance(value, UUID):
        raise RuntimeRequestDomainError(f"RuntimeRequest.{name} must be a UUID")


@dataclass(frozen=True, slots=True)
class RuntimeRequest:
    """One exact scoped runtime approval request for one Producer session.

    ``kind`` is exactly ``action_approval`` in v1; ``external_approval_id``
    is the exact external identifier that created the request and is
    correlation-unique per Producer session across all history. The typed
    ``resolution`` and the ``closed_at`` stamp finalize the request
    exactly once.
    """

    id: UUID
    workspace_id: UUID
    task_id: UUID
    producer_session_id: UUID
    kind: RuntimeRequestKind
    external_approval_id: str
    status: RuntimeRequestStatus
    resolution: RuntimeRequestResolution | None
    closed_at: datetime | None
    created_at: datetime

    def __post_init__(self) -> None:
        _require_uuid(self.id, "id")
        _require_uuid(self.workspace_id, "workspace_id")
        _require_uuid(self.task_id, "task_id")
        _require_uuid(self.producer_session_id, "producer_session_id")
        if not isinstance(self.kind, RuntimeRequestKind):
            raise RuntimeRequestDomainError(
                "RuntimeRequest.kind must be a RuntimeRequestKind "
                "(action_approval is the only v1 kind)"
            )
        if not isinstance(self.external_approval_id, str) or not self.external_approval_id.strip():
            raise RuntimeRequestDomainError(
                "RuntimeRequest.external_approval_id must be a non-empty string"
            )
        if not isinstance(self.status, RuntimeRequestStatus):
            raise RuntimeRequestDomainError(
                "RuntimeRequest.status must be a RuntimeRequestStatus "
                "(pending, resolved, expired, or cancelled)"
            )
        if self.resolution is not None and not isinstance(
            self.resolution, RuntimeRequestResolution
        ):
            raise RuntimeRequestDomainError(
                "RuntimeRequest.resolution must be None or a RuntimeRequestResolution"
            )
        # Terminal coherence, mirroring the database CHECK: the typed
        # resolution and the semantic stamp exist exactly when the request
        # is resolved; expired/cancelled carry the terminal stamp without a
        # typed control; pending carries neither.
        if self.status is RuntimeRequestStatus.PENDING:
            if self.resolution is not None or self.closed_at is not None:
                raise RuntimeRequestDomainError(
                    "a pending RuntimeRequest carries neither a typed "
                    "resolution nor a closed_at stamp"
                )
        elif self.status is RuntimeRequestStatus.RESOLVED:
            if self.resolution is None or self.closed_at is None:
                raise RuntimeRequestDomainError(
                    "a resolved RuntimeRequest carries its typed resolution and closed_at stamp"
                )
        else:  # expired / cancelled: stamped terminal facts, no typed control.
            if self.resolution is not None or self.closed_at is None:
                raise RuntimeRequestDomainError(
                    f"a {self.status.value} RuntimeRequest carries its terminal "
                    "closed_at stamp and no typed resolution"
                )

    @property
    def is_pending(self) -> bool:
        """Whether this request is still awaiting its terminal outcome."""
        return self.status is RuntimeRequestStatus.PENDING


def runtime_request_field_names() -> frozenset[str]:
    """Return the exact field set a RuntimeRequest exposes.

    Lets tests prove the request carries only its own correlation facts:
    the typed resolution control and the exact external identifier, with
    no free-form response document.
    """
    return frozenset(field.name for field in fields(RuntimeRequest))
