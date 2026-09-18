"""Repositories for RuntimeRequest persistence.

Explicit SQL repositories over the ``openorc`` schema for RuntimeRequest,
the durable record of one exact scoped Producer/runtime-originated
consequential-action approval (Phase 1, issue #24). Rows map to
transport-independent domain objects from
:mod:`openorc.domain.runtime_requests`; instants returned from Postgres
are normalized to timezone-aware UTC at this boundary.

- v1 requests are exactly ``action_approval`` kind: scoped consequential
  runtime actions. There is no generic Owner-decision, question, chat, or
  free-form request kind, and no general Owner-to-Producer channel.
- Creation anchors the request to the exact Task, Producer session, and
  external approval identifier. The composite foreign key keeps the
  session's Task/Workspace scope in agreement, and the creation
  transaction requires the referenced binding's role to be exactly
  ``producer`` — Reviewer sessions do not participate in runtime
  approvals. The correlation identity
  ``(producer_session_id, external_approval_id)`` is unique across all
  history: one exact external request is one row forever, and resolution
  never frees the external identity for a second historical row in the
  same Producer session (``UniqueViolation`` is the durable backstop).
- Terminal transitions are one-shot guarded updates applying only while
  the request is ``pending``; resolved, expired, and cancelled requests
  are immutable historical records with no rewrite path. Resolution
  carries the typed correlated control (approved or rejected); expiry and
  cancellation carry none.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any
from uuid import UUID

from openorc.domain.runtime_requests import (
    RuntimeRequest,
    RuntimeRequestDomainError,
    RuntimeRequestKind,
    RuntimeRequestResolution,
    RuntimeRequestStatus,
)
from openorc.persistence.pool import DatabasePool
from openorc.persistence.time import normalize_utc
from openorc.persistence.transactions import transaction

__all__ = [
    "cancel_runtime_request",
    "create_runtime_request",
    "expire_runtime_request",
    "get_runtime_request",
    "list_task_runtime_requests",
    "resolve_runtime_request",
]

_RUNTIME_REQUEST_COLUMNS = (
    "id, workspace_id, task_id, producer_session_id, kind, external_approval_id, "
    "status, resolution, closed_at, created_at"
)


def _runtime_request_from_row(row: Sequence[Any]) -> RuntimeRequest:
    closed_at = row[8]
    return RuntimeRequest(
        id=row[0],
        workspace_id=row[1],
        task_id=row[2],
        producer_session_id=row[3],
        kind=RuntimeRequestKind(row[4]),
        external_approval_id=row[5],
        status=RuntimeRequestStatus(row[6]),
        resolution=None if row[7] is None else RuntimeRequestResolution(row[7]),
        closed_at=None if closed_at is None else normalize_utc(closed_at),
        created_at=normalize_utc(row[9]),
    )


def create_runtime_request(
    pool: DatabasePool,
    *,
    workspace_id: UUID,
    task_id: UUID,
    producer_session_id: UUID,
    external_approval_id: str,
) -> RuntimeRequest:
    """Insert one pending ``action_approval`` RuntimeRequest.

    The request is tied to the exact Task, Producer TaskAgentSession, and
    external approval identifier that created it: the creation transaction
    reads the referenced binding and requires its role to be exactly
    ``producer`` (a Reviewer session is rejected with
    ``RuntimeRequestDomainError``; Task/Workspace scope agreement is
    durably enforced by the composite foreign key). The request is inserted
    ``pending`` explicitly — the migration declares no lifecycle default.
    The correlation
    identity ``(producer_session_id, external_approval_id)`` is unique
    across all history — one exact external request is one row forever, so
    re-submitting the same external identifier in the same Producer session
    raises ``UniqueViolation`` even after the first request is resolved.
    """
    if not isinstance(external_approval_id, str) or not external_approval_id.strip():
        raise RuntimeRequestDomainError(
            "RuntimeRequest.external_approval_id must be a non-empty string"
        )
    with transaction(pool) as conn:
        role_row = conn.execute(
            "select role from openorc.task_agent_sessions where id = %s",
            (producer_session_id,),
        ).fetchone()
        if role_row is None or role_row[0] != "producer":
            raise RuntimeRequestDomainError(
                "create_runtime_request requires the referenced TaskAgentSession "
                "to be the Task's producer session; Reviewer sessions do not "
                "participate in runtime approvals"
            )
        row = conn.execute(
            "insert into openorc.runtime_requests "
            "(workspace_id, task_id, producer_session_id, kind, status, external_approval_id) "
            "values (%s, %s, %s, %s, %s, %s) "
            f"returning {_RUNTIME_REQUEST_COLUMNS}",
            (
                workspace_id,
                task_id,
                producer_session_id,
                RuntimeRequestKind.ACTION_APPROVAL.value,
                RuntimeRequestStatus.PENDING.value,
                external_approval_id,
            ),
        ).fetchone()
    assert row is not None
    return _runtime_request_from_row(row)


def get_runtime_request(pool: DatabasePool, *, runtime_request_id: UUID) -> RuntimeRequest | None:
    """Return one RuntimeRequest by id, or ``None`` when it does not exist."""
    with transaction(pool) as conn:
        row = conn.execute(
            f"select {_RUNTIME_REQUEST_COLUMNS} from openorc.runtime_requests where id = %s",
            (runtime_request_id,),
        ).fetchone()
    return None if row is None else _runtime_request_from_row(row)


def list_task_runtime_requests(pool: DatabasePool, *, task_id: UUID) -> list[RuntimeRequest]:
    """List a Task's complete runtime-request history, in creation order.

    Every request of the Task — pending and terminal alike — is retained:
    terminal requests are immutable historical records.
    """
    with transaction(pool) as conn:
        rows = conn.execute(
            f"select {_RUNTIME_REQUEST_COLUMNS} from openorc.runtime_requests "
            "where task_id = %s order by created_at, id",
            (task_id,),
        ).fetchall()
    return [_runtime_request_from_row(row) for row in rows]


def _finish_pending_request(
    pool: DatabasePool,
    *,
    runtime_request_id: UUID,
    status: RuntimeRequestStatus,
    resolution: RuntimeRequestResolution | None,
) -> RuntimeRequest | None:
    """Apply one one-shot terminal transition to a pending request."""
    with transaction(pool) as conn:
        row = conn.execute(
            "update openorc.runtime_requests "
            "set status = %s, resolution = %s, closed_at = now() "
            "where id = %s and status = 'pending' "
            f"returning {_RUNTIME_REQUEST_COLUMNS}",
            (
                status.value,
                None if resolution is None else resolution.value,
                runtime_request_id,
            ),
        ).fetchone()
    return None if row is None else _runtime_request_from_row(row)


def resolve_runtime_request(
    pool: DatabasePool,
    *,
    runtime_request_id: UUID,
    resolution: RuntimeRequestResolution,
) -> RuntimeRequest | None:
    """Resolve a pending request with its typed correlated Owner control.

    ``resolution`` is the typed control to that exact pending request —
    approved or rejected — never a free-form response. The transition
    applies only while the request is pending; ``None`` means the request
    is missing or already terminal (a rejected no-op that must not be
    retried blindly), and a terminal request is an immutable historical
    record.
    """
    if not isinstance(resolution, RuntimeRequestResolution):
        raise RuntimeRequestDomainError(
            "resolve_runtime_request requires a RuntimeRequestResolution "
            "(approved or rejected): the typed correlated Owner control"
        )
    return _finish_pending_request(
        pool,
        runtime_request_id=runtime_request_id,
        status=RuntimeRequestStatus.RESOLVED,
        resolution=resolution,
    )


def expire_runtime_request(
    pool: DatabasePool, *, runtime_request_id: UUID
) -> RuntimeRequest | None:
    """Expire a pending request as a one-shot terminal transition.

    Applies only while the request is pending; ``None`` means the request
    is missing or already terminal. Expiry is a terminal historical fact
    carrying no typed resolution.
    """
    return _finish_pending_request(
        pool,
        runtime_request_id=runtime_request_id,
        status=RuntimeRequestStatus.EXPIRED,
        resolution=None,
    )


def cancel_runtime_request(
    pool: DatabasePool, *, runtime_request_id: UUID
) -> RuntimeRequest | None:
    """Cancel a pending request as a one-shot terminal transition.

    Applies only while the request is pending; ``None`` means the request
    is missing or already terminal. Cancellation is a terminal historical
    fact carrying no typed resolution.
    """
    return _finish_pending_request(
        pool,
        runtime_request_id=runtime_request_id,
        status=RuntimeRequestStatus.CANCELLED,
        resolution=None,
    )
