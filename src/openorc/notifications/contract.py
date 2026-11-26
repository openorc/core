"""Fixed internal PostgreSQL notification contract for OpenOrc (issue #397).

This module defines the one fixed notification channel and the exact typed
identifier-only payload envelopes shared by G4b database publication (SQL
side) and G4c authorized Workspace SSE consumption (services side).

Boundary rules:

- The channel and the two message kinds are a fixed, documented contract.
  There is no dynamic channel set, no version field, no sequence or replay
  data: future evolution is a deliberate contract change, never a tolerated
  unknown shape.
- Payloads carry internal UUID identities and the fixed resource vocabulary
  only. Raw rows, content, credentials, URLs, and provider payloads never
  appear in a notification payload.
- Incoming payloads are validated strictly. Malformed or unknown shapes are
  rejected fail-closed with sanitized diagnostics: error messages carry only
  shape facts and contract field names, never payload-derived values, so
  they are safe to log.
- These notifications are internal hints. They are not authorization input,
  not workflow authority, and not public DTOs; downstream consumers own all
  authorization and synchronization.
- Notification visibility is transaction-coupled (documented for G4b): the
  outer commit makes a transaction's notifications visible, rollback or
  savepoint rollback suppresses them, and identical channel+payload pairs
  may coalesce within one transaction. There is no historical catchup.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from enum import StrEnum
from typing import Final
from uuid import UUID

__all__ = [
    "DATA_CHANGED_ENVELOPE_KEYS",
    "KIND_DATA_CHANGED",
    "KIND_WORKFLOW_EVENT",
    "NOTIFICATION_CHANNEL",
    "WORKFLOW_EVENT_ENVELOPE_KEYS",
    "DataChangedNotification",
    "NotificationContractError",
    "NotificationMessage",
    "NotificationResourceType",
    "WorkflowEventNotification",
    "decode_notification_payload",
    "encode_notification_payload",
]

# The one fixed OpenOrc notification channel. G4b SQL publishes on this
# channel; the listener registers LISTEN for exactly this channel. Payloads
# are discriminated by the envelope ``kind`` field, not by channel.
NOTIFICATION_CHANNEL: Final[str] = "openorc_notifications"

# The two typed internal message kinds (the closed envelope discriminator).
KIND_WORKFLOW_EVENT: Final[str] = "workflow_event"
KIND_DATA_CHANGED: Final[str] = "data_changed"

# The exact top-level key sets of the two envelope shapes. Any other key set
# is rejected. Encoding order is fixed and canonical (kind first, then the
# identity fields) so G4b SQL can replicate the byte shape exactly.
WORKFLOW_EVENT_ENVELOPE_KEYS: Final[frozenset[str]] = frozenset(
    {"kind", "workspace_id", "workflow_event_id"}
)
DATA_CHANGED_ENVELOPE_KEYS: Final[frozenset[str]] = frozenset(
    {"kind", "workspace_id", "resource_type", "resource_id"}
)


class NotificationResourceType(StrEnum):
    """The fixed closed vocabulary of ``data_changed`` resource types.

    Every resource type is Workspace-owned; the workspace resource's ID
    equals the workspace ID itself.
    """

    WORKSPACE = "workspace"
    PROJECT = "project"
    REPOSITORY = "repository"
    ISSUE = "issue"
    TASK = "task"
    CONNECTION = "connection"
    ROLE_BINDING = "role_binding"
    REFRESH = "refresh"


@dataclass(frozen=True, slots=True)
class WorkflowEventNotification:
    """Identifier-only hint that one durable WorkflowEvent exists.

    ``workflow_event_id`` addresses the existing G2c durable audit row; the
    authorized public DTO is fetched by downstream consumers through the
    existing read contracts, never carried in the notification.
    """

    workspace_id: UUID
    workflow_event_id: UUID


@dataclass(frozen=True, slots=True)
class DataChangedNotification:
    """Identifier-only hint that one durable resource changed.

    ``resource_id`` is the resource's internal UUID; for the workspace
    resource it equals ``workspace_id``.
    """

    workspace_id: UUID
    resource_type: NotificationResourceType
    resource_id: UUID


NotificationMessage = WorkflowEventNotification | DataChangedNotification


class NotificationContractError(Exception):
    """Raised when a notification payload violates the fixed envelope contract.

    The message carries only shape facts (field names, expected vocabulary,
    structural violations) and never payload-derived values, so the message
    is safe to log.
    """


def encode_notification_payload(message: NotificationMessage) -> str:
    """Encode one typed notification into the canonical compact JSON envelope.

    The encoding is the documented byte-exact shape G4b SQL must produce:
    fixed key order (``kind`` first), canonical lowercase hyphenated UUID
    strings, compact separators, no whitespace.
    """
    if isinstance(message, WorkflowEventNotification):
        envelope: dict[str, str] = {
            "kind": KIND_WORKFLOW_EVENT,
            "workspace_id": str(message.workspace_id),
            "workflow_event_id": str(message.workflow_event_id),
        }
    else:
        if (
            message.resource_type is NotificationResourceType.WORKSPACE
            and message.resource_id != message.workspace_id
        ):
            raise NotificationContractError(
                "cannot encode a workspace data_changed notification whose "
                "resource_id differs from workspace_id"
            )
        envelope = {
            "kind": KIND_DATA_CHANGED,
            "workspace_id": str(message.workspace_id),
            "resource_type": message.resource_type.value,
            "resource_id": str(message.resource_id),
        }
    return json.dumps(envelope, separators=(",", ":"))


def decode_notification_payload(payload: str) -> NotificationMessage:
    """Strictly validate and decode one raw notification payload.

    Accepts any JSON object whose top-level key set matches one of the two
    fixed envelopes exactly (whitespace and key order are not significant,
    but no extra or missing keys are tolerated) with canonical lowercase
    hyphenated UUID strings and a resource type from the fixed vocabulary.
    Any other shape raises :class:`NotificationContractError` with sanitized
    diagnostics — the payload itself is never echoed.
    """
    try:
        decoded = json.loads(payload)
    except ValueError:
        raise NotificationContractError("notification payload is not valid JSON") from None
    if not isinstance(decoded, dict):
        raise NotificationContractError("notification payload is not a JSON object")

    kind = decoded.get("kind")
    if kind not in (KIND_WORKFLOW_EVENT, KIND_DATA_CHANGED):
        raise NotificationContractError("notification payload has a missing or unknown kind")
    if kind == KIND_WORKFLOW_EVENT:
        _require_exact_keys(decoded, WORKFLOW_EVENT_ENVELOPE_KEYS, KIND_WORKFLOW_EVENT)
        return WorkflowEventNotification(
            workspace_id=_parse_canonical_uuid(decoded, "workspace_id"),
            workflow_event_id=_parse_canonical_uuid(decoded, "workflow_event_id"),
        )
    _require_exact_keys(decoded, DATA_CHANGED_ENVELOPE_KEYS, KIND_DATA_CHANGED)
    raw_resource_type = decoded.get("resource_type")
    if not isinstance(raw_resource_type, str):
        raise NotificationContractError(
            "notification payload field 'resource_type' is missing or not a string"
        )
    try:
        resource_type = NotificationResourceType(raw_resource_type)
    except ValueError:
        raise NotificationContractError(
            "notification payload field 'resource_type' is not in the fixed resource vocabulary"
        ) from None
    workspace_id = _parse_canonical_uuid(decoded, "workspace_id")
    resource_id = _parse_canonical_uuid(decoded, "resource_id")
    if resource_type is NotificationResourceType.WORKSPACE and resource_id != workspace_id:
        raise NotificationContractError(
            "notification payload field 'resource_id' must equal field "
            "'workspace_id' for the workspace resource"
        )
    return DataChangedNotification(
        workspace_id=workspace_id,
        resource_type=resource_type,
        resource_id=resource_id,
    )


def _require_exact_keys(
    decoded: dict[str, object], expected_keys: frozenset[str], kind: str
) -> None:
    actual_keys = frozenset(decoded.keys())
    if actual_keys != expected_keys:
        raise NotificationContractError(
            f"notification payload keys do not match the {kind!r} envelope "
            f"(expected exactly {sorted(expected_keys)})"
        )


def _parse_canonical_uuid(decoded: dict[str, object], field_name: str) -> UUID:
    value = decoded.get(field_name)
    if not isinstance(value, str):
        raise NotificationContractError(
            f"notification payload field {field_name!r} is missing or not a string"
        )
    try:
        parsed = UUID(value)
    except ValueError:
        raise NotificationContractError(
            f"notification payload field {field_name!r} is not a UUID string"
        ) from None
    # Canonical form only: lowercase, hyphenated, no braces or urn prefix.
    if str(parsed) != value:
        raise NotificationContractError(
            f"notification payload field {field_name!r} is not a canonical "
            "lowercase hyphenated UUID string"
        )
    return parsed
