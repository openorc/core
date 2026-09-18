"""TaskBlock domain models.

A TaskBlock is one durable blocking condition on a Task, persisted with
its reason and its reason/recovery context (Phase 1, issue #24).

- The initial reason vocabulary is exactly the twelve settled values:
  REVIEW_FAILURE, RUNTIME_FAILURE, AGENT_SESSION_LOST,
  CONNECTION_UNAVAILABLE, INVALID_CREDENTIALS,
  EXTERNAL_OPERATION_UNCERTAIN, STALE_OPERATION, GITHUB_SOURCE_CHANGED,
  RUNTIME_REQUEST_REJECTED, PR_CLOSED_UNMERGED, OWNER_ACTION_REQUIRED,
  and UNKNOWN.
- ReviewLoop iteration-limit exhaustion is deliberately not a TaskBlock
  reason: it is represented through the Task's ``waiting_for_owner``
  status plus a REVIEW_RESOLUTION OwnerGate. It is structurally excluded
  from this vocabulary.
- ``context`` is the reason-specific recovery/continuation context, a
  canonical JSON object (string keys at every level, JSON-representable
  values only, sequences normalized to lists). It is persisted for the
  block's lifetime and retained after resolution as historical evidence.
- Recovery is reason/context-specific: persistence encodes no universal
  blocked-to-next-state transition. Which recovery applies — and when a
  block is resolved — belongs to later orchestration.
- A resolved block remains historical: ``resolved_at`` is stamped exactly
  once, and the block's reason and context survive resolution intact.

This module carries transport-independent validation only. It performs no
recovery orchestration and no Task state transitions.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, fields
from datetime import datetime
from enum import StrEnum
from math import isinf, isnan
from types import MappingProxyType
from uuid import UUID

__all__ = [
    "TaskBlock",
    "TaskBlockDomainError",
    "TaskBlockReason",
    "canonical_task_block_context",
    "task_block_field_names",
]


class TaskBlockDomainError(Exception):
    """Raised when a TaskBlock domain invariant is violated."""


class TaskBlockReason(StrEnum):
    """The settled v1 TaskBlock reason vocabulary.

    ReviewLoop iteration-limit exhaustion is deliberately absent: it is
    represented through the Task's ``waiting_for_owner`` status plus a
    REVIEW_RESOLUTION OwnerGate, never as a TaskBlock reason.
    """

    REVIEW_FAILURE = "review_failure"
    RUNTIME_FAILURE = "runtime_failure"
    AGENT_SESSION_LOST = "agent_session_lost"
    CONNECTION_UNAVAILABLE = "connection_unavailable"
    INVALID_CREDENTIALS = "invalid_credentials"
    EXTERNAL_OPERATION_UNCERTAIN = "external_operation_uncertain"
    STALE_OPERATION = "stale_operation"
    GITHUB_SOURCE_CHANGED = "github_source_changed"
    RUNTIME_REQUEST_REJECTED = "runtime_request_rejected"
    PR_CLOSED_UNMERGED = "pr_closed_unmerged"
    OWNER_ACTION_REQUIRED = "owner_action_required"
    UNKNOWN = "unknown"


def _require_uuid(value: object, name: str) -> None:
    if not isinstance(value, UUID):
        raise TaskBlockDomainError(f"TaskBlock.{name} must be a UUID")


def canonical_task_block_context(value: object) -> Mapping[str, object]:
    """Canonicalize a TaskBlock's reason/recovery context.

    Canonical JSON-object semantics, mirroring ``canonical_safe_config``:
    the value must be a mapping with string keys at every level, and every
    value must be JSON-representable — nested mappings, sequences, and
    scalars only. Sequences normalize to lists (tuples do not silently
    survive), NaN/Infinity are rejected, and nothing relies on silent key
    coercion. The returned frozen view is exactly what jsonb stores and
    what a reload reads back.
    """
    if not isinstance(value, Mapping):
        raise TaskBlockDomainError("TaskBlock.context must be a mapping")
    canonical: dict[str, object] = {}
    for key, item in value.items():
        if not isinstance(key, str):
            raise TaskBlockDomainError(
                "TaskBlock.context keys must be strings at every level (canonical JSON object keys)"
            )
        canonical[key] = _canonical_json_value(item)
    return MappingProxyType(canonical)


def _canonical_json_value(value: object) -> object:
    """Return the canonical JSON representation of one context value."""
    if isinstance(value, Mapping):
        nested: dict[str, object] = {}
        for key, item in value.items():
            if not isinstance(key, str):
                raise TaskBlockDomainError(
                    "TaskBlock.context keys must be strings at every level "
                    "(canonical JSON object keys)"
                )
            nested[key] = _canonical_json_value(item)
        return nested
    if isinstance(value, (list, tuple)):
        return [_canonical_json_value(item) for item in value]
    if isinstance(value, (str, bool, int, float)) or value is None:
        if isinstance(value, float) and (isnan(value) or isinf(value)):
            raise TaskBlockDomainError(
                "TaskBlock.context must be canonical JSON; NaN and Infinity are not JSON"
            )
        return value
    raise TaskBlockDomainError(
        "TaskBlock.context values must be JSON-representable "
        f"(mapping, sequence, string, number, boolean, or null); got {type(value).__name__}"
    )


@dataclass(frozen=True, slots=True)
class TaskBlock:
    """One durable blocking condition on a Task.

    ``reason`` is from the settled v1 vocabulary; ``context`` is the
    reason-specific recovery/continuation context (canonical JSON object).
    ``resolved_at`` is stamped exactly once when the block is resolved; a
    resolved block remains historical and retains its reason and context.
    """

    id: UUID
    workspace_id: UUID
    task_id: UUID
    reason: TaskBlockReason
    context: Mapping[str, object]
    resolved_at: datetime | None
    created_at: datetime

    def __post_init__(self) -> None:
        _require_uuid(self.id, "id")
        _require_uuid(self.workspace_id, "workspace_id")
        _require_uuid(self.task_id, "task_id")
        if not isinstance(self.reason, TaskBlockReason):
            raise TaskBlockDomainError(
                "TaskBlock.reason must be a TaskBlockReason (the settled v1 "
                "reason vocabulary; ReviewLoop exhaustion is not a reason)"
            )
        if not isinstance(self.context, Mapping):
            raise TaskBlockDomainError("TaskBlock.context must be a mapping")
        # Canonical form always holds, whether the block is current or
        # historical: the frozen view is what jsonb stores and reads back.
        object.__setattr__(self, "context", canonical_task_block_context(self.context))


def task_block_field_names() -> frozenset[str]:
    """Return the exact field set a TaskBlock exposes.

    Lets tests prove the block carries only its own facts: reason,
    recovery context, and the semantic resolution stamp — no universal
    blocked-to-next-state transition field.
    """
    return frozenset(field.name for field in fields(TaskBlock))
