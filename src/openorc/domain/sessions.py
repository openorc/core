"""Task agent session domain models.

A TaskAgentSession is the durable Task/role <-> external-session binding that
preserves Task-scoped conversational continuity (Phase 1, issue #22). It
carries continuity and representation invariants only: no runtime behavior
lives here — no Cline SDK/session creation, no ``session_ready`` protocol
handling, no health/telemetry polling, no blocking transitions, and no
recovery/replacement workflow.

- Exactly one binding exists per ``(Task, role)`` in v1 for PRODUCER and
  REVIEWER. Establishment is idempotent for the same Connection and a
  deterministic conflict against a different one; a binding is never
  silently repointed, replaced, or duplicated.
- ``external_session_id`` is the opaque external-session identity. It is
  None while CONNECTING and, once successfully initialized, immutable for
  the binding's lifetime: no successor session ever replaces it. A non-null
  external session identity belongs to exactly one Task/role binding within
  its Connection.
- Lifecycle vocabulary is CONNECTING, READY, LOST, ENDED. CONNECTING is by
  definition not yet a bound session (the identity is still None); READY
  means the external session was successfully initialized; LOST records
  genuine loss of the exact external session on the same binding (later
  workflow services translate it into ``AGENT_SESSION_LOST`` blocking
  behavior); ENDED is normal termination, reachable before or after
  initialization. Legal transitions are CONNECTING -> READY | ENDED and
  READY -> LOST | ENDED; LOST and ENDED are absorbing. Runtime/Hub
  unavailability is not lifecycle state: a recoverable Hub restart does not
  create a replacement binding.
- ``ended_at`` is the semantic ENDED timestamp: set exactly when the
  lifecycle status is ENDED, never substituted by ``updated_at``.
- ``effective_config_snapshot`` is the NON-SECRET effective runtime/session
  configuration snapshot captured at initialization. It is assembled by the
  establishing caller; it is never populated by blindly serializing
  Connection configuration or authentication material, and raw
  credentials/tokens must never enter it. Once a session has initialized,
  its snapshot is historical for that Task session: later Workspace
  role-binding/Connection configuration changes affect future sessions and
  never rewrite an initialized session's snapshot.
- ``initialization_protocol_version`` records the protocol version used to
  initialize the session (opaque string; absence is valid).
  ``reported_provider``/``reported_model``/``reported_runtime_version`` are
  nullable opaque runtime-reported provenance observations — never
  configuration authority.
- Capacity is Connection-scoped: Producer and Reviewer sessions sharing one
  Connection each consume occupancy against that Connection's
  Owner-configured session capacity. This module carries the facts;
  admission decisions belong to later services.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, fields
from datetime import datetime
from enum import StrEnum
from math import isinf, isnan
from types import MappingProxyType
from uuid import UUID

from openorc.domain.connections import WorkflowRole

__all__ = [
    "TASK_SESSION_LIFECYCLE_TRANSITIONS",
    "TaskAgentSession",
    "TaskAgentSessionDomainError",
    "TaskSessionLifecycleStatus",
    "canonical_effective_config_snapshot",
    "task_agent_session_field_names",
]


class TaskAgentSessionDomainError(Exception):
    """Raised when a TaskAgentSession domain invariant is violated."""


class TaskSessionLifecycleStatus(StrEnum):
    """Lifecycle of one Task/role external-session binding.

    CONNECTING is establishment in progress and is by definition not yet a
    bound session: the external session identity is still None. READY means
    the external session was successfully initialized. LOST records genuine
    loss of the exact external session on the same binding. ENDED is normal
    termination. LOST and ENDED are absorbing.
    """

    CONNECTING = "connecting"
    READY = "ready"
    LOST = "lost"
    ENDED = "ended"


TASK_SESSION_LIFECYCLE_TRANSITIONS: Mapping[
    TaskSessionLifecycleStatus, frozenset[TaskSessionLifecycleStatus]
] = MappingProxyType(
    {
        # CONNECTING has not bound an external session yet, so it cannot
        # become LOST: only initialization (READY) or ending the
        # establishment attempt (ENDED) leave it.
        TaskSessionLifecycleStatus.CONNECTING: frozenset(
            {TaskSessionLifecycleStatus.READY, TaskSessionLifecycleStatus.ENDED}
        ),
        # An initialized session can genuinely lose its exact external
        # session/context or terminate normally. LOST is lifecycle history on
        # the same binding, never a replacement trigger.
        TaskSessionLifecycleStatus.READY: frozenset(
            {TaskSessionLifecycleStatus.LOST, TaskSessionLifecycleStatus.ENDED}
        ),
        TaskSessionLifecycleStatus.LOST: frozenset(),
        TaskSessionLifecycleStatus.ENDED: frozenset(),
    }
)


def _require_uuid(value: object, name: str) -> None:
    if not isinstance(value, UUID):
        raise TaskAgentSessionDomainError(f"TaskAgentSession.{name} must be a UUID")


def _require_nonblank_str_or_none(value: object, name: str) -> None:
    if value is not None and (not isinstance(value, str) or not value.strip()):
        raise TaskAgentSessionDomainError(
            f"TaskAgentSession.{name} must be None or a non-empty string"
        )


def canonical_effective_config_snapshot(value: object) -> Mapping[str, object]:
    """Canonicalize a non-secret effective runtime/session configuration snapshot.

    Canonical JSON-object semantics, identical in shape to Connection
    ``safe_config``: string keys at every level, JSON-representable values
    only, sequences normalized to lists (tuples do not silently survive),
    NaN/Infinity are rejected, and nothing relies on silent key coercion. The
    returned frozen view is exactly what jsonb stores and what a reload reads
    back.

    This validates canonical form only. The non-secret contract is carried by
    the callers and the guidance: the snapshot is caller-assembled effective
    runtime/session configuration, never a blind serialization of Connection
    configuration or authentication material, and raw credentials/tokens must
    never enter it.
    """
    if not isinstance(value, Mapping):
        raise TaskAgentSessionDomainError(
            "TaskAgentSession.effective_config_snapshot must be a mapping"
        )
    canonical: dict[str, object] = {}
    for key, item in value.items():
        if not isinstance(key, str):
            raise TaskAgentSessionDomainError(
                "TaskAgentSession.effective_config_snapshot keys must be strings at "
                "every level (canonical JSON object keys)"
            )
        canonical[key] = _canonical_snapshot_value(item)
    return MappingProxyType(canonical)


def _canonical_snapshot_value(value: object) -> object:
    """Return the canonical JSON representation of one snapshot value."""
    if isinstance(value, Mapping):
        nested: dict[str, object] = {}
        for key, item in value.items():
            if not isinstance(key, str):
                raise TaskAgentSessionDomainError(
                    "TaskAgentSession.effective_config_snapshot keys must be strings at "
                    "every level (canonical JSON object keys)"
                )
            nested[key] = _canonical_snapshot_value(item)
        return nested
    if isinstance(value, (list, tuple)):
        return [_canonical_snapshot_value(item) for item in value]
    if isinstance(value, (str, bool, int, float)) or value is None:
        if isinstance(value, float) and (isnan(value) or isinf(value)):
            raise TaskAgentSessionDomainError(
                "TaskAgentSession.effective_config_snapshot must be canonical JSON; "
                "NaN and Infinity are not JSON"
            )
        return value
    raise TaskAgentSessionDomainError(
        "TaskAgentSession.effective_config_snapshot values must be JSON-representable "
        f"(mapping, sequence, string, number, boolean, or null); got {type(value).__name__}"
    )


@dataclass(frozen=True, slots=True)
class TaskAgentSession:
    """The durable Task/role <-> external-session binding for one Task.

    One binding per (Task, role) in v1. ``external_session_id`` is None while
    CONNECTING and immutable once initialization succeeds; the lifecycle
    vocabulary and transition rules are the continuity contract; the
    effective configuration snapshot is the non-secret historical
    configuration the session was initialized with; and the reported
    provenance fields are opaque runtime observations.
    """

    id: UUID
    workspace_id: UUID
    task_id: UUID
    role: WorkflowRole
    connection_id: UUID
    external_session_id: str | None
    lifecycle_status: TaskSessionLifecycleStatus
    initialization_protocol_version: str | None
    effective_config_snapshot: Mapping[str, object] | None
    reported_provider: str | None
    reported_model: str | None
    reported_runtime_version: str | None
    initialized_at: datetime | None
    ended_at: datetime | None
    created_at: datetime
    updated_at: datetime

    def __post_init__(self) -> None:
        if not isinstance(self.role, WorkflowRole):
            raise TaskAgentSessionDomainError(
                "TaskAgentSession.role must be a WorkflowRole (producer or reviewer)"
            )
        if not isinstance(self.lifecycle_status, TaskSessionLifecycleStatus):
            raise TaskAgentSessionDomainError(
                "TaskAgentSession.lifecycle_status must be a TaskSessionLifecycleStatus"
            )
        _require_nonblank_str_or_none(self.external_session_id, "external_session_id")
        _require_nonblank_str_or_none(
            self.initialization_protocol_version, "initialization_protocol_version"
        )
        if self.effective_config_snapshot is not None:
            # Validate canonical form in place: the frozen snapshot view is
            # exactly what jsonb stores and what a reload reads back.
            object.__setattr__(
                self,
                "effective_config_snapshot",
                canonical_effective_config_snapshot(self.effective_config_snapshot),
            )
        for field_name in ("reported_provider", "reported_model", "reported_runtime_version"):
            value = getattr(self, field_name)
            # Opaque runtime-reported provenance: any string, or None. No
            # vocabulary, no enum, no normalization — never configuration
            # authority.
            if value is not None and not isinstance(value, str):
                raise TaskAgentSessionDomainError(
                    f"TaskAgentSession.{field_name} must be None or a string"
                )
        _require_uuid(self.task_id, "task_id")
        _require_uuid(self.connection_id, "connection_id")
        # Initialization coherence, mirrored by the database CHECK: CONNECTING
        # requires both NULL; READY and LOST require both non-NULL; ENDED
        # permits either coherent form. Mixed forms (one NULL, one non-NULL)
        # are rejected for every lifecycle status.
        if self.lifecycle_status is TaskSessionLifecycleStatus.CONNECTING:
            if self.external_session_id is not None or self.initialized_at is not None:
                raise TaskAgentSessionDomainError(
                    "a connecting TaskAgentSession has not initialized an external session "
                    "yet: external_session_id and initialized_at must both be None"
                )
        elif self.lifecycle_status is TaskSessionLifecycleStatus.ENDED:
            if (self.external_session_id is None) != (self.initialized_at is None):
                raise TaskAgentSessionDomainError(
                    "an ended TaskAgentSession must carry a coherent initialization "
                    "state: external_session_id and initialized_at are both None "
                    "(ended before initialization) or both set (ended after a "
                    "successful initialization)"
                )
        else:  # READY or LOST
            if self.external_session_id is None or self.initialized_at is None:
                raise TaskAgentSessionDomainError(
                    f"a {self.lifecycle_status.value} TaskAgentSession has an initialized "
                    "external session: external_session_id and initialized_at must "
                    "both be set"
                )
        # ``ended_at`` is the semantic ENDED timestamp: set exactly when the
        # lifecycle status is ENDED, never substituted by ``updated_at``.
        if (self.ended_at is not None) != (
            self.lifecycle_status is TaskSessionLifecycleStatus.ENDED
        ):
            raise TaskAgentSessionDomainError(
                "TaskAgentSession.ended_at must be set exactly when lifecycle_status is ENDED"
            )


def task_agent_session_field_names() -> frozenset[str]:
    """Return the exact field set a TaskAgentSession exposes.

    Lets tests prove the session binding carries only its own continuity
    facts: an opaque external session identity without replacement machinery,
    a non-secret configuration snapshot without authentication material, and
    lifecycle state without runtime health/telemetry or conversational
    content.
    """
    return frozenset(field.name for field in fields(TaskAgentSession))
