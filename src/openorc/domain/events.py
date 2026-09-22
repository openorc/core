"""WorkflowEvent domain models.

A WorkflowEvent is one append-oriented durable audit fact about a
consequential OpenOrc workflow occurrence (Phase 1, issue #26).

- WorkflowEvent is audit history, not canonical workflow state and not
  event sourcing. Canonical current Task/workflow state keeps its own
  durable homes (Task, PlanRevision, ReviewIteration, OwnerGate,
  Execution, RuntimeRequest, TaskBlock, TaskPullRequest, ...); an event
  must never become the mechanism used to reconstruct that state. Events
  record that a fact happened; they never replace the fact's canonical
  record.
- WorkflowEvent is not runtime telemetry storage. High-frequency
  model/tool/activity telemetry belongs to the later runtime-telemetry/
  SSE boundary unless an individual fact acquires independent OpenOrc
  audit meaning.
- Events are immutable once created: no correction, replacement, or
  rewrite semantics exist. A later occurrence of the same kind of fact
  is a new event, never a mutation of an older one.
- The actor vocabulary is exactly OWNER, OPENORC, PRODUCER, REVIEWER,
  RUNTIME, and GITHUB. OWNER is the human-authority actor terminology;
  HUMAN is not an OpenOrc actor. ``actor_id`` is optional opaque logical
  actor identity where meaningful — its kind varies by actor type — and
  logical actor identity is never moved into ``context``.
- Every event carries direct Workspace scope. Task scope is optional: a
  Workspace-level event legitimately has no Task, and a Task-related
  event's scope must agree with its Task's Workspace.
- ``subject_type``/``subject_id`` are the one primary generic subject
  reference: plain validated text plus a UUID, deliberately not a closed
  enum and not foreign-key bound, so a new auditable subject kind never
  requires a schema migration. They are present as a pair or absent as a
  pair.
- ``context`` is subordinate, event-specific metadata — a canonical JSON
  object. It is not a closed schema and remains intentionally extensible,
  but it is never a shadow copy of canonical records and never the
  canonical home for modeled domain state.

This module carries transport-independent validation only. It performs
no event-emission orchestration and no workflow state transitions.
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
    "WorkflowEvent",
    "WorkflowEventActor",
    "WorkflowEventDomainError",
    "WorkflowEventType",
    "canonical_workflow_event_context",
    "workflow_event_field_names",
]


class WorkflowEventDomainError(Exception):
    """Raised when a WorkflowEvent domain invariant is violated."""


class WorkflowEventType(StrEnum):
    """The locked v1 workflow-event vocabulary.

    Meaningful workflow/audit facts, not CRUD symmetry over persistence
    mutations: archival/currentness, branch binding, session lifecycle
    fields, and TaskPullRequest reconciliation already have canonical
    storage homes and deliberately have no shadow event vocabulary here.
    New members are added deliberately — never for persistence-mutation
    symmetry — and extended by migration alongside the database CHECK.
    """

    TASK_CREATED = "task_created"
    AGENT_SESSION_CREATED = "agent_session_created"
    AGENT_SESSION_BOUND = "agent_session_bound"
    AGENT_SESSION_LOST = "agent_session_lost"
    PLANNING_STARTED = "planning_started"
    PLAN_REVISION_CREATED = "plan_revision_created"
    PLAN_REVIEWED = "plan_reviewed"
    REVIEW_LIMIT_REACHED = "review_limit_reached"
    PLAN_REVISION_REVISED = "plan_revision_revised"
    PLAN_READY = "plan_ready"
    IMPLEMENTATION_AUTHORIZED = "implementation_authorized"
    EXECUTION_STARTED = "execution_started"
    EXECUTION_COMPLETED = "execution_completed"
    EXECUTION_FAILED = "execution_failed"
    RUNTIME_REQUEST_CREATED = "runtime_request_created"
    RUNTIME_REQUEST_RESOLVED = "runtime_request_resolved"
    RUNTIME_REQUEST_CANCELLED = "runtime_request_cancelled"
    RETRY_STARTED = "retry_started"
    TASK_BLOCKED = "task_blocked"
    OWNER_GATE_CREATED = "owner_gate_created"
    OWNER_GATE_RESOLVED = "owner_gate_resolved"
    OWNER_REVIEWER_DISCUSSION_MESSAGE = "owner_reviewer_discussion_message"
    PR_CREATED = "pr_created"
    PR_REVIEWED = "pr_reviewed"
    TASK_RELATIONSHIP_SYNCED = "task_relationship_synced"
    TASK_DEPENDENCY_SYNCED = "task_dependency_synced"
    PR_HEAD_CHANGED = "pr_head_changed"
    MERGE_REQUESTED = "merge_requested"
    MERGE_REJECTED_BY_GITHUB = "merge_rejected_by_github"
    PR_MERGED = "pr_merged"
    TASK_CANCELLED = "task_cancelled"
    TASK_COMPLETED = "task_completed"
    WORKSPACE_CONFIGURATION_CHANGED = "workspace_configuration_changed"


class WorkflowEventActor(StrEnum):
    """The locked v1 event actor vocabulary.

    OWNER is the human-authority actor. HUMAN is deliberately absent: it
    is not an OpenOrc actor. ``OPENORC`` is OpenOrc itself acting as the
    control plane; ``PRODUCER``/``REVIEWER`` are the workflow-role
    sessions; ``RUNTIME`` is the agent runtime as a system actor;
    ``GITHUB`` is the external engineering system of record.
    """

    OWNER = "owner"
    OPENORC = "openorc"
    PRODUCER = "producer"
    REVIEWER = "reviewer"
    RUNTIME = "runtime"
    GITHUB = "github"


def _require_uuid(value: object, name: str) -> None:
    if not isinstance(value, UUID):
        raise WorkflowEventDomainError(f"WorkflowEvent.{name} must be a UUID")


def canonical_workflow_event_context(value: object) -> Mapping[str, object]:
    """Canonicalize a WorkflowEvent's ``context`` payload.

    Generic canonical JSON-object semantics, mirroring the other domain
    JSON containers: the value must be a mapping with string keys at
    every level, and every value must be JSON-representable — nested
    mappings, sequences, and scalars only. Sequences normalize to lists
    (tuples do not silently survive), NaN/Infinity are rejected, and
    nothing relies on silent key coercion. The returned frozen view is
    exactly what jsonb stores and what a reload reads back.

    This is a form validator, not a closed schema: event context remains
    intentionally extensible and no forbidden-key list is applied here.
    The architectural rule that context must not shadow canonical domain
    records is an application/design rule, not a JSON key contract.
    """
    if not isinstance(value, Mapping):
        raise WorkflowEventDomainError("WorkflowEvent.context must be a mapping")
    canonical: dict[str, object] = {}
    for key, item in value.items():
        if not isinstance(key, str):
            raise WorkflowEventDomainError(
                "WorkflowEvent.context keys must be strings at every level "
                "(canonical JSON object keys)"
            )
        canonical[key] = _canonical_json_value(item)
    return MappingProxyType(canonical)


def _canonical_json_value(value: object) -> object:
    """Return the canonical JSON representation of one context value."""
    if isinstance(value, Mapping):
        nested: dict[str, object] = {}
        for key, item in value.items():
            if not isinstance(key, str):
                raise WorkflowEventDomainError(
                    "WorkflowEvent.context keys must be strings at every level "
                    "(canonical JSON object keys)"
                )
            nested[key] = _canonical_json_value(item)
        return nested
    if isinstance(value, (list, tuple)):
        return [_canonical_json_value(item) for item in value]
    if isinstance(value, (str, bool, int, float)) or value is None:
        if isinstance(value, float) and (isnan(value) or isinf(value)):
            raise WorkflowEventDomainError(
                "WorkflowEvent.context must be canonical JSON; NaN and Infinity are not JSON"
            )
        return value
    raise WorkflowEventDomainError(
        "WorkflowEvent.context values must be JSON-representable "
        f"(mapping, sequence, string, number, boolean, or null); got {type(value).__name__}"
    )


@dataclass(frozen=True, slots=True)
class WorkflowEvent:
    """One immutable audit fact in the WorkflowEvent stream.

    ``event_type`` is from the locked v1 vocabulary and ``actor_type``
    from the locked actor vocabulary (``actor_id`` is optional opaque
    logical actor identity). ``task_id`` is present exactly for
    Task-related events and absent for Workspace-level ones.
    ``subject_type``/``subject_id`` form the one generic subject
    reference when the fact has a primary subject. ``context`` is
    subordinate event-specific metadata, never a shadow copy of
    canonical records. There is deliberately no update, delete, or
    correction semantics: an event is immutable once created.
    """

    id: UUID
    workspace_id: UUID
    task_id: UUID | None
    event_type: WorkflowEventType
    actor_type: WorkflowEventActor
    actor_id: str | None
    subject_type: str | None
    subject_id: UUID | None
    context: Mapping[str, object]
    created_at: datetime

    def __post_init__(self) -> None:
        for name in ("id", "workspace_id"):
            _require_uuid(getattr(self, name), name)
        if self.task_id is not None:
            _require_uuid(self.task_id, "task_id")
        if not isinstance(self.event_type, WorkflowEventType):
            raise WorkflowEventDomainError(
                "WorkflowEvent.event_type must be a WorkflowEventType (the locked v1 vocabulary)"
            )
        if not isinstance(self.actor_type, WorkflowEventActor):
            raise WorkflowEventDomainError(
                "WorkflowEvent.actor_type must be a WorkflowEventActor (the locked "
                "actor vocabulary; HUMAN is not an OpenOrc actor)"
            )
        if self.actor_id is not None and (
            not isinstance(self.actor_id, str) or not self.actor_id.strip()
        ):
            raise WorkflowEventDomainError(
                "WorkflowEvent.actor_id must be a non-empty string when present"
            )
        # The generic subject reference is a pair: both present or both absent.
        if (self.subject_type is None) != (self.subject_id is None):
            raise WorkflowEventDomainError(
                "WorkflowEvent.subject_type and subject_id are a pair: both must be "
                "present or both absent"
            )
        if self.subject_type is not None:
            if not isinstance(self.subject_type, str) or not self.subject_type.strip():
                raise WorkflowEventDomainError(
                    "WorkflowEvent.subject_type must be a non-empty string when present"
                )
            _require_uuid(self.subject_id, "subject_id")
        if not isinstance(self.context, Mapping):
            raise WorkflowEventDomainError("WorkflowEvent.context must be a mapping")
        # Canonical form always holds: the frozen view is what jsonb
        # stores and what a reload reads back.
        object.__setattr__(self, "context", canonical_workflow_event_context(self.context))


def workflow_event_field_names() -> frozenset[str]:
    """Return the exact field set a WorkflowEvent exposes.

    Lets tests prove the event carries only its own audit facts — the
    locked vocabularies, scope, subject reference, and subordinate
    context — and no duplicated canonical domain state (Task status,
    review outcomes, gate decisions, Execution/RuntimeRequest/PR state,
    ... live on their canonical records, never restated on the event).
    """
    return frozenset(field.name for field in fields(WorkflowEvent))
