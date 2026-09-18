"""Repositories for WorkflowEvent persistence.

Explicit SQL repositories over the ``openorc`` schema for the
append-oriented WorkflowEvent audit stream (Phase 1, issue #26). Rows
map to transport-independent domain objects from
:mod:`openorc.domain.events`; instants returned from Postgres are
normalized to timezone-aware UTC at this boundary.

- The event stream is append-oriented durable audit/history for
  consequential OpenOrc facts. It is not event sourcing and not a second
  source of workflow state: no event read path exists for reconstructing
  canonical current Task state, and event ``context`` is subordinate
  JSONB (canonical JSON object through the domain canonicalizer and the
  explicit ``Jsonb`` adapter) — never a shadow copy of canonical records.
- Append-only is a surface contract: :func:`record_workflow_event`
  always INSERTs a new row, and this module exposes no update, delete,
  or upsert operation. No database trigger is used; the persistence
  module's public API owns the contract. There is deliberately no
  event-correction or replacement semantics.
- The actor vocabulary is the locked owner/openorc/producer/reviewer/
  runtime/github set (HUMAN is not an actor), CHECK-enforced in the
  database. ``actor_id`` is optional opaque logical actor identity.
- Reads match the demonstrated v1 query paths and their indexes: recent
  Workspace activity, Task history, event type/time, actor, and generic
  subject lookup. All reads are Workspace-scoped (Workspace isolation is
  a security boundary) and deterministically ordered newest-first with
  the event id as tiebreaker.

This module knows nothing about prompt mutation: prompt override
repositories do not write events, and this module does not reference
them. Coordinating a state mutation with its audit event is application/
service-layer work.

Violated database invariants surface as driver exceptions (for example
``ForeignKeyViolation`` from the scope-agreement composite foreign key);
translating driver exceptions into typed application errors is a
service-layer concern, not a persistence one.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any
from uuid import UUID

from psycopg.types.json import Jsonb

from openorc.domain.events import (
    WorkflowEvent,
    WorkflowEventActor,
    WorkflowEventDomainError,
    WorkflowEventType,
    canonical_workflow_event_context,
)
from openorc.persistence.pool import DatabasePool
from openorc.persistence.time import normalize_utc
from openorc.persistence.transactions import transaction

__all__ = [
    "find_events_by_subject",
    "get_workflow_event",
    "list_recent_events_by_actor",
    "list_recent_events_by_type",
    "list_recent_workspace_events",
    "list_task_events",
    "record_workflow_event",
]

_WORKFLOW_EVENT_COLUMNS = (
    "id, workspace_id, task_id, event_type, actor_type, actor_id, "
    "subject_type, subject_id, context, created_at"
)


def _workflow_event_from_row(row: Sequence[Any]) -> WorkflowEvent:
    return WorkflowEvent(
        id=row[0],
        workspace_id=row[1],
        task_id=row[2],
        event_type=WorkflowEventType(row[3]),
        actor_type=WorkflowEventActor(row[4]),
        actor_id=row[5],
        subject_type=row[6],
        subject_id=row[7],
        context=row[8],
        created_at=normalize_utc(row[9]),
    )


def _require_uuid(value: object, name: str) -> None:
    if not isinstance(value, UUID):
        raise WorkflowEventDomainError(f"{name} must be a UUID")


def _require_positive_int(value: object, name: str) -> None:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise WorkflowEventDomainError(f"{name} must be a positive integer")


def record_workflow_event(
    pool: DatabasePool,
    *,
    workspace_id: UUID,
    event_type: WorkflowEventType,
    actor_type: WorkflowEventActor,
    task_id: UUID | None = None,
    actor_id: str | None = None,
    subject_type: str | None = None,
    subject_id: UUID | None = None,
    context: Any = None,
) -> WorkflowEvent:
    """Insert one immutable WorkflowEvent (the only write path).

    Always INSERTs a new row: this module exposes no update, delete, or
    upsert operation, and no event-correction/replacement semantics
    exist. ``event_type`` must be from the locked v1 vocabulary and
    ``actor_type`` from the locked actor vocabulary (HUMAN is not an
    actor). The generic subject reference is validated as a pair — both
    present or both absent. The context is canonicalized through the
    domain (canonical JSON-object semantics) and written through the
    explicit ``Jsonb`` adapter; an omitted context persists as the empty
    object.

    Scope: ``workspace_id`` is always required; ``task_id`` is optional
    and, when present, must belong to the same Workspace (the composite
    foreign key is the durable backstop — ``ForeignKeyViolation``). A
    Workspace-level event legitimately has no Task.
    """
    _require_uuid(workspace_id, "workspace_id")
    if task_id is not None:
        _require_uuid(task_id, "task_id")
    if not isinstance(event_type, WorkflowEventType):
        raise WorkflowEventDomainError("record_workflow_event requires a WorkflowEventType")
    if not isinstance(actor_type, WorkflowEventActor):
        raise WorkflowEventDomainError("record_workflow_event requires a WorkflowEventActor")
    if actor_id is not None and (not isinstance(actor_id, str) or not actor_id.strip()):
        raise WorkflowEventDomainError(
            "WorkflowEvent.actor_id must be a non-empty string when present"
        )
    if (subject_type is None) != (subject_id is None):
        raise WorkflowEventDomainError(
            "WorkflowEvent.subject_type and subject_id are a pair: both must be "
            "present or both absent"
        )
    if subject_type is not None:
        if not isinstance(subject_type, str) or not subject_type.strip():
            raise WorkflowEventDomainError(
                "WorkflowEvent.subject_type must be a non-empty string when present"
            )
        _require_uuid(subject_id, "subject_id")
    canonical_context = canonical_workflow_event_context({} if context is None else context)
    with transaction(pool) as conn:
        row = conn.execute(
            "insert into openorc.workflow_events "
            "(workspace_id, task_id, event_type, actor_type, actor_id, "
            "subject_type, subject_id, context) "
            "values (%s, %s, %s, %s, %s, %s, %s, %s) "
            f"returning {_WORKFLOW_EVENT_COLUMNS}",
            (
                workspace_id,
                task_id,
                event_type.value,
                actor_type.value,
                actor_id,
                subject_type,
                subject_id,
                Jsonb(dict(canonical_context)),
            ),
        ).fetchone()
    assert row is not None
    return _workflow_event_from_row(row)


def get_workflow_event(pool: DatabasePool, *, workflow_event_id: UUID) -> WorkflowEvent | None:
    """Return one WorkflowEvent by id, or ``None`` when it does not exist."""
    _require_uuid(workflow_event_id, "workflow_event_id")
    with transaction(pool) as conn:
        row = conn.execute(
            f"select {_WORKFLOW_EVENT_COLUMNS} from openorc.workflow_events where id = %s",
            (workflow_event_id,),
        ).fetchone()
    return None if row is None else _workflow_event_from_row(row)


def list_recent_workspace_events(
    pool: DatabasePool, *, workspace_id: UUID, limit: int
) -> list[WorkflowEvent]:
    """List a Workspace's recent events, newest first (Workspace activity feed).

    Serves ``workflow_events_workspace_activity_idx (workspace_id,
    created_at)``. ``limit`` must be a positive integer; ordering is
    deterministic (``created_at desc, id desc``).
    """
    _require_uuid(workspace_id, "workspace_id")
    _require_positive_int(limit, "limit")
    with transaction(pool) as conn:
        rows = conn.execute(
            f"select {_WORKFLOW_EVENT_COLUMNS} from openorc.workflow_events "
            "where workspace_id = %s order by created_at desc, id desc limit %s",
            (workspace_id, limit),
        ).fetchall()
    return [_workflow_event_from_row(row) for row in rows]


def list_task_events(pool: DatabasePool, *, task_id: UUID, limit: int) -> list[WorkflowEvent]:
    """List a Task's event history, newest first (Task history path).

    Serves ``workflow_events_task_history_idx (task_id, created_at)``.
    Workspace-level events carry no Task and are outside this read.
    """
    _require_uuid(task_id, "task_id")
    _require_positive_int(limit, "limit")
    with transaction(pool) as conn:
        rows = conn.execute(
            f"select {_WORKFLOW_EVENT_COLUMNS} from openorc.workflow_events "
            "where task_id = %s order by created_at desc, id desc limit %s",
            (task_id, limit),
        ).fetchall()
    return [_workflow_event_from_row(row) for row in rows]


def list_recent_events_by_type(
    pool: DatabasePool, *, workspace_id: UUID, event_type: WorkflowEventType, limit: int
) -> list[WorkflowEvent]:
    """List a Workspace's recent events of one type, newest first.

    Serves ``workflow_events_type_time_idx (event_type, created_at)``
    (Workspace-scoped). ``event_type`` must be from the locked v1
    vocabulary.
    """
    _require_uuid(workspace_id, "workspace_id")
    if not isinstance(event_type, WorkflowEventType):
        raise WorkflowEventDomainError("list_recent_events_by_type requires a WorkflowEventType")
    _require_positive_int(limit, "limit")
    with transaction(pool) as conn:
        rows = conn.execute(
            f"select {_WORKFLOW_EVENT_COLUMNS} from openorc.workflow_events "
            "where workspace_id = %s and event_type = %s "
            "order by created_at desc, id desc limit %s",
            (workspace_id, event_type.value, limit),
        ).fetchall()
    return [_workflow_event_from_row(row) for row in rows]


def list_recent_events_by_actor(
    pool: DatabasePool, *, workspace_id: UUID, actor_type: WorkflowEventActor, limit: int
) -> list[WorkflowEvent]:
    """List a Workspace's recent events by actor type, newest first.

    Serves ``workflow_events_workspace_actor_idx (workspace_id,
    actor_type, created_at)``. ``actor_type`` must be from the locked
    actor vocabulary.
    """
    _require_uuid(workspace_id, "workspace_id")
    if not isinstance(actor_type, WorkflowEventActor):
        raise WorkflowEventDomainError("list_recent_events_by_actor requires a WorkflowEventActor")
    _require_positive_int(limit, "limit")
    with transaction(pool) as conn:
        rows = conn.execute(
            f"select {_WORKFLOW_EVENT_COLUMNS} from openorc.workflow_events "
            "where workspace_id = %s and actor_type = %s "
            "order by created_at desc, id desc limit %s",
            (workspace_id, actor_type.value, limit),
        ).fetchall()
    return [_workflow_event_from_row(row) for row in rows]


def find_events_by_subject(
    pool: DatabasePool,
    *,
    workspace_id: UUID,
    subject_type: str,
    subject_id: UUID,
    limit: int,
) -> list[WorkflowEvent]:
    """Find a Workspace's events referencing one generic subject, newest first.

    Serves ``workflow_events_subject_idx (subject_type, subject_id)``.
    ``subject_type`` is the open validated-text discriminator (no closed
    enum, no foreign key); the lookup is Workspace-scoped.
    """
    _require_uuid(workspace_id, "workspace_id")
    if not isinstance(subject_type, str) or not subject_type.strip():
        raise WorkflowEventDomainError("subject_type must be a non-empty string")
    _require_uuid(subject_id, "subject_id")
    _require_positive_int(limit, "limit")
    with transaction(pool) as conn:
        rows = conn.execute(
            f"select {_WORKFLOW_EVENT_COLUMNS} from openorc.workflow_events "
            "where workspace_id = %s and subject_type = %s and subject_id = %s "
            "order by created_at desc, id desc limit %s",
            (workspace_id, subject_type, subject_id, limit),
        ).fetchall()
    return [_workflow_event_from_row(row) for row in rows]
