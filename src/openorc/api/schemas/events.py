"""Wire DTOs for the authorized WorkflowEvent audit read surface (issue #377).

Thin transport models only: the Owner-authorized application services own
every semantic rule (Workspace/Task scope authorization, filter validation,
cursor binding, and the safe public projection) and these models never
duplicate service/domain validation — FastAPI parses the declared shapes and
the router delegates. The record shape is the exact committed audit fact —
the locked vocabularies, scope, subject pair, actor provenance, and the
service-projected safe context — with no github/openorc wrapper, no
human-readable descriptions, no generated prose, and no UI presentation
metadata.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any
from uuid import UUID

from pydantic import BaseModel, StrictStr

__all__ = [
    "WorkflowEventListRead",
    "WorkflowEventRead",
]


class WorkflowEventRead(BaseModel):
    """The public WorkflowEvent audit record (issue #377).

    ``task_id`` is null for Workspace-level events; ``actor_id`` is the
    nullable opaque safe logical actor identity; ``subject_type``/
    ``subject_id`` are null together when the fact has no distinct primary
    subject; ``context`` is the event's safe projected JSON object.
    """

    id: UUID
    workspace_id: UUID
    task_id: UUID | None
    event_type: StrictStr
    actor_type: StrictStr
    actor_id: StrictStr | None
    subject_type: StrictStr | None
    subject_id: UUID | None
    context: dict[str, Any]
    created_at: datetime


class WorkflowEventListRead(BaseModel):
    """The bounded event list envelope with its opaque deterministic cursor."""

    items: list[WorkflowEventRead]
    next_cursor: StrictStr | None
