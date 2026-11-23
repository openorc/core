"""Wire DTOs for the Workspace Project surface (issue #372).

Thin transport models only: the application service owns every semantic rule
(the account-mutation barrier, Workspace ownership, the Owner-authored name
rule) and these models never duplicate service/domain validation. A Project
name is genuine Owner configuration, never a GitHub observation.
"""

from __future__ import annotations

from datetime import datetime
from uuid import UUID

from pydantic import BaseModel

__all__ = ["ProjectCreate", "ProjectRead"]


class ProjectCreate(BaseModel):
    """The creation command: one non-blank, bounded Owner-authored Project name."""

    name: str


class ProjectRead(BaseModel):
    """One Project's durable identity within its Workspace."""

    id: UUID
    workspace_id: UUID
    name: str
    created_at: datetime
    updated_at: datetime
