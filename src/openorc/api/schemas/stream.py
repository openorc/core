"""Wire DTOs for the authorized Workspace SSE live-update stream (issue #399).

Thin transport models only: the subscription policy belongs to the application
services and these models never duplicate it. The ``data_changed`` message is
the exact identifier-only invalidation envelope; the ``workflow_event``
message reuses the existing G2c :class:`WorkflowEventRead` DTO unchanged so
the two wire shapes for the same audit fact can never drift.
"""

from __future__ import annotations

from uuid import UUID

from pydantic import BaseModel, StrictStr

__all__ = ["WorkspaceStreamDataChangedRead"]


class WorkspaceStreamDataChangedRead(BaseModel):
    """The public ``data_changed`` live-stream message (issue #399).

    Identifier-only invalidation hint: the Workspace scope, the changed
    resource's type from the closed vocabulary (workspace, project,
    repository, issue, task, connection, role_binding, refresh), and the
    resource's internal UUID. The workspace resource's ID equals the
    ``workspace_id``; the role_binding resource ID is the binding row's own
    identity (or the broader Workspace hint as published). The resource may
    already be deleted: a correctly scoped invalidation never requires the
    resource to still exist. No full resources, patches, frontend cache keys,
    table names, or raw metadata are carried, and dependent list/detail/
    history/action query invalidation is documented against the G4b
    publication coverage map.
    """

    workspace_id: UUID
    resource_type: StrictStr
    resource_id: UUID
