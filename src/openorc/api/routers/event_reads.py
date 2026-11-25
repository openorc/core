"""Thin authorized WorkflowEvent audit read transport (issue #377).

Public paths compose under the versioned ``/api/v1`` namespace (issue #369).

This router owns HTTP concerns only: authenticated Profile extraction, path
and query parsing (the ``from`` query name is bound through an alias — it is
a Python keyword), delegation to the Owner-authorized event read services,
and the projected public record's wire mapping. It owns no authorization
policy, no filter/cursor validation, and no context sanitization — those
belong to the service layer, which projects the safe public shape. The reads
are database-only and side-effect free: no GET ever calls GitHub or a runtime
provider, mutates workflow, initiates recovery, or reconstructs canonical
Task state from events. Blocking database work runs off the event loop (sync
route handlers; the v1 persistence surface is synchronous). The addressed
errors are the typed application vocabulary, mapped to HTTP by the
application-level handler. The listing is historical ordering over visible
committed records — not a frozen multi-request snapshot and not a
commit-order/replay sequence; G4 (issue #94) owns replay refinement, and
WorkflowEvents are audit facts, not client data-invalidation notifications.
"""

from __future__ import annotations

from datetime import datetime
from typing import Annotated, Any
from uuid import UUID

from fastapi import APIRouter, Depends, Query, Request

from openorc.api.dependencies.application_errors import (
    DOCUMENTED_AUTH_401,
    DOCUMENTED_INVALID_COMMAND_422,
    DOCUMENTED_NOT_FOUND_404,
    DOCUMENTED_UNAVAILABLE_503,
)
from openorc.api.dependencies.authentication import authenticated_profile
from openorc.api.schemas.events import WorkflowEventListRead, WorkflowEventRead
from openorc.config import Settings
from openorc.domain.events import WorkflowEventActor, WorkflowEventType
from openorc.domain.ownership import Profile
from openorc.persistence.pool import get_database_pool
from openorc.services import event_reads
from openorc.services.errors import InvalidCommandError

router = APIRouter(tags=["tasks"])

AuthenticatedProfile = Annotated[Profile, Depends(authenticated_profile)]

_WORKSPACE_EVENTS_PATH = "/workspaces/{workspace_id}/events"
_TASK_EVENTS_PATH = "/workspaces/{workspace_id}/tasks/{task_id}/events"

_READ_RESPONSES: dict[int | str, dict[str, Any]] = {
    401: DOCUMENTED_AUTH_401,
    404: DOCUMENTED_NOT_FOUND_404,
    422: DOCUMENTED_INVALID_COMMAND_422,
    503: DOCUMENTED_UNAVAILABLE_503,
}

_TIME_BOUND_DESCRIPTION = (
    "Timezone-aware instant, UTC-normalized by the service; a naive timestamp is rejected"
)


def _event_to_wire(event: event_reads.PublicWorkflowEvent) -> WorkflowEventRead:
    """Map one projected public event to its exact wire shape."""
    return WorkflowEventRead(
        id=event.id,
        workspace_id=event.workspace_id,
        task_id=event.task_id,
        event_type=event.event_type,
        actor_type=event.actor_type,
        actor_id=event.actor_id,
        subject_type=event.subject_type,
        subject_id=event.subject_id,
        context=dict(event.context),
        created_at=event.created_at,
    )


def _parse_event_type(event_type: str | None) -> WorkflowEventType | None:
    """Parse the canonical event_type filter (one WorkflowEventType value)."""
    if event_type is None:
        return None
    try:
        return WorkflowEventType(event_type)
    except ValueError as error:
        raise InvalidCommandError(
            "the event_type filter must be one canonical WorkflowEventType value"
        ) from error


def _parse_actor_type(actor_type: str | None) -> WorkflowEventActor | None:
    """Parse the canonical actor_type filter (one WorkflowEventActor value)."""
    if actor_type is None:
        return None
    try:
        return WorkflowEventActor(actor_type)
    except ValueError as error:
        raise InvalidCommandError(
            "the actor_type filter must be one canonical WorkflowEventActor value"
        ) from error


@router.get(
    _WORKSPACE_EVENTS_PATH,
    response_model=WorkflowEventListRead,
    operation_id="list_workspace_events",
    responses=_READ_RESPONSES,
)
def list_workspace_events(
    request: Request,
    profile: AuthenticatedProfile,
    workspace_id: UUID,
    event_type: Annotated[
        str | None, Query(description="One canonical WorkflowEventType value")
    ] = None,
    actor_type: Annotated[
        str | None, Query(description="One canonical WorkflowEventActor value")
    ] = None,
    from_: Annotated[
        datetime | None,
        Query(alias="from", description=f"{_TIME_BOUND_DESCRIPTION} (inclusive)"),
    ] = None,
    to: Annotated[
        datetime | None, Query(description=f"{_TIME_BOUND_DESCRIPTION} (exclusive)")
    ] = None,
    limit: Annotated[
        int, Query(ge=1, le=event_reads.EVENT_PAGE_LIMIT_MAX, description="Page size (bounded)")
    ] = event_reads.EVENT_PAGE_LIMIT,
    cursor: Annotated[
        str | None,
        Query(
            description=(
                "The previous page's opaque next_cursor (pages this exact scope and filter set)"
            )
        ),
    ] = None,
) -> WorkflowEventListRead:
    """List the Workspace's visible WorkflowEvent audit history.

    Bounded, deterministically ordered newest-first by ``(created_at, id)``,
    and advanced by the opaque ``next_cursor`` when more items exist. The
    page includes the Workspace's Task-scoped events and its Workspace-level
    facts. Range boundaries: ``from`` is inclusive and ``to`` is exclusive.
    This is historical ordering over visible committed records — not a
    frozen multi-request snapshot and not a commit-order/replay sequence;
    G4 (issue #94) owns replay refinement, and these audit facts are not
    client data-invalidation notifications.
    """
    settings: Settings = request.app.state.settings
    events, next_cursor = event_reads.list_workspace_events_for_owner(
        get_database_pool(settings),
        profile_id=profile.id,
        workspace_id=workspace_id,
        event_type=_parse_event_type(event_type),
        actor_type=_parse_actor_type(actor_type),
        from_created_at=from_,
        to_created_at=to,
        cursor=cursor,
        limit=limit,
    )
    return WorkflowEventListRead(
        items=[_event_to_wire(event) for event in events],
        next_cursor=next_cursor,
    )


@router.get(
    _TASK_EVENTS_PATH,
    response_model=WorkflowEventListRead,
    operation_id="list_task_events",
    responses=_READ_RESPONSES,
)
def list_task_events(
    request: Request,
    profile: AuthenticatedProfile,
    workspace_id: UUID,
    task_id: UUID,
    event_type: Annotated[
        str | None, Query(description="One canonical WorkflowEventType value")
    ] = None,
    actor_type: Annotated[
        str | None, Query(description="One canonical WorkflowEventActor value")
    ] = None,
    from_: Annotated[
        datetime | None,
        Query(alias="from", description=f"{_TIME_BOUND_DESCRIPTION} (inclusive)"),
    ] = None,
    to: Annotated[
        datetime | None, Query(description=f"{_TIME_BOUND_DESCRIPTION} (exclusive)")
    ] = None,
    limit: Annotated[
        int, Query(ge=1, le=event_reads.EVENT_PAGE_LIMIT_MAX, description="Page size (bounded)")
    ] = event_reads.EVENT_PAGE_LIMIT,
    cursor: Annotated[
        str | None,
        Query(
            description=(
                "The previous page's opaque next_cursor (pages this exact scope and filter set)"
            )
        ),
    ] = None,
) -> WorkflowEventListRead:
    """List one exact Task's WorkflowEvent audit history.

    The exact authorized Task attempt's events — current or archived — with
    the same filters, range boundaries, ordering, and cursor binding as the
    Workspace scope. Reads never reconstruct canonical Task state from
    events.
    """
    settings: Settings = request.app.state.settings
    events, next_cursor = event_reads.list_task_events_for_owner(
        get_database_pool(settings),
        profile_id=profile.id,
        workspace_id=workspace_id,
        task_id=task_id,
        event_type=_parse_event_type(event_type),
        actor_type=_parse_actor_type(actor_type),
        from_created_at=from_,
        to_created_at=to,
        cursor=cursor,
        limit=limit,
    )
    return WorkflowEventListRead(
        items=[_event_to_wire(event) for event in events],
        next_cursor=next_cursor,
    )
