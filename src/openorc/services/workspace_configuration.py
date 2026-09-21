"""Workspace configuration application service (issue #53).

The first-class Workspace settings — the review-loop iteration limit and the
Owner-authored guidance prose — as ownership-gated use cases over the
durable, typed fields on ``openorc.workspaces``. These are current, mutable
Workspace configuration: not a generic JSON settings bag, not a replacement
prompt-template table, and not historical evidence (no revisions, hashes, or
snapshots are created).

Every public operation takes the authenticated Profile UUID from the #52
authentication boundary and composes ownership through
:func:`require_profile_workspace` before any read or write — the same
authorization boundary every other service composes with. Database-only
validation and mutation compose inside one short ``composed_transaction``
(#51 external-I/O rule); the row-locked conditional updates in persistence
capture the exact before-state, so the returned audit-handoff facts never
come from a racy re-read.

Audit handoff (#56): each setter returns only the exact safe data needed to
record a consequential configuration-change event. For the review limit that
is the precise previous/new integer values; for guidance it is only the
semantic fact that the setting changed — Owner-authored prose is never
copied into event context. This module deliberately does not record
WorkflowEvents (#56 owns the event stream) and does not invent a generic
settings/audit framework.
"""

from __future__ import annotations

from dataclasses import dataclass
from uuid import UUID

from openorc.domain.ownership import Workspace
from openorc.persistence.ownership import (
    update_workspace_guidance,
    update_workspace_review_iteration_limit,
)
from openorc.persistence.pool import DatabasePool
from openorc.services.errors import InvalidCommandError, NotFoundError
from openorc.services.transaction_composition import composed_transaction
from openorc.services.workspace_authorization import require_profile_workspace

__all__ = [
    "ReviewIterationLimitUpdate",
    "WorkspaceGuidanceUpdate",
    "get_workspace_configuration",
    "set_guidance",
    "set_review_iteration_limit",
]


@dataclass(frozen=True, slots=True)
class ReviewIterationLimitUpdate:
    """Safe audit-handoff facts for one review-iteration-limit change.

    Carries the exact locked-in-transaction previous/new limit values for
    the #56 consequential configuration-change event. A no-op write reports
    ``changed=False`` with identical values.
    """

    workspace_id: UUID
    changed: bool
    previous_review_iteration_limit: int
    new_review_iteration_limit: int


@dataclass(frozen=True, slots=True)
class WorkspaceGuidanceUpdate:
    """The safe audit-handoff fact for one guidance change.

    The #56 event needs only the semantic fact that the Workspace guidance
    setting changed; Owner-authored prose is deliberately not part of the
    handoff so it is never copied into WorkflowEvent context. No history,
    hash, or snapshot exists for guidance.
    """

    workspace_id: UUID
    changed: bool


def get_workspace_configuration(
    pool: DatabasePool, *, profile_id: UUID, workspace_id: UUID
) -> Workspace:
    """Ownership-gated read of the Workspace with its current settings."""
    return require_profile_workspace(pool, profile_id=profile_id, workspace_id=workspace_id)


def set_review_iteration_limit(
    pool: DatabasePool,
    *,
    profile_id: UUID,
    workspace_id: UUID,
    review_iteration_limit: int,
) -> ReviewIterationLimitUpdate:
    """Set the Workspace review-loop iteration limit, failing closed.

    The configured limit must be a positive integer; invalid input is an
    invalid command rejected before any state is touched. A change affects
    future ReviewLoops only — existing loops keep their own stored effective
    limit as immutable historical configuration, and this operation never
    touches them. Same-value writes are no-ops.
    """
    if (
        isinstance(review_iteration_limit, bool)
        or not isinstance(review_iteration_limit, int)
        or review_iteration_limit <= 0
    ):
        raise InvalidCommandError("Workspace review iteration limit must be a positive integer")
    with composed_transaction(pool) as transaction_pool:
        require_profile_workspace(
            transaction_pool, profile_id=profile_id, workspace_id=workspace_id
        )
        result = update_workspace_review_iteration_limit(
            transaction_pool, workspace_id, review_iteration_limit=review_iteration_limit
        )
    if result is None:
        raise NotFoundError("the requested workspace is not available to this Profile")
    updated_workspace, previous_limit, changed = result
    return ReviewIterationLimitUpdate(
        workspace_id=workspace_id,
        changed=changed,
        previous_review_iteration_limit=previous_limit,
        new_review_iteration_limit=updated_workspace.review_iteration_limit,
    )


def set_guidance(
    pool: DatabasePool, *, profile_id: UUID, workspace_id: UUID, guidance: str
) -> WorkspaceGuidanceUpdate:
    """Replace the Workspace guidance prose, failing closed.

    Blank (including empty) guidance is valid and means no
    Workspace-specific guidance; arbitrary Owner-authored prose is stored
    verbatim as the single current value — no revision, hash, or snapshot is
    created. Same-value writes are no-ops.
    """
    if not isinstance(guidance, str):
        raise InvalidCommandError("Workspace guidance must be a string")
    with composed_transaction(pool) as transaction_pool:
        require_profile_workspace(
            transaction_pool, profile_id=profile_id, workspace_id=workspace_id
        )
        result = update_workspace_guidance(transaction_pool, workspace_id, guidance=guidance)
    if result is None:
        raise NotFoundError("the requested workspace is not available to this Profile")
    _, _, changed = result
    return WorkspaceGuidanceUpdate(workspace_id=workspace_id, changed=changed)
