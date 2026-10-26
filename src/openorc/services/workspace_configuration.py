"""Workspace configuration application service (issue #53).

The first-class Workspace settings — the review-loop iteration limit, the
Owner-authored guidance prose, and the Owner-authorized default USD Task
budget (issue #240) — as ownership-gated use cases over the durable, typed
fields on ``openorc.workspaces``. These are current, mutable
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

Audit coordination (#56): when a row-locked update reports an actual change,
the same composed transaction records the consequential
``WORKSPACE_CONFIGURATION_CHANGED`` event through
:mod:`openorc.services.event_coordination` — canonical mutation and event
insert commit or roll back together, and a failed/stale canonical mutation
writes no event. The review-limit event carries the precise previous/new
integer values; the default-budget event carries the precise previous/new
exact decimal values (or the ``None`` that disables spending policy); the
guidance event identifies only the setting change, so Owner-authored prose
is never copied into event context. Same-value no-op writes record no
event, and no generic "row updated" events exist.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from uuid import UUID

from openorc.domain.ownership import Workspace
from openorc.observability import annotate_span, application_span
from openorc.persistence.ownership import (
    update_workspace_default_task_budget_usd,
    update_workspace_guidance,
    update_workspace_review_iteration_limit,
)
from openorc.persistence.pool import DatabasePool
from openorc.services import event_coordination
from openorc.services.errors import InvalidCommandError, NotFoundError
from openorc.services.profile_lifecycle_guard import require_account_operational
from openorc.services.transaction_composition import composed_transaction
from openorc.services.workspace_authorization import require_profile_workspace

__all__ = [
    "DefaultTaskBudgetUpdate",
    "ReviewIterationLimitUpdate",
    "WorkspaceGuidanceUpdate",
    "get_workspace_configuration",
    "set_default_task_budget",
    "set_guidance",
    "set_review_iteration_limit",
]

# Application-service span boundaries (issues #108/#109): every public
# use-case operation of this module opens one span at the established service
# boundary. Only safe vocabulary attributes are attachable, so Workspace
# guidance prose has no supported path into telemetry.
_SERVICE_TRACER_SCOPE = "openorc.services.workspace_configuration"
_REVIEW_LIMIT_SPAN_NAME = "workspace_configuration.set_review_iteration_limit"
_GET_CONFIGURATION_SPAN_NAME = "workspace_configuration.get_workspace_configuration"
_SET_GUIDANCE_SPAN_NAME = "workspace_configuration.set_guidance"
_SET_DEFAULT_TASK_BUDGET_SPAN_NAME = "workspace_configuration.set_default_task_budget"


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
class DefaultTaskBudgetUpdate:
    """Safe audit-handoff facts for one default-Task-budget change (issue #240).

    Carries the exact locked-in-transaction previous/new USD values (either
    side may be ``None`` — disabling is a legitimate change) for the #56
    consequential configuration-change event. A no-op write reports
    ``changed=False`` with identical values.
    """

    workspace_id: UUID
    changed: bool
    previous_default_task_budget_usd: Decimal | None
    new_default_task_budget_usd: Decimal | None


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
    with application_span(_SERVICE_TRACER_SCOPE, _GET_CONFIGURATION_SPAN_NAME) as span:
        annotate_span(
            span,
            operation=_GET_CONFIGURATION_SPAN_NAME,
            workspace_id=str(workspace_id),
        )
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
    with application_span(_SERVICE_TRACER_SCOPE, _REVIEW_LIMIT_SPAN_NAME) as span:
        annotate_span(
            span,
            operation=_REVIEW_LIMIT_SPAN_NAME,
            workspace_id=str(workspace_id),
        )
        if (
            isinstance(review_iteration_limit, bool)
            or not isinstance(review_iteration_limit, int)
            or review_iteration_limit <= 0
        ):
            raise InvalidCommandError("Workspace review iteration limit must be a positive integer")
        with composed_transaction(pool) as transaction_pool:
            # The account-wide Owner-mutation barrier first (issue #97): the
            # Profile FOR KEY SHARE read is the first lock acquisition and
            # fails closed while an account deletion attempt is unresolved.
            require_account_operational(transaction_pool, profile_id=profile_id)
            require_profile_workspace(
                transaction_pool, profile_id=profile_id, workspace_id=workspace_id
            )
            result = update_workspace_review_iteration_limit(
                transaction_pool,
                workspace_id,
                review_iteration_limit=review_iteration_limit,
            )
            if result is None:
                raise NotFoundError("the requested workspace is not available to this Profile")
            updated_workspace, previous_limit, changed = result
            if changed:
                event_coordination.record_review_iteration_limit_changed_event(
                    transaction_pool,
                    workspace_id=workspace_id,
                    actor=event_coordination.owner_actor(profile_id),
                    previous_limit=previous_limit,
                    new_limit=updated_workspace.review_iteration_limit,
                )
    return ReviewIterationLimitUpdate(
        workspace_id=workspace_id,
        changed=changed,
        previous_review_iteration_limit=previous_limit,
        new_review_iteration_limit=updated_workspace.review_iteration_limit,
    )


def set_default_task_budget(
    pool: DatabasePool,
    *,
    profile_id: UUID,
    workspace_id: UUID,
    default_task_budget_usd: Decimal | None,
) -> DefaultTaskBudgetUpdate:
    """Set the Workspace's Owner-authorized default USD Task budget, failing closed.

    ``None`` disables the Workspace spending policy (no implicit limit —
    never zero); a configured value is a positive finite exact decimal that
    each future Task copies as its authorized budget at its successful Owner
    Start. Already-started Tasks keep the authorization they were started
    with: the default affects future Tasks only. Same-value writes are
    no-ops that record no audit event; an actual change (including enabling
    or disabling) records the consequential
    ``WORKSPACE_CONFIGURATION_CHANGED`` audit with the exact locked
    previous/new values.
    """
    with application_span(_SERVICE_TRACER_SCOPE, _SET_DEFAULT_TASK_BUDGET_SPAN_NAME) as span:
        annotate_span(
            span,
            operation=_SET_DEFAULT_TASK_BUDGET_SPAN_NAME,
            workspace_id=str(workspace_id),
        )
        if default_task_budget_usd is not None and (
            isinstance(default_task_budget_usd, bool)
            or not isinstance(default_task_budget_usd, Decimal)
            or not default_task_budget_usd.is_finite()
            or default_task_budget_usd <= 0
        ):
            raise InvalidCommandError(
                "Workspace default task budget must be None or a positive finite "
                "USD amount (an exact decimal)"
            )
        with composed_transaction(pool) as transaction_pool:
            # The account-wide Owner-mutation barrier first (issue #97): the
            # Profile FOR KEY SHARE read is the first lock acquisition and
            # fails closed while an account deletion attempt is unresolved.
            require_account_operational(transaction_pool, profile_id=profile_id)
            require_profile_workspace(
                transaction_pool, profile_id=profile_id, workspace_id=workspace_id
            )
            result = update_workspace_default_task_budget_usd(
                transaction_pool,
                workspace_id,
                default_task_budget_usd=default_task_budget_usd,
            )
            if result is None:
                raise NotFoundError("the requested workspace is not available to this Profile")
            updated_workspace, previous_budget, changed = result
            if changed:
                event_coordination.record_default_task_budget_changed_event(
                    transaction_pool,
                    workspace_id=workspace_id,
                    actor=event_coordination.owner_actor(profile_id),
                    previous_budget=previous_budget,
                    new_budget=updated_workspace.default_task_budget_usd,
                )
    return DefaultTaskBudgetUpdate(
        workspace_id=workspace_id,
        changed=changed,
        previous_default_task_budget_usd=previous_budget,
        new_default_task_budget_usd=updated_workspace.default_task_budget_usd,
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
    with application_span(_SERVICE_TRACER_SCOPE, _SET_GUIDANCE_SPAN_NAME) as span:
        annotate_span(
            span,
            operation=_SET_GUIDANCE_SPAN_NAME,
            workspace_id=str(workspace_id),
        )
        if not isinstance(guidance, str):
            raise InvalidCommandError("Workspace guidance must be a string")
        with composed_transaction(pool) as transaction_pool:
            # The account-wide Owner-mutation barrier first (issue #97): the
            # Profile FOR KEY SHARE read precedes any subject lock.
            require_account_operational(transaction_pool, profile_id=profile_id)
            require_profile_workspace(
                transaction_pool, profile_id=profile_id, workspace_id=workspace_id
            )
            result = update_workspace_guidance(transaction_pool, workspace_id, guidance=guidance)
            if result is None:
                raise NotFoundError("the requested workspace is not available to this Profile")
            _, _, changed = result
            if changed:
                event_coordination.record_guidance_changed_event(
                    transaction_pool,
                    workspace_id=workspace_id,
                    actor=event_coordination.owner_actor(profile_id),
                )
    return WorkspaceGuidanceUpdate(workspace_id=workspace_id, changed=changed)
