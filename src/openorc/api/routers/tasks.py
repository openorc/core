"""Thin Task discussion and conversation transport (issue #339).

This router owns HTTP concerns only: authenticated Profile extraction (the
#52 dependency), request path/body parsing, delegation to the shared
Owner-authorized application services, and the domain-to-wire response
mapping. It owns no authorization policy, no discussion admission semantics,
no receipt lifecycle, and no persistence behavior — those belong to the
service layer. Blocking database and submission work runs off the event
loop (sync route handlers; the v1 persistence surface is synchronous). The
addressed errors are the typed application vocabulary, mapped to HTTP by
the application-level application-error handler.

The discussion POST is asynchronous by contract: a 202 acknowledges that
the message was authorized and its admission operation accepted for
delivery — never that it was delivered, and never a model reply.
"""

from __future__ import annotations

from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, Request

from openorc.api.dependencies.authentication import authenticated_profile
from openorc.api.schemas.tasks import (
    DiscussionAdmissionRead,
    DiscussionMessageCreate,
    DiscussionStatusRead,
    ReviewerConversationMessageRead,
    ReviewerConversationRead,
)
from openorc.config import Settings
from openorc.domain.ownership import Profile
from openorc.persistence.pool import get_database_pool
from openorc.services.errors import (
    ExternalOperationFailedError,
    ExternalOperationUncertainError,
    NotFoundError,
)
from openorc.services.task_owner_reviewer_discussion import (
    get_owner_gate_discussion_status,
    prepare_owner_reviewer_discussion,
    release_prepared_discussion_receipt,
)
from openorc.services.task_reviewer_conversation_projection import (
    build_cline_reviewer_conversation_factory,
    project_task_reviewer_conversation,
)
from openorc.workers.invocation import (
    OperationInvocation,
    OperationKind,
    OperationSubmitter,
    SubmissionOutcome,
    TaskOwnerReviewerDiscussionPayload,
    TaskScope,
    WorkflowRoleRef,
)
from openorc.workers.task_workflow import default_reviewer_neutral_directory_provider

router = APIRouter(tags=["tasks"])

AuthenticatedProfile = Annotated[Profile, Depends(authenticated_profile)]


@router.post(
    "/api/workspaces/{workspace_id}/tasks/{task_id}/owner-gates/{owner_gate_id}/discussion-message",
    status_code=202,
    response_model=DiscussionAdmissionRead,
)
def send_discussion_message(
    workspace_id: UUID,
    task_id: UUID,
    owner_gate_id: UUID,
    payload: DiscussionMessageCreate,
    request: Request,
    profile: AuthenticatedProfile,
) -> DiscussionAdmissionRead:
    """Authorize and queue one Owner ↔ Reviewer discussion message.

    Synchronous preflight maps every immediately detectable failure to its
    typed HTTP error — unauthorized, absent subject, an outstanding run on
    the Reviewer session (conflict), or a no-longer-current/non-permitted
    gate (stale). A 202 acknowledges authorization and transport acceptance
    only; the receipt-status projection reports actual admission and
    refusal.
    """
    settings: Settings = request.app.state.settings
    submitter: OperationSubmitter = request.app.state.operation_submitter
    pool = get_database_pool(settings)
    prepared = prepare_owner_reviewer_discussion(
        pool,
        profile_id=profile.id,
        workspace_id=workspace_id,
        task_id=task_id,
        owner_gate_id=owner_gate_id,
        message=payload.message,
    )
    invocation = OperationInvocation(
        kind=OperationKind.TASK_OWNER_REVIEWER_DISCUSSION,
        payload=TaskOwnerReviewerDiscussionPayload(
            task=TaskScope(
                workspace_id=str(workspace_id),
                task_id=str(task_id),
                role=WorkflowRoleRef.REVIEWER,
            ),
            profile_id=str(profile.id),
            owner_gate_id=str(owner_gate_id),
            receipt_id=str(prepared.receipt.id),
            message=payload.message,
        ),
    )
    result = submitter.submit(invocation)
    if result.outcome is SubmissionOutcome.ACCEPTED_FOR_DELIVERY:
        return DiscussionAdmissionRead(
            receipt_id=prepared.receipt.id,
            owner_gate_id=owner_gate_id,
            reviewer_session_id=prepared.reviewer_session.id,
            task_id=task_id,
            status=prepared.receipt.status,
            claimed_at=prepared.receipt.claimed_at,
        )
    if result.outcome is SubmissionOutcome.KNOWN_REFUSED:
        # A positively refused submission means no worker will ever run: the
        # prepared receipt is released through the one-shot refusal
        # transition (a raced delivered admission is a rejected no-op), and
        # the response is the honest failure — never 202.
        release_prepared_discussion_receipt(
            pool,
            receipt_id=prepared.receipt.id,
            task_id=task_id,
            workspace_id=workspace_id,
        )
        raise ExternalOperationFailedError(
            "the discussion admission submission was refused before delivery; "
            "the prepared message was released and the reviewer session's "
            "discussion slot is free again"
        )
    # An uncertain submission is neither acceptance nor proven non-delivery:
    # the receipt stays conservatively claimed (the operation may still run),
    # OpenOrc never blindly re-submits, and the status projection remains the
    # Owner-visible truth.
    raise ExternalOperationUncertainError(
        "the outcome of the discussion admission submission is unknown; the "
        "prepared receipt stays conservatively claimed and the status "
        "projection reports whether the admission was delivered"
    )


@router.get(
    "/api/workspaces/{workspace_id}/tasks/{task_id}/owner-gates/{owner_gate_id}/discussion-status",
    response_model=DiscussionStatusRead,
)
def read_discussion_status(
    workspace_id: UUID,
    task_id: UUID,
    owner_gate_id: UUID,
    request: Request,
    profile: AuthenticatedProfile,
) -> DiscussionStatusRead:
    """Read one gate-bound discussion's durable lifecycle for its Owner."""
    settings: Settings = request.app.state.settings
    status = get_owner_gate_discussion_status(
        get_database_pool(settings),
        profile_id=profile.id,
        workspace_id=workspace_id,
        task_id=task_id,
        owner_gate_id=owner_gate_id,
    )
    if status is None:
        raise NotFoundError("the addressed owner gate has no discussion exchange in this workspace")
    return DiscussionStatusRead(
        receipt_id=status.receipt_id,
        status=status.status,
        claimed_at=status.claimed_at,
        admission_attempted_at=status.admission_attempted_at,
        admitted_at=status.admitted_at,
        settled_at=status.settled_at,
        outcome_label=status.outcome_label,
    )


@router.get(
    "/api/workspaces/{workspace_id}/tasks/{task_id}/reviewer-conversation",
    response_model=ReviewerConversationRead,
)
def read_reviewer_conversation(
    workspace_id: UUID,
    task_id: UUID,
    request: Request,
    profile: AuthenticatedProfile,
) -> ReviewerConversationRead:
    """Read the Task's Reviewer conversation through the runtime-native surface.

    A transient authorized read: the runtime owns the conversation, the
    projection takes no effect and records nothing durable, and the exact
    existing admitted READY Reviewer session is the only session ever read.
    """
    settings: Settings = request.app.state.settings
    pool = get_database_pool(settings)
    working_directory = default_reviewer_neutral_directory_provider(settings)
    factory = build_cline_reviewer_conversation_factory(
        pool,
        workspace_id=workspace_id,
        task_id=task_id,
        reviewer_working_directory=working_directory,
    )
    projection = project_task_reviewer_conversation(
        pool,
        profile_id=profile.id,
        workspace_id=workspace_id,
        task_id=task_id,
        reviewer_runtime_factory=factory,
    )
    return ReviewerConversationRead(
        task_id=projection.task_id,
        reviewer_session_id=projection.reviewer_session_id,
        messages=[
            ReviewerConversationMessageRead(
                index=message.index, kind=message.kind, text=message.text
            )
            for message in projection.messages
        ],
    )
