"""Thin Task discussion, conversation, and archived-purge transport (issues #339, #374).

Public paths compose under the versioned ``/api/v1`` namespace (issue #369).

This router owns HTTP concerns only: authenticated Profile extraction (the
#52 dependency), request path/body parsing, delegation to the shared
Owner-authorized application services, and the domain-to-wire response
mapping. It owns no authorization policy, no discussion admission semantics,
no receipt lifecycle, and no persistence behavior — those belong to the
service layer. The issue #374 archived-Task purge delegates to the issue #97
administrative-deletion service unchanged. Blocking database and submission
work runs off the event loop (sync route handlers; the v1 persistence
surface is synchronous). The
addressed errors are the typed application vocabulary, mapped to HTTP by
the application-level application-error handler.

The discussion POST is asynchronous by contract: a 202 acknowledges that
the message was authorized and its admission operation accepted for
delivery — never that it was delivered, and never a model reply.

The ordinary OwnerGate decision commands (issue #379) are synchronous
committed decisions, never blanket 202s: the implementation-authorization
and PR-authorization approvals resolve their exact current gate APPROVED
through the existing dedicated services and arrange the already-owned E5→E6
/ E7→E8 F1 continuations post-commit, documenting the separately-typed
delivery evidence without ever failing the committed approval on a refused
or uncertain submission; the merge decision delegates to the ONE Owner
merge command service with its GitHub App and Profile user-token
boundaries and the F10/F12 submission seams composed, and maps the
service's classified outcome kinds verbatim. The commands never re-derive
action menus (the G2b read owns that projection), never add cancellation
routes (G3c #380 owns the shared ``cancel_task`` command), and never treat
a projected ``approve`` action as granted authority.
"""

from __future__ import annotations

from typing import Annotated, Any
from uuid import UUID

from fastapi import APIRouter, Depends, Request

from openorc.api.dependencies.application_errors import (
    DOCUMENTED_AUTH_401,
    DOCUMENTED_CONFLICT_409,
    DOCUMENTED_INVALID_COMMAND_422,
    DOCUMENTED_NOT_FOUND_404,
    DOCUMENTED_UNAVAILABLE_503,
)
from openorc.api.dependencies.authentication import authenticated_profile
from openorc.api.dependencies.github_app_client import resolve_github_app_client
from openorc.api.dependencies.github_user_flow import resolve_profile_user_token_resolver
from openorc.api.schemas.tasks import (
    ArchivedTaskPurge,
    DiscussionAdmissionRead,
    DiscussionMessageCreate,
    DiscussionStatusRead,
    GitHubTaskPullRequestHeadRefRead,
    GitHubTaskPullRequestRead,
    GitHubTaskUserRefRead,
    ImplementationAuthorizationApprovalRead,
    MergeDecisionOutcomeRead,
    MergeRetryAttemptRead,
    OpenOrcTaskBlockSummaryRead,
    OpenOrcTaskTerminalizationRead,
    OwnerGateDecisionCommand,
    OwnerGateDecisionSnapshotRead,
    OwnerGateResolutionRead,
    PrAuthorizationApprovalRead,
    ReviewerConversationMessageRead,
    ReviewerConversationRead,
)
from openorc.config import Settings
from openorc.domain.blocks import TaskBlock
from openorc.domain.gates import OwnerGate
from openorc.domain.ownership import Profile
from openorc.domain.pull_requests import TaskPullRequest
from openorc.domain.tasks import Task
from openorc.persistence.pool import get_database_pool
from openorc.services import (
    administrative_deletion,
    task_implementation_authorization,
    task_merge_lifecycle,
    task_pr_authorization,
)
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
from openorc.services.task_reads import terminalization_facts
from openorc.services.task_reviewer_conversation_projection import (
    build_cline_reviewer_conversation_factory,
    project_task_reviewer_conversation,
)
from openorc.workers.invocation import (
    OperationDependencies,
    OperationInvocation,
    OperationKind,
    OperationSubmitter,
    SubmissionOutcome,
    TaskOwnerReviewerDiscussionPayload,
    TaskScope,
    WorkflowRoleRef,
)
from openorc.workers.task_cleanup_operations import F1TerminalFinalizationSubmission
from openorc.workers.task_workflow import (
    F1AuthorizedRetrySubmission,
    default_reviewer_neutral_directory_provider,
    submit_authorized_implementation_after_approval,
    submit_pr_composition_after_approval,
)

router = APIRouter(tags=["tasks"])

AuthenticatedProfile = Annotated[Profile, Depends(authenticated_profile)]


@router.post(
    "/workspaces/{workspace_id}/tasks/{task_id}/owner-gates/{owner_gate_id}/discussion-message",
    status_code=202,
    response_model=DiscussionAdmissionRead,
    operation_id="send_owner_gate_discussion_message",
    responses={
        401: DOCUMENTED_AUTH_401,
        404: DOCUMENTED_NOT_FOUND_404,
        409: DOCUMENTED_CONFLICT_409,
        503: DOCUMENTED_UNAVAILABLE_503,
    },
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
    "/workspaces/{workspace_id}/tasks/{task_id}/owner-gates/{owner_gate_id}/discussion-status",
    response_model=DiscussionStatusRead,
    operation_id="get_owner_gate_discussion_status",
    responses={
        401: DOCUMENTED_AUTH_401,
        404: DOCUMENTED_NOT_FOUND_404,
        503: DOCUMENTED_UNAVAILABLE_503,
    },
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
    "/workspaces/{workspace_id}/tasks/{task_id}/reviewer-conversation",
    response_model=ReviewerConversationRead,
    operation_id="get_task_reviewer_conversation",
    responses={
        401: DOCUMENTED_AUTH_401,
        404: DOCUMENTED_NOT_FOUND_404,
        503: DOCUMENTED_UNAVAILABLE_503,
    },
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


@router.delete(
    "/workspaces/{workspace_id}/tasks/{task_id}",
    response_model=ArchivedTaskPurge,
    operation_id="purge_archived_task",
    responses={
        401: DOCUMENTED_AUTH_401,
        404: DOCUMENTED_NOT_FOUND_404,
        409: DOCUMENTED_CONFLICT_409,
        503: DOCUMENTED_UNAVAILABLE_503,
    },
)
def purge_archived_task(
    workspace_id: UUID,
    task_id: UUID,
    request: Request,
    profile: AuthenticatedProfile,
) -> ArchivedTaskPurge:
    """Purge one archived Task attempt with its complete OpenOrc aggregate.

    Only archived (terminal) attempts are purgeable: a current Task is a
    typed conflict, and normal workflow cancellation is the supported
    route — this is never a cancellation substitute. The purge deletes
    OpenOrc's internal aggregate only; the backing GitHub issue, branch,
    and PR are never touched.
    """
    settings: Settings = request.app.state.settings
    purged = administrative_deletion.purge_archived_task_record(
        get_database_pool(settings),
        profile_id=profile.id,
        workspace_id=workspace_id,
        task_id=task_id,
    )
    return ArchivedTaskPurge(workspace_id=purged.workspace_id, task_id=purged.id)


# --- The ordinary OwnerGate decision commands (issue #379) --------------------


def _decision_snapshot_to_wire(task: Task) -> OwnerGateDecisionSnapshotRead:
    """Map one authoritative post-write Task snapshot onto the decision wire.

    Verbatim over the service-returned snapshot — the exact facts the
    guarded write committed, including the Task's actual returned status
    and ``archived_at`` (a committed terminalization can coexist with a
    still-pending archival). No current-state re-derivation happens here.
    """
    return OwnerGateDecisionSnapshotRead(
        task_id=task.id,
        workspace_id=task.workspace_id,
        repository_id=task.repository_id,
        github_issue_id=task.github_issue_id,
        github_issue_number=task.github_issue_number,
        status=task.status.value,
        state_token=task.state_token,
        created_at=task.created_at,
        updated_at=task.updated_at,
        archived_at=task.archived_at,
    )


def _gate_resolution_to_wire(gate: OwnerGate) -> OwnerGateResolutionRead:
    """Map one service-returned OwnerGate onto its subject-facts wire shape.

    Verbatim identity/type/status/decided_at plus the exact bound subject
    form. Deliberately no action menu: menus are the G2b read surface's
    current-context projection, never a command re-derivation.
    """
    return OwnerGateResolutionRead(
        id=gate.id,
        gate_type=gate.gate_type.value,
        status=gate.status.value,
        decided_at=gate.decided_at,
        plan_revision_id=gate.plan_revision_id,
        subject_head_sha=gate.subject_head_sha,
        task_pull_request_id=gate.task_pull_request_id,
    )


def _pull_request_to_wire(pull_request: TaskPullRequest) -> GitHubTaskPullRequestRead:
    """Map the authoritative reconciled canonical PR onto the #376 PR read."""
    return GitHubTaskPullRequestRead(
        id=pull_request.github_pr_id,
        number=pull_request.github_pr_number,
        title=pull_request.title,
        body=pull_request.body,
        html_url=pull_request.html_url,
        user=(
            None
            if pull_request.user_id is None or pull_request.user_login is None
            else GitHubTaskUserRefRead(id=pull_request.user_id, login=pull_request.user_login)
        ),
        state=pull_request.state.value,
        draft=pull_request.draft,
        created_at=pull_request.provider_created_at,
        updated_at=pull_request.provider_updated_at,
        closed_at=pull_request.provider_closed_at,
        merged_at=pull_request.merged_at,
        head=GitHubTaskPullRequestHeadRefRead(ref=pull_request.head_ref, sha=pull_request.head_sha),
        base=GitHubTaskPullRequestHeadRefRead(ref=pull_request.base_ref, sha=pull_request.base_sha),
        mergeable=pull_request.mergeable,
        merge_state_status=pull_request.merge_state_status,
        mergeable_observed_at=pull_request.pr_mergeability_observed_at,
        mergeable_head_sha=pull_request.pr_mergeability_head_sha,
    )


def _block_summary_to_wire(block: TaskBlock) -> OpenOrcTaskBlockSummaryRead:
    """Map one applied TaskBlock onto the #376 block summary shape."""
    return OpenOrcTaskBlockSummaryRead(
        id=block.id,
        reason=block.reason.value,
        created_at=block.created_at,
    )


def _terminalization_to_wire(task: Task) -> OpenOrcTaskTerminalizationRead | None:
    """Project the returned Task's committed terminalization facts (#376 shape)."""
    facts = terminalization_facts(task)
    if facts is None:
        return None
    return OpenOrcTaskTerminalizationRead(
        outcome=facts.outcome.value,
        origin=facts.origin,
        established_at=facts.established_at,
    )


def _continuation_dependencies(request: Request) -> OperationDependencies:
    """The continuation dependencies resolving this process's F1 submitter.

    The serving process's own submission seam (the bounded local executor
    in regular mode, the canonical queue submission in queue mode) is the
    transport the already-owned E5→E6 / E7→E8 continuation helpers submit
    through; no other dependency is ever resolved behind an HTTP request.
    """
    return OperationDependencies(
        settings=request.app.state.settings,
        continuation_submitter_provider=lambda _settings: request.app.state.operation_submitter,
    )


_GATE_COMMAND_RESPONSES: dict[int | str, dict[str, Any]] = {
    401: DOCUMENTED_AUTH_401,
    404: DOCUMENTED_NOT_FOUND_404,
    409: DOCUMENTED_CONFLICT_409,
    422: DOCUMENTED_INVALID_COMMAND_422,
    503: DOCUMENTED_UNAVAILABLE_503,
}

_GATE_PATH = "/workspaces/{workspace_id}/tasks/{task_id}/owner-gates/{owner_gate_id}"


@router.post(
    _GATE_PATH + "/approve-implementation-authorization",
    response_model=ImplementationAuthorizationApprovalRead,
    operation_id="approve_implementation_authorization",
    responses=_GATE_COMMAND_RESPONSES,
)
def approve_implementation_authorization(
    workspace_id: UUID,
    task_id: UUID,
    owner_gate_id: UUID,
    command: OwnerGateDecisionCommand,
    request: Request,
    profile: AuthenticatedProfile,
) -> ImplementationAuthorizationApprovalRead:
    """Approve the exact current implementation-authorization gate (E5).

    One synchronous committed decision through the existing dedicated
    service: the exact current pending gate bound to the review-cleared
    PlanRevision resolves APPROVED with truthful OWNER provenance, the
    planning loop closes, and the Task moves to ``implementing`` under a
    rotated token — or a stale/conflicting command fails with its typed
    HTTP outcome and applies nothing. The response is the committed
    post-write snapshot plus the separately-typed E5→E6 continuation
    delivery evidence the already-owned F1 continuation helper captured:
    ``accepted_for_delivery``, ``known_refused``, or ``uncertain`` — a
    refused or uncertain continuation never fails or reverses the
    committed approval. Approval never certifies implementation completion.
    """
    settings: Settings = request.app.state.settings
    authorization = task_implementation_authorization.approve_implementation_authorization(
        get_database_pool(settings),
        profile_id=profile.id,
        workspace_id=workspace_id,
        task_id=task_id,
        owner_gate_id=owner_gate_id,
        expected_state_token=command.state_token,
    )
    submission = submit_authorized_implementation_after_approval(
        authorization,
        profile_id=profile.id,
        dependencies=_continuation_dependencies(request),
    )
    return ImplementationAuthorizationApprovalRead(
        task=_decision_snapshot_to_wire(authorization.task),
        owner_gate=_gate_resolution_to_wire(authorization.owner_gate),
        continuation_outcome=submission.outcome.value,
    )


@router.post(
    _GATE_PATH + "/approve-pr-authorization",
    response_model=PrAuthorizationApprovalRead,
    operation_id="approve_pr_authorization",
    responses=_GATE_COMMAND_RESPONSES,
)
def approve_pr_authorization(
    workspace_id: UUID,
    task_id: UUID,
    owner_gate_id: UUID,
    command: OwnerGateDecisionCommand,
    request: Request,
    profile: AuthenticatedProfile,
) -> PrAuthorizationApprovalRead:
    """Approve publication of the exact gate head (E7).

    One synchronous committed decision through the existing dedicated
    service: the exact current pending PR_AUTHORIZATION gate bound to the
    authorized committed head (before a canonical PR exists) resolves
    APPROVED with truthful OWNER provenance under a rotated token, while
    the Task stays ``waiting_for_owner`` — this approval is neither a
    transition nor a PR creation. The response is the committed post-write
    snapshot plus the separately-typed E7→E8 PR-composition continuation
    delivery evidence; a refused or uncertain continuation never fails or
    reverses the committed approval. Approval never certifies PR
    publication.
    """
    settings: Settings = request.app.state.settings
    authorization = task_pr_authorization.approve_pr_authorization(
        get_database_pool(settings),
        profile_id=profile.id,
        workspace_id=workspace_id,
        task_id=task_id,
        owner_gate_id=owner_gate_id,
        expected_state_token=command.state_token,
    )
    submission = submit_pr_composition_after_approval(
        authorization,
        profile_id=profile.id,
        dependencies=_continuation_dependencies(request),
    )
    return PrAuthorizationApprovalRead(
        task=_decision_snapshot_to_wire(authorization.task),
        owner_gate=_gate_resolution_to_wire(authorization.owner_gate),
        continuation_outcome=submission.outcome.value,
    )


@router.post(
    _GATE_PATH + "/approve-merge-decision",
    response_model=MergeDecisionOutcomeRead,
    operation_id="approve_merge_decision",
    responses=_GATE_COMMAND_RESPONSES,
)
def approve_merge_decision(
    workspace_id: UUID,
    task_id: UUID,
    owner_gate_id: UUID,
    command: OwnerGateDecisionCommand,
    request: Request,
    profile: AuthenticatedProfile,
) -> MergeDecisionOutcomeRead:
    """Approve the exact current merge decision gate and command its merge (E10).

    One synchronous Owner-accountable command through the ONE Owner merge
    service: the exact current pending MERGE_DECISION gate bound to the
    canonical PR and the Reviewer-accepted head resolves APPROVED, the
    canonical PR is reconciled authoritatively and the source issue proven
    open, and the exact-head merge runs under the approving Profile's own
    GitHub user-write authority — or a hard precondition failure maps to
    its typed HTTP outcome and applies nothing. The response maps the
    service's classified outcome kinds verbatim — every kind is a committed
    durable disposition, never blanket 202: ``completed`` confirms the
    GitHub-confirmed merge (with the committed terminalization facts; the
    mandatory archival is the F10 finalizer's follow-up, so ``archived_at``
    can still be ``null``), ``retry_gate_installed`` installs the NEW
    pending exact-head gate, ``retry_authorized`` documents the durably
    authorized bounded retry, and the ``blocked_*`` kinds document the
    applied blocking condition. ``merge_commit_sha`` stays ``null`` exactly
    when the merge was confirmed through authoritative reconciliation
    without that SHA. Approval alone does not certify the merge: an
    uncertain outcome blocks.
    """
    settings: Settings = request.app.state.settings
    submitter: OperationSubmitter = request.app.state.operation_submitter
    outcome = task_merge_lifecycle.request_task_merge(
        get_database_pool(settings),
        resolve_github_app_client(settings),
        resolve_profile_user_token_resolver(settings),
        profile_id=profile.id,
        workspace_id=workspace_id,
        task_id=task_id,
        owner_gate_id=owner_gate_id,
        expected_state_token=command.state_token,
        terminal_finalization_submission=F1TerminalFinalizationSubmission(submitter),
        retry_submission=F1AuthorizedRetrySubmission(lambda: submitter),
    )
    return MergeDecisionOutcomeRead(
        kind=outcome.kind.value,
        task=_decision_snapshot_to_wire(outcome.task),
        pull_request=_pull_request_to_wire(outcome.pull_request),
        terminalization=_terminalization_to_wire(outcome.task),
        retry_gate=(
            None if outcome.retry_gate is None else _gate_resolution_to_wire(outcome.retry_gate)
        ),
        task_block=(
            None if outcome.task_block is None else _block_summary_to_wire(outcome.task_block)
        ),
        merge_commit_sha=outcome.merge_commit_sha,
        retry_attempt=(
            None
            if outcome.retry_attempt is None
            else MergeRetryAttemptRead(
                id=outcome.retry_attempt.id,
                ordinal=outcome.retry_attempt.ordinal,
                due=outcome.retry_attempt.not_before,
                error_code=outcome.retry_attempt.error_code.value,
            )
        ),
    )
