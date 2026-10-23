"""E8: Producer PR composition and race-safe canonical PR publication (issue #83).

E8 completes the next proven manual Producer handoff after E7 (issue #82)
durably APPROVED one exact-head ``PR_AUTHORIZATION`` OwnerGate: it requests
the Producer-authored PR title/body from the Task's existing Producer session
through the one canonical ``PR_COMPOSE`` interaction, publishes the Task's one
canonical pull request through the existing race-safe publication primitive
(issue #64) under the approving Profile's GitHub user-to-server authorization,
and atomically finalizes a clean publication into ``reviewing``.

Authority boundaries (continuing the merged E7 contract):

- **The publication authority is persisted evidence, never a transient.** The
  command addresses the approved historical gate by id and the expected
  post-approval Task ``state_token``; admission re-derives the gate's
  ``APPROVED`` status, its exact head-SHA subject, and the single
  OWNER-attributed ``owner_gate_resolved`` audit event from durable state.
  E7 intentionally cleared the current-gate pointer on approval, so the
  current-pointer guard is deliberately not used here. The authenticated
  caller Profile may be supplied for admission, but it must be the historical
  approving Profile: a mismatched caller/approver is rejected rather than
  substituted, and the approving Profile — re-proven account-operational and
  Owner — is the only publication identity.
- **The Producer supplies presentation only.** One ``PR_COMPOSE``
  interaction on the exact pre-existing READY Producer binding requests the
  ``pr_result``; the validated title/body can never override the
  authoritative repository, branch, base, head SHA, or approving identity,
  and a missing effective closing reference for the Task's exact issue is
  appended deterministically. No Reviewer interaction, no new session, no
  hidden prompt, and no semantic judgment of PR writing quality exists here.
- **Publication goes through the existing race-safe primitive exactly once.**
  ``pull_request_publication.publish_task_pull_request`` (issue #64) owns
  the preflight, the single create, the immediate reconciliation, and the
  outcome classification; this module never reimplements or wraps its
  provider/branch race matrix and never falls back to an
  installation-authenticated create, another Profile, or a PAT.
- **Clean publication is finalized atomically or fails closed truthfully.**
  Only the primitive's ``PUBLISHED`` outcome — or the authoritative
  reconciliation of this module's own interrupted publication (the narrow
  continuation below) — may advance the Task. The finalize re-proves the
  full review-eligibility guard under the Task row lock: the canonical PR
  must remain open and unmerged with the canonical head ref, the authorized
  base, the exact approved head SHA, and the stable GitHub identity, and the
  ``reviewing`` transition plus the one ``pr_created`` audit commit or roll
  back together.
- **Stale authority blocks recoverably; a merged PR is never blocked as an
  unmerged closure.** A preflight head mismatch (no PR created) and the
  post-create stale classifications atomically record the reason-specific
  ``TaskBlock`` (``stale_operation``; ``pr_closed_unmerged`` exclusively for
  a closed-unmerged PR) with the ``blocked`` transition and the
  ``task_blocked`` audit when the Task is still current at the expected
  token — and mutate nothing when it already moved on. An authoritatively
  confirmed merged PR refuses finalization with zero durable mutation:
  merge confirmation/completion belongs to the later recovery layer (E10,
  issue #85), which must never inherit a false unmerged-closure block.
- **The continuation is completion, not a framework.** If this module's own
  publication was interrupted after the primitive persisted the canonical
  record, a re-invocation whose admitted record carries the exact approved
  head reconciles it authoritatively through the established
  ``reconcile_task_pull_request`` seam (never repeating the composition,
  never re-creating, never adopting an unrelated PR) and finishes the state
  and event finalization when still valid. Generalized recovery, replay, and
  dispatch belong to E10 (issue #85) and Phase 2F, not here.

No universal dispatcher, operation outbox, retry scheduler, or conversation
checkpoint exists in this module. No database transaction spans the runtime
interaction, the reconciliation, or any GitHub call. Telemetry carries only
the sanctioned safe identifier vocabulary — never the PR title/body, issue
content, tokens, prompts, or provider bodies.
"""

from __future__ import annotations

import re
from collections.abc import Callable
from contextlib import AbstractContextManager, ExitStack
from dataclasses import dataclass
from typing import cast
from uuid import UUID

from openorc.adapters.agent_runtime.contract import AgentRuntimeAdapter
from openorc.adapters.agent_runtime.errors import (
    AgentRuntimeError,
    AgentRuntimeUncertainOutcomeError,
)
from openorc.adapters.github import GitHubAppClient
from openorc.domain.blocks import TaskBlockReason
from openorc.domain.connections import WorkflowRole
from openorc.domain.events import WorkflowEvent, WorkflowEventActor, WorkflowEventType
from openorc.domain.gates import OwnerGate, OwnerGateStatus, OwnerGateType
from openorc.domain.pull_requests import TaskPullRequest, TaskPullRequestState
from openorc.domain.sessions import TaskAgentSession, TaskSessionLifecycleStatus
from openorc.domain.tasks import Task, TaskStatus
from openorc.observability import annotate_span, application_span
from openorc.persistence import blocks as block_records
from openorc.persistence import events as event_records
from openorc.persistence import gates as gate_records
from openorc.persistence import ownership as ownership_records
from openorc.persistence import pull_requests as pull_request_records
from openorc.persistence import sessions as session_records
from openorc.persistence import tasks as task_records
from openorc.persistence.pool import DatabasePool
from openorc.protocol.interaction import PrComposeInteraction
from openorc.protocol.models import PR_RESULT_FAMILY, PrResult
from openorc.services import (
    event_coordination,
    pull_request_publication,
    task_cancellation,
    task_pull_request_reconciliation,
)
from openorc.services.errors import (
    AuthorizationError,
    ConflictError,
    ExternalOperationFailedError,
    ExternalOperationUncertainError,
    InvalidCommandError,
    NotFoundError,
    StaleOperationError,
)
from openorc.services.github_installation_route import (
    resolve_system_repository_installation_route,
)
from openorc.services.github_user_authorization import ProfileUserAccessTokenResolver
from openorc.services.profile_lifecycle_guard import require_account_operational
from openorc.services.pull_request_publication import TaskPullRequestPublicationOutcome
from openorc.services.task_subject_guards import require_current_task
from openorc.services.transaction_composition import composed_transaction
from openorc.services.workspace_authorization import require_workspace_task

__all__ = [
    "ProducerRuntimeAttachmentFactory",
    "TaskPrPublished",
    "compose_and_publish_task_pr",
]

_SERVICE_TRACER_SCOPE = "openorc.services.task_pr_composition"
_COMPOSE_SPAN_NAME = "task_pr_composition.compose_and_publish_task_pr"

# The presentation and its audit are OpenOrc continuations, never an Owner
# approval: publication provenance is OPENORC with no actor identity.
_OPENORC_ACTOR = event_coordination.WorkflowActorContext(WorkflowEventActor.OPENORC, None)

# The gate-resolution audit lookup is bounded: one gate carries at most its
# one-shot resolution history, and an ambiguous lookup is a refusal, not a
# guess.
_AUDIT_LOOKUP_LIMIT = 50

# The generic subject discriminators used by this module's audit lookups and
# recorder mapping (the open validated-text vocabulary).
_OWNER_GATE_SUBJECT_TYPE = "owner_gate"

# Deterministic closing-reference recognition: the GitHub closing-keyword
# vocabulary immediately before ``#<number>`` (optionally qualified by the
# exact ``owner/repo`` address). Fenced code blocks and inline code spans are
# quoted examples and never close anything.
_CLOSING_REFERENCE_PATTERN = re.compile(
    r"(?P<keyword>close[sd]?|fix(?:es|ed)?|resolve[sd]?)\s+"
    r"(?:(?P<owner_login>[A-Za-z0-9][A-Za-z0-9-]*)/(?P<repository_name>[A-Za-z0-9_.-]+))?"
    r"#(?P<issue_number>\d+)",
    re.IGNORECASE,
)
_FENCED_CODE_BLOCK = re.compile(r"```.*?(?:```|$)|~~~.*?(?:~~~|$)", re.DOTALL)
_INLINE_CODE_SPAN = re.compile(r"`[^`\n]*`")


# --- typed seams and outcomes -------------------------------------------------------


ProducerRuntimeAttachmentFactory = Callable[[], AbstractContextManager[AgentRuntimeAdapter]]


@dataclass(frozen=True, slots=True)
class TaskPrPublished:
    """The durable publication facts E9 (issue #84) consumes.

    ``task`` is the post-write Task (``reviewing`` under its rotated
    ``state_token``); ``pull_request`` is the one canonical TaskPullRequest
    with its GitHub-confirmed stable identity and exact approved head SHA.
    Dispatching the PR review belongs exclusively to E9; this module never
    starts it.
    """

    task: Task
    pull_request: TaskPullRequest


@dataclass(frozen=True, slots=True)
class _PublicationSubjects:
    """The durable publication subjects admitted for one invocation.

    ``continuation_pull_request`` is present exactly when the Task already
    carries the canonical record of this module's own interrupted publication
    (the narrow completion path); ``producer_session``/``guidance`` are
    present exactly on the fresh composition path.
    """

    approving_profile_id: UUID
    task: Task
    gate: OwnerGate
    canonical_branch: str
    base_ref: str
    repository_full_name: str
    continuation_pull_request: TaskPullRequest | None
    producer_session: TaskAgentSession | None
    guidance: str | None


class _StalePublicationGuard(Exception):
    """Internal signal: the persisted PR failed the review-eligibility guard.

    The finalize transaction rolls back with no partial state; the caller
    records the reason-specific recoverable block as its own short
    transaction. Carries only the safe block reason and its recovery context.
    """

    def __init__(self, reason: TaskBlockReason, context: dict[str, str]) -> None:
        super().__init__(reason.value)
        self.reason = reason
        self.context = context


# --- command helpers ----------------------------------------------------------------


def _require_uuid_command(value: object, name: str) -> None:
    """Reject a malformed UUID command argument before any state is touched."""
    if not isinstance(value, UUID):
        raise InvalidCommandError(f"{name} must be a UUID")


def _require_producer_factory(value: object) -> ProducerRuntimeAttachmentFactory:
    if not callable(value):
        raise InvalidCommandError(
            "producer_runtime_factory must be a callable returning one adapter "
            "attachment context manager for the exact resolved Producer session"
        )
    return cast(ProducerRuntimeAttachmentFactory, value)


# --- deterministic closing reference -------------------------------------------------


def _has_effective_closing_reference(
    body: str, *, repository_full_name: str, issue_number: int
) -> bool:
    """Whether the body already closes the Task's exact issue effectively.

    Only the GitHub closing-keyword vocabulary immediately before the issue
    reference counts; a reference to a different repository or a quoted
    example (a fenced code block or an inline code span) never closes this
    Task's issue.
    """
    prose = _INLINE_CODE_SPAN.sub(" ", _FENCED_CODE_BLOCK.sub(" ", body))
    for match in _CLOSING_REFERENCE_PATTERN.finditer(prose):
        owner_login = match.group("owner_login")
        if owner_login is not None:
            qualified = f"{owner_login}/{match.group('repository_name')}".lower()
            if qualified != repository_full_name.lower():
                continue
        if int(match.group("issue_number")) == issue_number:
            return True
    return False


def _normalized_pr_body(result: PrResult, *, repository_full_name: str, issue_number: int) -> str:
    """Normalize the validated presentation into the final PR body.

    An existing effective closing reference for the Task's exact issue in the
    authoritative repository is preserved without duplication; otherwise one
    deterministic ``Closes #<issue_number>`` line is appended. The
    authoritative issue itself is never modified and ``pr_result`` gains no
    machine fields.
    """
    if _has_effective_closing_reference(
        result.body, repository_full_name=repository_full_name, issue_number=issue_number
    ):
        return result.body
    return f"{result.body.rstrip()}\n\nCloses #{issue_number}"


# --- the durable approval audit ------------------------------------------------------


def _resolve_approved_resolution_audit(
    transaction_pool: DatabasePool, *, workspace_id: UUID, owner_gate_id: UUID
) -> UUID:
    """Resolve the gate's single OWNER-attributed approved resolution audit.

    The exact ``owner_gate_resolved`` event addressing this gate must carry
    the ``approved`` outcome, OWNER provenance, and a valid canonical Profile
    UUID ``actor_id`` — that Profile is the durable publication identity.
    Zero or multiple unambiguous resolutions are refusals, never guesses.
    """
    events = event_records.find_events_by_subject(
        transaction_pool,
        workspace_id=workspace_id,
        subject_type=_OWNER_GATE_SUBJECT_TYPE,
        subject_id=owner_gate_id,
        limit=_AUDIT_LOOKUP_LIMIT,
    )
    approved: list[WorkflowEvent] = []
    for event in events:
        if event.event_type is not WorkflowEventType.OWNER_GATE_RESOLVED:
            continue
        if event.actor_type is not WorkflowEventActor.OWNER or event.actor_id is None:
            continue
        if event.context.get("outcome") != "approved":
            continue
        approved.append(event)
    if not approved:
        raise ConflictError(
            "the pr authorization gate carries no owner-approved resolution "
            "audit; publication is not authorized"
        )
    if len(approved) > 1:
        raise ConflictError(
            "the pr authorization gate carries an ambiguous owner-approved "
            "resolution audit; publication is not authorized"
        )
    try:
        return UUID(approved[0].actor_id)
    except ValueError as error:
        raise ConflictError(
            "the owner-approved resolution audit carries no canonical profile "
            "identity; publication is not authorized"
        ) from error


# --- admission and revalidation ------------------------------------------------------


def _admit_publication_subjects(
    transaction_pool: DatabasePool,
    *,
    profile_id: UUID,
    workspace_id: UUID,
    task_id: UUID,
    owner_gate_id: UUID,
    expected_state_token: UUID,
) -> _PublicationSubjects:
    """One short DB-only admission pass over the exact publication subjects.

    The account-operational barrier composes first, then the exact
    Workspace/Task Owner authorization. Under the Task row lock the Task must
    be current, non-archived, ``waiting_for_owner``, at the expected token,
    with no current OwnerGate (E7 intentionally cleared the pointer on
    approval) and a bound canonical feature branch. The addressed historical
    gate must belong to this Task/Workspace, be the durably ``APPROVED``
    ``PR_AUTHORIZATION`` gate carrying exactly its head-SHA subject, and
    resolve to exactly one OWNER-attributed approved audit whose Profile is
    the caller — a mismatched caller/approver is rejected, never substituted.
    A canonical ``TaskPullRequest`` carrying the exact approved head routes
    the invocation to the narrow continuation path; a record carrying any
    other head is a conflict (unrelated external PRs are never adopted). The
    fresh path additionally resolves the original READY Producer binding,
    the current Workspace guidance, and the routed Repository presentation
    context — the workflow base comes from the routed Repository record,
    never from the Producer.
    """
    require_account_operational(transaction_pool, profile_id=profile_id)
    require_workspace_task(
        transaction_pool, profile_id=profile_id, workspace_id=workspace_id, task_id=task_id
    )
    locked = task_records.get_task_for_update(transaction_pool, task_id)
    if locked is None or locked.workspace_id != workspace_id:
        raise NotFoundError("the requested task is not available in this workspace")
    task = require_current_task(
        transaction_pool,
        workspace_id=workspace_id,
        task_id=task_id,
        expected_state_token=expected_state_token,
    )
    if task.status is not TaskStatus.WAITING_FOR_OWNER:
        raise ConflictError(
            "the addressed task is not waiting on its owner gate; pr publication is not available"
        )
    if task.current_owner_gate_id is not None:
        raise ConflictError(
            "the task carries a current owner gate; pr publication is not available"
        )
    if task.canonical_feature_branch is None:
        raise ConflictError(
            "the task carries no bound canonical feature branch; the publication "
            "subject is incomplete"
        )
    existing_pr = pull_request_records.get_task_pull_request_for_task(
        transaction_pool, task_id=task.id
    )
    gate = gate_records.get_owner_gate(transaction_pool, owner_gate_id=owner_gate_id)
    if gate is None or gate.task_id != task.id or gate.workspace_id != workspace_id:
        raise NotFoundError("the requested owner gate is not available for this task")
    if gate.gate_type is not OwnerGateType.PR_AUTHORIZATION:
        raise ConflictError(
            "only the durably approved pr authorization gate publishes through this seam"
        )
    if gate.status is not OwnerGateStatus.APPROVED:
        raise ConflictError(
            "the addressed pr authorization gate is not durably approved; "
            "publication is not authorized"
        )
    if (
        gate.subject_head_sha is None
        or gate.plan_revision_id is not None
        or gate.task_pull_request_id is not None
    ):
        raise ConflictError("the pr authorization gate does not carry the exact head-sha subject")
    approving_profile_id = _resolve_approved_resolution_audit(
        transaction_pool, workspace_id=workspace_id, owner_gate_id=owner_gate_id
    )
    if approving_profile_id != profile_id:
        raise AuthorizationError(
            "the pr authorization was approved by a different profile; "
            "publication is not available to this caller"
        )

    require_account_operational(transaction_pool, profile_id=approving_profile_id)
    require_workspace_task(
        transaction_pool,
        profile_id=approving_profile_id,
        workspace_id=workspace_id,
        task_id=task_id,
    )
    route = resolve_system_repository_installation_route(
        transaction_pool,
        workspace_id=workspace_id,
        repository_id=task.repository_id,
    )
    repository_full_name = (
        f"{route.repository.metadata.owner_login}/{route.repository.metadata.name}"
    )
    base_ref = route.repository.metadata.default_branch
    if base_ref is None or not base_ref.strip():
        raise ConflictError(
            "the routed repository carries no usable workflow base branch; "
            "publication is not available"
        )
    if existing_pr is not None:
        # The continuation path: only the canonical record of this module's
        # own interrupted publication may complete — never an unrelated
        # external PR and never a replacement row.
        if existing_pr.head_sha != gate.subject_head_sha:
            raise ConflictError(
                "the task's canonical pull request does not carry the approved head; "
                "unrelated pull requests are never adopted"
            )
        return _PublicationSubjects(
            approving_profile_id=approving_profile_id,
            task=task,
            gate=gate,
            canonical_branch=task.canonical_feature_branch,
            base_ref=base_ref,
            repository_full_name=repository_full_name,
            continuation_pull_request=existing_pr,
            producer_session=None,
            guidance=None,
        )
    session = session_records.get_task_agent_session(
        transaction_pool, task_id=task.id, role=WorkflowRole.PRODUCER
    )
    if session is None or session.task_id != task.id or session.workspace_id != workspace_id:
        raise NotFoundError("the requested producer session is not available for this task")
    if session.lifecycle_status is not TaskSessionLifecycleStatus.READY:
        raise ConflictError(
            "the producer session binding is not usable; pr composition is not available"
        )
    if session.external_session_id is None or not session.external_session_id.strip():
        raise ConflictError("the producer session binding carries no external session identity")
    workspace = ownership_records.get_workspace(transaction_pool, workspace_id)
    if workspace is None:
        raise NotFoundError("the requested workspace is not available to this Profile")
    guidance = workspace.guidance if workspace.guidance.strip() else None
    return _PublicationSubjects(
        approving_profile_id=approving_profile_id,
        task=task,
        gate=gate,
        canonical_branch=task.canonical_feature_branch,
        base_ref=base_ref,
        repository_full_name=repository_full_name,
        continuation_pull_request=None,
        producer_session=session,
        guidance=guidance,
    )


def _require_unchanged_publication_subjects(
    transaction_pool: DatabasePool,
    *,
    admitted: _PublicationSubjects,
    workspace_id: UUID,
    task_id: UUID,
    owner_gate_id: UUID,
    expected_state_token: UUID,
) -> _PublicationSubjects:
    """Re-admit and require the exact same subjects after acquisition.

    The fresh composition revalidates the identical approval facts, route,
    Producer binding, and still-absent canonical PR before it may publish —
    after the runtime interaction, Producer prose is never authorization to
    publish a moved-on subject.
    """
    current = _admit_publication_subjects(
        transaction_pool,
        profile_id=admitted.approving_profile_id,
        workspace_id=workspace_id,
        task_id=task_id,
        owner_gate_id=owner_gate_id,
        expected_state_token=expected_state_token,
    )
    if current.gate.id != admitted.gate.id or (
        current.gate.subject_head_sha != admitted.gate.subject_head_sha
    ):
        raise StaleOperationError(
            "the approved pr authorization gate moved on during composition; "
            "the publication is stale"
        )
    if current.canonical_branch != admitted.canonical_branch or (
        current.base_ref != admitted.base_ref
    ):
        raise StaleOperationError(
            "the publication route moved on during composition; the publication is stale"
        )
    if (current.continuation_pull_request is None) != (admitted.continuation_pull_request is None):
        raise StaleOperationError(
            "the task's canonical pull request moved on during composition; "
            "the publication is stale"
        )
    if admitted.producer_session is not None and (
        current.producer_session is None
        or current.producer_session.id != admitted.producer_session.id
    ):
        raise StaleOperationError(
            "the producer session binding moved on during composition; the publication is stale"
        )
    return current


# --- the recoverable stale-publication block -----------------------------------------


def _block_stale_publication(
    pool: DatabasePool,
    *,
    profile_id: UUID,
    workspace_id: UUID,
    task_id: UUID,
    expected_state_token: UUID,
    reason: TaskBlockReason,
    context: dict[str, str],
) -> Task:
    """Atomically block the Task on stale publication authority.

    One short composed transaction (the established E7 block shape): the
    approving Profile's account-operational barrier is re-proved FIRST (the
    admission check does not remain authoritative across the intervening
    external calls), then the durable stale evidence against the freshly
    locked Task: the ``reason`` TaskBlock, the ``waiting_for_owner →
    blocked`` transition with a rotated token, and the ``task_blocked``
    audit commit or roll back together. A Task that already moved on
    applies nothing and raises ``StaleOperationError`` — no PR is created
    here, no SHA is silently reauthorized, and no new Owner gate exists.
    Recovery from the blocked state belongs to later layers (E10, #85).
    """
    with composed_transaction(pool) as transaction_pool:
        require_account_operational(transaction_pool, profile_id=profile_id)
        locked = task_records.get_task_for_update(transaction_pool, task_id)
        if locked is None or locked.workspace_id != workspace_id:
            raise NotFoundError("the requested task is not available in this workspace")
        task = require_current_task(
            transaction_pool,
            workspace_id=workspace_id,
            task_id=task_id,
            expected_state_token=expected_state_token,
        )
        if task.status is not TaskStatus.WAITING_FOR_OWNER:
            raise ConflictError(
                "the addressed task is not waiting on its owner gate; the "
                "stale publication block is stale"
            )
        block = block_records.create_task_block(
            transaction_pool,
            workspace_id=workspace_id,
            task_id=task_id,
            reason=reason,
            context=context,
        )
        blocked = task_records.update_task_status(
            transaction_pool,
            task_id,
            expected_state_token=task.state_token,
            status=TaskStatus.BLOCKED,
        )
        if blocked is None:
            raise StaleOperationError(
                "the task could not move to blocked; the stale publication block is stale"
            )
        event_coordination.record_task_blocked_event(
            transaction_pool,
            workspace_id=workspace_id,
            task_id=task_id,
            task_block_id=block.id,
            actor=_OPENORC_ACTOR,
        )
        return blocked


# --- the review-eligibility guard ----------------------------------------------------


def _require_review_eligible_pull_request(
    *,
    pull_request: TaskPullRequest,
    task: Task,
    base_ref: str,
) -> None:
    """Require the persisted canonical PR to be eligible for owner review.

    The record must durably remain open and unmerged, with the Task's bound
    canonical head branch and the authorized publication base. A
    closed-unmerged PR, a confirmed merge, a retargeted base, or a changed
    head branch each fails closed with its own typed signal — never
    replacement, adoption, or dispatch. The confirmed closed-unmerged and
    confirmed-merge classifications deliberately precede the finalize's
    exact approved head-SHA drift handling (issue #229): an externally
    established terminal PR state is never masked as a stale publication
    subject. Generalized closed-PR and merge recovery belongs to E10.
    """
    if pull_request.state is not TaskPullRequestState.OPEN:
        if pull_request.merged_at is None:
            raise _StalePublicationGuard(
                reason=TaskBlockReason.PR_CLOSED_UNMERGED,
                context={
                    "pull_request_id": str(pull_request.id),
                    "github_pr_number": str(pull_request.github_pr_number),
                    "observed_state": str(pull_request.state.value),
                },
            )
        raise ConflictError(
            "the canonical pull request is already merged; merge confirmation "
            "and task completion belong to the later recovery layer"
        )
    if pull_request.head_ref != task.canonical_feature_branch:
        raise _StalePublicationGuard(
            reason=TaskBlockReason.STALE_OPERATION,
            context={
                "pull_request_id": str(pull_request.id),
                "github_pr_number": str(pull_request.github_pr_number),
                "observed_head_ref": pull_request.head_ref,
            },
        )
    if pull_request.base_ref != base_ref:
        raise _StalePublicationGuard(
            reason=TaskBlockReason.STALE_OPERATION,
            context={
                "pull_request_id": str(pull_request.id),
                "github_pr_number": str(pull_request.github_pr_number),
                "observed_base_ref": pull_request.base_ref,
            },
        )


# --- the atomic finalization ---------------------------------------------------------


def _finalize_published_task_pr(
    pool: DatabasePool,
    *,
    subjects: _PublicationSubjects,
    expected_github_pr_id: int | None,
    workspace_id: UUID,
    task_id: UUID,
    owner_gate_id: UUID,
    expected_state_token: UUID,
) -> TaskPrPublished:
    """Atomically finalize one clean publication into ``reviewing``.

    One short composed transaction: the approving Profile's
    account-operational barrier first, then the exact Owner scope, the Task
    re-locked and re-proven current under the expected token, the identical
    approved historical gate and its OWNER-attributed audit re-derived, and
    the canonical PR re-read from persistence — the publication result never
    overrides what is durable. The review-eligibility guard is proved from
    the persisted row before the ``waiting_for_owner → reviewing``
    transition with a rotated token and the one ``pr_created`` audit commit
    or roll back together. A ``_StalePublicationGuard`` escapes with no
    partial state; the caller records the recoverable block. Dispatching
    the review belongs exclusively to E9; this finalize never starts it.
    """
    with composed_transaction(pool) as transaction_pool:
        require_account_operational(transaction_pool, profile_id=subjects.approving_profile_id)
        require_workspace_task(
            transaction_pool,
            profile_id=subjects.approving_profile_id,
            workspace_id=workspace_id,
            task_id=task_id,
        )
        locked = task_records.get_task_for_update(transaction_pool, task_id)
        if locked is None or locked.workspace_id != workspace_id:
            raise NotFoundError("the requested task is not available in this workspace")
        task = require_current_task(
            transaction_pool,
            workspace_id=workspace_id,
            task_id=task_id,
            expected_state_token=expected_state_token,
        )
        if task.status is not TaskStatus.WAITING_FOR_OWNER:
            raise ConflictError(
                "the addressed task is not waiting on its owner gate; the "
                "publication finalization is stale"
            )
        if task.current_owner_gate_id is not None:
            raise ConflictError(
                "the task carries a current owner gate; the publication finalization is stale"
            )
        if task.canonical_feature_branch != subjects.canonical_branch:
            raise StaleOperationError(
                "the task's canonical branch moved on; the publication is stale"
            )
        gate = gate_records.get_owner_gate(transaction_pool, owner_gate_id=owner_gate_id)
        if gate is None or gate.task_id != task.id or gate.workspace_id != workspace_id:
            raise NotFoundError("the requested owner gate is not available for this task")
        approved_head_sha = subjects.gate.subject_head_sha
        if approved_head_sha is None:
            # Narrowed by admission; kept explicit for fail-closed safety.
            raise ConflictError(
                "the pr authorization gate does not carry the exact head-sha subject"
            )
        if (
            gate.gate_type is not OwnerGateType.PR_AUTHORIZATION
            or gate.status is not OwnerGateStatus.APPROVED
            or gate.subject_head_sha != approved_head_sha
        ):
            raise StaleOperationError(
                "the approved pr authorization gate moved on; the publication is stale"
            )
        approving_profile_id = _resolve_approved_resolution_audit(
            transaction_pool, workspace_id=workspace_id, owner_gate_id=owner_gate_id
        )
        if approving_profile_id != subjects.approving_profile_id:
            raise StaleOperationError(
                "the approving profile moved on during publication; the publication is stale"
            )
        pull_request = pull_request_records.get_task_pull_request_for_task(
            transaction_pool, task_id=task_id
        )
        if pull_request is None:
            raise ConflictError(
                "the task's canonical pull request record is missing; the "
                "publication cannot be finalized"
            )
        if expected_github_pr_id is not None and (
            pull_request.github_pr_id != expected_github_pr_id
        ):
            raise ConflictError(
                "the persisted canonical pull request does not match the "
                "publication result; canonical records are never replaced"
            )
        _require_review_eligible_pull_request(
            pull_request=pull_request,
            task=task,
            base_ref=subjects.base_ref,
        )
        if pull_request.head_sha != approved_head_sha:
            # Reconciled head-SHA drift — observed during the authoritative
            # continuation reconciliation or arising between the primitive's
            # success and this finalization — is recoverable stale
            # publication authority: it routes through the guard so the Task
            # is blocked recoverably where still current and mutated nowhere
            # when it has already moved on. The canonical record is never
            # recreated or replaced. The eligibility classification above
            # deliberately precedes this check (issue #229): a confirmed
            # closed-unmerged canonical PR is the externally established
            # terminal condition and must never be masked as a stale
            # publication subject.
            raise _StalePublicationGuard(
                reason=TaskBlockReason.STALE_OPERATION,
                context={
                    "authorized_head_sha": approved_head_sha,
                    "observed_head_sha": pull_request.head_sha,
                },
            )
        reviewing = task_records.update_task_status(
            transaction_pool,
            task_id,
            expected_state_token=task.state_token,
            status=TaskStatus.REVIEWING,
        )
        if reviewing is None:
            raise StaleOperationError(
                "the task could not move to reviewing; the publication is stale"
            )
        event_coordination.record_pr_created_event(
            transaction_pool,
            workspace_id=workspace_id,
            task_id=task_id,
            task_pull_request_id=pull_request.id,
            actor=_OPENORC_ACTOR,
        )
        return TaskPrPublished(task=reviewing, pull_request=pull_request)


def _finalize_or_block(
    pool: DatabasePool,
    *,
    subjects: _PublicationSubjects,
    expected_github_pr_id: int | None,
    workspace_id: UUID,
    task_id: UUID,
    owner_gate_id: UUID,
    expected_state_token: UUID,
) -> TaskPrPublished:
    """Finalize the clean publication, or record the recoverable stale block.

    A review-eligibility guard failure rolls the finalize back with no
    partial state; where the Task remains current at the expected token the
    reason-specific ``TaskBlock`` (``stale_operation``) is recorded
    atomically with the ``blocked`` transition and the ``task_blocked``
    audit, and the invocation surfaces the typed stale outcome. The
    confirmed closed-unmerged branch is the exception (issue #229): it is
    the externally established terminal condition, so instead of a BLOCKED
    result the trusted GitHub-observation cancellation entrypoint applies
    the terminal consequence after the rejecting transaction has rolled
    back. A confirmed merge refuses with zero durable mutation — its
    completion belongs to the later recovery layer.
    """
    try:
        return _finalize_published_task_pr(
            pool,
            subjects=subjects,
            expected_github_pr_id=expected_github_pr_id,
            workspace_id=workspace_id,
            task_id=task_id,
            owner_gate_id=owner_gate_id,
            expected_state_token=expected_state_token,
        )
    except _StalePublicationGuard as guard:
        if guard.reason is TaskBlockReason.PR_CLOSED_UNMERGED:
            # The confirmed closed-unmerged canonical PR is the externally
            # established terminal condition (issue #229): the rejecting
            # finalize transaction has already rolled back with no partial
            # state, and the trusted GitHub-observation cancellation
            # entrypoint applies the terminal consequence instead of a
            # recoverable BLOCKED result. The entrypoint re-proves the
            # durable evidence under its own locks and fails closed on any
            # conflict.
            task_cancellation.cancel_task_for_confirmed_pr_closure(
                pool,
                workspace_id=workspace_id,
                task_id=task_id,
            )
            raise StaleOperationError(
                "the canonical pull request is confirmed closed-unmerged; the "
                "task is terminally cancelled"
            ) from guard
        _block_stale_publication(
            pool,
            profile_id=subjects.approving_profile_id,
            workspace_id=workspace_id,
            task_id=task_id,
            expected_state_token=expected_state_token,
            reason=guard.reason,
            context=guard.context,
        )
        raise StaleOperationError(
            "the canonical pull request is not review-eligible; the task is "
            "blocked pending owner recovery"
        ) from guard


# --- the one PR_COMPOSE interaction --------------------------------------------------


def _compose_pr_with_producer(
    pool: DatabasePool,
    runtime_factory: ProducerRuntimeAttachmentFactory,
    *,
    admitted: _PublicationSubjects,
    workspace_id: UUID,
    task_id: UUID,
    owner_gate_id: UUID,
    expected_state_token: UUID,
) -> PrResult:
    """The one PR_COMPOSE interaction on the exact pre-existing binding.

    The deferred attachment seam is entered, the same publication subjects
    are revalidated after acquisition and before realization (R1), and one
    ``realize_control`` sends the rendered ``pr_compose`` prose expecting the
    ``pr_result`` family through the shared #67 → #65 formal funnel. Uncertain
    outcomes are never resent; known runtime/protocol failures are distinct
    and never reclassified. No durable effect exists on any failure.
    """
    session = admitted.producer_session
    if session is None or session.external_session_id is None:
        # Narrowed by admission; kept explicit for fail-closed safety.
        raise ConflictError("the producer session binding carries no external session identity")
    external_session_id = session.external_session_id
    with ExitStack() as attachment_stack:
        try:
            runtime = attachment_stack.enter_context(runtime_factory())
        except ExternalOperationUncertainError:
            raise
        except ExternalOperationFailedError:
            raise
        # R1: authority revalidation after acquisition, before realization.
        with composed_transaction(pool) as transaction_pool:
            _require_unchanged_publication_subjects(
                transaction_pool,
                admitted=admitted,
                workspace_id=workspace_id,
                task_id=task_id,
                owner_gate_id=owner_gate_id,
                expected_state_token=expected_state_token,
            )
        interaction = PrComposeInteraction(
            repository_full_name=admitted.repository_full_name,
            issue_number=admitted.task.github_issue_number,
            guidance=admitted.guidance,
        )
        try:
            realized = runtime.realize_control(
                external_session_id,
                interaction,
                message=interaction.render(),
                expected_family=PR_RESULT_FAMILY,
            )
        except AgentRuntimeUncertainOutcomeError as error:
            raise ExternalOperationUncertainError(
                "the outcome of the pr composition is unknown; the compose "
                "request may have been delivered and is never resent"
            ) from error
        except AgentRuntimeError as error:
            raise ExternalOperationFailedError(
                "the pr composition failed as a known runtime or protocol condition"
            ) from error
        if not isinstance(realized, PrResult):
            raise ExternalOperationFailedError(
                "the pr composition did not produce a validated pr_result"
            )
        return realized


# --- the public E8 orchestration -----------------------------------------------------


def compose_and_publish_task_pr(
    pool: DatabasePool,
    github: GitHubAppClient,
    user_token_resolver: ProfileUserAccessTokenResolver,
    producer_runtime_factory: ProducerRuntimeAttachmentFactory,
    *,
    profile_id: UUID,
    workspace_id: UUID,
    task_id: UUID,
    owner_gate_id: UUID,
    expected_state_token: UUID,
) -> TaskPrPublished:
    """Compose the Producer's PR presentation and publish the canonical PR.

    The next proven manual Producer handoff after E7's durable approval:
    admission re-derives the persisted approved ``PR_AUTHORIZATION`` gate,
    its exact head-SHA subject, and the OWNER-attributed approving Profile
    from durable state; the fresh path runs the one ``PR_COMPOSE``
    interaction on the exact pre-existing READY Producer binding, normalizes
    the deterministic closing reference, and invokes the existing race-safe
    publication primitive exactly once under the approving Profile's GitHub
    user-to-server authorization; the narrow continuation path reconciles
    this module's own interrupted publication authoritatively instead. Only
    a clean ``PUBLISHED`` result — or a still-valid reconciled record —
    finalizes ``waiting_for_owner → reviewing`` with the one ``pr_created``
    audit; every other outcome fails closed truthfully, blocking the Task
    recoverably where it is still current and mutating nothing when it has
    already moved on. No Reviewer dispatch, no second create, no adoption,
    and no database transaction spanning the runtime or GitHub calls.
    """
    _require_uuid_command(profile_id, "profile_id")
    _require_uuid_command(workspace_id, "workspace_id")
    _require_uuid_command(task_id, "task_id")
    _require_uuid_command(owner_gate_id, "owner_gate_id")
    _require_uuid_command(expected_state_token, "expected_state_token")
    runtime_factory = _require_producer_factory(producer_runtime_factory)
    with application_span(_SERVICE_TRACER_SCOPE, _COMPOSE_SPAN_NAME) as span:
        annotate_span(
            span,
            operation=_COMPOSE_SPAN_NAME,
            workspace_id=str(workspace_id),
            task_id=str(task_id),
        )
        # --- one short admission transaction (DB-only) ---
        with composed_transaction(pool) as transaction_pool:
            subjects = _admit_publication_subjects(
                transaction_pool,
                profile_id=profile_id,
                workspace_id=workspace_id,
                task_id=task_id,
                owner_gate_id=owner_gate_id,
                expected_state_token=expected_state_token,
            )
        if subjects.continuation_pull_request is not None:
            # --- the narrow continuation: authoritative reconciliation with
            # no transaction open, then the same atomic finalization. The
            # composition and the create are never repeated. ---
            task_pull_request_reconciliation.reconcile_task_pull_request(
                pool, github, workspace_id=workspace_id, task_id=task_id
            )
            return _finalize_or_block(
                pool,
                subjects=subjects,
                expected_github_pr_id=None,
                workspace_id=workspace_id,
                task_id=task_id,
                owner_gate_id=owner_gate_id,
                expected_state_token=expected_state_token,
            )

        # --- the one PR_COMPOSE interaction (external, no transaction) ---
        approved_head_sha = subjects.gate.subject_head_sha
        if approved_head_sha is None:
            # Narrowed by admission; kept explicit for fail-closed safety.
            raise ConflictError(
                "the pr authorization gate does not carry the exact head-sha subject"
            )
        pr_result = _compose_pr_with_producer(
            pool,
            runtime_factory,
            admitted=subjects,
            workspace_id=workspace_id,
            task_id=task_id,
            owner_gate_id=owner_gate_id,
            expected_state_token=expected_state_token,
        )
        # --- post-interaction revalidation before any publication ---
        with composed_transaction(pool) as transaction_pool:
            _require_unchanged_publication_subjects(
                transaction_pool,
                admitted=subjects,
                workspace_id=workspace_id,
                task_id=task_id,
                owner_gate_id=owner_gate_id,
                expected_state_token=expected_state_token,
            )
        # --- deterministic closing reference + the one B8 invocation ---
        command = pull_request_publication.PublicationCommand(
            workspace_id=workspace_id,
            task_id=task_id,
            profile_id=subjects.approving_profile_id,
            authorized_head_sha=approved_head_sha,
            canonical_branch=subjects.canonical_branch,
            base_ref=subjects.base_ref,
            state_token=expected_state_token,
            title=pr_result.title,
            body=_normalized_pr_body(
                pr_result,
                repository_full_name=subjects.repository_full_name,
                issue_number=subjects.task.github_issue_number,
            ),
        )
        publication = pull_request_publication.publish_task_pull_request(
            pool, github, user_token_resolver, command
        )
        # --- outcome handling: finalize clean, block recoverably, or fail
        # closed truthfully. The primitive's typed failures and uncertain
        # outcomes propagate unchanged and are never replayed. ---
        if publication.outcome is TaskPullRequestPublicationOutcome.PUBLISHED:
            published_pr = publication.pull_request
            return _finalize_or_block(
                pool,
                subjects=subjects,
                expected_github_pr_id=(
                    published_pr.github_pr_id if published_pr is not None else None
                ),
                workspace_id=workspace_id,
                task_id=task_id,
                owner_gate_id=owner_gate_id,
                expected_state_token=expected_state_token,
            )
        if publication.outcome is TaskPullRequestPublicationOutcome.PREFLIGHT_STALE:
            # No PR was created by this invocation; the approved head can no
            # longer be published. No SHA is silently reauthorized.
            _block_stale_publication(
                pool,
                profile_id=subjects.approving_profile_id,
                workspace_id=workspace_id,
                task_id=task_id,
                expected_state_token=expected_state_token,
                reason=TaskBlockReason.STALE_OPERATION,
                context={"authorized_head_sha": approved_head_sha},
            )
            raise StaleOperationError(
                "the published head no longer matches the approved authorization; "
                "the task is blocked pending owner recovery"
            )
        created = publication.pull_request
        if created is None:
            # The post-create classifications always carry the persisted
            # record; narrowed explicitly for fail-closed safety.
            raise StaleOperationError(
                "the publication outcome is stale and carries no canonical record"
            )
        observed_head_context = (
            {"observed_head_sha": created.head_sha}
            if publication.outcome is TaskPullRequestPublicationOutcome.POST_CREATE_HEAD_MISMATCH
            else {"github_pull_request_id": str(created.github_pr_id)}
        )
        # The created PR is an external fact that is never erased: where the
        # Task is still current the recoverable block records it, and where
        # it already moved on the block composition applies nothing.
        _block_stale_publication(
            pool,
            profile_id=subjects.approving_profile_id,
            workspace_id=workspace_id,
            task_id=task_id,
            expected_state_token=expected_state_token,
            reason=TaskBlockReason.STALE_OPERATION,
            context={"authorized_head_sha": approved_head_sha, **observed_head_context},
        )
        raise StaleOperationError(
            "the publication's durable authority moved on after creation; the "
            "task is blocked pending owner recovery"
        )
