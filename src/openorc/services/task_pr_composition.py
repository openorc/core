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

import logging
import re
from collections.abc import Callable
from contextlib import AbstractContextManager, ExitStack
from dataclasses import dataclass
from typing import TYPE_CHECKING, cast
from uuid import UUID

from openorc.adapters.agent_runtime.contract import AgentRuntimeAdapter
from openorc.adapters.agent_runtime.detached import AgentTurnAdmission
from openorc.adapters.agent_runtime.errors import (
    AgentRuntimeError,
    AgentRuntimeInterruptedTurnError,
    AgentRuntimeProtocolFailureError,
    AgentRuntimeTransportError,
    AgentRuntimeUncertainOutcomeError,
)
from openorc.adapters.github import GitHubAppClient
from openorc.domain.blocking_cases import Level3BlockingCondition
from openorc.domain.blocks import TaskBlockReason
from openorc.domain.connections import WorkflowRole
from openorc.domain.detached_turns import TaskDetachedTurnReceipt
from openorc.domain.events import WorkflowEvent, WorkflowEventActor, WorkflowEventType
from openorc.domain.gates import OwnerGate, OwnerGateStatus, OwnerGateType
from openorc.domain.operational_failures import OriginalAttemptKind
from openorc.domain.pending_interactions import PendingTaskRuntimeInteractionOperation
from openorc.domain.pull_requests import TaskPullRequest, TaskPullRequestState
from openorc.domain.send_claims import TaskRuntimeSendClaim
from openorc.domain.sessions import TaskAgentSession, TaskSessionLifecycleStatus
from openorc.domain.tasks import Task, TaskStatus
from openorc.observability import annotate_span, application_span
from openorc.persistence import events as event_records
from openorc.persistence import gates as gate_records
from openorc.persistence import ownership as ownership_records
from openorc.persistence import pull_requests as pull_request_records
from openorc.persistence import sessions as session_records
from openorc.persistence import tasks as task_records
from openorc.persistence.detached_turns import refuse_task_detached_turn_receipt
from openorc.persistence.pool import DatabasePool
from openorc.protocol.interaction import PrComposeInteraction
from openorc.protocol.models import PR_RESULT_FAMILY, PrResult
from openorc.services import (
    event_coordination,
    pull_request_publication,
    task_cancellation,
    task_pull_request_reconciliation,
)
from openorc.services import (
    task_detached_turn_correlation as turn_correlation,
)
from openorc.services.errors import (
    ApplicationError,
    AuthorizationError,
    ConflictError,
    ExternalOperationFailedError,
    ExternalOperationRejectedError,
    ExternalOperationUncertainError,
    InvalidCommandError,
    NotFoundError,
    StaleOperationError,
)
from openorc.services.github_installation_route import (
    resolve_system_repository_installation_route,
)
from openorc.services.github_user_authorization import ProfileUserAccessTokenResolver
from openorc.services.operation_attempt_evidence import InvocationAttemptEvidence
from openorc.services.profile_lifecycle_guard import require_account_operational
from openorc.services.pull_request_publication import TaskPullRequestPublicationOutcome
from openorc.services.task_abort import settle_observed_task_interrupt
from openorc.services.task_completion_handoff import DetachedTurnCompletionHandoff
from openorc.services.task_detached_turn_observation import (
    ObservationRegistrationRefusedError,
)
from openorc.services.task_exceptional_blocking import install_exceptional_block_gate
from openorc.services.task_level3_blocking_catalog import classify_blocking_case
from openorc.services.task_plan_production import PlanProductionObservationRegistrar
from openorc.services.task_runtime_send_claims import (
    RUNTIME_PRE_EFFECT_FAILURE_KIND,
    claim_pr_compose_send,
    record_send_claim_known_non_delivery,
    settle_pr_compose_send_claim,
)
from openorc.services.task_subject_guards import require_current_task
from openorc.services.transaction_composition import composed_transaction

if TYPE_CHECKING:
    from openorc.services.task_terminal_finalization import TerminalFinalizationSubmission
from openorc.services.workspace_authorization import require_workspace_task

__all__ = [
    "DispatchedPrComposition",
    "ProducerRuntimeAttachmentFactory",
    "SettledTaskPrPublication",
    "TaskPrPublished",
    "compose_and_publish_task_pr",
    "dispatch_pr_compose_detached",
    "publish_settled_task_pr_composition",
    "settle_observed_pr_compose_result",
]

_SERVICE_TRACER_SCOPE = "openorc.services.task_pr_composition"
_COMPOSE_SPAN_NAME = "task_pr_composition.compose_and_publish_task_pr"
_DISPATCH_COMPOSE_SPAN_NAME = "task_pr_composition.dispatch_pr_compose_detached"
_PUBLISH_SETTLED_SPAN_NAME = "task_pr_composition.publish_settled_task_pr_composition"

logger = logging.getLogger(__name__)

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
    task, gate, existing_pr = _revalidate_publication_subjects(
        transaction_pool,
        workspace_id=workspace_id,
        task_id=task_id,
        owner_gate_id=owner_gate_id,
        expected_state_token=expected_state_token,
    )
    approving_profile_id = _resolve_approved_resolution_audit(
        transaction_pool, workspace_id=workspace_id, owner_gate_id=owner_gate_id
    )
    if approving_profile_id != profile_id:
        raise AuthorizationError(
            "the pr authorization was approved by a different profile; "
            "publication is not available to this caller"
        )
    return _resolve_publication_route_subjects(
        transaction_pool,
        approving_profile_id=approving_profile_id,
        workspace_id=workspace_id,
        task=task,
        gate=gate,
        existing_pr=existing_pr,
    )


def _revalidate_publication_subjects(
    transaction_pool: DatabasePool,
    *,
    workspace_id: UUID,
    task_id: UUID,
    owner_gate_id: UUID,
    expected_state_token: UUID,
) -> tuple[Task, OwnerGate, TaskPullRequest | None]:
    """The shared locked publication-subject reproof (#337).

    Under the Task row lock the Task must be current, non-archived,
    ``waiting_for_owner``, at the expected token, with no current OwnerGate
    (E7 intentionally cleared the pointer on approval) and a bound canonical
    feature branch. The addressed historical gate must belong to this
    Task/Workspace, be the durably ``APPROVED`` ``PR_AUTHORIZATION`` gate
    carrying exactly its head-SHA subject. A canonical ``TaskPullRequest``
    carrying the exact approved head routes the invocation to the narrow
    continuation path; a record carrying any other head is a conflict
    (unrelated external PRs are never adopted). The caller-checked admission
    (issue #83) and the settled-composition continuation (issue #337) share
    this reproof.
    """
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
    return task, gate, existing_pr


def _resolve_publication_route_subjects(
    transaction_pool: DatabasePool,
    *,
    approving_profile_id: UUID,
    workspace_id: UUID,
    task: Task,
    gate: OwnerGate,
    existing_pr: TaskPullRequest | None,
) -> _PublicationSubjects:
    """The shared post-admission route/Producer/guidance resolution.

    The account-operational barrier is re-proved under the resolved
    approving Profile — publication's only identity — then the routed
    Repository presentation context is resolved (the workflow base comes
    from the routed Repository record, never from the Producer). The
    canonical record of this module's own interrupted publication routes the
    invocation to the narrow continuation path; the fresh path additionally
    resolves the original READY Producer binding and the current Workspace
    guidance. The caller-checked admission (issue #83) and the detached
    settlement's subject re-derivation (issue #337) share this resolution.
    """
    require_account_operational(transaction_pool, profile_id=approving_profile_id)
    require_workspace_task(
        transaction_pool,
        profile_id=approving_profile_id,
        workspace_id=workspace_id,
        task_id=task.id,
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
    canonical_branch = task.canonical_feature_branch
    if canonical_branch is None:
        # Narrowed by every subject revalidation; kept explicit for
        # fail-closed safety.
        raise ConflictError(
            "the task carries no bound canonical feature branch; the publication "
            "subject is incomplete"
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
            canonical_branch=canonical_branch,
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
        canonical_branch=canonical_branch,
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
    condition: Level3BlockingCondition,
    reason: TaskBlockReason,
    context: dict[str, str],
) -> Task:
    """Atomically block the Task on stale publication authority.

    One short composed transaction (the established E8 block shape): the
    approving Profile's account-operational barrier is re-proved FIRST (the
    admission check does not remain authoritative across the intervening
    external calls), then the durable stale evidence against the freshly
    locked Task is re-proved, and the F3 exceptional blocking foundation
    (#258) installs — atomically — the ``reason`` TaskBlock, its pending
    Level 3-selected ``BLOCK_RESOLUTION`` OwnerGate (the Type B diagnostic
    fallback since F8e, issue #290) bound to it as the only subject (F8b,
    issue #287),
    the ``waiting_for_owner → blocked`` transition with one rotated token,
    and the ``task_blocked`` / ``owner_gate_created`` audits under the
    truthful OPENORC actor — commit or roll back together. The publication
    gate is already approved at every call site, so the NULL-safe
    current-gate pointer expectation is the Task's locked (absent)
    pointer. The caller's ``reason``/``context`` carry the exact
    Owner-authorized head, the originating PR_AUTHORIZATION gate identity,
    and every reconciled PR/head fact available at its site (issue #280) —
    derived only from already-proven durable state, never manufactured. A
    Task that already moved on applies nothing and raises
    ``StaleOperationError`` — no PR is created here, no SHA is silently
    reauthorized, and the block never duplicates itself. Recovery from the
    blocked state belongs to later layers (E10, #85).
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
        # The F3 exceptional blocking foundation (#258): one atomic block,
        # gate, transition, token rotation, and audit pair. The stale
        # publication authority is declared to the Level 3 blocking-case
        # catalog (F8b, issue #287) with the site's proven evidence — the
        # authoritative policy, never this caller, selects the exceptional
        # OwnerGate case — and the installer's guarded currentness re-proof
        # is the durable backstop; a raced or stale caller commits nothing
        # at all.
        gate_type = classify_blocking_case(condition, context, reason)
        installation = install_exceptional_block_gate(
            transaction_pool,
            workspace_id=workspace_id,
            task_id=task_id,
            expected_state_token=expected_state_token,
            expected_status=task.status,
            expected_current_owner_gate_id=task.current_owner_gate_id,
            gate_type=gate_type,
            block_reason=reason,
            block_context=context,
            actor=_OPENORC_ACTOR,
        )
        return installation.task


# --- the review-eligibility guard ----------------------------------------------------


def _require_review_eligible_pull_request(
    *,
    pull_request: TaskPullRequest,
    task: Task,
    base_ref: str,
    authorized_head_sha: str,
    owner_gate_id: UUID,
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

    The stale-publication guard contexts carry the exact Owner-authorized
    head and originating PR_AUTHORIZATION gate identity (both already
    proven in the finalize composition) plus the reconciled PR evidence
    available on the persisted row, so the F7a TaskBlock preserves the
    authorization/mismatch facts later guarded recovery needs (issue
    #280). The closed-unmerged terminal context is unchanged: it feeds
    #229's cancellation, not a TaskBlock.
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
                "authorized_head_sha": authorized_head_sha,
                "owner_gate_id": str(owner_gate_id),
                "observed_head_ref": pull_request.head_ref,
                "observed_head_sha": pull_request.head_sha,
            },
        )
    if pull_request.base_ref != base_ref:
        raise _StalePublicationGuard(
            reason=TaskBlockReason.STALE_OPERATION,
            context={
                "pull_request_id": str(pull_request.id),
                "github_pr_number": str(pull_request.github_pr_number),
                "authorized_head_sha": authorized_head_sha,
                "authorized_base_ref": base_ref,
                "owner_gate_id": str(owner_gate_id),
                "observed_head_sha": pull_request.head_sha,
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
            authorized_head_sha=approved_head_sha,
            owner_gate_id=owner_gate_id,
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
            # publication subject. The context preserves the exact
            # Owner-authorized head, the originating gate identity, and the
            # canonical PR identity (issue #280).
            raise _StalePublicationGuard(
                reason=TaskBlockReason.STALE_OPERATION,
                context={
                    "pull_request_id": str(pull_request.id),
                    "github_pr_number": str(pull_request.github_pr_number),
                    "authorized_head_sha": approved_head_sha,
                    "owner_gate_id": str(owner_gate_id),
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
    terminal_finalization_submission: TerminalFinalizationSubmission | None = None,
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
                terminal_finalization_submission=terminal_finalization_submission,
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
            condition=Level3BlockingCondition.PR_PUBLICATION_AUTHORITY_MISMATCH,
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
    send_claim: TaskRuntimeSendClaim,
    workspace_id: UUID,
    task_id: UUID,
    owner_gate_id: UUID,
    expected_state_token: UUID,
) -> PrResult:
    """The one PR_COMPOSE interaction on the exact pre-existing binding.

    The deferred attachment seam is entered, the same publication subjects
    are revalidated after acquisition and before realization (R1), and one
    ``realize_control`` sends the rendered ``pr_compose`` prose expecting the
    ``pr_result`` family through the shared #67 → #65 formal funnel. The
    exact subject was durably claimed before this effect (issue #87):
    uncertain outcomes are never resent; a transport or protocol failure can
    arise only after the send has done work, so it is an unresolved outcome
    — never a proven non-delivery; a positively proven pre-effect failure
    records the truthful known non-delivery on the claim and never
    reclassifies. No durable effect exists on any failure.
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
        except AgentRuntimeInterruptedTurnError as error:
            settle_observed_task_interrupt(
                pool, runtime, task_id=task_id, external_session_id=external_session_id
            )
            logger.warning(
                "the pr composition ended with the runtime interrupting the send; the "
                "compose dispatch claim stays unresolved"
            )
            raise ExternalOperationUncertainError(
                "the pr composition's send was interrupted by the runtime; the compose "
                "dispatch is never resent"
            ) from error
        except AgentRuntimeUncertainOutcomeError as error:
            raise ExternalOperationUncertainError(
                "the outcome of the pr composition is unknown; the compose "
                "request may have been delivered and is never resent"
            ) from error
        except (AgentRuntimeTransportError, AgentRuntimeProtocolFailureError) as error:
            logger.warning(
                "the pr composition ended with a post-send runtime failure; the "
                "compose dispatch claim stays unresolved"
            )
            raise ExternalOperationUncertainError(
                "the pr composition's send may have been delivered but its response "
                "could not be interpreted; the compose dispatch is never resent"
            ) from error
        except AgentRuntimeError as error:
            # The remaining normalized taxonomy classes (configuration
            # rejection, runtime unavailability, and a not-live addressed
            # session) are positively proven to precede any effect: the claim
            # records the truthful known non-delivery first.
            record_send_claim_known_non_delivery(
                pool,
                claim=send_claim,
                workspace_id=workspace_id,
                task_id=task_id,
                failure_kind=RUNTIME_PRE_EFFECT_FAILURE_KIND,
            )
            raise ExternalOperationFailedError(
                "the pr composition failed as a known non-delivery before any effect"
            ) from error
        if not isinstance(realized, PrResult):
            raise ExternalOperationUncertainError(
                "the pr composition ended without a validated pr_result after "
                "possible composition work; the compose dispatch is never resent"
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
    attempt_evidence: InvocationAttemptEvidence | None = None,
    terminal_finalization_submission: TerminalFinalizationSubmission | None = None,
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
                terminal_finalization_submission=terminal_finalization_submission,
            )

        # --- the durable pre-send dispatch claim (database only) ---
        approved_head_sha = subjects.gate.subject_head_sha
        if approved_head_sha is None:
            # Narrowed by admission; kept explicit for fail-closed safety.
            raise ConflictError(
                "the pr authorization gate does not carry the exact head-sha subject"
            )
        producer_session = subjects.producer_session
        if producer_session is None:
            # Narrowed by admission (the fresh path always carries the
            # exact READY Producer binding); kept explicit for fail-closed
            # safety.
            raise ConflictError("the fresh pr composition carries no producer session binding")
        # Committed strictly before any runtime effect on the fresh
        # composition path for the exact approved gate/head: a replayed or
        # raced continuation never composes again, whatever the prior
        # attempt's outcome. The interrupted-publication continuation never
        # reaches this claim.
        send_claim = claim_pr_compose_send(
            pool,
            workspace_id=workspace_id,
            task_id=task_id,
            expected_state_token=expected_state_token,
            owner_gate=subjects.gate,
            producer=producer_session,
        )
        # --- the one PR_COMPOSE interaction (external, no transaction) ---
        if attempt_evidence is not None:
            # The F5b coordinator's handoff fact: this invocation's committed
            # PR_COMPOSE dispatch claim identity (#262).
            attempt_evidence.record_attempt(
                family=PendingTaskRuntimeInteractionOperation.PR_COMPOSE_DISPATCH,
                original_attempt_kind=OriginalAttemptKind.RUNTIME_SEND_CLAIM,
                original_attempt_id=send_claim.id,
                phase="pr_compose_dispatch",
            )
        pr_result = _compose_pr_with_producer(
            pool,
            runtime_factory,
            admitted=subjects,
            send_claim=send_claim,
            workspace_id=workspace_id,
            task_id=task_id,
            owner_gate_id=owner_gate_id,
            expected_state_token=expected_state_token,
        )
        # --- post-interaction revalidation + claim settlement before any
        # publication: the validated pr_result completed the composition, so
        # the send claim settles exactly once in the same short transaction —
        # without pretending GitHub publication has completed. ---
        with composed_transaction(pool) as transaction_pool:
            _require_unchanged_publication_subjects(
                transaction_pool,
                admitted=subjects,
                workspace_id=workspace_id,
                task_id=task_id,
                owner_gate_id=owner_gate_id,
                expected_state_token=expected_state_token,
            )
            settle_pr_compose_send_claim(
                transaction_pool,
                claim=send_claim,
                workspace_id=workspace_id,
                task_id=task_id,
                subject_head_sha=approved_head_sha,
            )
        # --- deterministic closing reference + the one B8 invocation + the
        # outcome handling: finalize clean, block recoverably, or fail closed
        # truthfully (shared with the detached continuation, issue #337). ---
        return _publish_and_finalize(
            pool,
            github,
            user_token_resolver,
            subjects=subjects,
            pr_result=pr_result,
            workspace_id=workspace_id,
            task_id=task_id,
            owner_gate_id=owner_gate_id,
            expected_state_token=expected_state_token,
            terminal_finalization_submission=terminal_finalization_submission,
        )


def _publish_and_finalize(
    pool: DatabasePool,
    github: GitHubAppClient,
    user_token_resolver: ProfileUserAccessTokenResolver,
    *,
    subjects: _PublicationSubjects,
    pr_result: PrResult,
    workspace_id: UUID,
    task_id: UUID,
    owner_gate_id: UUID,
    expected_state_token: UUID,
    terminal_finalization_submission: TerminalFinalizationSubmission | None,
) -> TaskPrPublished:
    """The bounded publication consequence both compositions share (#337).

    The deterministic closing reference normalizes the validated
    presentation, the one B8 invocation publishes through the existing
    race-safe primitive exactly once under the approving Profile's GitHub
    user-to-server authorization, and the outcome handling finalizes clean,
    blocks recoverably, or fails closed truthfully. The primitive's typed
    failures and uncertain outcomes propagate unchanged and are never
    replayed. No database transaction is ever held across the GitHub call.
    """
    approved_head_sha = subjects.gate.subject_head_sha
    if approved_head_sha is None:
        # Narrowed by every subject admission; kept explicit for fail-closed
        # safety.
        raise ConflictError("the pr authorization gate does not carry the exact head-sha subject")
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
    if publication.outcome is TaskPullRequestPublicationOutcome.PUBLISHED:
        published_pr = publication.pull_request
        return _finalize_or_block(
            pool,
            subjects=subjects,
            expected_github_pr_id=(published_pr.github_pr_id if published_pr is not None else None),
            workspace_id=workspace_id,
            task_id=task_id,
            owner_gate_id=owner_gate_id,
            expected_state_token=expected_state_token,
            terminal_finalization_submission=terminal_finalization_submission,
        )
    if publication.outcome is TaskPullRequestPublicationOutcome.PREFLIGHT_STALE:
        # No PR was created by this invocation; the approved head can no
        # longer be published. No SHA is silently reauthorized, and no
        # canonical PR record exists to cite — the block preserves the
        # exact authorized head and the originating gate identity
        # (issue #280) without manufacturing a PR identity.
        _block_stale_publication(
            pool,
            profile_id=subjects.approving_profile_id,
            workspace_id=workspace_id,
            task_id=task_id,
            expected_state_token=expected_state_token,
            condition=Level3BlockingCondition.PR_PUBLICATION_AUTHORITY_MISMATCH,
            reason=TaskBlockReason.STALE_OPERATION,
            context={
                "authorized_head_sha": approved_head_sha,
                "owner_gate_id": str(owner_gate_id),
            },
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
    # it already moved on the block composition applies nothing. The
    # context preserves the exact authorized head, the originating gate
    # identity, and the persisted canonical PR identity alongside the
    # outcome-specific observed evidence (issue #280).
    _block_stale_publication(
        pool,
        profile_id=subjects.approving_profile_id,
        workspace_id=workspace_id,
        task_id=task_id,
        expected_state_token=expected_state_token,
        condition=Level3BlockingCondition.PR_PUBLICATION_AUTHORITY_MISMATCH,
        reason=TaskBlockReason.STALE_OPERATION,
        context={
            "authorized_head_sha": approved_head_sha,
            "owner_gate_id": str(owner_gate_id),
            "pull_request_id": str(created.id),
            "github_pr_number": str(created.github_pr_number),
            **observed_head_context,
        },
    )
    raise StaleOperationError(
        "the publication's durable authority moved on after creation; the "
        "task is blocked pending owner recovery"
    )


# --- the detached PR_COMPOSE dispatch and settlement (issue #337) --------------

# Safe refusal-kind tokens for a receipt's durable refusal outcome after a
# proven pre-effect detached-dispatch failure (no exception text or error
# content ever enters durable state).
_REGISTRATION_REFUSAL_KIND = "observation_registration_refused"
_ADMISSION_REFUSAL_KIND = "admission_refused"


@dataclass(frozen=True, slots=True)
class SettledTaskPrPublication:
    """The durable publication facts of the settled-composition continuation.

    ``task`` is the post-write Task (``reviewing`` under its rotated
    ``state_token``), ``pull_request`` the one canonical TaskPullRequest,
    and ``approving_profile_id`` the re-derived approving Profile the F1
    PR-review dispatch addresses.
    """

    task: Task
    pull_request: TaskPullRequest
    approving_profile_id: UUID


@dataclass(frozen=True, slots=True)
class DispatchedPrComposition:
    """The typed detached PR_COMPOSE dispatch outcome (issue #337).

    The fresh path's authoritative facts exactly as the durable state reads
    after the dispatch: the post-claim Task (its token unrotated by the
    claim), the committed one-shot send claim, the claimed detached-turn
    receipt companion, and the exact Producer binding the canonical
    PR_COMPOSE instruction was admitted on. No runtime result and no
    validated ``pr_result`` exists yet: the independently observed completion
    owns the settlement, and the ONE post-commit completion handoff owns the
    bounded publication and the F1 PR-review submission.

    The narrow continuation path (this module's own interrupted publication)
    dispatches no turn at all: it reconciles and finalizes inline — the F1
    invocation's own bounded post-claim consequence, never a second
    composition — and carries exactly the finalized ``published`` result
    with ``None`` claim, receipt, and Producer session.
    """

    task: Task
    send_claim: TaskRuntimeSendClaim | None
    receipt: TaskDetachedTurnReceipt | None
    producer_session: TaskAgentSession | None
    published: TaskPrPublished | None


def _refuse_pr_compose_dispatch_quietly(
    pool: DatabasePool,
    receipt: TaskDetachedTurnReceipt,
    send_claim: TaskRuntimeSendClaim,
    *,
    refusal_kind: str,
) -> None:
    """Mark a claimed PR_COMPOSE receipt refused and its claim non-delivered.

    Best-effort containment after a positively proven pre-effect failure: a
    persistence failure here must never mask the typed refusal the caller
    raises. The claimed receipt stays the honest unresolved state under the
    accepted v1 recovery bounds and the failure is logged selectively.
    """
    try:
        refused = refuse_task_detached_turn_receipt(
            pool,
            receipt_id=receipt.id,
            task_id=receipt.task_id,
            workspace_id=receipt.workspace_id,
            refusal_kind=refusal_kind,
        )
    except Exception as error:  # noqa: BLE001 - containment, never masking
        logger.warning(
            "the refused pr-composition receipt could not be durably marked (%s); "
            "the claimed receipt stays the honest unresolved state",
            type(error).__name__,
        )
        refused = None
    if refused is None:  # pragma: no cover - a claimed receipt refuses once
        logger.warning(
            "the pr-composition receipt was no longer claimed when its pre-effect "
            "refusal was recorded"
        )
    try:
        record_send_claim_known_non_delivery(
            pool,
            claim=send_claim,
            workspace_id=send_claim.workspace_id,
            task_id=send_claim.task_id,
            failure_kind=RUNTIME_PRE_EFFECT_FAILURE_KIND,
        )
    except Exception as error:  # noqa: BLE001 - containment, never masking
        logger.warning(
            "the refused pr-composition claim could not be durably marked (%s); "
            "the claimed claim stays the honest unresolved state",
            type(error).__name__,
        )


def dispatch_pr_compose_detached(
    pool: DatabasePool,
    github: GitHubAppClient,
    user_token_resolver: ProfileUserAccessTokenResolver,
    producer_runtime_factory: ProducerRuntimeAttachmentFactory,
    *,
    workspace_id: UUID,
    task_id: UUID,
    profile_id: UUID,
    owner_gate_id: UUID,
    expected_state_token: UUID,
    observation_registrar: PlanProductionObservationRegistrar,
    completion_handoff: DetachedTurnCompletionHandoff,
    terminal_finalization_submission: TerminalFinalizationSubmission | None = None,
    attempt_evidence: InvocationAttemptEvidence | None = None,
) -> DispatchedPrComposition:
    """Dispatch the exact approved-head PR_COMPOSE as one detached agent turn.

    The detached counterpart of the ordinary PR composition (issues
    #325/#337): the same E8 admission of the persisted approved
    ``PR_AUTHORIZATION`` gate, its exact head-SHA subject, and the
    OWNER-attributed approving Profile — the same one-shot PR_COMPOSE
    dispatch claim — now committed together with its detached-turn receipt
    companion, followed by the canonical ``PrComposeInteraction`` admitted
    verbatim as one independently completed agent turn on the exact READY
    Producer session. The claim transaction re-proves the full publication
    subject under the Task row lock strictly after the attachment is
    acquired and strictly before the admission (the fresh path's R1
    reproof). No ``realize_control`` and no blocking wait exists in this
    path, and the dispatching worker returns after the acknowledgement
    attach. The independently observed completion settles the validated
    ``pr_result`` through :func:`settle_observed_pr_compose_result`; the ONE
    post-commit completion handoff owns the bounded publication and the
    typed PR-review submission from committed state.

    The narrow continuation path (the Task already carries this module's own
    interrupted publication record) performs no detached turn at all: it
    runs the existing authoritative reconciliation and atomic finalization
    inline and returns the finalized publication for the F1 invocation's
    PR-review submission — the composition and the create are never
    repeated.
    """
    _require_uuid_command(workspace_id, "workspace_id")
    _require_uuid_command(task_id, "task_id")
    _require_uuid_command(profile_id, "profile_id")
    _require_uuid_command(owner_gate_id, "owner_gate_id")
    _require_uuid_command(expected_state_token, "expected_state_token")
    with application_span(_SERVICE_TRACER_SCOPE, _DISPATCH_COMPOSE_SPAN_NAME) as span:
        annotate_span(
            span,
            operation=_DISPATCH_COMPOSE_SPAN_NAME,
            workspace_id=str(workspace_id),
            task_id=str(task_id),
        )

        # --- Phase 1: the one short admission transaction (database only). ---
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
            # The narrow continuation (issue #83): the authoritative
            # reconciliation with no transaction open, then the same atomic
            # finalization-or-block. No model turn exists to detach and no
            # claim is created.
            task_pull_request_reconciliation.reconcile_task_pull_request(
                pool, github, workspace_id=workspace_id, task_id=task_id
            )
            published = _finalize_or_block(
                pool,
                subjects=subjects,
                expected_github_pr_id=None,
                workspace_id=workspace_id,
                task_id=task_id,
                owner_gate_id=owner_gate_id,
                expected_state_token=expected_state_token,
                terminal_finalization_submission=terminal_finalization_submission,
            )
            return DispatchedPrComposition(
                task=published.task,
                send_claim=None,
                receipt=None,
                producer_session=None,
                published=published,
            )
        producer_session = subjects.producer_session
        if producer_session is None or producer_session.external_session_id is None:
            # Narrowed by the fresh-path admission; kept explicit for
            # fail-closed safety.
            raise ConflictError(
                "the fresh pr composition carries no ready producer session binding"
            )
        register = getattr(observation_registrar, "register_session", None)
        if not callable(register):
            raise InvalidCommandError(
                "the pr-composition observation registrar does not implement the "
                "established-session registration seam"
            )
        message = PrComposeInteraction(
            repository_full_name=subjects.repository_full_name,
            issue_number=subjects.task.github_issue_number,
            guidance=subjects.guidance,
        ).render()
        return _dispatch_pr_compose_turn(
            pool,
            producer_runtime_factory,
            workspace_id=workspace_id,
            task_id=task_id,
            subjects=subjects,
            producer_session=producer_session,
            message=message,
            register_session=register,
            completion_handoff=completion_handoff,
            attempt_evidence=attempt_evidence,
        )


def _dispatch_pr_compose_turn(
    pool: DatabasePool,
    producer_runtime_factory: ProducerRuntimeAttachmentFactory,
    *,
    workspace_id: UUID,
    task_id: UUID,
    subjects: _PublicationSubjects,
    producer_session: TaskAgentSession,
    message: str,
    register_session: Callable[..., object],
    completion_handoff: DetachedTurnCompletionHandoff,
    attempt_evidence: InvocationAttemptEvidence | None,
) -> DispatchedPrComposition:
    """Admit the canonical PR_COMPOSE instruction as one detached agent turn.

    The shared detached-dispatch mechanics of the fresh composition path
    (issue #337), mirroring the established detached-dispatch shape: the
    runtime lease and the detached capability are acquired and duck-typed
    fail-closed BEFORE any durable dispatch effect, the exact one-shot claim
    and the detached-turn receipt companion commit in ONE short transaction
    strictly before any runtime effect (the claim's own subject reproof
    under the Task row lock is the fresh path's post-attachment R1
    revalidation), one positively-required observation registration gates
    the admission, one detached-turn admission carries the composed
    canonical instruction verbatim, and the acknowledgement attach consumes
    any staged early completion exactly once. No ``realize_control`` and no
    blocking wait exists in this path, and no database transaction is ever
    held across a runtime call.

    Proven pre-effect failures refuse the receipt and record the claim's
    known non-delivery, then raise the typed operational-failure classes the
    F5b coordinator triages under the claim's own family. An uncertain
    admission leaves the claim and receipt conservatively claimed and is
    never re-dispatched. The dispatching worker returns after the
    acknowledgement attach; the observed completion owns the settlement.
    """
    producer_external_session_id = producer_session.external_session_id
    if producer_external_session_id is None or not producer_external_session_id.strip():
        raise ConflictError("the producer session binding carries no external session identity")
    runtime_factory = _require_producer_factory(producer_runtime_factory)
    with ExitStack() as attachment_stack:
        runtime = attachment_stack.enter_context(runtime_factory())
        # The detached capability is duck-typed fail-closed BEFORE any durable
        # dispatch effect: a runtime without it cannot serve the detached
        # dispatch, and no claim or receipt was ever recorded.
        admit = getattr(runtime, "admit_detached_turn", None)
        if not callable(admit):
            raise ExternalOperationFailedError(
                "the producer runtime does not implement the detached turn "
                "capability; the pr composition dispatch failed before any effect"
            )
        admit_detached_turn = cast(Callable[[str, str], AgentTurnAdmission], admit)

        # --- The one short dispatch transaction: the exact pre-send claim and
        # the detached-turn receipt companion, committed atomically strictly
        # before any runtime effect. The claim's own subject reproof under the
        # Task row lock is the fresh path's post-attachment R1 revalidation.
        # A replayed or raced continuation never claims or dispatches again.
        # ---
        with composed_transaction(pool) as transaction_pool:
            send_claim = claim_pr_compose_send(
                transaction_pool,
                workspace_id=workspace_id,
                task_id=task_id,
                expected_state_token=subjects.task.state_token,
                owner_gate=subjects.gate,
                producer=producer_session,
            )
            receipt = turn_correlation.record_task_detached_turn_receipt(
                transaction_pool,
                workspace_id=workspace_id,
                task_id=task_id,
                task_agent_session_id=producer_session.id,
                operation=PendingTaskRuntimeInteractionOperation.PR_COMPOSE_DISPATCH,
                original_attempt_kind=OriginalAttemptKind.RUNTIME_SEND_CLAIM,
                original_attempt_id=send_claim.id,
                expected_formal_family=PR_RESULT_FAMILY,
            )
        if attempt_evidence is not None:
            # The F5b coordinator's handoff fact: this invocation's committed
            # dispatch claim identity (issue #262).
            attempt_evidence.record_attempt(
                family=PendingTaskRuntimeInteractionOperation.PR_COMPOSE_DISPATCH,
                original_attempt_kind=OriginalAttemptKind.RUNTIME_SEND_CLAIM,
                original_attempt_id=send_claim.id,
                phase=PendingTaskRuntimeInteractionOperation.PR_COMPOSE_DISPATCH.value,
            )

        # --- The positive pre-admission observation registration: the
        # positively-required gate. A refusal is a proven pre-effect failure:
        # the just-recorded receipt is marked refused and the claim records
        # its known non-delivery, so a never-dispatched turn cannot hold the
        # session's single-outstanding slot. ---
        try:
            register_session(
                workspace_id=workspace_id,
                task_id=task_id,
                task_agent_session_id=producer_session.id,
                connection_id=producer_session.connection_id,
                external_session_id=producer_external_session_id,
            )
        except ObservationRegistrationRefusedError as error:
            _refuse_pr_compose_dispatch_quietly(
                pool, receipt, send_claim, refusal_kind=_REGISTRATION_REFUSAL_KIND
            )
            raise ExternalOperationFailedError(
                "the pr-composition observation registration was refused before "
                "any admission; the composition never dispatched"
            ) from error
        except Exception as error:
            from openorc.services.task_detached_turn_observation import (  # noqa: PLC0415
                ObservationRegistrationRefusalReason,
            )

            _refuse_pr_compose_dispatch_quietly(
                pool, receipt, send_claim, refusal_kind=_REGISTRATION_REFUSAL_KIND
            )
            raise ExternalOperationFailedError(
                "the pr-composition observation registration failed "
                f"({ObservationRegistrationRefusalReason.OBSERVATION_REGISTRATION_FAILED.value}) "
                "before any admission; the composition never dispatched"
            ) from error

        # --- The admission (external, no transaction held). One detached-turn
        # admission of the composed canonical PR_COMPOSE instruction
        # verbatim. ---
        try:
            admission = admit_detached_turn(producer_external_session_id, message)
        except AgentRuntimeUncertainOutcomeError as error:
            logger.warning(
                "the detached pr-composition admission ended with an uncertain "
                "outcome; the claim and receipt stay conservatively claimed "
                "and are never re-dispatched"
            )
            raise ExternalOperationUncertainError(
                "the outcome of the pr-composition admission is unknown; the "
                "claim and receipt stay conservatively claimed and the "
                "composition is never re-dispatched"
            ) from error
        except AgentRuntimeError as error:
            # A positively identified refusal (unreachable before effect,
            # runtime rejection, exact-session absence): proven pre-effect,
            # so the receipt is marked refused and the dispatch fails known.
            _refuse_pr_compose_dispatch_quietly(
                pool, receipt, send_claim, refusal_kind=_ADMISSION_REFUSAL_KIND
            )
            raise ExternalOperationRejectedError(
                "the pr-composition admission was refused before any effect; "
                "the receipt is refused and the composition never dispatched"
            ) from error

        # --- The acknowledgement attach (one short transaction). A staged
        # early completion is consumed exactly once here and routed through
        # the ordinary settlement, so a validated pr_result that finished
        # before the acknowledgement was persisted is correlated, never
        # ignored or duplicated. ---
        try:
            outcome: turn_correlation.TurnSettlementOutcome = (
                turn_correlation.attach_acknowledged_turn(
                    pool,
                    workspace_id=workspace_id,
                    task_id=task_id,
                    receipt_id=receipt.id,
                    admitted_turn_reference=admission.turn_reference,
                )
            )
        except ApplicationError:
            raise
        except Exception as error:
            raise ExternalOperationFailedError(
                "the pr-composition acknowledgement could not be durably "
                "attached after its known admission; authoritative recovery "
                "is required and the composition is never re-dispatched"
            ) from error

    # The attachment is released; the handoff is strictly post-dispatch and
    # never holds the runtime lease. A staged early completion settled the
    # claim and the receipt within the attach: its validated ``pr_result`` is
    # not durable anywhere (the v1 persistence decision, issue #337), so the
    # ONE post-commit completion handoff receives it in memory for the same
    # invocation's bounded publication continuation. Strictly post-commit,
    # contained, never a workflow authority.
    if outcome.kind is turn_correlation.TurnSettlementKind.SETTLED_VALIDATED_RESULT:
        validated_result = outcome.formal_result
        if not isinstance(validated_result, PrResult):
            raise ConflictError(
                "the early-completed pr composition settled without a validated "
                "pr_result; the publication continuation is not available"
            )
        try:
            completion_handoff.handoff_settled_completion(
                workspace_id=workspace_id,
                task_id=task_id,
                operation=PendingTaskRuntimeInteractionOperation.PR_COMPOSE_DISPATCH,
                validated_result=validated_result,
            )
        except Exception as error:  # noqa: BLE001 - contained handoff
            logger.warning(
                "the post-dispatch pr-completion handoff failed (%s); "
                "the durable state owns recovery",
                type(error).__name__,
            )
    reloaded_task = task_records.get_task(pool, task_id=task_id)
    if reloaded_task is None or reloaded_task.workspace_id != workspace_id:
        raise NotFoundError("the requested task is not available in this workspace")
    return DispatchedPrComposition(
        task=reloaded_task,
        send_claim=send_claim,
        receipt=outcome.receipt if outcome.receipt is not None else receipt,
        producer_session=producer_session,
        published=None,
    )


def _revalidate_settlement_subjects(
    transaction_pool: DatabasePool,
    *,
    workspace_id: UUID,
    task_id: UUID,
    send_claim: TaskRuntimeSendClaim,
) -> _PublicationSubjects:
    """The detached settlement's database-only subject re-derivation (#337).

    The exact durable facts the committed PR_COMPOSE claim addressed must
    still be the publication's subject set at observation time: the exact
    current ``waiting_for_owner`` Task — read under the exclusive Task row
    lock, because this settlement performs no conditional Task write of its
    own (the publication is the post-settlement consequence) and the lock is
    what serializes its currentness with every composed Task transition (an
    Owner Abort, blocking, or cancellation that wins the lock first is
    observed here; one that comes later serializes behind the settlement) —
    no current OwnerGate, a bound canonical feature branch, no canonical
    record, the claim's approved ``PR_AUTHORIZATION`` gate still carrying the
    claim's exact head SHA, and the claim's exact READY Producer binding. An
    existing canonical record means the composition already resolved — the
    existing recovery semantics own the residue, never a second create. The
    full authorization re-proof (the account barrier, the Workspace Owner
    scope) happens again in the bounded publication continuation, before any
    GitHub call.
    """
    task = task_records.get_task_for_update(transaction_pool, task_id)
    if task is None or task.workspace_id != workspace_id:
        raise NotFoundError("the requested task is not available in this workspace")
    if task.status is not TaskStatus.WAITING_FOR_OWNER:
        raise StaleOperationError(
            "the task is no longer waiting on its owner gate; the observed "
            "pr result can no longer apply"
        )
    if task.current_owner_gate_id is not None:
        raise StaleOperationError(
            "the task carries a current owner gate; the observed pr result can no longer apply"
        )
    if task.canonical_feature_branch is None:
        raise ConflictError(
            "the task carries no bound canonical feature branch; the observed "
            "pr result can no longer apply"
        )
    existing_pr = pull_request_records.get_task_pull_request_for_task(
        transaction_pool, task_id=task.id
    )
    if existing_pr is not None:
        raise StaleOperationError(
            "the task already carries a canonical pull request; the observed "
            "pr result can no longer apply"
        )
    owner_gate_id = send_claim.owner_gate_id
    if owner_gate_id is None:
        # Narrowed by the kind's subject form; explicit for fail-closed safety.
        raise ConflictError("the pr compose dispatch claim carries no owner-gate subject")
    gate = gate_records.get_owner_gate(transaction_pool, owner_gate_id=owner_gate_id)
    if gate is None or gate.task_id != task.id or gate.workspace_id != workspace_id:
        raise NotFoundError("the requested owner gate is not available for this task")
    if (
        gate.gate_type is not OwnerGateType.PR_AUTHORIZATION
        or gate.status is not OwnerGateStatus.APPROVED
        or gate.subject_head_sha is None
        or gate.subject_head_sha != send_claim.subject_head_sha
    ):
        raise StaleOperationError(
            "the approved pr authorization gate moved on; the observed pr result "
            "can no longer apply"
        )
    approving_profile_id = _resolve_approved_resolution_audit(
        transaction_pool, workspace_id=workspace_id, owner_gate_id=owner_gate_id
    )
    session = session_records.get_task_agent_session(
        transaction_pool, task_id=task.id, role=WorkflowRole.PRODUCER
    )
    if session is None or session.task_id != task.id or session.workspace_id != workspace_id:
        raise NotFoundError("the requested producer session is not available for this task")
    if (
        session.lifecycle_status is not TaskSessionLifecycleStatus.READY
        or session.id != send_claim.task_agent_session_id
        or session.external_session_id is None
        or not session.external_session_id.strip()
    ):
        raise StaleOperationError(
            "the claimed producer session binding moved on; the observed pr result "
            "can no longer apply"
        )
    return _resolve_publication_route_subjects(
        transaction_pool,
        approving_profile_id=approving_profile_id,
        workspace_id=workspace_id,
        task=task,
        gate=gate,
        existing_pr=None,
    )


def settle_observed_pr_compose_result(
    transaction_pool: DatabasePool,
    *,
    workspace_id: UUID,
    task_id: UUID,
    send_claim: TaskRuntimeSendClaim,
    result: PrResult,
) -> _PublicationSubjects | None:
    """Finalize one observed PR_COMPOSE result through the owning service.

    The detached composition settlement's owning-service composition (issue
    #337): the correlation seam funnels the attributed terminal observation
    through the shared formal contract and hands the validated ``PrResult``
    here inside its own settlement transaction. The settlement re-derives
    every subject from durable state and settles the send claim exactly once
    with only the safe approved head identity as its outcome — atomically
    with the receipt settlement the route performs next, and without
    pretending GitHub publication has completed. ``None`` means the subject
    set has moved on: the observation can no longer apply, nothing is
    rewritten, and the existing recovery semantics own the conservative
    residue.
    """
    if not isinstance(result, PrResult):
        raise InvalidCommandError("settle_observed_pr_compose_result requires a PrResult")
    if not isinstance(send_claim, TaskRuntimeSendClaim):
        raise InvalidCommandError("settle_observed_pr_compose_result requires a send claim")
    try:
        subjects = _revalidate_settlement_subjects(
            transaction_pool, workspace_id=workspace_id, task_id=task_id, send_claim=send_claim
        )
    except (NotFoundError, ConflictError, StaleOperationError):
        # The subject set moved on between the dispatch and the observed
        # completion (a rotated token, a lost session, a competing canonical
        # record, a moved-on gate): the validated result can no longer apply,
        # so nothing is rewritten and the receipt settles as
        # result-not-applied. Never a fabricated publication.
        return None
    approved_head_sha = subjects.gate.subject_head_sha
    if approved_head_sha is None:  # pragma: no cover - narrowed by revalidation
        raise ConflictError("the pr authorization gate does not carry the exact head-sha subject")
    settle_pr_compose_send_claim(
        transaction_pool,
        claim=send_claim,
        workspace_id=workspace_id,
        task_id=task_id,
        subject_head_sha=approved_head_sha,
    )
    return subjects


def publish_settled_task_pr_composition(
    pool: DatabasePool,
    github: GitHubAppClient,
    user_token_resolver: ProfileUserAccessTokenResolver,
    *,
    workspace_id: UUID,
    task_id: UUID,
    owner_gate_id: UUID,
    expected_state_token: UUID,
    pr_result: PrResult,
    terminal_finalization_submission: TerminalFinalizationSubmission | None = None,
) -> SettledTaskPrPublication:
    """Publish one settled composition's validated presentation (issue #337).

    The ONE post-commit completion handoff's bounded post-settlement
    consequence: the validated ``pr_result`` is presentation material only —
    the authoritative repository, branch, base, head SHA, and approving
    identity are re-derived from durable state in one short transaction at
    the Task's current token (a moved-on subject fails closed truthfully) —
    and then the existing bounded publication
    preflight/create/reconcile/finalize runs in this same
    completion-processing invocation, with no database transaction across
    the GitHub I/O. This is deliberately not a new model turn, not a second
    observer implementation, and not an independent F1 publication job; a
    process death after the settlement but before any GitHub create is the
    explicitly accepted narrow v1 liveness/stranding limitation (issue
    #337) — the claimed model turn is never repeated and no title/body is
    ever invented.
    """
    _require_uuid_command(workspace_id, "workspace_id")
    _require_uuid_command(task_id, "task_id")
    _require_uuid_command(owner_gate_id, "owner_gate_id")
    _require_uuid_command(expected_state_token, "expected_state_token")
    if not isinstance(pr_result, PrResult):
        raise InvalidCommandError(
            "publish_settled_task_pr_composition requires a validated PrResult"
        )
    with application_span(_SERVICE_TRACER_SCOPE, _PUBLISH_SETTLED_SPAN_NAME) as span:
        annotate_span(
            span,
            operation=_PUBLISH_SETTLED_SPAN_NAME,
            workspace_id=str(workspace_id),
            task_id=str(task_id),
        )
        # --- One short transaction: the approving Profile is re-derived from
        # the gate's OWNER-attributed approved audit and the full publication
        # subject set is revalidated at the Task's current token. The
        # account-operational barrier and the Workspace Owner scope are
        # re-proved under the resolved approving Profile — publication's only
        # identity — before any GitHub call. ---
        with composed_transaction(pool) as transaction_pool:
            approving_profile_id = _resolve_approved_resolution_audit(
                transaction_pool, workspace_id=workspace_id, owner_gate_id=owner_gate_id
            )
            task, gate, existing_pr = _revalidate_publication_subjects(
                transaction_pool,
                workspace_id=workspace_id,
                task_id=task_id,
                owner_gate_id=owner_gate_id,
                expected_state_token=expected_state_token,
            )
            subjects = _resolve_publication_route_subjects(
                transaction_pool,
                approving_profile_id=approving_profile_id,
                workspace_id=workspace_id,
                task=task,
                gate=gate,
                existing_pr=existing_pr,
            )
        # --- The bounded publication consequence in the same invocation,
        # with no transaction open across the GitHub I/O. ---
        published = _publish_and_finalize(
            pool,
            github,
            user_token_resolver,
            subjects=subjects,
            pr_result=pr_result,
            workspace_id=workspace_id,
            task_id=task_id,
            owner_gate_id=owner_gate_id,
            expected_state_token=expected_state_token,
            terminal_finalization_submission=terminal_finalization_submission,
        )
        return SettledTaskPrPublication(
            task=published.task,
            pull_request=published.pull_request,
            approving_profile_id=subjects.approving_profile_id,
        )
