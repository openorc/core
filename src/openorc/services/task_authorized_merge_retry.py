"""F5d: the one durably authorized exact-head merge retry (issue #267).

When F4 (#251) has durably authorized one bounded additional merge attempt
for an exact-head merge failure episode, this seam executes that single
authorization exactly once. An authorized retry is **not** a replay of the
E10a one-shot operation: it re-derives every durable subject from committed
state, settles every pre-send and post-send condition truthfully, and
resends the merge mutation exactly once under the ORIGINAL Owner approval —
never a re-approval, a gate mutation, or an invented ``MERGE_REQUESTED``
decision.

Composition (external preflights precede consumption; no database
transaction ever spans a GitHub call):

1. **Read-only durable pre-verification** (no writes): the
   account-operational barrier and the exact Profile's Owner scope, the
   current ``waiting_for_owner`` Task under the caller's ``state_token``
   with no current gate, the F4 attempt and its exact-head merge episode
   (non-terminally disposed, rooted at the approved ``MERGE_DECISION`` gate
   UUID, the episode's latest ordinal with every earlier ordinal consumed),
   the persisted due instant, the historical approved gate bound to the
   exact canonical TaskPullRequest and accepted head, and the durable
   open/unmerged same-head canonical PR row.
2. **External preflights, each with a guarded settlement — the authorization
   is never consumed by a failed preflight, and no settlement pretends the
   merge mutation was attempted:**

   - One authoritative reconciliation (the F5d repeat-safety observation):
     a GitHub-confirmed merged canonical PR takes the shared completion seam
     and never resends; a closed-unmerged PR keeps the existing refusal
     semantics (E10c's #229 reconciliation machinery owns the terminal
     consequence); a moved head keeps E10a's stale classification; an
     uncertain/inconclusive read installs the guarded ``BLOCK_RESOLUTION``
     escalation block with the truthful ``merge_mutation_attempted: false``
     context; a known access/credential refusal keeps E10a's existing
     definitive-no-merge settlement (the fresh pending ``MERGE_DECISION``
     gate is the retained E10a semantic for proven pre-delivery conditions).
   - The routed source-issue guard: a GitHub-confirmed closed (or otherwise
     not-open) source issue installs the guarded ``BLOCK_RESOLUTION`` block
     with reason ``owner_action_required`` and the explicit closed-issue
     evidence — deliberately NEVER the ``GITHUB_SOURCE_CHANGED``/
     ``GITHUB_ISSUE_CHANGE`` vocabulary, which is reserved for an actual
     requirements-fingerprint change whose Owner Ignore acknowledges that
     change (issue closure establishes no requirements change, and
     fabricating its acknowledgment would be false provenance); an
     uncertain issue read installs the escalation block; a known
     access/credential refusal keeps the E10a settlement. Issue openness is
     revalidated by the guard before any subsequent merge mutation.

3. **Atomic consumption + final revalidation** (one Task-row-first composed
   transaction): every Stage 1 verification re-proven under the Task row
   lock, then the one-shot consumption stamp (``consumed_at``) written
   through the repository conditional set-once update — a competing worker,
   duplicate delivery, or premature wakeup can never re-consume the
   authorization or reach the send, and any refusal rolls the whole
   composition back. After the commit the invocation records its own attempt
   identity (the consumed attempt UUID, with its consumption stamp as its
   own effect evidence) for the F5b failure handoff.
4. **The single send and truthful disposition**: the shared
   ``request_pull_request_merge`` primitive is called exactly once with the
   original accepted head and the approving Profile's credential, its final
   pre-send transaction re-proving the durable one-shot claim through the
   same ``claim_revalidation`` hook E10a uses. The merge send's OWN unknown
   outcome (the primitive's send-phase ``MergeRequestSendUncertainError``)
   propagates unchanged for the F5b coordinator's typed Level 2 handoff —
   eligibility is F4's, and only the failure of this consumed attempt is
   reportable to the episode. Every other outcome keeps its existing E10a
   semantics: a known-success merge completes through the shared completion
   seam; a confirmed-merge reconciliation-pending failure takes the existing
   known-success block (never resend); a GitHub-owned refusal, a known
   pre-delivery condition, and a stale head keep the E10a
   definitive-no-merge/stale semantics; a plain uncertain pre-send outcome
   inside the primitive is reported truthfully to the episode (which
   escalates — the code can never be retry-eligible) instead of orphaning
   the consumed authorization.

This seam never schedules wakeups (F11 #257 owns due-time submission), never
re-approves or mutates the gate, and never applies a second merge mutation
within one invocation.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Final
from uuid import UUID

from openorc.adapters.github import GitHubAppClient
from openorc.domain.blocks import TaskBlockReason, canonical_task_block_context
from openorc.domain.gates import OwnerGate, OwnerGateStatus, OwnerGateType
from openorc.domain.operational_failures import (
    FailureEffectEvidence,
    GitHubMergeOperation,
    OperationalFailureAttempt,
    OperationalFailureCode,
    OperationalFailureEpisode,
    OperationalFailureSourceKind,
    OriginalAttemptKind,
)
from openorc.domain.pull_requests import TaskPullRequest, TaskPullRequestState
from openorc.domain.tasks import Task, TaskStatus
from openorc.observability import annotate_span, application_span
from openorc.persistence import gates as gate_records
from openorc.persistence import operational_failures as failure_records
from openorc.persistence import tasks as task_records
from openorc.persistence.pool import DatabasePool
from openorc.persistence.pull_requests import get_task_pull_request
from openorc.services import (
    event_coordination,
    pull_request_merge,
    task_pull_request_reconciliation,
)
from openorc.services.errors import (
    ApplicationError,
    AuthorizationError,
    ConflictError,
    ExternalOperationFailedError,
    ExternalOperationRejectedError,
    ExternalOperationUncertainError,
    InvalidCommandError,
    MergeConfirmedReconciliationPendingError,
    NotFoundError,
    StaleOperationError,
)
from openorc.services.event_coordination import WorkflowActorContext
from openorc.services.github_owner_write_authorization import require_owner_write_authorization
from openorc.services.github_user_authorization import ProfileUserAccessTokenResolver
from openorc.services.operation_attempt_evidence import InvocationAttemptEvidence
from openorc.services.task_exceptional_blocking import install_exceptional_block_gate
from openorc.services.task_merge_lifecycle import (
    TaskMergeOutcome,
    TaskMergeOutcomeKind,
    _classify_after_stale_merge_request,
    _classify_after_unconfirmed_merge,
    _is_confirmed_merged,
    _merge_claim_revalidation,
    _require_source_issue_open,
    _settle_after_definitive_no_merge,
    complete_confirmed_task_merge,
)
from openorc.services.task_operational_failure_triage import (
    DispositionKind,
    OperationalFailureOperation,
    OperationalFailureReport,
    triage_operational_failure,
)
from openorc.services.task_subject_guards import require_current_task
from openorc.services.transaction_composition import composed_transaction

__all__ = [
    "execute_authorized_merge_retry",
]

_SERVICE_TRACER_SCOPE: Final[str] = "openorc.services.task_authorized_merge_retry"
_EXECUTE_SPAN_NAME: Final[str] = "task_authorized_merge_retry.execute"

# The safe machine phase labels the invocation records as it progresses; the
# latest one is the phase a failing invocation reached (F5b handoff fact).
_PHASE_PREFLIGHT: Final[str] = "merge_retry_preflight"
_PHASE_SEND: Final[str] = "merge_retry_send"

_OPENORC_ACTOR: Final[WorkflowActorContext] = event_coordination.WorkflowActorContext(
    event_coordination.WorkflowEventActor.OPENORC, None
)


@dataclass(frozen=True, slots=True)
class _MergeRetryAdmission:
    """The durably verified subjects one authorized merge retry executes.

    ``task_pull_request_id``/``subject_head_sha`` are the loader-narrowed
    non-null values of the verified gate's exact canonical-PR/head binding —
    the verification proved both present before admission.
    """

    task: Task
    episode: OperationalFailureEpisode
    attempt: OperationalFailureAttempt
    gate: OwnerGate
    pull_request: TaskPullRequest
    task_pull_request_id: UUID
    subject_head_sha: str


def _require_uuid_command(value: object, name: str) -> UUID:
    if not isinstance(value, UUID):
        raise InvalidCommandError(f"{name} must be a UUID")
    return value


def _load_merge_retry_subjects(
    pool: DatabasePool,
    *,
    profile_id: UUID,
    workspace_id: UUID,
    task_id: UUID,
    retry_attempt_id: UUID,
    expected_state_token: UUID,
    now: Callable[[], datetime],
) -> _MergeRetryAdmission:
    """Re-derive every retry subject from committed state (reads only).

    Shared by the read-only Stage 1 verification and the Stage 3 locked
    re-verification: the exact currentness, the F4 authorization and its
    episode/budget state, the persisted due instant, and the original claim
    evidence (the approved gate binding the canonical PR + accepted head).
    Every refusal is a typed error raised before anything is consumed.
    """
    # The account-operational barrier and the exact Profile's Owner scope
    # compose FIRST (the #143 required sequence).
    require_owner_write_authorization(
        pool, profile_id=profile_id, workspace_id=workspace_id, task_id=task_id
    )
    current = require_current_task(
        pool,
        workspace_id=workspace_id,
        task_id=task_id,
        expected_state_token=expected_state_token,
    )
    if current.status is not TaskStatus.WAITING_FOR_OWNER:
        raise StaleOperationError(
            "the task is not waiting for an owner merge decision; the merge retry is stale"
        )
    if current.current_owner_gate_id is not None:
        raise StaleOperationError("the task carries a current owner gate; the merge retry is stale")

    # --- the F4-issued authorization and its episode. ---
    attempt = failure_records.get_operational_failure_attempt(pool, attempt_id=retry_attempt_id)
    if attempt is None or attempt.workspace_id != workspace_id or attempt.task_id != task_id:
        raise NotFoundError("the requested retry attempt is not available for this task")
    if attempt.consumed_at is not None:
        raise ConflictError(
            "the retry authorization has already been consumed; it is never re-consumed"
        )
    episode = failure_records.get_operational_failure_episode(pool, episode_id=attempt.episode_id)
    if (
        episode is None
        or episode.workspace_id != workspace_id
        or episode.task_id != task_id
        or episode.operation_family is not GitHubMergeOperation.EXACT_HEAD_MERGE
        or episode.original_attempt_kind is not OriginalAttemptKind.MERGE_DECISION_GATE
    ):
        raise ConflictError(
            "the retry attempt's failure episode does not address this task's exact-head merge"
        )
    if episode.escalated_task_block_id is not None:
        raise ConflictError(
            "the retry attempt's failure episode is terminally disposed; "
            "the authorization can no longer be executed"
        )

    # --- the episode's attempt budget: latest ordinal, coherent history. ---
    attempts = failure_records.list_operational_failure_attempts(pool, episode_id=episode.id)
    if not attempts or attempts[-1].id != attempt.id:
        raise ConflictError("the retry authorization is not the failure episode's latest attempt")
    for earlier in attempts[:-1]:
        if earlier.consumed_at is None:
            raise ConflictError(
                "an earlier authorized merge attempt was never consumed; "
                "the episode's attempt history is incoherent"
            )

    # --- the persisted due instant, enforced at actual admission. ---
    if attempt.not_before > now():
        raise StaleOperationError(
            "the retry authorization is not yet due; the wakeup has not arrived"
        )

    # --- the original claim evidence: the approved MERGE_DECISION gate. ---
    gate = gate_records.get_owner_gate(pool, owner_gate_id=episode.original_attempt_id)
    if (
        gate is None
        or gate.workspace_id != workspace_id
        or gate.task_id != task_id
        or gate.gate_type is not OwnerGateType.MERGE_DECISION
        or gate.status is not OwnerGateStatus.APPROVED
        or gate.task_pull_request_id is None
        or gate.subject_head_sha is None
    ):
        raise ConflictError(
            "the merge episode's approved gate no longer binds the addressed merge subject"
        )
    pull_request = get_task_pull_request(pool, task_pull_request_id=gate.task_pull_request_id)
    if (
        pull_request is None
        or pull_request.workspace_id != workspace_id
        or pull_request.task_id != task_id
        or pull_request.state is not TaskPullRequestState.OPEN
        or pull_request.merged_at is not None
        or pull_request.head_sha != gate.subject_head_sha
    ):
        raise ConflictError(
            "the canonical pull request is no longer open at the approved head; "
            "the merge retry is stale"
        )
    # The verification proved both binding values present; narrowing for the
    # downstream typed seams.
    assert gate.task_pull_request_id is not None
    assert gate.subject_head_sha is not None
    return _MergeRetryAdmission(
        task=current,
        episode=episode,
        attempt=attempt,
        gate=gate,
        pull_request=pull_request,
        task_pull_request_id=gate.task_pull_request_id,
        subject_head_sha=gate.subject_head_sha,
    )


def _install_merge_retry_settlement_block(
    pool: DatabasePool,
    *,
    admission: _MergeRetryAdmission,
    expected_state_token: UUID,
    block_reason: TaskBlockReason,
    context: dict[str, object],
) -> TaskMergeOutcome:
    """Install the guarded F5d settlement block atomically (the #258 installer).

    The atomic exceptional-block installation is the truthful, actionable
    recovery surface for a pre-consumption failure: the unresolved TaskBlock,
    the pending ``BLOCK_RESOLUTION`` OwnerGate, the ``blocked`` transition,
    the token rotation, and the audit events compose in one short database
    transaction, so a pre-send operational condition never strands a waiting
    Task without a decision surface. The context always records that the
    merge mutation was not attempted and that the F4 authorization remains
    unconsumed — the settlement never pretends a send happened.
    """
    installation = install_exceptional_block_gate(
        pool,
        workspace_id=admission.task.workspace_id,
        task_id=admission.task.id,
        expected_state_token=expected_state_token,
        expected_status=TaskStatus.WAITING_FOR_OWNER,
        expected_current_owner_gate_id=None,
        gate_type=OwnerGateType.BLOCK_RESOLUTION,
        block_reason=block_reason,
        block_context=canonical_task_block_context(context),
        actor=_OPENORC_ACTOR,
    )
    return TaskMergeOutcome(
        kind=TaskMergeOutcomeKind.BLOCKED_UNCERTAIN,
        task=installation.task,
        pull_request=admission.pull_request,
        task_block=installation.task_block,
    )


def _merge_retry_subject_context(admission: _MergeRetryAdmission) -> dict[str, object]:
    """The bounded safe subject facts every settlement block context carries."""
    return {
        "owner_gate_id": str(admission.episode.original_attempt_id),
        "task_pull_request_id": str(admission.task_pull_request_id),
        "expected_head_sha": admission.subject_head_sha,
    }


def _settle_reconciliation_read_outcome(
    pool: DatabasePool,
    github: GitHubAppClient,
    *,
    admission: _MergeRetryAdmission,
    profile_id: UUID,
    expected_state_token: UUID,
    error: ApplicationError,
) -> TaskMergeOutcome:
    """The guarded settlement for a failed pre-send reconciliation read.

    An uncertain/inconclusive read cannot authorize the send: the merge
    mutation was not attempted, and the escalation block is installed with
    the truthful context. A known access/credential refusal keeps E10a's
    existing definitive-no-merge settlement (the retained semantic for
    proven pre-delivery conditions). Anything else is an unexpected typed
    outcome and fails closed.
    """
    if isinstance(error, ExternalOperationUncertainError):
        context = {
            "settlement": "merge_retry_presend_reconciliation_unavailable",
            "merge_mutation_attempted": False,
            "merge_retry_attempt_consumed": False,
            **_merge_retry_subject_context(admission),
        }
        return _install_merge_retry_settlement_block(
            pool,
            admission=admission,
            expected_state_token=expected_state_token,
            block_reason=TaskBlockReason.EXTERNAL_OPERATION_UNCERTAIN,
            context=context,
        )
    if isinstance(error, (AuthorizationError, ExternalOperationFailedError)):
        return _settle_after_definitive_no_merge(
            pool,
            github,
            profile_id=profile_id,
            workspace_id=admission.task.workspace_id,
            task_id=admission.task.id,
            owner_gate_id=admission.episode.original_attempt_id,
            expected_state_token=expected_state_token,
            task_pull_request_id=admission.task_pull_request_id,
            subject_head_sha=admission.subject_head_sha,
            github_rejected=False,
        )
    raise error


def _settle_source_issue_guard_outcome(
    pool: DatabasePool,
    github: GitHubAppClient,
    *,
    admission: _MergeRetryAdmission,
    profile_id: UUID,
    expected_state_token: UUID,
    error: ApplicationError,
) -> TaskMergeOutcome:
    """The guarded settlement for a failed source-issue preflight (F5d).

    A GitHub-confirmed not-open source issue refuses the merge retry WITHOUT
    consuming the authorization and settles as the truthful
    ``BLOCK_RESOLUTION``/``owner_action_required`` block carrying the
    explicit addressed-issue evidence — deliberately never the
    requirements-fingerprint ``GITHUB_SOURCE_CHANGED``/``GITHUB_ISSUE_CHANGE``
    vocabulary (issue closure establishes no requirements change; fabricating
    its Owner acknowledgment would be false provenance). An uncertain issue
    read settles as the mutation-not-attempted escalation block. A known
    access/credential refusal keeps E10a's existing definitive-no-merge
    settlement. The routed guard revalidates issue openness before any
    subsequent merge mutation.
    """
    if isinstance(error, ConflictError):
        context = {
            "settlement": "merge_retry_refused_source_issue_not_open",
            "addressed_github_issue_id": str(admission.task.github_issue_id),
            "merge_mutation_attempted": False,
            "merge_retry_attempt_consumed": False,
            "issue_openness_revalidated_before_any_merge": True,
            **_merge_retry_subject_context(admission),
        }
        return _install_merge_retry_settlement_block(
            pool,
            admission=admission,
            expected_state_token=expected_state_token,
            block_reason=TaskBlockReason.OWNER_ACTION_REQUIRED,
            context=context,
        )
    if isinstance(error, ExternalOperationUncertainError):
        context = {
            "settlement": "merge_retry_source_issue_read_unavailable",
            "merge_mutation_attempted": False,
            "merge_retry_attempt_consumed": False,
            **_merge_retry_subject_context(admission),
        }
        return _install_merge_retry_settlement_block(
            pool,
            admission=admission,
            expected_state_token=expected_state_token,
            block_reason=TaskBlockReason.EXTERNAL_OPERATION_UNCERTAIN,
            context=context,
        )
    if isinstance(error, (AuthorizationError, ExternalOperationFailedError)):
        return _settle_after_definitive_no_merge(
            pool,
            github,
            profile_id=profile_id,
            workspace_id=admission.task.workspace_id,
            task_id=admission.task.id,
            owner_gate_id=admission.episode.original_attempt_id,
            expected_state_token=expected_state_token,
            task_pull_request_id=admission.task_pull_request_id,
            subject_head_sha=admission.subject_head_sha,
            github_rejected=False,
        )
    raise error


def _preflight_before_consumption(
    pool: DatabasePool,
    github: GitHubAppClient,
    *,
    admission: _MergeRetryAdmission,
    profile_id: UUID,
    expected_state_token: UUID,
) -> TaskMergeOutcome | None:
    """The external preflights: reconciliation and the source-issue guard.

    Returns a settled ``TaskMergeOutcome`` when a preflight already decided
    the invocation (confirmed merge, stale head, or a guarded settlement),
    or ``None`` when both preflights passed and the invocation may consume
    the authorization and send. Nothing here consumes the authorization.
    """
    try:
        reconciliation = task_pull_request_reconciliation.reconcile_task_pull_request(
            pool,
            github,
            workspace_id=admission.task.workspace_id,
            task_id=admission.task.id,
        )
    except ApplicationError as error:
        return _settle_reconciliation_read_outcome(
            pool,
            github,
            admission=admission,
            profile_id=profile_id,
            expected_state_token=expected_state_token,
            error=error,
        )
    reconciled = reconciliation.pull_request
    if _is_confirmed_merged(reconciled):
        # A known merged result always takes the authoritative completion
        # seam — never a resend, and the authorization stays unconsumed.
        completion = complete_confirmed_task_merge(
            pool, workspace_id=admission.task.workspace_id, task_id=admission.task.id
        )
        return TaskMergeOutcome(
            kind=TaskMergeOutcomeKind.COMPLETED,
            task=completion.task,
            pull_request=completion.pull_request,
        )
    if reconciled.state is TaskPullRequestState.CLOSED:
        # E10a's existing closed-unmerged refusal; the terminal consequence
        # is owned by E10c's (#229) reconciliation machinery.
        raise ConflictError(
            "the canonical pull request is closed and unmerged; the merge retry is refused"
        )
    if reconciled.head_sha != admission.subject_head_sha:
        return _classify_after_stale_merge_request(
            pool,
            github,
            profile_id=profile_id,
            workspace_id=admission.task.workspace_id,
            task_id=admission.task.id,
            owner_gate_id=admission.episode.original_attempt_id,
            expected_state_token=expected_state_token,
            task_pull_request_id=admission.task_pull_request_id,
            subject_head_sha=admission.subject_head_sha,
        )
    # The routed source-issue guard (the same one E10a runs before every
    # merge mutation).
    try:
        _require_source_issue_open(pool, github, task=admission.task)
    except ApplicationError as error:
        return _settle_source_issue_guard_outcome(
            pool,
            github,
            admission=admission,
            profile_id=profile_id,
            expected_state_token=expected_state_token,
            error=error,
        )
    return None


def _settle_presend_uncertainty(
    pool: DatabasePool,
    *,
    admission: _MergeRetryAdmission,
    expected_state_token: UUID,
) -> TaskMergeOutcome:
    """Report the consumed attempt's pre-send uncertainty to F4 and settle.

    The consumed attempt's execution failed BEFORE the merge mutation was
    attempted (a pre-send read/credential condition inside the primitive):
    the episode receives the truthful ``PRIOR_AUTHORIZED`` report with the
    ``merge_mutation_attempted: false`` marker, and the merge family's
    eligibility can never admit it — F4 escalates through its safe F3
    installation, which is the Task's actionable recovery surface.
    """
    operation = OperationalFailureOperation(
        family=GitHubMergeOperation.EXACT_HEAD_MERGE,
        original_attempt_kind=OriginalAttemptKind.MERGE_DECISION_GATE,
        original_attempt_id=admission.episode.original_attempt_id,
        subject=admission.episode.subject,
    )
    report = OperationalFailureReport(
        reporting_attempt_kind=OperationalFailureSourceKind.PRIOR_AUTHORIZED,
        reporting_attempt_id=admission.attempt.id,
        error_code=OperationalFailureCode.EXTERNAL_OPERATION_UNCERTAIN,
        effect_evidence=FailureEffectEvidence.EFFECT_UNCLASSIFIED,
        effect_attempt_id=admission.attempt.id,
        diagnostic_context={
            "merge_mutation_attempted": False,
            "phase": _PHASE_PREFLIGHT,
        },
    )
    disposition = triage_operational_failure(
        pool,
        workspace_id=admission.task.workspace_id,
        task_id=admission.task.id,
        expected_state_token=expected_state_token,
        expected_status=TaskStatus.WAITING_FOR_OWNER,
        expected_current_owner_gate_id=None,
        actor=_OPENORC_ACTOR,
        operation=operation,
        report=report,
    )
    if disposition.kind is DispositionKind.BLOCKED:
        installation = disposition.installation
        assert installation is not None
        return TaskMergeOutcome(
            kind=TaskMergeOutcomeKind.BLOCKED_UNCERTAIN,
            task=installation.task,
            pull_request=admission.pull_request,
            task_block=installation.task_block,
        )
    if disposition.kind is DispositionKind.NO_OP:
        raise StaleOperationError(
            "the pre-send uncertainty report no longer addresses current task state"
        )
    # RETRY_AUTHORIZED is impossible for the unsupported code: a durable-state
    # invariant failure fails closed.
    raise ConflictError("the pre-send uncertainty disposition cannot authorize a merge retry")


def _consume_authorized_merge_retry(
    transaction_pool: DatabasePool,
    *,
    profile_id: UUID,
    workspace_id: UUID,
    task_id: UUID,
    retry_attempt_id: UUID,
    expected_state_token: UUID,
    now: Callable[[], datetime],
) -> _MergeRetryAdmission:
    """Atomic admission, final revalidation, and one-shot consumption.

    The Task row lock is taken first; every Stage 1 verification re-proves
    against the locked durable state, and only then does the repository's
    conditional set-once consumption stamp the authorization. A competing
    worker, duplicate delivery, or stale caller leaves zero consumed rows and
    the whole composition rolls back.
    """
    locked = task_records.get_task_for_update(transaction_pool, task_id)
    if locked is None or locked.workspace_id != workspace_id:
        raise NotFoundError("the requested task is not available in this workspace")
    admission = _load_merge_retry_subjects(
        transaction_pool,
        profile_id=profile_id,
        workspace_id=workspace_id,
        task_id=task_id,
        retry_attempt_id=retry_attempt_id,
        expected_state_token=expected_state_token,
        now=now,
    )
    consumed = failure_records.consume_operational_failure_attempt(
        transaction_pool,
        attempt_id=admission.attempt.id,
        task_id=task_id,
        workspace_id=workspace_id,
        consumed_at=now(),
    )
    if consumed is None:
        raise ConflictError(
            "the retry authorization could not be consumed; it was consumed concurrently"
        )
    return _MergeRetryAdmission(
        task=admission.task,
        episode=admission.episode,
        attempt=consumed,
        gate=admission.gate,
        pull_request=admission.pull_request,
        task_pull_request_id=admission.task_pull_request_id,
        subject_head_sha=admission.subject_head_sha,
    )


def execute_authorized_merge_retry(
    pool: DatabasePool,
    github: GitHubAppClient,
    user_token_resolver: ProfileUserAccessTokenResolver,
    *,
    profile_id: UUID,
    workspace_id: UUID,
    task_id: UUID,
    expected_state_token: UUID,
    retry_attempt_id: UUID,
    attempt_evidence: InvocationAttemptEvidence | None = None,
    clock: Callable[[], datetime] | None = None,
) -> TaskMergeOutcome:
    """Execute the one F4-authorized additional exact-head merge attempt.

    One Owner-accountable, exact-subject seam: the F4 authorization is
    atomically consumed exactly once before the single outbound merge, every
    pre-send and post-send condition settles truthfully through the existing
    E10a semantics or the guarded F3/F4 escalation, and the merge send's own
    unknown outcome propagates for the F5b coordinator's Level 2 handoff.
    The caller cannot replay a consumed authorization, revive a terminal
    episode, run a premature wakeup, or bypass any exact-subject revalidation.
    """
    _require_uuid_command(profile_id, "profile_id")
    _require_uuid_command(workspace_id, "workspace_id")
    _require_uuid_command(task_id, "task_id")
    _require_uuid_command(expected_state_token, "expected_state_token")
    _require_uuid_command(retry_attempt_id, "retry_attempt_id")
    now = clock if clock is not None else (lambda: datetime.now(UTC))
    with application_span(_SERVICE_TRACER_SCOPE, _EXECUTE_SPAN_NAME) as span:
        annotate_span(
            span,
            operation=_EXECUTE_SPAN_NAME,
            workspace_id=str(workspace_id),
            task_id=str(task_id),
        )
        # --- Stage 1: read-only durable pre-verification (no writes). ---
        admission = _load_merge_retry_subjects(
            pool,
            profile_id=profile_id,
            workspace_id=workspace_id,
            task_id=task_id,
            retry_attempt_id=retry_attempt_id,
            expected_state_token=expected_state_token,
            now=now,
        )
        # --- Stage 2: external preflights, each with a guarded settlement. ---
        settled = _preflight_before_consumption(
            pool,
            github,
            admission=admission,
            profile_id=profile_id,
            expected_state_token=expected_state_token,
        )
        if settled is not None:
            return settled
        # --- Stage 3: atomic consumption + final revalidation. ---
        with composed_transaction(pool) as transaction_pool:
            consumed_admission = _consume_authorized_merge_retry(
                transaction_pool,
                profile_id=profile_id,
                workspace_id=workspace_id,
                task_id=task_id,
                retry_attempt_id=retry_attempt_id,
                expected_state_token=expected_state_token,
                now=now,
            )
        if attempt_evidence is not None:
            # The F5b coordinator's handoff fact: this invocation's own
            # consumed retry-attempt identity — its one-shot consumption stamp
            # is the durable binding, and the effect identity is the attempt's
            # own UUID (the merge family has no resulting Execution). F5b's
            # durable binding lookup turns a later failure into F4's
            # PRIOR_AUTHORIZED report; this service duplicates none of that
            # reporting logic for the send path.
            attempt_evidence.record_attempt(
                family=GitHubMergeOperation.EXACT_HEAD_MERGE,
                original_attempt_kind=OriginalAttemptKind.MERGE_DECISION_GATE,
                original_attempt_id=consumed_admission.attempt.id,
                effect_attempt_id=consumed_admission.attempt.id,
                phase=_PHASE_PREFLIGHT,
            )
        # --- Stage 4: the single send, then truthful disposition. ---
        if attempt_evidence is not None:
            attempt_evidence.record_attempt(
                family=GitHubMergeOperation.EXACT_HEAD_MERGE,
                original_attempt_kind=OriginalAttemptKind.MERGE_DECISION_GATE,
                original_attempt_id=consumed_admission.attempt.id,
                effect_attempt_id=consumed_admission.attempt.id,
                phase=_PHASE_SEND,
            )
        try:
            merge_result = pull_request_merge.request_pull_request_merge(
                pool,
                github,
                user_token_resolver,
                profile_id=profile_id,
                workspace_id=workspace_id,
                task_id=task_id,
                expected_head_sha=consumed_admission.subject_head_sha,
                claim_revalidation=_merge_claim_revalidation(
                    workspace_id=workspace_id,
                    task_id=task_id,
                    owner_gate_id=consumed_admission.episode.original_attempt_id,
                    expected_post_approval_token=expected_state_token,
                    task_pull_request_id=consumed_admission.task_pull_request_id,
                    expected_head_sha=consumed_admission.subject_head_sha,
                ),
            )
        except MergeConfirmedReconciliationPendingError as error:
            # GitHub already answered MERGED; persistence/reconciliation is
            # pending. The existing known-success block semantics apply: the
            # evidence is preserved, the mutation is never repeated, and the
            # delayed webhook/sweep confirmation takes the completion seam.
            return _classify_after_unconfirmed_merge(
                pool,
                github,
                profile_id=profile_id,
                workspace_id=workspace_id,
                task_id=task_id,
                owner_gate_id=consumed_admission.episode.original_attempt_id,
                expected_state_token=expected_state_token,
                task_pull_request_id=consumed_admission.task_pull_request_id,
                expected_head_sha=consumed_admission.subject_head_sha,
                merge_commit_sha=error.merge_commit_sha,
            )
        except pull_request_merge.MergeRequestSendUncertainError:
            # The merge mutation send's OWN unknown outcome (F5d): the F5b
            # coordinator's typed handoff owns the PRIOR_AUTHORIZED report —
            # only this consumed attempt's failure is reportable, eligibility
            # is F4's, and the original exception always propagates unchanged.
            raise
        except ExternalOperationUncertainError:
            # A plain uncertain outcome from the primitive is a pre-send
            # read/credential condition, never merge-send evidence: the
            # consumed attempt's failure is reported truthfully to the
            # episode, which escalates (fail closed).
            return _settle_presend_uncertainty(
                pool,
                admission=consumed_admission,
                expected_state_token=expected_state_token,
            )
        except ExternalOperationRejectedError:
            # A GitHub-owned definitive refusal keeps E10a's exact semantics:
            # the refusal provenance plus the fresh pending exact-head
            # MERGE_DECISION gate (fresh Owner authorization; the episode
            # stays open-unescalated truthful history).
            return _settle_after_definitive_no_merge(
                pool,
                github,
                profile_id=profile_id,
                workspace_id=workspace_id,
                task_id=task_id,
                owner_gate_id=consumed_admission.episode.original_attempt_id,
                expected_state_token=expected_state_token,
                task_pull_request_id=consumed_admission.task_pull_request_id,
                subject_head_sha=consumed_admission.subject_head_sha,
                github_rejected=True,
            )
        except (ExternalOperationFailedError, AuthorizationError):
            # Plain known failures are pre-delivery conditions: credential
            # resolution, authentication/authorization rejection, rate
            # limiting. Proven non-delivery — never a GitHub-owned refusal.
            return _settle_after_definitive_no_merge(
                pool,
                github,
                profile_id=profile_id,
                workspace_id=workspace_id,
                task_id=task_id,
                owner_gate_id=consumed_admission.episode.original_attempt_id,
                expected_state_token=expected_state_token,
                task_pull_request_id=consumed_admission.task_pull_request_id,
                subject_head_sha=consumed_admission.subject_head_sha,
                github_rejected=False,
            )
        except StaleOperationError:
            return _classify_after_stale_merge_request(
                pool,
                github,
                profile_id=profile_id,
                workspace_id=workspace_id,
                task_id=task_id,
                owner_gate_id=consumed_admission.episode.original_attempt_id,
                expected_state_token=expected_state_token,
                task_pull_request_id=consumed_admission.task_pull_request_id,
                subject_head_sha=consumed_admission.subject_head_sha,
            )
        # Known success: the authoritative completion seam.
        completion = complete_confirmed_task_merge(pool, workspace_id=workspace_id, task_id=task_id)
        return TaskMergeOutcome(
            kind=TaskMergeOutcomeKind.COMPLETED,
            task=completion.task,
            pull_request=completion.pull_request,
            merge_commit_sha=merge_result.merge_commit_sha,
        )
