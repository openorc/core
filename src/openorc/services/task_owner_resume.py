"""F8g: the Owner Resume action for the resolved OWNER_ABORT episode (issue #292).

``resume_task_after_abort`` is the ONE Owner-authorized Resume command for a
Task blocked by an exactly-evidenced Owner Abort: it reauthorizes the
original Task's suspended work by atomically resolving the exact current
pending ``OWNER_ABORT`` gate with truthful OWNER provenance, resolving its
sole unresolved ``owner_action_required`` TaskBlock, restoring the guarded
pre-abort Task status under the rotated token, recording the truthful OWNER
audit, and committing the one-shot session-resume record — the durable
pre-dispatch guard that makes the qualified same-session continuation
(``ClineAdapter.resume_activity``, issue #300) exactly-once across crashes,
concurrent commands, and worker delays. The original gate and block remain
resolved history whatever subsequently happens to the runtime continuation;
a failed Resume is a NEW operational failure of the resume continuation,
never a reason to reopen them.

``dispatch_authorized_task_resume`` is the authorized continuation the
action hands off through the existing operation-invocation boundary. It
re-derives everything from committed state, claims the record one-shot
under the Task row lock (or records the proven never-dispatched
supersession when a newer Owner decision re-blocked the Task first),
dispatches the adapter-owned fixed continuation prompt to the exact
original session strictly outside any transaction, and routes the resumed
turn's formal result through the original workflow operation's EXISTING
completion logic — the same logical operation continuing, never a
replacement Execution, ReviewIteration, session, or dispatcher.

The command has exactly three atomic boundaries:

- **Authorization** (one short composed transaction, database only): the
  account-operational barrier, the exact Workspace/Task Owner scope, the
  locked non-archived ``blocked`` Task under the caller's token, the exact
  current pending ``OWNER_ABORT`` gate, its sole unresolved TaskBlock with
  coherent ``owner_abort`` evidence, the exact interrupted session
  identities re-derived from the block context against the durable READY
  bindings, every Interrupt intent bound to that block positively SETTLED,
  no independently outstanding or uncertain runtime work beyond the
  derived subject, and the result-routing derivation — the one durably
  identifiable interrupted workflow operation with its expected formal
  response family and existing result owner. Ambiguous, stale, or unsafe
  state refuses the whole action: nothing is applied and the Task stays
  ``blocked``.
- **Dispatch claim** (one short composed transaction under the Task row
  lock): currentness is re-proven against the record's recorded
  authorization token and restored status; a current record claims
  ``authorized`` → ``dispatched`` exactly once, while a moved-on Task
  positively establishes that the Resume was never dispatched (the claim
  is the only path to the adapter and it never committed) and records the
  terminal ``superseded`` outcome without touching the newer episode.
- **Result routing** (the owning service's own transaction discipline): the
  resumed formal result completes the SAME Execution, ReviewIteration,
  dispatch claim, or bootstrap stage through that service's existing
  finalize functions, and the record settles exactly once with the
  processed result.

Operational failures of the continuation are Level 1 failures of the
``TASK_SESSION_RESUME`` operation family: the coordinator reports them
against the record's durable identity and the existing Level 2 triage
escalates them through the standard Type B ``BLOCK_RESOLUTION`` fallback.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from contextlib import AbstractContextManager
from dataclasses import dataclass
from enum import StrEnum
from typing import TYPE_CHECKING, Final, Protocol, cast, runtime_checkable
from uuid import UUID

from openorc.adapters.agent_runtime.contract import AgentRuntimeAdapter
from openorc.adapters.agent_runtime.errors import (
    AgentRuntimeConfigurationRejectedError,
    AgentRuntimeError,
    AgentRuntimeInterruptedTurnError,
    AgentRuntimeUnavailableError,
    AgentSessionNotFoundError,
)
from openorc.adapters.agent_runtime.results import AgentTextResponse
from openorc.adapters.github import GitHubAppClient
from openorc.domain.blocking_cases import (
    BlockingCaseDomainError,
    Level3BlockingCondition,
    validate_blocking_case_evidence,
)
from openorc.domain.connections import WorkflowRole
from openorc.domain.executions import ExecutionStatus
from openorc.domain.gates import OwnerGate, OwnerGateStatus, OwnerGateType
from openorc.domain.intents import (
    TaskInterruptIntentStatus,
    TaskResumeIntent,
    TaskResumeIntentStatus,
)
from openorc.domain.operational_failures import (
    OriginalAttemptKind,
    PRAuthorizationPresentationOperation,
)
from openorc.domain.pending_interactions import PendingTaskRuntimeInteractionOperation
from openorc.domain.reviews import ReviewLoopPurpose, ReviewLoopStatus
from openorc.domain.send_claims import TaskRuntimeSendClaimKind, TaskRuntimeSendClaimStatus
from openorc.domain.sessions import TaskAgentSession, TaskSessionLifecycleStatus
from openorc.domain.tasks import Task, TaskStatus
from openorc.observability import annotate_span, application_span
from openorc.persistence import (
    blocks as block_records,
)
from openorc.persistence import (
    executions as execution_records,
)
from openorc.persistence import (
    reviews as review_records,
)
from openorc.persistence import (
    send_claims as send_claim_records,
)
from openorc.persistence import (
    sessions as session_records,
)
from openorc.persistence import (
    tasks as task_records,
)
from openorc.persistence.gates import resolve_owner_gate as resolve_gate_records
from openorc.persistence.intents import (
    claim_task_resume_dispatch,
    create_task_resume_intent,
    get_task_resume_intent,
    list_task_interrupt_intents,
    record_task_resume_intent_outcome,
    supersede_task_resume_intent,
)
from openorc.persistence.pool import DatabasePool
from openorc.persistence.pull_requests import get_task_pull_request_for_task
from openorc.persistence.tasks import get_task_planning_bootstrap_evidence
from openorc.protocol.models import (
    IMPLEMENTATION_RESULT_FAMILY,
    PLAN_RESULT_FAMILY,
    REVIEW_RESULT_FAMILY,
    WORKSPACE_RESULT_FAMILY,
    FormalResponse,
    ImplementationResult,
    PlanResult,
    ReviewResult,
    WorkspaceResult,
)
from openorc.services import event_coordination
from openorc.services.errors import (
    ConflictError,
    ExternalOperationFailedError,
    ExternalOperationUncertainError,
    InvalidCommandError,
    NotFoundError,
    StaleOperationError,
)
from openorc.services.event_coordination import owner_actor
from openorc.services.operation_attempt_evidence import InvocationAttemptEvidence
from openorc.services.profile_lifecycle_guard import require_account_operational
from openorc.services.task_abort import settle_observed_task_interrupt
from openorc.services.task_authorized_implementation_execution import (
    complete_resumed_implementation_result,
)
from openorc.services.task_implementation_authorization import require_resolution_applied
from openorc.services.task_plan_production import (
    complete_resumed_initial_plan_result,
    complete_resumed_plan_revision_result,
    produce_initial_task_plan,
)
from openorc.services.task_planning_bootstrap import (
    TaskBootstrapRuntimeComposition,
    complete_resumed_bootstrap_prepare,
)
from openorc.services.task_planning_review_decision import (
    TaskPlanningReviewDispatchOutcome,
    finalize_planning_review_decision,
)
from openorc.services.task_pr_authorization import open_pr_authorization
from openorc.services.task_pr_remediation import (
    complete_resumed_pr_remediation_result,
    continue_resumed_pr_remediation_chain,
)
from openorc.services.task_pr_review_decision import (
    TaskPrReviewDispatchOutcome,
    finalize_pr_review_decision,
)
from openorc.services.task_subject_guards import (
    require_current_owner_gate,
    require_current_task,
    require_owner_gate_unresolved_block_subject,
)
from openorc.services.transaction_composition import composed_transaction
from openorc.services.workspace_authorization import require_workspace_task

if TYPE_CHECKING:
    from openorc.domain.blocks import TaskBlock
    from openorc.services.task_planning_bootstrap import TaskBootstrapRuntimeComposition

__all__ = [
    "AuthorizedTaskResume",
    "SessionResumeOutcome",
    "SessionResumeOutcomeKind",
    "dispatch_authorized_task_resume",
    "resume_task_after_abort",
]

logger = logging.getLogger(__name__)

_SERVICE_TRACER_SCOPE: Final[str] = "openorc.services.task_owner_resume"
_RESUME_SPAN_NAME: Final[str] = "task_owner_resume.resume_task_after_abort"
_DISPATCH_SPAN_NAME: Final[str] = "task_owner_resume.dispatch_authorized_task_resume"

ProducerRuntimeAttachmentFactory = Callable[[], AbstractContextManager[AgentRuntimeAdapter]]
ReviewerRuntimeAttachmentFactory = Callable[
    [TaskAgentSession], AbstractContextManager[AgentRuntimeAdapter]
]


@runtime_checkable
class _ResumeCapableRuntime(Protocol):
    """The narrow resume-capable runtime surface the continuation dispatches.

    The qualified same-session Resume seam (issue #300) as this runtime-
    neutral continuation consumes it: one exact-session fixed-prompt
    dispatch with the shared formal-validation funnel. Concrete adapters own
    the mechanics; the protocol keeps the service honest about the narrow
    surface it uses.
    """

    def resume_activity(
        self, session_id: str, *, expected_family: str | None = None
    ) -> FormalResponse | AgentTextResponse: ...


@dataclass(frozen=True, slots=True)
class AuthorizedTaskResume:
    """The one atomic effect of a successful Owner Resume authorization.

    ``task`` is the post-authorization Task in its restored workflow status
    under the rotated token; ``resolved_owner_gate`` is the exceptional
    ``OWNER_ABORT`` gate resolved APPROVED with truthful OWNER provenance;
    ``resolved_task_block`` is the resolved blocking condition whose
    evidence stays retained untouched; ``resume`` is the committed one-shot
    session-resume record whose dispatch the submitted continuation owns.
    """

    task: Task
    resolved_owner_gate: OwnerGate
    resolved_task_block: TaskBlock
    resume: TaskResumeIntent


@dataclass(frozen=True, slots=True)
class _ResumeTarget:
    """One exact interrupted session binding re-derived from the block context."""

    session: TaskAgentSession


@dataclass(frozen=True, slots=True)
class _ResumeRouting:
    """The minimal result-routing facts of one interrupted workflow operation.

    Association only, never reconstruction: which existing durable operation
    the session's settled interruption belongs to, the expected formal
    response family its original send used, the exact durable subject
    reference, and the restored Task status the continuation presents from.
    """

    session: TaskAgentSession
    operation: PendingTaskRuntimeInteractionOperation
    expected_family: str
    subject_kind: str
    subject_id: UUID
    restore_status: TaskStatus

    def routing_context(self, *, authorized_state_token: UUID) -> dict[str, object]:
        """The immutable safe machine context written on the resume record."""
        return {
            "control": "owner_resume",
            "operation": self.operation.value,
            "expected_response_family": self.expected_family,
            "subject_kind": self.subject_kind,
            "subject_id": str(self.subject_id),
            "restored_status": self.restore_status.value,
            "authorized_state_token": str(authorized_state_token),
        }


def _require_uuid_command(value: object, name: str) -> UUID:
    """Reject a malformed command identifier before any state is touched."""
    if not isinstance(value, UUID):
        raise InvalidCommandError(f"{name} must be a UUID")
    return value


def _resolve_resume_targets(
    transaction_pool: DatabasePool,
    *,
    task: Task,
    context: TaskBlock,
) -> tuple[_ResumeTarget, ...]:
    """Re-derive the exact interrupted session bindings from the block context.

    The ``owner_abort`` evidence's parallel ``target_roles`` and
    ``task_agent_session_ids`` facts name the exact internal bindings the
    Abort interrupted; each is cross-checked against the durable Task/role
    binding — existing, in scope, still READY, and carrying its immutable
    external session identity. A moved, lost, or ended binding refuses the
    whole action.
    """
    roles = context.context.get("target_roles")
    session_ids = context.context.get("task_agent_session_ids")
    if not isinstance(roles, list) or not isinstance(session_ids, list):
        raise ConflictError("the owner_abort block context carries incoherent target facts")
    if len(roles) != len(session_ids) or not roles:
        raise ConflictError("the owner_abort block context carries incoherent target facts")
    targets: list[_ResumeTarget] = []
    for role_value, session_id_value in zip(roles, session_ids, strict=True):
        if not isinstance(role_value, str) or not isinstance(session_id_value, str):
            raise ConflictError("the owner_abort block context carries incoherent target facts")
        try:
            role = WorkflowRole(role_value)
            session_id = UUID(session_id_value)
        except ValueError as error:
            raise ConflictError(
                "the owner_abort block context carries incoherent target facts"
            ) from error
        session = session_records.get_task_agent_session(
            transaction_pool, task_id=task.id, role=role
        )
        if (
            session is None
            or session.id != session_id
            or session.task_id != task.id
            or session.workspace_id != task.workspace_id
        ):
            raise ConflictError("the interrupted session binding is not available for this task")
        if session.lifecycle_status is not TaskSessionLifecycleStatus.READY:
            raise ConflictError("the interrupted session binding is not a ready external session")
        if session.external_session_id is None or not session.external_session_id.strip():
            raise ConflictError(
                "the interrupted session binding carries no external session identity"
            )
        targets.append(_ResumeTarget(session=session))
    return tuple(targets)


def _require_episode_interruptions_settled(
    transaction_pool: DatabasePool,
    *,
    task: Task,
    block: TaskBlock,
    targets: tuple[_ResumeTarget, ...],
) -> None:
    """Require every episode interruption positively settled, once per target.

    The reauthorization precondition: each interrupted target session's
    Interrupt intent bound to THIS blocking episode must be positively
    ``settled`` — the D6b precondition the continuation's own adapter
    discipline also enforces. Any unsettled, failed, or unknown intent, or
    any target without its episode settlement, keeps the whole action
    refused and the Task blocked.
    """
    intents = list_task_interrupt_intents(transaction_pool, task_id=task.id)
    settled_sessions = {
        intent.task_agent_session_id
        for intent in intents
        if intent.status is TaskInterruptIntentStatus.SETTLED and intent.task_block_id == block.id
    }
    for target in targets:
        if target.session.id not in settled_sessions:
            raise ConflictError(
                "the addressed session's activity interruption is not positively "
                "settled for this blocking episode; the resume action is refused"
            )


def _derive_resume_routing(
    transaction_pool: DatabasePool,
    *,
    task: Task,
    targets: tuple[_ResumeTarget, ...],
) -> _ResumeRouting:
    """Derive the one durably identifiable interrupted workflow operation.

    Association only, from the same durable facts the Abort used to admit
    the interruption: a Producer target qualifies through the bootstrap's
    outstanding PREPARE control, a conservatively claimed F2 planning
    dispatch, or its anchored OPEN Execution; a Reviewer target through an
    OPEN ReviewLoop's unfinalized ReviewIteration. Sessions whose settled
    interruption corresponds to no durable workflow operation are never
    dispatched — there is no result to route and no work to continue, and a
    spurious continuation turn would be a gratuitous runtime effect. More
    than one identifiable operation is an unexpected durable state (the
    workflow's own sequencing keeps them mutually exclusive) and fails
    closed; so does zero, with Cancel remaining the safe accounted exit.
    """
    candidates: list[_ResumeRouting] = []
    for target in targets:
        candidates.extend(_derive_session_candidates(transaction_pool, task=task, target=target))
    if not candidates:
        raise ConflictError(
            "no durably identifiable interrupted workflow operation remains to "
            "resume; the episode's work is accounted and the task can only be "
            "cancelled through its gate"
        )
    if len(candidates) > 1:
        raise ConflictError(
            "multiple interrupted workflow operations claim continuation; the "
            "durable state is unsafe and the resume action is refused"
        )
    return candidates[0]


def _derive_session_candidates(
    transaction_pool: DatabasePool,
    *,
    task: Task,
    target: _ResumeTarget,
) -> list[_ResumeRouting]:
    """Derive one session's resumable-operation candidates from durable facts."""
    session = target.session
    if session.role is WorkflowRole.REVIEWER:
        return _derive_reviewer_candidates(transaction_pool, task=task, session=session)
    return _derive_producer_candidates(transaction_pool, task=task, session=session)


def _derive_producer_candidates(
    transaction_pool: DatabasePool,
    *,
    task: Task,
    session: TaskAgentSession,
) -> list[_ResumeRouting]:
    """Derive a Producer session's resumable-operation candidates.

    An anchored OPEN Execution is the implementation/remediation candidate —
    but only a genuinely RUNNING interrupted attempt: a queued or
    paused/paused-for-approval Execution is awaiting its own correlation,
    not an interrupted turn, and fails closed. The bootstrap's outstanding
    PREPARE control (claimed, stage still unset) and a conservatively
    claimed F2 planning dispatch are the planning-phase candidates; they
    are mutually exclusive by the workflow's own stage sequencing.
    """
    candidates: list[_ResumeRouting] = []
    for execution in execution_records.list_task_executions(transaction_pool, task_id=task.id):
        if execution.producer_session_id != session.id:
            continue
        if execution.status not in (
            ExecutionStatus.RUNNING,
            ExecutionStatus.QUEUED,
            ExecutionStatus.PAUSED,
            ExecutionStatus.PAUSED_FOR_APPROVAL,
        ):
            continue
        if execution.status is not ExecutionStatus.RUNNING:
            raise ConflictError(
                "the open execution is not a running interrupted attempt; the "
                "resume action cannot continue it"
            )
        if execution.review_iteration_id is None:
            candidates.append(
                _ResumeRouting(
                    session=session,
                    operation=PendingTaskRuntimeInteractionOperation.IMPLEMENTATION_EXECUTION,
                    expected_family=IMPLEMENTATION_RESULT_FAMILY,
                    subject_kind="execution",
                    subject_id=execution.id,
                    restore_status=TaskStatus.IMPLEMENTING,
                )
            )
        else:
            candidates.append(
                _ResumeRouting(
                    session=session,
                    operation=PendingTaskRuntimeInteractionOperation.PR_REMEDIATION_EXECUTION,
                    expected_family=IMPLEMENTATION_RESULT_FAMILY,
                    subject_kind="execution",
                    subject_id=execution.id,
                    restore_status=TaskStatus.REVIEWING,
                )
            )
    evidence = get_task_planning_bootstrap_evidence(transaction_pool, task_id=task.id)
    if evidence is not None and evidence.stage is None:
        candidates.append(
            _ResumeRouting(
                session=session,
                operation=PendingTaskRuntimeInteractionOperation.PLANNING_BOOTSTRAP_PREPARE,
                expected_family=WORKSPACE_RESULT_FAMILY,
                subject_kind="planning_bootstrap_claim",
                subject_id=evidence.claim_token,
                restore_status=TaskStatus.PLANNING,
            )
        )
    for claim in send_claim_records.list_task_runtime_send_claims(
        transaction_pool, task_id=task.id
    ):
        if claim.status is not TaskRuntimeSendClaimStatus.CLAIMED:
            continue
        if claim.kind is TaskRuntimeSendClaimKind.INITIAL_PLAN_DISPATCH:
            candidates.append(
                _ResumeRouting(
                    session=session,
                    operation=PendingTaskRuntimeInteractionOperation.INITIAL_PLAN_DISPATCH,
                    expected_family=PLAN_RESULT_FAMILY,
                    subject_kind="runtime_send_claim",
                    subject_id=claim.id,
                    restore_status=TaskStatus.PLANNING,
                )
            )
        elif claim.kind is TaskRuntimeSendClaimKind.PLANNING_REVISE_DISPATCH:
            candidates.append(
                _ResumeRouting(
                    session=session,
                    operation=PendingTaskRuntimeInteractionOperation.PLANNING_REVISE_DISPATCH,
                    expected_family=PLAN_RESULT_FAMILY,
                    subject_kind="runtime_send_claim",
                    subject_id=claim.id,
                    restore_status=TaskStatus.PLANNING,
                )
            )
    return candidates


def _derive_reviewer_candidates(
    transaction_pool: DatabasePool,
    *,
    task: Task,
    session: TaskAgentSession,
) -> list[_ResumeRouting]:
    """Derive a Reviewer session's resumable-operation candidates.

    An OPEN ReviewLoop carrying an unfinalized ReviewIteration is the
    dispatched review turn still in flight — the planning loop's iteration
    is the E3b planning review and the PR loop's is the E9a PR review, each
    resuming with its own expected review-result family and restore status.
    """
    candidates: list[_ResumeRouting] = []
    loops = review_records.list_task_review_loops(
        transaction_pool, task_id=task.id, status=ReviewLoopStatus.OPEN
    )
    for loop in loops:
        iterations = review_records.list_review_loop_iterations(
            transaction_pool, review_loop_id=loop.id
        )
        for iteration in iterations:
            if iteration.outcome is not None:
                continue
            restore_status = (
                TaskStatus.PLANNING
                if loop.purpose is ReviewLoopPurpose.PLANNING
                else TaskStatus.REVIEWING
            )
            candidates.append(
                _ResumeRouting(
                    session=session,
                    operation=PendingTaskRuntimeInteractionOperation.REVIEWER_REVIEW,
                    expected_family=REVIEW_RESULT_FAMILY,
                    subject_kind="review_iteration",
                    subject_id=iteration.id,
                    restore_status=restore_status,
                )
            )
    return candidates


def _require_no_independent_outstanding_work(
    transaction_pool: DatabasePool,
    *,
    task: Task,
    routing: _ResumeRouting,
) -> None:
    """Refuse independently outstanding or uncertain runtime work.

    The derived subject IS the suspended work the Resume continues; every
    other durable outstanding-work family must be accounted before any
    reauthorization: another open Execution, a pending RuntimeRequest, a
    conservatively claimed F2 dispatch beyond the subject, an unsettled
    bootstrap stage beyond the subject, and an unfinalized ReviewIteration
    beyond the subject. The enumeration mirrors the cancellation guard's
    durable-fact families with the classified subject exempted.
    """
    subject_execution = routing.subject_id if routing.subject_kind == "execution" else None
    subject_claim = routing.subject_id if routing.subject_kind == "runtime_send_claim" else None
    subject_iteration = routing.subject_id if routing.subject_kind == "review_iteration" else None
    subject_bootstrap = routing.operation is (
        PendingTaskRuntimeInteractionOperation.PLANNING_BOOTSTRAP_PREPARE
    )
    for execution in execution_records.list_task_executions(transaction_pool, task_id=task.id):
        if execution.status in _OPEN_EXECUTION_STATUSES and execution.id != subject_execution:
            raise ConflictError(
                "the task carries an independently outstanding open execution; "
                "the resume action is refused"
            )
    for claim in send_claim_records.list_task_runtime_send_claims(
        transaction_pool, task_id=task.id
    ):
        if claim.status is TaskRuntimeSendClaimStatus.CLAIMED and claim.id != subject_claim:
            raise ConflictError(
                "the task carries an independently outstanding dispatch claim; "
                "the resume action is refused"
            )
    evidence = get_task_planning_bootstrap_evidence(transaction_pool, task_id=task.id)
    if evidence is not None and evidence.stage is None and not subject_bootstrap:
        raise ConflictError(
            "the task's planning bootstrap is independently outstanding; "
            "the resume action is refused"
        )
    for loop in review_records.list_task_review_loops(
        transaction_pool, task_id=task.id, status=ReviewLoopStatus.OPEN
    ):
        for iteration in review_records.list_review_loop_iterations(
            transaction_pool, review_loop_id=loop.id
        ):
            if iteration.outcome is None and iteration.id != subject_iteration:
                raise ConflictError(
                    "the task carries an independently outstanding review turn; "
                    "the resume action is refused"
                )


_OPEN_EXECUTION_STATUSES: Final[frozenset[ExecutionStatus]] = frozenset(
    {
        ExecutionStatus.QUEUED,
        ExecutionStatus.RUNNING,
        ExecutionStatus.PAUSED,
        ExecutionStatus.PAUSED_FOR_APPROVAL,
    }
)


def resume_task_after_abort(
    pool: DatabasePool,
    *,
    profile_id: UUID,
    workspace_id: UUID,
    task_id: UUID,
    owner_gate_id: UUID,
    expected_state_token: UUID,
) -> AuthorizedTaskResume:
    """Reauthorize the original Task's suspended work after an Owner Abort.

    The ONE Owner-authorized Resume command for the current pending
    ``OWNER_ABORT`` gate (issue #292): one short composed transaction proves
    the account/Owner/currentness/episode guards, derives the one durably
    identifiable interrupted workflow operation with its result routing,
    and atomically resolves the gate APPROVED with truthful OWNER
    provenance, resolves its sole unresolved TaskBlock, restores the guarded
    pre-abort Task status under the rotated token, records the truthful
    OWNER audit, and commits the one-shot session-resume record. Nothing is
    dispatched here: the authorized continuation is submitted through the
    executing process's operation-invocation seam after this composition
    commits. Unsafe, stale, or unclassifiable state refuses the whole
    action and the Task stays ``blocked``.
    """
    _require_uuid_command(profile_id, "profile_id")
    _require_uuid_command(workspace_id, "workspace_id")
    _require_uuid_command(task_id, "task_id")
    _require_uuid_command(owner_gate_id, "owner_gate_id")
    _require_uuid_command(expected_state_token, "expected_state_token")
    with application_span(_SERVICE_TRACER_SCOPE, _RESUME_SPAN_NAME) as span:
        annotate_span(
            span,
            operation=_RESUME_SPAN_NAME,
            workspace_id=str(workspace_id),
            task_id=str(task_id),
        )
        with composed_transaction(pool) as transaction_pool:
            require_account_operational(transaction_pool, profile_id=profile_id)
            require_workspace_task(
                transaction_pool,
                profile_id=profile_id,
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
            if task.status is not TaskStatus.BLOCKED:
                raise ConflictError(
                    "the addressed task is not blocked; the resume action is not available"
                )
            gate = require_current_owner_gate(
                transaction_pool, task=task, owner_gate_id=owner_gate_id
            )
            if gate.gate_type is not OwnerGateType.OWNER_ABORT:
                raise ConflictError("only the current owner_abort gate resumes through this seam")
            block = require_owner_gate_unresolved_block_subject(
                transaction_pool, task=task, gate=gate
            )
            try:
                validate_blocking_case_evidence(
                    Level3BlockingCondition.OWNER_ABORT_ISSUED, block.context
                )
            except BlockingCaseDomainError as error:
                raise ConflictError(
                    "the owner_abort block carries incoherent episode evidence"
                ) from error
            targets = _resolve_resume_targets(transaction_pool, task=task, context=block)
            _require_episode_interruptions_settled(
                transaction_pool, task=task, block=block, targets=targets
            )
            routing = _derive_resume_routing(transaction_pool, task=task, targets=targets)
            _require_no_independent_outstanding_work(transaction_pool, task=task, routing=routing)
            resolution = resolve_gate_records(
                transaction_pool,
                owner_gate_id=gate.id,
                outcome=OwnerGateStatus.APPROVED,
                expected_task_state_token=task.state_token,
            )
            resolved_gate, gate_task = require_resolution_applied(resolution)
            resolved_block = block_records.resolve_task_block(
                transaction_pool, task_block_id=block.id
            )
            if resolved_block is None:
                raise StaleOperationError(
                    "the owner_abort block has already resolved; the resume action is stale"
                )
            restored = task_records.restore_blocked_task_status(
                transaction_pool,
                task_id,
                expected_state_token=gate_task.state_token,
                restored_status=routing.restore_status,
            )
            if restored is None:
                raise StaleOperationError(
                    "the blocked task could not be restored; the resume action is stale"
                )
            event_coordination.record_owner_gate_resolved_event(
                transaction_pool,
                workspace_id=workspace_id,
                task_id=task_id,
                owner_gate_id=resolved_gate.id,
                actor=owner_actor(profile_id),
                outcome=OwnerGateStatus.APPROVED,
            )
            resume_record = create_task_resume_intent(
                transaction_pool,
                workspace_id=workspace_id,
                task_id=task_id,
                task_agent_session_id=routing.session.id,
                task_block_id=block.id,
                workflow_role=routing.session.role,
                external_session_id=routing.session.external_session_id or "",
                requested_by_profile_id=profile_id,
                routing=routing.routing_context(authorized_state_token=restored.state_token),
            )
        return AuthorizedTaskResume(
            task=restored,
            resolved_owner_gate=resolved_gate,
            resolved_task_block=resolved_block,
            resume=resume_record,
        )


class SessionResumeOutcomeKind(StrEnum):
    """The typed outcome of one authorized session-resume continuation."""

    COMPLETED = "completed"
    INTERRUPTED = "interrupted"
    SUPERSEDED = "superseded"


@dataclass(frozen=True, slots=True)
class SessionResumeOutcome:
    """The typed outcome of one authorized session-resume continuation.

    ``completed`` means the resumed turn's formal result was routed through
    its original workflow operation's completion logic and the record
    settled; ``interrupted`` means the runtime positively terminated the
    resumed turn by interruption (a newer Owner decision's episode governs);
    ``superseded`` means the record was proven never-dispatched and was
    terminally superseded before any runtime effect. Failures raise the
    typed operational errors for the coordinator's Level 2 handoff.
    """

    kind: SessionResumeOutcomeKind
    resume: TaskResumeIntent
    detail: str


def _resume_routing_from_record(
    record: TaskResumeIntent,
) -> tuple[PendingTaskRuntimeInteractionOperation, UUID, TaskStatus, UUID]:
    """Decode the record's immutable routing context, failing closed."""
    routing = record.routing
    operation_value = routing.get("operation")
    subject_value = routing.get("subject_id")
    status_value = routing.get("restored_status")
    token_value = routing.get("authorized_state_token")
    if (
        not isinstance(operation_value, str)
        or not isinstance(subject_value, str)
        or not isinstance(status_value, str)
        or not isinstance(token_value, str)
    ):
        raise ConflictError("the session-resume record carries incoherent routing context")
    try:
        operation = PendingTaskRuntimeInteractionOperation(operation_value)
        subject_id = UUID(subject_value)
        restored_status = TaskStatus(status_value)
        authorized_token = UUID(token_value)
    except ValueError as error:
        raise ConflictError(
            "the session-resume record carries incoherent routing context"
        ) from error
    return operation, subject_id, restored_status, authorized_token


def dispatch_authorized_task_resume(
    pool: DatabasePool,
    *,
    workspace_id: UUID,
    task_id: UUID,
    task_resume_intent_id: UUID,
    profile_id: UUID,
    expected_state_token: UUID,
    producer_runtime_factory: ProducerRuntimeAttachmentFactory | None,
    reviewer_runtime_factory: ReviewerRuntimeAttachmentFactory | None,
    github: GitHubAppClient,
    bootstrap_composition: TaskBootstrapRuntimeComposition | None,
    attempt_evidence: InvocationAttemptEvidence | None = None,
) -> SessionResumeOutcome:
    """Dispatch one authorized session-resume continuation, exactly once.

    The submitted TASK_SESSION_RESUME continuation (issue #292): one short
    composed transaction under the Task row lock re-proves currentness
    against the record's recorded authorization token and restored status
    and claims ``authorized`` → ``dispatched`` exactly once — or, when a
    newer Owner decision re-blocked the Task first, positively establishes
    the never-dispatched supersession and records the terminal
    ``superseded`` outcome without touching the newer episode. The adapter-
    owned fixed continuation prompt is then dispatched to the exact original
    session strictly outside any transaction, and the resumed turn's formal
    result routes through the original workflow operation's EXISTING
    completion logic. Definite pre-effect refusals record the terminal
    ``non_delivered`` outcome and propagate for the Level 2 escalation;
    uncertain outcomes leave the record conservatively ``dispatched`` and
    are never replayed; an interrupted resumed turn settles the newer
    episode's intent and records the terminal ``interrupted`` outcome.
    """
    _require_uuid_command(workspace_id, "workspace_id")
    _require_uuid_command(task_id, "task_id")
    _require_uuid_command(task_resume_intent_id, "task_resume_intent_id")
    _require_uuid_command(profile_id, "profile_id")
    _require_uuid_command(expected_state_token, "expected_state_token")
    with application_span(_SERVICE_TRACER_SCOPE, _DISPATCH_SPAN_NAME) as span:
        annotate_span(
            span,
            operation=_DISPATCH_SPAN_NAME,
            workspace_id=str(workspace_id),
            task_id=str(task_id),
        )
        # --- Boundary B: the one-shot dispatch claim (or the proven
        # pre-dispatch supersession), under the Task row lock. ---
        with composed_transaction(pool) as transaction_pool:
            locked = task_records.get_task_for_update(transaction_pool, task_id)
            if locked is None or locked.workspace_id != workspace_id:
                raise NotFoundError("the requested task is not available in this workspace")
            record = get_task_resume_intent(
                transaction_pool, task_resume_intent_id=task_resume_intent_id
            )
            if record is None or record.workspace_id != workspace_id or record.task_id != task_id:
                raise NotFoundError(
                    "the addressed session-resume record is not available for this task"
                )
            if record.status is not TaskResumeIntentStatus.AUTHORIZED:
                raise ConflictError(
                    "the session-resume record is not awaiting its one-shot dispatch"
                )
            if record.requested_by_profile_id != profile_id or record.routing.get(
                "authorized_state_token"
            ) != str(expected_state_token):
                raise StaleOperationError(
                    "the continuation's payload identity disagrees with the "
                    "session-resume record; the continuation is stale"
                )
            operation, subject_id, restored_status, authorized_token = _resume_routing_from_record(
                record
            )
            if (
                locked.archived_at is not None
                or locked.state_token != authorized_token
                or locked.status is not restored_status
            ):
                superseded = supersede_task_resume_intent(
                    transaction_pool,
                    task_resume_intent_id=record.id,
                    outcome={"supersession": "pre_dispatch"},
                )
                if superseded is None:
                    raise ConflictError(
                        "the session-resume record could not be terminally superseded; "
                        "the dispatch claim is stale"
                    )
                return SessionResumeOutcome(
                    kind=SessionResumeOutcomeKind.SUPERSEDED,
                    resume=superseded,
                    detail="pre_dispatch_supersession",
                )
            claimed = claim_task_resume_dispatch(transaction_pool, task_resume_intent_id=record.id)
            if claimed is None:
                raise ConflictError(
                    "the session-resume dispatch was claimed concurrently; the "
                    "continuation is stale"
                )
            if attempt_evidence is not None:
                attempt_evidence.record_task_token(locked.state_token)
                attempt_evidence.record_attempt(
                    family=PendingTaskRuntimeInteractionOperation.SESSION_RESUME,
                    original_attempt_kind=OriginalAttemptKind.OWNER_CONTROL_INTENT,
                    original_attempt_id=record.id,
                    phase="session_resume_dispatch",
                )

        # --- Boundary B2: the one fixed-prompt dispatch to the exact
        # original session (external, no transaction open). ---
        if record.workflow_role is WorkflowRole.PRODUCER:
            if producer_runtime_factory is None:
                raise InvalidCommandError(
                    "the continuation carries no producer runtime attachment factory"
                )
            factory_cm: Callable[[], AbstractContextManager[AgentRuntimeAdapter]] = (
                producer_runtime_factory
            )
        else:
            if reviewer_runtime_factory is None:
                raise InvalidCommandError(
                    "the continuation carries no reviewer runtime attachment factory"
                )
            reviewer_session = session_records.get_task_agent_session(
                pool, task_id=task_id, role=WorkflowRole.REVIEWER
            )
            if reviewer_session is None:
                raise ConflictError("the reviewer session binding is not available for this task")
            factory_cm = lambda: reviewer_runtime_factory(  # noqa: E731
                reviewer_session
            )
        expected_family_value = record.routing.get("expected_response_family")
        if not isinstance(expected_family_value, str) or not expected_family_value.strip():
            raise ConflictError("the session-resume record carries no expected response family")
        with factory_cm() as attached_adapter:
            try:
                resume_runtime = cast(_ResumeCapableRuntime, attached_adapter)
                response = resume_runtime.resume_activity(
                    record.external_session_id,
                    expected_family=expected_family_value,
                )
            except AgentRuntimeInterruptedTurnError as error:
                settle_observed_task_interrupt(
                    pool,
                    attached_adapter,
                    task_id=task_id,
                    external_session_id=record.external_session_id,
                )
                recorded = record_task_resume_intent_outcome(
                    pool,
                    task_resume_intent_id=record.id,
                    status=TaskResumeIntentStatus.INTERRUPTED,
                    outcome={"dispatch": "interrupted"},
                )
                if recorded is None:
                    raise StaleOperationError(
                        "the session-resume record could not record its interrupted "
                        "outcome; the dispatch classification is stale"
                    ) from error
                return SessionResumeOutcome(
                    kind=SessionResumeOutcomeKind.INTERRUPTED,
                    resume=recorded,
                    detail="resumed_turn_interrupted",
                )
            except (
                AgentSessionNotFoundError,
                AgentRuntimeUnavailableError,
                AgentRuntimeConfigurationRejectedError,
            ) as error:
                record_task_resume_intent_outcome(
                    pool,
                    task_resume_intent_id=record.id,
                    status=TaskResumeIntentStatus.NON_DELIVERED,
                    outcome={"dispatch": "non_delivered"},
                )
                raise ExternalOperationFailedError(
                    "the session-resume dispatch failed as a positively proven "
                    "pre-effect non-delivery; the record carries its terminal outcome"
                ) from error
            except AgentRuntimeError as error:
                raise ExternalOperationUncertainError(
                    "the outcome of the session-resume dispatch is unknown; the "
                    "resumed turn is never replayed"
                ) from error

            # --- Boundary C: route the resumed formal result through the
            # original workflow operation's existing completion logic. ---
            return _route_resumed_result(
                pool,
                attached_adapter,
                github,
                bootstrap_composition=bootstrap_composition,
                producer_runtime_factory=producer_runtime_factory,
                reviewer_runtime_factory=reviewer_runtime_factory,
                record=record,
                operation=operation,
                subject_id=subject_id,
                authorized_token=authorized_token,
                response=response,
                attempt_evidence=attempt_evidence,
            )


def _settle_resume_record(pool: DatabasePool, *, task_resume_intent_id: UUID) -> TaskResumeIntent:
    """Settle the record exactly once after its result was durably processed."""
    with composed_transaction(pool) as transaction_pool:
        recorded = record_task_resume_intent_outcome(
            transaction_pool,
            task_resume_intent_id=task_resume_intent_id,
            status=TaskResumeIntentStatus.SETTLED,
            outcome={"result": "processed"},
        )
    if recorded is None:
        raise StaleOperationError(
            "the session-resume record is no longer awaiting its settled outcome; "
            "the result routing is stale"
        )
    return recorded


def _route_resumed_result(
    pool: DatabasePool,
    adapter: AgentRuntimeAdapter,
    github: GitHubAppClient,
    *,
    bootstrap_composition: TaskBootstrapRuntimeComposition | None,
    producer_runtime_factory: ProducerRuntimeAttachmentFactory | None,
    reviewer_runtime_factory: ReviewerRuntimeAttachmentFactory | None,
    record: TaskResumeIntent,
    operation: PendingTaskRuntimeInteractionOperation,
    subject_id: UUID,
    authorized_token: UUID,
    response: object,
    attempt_evidence: InvocationAttemptEvidence | None,
) -> SessionResumeOutcome:
    """Route the resumed formal result to its original workflow operation.

    Association, not reconstruction: each supported operation hands the
    typed resumed result to the owning service's EXISTING completion logic —
    the same Execution, dispatch claim, ReviewIteration, or bootstrap claim
    the interruption suspended — and the record settles exactly once with
    the processed result. Post-completion continuations the ordinary flow
    performs (the PR-authorization presentation, the remediation chain, the
    bootstrap's first PLAN dispatch) re-enter their existing services with
    the completed subjects.
    """
    profile_id = record.requested_by_profile_id
    if operation is PendingTaskRuntimeInteractionOperation.IMPLEMENTATION_EXECUTION:
        if not isinstance(response, ImplementationResult):
            raise ExternalOperationUncertainError(
                "the resumed implementation turn did not return the validated "
                "implementation result family; the continuation remains unresolved"
            )
        with composed_transaction(pool) as transaction_pool:
            _task, execution = complete_resumed_implementation_result(
                transaction_pool,
                workspace_id=record.workspace_id,
                task_id=record.task_id,
                expected_state_token=authorized_token,
                execution_id=subject_id,
                result=response,
            )
            recorded = record_task_resume_intent_outcome(
                transaction_pool,
                task_resume_intent_id=record.id,
                status=TaskResumeIntentStatus.SETTLED,
                outcome={"result": "processed"},
            )
            if recorded is None:
                raise StaleOperationError(
                    "the session-resume record could not settle with its processed "
                    "result; the completion composition is stale"
                )
        if attempt_evidence is not None:
            attempt_evidence.record_attempt(
                family=PRAuthorizationPresentationOperation.EXACT_HEAD_PR_AUTHORIZATION_PRESENTATION,
                original_attempt_kind=OriginalAttemptKind.EXECUTION,
                original_attempt_id=execution.id,
                phase="resumed_implementation_presentation",
            )
        open_pr_authorization(
            pool,
            github,
            profile_id=profile_id,
            workspace_id=record.workspace_id,
            task_id=record.task_id,
            expected_state_token=_task.state_token,
            execution_id=execution.id,
        )
        return SessionResumeOutcome(
            kind=SessionResumeOutcomeKind.COMPLETED,
            resume=recorded,
            detail="implementation_completed",
        )
    if operation is PendingTaskRuntimeInteractionOperation.PR_REMEDIATION_EXECUTION:
        if not isinstance(response, ImplementationResult):
            raise ExternalOperationUncertainError(
                "the resumed remediation turn did not return the validated "
                "implementation result family; the continuation remains unresolved"
            )
        if reviewer_runtime_factory is None:
            raise InvalidCommandError(
                "the remediation continuation carries no reviewer attachment factory"
            )
        with composed_transaction(pool) as transaction_pool:
            _task, execution = complete_resumed_pr_remediation_result(
                transaction_pool,
                workspace_id=record.workspace_id,
                task_id=record.task_id,
                expected_state_token=authorized_token,
                execution_id=subject_id,
                result=response,
            )
            recorded = record_task_resume_intent_outcome(
                transaction_pool,
                task_resume_intent_id=record.id,
                status=TaskResumeIntentStatus.SETTLED,
                outcome={"result": "processed"},
            )
            if recorded is None:
                raise StaleOperationError(
                    "the session-resume record could not settle with its processed "
                    "result; the completion composition is stale"
                )
        continue_resumed_pr_remediation_chain(
            pool,
            github,
            cast(ReviewerRuntimeAttachmentFactory, reviewer_runtime_factory),
            workspace_id=record.workspace_id,
            task_id=record.task_id,
            profile_id=profile_id,
            finalized_execution=execution,
            result=response,
        )
        return SessionResumeOutcome(
            kind=SessionResumeOutcomeKind.COMPLETED,
            resume=recorded,
            detail="remediation_completed",
        )
    if operation is PendingTaskRuntimeInteractionOperation.PLANNING_BOOTSTRAP_PREPARE:
        if not isinstance(response, WorkspaceResult):
            raise ExternalOperationUncertainError(
                "the resumed bootstrap turn did not return the validated "
                "workspace result family; the continuation remains unresolved"
            )
        if bootstrap_composition is None:
            raise InvalidCommandError("the bootstrap continuation carries no runtime composition")
        preparation = complete_resumed_bootstrap_prepare(
            pool,
            bootstrap_composition,
            workspace_id=record.workspace_id,
            profile_id=profile_id,
            task_id=record.task_id,
            expected_state_token=authorized_token,
            result=response,
        )
        recorded = _settle_resume_record(pool, task_resume_intent_id=record.id)
        produce_initial_task_plan(
            pool,
            adapter,
            github,
            workspace_id=record.workspace_id,
            task_id=record.task_id,
            profile_id=profile_id,
            preparation=preparation,
            attempt_evidence=attempt_evidence,
        )
        return SessionResumeOutcome(
            kind=SessionResumeOutcomeKind.COMPLETED,
            resume=recorded,
            detail="bootstrap_prepared",
        )
    if operation is PendingTaskRuntimeInteractionOperation.INITIAL_PLAN_DISPATCH:
        complete_resumed_initial_plan_result(
            pool,
            github,
            workspace_id=record.workspace_id,
            task_id=record.task_id,
            expected_state_token=authorized_token,
            profile_id=profile_id,
            task_resume_intent_id=record.id,
            plan_result=_require_plan_result(response),
        )
        return SessionResumeOutcome(
            kind=SessionResumeOutcomeKind.COMPLETED,
            resume=record,
            detail="initial_plan_committed",
        )
    if operation is PendingTaskRuntimeInteractionOperation.PLANNING_REVISE_DISPATCH:
        complete_resumed_plan_revision_result(
            pool,
            github,
            workspace_id=record.workspace_id,
            task_id=record.task_id,
            expected_state_token=authorized_token,
            profile_id=profile_id,
            task_resume_intent_id=record.id,
            plan_result=_require_plan_result(response),
        )
        return SessionResumeOutcome(
            kind=SessionResumeOutcomeKind.COMPLETED,
            resume=record,
            detail="plan_revision_committed",
        )
    if operation is PendingTaskRuntimeInteractionOperation.REVIEWER_REVIEW:
        return _route_resumed_review_result(
            pool,
            adapter,
            github,
            producer_runtime_factory=producer_runtime_factory,
            reviewer_runtime_factory=reviewer_runtime_factory,
            record=record,
            subject_id=subject_id,
            authorized_token=authorized_token,
            response=response,
            profile_id=profile_id,
        )
    raise ConflictError("the session-resume record's operation is not a supported continuation")


def _require_plan_result(response: object) -> PlanResult:
    """Require the resumed planning turn's validated plan result."""
    if not isinstance(response, PlanResult):
        raise ExternalOperationUncertainError(
            "the resumed planning turn did not return the validated plan result "
            "family; the continuation remains unresolved"
        )
    return response


def _route_resumed_review_result(
    pool: DatabasePool,
    adapter: AgentRuntimeAdapter,
    github: GitHubAppClient,
    *,
    producer_runtime_factory: ProducerRuntimeAttachmentFactory | None,
    reviewer_runtime_factory: ReviewerRuntimeAttachmentFactory | None,
    record: TaskResumeIntent,
    subject_id: UUID,
    authorized_token: UUID,
    response: object,
    profile_id: UUID,
) -> SessionResumeOutcome:
    """Route the resumed review result through its existing decision service.

    The reserved unfinalized ReviewIteration and its OPEN loop are re-derived
    from durable state, the resumed turn's validated ``ReviewResult``
    completes the SAME iteration, and the ordinary decision entry records the
    result and commits its durable next decision — the planning decision or
    the PR decision with its remediation/re-dispatch chain, exactly as the
    ordinary dispatch→decision handoff does.
    """
    if not isinstance(response, ReviewResult):
        raise ExternalOperationUncertainError(
            "the resumed review turn did not return the validated review result "
            "family; the continuation remains unresolved"
        )
    iteration = review_records.get_review_iteration(pool, review_iteration_id=subject_id)
    if (
        iteration is None
        or iteration.workspace_id != record.workspace_id
        or iteration.task_id != record.task_id
        or iteration.outcome is not None
    ):
        raise StaleOperationError(
            "the addressed review iteration is not the task's unfinalized "
            "reserved attempt; the resumed review result is stale"
        )
    loop = review_records.get_review_loop(pool, review_loop_id=iteration.review_loop_id)
    if loop is None or loop.workspace_id != record.workspace_id:
        raise StaleOperationError("the addressed review iteration's review loop is not available")
    reviewer_session = session_records.get_task_agent_session(
        pool, task_id=record.task_id, role=WorkflowRole.REVIEWER
    )
    if (
        reviewer_session is None
        or reviewer_session.lifecycle_status is not TaskSessionLifecycleStatus.READY
    ):
        raise StaleOperationError(
            "the reviewer binding is not ready; the resumed review result is stale"
        )
    if loop.purpose is ReviewLoopPurpose.PLANNING:
        finalize_planning_review_decision(
            pool,
            adapter,
            github,
            profile_id=profile_id,
            workspace_id=record.workspace_id,
            task_id=record.task_id,
            expected_state_token=authorized_token,
            dispatch=TaskPlanningReviewDispatchOutcome(
                review_loop=loop,
                review_iteration=iteration,
                review_result=response,
                reviewer_session=reviewer_session,
            ),
        )
        recorded = _settle_resume_record(pool, task_resume_intent_id=record.id)
        return SessionResumeOutcome(
            kind=SessionResumeOutcomeKind.COMPLETED,
            resume=recorded,
            detail="planning_review_decided",
        )
    pull_request = get_task_pull_request_for_task(pool, task_id=record.task_id)
    if pull_request is None or pull_request.workspace_id != record.workspace_id:
        raise StaleOperationError(
            "the pr review iteration's canonical pull request is not available"
        )
    if producer_runtime_factory is None or reviewer_runtime_factory is None:
        raise InvalidCommandError(
            "the pr review continuation carries no runtime attachment factories"
        )
    task_for_dispatch = task_records.get_task(pool, task_id=record.task_id)
    if task_for_dispatch is None or task_for_dispatch.workspace_id != record.workspace_id:
        raise StaleOperationError(
            "the pr review iteration's task is not available; the resumed review result is stale"
        )
    with producer_runtime_factory() as producer_adapter:
        finalize_pr_review_decision(
            pool,
            producer_adapter,
            github,
            cast(ReviewerRuntimeAttachmentFactory, reviewer_runtime_factory),
            profile_id=profile_id,
            workspace_id=record.workspace_id,
            task_id=record.task_id,
            expected_state_token=authorized_token,
            dispatch=TaskPrReviewDispatchOutcome(
                review_loop=loop,
                review_iteration=iteration,
                review_result=response,
                reviewer_session=reviewer_session,
                task=task_for_dispatch,
                pull_request=pull_request,
                reconciled_head_sha=pull_request.head_sha,
            ),
        )
    recorded = _settle_resume_record(pool, task_resume_intent_id=record.id)
    return SessionResumeOutcome(
        kind=SessionResumeOutcomeKind.COMPLETED,
        resume=recorded,
        detail="pr_review_decided",
    )
