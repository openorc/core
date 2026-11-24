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
supersession when a newer Owner decision re-blocked the Task first), and
admits the fixed authorized resume instruction as one independent detached
agent turn on the exact original still-live session (issue #343; issue
#347 runs the same detached dispatch in every execution mode): the claim
transaction also commits the ``session_resume`` detached-turn receipt
companion, the admission runs strictly outside any transaction, and the
dispatching worker exits after the acknowledgement attach — the
independently observed completion settles through the shared correlation
seam, which completes the ORIGINAL workflow operation, settles the
record's truthful terminal outcome (``settled``/``interrupted``/``failed``,
never re-opening the resolved gate), and hands the ONE post-commit handoff
to the original family. The same logical operation continues in both
modes — never a replacement Execution, ReviewIteration, session, or
dispatcher.

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
  terminal ``superseded`` outcome without touching the newer episode. In
  regular mode the same transaction commits the record's ``session_resume``
  detached-turn receipt companion (issue #343).
- **Result routing** (the owning service's own transaction discipline): in
  the queue-mode interim the resumed formal result completes the SAME
  Execution, ReviewIteration, dispatch claim, or bootstrap stage through
  that service's existing finalize functions, and the record settles
  exactly once with the processed result. In regular mode the routing
  happens when the independently observed terminal arrives, through the
  shared correlation seam's own settlement composition.

Operational failures of the continuation are Level 1 failures of the
``TASK_SESSION_RESUME`` operation family: the coordinator reports them
against the record's durable identity and the existing Level 2 triage
escalates them through the standard Type B ``BLOCK_RESOLUTION`` fallback —
the family has no authorized-retry path, so a positively observed
post-effect terminal failure settles the record's truthful ``failed``
outcome and the Owner Cancel action stays reachable through the
cancellation accounting's resume lineage (issue #343).
"""

from __future__ import annotations

import logging
from collections.abc import Callable, Mapping
from contextlib import AbstractContextManager, ExitStack
from dataclasses import dataclass
from enum import StrEnum
from types import MappingProxyType
from typing import TYPE_CHECKING, Final, Protocol, cast, runtime_checkable
from uuid import UUID

from openorc.adapters.agent_runtime.contract import AgentRuntimeAdapter
from openorc.adapters.agent_runtime.detached import AgentTurnAdmission
from openorc.adapters.agent_runtime.errors import (
    AgentRuntimeError,
    AgentRuntimeUncertainOutcomeError,
)
from openorc.adapters.agent_runtime.instructions import RESUMPTION_INSTRUCTION
from openorc.domain.blocking_cases import (
    BlockingCaseDomainError,
    Level3BlockingCondition,
    validate_blocking_case_evidence,
)
from openorc.domain.connections import WorkflowRole
from openorc.domain.detached_turns import TaskDetachedTurnReceipt
from openorc.domain.executions import ExecutionStatus
from openorc.domain.gates import OwnerGate, OwnerGateStatus, OwnerGateType
from openorc.domain.intents import (
    TaskInterruptIntentStatus,
    TaskResumeIntent,
    TaskResumeIntentStatus,
)
from openorc.domain.operational_failures import (
    OriginalAttemptKind,
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
from openorc.persistence.detached_turns import refuse_task_detached_turn_receipt
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
from openorc.persistence.tasks import get_task_planning_bootstrap_evidence
from openorc.protocol.models import (
    IMPLEMENTATION_RESULT_FAMILY,
    PLAN_RESULT_FAMILY,
    REVIEW_RESULT_FAMILY,
    WORKSPACE_RESULT_FAMILY,
)
from openorc.services import event_coordination
from openorc.services import task_detached_turn_correlation as turn_correlation
from openorc.services.errors import (
    ApplicationError,
    ConflictError,
    ExternalOperationFailedError,
    ExternalOperationRejectedError,
    ExternalOperationUncertainError,
    InvalidCommandError,
    NotFoundError,
    ObservationRegistrationRefusedError,
    StaleOperationError,
)
from openorc.services.event_coordination import owner_actor
from openorc.services.operation_attempt_evidence import InvocationAttemptEvidence
from openorc.services.profile_lifecycle_guard import require_account_operational
from openorc.services.task_completion_handoff import DetachedTurnCompletionHandoff
from openorc.services.task_implementation_authorization import (
    require_resolution_applied,
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

__all__ = [
    "AuthorizedTaskResume",
    "SessionResumeObservationRegistrar",
    "SessionResumeOutcome",
    "SessionResumeOutcomeKind",
    "blocked_continuation_durably_offered",
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
class SessionResumeObservationRegistrar(Protocol):
    """The structural registration seam of the process's detached-turn observer.

    The same positively-required pre-admission gate the detached
    establishment, implementation, plan, review, and discussion dispatches
    consume (issues #323/#333–#339): the detached Owner Resume dispatch
    (issue #343) registers the exact bound original session before any
    admission, so an unreplayable resumed turn can never be produced
    without active observation. Failures are the observer's typed refusals,
    routed through the pre-effect failure machinery.
    """

    def register_session(
        self,
        *,
        workspace_id: UUID,
        task_id: UUID,
        task_agent_session_id: UUID,
        connection_id: UUID,
        external_session_id: str,
    ) -> None:
        """Establish the positive detached-turn observation of one exact session."""
        ...


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
                "settled for this blocking episode; the guarded continuation action is refused"
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
            "durable state is unsafe and the guarded continuation action is refused"
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
                "guarded continuation cannot continue it"
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
                "the guarded continuation action is refused"
            )
    for claim in send_claim_records.list_task_runtime_send_claims(
        transaction_pool, task_id=task.id
    ):
        if claim.status is TaskRuntimeSendClaimStatus.CLAIMED and claim.id != subject_claim:
            raise ConflictError(
                "the task carries an independently outstanding dispatch claim; "
                "the guarded continuation action is refused"
            )
    evidence = get_task_planning_bootstrap_evidence(transaction_pool, task_id=task.id)
    if evidence is not None and evidence.stage is None and not subject_bootstrap:
        raise ConflictError(
            "the task's planning bootstrap is independently outstanding; "
            "the guarded continuation action is refused"
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
                    "the guarded continuation action is refused"
                )


_OPEN_EXECUTION_STATUSES: Final[frozenset[ExecutionStatus]] = frozenset(
    {
        ExecutionStatus.QUEUED,
        ExecutionStatus.RUNNING,
        ExecutionStatus.PAUSED,
        ExecutionStatus.PAUSED_FOR_APPROVAL,
    }
)


@dataclass(frozen=True, slots=True)
class _BlockedContinuationSubject:
    """The guarded subject one blocked-Task Owner continuation addresses.

    Everything the shared authorization prologue proved under one composed
    transaction: the locked blocked Task under the caller's token, the exact
    current pending exceptional gate of the addressed type, its sole
    unresolved TaskBlock with coherent episode evidence, the exact
    interrupted session targets re-derived from the block context, and the
    one durably identifiable interrupted workflow operation with its result
    routing. Ambiguous, stale, or unsafe state refuses before this subject
    exists.
    """

    task: Task
    gate: OwnerGate
    block: TaskBlock
    targets: tuple[_ResumeTarget, ...]
    routing: _ResumeRouting


def _authorize_blocked_task_continuation(
    transaction_pool: DatabasePool,
    *,
    profile_id: UUID,
    workspace_id: UUID,
    task_id: UUID,
    owner_gate_id: UUID,
    expected_state_token: UUID,
    gate_type: OwnerGateType,
    blocking_condition: Level3BlockingCondition,
    action: str,
) -> _BlockedContinuationSubject:
    """The shared guarded authorization prologue of the blocked-Task Owner
    continuations (issue #292 Resume; issue #291 COST_LIMIT Continue).

    Called INSIDE an open composed transaction. One guard sequence proves
    the account-operational barrier, the exact Workspace/Task Owner scope,
    the locked non-archived ``blocked`` Task under the caller's token, the
    exact current pending gate of the addressed type, its sole unresolved
    TaskBlock with coherent episode evidence for the addressed blocking
    condition, the exact interrupted session identities re-derived from the
    block context against the durable READY bindings, every Interrupt intent
    bound to that block positively SETTLED, no independently outstanding or
    uncertain runtime work beyond the derived subject, and the result-routing
    derivation — the one durably identifiable interrupted workflow operation
    with its expected formal response family and existing result owner.
    Ambiguous, stale, or unsafe state refuses the whole action: nothing is
    applied and the Task stays ``blocked``.
    """
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
            f"the addressed task is not blocked; the {action} action is not available"
        )
    gate = require_current_owner_gate(transaction_pool, task=task, owner_gate_id=owner_gate_id)
    if gate.gate_type is not gate_type:
        raise ConflictError(f"only the current {gate_type.value} gate continues through this seam")
    block = require_owner_gate_unresolved_block_subject(transaction_pool, task=task, gate=gate)
    try:
        validate_blocking_case_evidence(blocking_condition, block.context)
    except BlockingCaseDomainError as error:
        raise ConflictError(
            f"the {blocking_condition.value} block carries incoherent episode evidence"
        ) from error
    targets = _resolve_resume_targets(transaction_pool, task=task, context=block)
    _require_episode_interruptions_settled(
        transaction_pool, task=task, block=block, targets=targets
    )
    routing = _derive_resume_routing(transaction_pool, task=task, targets=targets)
    _require_no_independent_outstanding_work(transaction_pool, task=task, routing=routing)
    return _BlockedContinuationSubject(
        task=task, gate=gate, block=block, targets=targets, routing=routing
    )


# The exceptional blocked-Task gate kinds whose guarded continuation runs
# through this module's shared derivation: the OWNER_ABORT gate's Resume
# (issue #292) and the COST_LIMIT gate's Continue (issue #291), each
# addressing its own Level 3 blocking condition (issue #286).
_BLOCKED_CONTINUATION_GATE_CONDITIONS: Final[Mapping[OwnerGateType, Level3BlockingCondition]] = (
    MappingProxyType(
        {
            OwnerGateType.OWNER_ABORT: Level3BlockingCondition.OWNER_ABORT_ISSUED,
            OwnerGateType.COST_LIMIT: Level3BlockingCondition.TASK_COST_LIMIT_REACHED,
        }
    )
)


def blocked_continuation_durably_offered(
    transaction_pool: DatabasePool, *, task: Task, gate: OwnerGate
) -> bool:
    """Whether the durable facts positively support the blocked-Task continuation
    actions — the OWNER_ABORT gate's ``resume`` and the COST_LIMIT gate's
    ``continue`` (#376).

    The authorized read surface's backend-generated action projection reuses
    this module's authoritative guarded-continuation derivation in
    predicate mode: the blocked status, the exact current pending gate of a
    supported continuation type, its sole unresolved TaskBlock with coherent
    episode evidence for the addressed blocking condition, the interrupted
    session targets re-derived from the block context, every Interrupt
    intent bound to that episode positively ``settled`` (the #342
    precondition — an unsettled current interruption never advertises the
    continuation), the result-routing derivation, and no independently
    outstanding or uncertain runtime work. Evaluated inside the caller's own
    transaction with NO row locks, never persisted, never an external call,
    and never a second policy engine; a refused, stale, or unclassifiable
    shape is simply not offered. The command still revalidates the exact
    currentness and targets freshly at execution under its own locks —
    offering is neither permission nor a promise.
    """
    condition = _BLOCKED_CONTINUATION_GATE_CONDITIONS.get(gate.gate_type)
    if condition is None:
        return False
    if task.status is not TaskStatus.BLOCKED:
        return False
    if gate.status is not OwnerGateStatus.PENDING or gate.task_id != task.id:
        return False
    try:
        block = require_owner_gate_unresolved_block_subject(transaction_pool, task=task, gate=gate)
        try:
            validate_blocking_case_evidence(condition, block.context)
        except BlockingCaseDomainError as error:
            raise ConflictError(
                f"the {condition.value} block carries incoherent episode evidence"
            ) from error
        targets = _resolve_resume_targets(transaction_pool, task=task, context=block)
        _require_episode_interruptions_settled(
            transaction_pool, task=task, block=block, targets=targets
        )
        routing = _derive_resume_routing(transaction_pool, task=task, targets=targets)
        _require_no_independent_outstanding_work(transaction_pool, task=task, routing=routing)
    except ApplicationError:
        return False
    return True


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
            subject = _authorize_blocked_task_continuation(
                transaction_pool,
                profile_id=profile_id,
                workspace_id=workspace_id,
                task_id=task_id,
                owner_gate_id=owner_gate_id,
                expected_state_token=expected_state_token,
                gate_type=OwnerGateType.OWNER_ABORT,
                blocking_condition=Level3BlockingCondition.OWNER_ABORT_ISSUED,
                action="resume",
            )
            resolution = resolve_gate_records(
                transaction_pool,
                owner_gate_id=subject.gate.id,
                outcome=OwnerGateStatus.APPROVED,
                expected_task_state_token=subject.task.state_token,
            )
            resolved_gate, gate_task = require_resolution_applied(resolution)
            resolved_block = block_records.resolve_task_block(
                transaction_pool, task_block_id=subject.block.id
            )
            if resolved_block is None:
                raise StaleOperationError(
                    "the owner_abort block has already resolved; the resume action is stale"
                )
            restored = task_records.restore_blocked_task_status(
                transaction_pool,
                task_id,
                expected_state_token=gate_task.state_token,
                restored_status=subject.routing.restore_status,
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
                task_agent_session_id=subject.routing.session.id,
                task_block_id=subject.block.id,
                workflow_role=subject.routing.session.role,
                external_session_id=subject.routing.session.external_session_id or "",
                requested_by_profile_id=profile_id,
                routing=subject.routing.routing_context(
                    authorized_state_token=restored.state_token
                ),
            )
        return AuthorizedTaskResume(
            task=restored,
            resolved_owner_gate=resolved_gate,
            resolved_task_block=resolved_block,
            resume=resume_record,
        )


class SessionResumeOutcomeKind(StrEnum):
    """The typed outcome of one authorized session-resume continuation.

    ``admitted`` is the detached dispatch (issues #343/#347): the one-shot
    continuation was durably admitted as one independent detached turn and
    the dispatching worker returned after the acknowledgement attach; the
    independently observed completion owns the settlement through the shared
    correlation seam, so no in-process result routing happens. ``superseded``
    means the record was proven never-dispatched and was terminally
    superseded before any runtime effect. Failures raise the typed
    operational errors for the coordinator's Level 2 handoff.
    """

    ADMITTED = "admitted"
    SUPERSEDED = "superseded"


@dataclass(frozen=True, slots=True)
class SessionResumeOutcome:
    """The typed outcome of one authorized session-resume continuation."""

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
    attempt_evidence: InvocationAttemptEvidence | None = None,
    observation_registrar: SessionResumeObservationRegistrar | None = None,
    completion_handoff: DetachedTurnCompletionHandoff | None = None,
) -> SessionResumeOutcome:
    """Dispatch one authorized session-resume continuation, exactly once.

    The submitted TASK_SESSION_RESUME continuation (issue #292): one short
    composed transaction under the Task row lock re-proves currentness
    against the record's recorded authorization token and restored status
    and claims ``authorized`` → ``dispatched`` exactly once — or, when a
    newer Owner decision re-blocked the Task first, positively establishes
    the never-dispatched supersession and records the terminal
    ``superseded`` outcome without touching the newer episode.

    The same one-shot claim transaction additionally commits the
    ``session_resume`` detached-turn receipt companion of the record (the
    ORIGINAL owner-control-intent identity, expecting the record routing's
    original formal family), and the fixed authorized resume instruction is
    admitted verbatim as one independent detached turn on the exact original
    still-live session (issues #343/#347) — never the original
    planning/review/implementation prompt, never a new session, never a new
    Execution/ReviewIteration/PlanRevision, and no workflow authority ever
    derived from the acknowledged turn reference. A missing observation
    registrar or completion handoff fails closed before any effect. Proven
    pre-effect failures refuse the receipt and record the terminal
    ``non_delivered`` outcome; an uncertain admission leaves the receipt
    conservatively claimed and the record ``dispatched`` and is never
    replayed. The independently observed completion settles through the
    shared correlation seam, which completes the ORIGINAL workflow operation
    and hands the ONE post-commit handoff to that original family.
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
            expected_family_value = record.routing.get("expected_response_family")
            if not isinstance(expected_family_value, str) or not expected_family_value.strip():
                raise ConflictError("the session-resume record carries no expected response family")
            # The detached dispatch's receipt companion (issues #343/#347):
            # committed inside the SAME one-shot claim transaction, strictly
            # before any runtime effect, so the single-outstanding invariant
            # and the receipt lifecycle are durable from the first possible
            # effect. The receipt roots at the record's own
            # owner-control-intent identity and expects the record routing's
            # ORIGINAL formal family.
            receipt = turn_correlation.record_task_detached_turn_receipt(
                transaction_pool,
                workspace_id=workspace_id,
                task_id=task_id,
                task_agent_session_id=record.task_agent_session_id,
                operation=PendingTaskRuntimeInteractionOperation.SESSION_RESUME,
                original_attempt_kind=OriginalAttemptKind.OWNER_CONTROL_INTENT,
                original_attempt_id=record.id,
                expected_formal_family=expected_family_value,
            )

        # The detached dispatch (issues #343/#347): the claimed record and
        # its receipt companion are durable; the fixed authorized resume
        # instruction is admitted as one independent detached turn on the
        # exact original session and the worker returns after the
        # acknowledgement attach. A missing seam fails closed before any
        # effect — never a blocking fallback.
        if observation_registrar is None:
            raise InvalidCommandError(
                "the detached session-resume dispatch carries no observation registrar"
            )
        if completion_handoff is None:
            raise InvalidCommandError(
                "the detached session-resume dispatch carries no completion handoff"
            )
        return _dispatch_authorized_resume_detached(
            pool,
            workspace_id=workspace_id,
            task_id=task_id,
            record=record,
            receipt=receipt,
            expected_family=expected_family_value,
            producer_runtime_factory=producer_runtime_factory,
            reviewer_runtime_factory=reviewer_runtime_factory,
            observation_registrar=observation_registrar,
            completion_handoff=completion_handoff,
        )


_RESUME_REGISTRATION_REFUSAL_KIND: Final[str] = "observation_registration_refused"
_RESUME_ADMISSION_REFUSAL_KIND: Final[str] = "admission_refused"
_RESUME_ATTACHMENT_REFUSAL_KIND: Final[str] = "runtime_attachment_failed"
_RESUME_CAPABILITY_REFUSAL_KIND: Final[str] = "no_detached_capability"


def _refuse_resume_dispatch_quietly(
    pool: DatabasePool,
    receipt: TaskDetachedTurnReceipt,
    *,
    refusal_kind: str,
) -> None:
    """Mark a claimed resume receipt refused after a proven pre-effect failure.

    Best-effort containment: a persistence failure here must never mask the
    typed refusal the caller raises. The claimed receipt stays the honest
    unresolved state under the accepted v1 recovery bounds and the failure
    is logged selectively. The record's own terminal ``non_delivered``
    outcome is recorded by the caller's typed failure path.
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
            "the refused session-resume receipt could not be durably marked (%s); "
            "the claimed receipt stays the honest unresolved state",
            type(error).__name__,
        )
        refused = None
    if refused is None:  # pragma: no cover - a claimed receipt refuses once
        logger.warning(
            "the session-resume receipt was no longer claimed when its pre-effect "
            "refusal was recorded"
        )


def _dispatch_authorized_resume_detached(
    pool: DatabasePool,
    *,
    workspace_id: UUID,
    task_id: UUID,
    record: TaskResumeIntent,
    receipt: TaskDetachedTurnReceipt,
    expected_family: str,
    producer_runtime_factory: ProducerRuntimeAttachmentFactory | None,
    reviewer_runtime_factory: ReviewerRuntimeAttachmentFactory | None,
    observation_registrar: SessionResumeObservationRegistrar,
    completion_handoff: DetachedTurnCompletionHandoff,
) -> SessionResumeOutcome:
    """Admit the authorized resume instruction as one detached agent turn.

    The detached Owner-authorized same-session Resume's runtime boundary
    (issue #343), mirroring the established detached-dispatch mechanics: the
    runtime lease and the detached capability are acquired and duck-typed
    fail-closed (the claim and its receipt companion are already durable —
    a capability-less runtime is a proven pre-effect non-delivery of the
    just-claimed record), one positively-required observation registration
    gates the admission, one detached-turn admission carries the fixed
    authorized resume instruction verbatim — the ONLY content the
    independent run ever receives, never the original
    planning/review/implementation prompt — and the acknowledgement attach
    consumes any staged early completion exactly once. No ``send`` and no
    blocking wait exists in this path, and no database transaction is ever
    held across a runtime call. The dispatching worker returns after the
    attach; the independently observed completion owns the settlement
    through the shared correlation seam, whose routing completes the
    ORIGINAL workflow operation and hands the ONE post-commit handoff to
    that original family.

    Proven pre-effect failures refuse the receipt, record the record's
    terminal ``non_delivered`` outcome, and raise the typed
    operational-failure classes the F5b coordinator triages under the
    record's own family. An uncertain admission leaves the receipt
    conservatively claimed and the record ``dispatched`` and is never
    re-dispatched. The original gate and block stay resolved history
    whatever subsequently happens to the continuation.
    """

    def _record_non_delivered(*, refusal: str) -> None:
        outcome = record_task_resume_intent_outcome(
            pool,
            task_resume_intent_id=record.id,
            status=TaskResumeIntentStatus.NON_DELIVERED,
            outcome={"dispatch": "non_delivered", "refusal": refusal},
        )
        if outcome is None:  # pragma: no cover - the claimed record is non-terminal
            logger.warning(
                "the session-resume record was no longer non-terminal when its "
                "proven pre-effect non-delivery was recorded"
            )

    register = getattr(observation_registrar, "register_session", None)
    if not callable(register):
        # A misconfigured registrar can never gate an admission: the proven
        # pre-effect accounting applies (issue #343 review) — the receipt is
        # refused and the record carries its terminal non-delivery.
        _refuse_resume_dispatch_quietly(
            pool, receipt, refusal_kind=_RESUME_REGISTRATION_REFUSAL_KIND
        )
        _record_non_delivered(refusal="registrar_misconfigured")
        raise InvalidCommandError(
            "the session-resume observation registrar does not implement the "
            "established-session registration seam"
        )
    external_session_id = record.external_session_id
    if not external_session_id.strip():
        # The record's binding incoherence positively establishes that no
        # admission can ever address the session: same proven pre-effect
        # accounting (issue #343 review).
        _refuse_resume_dispatch_quietly(
            pool, receipt, refusal_kind=_RESUME_REGISTRATION_REFUSAL_KIND
        )
        _record_non_delivered(refusal="binding_unresolved")
        raise ConflictError(
            "the session-resume record's binding carries no external session identity"
        )
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

    # __RESUME_DETACHED_ADMIT__

    if expected_family != receipt.expected_formal_family:  # pragma: no cover - same transaction
        raise ConflictError(
            "the detached session-resume receipt's expected family disagrees with "
            "the record routing; the dispatch is stale"
        )

    with ExitStack() as attachment_stack:
        # The runtime lease is acquired before any admission effect: an
        # attachment-construction failure (a bridge process that cannot
        # start, a connection that cannot attach) is a positively proven
        # pre-effect non-delivery — nothing could ever have been enqueued —
        # so the receipt is refused and the record carries its terminal
        # non-delivery before the classified failure propagates. A stranded
        # ``dispatched`` record with a ``claimed`` receipt and NO native turn
        # would otherwise permanently veto the Owner Cancel action behind the
        # escalation's Type B blocking gate (issue #343 review).
        try:
            runtime = attachment_stack.enter_context(factory_cm())
        except ApplicationError:
            _refuse_resume_dispatch_quietly(
                pool, receipt, refusal_kind=_RESUME_ATTACHMENT_REFUSAL_KIND
            )
            _record_non_delivered(refusal="runtime_attachment_failed")
            raise
        except Exception as error:
            logger.warning(
                "the session-resume runtime attachment failed (%s) before any "
                "admission; the receipt is refused and the instruction never "
                "dispatched",
                type(error).__name__,
            )
            _refuse_resume_dispatch_quietly(
                pool, receipt, refusal_kind=_RESUME_ATTACHMENT_REFUSAL_KIND
            )
            _record_non_delivered(refusal="runtime_attachment_failed")
            raise ExternalOperationFailedError(
                "the session-resume runtime attachment failed before any "
                "admission; the receipt is refused and the instruction never "
                "dispatched"
            ) from error
        # The detached capability is duck-typed fail-closed BEFORE the
        # admission effect: a runtime without it cannot serve the detached
        # dispatch, and the just-claimed record carries its proven pre-effect
        # non-delivery — nothing could ever have been enqueued.
        admit = getattr(runtime, "admit_detached_turn", None)
        if not callable(admit):
            _refuse_resume_dispatch_quietly(
                pool, receipt, refusal_kind=_RESUME_CAPABILITY_REFUSAL_KIND
            )
            _record_non_delivered(refusal="no_detached_capability")
            raise ExternalOperationFailedError(
                "the runtime does not implement the detached turn capability; "
                "the resume dispatch failed before any effect"
            )
        admit_detached_turn = cast(Callable[[str, str], AgentTurnAdmission], admit)

        # --- The positive pre-admission observation registration: the
        # positively-required gate. A refusal is a proven pre-effect failure:
        # the receipt is refused and the record carries its terminal
        # non-delivery, so a never-dispatched turn holds neither the
        # session's single-outstanding slot nor the record's authorization. ---
        binding = session_records.get_task_agent_session(
            pool, task_id=task_id, role=record.workflow_role
        )
        if (
            binding is None
            or binding.id != record.task_agent_session_id
            or binding.workspace_id != workspace_id
            or binding.external_session_id != external_session_id
        ):
            _refuse_resume_dispatch_quietly(
                pool, receipt, refusal_kind=_RESUME_REGISTRATION_REFUSAL_KIND
            )
            _record_non_delivered(refusal="binding_unresolved")
            raise ExternalOperationFailedError(
                "the session-resume record's binding does not resolve to its "
                "addressed session; the dispatch failed before any effect"
            )
        try:
            register(
                workspace_id=workspace_id,
                task_id=task_id,
                task_agent_session_id=record.task_agent_session_id,
                connection_id=binding.connection_id,
                external_session_id=external_session_id,
            )
        except ObservationRegistrationRefusedError as error:
            _refuse_resume_dispatch_quietly(
                pool, receipt, refusal_kind=_RESUME_REGISTRATION_REFUSAL_KIND
            )
            _record_non_delivered(refusal="observation_registration_refused")
            raise ExternalOperationFailedError(
                "the session-resume observation registration was refused before "
                "any admission; the instruction never dispatched"
            ) from error
        except Exception as error:
            logger.warning(
                "the session-resume observation registration failed (%s) before "
                "any admission; the receipt is refused and the instruction never "
                "dispatched",
                type(error).__name__,
            )
            _refuse_resume_dispatch_quietly(
                pool, receipt, refusal_kind=_RESUME_REGISTRATION_REFUSAL_KIND
            )
            _record_non_delivered(refusal="observation_registration_failed")
            raise ExternalOperationFailedError(
                "the session-resume observation registration failed before any "
                "admission; the instruction never dispatched"
            ) from error

        # --- The admission (external, no transaction held). One
        # detached-turn admission of the fixed authorized resume instruction
        # verbatim — the only content the independent run ever receives. ---
        try:
            admission = admit_detached_turn(external_session_id, RESUMPTION_INSTRUCTION)
        except AgentRuntimeUncertainOutcomeError as error:
            logger.warning(
                "the detached session-resume admission ended with an uncertain "
                "outcome; the receipt stays conservatively claimed and the record "
                "stays dispatched and is never re-dispatched"
            )
            raise ExternalOperationUncertainError(
                "the outcome of the session-resume admission is unknown; the "
                "receipt stays conservatively claimed and the record stays "
                "dispatched and the instruction is never re-dispatched"
            ) from error
        except AgentRuntimeError as error:
            # A positively identified refusal (unreachable before effect,
            # runtime rejection, exact-session absence): proven pre-effect, so
            # the receipt is refused and the record carries its terminal
            # non-delivery.
            _refuse_resume_dispatch_quietly(
                pool, receipt, refusal_kind=_RESUME_ADMISSION_REFUSAL_KIND
            )
            _record_non_delivered(refusal="admission_refused")
            raise ExternalOperationRejectedError(
                "the session-resume admission was refused before any effect; "
                "the receipt is refused and the instruction never dispatched"
            ) from error

        # --- The acknowledgement attach (one short transaction). A staged
        # early completion is consumed exactly once here and routed through
        # the ordinary settlement, so a resumed result that finished before
        # the acknowledgement was persisted is correlated, never ignored. ---
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
                "the session-resume acknowledgement could not be durably attached "
                "after its known admission; authoritative recovery is required "
                "and the instruction is never re-dispatched"
            ) from error

    # The attachment is released; the handoffs are strictly post-dispatch
    # and never hold the runtime lease.
    if outcome.kind is turn_correlation.TurnSettlementKind.SETTLED_VALIDATED_RESULT:
        # A staged early completion settled the original operation and the
        # receipt within this dispatch: the ONE post-commit completion
        # handoff owns the family routing — by the ORIGINAL operation family
        # the outcome carries, exactly as it routes that family's ordinary
        # detached settlements. Strictly post-commit, contained, never a
        # workflow authority; a failed routing leaves the durable state to
        # the existing recovery semantics.
        handoff_operation = (
            outcome.handoff_operation
            if outcome.handoff_operation is not None
            else outcome.receipt.operation
            if outcome.receipt is not None
            else None
        )
        if handoff_operation is not None:
            try:
                completion_handoff.handoff_settled_completion(
                    workspace_id=workspace_id,
                    task_id=task_id,
                    operation=handoff_operation,
                )
            except Exception as error:  # noqa: BLE001 - contained handoff
                logger.warning(
                    "the post-dispatch session-resume completion handoff failed "
                    "(%s); the durable state owns recovery",
                    type(error).__name__,
                )
    if (
        outcome.kind is turn_correlation.TurnSettlementKind.SETTLED_INTERRUPTED_TURN
        and outcome.receipt is not None
    ):
        # A staged interrupted completion settled the receipt within this
        # dispatch (issue #342's shared interrupted routing, which also
        # settles the record's terminal ``interrupted`` outcome): the ONE
        # post-commit completion handoff owns the Interrupt settlement — the
        # attached adapter's D6b guard release and the annotated intent's
        # outcome recording. Strictly post-attach, contained best-effort
        # inside the handoff, never a workflow authority.
        completion_handoff.handoff_settled_interrupt(
            workspace_id=workspace_id,
            task_id=task_id,
            task_agent_session_id=outcome.receipt.task_agent_session_id,
            external_session_id=external_session_id,
        )
    reloaded = get_task_resume_intent(pool, task_resume_intent_id=record.id)
    if reloaded is None:  # pragma: no cover - the claimed record cannot vanish
        raise NotFoundError(
            "the addressed session-resume record is not available after its dispatch"
        )
    return SessionResumeOutcome(
        kind=SessionResumeOutcomeKind.ADMITTED,
        resume=reloaded,
        detail="detached_resume_admitted",
    )
