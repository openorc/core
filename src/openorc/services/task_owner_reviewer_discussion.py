"""E4: detached Owner ↔ Reviewer discussion without transcript persistence.

One focused Owner-authenticated capability routes free-form Owner ↔ Reviewer
discussion at a pending current REVIEW_RESOLUTION, IMPLEMENTATION_AUTHORIZATION,
or MERGE_DECISION OwnerGate of a non-archived ``waiting_for_owner`` Task
through the Task's existing admitted READY Reviewer session as ONE detached
agent turn (issue #339). Discussion is never admitted at a PR_AUTHORIZATION
or exceptional gate. At ordinary gates the exchange is strictly advisory. On
the independently attributed completion text, an explicitly recognizable
standalone, schema-valid Reviewer ``review_result`` is routed by the
gate-specific decision rules to the existing owning settlements: any formal
result at a REVIEW_RESOLUTION gate (plan or PR/head subject) settles that
gate through the owning E3c/E9 resolution settlement; at a plan-subject
IMPLEMENTATION_AUTHORIZATION gate issue #219 makes exactly one reply formal
(a standalone ``CHANGES_REQUESTED`` supersedes the gate through the owning
E3 change-request handoff); at a pending PR/head MERGE_DECISION gate only a
standalone ``CHANGES_REQUESTED`` retires the gate through the owning E9
supersession (a formal ACCEPTED there — like ordinary prose — stays advisory
while the head is unchanged). Every formal continuation commits atomically
with the receipt's settlement inside the correlation seam's transaction; the
exact new ReviewIteration is created there, only the current gate is retired
or settled, and the Producer work is submitted strictly after commit by the
ONE post-commit completion handoff re-deriving from committed state. E4 owns
admission routing, recognition, and gate-specific decision rules; it never
mutates gates, loops, or iterations directly, and no automated formal REVIEW
claim is ever invented. The runtime owns conversation context; OpenOrc
records no transcript and no conversation text in any durable row.

The discussion is the one detached dispatch whose claim and admission live in
different invocations (the Owner-facing asynchronous seam), so the shared
receipt machinery carries two discussion-specific duties:

- **prepare** (:func:`prepare_owner_reviewer_discussion`) — the synchronous
  authorization + receipt commit: one short transaction (Task row lock
  first) proving the account-operational barrier, the exact authenticated
  Owner Workspace/Task authorization for the claimed ``profile_id`` (never
  the Workspace's recorded owner as proof of an unproven caller), the exact
  current ``WAITING_FOR_OWNER`` Task, the exact pending current permitted
  gate, its exact real subject form (current PlanRevision for plan gates;
  the canonical TaskPullRequest plus its exact reconciled head for PR/head
  gates), and the exact admitted READY Reviewer binding — then the
  gate-bound receipt companion commits atomically (operation
  ``owner_reviewer_discussion``, attempt kind ``owner_gate_discussion``,
  identity the exact OwnerGate UUID, expected family the dedicated
  non-formal discussion token). The single-outstanding-receipt invariant is
  the writer-exclusion backstop.
- **admit** (:func:`admit_owner_reviewer_discussion_turn`) — the
  asynchronous admission operation: one short reproof transaction (the same
  subjects, the claimed ``profile_id`` re-proven, the receipt still claimed
  and unstamped) that also takes the receipt's set-once one-shot admission
  stamp — a duplicate or re-delivered admission execution can never reach a
  second enqueue of the same prepared message; then the exact Reviewer
  attachment, the positive observation registration, the verbatim message
  enqueue, and the acknowledgement attach, each outside any transaction. A
  positively proven refusal — the attachment, the observation registration,
  or the runtime admission — marks the receipt ``refused`` (the slot is
  freed); an uncertain admission stays conservatively claimed (stamped,
  never re-dispatched); the attach consumes any staged early completion.

**settlement** (:func:`settle_observed_discussion_result`) — the attributed
completion routes here from the correlation seam's discussion branch, atomic
with the receipt settlement: the Task row lock, the full currentness reproof,
the existing strict standalone ``review_result`` recognition, the
gate-specific decisions through the owning E3c/E9 transaction phases, or the
two direction-only ``OWNER_REVIEWER_DISCUSSION_MESSAGE`` audit events for an
ordinary or advisory exchange. A discussion whose gate has moved on settles
``result_not_applied``; an observed turn failure settles the receipt
truthfully and stops (a discussion is deliberately not a Level 2
operational-failure family).

The Owner-facing contract is asynchronous by construction: the request
acknowledges authorization and queueing, the receipt-status projection
reports actual admission and refusal, and the reply is read through the
runtime-native conversation surface. Transport acceptance is never presented
as successful message delivery, and no conversation text ever enters a
workflow event, a receipt outcome, or any OpenOrc transcript table.
"""

from __future__ import annotations

import json
import logging
import re
from collections.abc import Callable
from contextlib import AbstractContextManager, ExitStack
from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from typing import Any, Final, Protocol, cast
from uuid import UUID

from openorc.adapters.agent_runtime.contract import AgentRuntimeAdapter
from openorc.adapters.agent_runtime.detached import AgentTurnAdmission
from openorc.adapters.agent_runtime.errors import (
    AgentRuntimeError,
    AgentRuntimeUncertainOutcomeError,
)
from openorc.domain.connections import WorkflowRole
from openorc.domain.detached_turns import (
    OWNER_REVIEWER_DISCUSSION_FAMILY,
    REFUSAL_OUTCOME_KIND_KEY,
    SETTLEMENT_DISCUSSION_EXCHANGE,
    SETTLEMENT_DISCUSSION_FORMAL_CONTINUATION,
    SETTLEMENT_OUTCOME_LABEL_KEY,
    TaskDetachedTurnReceipt,
    TaskDetachedTurnReceiptStatus,
)
from openorc.domain.events import WorkflowEventActor
from openorc.domain.gates import OwnerGate, OwnerGateStatus, OwnerGateType
from openorc.domain.operational_failures import OriginalAttemptKind
from openorc.domain.pending_interactions import PendingTaskRuntimeInteractionOperation
from openorc.domain.sessions import TaskAgentSession, TaskSessionLifecycleStatus
from openorc.domain.tasks import Task, TaskStatus
from openorc.observability import annotate_span, application_span
from openorc.persistence import detached_turns as turn_records
from openorc.persistence import gates as gate_records
from openorc.persistence import tasks as task_records
from openorc.persistence.ownership import get_workspace
from openorc.persistence.pool import DatabasePool
from openorc.persistence.sessions import get_task_agent_session
from openorc.protocol.errors import ProtocolError
from openorc.protocol.models import REVIEW_OUTCOME_CHANGES_REQUESTED, ReviewResult
from openorc.protocol.parsing import parse_formal_response
from openorc.services import event_coordination
from openorc.services.errors import (
    ApplicationError,
    ConflictError,
    ExternalOperationFailedError,
    ExternalOperationRejectedError,
    ExternalOperationUncertainError,
    InvalidCommandError,
    NotFoundError,
    StaleOperationError,
)
from openorc.services.profile_lifecycle_guard import require_account_operational
from openorc.services.task_completion_handoff import DetachedTurnCompletionHandoff
from openorc.services.task_detached_turn_correlation import (
    TurnSettlementKind,
    TurnSettlementOutcome,
    attach_acknowledged_turn,
    record_task_detached_turn_receipt,
)
from openorc.services.task_detached_turn_observation import ObservationRegistrationRefusedError
from openorc.services.task_planning_review_resolution import (
    settle_observed_planning_review_resolution,
    supersede_observed_implementation_authorization_with_changes,
)
from openorc.services.task_pr_review_decision import (
    retire_observed_pr_merge_decision_with_changes,
    settle_observed_pr_review_resolution,
)
from openorc.services.task_subject_guards import (
    require_current_owner_gate,
    require_current_plan_revision,
    require_task_pull_request_head,
)
from openorc.services.transaction_composition import composed_transaction
from openorc.services.workspace_authorization import require_workspace_task

__all__ = [
    "AdmittedOwnerReviewerDiscussion",
    "DiscussionObservationRegistrar",
    "ObservedDiscussionSettlement",
    "ObservedDiscussionSettlementKind",
    "OwnerGateDiscussionStatus",
    "PreparedOwnerReviewerDiscussion",
    "ReviewerRuntimeAttachmentFactory",
    "admit_owner_reviewer_discussion_turn",
    "get_owner_gate_discussion_status",
    "prepare_owner_reviewer_discussion",
    "release_prepared_discussion_receipt",
    "settle_observed_discussion_result",
]

_SERVICE_TRACER_SCOPE = "openorc.services.task_owner_reviewer_discussion"
_PREPARE_SPAN_NAME = "task_owner_reviewer_discussion.prepare_owner_reviewer_discussion"
_ADMIT_SPAN_NAME = "task_owner_reviewer_discussion.admit_owner_reviewer_discussion_turn"
_SETTLEMENT_SPAN_NAME = "task_owner_reviewer_discussion.settle_observed_discussion_result"
_STATUS_SPAN_NAME = "task_owner_reviewer_discussion.get_owner_gate_discussion_status"

# The bounded wire/queue shape of one verbatim Owner message: far above any
# ordinary conversational message, far below unbounded bodies. The text is
# sent verbatim and never durably stored by OpenOrc.
_MAX_DISCUSSION_MESSAGE_LENGTH: Final[int] = 20_000

# The positively proven pre-effect refusal kinds of the discussion admission.
_ATTACHMENT_REFUSAL_KIND: Final[str] = "admission_attachment_refused"
_REGISTRATION_REFUSAL_KIND: Final[str] = "observation_registration_refused"
_ADMISSION_REFUSAL_KIND: Final[str] = "admission_refused"
# The positively proven refusal of the operation submission itself (issue
# #339): no worker will run, so the prepared receipt is released truthfully.
_SUBMISSION_REFUSAL_KIND: Final[str] = "admission_submission_refused"

logger = logging.getLogger(__name__)

# The deferred attachment seam: the service decides WHEN the adapter is
# acquired; the caller owns HOW the production attachment is composed (the
# #190 BOUND ``attach_cline_runtime`` composition). No adapter is ever
# acquired before the authorization transaction has committed.
ReviewerRuntimeAttachmentFactory = Callable[
    [TaskAgentSession], AbstractContextManager[AgentRuntimeAdapter]
]


class DiscussionObservationRegistrar(Protocol):
    """The positive pre-admission observation registration seam.

    The serving process's detached-turn observation host implements it: the
    admission must not proceed without the exact session being actively
    observed, because the runtime integration cannot replay an arbitrary
    missed terminal result.
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
        """Establish the positive detached-turn observation of one session."""
        ...


# Standalone fenced-object recognition: the whole normalized text must be
# exactly one fence whose inner content is one JSON object, with nothing
# outside the fence. The optional info string is runtime-neutral decoration.
_FENCED_OBJECT = re.compile(
    r"\A```[ \t]*[\w.+-]*[ \t]*\r?\n(?P<body>.*)\r?\n```[ \t]*\Z",
    re.DOTALL,
)


class ObservedDiscussionSettlementKind(StrEnum):
    """The classified durable outcome of one observed discussion completion."""

    ORDINARY_EXCHANGE = "ordinary_exchange"
    FORMAL_CONTINUATION = "formal_continuation"


@dataclass(frozen=True, slots=True)
class PreparedOwnerReviewerDiscussion:
    """The safe durable result of one prepared Owner ↔ Reviewer discussion.

    The gate-bound receipt companion is committed and the message is
    authorized for its single asynchronous admission. The message text is
    carried by the operation invocation only — never by this result, never
    by any durable row.
    """

    receipt: TaskDetachedTurnReceipt
    owner_gate: OwnerGate
    reviewer_session: TaskAgentSession
    task: Task


@dataclass(frozen=True, slots=True)
class AdmittedOwnerReviewerDiscussion:
    """The safe durable result of one discussion admission invocation.

    ``receipt`` is the receipt after the acknowledgement attach;
    ``settlement_outcome`` carries the attach's classification — a staged
    early completion may have settled the exchange within this very
    invocation, and its post-commit handoff has already run. No message or
    reply text is carried. A failed admission returns a typed error instead
    of an outcome, never a falsely successful one.
    """

    receipt: TaskDetachedTurnReceipt
    owner_gate: OwnerGate
    reviewer_session: TaskAgentSession
    task: Task
    settlement_outcome: TurnSettlementOutcome


@dataclass(frozen=True, slots=True)
class ObservedDiscussionSettlement:
    """The durable outcome of one settled discussion completion.

    ``kind`` classifies the exchange; ``settlement_label`` is the safe
    machine label the receipt outcome records. No conversation text is
    carried; the formal continuations' durable facts live in the owning
    settlements' commits, and their Producer work belongs to the post-commit
    completion handoff.
    """

    kind: ObservedDiscussionSettlementKind
    settlement_label: str
    owner_gate: OwnerGate
    task: Task


@dataclass(frozen=True, slots=True)
class OwnerGateDiscussionStatus:
    """The Owner-visible lifecycle of one gate-bound discussion exchange.

    The receipt is the single durable fact; ``outcome_label`` is the safe
    settlement or refusal label exactly as recorded (``None`` while the
    receipt is still claimed or admitted). ``admission_attempted_at`` lets a
    ``claimed`` status distinguish an authorized message whose admission has
    not yet been attempted from one whose attempt is unresolved.
    """

    receipt_id: UUID
    status: TaskDetachedTurnReceiptStatus
    claimed_at: datetime
    admission_attempted_at: datetime | None
    admitted_at: datetime | None
    settled_at: datetime | None
    outcome_label: str | None


@dataclass(frozen=True, slots=True)
class _DiscussionSubjects:
    """The exact current subjects one discussion exchange is admitted at."""

    task: Task
    gate: OwnerGate
    reviewer: TaskAgentSession


# --- command validation ---------------------------------------------------------


def _require_uuid_command(value: object, name: str) -> UUID:
    """Reject a malformed command identifier before any read or effect."""
    if not isinstance(value, UUID):
        raise InvalidCommandError(f"{name} must be a UUID")
    return value


def _require_message(value: object) -> str:
    """Require one bounded nonblank Owner message; the text is sent verbatim."""
    if not isinstance(value, str) or not value.strip():
        raise InvalidCommandError("message must be a non-empty string")
    if len(value) > _MAX_DISCUSSION_MESSAGE_LENGTH:
        raise InvalidCommandError("message exceeds the bounded discussion message length")
    return value


def _require_reviewer_factory(value: object) -> ReviewerRuntimeAttachmentFactory:
    """Require the deferred Reviewer attachment factory dependency."""
    if not callable(value):
        raise InvalidCommandError(
            "reviewer_runtime_factory must be a callable returning one adapter "
            "attachment context manager for the exact resolved Reviewer binding"
        )
    return cast(ReviewerRuntimeAttachmentFactory, value)


def _require_observation_registrar(value: object) -> DiscussionObservationRegistrar:
    """Require the positive pre-admission observation registration seam."""
    register = getattr(value, "register_session", None)
    if not callable(register):
        raise InvalidCommandError(
            "observation_registrar does not implement the established-session registration seam"
        )
    return cast(DiscussionObservationRegistrar, value)


def _require_completion_handoff(value: object) -> DetachedTurnCompletionHandoff:
    """Require the post-commit completion handoff dependency."""
    handoff = getattr(value, "handoff_settled_completion", None)
    if not callable(handoff):
        raise InvalidCommandError(
            "completion_handoff does not implement the detached-completion handoff seam"
        )
    return cast(DetachedTurnCompletionHandoff, value)


# --- subject-form validation ------------------------------------------------------


def _require_pull_request_head_subject(pool: DatabasePool, *, task: Task, gate: OwnerGate) -> None:
    """Prove a PR-subject gate still binds the canonical PR and its exact head.

    The canonical TaskPullRequest is resolved from durable state (never a
    caller-supplied id), the gate's bound PR record must still be exactly
    that canonical record, and the gate's exact head SHA must equal the
    current reconciled head. Base-branch movement is deliberately never
    consulted; a changed head is a stale operation that applies nothing.
    """
    if gate.task_pull_request_id is None or gate.subject_head_sha is None:
        raise ConflictError(
            "the owner gate does not carry the canonical pull-request/head subject form"
        )
    pull_request = require_task_pull_request_head(
        pool, task=task, expected_head_sha=gate.subject_head_sha
    )
    if pull_request.id != gate.task_pull_request_id:
        raise StaleOperationError(
            "the owner gate no longer addresses the task's canonical pull request; "
            "the operation is stale"
        )


def _validate_gate_subject(pool: DatabasePool, *, task: Task, gate: OwnerGate) -> None:
    """Prove the addressed gate still binds its exact real subject form.

    Every pending gate type admits discussion only against its real
    subject: plan-subject gates need the exact current PlanRevision;
    ``PR_AUTHORIZATION`` needs its exact already-recorded committed head
    and no invented PR; ``MERGE_DECISION`` and the PR/head form of
    ``REVIEW_RESOLUTION`` need the canonical TaskPullRequest plus the
    exact reconciled current head. E4 never decides review, GitHub
    reconciliation, permission to implement, or permission to merge — it
    only refuses discussion at foreign, stale, or replaced subjects.
    """
    if gate.gate_type is OwnerGateType.IMPLEMENTATION_AUTHORIZATION:
        if gate.plan_revision_id is None:
            raise ConflictError(
                "the implementation authorization gate carries no plan revision subject"
            )
        require_current_plan_revision(pool, task=task, plan_revision_id=gate.plan_revision_id)
        return
    if gate.gate_type is OwnerGateType.PR_AUTHORIZATION:
        # The pre-PR gate's subject IS its already-recorded committed head;
        # the domain/DB CHECK guarantees the exact form. No TaskPullRequest
        # is resolved, compared, or invented here.
        if (
            gate.subject_head_sha is None
            or gate.plan_revision_id is not None
            or gate.task_pull_request_id is not None
        ):
            raise ConflictError(
                "the pr_authorization gate does not carry the exact pre-PR "
                "committed-head subject form"
            )
        return
    if gate.gate_type is OwnerGateType.MERGE_DECISION:
        _require_pull_request_head_subject(pool, task=task, gate=gate)
        return
    if gate.gate_type is OwnerGateType.REVIEW_RESOLUTION:
        if gate.plan_revision_id is not None:
            require_current_plan_revision(pool, task=task, plan_revision_id=gate.plan_revision_id)
            return
        _require_pull_request_head_subject(pool, task=task, gate=gate)
        return
    raise ConflictError("the owner gate type does not admit reviewer discussion")


# --- reviewer session resolution ----------------------------------------------------


def _require_ready_reviewer(
    pool: DatabasePool, *, workspace_id: UUID, task_id: UUID
) -> TaskAgentSession:
    """Resolve the exact admitted READY Reviewer role/session binding.

    Discussion reuses the original admitted Reviewer binding only: a
    missing, cross-Workspace, or cross-Task binding is the uniform
    not-found outcome; a CONNECTING/LOST/ENDED binding is a conflict. No
    rebinding, second session, or fresh initialization ever happens here —
    session loss is a blocking/recovery condition, never a silent
    replacement.
    """
    binding = get_task_agent_session(pool, task_id=task_id, role=WorkflowRole.REVIEWER)
    if binding is None or binding.workspace_id != workspace_id or binding.task_id != task_id:
        raise NotFoundError("the requested reviewer session binding is not available for this task")
    if binding.lifecycle_status is not TaskSessionLifecycleStatus.READY:
        raise ConflictError(
            "the reviewer session binding is not READY; the discussion uses the exact "
            "admitted session only and never recreates or reinitializes one"
        )
    if binding.external_session_id is None:
        raise ConflictError("the reviewer session binding has no external session identity")
    return binding


def _require_discussion_gate(gate: OwnerGate) -> None:
    """Refuse discussion at any gate type outside the permitted three.

    Discussion is limited to the currently pending REVIEW_RESOLUTION,
    IMPLEMENTATION_AUTHORIZATION, and MERGE_DECISION gates — never a
    PR_AUTHORIZATION gate, never an exceptional blocked-Task gate, never
    BLOCKED state. A non-permitted current gate is a typed conflict.
    """
    if gate.gate_type not in (
        OwnerGateType.REVIEW_RESOLUTION,
        OwnerGateType.IMPLEMENTATION_AUTHORIZATION,
        OwnerGateType.MERGE_DECISION,
    ):
        raise ConflictError("discussion is not admitted at this owner gate type")


def _load_discussion_subjects(
    pool: DatabasePool,
    *,
    workspace_id: UUID,
    task_id: UUID,
    owner_gate_id: UUID,
) -> _DiscussionSubjects:
    """Load and prove the exact current discussion subjects under the Task lock.

    The one shared currentness loader of the discussion's authorization
    transactions (prepare and admission): the Task row locked ``FOR UPDATE``
    first — serializing the gate-bound receipt facts and the admission stamp
    against every concurrent Task mutation (an Owner gate resolution stamps
    the gate and rotates the token under the same row lock) — then the exact
    current non-archived ``WAITING_FOR_OWNER`` Task, the exact pending
    current permitted gate addressed by ``owner_gate_id``, the gate's real
    subject form, and the exact admitted READY Reviewer binding. The Task's
    own current ``state_token`` is the only currentness anchor these
    Owner-facing invocations have: the caller supplies no token.
    """
    task = task_records.get_task_for_update(pool, task_id=task_id)
    if task is None or task.workspace_id != workspace_id:
        raise NotFoundError("the requested task is not available in this workspace")
    if task.archived_at is not None:
        raise StaleOperationError("the task has already been archived; the operation is stale")
    if task.terminalization_outcome is not None:
        raise StaleOperationError(
            "the task's terminalization is committed and its archival is pending; "
            "the operation is stale"
        )
    if task.status is not TaskStatus.WAITING_FOR_OWNER:
        raise ConflictError("the addressed task is not waiting on its owner gate")
    gate = require_current_owner_gate(pool, task=task, owner_gate_id=owner_gate_id)
    _require_discussion_gate(gate)
    _validate_gate_subject(pool, task=task, gate=gate)
    reviewer = _require_ready_reviewer(pool, workspace_id=workspace_id, task_id=task_id)
    return _DiscussionSubjects(task=task, gate=gate, reviewer=reviewer)


def _refuse_discussion_receipt_quietly(
    pool: DatabasePool, receipt: TaskDetachedTurnReceipt, refusal_kind: str
) -> None:
    """Mark a proven never-dispatched discussion receipt refused, best-effort.

    The positively proven pre-effect refusal of the admission (a refused
    observation registration or a positively refused runtime admission):
    the receipt records ``refused`` so a never-dispatched message cannot
    hold the Reviewer session's single-outstanding slot. A failed refusal
    write is contained and logged — the durable state owns recovery — and
    never turns into reported success.
    """
    try:
        refused = turn_records.refuse_task_detached_turn_receipt(
            pool,
            receipt_id=receipt.id,
            task_id=receipt.task_id,
            workspace_id=receipt.workspace_id,
            refusal_kind=refusal_kind,
        )
    except Exception as error:  # noqa: BLE001 - contained refusal write
        logger.warning(
            "the discussion receipt could not be marked refused (%s); "
            "authoritative recovery is required",
            type(error).__name__,
        )
        return
    if refused is None:
        logger.warning(
            "the discussion receipt could not be marked refused; it is no longer claimed"
        )


def release_prepared_discussion_receipt(
    pool: DatabasePool,
    *,
    receipt_id: UUID,
    task_id: UUID,
    workspace_id: UUID,
) -> None:
    """Release a prepared discussion receipt after a proven submission refusal.

    The Owner-facing POST's known-refusal path (issue #339): the receipt was
    committed and the admission operation submission was positively refused,
    so no worker will ever run and the claimed receipt would otherwise hold
    the Reviewer session's single-outstanding slot forever. The receipt is
    settled ``refused`` with the submission-refusal kind through the
    existing one-shot refusal transition — a raced concurrent admission (an
    uncertainty that turns out to have been delivered) is a rejected no-op,
    never a false release. The caller maps the refusal to its honest HTTP
    failure afterwards; nothing here reports success.
    """
    try:
        refused = turn_records.refuse_task_detached_turn_receipt(
            pool,
            receipt_id=receipt_id,
            task_id=task_id,
            workspace_id=workspace_id,
            refusal_kind=_SUBMISSION_REFUSAL_KIND,
        )
    except Exception as error:  # noqa: BLE001 - contained refusal write
        logger.warning(
            "the prepared discussion receipt could not be released after its "
            "submission refusal (%s); authoritative recovery is required",
            type(error).__name__,
        )
        return
    if refused is None:
        logger.warning(
            "the prepared discussion receipt could not be released after its "
            "submission refusal; it is no longer claimed"
        )


# --- formal-result recognition -------------------------------------------------------


def _isolated_standalone_object(text: str) -> dict[str, Any] | None:
    """Isolate a standalone unfenced or single-fenced JSON object, or None.

    The whole normalized text must be exactly one JSON object, or exactly
    one fenced code block whose inner content is one JSON object with
    nothing outside the fence. Prose-wrapped, buried, quoted, or example
    JSON, multiple objects, and malformed spans are ordinary conversation,
    never extracted.
    """
    normalized = text.strip()
    if not normalized:
        return None
    try:
        direct = json.loads(normalized)
    except ValueError:
        direct = None
    if isinstance(direct, dict):
        return direct
    fenced = _FENCED_OBJECT.match(normalized)
    if fenced is None:
        return None
    try:
        inner = json.loads(fenced.group("body"))
    except ValueError:
        return None
    return inner if isinstance(inner, dict) else None


def _recognize_formal_review_result(text: str) -> ReviewResult | None:
    """Recognize one standalone schema-valid ``review_result``, or None.

    Only an explicitly-typed, schema-valid standalone ``review_result``
    becomes a formal Reviewer trigger. The existing #65 parser validates
    and types the already-isolated candidate; any protocol failure —
    unknown type, unsupported schema version, schema-invalid payload,
    malformed object — and any other response family leave the reply as
    ordinary displayed conversation. No parser behavior is duplicated,
    changed, or extended here.
    """
    payload = _isolated_standalone_object(text)
    if payload is None:
        return None
    try:
        formal = parse_formal_response(payload)
    except ProtocolError:
        return None
    if not isinstance(formal, ReviewResult):
        return None
    return formal


# --- audit helpers ---------------------------------------------------------------------


def _reviewer_actor(reviewer: TaskAgentSession) -> event_coordination.WorkflowActorContext:
    """The safe REVIEWER logical actor identity: the exact session UUID."""
    return event_coordination.WorkflowActorContext(WorkflowEventActor.REVIEWER, str(reviewer.id))


def _record_ordinary_exchange_events(
    transaction_pool: DatabasePool,
    *,
    workspace_id: UUID,
    task_id: UUID,
    gate: OwnerGate,
    reviewer: TaskAgentSession,
    profile_id: UUID,
) -> None:
    """Record the two direction-only message facts of one ordinary exchange.

    Called inside the same short transaction as the post-send recheck, so a
    still-current ordinary exchange commits its audit atomically and a
    raced resolution records nothing. Direction-only context; the mapping
    helper cannot carry text.
    """
    event_coordination.record_owner_reviewer_discussion_message_event(
        transaction_pool,
        workspace_id=workspace_id,
        task_id=task_id,
        owner_gate_id=gate.id,
        actor=event_coordination.owner_actor(profile_id),
        direction=event_coordination.OwnerReviewerDiscussionDirection.OWNER_TO_REVIEWER,
    )
    event_coordination.record_owner_reviewer_discussion_message_event(
        transaction_pool,
        workspace_id=workspace_id,
        task_id=task_id,
        owner_gate_id=gate.id,
        actor=_reviewer_actor(reviewer),
        direction=event_coordination.OwnerReviewerDiscussionDirection.REVIEWER_TO_OWNER,
    )


# --- the detached dispatch: prepare (synchronous) and admit (asynchronous) ---


def prepare_owner_reviewer_discussion(
    pool: DatabasePool,
    *,
    profile_id: UUID,
    workspace_id: UUID,
    task_id: UUID,
    owner_gate_id: UUID,
    message: str,
) -> PreparedOwnerReviewerDiscussion:
    """Authorize one Owner message and commit its gate-bound discussion receipt.

    The synchronous prepare of the split Owner-facing seam (issue #339):
    every immediately detectable failure is a typed error before any
    submission. One short transaction (the Task row ``FOR UPDATE`` taken
    first) proves the account-operational barrier, the exact authenticated
    Owner Workspace/Task authorization for the claimed ``profile_id`` — never
    the Workspace's recorded owner as proof of an unproven caller — and the
    exact current discussion subjects, then commits the gate-bound receipt
    companion atomically. The single-outstanding-receipt invariant is the
    writer-exclusion backstop: a concurrent outstanding run on the exact
    Reviewer session binding is a typed conflict. The message is validated
    and bounded here and travels with the operation invocation only.
    """
    with application_span(_SERVICE_TRACER_SCOPE, _PREPARE_SPAN_NAME) as span:
        # --- Phase 0: command shape (no I/O). ---
        _require_uuid_command(profile_id, "profile_id")
        _require_uuid_command(workspace_id, "workspace_id")
        _require_uuid_command(task_id, "task_id")
        _require_uuid_command(owner_gate_id, "owner_gate_id")
        _require_message(message)
        annotate_span(
            span,
            operation=_PREPARE_SPAN_NAME,
            workspace_id=str(workspace_id),
            task_id=str(task_id),
        )

        # --- Phase 1: authorization + currentness + the receipt companion. ---
        with composed_transaction(pool) as transaction_pool:
            require_account_operational(transaction_pool, profile_id=profile_id)
            require_workspace_task(
                transaction_pool,
                profile_id=profile_id,
                workspace_id=workspace_id,
                task_id=task_id,
            )
            subjects = _load_discussion_subjects(
                transaction_pool,
                workspace_id=workspace_id,
                task_id=task_id,
                owner_gate_id=owner_gate_id,
            )
            receipt = record_task_detached_turn_receipt(
                transaction_pool,
                workspace_id=workspace_id,
                task_id=task_id,
                task_agent_session_id=subjects.reviewer.id,
                operation=PendingTaskRuntimeInteractionOperation.OWNER_REVIEWER_DISCUSSION,
                original_attempt_kind=OriginalAttemptKind.OWNER_GATE_DISCUSSION,
                original_attempt_id=owner_gate_id,
                expected_formal_family=OWNER_REVIEWER_DISCUSSION_FAMILY,
            )
        return PreparedOwnerReviewerDiscussion(
            receipt=receipt,
            owner_gate=subjects.gate,
            reviewer_session=subjects.reviewer,
            task=subjects.task,
        )


def admit_owner_reviewer_discussion_turn(
    pool: DatabasePool,
    *,
    profile_id: UUID,
    workspace_id: UUID,
    task_id: UUID,
    owner_gate_id: UUID,
    receipt_id: UUID,
    message: str,
    reviewer_runtime_factory: ReviewerRuntimeAttachmentFactory,
    observation_registrar: DiscussionObservationRegistrar,
    completion_handoff: DetachedTurnCompletionHandoff,
) -> AdmittedOwnerReviewerDiscussion:
    """Admit the prepared Owner message as ONE detached turn, exactly once.

    The asynchronous admission of the split Owner-facing seam (issue #339).
    One short reproof transaction (the Task row lock first, then the receipt
    row) re-proves the claimed ``profile_id``'s authority and every exact
    current subject, requires the receipt still claimed and unstamped, and
    takes the receipt's set-once one-shot admission stamp — a duplicate or
    re-delivered execution of the same prepared message can never reach a
    second enqueue. Then, with no transaction open: the exact Reviewer
    attachment (a positively proven attachment failure refuses the receipt
    and frees the session's slot — the message was never enqueued; the
    detached capability is duck-typed fail-closed), the positive
    observation registration, the verbatim message enqueue, and the
    acknowledgement attach. A positively proven refusal — attachment,
    registration, or runtime — marks the receipt ``refused`` so the
    never-dispatched message cannot hold the Reviewer session's slot; an
    uncertain admission stays conservatively claimed and is never
    re-dispatched. The attach consumes any staged early completion, and its
    post-commit handoff runs when a staged completion settled the exchange
    within this invocation.
    """
    with application_span(_SERVICE_TRACER_SCOPE, _ADMIT_SPAN_NAME) as span:
        # --- Phase 0: command shape (no I/O). ---
        _require_uuid_command(profile_id, "profile_id")
        _require_uuid_command(workspace_id, "workspace_id")
        _require_uuid_command(task_id, "task_id")
        _require_uuid_command(owner_gate_id, "owner_gate_id")
        _require_uuid_command(receipt_id, "receipt_id")
        _require_message(message)
        registrar = _require_observation_registrar(observation_registrar)
        handoff = _require_completion_handoff(completion_handoff)
        reviewer_factory = _require_reviewer_factory(reviewer_runtime_factory)
        annotate_span(
            span,
            operation=_ADMIT_SPAN_NAME,
            workspace_id=str(workspace_id),
            task_id=str(task_id),
        )

        # --- Phase 1: the reproof + the one-shot admission stamp. ---
        with composed_transaction(pool) as transaction_pool:
            require_account_operational(transaction_pool, profile_id=profile_id)
            require_workspace_task(
                transaction_pool,
                profile_id=profile_id,
                workspace_id=workspace_id,
                task_id=task_id,
            )
            subjects = _load_discussion_subjects(
                transaction_pool,
                workspace_id=workspace_id,
                task_id=task_id,
                owner_gate_id=owner_gate_id,
            )
            receipt = turn_records.get_task_detached_turn_receipt_for_update(
                transaction_pool, receipt_id=receipt_id
            )
            if (
                receipt is None
                or receipt.workspace_id != workspace_id
                or receipt.task_id != task_id
                or receipt.operation
                is not PendingTaskRuntimeInteractionOperation.OWNER_REVIEWER_DISCUSSION
                or receipt.original_attempt_id != owner_gate_id
                or receipt.task_agent_session_id != subjects.reviewer.id
            ):
                raise NotFoundError(
                    "the addressed discussion receipt is not available for this task"
                )
            if receipt.status is not TaskDetachedTurnReceiptStatus.CLAIMED:
                raise StaleOperationError(
                    "the discussion receipt is no longer claimed; the admission is stale"
                )
            if receipt.admission_attempted_at is not None:
                raise StaleOperationError(
                    "the discussion receipt's single admission attempt was already "
                    "taken; the prepared message is never re-enqueued"
                )
            stamped = turn_records.mark_task_detached_turn_admission_attempted(
                transaction_pool,
                receipt_id=receipt.id,
                task_id=task_id,
                workspace_id=workspace_id,
            )
            if not stamped:  # pragma: no cover - locked claimed receipt must stamp
                raise StaleOperationError(
                    "the discussion receipt's one-shot admission stamp could not be "
                    "taken; the message is never re-enqueued"
                )

        # --- Phases 2–5: attachment, registration, enqueue, attach — no
        # database transaction spans any of them. ---
        reviewer_external_session_id = subjects.reviewer.external_session_id
        if reviewer_external_session_id is None or not reviewer_external_session_id.strip():
            raise ConflictError("the reviewer session binding carries no external session identity")
        with ExitStack() as attachment_stack:
            # The exact Reviewer attachment. A positively proven attachment
            # failure — a disabled connection, an unusable credential, an
            # invalid configuration, a failed SDK attachment — is strictly
            # pre-admission evidence: the message cannot have been enqueued
            # yet, so the receipt is refused through the existing one-shot
            # refusal path and the Reviewer session's single-outstanding slot
            # is freed. Typed application errors keep their identity.
            try:
                runtime = attachment_stack.enter_context(reviewer_factory(subjects.reviewer))
            except ApplicationError as error:
                logger.warning(
                    "the discussion attachment was refused before any admission (%s); "
                    "the receipt is refused and the message never dispatched",
                    type(error).__name__,
                )
                _refuse_discussion_receipt_quietly(pool, receipt, _ATTACHMENT_REFUSAL_KIND)
                raise
            except Exception as error:
                logger.warning(
                    "the discussion attachment failed before any admission (%s); "
                    "the receipt is refused and the message never dispatched",
                    type(error).__name__,
                )
                _refuse_discussion_receipt_quietly(pool, receipt, _ATTACHMENT_REFUSAL_KIND)
                raise ExternalOperationFailedError(
                    "the reviewer runtime attachment failed before any admission; "
                    "the receipt is refused and the message never dispatched"
                ) from error
            # The detached capability is duck-typed fail-closed BEFORE any
            # admission effect: a runtime without it cannot serve the
            # detached discussion, and the just-stamped receipt is refused.
            admit = getattr(runtime, "admit_detached_turn", None)
            if not callable(admit):
                _refuse_discussion_receipt_quietly(pool, receipt, _ADMISSION_REFUSAL_KIND)
                raise ExternalOperationFailedError(
                    "the reviewer runtime does not implement the detached turn "
                    "capability; the discussion admission failed before any effect"
                )
            admit_detached_turn = cast(Callable[[str, str], AgentTurnAdmission], admit)

            # The positive pre-admission observation registration: the
            # positively-required gate. A refusal is a proven pre-effect
            # failure: the receipt is marked refused, so a never-dispatched
            # message cannot hold the session's single-outstanding slot.
            try:
                registrar.register_session(
                    workspace_id=workspace_id,
                    task_id=task_id,
                    task_agent_session_id=subjects.reviewer.id,
                    connection_id=subjects.reviewer.connection_id,
                    external_session_id=reviewer_external_session_id,
                )
            except ObservationRegistrationRefusedError as error:
                _refuse_discussion_receipt_quietly(pool, receipt, _REGISTRATION_REFUSAL_KIND)
                raise ExternalOperationFailedError(
                    "the discussion observation registration was refused before "
                    "any admission; the message never dispatched"
                ) from error
            except Exception as error:
                logger.warning(
                    "the discussion observation registration failed (%s) before any "
                    "admission; the receipt is refused and the message never dispatched",
                    type(error).__name__,
                )
                _refuse_discussion_receipt_quietly(pool, receipt, _REGISTRATION_REFUSAL_KIND)
                raise ExternalOperationFailedError(
                    "the discussion observation registration failed before any "
                    "admission; the message never dispatched"
                ) from error

            # The ONE verbatim enqueue. Uncertainty keeps the receipt
            # conservatively claimed (stamped) and never re-dispatches; a
            # positively identified refusal is proven pre-effect, so the
            # receipt is marked refused and the admission fails known.
            try:
                admission = admit_detached_turn(reviewer_external_session_id, message)
            except AgentRuntimeUncertainOutcomeError as error:
                logger.warning(
                    "the detached discussion admission ended with an uncertain outcome; "
                    "the receipt stays conservatively claimed and is never re-dispatched"
                )
                raise ExternalOperationUncertainError(
                    "the outcome of the discussion admission is unknown; the receipt "
                    "stays conservatively claimed and the message is never re-dispatched"
                ) from error
            except AgentRuntimeError as error:
                _refuse_discussion_receipt_quietly(pool, receipt, _ADMISSION_REFUSAL_KIND)
                raise ExternalOperationRejectedError(
                    "the discussion admission was refused before any effect; "
                    "the receipt is refused and the message never dispatched"
                ) from error

            # The acknowledgement attach (one short transaction). A staged
            # early completion is consumed exactly once here and routed
            # through the ordinary settlement, so a discussion exchange that
            # finished before the acknowledgement was persisted is
            # correlated, never ignored or duplicated.
            try:
                outcome: TurnSettlementOutcome = attach_acknowledged_turn(
                    pool,
                    workspace_id=workspace_id,
                    task_id=task_id,
                    receipt_id=receipt.id,
                    admitted_turn_reference=admission.turn_reference,
                )
            except ApplicationError:
                raise
            except Exception as error:
                raise ExternalOperationFailedError(
                    "the discussion acknowledgement could not be durably attached "
                    "after its known admission; authoritative recovery is required "
                    "and the message is never re-dispatched"
                ) from error

        # The attachment is released; the handoff is strictly post-dispatch
        # and never holds the runtime lease.
        if outcome.kind is TurnSettlementKind.SETTLED_VALIDATED_RESULT:
            try:
                handoff.handoff_settled_completion(
                    workspace_id=workspace_id,
                    task_id=task_id,
                    operation=PendingTaskRuntimeInteractionOperation.OWNER_REVIEWER_DISCUSSION,
                )
            except Exception as error:  # noqa: BLE001 - contained handoff
                logger.warning(
                    "the post-admission discussion completion handoff failed (%s); "
                    "the durable state owns recovery",
                    type(error).__name__,
                )
        if (
            outcome.kind is TurnSettlementKind.SETTLED_INTERRUPTED_TURN
            and outcome.receipt is not None
        ):
            # A staged interrupted completion settled the receipt within
            # this admission (issue #342): the ONE post-commit completion
            # handoff owns the Interrupt settlement — the attached adapter's
            # D6b guard release and the annotated intent's outcome recording
            # — exactly as it owns the validated families' continuations.
            # Strictly post-attach, contained best-effort inside the
            # handoff, never a workflow authority; a failed settlement
            # leaves the durable annotated intent exactly as committed.
            handoff.handoff_settled_interrupt(
                workspace_id=workspace_id,
                task_id=task_id,
                task_agent_session_id=outcome.receipt.task_agent_session_id,
                external_session_id=reviewer_external_session_id,
            )
        reloaded_task = task_records.get_task(pool, task_id=task_id)
        if reloaded_task is None or reloaded_task.workspace_id != workspace_id:
            raise NotFoundError("the requested task is not available in this workspace")
        return AdmittedOwnerReviewerDiscussion(
            receipt=outcome.receipt if outcome.receipt is not None else receipt,
            owner_gate=subjects.gate,
            reviewer_session=subjects.reviewer,
            task=reloaded_task,
            settlement_outcome=outcome,
        )


# --- the observed completion settlement ----------------------------------------


def _settle_observed_formal_continuation(
    transaction_pool: DatabasePool,
    *,
    owner_profile_id: UUID,
    workspace_id: UUID,
    task_id: UUID,
    expected_state_token: UUID,
    gate: OwnerGate,
    formal_result: ReviewResult,
) -> None:
    """Dispatch one actionable formal continuation to its owning settlement phase.

    The gate-specific decision rules on the recognized standalone
    ``review_result``: any formal result at a REVIEW_RESOLUTION gate settles
    that gate through the owning E3c (plan-subject) or E9 (PR/head-subject)
    resolution settlement; at a plan-subject IMPLEMENTATION_AUTHORIZATION
    gate only a CHANGES_REQUESTED reaches the owning E3 supersession; at a
    pending PR/head MERGE_DECISION gate only a CHANGES_REQUESTED reaches the
    owning E9 retirement. Each phase runs inside the caller's transaction —
    the exact new ReviewIteration, the gate retirement/settlement, the audit
    events, and the direction-only discussion audit commit atomically with
    the receipt's settlement — and no runtime or GitHub dependency is
    involved: the Producer work belongs to the post-commit completion
    handoff.
    """
    if gate.gate_type is OwnerGateType.REVIEW_RESOLUTION:
        if gate.plan_revision_id is not None:
            settle_observed_planning_review_resolution(
                transaction_pool,
                profile_id=owner_profile_id,
                workspace_id=workspace_id,
                task_id=task_id,
                expected_state_token=expected_state_token,
                result=formal_result,
                record_discussion_message_events=True,
            )
            return
        settle_observed_pr_review_resolution(
            transaction_pool,
            profile_id=owner_profile_id,
            workspace_id=workspace_id,
            task_id=task_id,
            expected_state_token=expected_state_token,
            result=formal_result,
            record_discussion_message_events=True,
        )
        return
    if gate.gate_type is OwnerGateType.IMPLEMENTATION_AUTHORIZATION:
        # Issue #219: at the implementation gate only a valid CHANGES_REQUESTED
        # reply is an actionable formal trigger; a standalone ACCEPTED — like
        # ordinary prose — stays advisory and the Owner may still approve.
        supersede_observed_implementation_authorization_with_changes(
            transaction_pool,
            profile_id=owner_profile_id,
            workspace_id=workspace_id,
            task_id=task_id,
            expected_state_token=expected_state_token,
            result=formal_result,
            record_discussion_message_events=True,
        )
        return
    # Issue #84: at the pending PR/head merge gate only a standalone valid
    # CHANGES_REQUESTED is actionable; the retirement seam re-validates the
    # outcome as its backstop.
    retire_observed_pr_merge_decision_with_changes(
        transaction_pool,
        profile_id=owner_profile_id,
        workspace_id=workspace_id,
        task_id=task_id,
        expected_state_token=expected_state_token,
        result=formal_result,
        record_discussion_message_events=True,
    )


def settle_observed_discussion_result(
    transaction_pool: DatabasePool,
    *,
    workspace_id: UUID,
    task_id: UUID,
    receipt: TaskDetachedTurnReceipt,
    result_text: str | None,
) -> ObservedDiscussionSettlement | None:
    """Settle one attributed discussion completion, atomic with the receipt.

    Called inside the correlation seam's settlement transaction (issue
    #339): the Task row ``FOR UPDATE`` taken FIRST — serializing the
    still-current decision against concurrent Task mutations — then the full
    currentness reproof (the exact gate-bound discussion subject still the
    Task's current pending permitted gate with its intact subject form, and
    the exact READY Reviewer binding). The raw attributed text — never
    funneled through the shared formal contract, because the discussion's
    permitted result is a non-formal attributed text — goes through the
    existing strict standalone ``review_result`` recognizer and the
    gate-specific decision rules: an actionable formal continuation commits
    through the owning E3c/E9 settlement phase; an ordinary or advisory
    exchange records exactly the two direction-only
    ``OWNER_REVIEWER_DISCUSSION_MESSAGE`` events. The Workspace's single
    Owner identity is the durable OWNER actor of the exchange audit and the
    owning settlements' authorization input — the v1 identity every
    discussion caller must re-prove as. ``None`` means the discussion moved
    on (a raced resolution, a superseded gate, a moved binding): the receipt
    settles ``result_not_applied`` and nothing is applied.
    """
    with application_span(_SERVICE_TRACER_SCOPE, _SETTLEMENT_SPAN_NAME) as span:
        annotate_span(
            span,
            operation=_SETTLEMENT_SPAN_NAME,
            workspace_id=str(workspace_id),
            task_id=str(task_id),
        )
        # --- The Task row lock FIRST. ---
        locked = task_records.get_task_for_update(transaction_pool, task_id=task_id)
        if locked is None or locked.workspace_id != workspace_id:
            raise NotFoundError("the requested task is not available in this workspace")
        # --- The currentness reproof under the lock. ---
        gate = gate_records.get_owner_gate(
            transaction_pool, owner_gate_id=receipt.original_attempt_id
        )
        if (
            gate is None
            or gate.workspace_id != workspace_id
            or gate.task_id != task_id
            or locked.current_owner_gate_id != gate.id
            or gate.status is not OwnerGateStatus.PENDING
        ):
            return None
        try:
            _require_discussion_gate(gate)
            _validate_gate_subject(transaction_pool, task=locked, gate=gate)
        except (ConflictError, NotFoundError, StaleOperationError):
            return None
        binding = get_task_agent_session(
            transaction_pool, task_id=task_id, role=WorkflowRole.REVIEWER
        )
        if (
            binding is None
            or binding.id != receipt.task_agent_session_id
            or binding.lifecycle_status is not TaskSessionLifecycleStatus.READY
        ):
            return None
        workspace = get_workspace(transaction_pool, workspace_id)
        if workspace is None:  # pragma: no cover - the Task's scope proved it
            raise NotFoundError("the requested workspace is not available")
        owner_profile_id = workspace.owner_profile_id
        expected_state_token = locked.state_token

        # --- Recognition and the gate-specific decision rules. ---
        recognized = (
            _recognize_formal_review_result(result_text) if result_text is not None else None
        )
        formal_result: ReviewResult | None = None
        if recognized is not None:
            if gate.gate_type is OwnerGateType.REVIEW_RESOLUTION:
                # Any standalone schema-valid formal result settles the
                # resolution gate through its owning E3c/E9 settlement.
                formal_result = recognized
            elif recognized.outcome == REVIEW_OUTCOME_CHANGES_REQUESTED and gate.gate_type in (
                OwnerGateType.IMPLEMENTATION_AUTHORIZATION,
                OwnerGateType.MERGE_DECISION,
            ):
                # Only a CHANGES_REQUESTED is actionable at the approval
                # gates; a standalone ACCEPTED — like ordinary prose — stays
                # advisory while the Owner authority is intact.
                formal_result = recognized
        if formal_result is not None:
            _settle_observed_formal_continuation(
                transaction_pool,
                owner_profile_id=owner_profile_id,
                workspace_id=workspace_id,
                task_id=task_id,
                expected_state_token=expected_state_token,
                gate=gate,
                formal_result=formal_result,
            )
            return ObservedDiscussionSettlement(
                kind=ObservedDiscussionSettlementKind.FORMAL_CONTINUATION,
                settlement_label=SETTLEMENT_DISCUSSION_FORMAL_CONTINUATION,
                owner_gate=gate,
                task=locked,
            )
        # Ordinary text, unrecognized JSON, or a nonactionable formal ACCEPTED
        # at an approval gate: Owner authority intact, only the existing
        # direction-only audit.
        _record_ordinary_exchange_events(
            transaction_pool,
            workspace_id=workspace_id,
            task_id=task_id,
            gate=gate,
            reviewer=binding,
            profile_id=owner_profile_id,
        )
        return ObservedDiscussionSettlement(
            kind=ObservedDiscussionSettlementKind.ORDINARY_EXCHANGE,
            settlement_label=SETTLEMENT_DISCUSSION_EXCHANGE,
            owner_gate=gate,
            task=locked,
        )


# --- the Owner-visible receipt-status projection -------------------------------


def get_owner_gate_discussion_status(
    pool: DatabasePool,
    *,
    profile_id: UUID,
    workspace_id: UUID,
    task_id: UUID,
    owner_gate_id: UUID,
) -> OwnerGateDiscussionStatus | None:
    """Project one gate-bound discussion's durable lifecycle for its Owner.

    The correlated asynchronous status projection of the split Owner-facing
    seam (issue #339): a read-only service over the receipts bound to the
    addressed durable identity — the exact OwnerGate UUID — scoped to the
    authenticated Owner's Task. The outstanding receipt (claimed or
    admitted) is reported while it exists, so the Owner can distinguish an
    authorized-and-queued message (claimed, not yet attempted), an
    unresolved admission attempt (claimed, stamped), and a runtime-acknowledged
    turn (admitted); otherwise the newest terminal record reports the
    recorded exchange, the dispatched formal continuation, a proven refusal,
    or an unresolved failure. ``None`` is the honest no-discussion fact: a
    foreign identity is indistinguishable from addressing something absent.
    Transport acceptance is never reported here as delivery — the receipt's
    own lifecycle is.
    """
    with application_span(_SERVICE_TRACER_SCOPE, _STATUS_SPAN_NAME) as span:
        # --- Phase 0: command shape (no I/O). ---
        _require_uuid_command(profile_id, "profile_id")
        _require_uuid_command(workspace_id, "workspace_id")
        _require_uuid_command(task_id, "task_id")
        _require_uuid_command(owner_gate_id, "owner_gate_id")
        annotate_span(
            span,
            operation=_STATUS_SPAN_NAME,
            workspace_id=str(workspace_id),
            task_id=str(task_id),
        )
        # --- Phase 1: the read-only authorization. ---
        with composed_transaction(pool) as transaction_pool:
            require_account_operational(transaction_pool, profile_id=profile_id)
            require_workspace_task(
                transaction_pool,
                profile_id=profile_id,
                workspace_id=workspace_id,
                task_id=task_id,
            )
        # --- The scoped read. ---
        receipts = turn_records.list_task_detached_turn_receipts_for_original_attempt(
            pool,
            workspace_id=workspace_id,
            task_id=task_id,
            original_attempt_kind=OriginalAttemptKind.OWNER_GATE_DISCUSSION,
            original_attempt_id=owner_gate_id,
        )
        if not receipts:
            return None
        outstanding = next(
            (
                receipt
                for receipt in receipts
                if receipt.status
                in (TaskDetachedTurnReceiptStatus.CLAIMED, TaskDetachedTurnReceiptStatus.ADMITTED)
            ),
            None,
        )
        chosen = outstanding if outstanding is not None else receipts[0]
        outcome_label: str | None = None
        if chosen.status is TaskDetachedTurnReceiptStatus.SETTLED:
            label = chosen.outcome.get(SETTLEMENT_OUTCOME_LABEL_KEY)
            outcome_label = label if isinstance(label, str) else None
        elif chosen.status is TaskDetachedTurnReceiptStatus.REFUSED:
            label = chosen.outcome.get(REFUSAL_OUTCOME_KIND_KEY)
            outcome_label = label if isinstance(label, str) else None
        return OwnerGateDiscussionStatus(
            receipt_id=chosen.id,
            status=chosen.status,
            claimed_at=chosen.claimed_at,
            admission_attempted_at=chosen.admission_attempted_at,
            admitted_at=chosen.admitted_at,
            settled_at=chosen.settled_at,
            outcome_label=outcome_label,
        )
