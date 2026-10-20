"""WorkflowEvent coordination for consequential service actions (issue #56).

The service-layer audit boundary between "a consequential workflow fact
occurred" and the durable event record: application services decide that a
consequential fact occurred and supply its semantic fields through the typed
helpers here; persistence
(:func:`openorc.persistence.events.record_workflow_event`) remains
responsible only for inserting the immutable record.

The coordination contract:

- These helpers are called from inside a service's
  :func:`openorc.services.transaction_composition.composed_transaction`
  block, so the canonical state mutation and the promised event insert
  commit as one database transaction. If event insertion fails, the paired
  canonical mutation rolls back; if the canonical mutation fails or is
  stale, no event is written — the service records the event only after its
  conditional write actually applied.
- There is deliberately no transport-facing "emit an arbitrary
  WorkflowEvent" command and no generic CRUD audit middleware. API routers
  and RQ jobs invoke business services; services produce the appropriate
  event through the focused mappings below. Events are append-only audit
  history, never a reconstruction source for current workflow state.
- Actor identity (:class:`WorkflowActorContext`) carries the locked v1
  ``WorkflowEventActor`` plus optional safe logical ``actor_id`` — the
  authenticated Profile UUID string for OWNER actions. Raw tokens or
  credentials, request bodies, mutable display names, and Workspace
  guidance prose never become actor or context material. HUMAN is not an
  OpenOrc actor (Phase 1 uses OWNER for human authority).
- Exact subject/context discipline: ``task_id`` is set only for truly
  Task-scoped events (a Task-subject event carries ``task_id`` and no
  redundant subject pair; a distinct modeled subject — an OwnerGate — rides
  on ``subject_type``/``subject_id`` over the Task scope). ``context`` is
  small, event-specific, canonical JSON: setting keys plus safe values. It
  never shadows canonical records (Task status, plan/review/gate/PR state)
  and never carries Workspace guidance prose, prompt/template metadata,
  hashes, or credential material.

Coordinated in this module for Phase 2A: the #53 Workspace configuration
changes (the demonstrated ``WORKSPACE_CONFIGURATION_CHANGED`` vocabulary
extension), the #54 terminal Task archival (``TASK_CANCELLED`` /
``TASK_COMPLETED``), and exact current OwnerGate resolution
(``OWNER_GATE_RESOLVED``). Branch binding and other Phase 1 facts that
deliberately have no event type remain non-evented; later Phase 2 workflow
leaves add their own mappings through this same boundary.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from uuid import UUID

from openorc.domain.connections import WorkflowRole
from openorc.domain.events import WorkflowEventActor, WorkflowEventType
from openorc.domain.gates import OwnerGateStatus
from openorc.domain.reviews import ReviewOutcome
from openorc.domain.tasks import TaskStatus
from openorc.persistence import events as event_records
from openorc.persistence.pool import DatabasePool
from openorc.services.errors import InvalidCommandError

__all__ = [
    "OwnerReviewerDiscussionDirection",
    "WorkflowActorContext",
    "owner_actor",
    "record_guidance_changed_event",
    "record_owner_gate_created_event",
    "record_owner_gate_resolved_event",
    "record_owner_reviewer_discussion_message_event",
    "record_plan_reviewed_event",
    "record_plan_revision_created_event",
    "record_plan_revision_revised_event",
    "record_planning_started_event",
    "record_review_iteration_limit_changed_event",
    "record_review_limit_reached_event",
    "record_role_binding_configuration_changed_event",
    "record_task_terminal_event",
]

# The open validated-text subject discriminators demonstrated so far. New
# subject kinds need no schema migration, only a deliberate mapping here.
_SUBJECT_TYPE_WORKSPACE = "workspace"
_SUBJECT_TYPE_OWNER_GATE = "owner_gate"
_SUBJECT_TYPE_PLAN_REVISION = "plan_revision"
_SUBJECT_TYPE_REVIEW_ITERATION = "review_iteration"
_SUBJECT_TYPE_REVIEW_LOOP = "review_loop"

_TERMINAL_TASK_EVENT: dict[TaskStatus, WorkflowEventType] = {
    TaskStatus.CANCELLED: WorkflowEventType.TASK_CANCELLED,
    TaskStatus.COMPLETED: WorkflowEventType.TASK_COMPLETED,
}


@dataclass(frozen=True, slots=True)
class WorkflowActorContext:
    """The safe application actor identity for one consequential service action.

    ``actor_type`` is from the locked v1 actor vocabulary; ``actor_id`` is
    the optional safe logical actor identity — the authenticated Profile
    UUID string for OWNER actions, and a stable role/session/runtime/GitHub
    identifier where a later capability has one. It deliberately carries
    nothing else: no raw tokens or credentials, no request bodies, no
    mutable display names, and no Workspace guidance prose ever become actor
    material. HUMAN is not an OpenOrc actor; human authority is OWNER.
    """

    actor_type: WorkflowEventActor
    actor_id: str | None

    def __post_init__(self) -> None:
        if not isinstance(self.actor_type, WorkflowEventActor):
            raise InvalidCommandError("actor context requires a WorkflowEventActor")
        if self.actor_id is not None and (
            not isinstance(self.actor_id, str) or not self.actor_id.strip()
        ):
            raise InvalidCommandError("actor_id must be a non-empty string when present")
        if self.actor_type is WorkflowEventActor.OWNER:
            # A human-authority action is identified by the canonical OpenOrc
            # application identity — the authenticated Profile UUID — never a
            # GitHub username, email, or display name.
            if self.actor_id is None:
                raise InvalidCommandError(
                    "OWNER actions require the authenticated Profile UUID as actor identity"
                )
            try:
                UUID(self.actor_id)
            except ValueError as error:
                raise InvalidCommandError(
                    "OWNER actor_id must be the canonical Profile UUID string"
                ) from error


def owner_actor(profile_id: UUID) -> WorkflowActorContext:
    """The actor context for one authenticated OWNER action."""
    return WorkflowActorContext(WorkflowEventActor.OWNER, str(profile_id))


def record_review_iteration_limit_changed_event(
    transaction_pool: DatabasePool,
    *,
    workspace_id: UUID,
    actor: WorkflowActorContext,
    previous_limit: int,
    new_limit: int,
) -> None:
    """Record one review-iteration-limit Workspace configuration change.

    Workspace-scoped (no Task), subject the Workspace, context the setting
    key plus the exact locked previous/new integer values — safe numbers
    only, never anything else from the Workspace row.
    """
    _require_uuid_command(workspace_id, "workspace_id")
    _require_int_command(previous_limit, "previous_limit")
    _require_int_command(new_limit, "new_limit")
    event_records.record_workflow_event(
        transaction_pool,
        workspace_id=workspace_id,
        event_type=WorkflowEventType.WORKSPACE_CONFIGURATION_CHANGED,
        actor_type=actor.actor_type,
        actor_id=actor.actor_id,
        subject_type=_SUBJECT_TYPE_WORKSPACE,
        subject_id=workspace_id,
        context={
            "setting": "review_iteration_limit",
            "previous": previous_limit,
            "new": new_limit,
        },
    )


def record_guidance_changed_event(
    transaction_pool: DatabasePool,
    *,
    workspace_id: UUID,
    actor: WorkflowActorContext,
) -> None:
    """Record one Workspace guidance configuration change.

    Workspace-scoped (no Task), subject the Workspace, context identifies
    only the ``guidance`` setting change. Owner-authored guidance prose —
    old or new, and equally any template/hash/version metadata — is
    deliberately absent: the helper accepts no prose parameter at all, so
    it can never be copied into the event.
    """
    _require_uuid_command(workspace_id, "workspace_id")
    event_records.record_workflow_event(
        transaction_pool,
        workspace_id=workspace_id,
        event_type=WorkflowEventType.WORKSPACE_CONFIGURATION_CHANGED,
        actor_type=actor.actor_type,
        actor_id=actor.actor_id,
        subject_type=_SUBJECT_TYPE_WORKSPACE,
        subject_id=workspace_id,
        context={"setting": "guidance"},
    )


def record_role_binding_configuration_changed_event(
    transaction_pool: DatabasePool,
    *,
    workspace_id: UUID,
    actor: WorkflowActorContext,
    role: WorkflowRole,
) -> None:
    """Record one workflow role-binding configuration change (issue #162).

    Workspace-scoped (no Task), subject the Workspace, context identifies
    only the ``workflow_role_binding`` setting and the affected role. The
    Owner's provider/model identifiers and role-prompt Markdown have no
    parameter in this helper at all, so they can never be copied into the
    event.
    """
    _require_uuid_command(workspace_id, "workspace_id")
    if not isinstance(role, WorkflowRole):
        raise InvalidCommandError("role-binding configuration events require a WorkflowRole")
    event_records.record_workflow_event(
        transaction_pool,
        workspace_id=workspace_id,
        event_type=WorkflowEventType.WORKSPACE_CONFIGURATION_CHANGED,
        actor_type=actor.actor_type,
        actor_id=actor.actor_id,
        subject_type=_SUBJECT_TYPE_WORKSPACE,
        subject_id=workspace_id,
        context={"setting": "workflow_role_binding", "role": role.value},
    )


def record_planning_started_event(
    transaction_pool: DatabasePool,
    *,
    workspace_id: UUID,
    task_id: UUID,
    actor: WorkflowActorContext,
) -> None:
    """Record one planning-start claim as its exact locked event type (issue #76).

    Task-scoped: the Task is the subject and ``task_id`` is the exact
    reference, so no redundant subject pair is added, and the helper accepts
    no provider/model/prompt/session parameter — the event records only that
    this Task's capacity admission claimed the planning startup. It is
    recorded by the same composed transaction that commits the
    ``queued``/``ready_to_plan`` -> ``planning`` conditional state write, so
    a stale or failed mutation never writes it and a successful claim never
    omits it.
    """
    _require_uuid_command(workspace_id, "workspace_id")
    _require_uuid_command(task_id, "task_id")
    event_records.record_workflow_event(
        transaction_pool,
        workspace_id=workspace_id,
        task_id=task_id,
        event_type=WorkflowEventType.PLANNING_STARTED,
        actor_type=actor.actor_type,
        actor_id=actor.actor_id,
    )


def record_task_terminal_event(
    transaction_pool: DatabasePool,
    *,
    workspace_id: UUID,
    task_id: UUID,
    actor: WorkflowActorContext,
    terminal_status: TaskStatus,
) -> None:
    """Record one terminal Task archival as its exact locked event type.

    Task-scoped: the Task is the subject and ``task_id`` is the exact
    reference, so no redundant subject pair is added. Cancellation maps to
    ``TASK_CANCELLED``, completion to ``TASK_COMPLETED``; any nonterminal
    status is an invalid command (never fabricated into an event).
    """
    _require_uuid_command(workspace_id, "workspace_id")
    _require_uuid_command(task_id, "task_id")
    event_type = (
        _TERMINAL_TASK_EVENT.get(terminal_status)
        if isinstance(terminal_status, TaskStatus)
        else None
    )
    if event_type is None:
        raise InvalidCommandError(
            "terminal archival events require a terminal TaskStatus (cancelled or completed)"
        )
    event_records.record_workflow_event(
        transaction_pool,
        workspace_id=workspace_id,
        task_id=task_id,
        event_type=event_type,
        actor_type=actor.actor_type,
        actor_id=actor.actor_id,
    )


def record_owner_gate_resolved_event(
    transaction_pool: DatabasePool,
    *,
    workspace_id: UUID,
    task_id: UUID,
    owner_gate_id: UUID,
    actor: WorkflowActorContext,
    outcome: OwnerGateStatus,
    review_iteration_id: UUID | None = None,
    review_outcome: ReviewOutcome | None = None,
) -> None:
    """Record one exact current OwnerGate resolution.

    Task-scoped; the primary modeled subject is the resolved OwnerGate
    (``subject_type``/``subject_id``) over the Task scope. Context carries
    the single safe semantic ``outcome`` value. Ordinary Owner-driven
    resolutions (approved/rejected/cancelled) carry exactly that context
    and nothing else. A Reviewer-driven settlement (``settled``, issue
    #211) additionally carries the settling formal result's safe
    references — the finalized ``review_iteration_id`` and its
    ``review_outcome`` (the accepted/changes_requested judgment) — and both
    are required with ``settled`` and forbidden with any Owner outcome, so
    Owner-resolved events keep their exact pre-#211 shape. A pending
    outcome is an invalid command, not an event. The summary, findings,
    gate record, and every other canonical fact are never copied into the
    event.
    """
    _require_uuid_command(workspace_id, "workspace_id")
    _require_uuid_command(task_id, "task_id")
    _require_uuid_command(owner_gate_id, "owner_gate_id")
    if not isinstance(outcome, OwnerGateStatus) or outcome is OwnerGateStatus.PENDING:
        raise InvalidCommandError(
            "resolved-gate events require a terminal OwnerGateStatus outcome "
            "(approved, rejected, cancelled, or settled)"
        )
    context: dict[str, str]
    if outcome is OwnerGateStatus.SETTLED:
        if review_iteration_id is None or review_outcome is None:
            raise InvalidCommandError(
                "settled-gate events require the settling review iteration "
                "id and its reviewer outcome"
            )
        _require_uuid_command(review_iteration_id, "review_iteration_id")
        if not isinstance(review_outcome, ReviewOutcome):
            raise InvalidCommandError("settled-gate events require a ReviewOutcome review_outcome")
        context = {
            "outcome": outcome.value,
            "review_iteration_id": str(review_iteration_id),
            "review_outcome": review_outcome.value,
        }
    else:
        if review_iteration_id is not None or review_outcome is not None:
            raise InvalidCommandError(
                "only settled-gate events carry the settling review iteration "
                "references; owner resolutions carry the outcome value alone"
            )
        context = {"outcome": outcome.value}
    event_records.record_workflow_event(
        transaction_pool,
        workspace_id=workspace_id,
        task_id=task_id,
        event_type=WorkflowEventType.OWNER_GATE_RESOLVED,
        actor_type=actor.actor_type,
        actor_id=actor.actor_id,
        subject_type=_SUBJECT_TYPE_OWNER_GATE,
        subject_id=owner_gate_id,
        context=context,
    )


class OwnerReviewerDiscussionDirection(StrEnum):
    """The safe direction-only vocabulary for Owner ↔ Reviewer discussion audit.

    One discussion exchange produces exactly two message facts: the Owner's
    message routed to the Reviewer, and the Reviewer's reply routed back.
    The direction value is the only conversation-shaped fact an event
    carries — never the message or reply text itself.
    """

    OWNER_TO_REVIEWER = "owner_to_reviewer"
    REVIEWER_TO_OWNER = "reviewer_to_owner"


def record_owner_reviewer_discussion_message_event(
    transaction_pool: DatabasePool,
    *,
    workspace_id: UUID,
    task_id: UUID,
    owner_gate_id: UUID,
    actor: WorkflowActorContext,
    direction: OwnerReviewerDiscussionDirection,
) -> None:
    """Record one direction-only Owner ↔ Reviewer discussion message fact (issue #79).

    Task-scoped; the primary modeled subject is the addressed OwnerGate
    (``subject_type``/``subject_id``) over the Task scope, because the
    discussion is admitted at exactly one pending current gate. Context
    carries the single safe ``direction`` value and nothing else. The
    helper accepts no message, reply, prose, prompt, or content parameter,
    so no discussion text can ever be copied into the event: this audits
    THAT the exchange occurred, not what was said. The caller supplies the
    safe actor context (the authenticated OWNER Profile identity for the
    Owner message, the REVIEWER session identity for the reply).
    """
    _require_uuid_command(workspace_id, "workspace_id")
    _require_uuid_command(task_id, "task_id")
    _require_uuid_command(owner_gate_id, "owner_gate_id")
    if not isinstance(actor, WorkflowActorContext):
        raise InvalidCommandError("discussion message events require a WorkflowActorContext actor")
    if not isinstance(direction, OwnerReviewerDiscussionDirection):
        raise InvalidCommandError(
            "discussion message events require an OwnerReviewerDiscussionDirection"
        )
    event_records.record_workflow_event(
        transaction_pool,
        workspace_id=workspace_id,
        task_id=task_id,
        event_type=WorkflowEventType.OWNER_REVIEWER_DISCUSSION_MESSAGE,
        actor_type=actor.actor_type,
        actor_id=actor.actor_id,
        subject_type=_SUBJECT_TYPE_OWNER_GATE,
        subject_id=owner_gate_id,
        context={"direction": direction.value},
    )


def record_plan_revision_created_event(
    transaction_pool: DatabasePool,
    *,
    workspace_id: UUID,
    task_id: UUID,
    plan_revision_id: UUID,
    actor: WorkflowActorContext,
) -> None:
    """Record one immutable PlanRevision becoming the Task's first plan (issue #77).

    Task-scoped; the primary modeled subject is the created PlanRevision
    (``subject_type``/``subject_id``) over the Task scope. The actor is the
    safe bound Producer logical identity supplied by the calling capability.
    The helper accepts no plan-text, findings, prompt, or repository-context
    parameter, so nothing but the modeled subject fact can ever be copied
    into the event.
    """
    _record_plan_revision_event(
        transaction_pool,
        workspace_id=workspace_id,
        task_id=task_id,
        plan_revision_id=plan_revision_id,
        actor=actor,
        event_type=WorkflowEventType.PLAN_REVISION_CREATED,
    )


def record_plan_revision_revised_event(
    transaction_pool: DatabasePool,
    *,
    workspace_id: UUID,
    task_id: UUID,
    plan_revision_id: UUID,
    actor: WorkflowActorContext,
) -> None:
    """Record one E3-directed PlanRevision superseding the current plan (issue #77).

    Task-scoped; the primary modeled subject is the new PlanRevision
    (``subject_type``/``subject_id``) over the Task scope. The actor is the
    safe bound Producer logical identity supplied by the calling capability.
    The helper accepts no plan-text, findings, prompt, or repository-context
    parameter, so nothing but the modeled subject fact can ever be copied
    into the event.
    """
    _record_plan_revision_event(
        transaction_pool,
        workspace_id=workspace_id,
        task_id=task_id,
        plan_revision_id=plan_revision_id,
        actor=actor,
        event_type=WorkflowEventType.PLAN_REVISION_REVISED,
    )


def _record_plan_revision_event(
    transaction_pool: DatabasePool,
    *,
    workspace_id: UUID,
    task_id: UUID,
    plan_revision_id: UUID,
    actor: WorkflowActorContext,
    event_type: WorkflowEventType,
) -> None:
    """Write one plan-revision event with its exact locked event type."""
    _require_uuid_command(workspace_id, "workspace_id")
    _require_uuid_command(task_id, "task_id")
    _require_uuid_command(plan_revision_id, "plan_revision_id")
    if not isinstance(actor, WorkflowActorContext):
        raise InvalidCommandError("plan revision events require a WorkflowActorContext actor")
    event_records.record_workflow_event(
        transaction_pool,
        workspace_id=workspace_id,
        task_id=task_id,
        event_type=event_type,
        actor_type=actor.actor_type,
        actor_id=actor.actor_id,
        subject_type=_SUBJECT_TYPE_PLAN_REVISION,
        subject_id=plan_revision_id,
    )


def record_plan_reviewed_event(
    transaction_pool: DatabasePool,
    *,
    workspace_id: UUID,
    task_id: UUID,
    review_iteration_id: UUID,
    actor: WorkflowActorContext,
    outcome: ReviewOutcome,
) -> None:
    """Record one finalized review decision on its iteration (issue #210).

    Task-scoped; the primary modeled subject is the finalized
    ReviewIteration — the canonical home of the recorded result — over the
    Task scope. Context carries the single safe semantic ``outcome`` value
    (accepted/changes_requested), mirroring the resolved-gate event: the
    summary, the findings document, and every other canonical iteration
    fact are never copied into the event. The actor is supplied by the
    calling capability — the exact REVIEWER session records the Reviewer's
    decision.
    """
    _require_uuid_command(workspace_id, "workspace_id")
    _require_uuid_command(task_id, "task_id")
    _require_uuid_command(review_iteration_id, "review_iteration_id")
    if not isinstance(outcome, ReviewOutcome):
        raise InvalidCommandError("reviewed events require a ReviewOutcome")
    event_records.record_workflow_event(
        transaction_pool,
        workspace_id=workspace_id,
        task_id=task_id,
        event_type=WorkflowEventType.PLAN_REVIEWED,
        actor_type=actor.actor_type,
        actor_id=actor.actor_id,
        subject_type=_SUBJECT_TYPE_REVIEW_ITERATION,
        subject_id=review_iteration_id,
        context={"outcome": outcome.value},
    )


def record_owner_gate_created_event(
    transaction_pool: DatabasePool,
    *,
    workspace_id: UUID,
    task_id: UUID,
    owner_gate_id: UUID,
    actor: WorkflowActorContext,
) -> None:
    """Record one pending OwnerGate decision record (issue #210).

    Task-scoped; the primary modeled subject is the new pending gate over
    the Task scope. No context is recorded: the gate's type, exact subject,
    and status are canonical on the gate row and are never shadowed into
    the event.
    """
    _require_uuid_command(workspace_id, "workspace_id")
    _require_uuid_command(task_id, "task_id")
    _require_uuid_command(owner_gate_id, "owner_gate_id")
    event_records.record_workflow_event(
        transaction_pool,
        workspace_id=workspace_id,
        task_id=task_id,
        event_type=WorkflowEventType.OWNER_GATE_CREATED,
        actor_type=actor.actor_type,
        actor_id=actor.actor_id,
        subject_type=_SUBJECT_TYPE_OWNER_GATE,
        subject_id=owner_gate_id,
    )


def record_review_limit_reached_event(
    transaction_pool: DatabasePool,
    *,
    workspace_id: UUID,
    task_id: UUID,
    review_loop_id: UUID,
    actor: WorkflowActorContext,
) -> None:
    """Record the FIRST exhaustion of a review loop's autonomous budget.

    Task-scoped; the primary modeled subject is the ReviewLoop whose
    snapshotted budget was crossed. No context is recorded: the
    snapshotted ``iteration_limit`` is canonical on the loop row and is
    never shadowed into the event. The calling capability emits this event
    exactly once per loop — on the first exhaustion only; later
    Owner-mediated disagreements create fresh REVIEW_RESOLUTION gates
    without re-representing the already-exhausted budget as newly reached.
    """
    _require_uuid_command(workspace_id, "workspace_id")
    _require_uuid_command(task_id, "task_id")
    _require_uuid_command(review_loop_id, "review_loop_id")
    event_records.record_workflow_event(
        transaction_pool,
        workspace_id=workspace_id,
        task_id=task_id,
        event_type=WorkflowEventType.REVIEW_LIMIT_REACHED,
        actor_type=actor.actor_type,
        actor_id=actor.actor_id,
        subject_type=_SUBJECT_TYPE_REVIEW_LOOP,
        subject_id=review_loop_id,
    )


def _require_uuid_command(value: object, name: str) -> None:
    """Reject a malformed UUID argument before any event is written."""
    if not isinstance(value, UUID):
        raise InvalidCommandError(f"{name} must be a UUID")


def _require_int_command(value: object, name: str) -> None:
    """Reject a malformed integer argument before any event is written."""
    if isinstance(value, bool) or not isinstance(value, int):
        raise InvalidCommandError(f"{name} must be an integer")
