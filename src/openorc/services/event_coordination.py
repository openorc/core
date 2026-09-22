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
from uuid import UUID

from openorc.domain.events import WorkflowEventActor, WorkflowEventType
from openorc.domain.gates import OwnerGateStatus
from openorc.domain.tasks import TaskStatus
from openorc.persistence import events as event_records
from openorc.persistence.pool import DatabasePool
from openorc.services.errors import InvalidCommandError

__all__ = [
    "WorkflowActorContext",
    "owner_actor",
    "record_guidance_changed_event",
    "record_owner_gate_resolved_event",
    "record_review_iteration_limit_changed_event",
    "record_task_terminal_event",
]

# The open validated-text subject discriminators demonstrated so far. New
# subject kinds need no schema migration, only a deliberate mapping here.
_SUBJECT_TYPE_WORKSPACE = "workspace"
_SUBJECT_TYPE_OWNER_GATE = "owner_gate"

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
) -> None:
    """Record one exact current OwnerGate resolution.

    Task-scoped; the primary modeled subject is the resolved OwnerGate
    (``subject_type``/``subject_id``) over the Task scope. Context carries
    the single safe semantic ``outcome`` value (approved/rejected/
    cancelled) — never the gate record, its canonical metadata, or any
    duplicated state. A pending outcome is an invalid command, not an
    event.
    """
    _require_uuid_command(workspace_id, "workspace_id")
    _require_uuid_command(task_id, "task_id")
    _require_uuid_command(owner_gate_id, "owner_gate_id")
    if not isinstance(outcome, OwnerGateStatus) or outcome is OwnerGateStatus.PENDING:
        raise InvalidCommandError(
            "resolved-gate events require a terminal OwnerGateStatus outcome "
            "(approved, rejected, or cancelled)"
        )
    event_records.record_workflow_event(
        transaction_pool,
        workspace_id=workspace_id,
        task_id=task_id,
        event_type=WorkflowEventType.OWNER_GATE_RESOLVED,
        actor_type=actor.actor_type,
        actor_id=actor.actor_id,
        subject_type=_SUBJECT_TYPE_OWNER_GATE,
        subject_id=owner_gate_id,
        context={"outcome": outcome.value},
    )


def _require_uuid_command(value: object, name: str) -> None:
    """Reject a malformed UUID argument before any event is written."""
    if not isinstance(value, UUID):
        raise InvalidCommandError(f"{name} must be a UUID")


def _require_int_command(value: object, name: str) -> None:
    """Reject a malformed integer argument before any event is written."""
    if isinstance(value, bool) or not isinstance(value, int):
        raise InvalidCommandError(f"{name} must be an integer")
