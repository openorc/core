"""Authoritative Task mutation services (issue #54).

The six foundational Task mutations — coarse-status movement, terminal
archival, canonical-branch binding, current PlanRevision installation,
current OwnerGate installation, and exact current-gate resolution — as
focused application services over the Phase 1 conditional persistence
primitives. Later transports, workers, and workflow commands compose these
operations instead of calling raw Task mutations directly; there is no
generic Task patch and no arbitrary subject bag.

The discipline every operation here enforces:

- The operation is validated as a command first (``InvalidCommandError``);
  nothing about current durable state is consulted to interpret it.
- Database-only validation and mutation compose inside one short
  ``composed_transaction`` (#51): the currentness guard reloads the
  addressed Task, installation mutations prove the named subject's
  Task/Workspace scope through the scope-only guards
  (:mod:`openorc.services.task_subject_guards`), and the conditional,
  ``state_token``-rotating persistence write applies the effect.
- A rejected conditional write (a ``None`` result) is never success, never
  a blind retry, and never a generic failure: the minimum authoritative
  state is reloaded under the Task row lock inside the same transaction
  (the #53 locked-read pattern, via the persistence locked re-read) and
  translated into the typed application outcomes — a missing subject is a
  ``NotFoundError``; an archived Task, a stale token, or raced subject
  state is a ``StaleOperationError``; a genuine current-state conflict
  (branch already bound, gate already installed) is a ``ConflictError``;
  a repository-wide canonical-branch collision (another current Task of
  the Repository already owns the branch) surfaces as the driver's
  ``UniqueViolation`` and is translated by the bind mutation into the same
  stable ``ConflictError`` without exposing the other Task; any otherwise
  unexplained rejection fails closed as a ``StaleOperationError``.
- A successful mutation returns the post-write Task carrying the newly
  rotated ``state_token``. Callers must continue from the returned token;
  a failed mutation never fabricates a replacement token.
- Exact current-gate resolution translates the persistence outcome
  contract (``RESOLVED``/``STALE``/``NO_OP``) into the same typed
  vocabulary instead of leaking persistence shapes to callers.

These operations preserve the repository/domain rules and validate
currentness only: they decide no workflow-stage transition policy (later
Phase 2E workflow capabilities decide when a particular transition is
allowed) and call no external system. Terminal archival and exact
current-gate resolution coordinate their locked event types
(``TASK_CANCELLED``/``TASK_COMPLETED``, ``OWNER_GATE_RESOLVED``) through
:mod:`openorc.services.event_coordination` inside the same composed
transaction (#56): a successful mutation commits its event with it, and a
failed or stale mutation writes no event. The remaining foundational
mutations (coarse-status movement, canonical-branch binding, plan/gate
installation) are deliberately non-evented Phase 1 facts. Actor
authorization is composed by the command capabilities that call these; the
audited mutations require the safe
:class:`~openorc.services.event_coordination.WorkflowActorContext`, while
the primitives here enforce exact-subject currentness and Workspace
linkage only.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import NoReturn
from uuid import UUID

from psycopg.errors import UniqueViolation

from openorc.domain.gates import OwnerGate, OwnerGateStatus
from openorc.domain.tasks import Task, TaskStatus
from openorc.observability import annotate_span, application_span
from openorc.persistence import gates as gate_records
from openorc.persistence import tasks as task_records
from openorc.persistence.pool import DatabasePool
from openorc.services import event_coordination
from openorc.services.errors import (
    ConflictError,
    InvalidCommandError,
    NotFoundError,
    StaleOperationError,
)
from openorc.services.task_subject_guards import (
    require_current_task,
    require_pending_owner_gate_in_task,
    require_plan_revision_in_task,
)
from openorc.services.transaction_composition import composed_transaction

__all__ = [
    "ResolvedOwnerGate",
    "archive_task",
    "bind_canonical_branch",
    "resolve_owner_gate",
    "set_current_owner_gate",
    "set_current_plan_revision",
    "update_task_status",
]

# Application-service span boundaries (issues #108/#109): every authoritative
# Task mutation opens one span at its use-case boundary. Subject guards, the
# locked re-read, and the conditional persistence writes run inside these
# spans without spans of their own; telemetry is observational and never
# workflow authority.
_TASK_MUTATIONS_TRACER_SCOPE = "openorc.services.task_mutations"
_UPDATE_TASK_STATUS_SPAN_NAME = "task_mutations.update_task_status"
_ARCHIVE_TASK_SPAN_NAME = "task_mutations.archive_task"
_BIND_CANONICAL_BRANCH_SPAN_NAME = "task_mutations.bind_canonical_branch"
_SET_CURRENT_PLAN_REVISION_SPAN_NAME = "task_mutations.set_current_plan_revision"
_SET_CURRENT_OWNER_GATE_SPAN_NAME = "task_mutations.set_current_owner_gate"
_RESOLVE_OWNER_GATE_SPAN_NAME = "task_mutations.resolve_owner_gate"


@dataclass(frozen=True, slots=True)
class ResolvedOwnerGate:
    """The success result of resolving the Task's exact current OwnerGate.

    Carries the resolved (immutable) gate record and the post-resolution
    Task with its rotated ``state_token``; stale and no-op outcomes raise
    typed errors instead of returning fabricated state.
    """

    gate: OwnerGate
    task: Task


def _require_uuid_command(value: object, name: str) -> None:
    """Reject a malformed UUID command argument before any state is touched."""
    if not isinstance(value, UUID):
        raise InvalidCommandError(f"{name} must be a UUID")


def _require_nonblank_command(value: object, name: str) -> None:
    """Reject a malformed string command argument before any state is touched."""
    if not isinstance(value, str) or not value.strip():
        raise InvalidCommandError(f"{name} must be a non-empty string")


def _raise_unexplained_rejection() -> NoReturn:
    """Fail closed on a rejected conditional write with no classified cause."""
    raise StaleOperationError(
        "the mutation did not apply against the current task state; the operation is stale"
    )


def _require_classified_currentness(
    transaction_pool: DatabasePool, *, task_id: UUID, expected_state_token: UUID
) -> Task:
    """Reload the Task row under lock to classify a rejected conditional write.

    Runs inside the same composed transaction as the failed conditional
    write: the locked re-read observes the durable state at write time (a
    concurrent token rotation or archival serializes on the row lock), so
    the classification never comes from a racy re-read. The typed outcomes:
    the Task row is gone → ``NotFoundError``; the Task is
    archived/terminal → ``StaleOperationError``; the caller's expected
    ``state_token`` no longer matches → ``StaleOperationError`` (never
    retried blindly). Callers translate any remaining family-specific
    conflict themselves and fail closed on anything unexplained.
    """
    task = task_records.get_task_for_update(transaction_pool, task_id)
    if task is None:
        raise NotFoundError("the requested task is not available in this workspace")
    if task.archived_at is not None:
        raise StaleOperationError("the task has already been archived; the operation is stale")
    if task.state_token != expected_state_token:
        raise StaleOperationError(
            "the task state has moved on since this operation was prepared; the operation is stale"
        )
    return task


def update_task_status(
    pool: DatabasePool,
    *,
    workspace_id: UUID,
    task_id: UUID,
    expected_state_token: UUID,
    status: TaskStatus,
) -> Task:
    """Move the current Task to a nonterminal coarse status.

    Terminal outcomes go exclusively through :func:`archive_task`. The
    currentness guard and the conditional, token-rotating persistence write
    compose inside one short transaction: a stale token or an archived Task
    applies nothing and raises ``StaleOperationError``. No workflow-stage
    transition policy is decided here — later workflow capabilities decide
    when a particular transition is allowed. A successful mutation returns
    the post-write Task carrying the newly rotated token.
    """
    with application_span(_TASK_MUTATIONS_TRACER_SCOPE, _UPDATE_TASK_STATUS_SPAN_NAME) as span:
        annotate_span(
            span,
            operation=_UPDATE_TASK_STATUS_SPAN_NAME,
            workspace_id=str(workspace_id),
            task_id=str(task_id),
        )
        _require_uuid_command(workspace_id, "workspace_id")
        _require_uuid_command(task_id, "task_id")
        _require_uuid_command(expected_state_token, "expected_state_token")
        if not isinstance(status, TaskStatus) or status.is_terminal:
            raise InvalidCommandError(
                "update_task_status requires a nonterminal TaskStatus; "
                "terminal outcomes go exclusively through archive_task"
            )
        with composed_transaction(pool) as transaction_pool:
            require_current_task(
                transaction_pool,
                workspace_id=workspace_id,
                task_id=task_id,
                expected_state_token=expected_state_token,
            )
            updated = task_records.update_task_status(
                transaction_pool,
                task_id,
                expected_state_token=expected_state_token,
                status=status,
            )
            if updated is None:
                _require_classified_currentness(
                    transaction_pool, task_id=task_id, expected_state_token=expected_state_token
                )
                _raise_unexplained_rejection()
    return updated


def archive_task(
    pool: DatabasePool,
    *,
    workspace_id: UUID,
    task_id: UUID,
    expected_state_token: UUID,
    terminal_status: TaskStatus,
    actor: event_coordination.WorkflowActorContext,
) -> Task:
    """Archive the current Task attempt with its terminal outcome.

    The only terminal transition path: ``terminal_status`` must be
    ``cancelled`` or ``completed``; archival atomically stamps the outcome,
    stamps ``archived_at``, and rotates the token. Cancellation ends
    OpenOrc orchestration without implicitly mutating GitHub artifacts —
    that external discipline belongs to later workflow capabilities, never
    to this primitive. A successful mutation returns the post-write Task
    with its newly rotated token, and its terminal event
    (``TASK_CANCELLED``/``TASK_COMPLETED``) commits in the same composed
    transaction through the supplied actor context; a stale or failed
    mutation writes no event.
    """
    with application_span(_TASK_MUTATIONS_TRACER_SCOPE, _ARCHIVE_TASK_SPAN_NAME) as span:
        annotate_span(
            span,
            operation=_ARCHIVE_TASK_SPAN_NAME,
            workspace_id=str(workspace_id),
            task_id=str(task_id),
        )
        _require_uuid_command(workspace_id, "workspace_id")
        _require_uuid_command(task_id, "task_id")
        _require_uuid_command(expected_state_token, "expected_state_token")
        if not isinstance(terminal_status, TaskStatus) or not terminal_status.is_terminal:
            raise InvalidCommandError(
                "archive_task requires a terminal TaskStatus (cancelled or completed); "
                "nonterminal transitions use update_task_status"
            )
        with composed_transaction(pool) as transaction_pool:
            require_current_task(
                transaction_pool,
                workspace_id=workspace_id,
                task_id=task_id,
                expected_state_token=expected_state_token,
            )
            updated = task_records.archive_task(
                transaction_pool,
                task_id,
                expected_state_token=expected_state_token,
                terminal_status=terminal_status,
            )
            if updated is None:
                _require_classified_currentness(
                    transaction_pool, task_id=task_id, expected_state_token=expected_state_token
                )
                _raise_unexplained_rejection()
            event_coordination.record_task_terminal_event(
                transaction_pool,
                workspace_id=workspace_id,
                task_id=task_id,
                actor=actor,
                terminal_status=terminal_status,
            )
    return updated


def bind_canonical_branch(
    pool: DatabasePool,
    *,
    workspace_id: UUID,
    task_id: UUID,
    expected_state_token: UUID,
    canonical_feature_branch: str,
) -> Task:
    """Bind the Task's canonical feature branch exactly once.

    Canonical branch ownership is a one-time Task-level fact: the branch is
    bound only while it is still unbound, and a current Task never rebinds
    or releases it — a rebinding attempt against current state is a
    ``ConflictError``, never a rewrite. Branch names are also exclusive
    across the Repository's current Tasks: when another current Task
    already owns the branch, the durable partial unique index rejects the
    write with a driver ``UniqueViolation`` that is translated here into
    the same stable ``ConflictError`` without exposing the other Task. A
    successful mutation returns the post-write Task with its newly rotated
    token.
    """
    with application_span(_TASK_MUTATIONS_TRACER_SCOPE, _BIND_CANONICAL_BRANCH_SPAN_NAME) as span:
        annotate_span(
            span,
            operation=_BIND_CANONICAL_BRANCH_SPAN_NAME,
            workspace_id=str(workspace_id),
            task_id=str(task_id),
        )
        _require_uuid_command(workspace_id, "workspace_id")
        _require_uuid_command(task_id, "task_id")
        _require_uuid_command(expected_state_token, "expected_state_token")
        _require_nonblank_command(canonical_feature_branch, "canonical_feature_branch")
        with composed_transaction(pool) as transaction_pool:
            require_current_task(
                transaction_pool,
                workspace_id=workspace_id,
                task_id=task_id,
                expected_state_token=expected_state_token,
            )
            try:
                updated = task_records.bind_canonical_branch(
                    transaction_pool,
                    task_id,
                    expected_state_token=expected_state_token,
                    canonical_feature_branch=canonical_feature_branch,
                )
            except UniqueViolation as exc:
                # The repository-wide branch-ownership index rejected the bind:
                # another current Task of this Repository already owns the
                # branch. Translated into the stable application conflict
                # without exposing the other Task; the composition rolls back
                # and nothing was applied.
                raise ConflictError(
                    "the canonical feature branch is already owned by another "
                    "current task in this repository"
                ) from exc
            if updated is None:
                current = _require_classified_currentness(
                    transaction_pool, task_id=task_id, expected_state_token=expected_state_token
                )
                if current.canonical_feature_branch is not None:
                    raise ConflictError(
                        "the task's canonical feature branch is already bound; "
                        "a current Task never rebinds or releases it"
                    )
                _raise_unexplained_rejection()
    return updated


def set_current_plan_revision(
    pool: DatabasePool,
    *,
    workspace_id: UUID,
    task_id: UUID,
    expected_state_token: UUID,
    plan_revision_id: UUID,
) -> Task:
    """Install the exact same-Task/Workspace PlanRevision as the current plan.

    The named revision is proved to belong to the addressed Task and
    Workspace through the scope-only resolver before persistence is
    invoked — a caller-supplied revision UUID is never trusted merely
    because the row exists, and the database composite foreign key stays a
    backstop. The pointer move is conditional on the expected token and
    rotates it; the pointer is never cleared implicitly (archival ends a
    Task's attempt). A successful mutation returns the post-write Task
    with its newly rotated token.
    """
    with application_span(
        _TASK_MUTATIONS_TRACER_SCOPE, _SET_CURRENT_PLAN_REVISION_SPAN_NAME
    ) as span:
        annotate_span(
            span,
            operation=_SET_CURRENT_PLAN_REVISION_SPAN_NAME,
            workspace_id=str(workspace_id),
            task_id=str(task_id),
        )
        _require_uuid_command(workspace_id, "workspace_id")
        _require_uuid_command(task_id, "task_id")
        _require_uuid_command(expected_state_token, "expected_state_token")
        _require_uuid_command(plan_revision_id, "plan_revision_id")
        with composed_transaction(pool) as transaction_pool:
            task = require_current_task(
                transaction_pool,
                workspace_id=workspace_id,
                task_id=task_id,
                expected_state_token=expected_state_token,
            )
            require_plan_revision_in_task(
                transaction_pool, task=task, plan_revision_id=plan_revision_id
            )
            updated = task_records.set_current_plan_revision(
                transaction_pool,
                task_id,
                expected_state_token=expected_state_token,
                plan_revision_id=plan_revision_id,
            )
            if updated is None:
                _require_classified_currentness(
                    transaction_pool, task_id=task_id, expected_state_token=expected_state_token
                )
                _raise_unexplained_rejection()
    return updated


def set_current_owner_gate(
    pool: DatabasePool,
    *,
    workspace_id: UUID,
    task_id: UUID,
    expected_state_token: UUID,
    owner_gate_id: UUID,
) -> Task:
    """Install a pending same-Task/Workspace OwnerGate as the current gate.

    The named gate is proved pending and Task/Workspace-scoped through the
    scope-only resolver before persistence is invoked. The write can never
    replace an existing current gate, so a raced or already-installed
    current gate classifies as a ``ConflictError`` — a pending gate must be
    resolved before another is installed — while a gate that lost its
    pending status in a race classifies as a stale operation. A successful
    mutation returns the post-write Task with its newly rotated token.
    """
    with application_span(_TASK_MUTATIONS_TRACER_SCOPE, _SET_CURRENT_OWNER_GATE_SPAN_NAME) as span:
        annotate_span(
            span,
            operation=_SET_CURRENT_OWNER_GATE_SPAN_NAME,
            workspace_id=str(workspace_id),
            task_id=str(task_id),
        )
        _require_uuid_command(workspace_id, "workspace_id")
        _require_uuid_command(task_id, "task_id")
        _require_uuid_command(expected_state_token, "expected_state_token")
        _require_uuid_command(owner_gate_id, "owner_gate_id")
        with composed_transaction(pool) as transaction_pool:
            task = require_current_task(
                transaction_pool,
                workspace_id=workspace_id,
                task_id=task_id,
                expected_state_token=expected_state_token,
            )
            require_pending_owner_gate_in_task(
                transaction_pool, task=task, owner_gate_id=owner_gate_id
            )
            updated = task_records.set_current_owner_gate(
                transaction_pool,
                task_id,
                expected_state_token=expected_state_token,
                owner_gate_id=owner_gate_id,
            )
            if updated is None:
                current = _require_classified_currentness(
                    transaction_pool, task_id=task_id, expected_state_token=expected_state_token
                )
                if current.current_owner_gate_id is not None:
                    raise ConflictError(
                        "the task already has a current owner gate installed; "
                        "resolve it before installing another"
                    )
                gate = gate_records.get_owner_gate(transaction_pool, owner_gate_id=owner_gate_id)
                if gate is None:
                    raise NotFoundError("the requested owner gate is not available for this task")
                if gate.status is not OwnerGateStatus.PENDING:
                    raise StaleOperationError(
                        "the owner gate is no longer pending; the operation is stale"
                    )
                _raise_unexplained_rejection()
    return updated


def resolve_owner_gate(
    pool: DatabasePool,
    *,
    workspace_id: UUID,
    task_id: UUID,
    expected_state_token: UUID,
    owner_gate_id: UUID,
    outcome: OwnerGateStatus,
    actor: event_coordination.WorkflowActorContext,
) -> ResolvedOwnerGate:
    """Resolve the Task's exact current OwnerGate with the expected Task token.

    The gate must be a pending gate of the addressed Task and Workspace
    (scope-only resolution). The authoritative resolution itself succeeds
    only while the gate is still the Task's current gate with the caller's
    expected Task token; the persistence outcome contract is translated
    here: ``RESOLVED`` returns the resolved (immutable) gate plus the
    post-resolution Task with its rotated token, while a stale token, a
    gate that is no longer current, or a raced already-resolved gate is a
    ``StaleOperationError`` and a vanished gate a ``NotFoundError``.
    Gate outcomes are one-shot facts; nothing is applied on any failure
    and the pending gate is never rewritten after losing currency. A
    successful resolution commits its ``OWNER_GATE_RESOLVED`` event in the
    same composed transaction through the supplied actor context; every
    failure path writes no event.
    """
    with application_span(_TASK_MUTATIONS_TRACER_SCOPE, _RESOLVE_OWNER_GATE_SPAN_NAME) as span:
        annotate_span(
            span,
            operation=_RESOLVE_OWNER_GATE_SPAN_NAME,
            workspace_id=str(workspace_id),
            task_id=str(task_id),
        )
        _require_uuid_command(workspace_id, "workspace_id")
        _require_uuid_command(task_id, "task_id")
        _require_uuid_command(expected_state_token, "expected_state_token")
        _require_uuid_command(owner_gate_id, "owner_gate_id")
        if not isinstance(outcome, OwnerGateStatus) or outcome is OwnerGateStatus.PENDING:
            raise InvalidCommandError(
                "resolve_owner_gate requires a terminal OwnerGateStatus outcome "
                "(approved, rejected, or cancelled)"
            )
        with composed_transaction(pool) as transaction_pool:
            task = require_current_task(
                transaction_pool,
                workspace_id=workspace_id,
                task_id=task_id,
                expected_state_token=expected_state_token,
            )
            require_pending_owner_gate_in_task(
                transaction_pool, task=task, owner_gate_id=owner_gate_id
            )
            resolution = gate_records.resolve_owner_gate(
                transaction_pool,
                owner_gate_id=owner_gate_id,
                outcome=outcome,
                expected_task_state_token=expected_state_token,
            )
            if resolution.outcome is gate_records.OwnerGateResolutionOutcome.RESOLVED:
                resolved_gate = resolution.gate
                resolved_task = resolution.task
                if resolved_gate is None or resolved_task is None:
                    _raise_unexplained_rejection()
                event_coordination.record_owner_gate_resolved_event(
                    transaction_pool,
                    workspace_id=workspace_id,
                    task_id=task_id,
                    owner_gate_id=owner_gate_id,
                    actor=actor,
                    outcome=outcome,
                )
                return ResolvedOwnerGate(gate=resolved_gate, task=resolved_task)
            if resolution.gate is None:
                raise NotFoundError("the requested owner gate is not available for this task")
            if resolution.outcome is gate_records.OwnerGateResolutionOutcome.NO_OP:
                raise StaleOperationError(
                    "the owner gate has already been resolved; the operation is stale"
                )
            raise StaleOperationError(
                "the owner gate is no longer the task's current gate; the operation is stale"
            )
