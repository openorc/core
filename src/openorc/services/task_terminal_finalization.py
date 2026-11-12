"""The shared authoritative terminal finalization (issue #317).

One terminalization decision, one finalizer. Every existing terminal
authority — the unified Owner Cancel command and the delegating E5/E7
gate-cancellation entrypoints (E10b), the trusted GitHub closed-unmerged
cancellation (E10c), and the confirmed-merge completion (E10a) — commits its
existing decision composition MINUS the inline archival, PLUS the exact-Task
terminalization commitment, and then hands off here strictly after that
commit. This module owns what happens next, in order:

1. **The optional bounded cleanup.** If the original admitted Cline Producer
   has an applicable checkout and the existing safety evidence permits
   deletion, the finalizer takes the one-shot ``cleanup_send_claimed_at``
   claim (a compare-and-set: a concurrent or duplicate finalizer can never
   double-send), realizes exactly one ``CLEANUP_WORKSPACE`` request through
   :mod:`openorc.services.task_terminal_cleanup`, and settles the claim
   afterwards — whatever the observed outcome. An unsettled claim owned by a
   concurrent attempt defers archival: a duplicate finalizer must never end
   a session, or release Connection capacity, while another attempt's
   bounded response-or-timeout window is still open. If the executing worker
   dies silently, the claim stays unsettled and terminal archival stays
   unfinished — the accepted system-wide v1 limitation recorded in the #317
   reconciliation comment; no deadline, recovery, or discovery mechanism
   exists here.
2. **The mandatory terminal archival.** Under the per-Workspace arbitration
   advisory lock taken FIRST, one composed transaction archives the Task
   with exactly the committed outcome (and the trusted closed-unmerged
   path's canonical ``pr_closed_unmerged`` terminal cause), commits the
   terminal audit event with the truthful actor provenance selected by the
   committed origin, and transitions the Task's remaining CONNECTING/READY
   Producer and Reviewer bindings to ``ENDED`` (``ended_at`` once; LOST and
   already-ENDED history untouched; immutable external identities and
   snapshots preserved). Archive, terminal audit, and session finalization
   commit or roll back together. The authorized CANCELLED/COMPLETED outcome
   is never reported as committed before this transaction commits.

Delivery goes through the F1 execution modes (#86) via the transport-facing
submission seam below. A KNOWN-REFUSED delivery is an observed delivery
failure — never the silent-death limitation — and falls back in-process to
this same authoritative finalization service in archive-only mode (no Cline
cleanup attempt), so mandatory archival never depends on the delivery
channel. An UNCERTAIN submission is handled conservatively: never blindly
resubmitted, never polled. An unwired seam falls back the same way. This
service is deliberately not enrolled in the F5b Level 2/3 operational-failure
path: cleanup failures stay contained typed dispositions, and there is no
retry, no Owner gate, and no cleanup-scheduler machinery anywhere in this
leaf.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from enum import StrEnum
from typing import TYPE_CHECKING, Protocol, cast
from uuid import UUID

from openorc.domain.connections import WorkflowRole
from openorc.domain.sessions import TaskSessionLifecycleStatus
from openorc.domain.tasks import (
    Task,
    TaskTerminalCause,
    TaskTerminalizationOrigin,
)
from openorc.observability import annotate_span, application_span
from openorc.persistence import sessions as session_records
from openorc.persistence import tasks as task_records
from openorc.persistence.ownership import get_workspace
from openorc.persistence.pool import DatabasePool
from openorc.persistence.tasks import lock_workspace_task_arbitration
from openorc.services import event_coordination
from openorc.services.errors import (
    ConflictError,
    InvalidCommandError,
    NotFoundError,
    StaleOperationError,
)
from openorc.services.task_terminal_cleanup import (
    TerminalCheckoutCleanupRuntime,
    execute_terminal_checkout_cleanup,
)
from openorc.services.transaction_composition import composed_transaction

if TYPE_CHECKING:
    from openorc.domain.sessions import TaskAgentSession

__all__ = [
    "TerminalFinalizationArrangement",
    "TerminalFinalizationKind",
    "TerminalFinalizationOutcome",
    "TerminalFinalizationSubmission",
    "TerminalFinalizationSubmissionOutcome",
    "arrange_task_terminal_finalization",
    "finalize_task_terminalization",
]

logger = logging.getLogger(__name__)

_SERVICE_TRACER_SCOPE = "openorc.services.task_terminal_finalization"
_ARRANGE_SPAN_NAME = "task_terminal_finalization.arrange_task_terminal_finalization"
_FINALIZE_SPAN_NAME = "task_terminal_finalization.finalize_task_terminalization"

_GITHUB_ACTOR: event_coordination.WorkflowActorContext = event_coordination.WorkflowActorContext(
    event_coordination.WorkflowEventActor.GITHUB, None
)
_OPENORC_ACTOR: event_coordination.WorkflowActorContext = event_coordination.WorkflowActorContext(
    event_coordination.WorkflowEventActor.OPENORC, None
)

_ENDING_ACTIVE_STATUSES: frozenset[TaskSessionLifecycleStatus] = frozenset(
    {TaskSessionLifecycleStatus.CONNECTING, TaskSessionLifecycleStatus.READY}
)


def _require_uuid_command(value: object, name: str) -> None:
    """Reject a malformed UUID command argument before any state is touched."""
    if not isinstance(value, UUID):
        raise InvalidCommandError(f"{name} must be a UUID")


class TerminalFinalizationSubmissionOutcome(StrEnum):
    """The bounded submission outcome of one finalization arrangement.

    Mirrors the F1 submission vocabulary: acceptance only means the
    finalizer MAY run. A known refusal is an observed delivery failure that
    the arranging caller owns the fallback for; an uncertain outcome is
    handled conservatively and never replayed here.
    """

    ACCEPTED_FOR_DELIVERY = "accepted_for_delivery"
    KNOWN_REFUSED = "known_refused"
    UNCERTAIN = "uncertain"


class TerminalFinalizationSubmission(Protocol):
    """The transport-facing terminal finalization submission seam.

    Implemented by the worker layer over the executing process's active F1
    :class:`~openorc.workers.invocation.OperationSubmitter` (the bounded
    local executor in regular execution mode, the canonical queue
    submission in queue mode). The seam owns no durable state and makes no
    Task-state or authorization decision.
    """

    def submit(
        self, *, workspace_id: UUID, task_id: UUID
    ) -> TerminalFinalizationSubmissionOutcome: ...


class TerminalFinalizationKind(StrEnum):
    """The bounded outcome of one terminal finalization invocation.

    ``ARCHIVED`` is the only kind that reports the authorized
    CANCELLED/COMPLETED outcome as committed — and only the returned Task,
    archived in the same transaction as its terminal audit and session
    finalization, carries it. ``ALREADY_ARCHIVED`` is the idempotent
    duplicate/race no-op. ``CLEANUP_IN_FLIGHT`` defers archival to the
    concurrent attempt that owns the unsettled one-shot cleanup claim.
    """

    ARCHIVED = "archived"
    ALREADY_ARCHIVED = "already_archived"
    CLEANUP_IN_FLIGHT = "cleanup_in_flight"


@dataclass(frozen=True, slots=True)
class TerminalFinalizationOutcome:
    """The typed outcome of one terminal finalization invocation."""

    kind: TerminalFinalizationKind
    task: Task | None
    # The committed capacity-release fact (issue #317): the bindings this
    # finalization transitioned to ENDED, each carrying its Connection
    # occupancy. #257 (F11) consumes this fact to wake the durable FIFO
    # processor; this leaf implements only the signal.
    released_session_bindings: tuple[TaskAgentSession, ...]


@dataclass(frozen=True, slots=True)
class TerminalFinalizationArrangement:
    """The bounded result of one post-commit finalization arrangement.

    ``finalization`` is present only when the in-process archive-only
    fallback ran (a known-refused delivery or an unwired seam) — in that
    case the mandatory archival has already been attempted synchronously.
    An accepted or uncertain delivery returns ``finalization=None``: the
    finalizer is pending elsewhere (or possibly stranded, the accepted
    limitation), and nothing is resubmitted here.
    """

    submission_outcome: TerminalFinalizationSubmissionOutcome
    finalization: TerminalFinalizationOutcome | None


def arrange_task_terminal_finalization(
    pool: DatabasePool,
    submission: TerminalFinalizationSubmission | None,
    *,
    workspace_id: UUID,
    task_id: UUID,
) -> TerminalFinalizationArrangement:
    """Arrange one bounded terminal finalization strictly after the commitment.

    The shared post-commit hook every terminal authority calls for a newly
    committed terminalization — never for a duplicate observation, never
    inside a transaction, and never delaying or reversing the decision, the
    issue release, or the PR-close obligation. The finalization is delivered
    through the F1 seam when one is wired; a KNOWN-REFUSED delivery (an
    observed delivery failure, never the accepted silent-worker-death
    limitation) and an unwired seam fall back in-process to the SAME
    authoritative finalization service in archive-only mode (no Cline
    cleanup attempt), so mandatory archival never depends on the delivery
    channel. An UNCERTAIN submission is conservative: nothing is resubmitted
    and nothing is polled. Submission failures are contained with safe
    classification; fallback finalization failures propagate like the inline
    archival failures they replace.
    """
    _require_uuid_command(workspace_id, "workspace_id")
    _require_uuid_command(task_id, "task_id")
    with application_span(_SERVICE_TRACER_SCOPE, _ARRANGE_SPAN_NAME) as span:
        annotate_span(
            span,
            operation=_ARRANGE_SPAN_NAME,
            workspace_id=str(workspace_id),
            task_id=str(task_id),
        )
        if submission is None:
            return TerminalFinalizationArrangement(
                submission_outcome=TerminalFinalizationSubmissionOutcome.KNOWN_REFUSED,
                finalization=finalize_task_terminalization(
                    pool, None, workspace_id=workspace_id, task_id=task_id
                ),
            )
        try:
            outcome = submission.submit(workspace_id=workspace_id, task_id=task_id)
        except Exception as error:  # noqa: BLE001 - contained with safe classification
            logger.warning(
                "terminal finalization submission failed (error type %s)",
                type(error).__name__,
            )
            return TerminalFinalizationArrangement(
                submission_outcome=TerminalFinalizationSubmissionOutcome.UNCERTAIN,
                finalization=None,
            )
        if outcome is TerminalFinalizationSubmissionOutcome.KNOWN_REFUSED:
            return TerminalFinalizationArrangement(
                submission_outcome=outcome,
                finalization=finalize_task_terminalization(
                    pool, None, workspace_id=workspace_id, task_id=task_id
                ),
            )
        return TerminalFinalizationArrangement(submission_outcome=outcome, finalization=None)


def finalize_task_terminalization(
    pool: DatabasePool,
    runtime: TerminalCheckoutCleanupRuntime | None,
    *,
    workspace_id: UUID,
    task_id: UUID,
) -> TerminalFinalizationOutcome:
    """Run the ONE idempotent, bounded terminal finalization (issue #317).

    Rechecks every authority, identity, and safety fact from durable state —
    the caller carries only the scoped Workspace/Task identifiers, so a
    replayed or delayed invocation is harmless. A ``None`` runtime is the
    archive-only mode the known-refusal and unwired-seam fallbacks use: the
    optional Cline cleanup is not attempted (and its claim is not taken), so
    mandatory archival never depends on the delivery channel or the runtime
    wiring.

    No database transaction is ever held across the Cline call; the archival
    transaction is short and takes the per-Workspace arbitration advisory
    lock first, before any occupancy-affecting row lock.
    """
    _require_uuid_command(workspace_id, "workspace_id")
    _require_uuid_command(task_id, "task_id")
    with application_span(_SERVICE_TRACER_SCOPE, _FINALIZE_SPAN_NAME) as span:
        annotate_span(
            span,
            operation=_FINALIZE_SPAN_NAME,
            workspace_id=str(workspace_id),
            task_id=str(task_id),
        )

        # --- Authority recheck from durable state. ---
        task = task_records.get_task(pool, task_id)
        if task is None or task.workspace_id != workspace_id:
            raise NotFoundError("the requested task is not available in this workspace")
        if task.terminalization_outcome is None:
            raise StaleOperationError(
                "the task carries no committed terminalization; the finalization is stale"
            )
        if task.archived_at is not None:
            # The archival already committed (a late or duplicate delivery,
            # or a concurrent finalizer that won): the idempotent no-op. The
            # committed outcome is durable history; no cleanup is ever
            # initiated after archival.
            return TerminalFinalizationOutcome(
                kind=TerminalFinalizationKind.ALREADY_ARCHIVED,
                task=task,
                released_session_bindings=(),
            )

        # --- The optional bounded cleanup under its one-shot claim. ---
        if runtime is not None:
            claimed = None
            try:
                claimed = task_records.claim_task_terminal_cleanup_send(pool, task_id)
            except Exception as error:  # noqa: BLE001 - contained; classified from a fresh read
                logger.warning(
                    "terminal cleanup send claim failed (error type %s)",
                    type(error).__name__,
                )
            if claimed is not None:
                # This invocation owns the attempt: one bounded send, then
                # the settle closes the window whatever the observed outcome.
                try:
                    try:
                        execute_terminal_checkout_cleanup(
                            pool, runtime, workspace_id=workspace_id, task_id=task_id
                        )
                    except Exception as error:  # noqa: BLE001 - optional housekeeping never blocks archival
                        logger.warning(
                            "terminal checkout cleanup attempt failed (error type %s)",
                            type(error).__name__,
                        )
                finally:
                    try:
                        task_records.settle_task_terminal_cleanup_send(pool, task_id)
                    except Exception as error:  # noqa: BLE001 - contained; the archival recheck owns the window
                        logger.warning(
                            "terminal cleanup send settle failed (error type %s)",
                            type(error).__name__,
                        )
            else:
                # The claim was not taken by this invocation. Classify the
                # current claim state from a fresh read: an unsettled claim
                # owned by a concurrent attempt defers archival — never end a
                # session, or release capacity, mid-attempt. A settled claim
                # (or an unclaimed one after a failed claim read) proceeds;
                # the archival transaction rechecks the claim state under the
                # Task row lock, which is the authoritative window guard.
                current = task_records.get_task(pool, task_id)
                if (
                    current is not None
                    and current.cleanup_send_claimed_at is not None
                    and current.cleanup_send_settled_at is None
                ):
                    return TerminalFinalizationOutcome(
                        kind=TerminalFinalizationKind.CLEANUP_IN_FLIGHT,
                        task=current,
                        released_session_bindings=(),
                    )

        return _archive_committed_terminalization(pool, workspace_id=workspace_id, task_id=task_id)


def _archive_committed_terminalization(
    pool: DatabasePool,
    *,
    workspace_id: UUID,
    task_id: UUID,
) -> TerminalFinalizationOutcome:
    """Archive the committed terminalization atomically with its session end.

    One short composed transaction: the per-Workspace arbitration advisory
    lock FIRST (the archival transitions active Connection occupancy, so it
    composes the same serialization boundary every Task-capacity arbitration
    holds, before any occupancy-affecting row lock), then the locked
    authoritative recheck of the exact commitment, the guarded archival with
    exactly the committed outcome and the trusted closed-unmerged terminal
    cause, the terminal audit event with the origin-selected truthful actor
    provenance, and the absorbing ENDED transition of the remaining active
    Producer and Reviewer bindings. Everything commits or rolls back
    together.
    """
    with composed_transaction(pool) as transaction_pool:
        lock_workspace_task_arbitration(transaction_pool, workspace_id=workspace_id)
        workspace = get_workspace(transaction_pool, workspace_id)
        if workspace is None:
            raise NotFoundError("the requested workspace is not available")
        locked = task_records.get_task_for_update(transaction_pool, task_id)
        if locked is None or locked.workspace_id != workspace_id:
            raise NotFoundError("the requested task is not available in this workspace")
        if locked.terminalization_outcome is None:
            raise StaleOperationError(
                "the task carries no committed terminalization; the finalization is stale"
            )
        if locked.archived_at is not None:
            return TerminalFinalizationOutcome(
                kind=TerminalFinalizationKind.ALREADY_ARCHIVED,
                task=locked,
                released_session_bindings=(),
            )
        if locked.cleanup_send_claimed_at is not None and locked.cleanup_send_settled_at is None:
            # The authoritative under-lock window guard: a concurrent cleanup
            # attempt owns its bounded response-or-timeout window, and this
            # transaction must not archive — ending sessions or releasing
            # capacity — mid-attempt. The owning attempt (or a later
            # delivery, after the settle) finalizes the archival.
            return TerminalFinalizationOutcome(
                kind=TerminalFinalizationKind.CLEANUP_IN_FLIGHT,
                task=locked,
                released_session_bindings=(),
            )
        archived = task_records.archive_task(
            transaction_pool,
            task_id,
            expected_state_token=locked.state_token,
            terminal_status=locked.terminalization_outcome,
            terminal_cause=(
                TaskTerminalCause.PR_CLOSED_UNMERGED
                if locked.terminalization_origin is TaskTerminalizationOrigin.GITHUB_CLOSED_UNMERGED
                else None
            ),
        )
        if archived is None:
            rearchived = task_records.get_task_for_update(transaction_pool, task_id)
            if rearchived is not None and rearchived.archived_at is not None:
                # A concurrent finalizer's archival committed first: the
                # truthful idempotent no-op, never a retried write.
                return TerminalFinalizationOutcome(
                    kind=TerminalFinalizationKind.ALREADY_ARCHIVED,
                    task=rearchived,
                    released_session_bindings=(),
                )
            raise StaleOperationError(
                "the committed terminalization could not be archived; the finalization is stale"
            )
        released: list[TaskAgentSession] = []
        for role in (WorkflowRole.PRODUCER, WorkflowRole.REVIEWER):
            ended = session_records.mark_task_agent_session_ended(
                transaction_pool, task_id=task_id, role=role
            )
            if ended is None:
                # Nothing active to end (no binding, or LOST/ENDED history)
                # — or a concurrent transition committed between the write
                # and its guard. Reload and classify from current durable
                # state; only a still-active binding is a genuine conflict.
                reloaded = session_records.get_task_agent_session(
                    transaction_pool, task_id=task_id, role=role
                )
                if reloaded is None or reloaded.lifecycle_status not in _ENDING_ACTIVE_STATUSES:
                    continue
                raise ConflictError(
                    "the task/role session binding state changed concurrently; "
                    "the terminal session end was not applied"
                )
            released.append(ended)
        event_coordination.record_task_terminal_event(
            transaction_pool,
            workspace_id=workspace_id,
            task_id=task_id,
            # The commitment coherence (the database CHECK and the domain
            # validation) guarantees a present origin whenever the outcome is
            # present; the narrow cast records that invariant for the type
            # checker without weakening the runtime guards above.
            actor=_actor_for_origin(
                workspace.owner_profile_id,
                cast(
                    TaskTerminalizationOrigin,
                    locked.terminalization_origin,
                ),
            ),
            terminal_status=locked.terminalization_outcome,
        )
    return TerminalFinalizationOutcome(
        kind=TerminalFinalizationKind.ARCHIVED,
        task=archived,
        released_session_bindings=tuple(released),
    )


def _actor_for_origin(
    owner_profile_id: UUID, origin: TaskTerminalizationOrigin
) -> event_coordination.WorkflowActorContext:
    """Select the terminal event's truthful actor provenance from the origin.

    The provenance echo never replaces the decision records' accountable
    audit; it only reconstructs the same actor context the committing
    authority's terminal event has always carried: the Workspace's Owner for
    an Owner-attributed cancellation (v1 has exactly one Owner per
    Workspace, and Cancel requires that exact Owner authority), GitHub for
    the trusted closed-unmerged observation, and OpenOrc for the
    confirmed-merge completion consequence.
    """
    if origin is TaskTerminalizationOrigin.OWNER_CANCELLED:
        return event_coordination.owner_actor(owner_profile_id)
    if origin is TaskTerminalizationOrigin.GITHUB_CLOSED_UNMERGED:
        return _GITHUB_ACTOR
    return _OPENORC_ACTOR
