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

The detached cleanup admission (issues #344/#347) converts the cleanup
attempt from blocking send to ONE promptly admitted independent run
on the original session, in both execution modes, without weakening the
terminal commitment or the one-shot safety. The execution mode is selected
EXPLICITLY by the worker layer (never inferred from registrar presence
inside this service): both modes' detached dispatch establishes cleanup
eligibility and outstanding-work safety BEFORE any claim (a positively
established
ineligible or unsafe terminalization takes the archive-only path without a
claim or receipt), takes the one-shot claim and its runtime-neutral
detached-turn receipt companion in one short transaction, gates the
admission behind a positively required observation registration, admits the
canonical instruction once through the runtime's optional detached-turn
capability (the native same-session ACT reconstruction and Hub enqueue stay
inside the Cline integration), and exits upon admission/refusal/uncertainty.
A proven pre-effect refusal (including an unwired or failed observation
path) refuses the receipt, settles the claim, and continues to the
established archive-only finalization; an admitted or uncertain outstanding
attempt stays claimed with capacity preserved for the independently observed
settlement (issue #345): the attributable completion settles the receipt
truthfully and the one-shot claim atomically with it and names the
``TERMINAL_CLEANUP`` handoff family, so the ONE post-commit completion
handoff — the observation host's, or the dispatching invocation's own when
its acknowledgement attach consumes a staged early completion — re-invokes
this idempotent finalization, whose entry classification and under-lock
window guard proceed archive-only to the mandatory atomic archival and, on
the established finite cleanup deadline, settle a past-deadline claim inline
before archiving. Never a redispatch, no scheduler, no watchdog. The cleanup
family is the terminalization
exception: it is never gated by ordinary currentness machinery and never
enrolled in Level 2/3 — the Task's terminal commitment stays sole authority.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from typing import TYPE_CHECKING, Final, Protocol, cast, runtime_checkable
from uuid import UUID

from openorc.adapters.agent_runtime import AgentTurnAdmission
from openorc.adapters.agent_runtime.errors import (
    AgentRuntimeError,
    AgentRuntimeUncertainOutcomeError,
)
from openorc.domain.connections import WorkflowRole
from openorc.domain.operational_failures import OriginalAttemptKind
from openorc.domain.pending_interactions import PendingTaskRuntimeInteractionOperation
from openorc.domain.sessions import TaskSessionLifecycleStatus
from openorc.domain.tasks import (
    Task,
    TaskTerminalCause,
    TaskTerminalizationOrigin,
)
from openorc.observability import annotate_span, application_span
from openorc.persistence import detached_turns as turn_records
from openorc.persistence import sessions as session_records
from openorc.persistence import tasks as task_records
from openorc.persistence.ownership import get_workspace
from openorc.persistence.pool import DatabasePool
from openorc.persistence.tasks import lock_workspace_task_arbitration
from openorc.protocol.models import WORKSPACE_RESULT_FAMILY
from openorc.services import event_coordination
from openorc.services.errors import (
    ApplicationError,
    ConflictError,
    InvalidCommandError,
    NotFoundError,
    StaleOperationError,
)
from openorc.services.task_completion_handoff import DetachedTurnCompletionHandoff
from openorc.services.task_detached_turn_correlation import (
    TurnSettlementKind,
    attach_acknowledged_turn,
    record_task_detached_turn_receipt,
)
from openorc.services.task_queue_wake import (
    TaskQueueWakeSubmission,
    arrange_task_queue_wake,
)
from openorc.services.task_terminal_cleanup import (
    TerminalCheckoutCleanupEstablishment,
    TerminalCheckoutCleanupRuntime,
    TerminalCleanupEstablishmentKind,
    establish_terminal_checkout_cleanup,
)
from openorc.services.transaction_composition import composed_transaction

if TYPE_CHECKING:
    from openorc.domain.detached_turns import TaskDetachedTurnReceipt
    from openorc.domain.sessions import TaskAgentSession

__all__ = [
    "TerminalCleanupDispatch",
    "TerminalCleanupDispatchSelection",
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


class TerminalCleanupDispatchSelection(StrEnum):
    """The explicitly selected cleanup execution mode (issues #344/#347).

    Resolved by the worker layer — never inferred from registrar presence
    inside this service.

    ``DETACHED_REGULAR`` dispatches the cleanup as ONE independently observed
    detached turn. ``OBSERVATION_UNAVAILABLE`` is a process whose detached-turn
    observation could not resolve: optional housekeeping is skipped positively
    (no claim, no receipt, no blocking send) and the mandatory archival
    proceeds — unavailable observation never selects blocking execution and
    never strands a terminalization.
    """

    DETACHED_REGULAR = "detached_regular"
    OBSERVATION_UNAVAILABLE = "observation_unavailable"


@dataclass(frozen=True, slots=True)
class TerminalCleanupDispatch:
    """The worker-resolved explicit cleanup dispatch selection (issue #344).

    One typed value the finalizer dispatches on. The regular-mode detached
    selection carries the process's observation registrar and — issue #345 —
    the process's ONE post-commit completion handoff, which the dispatching
    invocation routes when the acknowledgement attach consumes a staged early
    completion; every other selection carries neither, and an inconsistent
    construction is rejected so a mode can never be inferred from
    registrar or handoff presence.
    """

    selection: TerminalCleanupDispatchSelection
    observation_registrar: TerminalCleanupObservationRegistrar | None = None
    completion_handoff: DetachedTurnCompletionHandoff | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.selection, TerminalCleanupDispatchSelection):
            raise ValueError("TerminalCleanupDispatch.selection must be a selection member")
        if self.selection is TerminalCleanupDispatchSelection.DETACHED_REGULAR:
            if self.observation_registrar is None:
                raise ValueError(
                    "a detached-regular cleanup dispatch carries its observation registrar"
                )
        elif self.observation_registrar is not None:
            raise ValueError(
                "only a detached-regular cleanup dispatch carries an observation registrar"
            )
        if (
            self.selection is not TerminalCleanupDispatchSelection.DETACHED_REGULAR
            and self.completion_handoff is not None
        ):
            raise ValueError(
                "only a detached-regular cleanup dispatch carries a completion handoff"
            )


@runtime_checkable
class TerminalCleanupObservationRegistrar(Protocol):
    """The structural registration seam of the process's detached-turn observer.

    The same positively-required pre-admission gate every detached dispatch
    consumes (issues #323/#333–#344): the detached terminal cleanup registers
    the exact bound original Producer session before any admission, so an
    unreplayable cleanup turn can never be produced without active
    observation. Failures are the observer's typed refusals, contained at
    the finalizer's own archive-only boundary (never Level 2/3 machinery).
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
    ``CLEANUP_OUTSTANDING`` (issue #344) is the owning attempt's own
    outcome when an admitted or uncertain detached cleanup run is
    outstanding on its unsettled claim: the dispatching invocation exits
    without settling, and the independently observed settlement routes the
    ONE post-commit completion handoff back to this finalization (issue
    #345). ``CLEANUP_EARLY_SETTLED`` (issue #345) is the dispatching
    invocation's own outcome when the acknowledgement attach consumed a
    staged early completion: the receipt and the one-shot claim settled
    atomically inside the attach and the handoff was routed best-effort —
    the returned fresh Task snapshot shows whether that handoff's own
    finalization already archived (``archived_at`` set) or the mandatory
    archival stays with a later explicitly invoked finalizer or the finite
    deadline disposition.
    """

    ARCHIVED = "archived"
    ALREADY_ARCHIVED = "already_archived"
    CLEANUP_IN_FLIGHT = "cleanup_in_flight"
    CLEANUP_OUTSTANDING = "cleanup_outstanding"
    CLEANUP_EARLY_SETTLED = "cleanup_early_settled"


# The established finite terminal-cleanup deadline (issue #345): the bounded
# magnitude of the #317 blocking realization's own request window (the Cline
# bridge process's bounded request timeout) applied to the detached
# admission's outstanding one-shot claim. It is consumed ONLY by an actually
# invoked finalizer — never by a scheduler, watchdog, or any automatic
# worker-death detector (the accepted v1 limitation on issue #317).
_TERMINAL_CLEANUP_DEADLINE: Final[timedelta] = timedelta(seconds=900)


def _cleanup_claim_deadline_open(claimed_at: datetime) -> bool:
    """Whether the claimed cleanup attempt is still inside its finite deadline.

    The claim timestamp is the DB-clock instant the one-shot claim committed;
    the comparison uses this process's UTC clock, bounded well beyond any
    realistic application/database skew by the deadline's magnitude.
    """
    return datetime.now(UTC) - claimed_at < _TERMINAL_CLEANUP_DEADLINE


def _claimed_cleanup_disposition(
    pool: DatabasePool,
    task: Task,
    *,
    workspace_id: UUID,
    task_id: UUID,
) -> TerminalFinalizationOutcome | None:
    """The committed one-shot claim's owning disposition (issue #345).

    ``None`` when the Task is genuinely unclaimed — the normal state of a
    fresh terminalization whose first eligible finalizer invocation still
    owns the full cleanup establishment and one-shot dispatch path. A settled
    prior attempt archives archive-only: the one-shot window is closed, so
    the establishment never re-runs and no second dispatch ever follows. An
    unsettled attempt inside its finite deadline defers the archival exactly
    as the authoritative under-lock window guard does — a concurrent attempt
    owns its window, and ending sessions or releasing capacity mid-attempt
    is forbidden. An unsettled attempt past the deadline stops waiting
    conservatively: the archival's under-lock guard settles the claim and
    archives, the stale checkout is accepted, the receipt is left as-is
    (a timeout proves nothing about the native turn), and no second cleanup
    dispatch ever follows.
    """
    if task.cleanup_send_claimed_at is None:
        return None
    if task.cleanup_send_settled_at is not None or not _cleanup_claim_deadline_open(
        task.cleanup_send_claimed_at
    ):
        return _archive_committed_terminalization(pool, workspace_id=workspace_id, task_id=task_id)
    return TerminalFinalizationOutcome(
        kind=TerminalFinalizationKind.CLEANUP_IN_FLIGHT,
        task=task,
        released_session_bindings=(),
    )


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
    queue_wake: TaskQueueWakeSubmission | None = None,
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

    Strictly after an in-process fallback finalization whose archival
    actually released occupancy (non-empty ``released_session_bindings``),
    this hook arranges one Task-queue wake invitation through the optional
    ``queue_wake`` seam (issue #257) — a no-op or duplicate finalization
    released nothing and invents no wake. The F10 handler and the detached
    cleanup settlement handoff — the finalizer's production completion and
    settlement callers — wire the executing process's bridge themselves; an
    authority caller that leaves this seam unwired leaves the fallback
    release's wake to the bounded restart/recovery triggers, exactly like
    every other wake the delivery channel can lose.
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
            finalization = finalize_task_terminalization(
                pool, None, workspace_id=workspace_id, task_id=task_id
            )
            _arrange_post_finalization_queue_wake(
                queue_wake, finalization, workspace_id=workspace_id, origin="terminal_arrangement"
            )
            return TerminalFinalizationArrangement(
                submission_outcome=TerminalFinalizationSubmissionOutcome.KNOWN_REFUSED,
                finalization=finalization,
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
            finalization = finalize_task_terminalization(
                pool, None, workspace_id=workspace_id, task_id=task_id
            )
            _arrange_post_finalization_queue_wake(
                queue_wake, finalization, workspace_id=workspace_id, origin="terminal_arrangement"
            )
            return TerminalFinalizationArrangement(
                submission_outcome=outcome,
                finalization=finalization,
            )
        return TerminalFinalizationArrangement(submission_outcome=outcome, finalization=None)


def _arrange_post_finalization_queue_wake(
    queue_wake: TaskQueueWakeSubmission | None,
    finalization: TerminalFinalizationOutcome,
    *,
    workspace_id: UUID,
    origin: str,
) -> None:
    """Arrange the F11 queue wake for one in-process finalization's release."""
    if not finalization.released_session_bindings:
        return
    arrange_task_queue_wake(queue_wake, workspace_id=workspace_id, origin=origin)


def finalize_task_terminalization(
    pool: DatabasePool,
    runtime: TerminalCheckoutCleanupRuntime | None,
    *,
    workspace_id: UUID,
    task_id: UUID,
    dispatch: TerminalCleanupDispatch | None = None,
) -> TerminalFinalizationOutcome:
    """Run the ONE idempotent, bounded terminal finalization (issue #317).

    Rechecks every authority, identity, and safety fact from durable state —
    the caller carries only the scoped Workspace/Task identifiers, so a
    replayed or delayed invocation is harmless. A ``None`` runtime is the
    archive-only mode the known-refusal and unwired-seam fallbacks use: the
    optional Cline cleanup is not attempted (and its claim is not taken), so
    mandatory archival never depends on the delivery channel or the runtime
    wiring.

    The optional cleanup's execution mode is the caller's EXPLICIT
    ``dispatch`` selection (issues #344/#347) — an absent dispatch with a
    present runtime is the observation-unavailable selection, so an unwired
    dispatch can never silently select blocking execution. The detached
    selection establishes eligibility and
    outstanding-work safety BEFORE any claim, commits the one-shot claim and
    its detached-turn receipt companion in one short transaction, gates the
    admission behind a positively required observation registration, and
    admits the canonical cleanup instruction once through the runtime's
    optional detached capability — the dispatching worker exits on
    admission/refusal/uncertainty, and an admitted or uncertain outstanding
    attempt stays claimed (``CLEANUP_OUTSTANDING``) for the independent
    settlement/deadline disposition (#345). ``OBSERVATION_UNAVAILABLE``
    positively skips the optional cleanup (no claim, no receipt, no send)
    and proceeds to the mandatory archival.

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

        # --- The committed one-shot claim's disposition (issue #345). A
        # genuinely unclaimed commitment falls through to the dispatch
        # selection below: the first eligible finalizer invocation keeps the
        # full cleanup establishment and one-shot dispatch path. ---
        claimed = _claimed_cleanup_disposition(
            pool, task, workspace_id=workspace_id, task_id=task_id
        )
        if claimed is not None:
            return claimed

        # --- The optional bounded cleanup under its one-shot claim. ---
        if runtime is not None:
            selection = (
                dispatch.selection
                if dispatch is not None
                else TerminalCleanupDispatchSelection.OBSERVATION_UNAVAILABLE
            )
            if selection is TerminalCleanupDispatchSelection.OBSERVATION_UNAVAILABLE:
                # The regular-mode detached-turn observation could not
                # resolve (issue #344): positively skip the optional
                # housekeeping — no claim, no receipt, and never a blocking
                # send — so the mandatory archival always proceeds.
                logger.warning(
                    "terminal cleanup skipped: the detached-turn observation is "
                    "unavailable in this process; proceeding archive-only"
                )
            elif selection is TerminalCleanupDispatchSelection.DETACHED_REGULAR:
                registrar = dispatch.observation_registrar if dispatch is not None else None
                if registrar is None:  # pragma: no cover - validated by construction
                    raise ConflictError(
                        "the detached terminal cleanup dispatch carries no observation registrar"
                    )
                outcome = _attempt_detached_terminal_cleanup(
                    pool,
                    runtime,
                    workspace_id=workspace_id,
                    task_id=task_id,
                    observation_registrar=registrar,
                    completion_handoff=(
                        dispatch.completion_handoff if dispatch is not None else None
                    ),
                )
                if outcome is not None:
                    # The detached attempt owns an outstanding (or settled)
                    # run: the dispatching worker exits without settling or
                    # archiving. A ``None`` result continues to the
                    # established archive-only finalization below.
                    return outcome
            else:  # pragma: no cover - the closed selection enum
                # Defensive fail-closed classification: the closed selection
                # enum carries no blocking member (issue #347 retired the
                # blocking queue-mode interim) — an unknown selection is
                # never silently executed.
                raise ConflictError(
                    f"unsupported terminal cleanup dispatch selection {selection.value!r}"
                )

        return _archive_committed_terminalization(pool, workspace_id=workspace_id, task_id=task_id)


_CLEANUP_REGISTRATION_REFUSAL_KIND: Final[str] = "observation_registration_refused"
_CLEANUP_ADMISSION_REFUSAL_KIND: Final[str] = "admission_refused"
_CLEANUP_ATTACHMENT_REFUSAL_KIND: Final[str] = "runtime_attachment_failed"


def _refuse_cleanup_receipt_and_settle_claim(
    pool: DatabasePool,
    receipt: TaskDetachedTurnReceipt,
    *,
    refusal_kind: str,
) -> None:
    """Refuse a claimed cleanup receipt and settle its one-shot claim.

    The proven pre-effect accounting (issues #343/#344): a positively
    identified pre-admission refusal means nothing was ever enqueued, so the
    receipt records ``refused`` (the session's single-outstanding slot is
    freed) and the claim settles, closing the one-shot window. The caller
    continues to the established archive-only finalization — cleanup is
    deliberately outside Level 2/3, so no escalation, no re-send, and no
    disposition follows. Each step is best-effort containment: a persistence
    failure never masks the caller's outcome, and the archival's own
    authoritative window guard rechecks the claim state either way.
    """
    try:
        refused = turn_records.refuse_task_detached_turn_receipt(
            pool,
            receipt_id=receipt.id,
            task_id=receipt.task_id,
            workspace_id=receipt.workspace_id,
            refusal_kind=refusal_kind,
        )
    except Exception as error:  # noqa: BLE001 - containment, never masking
        logger.warning(
            "the refused terminal-cleanup receipt could not be durably marked (%s)",
            type(error).__name__,
        )
        refused = None
    if refused is None:  # pragma: no cover - a claimed receipt refuses once
        logger.warning(
            "the terminal-cleanup receipt was no longer claimed when its pre-effect "
            "refusal was recorded"
        )
    try:
        task_records.settle_task_terminal_cleanup_send(pool, receipt.task_id)
    except Exception as error:  # noqa: BLE001 - contained; the archival recheck owns the window
        logger.warning("terminal cleanup send settle failed (%s)", type(error).__name__)


def _claim_terminal_cleanup_with_receipt(
    pool: DatabasePool,
    *,
    workspace_id: UUID,
    task_id: UUID,
    establishment: TerminalCheckoutCleanupEstablishment,
) -> TaskDetachedTurnReceipt | None:
    """Take the one-shot cleanup claim and commit its receipt companion atomically.

    One short composed transaction under the Task row lock (issue #344): the
    exact subject is re-proven — a committed, non-archived terminalization,
    the claim still NULL, and the Producer binding still READY with the same
    identity the establishment validated — before the existing claim CAS and
    the runtime-neutral detached-turn receipt companion commit TOGETHER,
    strictly before any runtime effect. The receipt roots at the exact Task
    UUID (the Task-scoped one-shot cleanup fact, never an Execution, a
    bootstrap claim, or a control intent) and pins the canonical
    ``workspace_result`` family. A single-outstanding conflict (an unsettled
    detached receipt appeared on the binding between the establishment read
    and this transaction) rolls the whole composition back — no claim, no
    receipt — and classifies as a positively established skip, never a
    stranded claim and never a retry. Every other under-lock divergence
    (archival won the race, the claim moved on, the binding changed) also
    classifies as a skip: the archival continuation's own window guard owns
    the truthful outcome.
    """
    if establishment.binding is None or establishment.external_session_id is None:
        # pragma: no cover - an eligible establishment carries its subjects
        raise ApplicationError("the cleanup establishment carries no eligible subjects")
    binding_id = establishment.binding.id
    external_session_id = establishment.external_session_id
    try:
        with composed_transaction(pool) as transaction_pool:
            locked = task_records.get_task_for_update(transaction_pool, task_id)
            if locked is None or locked.workspace_id != workspace_id:
                raise NotFoundError("the requested task is not available in this workspace")
            if (
                locked.terminalization_outcome is None
                or locked.archived_at is not None
                or locked.cleanup_send_claimed_at is not None
            ):
                # The subject moved on between the establishment and this
                # claim (a concurrent archival or a claim that won): the
                # skip classification lets the archival continuation report
                # the truthful idempotent/deferred outcome.
                return None
            binding = session_records.get_task_agent_session(
                transaction_pool, task_id=task_id, role=WorkflowRole.PRODUCER
            )
            if (
                binding is None
                or binding.id != binding_id
                or binding.external_session_id != external_session_id
                or binding.lifecycle_status is not TaskSessionLifecycleStatus.READY
            ):
                return None
            if task_records.claim_task_terminal_cleanup_send(transaction_pool, task_id) is None:
                return None  # pragma: no cover - the locked recheck owns the window
            return record_task_detached_turn_receipt(
                transaction_pool,
                workspace_id=workspace_id,
                task_id=task_id,
                task_agent_session_id=binding.id,
                operation=PendingTaskRuntimeInteractionOperation.TERMINAL_CLEANUP,
                original_attempt_kind=OriginalAttemptKind.TERMINAL_CLEANUP_CLAIM,
                original_attempt_id=task_id,
                expected_formal_family=WORKSPACE_RESULT_FAMILY,
            )
    except ConflictError as error:
        # The receipt companion's single-outstanding backstop (or another
        # typed refusal from the receipt seam): the whole composition rolled
        # back — no claim, no receipt — and the positively established
        # conflict classifies as an unsafe skip, never a retry.
        logger.warning("terminal cleanup claim refused (%s)", error)
        return None


def _attempt_detached_terminal_cleanup(
    pool: DatabasePool,
    runtime: TerminalCheckoutCleanupRuntime,
    *,
    workspace_id: UUID,
    task_id: UUID,
    observation_registrar: TerminalCleanupObservationRegistrar,
    completion_handoff: DetachedTurnCompletionHandoff | None,
) -> TerminalFinalizationOutcome | None:
    """Dispatch the optional cleanup as ONE independently observed detached run.

    The regular-mode detached cleanup dispatch (issue #344), mirroring the
    established detached-dispatch mechanics: the shared read-only
    establishment derives eligibility and outstanding-work safety BEFORE any
    claim (a positively established ineligible or unsafe terminalization
    skips without a claim or receipt), the runtime's optional detached
    capability is duck-typed fail-closed, the positively required
    observation registration gates the admission, the one-shot claim and its
    receipt companion commit in one short transaction strictly before any
    effect, and ONE detached-turn admission carries the canonical rendered
    CLEANUP_WORKSPACE instruction verbatim — the only content the
    independent run ever receives. No ``send`` and no blocking wait exists
    in this path, and no database transaction is ever held across the
    runtime attach, the same-session ACT reconstruction, or the Hub
    admission (those mechanics stay inside the Cline integration).

    Returns ``None`` when the attempt positively did not dispatch (a skip, a
    proven pre-effect refusal, or a lost claim race): the caller continues
    to the established archive-only finalization, whose authoritative window
    guard owns the archival decision. Returns ``CLEANUP_OUTSTANDING`` when
    the run was admitted (or the admission ended uncertain): the receipt
    records the acknowledged truth (or stays conservatively claimed), the
    claim stays unsettled for the independently observed settlement (issue
    #345), capacity stays held, and the dispatching worker exits. Returns
    ``CLEANUP_EARLY_SETTLED`` when the acknowledgement attach consumed a
    staged early completion: the receipt and the claim settled atomically
    inside the attach and this invocation routed the ONE post-commit
    completion handoff best-effort (issue #345). A successful enqueue was
    never cleanup success, and the Task's terminal commitment stays sole
    workflow authority.
    """
    establishment = establish_terminal_checkout_cleanup(
        pool, workspace_id=workspace_id, task_id=task_id
    )
    if establishment.kind is not TerminalCleanupEstablishmentKind.ELIGIBLE:
        logger.info(
            "detached terminal cleanup skipped (%s); proceeding archive-only",
            establishment.kind.value,
        )
        return None
    # The optional detached capability is duck-typed fail-closed BEFORE any
    # claim: a runtime without it cannot serve the detached dispatch, and
    # nothing is claimed that could never be dispatched.
    admit = getattr(runtime, "admit_cleanup_turn", None)
    if not callable(admit):
        logger.info(
            "detached terminal cleanup skipped: the cleanup runtime carries no "
            "detached admission capability; proceeding archive-only"
        )
        return None
    register = getattr(observation_registrar, "register_session", None)
    if not callable(register):
        # A misconfigured registrar can never gate an admission: positively
        # skip before any claim.
        logger.warning(
            "detached terminal cleanup skipped: the observation registrar does "
            "not implement the established-session registration seam"
        )
        return None
    if (
        establishment.task is None
        or establishment.binding is None
        or establishment.interaction is None
        or establishment.external_session_id is None
        or establishment.original_base is None
    ):  # pragma: no cover - the establishment's coherence validation
        raise ApplicationError("the cleanup establishment carries no eligible subjects")
    binding = establishment.binding
    external_session_id = establishment.external_session_id
    original_base = establishment.original_base
    interaction = establishment.interaction

    # --- The one-shot claim and its receipt companion (one short transaction). ---
    receipt = _claim_terminal_cleanup_with_receipt(
        pool, workspace_id=workspace_id, task_id=task_id, establishment=establishment
    )
    if receipt is None:
        return None

    # --- The positively-required observation registration gate. A refusal
    # or failure is a proven pre-effect failure: the receipt is refused, the
    # claim settles, and the archive-only finalization follows — a
    # never-dispatched turn holds neither the session's single-outstanding
    # slot nor the claim's window. ---
    try:
        register(
            workspace_id=workspace_id,
            task_id=task_id,
            task_agent_session_id=binding.id,
            connection_id=binding.connection_id,
            external_session_id=external_session_id,
        )
    except Exception as error:  # noqa: BLE001 - contained at the archive-only boundary
        logger.warning(
            "the terminal cleanup observation registration failed before any "
            "admission (%s); the receipt is refused and the claim settles",
            type(error).__name__,
        )
        _refuse_cleanup_receipt_and_settle_claim(
            pool, receipt, refusal_kind=_CLEANUP_REGISTRATION_REFUSAL_KIND
        )
        return None

    # --- The admission (external, no transaction held). One detached-turn
    # admission of the canonical rendered instruction verbatim — the only
    # content the independent run ever receives. ---
    try:
        admission = admit(
            pool,
            workspace_id=workspace_id,
            task_id=task_id,
            external_session_id=external_session_id,
            working_directory=original_base,
            interaction=interaction,
        )
    except AgentRuntimeUncertainOutcomeError:
        logger.warning(
            "the detached terminal cleanup admission ended with an uncertain "
            "outcome; the receipt stays conservatively claimed, the claim stays "
            "unsettled, and the instruction is never re-dispatched"
        )
        return _cleanup_outstanding_outcome(pool, workspace_id=workspace_id, task_id=task_id)
    except AgentRuntimeError:
        # A positively identified refusal (unreachable before effect, runtime
        # rejection, exact-session absence, or the adapter's D6a/D6b
        # quiescence discipline): proven pre-effect, so the receipt is
        # refused, the claim settles, and the archive-only finalization
        # follows.
        logger.warning(
            "the detached terminal cleanup admission was refused before any effect; "
            "the receipt is refused and the claim settles"
        )
        _refuse_cleanup_receipt_and_settle_claim(
            pool, receipt, refusal_kind=_CLEANUP_ADMISSION_REFUSAL_KIND
        )
        return None
    except ApplicationError as error:
        # The production composition's attachment seam (the #190 lease and
        # the same-ID reconstruction it guards) fails with typed application
        # errors strictly BEFORE the adapter's enqueue is ever reached — the
        # seam's own classification attests "no workflow effect was
        # dispatched". A proven pre-effect failure refuses the receipt,
        # settles the claim, and continues the established archive-only
        # finalization (issue #344 review); the terminalization is never
        # stranded by an attachment failure.
        logger.warning(
            "the detached terminal cleanup attachment failed before any effect "
            "(%s); the receipt is refused and the claim settles",
            type(error).__name__,
        )
        _refuse_cleanup_receipt_and_settle_claim(
            pool, receipt, refusal_kind=_CLEANUP_ATTACHMENT_REFUSAL_KIND
        )
        return None
    except Exception as error:  # noqa: BLE001 - unclassified: the effect cannot be ruled out
        # An unclassified failure inside the composition: whether the turn
        # was enqueued cannot be ruled out, so the attempt stays
        # conservatively outstanding — never a proven non-delivery, never a
        # re-send, and the independently observed settlement (issue #345)
        # owns the later disposition.
        logger.warning(
            "the detached terminal cleanup admission failed with an unclassified "
            "error (%s); the receipt stays conservatively claimed and the claim "
            "stays unsettled",
            type(error).__name__,
        )
        return _cleanup_outstanding_outcome(pool, workspace_id=workspace_id, task_id=task_id)
    if not isinstance(admission, AgentTurnAdmission):
        # Defensive composition invariant: the detached capability returns
        # the acknowledged admission. Never guessed around.
        _refuse_cleanup_receipt_and_settle_claim(
            pool, receipt, refusal_kind=_CLEANUP_ATTACHMENT_REFUSAL_KIND
        )
        raise ConflictError("the cleanup admission returned no acknowledged turn reference")

    # --- The acknowledgement attach (one short transaction). A staged
    # early completion is consumed exactly once here and routed through the
    # terminal-cleanup settlement routing, so an early result that finished
    # before the acknowledgement was persisted is correlated, never ignored. ---
    try:
        attach_outcome = attach_acknowledged_turn(
            pool,
            workspace_id=workspace_id,
            task_id=task_id,
            receipt_id=receipt.id,
            admitted_turn_reference=admission.turn_reference,
        )
    except ApplicationError:
        raise
    except Exception as error:
        raise ConflictError(
            "the terminal cleanup acknowledgement could not be durably attached "
            "after its known admission; authoritative recovery is required and "
            "the instruction is never re-dispatched"
        ) from error
    if (
        attach_outcome.kind
        in (
            TurnSettlementKind.SETTLED_VALIDATED_RESULT,
            TurnSettlementKind.SETTLED_OBSERVED_FAILURE,
        )
        and attach_outcome.handoff_operation is not None
    ):
        # A staged early completion settled the one-shot attempt inside the
        # attach composition (issue #345): the receipt and the claim settled
        # atomically there, so no later observer event is guaranteed. This
        # dispatching invocation routes the SAME ONE post-commit completion
        # handoff the independent observer uses — best-effort and idempotent.
        # A failed handoff leaves the durably settled state to a later
        # explicitly invoked finalizer or the finite deadline disposition:
        # never a re-dispatch, never a second archival service.
        if completion_handoff is not None:
            try:
                completion_handoff.handoff_settled_completion(
                    workspace_id=workspace_id,
                    task_id=task_id,
                    operation=attach_outcome.handoff_operation,
                    validated_result=None,
                )
            except Exception as error:  # noqa: BLE001 - best-effort handoff
                logger.warning(
                    "the terminal cleanup early-completion handoff failed (%s); the "
                    "durably settled attempt owns recovery through a later finalizer",
                    type(error).__name__,
                )
        else:
            logger.warning(
                "the terminal cleanup dispatch carries no completion handoff; the "
                "early-settled attempt's archival waits for a later finalizer invocation"
            )
        return _cleanup_early_settled_outcome(pool, workspace_id=workspace_id, task_id=task_id)
    return _cleanup_outstanding_outcome(pool, workspace_id=workspace_id, task_id=task_id)


def _cleanup_outstanding_outcome(
    pool: DatabasePool, *, workspace_id: UUID, task_id: UUID
) -> TerminalFinalizationOutcome:
    """The owning attempt's ``CLEANUP_OUTSTANDING`` outcome (issue #344).

    The one-shot claim deliberately stays unsettled: the admitted (or
    conservatively uncertain) run is outstanding, capacity stays held, and
    the archival stays deferred to the independently observed settlement —
    which routes the ONE post-commit completion handoff back to this
    finalization (issue #345) — or to the finite deadline disposition. A
    competing finalizer's window guard defers exactly the same way.
    """
    task = task_records.get_task(pool, task_id)
    if task is None or task.workspace_id != workspace_id:  # pragma: no cover
        raise NotFoundError("the requested task is not available in this workspace")
    return TerminalFinalizationOutcome(
        kind=TerminalFinalizationKind.CLEANUP_OUTSTANDING,
        task=task,
        released_session_bindings=(),
    )


def _cleanup_early_settled_outcome(
    pool: DatabasePool, *, workspace_id: UUID, task_id: UUID
) -> TerminalFinalizationOutcome:
    """The attach-settled attempt's ``CLEANUP_EARLY_SETTLED`` outcome (#345).

    A staged early completion settled the receipt and the one-shot claim
    atomically inside the acknowledgement attach and the ONE post-commit
    completion handoff was routed best-effort: the returned fresh Task
    snapshot shows whether that handoff's own finalization already archived
    (``archived_at`` set) or the mandatory archival stays with a later
    explicitly invoked finalizer or the finite deadline disposition — never
    a re-dispatch, never a second archival service.
    """
    task = task_records.get_task(pool, task_id)
    if task is None or task.workspace_id != workspace_id:  # pragma: no cover
        raise NotFoundError("the requested task is not available in this workspace")
    return TerminalFinalizationOutcome(
        kind=TerminalFinalizationKind.CLEANUP_EARLY_SETTLED,
        task=task,
        released_session_bindings=(),
    )


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
            if _cleanup_claim_deadline_open(locked.cleanup_send_claimed_at):
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
            # The established finite deadline elapsed while the attempt stayed
            # unobserved (issue #345): conservatively stop waiting — settle
            # the one-shot claim inline in this same transaction and archive.
            # The receipt is deliberately left as-is (a timeout proves
            # nothing about the native turn), the stale checkout is accepted,
            # and no second cleanup dispatch ever follows. Under the row lock
            # held since the reproof the settle CAS cannot fail; a
            # hypothetical miss defers.
            if task_records.settle_task_terminal_cleanup_send(transaction_pool, task_id) is None:
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
