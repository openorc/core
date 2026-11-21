"""F10 terminal Producer checkout cleanup (issue #256, reworked by issue #317).

Before a Task's terminal archival commits, the terminal finalizer arranges
ONE bounded attempt to remove its original dedicated Cline Producer checkout
by reconstructing the SAME original bound Producer session in ACT at the
original repository base (outside the checkout) and realizing the canonical
``CLEANUP_WORKSPACE`` control for the single exact checkout. The Producer
deletes the checkout with its own tools; Core never touches the filesystem.

This module deliberately carries no durable cleanup obligation/claim/outcome
record of its own: the one-shot send claim, the bounded response-or-timeout
window, and the pre-archival terminalization commitment live on the Task row
(issue #317) and are owned by the shared terminal finalization service
(:mod:`openorc.services.task_terminal_finalization`), which is this module's
only caller. A skipped, failed, uncertain, or timed-out cleanup accepts the
stale directory and never blocks the mandatory archival. No retry, later
wakeup, or recovery follows a contained failure; the system-wide silent
worker death limitation is recorded on issue #317, never solved here.

Boundary rules enforced here:

- **Committed terminalization first.** Execution runs only for a Task whose
  exact-Task terminalization commitment is durable and whose archival has
  not committed — the cleanup is the Producer session's last bounded
  interaction BEFORE archival. A late or duplicate observation (including
  one arriving after archival) is the accepted no-op: it never initiates a
  fresh attempt.
- **Exact destructive target.** The checkout passed to cleanup is derived
  ONLY from the original bound Producer ``TaskAgentSession``
  ``effective_config_snapshot`` and the persisted Task identity
  (``<original admitted repository_base>/<Task UUID>``), through the same
  fail-closed snapshot recovery the planning bootstrap established. Live
  Connection edits, supplied directories, model-reported paths, and
  substituted roles/sessions can never redirect it.
- **Do not delete a potentially active checkout.**
  :func:`execute_terminal_checkout_cleanup` rechecks current durable
  outstanding-work facts (unsettled Interrupt intents, open resume
  records, open Executions, pending RuntimeRequests, conservatively
  claimed runtime-send claims, and — issue #344 — any unsettled
  detached-turn receipt on the binding) as a narrow pre-send guard. Lack of
  positive safety evidence is a skip — never a poll, retry, or recovery
  workflow. Owner Cancel's preterminal runtime-work veto is NOT imposed on
  GitHub-confirmed terminalization: an external terminalization while
  Producer work remains outstanding simply skips cleanup.
- **No replacement sessions.** Only the original READY binding's external
  session identity is reconstructed; a lost/ended/missing binding is a
  skip, never a new session or a resumed implementation.
- **Outcome is best effort.** The realized ``workspace_result`` is
  validated against ``operation=CLEANUP`` and ``COMPLETED|FAILED``;
  model-reported success is never independent filesystem proof, runtime
  and control failures are contained with type-only sanitized logging, and
  an absent/uncertain result never triggers another send.

The runtime mechanics stay behind :class:`TerminalCheckoutCleanupRuntime`
(runtime-neutral seam); the production Cline composition lives in
``services/cline_terminal_cleanup``. No persistence migration, no queue
semantics, no API routes or RQ jobs here.

Issue #344 adds the detached admission beside the blocking realization:
the shared :func:`establish_terminal_checkout_cleanup` derivation is the
read-only eligibility establishment both paths consume, and concrete
runtimes may OPTIONALLY expose ``admit_cleanup_turn`` — the same exact
subjects and interaction, admitted as one independent detached turn
through the runtime's own nonblocking admission instead of realized as a
blocking send. The capability is duck-typed fail-closed (never a universal
or expanded seam member); a runtime without it keeps the blocking
realization.
"""

from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import StrEnum
from typing import TYPE_CHECKING, Final
from uuid import UUID

from openorc.domain.connections import WorkflowRole
from openorc.domain.executions import ExecutionStatus
from openorc.domain.intents import TaskInterruptIntentStatus
from openorc.domain.runtime_requests import RuntimeRequestStatus
from openorc.domain.send_claims import TaskRuntimeSendClaimStatus
from openorc.domain.sessions import TaskSessionLifecycleStatus
from openorc.persistence import detached_turns as detached_turn_records
from openorc.persistence import executions as execution_records
from openorc.persistence import intents as intent_records
from openorc.persistence import runtime_requests as runtime_request_records
from openorc.persistence import send_claims as send_claim_records
from openorc.persistence import sessions as session_records
from openorc.persistence import tasks as task_records
from openorc.persistence.ownership import get_repository
from openorc.persistence.pool import DatabasePool
from openorc.protocol.interaction import CleanupWorkspaceInteraction
from openorc.protocol.models import WorkspaceResult
from openorc.services.errors import ApplicationError
from openorc.services.task_planning_bootstrap import _producer_snapshot_repository_base

if TYPE_CHECKING:
    from openorc.domain.sessions import TaskAgentSession
    from openorc.domain.tasks import Task

__all__ = [
    "TerminalCheckoutCleanupDisposition",
    "TerminalCheckoutCleanupEstablishment",
    "TerminalCheckoutCleanupExecution",
    "TerminalCleanupEstablishmentKind",
    "TerminalCheckoutCleanupRuntime",
    "establish_terminal_checkout_cleanup",
]

logger = logging.getLogger(__name__)

_SERVICE_TRACER_SCOPE = "openorc.services.task_terminal_cleanup"
_EXECUTE_SPAN_NAME = "task_terminal_cleanup.execute_terminal_checkout_cleanup"

# The Execution statuses whose runtime work is still outstanding (the same
# conservative open-work vocabulary the shared cancellation veto reads).
_OPEN_EXECUTION_STATUSES: Final[frozenset[ExecutionStatus]] = frozenset(
    {
        ExecutionStatus.QUEUED,
        ExecutionStatus.RUNNING,
        ExecutionStatus.PAUSED,
        ExecutionStatus.PAUSED_FOR_APPROVAL,
    }
)

_CLEANUP_OPERATION: Final = "CLEANUP"
_ACCEPTED_CLEANUP_STATUSES: Final[frozenset[str]] = frozenset({"COMPLETED", "FAILED"})


class TerminalCheckoutCleanupDisposition(StrEnum):
    """The bounded in-memory disposition of one cleanup execution.

    Observational only — never persisted, never a cleanup status record,
    and never workflow authority. ``CLEANUP_SENT`` means the canonical
    control was realized and its typed result validated; it is not a claim
    that any filesystem effect occurred.
    """

    CLEANUP_SENT = "cleanup_sent"
    CONTAINED_FAILURE = "contained_failure"
    SKIPPED_NO_TERMINAL_SUBJECT = "skipped_no_terminal_subject"
    SKIPPED_NO_APPLICABLE_CHECKOUT = "skipped_no_applicable_checkout"
    SKIPPED_SESSION_UNAVAILABLE = "skipped_session_unavailable"
    SKIPPED_UNSAFE_OUTSTANDING_WORK = "skipped_unsafe_outstanding_work"


@dataclass(frozen=True, slots=True)
class TerminalCheckoutCleanupExecution:
    """The typed outcome of one cleanup execution attempt."""

    disposition: TerminalCheckoutCleanupDisposition


class TerminalCleanupEstablishmentKind(StrEnum):
    """The bounded eligibility status of one cleanup establishment.

    The read-only pre-claim establishment both cleanup paths consume
    (issue #344): the skip members carry the exact same bounded facts the
    blocking execution's dispositions carry, and ``ELIGIBLE`` carries the
    exact derived subjects. An ineligible or unsafe terminalization skips
    optional cleanup and proceeds to the mandatory archival — never a
    claim, a receipt, or a runtime effect.
    """

    ELIGIBLE = "eligible"
    SKIPPED_NO_TERMINAL_SUBJECT = "skipped_no_terminal_subject"
    SKIPPED_NO_APPLICABLE_CHECKOUT = "skipped_no_applicable_checkout"
    SKIPPED_SESSION_UNAVAILABLE = "skipped_session_unavailable"
    SKIPPED_UNSAFE_OUTSTANDING_WORK = "skipped_unsafe_outstanding_work"


@dataclass(frozen=True, slots=True)
class TerminalCheckoutCleanupEstablishment:
    """The read-only eligibility establishment of one cleanup attempt.

    Derived once from current durable state (issue #344) and consumed by
    both the blocking realization and the detached admission: no claim is
    taken, no receipt is recorded, and no runtime effect occurs here. The
    exact subjects are present only for ``ELIGIBLE``.
    """

    kind: TerminalCleanupEstablishmentKind
    task: Task | None = None
    binding: TaskAgentSession | None = None
    interaction: CleanupWorkspaceInteraction | None = None
    external_session_id: str | None = None
    original_base: str | None = None

    def __post_init__(self) -> None:
        eligible = self.kind is TerminalCleanupEstablishmentKind.ELIGIBLE
        subjects = (
            self.task is not None
            and self.binding is not None
            and self.interaction is not None
            and self.external_session_id is not None
            and self.original_base is not None
        )
        if eligible and not subjects:
            raise ValueError("an eligible cleanup establishment carries its exact subjects")
        if not eligible and subjects:
            raise ValueError("a skipped cleanup establishment carries no subjects")


class TerminalCheckoutCleanupRuntime(ABC):
    """The runtime-neutral seam of the checkout-cleanup realization.

    The workflow-neutral boundary between this service and the concrete
    Agent Runtime: this service owns every identity, eligibility, and
    safety decision and derives the exact interaction; the seam owns only
    the runtime mechanics of reconstructing the exact existing Producer
    session in its authorized cleanup working context and realizing the
    already-authorized cleanup control through it. Implementations surface
    failures as the normalized adapter-error taxonomy; they never decide
    workflow meaning, never mutate durable OpenOrc persistence, and never
    create or replace sessions.
    """

    @abstractmethod
    def cleanup_checkout(
        self,
        pool: DatabasePool,
        *,
        workspace_id: UUID,
        task_id: UUID,
        external_session_id: str,
        working_directory: str,
        interaction: CleanupWorkspaceInteraction,
    ) -> WorkspaceResult:
        """Realize the authorized cleanup control on the exact bound session.

        ``working_directory`` is the caller-derived original repository
        base — the Producer session's authorized cleanup construction
        context, outside the checkout being removed. The exact external
        session identity and its raw conversation are preserved; no
        initialization is resent and no replacement session is created.
        """


def _producer_work_quiescent(
    pool: DatabasePool, *, task_id: UUID, task_agent_session_id: UUID
) -> bool:
    """Whether applicable durable outstanding-work facts establish safety.

    The narrow, conservative pre-send guard: every applicable family is
    read from its own durable settlement/outcome facts. Unsettled
    Interrupt intents, open session-resume records, open Executions,
    pending RuntimeRequests, conservatively claimed runtime-send claims,
    and any unsettled detached-turn receipt on the exact session binding
    (issue #344) all veto — without re-deriving per-claim subject currency,
    a claimed claim stays outstanding (a skip is always acceptable).
    Missing evidence is never proof of quiescence.
    """
    for intent in intent_records.list_task_interrupt_intents(pool, task_id=task_id):
        if intent.status is not TaskInterruptIntentStatus.SETTLED:
            return False
    if intent_records.list_open_task_resume_intents(pool, task_id=task_id):
        return False
    for execution in execution_records.list_task_executions(pool, task_id=task_id):
        if execution.status in _OPEN_EXECUTION_STATUSES:
            return False
    for request in runtime_request_records.list_task_runtime_requests(pool, task_id=task_id):
        if request.status is RuntimeRequestStatus.PENDING:
            return False
    for claim in send_claim_records.list_task_runtime_send_claims(pool, task_id=task_id):
        if claim.status is TaskRuntimeSendClaimStatus.CLAIMED:
            return False
    # An outstanding detached turn (issue #344): the session binding's
    # single-outstanding slot is held, and its run is Producer work the
    # cleanup must never race.
    return (
        detached_turn_records.get_outstanding_task_detached_turn_receipt(
            pool, task_agent_session_id=task_agent_session_id
        )
        is None
    )


def establish_terminal_checkout_cleanup(
    pool: DatabasePool, *, workspace_id: UUID, task_id: UUID
) -> TerminalCheckoutCleanupEstablishment:
    """Establish one cleanup attempt's eligibility and exact subjects (issue #344).

    The read-only derivation both cleanup paths consume, in the established
    guard order: the committed terminalization subject, the original bound
    READY Producer session (never a replacement), the fail-closed snapshot
    recovery of the original repository base and the exact single checkout,
    the conservative outstanding-work guard, and the durable repository
    record behind the composed canonical interaction. No claim is taken, no
    receipt is recorded, and no runtime effect occurs here: every skip is a
    legitimate outcome the caller maps to its own disposition, and the
    mandatory archival never depends on this establishment being eligible.
    """
    # --- Current terminal-subject recheck: the committed terminalization. ---
    task = task_records.get_task(pool, task_id)
    if (
        task is None
        or task.workspace_id != workspace_id
        or task.terminalization_outcome is None
        or task.archived_at is not None
    ):
        # A stale, duplicate, or non-committed observation — including a
        # late one arriving after the archival already committed: the
        # accepted no-op. Duplicate terminal observations never
        # intentionally initiate fresh attempts, and a cleanup is never
        # initiated after archival.
        return TerminalCheckoutCleanupEstablishment(
            kind=TerminalCleanupEstablishmentKind.SKIPPED_NO_TERMINAL_SUBJECT
        )

    # --- The original bound Producer session (never a replacement). ---
    binding = session_records.get_task_agent_session(
        pool, task_id=task_id, role=WorkflowRole.PRODUCER
    )
    if binding is None or binding.workspace_id != workspace_id:
        return TerminalCheckoutCleanupEstablishment(
            kind=TerminalCleanupEstablishmentKind.SKIPPED_NO_APPLICABLE_CHECKOUT
        )
    if (
        binding.lifecycle_status is not TaskSessionLifecycleStatus.READY
        or binding.external_session_id is None
    ):
        # A lost/ended/uninitialized original binding is session-loss
        # evidence for cleanup purposes: skip, never replace or resume.
        return TerminalCheckoutCleanupEstablishment(
            kind=TerminalCleanupEstablishmentKind.SKIPPED_SESSION_UNAVAILABLE
        )

    # --- The exact destructive target, fail-closed snapshot recovery. ---
    try:
        original_base = _producer_snapshot_repository_base(pool, binding, workspace_id=workspace_id)
    except ApplicationError:
        # A missing/inconsistent snapshot (including any non-Cline admitted
        # configuration) fails closed: no OpenOrc-managed checkout cleanup
        # applies. No path is ever substituted.
        return TerminalCheckoutCleanupEstablishment(
            kind=TerminalCleanupEstablishmentKind.SKIPPED_NO_APPLICABLE_CHECKOUT
        )
    checkout_directory = f"{original_base}/{task.id}"

    # --- The narrow pre-send safety guard. ---
    if not _producer_work_quiescent(pool, task_id=task.id, task_agent_session_id=binding.id):
        return TerminalCheckoutCleanupEstablishment(
            kind=TerminalCleanupEstablishmentKind.SKIPPED_UNSAFE_OUTSTANDING_WORK
        )

    # --- The durable repository record behind the composed interaction. ---
    repository = get_repository(pool, task.repository_id)
    if repository is None or repository.workspace_id != workspace_id:
        # An archived Task's repository is durable history; a divergence is
        # an unexpected durable-state invariant failure. Contained as a skip
        # with a sanitized log; cleanup never applies partially.
        logger.warning(
            "terminal checkout cleanup skipped: the task's repository record is not available"
        )
        return TerminalCheckoutCleanupEstablishment(
            kind=TerminalCleanupEstablishmentKind.SKIPPED_NO_APPLICABLE_CHECKOUT
        )
    interaction = CleanupWorkspaceInteraction(
        repository_full_name=(f"{repository.metadata.owner_login}/{repository.metadata.name}"),
        issue_number=task.github_issue_number,
        task_uuid=task.id,
        target_directory=checkout_directory,
    )
    return TerminalCheckoutCleanupEstablishment(
        kind=TerminalCleanupEstablishmentKind.ELIGIBLE,
        task=task,
        binding=binding,
        interaction=interaction,
        external_session_id=_require_cleanup_session_id(binding),
        original_base=original_base,
    )


def _require_cleanup_session_id(binding: TaskAgentSession) -> str:
    """Return the binding's exact external session identity for cleanup."""
    external_session_id = binding.external_session_id
    if external_session_id is None:
        # Unreachable: the caller checked the READY binding's identity. Kept
        # explicit for fail-closed safety and type narrowing.
        raise ApplicationError("the producer binding carries no external session identity")
    return external_session_id
