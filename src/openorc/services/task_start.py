"""Authoritative queue-first Task start and the durable FIFO admission processor (issues #76/#257).

Start an already-intaken, current, ``ready_to_plan`` Task only after fresh
authoritative GitHub eligibility. A successful Owner Start ONLY enqueues: it
freezes the freshly verified requirements fingerprint and the then-current
Workspace Task-budget authorization in the SAME guarded ``queued`` write,
stamps the original durable ``queued_at``, and reserves nothing — it never
scans or admits older Tasks and never directly enters ``planning``. Every
successfully started Task, even with an empty queue and free capacity,
advances exclusively through the ONE bounded FIFO processor,
:func:`reevaluate_queued_tasks`, which alone admits the required Producer and
Reviewer session reservations, commits ``queued → planning``, and whose
admitted outcome carries the exact post-claim token and accountable Owner
facts the next planning-bootstrap handoff addresses (the #319 detached-turn
implementation owns everything after that handoff; this module never
submits, executes, or settles agent work). The queue order is the durable
``(queued_at, id)`` Task fact — never an RQ/Valkey job, a second queue
table, or process memory — and every decision is one short, database-only
transaction.

The invocable commands:

- ``start_task_for_owner`` (authenticated Owner): an authorization phase
  composes the account-operational barrier FIRST and the exact Workspace/Task
  ownership boundary in one short transaction, closed before any external
  I/O — the B3/B4 reconciliation primitives are system-capable and authorize
  nothing themselves, so an unauthorized or account-deletion-blocked caller
  can never trigger GitHub reads through this command. A startable
  ``ready_to_plan`` subject precheck refuses non-start states (including an
  already-claimed ``planning`` Task, which never receives a second admission
  or a second logical start) before any GitHub work. The GitHub freshness
  phase — no database transaction open — revalidates the exact same stable
  Repository + issue identity, the issue's OPEN state, the not-blocked-by
  dependency state, and freshly reconciles the requirements fingerprint the
  successful start will freeze: the Task carries NO baseline before Start
  (issue #233), so issue title/body edits while it remains
  ``ready_to_plan`` are not source drift; a blocked/closed issue, an
  unobservable blocking state, or a lost route is NOT capacity waiting and
  fails closed as the typed non-start condition (generalized TaskBlock/
  source-drift recovery remains issue #85). The arbitration phase composes
  the account barrier FIRST again, the per-Workspace advisory arbitration
  lock, the locked Task re-read bound to the caller's exact ``state_token``,
  the durable projection-based eligibility guard (against the fingerprint
  being frozen), and the structural feasibility verdict — a permanently
  infeasible route is the actionable typed configuration error, never an
  indefinite capacity wait — and then durably queues the target, freezing
  the freshly verified fingerprint and the original Task budget in the SAME
  guarded ``queued`` write (a capacity-queued Task has started for
  authority/baseline purposes even though agents have not yet run); a
  losing, stale, or failed start leaves no baseline. Strictly after that
  commit the start arranges one Task-queue wake invitation through the
  optional :class:`~openorc.services.task_queue_wake.TaskQueueWakeSubmission`
  seam — never inside the transaction, and never submitting this Task's
  bootstrap itself: the ONE FIFO processor owns every admission.
- ``reevaluate_queued_tasks`` (system invocation over existing exact
  subjects; never a new Owner authorization and never changing the
  accountable Owner): the single bounded FIFO processor. Bounded per-page
  arbitration units scan the Workspace's queued Tasks in durable
  ``(queued_at, id)`` order and attempt admission for each currently
  admissible candidate, so older eligible Tasks actually advance instead of
  waiting; an older currently unadmittable, structurally invalid, or
  known-ineligible candidate never indefinitely blocks a feasible follower.
  When the candidate budget bounds the sweep before exhausting the queued
  set, the returned outcome exposes the keyset continuation cursor past the
  examined tail — a paging hint, never Task or scheduling authority — so the
  wake caller arranges bounded continuation through the unexamined tail
  instead of repeatedly revisiting the same prefix.
- ``find_unclaimed_bootstrap_handoff`` (read-only recovery classification,
  issue #257): the restart/recovery seam that positively proves a committed
  ``planning`` Task's F1 bootstrap handoff was never consumed — its E1b
  one-shot ``planning_bootstrap_claim_token`` is still NULL — so the bounded
  recovery may arrange exactly one current-token bootstrap handoff; any
  claimed, admitted, or uncertain runtime turn is never replayed here.

Structural feasibility (the session-admission seam) is computed from the
complete required role-to-Connection multiset — existing valid Task
reservations included in the total requirement, other Tasks' occupancy
excluded — so an impossible configuration (both roles on one capacity-1
Connection, with or without an existing reservation) is an actionable typed
configuration error BEFORE any queue write, never an indefinite capacity
wait. Temporary occupation remains exactly #68's all-or-nothing
``ADMITTED``/``AWAITING_CAPACITY`` decision.

FIFO fairness rests on a real serialization boundary: every transaction that
changes active (CONNECTING/READY) Connection occupancy — this module's
arbitrations and the capacity-releasing lifecycle transitions in
:mod:`openorc.services.session_runtime` — composes the same per-Workspace
transaction-scoped advisory lock (``lock_workspace_task_arbitration``) before
any occupancy read it must trust, so a capacity release can never commit
between a scan and a decision.

A queued Task is waiting for session capacity — a scheduling state, not a
failure, not health, not a TaskBlock: ``Task.status`` plus ``queued_at`` are
canonical, no ``task_queued`` event exists, repeated failed capacity checks
preserve the original queue-entry timestamp, and a queued Task whose routing
became structurally impossible after configuration changes surfaces an
actionable per-candidate condition instead of waiting forever (its original
``queued_at`` is never rewritten to hide the condition). Successful admission
establishes/reuses exactly the two distinct role reservations (CONNECTING or
already READY), commits ``Task.status = planning`` and exactly one
``planning_started`` event as one durable logical claim, and its admitted
outcome exposes the exact post-claim token and accountable Workspace Owner
the next F1 planning-bootstrap handoff addresses (issue #257) — the facts
are captured from the actual claim, never synthesized from an ``ADMITTED``
marker alone. ``planning`` here means capacity admitted and startup claimed —
nothing more. No runtime, model, Cline, or filesystem work happens here.
"""

from __future__ import annotations

import logging
from collections.abc import Mapping
from dataclasses import dataclass, replace
from datetime import datetime
from decimal import Decimal
from enum import StrEnum
from types import MappingProxyType
from typing import NoReturn
from uuid import UUID

from openorc.adapters.github import GitHubAppClient
from openorc.domain.connections import WorkflowRole
from openorc.domain.events import WorkflowEventActor
from openorc.domain.github_issues import GitHubIssueState
from openorc.domain.tasks import Task, TaskStatus
from openorc.observability import annotate_span, application_span
from openorc.persistence import tasks as task_records
from openorc.persistence.github_issue_relations import list_github_issue_blocked_by_edges
from openorc.persistence.github_issues import find_github_issue
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
from openorc.services.event_coordination import WorkflowActorContext
from openorc.services.github_installation_route import (
    resolve_system_repository_installation_route,
)
from openorc.services.github_issue_relations import synchronize_repository_issue_dependencies
from openorc.services.github_reconciliation import reconcile_repository_issue
from openorc.services.profile_lifecycle_guard import require_account_operational
from openorc.services.session_admission import (
    AdmissionStatus,
    StructuralFeasibilityStatus,
    admit_task_agent_sessions,
    assess_structural_route_feasibility,
)
from openorc.services.task_intake import IssueBlockedEligibilityError
from openorc.services.task_queue_wake import (
    TaskQueueWakeSubmission,
    TaskQueueWakeSubmissionOutcome,
    arrange_task_queue_wake,
)
from openorc.services.transaction_composition import composed_transaction
from openorc.services.workspace_authorization import require_workspace_task

__all__ = [
    "QueuedCandidateDisposition",
    "QueuedCandidateOutcome",
    "QueuedSweepOutcome",
    "TaskAlreadyPlanningError",
    "TaskCapacityConfigurationError",
    "TaskEligibilityContradictedError",
    "TaskEligibilityFailure",
    "TaskSourceIdentityLossError",
    "start_durably_offered",
    "TaskStartOutcome",
    "TaskStartStateConflictError",
    "find_unclaimed_bootstrap_handoff",
    "reevaluate_queued_tasks",
    "start_task_for_owner",
]

_SERVICE_TRACER_SCOPE = "openorc.services.task_start"
_START_SPAN_NAME = "task_start.start_task_for_owner"
_ARBITRATION_SPAN_NAME = "task_start.arbitrate_target_start"
_SWEEP_SPAN_NAME = "task_start.reevaluate_queued_tasks"

logger = logging.getLogger(__name__)

# The v1 workflow start always claims both required roles.
_REQUIRED_ROLES: tuple[WorkflowRole, WorkflowRole] = (WorkflowRole.PRODUCER, WorkflowRole.REVIEWER)

# Bounded arbitration work: the establishment pass reads at most this many
# candidates per invocation (page-bounded keyset reads inside it). Hitting
# the budget before a fixpoint means the target durably queues itself —
# conservative deferral, never an unbounded transaction.
_QUEUE_PAGE_LIMIT = 50
_ARBITRATION_CANDIDATE_BUDGET = 500

_OPENORC_ACTOR = WorkflowActorContext(WorkflowEventActor.OPENORC, None)


class TaskAlreadyPlanningError(ConflictError):
    """The addressed Task is already claimed for planning.

    A repeated start or wakeup for a ``planning`` Task must never issue
    another admission, rotate another logical start, or create a second
    session: the claim is one durable fact, and this conflict surfaces the
    existing one.
    """


class TaskStartStateConflictError(ConflictError):
    """The addressed Task's current state is not a startable one.

    ``queued``, ``waiting_for_owner``, ``blocked``, ``implementing``, and
    ``reviewing`` Tasks are not started again through this command; a queued
    Task is re-evaluated by :func:`reevaluate_queued_tasks`, and the other
    states are owned by their own workflow capabilities.
    """


class TaskCapacityConfigurationError(ConflictError):
    """The Task's routed roles can never complete admission.

    The complete required role-to-Connection multiset exceeds a routed
    Connection's configured capacity (for example both roles on one
    capacity-1 Connection) — an actionable Owner configuration condition,
    never capacity waiting and never a queue state. Re-evaluate after the
    Owner changes the routing or capacity.
    """


class TaskSourceIdentityLossError(ConflictError):
    """The fresh authoritative issue no longer carries the Task's identity.

    The resolved stable GitHub issue identity differs from the Task's
    durable ``github_issue_id`` baseline — the addressed number now names a
    different issue (or the identity moved). Not capacity waiting; fail
    closed.
    """


class TaskEligibilityFailure(StrEnum):
    """The typed reason a queued Task's durable eligibility is contradicted.

    Safe classification values only — never projection content, never
    mutable issue metadata.
    """

    UNAVAILABLE = "unavailable"
    IDENTITY_LOST = "identity_lost"
    CLOSED_IN_PROJECTION = "closed_in_projection"
    FINGERPRINT_DRIFT = "fingerprint_drift"
    BLOCKED_IN_PROJECTION = "blocked_in_projection"


_ELIGIBILITY_MESSAGES: Mapping[TaskEligibilityFailure, str] = MappingProxyType(
    {
        TaskEligibilityFailure.UNAVAILABLE: (
            "the durable github issue projection for this task is not available"
        ),
        TaskEligibilityFailure.IDENTITY_LOST: (
            "the durable github issue projection no longer matches the task's stable issue identity"
        ),
        TaskEligibilityFailure.CLOSED_IN_PROJECTION: (
            "the durable github issue projection reports the issue closed"
        ),
        TaskEligibilityFailure.FINGERPRINT_DRIFT: (
            "the durable github issue projection's requirements no longer match "
            "the task's immutable source baseline"
        ),
        TaskEligibilityFailure.BLOCKED_IN_PROJECTION: (
            "the durable github issue projection reports the issue blocked by an issue dependency"
        ),
    }
)


class TaskEligibilityContradictedError(ConflictError):
    """The durable GitHub projection contradicts a Task's queued eligibility.

    A Task can enter the queue while its issue is open, unblocked, and
    consistent with its immutable baseline, and become ineligible while
    waiting (a later reconciliation observes the issue closing, acquiring a
    dependency blocker, or changing requirements). The capacity path fails
    closed against the durable mirror — it never advances a known-changed
    Task and never makes a fresh GitHub network call per probe. Carries only
    the safe typed :class:`TaskEligibilityFailure` reason.
    """

    def __init__(self, reason: TaskEligibilityFailure) -> None:
        super().__init__(_ELIGIBILITY_MESSAGES[reason])
        self.reason = reason


@dataclass(frozen=True, slots=True)
class TaskStartOutcome:
    """The complete result of one queue-first Task-start invocation (issue #257).

    A successful start ONLY reports the durable ``queued`` outcome: the
    post-write Task carries the frozen requirements fingerprint and budget,
    the original ``queued_at`` entry timestamp, and the rotated
    ``state_token``. There is no synchronous ``planning`` result, no session
    reservations, and no bootstrap submission from Start — the ONE FIFO
    processor owns every admission, and its ``ADMITTED`` outcomes carry the
    post-claim facts the next F1 handoff addresses.

    ``queue_wake_outcome`` is the actual post-commit wake-delivery evidence
    returned by the shared arrangement helper (issue #378): the bounded
    submission outcome exactly as the seam reported it, or ``None`` for an
    unwired seam. It is transport-facing delivery evidence only — never a
    Task-state, currentness, or authorization fact, and never a condition
    of the committed queue entry.
    """

    task: Task
    queue_wake_outcome: TaskQueueWakeSubmissionOutcome | None = None


class QueuedCandidateDisposition(StrEnum):
    """One queued candidate's bounded arbitration outcome."""

    ADMITTED = "admitted"
    STILL_QUEUED = "still_queued"
    INELIGIBLE = "ineligible"
    CONFIGURATION_ERROR = "configuration_error"
    CONFLICT = "conflict"
    NO_LONGER_QUEUED = "no_longer_queued"


@dataclass(frozen=True, slots=True)
class QueuedCandidateOutcome:
    """One queued Task's per-candidate arbitration outcome (safe facts only).

    An ``ADMITTED`` outcome additionally carries the exact durable post-claim
    facts the next F1 planning-bootstrap handoff addresses — the claimed
    Task with its rotated ``state_token``, and the accountable Workspace
    Owner identity — captured from the actual claim, never synthesized from
    the ``ADMITTED`` marker alone (issue #257). Every other disposition
    carries neither.
    """

    task_id: UUID
    github_issue_number: int
    disposition: QueuedCandidateDisposition
    eligibility_reason: TaskEligibilityFailure | None = None
    admitted_task: Task | None = None
    owner_profile_id: UUID | None = None


@dataclass(frozen=True, slots=True)
class QueuedSweepOutcome:
    """The result of one queued-Tasks FIFO re-evaluation sweep (issues #76/#257).

    ``exhausted`` is true when the bounded sweep examined the Workspace's
    entire queued set; false means the candidate budget bounded the sweep and
    the unexamined tail stays queued in durable FIFO order. When the sweep is
    not exhausted, ``continuation_cursor`` is the keyset position past the
    last examined candidate — a paging hint the wake caller may arrange one
    bounded continuation sweep after, never Task or scheduling authority, and
    every resumed candidate is revalidated on execution.
    """

    outcomes: tuple[QueuedCandidateOutcome, ...]
    exhausted: bool
    continuation_cursor: tuple[datetime, UUID] | None = None


def _require_uuid_command(value: object, name: str) -> None:
    """Reject a malformed UUID command argument before any state is touched."""
    if not isinstance(value, UUID):
        raise InvalidCommandError(f"{name} must be a UUID")


def _contradicted(reason: TaskEligibilityFailure) -> NoReturn:
    """Raise the typed durable-eligibility contradiction for one reason."""
    raise TaskEligibilityContradictedError(reason)


def _require_durable_queue_eligibility(
    transaction_pool: DatabasePool,
    *,
    task: Task,
    source_requirements_fingerprint: str | None = None,
) -> None:
    """Prove the Task is still eligible against the durable GitHub projection.

    The database-only eligibility guard applied before claiming ANY Task's
    runtime capacity (issue #76): the durable projection for the Task's
    stable issue identity must exist, carry exactly that identity, report
    the issue open, carry a requirements fingerprint equal to the compared
    baseline, and the mirrored blocked-by dependency state must be empty. A
    Task that queued while eligible can become ineligible while waiting; a
    capacity wakeup must not advance a known-changed Task. No GitHub network
    call and no projection rewrite happens here — the mirror is refreshed by
    the reconciliation capabilities, and this guard revalidates against
    exactly what those capabilities durably observed.

    The compared requirements baseline is explicit (issue #233): a queued
    candidate compares its own frozen baseline (the default, read from the
    Task row), while a starting target compares the freshly verified
    fingerprint that this same arbitration transaction will freeze — the
    target's row baseline is still NULL until the capture commits. A NULL
    compared baseline (possible only for a never-started Task outside its
    own start) compares nothing.
    """
    projection = find_github_issue(
        transaction_pool, repository_id=task.repository_id, github_issue_id=task.github_issue_id
    )
    if projection is None or projection.repository_id != task.repository_id:
        _contradicted(TaskEligibilityFailure.UNAVAILABLE)
    if projection.identity.github_issue_id != task.github_issue_id:
        _contradicted(TaskEligibilityFailure.IDENTITY_LOST)
    if projection.state is not GitHubIssueState.OPEN:
        _contradicted(TaskEligibilityFailure.CLOSED_IN_PROJECTION)
    compared_baseline = (
        source_requirements_fingerprint
        if source_requirements_fingerprint is not None
        else task.source_requirements_fingerprint
    )
    if compared_baseline is not None and projection.requirements_fingerprint != compared_baseline:
        _contradicted(TaskEligibilityFailure.FINGERPRINT_DRIFT)
    blockers = list_github_issue_blocked_by_edges(
        transaction_pool, repository_id=task.repository_id, github_issue_id=task.github_issue_id
    )
    if blockers:
        _contradicted(TaskEligibilityFailure.BLOCKED_IN_PROJECTION)


def _claim_task_for_planning(
    transaction_pool: DatabasePool,
    *,
    task: Task,
    actor: WorkflowActorContext,
    source_requirements_fingerprint: str | None = None,
    authorized_task_budget_usd: Decimal | None = None,
) -> Task:
    """Atomically claim the Task for bootstrap: planning + baseline + one start event.

    The conditional, token-rotating status write (``queued_at`` is cleared by
    the same statement when the Task leaves ``queued``) and the
    ``planning_started`` event commit together as one durable logical claim.
    The Owner Start path passes ``source_requirements_fingerprint`` so the
    freshly verified fingerprint is frozen by the SAME statement (issue
    #233): the capture is conditioned on the still-NULL baseline, so a
    successful planning claim and its immutable baseline commit — or roll
    back — together. It also passes the then-current Workspace
    ``authorized_task_budget_usd`` (issue #240), frozen by the same
    statement and conditioned on the still-NULL authorization. The
    queued-candidate sweep omits both: an already-queued candidate's
    baseline and authorization were frozen at its queue entry and the
    coalesce captures are no-ops — a queued wakeup never rewrites them. A
    rejected write — a concurrent mutation won between the caller's
    locked read and this write — is never applied and never retried blindly:
    the typed stale outcome propagates to the caller's classification.
    """
    claimed = task_records.update_task_status(
        transaction_pool,
        task.id,
        expected_state_token=task.state_token,
        status=TaskStatus.PLANNING,
        capture_source_requirements_fingerprint=source_requirements_fingerprint,
        capture_authorized_task_budget_usd=authorized_task_budget_usd,
    )
    if claimed is None:
        raise StaleOperationError(
            "the task state moved on during the capacity decision; the start is stale"
        )
    event_coordination.record_planning_started_event(
        transaction_pool,
        workspace_id=task.workspace_id,
        task_id=task.id,
        actor=actor,
    )
    return claimed


def _queue_task_for_capacity(
    transaction_pool: DatabasePool,
    *,
    task: Task,
    source_requirements_fingerprint: str | None = None,
    authorized_task_budget_usd: Decimal | None = None,
) -> Task:
    """Durably enter the Task into the FIFO capacity queue.

    The conditional, token-rotating status write stamps ``queued_at`` from
    the database clock exactly once on entering ``queued``. The Owner Start
    path passes ``source_requirements_fingerprint`` so the freshly verified
    fingerprint is frozen by the SAME statement (issue #233): entering
    ``queued`` IS a started Task for authority and baseline purposes even
    though agents have not yet run, and the capture is conditioned on the
    still-NULL baseline so it commits — or rolls back — atomically with the
    queue entry. It also passes the then-current Workspace
    ``authorized_task_budget_usd`` (issue #240), frozen by the same
    statement: a queued Task carries its original spending authorization
    when capacity later becomes available, and no later default edit or
    wakeup rewrites it. No reservation is made and no scheduler/capacity
    event exists here — the one ``task_started`` audit fact is the Owner
    Start composition's, recorded atomically beside this guarded write
    (issue #377) — and the returned Task carries the entry timestamp the
    FIFO order uses.
    """
    queued = task_records.update_task_status(
        transaction_pool,
        task.id,
        expected_state_token=task.state_token,
        status=TaskStatus.QUEUED,
        capture_source_requirements_fingerprint=source_requirements_fingerprint,
        capture_authorized_task_budget_usd=authorized_task_budget_usd,
    )
    if queued is None:
        raise StaleOperationError(
            "the task state moved on during the capacity decision; the queueing is stale"
        )
    return queued


def _arbitrate_queued_candidate(
    transaction_pool: DatabasePool,
    *,
    candidate: Task,
    profile_id: UUID,
    workspace_id: UUID,
) -> QueuedCandidateOutcome:
    """Attempt one queued Task's full claim as one savepoint-scoped decision.

    All-or-nothing for this candidate inside the caller's arbitration
    composition: locked re-read, durable eligibility guard, structural
    feasibility, the #68 all-or-nothing reservation, and the atomic
    planning claim — or nothing. A classified condition becomes a returned
    per-candidate outcome (the candidate stays queued with its original
    timestamp) and the caller's transaction stays usable; an unclassified
    failure propagates and fails the caller's transaction closed.
    """
    try:
        with transaction_pool.connection():
            current = task_records.get_task_for_update(transaction_pool, task_id=candidate.id)
            if (
                current is None
                or current.workspace_id != workspace_id
                or current.status is not TaskStatus.QUEUED
            ):
                # Concurrently archived, moved, or already claimed between the
                # queue read and this decision: nothing to arbitrate.
                return QueuedCandidateOutcome(
                    task_id=candidate.id,
                    github_issue_number=candidate.github_issue_number,
                    disposition=QueuedCandidateDisposition.NO_LONGER_QUEUED,
                )
            _require_durable_queue_eligibility(transaction_pool, task=current)
            feasibility = assess_structural_route_feasibility(
                transaction_pool,
                profile_id=profile_id,
                workspace_id=workspace_id,
                task_id=current.id,
                roles=_REQUIRED_ROLES,
            )
            if feasibility.status is StructuralFeasibilityStatus.STRUCTURALLY_INFEASIBLE:
                return QueuedCandidateOutcome(
                    task_id=current.id,
                    github_issue_number=current.github_issue_number,
                    disposition=QueuedCandidateDisposition.CONFIGURATION_ERROR,
                )
            admission = admit_task_agent_sessions(
                transaction_pool,
                profile_id=profile_id,
                workspace_id=workspace_id,
                task_id=current.id,
                roles=_REQUIRED_ROLES,
            )
            if admission.status is not AdmissionStatus.ADMITTED:
                return QueuedCandidateOutcome(
                    task_id=current.id,
                    github_issue_number=current.github_issue_number,
                    disposition=QueuedCandidateDisposition.STILL_QUEUED,
                )
            claimed = _claim_task_for_planning(transaction_pool, task=current, actor=_OPENORC_ACTOR)
            if claimed.status is not TaskStatus.PLANNING:  # pragma: no cover
                # The claim write is the only source of this outcome's facts;
                # it cannot legally return a non-planning row. Fail closed
                # rather than report an admitted candidate without its
                # post-claim facts.
                raise StaleOperationError(
                    "the planning claim did not persist its post-claim facts; the sweep is stale"
                )
            return QueuedCandidateOutcome(
                task_id=current.id,
                github_issue_number=current.github_issue_number,
                disposition=QueuedCandidateDisposition.ADMITTED,
                admitted_task=claimed,
                owner_profile_id=profile_id,
            )
    except TaskEligibilityContradictedError as error:
        return QueuedCandidateOutcome(
            task_id=candidate.id,
            github_issue_number=candidate.github_issue_number,
            disposition=QueuedCandidateDisposition.INELIGIBLE,
            eligibility_reason=error.reason,
        )
    except TaskCapacityConfigurationError:
        return QueuedCandidateOutcome(
            task_id=candidate.id,
            github_issue_number=candidate.github_issue_number,
            disposition=QueuedCandidateDisposition.CONFIGURATION_ERROR,
        )
    except NotFoundError:
        # A required routing subject is gone — a missing role binding or a
        # missing Connection for this candidate's current routes. This is an
        # actionable configuration condition for THIS candidate only
        # (typically a role-binding change after it queued): it stays queued
        # with its original timestamp and arbitration continues, so one
        # anomalous waiter never blocks feasible followers.
        return QueuedCandidateOutcome(
            task_id=candidate.id,
            github_issue_number=candidate.github_issue_number,
            disposition=QueuedCandidateDisposition.CONFLICT,
        )
    except ConflictError:
        # Route conflict, terminal historical session, or a raced subject
        # state: an actionable condition for this candidate only — never
        # capacity waiting, never silently swallowed, and never a reason to
        # stall the remaining candidates.
        return QueuedCandidateOutcome(
            task_id=candidate.id,
            github_issue_number=candidate.github_issue_number,
            disposition=QueuedCandidateDisposition.CONFLICT,
        )


def _revalidate_fresh_github_eligibility(
    pool: DatabasePool, github: GitHubAppClient, *, task: Task
) -> str:
    """Freshly revalidate the Task's authoritative eligibility (no transaction open).

    The same B3/B4 critical path intake performs, without creating a
    baseline: resolve the explicit currently usable installation route;
    fresh issue reconciliation (the exact same stable Repository + issue
    identity, the issue still OPEN, the current requirements fingerprint);
    the fresh blocked-by dependency observation. A blocked/closed issue, an
    unobservable blocking state, or a lost route is not capacity waiting:
    the typed non-start condition fails closed.

    Returns the freshly reconciled requirements fingerprint that a
    successful start freezes (issue #233): the Task carries NO baseline
    before Start, so there is nothing to compare against here — issue
    title/body edits while the Task remains ``ready_to_plan`` are not source
    drift, and the frozen baseline is exactly what this fresh observation
    returns at the moment the start commits.
    """
    resolve_system_repository_installation_route(
        pool, workspace_id=task.workspace_id, repository_id=task.repository_id
    )
    reconciliation = reconcile_repository_issue(
        pool,
        github,
        workspace_id=task.workspace_id,
        repository_id=task.repository_id,
        issue_number=task.github_issue_number,
    )
    projection = reconciliation.issue
    if projection.state is not GitHubIssueState.OPEN:
        # The fresh authoritative check: a closed issue is not startable.
        raise NotFoundError("the addressed GitHub issue is not open")
    if projection.identity.github_issue_id != task.github_issue_id:
        raise TaskSourceIdentityLossError(
            "the fresh authoritative issue no longer carries the task's stable issue identity"
        )
    dependency_sync = synchronize_repository_issue_dependencies(
        pool,
        github,
        workspace_id=task.workspace_id,
        repository_id=task.repository_id,
        issue_number=task.github_issue_number,
        github_issue_id=projection.identity.github_issue_id,
    )
    if dependency_sync.blocked:
        raise IssueBlockedEligibilityError(
            "GitHub currently reports the addressed issue as blocked by an issue dependency"
        )
    return projection.requirements_fingerprint


def _require_startable_task(task: Task) -> None:
    """Classify the subject's current state before any GitHub work.

    Only a current, non-archived ``ready_to_plan`` Task starts. An already
    claimed ``planning`` Task is a distinct typed conflict: a repeated start
    must not issue another admission or create another logical start.
    """
    if task.archived_at is not None:
        raise StaleOperationError("the task has already been archived; the operation is stale")
    if task.status is TaskStatus.PLANNING:
        raise TaskAlreadyPlanningError(
            "the task is already claimed for planning; the start is a no-op"
        )
    if task.status is not TaskStatus.READY_TO_PLAN:
        raise TaskStartStateConflictError(
            "only a ready_to_plan task can be started; a queued task is re-evaluated "
            "by the queued-Tasks sweep"
        )


def start_durably_offered(transaction_pool: DatabasePool, *, task: Task) -> bool:
    """Whether the durable facts positively support offering ``start`` (#376).

    The authorized read surface's backend-generated action projection reuses
    the authoritative Start eligibility predicates — the startable-state
    rule and the durable GitHub-projection queue eligibility — exactly as
    the command boundary applies them, evaluated inside the caller's own
    transaction and never persisted, never an external call, and never a
    second policy engine. A ready_to_plan Task whose durable projection has
    closed, lost its stable identity, acquired a mirrored blocker, or
    drifted from its frozen baseline is never offered. The command still
    revalidates everything freshly at execution; offering is neither
    permission nor a promise.
    """
    try:
        _require_startable_task(task)
        _require_durable_queue_eligibility(transaction_pool, task=task)
    except (TaskEligibilityContradictedError, ConflictError, StaleOperationError):
        return False
    return True


def start_task_for_owner(
    pool: DatabasePool,
    github: GitHubAppClient,
    *,
    profile_id: UUID,
    workspace_id: UUID,
    task_id: UUID,
    expected_state_token: UUID,
    queue_wake: TaskQueueWakeSubmission | None = None,
) -> TaskStartOutcome:
    """Start a ``ready_to_plan`` Task under the authenticated Owner boundary.

    Three strictly ordered phases: the short authorization transaction (the
    account-operational barrier FIRST, exact Workspace/Task ownership, the
    subject load and startability precheck — closed before external I/O);
    the fresh authoritative GitHub revalidation with no database transaction
    open; and the one short arbitration transaction that durably queues the
    Task with its frozen fingerprint and budget (issue #257 — Start never
    reserves sessions or claims ``planning`` itself). Raises the typed
    authorization vocabulary uniformly for absent/foreign subjects and the
    typed non-start conditions above for ineligible sources and states;
    queueing is the successful outcome, never an error. Strictly after the
    commit the start arranges one Task-queue wake invitation through the
    optional ``queue_wake`` seam — an unwired, refused, or uncertain wake
    never fails or reverses the committed queue entry, and the durable
    ``queued`` facts own recovery.
    """
    _require_uuid_command(profile_id, "profile_id")
    _require_uuid_command(workspace_id, "workspace_id")
    _require_uuid_command(task_id, "task_id")
    _require_uuid_command(expected_state_token, "expected_state_token")
    with application_span(_SERVICE_TRACER_SCOPE, _START_SPAN_NAME) as span:
        annotate_span(
            span,
            operation=_START_SPAN_NAME,
            workspace_id=str(workspace_id),
            task_id=str(task_id),
        )
        # Phase A: the authorization + subject-load transaction. Closed
        # before any external GitHub I/O — the reconciliation primitives are
        # system-capable and authorize nothing themselves.
        with composed_transaction(pool) as transaction_pool:
            require_account_operational(transaction_pool, profile_id=profile_id)
            require_workspace_task(
                transaction_pool,
                profile_id=profile_id,
                workspace_id=workspace_id,
                task_id=task_id,
            )
            task = task_records.get_task(transaction_pool, task_id)
            if task is None or task.workspace_id != workspace_id:
                raise NotFoundError("the requested task is not available in this workspace")
            _require_startable_task(task)
        # Phase B: fresh authoritative eligibility, no database transaction
        # open (the B3/B4 critical path; an unobservable state fails closed).
        # The returned fingerprint is what a successful start freezes.
        fresh_fingerprint = _revalidate_fresh_github_eligibility(pool, github, task=task)
        # Phase C: the arbitration decision under the serialization boundary,
        # strictly followed by the post-commit queue-processor wake.
        outcome = _arbitrate_target_start(
            pool,
            profile_id=profile_id,
            workspace_id=workspace_id,
            task_id=task_id,
            expected_state_token=expected_state_token,
            source_requirements_fingerprint=fresh_fingerprint,
        )
        # The post-commit queue-processor wake: the actual delivery outcome
        # the shared seam returned (including the unwired ``None``) is part
        # of the truthful invocation result (issue #378). An unwired,
        # refused, or uncertain wake never fails or reverses the committed
        # queue entry — the durable ``queued`` facts own recovery.
        wake_outcome = arrange_task_queue_wake(
            queue_wake,
            workspace_id=workspace_id,
            origin="owner_task_start",
        )
        return replace(outcome, queue_wake_outcome=wake_outcome)


def _arbitrate_target_start(
    pool: DatabasePool,
    *,
    profile_id: UUID,
    workspace_id: UUID,
    task_id: UUID,
    expected_state_token: UUID,
    source_requirements_fingerprint: str,
) -> TaskStartOutcome:
    """One short arbitration: durably queue the target, freezing its authorization.

    The account-operational barrier is the FIRST lock acquisition; the
    per-Workspace advisory arbitration lock next — the same boundary every
    capacity-releasing lifecycle transition composes, so no occupancy
    change can commit between the locked re-read and the queue entry. The
    locked re-read binds the decision to the caller's exact ``state_token``;
    the durable eligibility guard (against the freshly verified fingerprint
    being frozen) and the structural feasibility verdict decide against one
    consistent picture. A successful decision freezes
    ``source_requirements_fingerprint`` and the then-current Workspace
    ``authorized_task_budget_usd`` in the same guarded statement that
    durably queues the Task (issues #233/#240) — Start never reserves
    sessions, never scans or admits older Tasks, and never claims
    ``planning`` itself: the ONE FIFO processor owns every admission and
    derives queue eligibility from Postgres after the commit. A losing,
    stale, or failed decision rolls back with no baseline.
    """
    with application_span(_SERVICE_TRACER_SCOPE, _ARBITRATION_SPAN_NAME) as span:
        annotate_span(
            span,
            operation=_ARBITRATION_SPAN_NAME,
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
            lock_workspace_task_arbitration(transaction_pool, workspace_id=workspace_id)
            task = task_records.get_task_for_update(transaction_pool, task_id)
            if task is None or task.workspace_id != workspace_id:
                raise NotFoundError("the requested task is not available in this workspace")
            if task.archived_at is not None or task.state_token != expected_state_token:
                raise StaleOperationError(
                    "the task state has moved on since this start was prepared; "
                    "the operation is stale"
                )
            _require_startable_task(task)
            if task.source_requirements_fingerprint is not None:
                # Post-migration durable invariant (issue #233): a current
                # ready_to_plan Task carries no baseline. A present baseline
                # here would be a stale intake capture the corrected contract
                # never froze — refuse instead of silently re-baselining or
                # comparing against it.
                raise ConflictError(
                    "the current ready_to_plan task already carries a source "
                    "requirements baseline; the corrected contract freezes it "
                    "only at a successful Owner Start"
                )
            # The then-current Workspace spending default (issue #240): read
            # inside the same arbitration transaction so the authorization is
            # captured atomically with the guarded start transition and can
            # never silently diverge from a concurrent default edit.
            workspace = get_workspace(transaction_pool, workspace_id)
            if workspace is None:
                raise NotFoundError("the requested workspace is not available")
            # The fresh mirror Phase B just refreshed is the eligibility
            # authority for the target's queue entry too; the compared
            # baseline is the fingerprint this decision will freeze
            # (issue #233).
            _require_durable_queue_eligibility(
                transaction_pool,
                task=task,
                source_requirements_fingerprint=source_requirements_fingerprint,
            )
            feasibility = assess_structural_route_feasibility(
                transaction_pool,
                profile_id=profile_id,
                workspace_id=workspace_id,
                task_id=task_id,
                roles=_REQUIRED_ROLES,
            )
            if feasibility.status is not StructuralFeasibilityStatus.FEASIBLE:
                raise TaskCapacityConfigurationError(
                    "the task's routed roles can never complete admission on the "
                    "configured connections; change the role routing or capacity"
                )
            # The queue-only Start decision (issue #257): the target durably
            # queues with its own entry timestamp — with free capacity or
            # not — and the ONE FIFO processor admits it in durable FIFO
            # order after this commit, exactly like every other queued
            # candidate.
            queued = _queue_task_for_capacity(
                transaction_pool,
                task=task,
                source_requirements_fingerprint=source_requirements_fingerprint,
                authorized_task_budget_usd=workspace.default_task_budget_usd,
            )
            # The Owner Start audit fact (issue #377): the authenticated
            # Owner's successful Start authorization is recorded in the SAME
            # transaction as the guarded QUEUED write it authorized — event
            # insertion failure rolls the start back, and a losing, stale, or
            # failed decision never reaches this emission. The actor is the
            # authenticated caller's Profile, never a supplied actor ID or
            # the Workspace Owner inferred later.
            event_coordination.record_task_started_event(
                transaction_pool,
                workspace_id=workspace_id,
                task_id=task_id,
                actor=event_coordination.owner_actor(profile_id),
            )
            return TaskStartOutcome(task=queued)


def reevaluate_queued_tasks(
    pool: DatabasePool,
    *,
    workspace_id: UUID,
    after_queued_at: datetime | None = None,
    after_task_id: UUID | None = None,
) -> QueuedSweepOutcome:
    """Re-evaluate the Workspace's queued Tasks after capacity may have freed.

    The ONE durable FIFO admission processor (issues #76/#257): the system
    invocation over existing exact Task subjects, never a new Owner
    authorization and never a change of the accountable Owner — each
    candidate's claim carries the OPENORC system actor and the accountable
    Profile is the Workspace's durable Owner. Bounded per-page arbitration
    units scan the durable ``(queued_at, id)`` FIFO order to exhaustion (or
    the candidate budget); every unit reads fresh occupancy under its own
    locks, so capacity released between units is respected, and each
    candidate's claim commits as its own atomic unit of work. Classified
    conditions are per-candidate outcomes — an ineligible, conflicted, or
    structurally infeasible candidate stays queued with its original
    timestamp and the condition is surfaced, not swallowed, so it never
    blocks a feasible follower; an unclassified failure propagates and fails
    the sweep closed. The decision path is database-only: no GitHub network
    read, SDK call, F1 submission, or other external I/O happens while a
    transaction is open — the wake caller owns every post-commit handoff.

    ``after_queued_at``/``after_task_id`` resume the scan after a prior
    sweep's keyset continuation cursor (a paging hint, never authority); the
    sweep requires both together or neither. A budget-bounded sweep returns
    ``exhausted=False`` with the continuation cursor past its examined tail
    so the wake caller arranges bounded continuation through the unexamined
    tail instead of repeatedly revisiting the same prefix.
    """
    _require_uuid_command(workspace_id, "workspace_id")
    if (after_queued_at is None) != (after_task_id is None):
        raise InvalidCommandError(
            "the sweep continuation cursor requires both its timestamp and task id"
        )
    with application_span(_SERVICE_TRACER_SCOPE, _SWEEP_SPAN_NAME) as span:
        annotate_span(span, operation=_SWEEP_SPAN_NAME, workspace_id=str(workspace_id))
        outcomes: list[QueuedCandidateOutcome] = []
        last_examined: tuple[datetime, UUID] | None = (
            None
            if after_queued_at is None or after_task_id is None
            else (after_queued_at, after_task_id)
        )
        exhausted = False
        while not exhausted:
            with composed_transaction(pool) as transaction_pool:
                workspace = get_workspace(transaction_pool, workspace_id)
                if workspace is None:
                    raise NotFoundError("the requested workspace is not available")
                # The barrier is the FIRST lock acquisition of every unit;
                # the arbitration lock is the same boundary the lifecycle
                # capacity releases compose.
                require_account_operational(transaction_pool, profile_id=workspace.owner_profile_id)
                lock_workspace_task_arbitration(transaction_pool, workspace_id=workspace_id)
                page = task_records.list_queued_tasks(
                    transaction_pool,
                    workspace_id=workspace_id,
                    after_queued_at=None if last_examined is None else last_examined[0],
                    after_task_id=None if last_examined is None else last_examined[1],
                    limit=_QUEUE_PAGE_LIMIT,
                )
                if not page:
                    exhausted = True
                    continue
                for candidate in page:
                    if len(outcomes) >= _ARBITRATION_CANDIDATE_BUDGET:
                        exhausted = False
                        break
                    outcomes.append(
                        _arbitrate_queued_candidate(
                            transaction_pool,
                            candidate=candidate,
                            profile_id=workspace.owner_profile_id,
                            workspace_id=workspace_id,
                        )
                    )
                    if candidate.queued_at is None:  # pragma: no cover - queued rows carry it
                        raise StaleOperationError(
                            "the durable queue key disappeared during the sweep; the sweep is stale"
                        )
                    last_examined = (candidate.queued_at, candidate.id)
                else:
                    continue
                break
        continuation_cursor = None if exhausted else last_examined
        return QueuedSweepOutcome(
            outcomes=tuple(outcomes),
            exhausted=exhausted,
            continuation_cursor=continuation_cursor,
        )


def find_unclaimed_bootstrap_handoff(
    pool: DatabasePool,
    *,
    workspace_id: UUID,
    task_id: UUID,
) -> Task | None:
    """Classify the lost F1 planning-bootstrap handoff state, read-only.

    The bounded restart/recovery classification (issue #257): the Task must
    be current, non-archived, coarse-``planning``, and its E1b one-shot
    ``planning_bootstrap_claim_token`` still NULL — the claim is the
    bootstrap's first durable effect, written exactly once strictly before
    any establishment dispatch (issue #203), so a NULL claim positively
    proves the F1 bootstrap invocation was never consumed and the admitted
    ``PLANNING`` commit's handoff was lost. Presence means the current Task
    is returned for the handoff's payload composition; ``None`` means the
    durable state does not (or no longer) prove the unclaimed handoff and
    the recovery submits nothing — the decision is re-derived from committed
    state on every trigger, never assumed. A claimed, admitted, or uncertain
    runtime turn is never replayed here: the claim's existence routes every
    consumed effect to the existing detached correlation and the Level 1/2/3
    machinery, and the accepted silent-worker-death limitation stands.
    """
    _require_uuid_command(workspace_id, "workspace_id")
    _require_uuid_command(task_id, "task_id")
    with composed_transaction(pool) as transaction_pool:
        task = task_records.get_task(transaction_pool, task_id=task_id)
        if task is None or task.workspace_id != workspace_id:
            return None
        if task.status is not TaskStatus.PLANNING or task.archived_at is not None:
            return None
        if (
            task_records.get_task_planning_bootstrap_claim(transaction_pool, task_id=task_id)
            is not None
        ):
            return None
        return task
