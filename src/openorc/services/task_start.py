"""Authoritative Task start, structural capacity, and durable FIFO admission (issue #76).

Start an already-intaken, current, ``ready_to_plan`` Task only after fresh
authoritative GitHub eligibility; fairly arbitrate the required Producer and
Reviewer capacity across the Owner-configured Connection routes; durably
queue the Task when capacity is temporarily occupied; and claim the Task for
bootstrap (``planning``) atomically with both role reservations AND the
frozen requirements baseline. The queue
order is the durable ``(queued_at, id)`` Task fact — never an RQ/Valkey job,
a second queue table, or process memory — and every decision is one short,
database-only transaction.

The two invocable commands:

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
  being frozen), the structural feasibility verdict, one bounded FIFO
  establishment pass, and the target's own decision — which freezes the
  freshly verified fingerprint atomically with the ``planning`` claim or the
  durable ``queued`` entry (a capacity-queued Task has started for
  authority/baseline purposes even though agents have not yet run); a
  losing, stale, or failed start leaves no baseline.
- ``reevaluate_queued_tasks`` (system invocation over existing exact
  subjects; never a new Owner authorization and never changing the
  accountable Owner): bounded per-page arbitration units that scan the
  Workspace's queued Tasks in durable ``(queued_at, id)`` order and attempt
  admission for each currently admissible candidate, so older eligible Tasks
  actually advance instead of waiting.

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
between a scan and a decision. Where bounded work cannot establish that no
older candidate is bypassed, the newer Task durably queues itself with its
own timestamp instead of claiming — FIFO order is preserved in every case.

A queued Task is waiting for session capacity — a scheduling state, not a
failure, not health, not a TaskBlock: ``Task.status`` plus ``queued_at`` are
canonical, no ``task_queued`` event exists, repeated failed capacity checks
preserve the original queue-entry timestamp, and a queued Task whose routing
became structurally impossible after configuration changes surfaces an
actionable per-candidate condition instead of waiting forever (its original
``queued_at`` is never rewritten to hide the condition). Successful admission
establishes/reuses exactly the two distinct role reservations (CONNECTING or
already READY), commits ``Task.status = planning`` and exactly one
``planning_started`` event as one durable logical claim, and returns the
rotated token plus both exact role bindings for the E1b handoff (issue #203).
``planning`` here means capacity admitted and startup claimed — nothing more.
No runtime, model, Cline, or filesystem work happens here.
"""

from __future__ import annotations

import logging
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from types import MappingProxyType
from typing import NoReturn
from uuid import UUID

from openorc.adapters.github import GitHubAppClient
from openorc.domain.connections import WorkflowRole
from openorc.domain.events import WorkflowEventActor
from openorc.domain.github_issues import GitHubIssueState
from openorc.domain.sessions import TaskAgentSession
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
    "TaskStartOutcome",
    "TaskStartQueueReason",
    "TaskStartStateConflictError",
    "TaskStartStatus",
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


class TaskStartStatus(StrEnum):
    """The two durable outcomes of one Task-start arbitration."""

    PLANNING = "planning"
    QUEUED = "queued"


class TaskStartQueueReason(StrEnum):
    """Why a startably routed Task durably queued instead of claiming."""

    AWAITING_CAPACITY = "awaiting_capacity"
    ARBITRATION_DEFERRED = "arbitration_deferred"


@dataclass(frozen=True, slots=True)
class TaskStartOutcome:
    """The complete result of one Task-start invocation (issue #76).

    ``PLANNING`` carries the post-write Task (rotated token, ``queued_at``
    cleared) and both exact role bindings for the #203 handoff. ``QUEUED``
    carries the post-write Task with its durable ``queued_at`` entry
    timestamp, no reservations, and the queue reason. ``PLANNING`` means
    capacity admitted and startup claimed — nothing more.
    """

    status: TaskStartStatus
    task: Task
    sessions: Mapping[WorkflowRole, TaskAgentSession]
    queue_reason: TaskStartQueueReason | None


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
    """One queued Task's per-candidate arbitration outcome (safe facts only)."""

    task_id: UUID
    github_issue_number: int
    disposition: QueuedCandidateDisposition
    eligibility_reason: TaskEligibilityFailure | None = None


@dataclass(frozen=True, slots=True)
class QueuedSweepOutcome:
    """The result of one queued-Tasks re-evaluation sweep (issue #76).

    ``exhausted`` is true when the bounded sweep examined the Workspace's
    entire queued set; false means the candidate budget bounded the sweep
    and the remaining candidates stay queued for the next wakeup, in durable
    FIFO order.
    """

    outcomes: tuple[QueuedCandidateOutcome, ...]
    exhausted: bool


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
) -> Task:
    """Atomically claim the Task for bootstrap: planning + baseline + one start event.

    The conditional, token-rotating status write (``queued_at`` is cleared by
    the same statement when the Task leaves ``queued``) and the
    ``planning_started`` event commit together as one durable logical claim.
    The Owner Start path passes ``source_requirements_fingerprint`` so the
    freshly verified fingerprint is frozen by the SAME statement (issue
    #233): the capture is conditioned on the still-NULL baseline, so a
    successful planning claim and its immutable baseline commit — or roll
    back — together. The queued-candidate sweep omits it: an already-queued
    candidate's baseline is already frozen and the coalesce capture is a
    no-op. A rejected write — a concurrent mutation won between the caller's
    locked read and this write — is never applied and never retried blindly:
    the typed stale outcome propagates to the caller's classification.
    """
    claimed = task_records.update_task_status(
        transaction_pool,
        task.id,
        expected_state_token=task.state_token,
        status=TaskStatus.PLANNING,
        capture_source_requirements_fingerprint=source_requirements_fingerprint,
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
) -> Task:
    """Durably enter the Task into the FIFO capacity queue.

    The conditional, token-rotating status write stamps ``queued_at`` from
    the database clock exactly once on entering ``queued``. The Owner Start
    path passes ``source_requirements_fingerprint`` so the freshly verified
    fingerprint is frozen by the SAME statement (issue #233): entering
    ``queued`` IS a started Task for authority and baseline purposes even
    though agents have not yet run, and the capture is conditioned on the
    still-NULL baseline so it commits — or rolls back — atomically with the
    queue entry. No reservation is made, no queue event exists, and the
    returned Task carries the entry timestamp the FIFO order uses.
    """
    queued = task_records.update_task_status(
        transaction_pool,
        task.id,
        expected_state_token=task.state_token,
        status=TaskStatus.QUEUED,
        capture_source_requirements_fingerprint=source_requirements_fingerprint,
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
            _claim_task_for_planning(transaction_pool, task=current, actor=_OPENORC_ACTOR)
            return QueuedCandidateOutcome(
                task_id=current.id,
                github_issue_number=current.github_issue_number,
                disposition=QueuedCandidateDisposition.ADMITTED,
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


def _scan_and_advance_queue(
    transaction_pool: DatabasePool,
    *,
    profile_id: UUID,
    workspace_id: UUID,
    target_task_id: UUID,
) -> tuple[bool, list[QueuedCandidateOutcome]]:
    """Advance admissible queued Tasks in durable FIFO order, bounded.

    Repeated bounded keyset pages over the Workspace's queued Tasks,
    oldest-first, arbitrating each candidate through
    :func:`_arbitrate_queued_candidate`. Returns ``(established,
    outcomes)``: ``established`` is true only when the scan reached a
    fixpoint — every candidate present at scan time was examined under the
    caller's serialization (so no admissible older candidate was skipped)
    within the fixed work budget. ``False`` means the budget bounded the
    scan: the caller must NOT claim the target on unexamined candidates and
    durably queues it instead — its entry timestamp is later than every
    examined candidate's, so FIFO order holds in every case.
    """
    outcomes: list[QueuedCandidateOutcome] = []
    cursor: tuple[datetime, UUID] | None = None
    examined = 0
    while True:
        page = task_records.list_queued_tasks(
            transaction_pool,
            workspace_id=workspace_id,
            after_queued_at=None if cursor is None else cursor[0],
            after_task_id=None if cursor is None else cursor[1],
            limit=_QUEUE_PAGE_LIMIT,
        )
        if not page:
            return True, outcomes
        for candidate in page:
            examined += 1
            if examined > _ARBITRATION_CANDIDATE_BUDGET:
                return False, outcomes
            if candidate.id == target_task_id:
                continue
            outcomes.append(
                _arbitrate_queued_candidate(
                    transaction_pool,
                    candidate=candidate,
                    profile_id=profile_id,
                    workspace_id=workspace_id,
                )
            )
        last = page[-1]
        if last.queued_at is None:  # pragma: no cover - queued rows carry the timestamp
            raise StaleOperationError(
                "the durable queue key disappeared during the scan; the decision is stale"
            )
        cursor = (last.queued_at, last.id)


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


def start_task_for_owner(
    pool: DatabasePool,
    github: GitHubAppClient,
    *,
    profile_id: UUID,
    workspace_id: UUID,
    task_id: UUID,
    expected_state_token: UUID,
) -> TaskStartOutcome:
    """Start a ``ready_to_plan`` Task under the authenticated Owner boundary.

    Three strictly ordered phases: the short authorization transaction (the
    account-operational barrier FIRST, exact Workspace/Task ownership, the
    subject load and startability precheck — closed before external I/O);
    the fresh authoritative GitHub revalidation with no database transaction
    open; and the one short arbitration transaction that claims the planning
    startup with both role reservations or durably queues the Task. Raises
    the typed authorization vocabulary uniformly for absent/foreign subjects
    and the typed non-start conditions above for ineligible sources and
    states; capacity waiting is a returned ``QUEUED`` outcome, never an
    error.
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
        # Phase C: the arbitration decision under the serialization boundary.
        return _arbitrate_target_start(
            pool,
            profile_id=profile_id,
            workspace_id=workspace_id,
            task_id=task_id,
            expected_state_token=expected_state_token,
            source_requirements_fingerprint=fresh_fingerprint,
        )


def _arbitrate_target_start(
    pool: DatabasePool,
    *,
    profile_id: UUID,
    workspace_id: UUID,
    task_id: UUID,
    expected_state_token: UUID,
    source_requirements_fingerprint: str,
) -> TaskStartOutcome:
    """One short arbitration: FIFO establishment pass, then the target's decision.

    The account-operational barrier is the FIRST lock acquisition; the
    per-Workspace advisory arbitration lock next — the same boundary every
    capacity-releasing lifecycle transition composes, so no occupancy
    change can commit between the establishment scan and the decision. The
    locked re-read binds the decision to the caller's exact
    ``state_token``; the durable eligibility guard (against the freshly
    verified fingerprint being frozen), the structural verdict, and the #68
    all-or-nothing admission decide against one consistent occupancy
    picture. A successful decision freezes
    ``source_requirements_fingerprint`` in the same guarded statement that
    claims ``planning`` or durably queues the Task (issue #233); a losing,
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
            # The fresh mirror Phase B just refreshed is the eligibility
            # authority for the target's claim too; the compared baseline is
            # the fingerprint this decision will freeze (issue #233).
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
            established, _ = _scan_and_advance_queue(
                transaction_pool,
                profile_id=profile_id,
                workspace_id=workspace_id,
                target_task_id=task_id,
            )
            if established:
                admission = admit_task_agent_sessions(
                    transaction_pool,
                    profile_id=profile_id,
                    workspace_id=workspace_id,
                    task_id=task_id,
                    roles=_REQUIRED_ROLES,
                )
                if admission.status is AdmissionStatus.ADMITTED:
                    claimed = _claim_task_for_planning(
                        transaction_pool,
                        task=task,
                        actor=event_coordination.owner_actor(profile_id),
                        source_requirements_fingerprint=source_requirements_fingerprint,
                    )
                    return TaskStartOutcome(
                        status=TaskStartStatus.PLANNING,
                        task=claimed,
                        sessions=admission.sessions,
                        queue_reason=None,
                    )
            # Temporary capacity contention — or bounded work that could not
            # establish no-bypass: the target durably queues with its own
            # entry timestamp, never rejected for having priority, and the
            # next bounded wakeup advances it in FIFO order.
            queued = _queue_task_for_capacity(
                transaction_pool,
                task=task,
                source_requirements_fingerprint=source_requirements_fingerprint,
            )
            return TaskStartOutcome(
                status=TaskStartStatus.QUEUED,
                task=queued,
                sessions=MappingProxyType({}),
                queue_reason=(
                    TaskStartQueueReason.AWAITING_CAPACITY
                    if established
                    else TaskStartQueueReason.ARBITRATION_DEFERRED
                ),
            )


def reevaluate_queued_tasks(pool: DatabasePool, *, workspace_id: UUID) -> QueuedSweepOutcome:
    """Re-evaluate the Workspace's queued Tasks after capacity may have freed.

    The system invocation over existing exact Task subjects (issue #76):
    never a new Owner authorization and never a change of the accountable
    Owner — each candidate's claim carries the OPENORC system actor and the
    accountable Profile is the Workspace's durable Owner. Bounded
    per-page arbitration units scan the durable ``(queued_at, id)`` FIFO
    order to exhaustion (or the candidate budget); every unit reads fresh
    occupancy under its own locks, so capacity released between units is
    respected, and each candidate's claim commits as its own atomic unit of
    work. Classified conditions are per-candidate outcomes — an ineligible,
    conflicted, or structurally infeasible candidate stays queued with its
    original timestamp and the condition is surfaced, not swallowed; an
    unclassified failure propagates and fails the sweep closed.
    """
    _require_uuid_command(workspace_id, "workspace_id")
    with application_span(_SERVICE_TRACER_SCOPE, _SWEEP_SPAN_NAME) as span:
        annotate_span(span, operation=_SWEEP_SPAN_NAME, workspace_id=str(workspace_id))
        outcomes: list[QueuedCandidateOutcome] = []
        cursor: tuple[datetime, UUID] | None = None
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
                    after_queued_at=None if cursor is None else cursor[0],
                    after_task_id=None if cursor is None else cursor[1],
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
                else:
                    last = page[-1]
                    if last.queued_at is None:  # pragma: no cover - queued rows carry it
                        raise StaleOperationError(
                            "the durable queue key disappeared during the sweep; the sweep is stale"
                        )
                    cursor = (last.queued_at, last.id)
                    continue
                break
        return QueuedSweepOutcome(outcomes=tuple(outcomes), exhausted=exhausted)
