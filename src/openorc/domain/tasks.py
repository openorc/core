"""Task domain models.

A Task is one governed attempt to resolve one GitHub engineering issue
through OpenOrc orchestration. It is the durable aggregate root for
Task-scoped workflow state: sessions, plan revisions, review loops, gates,
executions, runtime requests, blocks, and pull requests all live inside a
Task's boundary.

Task identity and lifecycle semantics (Phase 1, issue #21):

- A Task belongs to one Workspace-scoped Repository and is backed by one
  stable GitHub issue identity (``github_issue_id``), which survives issue
  title/state changes. The repository-local issue number is observed address
  metadata and never the sole identity. Issue title/state are GitHub-owned
  presentation facts with no Phase 1 consumer; they are deliberately not
  stored on the Task.
- One GitHub issue has at most one current (non-archived) Task per
  Workspace Repository. Archival is represented independently from terminal
  outcome: ``archived_at`` records that an attempt is no longer current,
  while ``status`` preserves WHICH terminal outcome was reached. Both
  ``CANCELLED`` and ``COMPLETED`` attempts become archived history and
  remain distinguishable. A fresh Task can be created for an open issue
  after cancellation and for a previously completed issue if GitHub later
  reopens it.
- ``status`` is the canonical coarse primary workflow state. It never
  absorbs subordinate facts that have their own canonical homes: Owner gate
  type/subject belong on OwnerGate (v1 gate types:
  IMPLEMENTATION_AUTHORIZATION, PR_AUTHORIZATION, MERGE_DECISION,
  REVIEW_RESOLUTION), review state/outcomes belong on ReviewLoop and
  ReviewIteration, and blocking reasons/context belong on TaskBlock.
  ``ready_to_plan`` is an eligible leaf Task before autonomous work starts;
  ``queued`` is normal runtime-capacity backpressure, not failure;
  ``waiting_for_owner`` is the primary state shared by every Owner-facing
  gate wait; ``blocked`` is the primary state for blocking conditions. Only
  ``CANCELLED`` and ``COMPLETED`` are terminal outcomes.
- The canonical feature branch is a Task-level fact: once later
  workflow/runtime logic verifies and binds the Producer-created branch, the
  branch identity is owned by the Task aggregate, not by individual
  Executions. A current Task's canonical branch is exclusive within its
  Repository; retries, later Executions, and PR remediation for that Task
  continue on the same branch rather than creating competing branch
  ownership.
- ``state_token`` is the opaque optimistic-concurrency state token. Every
  authoritative Task-state mutation must replace it, and mutations are
  conditional on the caller's expected token: stale operations fail instead
  of mutating newer subjects. It is not history/revision numbering.
- ``current_plan_revision_id``/``current_owner_gate_id`` are nullable
  current-object pointers. They identify related records; they never
  duplicate those records' content or review outcomes (one fact, one home).
  There is deliberately no singular current Execution pointer.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, fields
from datetime import datetime
from decimal import Decimal
from enum import StrEnum
from uuid import UUID

__all__ = [
    "TERMINAL_STATUSES",
    "Task",
    "TaskDomainError",
    "TaskPlanningBootstrapEvidence",
    "TaskPlanningBootstrapStage",
    "TaskStatus",
    "TaskTerminalCause",
    "TaskTerminalizationOrigin",
    "task_field_names",
]

_FINGERPRINT_PATTERN = re.compile(r"^[0-9a-f]{64}$")


class TaskDomainError(Exception):
    """Raised when a Task domain invariant is violated."""


class TaskStatus(StrEnum):
    """Canonical coarse primary workflow state for a Task.

    Only ``CANCELLED`` and ``COMPLETED`` are terminal outcomes. The status
    never absorbs subordinate facts: gate detail lives on OwnerGate, review
    state/outcomes on ReviewLoop/ReviewIteration, blocking context on
    TaskBlock.
    """

    READY_TO_PLAN = "ready_to_plan"
    QUEUED = "queued"
    PLANNING = "planning"
    WAITING_FOR_OWNER = "waiting_for_owner"
    IMPLEMENTING = "implementing"
    REVIEWING = "reviewing"
    BLOCKED = "blocked"
    CANCELLED = "cancelled"
    COMPLETED = "completed"

    @property
    def is_terminal(self) -> bool:
        """Whether this status is a terminal Task outcome."""
        return self in TERMINAL_STATUSES


TERMINAL_STATUSES: frozenset[TaskStatus] = frozenset({TaskStatus.CANCELLED, TaskStatus.COMPLETED})


class TaskPlanningBootstrapStage(StrEnum):
    """The forward-only settlement stages of the claimed planning bootstrap.

    The minimal durable stage/settlement evidence (issue #87) beside the E1b
    one-shot ``planning_bootstrap_claim_token`` (issue #203): a claimed
    bootstrap whose stage is still ``None`` has an outstanding PREPARE/control
    (or an unresolved establishment); ``prepared`` records the validated
    ``workspace_result`` settlement of the PREPARE control; and
    ``ready_for_planning`` records the settled planning-context transition —
    a continuation waiting for the first PLAN dispatch. The stages are
    observational evidence, never authoritative workflow state: they never
    rotate ``state_token`` and never admit a second bootstrap.
    """

    PREPARED = "prepared"
    READY_FOR_PLANNING = "ready_for_planning"


# Statuses that may carry no frozen source requirements baseline (issue #233):
# a Task that has never successfully started. `cancelled` is included because
# an attempt can be cancelled directly from ready_to_plan without any start
# ever committing; a cancelled attempt that did start retains its frozen
# non-null baseline. Every other status requires a present frozen fingerprint.
_FINGERPRINT_OPTIONAL_STATUSES: frozenset[TaskStatus] = frozenset(
    {TaskStatus.READY_TO_PLAN, TaskStatus.CANCELLED}
)


class TaskTerminalCause(StrEnum):
    """The settled v1 terminal-cause vocabulary for archived Tasks.

    A terminal cause is the canonical, queryable reason an attempt reached
    its ``CANCELLED`` outcome when an external authority — not an OpenOrc
    Owner — established it (issue #229). It is deliberately separate from
    ``TaskBlockReason`` (``domain.blocks``): an active TaskBlock describes a
    recoverable blocking condition on a current Task, while a terminal cause
    describes immutable archived history. The vocabulary carries exactly the
    values its producers need — currently the single E10c value — and never
    speculative ones.
    """

    PR_CLOSED_UNMERGED = "pr_closed_unmerged"


class TaskTerminalizationOrigin(StrEnum):
    """The settled v1 terminalization-origin vocabulary (issue #317).

    The durable, minimal provenance echo of the authority that committed a
    Task's pre-archival terminalization: the exact authority whose existing
    shared composition decided the terminal outcome. It is the smallest fact
    the terminal finalizer cannot safely re-derive from the decision records
    alone (an Owner-attributed cancellation and a GitHub-confirmed
    closed-unmerged cancellation can leave indistinguishable durable PR facts
    at finalization time), and it selects the terminal event's truthful actor
    provenance and the archived terminal cause. It never replaces the decision
    records: they remain the accountable audit of actor, gate, ReviewLoop, and
    PR-close provenance, and no second source of terminal truth is created.
    """

    OWNER_CANCELLED = "owner_cancelled"
    GITHUB_CLOSED_UNMERGED = "github_closed_unmerged"
    GITHUB_CONFIRMED_MERGED = "github_confirmed_merged"


def _require_positive_int(value: object, name: str) -> None:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise TaskDomainError(f"Task.{name} must be a positive integer")


def _require_uuid(value: object, name: str, *, nullable: bool) -> None:
    if value is None and nullable:
        return
    if not isinstance(value, UUID):
        expectation = "None or a UUID" if nullable else "a UUID"
        raise TaskDomainError(f"Task.{name} must be {expectation}")


@dataclass(frozen=True, slots=True)
class Task:
    """One governed attempt to resolve one GitHub issue through OpenOrc.

    ``github_issue_id`` is the stable external identity; ``status`` is the
    coarse primary workflow state; ``archived_at`` separates current from
    archived history without erasing the terminal outcome; ``queued_at`` is
    the durable FIFO queue-entry timestamp carried exactly while the status
    is queued (issue #76); and ``state_token`` is the opaque
    optimistic-concurrency token rotated by every authoritative state
    mutation. Current-object pointers identify related records without
    duplicating their content. ``terminal_cause`` (issue #229) is the
    optional canonical terminal cause of an externally established
    cancellation — a Task-owned archived fact, never an active TaskBlock.
    """

    id: UUID
    workspace_id: UUID
    repository_id: UUID
    github_issue_id: int
    github_issue_number: int
    status: TaskStatus
    archived_at: datetime | None
    queued_at: datetime | None
    canonical_feature_branch: str | None
    state_token: UUID
    current_plan_revision_id: UUID | None
    current_owner_gate_id: UUID | None
    source_requirements_fingerprint: str | None
    created_at: datetime
    updated_at: datetime
    # The canonical terminal cause (issue #229): populated exactly once,
    # atomically with the guarded archival, when an external authority —
    # never an OpenOrc Owner — established the CANCELLED outcome. The default
    # keeps every existing construction site truthful: an Owner-attributed
    # cancellation carries no terminal cause (its provenance lives on the
    # terminal event), and a COMPLETED attempt never carries one.
    terminal_cause: TaskTerminalCause | None = None
    # The Owner-authorized USD Task budget (issue #240): captured exactly
    # once, atomically with the successful Owner Start transition out of
    # ``ready_to_plan``, from the then-current Workspace default. The default
    # keeps every existing construction site truthful; NULL is the ordinary
    # unstarted and pre-migration-started state (spending policy disabled —
    # never an implicit limit, never zero). Carried as an exact decimal,
    # never a binary float, and never rewritten by a later Workspace default
    # edit or queued wakeup.
    authorized_task_budget_usd: Decimal | None = None
    # The refreshed effective cumulative spending threshold (issue #291,
    # F8f): committed atomically with the Owner Continue's guarded COST_LIMIT
    # resolution as the latest retained known cumulative spend plus the
    # frozen original budget. The default keeps every existing construction
    # site truthful; NULL is the ordinary pre-Continue state whose
    # enforcement threshold is exactly the frozen authorized budget. Carried
    # as an exact decimal, never a binary float, and never rewritten by
    # anything but a later fully guarded Continue of the next episode.
    effective_cost_threshold_usd: Decimal | None = None
    # The exact-Task terminalization commitment (issue #317): the set-once,
    # pre-archival commitment every shared terminal authority commits
    # atomically with its existing decision composition. It is the explicit
    # authority/currentness barrier across ordinary Task interactions and
    # preserves the authorized terminal outcome across the bounded optional
    # cleanup interaction; the terminal finalizer archives exactly this
    # outcome with the truthful actor provenance and terminal cause. The
    # facts stay as truthful history after archival and are never cleared.
    terminalization_outcome: TaskStatus | None = None
    terminalization_origin: TaskTerminalizationOrigin | None = None
    terminalization_established_at: datetime | None = None
    # The minimal one-shot evidence of the optional Cline checkout-cleanup
    # send (issue #317): the claim is taken before the external call so a
    # concurrent or duplicate finalizer can never double-send, and the settle
    # stamps the bounded response-or-timeout window's conclusion. An
    # unsettled claim defers terminal archival to the in-flight attempt; a
    # silently dead worker leaves it unsettled — the accepted v1 limitation.
    cleanup_send_claimed_at: datetime | None = None
    cleanup_send_settled_at: datetime | None = None

    def __post_init__(self) -> None:
        _require_positive_int(self.github_issue_id, "github_issue_id")
        _require_positive_int(self.github_issue_number, "github_issue_number")
        if not isinstance(self.status, TaskStatus):
            raise TaskDomainError("Task.status must be a TaskStatus")
        # Settled lifecycle invariant, mirrored by the database CHECK:
        # archived_at IS NULL iff the status is nonterminal.
        if self.archived_at is not None:
            if not self.status.is_terminal:
                raise TaskDomainError(
                    "an archived Task must have reached a terminal outcome (cancelled or completed)"
                )
        elif self.status.is_terminal:
            raise TaskDomainError(
                "a terminal Task attempt (cancelled or completed) must be archived"
            )
        # The canonical terminal cause (issue #229): an archived CANCELLED
        # attempt may carry exactly one external-authority terminal cause; a
        # current attempt never does, and a COMPLETED attempt never does.
        if self.terminal_cause is not None:
            if not isinstance(self.terminal_cause, TaskTerminalCause):
                raise TaskDomainError("Task.terminal_cause must be None or a TaskTerminalCause")
            if self.archived_at is None or self.status is not TaskStatus.CANCELLED:
                raise TaskDomainError(
                    "a terminal cause belongs only to an archived cancelled attempt"
                )
        # The exact-Task terminalization commitment (issue #317): outcome,
        # origin, and establishment timestamp are present or absent together;
        # a committed outcome is always a terminal status; the one-shot
        # cleanup evidence never exists without a committed terminalization,
        # and a settled send always carries its claim. The commitment may
        # coexist with an archived row (it stays as truthful history) and is
        # never cleared.
        if (self.terminalization_outcome is None) != (self.terminalization_origin is None) or (
            self.terminalization_outcome is None
        ) != (self.terminalization_established_at is None):
            raise TaskDomainError(
                "Task.terminalization_outcome, terminalization_origin, and "
                "terminalization_established_at are present or absent together"
            )
        if self.terminalization_outcome is not None:
            if (
                not isinstance(self.terminalization_outcome, TaskStatus)
                or not self.terminalization_outcome.is_terminal
            ):
                raise TaskDomainError(
                    "Task.terminalization_outcome must be None or a terminal TaskStatus"
                )
            if not isinstance(self.terminalization_origin, TaskTerminalizationOrigin):
                raise TaskDomainError(
                    "Task.terminalization_origin must be None or a TaskTerminalizationOrigin"
                )
        if self.cleanup_send_claimed_at is not None and self.terminalization_outcome is None:
            raise TaskDomainError(
                "Task.cleanup_send_claimed_at requires a committed terminalization"
            )
        if self.cleanup_send_settled_at is not None and self.cleanup_send_claimed_at is None:
            raise TaskDomainError(
                "Task.cleanup_send_settled_at requires its one-shot cleanup send claim"
            )
        if self.canonical_feature_branch is not None and (
            not isinstance(self.canonical_feature_branch, str)
            or not self.canonical_feature_branch.strip()
        ):
            raise TaskDomainError(
                "Task.canonical_feature_branch must be None or a non-empty string"
            )
        # The durable FIFO queue-entry timestamp (issue #76): populated by the
        # database clock exactly once when the Task first enters QUEUED,
        # preserved unchanged for the lifetime of that queue wait, and cleared
        # on the successful QUEUED -> PLANNING claim and on any later
        # authorized path out of QUEUED. It is non-null iff the current status
        # is QUEUED — the domain invariant mirrored by the database CHECK.
        if self.queued_at is not None:
            if not isinstance(self.queued_at, datetime):
                raise TaskDomainError("Task.queued_at must be None or a datetime")
            if self.queued_at.tzinfo is None:
                raise TaskDomainError("Task.queued_at must be timezone-aware")
            if self.status is not TaskStatus.QUEUED:
                raise TaskDomainError("Task.queued_at is populated only while the status is queued")
        elif self.status is TaskStatus.QUEUED:
            raise TaskDomainError("a queued Task carries its durable queue-entry timestamp")
        _require_uuid(self.state_token, "state_token", nullable=False)
        _require_uuid(self.current_plan_revision_id, "current_plan_revision_id", nullable=True)
        _require_uuid(self.current_owner_gate_id, "current_owner_gate_id", nullable=True)
        # The immutable Task source baseline (issue #60, corrected by issue
        # #233): frozen by a successful Owner Start atomically with the
        # ready_to_plan -> queued/planning transition. The value is the Task's
        # last Owner-accepted comparison value: never rewritten by GitHub
        # requirements changes, and overwritten exactly once by the Owner's
        # guarded Ignore of an exactly-matched GITHUB_ISSUE_CHANGE gate
        # (issue #289), whose acknowledged observed value becomes the new
        # comparison value. Before Start the baseline does not exist: a
        # never-started Task carries None, and the only statuses that may
        # carry None are ready_to_plan and a cancelled attempt that never
        # started. Any present value is a lowercase hex SHA-256 digest. The
        # issue body/title itself is deliberately not duplicated here.
        if self.source_requirements_fingerprint is None:
            if self.status not in _FINGERPRINT_OPTIONAL_STATUSES:
                raise TaskDomainError(
                    "a started Task carries its frozen source requirements fingerprint"
                )
        elif not isinstance(
            self.source_requirements_fingerprint, str
        ) or not _FINGERPRINT_PATTERN.match(self.source_requirements_fingerprint):
            raise TaskDomainError(
                "Task.source_requirements_fingerprint must be None or a lowercase hex "
                "SHA-256 digest"
            )
        # The Owner-authorized spending authorization (issue #240), captured
        # atomically with the same guarded start transition that froze the
        # baseline: a never-started Task carries None, a started Task keeps
        # the authorization it started with (including a cancelled attempt's
        # immutable historical value), and a present value is a positive
        # finite exact decimal. The database CHECK mirrors the never-started
        # NULL invariant.
        if self.authorized_task_budget_usd is not None:
            if (
                isinstance(self.authorized_task_budget_usd, bool)
                or not isinstance(self.authorized_task_budget_usd, Decimal)
                or not self.authorized_task_budget_usd.is_finite()
                or self.authorized_task_budget_usd <= 0
            ):
                raise TaskDomainError(
                    "Task.authorized_task_budget_usd must be None or a positive "
                    "finite USD amount (an exact decimal; NULL disables spending policy)"
                )
            if self.status is TaskStatus.READY_TO_PLAN:
                raise TaskDomainError(
                    "a ready_to_plan Task carries no authorized budget yet: the "
                    "authorization is captured only by a successful Owner Start"
                )
        # The refreshed effective cumulative spending threshold (issue #291,
        # F8f): written only by the Owner Continue's guarded restore statement
        # as the latest retained known spend plus the frozen original budget.
        # A present value is a positive finite exact decimal and is coherent
        # only with a present authorized budget; a ready_to_plan Task carries
        # neither. The database CHECK mirrors the never-started NULL
        # invariant.
        if self.effective_cost_threshold_usd is not None:
            if (
                isinstance(self.effective_cost_threshold_usd, bool)
                or not isinstance(self.effective_cost_threshold_usd, Decimal)
                or not self.effective_cost_threshold_usd.is_finite()
                or self.effective_cost_threshold_usd <= 0
            ):
                raise TaskDomainError(
                    "Task.effective_cost_threshold_usd must be None or a positive "
                    "finite USD amount (an exact decimal)"
                )
            if self.authorized_task_budget_usd is None:
                raise TaskDomainError(
                    "an effective cost threshold is coherent only with the "
                    "Task's present authorized budget"
                )
            if self.status is TaskStatus.READY_TO_PLAN:
                raise TaskDomainError(
                    "a ready_to_plan Task carries no effective cost threshold: "
                    "the refresh is committed only by a guarded Owner Continue"
                )


def task_field_names() -> frozenset[str]:
    """Return the exact field set a Task exposes.

    Lets tests prove the Task aggregate carries only its own facts: current
    pointers without duplicated PlanRevision/OwnerGate semantic content or
    review outcomes, and no singular current-Execution pointer.
    """
    return frozenset(field.name for field in fields(Task))


@dataclass(frozen=True, slots=True)
class TaskPlanningBootstrapEvidence:
    """The Task's durable planning-bootstrap claim and stage evidence.

    The focused read of the E1b replay-safety fact (issue #203) plus the F2
    stage/settlement evidence (issue #87): the exactly-once claim token,
    its database-stamped claim instant (``None`` is honest residue claimed
    before the instant evidence existed), the forward-only settled stage,
    and that stage's settlement instant. Presence of the claim token is the
    replay-safety gate; the stages distinguish an outstanding
    PREPARE/control from completed preparation or a continuation waiting
    for the first PLAN dispatch. Representation only: classifying the
    facts into workflow outcomes is a service-layer concern.
    """

    claim_token: UUID
    claimed_at: datetime | None
    stage: TaskPlanningBootstrapStage | None
    stage_settled_at: datetime | None

    def __post_init__(self) -> None:
        if not isinstance(self.claim_token, UUID):
            raise TaskDomainError("claim_token must be a UUID")
        if self.claimed_at is not None:
            if not isinstance(self.claimed_at, datetime):
                raise TaskDomainError("claimed_at must be None or a datetime")
            if self.claimed_at.tzinfo is None:
                raise TaskDomainError("claimed_at must be timezone-aware")
        if self.stage is not None and not isinstance(self.stage, TaskPlanningBootstrapStage):
            raise TaskDomainError("stage must be None or a TaskPlanningBootstrapStage")
        if self.stage_settled_at is not None:
            if not isinstance(self.stage_settled_at, datetime):
                raise TaskDomainError("stage_settled_at must be None or a datetime")
            if self.stage_settled_at.tzinfo is None:
                raise TaskDomainError("stage_settled_at must be timezone-aware")
