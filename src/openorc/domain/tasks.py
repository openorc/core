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

from dataclasses import dataclass, fields
from datetime import datetime
from enum import StrEnum
from uuid import UUID

__all__ = [
    "TERMINAL_STATUSES",
    "Task",
    "TaskDomainError",
    "TaskStatus",
    "task_field_names",
]


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
    archived history without erasing the terminal outcome; and
    ``state_token`` is the opaque optimistic-concurrency token rotated by
    every authoritative state mutation. Current-object pointers identify
    related records without duplicating their content.
    """

    id: UUID
    workspace_id: UUID
    repository_id: UUID
    github_issue_id: int
    github_issue_number: int
    status: TaskStatus
    archived_at: datetime | None
    canonical_feature_branch: str | None
    state_token: UUID
    current_plan_revision_id: UUID | None
    current_owner_gate_id: UUID | None
    created_at: datetime
    updated_at: datetime

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
        if self.canonical_feature_branch is not None and (
            not isinstance(self.canonical_feature_branch, str)
            or not self.canonical_feature_branch.strip()
        ):
            raise TaskDomainError(
                "Task.canonical_feature_branch must be None or a non-empty string"
            )
        _require_uuid(self.state_token, "state_token", nullable=False)
        _require_uuid(self.current_plan_revision_id, "current_plan_revision_id", nullable=True)
        _require_uuid(self.current_owner_gate_id, "current_owner_gate_id", nullable=True)


def task_field_names() -> frozenset[str]:
    """Return the exact field set a Task exposes.

    Lets tests prove the Task aggregate carries only its own facts: current
    pointers without duplicated PlanRevision/OwnerGate semantic content or
    review outcomes, and no singular current-Execution pointer.
    """
    return frozenset(field.name for field in fields(Task))
