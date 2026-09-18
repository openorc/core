"""Execution domain models.

An Execution is one historical attempt record inside a Task's Producer
session (Phase 1, issue #24). Executions are attempt/history facts, not
orchestration state machines: they record that an attempt ran and how it
ended.

- Executions live inside the Task's Producer TaskAgentSession: the
  referenced binding must be the same Task/Workspace and carry the
  ``producer`` role. Reviewer sessions never anchor an Execution.
- Attempt ordering is the per-Task ``execution_number`` sequence. It is
  identity and history lookup only: it deliberately enforces no
  single-active-execution exclusivity, and parallel active Executions
  remain possible. There is deliberately no singular current-Execution
  pointer on the Task.
- A retry, recovery, or continuation is a fresh Execution row with the
  next per-Task number, retaining the same Producer session — never a
  rewrite of a finalized attempt. Finalized history is never rewritten to
  manufacture a different past.
- The lifecycle vocabulary is exactly QUEUED, RUNNING, PAUSED,
  PAUSED_FOR_APPROVAL, SUCCEEDED, FAILED_TRANSIENT, FAILED_FINAL, and
  CANCELLED. ``SUCCEEDED``, ``FAILED_TRANSIENT``, ``FAILED_FINAL``, and
  ``CANCELLED`` are final: once an Execution reaches a final status it is
  absorbing history. Which transitions are legal between nonfinal
  statuses, and when retries happen, belong to later orchestration; this
  module carries the durable facts only.
- Executions carry no Git SHA field of any kind. Cline's sandbox/
  worktree/clone is runtime-private execution state; local Git HEAD is not
  OpenOrc's canonical engineering truth. GitHub owns durable committed
  repository truth once the Producer pushes the Task branch, and OpenOrc
  later reconciles authoritative branch/head state through GitHub. The
  canonical feature branch remains a Task-level exclusive ownership fact:
  an Execution may observe repository state but never becomes a competing
  branch owner.

This module carries transport-independent validation only. It performs no
runtime dispatch, no workflow orchestration, and no Task state transitions.
"""

from __future__ import annotations

from dataclasses import dataclass, fields
from datetime import datetime
from enum import StrEnum
from uuid import UUID

__all__ = [
    "FINAL_EXECUTION_STATUSES",
    "Execution",
    "ExecutionDomainError",
    "ExecutionStatus",
    "execution_field_names",
]


class ExecutionDomainError(Exception):
    """Raised when an Execution domain invariant is violated."""


class ExecutionStatus(StrEnum):
    """The settled v1 Execution lifecycle vocabulary."""

    QUEUED = "queued"
    RUNNING = "running"
    PAUSED = "paused"
    PAUSED_FOR_APPROVAL = "paused_for_approval"
    SUCCEEDED = "succeeded"
    FAILED_TRANSIENT = "failed_transient"
    FAILED_FINAL = "failed_final"
    CANCELLED = "cancelled"


FINAL_EXECUTION_STATUSES: frozenset[ExecutionStatus] = frozenset(
    {
        ExecutionStatus.SUCCEEDED,
        ExecutionStatus.FAILED_TRANSIENT,
        ExecutionStatus.FAILED_FINAL,
        ExecutionStatus.CANCELLED,
    }
)


def _require_uuid(value: object, name: str) -> None:
    if not isinstance(value, UUID):
        raise ExecutionDomainError(f"Execution.{name} must be a UUID")


@dataclass(frozen=True, slots=True)
class Execution:
    """One historical attempt record inside a Task's Producer session.

    ``execution_number`` orders the Task's attempts (identity, never
    exclusivity); ``status`` is the settled lifecycle vocabulary. A
    finalized Execution is immutable history — a new attempt is a new row.
    """

    id: UUID
    workspace_id: UUID
    task_id: UUID
    producer_session_id: UUID
    execution_number: int
    status: ExecutionStatus
    created_at: datetime
    updated_at: datetime

    def __post_init__(self) -> None:
        _require_uuid(self.id, "id")
        _require_uuid(self.workspace_id, "workspace_id")
        _require_uuid(self.task_id, "task_id")
        _require_uuid(self.producer_session_id, "producer_session_id")
        if (
            isinstance(self.execution_number, bool)
            or not isinstance(self.execution_number, int)
            or self.execution_number <= 0
        ):
            raise ExecutionDomainError("Execution.execution_number must be a positive integer")
        if not isinstance(self.status, ExecutionStatus):
            raise ExecutionDomainError(
                "Execution.status must be an ExecutionStatus (queued, running, "
                "paused, paused_for_approval, succeeded, failed_transient, "
                "failed_final, or cancelled)"
            )

    @property
    def is_final(self) -> bool:
        """Whether this Execution has reached a final, immutable outcome."""
        return self.status in FINAL_EXECUTION_STATUSES


def execution_field_names() -> frozenset[str]:
    """Return the exact field set an Execution exposes.

    Lets tests prove the attempt record carries only its own facts: no Git
    SHA observation, no branch ownership, and no current-Execution pointer
    concept exists.
    """
    return frozenset(field.name for field in fields(Execution))
