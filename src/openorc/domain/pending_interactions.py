"""Runtime-neutral pending-interaction projection models (F2, issue #87).

The read-only projection of one Task's current outstanding consequential
automated runtime interactions — the exact facts the stall-observation layer
(F2a, issue #238) consumes without any runtime-status probe. The projection
decides nothing and normalizes nothing: activity observation and
``WORKING``/``NOT_WORKING``/``UNKNOWN`` classification are F2a's separate
adapter-capability work, and block/recovery decisions are F4's.

- ``operation`` is the closed vocabulary of outstanding interaction kinds
  over the existing claim identities (the E3a reserved unfinalized
  ReviewIteration, the E6/E9 RUNNING Execution claim, the E1b bootstrap
  claim plus its stage evidence) and the F2 runtime-send claims. It is not
  a workflow-phase model: the projection enumerates unfinished work only.
- ``subject`` is a small canonical JSON mapping of safe stable identifiers
  (claim/iteration/loop/gate/execution ids, exact head SHAs, iteration
  numbers). No prompts, plan text, review content, or outcome payloads.
- ``started_at`` is the committed start instant of the outstanding
  operation. ``None`` is the honest unknown: residue claimed before the
  start-instant evidence existed carries no fabricated instant.
- ``formal_result_outstanding`` is implicit: the projection lists only
  operations whose formal result is still outstanding; a listed row is
  exactly one unfinished interaction on one eligible active Task.

This module carries transport-independent validation only.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from uuid import UUID

from openorc.domain.connections import WorkflowRole
from openorc.domain.intents import canonical_intent_outcome

__all__ = [
    "PendingTaskRuntimeInteraction",
    "PendingTaskRuntimeInteractionOperation",
]


class PendingTaskRuntimeInteractionOperation(StrEnum):
    """The closed vocabulary of outstanding runtime interactions.

    Exactly the automated workflow's claimed consequential interactions that
    can still be unfinished on an eligible active Task. The three
    ``*_dispatch`` operations mirror the F2 runtime-send claim kinds; the
    others enumerate the existing durable claim identities.
    """

    PLANNING_BOOTSTRAP_PREPARE = "planning_bootstrap_prepare"
    INITIAL_PLAN_DISPATCH = "initial_plan_dispatch"
    PLANNING_REVISE_DISPATCH = "planning_revise_dispatch"
    REVIEWER_REVIEW = "reviewer_review"
    IMPLEMENTATION_EXECUTION = "implementation_execution"
    PR_REMEDIATION_EXECUTION = "pr_remediation_execution"
    PR_COMPOSE_DISPATCH = "pr_compose_dispatch"


@dataclass(frozen=True, slots=True)
class PendingTaskRuntimeInteraction:
    """One outstanding consequential automated runtime interaction.

    A read/service result row over durable claim facts only: which
    operation, bound to which Task/Workspace and role/session, addressing
    which exact stable subject, since which committed instant. It makes no
    runtime-status probe, decides no block, and never treats a superseded
    historical attempt as current work.
    """

    operation: PendingTaskRuntimeInteractionOperation
    workspace_id: UUID
    task_id: UUID
    role: WorkflowRole
    task_agent_session_id: UUID | None
    subject: Mapping[str, object]
    started_at: datetime | None

    def __post_init__(self) -> None:
        if not isinstance(self.operation, PendingTaskRuntimeInteractionOperation):
            raise ValueError(
                "PendingTaskRuntimeInteraction.operation must be a "
                "PendingTaskRuntimeInteractionOperation"
            )
        for name in ("workspace_id", "task_id"):
            value = getattr(self, name)
            if not isinstance(value, UUID):
                raise ValueError(f"PendingTaskRuntimeInteraction.{name} must be a UUID")
        session_id = self.task_agent_session_id
        if session_id is not None and not isinstance(session_id, UUID):
            raise ValueError(
                "PendingTaskRuntimeInteraction.task_agent_session_id must be None or a UUID"
            )
        if not isinstance(self.role, WorkflowRole):
            raise ValueError("PendingTaskRuntimeInteraction.role must be a WorkflowRole")
        if not isinstance(self.subject, Mapping):
            raise ValueError("PendingTaskRuntimeInteraction.subject must be a mapping")
        object.__setattr__(self, "subject", canonical_intent_outcome(self.subject))
        started_at = self.started_at
        if started_at is not None:
            if not isinstance(started_at, datetime):
                raise ValueError(
                    "PendingTaskRuntimeInteraction.started_at must be None or a datetime"
                )
            if started_at.tzinfo is None:
                raise ValueError("PendingTaskRuntimeInteraction.started_at must be timezone-aware")
