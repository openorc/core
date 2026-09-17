"""Plan revision domain models.

A PlanRevision is one versioned, immutable Producer artifact: the exact plan
content authored for a Task attempt, together with the repository-base
context it was created and reviewed against (Phase 1, issue #23).

- Versioning is a per-Task ``revision_number`` sequence: a changed plan is a
  fresh revision, never a mutation of an existing one, and a Task's planning
  history remains complete and intact. Fresh-revision behavior is the only
  evolution path.
- ``content`` is the exact Producer artifact, preserved verbatim.
- ``repository_base_sha`` is audit/context metadata for the plan's
  creation/review context. Later movement of the repository base does not by
  itself invalidate a previously accepted/authorized revision and never
  authorizes rewriting one.
- Immutability is a persistence property: no update path exists for a
  committed revision. Revisions and results are not rewritten to manufacture
  a different past.
- The Task's ``current_plan_revision_id`` pointer identifies the
  authoritative current revision (same Task and Workspace, durably enforced
  by the composite foreign key); it never duplicates revision content or
  review outcomes on the Task row (one fact, one home).

This module carries transport-independent validation only. It performs no
workflow orchestration, no GitHub publishing, and no formal response
parsing.
"""

from __future__ import annotations

from dataclasses import dataclass, fields
from datetime import datetime
from uuid import UUID

__all__ = [
    "PlanRevision",
    "PlanRevisionDomainError",
    "plan_revision_field_names",
]


class PlanRevisionDomainError(Exception):
    """Raised when a PlanRevision domain invariant is violated."""


def _require_uuid(value: object, name: str) -> None:
    if not isinstance(value, UUID):
        raise PlanRevisionDomainError(f"PlanRevision.{name} must be a UUID")


@dataclass(frozen=True, slots=True)
class PlanRevision:
    """One versioned, immutable Producer plan artifact for one Task.

    ``revision_number`` versions the plan per Task: a changed plan is a
    fresh revision, never a mutation of an existing one. ``content`` is the
    exact plan text; ``repository_base_sha`` is the audit/context base the
    revision was created and reviewed against — later base movement neither
    invalidates an accepted/authorized revision nor authorizes rewriting
    one.
    """

    id: UUID
    workspace_id: UUID
    task_id: UUID
    revision_number: int
    content: str
    repository_base_sha: str
    created_at: datetime

    def __post_init__(self) -> None:
        _require_uuid(self.id, "id")
        _require_uuid(self.workspace_id, "workspace_id")
        _require_uuid(self.task_id, "task_id")
        if (
            isinstance(self.revision_number, bool)
            or not isinstance(self.revision_number, int)
            or self.revision_number <= 0
        ):
            raise PlanRevisionDomainError("PlanRevision.revision_number must be a positive integer")
        if not isinstance(self.content, str) or not self.content.strip():
            raise PlanRevisionDomainError("PlanRevision.content must be a non-empty string")
        if not isinstance(self.repository_base_sha, str) or not self.repository_base_sha.strip():
            raise PlanRevisionDomainError(
                "PlanRevision.repository_base_sha must be a non-empty string"
            )


def plan_revision_field_names() -> frozenset[str]:
    """Return the exact field set a PlanRevision exposes.

    Lets tests prove the revision carries only its own facts: the exact
    content and repository-base context of one version, without review
    outcomes (they live on ReviewIteration), without Task-level status, and
    without any mutable last-change timestamp (an immutable artifact has no
    update path).
    """
    return frozenset(field.name for field in fields(PlanRevision))
