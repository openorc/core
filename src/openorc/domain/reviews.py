"""Review loop and review iteration domain models.

A ReviewLoop is one governed review loop for one Task: it owns the loop's
purpose, effective iteration limit, and current lifecycle. Its
ReviewIterations are the immutable review history: each iteration binds the
exact subject reviewed and, once finalized, carries the complete Reviewer
result (Phase 1, issue #23).

- v1 ReviewLoop purposes are exactly PLANNING and PR_REVIEW. No
  implementation-review loop and no speculative additional review-loop
  categories exist.
- ``iteration_limit`` is the effective limit used by the loop, preserved
  durably for historical reconstruction. The v1 default (5) is the
  configured-boundary constant below; Workspace-level configurability, when
  it arrives, supplies the value through that setting boundary rather than
  hard-coding review orchestration semantics into individual iterations.
- Loop lifecycle is minimal: OPEN (accepting iterations) or CLOSED (no more
  iterations). ``closed_at`` is the semantic closure timestamp, set exactly
  when the status is CLOSED. Closure policy and max-iteration transitions
  belong to later orchestration services; this module carries the durable
  facts only.
- ReviewIterations are immutable once a result is recorded. The complete
  finalized Reviewer result — ``outcome``, ``summary``, ``findings``,
  ``decided_at`` — finalizes atomically: an unfinalized iteration carries
  all four facts None; a finalized iteration carries all four set. No
  partially recorded result exists, and no rewrite path exists afterwards:
  revisions and results are not rewritten to manufacture a different past.
- Reviewer outcomes are exactly ACCEPTED and CHANGES_REQUESTED.
  Provider/runtime/protocol failures are not reviewer judgments and are
  structurally excluded from the outcome vocabulary.
- ``findings`` is the protocol-settled Reviewer-result findings document: a
  JSON array of finding objects, persisted verbatim as historical evidence.
  Phase 1 validates canonical JSON form only
  (:func:`canonical_review_findings`); per-item ``review_result`` protocol
  schema validation (finding objects with non-empty summary and details)
  belongs to Phase 2 protocol validation in ``openorc.protocol``, not here.
  The settled coherence holds: ACCEPTED clears with zero findings;
  CHANGES_REQUESTED retains at least one finding of unresolved work. A
  later REVIEW_RESOLUTION consumes the retained findings and iteration
  history. The ``review_result`` wire envelope (``type``,
  ``schema_version``) remains a protocol concern and is deliberately not
  stored on the iteration.
- Each iteration references the exact subject reviewed: v1 planning
  iterations bind a specific PlanRevision of the same Task. PR review
  support later binds one TaskPullRequest plus exact head SHA on the same
  review-history model.
- Iteration numbering is unique per ReviewLoop.

This module carries transport-independent validation only. It performs no
ReviewLoop orchestration, no Producer/Reviewer messaging, and no formal
response parsing.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, fields
from datetime import datetime
from enum import StrEnum
from math import isinf, isnan
from typing import Final
from uuid import UUID

__all__ = [
    "DEFAULT_REVIEW_LOOP_ITERATION_LIMIT",
    "ReviewIteration",
    "ReviewLoop",
    "ReviewLoopDomainError",
    "ReviewLoopPurpose",
    "ReviewLoopStatus",
    "ReviewOutcome",
    "canonical_review_findings",
    "review_iteration_field_names",
    "review_loop_field_names",
]


class ReviewLoopDomainError(Exception):
    """Raised when a ReviewLoop or ReviewIteration domain invariant is violated."""


class ReviewLoopPurpose(StrEnum):
    """v1 ReviewLoop purposes: exactly PLANNING and PR_REVIEW.

    No implementation-review loop and no speculative additional review-loop
    categories exist in v1.
    """

    PLANNING = "planning"
    PR_REVIEW = "pr_review"


class ReviewOutcome(StrEnum):
    """Reviewer outcomes: the only two reviewer judgments.

    Provider/runtime/protocol failures are not reviewer judgments and are
    structurally excluded from this vocabulary.
    """

    ACCEPTED = "accepted"
    CHANGES_REQUESTED = "changes_requested"


class ReviewLoopStatus(StrEnum):
    """Minimal ReviewLoop lifecycle: open or closed.

    OPEN accepts iterations; CLOSED does not. Closure policy and richer
    orchestration states belong to later workflow services; this is the
    durable lifecycle vocabulary only.
    """

    OPEN = "open"
    CLOSED = "closed"


# The v1 default review-loop iteration limit: the configured-boundary value.
# The effective limit a loop was created with is stored on the loop and
# preserved for historical reconstruction. Workspace-level configurability,
# when it arrives, supplies the value through the configured setting boundary
# rather than hard-coding review orchestration semantics into individual
# iterations.
DEFAULT_REVIEW_LOOP_ITERATION_LIMIT: Final[int] = 5


def canonical_review_findings(value: object) -> list[object]:
    """Canonicalize the protocol-settled Reviewer-result findings document.

    The settled ``review_result`` contract carries findings as a JSON array.
    Canonical JSON semantics, identical in discipline to the effective
    config snapshot: the top level must be a list or tuple and canonicalizes
    to a plain Python list; nested sequences normalize to lists (tuples
    never survive); nested mappings require string keys at every level;
    values must be JSON-representable; NaN and Infinity are rejected. The
    returned canonical form is exactly the shape jsonb stores and psycopg
    reads back, so a reload re-canonicalizes to an identical plain list.

    This validates canonical form only. Per-item ``review_result`` protocol
    schema validation (finding objects with non-empty summary and details)
    belongs to Phase 2 protocol validation in ``openorc.protocol``, not
    here.
    """
    if not isinstance(value, (list, tuple)):
        raise ReviewLoopDomainError(
            "ReviewIteration.findings must be a JSON array (a list of findings)"
        )
    return [_canonical_findings_value(item) for item in value]


def _canonical_findings_value(value: object) -> object:
    """Return the canonical JSON representation of one findings value."""
    if isinstance(value, Mapping):
        canonical: dict[str, object] = {}
        for key, item in value.items():
            if not isinstance(key, str):
                raise ReviewLoopDomainError(
                    "ReviewIteration.findings keys must be strings at every level "
                    "(canonical JSON object keys)"
                )
            canonical[key] = _canonical_findings_value(item)
        return canonical
    if isinstance(value, (list, tuple)):
        return [_canonical_findings_value(item) for item in value]
    if isinstance(value, (str, bool, int, float)) or value is None:
        if isinstance(value, float) and (isnan(value) or isinf(value)):
            raise ReviewLoopDomainError(
                "ReviewIteration.findings must be canonical JSON; NaN and Infinity are not JSON"
            )
        return value
    raise ReviewLoopDomainError(
        "ReviewIteration.findings values must be JSON-representable "
        f"(mapping, sequence, string, number, boolean, or null); got {type(value).__name__}"
    )


def _require_uuid(value: object, name: str) -> None:
    if not isinstance(value, UUID):
        raise ReviewLoopDomainError(f"ReviewLoop.{name} must be a UUID")


def _require_positive_int(value: object, name: str) -> None:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ReviewLoopDomainError(f"ReviewLoop.{name} must be a positive integer")


@dataclass(frozen=True, slots=True)
class ReviewLoop:
    """One governed review loop for one Task.

    Owns the loop's purpose, effective iteration limit, and current
    lifecycle. It does not own review outcomes: outcomes live on its
    ReviewIterations (one fact, one home). Closure policy and
    max-iteration transitions belong to later orchestration; this record is
    the durable lifecycle fact.
    """

    id: UUID
    workspace_id: UUID
    task_id: UUID
    purpose: ReviewLoopPurpose
    iteration_limit: int
    status: ReviewLoopStatus
    closed_at: datetime | None
    created_at: datetime

    def __post_init__(self) -> None:
        for name in ("id", "workspace_id", "task_id"):
            _require_uuid(getattr(self, name), name)
        if not isinstance(self.purpose, ReviewLoopPurpose):
            raise ReviewLoopDomainError("ReviewLoop.purpose must be a ReviewLoopPurpose")
        _require_positive_int(self.iteration_limit, "iteration_limit")
        if not isinstance(self.status, ReviewLoopStatus):
            raise ReviewLoopDomainError("ReviewLoop.status must be a ReviewLoopStatus")
        # ``closed_at`` is the semantic closure timestamp: set exactly when
        # the status is CLOSED, never substituted by another timestamp.
        if (self.closed_at is not None) != (self.status is ReviewLoopStatus.CLOSED):
            raise ReviewLoopDomainError(
                "ReviewLoop.closed_at must be set exactly when status is CLOSED"
            )


def review_loop_field_names() -> frozenset[str]:
    """Return the exact field set a ReviewLoop exposes.

    Lets tests prove the loop carries only its own facts: purpose, effective
    iteration limit, and minimal lifecycle — no review outcomes (they live
    on ReviewIteration), no orchestration policy, and no mutable
    last-change timestamp beyond the semantic ``closed_at``.
    """
    return frozenset(field.name for field in fields(ReviewLoop))


@dataclass(frozen=True, slots=True)
class ReviewIteration:
    """One immutable-once-finalized review iteration inside a ReviewLoop.

    Binds the exact subject reviewed (a PlanRevision of the same Task in
    v1; PR-review head binding arrives later on the same model) and, once
    finalized, carries the complete Reviewer result — outcome, summary,
    findings, decided_at — as one atomically recorded, never-rewritten fact
    set. Provider/runtime/protocol failures are not outcomes.
    """

    id: UUID
    workspace_id: UUID
    task_id: UUID
    review_loop_id: UUID
    iteration_number: int
    plan_revision_id: UUID
    outcome: ReviewOutcome | None
    summary: str | None
    findings: list[object] | None
    decided_at: datetime | None
    created_at: datetime

    def __post_init__(self) -> None:
        for name in ("id", "workspace_id", "task_id", "review_loop_id"):
            _require_uuid(getattr(self, name), name)
        # v1 subjects are planning revisions: the exact PlanRevision under
        # review is required. PR-review subject binding arrives later on the
        # same model.
        if not isinstance(self.plan_revision_id, UUID):
            raise ReviewLoopDomainError(
                "ReviewIteration.plan_revision_id must be a UUID (the exact subject reviewed)"
            )
        if (
            isinstance(self.iteration_number, bool)
            or not isinstance(self.iteration_number, int)
            or self.iteration_number <= 0
        ):
            raise ReviewLoopDomainError(
                "ReviewIteration.iteration_number must be a positive integer"
            )
        if self.outcome is not None and not isinstance(self.outcome, ReviewOutcome):
            raise ReviewLoopDomainError("ReviewIteration.outcome must be None or a ReviewOutcome")
        if self.summary is not None and (
            not isinstance(self.summary, str) or not self.summary.strip()
        ):
            raise ReviewLoopDomainError(
                "ReviewIteration.summary must be None or a non-empty string"
            )
        if self.findings is not None:
            # Canonical form validated in place: the canonical list is
            # exactly what jsonb stores and what a reload reads back.
            object.__setattr__(self, "findings", canonical_review_findings(self.findings))
        # Finalize coherence, mirrored by the database CHECK: the four
        # result facts move atomically. An unfinalized iteration carries all
        # four None; a finalized iteration carries all four set. No
        # partially recorded result exists.
        result_facts = (self.outcome, self.summary, self.findings, self.decided_at)
        any_fact_set = any(fact is not None for fact in result_facts)
        all_facts_set = all(fact is not None for fact in result_facts)
        if any_fact_set != all_facts_set:
            raise ReviewLoopDomainError(
                "a finalized ReviewIteration carries its complete result atomically: "
                "outcome, summary, findings, and decided_at are all None (unfinalized) "
                "or all set (finalized); partially recorded results do not exist"
            )
        # Settled Reviewer-result coherence: ACCEPTED clears with zero
        # findings (nothing unresolved); CHANGES_REQUESTED retains at least
        # one finding of unresolved work.
        if (
            self.outcome is ReviewOutcome.ACCEPTED
            and self.findings is not None
            and len(self.findings) != 0
        ):
            raise ReviewLoopDomainError(
                "an accepted review iteration clears with zero findings; "
                "acceptance with outstanding findings does not exist"
            )
        if (
            self.outcome is ReviewOutcome.CHANGES_REQUESTED
            and self.findings is not None
            and len(self.findings) == 0
        ):
            raise ReviewLoopDomainError(
                "a changes_requested review iteration retains at least one finding "
                "of unresolved work"
            )


def review_iteration_field_names() -> frozenset[str]:
    """Return the exact field set a ReviewIteration exposes.

    Lets tests prove the iteration carries only its own facts: the exact
    subject, its numbering, and the complete finalized result — no
    ``review_result`` wire-envelope fields (``type`` and ``schema_version``
    remain protocol concerns), no loop status duplication, and no mutable
    last-change timestamp beyond the semantic ``decided_at``.
    """
    return frozenset(field.name for field in fields(ReviewIteration))
