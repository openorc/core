"""OwnerGate domain models.

An OwnerGate is one durable human-authority decision record for one Task
(Phase 1, issue #24). OpenOrc's human implementation authorization and
merge decisions are explicit authority boundaries: the gate records the
exact Owner decision bound to the exact subject it governs.

- v1 gate types are exactly IMPLEMENTATION_AUTHORIZATION,
  PR_AUTHORIZATION, MERGE_DECISION, REVIEW_RESOLUTION, and the three
  exceptional types BLOCK_RESOLUTION, GITHUB_ISSUE_CHANGE, and
  GITHUB_STATE_MISMATCH (issue #258). No speculative additional
  human-authority decision type exists.
- Statuses are exactly PENDING, APPROVED, REJECTED, CANCELLED, and
  SETTLED. A gate is created PENDING; APPROVED, REJECTED, CANCELLED, and
  SETTLED are terminal and immutable once stamped with ``decided_at``.
  SETTLED (issue #211) is the Reviewer-driven settlement outcome of a
  REVIEW_RESOLUTION gate: it records that a formal Reviewer judgment —
  never an Owner verdict — resolved the gate, and no other gate type may
  carry it (enforced by the database CHECK and the settlement service).
- OwnerGates are historical decision records. Resolved gates are immutable
  and never recycled: a later gate of the same type is a new row, never a
  rewrite of an old one. A pending gate that has lost currency (superseded
  or never installed) remains pending as history; authoritative resolution
  applies only to the Task's current gate.
- Exact subjects: an IMPLEMENTATION_AUTHORIZATION gate binds the exact
  review-cleared PlanRevision it authorizes; a PR_AUTHORIZATION gate binds
  only the exact latest committed Producer head SHA presented to the Owner
  (it happens before the canonical PR exists, so it carries no PR
  binding); a MERGE_DECISION gate binds the canonical TaskPullRequest plus
  the exact reviewed or Owner-overridden head SHA; a REVIEW_RESOLUTION
  gate binds exactly one subject — the exhausted planning PlanRevision, or
  the PR-review subject as TaskPullRequest plus exact head SHA. A
  PR-subject gate is never representable as a bare head SHA. Stale
  workflow-changing operations must never be applied to newer subjects or
  gates, so the exact binding is validated wherever the gate is used.
- Exceptional gates (issue #258) are the blocked-Task authority seam: each
  binds exactly one unresolved TaskBlock as its only subject —
  ``task_block_id`` is required and every other subject form is NULL. Their
  possible Owner actions are a fixed domain vocabulary, never a
  per-Workspace or database-configured registry and never an action
  engine: BLOCK_RESOLUTION may be retried, discussed, or cancel the Task;
  GITHUB_ISSUE_CHANGE may be ignored or cancel the Task;
  GITHUB_STATE_MISMATCH may be retried or cancel the Task. This module
  does not execute any action; the action services independently enforce
  their own identity, currentness, and effect safety (issue #254).
- REVIEW_RESOLUTION approval is an explicit Owner override of an unresolved
  Reviewer objection. It is a separate durable fact and never rewrites the
  historical Reviewer result to ACCEPTED; plan-subject approval leads to a
  new IMPLEMENTATION_AUTHORIZATION and PR/head-subject approval to a new
  MERGE_DECISION (orchestration concerns, not stored on the gate). A
  Reviewer-driven settlement instead stamps SETTLED with Reviewer
  provenance (issue #211): it is likewise a separate durable fact that
  never rewrites the historical result and never reads as an Owner
  approval.
- The Task's ``current_owner_gate_id`` pointer identifies the one gate the
  Task is currently waiting on. The normal lifecycle is: install one
  pending gate as current; resolve it while current (which clears the
  pointer and rotates the Task's ``state_token`` atomically); only then
  install a new pending gate. The pointer never duplicates gate type,
  subject, or outcome on the Task (one fact, one home). There is
  deliberately no singular current-Execution pointer.

This module carries transport-independent validation only. It performs no
Owner authorization, no workflow orchestration, and no Task state
transitions.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, fields
from datetime import datetime
from enum import StrEnum
from types import MappingProxyType
from uuid import UUID

__all__ = [
    "ExceptionalOwnerGateAction",
    "OwnerGate",
    "OwnerGateDomainError",
    "OwnerGateStatus",
    "OwnerGateType",
    "exceptional_owner_gate_actions",
    "owner_gate_field_names",
]


class OwnerGateDomainError(Exception):
    """Raised when an OwnerGate domain invariant is violated."""


class OwnerGateType(StrEnum):
    """The settled v1 OwnerGate type vocabulary.

    The four ordinary types carry the human-authority decisions of the
    autonomous workflow; the three exceptional types (issue #258) are the
    blocked-Task OwnerGate forms, each bound to exactly one unresolved
    TaskBlock subject.
    """

    IMPLEMENTATION_AUTHORIZATION = "implementation_authorization"
    PR_AUTHORIZATION = "pr_authorization"
    MERGE_DECISION = "merge_decision"
    REVIEW_RESOLUTION = "review_resolution"
    BLOCK_RESOLUTION = "block_resolution"
    GITHUB_ISSUE_CHANGE = "github_issue_change"
    GITHUB_STATE_MISMATCH = "github_state_mismatch"


class OwnerGateStatus(StrEnum):
    """The settled v1 OwnerGate status vocabulary.

    ``PENDING`` is the only nonterminal status; the others are terminal
    outcomes stamped with ``decided_at`` and immutable afterwards.
    ``SETTLED`` (issue #211) is the Reviewer-driven settlement outcome,
    valid only for review_resolution gates.
    """

    PENDING = "pending"
    APPROVED = "approved"
    REJECTED = "rejected"
    CANCELLED = "cancelled"
    SETTLED = "settled"


_TERMINAL_GATE_STATUSES: frozenset[OwnerGateStatus] = frozenset(
    {
        OwnerGateStatus.APPROVED,
        OwnerGateStatus.REJECTED,
        OwnerGateStatus.CANCELLED,
        OwnerGateStatus.SETTLED,
    }
)


class ExceptionalOwnerGateAction(StrEnum):
    """The fixed possible Owner actions for the exceptional gate types.

    Fixed in the domain (issue #49 §14.1): never a per-Workspace or
    database-configured action registry, and never an action-execution
    engine. The action services independently enforce identity,
    currentness, claims, and effect safety (issue #254).
    """

    RETRY = "retry"
    DISCUSS = "discuss"
    CANCEL_TASK = "cancel_task"
    IGNORE = "ignore"


_EXCEPTIONAL_GATE_ACTIONS: Mapping[OwnerGateType, tuple[ExceptionalOwnerGateAction, ...]] = (
    MappingProxyType(
        {
            OwnerGateType.BLOCK_RESOLUTION: (
                ExceptionalOwnerGateAction.RETRY,
                ExceptionalOwnerGateAction.DISCUSS,
                ExceptionalOwnerGateAction.CANCEL_TASK,
            ),
            OwnerGateType.GITHUB_ISSUE_CHANGE: (
                ExceptionalOwnerGateAction.IGNORE,
                ExceptionalOwnerGateAction.CANCEL_TASK,
            ),
            OwnerGateType.GITHUB_STATE_MISMATCH: (
                ExceptionalOwnerGateAction.RETRY,
                ExceptionalOwnerGateAction.CANCEL_TASK,
            ),
        }
    )
)


def exceptional_owner_gate_actions(
    gate_type: OwnerGateType,
) -> tuple[ExceptionalOwnerGateAction, ...]:
    """Return the fixed possible Owner actions for an exceptional gate type.

    Ordinary gate types carry no exceptional action vocabulary: their
    decisions are the settled APPROVED/REJECTED/CANCELLED/SETTLED outcomes.
    """
    if not isinstance(gate_type, OwnerGateType) or gate_type not in _EXCEPTIONAL_GATE_ACTIONS:
        raise OwnerGateDomainError(
            "exceptional_owner_gate_actions requires an exceptional "
            "OwnerGateType (block_resolution, github_issue_change, or "
            "github_state_mismatch)"
        )
    return _EXCEPTIONAL_GATE_ACTIONS[gate_type]


def _require_uuid(value: object, name: str, *, nullable: bool) -> None:
    if value is None and nullable:
        return
    if not isinstance(value, UUID):
        expectation = "None or a UUID" if nullable else "a UUID"
        raise OwnerGateDomainError(f"OwnerGate.{name} must be {expectation}")


@dataclass(frozen=True, slots=True)
class OwnerGate:
    """One durable human-authority decision record for one Task.

    ``gate_type`` selects the settled authority decision; the subject is
    carried exactly: ``plan_revision_id`` for plan-subject gates,
    ``subject_head_sha`` for pre-PR head-subject gates, and
    ``task_pull_request_id`` plus the exact ``subject_head_sha`` for the
    PR-subject forms (merge decision, PR-review resolution) — exactly one
    form per type, enforced in :meth:`__post_init__` to mirror the
    database CHECK. The three exceptional types (issue #258) instead bind
    exactly one unresolved TaskBlock: ``task_block_id`` is their only
    subject and every other subject form is NULL. ``status`` moves from
    PENDING to exactly one terminal outcome, stamped with ``decided_at``;
    there is no rewrite path afterwards.
    """

    id: UUID
    workspace_id: UUID
    task_id: UUID
    gate_type: OwnerGateType
    status: OwnerGateStatus
    plan_revision_id: UUID | None
    subject_head_sha: str | None
    task_pull_request_id: UUID | None
    task_block_id: UUID | None
    decided_at: datetime | None
    created_at: datetime

    def __post_init__(self) -> None:
        _require_uuid(self.id, "id", nullable=False)
        _require_uuid(self.workspace_id, "workspace_id", nullable=False)
        _require_uuid(self.task_id, "task_id", nullable=False)
        if not isinstance(self.gate_type, OwnerGateType):
            raise OwnerGateDomainError(
                "OwnerGate.gate_type must be an OwnerGateType "
                "(implementation_authorization, pr_authorization, "
                "merge_decision, review_resolution, block_resolution, "
                "github_issue_change, or github_state_mismatch)"
            )
        if not isinstance(self.status, OwnerGateStatus):
            raise OwnerGateDomainError(
                "OwnerGate.status must be an OwnerGateStatus "
                "(pending, approved, rejected, cancelled, or settled)"
            )
        _require_uuid(self.plan_revision_id, "plan_revision_id", nullable=True)
        _require_uuid(self.task_pull_request_id, "task_pull_request_id", nullable=True)
        _require_uuid(self.task_block_id, "task_block_id", nullable=True)
        if self.subject_head_sha is not None and (
            not isinstance(self.subject_head_sha, str) or not self.subject_head_sha.strip()
        ):
            raise OwnerGateDomainError(
                "OwnerGate.subject_head_sha must be None or a non-empty string"
            )
        # Exact-subject coherence per gate type, mirroring the database
        # CHECK: each gate binds exactly the authority subject it governs.
        # The three exceptional types (issue #258) bind exactly one
        # unresolved TaskBlock as their only subject. PR_AUTHORIZATION is
        # the pre-PR exact-head gate (no PR binding: it happens before the
        # canonical PR exists); MERGE_DECISION and the PR form of
        # REVIEW_RESOLUTION bind the canonical TaskPullRequest plus the
        # exact head SHA.
        if self.gate_type in _EXCEPTIONAL_GATE_ACTIONS:
            if (
                not isinstance(self.task_block_id, UUID)
                or self.plan_revision_id is not None
                or self.subject_head_sha is not None
                or self.task_pull_request_id is not None
            ):
                raise OwnerGateDomainError(
                    f"an {self.gate_type.value} gate binds exactly one "
                    "unresolved TaskBlock subject (task_block_id; no "
                    "PlanRevision, head SHA, or PR binding)"
                )
        elif self.gate_type is OwnerGateType.IMPLEMENTATION_AUTHORIZATION:
            if (
                self.plan_revision_id is None
                or self.subject_head_sha is not None
                or self.task_pull_request_id is not None
                or self.task_block_id is not None
            ):
                raise OwnerGateDomainError(
                    "an implementation_authorization gate binds exactly the "
                    "review-cleared PlanRevision subject (no head SHA, no PR "
                    "binding, no TaskBlock binding)"
                )
        elif self.gate_type is OwnerGateType.PR_AUTHORIZATION:
            if (
                self.plan_revision_id is not None
                or self.subject_head_sha is None
                or self.task_pull_request_id is not None
                or self.task_block_id is not None
            ):
                raise OwnerGateDomainError(
                    "a pr_authorization gate binds exactly the head-SHA subject "
                    "(no PlanRevision, no PR binding, no TaskBlock binding: it "
                    "happens before the canonical PR exists)"
                )
        elif self.gate_type is OwnerGateType.MERGE_DECISION:
            if (
                self.plan_revision_id is not None
                or self.subject_head_sha is None
                or not isinstance(self.task_pull_request_id, UUID)
                or self.task_block_id is not None
            ):
                raise OwnerGateDomainError(
                    "a merge_decision gate binds exactly the TaskPullRequest plus "
                    "the exact head SHA (no PlanRevision, no TaskBlock binding)"
                )
        else:  # REVIEW_RESOLUTION: exactly one subject form.
            if self.plan_revision_id is not None:
                # Planning-exhaustion form: the PlanRevision only.
                if (
                    self.subject_head_sha is not None
                    or self.task_pull_request_id is not None
                    or self.task_block_id is not None
                ):
                    raise OwnerGateDomainError(
                        "a review_resolution gate on the planning subject binds "
                        "exactly the exhausted PlanRevision: no head SHA, no PR "
                        "binding, and no TaskBlock binding"
                    )
            else:
                # PR-review-exhaustion form: TaskPullRequest + exact head SHA.
                if (
                    self.subject_head_sha is None
                    or not isinstance(self.task_pull_request_id, UUID)
                    or self.task_block_id is not None
                ):
                    raise OwnerGateDomainError(
                        "a review_resolution gate on the PR-review subject binds "
                        "exactly the TaskPullRequest plus the exact head SHA "
                        "(no TaskBlock binding)"
                    )
        # Resolution coherence, mirroring the database CHECK: the terminal
        # stamp is non-NULL exactly when the status is terminal.
        if self.status is OwnerGateStatus.PENDING:
            if self.decided_at is not None:
                raise OwnerGateDomainError("a pending OwnerGate carries no decided_at stamp")
        elif self.decided_at is None:
            raise OwnerGateDomainError(
                f"a {self.status.value} OwnerGate carries its decided_at stamp"
            )

    @property
    def is_pending(self) -> bool:
        """Whether this gate is still awaiting its authoritative outcome."""
        return self.status is OwnerGateStatus.PENDING

    @property
    def is_resolved(self) -> bool:
        """Whether this gate has reached a terminal, immutable outcome."""
        return self.status in _TERMINAL_GATE_STATUSES


def owner_gate_field_names() -> frozenset[str]:
    """Return the exact field set an OwnerGate exposes.

    Lets tests prove the gate carries only its own authority facts: the
    exact subject binding and the resolution outcome live here, never
    duplicated on the Task row.
    """
    return frozenset(field.name for field in fields(OwnerGate))
