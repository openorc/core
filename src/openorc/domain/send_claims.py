"""Durable runtime-send claim domain models (F2, issue #87).

The automated workflow's consequential interactions with a connected agent
runtime — the initial PLAN dispatch, the planning REVISE dispatch, and the
PR_COMPOSE dispatch — are each admitted by exactly one durable pre-send
claim of one exact logical workflow subject. These models are the durable
claim identity and its safe, forward-only outcome record. They carry
decision support only: workflow meaning (whether a send may proceed, what a
status means for the workflow) belongs to the application services;
persistence owns durable representation.

- The closed kind vocabulary is exactly the three consequential automated
  sends with a committed pre-send identity. Other claims already carry their
  own durable identities (the planning-bootstrap claim token, the reserved
  unfinalized ReviewIteration, the Execution claim) and are deliberately not
  represented here; free-form Owner↔Reviewer chat is outside the automated
  workflow and carries no claims at all.
- ``status`` is forward-only: ``claimed`` is the committed pre-send claim
  whose formal result is still outstanding (the conservatively
  potentially-dispatched record); ``settled`` records a validated formal
  result that completed the operation; ``known_nondelivery`` records an
  error positively proven to precede any effect. An unresolved claim is
  never replayed.
- ``outcome`` is a small canonical JSON object of safe machine facts only —
  stable internal identifiers and classified outcome labels. Prompts, plan
  text, review content, reasoning, formal response bodies, credentials, and
  runtime SDK data never enter it.
- The source subject is exactly one closed form per kind: none for the
  initial PLAN dispatch (one per Task/Producer session); the finalized
  planning ``CHANGES_REQUESTED`` ReviewIteration for the planning REVISE
  dispatch; and the approved ``PR_AUTHORIZATION`` OwnerGate plus its exact
  approved head SHA for the PR_COMPOSE dispatch. A ``PlanRevision`` is
  immutable produced output and is never a pre-send claim.

This module carries transport-independent validation only.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, fields
from datetime import datetime
from enum import StrEnum
from math import isinf, isnan
from types import MappingProxyType
from uuid import UUID

__all__ = [
    "TERMINAL_TASK_RUNTIME_SEND_CLAIM_STATUSES",
    "TaskRuntimeSendClaim",
    "TaskRuntimeSendClaimKind",
    "TaskRuntimeSendClaimStatus",
    "canonical_send_claim_outcome",
    "task_runtime_send_claim_field_names",
]


class TaskRuntimeSendClaimKind(StrEnum):
    """The closed vocabulary of claimed consequential automated sends.

    Exactly the three automated workflow sends whose pre-send identity is
    durably committed before any runtime effect (issue #87). Every other
    claimed interaction keeps its own existing durable identity.
    """

    INITIAL_PLAN_DISPATCH = "initial_plan_dispatch"
    PLANNING_REVISE_DISPATCH = "planning_revise_dispatch"
    PR_COMPOSE_DISPATCH = "pr_compose_dispatch"


class TaskRuntimeSendClaimStatus(StrEnum):
    """The forward-only status vocabulary of one runtime-send claim.

    ``claimed`` is the only nonterminal status: the exact subject was
    durably claimed before any runtime effect and its formal result is
    still outstanding. The terminal values are outcome records, written
    exactly once: ``settled`` (a validated formal result completed the
    operation) and ``known_nondelivery`` (an error positively proven to
    precede any effect). Uncertain outcomes stay ``claimed`` — never
    reclassified as either terminal form — and are never replayed.
    """

    CLAIMED = "claimed"
    SETTLED = "settled"
    KNOWN_NON_DELIVERY = "known_nondelivery"


TERMINAL_TASK_RUNTIME_SEND_CLAIM_STATUSES: frozenset[TaskRuntimeSendClaimStatus] = frozenset(
    {
        TaskRuntimeSendClaimStatus.SETTLED,
        TaskRuntimeSendClaimStatus.KNOWN_NON_DELIVERY,
    }
)


def _require_uuid(value: object, name: str, *, nullable: bool = False) -> None:
    if value is None:
        if nullable:
            return
        raise ValueError(f"send claim {name} must be a UUID")
    if not isinstance(value, UUID):
        raise ValueError(f"send claim {name} must be a UUID")


def _require_nonblank_str(value: object, name: str) -> None:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"send claim {name} must be a non-empty string")


def _canonical_json_value(value: object) -> object:
    """Return the canonical JSON representation of one outcome value."""
    if isinstance(value, Mapping):
        nested: dict[str, object] = {}
        for key, item in value.items():
            if not isinstance(key, str):
                raise ValueError("send claim outcome keys must be strings at every level")
            nested[key] = _canonical_json_value(item)
        return nested
    if isinstance(value, (list, tuple)):
        return [_canonical_json_value(item) for item in value]
    if isinstance(value, (str, bool, int, float)) or value is None:
        if isinstance(value, float) and (isnan(value) or isinf(value)):
            raise ValueError(
                "send claim outcome must be canonical JSON; NaN and Infinity are not JSON"
            )
        return value
    raise ValueError(
        "send claim outcome values must be JSON-representable "
        f"(mapping, sequence, string, number, boolean, or null); got {type(value).__name__}"
    )


def canonical_send_claim_outcome(value: object) -> Mapping[str, object]:
    """Canonicalize a send claim's safe outcome payload.

    Canonical JSON-object semantics mirroring the other domain JSON
    containers: the value must be a mapping with string keys at every
    level, and every value must be JSON-representable — nested mappings,
    sequences, and scalars only. Sequences normalize to lists (tuples do
    not silently survive), NaN/Infinity are rejected, and nothing relies on
    silent key coercion. The returned frozen view is exactly what jsonb
    stores and what a reload reads back.
    """
    if not isinstance(value, Mapping):
        raise ValueError("send claim outcome must be a mapping")
    canonical: dict[str, object] = {}
    for key, item in value.items():
        if not isinstance(key, str):
            raise ValueError("send claim outcome keys must be strings at every level")
        canonical[key] = _canonical_json_value(item)
    return MappingProxyType(canonical)


@dataclass(frozen=True, slots=True)
class TaskRuntimeSendClaim:
    """The one durable pre-send claim of one consequential automated send.

    Committed atomically before any runtime effect for the exact logical
    workflow subject, under the locked current Task. ``status`` is
    forward-only; ``outcome`` records the safe settlement facts once they
    exist. The claim is never rewritten and never replayed: a claimed
    subject with an outstanding formal result stays claimed through
    uncertainty, and only a validated formal result or a positively proven
    pre-effect error settles it.
    """

    id: UUID
    workspace_id: UUID
    task_id: UUID
    kind: TaskRuntimeSendClaimKind
    task_agent_session_id: UUID
    review_iteration_id: UUID | None
    owner_gate_id: UUID | None
    subject_head_sha: str | None
    status: TaskRuntimeSendClaimStatus
    outcome: Mapping[str, object]
    claimed_at: datetime
    settled_at: datetime | None
    updated_at: datetime

    def __post_init__(self) -> None:
        for name in ("id", "workspace_id", "task_id", "task_agent_session_id"):
            _require_uuid(getattr(self, name), name)
        if not isinstance(self.kind, TaskRuntimeSendClaimKind):
            raise ValueError("TaskRuntimeSendClaim.kind must be a TaskRuntimeSendClaimKind")
        if not isinstance(self.status, TaskRuntimeSendClaimStatus):
            raise ValueError("TaskRuntimeSendClaim.status must be a TaskRuntimeSendClaimStatus")
        _require_uuid(self.review_iteration_id, "review_iteration_id", nullable=True)
        _require_uuid(self.owner_gate_id, "owner_gate_id", nullable=True)
        if self.subject_head_sha is not None:
            _require_nonblank_str(self.subject_head_sha, "subject_head_sha")
        # The closed source-subject form: exactly one subject shape per kind.
        if self.kind is TaskRuntimeSendClaimKind.INITIAL_PLAN_DISPATCH:
            if (
                self.review_iteration_id is not None
                or self.owner_gate_id is not None
                or self.subject_head_sha is not None
            ):
                raise ValueError(
                    "an initial_plan_dispatch claim carries no source subject beyond "
                    "the Task/Producer session"
                )
        elif self.kind is TaskRuntimeSendClaimKind.PLANNING_REVISE_DISPATCH:
            if self.review_iteration_id is None or (
                self.owner_gate_id is not None or self.subject_head_sha is not None
            ):
                raise ValueError(
                    "a planning_revise_dispatch claim addresses exactly one finalized "
                    "planning ReviewIteration"
                )
        else:
            if (
                self.owner_gate_id is None
                or self.subject_head_sha is None
                or (self.review_iteration_id is not None)
            ):
                raise ValueError(
                    "a pr_compose_dispatch claim addresses exactly one approved "
                    "PR_AUTHORIZATION gate and its exact approved head SHA"
                )
        if not isinstance(self.outcome, Mapping):
            raise ValueError("TaskRuntimeSendClaim.outcome must be a mapping")
        object.__setattr__(self, "outcome", canonical_send_claim_outcome(self.outcome))
        # Terminal coherence: a terminal status and its settlement stamp are
        # written together (the database CHECK backstops the same invariant).
        if self.status in TERMINAL_TASK_RUNTIME_SEND_CLAIM_STATUSES:
            if self.settled_at is None:
                raise ValueError("a terminal send claim carries its settlement instant")
        elif self.settled_at is not None:
            raise ValueError("a claimed send claim carries no settlement instant")


def task_runtime_send_claim_field_names() -> frozenset[str]:
    """Return the exact field set a TaskRuntimeSendClaim exposes."""
    return frozenset(field.name for field in fields(TaskRuntimeSendClaim))
