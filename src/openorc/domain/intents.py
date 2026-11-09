"""One-shot external-effect intent domain models (E10b, issue #228).

The Owner Abort and unified Task Cancel commands each admit exactly one
external effect outside their authoritative database transaction — the
ClineBox activity Interrupt and the canonical pull-request closure request.
These models are the durable one-shot identities of those admitted effects
and their safe, inspectable outcomes. They carry decision support only:
workflow meaning (whether a command is applicable, what a status means for
the workflow) belongs to the application services; persistence owns durable
representation.

- A TaskInterruptIntent is registered once per exact admitted Task/role
  session, atomically with the Owner Abort's blocking composition. Its
  status is forward-only: ``unsettled`` is the registered-but-unaccounted
  state (the interruption's outcome is not yet durably known), and
  ``settled``/``failed``/``unknown`` are terminal outcome records. At most
  one unsettled intent may exist per session binding. Each intent carries
  the exact ``task_block_id`` of the blocking composition that registered
  it — the durable episode identity that distinguishes the current
  interruption from a historical settled intent for the same reused
  session binding (issue #299); historical rows predating the linkage
  carry ``None`` and never satisfy a current-episode guard.
- A TaskPullRequestCloseIntent is the cancellation's one-shot PR-close
  obligation, committed atomically with the terminal archival. Its status
  is forward-only: ``pending`` is the unresolved obligation,
  ``close_requested``/``no_action``/``failed``/``unknown`` are terminal
  outcome records. One canonical PR per Task means at most one close intent
  per Task.
- A TaskResumeIntent (F8g, issue #292) is the one-shot session-resume
  authorization of one Owner Resume decision: committed with the atomic
  resolution of the exact ``OWNER_ABORT`` gate and its TaskBlock, it is the
  durable pre-dispatch guard that makes the qualified same-session
  continuation exactly-once across crashes and concurrent commands. Its
  status is forward-only: ``authorized`` is the committed-but-never-
  dispatched authorization, ``dispatched`` is the claimed in-flight resumed
  turn, and ``settled``/``non_delivered``/``interrupted``/``superseded`` are
  terminal outcome records. At most one non-terminal record may exist per
  session binding. Its immutable ``routing`` mapping is the safe machine
  context established at authorization — the original workflow operation it
  continues, the expected formal response family, the exact durable subject
  reference, the restored Task status, and the authorization token.
- ``outcome`` is a small canonical JSON object of safe machine facts only:
  stable internal identifiers, role names, and classified outcome labels.
  Tokens, credentials, transcript content, prompts, and raw provider errors
  never enter it.

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

from openorc.domain.connections import WorkflowRole

__all__ = [
    "TERMINAL_TASK_INTERRUPT_INTENT_STATUSES",
    "TERMINAL_TASK_PULL_REQUEST_CLOSE_INTENT_STATUSES",
    "TERMINAL_TASK_RESUME_INTENT_STATUSES",
    "TaskInterruptIntent",
    "TaskInterruptIntentStatus",
    "TaskPullRequestCloseIntent",
    "TaskPullRequestCloseIntentStatus",
    "TaskResumeIntent",
    "TaskResumeIntentStatus",
    "canonical_intent_outcome",
    "task_interrupt_intent_field_names",
    "task_pull_request_close_intent_field_names",
    "task_resume_intent_field_names",
]


class TaskInterruptIntentStatus(StrEnum):
    """The forward-only status vocabulary of one activity-Interrupt intent.

    ``unsettled`` is the only nonterminal status: the intent was durably
    registered and its exact-session interruption outcome is not yet
    accounted for. The terminal values are outcome records, written exactly
    once: ``settled`` (positive settlement evidence), ``failed`` (a known
    non-delivery or definitive dispatch failure), and ``unknown`` (an
    uncertain outcome that must never be blindly replayed).
    """

    UNSETTLED = "unsettled"
    SETTLED = "settled"
    FAILED = "failed"
    UNKNOWN = "unknown"


TERMINAL_TASK_INTERRUPT_INTENT_STATUSES: frozenset[TaskInterruptIntentStatus] = frozenset(
    {
        TaskInterruptIntentStatus.SETTLED,
        TaskInterruptIntentStatus.FAILED,
        TaskInterruptIntentStatus.UNKNOWN,
    }
)


class TaskPullRequestCloseIntentStatus(StrEnum):
    """The forward-only status vocabulary of one PR-close obligation.

    ``pending`` is the only nonterminal status: the obligation is durably
    observable and unresolved. The terminal values are outcome records,
    written exactly once: ``close_requested`` (a delivered closure whose
    observed outcome is recorded in ``outcome``), ``no_action`` (an
    authoritative observation made no write necessary), ``failed`` (a known
    failure of the admitted close request), and ``unknown`` (an
    unconfirmable outcome that must never be blindly replayed).
    """

    PENDING = "pending"
    CLOSE_REQUESTED = "close_requested"
    NO_ACTION = "no_action"
    FAILED = "failed"
    UNKNOWN = "unknown"


TERMINAL_TASK_PULL_REQUEST_CLOSE_INTENT_STATUSES: frozenset[TaskPullRequestCloseIntentStatus] = (
    frozenset(
        {
            TaskPullRequestCloseIntentStatus.CLOSE_REQUESTED,
            TaskPullRequestCloseIntentStatus.NO_ACTION,
            TaskPullRequestCloseIntentStatus.FAILED,
            TaskPullRequestCloseIntentStatus.UNKNOWN,
        }
    )
)


def _require_uuid(value: object, name: str) -> None:
    if not isinstance(value, UUID):
        raise ValueError(f"intent {name} must be a UUID")


def _require_nonblank_str(value: object, name: str) -> None:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"intent {name} must be a non-empty string")


def _canonical_json_value(value: object) -> object:
    """Return the canonical JSON representation of one outcome value."""
    if isinstance(value, Mapping):
        nested: dict[str, object] = {}
        for key, item in value.items():
            if not isinstance(key, str):
                raise ValueError("intent outcome keys must be strings at every level")
            nested[key] = _canonical_json_value(item)
        return nested
    if isinstance(value, (list, tuple)):
        return [_canonical_json_value(item) for item in value]
    if isinstance(value, (str, bool, int, float)) or value is None:
        if isinstance(value, float) and (isnan(value) or isinf(value)):
            raise ValueError("intent outcome must be canonical JSON; NaN and Infinity are not JSON")
        return value
    raise ValueError(
        "intent outcome values must be JSON-representable "
        f"(mapping, sequence, string, number, boolean, or null); got {type(value).__name__}"
    )


def canonical_intent_outcome(value: object) -> Mapping[str, object]:
    """Canonicalize an intent's safe outcome payload.

    Canonical JSON-object semantics mirroring the other domain JSON
    containers: the value must be a mapping with string keys at every
    level, and every value must be JSON-representable — nested mappings,
    sequences, and scalars only. Sequences normalize to lists (tuples do
    not silently survive), NaN/Infinity are rejected, and nothing relies on
    silent key coercion. The returned frozen view is exactly what jsonb
    stores and what a reload reads back.
    """
    if not isinstance(value, Mapping):
        raise ValueError("intent outcome must be a mapping")
    canonical: dict[str, object] = {}
    for key, item in value.items():
        if not isinstance(key, str):
            raise ValueError("intent outcome keys must be strings at every level")
        canonical[key] = _canonical_json_value(item)
    return MappingProxyType(canonical)


@dataclass(frozen=True, slots=True)
class TaskInterruptIntent:
    """The one-shot exact-session Interrupt intent of one Owner Abort.

    The row is registered atomically with the Abort's blocking composition
    and stays the durable identity of the admitted interruption across
    crashes, duplicate commands, and new adapter instances. ``status`` is
    forward-only; ``outcome`` records the safe dispatch/settlement facts
    once they exist.
    """

    id: UUID
    workspace_id: UUID
    task_id: UUID
    task_agent_session_id: UUID
    # The exact blocking composition that registered this intent (issue
    # #299): the durable episode identity binding the interruption to its
    # originating TaskBlock. Historical rows predating the linkage carry
    # ``None`` and never satisfy a current-episode guard.
    task_block_id: UUID | None
    workflow_role: WorkflowRole
    external_session_id: str
    requested_by_profile_id: UUID
    status: TaskInterruptIntentStatus
    outcome: Mapping[str, object]
    created_at: datetime
    updated_at: datetime

    def __post_init__(self) -> None:
        for name in (
            "id",
            "workspace_id",
            "task_id",
            "task_agent_session_id",
            "requested_by_profile_id",
        ):
            _require_uuid(getattr(self, name), name)
        if self.task_block_id is not None:
            _require_uuid(self.task_block_id, "task_block_id")
        if not isinstance(self.workflow_role, WorkflowRole):
            raise ValueError("TaskInterruptIntent.workflow_role must be a WorkflowRole")
        _require_nonblank_str(self.external_session_id, "external_session_id")
        if not isinstance(self.status, TaskInterruptIntentStatus):
            raise ValueError("TaskInterruptIntent.status must be a TaskInterruptIntentStatus")
        if not isinstance(self.outcome, Mapping):
            raise ValueError("TaskInterruptIntent.outcome must be a mapping")
        object.__setattr__(self, "outcome", canonical_intent_outcome(self.outcome))


@dataclass(frozen=True, slots=True)
class TaskPullRequestCloseIntent:
    """The one-shot PR-close obligation of one Task cancellation.

    Committed atomically with the terminal archival whenever the canonical
    open/unmerged pull request may require closure.
    ``requested_by_profile_id`` is the accountable Owner Profile whose
    GitHub user authorization must perform the write; ``github_pr_id`` is
    the stable external identity the close is bound to. ``status`` is
    forward-only; archived terminal Task history is never rewritten by any
    outcome.
    """

    id: UUID
    workspace_id: UUID
    task_id: UUID
    task_pull_request_id: UUID
    github_pr_id: int
    github_pr_number: int
    requested_by_profile_id: UUID
    status: TaskPullRequestCloseIntentStatus
    outcome: Mapping[str, object]
    created_at: datetime
    updated_at: datetime

    def __post_init__(self) -> None:
        for name in (
            "id",
            "workspace_id",
            "task_id",
            "task_pull_request_id",
            "requested_by_profile_id",
        ):
            _require_uuid(getattr(self, name), name)
        if (
            isinstance(self.github_pr_id, bool)
            or not isinstance(self.github_pr_id, int)
            or (self.github_pr_id <= 0)
        ):
            raise ValueError("TaskPullRequestCloseIntent.github_pr_id must be a positive integer")
        if (
            isinstance(self.github_pr_number, bool)
            or not isinstance(self.github_pr_number, int)
            or self.github_pr_number <= 0
        ):
            raise ValueError(
                "TaskPullRequestCloseIntent.github_pr_number must be a positive integer"
            )
        if not isinstance(self.status, TaskPullRequestCloseIntentStatus):
            raise ValueError(
                "TaskPullRequestCloseIntent.status must be a TaskPullRequestCloseIntentStatus"
            )
        if not isinstance(self.outcome, Mapping):
            raise ValueError("TaskPullRequestCloseIntent.outcome must be a mapping")
        object.__setattr__(self, "outcome", canonical_intent_outcome(self.outcome))


def task_interrupt_intent_field_names() -> frozenset[str]:
    """Return the exact field set a TaskInterruptIntent exposes."""
    return frozenset(field.name for field in fields(TaskInterruptIntent))


def task_pull_request_close_intent_field_names() -> frozenset[str]:
    """Return the exact field set a TaskPullRequestCloseIntent exposes."""
    return frozenset(field.name for field in fields(TaskPullRequestCloseIntent))


class TaskResumeIntentStatus(StrEnum):
    """The forward-only status vocabulary of one session-resume authorization.

    ``authorized`` is the committed-but-never-dispatched authorization: the
    record exists, the durable pre-dispatch one-shot guard stands, and no
    runtime effect has been claimed. ``dispatched`` is the claimed in-flight
    resumed turn — the only state from which a runtime effect may have
    begun. The terminal values are outcome records written exactly once:
    ``settled`` (the resumed turn's result was durably processed by its
    original workflow owner), ``non_delivered`` (a positively proven
    pre-effect refusal — nothing reached the runtime), ``interrupted`` (the
    runtime positively terminated the resumed turn by interruption), and
    ``superseded`` (proven never-dispatched supersession: a later Owner
    decision re-blocked the Task before the dispatch claim committed).
    """

    AUTHORIZED = "authorized"
    DISPATCHED = "dispatched"
    SETTLED = "settled"
    NON_DELIVERED = "non_delivered"
    INTERRUPTED = "interrupted"
    SUPERSEDED = "superseded"


TERMINAL_TASK_RESUME_INTENT_STATUSES: frozenset[TaskResumeIntentStatus] = frozenset(
    {
        TaskResumeIntentStatus.SETTLED,
        TaskResumeIntentStatus.NON_DELIVERED,
        TaskResumeIntentStatus.INTERRUPTED,
        TaskResumeIntentStatus.SUPERSEDED,
    }
)


@dataclass(frozen=True, slots=True)
class TaskResumeIntent:
    """The one-shot session-resume authorization of one Owner Resume decision.

    Committed atomically with the resolution of the exact ``OWNER_ABORT``
    gate and its TaskBlock, the record is the durable pre-dispatch guard
    that makes the qualified same-session continuation exactly-once across
    crashes, concurrent commands, and worker delays. ``task_block_id`` is
    the resolved blocking episode the Resume continues — the lineage the
    cancellation accounting reads. ``routing`` is written once at
    authorization and never mutated; ``status`` is forward-only;
    ``outcome`` records the safe dispatch/supersession/settlement facts
    once they exist.
    """

    id: UUID
    workspace_id: UUID
    task_id: UUID
    task_agent_session_id: UUID
    task_block_id: UUID
    workflow_role: WorkflowRole
    external_session_id: str
    requested_by_profile_id: UUID
    status: TaskResumeIntentStatus
    routing: Mapping[str, object]
    outcome: Mapping[str, object]
    created_at: datetime
    updated_at: datetime

    def __post_init__(self) -> None:
        for name in (
            "id",
            "workspace_id",
            "task_id",
            "task_agent_session_id",
            "task_block_id",
            "requested_by_profile_id",
        ):
            _require_uuid(getattr(self, name), name)
        if not isinstance(self.workflow_role, WorkflowRole):
            raise ValueError("TaskResumeIntent.workflow_role must be a WorkflowRole")
        _require_nonblank_str(self.external_session_id, "external_session_id")
        if not isinstance(self.status, TaskResumeIntentStatus):
            raise ValueError("TaskResumeIntent.status must be a TaskResumeIntentStatus")
        if not isinstance(self.routing, Mapping):
            raise ValueError("TaskResumeIntent.routing must be a mapping")
        if not isinstance(self.outcome, Mapping):
            raise ValueError("TaskResumeIntent.outcome must be a mapping")
        object.__setattr__(self, "routing", canonical_intent_outcome(self.routing))
        object.__setattr__(self, "outcome", canonical_intent_outcome(self.outcome))


def task_resume_intent_field_names() -> frozenset[str]:
    """Return the exact field set a TaskResumeIntent exposes."""
    return frozenset(field.name for field in fields(TaskResumeIntent))
