"""Durable Hub-run receipt domain models (detached agent turns, issue #322).

The detached-run seam binds an already-authorized OpenOrc operation to the
native Hub run that executes it, then settles an independently observed
terminal result through the existing owning services. The receipt is the
durable claim-to-run evidence for exactly that correlation duty — never a
workflow decision, never a per-turn domain entity, and never a transcript
ledger.

- One receipt is committed atomically with the owning one-shot claim (the
  F2 runtime-send claim, the E6/E9 Execution reservation, or the F8g
  session-resume record) **before** any Hub enqueue. The receipt's
  ``original_attempt`` pair names that existing durable identity — the same
  identity vocabulary the Level 2 failure episodes root at — so the receipt
  adds no second claim identity.
- ``status`` is forward-only. ``claimed`` is the committed pre-enqueue
  receipt whose run identity is still unknown (the admission race window).
  ``admitted`` records the acknowledged native ``runId``. ``settled`` is the
  terminal record of one consumed terminal observation routed through the
  owning services. ``refused`` records a positively proven pre-effect
  admission refusal, so a never-dispatched run cannot hold the session's
  single-outstanding slot.
- The single-outstanding-run invariant — at most one unsettled
  OpenOrc-dispatched Hub run per exact authoritative ``TaskAgentSession``
  binding — is the correlation basis for v1 (the pinned SDK's terminal
  events carry no run identity). A durable partial unique index is its
  backstop; the external identifier is never the scope: different
  Connections own independent external-session namespaces, so resolution
  always happens within the authenticated Connection/Workspace scope
  against the binding row.
- ``outcome`` is a small canonical JSON object of safe machine facts only —
  the settlement/refusal label and the consumed Hub sequence marker when the
  observation carried one. Prompts, plan text, review content, reasoning,
  formal response bodies, credentials, and SDK payloads never enter a
  receipt.

The module also defines the runtime-neutral normalized observation values
the settlement seam consumes: a terminal Hub-run completion (kind, durable
sequence marker when present, and the formal candidate carried by the
native result — forwarded verbatim, never interpreted here) and a
status-only run observation from ``run.list`` (never success evidence).
This module carries transport-independent validation only.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, fields
from datetime import datetime
from enum import StrEnum
from typing import Final
from uuid import UUID

from openorc.domain.intents import canonical_intent_outcome
from openorc.domain.operational_failures import OriginalAttemptKind
from openorc.domain.pending_interactions import PendingTaskRuntimeInteractionOperation

__all__ = [
    "HubRunObservationKind",
    "ObservedHubRunCompletion",
    "ObservedHubRunStatus",
    "REFUSAL_OUTCOME_KIND_KEY",
    "SETTLEMENT_OBSERVED_FAILURE",
    "SETTLEMENT_OUTCOME_LABEL_KEY",
    "SETTLEMENT_OUTCOME_SEQUENCE_KEY",
    "SETTLEMENT_RESULT_NOT_APPLIED",
    "SETTLEMENT_VALIDATED_RESULT",
    "TaskHubRunEarlyCompletion",
    "TaskHubRunReceipt",
    "TaskHubRunReceiptStatus",
    "task_hub_run_early_completion_field_names",
    "task_hub_run_receipt_field_names",
]


class TaskHubRunReceiptStatus(StrEnum):
    """The forward-only status vocabulary of one Hub-run receipt.

    ``claimed`` is the only pre-admission status: the owning claim is
    committed, the enqueue has not been acknowledged, and the run identity
    is unknown. ``admitted`` carries the acknowledged ``runId`` and is the
    correlation-ready state. ``settled`` records one consumed terminal
    observation routed through the owning services. ``refused`` records a
    positively proven pre-effect admission refusal.
    """

    CLAIMED = "claimed"
    ADMITTED = "admitted"
    SETTLED = "settled"
    REFUSED = "refused"


# The safe machine labels of a receipt's terminal ``outcome`` payload. A
# settlement label records what happened to the consumed observation — the
# workflow consequences live in the owning services and never in the receipt.
SETTLEMENT_OUTCOME_LABEL_KEY: Final[str] = "settlement"
SETTLEMENT_OUTCOME_SEQUENCE_KEY: Final[str] = "consumed_sequence"
SETTLEMENT_VALIDATED_RESULT: Final[str] = "validated_result"
SETTLEMENT_OBSERVED_FAILURE: Final[str] = "observed_failure"
SETTLEMENT_RESULT_NOT_APPLIED: Final[str] = "result_not_applied"
REFUSAL_OUTCOME_KIND_KEY: Final[str] = "refusal_kind"


class HubRunObservationKind(StrEnum):
    """The closed vocabulary of terminal Hub-run observations the seam consumes.

    Exactly the two qualified terminal run events (issue #320): an accepted
    queued run can also fail without any terminal event at all, which is
    why status-only ``run.list`` reconciliation exists beside this
    vocabulary.
    """

    RUN_COMPLETED = "run.completed"
    RUN_FAILED = "run.failed"


# --- validation helpers -------------------------------------------------------


def _require_uuid(value: object, name: str) -> None:
    if not isinstance(value, UUID):
        raise ValueError(f"hub-run receipt {name} must be a UUID")


def _require_nonblank_str(value: object, name: str) -> None:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"hub-run receipt {name} must be a non-empty string")


def _require_instant(value: object, name: str, *, nullable: bool = False) -> None:
    if value is None and nullable:
        return
    if not isinstance(value, datetime) or value.tzinfo is None:
        raise ValueError(f"hub-run receipt {name} must be a timezone-aware datetime")


@dataclass(frozen=True, slots=True)
class ObservedHubRunCompletion:
    """One normalized terminal Hub-run observation, runtime-neutral.

    ``external_session_id`` is the observation's own external session
    identity (never synthesized), ``kind`` the terminal event kind,
    ``sequence`` the Hub's durable-log sequence when the envelope carried
    one, and ``result_candidate`` the formal candidate exactly as extracted
    from the native result — forwarded verbatim for the shared formal
    validation, never parsed or repaired here. A ``run.completed`` without a
    candidate is a legitimate observation that cannot settle a validated
    result.
    """

    external_session_id: str
    kind: HubRunObservationKind
    sequence: int | None
    result_candidate: str | None

    def __post_init__(self) -> None:
        if not isinstance(self.external_session_id, str) or not self.external_session_id.strip():
            raise ValueError(
                "ObservedHubRunCompletion.external_session_id must be a non-empty string"
            )
        if not isinstance(self.kind, HubRunObservationKind):
            raise ValueError("ObservedHubRunCompletion.kind must be a HubRunObservationKind")
        if self.sequence is not None and type(self.sequence) is not int:
            raise ValueError("ObservedHubRunCompletion.sequence must be None or an integer")
        if self.result_candidate is not None and (
            not isinstance(self.result_candidate, str) or not self.result_candidate.strip()
        ):
            raise ValueError(
                "ObservedHubRunCompletion.result_candidate must be None or a non-empty string"
            )


@dataclass(frozen=True, slots=True)
class ObservedHubRunStatus:
    """One status-only Hub-run record observation (``run.list``, issue #321).

    ``status`` is the Hub's own opaque status token, forwarded verbatim. A
    status observation is never success/result evidence: only the exact
    acknowledged ``runId`` reported ``failed`` classifies through the typed
    operation-failure path, and ``completed`` alone never settles a result.
    """

    run_id: str
    status: str

    def __post_init__(self) -> None:
        if not isinstance(self.run_id, str) or not self.run_id.strip():
            raise ValueError("ObservedHubRunStatus.run_id must be a non-empty string")
        if not isinstance(self.status, str) or not self.status.strip():
            raise ValueError("ObservedHubRunStatus.status must be a non-empty string")


@dataclass(frozen=True, slots=True)
class TaskHubRunReceipt:
    """The durable claim-to-run receipt of one authorized operation.

    The ``original_attempt`` pair names the existing durable claim/attempt
    identity the receipt binds (never a Task state token or a mutable
    phase). ``run_id`` is the exact opaque native run identifier the Hub
    acknowledged — execution correlation evidence, never OpenOrc workflow
    authority. A claimed receipt carries no run identity; an admitted or
    settled receipt carries exactly one; a refused receipt never ran.
    """

    id: UUID
    workspace_id: UUID
    task_id: UUID
    task_agent_session_id: UUID
    operation: PendingTaskRuntimeInteractionOperation
    original_attempt_kind: OriginalAttemptKind
    original_attempt_id: UUID
    expected_formal_family: str
    run_id: str | None
    status: TaskHubRunReceiptStatus
    outcome: Mapping[str, object]
    claimed_at: datetime
    admitted_at: datetime | None
    settled_at: datetime | None
    updated_at: datetime

    def __post_init__(self) -> None:
        for name in ("id", "workspace_id", "task_id", "task_agent_session_id"):
            _require_uuid(getattr(self, name), name)
        _require_uuid(self.original_attempt_id, "original_attempt_id")
        if not isinstance(self.operation, PendingTaskRuntimeInteractionOperation):
            raise ValueError(
                "TaskHubRunReceipt.operation must be a PendingTaskRuntimeInteractionOperation"
            )
        if not isinstance(self.original_attempt_kind, OriginalAttemptKind):
            raise ValueError(
                "TaskHubRunReceipt.original_attempt_kind must be an OriginalAttemptKind"
            )
        _require_nonblank_str(self.expected_formal_family, "expected_formal_family")
        if self.run_id is not None:
            _require_nonblank_str(self.run_id, "run_id")
        if not isinstance(self.status, TaskHubRunReceiptStatus):
            raise ValueError("TaskHubRunReceipt.status must be a TaskHubRunReceiptStatus")
        if not isinstance(self.outcome, Mapping):
            raise ValueError("TaskHubRunReceipt.outcome must be a mapping")
        object.__setattr__(self, "outcome", canonical_intent_outcome(self.outcome))
        _require_instant(self.claimed_at, "claimed_at")
        _require_instant(self.admitted_at, "admitted_at", nullable=True)
        _require_instant(self.settled_at, "settled_at", nullable=True)
        _require_instant(self.updated_at, "updated_at")
        self._require_status_coherence()

    def _require_status_coherence(self) -> None:
        """The exact forward-only lifecycle coherence per status."""
        if self.status is TaskHubRunReceiptStatus.CLAIMED:
            if (
                self.run_id is not None
                or self.admitted_at is not None
                or self.settled_at is not None
                or self.outcome
            ):
                raise ValueError(
                    "a claimed hub-run receipt carries no run identity, admission, "
                    "settlement, or outcome"
                )
        elif self.status is TaskHubRunReceiptStatus.ADMITTED:
            if (
                self.run_id is None
                or self.admitted_at is None
                or self.settled_at is not None
                or self.outcome
            ):
                raise ValueError(
                    "an admitted hub-run receipt carries its acknowledged runId and "
                    "admission instant and no settlement"
                )
        elif self.status is TaskHubRunReceiptStatus.SETTLED:
            label = self.outcome.get(SETTLEMENT_OUTCOME_LABEL_KEY)
            if (
                self.run_id is None
                or self.admitted_at is None
                or self.settled_at is None
                or not isinstance(label, str)
                or not label.strip()
            ):
                raise ValueError(
                    "a settled hub-run receipt carries its runId, both instants, and a "
                    "settlement label"
                )
        else:
            refusal_kind = self.outcome.get(REFUSAL_OUTCOME_KIND_KEY)
            if (
                self.run_id is not None
                or self.admitted_at is not None
                or self.settled_at is None
                or not isinstance(refusal_kind, str)
                or not refusal_kind.strip()
            ):
                raise ValueError(
                    "a refused hub-run receipt carries no run identity, its refusal "
                    "instant, and a refusal kind"
                )


@dataclass(frozen=True, slots=True)
class TaskHubRunEarlyCompletion:
    """One staged terminal observation awaiting its receipt's acknowledgement.

    The smallest safe handoff for the admission race: a terminal event that
    resolves to a receipt still ``claimed`` (acknowledgement not yet
    persisted) is staged on that exact receipt and consumed exactly once by
    the acknowledgement attach, which then routes it through the ordinary
    settlement. At most one staged completion exists per receipt; the
    candidate is the formal candidate exactly as received and is deleted at
    consumption — unvalidated agent-produced content never becomes permanent
    durable history.
    """

    id: UUID
    workspace_id: UUID
    task_id: UUID
    task_hub_run_receipt_id: UUID
    kind: HubRunObservationKind
    sequence: int | None
    result_candidate: str | None
    staged_at: datetime

    def __post_init__(self) -> None:
        for name in ("id", "workspace_id", "task_id", "task_hub_run_receipt_id"):
            _require_uuid(getattr(self, name), name)
        if not isinstance(self.kind, HubRunObservationKind):
            raise ValueError("TaskHubRunEarlyCompletion.kind must be a HubRunObservationKind")
        if self.sequence is not None and type(self.sequence) is not int:
            raise ValueError("TaskHubRunEarlyCompletion.sequence must be None or an integer")
        if self.result_candidate is not None and (
            not isinstance(self.result_candidate, str) or not self.result_candidate.strip()
        ):
            raise ValueError(
                "TaskHubRunEarlyCompletion.result_candidate must be None or a non-empty string"
            )
        _require_instant(self.staged_at, "staged_at")


def task_hub_run_receipt_field_names() -> frozenset[str]:
    """Return the exact field set a TaskHubRunReceipt exposes."""
    return frozenset(field.name for field in fields(TaskHubRunReceipt))


def task_hub_run_early_completion_field_names() -> frozenset[str]:
    """Return the exact field set a TaskHubRunEarlyCompletion exposes."""
    return frozenset(field.name for field in fields(TaskHubRunEarlyCompletion))
