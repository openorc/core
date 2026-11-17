"""Durable detached-turn receipt domain models (detached agent turns).

The detached-turn seam binds an already-authorized OpenOrc operation to the
externally admitted runtime turn that executes it, then settles an
independently observed terminal result through the existing owning services.
The receipt is the durable claim-to-turn evidence for exactly that
correlation duty — never a workflow decision, never a per-turn domain
entity, and never a transcript ledger.

- One receipt is committed atomically with the owning one-shot claim (the
  F2 runtime-send claim, the E6/E9 Execution reservation, or the F8g
  session-resume record) **before** any runtime admission. The receipt's
  ``original_attempt`` pair names that existing durable identity — the same
  identity vocabulary the Level 2 failure episodes root at — so the receipt
  adds no second claim identity.
- ``status`` is forward-only. ``claimed`` is the committed pre-admission
  receipt whose turn reference is still unknown (the admission race window).
  ``admitted`` records the acknowledged opaque turn reference. ``settled``
  is the terminal record of one consumed terminal observation routed
  through the owning services. ``refused`` records a positively proven
  pre-effect admission refusal, so a never-dispatched turn cannot hold the
  session's single-outstanding slot.
- The single-outstanding-turn invariant — at most one unsettled
  OpenOrc-dispatched turn per exact authoritative ``TaskAgentSession``
  binding — is the correlation basis for v1. A durable partial unique index
  is its backstop; the external identifier is never the scope: different
  Connections own independent external-session namespaces, so resolution
  always happens within the authenticated Connection/Workspace scope
  against the binding row. Stale replays are rejected against the durable
  consumed ordering markers the settled receipts already record, and an
  explicitly observed contradictory turn identity sets the receipt's
  set-once ``contradicted_at`` marker, permanently blocking that receipt's
  settlement.
- ``outcome`` is a small canonical JSON object of safe machine facts only —
  the settlement/refusal label and the consumed ordering marker when the
  observation carried one. Prompts, plan text, review content, reasoning,
  formal response bodies, credentials, and runtime payloads never enter a
  receipt.

This module also defines the runtime-neutral evidence and attribution
vocabulary of the detached-turn boundary (issue #348). The concrete
runtime integration delivers opaque native evidence and classifies it —
native start-identity semantics, terminal ordering/replay, and status-only
record interpretation live behind the adapter boundary — while shared
application code consumes only these neutral verdicts and owns every
durable consequence. This module carries transport-independent validation
only: no runtime event names, payload keys, status tokens, or SDK types
appear here.
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
from openorc.domain.sessions import canonical_effective_config_snapshot

__all__ = [
    "DetachedTurnObservationKind",
    "DetachedTurnObservationSourceError",
    "NativeTurnEvidence",
    "ObservationLease",
    "ObservedTurnEvidence",
    "OWNER_REVIEWER_DISCUSSION_FAMILY",
    "REFUSAL_OUTCOME_KIND_KEY",
    "SETTLEMENT_DISCUSSION_EXCHANGE",
    "SETTLEMENT_DISCUSSION_FORMAL_CONTINUATION",
    "SETTLEMENT_OBSERVED_FAILURE",
    "SETTLEMENT_OUTCOME_LABEL_KEY",
    "SETTLEMENT_OUTCOME_SEQUENCE_KEY",
    "SETTLEMENT_RESULT_NOT_APPLIED",
    "SETTLEMENT_VALIDATED_RESULT",
    "TaskDetachedTurnEarlyCompletion",
    "TaskDetachedTurnReceipt",
    "TaskDetachedTurnReceiptStatus",
    "TurnAttribution",
    "TurnEvidenceFamily",
    "TurnFreshness",
    "TurnStatusEvidenceVerdict",
    "TurnTerminalClassification",
    "TurnTerminalOutcome",
    "task_detached_turn_early_completion_field_names",
    "task_detached_turn_receipt_field_names",
]


class TaskDetachedTurnReceiptStatus(StrEnum):
    """The forward-only status vocabulary of one detached-turn receipt.

    ``claimed`` is the only pre-admission status: the owning claim is
    committed, the admission has not been acknowledged, and the turn
    reference is unknown. ``admitted`` carries the acknowledged opaque turn
    reference and is the correlation-ready state. ``settled`` records one
    consumed terminal observation routed through the owning services.
    ``refused`` records a positively proven pre-effect admission refusal.
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
# The discussion family's own settlement labels (issue #339): the ordinary
# recorded exchange and the formal continuation dispatched through an owning
# E3c/E9 settlement. No conversation text ever enters the outcome payload.
SETTLEMENT_DISCUSSION_EXCHANGE: Final[str] = "discussion_exchange"
SETTLEMENT_DISCUSSION_FORMAL_CONTINUATION: Final[str] = "discussion_formal_continuation"
REFUSAL_OUTCOME_KIND_KEY: Final[str] = "refusal_kind"

# The dedicated NON-formal expected-family token of a detached Owner ↔
# Reviewer discussion receipt (issue #339). The discussion turn's completion
# is ordinary attributed text — never a formal response — so its receipt
# deliberately carries no canonical v1 response family. The token is pinned
# to exactly the ``owner_reviewer_discussion`` receipt operation and is not a
# member of the formal-response family vocabulary.
OWNER_REVIEWER_DISCUSSION_FAMILY: Final[str] = "owner_reviewer_discussion"


class DetachedTurnObservationKind(StrEnum):
    """The closed vocabulary of terminal detached-turn observations.

    Exactly the two qualified terminal outcomes an admitted turn can be
    observed to reach: a validated result delivery, or a genuine failure.
    The values are OpenOrc's own neutral encoding — the concrete runtime
    integration maps its native terminal evidence onto this vocabulary, and
    no runtime event name appears here. A runtime whose admitted turns can
    also fail without any terminal observation at all is handled by the
    status-only evidence classification beside this vocabulary.
    """

    COMPLETED = "completed"
    FAILED = "failed"


# --- validation helpers -------------------------------------------------------


def _require_uuid(value: object, name: str) -> None:
    if not isinstance(value, UUID):
        raise ValueError(f"detached-turn receipt {name} must be a UUID")


def _require_nonblank_str(value: object, name: str) -> None:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"detached-turn receipt {name} must be a non-empty string")


def _require_instant(value: object, name: str, *, nullable: bool = False) -> None:
    if value is None and nullable:
        return
    if not isinstance(value, datetime) or value.tzinfo is None:
        raise ValueError(f"detached-turn receipt {name} must be a timezone-aware datetime")


class TurnEvidenceFamily(StrEnum):
    """The neutral family of one delivered turn observation.

    The concrete integration recognizes which of its native observations
    speak about a turn's start or terminal outcome and delivers only those,
    each with its own opaque native evidence. Every other native event is
    not a turn observation and is never delivered.
    """

    TURN_START = "turn_start"
    TURN_TERMINAL = "turn_terminal"


class TurnAttribution(StrEnum):
    """The neutral attribution verdict of one observed turn start.

    ``ATTRIBUTED``: the runtime's own evidence positively attributes the
    observed start to the outstanding admitted turn reference.
    ``CONTRADICTED``: the runtime's evidence positively establishes a
    concretely different turn identity — the classification the shared seam
    records as the receipt's permanent contradiction marker.
    ``UNATTRIBUTABLE``: the evidence carries no identity claim to decide
    with; the correlation stays honestly unresolved instead of inventing a
    match.
    """

    ATTRIBUTED = "attributed"
    CONTRADICTED = "contradicted"
    UNATTRIBUTABLE = "unattributable"


class TurnFreshness(StrEnum):
    """The neutral ordering verdict of one terminal turn observation.

    ``FRESH``: the runtime's ordering evidence proves the observation is
    newer than every consumed marker provided for the exact session.
    ``STALE``: the runtime's ordering evidence proves the observation is an
    already-consumed or replayed notification — the classification the
    shared seam rejects durably.
    ``UNORDERED``: the observation carries no ordering evidence; whether it
    can settle is decided by the shared durable rules, never by an
    ordering guess.
    """

    FRESH = "fresh"
    STALE = "stale"
    UNORDERED = "unordered"


class TurnStatusEvidenceVerdict(StrEnum):
    """The neutral verdict of one bounded status-only evidence consultation.

    The concrete integration interprets its native status records for the
    outstanding admitted turn reference. ``FAILURE_EVIDENCE`` positively
    classifies a genuine failure of the referenced turn.
    ``COMPLETION_WITHOUT_RESULT_EVIDENCE`` records a completed turn whose
    status alone is never success or result evidence. Every other observed
    shape — a missing record, an ambiguous duplicate, or a state the
    integration does not classify — is the honest ``UNRESOLVED_EVIDENCE``;
    nothing is ever fabricated from absence.
    """

    FAILURE_EVIDENCE = "failure_evidence"
    COMPLETION_WITHOUT_RESULT_EVIDENCE = "completion_without_result_evidence"
    UNRESOLVED_EVIDENCE = "unresolved_evidence"


class ObservationLease:
    """Opaque handle of one established runtime session observation.

    The runtime integration returns one lease per established session
    observation and releases exactly that observation when the lease is
    handed back; shared application code never inspects it. A lease whose
    underlying attachment was dropped releases as a no-op.
    """

    __slots__ = ()


class DetachedTurnObservationSourceError(Exception):
    """A bounded, positively established failure of the observation source's
    own transport mechanics (an attachment or a subscription).

    The neutral contract error the concrete integration raises for its own
    bounded transport failures; the reason is a safe machine token from the
    closed vocabulary below and no native payload material ever appears in
    the message or the reason.
    """

    ATTACHMENT_FAILED = "attachment_failed"
    ATTACHMENT_UNAVAILABLE = "attachment_unavailable"
    SUBSCRIPTION_FAILED = "subscription_failed"

    def __init__(self, reason: str) -> None:
        super().__init__(f"the detached-turn observation source failed ({reason})")
        self.reason = reason

    def __str__(self) -> str:
        return self.args[0]


class NativeTurnEvidence:
    """Opaque carrier of one runtime-native turn-evidence item.

    The detached-turn boundary delivers and classifies runtime evidence
    through the concrete integration: shared application code type-checks
    this marker, routes it, and hands it back to the classifier that
    produced it, and never inspects, persists, logs, or interprets its
    contents. Concrete integrations subclass it with private native state
    and keep payloads out of representations, errors, and logs.
    """

    __slots__ = ()


@dataclass(frozen=True, slots=True)
class ObservedTurnEvidence:
    """One delivered runtime turn observation awaiting classification.

    ``external_session_id`` is the observation's own external session
    identity (never synthesized), ``family`` the neutral evidence family
    the integration recognized, and ``native_evidence`` the opaque carrier
    only the producing integration's classifier interprets. Shared code
    routes the envelope by family and authenticated scope; the native
    payload never crosses the integration boundary in readable form.
    """

    external_session_id: str
    family: TurnEvidenceFamily
    native_evidence: NativeTurnEvidence

    def __post_init__(self) -> None:
        if not isinstance(self.external_session_id, str) or not self.external_session_id.strip():
            raise ValueError("ObservedTurnEvidence.external_session_id must be a non-empty string")
        if not isinstance(self.family, TurnEvidenceFamily):
            raise ValueError("ObservedTurnEvidence.family must be a TurnEvidenceFamily")
        if not isinstance(self.native_evidence, NativeTurnEvidence):
            raise ValueError(
                "ObservedTurnEvidence.native_evidence must be a NativeTurnEvidence carrier"
            )


@dataclass(frozen=True, slots=True)
class TurnTerminalOutcome:
    """One classified terminal turn outcome, runtime-neutral.

    ``kind`` is the neutral terminal outcome vocabulary and
    ``result_candidate`` the formal candidate exactly as extracted by the
    integration from the native result — forwarded verbatim for the shared
    formal validation, never parsed or repaired. A completed turn without
    a candidate is a legitimate classification that cannot settle a
    validated result.
    """

    kind: DetachedTurnObservationKind
    result_candidate: str | None

    def __post_init__(self) -> None:
        if not isinstance(self.kind, DetachedTurnObservationKind):
            raise ValueError("TurnTerminalOutcome.kind must be a DetachedTurnObservationKind")
        if self.result_candidate is not None and (
            not isinstance(self.result_candidate, str) or not self.result_candidate.strip()
        ):
            raise ValueError(
                "TurnTerminalOutcome.result_candidate must be None or a non-empty string"
            )


@dataclass(frozen=True, slots=True)
class TurnTerminalClassification:
    """One adapter-produced terminal classification of delivered evidence.

    ``freshness`` is the runtime's ordering verdict against the consumed
    markers the seam provided, ``outcome`` the neutral terminal outcome,
    and ``ordering_marker`` the runtime's opaque durable ordering token to
    record with the receipt's settlement when the observation is consumed
    (``None`` when the runtime supplied no ordering evidence). The shared
    seam owns every durable consequence of the classification.
    """

    freshness: TurnFreshness
    outcome: TurnTerminalOutcome
    ordering_marker: int | None

    def __post_init__(self) -> None:
        if not isinstance(self.freshness, TurnFreshness):
            raise ValueError("TurnTerminalClassification.freshness must be a TurnFreshness")
        if not isinstance(self.outcome, TurnTerminalOutcome):
            raise ValueError("TurnTerminalClassification.outcome must be a TurnTerminalOutcome")
        if self.ordering_marker is not None and type(self.ordering_marker) is not int:
            raise ValueError(
                "TurnTerminalClassification.ordering_marker must be None or an integer"
            )


@dataclass(frozen=True, slots=True)
class TaskDetachedTurnReceipt:
    """The durable claim-to-turn receipt of one authorized operation.

    The ``original_attempt`` pair names the existing durable claim/attempt
    identity the receipt binds (never a Task state token or a mutable
    phase). ``admitted_turn_reference`` is the exact opaque externally admitted
    turn reference the runtime acknowledged — correlation evidence, never
    OpenOrc workflow authority. A claimed receipt carries no turn
    reference; an admitted or settled receipt carries exactly one; a
    refused receipt never ran.
    """

    id: UUID
    workspace_id: UUID
    task_id: UUID
    task_agent_session_id: UUID
    operation: PendingTaskRuntimeInteractionOperation
    original_attempt_kind: OriginalAttemptKind
    original_attempt_id: UUID
    expected_formal_family: str
    admitted_turn_reference: str | None
    status: TaskDetachedTurnReceiptStatus
    outcome: Mapping[str, object]
    contradicted_at: datetime | None
    claimed_at: datetime
    admitted_at: datetime | None
    settled_at: datetime | None
    updated_at: datetime
    # The detached session establishment's minimum separate provisional
    # establishment evidence (issue #324). The authoritative binding carries
    # no external identity while CONNECTING; the initialization receipt is
    # the separate durable fact. Only a ``session_initialization`` receipt
    # carries these; every other operation carries none of them.
    created_external_session_id: str | None = None
    effective_config_snapshot: Mapping[str, object] | None = None
    created_session_provenance: Mapping[str, object] | None = None
    # The set-once one-shot admission-attempt stamp (issue #339). A dispatch
    # whose claim and admission live in different invocations (the split
    # Owner-facing discussion prepare/admit seam) stamps the receipt exactly
    # once, strictly before its enqueue, so a duplicate or re-delivered
    # admission execution can never reach a second enqueue of the same
    # prepared message. The monolithic dispatches gate on their own claim
    # transactions and never stamp; a positively refused submission settles
    # the receipt ``refused`` without a stamp, while an attempted admission —
    # acknowledged, refused at the runtime, or uncertain — carries the stamp
    # truthfully.
    admission_attempted_at: datetime | None = None

    def __post_init__(self) -> None:
        for name in ("id", "workspace_id", "task_id", "task_agent_session_id"):
            _require_uuid(getattr(self, name), name)
        _require_uuid(self.original_attempt_id, "original_attempt_id")
        if not isinstance(self.operation, PendingTaskRuntimeInteractionOperation):
            raise ValueError(
                "TaskDetachedTurnReceipt.operation must be a PendingTaskRuntimeInteractionOperation"
            )
        if not isinstance(self.original_attempt_kind, OriginalAttemptKind):
            raise ValueError(
                "TaskDetachedTurnReceipt.original_attempt_kind must be an OriginalAttemptKind"
            )
        _require_nonblank_str(self.expected_formal_family, "expected_formal_family")
        if self.admitted_turn_reference is not None:
            _require_nonblank_str(self.admitted_turn_reference, "admitted_turn_reference")
        if not isinstance(self.status, TaskDetachedTurnReceiptStatus):
            raise ValueError(
                "TaskDetachedTurnReceipt.status must be a TaskDetachedTurnReceiptStatus"
            )
        if not isinstance(self.outcome, Mapping):
            raise ValueError("TaskDetachedTurnReceipt.outcome must be a mapping")
        object.__setattr__(self, "outcome", canonical_intent_outcome(self.outcome))
        # The provisional establishment evidence is coherent per operation
        # (mirroring the migration's conditional CHECKs): an initialization
        # receipt must carry the nonempty provisional identity and the
        # canonical non-secret snapshot, and every other operation carries
        # none of the three.
        if self.operation is PendingTaskRuntimeInteractionOperation.SESSION_INITIALIZATION:
            _require_nonblank_str(self.created_external_session_id, "created_external_session_id")
            if self.effective_config_snapshot is None:
                raise ValueError(
                    "a session_initialization receipt carries its dispatch-composed "
                    "effective configuration snapshot"
                )
            object.__setattr__(
                self,
                "effective_config_snapshot",
                canonical_effective_config_snapshot(self.effective_config_snapshot),
            )
            if self.created_session_provenance is not None:
                object.__setattr__(
                    self,
                    "created_session_provenance",
                    canonical_intent_outcome(self.created_session_provenance),
                )
        elif (
            self.created_external_session_id is not None
            or self.effective_config_snapshot is not None
            or self.created_session_provenance is not None
        ):
            raise ValueError(
                "only a session_initialization detached-turn receipt carries provisional "
                "establishment evidence"
            )
        _require_instant(self.contradicted_at, "contradicted_at", nullable=True)
        _require_instant(self.claimed_at, "claimed_at")
        _require_instant(self.admitted_at, "admitted_at", nullable=True)
        _require_instant(self.settled_at, "settled_at", nullable=True)
        _require_instant(self.updated_at, "updated_at")
        _require_instant(self.admission_attempted_at, "admission_attempted_at", nullable=True)
        self._require_status_coherence()

    def _require_status_coherence(self) -> None:
        """The exact forward-only lifecycle coherence per status."""
        # The explicit contradiction marker exists only on an outstanding
        # admitted receipt and permanently blocks its settlement.
        if (
            self.contradicted_at is not None
            and self.status is not TaskDetachedTurnReceiptStatus.ADMITTED
        ):
            raise ValueError(
                "only an admitted detached-turn receipt can carry the contradiction marker"
            )
        if self.status is TaskDetachedTurnReceiptStatus.CLAIMED:
            if (
                self.admitted_turn_reference is not None
                or self.admitted_at is not None
                or self.settled_at is not None
                or self.outcome
            ):
                raise ValueError(
                    "a claimed detached-turn receipt carries no turn reference, admission, "
                    "settlement, or outcome"
                )
        elif self.status is TaskDetachedTurnReceiptStatus.ADMITTED:
            if (
                self.admitted_turn_reference is None
                or self.admitted_at is None
                or self.settled_at is not None
                or self.outcome
            ):
                raise ValueError(
                    "an admitted detached-turn receipt carries its acknowledged turn reference and "
                    "admission instant and no settlement"
                )
        elif self.status is TaskDetachedTurnReceiptStatus.SETTLED:
            label = self.outcome.get(SETTLEMENT_OUTCOME_LABEL_KEY)
            if (
                self.admitted_turn_reference is None
                or self.admitted_at is None
                or self.settled_at is None
                or not isinstance(label, str)
                or not label.strip()
            ):
                raise ValueError(
                    "a settled detached-turn receipt carries its turn reference, "
                    "both instants, and a settlement label"
                )
        else:
            refusal_kind = self.outcome.get(REFUSAL_OUTCOME_KIND_KEY)
            if (
                self.admitted_turn_reference is not None
                or self.admitted_at is not None
                or self.settled_at is None
                or not isinstance(refusal_kind, str)
                or not refusal_kind.strip()
            ):
                raise ValueError(
                    "a refused detached-turn receipt carries no turn reference, its refusal "
                    "instant, and a refusal kind"
                )


@dataclass(frozen=True, slots=True)
class TaskDetachedTurnEarlyCompletion:
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
    task_detached_turn_receipt_id: UUID
    kind: DetachedTurnObservationKind
    ordering_marker: int | None
    result_candidate: str | None
    staged_at: datetime

    def __post_init__(self) -> None:
        for name in ("id", "workspace_id", "task_id", "task_detached_turn_receipt_id"):
            _require_uuid(getattr(self, name), name)
        if not isinstance(self.kind, DetachedTurnObservationKind):
            raise ValueError(
                "TaskDetachedTurnEarlyCompletion.kind must be a DetachedTurnObservationKind"
            )
        if self.ordering_marker is not None and type(self.ordering_marker) is not int:
            raise ValueError(
                "TaskDetachedTurnEarlyCompletion.ordering_marker must be None or an integer"
            )
        if self.result_candidate is not None and (
            not isinstance(self.result_candidate, str) or not self.result_candidate.strip()
        ):
            raise ValueError(
                "TaskDetachedTurnEarlyCompletion.result_candidate must be None or "
                "a non-empty string"
            )
        _require_instant(self.staged_at, "staged_at")


def task_detached_turn_receipt_field_names() -> frozenset[str]:
    """Return the exact field set a TaskDetachedTurnReceipt exposes."""
    return frozenset(field.name for field in fields(TaskDetachedTurnReceipt))


def task_detached_turn_early_completion_field_names() -> frozenset[str]:
    """Return the exact field set a TaskDetachedTurnEarlyCompletion exposes."""
    return frozenset(field.name for field in fields(TaskDetachedTurnEarlyCompletion))
