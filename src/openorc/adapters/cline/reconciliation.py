"""Typed reconciliation results and the conservative classification policy
of the Cline adapter (issue #148, D6b).

After a local bridge/client/transport interruption, a Cline activity abort,
or a managed Hub restart once connectivity has been independently restored,
the caller-invoked D6b seam on
:class:`openorc.adapters.cline.adapter.ClineAdapter` reconciles the exact
bound Task/role session through the already-supported public backend reads
(``get``, ``read_messages``, and, where needed to disambiguate presence,
``list_history``; ``get_accumulated_usage`` as optional non-authoritative
support). This module defines the typed, content-free results of that
reconciliation and the pure classification over the observed evidence; the
adapter owns the reads, the binding resolution, and the concurrency.

Semantic distinctions (never collapsed into one another):

- ``ACCESSIBLE`` — the exact persisted record/transcript is demonstrably
  accessible on the attached runtime.
- ``RECOVERABLE`` — the live record/incarnation is non-addressable while
  the same exact persisted conversation is confirmed available, so the
  existing D5a same-ID reconstruction seam can restore it.
- ``LOST`` — the exact persisted context is positively and coherently
  established absent across independent public reads on a demonstrably
  healthy attachment, gated on a demonstrated exhaustive public-history
  contract (the pinned #71 backend contract makes no such guarantee, so
  the adapter never asserts it today).
- ``UNKNOWN`` — the evidence is inadequate or contradictory; continuity is
  unknown. This is the conservative default and never destroys the durable
  binding.

Non-inferences enforced by the classification policy: a ``get == None``, a
missing history entry, a runtime-operation ``session_not_found``, or an
unavailable incarnation on its own is never terminal loss; Cline record
``status``, ``metadata.mode``, event gaps, accumulated usage, and the SDK
client's local session registry are never authority; and message presence
beyond a caller-supplied pre-dispatch checkpoint is never proof that an
interrupted logical send completed — the delivery/currentness decision
belongs to the caller's durable dispatch identity, never to this
classification. The policy branches only on presence/absence and read
outcomes, never on record field values.

Secret safety: observations are bounded counts and booleans. Raw
transcripts stay transient inside the adapter's reconciliation call and
never enter these results, errors, logs, spans, or telemetry.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import StrEnum

from openorc.adapters.cline.values import JsonObject

__all__ = [
    "ClineDispatchCheckpoint",
    "ClineReconciliationResult",
    "ClineSessionContinuity",
    "ClineSessionEvidence",
    "ClineSettlementOutcome",
    "ClineSettlementRefusal",
    "classify_session_evidence",
]


class ClineSessionContinuity(StrEnum):
    """The conservative continuity classification of one bound session."""

    ACCESSIBLE = "accessible"
    RECOVERABLE = "recoverable"
    LOST = "lost"
    UNKNOWN = "unknown"


@dataclass(frozen=True)
class ClineDispatchCheckpoint:
    """The caller's already-known pre-dispatch transcript observation.

    The one bounded fact the caller supplies so a reconciliation after an
    interrupted send can derive clearly identified new-turn observations:
    the transcript length (message count) observed before the interrupted
    dispatch. It is an observation aid, never dispatch identity and never
    delivery evidence; message presence beyond it never proves that the
    interrupted logical send completed.
    """

    transcript_message_count: int

    def __post_init__(self) -> None:
        if (
            not isinstance(self.transcript_message_count, int)
            or isinstance(self.transcript_message_count, bool)
            or self.transcript_message_count < 0
        ):
            raise ValueError(
                "ClineDispatchCheckpoint.transcript_message_count must be a non-negative integer"
            )


@dataclass(frozen=True)
class ClineSessionEvidence:
    """The raw public-read evidence gathered for one reconciliation call.

    Internal adapter-side input of :func:`classify_session_evidence`; the
    raw ``transcript`` stays transient inside the adapter's reconciliation
    call and is never exported, logged, or persisted.

    - ``record_present`` — ``get`` answered with a record (True), positively
      answered absence (None result or session-not-found; False), or was
      never consultable (None).
    - ``transcript`` — the successful ``read_messages`` array (possibly
      empty), or None when the read failed or positively reported absence.
    - ``transcript_absent`` — the read positively reported session absence.
    - ``history_presence`` — True when a successful history enumeration
      positively identifies the exact session ID, False when it
      demonstrably carries session identity and the exact ID is absent,
      None when not consulted or inconclusive. A False answer observes
      that the enumeration is readable, never that it exhaustively
      enumerated every persisted session, so it is never terminal-loss
      proof on its own.
    - ``history_exhaustive`` — True only when the backend contract
      demonstrates that a successful history enumeration exhaustively
      covers every persisted session. The pinned #71 contract makes no
      such guarantee, so the adapter never asserts it today; the flag
      exists so a future documented contract can enable terminal-loss
      classification without changing this seam's shape.

    The raw ``transcript`` is excluded from the representation: it is
    transient conversation content and never enters reprs, errors, logs,
    or telemetry.
    """

    record_present: bool | None
    transcript: list[JsonObject] | None = field(repr=False)
    transcript_absent: bool
    history_presence: bool | None
    history_exhaustive: bool = False


@dataclass(frozen=True)
class ClineReconciliationResult:
    """The typed, content-free outcome of one session reconciliation.

    ``continuity`` is the conservative classification. The remaining fields
    are bounded observations: message counts (never content), whether the
    public record was observed, and whether accumulated usage was observed
    (usage may reset across Hub lifetimes and never establishes delivery,
    completion, or continuity). ``messages_beyond_checkpoint`` is present
    only when the caller supplied a :class:`ClineDispatchCheckpoint` and
    the transcript read succeeded non-contradictorily.
    """

    continuity: ClineSessionContinuity
    transcript_message_count: int | None = None
    messages_beyond_checkpoint: int | None = None
    record_observed: bool | None = None
    usage_observed: bool | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.continuity, ClineSessionContinuity):
            raise ValueError(
                "ClineReconciliationResult.continuity must be a ClineSessionContinuity"
            )
        for name in ("transcript_message_count", "messages_beyond_checkpoint"):
            value = getattr(self, name)
            if value is not None and (
                not isinstance(value, int) or isinstance(value, bool) or value < 0
            ):
                raise ValueError(
                    f"ClineReconciliationResult.{name} must be None or a non-negative integer"
                )
        for name in ("record_observed", "usage_observed"):
            value = getattr(self, name)
            if value is not None and not isinstance(value, bool):
                raise ValueError(f"ClineReconciliationResult.{name} must be None or a boolean")


class ClineSettlementRefusal(StrEnum):
    """Safe machine reasons a settlement attempt refused to clear a guard."""

    OUTSTANDING_ABORT = "outstanding_abort"
    UNRESOLVED_ABORT = "unresolved_abort"
    RECONSTRUCTION_ACTIVE = "reconstruction_active"
    NOTHING_TO_SETTLE = "nothing_to_settle"


@dataclass(frozen=True)
class ClineSettlementOutcome:
    """The outcome of one explicit guard-settlement attempt.

    ``settled`` is True only when the requested kind's local protection was
    cleared atomically after the final concurrency recheck. A refusal keeps
    every guard latched and names the safe machine reason; an unresolved
    concurrent abort or an active reconstruction is always a veto.
    """

    settled: bool
    refusal: ClineSettlementRefusal | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.settled, bool):
            raise ValueError("ClineSettlementOutcome.settled must be a boolean")
        if self.settled and self.refusal is not None:
            raise ValueError("a settled outcome carries no refusal reason")
        if not self.settled and self.refusal is None:
            raise ValueError("a refused outcome carries its safe refusal reason")


def classify_session_evidence(
    evidence: ClineSessionEvidence,
    *,
    checkpoint: ClineDispatchCheckpoint | None,
) -> ClineReconciliationResult:
    """Classify one reconciliation's evidence conservatively.

    The policy is deliberately narrow: a readable non-empty transcript with
    a present record is ``ACCESSIBLE``; a readable non-empty transcript
    without an addressable record is ``RECOVERABLE`` (the persisted
    conversation survives and the D5a same-ID reconstruction seam applies);
    terminal ``LOST`` requires the transcript to be positively absent AND
    the record positively absent AND a healthy, identity-carrying history
    enumeration to coherently lack the exact ID AND a demonstrated
    exhaustive public-history contract (``history_exhaustive``): a
    non-exhaustive enumeration proves readability, never exhaustive
    coverage, so an omitted older session stays ``UNKNOWN``; everything
    else — an empty
    transcript on an established session, contradictory reads, inadequate
    evidence — stays ``UNKNOWN``. A transcript that shrank below the
    caller's checkpoint is contradictory and forces ``UNKNOWN``.
    """
    transcript = evidence.transcript
    messages_beyond_checkpoint: int | None = None
    if transcript is not None and len(transcript) > 0:
        continuity = (
            ClineSessionContinuity.ACCESSIBLE
            if evidence.record_present
            else ClineSessionContinuity.RECOVERABLE
        )
        if checkpoint is not None:
            if len(transcript) < checkpoint.transcript_message_count:
                continuity = ClineSessionContinuity.UNKNOWN
            else:
                messages_beyond_checkpoint = len(transcript) - checkpoint.transcript_message_count
    elif transcript is not None:
        # An established bound session never has an empty persisted
        # transcript; an empty read is inadequate evidence, never loss.
        continuity = ClineSessionContinuity.UNKNOWN
    elif evidence.transcript_absent:
        if (
            evidence.record_present is False
            and evidence.history_presence is False
            and evidence.history_exhaustive
        ):
            # Terminal loss additionally requires a demonstrated exhaustive
            # public-history contract: a non-exhaustive enumeration proves
            # readability, never that every persisted session was covered,
            # so an omitted older session must stay unknown, not lost.
            continuity = ClineSessionContinuity.LOST
        else:
            continuity = ClineSessionContinuity.UNKNOWN
    else:
        continuity = ClineSessionContinuity.UNKNOWN
    return ClineReconciliationResult(
        continuity=continuity,
        transcript_message_count=len(transcript) if transcript is not None else None,
        messages_beyond_checkpoint=messages_beyond_checkpoint,
        record_observed=evidence.record_present,
    )
