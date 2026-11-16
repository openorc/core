"""Hub-run evidence interpretation (detached agent turns, issues #323/#348).

The Cline integration's own interpretation of the pinned SDK's Hub event
envelopes and status-only ``run.list`` records, projected onto the
runtime-neutral evidence and attribution vocabulary the detached-turn
settlement seam consumes (:mod:`openorc.domain.detached_turns`). This
module is pure interpretation: it owns the qualified pinned-version payload
shapes and the native semantics of which evidence proves or counters an
admitted turn, and nothing else — no transport, no workflow meaning, no
persistence, no subscription lifecycle. Shared application code never sees
a native payload, event name, status token, or identity comparison; it
consumes only the neutral families and verdicts produced here.

Discipline (mirrors the adapter's established normalization rules):

- Identity is never synthesized. A terminal or start observation carries
  the Hub envelope's own session identity; an envelope without one is
  unattributable and is never delivered — never guessed from payload
  contents, timestamps, or ``snapshot.prompt``.
- Terminal events carry neither run nor request identity (issue #320): the
  classification forwards the envelope's durable-log sequence verbatim as
  the ordering marker and extracts the reply candidate exactly as the
  blocking send path does — the native ``AgentResult``'s ``text`` — without
  parsing, repairing, or interpreting it. A completed run without a usable
  candidate text is a legitimate classification (``result_candidate=None``)
  that the settlement seam routes as it does every unvalidated completion;
  the classification never invents one and never reads the SDK-native
  ``error`` payload into any message.
- The start attribution applies the pinned Hub's serial-run semantics: a
  ``run.started`` payload's ``requestId`` is the runtime's own claim of the
  executing turn, so a claim equal to the outstanding admitted reference
  attributes it and a concretely different claim contradicts it. A start
  without a usable claim is honestly unattributable.
- The stale-replay ordering applies the pinned Hub's durable-log sequence
  semantics per exact session: a terminal observation whose sequence is
  not newer than a consumed marker is an already-consumed or replayed
  notification; an observation without a sequence carries no ordering
  evidence and is never ordered.
- Status-only ``run.list`` records carry the Hub's own opaque ``runId`` and
  ``state`` tokens. A record that cannot be interpreted fails closed (a
  typed classification error) instead of being silently dropped: a dropped
  failed-run record could hide exactly the failure the status-only
  reconciliation exists to classify. Only the exact outstanding reference
  reported ``failed`` is failure evidence; ``completed`` alone is never
  success or result evidence; every other shape stays honestly unresolved.
- Payload contents never enter representations, errors, or logs.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Final

from openorc.adapters.cline.values import ClineHubEvent, JsonObject
from openorc.domain.detached_turns import (
    DetachedTurnObservationKind,
    TurnAttribution,
    TurnEvidenceFamily,
    TurnFreshness,
    TurnStatusEvidenceVerdict,
    TurnTerminalClassification,
    TurnTerminalOutcome,
)

__all__ = [
    "HubRunClassificationError",
    "classify_hub_run_start",
    "classify_hub_run_statuses",
    "classify_terminal_hub_run",
    "hub_evidence_family",
]

#: The qualified terminal run event names (issue #320) mapped to the seam's
#: closed observation vocabulary. Every other event name is not a terminal
#: run observation.
_TERMINAL_EVENT_KINDS: Final[dict[str, DetachedTurnObservationKind]] = {
    "run.completed": DetachedTurnObservationKind.COMPLETED,
    "run.failed": DetachedTurnObservationKind.FAILED,
}

#: The qualified run-start event name whose ``requestId`` payload field is
#: the only event-side run handle (verified equal to the acknowledged
#: ``runId``; issues #320/#321).
_START_EVENT_KIND: Final[str] = "run.started"

_RUN_LIST_RECORD_ID_KEY: Final[str] = "runId"
_RUN_LIST_RECORD_STATUS_KEY: Final[str] = "state"

#: The Hub's own status token that positively classifies a failed run.
_RUN_LIST_FAILED_STATUS: Final[str] = "failed"

#: The Hub's own status token of a completed run: never success or result
#: evidence by itself.
_RUN_LIST_COMPLETED_STATUS: Final[str] = "completed"


class HubRunClassificationError(ValueError):
    """One observed Hub-run payload cannot be interpreted under the pinned
    contract. The observation is discarded as uninterpretable — never
    guessed around — and the observation's own safe event kind is all any
    caller may log."""


def _non_blank_str(value: object) -> str | None:
    return value if isinstance(value, str) and value.strip() else None


def hub_evidence_family(event: ClineHubEvent) -> TurnEvidenceFamily | None:
    """Recognize one Hub event envelope's neutral turn-evidence family.

    Returns ``None`` for every event that is not a qualified turn
    observation and for any envelope without its own session identity:
    neither is an attributable turn observation, and neither is invented
    into one.
    """
    if _non_blank_str(event.session_id) is None:
        return None
    if event.kind in _TERMINAL_EVENT_KINDS:
        return TurnEvidenceFamily.TURN_TERMINAL
    if event.kind == _START_EVENT_KIND:
        return TurnEvidenceFamily.TURN_START
    return None


def classify_terminal_hub_run(
    event: ClineHubEvent, *, consumed_ordering_markers: Sequence[int]
) -> TurnTerminalClassification:
    """Classify one delivered terminal Hub event against the consumed markers.

    The caller delivers only envelopes :func:`hub_evidence_family` already
    recognized as terminal turn evidence with their own session identity.
    The reply candidate is the native result's ``text`` forwarded verbatim;
    a completed run without a usable candidate text stays a legitimate
    classification with ``result_candidate=None``. The ordering verdict
    applies the pinned Hub's durable-log sequence semantics: a sequence not
    newer than any provided consumed marker is a stale replay, and a
    sequence-less envelope carries no ordering evidence and is never
    ordered.
    """
    kind = _TERMINAL_EVENT_KINDS[event.kind]
    result = event.payload.get("result")
    candidate: str | None = None
    if isinstance(result, dict):
        candidate = _non_blank_str(result.get("text"))
    marker = event.sequence
    if marker is None:
        freshness = TurnFreshness.UNORDERED
    elif any(consumed >= marker for consumed in consumed_ordering_markers):
        freshness = TurnFreshness.STALE
    else:
        freshness = TurnFreshness.FRESH
    return TurnTerminalClassification(
        freshness=freshness,
        outcome=TurnTerminalOutcome(kind=kind, result_candidate=candidate),
        ordering_marker=marker,
    )


def classify_hub_run_start(
    event: ClineHubEvent, *, outstanding_turn_reference: str
) -> TurnAttribution:
    """Classify one delivered ``run.started`` envelope against the outstanding
    acknowledged turn reference.

    The pinned Hub's ``requestId`` payload field is the only event-side run
    handle and is never taken from any other payload field. Under the
    pinned Hub's serial-run semantics a claim equal to the outstanding
    reference attributes the start and a concretely different claim
    contradicts it; a start without a usable claim is honestly
    unattributable.
    """
    claim = _non_blank_str(event.payload.get("requestId"))
    if claim is None:
        return TurnAttribution.UNATTRIBUTABLE
    if claim == outstanding_turn_reference:
        return TurnAttribution.ATTRIBUTED
    return TurnAttribution.CONTRADICTED


def classify_hub_run_statuses(
    records: Sequence[JsonObject], *, outstanding_turn_reference: str
) -> TurnStatusEvidenceVerdict:
    """Classify status-only ``run.list`` records for the outstanding reference.

    Only the exact outstanding reference reported ``failed`` is failure
    evidence; ``completed`` alone is never success or result evidence; a
    missing record, an ambiguous duplicate identity, or any other state
    token stays honestly unresolved. A record without an interpretable
    identity or status token raises :class:`HubRunClassificationError`: the
    consultation that produced it is abandoned rather than partially
    interpreted, because a silently dropped record could hide exactly the
    failed run the status-only reconciliation exists to classify.
    """
    matching_statuses: list[str] = []
    for record in records:
        if not isinstance(record, dict):
            raise HubRunClassificationError("a run.list record was not a usable record object")
        run_id = _non_blank_str(record.get(_RUN_LIST_RECORD_ID_KEY))
        status = _non_blank_str(record.get(_RUN_LIST_RECORD_STATUS_KEY))
        if run_id is None or status is None:
            raise HubRunClassificationError(
                "a run.list record carried no interpretable run identity or status token"
            )
        if run_id == outstanding_turn_reference:
            matching_statuses.append(status)
    if len(matching_statuses) != 1:
        return TurnStatusEvidenceVerdict.UNRESOLVED_EVIDENCE
    if matching_statuses[0] == _RUN_LIST_FAILED_STATUS:
        return TurnStatusEvidenceVerdict.FAILURE_EVIDENCE
    if matching_statuses[0] == _RUN_LIST_COMPLETED_STATUS:
        return TurnStatusEvidenceVerdict.COMPLETION_WITHOUT_RESULT_EVIDENCE
    return TurnStatusEvidenceVerdict.UNRESOLVED_EVIDENCE
