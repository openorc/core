"""Hub-run observation projection (detached agent turns, issue #323).

The narrow adapter-owned normalization between the pinned SDK's Hub event
envelopes / status-only ``run.list`` records and the runtime-neutral
normalized observation values the settlement seam consumes
(:mod:`openorc.domain.hub_runs`). This module is pure projection: it owns
the qualified pinned-version payload shapes and nothing else — no transport,
no workflow meaning, no persistence, no subscription lifecycle.

Discipline (mirrors the adapter's established normalization rules):

- Identity is never synthesized. A terminal or start observation carries
  the Hub envelope's own session identity; an envelope without one is
  unattributable and projects to ``None`` — never guessed from payload
  contents, timestamps, or ``snapshot.prompt``.
- Terminal events carry neither run nor request identity (issue #320): the
  projection forwards the envelope's durable-log sequence verbatim and
  extracts the reply candidate exactly as the blocking send path does —
  the native ``AgentResult``'s ``text`` — without parsing, repairing, or
  interpreting it. A terminal observation without a usable candidate text
  is a legitimate observation (``result_candidate=None``) that the
  settlement seam routes as it does every unvalidated completion; the
  projection never invents one and never reads the SDK-native ``error``
  payload into any message.
- Status-only ``run.list`` records carry the Hub's own opaque ``runId`` and
  ``state`` tokens verbatim. A record that cannot be interpreted fails
  closed (a typed projection error) instead of being silently dropped:
  a dropped failed-run record could hide exactly the failure the
  status-only reconciliation exists to classify.
- Payload contents never enter representations, errors, or logs.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from typing import Final

from openorc.adapters.cline.values import ClineHubEvent, JsonObject
from openorc.domain.hub_runs import (
    HubRunObservationKind,
    ObservedHubRunCompletion,
    ObservedHubRunStatus,
)

__all__ = [
    "ClineHubRunStartObservation",
    "HubRunProjectionError",
    "project_hub_run_start",
    "project_hub_run_statuses",
    "project_terminal_hub_run",
]

#: The qualified terminal run event names (issue #320) mapped to the seam's
#: closed observation vocabulary. Every other event name is not a terminal
#: run observation and projects to ``None``.
_TERMINAL_EVENT_KINDS: Final[dict[str, HubRunObservationKind]] = {
    "run.completed": HubRunObservationKind.RUN_COMPLETED,
    "run.failed": HubRunObservationKind.RUN_FAILED,
}

#: The qualified run-start event name whose ``requestId`` payload field is
#: the only event-side run handle (verified equal to the acknowledged
#: ``runId``; issues #320/#321).
_START_EVENT_KIND: Final[str] = "run.started"

_RUN_LIST_RECORD_ID_KEY: Final[str] = "runId"
_RUN_LIST_RECORD_STATUS_KEY: Final[str] = "state"


class HubRunProjectionError(ValueError):
    """One observed Hub-run payload cannot be interpreted under the pinned
    contract. The observation is discarded as uninterpretable — never
    guessed around — and the observation's own safe event kind is all any
    caller may log."""


@dataclass(frozen=True, slots=True)
class ClineHubRunStartObservation:
    """One projected ``run.started`` identity observation.

    ``external_session_id`` is the envelope's own session identity (never
    synthesized), ``request_id`` the payload's ``requestId`` run handle when
    it carried one (``None`` is a legitimate start shape the seam
    classifies as unattributable), and ``sequence`` the envelope's
    durable-log sequence when present.
    """

    external_session_id: str
    request_id: str | None
    sequence: int | None

    def __post_init__(self) -> None:
        if not isinstance(self.external_session_id, str) or not self.external_session_id.strip():
            raise ValueError(
                "ClineHubRunStartObservation.external_session_id must be a non-empty string"
            )
        if self.request_id is not None and (
            not isinstance(self.request_id, str) or not self.request_id.strip()
        ):
            raise ValueError(
                "ClineHubRunStartObservation.request_id must be None or a non-empty string"
            )
        if self.sequence is not None and type(self.sequence) is not int:
            raise ValueError("ClineHubRunStartObservation.sequence must be None or an integer")


def _non_blank_str(value: object) -> str | None:
    return value if isinstance(value, str) and value.strip() else None


def project_terminal_hub_run(event: ClineHubEvent) -> ObservedHubRunCompletion | None:
    """Project one Hub event envelope into a terminal run observation.

    Returns ``None`` for every non-terminal event and for a terminal
    envelope without its own session identity: neither is an attributable
    terminal run observation, and neither is invented into one. The reply
    candidate is the native result's ``text`` forwarded verbatim; a
    completed run without a usable candidate text stays a legitimate
    observation with ``result_candidate=None``.
    """
    kind = _TERMINAL_EVENT_KINDS.get(event.kind)
    if kind is None:
        return None
    external_session_id = _non_blank_str(event.session_id)
    if external_session_id is None:
        return None
    result = event.payload.get("result")
    candidate: str | None = None
    if isinstance(result, dict):
        candidate = _non_blank_str(result.get("text"))
    return ObservedHubRunCompletion(
        external_session_id=external_session_id,
        kind=kind,
        sequence=event.sequence,
        result_candidate=candidate,
    )


def project_hub_run_start(event: ClineHubEvent) -> ClineHubRunStartObservation | None:
    """Project one Hub event envelope into a run-start identity observation.

    Returns ``None`` for every non-start event and for a start envelope
    without its own session identity. The ``requestId`` run handle is
    forwarded verbatim when the payload carried one and is never taken from
    any other payload field.
    """
    if event.kind != _START_EVENT_KIND:
        return None
    external_session_id = _non_blank_str(event.session_id)
    if external_session_id is None:
        return None
    return ClineHubRunStartObservation(
        external_session_id=external_session_id,
        request_id=_non_blank_str(event.payload.get("requestId")),
        sequence=event.sequence,
    )


def project_hub_run_statuses(records: Sequence[JsonObject]) -> list[ObservedHubRunStatus]:
    """Project status-only ``run.list`` records into seam observations.

    The Hub's own ``runId`` and ``state`` tokens are forwarded verbatim. A
    record without an interpretable identity or status token raises
    :class:`HubRunProjectionError`: the consultation that produced it is
    abandoned rather than partially interpreted, because a silently dropped
    record could hide exactly the failed run the status-only reconciliation
    exists to classify.
    """
    projected: list[ObservedHubRunStatus] = []
    for record in records:
        if not isinstance(record, dict):
            raise HubRunProjectionError("a run.list record was not a usable record object")
        run_id = _non_blank_str(record.get(_RUN_LIST_RECORD_ID_KEY))
        status = _non_blank_str(record.get(_RUN_LIST_RECORD_STATUS_KEY))
        if run_id is None or status is None:
            raise HubRunProjectionError(
                "a run.list record carried no interpretable run identity or status token"
            )
        projected.append(ObservedHubRunStatus(run_id=run_id, status=status))
    return projected
