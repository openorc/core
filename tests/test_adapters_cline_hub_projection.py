"""Adapter-level tests for the Cline Hub-run evidence classification
(issues #323/#348).

The integration's own interpretation only: the neutral evidence-family
recognition, the terminal classification (kind mapping, verbatim ordering
marker and candidate extraction, the stale-replay ordering verdict), the
start attribution under the pinned serial-run semantics, the status-only
record reduction, and the fail-closed handling of unattributable or
uninterpretable observations — plus a native fixture variation proving the
normalization stays local to this integration. The established protocol,
bridge, and settlement matrices are not re-proven here.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import cast

import pytest

from openorc.adapters.cline import (
    HubRunClassificationError,
    classify_hub_run_start,
    classify_hub_run_statuses,
    classify_terminal_hub_run,
    hub_evidence_family,
)
from openorc.adapters.cline.values import ClineHubEvent, JsonObject
from openorc.domain.detached_turns import (
    DetachedTurnObservationKind,
    TurnAttribution,
    TurnEvidenceFamily,
    TurnFreshness,
    TurnStatusEvidenceVerdict,
)


def test_a_terminal_envelope_recognizes_the_terminal_family_with_its_own_identity() -> None:
    completed = ClineHubEvent(
        session_id="sess-1",
        kind="run.completed",
        sequence=9,
        payload={"reason": "finished", "result": {"text": '{"ok": true}'}},
    )
    failed = ClineHubEvent(
        session_id="sess-1", kind="run.failed", sequence=10, payload={"reason": "error"}
    )

    assert hub_evidence_family(completed) is TurnEvidenceFamily.TURN_TERMINAL
    assert hub_evidence_family(failed) is TurnEvidenceFamily.TURN_TERMINAL


def test_a_start_envelope_recognizes_the_start_family_and_everything_else_is_not_delivered() -> (
    None
):
    start = ClineHubEvent(
        session_id="sess-1", kind="run.started", sequence=7, payload={"requestId": "hub-run-1"}
    )
    identity_less = ClineHubEvent(
        session_id=None, kind="run.completed", sequence=13, payload={"result": {"text": "x"}}
    )

    assert hub_evidence_family(start) is TurnEvidenceFamily.TURN_START
    for kind in ("usage.updated", "session.updated"):
        assert (
            hub_evidence_family(
                ClineHubEvent(session_id="sess-1", kind=kind, sequence=1, payload={})
            )
            is None
        )
    assert hub_evidence_family(identity_less) is None


def test_a_terminal_classification_maps_the_kind_and_candidate_verbatim() -> None:
    completed = ClineHubEvent(
        session_id="sess-1",
        kind="run.completed",
        sequence=9,
        payload={"reason": "finished", "result": {"text": '{"ok": true}'}},
    )
    failed = ClineHubEvent(
        session_id="sess-1",
        kind="run.failed",
        sequence=11,
        payload={"reason": "error", "result": {"text": "partial"}, "snapshot": {}},
    )

    completed_classification = classify_terminal_hub_run(completed, consumed_ordering_markers=())
    failed_classification = classify_terminal_hub_run(failed, consumed_ordering_markers=())

    assert completed_classification.outcome.kind is DetachedTurnObservationKind.COMPLETED
    assert completed_classification.outcome.result_candidate == '{"ok": true}'
    assert completed_classification.ordering_marker == 9
    assert completed_classification.freshness is TurnFreshness.FRESH
    assert failed_classification.outcome.kind is DetachedTurnObservationKind.FAILED
    assert failed_classification.outcome.result_candidate == "partial"


def test_a_completed_run_without_a_usable_candidate_text_is_a_legitimate_classification() -> None:
    event = ClineHubEvent(
        session_id="sess-1",
        kind="run.completed",
        sequence=12,
        payload={"reason": "finished", "result": {}},
    )

    classification = classify_terminal_hub_run(event, consumed_ordering_markers=())

    assert classification.outcome.result_candidate is None
    assert classification.freshness is TurnFreshness.FRESH


def test_the_ordering_verdict_applies_the_pinned_durable_log_semantics() -> None:
    fresh = ClineHubEvent(
        session_id="sess-1", kind="run.completed", sequence=43, payload={"result": {}}
    )
    stale = ClineHubEvent(
        session_id="sess-1", kind="run.completed", sequence=41, payload={"result": {}}
    )
    unordered = ClineHubEvent(
        session_id="sess-1", kind="run.completed", sequence=None, payload={"result": {}}
    )

    assert (
        classify_terminal_hub_run(fresh, consumed_ordering_markers=[41]).freshness
        is TurnFreshness.FRESH
    )
    assert (
        classify_terminal_hub_run(stale, consumed_ordering_markers=[41]).freshness
        is TurnFreshness.STALE
    )
    assert (
        classify_terminal_hub_run(unordered, consumed_ordering_markers=[41]).freshness
        is TurnFreshness.UNORDERED
    )


def test_the_start_attribution_applies_the_pinned_serial_run_semantics() -> None:
    matching = ClineHubEvent(
        session_id="sess-1", kind="run.started", sequence=7, payload={"requestId": "hub-run-1"}
    )
    different = ClineHubEvent(
        session_id="sess-1", kind="run.started", sequence=7, payload={"requestId": "hub-run-2"}
    )
    claim_less = ClineHubEvent(
        session_id="sess-1", kind="run.started", sequence=7, payload={"snapshot": {}}
    )

    assert (
        classify_hub_run_start(matching, outstanding_turn_reference="hub-run-1")
        is TurnAttribution.ATTRIBUTED
    )
    assert (
        classify_hub_run_start(different, outstanding_turn_reference="hub-run-1")
        is TurnAttribution.CONTRADICTED
    )
    assert (
        classify_hub_run_start(claim_less, outstanding_turn_reference="hub-run-1")
        is TurnAttribution.UNATTRIBUTABLE
    )


def test_status_only_records_reduce_to_one_verdict_for_the_outstanding_reference() -> None:
    records = [
        {"runId": "other-run", "state": "queued", "acceptedAt": 1794712612934},
        {"runId": "hub-run-1", "state": "failed", "error": "session not found"},
    ]

    assert (
        classify_hub_run_statuses(records, outstanding_turn_reference="hub-run-1")
        is TurnStatusEvidenceVerdict.FAILURE_EVIDENCE
    )
    assert (
        classify_hub_run_statuses(
            [{"runId": "hub-run-1", "state": "completed"}],
            outstanding_turn_reference="hub-run-1",
        )
        is TurnStatusEvidenceVerdict.COMPLETION_WITHOUT_RESULT_EVIDENCE
    )
    assert (
        classify_hub_run_statuses(
            [{"runId": "hub-run-1", "state": "queued"}],
            outstanding_turn_reference="hub-run-1",
        )
        is TurnStatusEvidenceVerdict.UNRESOLVED_EVIDENCE
    )
    assert (
        classify_hub_run_statuses(
            [{"runId": "other-run", "state": "failed"}],
            outstanding_turn_reference="hub-run-1",
        )
        is TurnStatusEvidenceVerdict.UNRESOLVED_EVIDENCE
    )
    assert (
        classify_hub_run_statuses([], outstanding_turn_reference="hub-run-1")
        is TurnStatusEvidenceVerdict.UNRESOLVED_EVIDENCE
    )
    assert (
        classify_hub_run_statuses(
            [
                {"runId": "hub-run-1", "state": "failed"},
                {"runId": "hub-run-1", "state": "failed"},
            ],
            outstanding_turn_reference="hub-run-1",
        )
        is TurnStatusEvidenceVerdict.UNRESOLVED_EVIDENCE
    )


def test_an_uninterpretable_status_record_fails_closed_instead_of_being_dropped() -> None:
    with pytest.raises(HubRunClassificationError):
        classify_hub_run_statuses(
            [{"runId": "r1", "state": "failed"}, {"state": "failed"}],
            outstanding_turn_reference="r1",
        )
    with pytest.raises(HubRunClassificationError):
        classify_hub_run_statuses(
            cast(Sequence[JsonObject], ["not-a-record"]),
            outstanding_turn_reference="r1",
        )


def test_a_terminal_interruption_marker_is_the_only_positive_interrupted_verdict() -> None:
    """The R2b-qualified native finish reason — read kind-agnostically from
    either terminal event — is the only positive interrupted verdict; every
    other shape is never invented into interruption evidence (issue #342)."""
    completed_interrupted = ClineHubEvent(
        session_id="sess-1",
        kind="run.completed",
        sequence=9,
        payload={"reason": "finished", "result": {"finishReason": "aborted"}},
    )
    failed_interrupted = ClineHubEvent(
        session_id="sess-1",
        kind="run.failed",
        sequence=10,
        payload={"reason": "error", "result": {"finishReason": "aborted"}},
    )
    finished = ClineHubEvent(
        session_id="sess-1",
        kind="run.completed",
        sequence=11,
        payload={"reason": "finished", "result": {"finishReason": "completed", "text": "x"}},
    )
    failed_other = ClineHubEvent(
        session_id="sess-1",
        kind="run.failed",
        sequence=12,
        payload={"reason": "error", "result": {"finishReason": "error"}},
    )
    no_result = ClineHubEvent(
        session_id="sess-1", kind="run.failed", sequence=13, payload={"reason": "error"}
    )
    non_string = ClineHubEvent(
        session_id="sess-1",
        kind="run.completed",
        sequence=14,
        payload={"result": {"finishReason": 7}},
    )

    assert (
        classify_terminal_hub_run(
            completed_interrupted, consumed_ordering_markers=()
        ).outcome.interrupted
        is True
    )
    assert (
        classify_terminal_hub_run(
            failed_interrupted, consumed_ordering_markers=()
        ).outcome.interrupted
        is True
    )
    for event in (finished, failed_other, no_result, non_string):
        classification = classify_terminal_hub_run(event, consumed_ordering_markers=())
        assert classification.outcome.interrupted is False
    # The verdict never manufactures a candidate either: an interrupted
    # result carries the candidate exactly as extracted, verbatim or None.
    assert (
        classify_terminal_hub_run(
            completed_interrupted, consumed_ordering_markers=()
        ).outcome.result_candidate
        is None
    )


def test_a_native_fixture_variation_normalizes_locally_to_the_same_verdicts() -> None:
    """A coordinated SDK revision that renames native fields is absorbed by a
    local interpretation change: the variant fixture normalizes to identical
    neutral verdicts with no shared application, domain, or persistence
    change."""

    def _normalize_variant_records(records: Sequence[JsonObject]) -> list[JsonObject]:
        # The adapter-local pre-normalization a native field rename would
        # add inside this integration; nothing outside the adapter sees it.
        _STATUS_TOKENS = {"errored": "failed", "done": "completed"}
        return [
            {"runId": record["runKey"], "state": _STATUS_TOKENS[record["phase"]]}
            for record in records
            if isinstance(record, dict) and "runKey" in record and "phase" in record
        ]

    variant_failed = [{"runKey": "hub-run-1", "phase": "errored"}]
    variant_completed = [{"runKey": "hub-run-1", "phase": "done"}]

    assert (
        classify_hub_run_statuses(
            _normalize_variant_records(variant_failed),
            outstanding_turn_reference="hub-run-1",
        )
        is TurnStatusEvidenceVerdict.FAILURE_EVIDENCE
    )
    assert (
        classify_hub_run_statuses(
            _normalize_variant_records(variant_completed),
            outstanding_turn_reference="hub-run-1",
        )
        is TurnStatusEvidenceVerdict.COMPLETION_WITHOUT_RESULT_EVIDENCE
    )
