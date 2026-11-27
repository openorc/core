"""Adapter-level tests for the Cline detached-turn observation source
(issues #323/#348).

The source's own behavior only: the per-connection Hub attachment through
the neutral attachment inputs, the exact-session subscriptions delivering
wrapped neutral evidence envelopes, the bounded status-only consultations,
the lease-based release discipline, the neutral source-error taxonomy, and
the classification seam's carrier discipline. The classification semantics
themselves are proven by the projection tests; the host's registry, gate,
and reconnection policy by the observation-host tests.
"""

from __future__ import annotations

from decimal import Decimal
from typing import Any, cast
from uuid import uuid4

import pytest

from openorc.adapters.cline import (
    ClineHubEvent,
    ClineSessionEvent,
    Subscription,
    hub_evidence_family,
)
from openorc.adapters.cline import hub_observation as source_module
from openorc.adapters.cline.errors import ClineBackendUncertainOutcomeError
from openorc.adapters.cline.hub_observation import (
    OPENORC_CLIENT_IDENTITY,
    ClineHubTurnObservationSource,
)
from openorc.domain.detached_turns import (
    DetachedTurnObservationSourceError,
    NativeTurnEvidence,
    ObservationLease,
    TurnAttribution,
    TurnEvidenceFamily,
)
from tests.fakes.cline_sdk_backend import FakeClineSdkBackend

_CONNECTION_ID = uuid4()
_ENDPOINT = "ws://hub.example.test:9/hub"
_EXTERNAL_SESSION = "ext-session-1"
_TOKEN = "observation-attach-token"
_RESULT_TEXT = '{"kind": "implementation_result"}'
_RUN_ID = "hub-run-1"


class _Inputs:
    """The structural attachment-inputs double the composition would pass."""

    endpoint: str
    auth_token: str

    def __init__(self) -> None:
        self.endpoint = _ENDPOINT
        self.auth_token = _TOKEN


def _attach(source: ClineHubTurnObservationSource, backend: FakeClineSdkBackend) -> None:
    source.attach_connection(connection_id=_CONNECTION_ID, inputs=cast(Any, _Inputs()))
    assert source.connection_attached(_CONNECTION_ID)
    connect_calls = [call for call in backend.calls() if call.operation == "connect"]
    assert len(connect_calls) == 1
    remote = connect_calls[0].remote
    assert remote is not None
    assert remote.endpoint == _ENDPOINT
    assert remote.client_identity == OPENORC_CLIENT_IDENTITY
    assert remote.auth_token == _TOKEN


def _terminal_event(
    *,
    session_id: str | None = _EXTERNAL_SESSION,
    sequence: int | None = 41,
) -> ClineHubEvent:
    return ClineHubEvent(
        session_id=session_id,
        kind="run.completed",
        sequence=sequence,
        payload={"reason": "finished", "result": {"text": _RESULT_TEXT}},
    )


def test_the_attachment_dials_the_exact_neutral_inputs_and_shares_the_connection() -> None:
    backend = FakeClineSdkBackend()
    source = ClineHubTurnObservationSource(backend_factory=lambda: backend)

    _attach(source, backend)

    # The attachment is shared: a second attach for the same connection is
    # the host's discipline to avoid, and the source simply owns one live
    # attachment per connection identity.
    assert source.connection_attached(_CONNECTION_ID) is True


def test_a_failed_attachment_is_a_typed_neutral_source_error_and_disposes() -> None:
    backend = FakeClineSdkBackend()
    backend.queue_connect(ClineBackendUncertainOutcomeError("the attach outcome is unknown"))
    source = ClineHubTurnObservationSource(backend_factory=lambda: backend)

    with pytest.raises(DetachedTurnObservationSourceError) as failed:
        source.attach_connection(connection_id=_CONNECTION_ID, inputs=cast(Any, _Inputs()))

    assert failed.value.reason == DetachedTurnObservationSourceError.ATTACHMENT_FAILED
    assert source.connection_attached(_CONNECTION_ID) is False
    assert any(call.operation == "dispose" for call in backend.calls())


def test_a_disposed_source_refuses_a_late_attachment_as_unavailable() -> None:
    backend = FakeClineSdkBackend()
    source = ClineHubTurnObservationSource(backend_factory=lambda: backend)
    source.dispose()

    with pytest.raises(DetachedTurnObservationSourceError) as unavailable:
        source.attach_connection(connection_id=_CONNECTION_ID, inputs=cast(Any, _Inputs()))

    assert unavailable.value.reason == DetachedTurnObservationSourceError.ATTACHMENT_UNAVAILABLE
    assert any(call.operation == "dispose" for call in backend.calls())


def test_the_observation_delivers_only_qualified_turn_evidence_envelopes() -> None:
    delivered: list[Any] = []
    backend = FakeClineSdkBackend()
    source = ClineHubTurnObservationSource(backend_factory=lambda: backend)
    _attach(source, backend)

    source.observe_session(
        connection_id=_CONNECTION_ID,
        external_session_id=_EXTERNAL_SESSION,
        deliver=delivered.append,
    )
    backend.emit_hub(_terminal_event())
    backend.emit_hub(
        ClineHubEvent(
            session_id=_EXTERNAL_SESSION,
            kind="run.started",
            sequence=3,
            payload={"requestId": _RUN_ID},
        )
    )
    backend.emit_hub(
        ClineHubEvent(session_id=_EXTERNAL_SESSION, kind="usage.updated", sequence=4, payload={})
    )

    assert len(delivered) == 2
    assert delivered[0].family is TurnEvidenceFamily.TURN_TERMINAL
    assert delivered[0].external_session_id == _EXTERNAL_SESSION
    carried = cast(source_module._ClineHubEventEvidence, delivered[0].native_evidence)
    assert carried.event.payload["result"]["text"] == _RESULT_TEXT
    assert delivered[1].family is TurnEvidenceFamily.TURN_START


class _ForeignLease(ObservationLease):
    """A lease some other runtime integration issued."""

    __slots__ = ()


def test_the_observation_lease_releases_exactly_its_own_subscription() -> None:
    delivered: list[Any] = []
    backend = FakeClineSdkBackend()
    source = ClineHubTurnObservationSource(backend_factory=lambda: backend)
    _attach(source, backend)
    lease = source.observe_session(
        connection_id=_CONNECTION_ID,
        external_session_id=_EXTERNAL_SESSION,
        deliver=delivered.append,
    )

    source.release_observation(lease)
    source.release_observation(lease)  # idempotent
    # A lease some other integration issued is a no-op for this source.
    source.release_observation(_ForeignLease())

    backend.emit_hub(_terminal_event())
    assert delivered == []
    subscribe_calls = [call for call in backend.calls() if call.operation == "subscribe_hub"]
    assert [call.session_id for call in subscribe_calls] == [_EXTERNAL_SESSION]


def test_a_failed_subscription_is_a_typed_neutral_source_error() -> None:
    backend = FakeClineSdkBackend()

    def failing_subscribe(session_id: str | None, listener: Any) -> Subscription:
        raise ClineBackendUncertainOutcomeError("the hub attachment is no longer available")

    backend.subscribe_hub = failing_subscribe  # type: ignore[method-assign]
    source = ClineHubTurnObservationSource(backend_factory=lambda: backend)
    _attach(source, backend)

    with pytest.raises(DetachedTurnObservationSourceError) as failed:
        source.observe_session(
            connection_id=_CONNECTION_ID,
            external_session_id=_EXTERNAL_SESSION,
            deliver=lambda evidence: None,
        )

    assert failed.value.reason == DetachedTurnObservationSourceError.SUBSCRIPTION_FAILED


def test_the_status_consultation_carries_the_records_verbatim() -> None:
    records = [{"runId": _RUN_ID, "state": "failed", "error": "session not found"}]
    backend = FakeClineSdkBackend()
    backend.queue_list_runs(_EXTERNAL_SESSION, records)
    source = ClineHubTurnObservationSource(backend_factory=lambda: backend)
    _attach(source, backend)

    evidence = source.consult_status_evidence(
        connection_id=_CONNECTION_ID, external_session_id=_EXTERNAL_SESSION
    )

    carrier = cast(source_module._ClineStatusRecordsEvidence, evidence)
    assert carrier.records == records


def test_the_classification_seam_consumes_its_own_carriers_only() -> None:
    backend = FakeClineSdkBackend()
    source = ClineHubTurnObservationSource(backend_factory=lambda: backend)

    attribution = source.classify_turn_start(
        source_module._ClineHubEventEvidence(
            ClineHubEvent(
                session_id=_EXTERNAL_SESSION,
                kind="run.started",
                sequence=3,
                payload={"requestId": _RUN_ID},
            )
        ),
        outstanding_turn_reference=_RUN_ID,
    )
    assert attribution is TurnAttribution.ATTRIBUTED

    with pytest.raises(ValueError, match="carrier"):
        source.classify_turn_start(
            cast(NativeTurnEvidence, object()), outstanding_turn_reference=_RUN_ID
        )
    with pytest.raises(ValueError, match="carrier"):
        source.classify_turn_terminal(
            cast(NativeTurnEvidence, object()), consumed_ordering_markers=[]
        )
    with pytest.raises(ValueError, match="carrier"):
        source.classify_turn_statuses(
            cast(NativeTurnEvidence, object()), outstanding_turn_reference=_RUN_ID
        )


def test_dropping_the_connection_clears_the_attachment_and_its_observations() -> None:
    backend = FakeClineSdkBackend()
    source = ClineHubTurnObservationSource(backend_factory=lambda: backend)
    _attach(source, backend)
    source.observe_session(
        connection_id=_CONNECTION_ID,
        external_session_id=_EXTERNAL_SESSION,
        deliver=lambda evidence: None,
    )

    source.drop_connection(_CONNECTION_ID)

    assert source.connection_attached(_CONNECTION_ID) is False
    assert any(call.operation == "dispose" for call in backend.calls())
    with pytest.raises(DetachedTurnObservationSourceError):
        source.consult_status_evidence(
            connection_id=_CONNECTION_ID, external_session_id=_EXTERNAL_SESSION
        )


def test_disposal_releases_every_observation_and_attachment() -> None:
    delivered: list[Any] = []
    backend = FakeClineSdkBackend()
    source = ClineHubTurnObservationSource(backend_factory=lambda: backend)
    _attach(source, backend)
    source.observe_session(
        connection_id=_CONNECTION_ID,
        external_session_id=_EXTERNAL_SESSION,
        deliver=delivered.append,
    )

    source.dispose()
    source.dispose()  # idempotent

    backend.emit_hub(_terminal_event())
    assert delivered == []
    assert any(call.operation == "dispose" for call in backend.calls())


def test_the_family_recognition_is_exported_for_the_delivery_wrap() -> None:
    assert hub_evidence_family(_terminal_event()) is TurnEvidenceFamily.TURN_TERMINAL


# --- the exact-session activity/usage pulses (issue #341) -------------------------


def _activity_event() -> ClineSessionEvent:
    """One qualifying model-backed activity event (thinking began)."""
    return ClineSessionEvent(
        session_id=_EXTERNAL_SESSION,
        kind="agent_event",
        payload={"event": {"type": "content_start", "contentType": "reasoning"}},
    )


def _usage_signal_event() -> ClineSessionEvent:
    """One live usage signal event."""
    return ClineSessionEvent(
        session_id=_EXTERNAL_SESSION,
        kind="agent_event",
        payload={"event": {"type": "usage", "totalCost": 1.5}},
    )


def _observe_session(
    delivered: list[Any],
) -> tuple[FakeClineSdkBackend, ClineHubTurnObservationSource, ObservationLease]:
    backend = FakeClineSdkBackend()
    source = ClineHubTurnObservationSource(backend_factory=lambda: backend)
    _attach(source, backend)
    lease = source.observe_session(
        connection_id=_CONNECTION_ID,
        external_session_id=_EXTERNAL_SESSION,
        deliver=delivered.append,
    )
    return backend, source, lease


def test_the_observation_delivers_content_free_activity_and_usage_pulses() -> None:
    delivered: list[Any] = []
    backend, _source, _lease = _observe_session(delivered)

    backend.emit(_activity_event())
    backend.emit(_usage_signal_event())
    backend.emit(
        ClineSessionEvent(session_id=_EXTERNAL_SESSION, kind="status", payload={"status": "idle"})
    )
    backend.emit(
        ClineSessionEvent(session_id=_EXTERNAL_SESSION, kind="ended", payload={"reason": "done"})
    )

    assert [evidence.family for evidence in delivered] == [
        TurnEvidenceFamily.TURN_ACTIVITY,
        TurnEvidenceFamily.TURN_USAGE,
    ]
    for evidence in delivered:
        assert evidence.external_session_id == _EXTERNAL_SESSION
        # The pulse carrier is content-free: no kind, detail, or event
        # material crosses the boundary.
        assert repr(evidence.native_evidence) == "_ClineActivityPulseEvidence(<content-free>)"
    # The Hub stream events of the same session still route as before.
    backend.emit_hub(_terminal_event())
    assert delivered[-1].family is TurnEvidenceFamily.TURN_TERMINAL


def test_the_usage_consultation_returns_the_neutral_aggregate_reading() -> None:
    backend = FakeClineSdkBackend()
    source = ClineHubTurnObservationSource(backend_factory=lambda: backend)
    _attach(source, backend)
    backend.queue_get_accumulated_usage(
        _EXTERNAL_SESSION, {"usage": {"totalCost": 99.0}, "aggregateUsage": {"totalCost": 26.5}}
    )

    reading = source.consult_session_usage(
        connection_id=_CONNECTION_ID, external_session_id=_EXTERNAL_SESSION
    )

    assert reading.aggregate_total_usd == Decimal("26.5")  # only the aggregate scope


def test_the_usage_consultation_without_a_usable_aggregate_is_unknown() -> None:
    backend = FakeClineSdkBackend()
    source = ClineHubTurnObservationSource(backend_factory=lambda: backend)
    _attach(source, backend)
    backend.queue_get_accumulated_usage(_EXTERNAL_SESSION, {"usage": {"totalCost": 99.0}})

    reading = source.consult_session_usage(
        connection_id=_CONNECTION_ID, external_session_id=_EXTERNAL_SESSION
    )

    assert reading.aggregate_total_usd is None


def test_the_usage_consultation_reports_an_observed_zero_as_real_evidence() -> None:
    backend = FakeClineSdkBackend()
    source = ClineHubTurnObservationSource(backend_factory=lambda: backend)
    _attach(source, backend)
    # The qualified free model (SDK2c, issue #413) observably reports a
    # totalCost of 0: the exact decimal zero is real evidence — never
    # unknown, never collapsed with an absent scope or substituted from the
    # root/lead-agent scope.
    backend.queue_get_accumulated_usage(
        _EXTERNAL_SESSION, {"usage": {"totalCost": 0.0}, "aggregateUsage": {"totalCost": 0}}
    )

    reading = source.consult_session_usage(
        connection_id=_CONNECTION_ID, external_session_id=_EXTERNAL_SESSION
    )

    assert reading.aggregate_total_usd == Decimal("0")


def test_the_observation_lease_releases_both_stream_subscriptions() -> None:
    delivered: list[Any] = []
    backend, source, lease = _observe_session(delivered)

    source.release_observation(lease)

    # Neither stream delivers after the release: both subscriptions were
    # released together with the lease.
    backend.emit(_activity_event())
    backend.emit_hub(_terminal_event())
    assert delivered == []
