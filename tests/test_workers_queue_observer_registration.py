"""Deterministic tests for the queue dispatch worker's remote registration
proxy (issue #346).

The worker-side half of the private authenticated registration transport,
over a fake bounded transport: the exact deployment request shape, the
positive acknowledgement contract, the typed refusal passthrough, and the
fail-closed mapping of every transport failure, unexpected status, and
unusable response. The worker never treats a sent request as registration
success, and the deployment credential never appears in any error.
"""

from __future__ import annotations

import json
import urllib.error
import urllib.request
from typing import Any
from uuid import uuid4

import pytest

from openorc.services.errors import ObservationRegistrationRefusedError
from openorc.services.task_detached_turn_observation import (
    ObservationRegistrationRefusalReason,
)
from openorc.workers.queue_observer_registration import (
    QueueDetachedTurnObservationRegistrar,
)

_REGISTRATION_URL = "http://control-plane.example.test"
_REGISTRATION_SECRET = "deployment-registration-secret"
_IDENTITIES: dict[str, Any] = {
    "workspace_id": uuid4(),
    "task_id": uuid4(),
    "task_agent_session_id": uuid4(),
    "connection_id": uuid4(),
    "external_session_id": "ext-session-1",
}


class _ScriptedTransport:
    """One canned HTTP exchange; records every request it was asked to send."""

    def __init__(
        self,
        *,
        status: int = 200,
        body: bytes = b'{"acknowledged": true}',
        error: Exception | None = None,
    ) -> None:
        self.status = status
        self.body = body
        self.error = error
        self.requests: list[tuple[urllib.request.Request, float]] = []

    def __call__(self, request: urllib.request.Request, timeout: float) -> tuple[int, bytes]:
        self.requests.append((request, timeout))
        if self.error is not None:
            raise self.error
        return self.status, self.body


def _registrar(transport: _ScriptedTransport, *, timeout_seconds: float = 10.0):
    return QueueDetachedTurnObservationRegistrar(
        registration_url=_REGISTRATION_URL,
        registration_secret=_REGISTRATION_SECRET,
        timeout_seconds=timeout_seconds,
        transport=transport,
    )


def _registration_body(request: urllib.request.Request) -> dict[str, Any]:
    data = request.data
    assert isinstance(data, bytes)
    document = json.loads(data.decode("utf-8"))
    assert isinstance(document, dict)
    return document


def test_a_positive_acknowledgement_sends_the_exact_deployment_request() -> None:
    transport = _ScriptedTransport()
    registrar = _registrar(transport)

    registrar.register_session(**_IDENTITIES)

    assert len(transport.requests) == 1
    request, timeout = transport.requests[0]
    assert request.full_url == f"{_REGISTRATION_URL}/internal/observer/registrations"
    assert request.get_method() == "POST"
    assert request.headers.get("Authorization") == f"Bearer {_REGISTRATION_SECRET}"
    assert request.headers.get("Content-type") == "application/json"
    assert timeout == 10.0
    assert _registration_body(request) == {
        "kind": "established_session",
        **{name: str(value) for name, value in _IDENTITIES.items()},
    }


def test_the_establishment_registration_carries_the_establishing_kind() -> None:
    transport = _ScriptedTransport()
    registrar = _registrar(transport)

    registrar.register_establishment_session(**_IDENTITIES)

    assert _registration_body(transport.requests[0][0])["kind"] == "establishing_session"


def test_a_typed_refusal_is_reraised_with_the_same_safe_reason() -> None:
    transport = _ScriptedTransport(
        status=409,
        body=b'{"acknowledged": false, "refusal_reason": "session_binding_unresolved"}',
    )
    registrar = _registrar(transport)

    with pytest.raises(ObservationRegistrationRefusedError) as refused:
        registrar.register_session(**_IDENTITIES)

    assert refused.value.reason == ObservationRegistrationRefusalReason.SESSION_BINDING_UNRESOLVED


def test_a_refusal_without_a_closed_reason_is_the_unavailable_refusal() -> None:
    transport = _ScriptedTransport(status=409, body=b'{"detail": "unexpected"}')
    registrar = _registrar(transport)

    with pytest.raises(ObservationRegistrationRefusedError) as refused:
        registrar.register_session(**_IDENTITIES)

    assert refused.value.reason == ObservationRegistrationRefusalReason.OBSERVER_UNAVAILABLE


@pytest.mark.parametrize("status", [401, 403, 404, 500, 503])
def test_other_error_statuses_are_the_unavailable_refusal(status: int) -> None:
    transport = _ScriptedTransport(status=status, body=b'{"detail": "no"}')
    registrar = _registrar(transport)

    with pytest.raises(ObservationRegistrationRefusedError) as refused:
        registrar.register_session(**_IDENTITIES)

    assert refused.value.reason == ObservationRegistrationRefusalReason.OBSERVER_UNAVAILABLE


def test_an_unusable_acknowledgement_is_a_registration_failure() -> None:
    transport = _ScriptedTransport(status=200, body=b'{"detail": "not an acknowledgement"}')
    registrar = _registrar(transport)

    with pytest.raises(ObservationRegistrationRefusedError) as refused:
        registrar.register_session(**_IDENTITIES)

    reason = refused.value.reason
    assert reason == ObservationRegistrationRefusalReason.OBSERVATION_REGISTRATION_FAILED


def test_an_oversized_response_is_refused_without_being_trusted() -> None:
    transport = _ScriptedTransport(status=200, body=b'{"acknowledged": true}' + b"x" * (64 * 1024))
    registrar = _registrar(transport)

    with pytest.raises(ObservationRegistrationRefusedError) as refused:
        registrar.register_session(**_IDENTITIES)

    reason = refused.value.reason
    assert reason == ObservationRegistrationRefusalReason.OBSERVATION_REGISTRATION_FAILED


@pytest.mark.parametrize(
    "error",
    [
        urllib.error.URLError(TimeoutError("the registration transport timed out")),
        TimeoutError("the registration transport timed out"),
        ConnectionResetError("the control-plane connection was reset"),
    ],
)
def test_a_transport_failure_is_the_unavailable_refusal_and_never_leaks_the_deployment_credential(
    error: Exception,
) -> None:
    transport = _ScriptedTransport(error=error)
    registrar = _registrar(transport)

    with pytest.raises(ObservationRegistrationRefusedError) as refused:
        registrar.register_session(**_IDENTITIES)

    assert refused.value.reason == ObservationRegistrationRefusalReason.OBSERVER_UNAVAILABLE
    # The typed refusal carries only safe classification: the deployment
    # credential and the transport endpoint never appear in its message.
    assert _REGISTRATION_SECRET not in str(refused.value)
    assert _REGISTRATION_URL not in str(refused.value)


def test_a_blank_registration_base_url_is_a_construction_error() -> None:
    with pytest.raises(ValueError):
        QueueDetachedTurnObservationRegistrar(
            registration_url="   ",
            registration_secret=_REGISTRATION_SECRET,
        )
