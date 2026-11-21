"""Deterministic transport tests for the private queue-mode detached-turn
observation registration route (issue #346).

The deployment-only authenticated handshake at the API boundary: the uniform
detail-free credential rejection, the fail-closed unconfigured-secret case,
the strictly-shaped registration payload, the typed refusal mapping with its
safe reason tokens, the positively-refused unavailable host, and the focused
fake two-process registration-before-admission handshake whose independent
completion settles after the dispatching workhorse is gone.
"""

from __future__ import annotations

import urllib.parse
import urllib.request
from typing import Any
from uuid import UUID, uuid4

import pytest
from fastapi.testclient import TestClient

from openorc.api import app as app_module
from openorc.api.app import create_app
from openorc.config import ExecutionMode
from openorc.observability import shutdown_observability
from openorc.services import task_detached_turn_observation as observation_module
from openorc.services.errors import ObservationRegistrationRefusedError
from openorc.services.task_detached_turn_correlation import TurnSettlementKind
from openorc.services.task_detached_turn_observation import DetachedTurnObservationHost
from openorc.workers.invocation import OperationDependencies, RQQueueOperationSubmission
from openorc.workers.queue_observer_registration import (
    QueueDetachedTurnObservationRegistrar,
)
from tests.fakes.cline_sdk_backend import FakeClineSdkBackend
from tests.test_services_task_detached_turn_observation import (
    _EXTERNAL_SESSION,
    _SESSION_ID,
    _TASK_ID,
    _WORKSPACE_ID,
    ScriptedConnection,
    _admitted_receipt_row,
    _binding_row,
    _BindingRow,
    _make_observer,
    _pool,
    _ReceiptRow,
    _script_bindings,
    _script_outstanding_receipts,
    _script_restart_candidates,
    _SettleRecorder,
    _subscribe_calls,
    _terminal_event,
)

_QUEUE_SETTINGS: dict[str, Any] = {
    "execution_mode": ExecutionMode.QUEUE,
    "valkey_url": "redis://127.0.0.1:6379/9",
    "queue_observer_registration_url": "http://127.0.0.1:3999",
    "queue_observer_registration_secret": "deployment-registration-secret",
}
_REGISTRATION_SECRET = "deployment-registration-secret"
_REGISTRATION_HEADERS = {"Authorization": f"Bearer {_REGISTRATION_SECRET}"}


def _registration_body(
    *, kind: str = "established_session", external_session_id: str = _EXTERNAL_SESSION
) -> dict[str, Any]:
    return {
        "kind": kind,
        "workspace_id": str(_WORKSPACE_ID),
        "task_id": str(_TASK_ID),
        "task_agent_session_id": str(_SESSION_ID),
        "connection_id": str(uuid4()),
        "external_session_id": external_session_id,
    }


class _RecordingRegistrar:
    """The route-facing registrar stand-in recording every registration."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, dict[str, Any]]] = []
        self.refusal: str | None = None

    def register_session(self, **kwargs: Any) -> None:
        self._record("established_session", kwargs)

    def register_establishment_session(self, **kwargs: Any) -> None:
        self._record("establishing_session", kwargs)

    def _record(self, kind: str, kwargs: dict[str, Any]) -> None:
        self.calls.append((kind, kwargs))
        if self.refusal is not None:
            raise ObservationRegistrationRefusedError("refused", reason=self.refusal)


def _queue_dependencies(settings: Any, conn: ScriptedConnection) -> OperationDependencies:
    """Queue-mode operation dependencies whose pool is the scripted connection."""
    return OperationDependencies(
        settings=settings,
        database_pool_provider=lambda _settings: _pool(conn),
    )


def _queue_client(
    settings: Any, conn: ScriptedConnection, monkeypatch: Any
) -> tuple[TestClient, Any]:
    """A serving queue-mode app whose lifespan reconciliation finds no candidates."""
    _script_restart_candidates(conn, [])
    monkeypatch.setattr(
        app_module,
        "build_default_operation_dependencies",
        lambda settings: _queue_dependencies(settings, conn),
        raising=True,
    )
    app = create_app(settings)
    return TestClient(app), app


def _testclient_transport(client: TestClient) -> Any:
    """The fake two-process transport: the worker proxy's exchange against
    this test's serving app, addressed by path only."""

    def transport(request: urllib.request.Request, timeout: float) -> tuple[int, bytes]:
        path = urllib.parse.urlparse(request.full_url).path
        response = client.post(path, content=request.data, headers=dict(request.headers.items()))
        return response.status_code, response.content

    return transport


def _identities(external_session_id: str) -> dict[str, Any]:
    return {
        "workspace_id": _WORKSPACE_ID,
        "task_id": _TASK_ID,
        "task_agent_session_id": _SESSION_ID,
        "connection_id": uuid4(),
        "external_session_id": external_session_id,
    }


@pytest.mark.parametrize(
    "authorization",
    [
        None,
        "",
        "Basic c29tZXRoaW5n",
        "Bearer ",
        "Bearer wrong-deployment-secret",
        "bearer deployment-registration-secret",
    ],
)
def test_every_invalid_credential_is_the_same_uniform_detail_free_rejection(
    settings_factory: Any, monkeypatch: Any, authorization: str | None
) -> None:
    conn = ScriptedConnection()
    settings = settings_factory(**_QUEUE_SETTINGS)
    client, _app = _queue_client(settings, conn, monkeypatch)
    with client:
        headers = {} if authorization is None else {"Authorization": authorization}
        response = client.post(
            "/internal/observer/registrations", json=_registration_body(), headers=headers
        )

    assert response.status_code == 401
    assert response.json()["detail"] == (
        "authentication failed: the deployment registration credential is missing or invalid"
    )
    # No credential material is ever reflected back.
    assert _REGISTRATION_SECRET not in response.text


def test_an_unconfigured_registration_secret_fails_closed(settings_factory: Any) -> None:
    # A regular-mode serving process has no registration secret: the route
    # refuses loudly rather than accepting unverified registrations.
    with TestClient(create_app(settings_factory())) as client:
        response = client.post("/internal/observer/registrations", json=_registration_body())

    assert response.status_code == 503
    assert "registration secret is not configured" in response.json()["detail"]


def test_malformed_registration_shapes_are_rejected(
    settings_factory: Any, monkeypatch: Any
) -> None:
    conn = ScriptedConnection()
    settings = settings_factory(**_QUEUE_SETTINGS)
    client, _app = _queue_client(settings, conn, monkeypatch)
    with client:
        assert (
            client.post(
                "/internal/observer/registrations",
                json={**_registration_body(), "unexpected": "field"},
                headers=_REGISTRATION_HEADERS,
            ).status_code
            == 422
        )
        assert (
            client.post(
                "/internal/observer/registrations",
                json={**_registration_body(), "workspace_id": "not-a-uuid"},
                headers=_REGISTRATION_HEADERS,
            ).status_code
            == 422
        )
        assert (
            client.post(
                "/internal/observer/registrations",
                json={**_registration_body(), "kind": "mystery_kind"},
                headers=_REGISTRATION_HEADERS,
            ).status_code
            == 422
        )


def test_a_valid_established_registration_is_positively_acknowledged(
    settings_factory: Any, monkeypatch: Any
) -> None:
    conn = ScriptedConnection()
    settings = settings_factory(**_QUEUE_SETTINGS)
    registrar = _RecordingRegistrar()
    body = _registration_body()
    client, app = _queue_client(settings, conn, monkeypatch)
    with client:
        app.state.detached_turn_observer = registrar
        response = client.post(
            "/internal/observer/registrations",
            json=body,
            headers=_REGISTRATION_HEADERS,
        )

    assert response.status_code == 200
    assert response.json() == {"acknowledged": True}
    assert registrar.calls == [
        (
            "established_session",
            {
                "workspace_id": UUID(body["workspace_id"]),
                "task_id": UUID(body["task_id"]),
                "task_agent_session_id": UUID(body["task_agent_session_id"]),
                "connection_id": UUID(body["connection_id"]),
                "external_session_id": body["external_session_id"],
            },
        )
    ]


def test_a_valid_establishment_registration_routes_to_the_establishment_gate(
    settings_factory: Any, monkeypatch: Any
) -> None:
    conn = ScriptedConnection()
    settings = settings_factory(**_QUEUE_SETTINGS)
    registrar = _RecordingRegistrar()
    body = _registration_body(kind="establishing_session", external_session_id="ext-init-9")
    client, app = _queue_client(settings, conn, monkeypatch)
    with client:
        app.state.detached_turn_observer = registrar
        response = client.post(
            "/internal/observer/registrations",
            json=body,
            headers=_REGISTRATION_HEADERS,
        )

    assert response.status_code == 200
    assert response.json() == {"acknowledged": True}
    assert registrar.calls == [
        (
            "establishing_session",
            {
                "workspace_id": UUID(body["workspace_id"]),
                "task_id": UUID(body["task_id"]),
                "task_agent_session_id": UUID(body["task_agent_session_id"]),
                "connection_id": UUID(body["connection_id"]),
                "external_session_id": body["external_session_id"],
            },
        )
    ]


def test_a_typed_refusal_maps_to_its_safe_reason_token(
    settings_factory: Any, monkeypatch: Any
) -> None:
    conn = ScriptedConnection()
    settings = settings_factory(**_QUEUE_SETTINGS)
    registrar = _RecordingRegistrar()
    registrar.refusal = "session_binding_unresolved"
    client, app = _queue_client(settings, conn, monkeypatch)
    with client:
        app.state.detached_turn_observer = registrar
        response = client.post(
            "/internal/observer/registrations",
            json=_registration_body(),
            headers=_REGISTRATION_HEADERS,
        )

    assert response.status_code == 409
    assert response.json() == {
        "acknowledged": False,
        "refusal_reason": "session_binding_unresolved",
    }


def test_an_unavailable_host_refuses_positively_instead_of_accepting(
    settings_factory: Any, monkeypatch: Any
) -> None:
    conn = ScriptedConnection()
    settings = settings_factory(**_QUEUE_SETTINGS)
    client, app = _queue_client(settings, conn, monkeypatch)
    with client:
        app.state.detached_turn_observer = None
        response = client.post(
            "/internal/observer/registrations",
            json=_registration_body(),
            headers=_REGISTRATION_HEADERS,
        )

    assert response.status_code == 409
    assert response.json() == {"acknowledged": False, "refusal_reason": "observer_unavailable"}


def test_lifespan_hosts_exactly_one_observer_in_queue_mode(
    settings_factory: Any, monkeypatch: Any
) -> None:
    conn = ScriptedConnection()
    settings = settings_factory(**_QUEUE_SETTINGS)
    app = create_app(settings)
    _script_restart_candidates(conn, [])
    monkeypatch.setattr(
        app_module,
        "build_default_operation_dependencies",
        lambda settings: _queue_dependencies(settings, conn),
        raising=True,
    )

    with TestClient(app) as client:
        # No executor is ever created in queue mode; the one long-lived
        # detached-turn observation host is hosted by the serving
        # API/control-plane process, never inside a workhorse (issue #346).
        assert app.state.operation_executor is None
        assert isinstance(app.state.operation_submitter, RQQueueOperationSubmission)
        observer = app.state.detached_turn_observer
        assert isinstance(observer, DetachedTurnObservationHost)
        assert observer.disposed is False
        assert client.get("/healthz").status_code == 200

    assert observer.disposed is True
    shutdown_observability()


def test_the_worker_proxy_attests_the_hosted_registration_before_any_admission(
    settings_factory: Any, monkeypatch: Any
) -> None:
    conn = ScriptedConnection()
    settings = settings_factory(**_QUEUE_SETTINGS)
    _script_bindings(conn, _BindingRow(rows={_EXTERNAL_SESSION: _binding_row()}))
    _script_outstanding_receipts(conn, _ReceiptRow(rows={_SESSION_ID: _admitted_receipt_row()}))
    backend = FakeClineSdkBackend()
    settle = _SettleRecorder(outcome_kind=TurnSettlementKind.SETTLED_VALIDATED_RESULT)
    monkeypatch.setattr(observation_module, "settle_observed_turn_completion", settle, raising=True)

    client, app = _queue_client(settings, conn, monkeypatch)
    with client:
        # The dispatch worker's remote registrar, transported against this
        # test's serving app: the focused fake two-process seam (issue #346).
        registrar = QueueDetachedTurnObservationRegistrar(
            registration_url="http://control-plane.test",
            registration_secret=_REGISTRATION_SECRET,
            transport=_testclient_transport(client),
        )
        # The hosting process's scripted observation host: the exact session
        # is actively observed in the API/control-plane process only.
        hosted = _make_observer(conn, backends=[backend])
        app.state.detached_turn_observer = hosted

        # Registration-before-admission: the positive acknowledgement unlocks
        # the (fake) detached admission; the request being sent was never
        # treated as success.
        registrar.register_session(**_identities(_EXTERNAL_SESSION))
        admissions = ["enqueued"]

        # The dispatching workhorse is now gone: the independently delivered
        # terminal observation settles through the shared seam in the hosting
        # process, with no further involvement of the registering worker.
        backend.emit_hub(_terminal_event())

    assert admissions == ["enqueued"]
    assert [call.session_id for call in _subscribe_calls(backend)] == [_EXTERNAL_SESSION]
    assert len(settle.calls) == 1


def test_a_refused_registration_never_admits(settings_factory: Any, monkeypatch: Any) -> None:
    conn = ScriptedConnection()
    settings = settings_factory(**_QUEUE_SETTINGS)
    _script_bindings(conn, _BindingRow(rows={}))  # no binding resolves anything
    _script_outstanding_receipts(conn, _ReceiptRow(rows={}))
    backend = FakeClineSdkBackend()
    settle = _SettleRecorder(outcome_kind=TurnSettlementKind.SETTLED_VALIDATED_RESULT)
    monkeypatch.setattr(observation_module, "settle_observed_turn_completion", settle, raising=True)

    client, app = _queue_client(settings, conn, monkeypatch)
    with client:
        registrar = QueueDetachedTurnObservationRegistrar(
            registration_url="http://control-plane.test",
            registration_secret=_REGISTRATION_SECRET,
            transport=_testclient_transport(client),
        )
        app.state.detached_turn_observer = _make_observer(conn, backends=[backend])

        with pytest.raises(ObservationRegistrationRefusedError) as refused:
            registrar.register_session(**_identities(_EXTERNAL_SESSION))

    # The typed refusal carried the hosting host's own safe reason, and the
    # detached admission (the native enqueue) never happened.
    assert refused.value.reason == "session_binding_unresolved"
    assert settle.calls == []
    assert backend.calls() == ()
