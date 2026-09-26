"""Tests for the thin GitHub webhook transport (issue #61).

Route-level coverage of the HTTP mapping: exact raw-body fidelity through the
seam, uniform detail-free 401 signature rejection, 400 header validation,
204 acknowledgement of accepted/duplicate/ignored/unusable outcomes, and the
fail-closed 503 when the webhook secret is unconfigured. The route is proven
thin: it delegates to the (patched) intake service and contains no
reconciliation logic of its own.
"""

from __future__ import annotations

import hashlib
import hmac
import uuid
from datetime import UTC, datetime
from typing import Any

import pytest
from fastapi.testclient import TestClient

from openorc.api.app import create_app
from openorc.config import Settings
from openorc.domain.github_webhooks import (
    GitHubWebhookDelivery,
    GitHubWebhookDeliveryClassification,
    GitHubWebhookRoutingResolution,
    GitHubWebhookRoutingTarget,
)
from openorc.services.errors import (
    AuthenticationError,
    IntegrationNotConfiguredError,
)
from openorc.services.github_webhook_intake import GitHubWebhookIntake

SECRET = "test-webhook-secret"
BODY = (
    b'{"action": "edited", "installation": {"id": 1}, '
    b'"repository": {"id": 2}, "issue": {"number": 3}}'
)


def _signature(body: bytes) -> str:
    return "sha256=" + hmac.new(SECRET.encode(), body, hashlib.sha256).hexdigest()


def _settings(*, with_secret: bool = True) -> Settings:
    return Settings(
        environment="test",
        api_host="127.0.0.1",
        api_port=3999,
        api_reload=False,
        valkey_url="redis://127.0.0.1:6379/0",
        database_url="postgresql://postgres:postgres@127.0.0.1:54322/postgres",
        db_pool_min=1,
        db_pool_max=5,
        db_pool_timeout=5.0,
        github_webhook_secret=SECRET if with_secret else None,
    )


@pytest.fixture
def intake_calls(monkeypatch: pytest.MonkeyPatch):
    calls: list[dict[str, Any]] = []

    def _make(result: Any = None, error: Exception | None = None) -> list[dict[str, Any]]:
        def _intake(
            pool: Any,
            settings: Settings,
            *,
            raw_body: bytes,
            signature_header: str | None,
            event_name: str,
            delivery_guid: str,
        ) -> Any:
            calls.append(
                {
                    "raw_body": raw_body,
                    "signature_header": signature_header,
                    "event_name": event_name,
                    "delivery_guid": delivery_guid,
                    "settings": settings,
                }
            )
            if error is not None:
                raise error
            return result

        monkeypatch.setattr("openorc.api.routers.github_webhooks.intake_github_webhook", _intake)
        return calls

    return _make


def _client(settings: Settings) -> TestClient:
    return TestClient(create_app(settings), raise_server_exceptions=False)


def _headers(*, signature: str | None = _signature(BODY)) -> dict[str, str]:
    headers = {"X-GitHub-Event": "issues", "X-GitHub-Delivery": "guid-1"}
    if signature is not None:
        headers["X-Hub-Signature-256"] = signature
    return headers


def test_the_route_passes_the_exact_raw_body_and_headers_to_the_service(intake_calls) -> None:
    calls = intake_calls()
    client = _client(_settings())

    response = client.post(
        "/api/github/webhooks", content=BODY, headers=_headers(signature=_signature(BODY))
    )

    assert response.status_code == 204
    assert len(calls) == 1
    # Exact raw bytes: no re-serialization or normalization.
    assert calls[0]["raw_body"] == BODY
    assert calls[0]["signature_header"] == _signature(BODY)
    assert calls[0]["event_name"] == "issues"
    assert calls[0]["delivery_guid"] == "guid-1"
    assert calls[0]["settings"].github_webhook_secret == SECRET


@pytest.mark.parametrize("signature", [None, "sha256=" + "a" * 64, "bogus"])
def test_signature_failures_map_to_a_uniform_401(intake_calls, signature: str | None) -> None:
    intake_calls(error=AuthenticationError("rejected"))
    client = _client(_settings())

    response = client.post(
        "/api/github/webhooks", content=BODY, headers=_headers(signature=signature)
    )

    assert response.status_code == 401
    # Uniform, detail-free rejection: no probing detail leaves the boundary.
    assert response.content == b""


def test_missing_delivery_or_event_headers_map_to_400(intake_calls) -> None:
    intake_calls()
    client = _client(_settings())
    signature = _signature(BODY)

    missing_delivery = client.post(
        "/api/github/webhooks",
        content=BODY,
        headers={"X-Hub-Signature-256": signature, "X-GitHub-Event": "issues"},
    )
    missing_event = client.post(
        "/api/github/webhooks",
        content=BODY,
        headers={"X-Hub-Signature-256": signature, "X-GitHub-Delivery": "guid-1"},
    )

    assert missing_delivery.status_code == 400
    assert missing_event.status_code == 400


def test_an_unconfigured_webhook_secret_maps_to_503(intake_calls) -> None:
    intake_calls(error=IntegrationNotConfiguredError("not configured"))
    client = _client(_settings(with_secret=False))

    response = client.post("/api/github/webhooks", content=BODY, headers=_headers())

    assert response.status_code == 503


def test_accepted_duplicate_ignored_and_unusable_all_acknowledge_204(intake_calls) -> None:
    delivery = GitHubWebhookDelivery(
        id=uuid.uuid4(),
        delivery_guid="guid-1",
        event_name="issues",
        action="edited",
        classification=GitHubWebhookDeliveryClassification.RELEVANT,
        routing_target=GitHubWebhookRoutingTarget.ISSUE_STATE,
        routing_resolution=GitHubWebhookRoutingResolution.RESOLVED,
        github_installation_id=1,
        github_repository_id=2,
        github_issue_number=3,
        github_pull_request_number=None,
        received_at=datetime.now(UTC),
    )
    client = _client(_settings())
    outcomes = (
        ("accepted", GitHubWebhookIntake(delivery=delivery)),
        ("duplicate", GitHubWebhookIntake(delivery=None, duplicate=True)),
        ("ignored", GitHubWebhookIntake(delivery=delivery, ignored=True)),
        ("unusable", GitHubWebhookIntake(delivery=delivery, unusable=True)),
    )

    for name, outcome in outcomes:
        calls = intake_calls(result=outcome)

        response = client.post("/api/github/webhooks", content=BODY, headers=_headers())

        assert response.status_code == 204, name
        assert len(calls) == 1
        calls.clear()


def test_the_route_registers_on_the_application(intake_calls) -> None:
    intake_calls()
    client = _client(_settings())

    response = client.post("/api/github/webhooks", content=b"")

    # The route exists (bad headers -> 400, not 404).
    assert response.status_code == 400
