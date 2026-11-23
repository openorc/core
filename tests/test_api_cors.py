"""Tests for the optional deployment-configured browser-CORS mechanism (issue #369).

CORS is HTTP mechanism only: it is off unless an explicit origin allow-list is
configured. Disabled means no CORS headers for any origin; enabled grants
headers only to configured origins (preflight included), exposes the
correlation request-ID header to allowed browser clients, and never sets
allow-credentials (Bearer-token authentication is not CORS-credentialed).
"""

from __future__ import annotations

from typing import Any

from fastapi.testclient import TestClient

from openorc.api.app import create_app

_ALLOWED_ORIGIN = "https://app.example.com"


def _client(settings: Any) -> TestClient:
    return TestClient(create_app(settings))


def test_cors_is_disabled_by_default(settings_factory) -> None:
    client = _client(settings_factory())

    response = client.get("/healthz", headers={"Origin": _ALLOWED_ORIGIN})

    assert response.status_code == 200
    assert "access-control-allow-origin" not in response.headers


def test_a_configured_origin_receives_the_cors_headers(settings_factory) -> None:
    client = _client(settings_factory(cors_allowed_origins=(_ALLOWED_ORIGIN,)))

    response = client.get("/healthz", headers={"Origin": _ALLOWED_ORIGIN})

    assert response.status_code == 200
    assert response.headers["access-control-allow-origin"] == _ALLOWED_ORIGIN
    # The correlation request ID stays visible to the allowed browser client.
    assert "x-openorc-request-id" in response.headers["access-control-expose-headers"]
    assert "x-openorc-request-id" in response.headers
    # Bearer-token authentication is not CORS-credentialed.
    assert "access-control-allow-credentials" not in response.headers


def test_an_unconfigured_origin_receives_no_cors_headers(settings_factory) -> None:
    client = _client(settings_factory(cors_allowed_origins=(_ALLOWED_ORIGIN,)))

    response = client.get("/healthz", headers={"Origin": "https://other.example.net"})

    assert response.status_code == 200
    assert "access-control-allow-origin" not in response.headers


def test_a_preflight_from_a_configured_origin_is_short_circuited(settings_factory) -> None:
    client = _client(settings_factory(cors_allowed_origins=(_ALLOWED_ORIGIN,)))

    preflight = client.options(
        "/healthz",
        headers={
            "Origin": _ALLOWED_ORIGIN,
            "Access-Control-Request-Method": "PUT",
        },
    )

    assert preflight.status_code == 200
    assert preflight.headers["access-control-allow-origin"] == _ALLOWED_ORIGIN
    allowed_methods = preflight.headers["access-control-allow-methods"].replace(" ", "")
    assert {"GET", "POST", "PUT"} <= set(allowed_methods.split(","))
    assert "authorization" in preflight.headers["access-control-allow-headers"].lower()
    # The preflight is browser mechanics, not an application request: it
    # short-circuits before the correlation middleware, so no application
    # request ID is minted for it.
    assert "x-openorc-request-id" not in preflight.headers
