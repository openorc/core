"""Tests for the Profile's GitHub user-authorization transport (issue #373).

Route-level coverage of the HTTP mapping: transport thinness — the routes
delegate every decision to the patched application-service boundaries with
the exact command values — the credential-free read models (no reference,
verifier, correlation-state, or token field anywhere in any response), the
uniform 401 isolation outcome, the documented typed-error mapping, the
unconfigured-deployment fail-closed 503 on the flow-start surfaces, and the
wire-shape rejections that never reach the services. The flow-service
behavior matrices stay in the issue #373 service tests.
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
from fastapi.testclient import TestClient

from openorc.api.app import create_app
from openorc.api.dependencies import authentication as authentication_dependency
from openorc.api.routers import github_user_authorization as user_authorization_router
from openorc.domain.github_user_authorization import (
    GitHubUserAuthorization,
    GitHubUserAuthorizationStatus,
)
from openorc.domain.github_user_authorization_flows import (
    GitHubUserAuthorizationFlowPurpose,
)
from openorc.domain.ownership import Profile
from openorc.services import github_user_authorization_flows
from openorc.services.errors import (
    AuthenticationError,
    ConflictError,
    IntegrationNotConfiguredError,
    NotFoundError,
)

_OBSERVED = datetime(2026, 11, 23, 12, 0, 0, tzinfo=UTC)
_EXPIRES = _OBSERVED + timedelta(seconds=600)
_AUTHORIZE_URL = (
    "https://github.com/login/oauth/authorize"
    "?client_id=Iv1.x&state=correlation-state&code_challenge=challenge&code_challenge_method=S256"
)


class _ServiceSeam:
    def __init__(self) -> None:
        self.statuses: list[dict[str, Any]] = []
        self.starts: list[dict[str, Any]] = []
        self.revokes: list[dict[str, Any]] = []
        self.pool_acquisitions = 0


@pytest.fixture
def service_seam(monkeypatch: pytest.MonkeyPatch):
    def _make(
        *,
        status_result: Any = None,
        status_error: Exception | None = None,
        start_result: Any = None,
        start_error: Exception | None = None,
        revoke_result: Any = None,
        revoke_error: Exception | None = None,
    ) -> _ServiceSeam:
        seam = _ServiceSeam()

        def _fake_get_database_pool(settings: Any) -> object:
            seam.pool_acquisitions += 1
            return object()

        def _status(pool: Any, *, profile_id: Any) -> Any:
            seam.statuses.append({"profile_id": profile_id})
            if status_error is not None:
                raise status_error
            assert status_result is not None
            return status_result

        def _start(pool: Any, **command: Any) -> Any:
            seam.starts.append(command)
            if start_error is not None:
                raise start_error
            assert start_result is not None
            return start_result

        def _revoke(pool: Any, *, profile_id: Any) -> Any:
            seam.revokes.append({"profile_id": profile_id})
            if revoke_error is not None:
                raise revoke_error
            assert revoke_result is not None
            return revoke_result

        monkeypatch.setattr(user_authorization_router, "get_database_pool", _fake_get_database_pool)
        monkeypatch.setattr(
            github_user_authorization_flows,
            "read_github_user_authorization_status",
            _status,
        )
        monkeypatch.setattr(
            github_user_authorization_flows,
            "start_github_user_authorization_flow",
            _start,
        )
        monkeypatch.setattr(
            github_user_authorization_flows,
            "revoke_profile_github_user_authorization",
            _revoke,
        )
        return seam

    return _make


@pytest.fixture
def user_flow_composition(monkeypatch: pytest.MonkeyPatch):
    """The per-request user-flow composition seam (configured by default)."""

    def _make(*, configured: bool = True) -> list[dict[str, Any]]:
        calls: list[dict[str, Any]] = []

        class _Composition:
            user_flow_client_id = "Iv1.user-flow-client-id"
            admin_client = object()
            token_client = object()

        def _resolve(settings: Any) -> Any:
            calls.append({"configured": configured})
            if not configured:
                raise IntegrationNotConfiguredError(
                    "the GitHub App user-flow credentials are not configured for this process"
                )
            return _Composition()

        monkeypatch.setattr(user_authorization_router, "resolve_user_flow_composition", _resolve)
        return calls

    return _make


@pytest.fixture
def owner_profile() -> Profile:
    return Profile(id=uuid.uuid4(), created_at=_OBSERVED)


def _client(settings: Any, owner_profile: Profile) -> TestClient:
    app = create_app(settings)
    app.dependency_overrides[authentication_dependency.authenticated_profile] = lambda: (
        owner_profile
    )
    return TestClient(app)


# --- status read -------------------------------------------------------------


def test_status_delegates_and_projects_the_safe_read_model(
    service_seam, settings_factory, owner_profile
) -> None:
    status = github_user_authorization_flows.GitHubUserAuthorizationStatusRead(
        status="active",
        github_user_id=5432,
        github_login="octocat",
        authorized_at=_OBSERVED,
        refresh_expires_at=_OBSERVED,
        pending_flow=github_user_authorization_flows.GitHubUserAuthorizationPendingFlow(
            purpose=GitHubUserAuthorizationFlowPurpose.AUTHORIZE,
            started_at=_OBSERVED,
            expires_at=_EXPIRES,
        ),
    )
    seam = service_seam(status_result=status)
    client = _client(settings_factory(), owner_profile)

    response = client.get("/api/v1/profile/github-user-authorization")

    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "active"
    assert body["github_user_id"] == 5432
    assert body["github_login"] == "octocat"
    assert body["pending_flow"]["purpose"] == "authorize"
    # Structurally credential-free: no reference, verifier, correlation
    # state, generation, or count field exists on the read model.
    for forbidden in (
        "refresh_secret_reference",
        "pkce_verifier",
        "pkce_verifier_reference",
        "state",
        "refresh_generation",
        "revocation_count",
        "token",
    ):
        assert forbidden not in body
        assert forbidden not in body["pending_flow"]
    assert seam.statuses == [{"profile_id": owner_profile.id}]


def test_start_returns_the_authorize_url_and_delegates_the_authorize_purpose(
    service_seam, user_flow_composition, settings_factory, owner_profile
) -> None:
    start = github_user_authorization_flows.GitHubUserAuthorizationFlowStart(
        authorize_url=_AUTHORIZE_URL,
        purpose=GitHubUserAuthorizationFlowPurpose.AUTHORIZE,
        expires_at=_EXPIRES,
    )
    seam = service_seam(start_result=start)
    composition_calls = user_flow_composition()
    client = _client(settings_factory(), owner_profile)

    response = client.post("/api/v1/profile/github-user-authorization/flows")

    assert response.status_code == 201
    body = response.json()
    assert body["authorize_url"] == _AUTHORIZE_URL
    assert body["purpose"] == "authorize"
    # The authorize URL is the only projected correlation material; no
    # verifier, client-secret, or reference field exists on the model.
    for forbidden in ("code_verifier", "client_secret", "pkce_verifier_reference"):
        assert forbidden not in body
    assert composition_calls == [{"configured": True}]
    assert seam.starts == [
        {
            "profile_id": owner_profile.id,
            "purpose": GitHubUserAuthorizationFlowPurpose.AUTHORIZE,
            "user_flow_client_id": "Iv1.user-flow-client-id",
        }
    ]


# --- reauthorization, revoke, and the error mapping --------------------------


def test_reauthorization_delegates_the_reauthorize_purpose(
    service_seam, user_flow_composition, settings_factory, owner_profile
) -> None:
    start = github_user_authorization_flows.GitHubUserAuthorizationFlowStart(
        authorize_url=_AUTHORIZE_URL,
        purpose=GitHubUserAuthorizationFlowPurpose.REAUTHORIZE,
        expires_at=_EXPIRES,
    )
    seam = service_seam(start_result=start)
    user_flow_composition()
    client = _client(settings_factory(), owner_profile)

    response = client.post("/api/v1/profile/github-user-authorization/reauthorization")

    assert response.status_code == 201
    assert response.json()["purpose"] == "reauthorize"
    assert seam.starts == [
        {
            "profile_id": owner_profile.id,
            "purpose": GitHubUserAuthorizationFlowPurpose.REAUTHORIZE,
            "user_flow_client_id": "Iv1.user-flow-client-id",
        }
    ]


def test_revoke_maps_the_durable_revocation(service_seam, settings_factory, owner_profile) -> None:
    revoked = GitHubUserAuthorization(
        profile_id=owner_profile.id,
        github_user_id=5432,
        github_login="octocat",
        status=GitHubUserAuthorizationStatus.REVOKED,
        refresh_secret_reference=None,
        refresh_expires_at=None,
        refresh_generation=8,
        revocation_count=4,
        authorized_at=_OBSERVED,
        revoked_at=_OBSERVED,
        created_at=_OBSERVED,
        updated_at=_OBSERVED,
    )
    seam = service_seam(revoke_result=revoked)
    client = _client(settings_factory(), owner_profile)

    response = client.delete("/api/v1/profile/github-user-authorization")

    assert response.status_code == 200
    body = response.json()
    assert body["github_user_id"] == revoked.github_user_id
    assert body["revoked_at"] is not None
    # No credential reference, fence, or generation field exists on the
    # revoked read model.
    for forbidden in ("refresh_secret_reference", "revocation_count", "refresh_generation"):
        assert forbidden not in body
    assert seam.revokes == [{"profile_id": owner_profile.id}]


def test_typed_application_errors_map_to_the_documented_statuses(
    service_seam, user_flow_composition, settings_factory, owner_profile
) -> None:
    seam = service_seam(
        status_error=IntegrationNotConfiguredError(
            "the Supabase project URL is not configured for this process"
        ),
        start_error=ConflictError("an active GitHub authorization already exists for this Profile"),
        revoke_error=NotFoundError("this Profile has no GitHub authorization to revoke"),
    )
    assert seam.pool_acquisitions == 0
    user_flow_composition()
    client = _client(settings_factory(), owner_profile)

    status_response = client.get("/api/v1/profile/github-user-authorization")
    start_response = client.post("/api/v1/profile/github-user-authorization/flows")
    reauth_response = client.post("/api/v1/profile/github-user-authorization/reauthorization")
    revoke_response = client.delete("/api/v1/profile/github-user-authorization")

    assert status_response.status_code == 503
    assert start_response.status_code == 409
    assert reauth_response.status_code == 409
    assert revoke_response.status_code == 404


def test_unconfigured_user_flow_credentials_fail_the_start_surfaces_closed(
    service_seam, user_flow_composition, settings_factory, owner_profile
) -> None:
    seam = service_seam()
    user_flow_composition(configured=False)
    client = _client(settings_factory(), owner_profile)

    start_response = client.post("/api/v1/profile/github-user-authorization/flows")
    reauth_response = client.post("/api/v1/profile/github-user-authorization/reauthorization")

    assert start_response.status_code == 503
    assert reauth_response.status_code == 503
    # The services were never reached: the composition boundary failed first.
    assert seam.starts == []


def test_missing_or_malformed_credentials_are_a_uniform_401(settings_factory) -> None:
    app = create_app(settings_factory(supabase_url="https://supabase.example.com"))

    def _fail() -> None:
        raise AuthenticationError("authentication failed: the access token is missing or malformed")

    app.dependency_overrides[authentication_dependency.authenticated_profile] = _fail
    client = TestClient(app)

    response = client.get("/api/v1/profile/github-user-authorization")

    assert response.status_code == 401
    assert response.json()["detail"] == (
        "authentication failed: the access token is missing or malformed"
    )


# --- the provider callback ingress -------------------------------------------


@pytest.fixture
def callback_seam(monkeypatch: pytest.MonkeyPatch):
    """Seam for the unauthenticated provider callback route."""

    def _make(
        *,
        complete_error: Exception | None = None,
        composition_configured: bool = True,
    ) -> dict[str, list[Any]]:
        recorded: dict[str, list[Any]] = {"completions": [], "aborts": [], "compositions": []}

        def _fake_get_database_pool(settings: Any) -> object:
            return object()

        def _complete(pool: Any, **command: Any) -> Any:
            recorded["completions"].append(command)
            if complete_error is not None:
                raise complete_error
            return object()

        def _abort(pool: Any, *, state: str) -> None:
            recorded["aborts"].append({"state": state})

        def _resolve(settings: Any) -> Any:
            recorded["compositions"].append({})
            if not composition_configured:
                raise IntegrationNotConfiguredError(
                    "the GitHub App user-flow credentials are not configured for this process"
                )

            class _Composition:
                admin_client = object()
                token_client = object()

            return _Composition()

        import openorc.api.routers.github_user_authorization_callback as callback_router

        monkeypatch.setattr(callback_router, "get_database_pool", _fake_get_database_pool)
        monkeypatch.setattr(
            github_user_authorization_flows,
            "complete_github_user_authorization_flow",
            _complete,
        )
        monkeypatch.setattr(
            github_user_authorization_flows,
            "abort_pending_github_user_authorization_flow",
            _abort,
        )
        monkeypatch.setattr(callback_router, "resolve_user_flow_composition", _resolve)
        return recorded

    return _make


_CALLBACK = "/api/github/user-authorization/callback"


def test_the_callback_completes_the_flow_and_answers_the_safe_success_landing(
    callback_seam, settings_factory
) -> None:
    recorded = callback_seam()
    app = create_app(settings_factory())
    client = TestClient(app)

    response = client.get(
        _CALLBACK, params={"code": "the-oauth-code", "state": "the-correlation-state"}
    )

    assert response.status_code == 200
    assert "GitHub authorization completed" in response.text
    assert response.headers["content-type"].startswith("text/html")
    assert response.headers["cache-control"] == "no-store"
    # The callback completes without authentication and never reflects any
    # query value into the response body.
    assert "the-oauth-code" not in response.text
    assert "the-correlation-state" not in response.text
    assert recorded["completions"] == [
        {
            "state": "the-correlation-state",
            "code": "the-oauth-code",
            "admin_client": recorded["completions"][0]["admin_client"],
            "token_client": recorded["completions"][0]["token_client"],
        }
    ]


def test_the_provider_terminated_flow_aborts_and_answers_the_uniform_failure_landing(
    callback_seam, settings_factory
) -> None:
    recorded = callback_seam()
    app = create_app(settings_factory())
    client = TestClient(app)

    response = client.get(_CALLBACK, params={"error": "access_denied", "state": "the-state"})

    assert response.status_code == 400
    assert "GitHub authorization was not completed" in response.text
    # GitHub's error value is never echoed anywhere.
    assert "access_denied" not in response.text
    assert recorded["aborts"] == [{"state": "the-state"}]
    assert recorded["completions"] == []


def test_the_callback_answers_one_uniform_failure_for_typed_application_errors(
    callback_seam, settings_factory
) -> None:
    recorded = callback_seam(
        complete_error=github_user_authorization_flows.GitHubUserAuthorizationFlowUnavailableError(
            condition="unavailable",
            message="the GitHub authorization flow cannot be completed",
        )
    )
    app = create_app(settings_factory())
    client = TestClient(app)

    response = client.get(_CALLBACK, params={"code": "code", "state": "state"})

    assert response.status_code == 400
    assert "GitHub authorization was not completed" in response.text
    assert recorded["completions"] != []


def test_the_callback_rejects_unusable_inputs_without_any_service_call(
    callback_seam, settings_factory
) -> None:
    recorded = callback_seam()
    app = create_app(settings_factory())
    client = TestClient(app)

    missing_state = client.get(_CALLBACK, params={"code": "code"})
    missing_code = client.get(_CALLBACK, params={"state": "state"})
    oversized_state = client.get(_CALLBACK, params={"code": "code", "state": "s" * 2049})
    no_parameters = client.get(_CALLBACK)

    for response in (missing_state, missing_code, oversized_state, no_parameters):
        assert response.status_code == 400
        assert "was not completed" in response.text
    assert recorded["completions"] == []
    assert recorded["aborts"] == []
    assert recorded["compositions"] == []


def test_the_unconfigured_deployment_fails_the_callback_closed(
    callback_seam, settings_factory
) -> None:
    callback_seam(composition_configured=False)
    app = create_app(settings_factory())
    client = TestClient(app)

    response = client.get(_CALLBACK, params={"code": "code", "state": "state"})

    # The typed integration-unconfigured failure is an ApplicationError, so
    # the browser still receives the uniform failure landing.
    assert response.status_code == 400
    assert "was not completed" in response.text
