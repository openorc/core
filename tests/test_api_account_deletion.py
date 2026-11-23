"""Tests for the authenticated account-deletion transport (issue #374).

Route-level coverage of the HTTP mapping: transport thinness — the route
delegates to the patched issue #97 account-lifecycle service with the exact
authenticated Profile id and the per-request composed Supabase Auth Admin
boundary — the truthful ``deleted``/``already_absent`` outcome bodies that
carry nothing but the Profile identity and the result, the guarded
single-flight conflict, the honest 503s for known identity-provider
failures, unknown outcomes, and the unconfigured-deployment fail-closed
boundary, and a representative uniform 401. The account-deletion
state-machine and reconciliation matrices stay in the issue #97 service
tests (``test_services_account_lifecycle.py``) and are not re-proven here.
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime
from typing import Any

import pytest
from fastapi.testclient import TestClient

from openorc.api.app import create_app
from openorc.api.dependencies import authentication as authentication_dependency
from openorc.api.routers import account as account_router
from openorc.domain.ownership import Profile
from openorc.services import account_lifecycle
from openorc.services.account_lifecycle import AccountDeletionOutcome
from openorc.services.errors import (
    AuthenticationError,
    ConflictError,
    ExternalOperationFailedError,
    ExternalOperationUncertainError,
    IntegrationNotConfiguredError,
)

_OBSERVED = datetime(2026, 11, 23, 12, 0, 0, tzinfo=UTC)


@pytest.fixture
def owner_profile() -> Profile:
    return Profile(id=uuid.uuid4(), created_at=_OBSERVED)


class _ServiceSeam:
    """The patched issue #97 boundary and its deployment composition."""

    def __init__(self) -> None:
        self.deletions: list[dict[str, Any]] = []
        self.compositions: list[dict[str, Any]] = []
        self.pool_acquisitions = 0
        self.admin_client: Any = None


@pytest.fixture
def deletion_seam(monkeypatch: pytest.MonkeyPatch):
    def _make(
        *,
        outcome: AccountDeletionOutcome | None = None,
        deletion_error: Exception | None = None,
        configured: bool = True,
    ) -> _ServiceSeam:
        seam = _ServiceSeam()
        admin_client = object()
        seam.admin_client = admin_client

        def _fake_get_database_pool(settings: Any) -> object:
            seam.pool_acquisitions += 1
            return object()

        def _resolve(settings: Any) -> Any:
            seam.compositions.append({"configured": configured})
            if not configured:
                raise IntegrationNotConfiguredError(
                    "the Supabase Auth Admin boundary is not configured for this process"
                )
            return admin_client

        def _delete(pool: Any, composed_admin_client: Any, *, profile_id: Any) -> Any:
            seam.deletions.append({"profile_id": profile_id, "admin_client": composed_admin_client})
            if deletion_error is not None:
                raise deletion_error
            assert outcome is not None
            return outcome

        monkeypatch.setattr(account_router, "get_database_pool", _fake_get_database_pool)
        monkeypatch.setattr(account_router, "resolve_account_admin_client", _resolve)
        monkeypatch.setattr(account_lifecycle, "delete_account", _delete)
        return seam

    return _make


def _client(settings: Any, owner_profile: Profile) -> TestClient:
    app = create_app(settings)
    app.dependency_overrides[authentication_dependency.authenticated_profile] = lambda: (
        owner_profile
    )
    return TestClient(app)


def test_delete_account_reports_the_truthful_deleted_outcome(
    deletion_seam, settings_factory, owner_profile
) -> None:
    seam = deletion_seam(
        outcome=AccountDeletionOutcome(
            profile_id=owner_profile.id, result=account_lifecycle.RESULT_DELETED
        )
    )
    client = _client(settings_factory(), owner_profile)

    response = client.delete("/api/v1/profile/account")

    assert response.status_code == 200
    # The outcome body carries exactly the typed, secret-free end state —
    # no credential, key, or identity-provider record content, ever.
    assert response.json() == {
        "profile_id": str(owner_profile.id),
        "result": "deleted",
    }
    assert len(seam.compositions) == 1
    assert seam.deletions == [{"profile_id": owner_profile.id, "admin_client": seam.admin_client}]
    assert seam.pool_acquisitions == 1


def test_delete_account_reports_the_idempotent_already_absent_outcome(
    deletion_seam, settings_factory, owner_profile
) -> None:
    seam = deletion_seam(
        outcome=AccountDeletionOutcome(
            profile_id=owner_profile.id, result=account_lifecycle.RESULT_ALREADY_ABSENT
        )
    )
    client = _client(settings_factory(), owner_profile)

    response = client.delete("/api/v1/profile/account")

    assert response.status_code == 200
    assert response.json() == {
        "profile_id": str(owner_profile.id),
        "result": "already_absent",
    }
    assert len(seam.deletions) == 1


def test_guarded_single_flight_is_a_conflict_never_a_success(
    deletion_seam, settings_factory, owner_profile
) -> None:
    seam = deletion_seam(
        deletion_error=ConflictError(
            "an account deletion attempt is already in progress for this account"
        )
    )
    client = _client(settings_factory(), owner_profile)

    response = client.delete("/api/v1/profile/account")

    assert response.status_code == 409
    assert response.json()["detail"] == (
        "an account deletion attempt is already in progress for this account"
    )
    assert len(seam.deletions) == 1


def test_known_identity_provider_failure_is_an_honest_503(
    deletion_seam, settings_factory, owner_profile
) -> None:
    seam = deletion_seam(
        deletion_error=ExternalOperationFailedError(
            "the account was not deleted: the identity provider did not apply "
            "the permanent deletion"
        )
    )
    client = _client(settings_factory(), owner_profile)

    response = client.delete("/api/v1/profile/account")

    assert response.status_code == 503
    assert response.json()["detail"] == (
        "the account was not deleted: the identity provider did not apply the permanent deletion"
    )
    assert len(seam.deletions) == 1


def test_unknown_outcome_is_an_honest_503(deletion_seam, settings_factory, owner_profile) -> None:
    seam = deletion_seam(
        deletion_error=ExternalOperationUncertainError(
            "the outcome of the external operation is unknown"
        )
    )
    client = _client(settings_factory(), owner_profile)

    response = client.delete("/api/v1/profile/account")

    assert response.status_code == 503
    assert response.json()["detail"] == "the outcome of the external operation is unknown"
    assert len(seam.deletions) == 1


def test_unconfigured_admin_boundary_fails_closed_as_a_503(
    deletion_seam, settings_factory, owner_profile
) -> None:
    seam = deletion_seam(configured=False)
    client = _client(settings_factory(), owner_profile)

    response = client.delete("/api/v1/profile/account")

    assert response.status_code == 503
    assert response.json()["detail"] == (
        "the Supabase Auth Admin boundary is not configured for this process"
    )
    # The unconfigured composition refuses before any destructive call.
    assert seam.deletions == []


def test_representative_uniform_401(settings_factory) -> None:
    app = create_app(settings_factory(supabase_url="https://supabase.example.com"))

    def _fail() -> None:
        raise AuthenticationError("authentication failed: the access token is missing or malformed")

    app.dependency_overrides[authentication_dependency.authenticated_profile] = _fail
    client = TestClient(app)

    response = client.delete("/api/v1/profile/account")

    assert response.status_code == 401
    assert "account" not in response.json()["detail"]
