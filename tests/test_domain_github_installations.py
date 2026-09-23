"""Deterministic domain tests for GitHub App installation models (issue #57).

Validation, identity-vs-observation semantics, and the credential-free surface
of the Workspace-scoped GitHubInstallation record. No database and no external
GitHub call is involved; durable constraint behavior is proven by the
integration-marked suite and the migration convention tests.
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime

import pytest

from openorc.domain.github_installations import (
    GitHubInstallation,
    GitHubInstallationAccount,
    GitHubInstallationDomainError,
    GitHubInstallationIdentity,
    github_installation_field_names,
)
from openorc.domain.ownership import (
    GitHubRepositoryIdentity,
    Repository,
    RepositoryMetadata,
)

_OBSERVED = datetime(2026, 9, 23, 12, 0, 0, tzinfo=UTC)


def _account() -> GitHubInstallationAccount:
    return GitHubInstallationAccount(github_account_id=501, login="octocat", type="Organization")


def _installation(suspended_at: datetime | None = None) -> GitHubInstallation:
    return GitHubInstallation(
        id=uuid.uuid4(),
        workspace_id=uuid.uuid4(),
        identity=GitHubInstallationIdentity(github_installation_id=12345678),
        account=_account(),
        suspended_at=suspended_at,
        created_at=_OBSERVED,
        updated_at=_OBSERVED,
    )


def test_installation_identity_requires_a_positive_integer() -> None:
    assert (
        GitHubInstallationIdentity(github_installation_id=12345678).github_installation_id
        == 12345678
    )
    for bad in (0, -1, True, 1.5, "12345678", None):
        with pytest.raises(GitHubInstallationDomainError):
            GitHubInstallationIdentity(github_installation_id=bad)  # type: ignore[arg-type]


def test_account_requires_a_stable_positive_id_and_nonblank_observed_metadata() -> None:
    assert GitHubInstallationAccount(github_account_id=501, login="octocat", type="User")

    for kwargs in (
        {"github_account_id": 0, "login": "octocat", "type": "Organization"},
        {"github_account_id": True, "login": "octocat", "type": "Organization"},
        {"github_account_id": 501, "login": "   ", "type": "Organization"},
        {"github_account_id": 501, "login": "octocat", "type": ""},
        {"github_account_id": 501, "login": None, "type": "Organization"},
        {"github_account_id": 501, "login": "octocat", "type": 7},
    ):
        with pytest.raises(GitHubInstallationDomainError):
            GitHubInstallationAccount(**kwargs)


def test_field_names_are_exactly_the_durable_routing_and_observation_facts() -> None:
    assert github_installation_field_names() == {
        "id",
        "workspace_id",
        "identity",
        "account",
        "suspended_at",
        "created_at",
        "updated_at",
    }


def test_no_credential_bearing_field_exists() -> None:
    forbidden = ("token", "secret", "key", "credential", "pat", "oauth", "auth")
    for name in github_installation_field_names():
        for word in forbidden:
            assert word not in name.lower(), name


def test_suspended_at_is_carried_verbatim_with_no_usability_or_authorization_claim() -> None:
    unsuspended = _installation()
    suspended = _installation(suspended_at=_OBSERVED)
    assert unsuspended.suspended_at is None
    assert suspended.suspended_at == _OBSERVED

    # No derived usability/authorization claim exists on the record: deciding
    # whether the installation currently grants the required repository access
    # is later GitHub reconciliation work, never a property of this record.
    derived = [
        name
        for name in dir(suspended)
        if not name.startswith("_")
        and any(word in name for word in ("usable", "authorized", "health", "status"))
    ]
    assert derived == []


def test_stable_identity_is_independent_of_observed_account_metadata() -> None:
    first = GitHubInstallationAccount(github_account_id=501, login="old-name", type="Organization")
    second = GitHubInstallationAccount(github_account_id=501, login="new-name", type="User")
    identity = GitHubInstallationIdentity(github_installation_id=12345678)

    # The external installation identity is the stable fact; login/type are
    # mutable observations that never participate in it.
    assert identity == GitHubInstallationIdentity(github_installation_id=12345678)
    assert first.github_account_id == second.github_account_id
    assert first.login != second.login


def test_repository_route_defaults_to_unconfigured() -> None:
    repository = Repository(
        id=uuid.uuid4(),
        project_id=uuid.uuid4(),
        workspace_id=uuid.uuid4(),
        identity=GitHubRepositoryIdentity(github_repository_id=987654321),
        metadata=RepositoryMetadata(
            owner_login="octocat",
            name="hello-world",
            html_url="https://github.com/octocat/hello-world",
            is_private=False,
            default_branch="main",
        ),
        created_at=_OBSERVED,
        updated_at=_OBSERVED,
    )
    assert repository.github_installation_id is None
