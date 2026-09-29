"""Deterministic tests for the Profile-scoped GitHub authorization domain (issue #142).

The ordinary suite cannot execute Postgres: these tests prove the non-secret
domain invariants — the active/revoked currentness vocabulary, the
CHECK-mirroring lifecycle consistency rules, the strictly positive stable
GitHub identity and generation, and that no credential-bearing field exists
on the durable record.
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime
from uuid import UUID

import pytest

from openorc.domain.github_user_authorization import (
    GitHubUserAuthorization,
    GitHubUserAuthorizationDomainError,
    GitHubUserAuthorizationStatus,
)

_OBSERVED = datetime(2026, 9, 29, 12, 0, 0, tzinfo=UTC)


def _authorization(**overrides: object) -> GitHubUserAuthorization:
    values: dict[str, object] = {
        "profile_id": uuid.uuid4(),
        "github_user_id": 5432,
        "github_login": "octocat",
        "status": GitHubUserAuthorizationStatus.ACTIVE,
        "refresh_secret_reference": "openorc:github-user-refresh:v1:vault:" + str(uuid.uuid4()),
        "refresh_expires_at": _OBSERVED,
        "refresh_generation": 1,
        "authorized_at": _OBSERVED,
        "revoked_at": None,
        "created_at": _OBSERVED,
        "updated_at": _OBSERVED,
    }
    values.update(overrides)
    return GitHubUserAuthorization(
        profile_id=values["profile_id"],  # type: ignore[arg-type]
        github_user_id=values["github_user_id"],  # type: ignore[arg-type]
        github_login=values["github_login"],  # type: ignore[arg-type]
        status=values["status"],  # type: ignore[arg-type]
        refresh_secret_reference=values["refresh_secret_reference"],  # type: ignore[arg-type]
        refresh_expires_at=values["refresh_expires_at"],  # type: ignore[arg-type]
        refresh_generation=values["refresh_generation"],  # type: ignore[arg-type]
        authorized_at=values["authorized_at"],  # type: ignore[arg-type]
        revoked_at=values["revoked_at"],  # type: ignore[arg-type]
        created_at=values["created_at"],  # type: ignore[arg-type]
        updated_at=values["updated_at"],  # type: ignore[arg-type]
    )


def test_active_authorization_represents_the_durable_lifecycle_facts() -> None:
    authorization = _authorization()

    assert authorization.status is GitHubUserAuthorizationStatus.ACTIVE
    assert authorization.github_user_id == 5432
    assert authorization.refresh_generation == 1
    assert authorization.revoked_at is None


def test_durable_record_carries_no_credential_bearing_fields() -> None:
    # The durable record is the non-secret boundary: its field set contains
    # only the opaque reference pointer and safe metadata. Any access/refresh
    # token field would be a violation.
    assert not any("token" in field for field in GitHubUserAuthorization.__dataclass_fields__)


def test_revoked_authorization_is_representable_and_distinct_from_absence() -> None:
    revoked_at = _OBSERVED
    authorization = _authorization(
        status=GitHubUserAuthorizationStatus.REVOKED,
        refresh_secret_reference=None,
        refresh_expires_at=None,
        revoked_at=revoked_at,
    )

    assert authorization.status is GitHubUserAuthorizationStatus.REVOKED
    assert authorization.refresh_secret_reference is None
    assert authorization.revoked_at == revoked_at
    # The row persists with its generation: revoked is durable currentness
    # state, never a delete-and-reinsert hole.
    assert authorization.refresh_generation == 1


@pytest.mark.parametrize(
    "overrides",
    [
        {"github_user_id": 0},
        {"github_user_id": -1},
        {"github_user_id": True},
        {"refresh_generation": 0},
        {"refresh_generation": -3},
        {"status": "active"},  # type: ignore[dict-item]
    ],
)
def test_invalid_identity_generation_or_status_fails_closed(overrides: dict[str, object]) -> None:
    with pytest.raises(GitHubUserAuthorizationDomainError):
        _authorization(**overrides)


@pytest.mark.parametrize(
    "overrides",
    [
        {"refresh_secret_reference": None},
        {"refresh_expires_at": None},
        {"revoked_at": _OBSERVED},
    ],
)
def test_active_authorization_lifecycle_tuple_violations_fail_closed(
    overrides: dict[str, object],
) -> None:
    with pytest.raises(GitHubUserAuthorizationDomainError):
        _authorization(**overrides)


@pytest.mark.parametrize(
    "overrides",
    [
        {"refresh_secret_reference": "openorc:github-user-refresh:v1:vault:x"},
        {"refresh_expires_at": _OBSERVED},
        {"revoked_at": None},
    ],
)
def test_revoked_authorization_lifecycle_tuple_violations_fail_closed(
    overrides: dict[str, object],
) -> None:
    values: dict[str, object] = {
        "status": GitHubUserAuthorizationStatus.REVOKED,
        "refresh_secret_reference": None,
        "refresh_expires_at": None,
        "revoked_at": _OBSERVED,
    }
    values.update(overrides)
    with pytest.raises(GitHubUserAuthorizationDomainError):
        _authorization(**values)


def test_profile_id_is_the_row_identity_of_the_authorization() -> None:
    profile_id = uuid.uuid4()
    authorization = _authorization(profile_id=profile_id)

    assert isinstance(authorization.profile_id, UUID)
    assert authorization.profile_id == profile_id
