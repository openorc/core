"""Profile-scoped GitHub App user authorization domain models (issue #142).

A :class:`GitHubUserAuthorization` is the durable, Profile-scoped record of
one current GitHub App user-to-server authorization — the credential state
that lets OpenOrc act on GitHub on behalf of the same human Owner who signed
in to OpenOrc. Supabase Auth remains the canonical sign-in/session boundary
and ``Profile.id`` remains the application identity; the Supabase/GitHub
sign-in provider token is identity-establishment material only and is never
modeled here.

Durable properties:

- Exactly one authorization row per Profile (the Profile is the row
  identity). The row is created once and updated in place for its whole
  Profile lifetime — reauthorization never delete-and-reinserts, so
  ``refresh_generation`` is monotonic across the account's lifetime and no
  earlier generation (and therefore no in-memory access-token cache key bound
  to it) can ever become apparently current again.
- Currentness is explicit durable state: ``ACTIVE`` (usable, carries exactly
  one Vault refresh reference and its expiry) and ``REVOKED`` (explicitly
  unusable, carries neither). Revocation is deliberately distinguishable
  from never-authorized (no row).
- ``github_user_id`` is the stable numeric GitHub identity the authorization
  was proven against; ``github_login`` is mutable observed presentation
  metadata, never identity.
- Raw access/refresh tokens never appear in these models. The refresh
  credential lives exclusively in Supabase Vault behind the opaque
  ``refresh_secret_reference``; the user access token lives only in the
  service-owned bounded in-memory cache.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from uuid import UUID

__all__ = [
    "GitHubUserAuthorization",
    "GitHubUserAuthorizationDomainError",
    "GitHubUserAuthorizationStatus",
]


class GitHubUserAuthorizationDomainError(Exception):
    """Raised when a GitHubUserAuthorization invariant is violated."""


class GitHubUserAuthorizationStatus(StrEnum):
    """Durable currentness vocabulary of one Profile authorization.

    ``ACTIVE`` is a usable authorization carrying exactly one Vault-backed
    refresh credential. ``REVOKED`` is an explicitly unusable authorization
    — the usable refresh reference and its expiry are removed while the row
    persists, so revoked is never indistinguishable from never-authorized.
    """

    ACTIVE = "active"
    REVOKED = "revoked"


@dataclass(frozen=True, slots=True)
class GitHubUserAuthorization:
    """One current GitHub App user authorization for one Profile.

    The record carries only non-secret, durable authorization facts. The
    invariants mirror the database CHECK constraints so a row cannot be
    represented that the database would reject afterwards:

    - ``refresh_secret_reference`` is the opaque Vault pointer to the
      Profile's GitHub refresh credential — never the secret value; it is
      non-null exactly when the authorization is active.
    - ``refresh_expires_at`` is the durable expiry instant of the current
      refresh credential; non-null exactly when active.
    - ``refresh_generation`` is strictly monotonic per Profile; every
      credential lifecycle transition advances it.
    - ``revoked_at`` is non-null exactly when the status is revoked.
    """

    profile_id: UUID
    github_user_id: int
    github_login: str | None
    status: GitHubUserAuthorizationStatus
    refresh_secret_reference: str | None
    refresh_expires_at: datetime | None
    refresh_generation: int
    authorized_at: datetime
    revoked_at: datetime | None
    created_at: datetime
    updated_at: datetime

    def __post_init__(self) -> None:
        if (
            isinstance(self.github_user_id, bool)
            or not isinstance(self.github_user_id, int)
            or self.github_user_id <= 0
        ):
            raise GitHubUserAuthorizationDomainError(
                f"github_user_id must be a positive integer, got {self.github_user_id!r}"
            )
        if (
            isinstance(self.refresh_generation, bool)
            or not isinstance(self.refresh_generation, int)
            or self.refresh_generation < 1
        ):
            raise GitHubUserAuthorizationDomainError(
                f"refresh_generation must be a positive integer, got {self.refresh_generation!r}"
            )
        if not isinstance(self.status, GitHubUserAuthorizationStatus):
            raise GitHubUserAuthorizationDomainError(
                "status must be a GitHubUserAuthorizationStatus value"
            )
        if self.status is GitHubUserAuthorizationStatus.ACTIVE:
            if not isinstance(self.refresh_secret_reference, str) or not (
                self.refresh_secret_reference
            ):
                raise GitHubUserAuthorizationDomainError(
                    "an active authorization must carry its refresh secret reference"
                )
            if self.refresh_expires_at is None:
                raise GitHubUserAuthorizationDomainError(
                    "an active authorization must carry its refresh expiry instant"
                )
            if self.revoked_at is not None:
                raise GitHubUserAuthorizationDomainError(
                    "an active authorization never carries a revocation instant"
                )
        else:
            if self.refresh_secret_reference is not None:
                raise GitHubUserAuthorizationDomainError(
                    "a revoked authorization never carries a refresh secret reference"
                )
            if self.refresh_expires_at is not None:
                raise GitHubUserAuthorizationDomainError(
                    "a revoked authorization never carries a refresh expiry instant"
                )
            if self.revoked_at is None:
                raise GitHubUserAuthorizationDomainError(
                    "a revoked authorization always carries its revocation instant"
                )
