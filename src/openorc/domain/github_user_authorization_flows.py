"""Pending GitHub user authorization flow domain models (issue #373).

A :class:`GitHubUserAuthorizationFlow` is the durable, Profile-scoped record
of one pending OAuth start-to-callback correlation for the Profile-scoped
GitHub App user authorization. Only PENDING flows are durable: the row is
deleted atomically when the callback consumes it (the delete is the single-use
enforcement), when a newer flow supersedes it (at most one pending flow per
Profile), when an Owner revocation command invalidates it, or through the
sanctioned account-root cascade — so no terminal flow state is ever retained.

Security properties carried by the model:

- ``state`` is the unpredictable, expiring, single-use correlation value the
  authenticated start operation created and the provider callback presents.
  It selects the flow's subject and intended purpose; it is never identity
  authority — completion is fenced by the captured revocation evidence and
  the #142 same-human binding proof.
- ``pkce_verifier_reference`` is the opaque Vault pointer to the flow's PKCE
  code verifier — never the verifier value. The verifier is secret-bearing
  exchange material and lives exclusively behind the Vault boundary until
  the callback consumes the flow.
- ``observed_revocation_count`` is the durable revocation-epoch evidence the
  start operation captured from the Profile's authorization row (0 when no
  authorization existed). The final credential installation proceeds only
  when the durable count still equals this captured value, so any Owner
  revocation committed after the flow started permanently invalidates it.
- ``purpose`` is the intended flow the authenticated start declared:
  ``authorize`` for a first authorization, ``reauthorize`` for a new
  authorization over an existing row. The callback honors the captured
  subject; the purpose discriminates the start-time authority guards.
- ``expires_at`` bounds the flow's usability; expired flows can never be
  consumed.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from uuid import UUID

__all__ = [
    "GitHubUserAuthorizationFlow",
    "GitHubUserAuthorizationFlowDomainError",
    "GitHubUserAuthorizationFlowPurpose",
]


class GitHubUserAuthorizationFlowDomainError(Exception):
    """Raised when a GitHubUserAuthorizationFlow invariant is violated."""


class GitHubUserAuthorizationFlowPurpose(StrEnum):
    """The intended flow the authenticated start operation declared.

    ``AUTHORIZE`` starts a first authorization (the Profile's authorization
    row is absent or revoked); ``REAUTHORIZE`` starts a new authorization
    over an existing row. Both complete through the same #142 establishment;
    the purpose discriminates the start-time authority guards and the
    pending-flow status projection.
    """

    AUTHORIZE = "authorize"
    REAUTHORIZE = "reauthorize"


@dataclass(frozen=True, slots=True)
class GitHubUserAuthorizationFlow:
    """One pending OAuth start-to-callback correlation for one Profile.

    The invariants mirror the database CHECK constraints so a row cannot be
    represented that the database would reject afterwards:

    - ``state`` is a non-empty opaque correlation value (its unpredictability
      is the generator's property, validated at creation by the flow service).
    - ``pkce_verifier_reference`` is always present — a pending flow always
      carries exactly one Vault verifier reference.
    - ``observed_revocation_count`` is a non-negative integer.
    - ``expires_at`` is strictly after ``started_at``.
    """

    profile_id: UUID
    purpose: GitHubUserAuthorizationFlowPurpose
    state: str
    pkce_verifier_reference: str
    observed_revocation_count: int
    started_at: datetime
    expires_at: datetime

    def __post_init__(self) -> None:
        if not isinstance(self.purpose, GitHubUserAuthorizationFlowPurpose):
            raise GitHubUserAuthorizationFlowDomainError(
                "purpose must be a GitHubUserAuthorizationFlowPurpose value"
            )
        if not isinstance(self.state, str) or not self.state:
            raise GitHubUserAuthorizationFlowDomainError(
                f"state must be a non-empty string, got {self.state!r}"
            )
        if not isinstance(self.pkce_verifier_reference, str) or not (self.pkce_verifier_reference):
            raise GitHubUserAuthorizationFlowDomainError(
                "a pending flow must carry its PKCE verifier reference"
            )
        if (
            isinstance(self.observed_revocation_count, bool)
            or not isinstance(self.observed_revocation_count, int)
            or self.observed_revocation_count < 0
        ):
            raise GitHubUserAuthorizationFlowDomainError(
                "observed_revocation_count must be a non-negative integer, "
                f"got {self.observed_revocation_count!r}"
            )
        if self.expires_at <= self.started_at:
            raise GitHubUserAuthorizationFlowDomainError(
                "expires_at must be strictly after started_at"
            )
