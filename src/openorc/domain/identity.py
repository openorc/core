"""Authenticated-identity domain models.

The Phase 2A backend authentication boundary resolves a verified Supabase Auth
access token to the canonical OpenOrc application identity
(:class:`~openorc.domain.ownership.Profile`). The principal is the token-side
half of that resolution: deliberately minimal, carrying only the identity
OpenOrc needs. Provider presentation, email, session tracking, and revocation
are not identity and have no domain surface here.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from uuid import UUID

__all__ = ["AuthenticatedPrincipal"]


@dataclass(frozen=True, slots=True)
class AuthenticatedPrincipal:
    """The authenticated caller resolved from a verified Supabase access token.

    ``user_id`` is the canonical Supabase Auth user UUID — the verified JWT
    ``sub`` claim by value, identical to the corresponding
    :class:`~openorc.domain.ownership.Profile` identifier. ``expires_at`` is
    the verified ``exp`` claim normalized to UTC — the token's validity bound
    the authorized Workspace SSE stream boundary (issue #399) consumes to
    terminate a live stream no later than credential expiry. GitHub username,
    email, and other presentation claims are deliberately not carried: they
    never constitute OpenOrc identity, and no other token material is
    surfaced here.
    """

    user_id: UUID
    expires_at: datetime
