"""Wire DTOs for the Profile's GitHub user-authorization surface (issue #373).

Thin transport models only: the Owner-authorized application services own
every semantic rule (the account-mutation barrier, the purpose guards, the
single-use correlation, the revocation fence) and these models never
duplicate service/domain validation — FastAPI parses the declared shapes and
the router delegates.

The read models are structurally incapable of carrying credential material:
the refresh credential reference, the PKCE code verifier, and the OAuth
correlation state have no field anywhere, and no response ever echoes a
callback query value. The started-flow projection carries the documented
authorize URL — the only place the correlation state and the S256 code
challenge are ever projected, as GitHub's web-application flow requires.
"""

from __future__ import annotations

from datetime import datetime
from typing import Literal

from pydantic import BaseModel

__all__ = [
    "GitHubUserAuthorizationFlowStartRead",
    "GitHubUserAuthorizationRevokedRead",
    "GitHubUserAuthorizationStatusRead",
    "PendingFlowRead",
]


class PendingFlowRead(BaseModel):
    """The Profile's one outstanding pending authorization flow.

    The intended purpose and the usability window only: the correlation
    state, the PKCE verifier reference, and every credential have no field.
    """

    purpose: str
    started_at: datetime
    expires_at: datetime


class GitHubUserAuthorizationStatusRead(BaseModel):
    """The Profile's durable GitHub authorization status.

    ``missing`` (never authorized), ``active``, and ``revoked`` are the
    durable currentness states, deliberately distinguishable — never a live
    GitHub freshness claim. ``github_login`` is presentation metadata, never
    identity authority. ``refresh_expires_at`` is the durable expiry instant
    of the current refresh credential; the credential's reference has no
    field here.
    """

    status: Literal["missing", "active", "revoked"]
    github_user_id: int | None
    github_login: str | None
    authorized_at: datetime | None
    refresh_expires_at: datetime | None
    pending_flow: PendingFlowRead | None


class GitHubUserAuthorizationFlowStartRead(BaseModel):
    """One started authorization flow.

    ``authorize_url`` is the documented GitHub authorize URL the Owner's
    browser opens: the only location the correlation state and the S256 code
    challenge are ever projected. The PKCE verifier, the client secret, and
    every credential have no field here.
    """

    authorize_url: str
    purpose: str
    expires_at: datetime


class GitHubUserAuthorizationRevokedRead(BaseModel):
    """One revoked authorization's durable facts.

    The stable proven GitHub user ID and the revocation instant; the refresh
    credential's reference (removed by the revocation) has no field here.
    """

    github_user_id: int
    github_login: str | None
    revoked_at: datetime
