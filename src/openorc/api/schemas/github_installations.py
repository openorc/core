"""Wire DTOs for the Workspace GitHub installation mapping surface (issue #372).

Thin transport models only: the Owner-authorized application services own
every semantic rule (Workspace ownership, the account-mutation barrier, the
claim/observation trust boundary) and these models never duplicate
service/domain validation — FastAPI parses the declared shapes and the router
delegates.

The trust boundary is structural: the claim command carries the stable
external installation identity ALONE and has no field any observation fact
could occupy, and the read model separates the unverified claim from the
``observed`` block that only trusted GitHub reconciliation can populate.
Recorded status is durable evidence, never live GitHub freshness, and a
claim or its route never implies access or permission.
"""

from __future__ import annotations

from datetime import datetime
from uuid import UUID

from pydantic import BaseModel, StrictInt

__all__ = [
    "GitHubInstallationClaimCreate",
    "GitHubInstallationObservationRead",
    "GitHubInstallationRead",
]


class GitHubInstallationClaimCreate(BaseModel):
    """The claim command for one Workspace installation mapping.

    The stable external installation ID is the only field: an installation
    claim is unverified Owner configuration, never a GitHub observation — no
    account identity, login/type, or suspension fact is accepted here, and
    none can be supplied through this command.
    """

    github_installation_id: StrictInt


class GitHubInstallationObservationRead(BaseModel):
    """The observation block recorded through trusted GitHub reconciliation.

    Present only when trusted reconciliation has established the record: the
    stable account identity, the observed login/type presentation metadata,
    and the verbatim ``suspended_at`` observation (no derived usability or
    authorization claim attaches to it anywhere in OpenOrc).
    """

    github_account_id: int
    account_login: str
    account_type: str
    suspended_at: datetime | None


class GitHubInstallationRead(BaseModel):
    """One installation mapping record of the Workspace.

    ``github_installation_id`` is the claimed stable external identity; a
    null ``observed`` block is the unverified claim state, and observed facts
    appear only once trusted GitHub reconciliation has recorded them. This
    model deliberately carries no credential material and no derived
    usability/authorization status.
    """

    id: UUID
    workspace_id: UUID
    github_installation_id: int
    observed: GitHubInstallationObservationRead | None
    created_at: datetime
    updated_at: datetime
