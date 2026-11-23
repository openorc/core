"""Wire DTOs for the Workspace Repository mapping surface (issue #372).

Thin transport models only: the Owner-authorized application services own
every semantic rule (Workspace/Project ownership, the account-mutation
barrier, the claim/observation trust boundary, route binding) and these
models never duplicate service/domain validation.

The trust boundary is structural: the claim command carries the stable
external repository identity and its owning Project ALONE — no observation
field exists on it — and the read model separates the unverified claim from
the ``observed`` block only trusted GitHub reconciliation can populate, and
from the configured installation route reference. A claim or a route never
implies access or permission, and recorded status is durable evidence, never
live GitHub freshness.
"""

from __future__ import annotations

from datetime import datetime
from uuid import UUID

from pydantic import BaseModel, StrictInt

__all__ = [
    "InstallationRouteBinding",
    "RepositoryClaimCreate",
    "RepositoryInstallationRouteRead",
    "RepositoryObservationRead",
    "RepositoryRead",
]


class RepositoryClaimCreate(BaseModel):
    """The claim command for one Workspace Repository onboarding.

    The stable external repository ID and the owning Project are the only
    fields: a repository claim is unverified Owner configuration, never a
    GitHub observation — no owner login, name, URL, visibility, or default
    branch is accepted here, and none can be supplied through this command.
    """

    project_id: UUID
    github_repository_id: StrictInt


class RepositoryObservationRead(BaseModel):
    """The observation block recorded through trusted GitHub reconciliation.

    Present only when trusted reconciliation has established the record; the
    presentation metadata is GitHub-reported fact, carried verbatim
    (``default_branch`` stays ``None`` until observed).
    """

    owner_login: str
    name: str
    html_url: str
    is_private: bool
    default_branch: str | None


class RepositoryInstallationRouteRead(BaseModel):
    """The configured installation route reference of one Repository.

    A configuration fact only: ``installation_id`` is the OpenOrc
    installation record UUID and ``github_installation_id`` its stable
    external identity. A configured route never implies access or
    permission; the installation's own recorded state lives in the
    installation mapping resource.
    """

    installation_id: UUID
    github_installation_id: int


class RepositoryRead(BaseModel):
    """One Repository mapping record of the Workspace.

    ``github_repository_id`` is the claimed stable external identity; a null
    ``observed`` block is the unverified claim state. This model deliberately
    carries no credential material and no derived usability/authorization
    status.
    """

    id: UUID
    project_id: UUID
    workspace_id: UUID
    github_repository_id: int
    observed: RepositoryObservationRead | None
    installation_route: RepositoryInstallationRouteRead | None
    created_at: datetime
    updated_at: datetime


class InstallationRouteBinding(BaseModel):
    """The route-binding command: the installation record UUID to route to.

    The addressed installation must be a record of the authorized Workspace;
    cross-Workspace routes are the uniform not-found outcome at the service
    boundary and unrepresentable durably.
    """

    installation_id: UUID
