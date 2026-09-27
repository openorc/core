"""Durable Workspace Repository → GitHub App installation route resolution.

The shared trusted-system resolution used by the GitHub reconciliation and
canonical-workflow services (#59/#63): no authenticated actor participates,
and the boundary still fails closed uniformly — an absent or
foreign-Workspace repository, an unconfigured route (valid historical state
that is not usable for GitHub operations), or a missing/foreign installation
record is the uniform not-found outcome.

The resolution is a database-only read. A caller performing authoritative
GitHub operations resolves the route first, performs its external calls with
no database transaction open, and then revalidates the exact route under a
row lock inside its write transaction — an observation authorized through
installation A is never committed after the route moved to installation B.
"""

from __future__ import annotations

from dataclasses import dataclass
from uuid import UUID

from openorc.domain.ownership import Repository
from openorc.persistence.github_installations import get_github_installation
from openorc.persistence.ownership import get_repository
from openorc.persistence.pool import DatabasePool
from openorc.services.errors import NotFoundError

__all__ = [
    "ResolvedRepositoryInstallationRoute",
    "resolve_system_repository_installation_route",
]


@dataclass(frozen=True, slots=True)
class ResolvedRepositoryInstallationRoute:
    """One resolved durable Repository → GitHub App installation route.

    ``repository`` is the durable Repository record (whose stable GitHub
    repository identity the adapter addresses); ``installation_id`` is the
    OpenOrc installation record UUID the reads must be authorized through;
    ``github_installation_id`` is the stable external GitHub installation
    identity the adapter authenticates as.
    """

    repository: Repository
    installation_id: UUID
    github_installation_id: int


def resolve_system_repository_installation_route(
    pool: DatabasePool, *, workspace_id: UUID, repository_id: UUID
) -> ResolvedRepositoryInstallationRoute:
    """Resolve the durable Workspace Repository → installation route.

    Fails closed uniformly: an absent or foreign-Workspace repository, an
    unconfigured route, or a missing/foreign installation record is the
    typed not-found outcome (probing a Workspace's repositories and routes
    is indistinguishable from addressing something absent).
    """
    repository = get_repository(pool, repository_id)
    if repository is None or repository.workspace_id != workspace_id:
        raise NotFoundError("the requested repository is not available in this workspace")
    if repository.github_installation_id is None:
        raise NotFoundError("the requested repository has no configured github installation route")
    installation_id = repository.github_installation_id
    installation = get_github_installation(pool, installation_id)
    if installation is None or installation.workspace_id != workspace_id:
        raise NotFoundError(
            "the requested repository installation route is not available in this workspace"
        )
    return ResolvedRepositoryInstallationRoute(
        repository=repository,
        installation_id=installation_id,
        github_installation_id=installation.identity.github_installation_id,
    )
