"""Workspace Project/Repository onboarding services (issue #372).

The Owner-facing configuration entry that makes a v1 Workspace usable for the
GitHub installation/Repository mapping: Project creation and Repository
onboarding. A Repository claim is UNVERIFIED Owner configuration: the
authenticated Owner is authorized to configure which stable GitHub repository
identity belongs to which Workspace Project, but that authorization never
establishes that the submitted identity is genuine, so the claim carries the
stable external repository ID alone and is persisted in claim state — the
observation metadata NULL. It can never carry or write GitHub observation
facts (owner login, name, URL, visibility, default branch): only trusted
GitHub reconciliation later populates them. Claim state never masquerades as
a GitHub observation, claims and routes never imply access or permission, and
recorded status is durable evidence, never live GitHub freshness.

Project creation is genuine Owner configuration (an Owner-authored name), not
a GitHub observation.

Composition and discipline: Owner-facing mutations compose the account-wide
Owner-mutation barrier (issue #97) first inside one short composed
transaction, then the Workspace authorization boundary; reads compose the
authorization boundary without the account-mutation barrier. Command shape is
validated before any database work. A duplicate stable GitHub repository
identity within one Workspace is a conflict (the durable unique constraint is
the backstop; the same identity in another Workspace is an independent
record). Cross-Workspace subjects are the uniform not-found outcome.
"""

from __future__ import annotations

from dataclasses import dataclass
from uuid import UUID

from psycopg.errors import UniqueViolation

from openorc.domain.github_installations import GitHubInstallation
from openorc.domain.ownership import (
    GitHubRepositoryIdentity,
    OwnershipError,
    Project,
    Repository,
)
from openorc.observability import annotate_span, application_span
from openorc.persistence.github_installations import (
    get_github_installation,
    list_workspace_installations,
)
from openorc.persistence.ownership import (
    create_project,
    create_repository,
    find_repository_by_github_identity,
)
from openorc.persistence.ownership import (
    list_workspace_projects as _workspace_project_rows,
)
from openorc.persistence.ownership import (
    list_workspace_repositories as _workspace_repository_rows,
)
from openorc.persistence.pool import DatabasePool
from openorc.services.errors import ConflictError, InvalidCommandError, NotFoundError
from openorc.services.profile_lifecycle_guard import require_account_operational
from openorc.services.transaction_composition import composed_transaction
from openorc.services.workspace_authorization import (
    require_profile_workspace,
    require_workspace_project,
    require_workspace_repository,
)

__all__ = [
    "WorkspaceRepositoryStatus",
    "claim_workspace_repository",
    "create_workspace_project",
    "list_workspace_projects",
    "list_workspace_repositories",
    "read_workspace_repository",
]

# Application-service span boundaries (issues #108/#109): every public
# use-case operation of this module opens one span at the established service
# boundary. Only the safe attribute vocabulary is attachable.
_SERVICE_TRACER_SCOPE = "openorc.services.repository_onboarding"
_CREATE_PROJECT_SPAN_NAME = "repository_onboarding.create_workspace_project"
_LIST_PROJECTS_SPAN_NAME = "repository_onboarding.list_workspace_projects"
_CLAIM_REPOSITORY_SPAN_NAME = "repository_onboarding.claim_workspace_repository"
_LIST_REPOSITORIES_SPAN_NAME = "repository_onboarding.list_workspace_repositories"
_READ_REPOSITORY_SPAN_NAME = "repository_onboarding.read_workspace_repository"

# The Project ``name`` column is unconstrained durable data, so the
# command-shape rule is service-owned: a non-blank, bounded Owner-authored
# name (genuine Owner configuration, never a GitHub observation).
_MAX_PROJECT_NAME_LENGTH = 200


def _require_uuid_command(value: object, name: str) -> None:
    """Reject a malformed UUID command argument before any state is touched."""
    if not isinstance(value, UUID):
        raise InvalidCommandError(f"{name} must be a UUID")


def _validate_project_name(name: str) -> None:
    """Reject an invalid Project name before any database work."""
    if not isinstance(name, str) or not name.strip() or len(name) > _MAX_PROJECT_NAME_LENGTH:
        raise InvalidCommandError(
            "Project name must be a non-blank string of at most "
            f"{_MAX_PROJECT_NAME_LENGTH} characters"
        )


def _require_github_repository_identity(github_repository_id: object) -> GitHubRepositoryIdentity:
    """Validate the claimed stable repository identity before any database work."""
    if (
        isinstance(github_repository_id, bool)
        or not isinstance(github_repository_id, int)
        or github_repository_id <= 0
    ):
        raise InvalidCommandError("github_repository_id must be a positive integer")
    try:
        return GitHubRepositoryIdentity(github_repository_id=github_repository_id)
    except OwnershipError as error:  # pragma: no cover - the shape check above is exhaustive
        raise InvalidCommandError(f"invalid GitHub repository identity: {error}") from error


@dataclass(frozen=True, slots=True)
class WorkspaceRepositoryStatus:
    """One Workspace Repository's recorded mapping status.

    ``repository`` is the durable record: its stable GitHub identity, its
    observed metadata (``None`` is the claim state — unverified Owner
    configuration), and its configured route (``None`` is unconfigured).
    ``installation`` is the routed installation record — in claim or
    observation state — or ``None`` when unconfigured. Claim and route are
    configuration facts, never access or permission.
    """

    repository: Repository
    installation: GitHubInstallation | None


def create_workspace_project(
    pool: DatabasePool, *, profile_id: UUID, workspace_id: UUID, name: str
) -> Project:
    """Create one Project in the authorized Workspace, failing closed.

    The account-wide Owner-mutation barrier (issue #97) is the first lock
    acquisition inside the one short composed transaction: creation fails
    closed while an account-deletion attempt is unresolved. The name is the
    only creation input — genuine Owner-authored configuration, never a
    GitHub observation.
    """
    with application_span(_SERVICE_TRACER_SCOPE, _CREATE_PROJECT_SPAN_NAME) as span:
        _require_uuid_command(profile_id, "profile_id")
        _require_uuid_command(workspace_id, "workspace_id")
        _validate_project_name(name)
        annotate_span(span, operation=_CREATE_PROJECT_SPAN_NAME, workspace_id=str(workspace_id))
        with composed_transaction(pool) as transaction_pool:
            # The account-wide Owner-mutation barrier first (issue #97).
            require_account_operational(transaction_pool, profile_id=profile_id)
            workspace = require_profile_workspace(
                transaction_pool, profile_id=profile_id, workspace_id=workspace_id
            )
            return create_project(transaction_pool, workspace_id=workspace.id, name=name)


def list_workspace_projects(
    pool: DatabasePool, *, profile_id: UUID, workspace_id: UUID
) -> list[Project]:
    """List the authorized Workspace's Projects, deterministically ordered.

    The owner-scoped Workspace authorization IS the isolation boundary;
    read-only — this composes no account-mutation barrier.
    """
    with application_span(_SERVICE_TRACER_SCOPE, _LIST_PROJECTS_SPAN_NAME) as span:
        _require_uuid_command(profile_id, "profile_id")
        _require_uuid_command(workspace_id, "workspace_id")
        annotate_span(span, operation=_LIST_PROJECTS_SPAN_NAME, workspace_id=str(workspace_id))
        workspace = require_profile_workspace(
            pool, profile_id=profile_id, workspace_id=workspace_id
        )
        return _workspace_project_rows(pool, workspace_id=workspace.id)


def claim_workspace_repository(
    pool: DatabasePool,
    *,
    profile_id: UUID,
    workspace_id: UUID,
    project_id: UUID,
    github_repository_id: int,
) -> Repository:
    """Claim one stable GitHub repository identity into the authorized Workspace.

    The claim carries the stable external repository identity and its owning
    Project alone: it is unverified Owner configuration persisted in claim
    state — the observation metadata NULL — never a GitHub observation, and
    it never implies access or permission. A duplicate stable identity within
    one Workspace is a conflict; the same identity in another Workspace is a
    distinct, independent record. The addressed Project must belong to the
    authorized Workspace (uniform not-found outcome otherwise).
    """
    with application_span(_SERVICE_TRACER_SCOPE, _CLAIM_REPOSITORY_SPAN_NAME) as span:
        _require_uuid_command(profile_id, "profile_id")
        _require_uuid_command(workspace_id, "workspace_id")
        _require_uuid_command(project_id, "project_id")
        identity = _require_github_repository_identity(github_repository_id)
        annotate_span(span, operation=_CLAIM_REPOSITORY_SPAN_NAME, workspace_id=str(workspace_id))
        with composed_transaction(pool) as transaction_pool:
            # The account-wide Owner-mutation barrier first (issue #97).
            require_account_operational(transaction_pool, profile_id=profile_id)
            workspace = require_profile_workspace(
                transaction_pool, profile_id=profile_id, workspace_id=workspace_id
            )
            project = require_workspace_project(
                transaction_pool,
                profile_id=profile_id,
                workspace_id=workspace_id,
                project_id=project_id,
            )
            existing = find_repository_by_github_identity(
                transaction_pool, workspace_id=workspace.id, identity=identity
            )
            if existing is not None:
                raise ConflictError(
                    "the GitHub repository identity is already claimed in this workspace"
                )
            try:
                return create_repository(
                    transaction_pool,
                    project_id=project.id,
                    workspace_id=workspace.id,
                    identity=identity,
                    metadata=None,
                )
            except UniqueViolation as error:
                # A concurrent claim of the same stable identity won the
                # durable unique constraint; the claim is a conflict either
                # way and the composition rolls back.
                raise ConflictError(
                    "the GitHub repository identity is already claimed in this workspace"
                ) from error


def _status_for(
    repository: Repository, installations: dict[UUID, GitHubInstallation]
) -> WorkspaceRepositoryStatus:
    """Compose one repository's mapping status from the Workspace's installations.

    The composite route foreign key guarantees a routed repository's
    installation is a record of the same Workspace, so the Workspace
    installation map covers every possible route; a miss means the durable
    state moved on concurrently — fail closed rather than projecting a broken
    route.
    """
    installation = (
        None
        if repository.github_installation_id is None
        else installations.get(repository.github_installation_id)
    )
    if repository.github_installation_id is not None and installation is None:
        raise NotFoundError(
            "the requested repository installation route is not available in this workspace"
        )
    return WorkspaceRepositoryStatus(repository=repository, installation=installation)


def list_workspace_repositories(
    pool: DatabasePool, *, profile_id: UUID, workspace_id: UUID
) -> list[WorkspaceRepositoryStatus]:
    """List the authorized Workspace's Repository mapping statuses.

    The owner-scoped Workspace authorization IS the isolation boundary. Each
    status carries the record's observation state verbatim (``metadata is
    None`` is the claim state) and its configured route reference or ``None``;
    a claim or a route never implies access or permission.
    """
    with application_span(_SERVICE_TRACER_SCOPE, _LIST_REPOSITORIES_SPAN_NAME) as span:
        _require_uuid_command(profile_id, "profile_id")
        _require_uuid_command(workspace_id, "workspace_id")
        annotate_span(span, operation=_LIST_REPOSITORIES_SPAN_NAME, workspace_id=str(workspace_id))
        workspace = require_profile_workspace(
            pool, profile_id=profile_id, workspace_id=workspace_id
        )
        repositories = _workspace_repository_rows(pool, workspace_id=workspace.id)
        installations = {
            installation.id: installation
            for installation in list_workspace_installations(pool, workspace_id=workspace.id)
        }
        return [_status_for(repository, installations) for repository in repositories]


def read_workspace_repository(
    pool: DatabasePool, *, profile_id: UUID, workspace_id: UUID, repository_id: UUID
) -> WorkspaceRepositoryStatus:
    """Read one Repository mapping status of the authorized Workspace.

    A foreign or absent repository is the uniform not-found outcome, so
    probing exact record UUIDs across Workspaces is indistinguishable from
    addressing something absent.
    """
    with application_span(_SERVICE_TRACER_SCOPE, _READ_REPOSITORY_SPAN_NAME) as span:
        _require_uuid_command(profile_id, "profile_id")
        _require_uuid_command(workspace_id, "workspace_id")
        _require_uuid_command(repository_id, "repository_id")
        annotate_span(span, operation=_READ_REPOSITORY_SPAN_NAME, workspace_id=str(workspace_id))
        repository = require_workspace_repository(
            pool, profile_id=profile_id, workspace_id=workspace_id, repository_id=repository_id
        )
        installation: GitHubInstallation | None = None
        if repository.github_installation_id is not None:
            installation = get_github_installation(
                pool, installation_id=repository.github_installation_id
            )
            if installation is None or installation.workspace_id != workspace_id:
                # The composite route foreign key makes a foreign-Workspace
                # route unrepresentable; a failure here means the record
                # moved on concurrently. Fail closed rather than projecting
                # a broken route.
                raise NotFoundError(
                    "the requested repository installation route is not available in this workspace"
                )
        return WorkspaceRepositoryStatus(repository=repository, installation=installation)
