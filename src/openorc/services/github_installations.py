"""GitHub installation configuration services (issue #57).

The durable, Workspace-scoped GitHub App installation records and the explicit,
deterministic Repository -> GitHubInstallation route, as ownership-gated use
cases. Installations are a separate integration concept from Agent Runtime
Connections (issue #20): a Connection routes OpenOrc agent-runtime work, an
installation routes GitHub operations. Deleting or disconnecting OpenOrc
configuration never uninstalls the GitHub App or touches external GitHub
artifacts.

Routing, not authorization: these operations persist trusted GitHub
installation facts and manage the deterministic route — the durable fact of
WHICH installation later GitHub operations must use. They never assert current
external authorization, access to a routed repository, or effective
permissions: GitHub transport and authoritative reconciliation are outside
this leaf, and later capability work establishes current access before any
GitHub operation runs. An unconfigured Repository (``github_installation_id``
is None) fails closed as the uniform not-found outcome rather than being
treated as GitHub-authorized.

Every public operation takes the authenticated Profile UUID from the #52
authentication boundary and composes ownership through the
:mod:`openorc.services.workspace_authorization` resolvers —
:func:`require_profile_workspace` for Workspace-scoped operations and
:func:`require_workspace_repository` for repository-scoped ones (each
re-enforces ownership before any scope check; no subject resolver is composed
after an already-resolved Workspace object). Owner-facing
mutations compose the account-wide Owner-mutation barrier first (#97: the
Profile ``FOR KEY SHARE`` read inside one short ``composed_transaction``);
cross-Workspace routes are rejected at the service boundary as the uniform
not-found outcome and are unrepresentable durably through the composite route
foreign key. No credential material (private keys, installation access
tokens, PATs, human OAuth tokens) is accepted or stored.
"""

from __future__ import annotations

from datetime import datetime
from uuid import UUID

from psycopg.errors import ForeignKeyViolation

from openorc.domain.github_installations import (
    GitHubInstallation,
    GitHubInstallationAccount,
    GitHubInstallationDomainError,
    GitHubInstallationIdentity,
)
from openorc.domain.ownership import Repository
from openorc.observability import annotate_span, application_span
from openorc.persistence.github_installations import (
    create_or_reconcile_github_installation,
    get_github_installation,
)
from openorc.persistence.ownership import set_repository_installation_route
from openorc.persistence.pool import DatabasePool
from openorc.services.errors import ConflictError, InvalidCommandError, NotFoundError
from openorc.services.profile_lifecycle_guard import require_account_operational
from openorc.services.transaction_composition import composed_transaction
from openorc.services.workspace_authorization import (
    require_profile_workspace,
    require_workspace_repository,
)

__all__ = [
    "bind_repository_installation",
    "record_workspace_installation",
    "require_configured_repository_installation_route",
    "unbind_repository_installation",
]

# Application-service span boundaries (issues #108/#109): every public
# use-case operation of this module opens one span at the established service
# boundary. Only safe vocabulary attributes are attachable, so installation
# observation facts have no supported path into telemetry.
_SERVICE_TRACER_SCOPE = "openorc.services.github_installations"
_RECORD_SPAN_NAME = "github_installations.record_workspace_installation"
_BIND_SPAN_NAME = "github_installations.bind_repository_installation"
_UNBIND_SPAN_NAME = "github_installations.unbind_repository_installation"
_RESOLVE_SPAN_NAME = "github_installations.require_configured_repository_installation_route"


def record_workspace_installation(
    pool: DatabasePool,
    *,
    profile_id: UUID,
    workspace_id: UUID,
    github_installation_id: int,
    github_account_id: int,
    account_login: str,
    account_type: str,
    suspended_at: datetime | None,
) -> GitHubInstallation:
    """Create or reconcile one Workspace installation record from trusted facts.

    Owner-facing configuration over the Workspace authorization boundary. The
    inputs are trusted GitHub installation facts supplied by the caller that
    obtained them from GitHub (the transport that produces them is outside
    this leaf); validation is transport-independent domain validation.
    Reconciliation persists the reported facts as the current observations —
    account ID as reported, login/type, ``suspended_at`` — and never replaces
    the durable record identity (OpenOrc UUID, Workspace, external
    installation ID). No credential material is accepted or stored, and the
    operation requires no GitHub call.
    """
    with application_span(_SERVICE_TRACER_SCOPE, _RECORD_SPAN_NAME) as span:
        annotate_span(span, operation=_RECORD_SPAN_NAME, workspace_id=str(workspace_id))
        if suspended_at is not None and not isinstance(suspended_at, datetime):
            raise InvalidCommandError("suspended_at must be a datetime or None")
        try:
            identity = GitHubInstallationIdentity(github_installation_id=github_installation_id)
            account = GitHubInstallationAccount(
                github_account_id=github_account_id,
                login=account_login,
                type=account_type,
            )
        except GitHubInstallationDomainError as error:
            raise InvalidCommandError(f"invalid GitHub installation facts: {error}") from error
        with composed_transaction(pool) as transaction_pool:
            # The account-wide Owner-mutation barrier first (issue #97): the
            # Profile FOR KEY SHARE read is the first lock acquisition and
            # fails closed while an account deletion attempt is unresolved.
            require_account_operational(transaction_pool, profile_id=profile_id)
            workspace = require_profile_workspace(
                transaction_pool, profile_id=profile_id, workspace_id=workspace_id
            )
            return create_or_reconcile_github_installation(
                transaction_pool,
                workspace_id=workspace.id,
                identity=identity,
                account=account,
                suspended_at=suspended_at,
            )


def bind_repository_installation(
    pool: DatabasePool,
    *,
    profile_id: UUID,
    workspace_id: UUID,
    repository_id: UUID,
    github_installation_id: UUID,
) -> Repository:
    """Bind or rebind one Repository to one installation record of its Workspace.

    Owner-facing configuration over the Workspace authorization boundary. The
    installation must be a record of the authorized Workspace — a route into
    another Workspace is rejected at the service boundary as the uniform
    not-found outcome (and is unrepresentable durably through the composite
    route foreign key). The Repository's stable GitHub identity and observed
    metadata are never touched: binding changes the route configuration only,
    and it makes no claim about current repository access — later GitHub
    reconciliation establishes that.
    """
    with application_span(_SERVICE_TRACER_SCOPE, _BIND_SPAN_NAME) as span:
        annotate_span(span, operation=_BIND_SPAN_NAME, workspace_id=str(workspace_id))
        with composed_transaction(pool) as transaction_pool:
            # The account-wide Owner-mutation barrier first (issue #97).
            require_account_operational(transaction_pool, profile_id=profile_id)
            # require_workspace_repository re-enforces Profile ownership of
            # the Workspace before resolving the subject — no separate
            # Workspace read is composed.
            repository = require_workspace_repository(
                transaction_pool,
                profile_id=profile_id,
                workspace_id=workspace_id,
                repository_id=repository_id,
            )
            installation = get_github_installation(
                transaction_pool, installation_id=github_installation_id
            )
            if installation is None or installation.workspace_id != workspace_id:
                raise NotFoundError("the requested installation is not available in this workspace")
            try:
                updated = set_repository_installation_route(
                    transaction_pool,
                    repository_id=repository.id,
                    workspace_id=workspace_id,
                    github_installation_id=installation.id,
                )
            except ForeignKeyViolation as error:
                raise ConflictError(
                    "the installation route could not be applied: the installation record moved on"
                ) from error
            if updated is None:
                raise NotFoundError("the requested repository is not available in this workspace")
            return updated


def unbind_repository_installation(
    pool: DatabasePool,
    *,
    profile_id: UUID,
    workspace_id: UUID,
    repository_id: UUID,
) -> Repository:
    """Clear one Repository's installation route without deleting identity.

    Represents loss/absence of a configured route: the Repository record and
    its stable GitHub identity remain, the route configuration is simply
    absent, and the Repository is not usable for GitHub operations until it
    is explicitly bound again. Deleting or disconnecting OpenOrc
    configuration never uninstalls the GitHub App and never touches external
    GitHub artifacts.
    """
    with application_span(_SERVICE_TRACER_SCOPE, _UNBIND_SPAN_NAME) as span:
        annotate_span(span, operation=_UNBIND_SPAN_NAME, workspace_id=str(workspace_id))
        with composed_transaction(pool) as transaction_pool:
            # The account-wide Owner-mutation barrier first (issue #97).
            require_account_operational(transaction_pool, profile_id=profile_id)
            # require_workspace_repository re-enforces ownership before
            # resolving the subject — no separate Workspace read is composed.
            repository = require_workspace_repository(
                transaction_pool,
                profile_id=profile_id,
                workspace_id=workspace_id,
                repository_id=repository_id,
            )
            try:
                updated = set_repository_installation_route(
                    transaction_pool,
                    repository_id=repository.id,
                    workspace_id=workspace_id,
                    github_installation_id=None,
                )
            except ForeignKeyViolation as error:  # pragma: no cover - unrouting never violates
                raise ConflictError(
                    "the installation route could not be cleared: the repository record moved on"
                ) from error
            if updated is None:
                raise NotFoundError("the requested repository is not available in this workspace")
            return updated


def require_configured_repository_installation_route(
    pool: DatabasePool, *, profile_id: UUID, workspace_id: UUID, repository_id: UUID
) -> tuple[Repository, GitHubInstallation]:
    """Resolve the configured installation route for an authorized Workspace Repository.

    Returns the Repository and the exact GitHubInstallation record it is
    routed to. Raises ``NotFoundError`` — the uniform anti-probing outcome —
    when the Workspace is not owned by the Profile, the repository does not
    exist in it, or the repository has no configured route: a Repository
    without an explicit route is valid historical state but is not usable for
    GitHub operations, and OpenOrc never guesses an installation.

    This resolves the configured route only. It does not assert current
    external authorization, repository access, or effective permissions —
    resolution is deliberately independent of observed installation state
    (``suspended_at`` is not consulted). Later GitHub
    reconciliation/capability work must establish current access and
    permissions before any GitHub operation runs.
    """
    with application_span(_SERVICE_TRACER_SCOPE, _RESOLVE_SPAN_NAME) as span:
        annotate_span(span, operation=_RESOLVE_SPAN_NAME, workspace_id=str(workspace_id))
        # require_workspace_repository re-enforces ownership before resolving
        # the subject — no separate Workspace read is composed.
        repository = require_workspace_repository(
            pool, profile_id=profile_id, workspace_id=workspace_id, repository_id=repository_id
        )
        if repository.github_installation_id is None:
            raise NotFoundError(
                "the requested repository has no configured github installation route"
            )
        installation = get_github_installation(
            pool, installation_id=repository.github_installation_id
        )
        if installation is None or installation.workspace_id != workspace_id:
            # The composite route foreign key makes a foreign-Workspace route
            # unrepresentable and hard-deleting a routed installation is
            # rejected; a failure here means the installation record moved on
            # concurrently or durable state is inconsistent. Fail closed
            # rather than guessing.
            raise NotFoundError(
                "the requested repository installation route is not available in this workspace"
            )
        return repository, installation
