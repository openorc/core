"""GitHub installation claim services (issue #372).

The Owner-facing configuration entry for the Workspace GitHub App installation
mapping. An installation claim is UNVERIFIED Owner configuration: the
authenticated Owner is authorized to configure which installation routes the
Workspace's GitHub operations, but that authorization never establishes that
the submitted stable identity is genuine, so the claim carries the stable
external installation ID alone and is persisted in claim state — observation
columns NULL. It can never carry or write observation facts (the stable
account identity, login/type, ``suspended_at``): only trusted GitHub
reconciliation
(:func:`openorc.services.github_installations.record_workspace_installation`)
later populates them, atomically establishing the claim's stable account
identity. A claim or its route never implies access or permission, and
recorded status is durable evidence, never live GitHub freshness.

Owner-facing reads compose the existing Workspace authorization boundary.
Reads compose no account-mutation barrier; the claim mutation composes the
account-wide Owner-mutation barrier (issue #97) first, like every guarded
Owner mutation. Cross-Workspace subjects are the uniform not-found outcome.
"""

from __future__ import annotations

from uuid import UUID

from openorc.domain.github_installations import (
    GitHubInstallation,
    GitHubInstallationDomainError,
    GitHubInstallationIdentity,
)
from openorc.observability import annotate_span, application_span
from openorc.persistence.github_installations import (
    claim_github_installation,
    get_github_installation,
    list_workspace_installations,
)
from openorc.persistence.pool import DatabasePool
from openorc.services.errors import InvalidCommandError, NotFoundError
from openorc.services.profile_lifecycle_guard import require_account_operational
from openorc.services.transaction_composition import composed_transaction
from openorc.services.workspace_authorization import require_profile_workspace

__all__ = [
    "list_workspace_installation_claims",
    "read_installation_claim",
    "record_installation_claim",
]

# Application-service span boundaries (issues #108/#109): every public
# use-case operation of this module opens one span at the established service
# boundary. Only the safe attribute vocabulary is attachable.
_SERVICE_TRACER_SCOPE = "openorc.services.github_installation_claims"
_CLAIM_SPAN_NAME = "github_installation_claims.record_installation_claim"
_LIST_SPAN_NAME = "github_installation_claims.list_workspace_installation_claims"
_READ_SPAN_NAME = "github_installation_claims.read_installation_claim"


def _require_uuid_command(value: object, name: str) -> None:
    """Reject a malformed UUID command argument before any state is touched."""
    if not isinstance(value, UUID):
        raise InvalidCommandError(f"{name} must be a UUID")


def record_installation_claim(
    pool: DatabasePool,
    *,
    profile_id: UUID,
    workspace_id: UUID,
    github_installation_id: int,
) -> GitHubInstallation:
    """Record one Owner installation claim for the authorized Workspace.

    The claim carries the stable external installation ID alone: it is
    unverified Owner configuration persisted in claim state, never a GitHub
    observation, and it never implies access or permission. Re-claiming an
    already-claimed or already-established installation is idempotent and
    returns the durable record — in whatever state trusted reconciliation has
    established — unchanged.
    """
    with application_span(_SERVICE_TRACER_SCOPE, _CLAIM_SPAN_NAME) as span:
        _require_uuid_command(profile_id, "profile_id")
        _require_uuid_command(workspace_id, "workspace_id")
        try:
            identity = GitHubInstallationIdentity(github_installation_id=github_installation_id)
        except GitHubInstallationDomainError as error:
            raise InvalidCommandError(f"invalid GitHub installation identity: {error}") from error
        # Attach only after the caller-supplied identifiers proved valid: a
        # malformed command is classified without exporting its values.
        annotate_span(span, operation=_CLAIM_SPAN_NAME, workspace_id=str(workspace_id))
        with composed_transaction(pool) as transaction_pool:
            # The account-wide Owner-mutation barrier first (issue #97).
            require_account_operational(transaction_pool, profile_id=profile_id)
            workspace = require_profile_workspace(
                transaction_pool, profile_id=profile_id, workspace_id=workspace_id
            )
            return claim_github_installation(
                transaction_pool, workspace_id=workspace.id, identity=identity
            )


def list_workspace_installation_claims(
    pool: DatabasePool, *, profile_id: UUID, workspace_id: UUID
) -> list[GitHubInstallation]:
    """List the Workspace's installation mapping records (claims and established).

    The owner-scoped Workspace authorization IS the isolation boundary. The
    returned records carry their observation state verbatim: ``account is
    None`` is the unverified claim state, and observed facts appear only once
    trusted GitHub reconciliation has recorded them. Recorded status is not
    live GitHub freshness and never implies access or permission.
    """
    with application_span(_SERVICE_TRACER_SCOPE, _LIST_SPAN_NAME) as span:
        _require_uuid_command(profile_id, "profile_id")
        _require_uuid_command(workspace_id, "workspace_id")
        annotate_span(span, operation=_LIST_SPAN_NAME, workspace_id=str(workspace_id))
        workspace = require_profile_workspace(
            pool, profile_id=profile_id, workspace_id=workspace_id
        )
        return list_workspace_installations(pool, workspace_id=workspace.id)


def read_installation_claim(
    pool: DatabasePool, *, profile_id: UUID, workspace_id: UUID, installation_id: UUID
) -> GitHubInstallation:
    """Read one installation mapping record of the authorized Workspace.

    A foreign or absent installation is the uniform not-found outcome, so
    probing exact record UUIDs across Workspaces is indistinguishable from
    addressing something absent.
    """
    with application_span(_SERVICE_TRACER_SCOPE, _READ_SPAN_NAME) as span:
        _require_uuid_command(profile_id, "profile_id")
        _require_uuid_command(workspace_id, "workspace_id")
        _require_uuid_command(installation_id, "installation_id")
        annotate_span(span, operation=_READ_SPAN_NAME, workspace_id=str(workspace_id))
        workspace = require_profile_workspace(
            pool, profile_id=profile_id, workspace_id=workspace_id
        )
        installation = get_github_installation(pool, installation_id=installation_id)
        if installation is None or installation.workspace_id != workspace.id:
            raise NotFoundError("the requested installation is not available in this workspace")
        return installation
