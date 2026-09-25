"""GitHub repository/issue authoritative-state reconciliation services
(issue #59).

The trusted, system-capable reconciliation primitive that turns fresh
authoritative GitHub observations into durable OpenOrc state:

- Authoritative reads only: every invocation obtains a fresh GitHub API
  observation — never a webhook payload, which is a notification that
  triggers this work, not canonical input. GitHub I/O occurs with no
  database transaction open.
- Stable identity: the Repository is reconciled by its stable GitHub
  repository ID and the issue projection by stable GitHub issue ID; mutable
  presentation metadata (owner/login/name/URL/visibility/default branch and
  the repository-local issue number) never becomes identity and never
  rebinds it. Issue-number reuse is a classified conflict, never a silent
  rebind.
- Currentness: the exact durable Repository/installation route used for the
  reads is revalidated under a row lock before any write, so an observation
  authorized through installation A is never committed after the route has
  moved to installation B. A moved route classifies the operation as stale
  and applies nothing.
- Race-safe serialization: the projection row lock plus the durable
  uniqueness constraints make concurrent reconciliations converge, and the
  returned facts describe the durable before→current transition this
  invocation actually established. An unchanged authoritative state is a
  true durable no-op.
- Separation from workflow consequences: this leaf detects and reports the
  requirements fingerprint before/current facts. It emits no WorkflowEvent
  and decides no Task-state transition — the canonical ``BLOCKED /
  GITHUB_SOURCE_CHANGED`` consequence belongs to the later Phase 2E
  workflow services.

Two boundaries share one implementation:

- :func:`reconcile_repository_issue` is the trusted system primitive for the
  later webhook-intake (#61) and repeatable-reconciliation/recovery (#62)
  layers. It takes durably resolved Workspace/Repository identity — no
  authenticated actor, no Owner masquerade — and fails closed uniformly.
- :func:`reconcile_repository_issue_for_owner` composes the #52/#57
  authenticated Owner authorization (anti-probing, uniform not-found) and
  delegates to the primitive.
- The account-deletion Owner-mutation barrier (#97) is derived from the
  owning Workspace's owner Profile inside the write transaction, so
  system-triggered reconciliation serializes against an in-flight account
  deletion exactly as authenticated Owner mutations do.

Access loss / repository disappearance is the normalized integration
condition (typed ``AuthorizationError``) for later recovery; historical
OpenOrc state is never deleted or rebound on its account. Known provider
failures — including rate limits, deliberately not misread as lost
authorization — are ``ExternalOperationFailedError``; uncertain outcomes
are ``ExternalOperationUncertainError`` and are never replayed here.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime
from uuid import UUID

from openorc.adapters.github import (
    GitHubAppClient,
    GitHubAuthenticationRejectedError,
    GitHubAuthorizationRejectedError,
    GitHubIssueObservation,
    GitHubOutcomeUncertainError,
    GitHubRateLimitedError,
    GitHubRepositoryObservation,
    GitHubRequestRejectedError,
)
from openorc.domain.github_issues import (
    GitHubIssueIdentity,
    GitHubIssueProjection,
    GitHubIssueState,
    github_issue_requirements_fingerprint,
)
from openorc.domain.ownership import Repository, RepositoryMetadata
from openorc.observability import annotate_span, application_span
from openorc.persistence.github_installations import get_github_installation
from openorc.persistence.github_issues import (
    GitHubIssueReconcileOutcome,
    GitHubIssueReconcileResult,
    reconcile_github_issue,
)
from openorc.persistence.ownership import (
    get_repository,
    get_repository_for_update,
    get_workspace,
    update_repository_metadata,
)
from openorc.persistence.pool import DatabasePool
from openorc.services.errors import (
    ApplicationError,
    AuthorizationError,
    ConflictError,
    ExternalOperationFailedError,
    ExternalOperationUncertainError,
    InvalidCommandError,
    NotFoundError,
    StaleOperationError,
)
from openorc.services.profile_lifecycle_guard import require_account_operational
from openorc.services.transaction_composition import composed_transaction
from openorc.services.workspace_authorization import require_workspace_repository

__all__ = [
    "RepositoryIssueReconciliation",
    "reconcile_repository_issue",
    "reconcile_repository_issue_for_owner",
]

logger = logging.getLogger(__name__)

# Application-service span boundaries (issues #108/#109): one span per public
# use-case operation at the established service boundary. Only the safe
# vocabulary is attachable, so credential material and provider content have
# no supported path into telemetry.
_SERVICE_TRACER_SCOPE = "openorc.services.github_reconciliation"
_RECONCILE_SPAN_NAME = "github_reconciliation.reconcile_repository_issue"
_OWNER_RECONCILE_SPAN_NAME = "github_reconciliation.reconcile_repository_issue_for_owner"


@dataclass(frozen=True, slots=True)
class RepositoryIssueReconciliation:
    """The normalized before/current facts of one reconciliation invocation.

    Every flag describes the durable transition this invocation actually
    established after conflict serialization: ``issue_created`` is true only
    for the invocation whose insert created the projection;
    ``requirements_changed`` is true only when a prior projection existed
    and its fingerprint differs (creation is by definition not a
    requirements change); ``issue_state_changed`` is true only when the
    authoritative open/closed ``GitHubIssueState`` changed between the
    serialized pre-image and the post-write projection — a
    requirements-only or provider-metadata-only durable write reports
    false; ``repository_metadata_changed`` reports an actual durable
    repository-metadata write. An unchanged or otherwise no-op
    reconciliation reports false everywhere and performs no write.
    """

    repository: Repository
    repository_metadata_changed: bool
    issue: GitHubIssueProjection
    issue_created: bool
    issue_state_changed: bool
    requirements_changed: bool
    previous_fingerprint: str | None


def _require_uuid_command(value: object, name: str) -> None:
    """Reject a malformed UUID command argument before any state is touched."""
    if not isinstance(value, UUID):
        raise InvalidCommandError(f"{name} must be a UUID")


def _require_issue_number_command(value: object) -> None:
    """Reject a malformed issue-number command argument (the durable address)."""
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise InvalidCommandError("issue_number must be a positive integer")


def _resolve_system_repository_installation_route(
    pool: DatabasePool, *, workspace_id: UUID, repository_id: UUID
) -> tuple[Repository, UUID, int]:
    """Resolve the durable Workspace Repository → installation route.

    The trusted system resolution used by the reconciliation primitive: no
    authenticated actor participates, and the boundary still fails closed
    uniformly — an absent or foreign-Workspace repository, an unconfigured
    route (valid historical state that is not usable for GitHub
    operations), or a missing/foreign installation record is the uniform
    not-found outcome. Returns the Repository, the OpenOrc installation
    record UUID the reads must be authorized through, and the stable
    external GitHub repository identity the adapter must address.
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
    return repository, installation_id, installation.identity.github_installation_id


def _observe_authoritative_state(
    github: GitHubAppClient,
    *,
    github_installation_id: int,
    github_repository_id: int,
    issue_number: int,
) -> tuple[GitHubRepositoryObservation, GitHubIssueObservation]:
    """Perform the authoritative GitHub reads with no database transaction open.

    The repository observation comes first: the documented stable-ID
    installation listing proves the access condition and observes the fresh
    repository address in one walk, and the issue read is addressed through
    that fresh owner/name — never stored mutable metadata — so a rename or
    ownership transfer is reconciled instead of breaking the read.

    The caller must hold no database transaction: route resolution completes
    its short transactions before this function runs, and the adapter
    performs the external calls with none open (proven by the service
    tests' pool seam).
    """
    repository_observation = github.get_installation_repository(
        github_installation_id=github_installation_id,
        github_repository_id=github_repository_id,
    )
    issue_observation = github.get_repository_issue(
        github_installation_id=github_installation_id,
        owner_login=repository_observation.owner_login,
        repository_name=repository_observation.name,
        issue_number=issue_number,
    )
    return repository_observation, issue_observation


def _translate_github_outcome(error: Exception) -> ApplicationError:
    """Translate the adapter's classified outcomes into the typed vocabulary.

    Authorization absence (lost repository access, capability shortfall,
    suspension, or the addressed resource's disappearance) is the safe
    classified ``AuthorizationError`` — the normalized integration/access
    condition for later recovery, never a trigger for a human-credential
    fallback. A rate limit is a known provider condition, deliberately NOT
    classified as lost authorization. Other definitive rejections are known
    failures; timeout/connection loss/uninterpretable answers remain
    explicitly uncertain and are never replayed here.
    """
    if isinstance(error, GitHubAuthorizationRejectedError):
        return AuthorizationError(
            "the routed GitHub installation does not currently grant access "
            "to the addressed repository or its issues"
        )
    if isinstance(error, GitHubRateLimitedError):
        return ExternalOperationFailedError(
            "the GitHub authoritative reconciliation read was rate limited"
        )
    if isinstance(error, GitHubAuthenticationRejectedError):
        return ExternalOperationFailedError("GitHub rejected the OpenOrc GitHub App authentication")
    if isinstance(error, GitHubRequestRejectedError):
        return ExternalOperationFailedError("GitHub rejected the authoritative reconciliation read")
    assert isinstance(error, GitHubOutcomeUncertainError)
    return ExternalOperationUncertainError(
        "the outcome of the authoritative GitHub reconciliation read is unknown"
    )


def _apply_reconciled_state(
    pool: DatabasePool,
    *,
    workspace_id: UUID,
    repository_id: UUID,
    read_installation_id: UUID,
    repository_observation: GitHubRepositoryObservation,
    identity: GitHubIssueIdentity,
    issue_number: int,
    title: str,
    body: str | None,
    state: GitHubIssueState,
    requirements_fingerprint: str,
    provider_updated_at: datetime | None,
) -> RepositoryIssueReconciliation:
    """Apply the serialized durable write phase inside one short transaction.

    Order matters and is load-bearing: the derived account-deletion barrier
    is the first lock acquisition; the Repository row is then re-loaded
    under lock and the exact route revalidated, so an observation read
    through installation A can never be committed after the route moved to
    B (a moved or unbound route is stale and applies nothing); the mutable
    repository metadata is updated only on actual difference; and the issue
    projection is written through the lock-serialize-compute-write contract.
    """
    with composed_transaction(pool) as tx_pool:
        workspace = get_workspace(tx_pool, workspace_id)
        if workspace is None:
            raise NotFoundError("the requested workspace is not available")
        # Account-wide Owner-mutation barrier (#97), derived from durable
        # Workspace ownership rather than an authenticated caller: the FIRST
        # lock acquisition of this write phase.
        require_account_operational(tx_pool, profile_id=workspace.owner_profile_id)
        reloaded = get_repository_for_update(tx_pool, repository_id)
        if reloaded is None or reloaded.workspace_id != workspace_id:
            raise NotFoundError("the requested repository is not available in this workspace")
        if reloaded.github_installation_id != read_installation_id:
            # The route moved on while the authoritative reads were in
            # flight: the observation was authorized through the superseded
            # installation. Apply nothing — the next reconciliation under the
            # current route re-reads authoritatively.
            raise StaleOperationError(
                "the repository's github installation route changed during reconciliation"
            )
        observed_metadata = RepositoryMetadata(
            owner_login=repository_observation.owner_login,
            name=repository_observation.name,
            html_url=repository_observation.html_url,
            is_private=repository_observation.is_private,
            default_branch=repository_observation.default_branch,
        )
        if reloaded.metadata == observed_metadata:
            current_repository = reloaded
            metadata_changed = False
        else:
            updated = update_repository_metadata(tx_pool, reloaded.id, metadata=observed_metadata)
            if updated is None:  # pragma: no cover - the row lock excludes deletion
                raise NotFoundError("the requested repository is not available in this workspace")
            current_repository = updated
            metadata_changed = True
        reconcile_result = reconcile_github_issue(
            tx_pool,
            workspace_id=workspace_id,
            repository_id=reloaded.id,
            github_issue_id=identity.github_issue_id,
            issue_number=issue_number,
            title=title,
            body=body,
            state=state,
            requirements_fingerprint=requirements_fingerprint,
            provider_updated_at=provider_updated_at,
        )
        return _map_reconcile_result(
            current_repository,
            metadata_changed,
            reconcile_result,
            requirements_fingerprint=requirements_fingerprint,
        )


def _map_reconcile_result(
    repository: Repository,
    metadata_changed: bool,
    result: GitHubIssueReconcileResult,
    *,
    requirements_fingerprint: str,
) -> RepositoryIssueReconciliation:
    """Map the serialized persistence outcome into the typed facts.

    Conflict outcomes fail closed without rebinding any identity; the
    creation/update/no-op outcomes become the before/current facts describing
    the durable transition this invocation established.
    """
    if result.outcome is GitHubIssueReconcileOutcome.NUMBER_CONFLICT:
        raise ConflictError(
            "the GitHub issue number durably maps to a different stable issue identity"
        )
    if result.outcome is GitHubIssueReconcileOutcome.IDENTITY_NUMBER_MISMATCH:
        raise ConflictError(
            "the stable GitHub issue identity is durably recorded under a different issue number"
        )
    if result.outcome is GitHubIssueReconcileOutcome.UNRESOLVABLE:
        raise ConflictError(
            "the issue projection write could not be serialized against current durable state"
        )
    assert result.projection is not None
    if result.outcome is GitHubIssueReconcileOutcome.INSERTED:
        return RepositoryIssueReconciliation(
            repository=repository,
            repository_metadata_changed=metadata_changed,
            issue=result.projection,
            issue_created=True,
            issue_state_changed=False,
            requirements_changed=False,
            previous_fingerprint=None,
        )
    if result.outcome is GitHubIssueReconcileOutcome.UPDATED:
        return RepositoryIssueReconciliation(
            repository=repository,
            repository_metadata_changed=metadata_changed,
            issue=result.projection,
            issue_created=False,
            # Only a genuine open/closed transition is a state change: a
            # requirements-only or provider-metadata-only write is not one.
            issue_state_changed=(
                result.previous_state is not None
                and result.previous_state is not result.projection.state
            ),
            requirements_changed=result.previous_fingerprint != requirements_fingerprint,
            previous_fingerprint=result.previous_fingerprint,
        )
    return RepositoryIssueReconciliation(
        repository=repository,
        repository_metadata_changed=metadata_changed,
        issue=result.projection,
        issue_created=False,
        issue_state_changed=False,
        requirements_changed=False,
        previous_fingerprint=result.previous_fingerprint,
    )


def reconcile_repository_issue(
    pool: DatabasePool,
    github: GitHubAppClient,
    *,
    workspace_id: UUID,
    repository_id: UUID,
    issue_number: int,
) -> RepositoryIssueReconciliation:
    """Reconcile one Workspace Repository's GitHub issue from fresh authority.

    The trusted, system-capable primitive for the later webhook-intake and
    repeatable-reconciliation layers. It takes durably resolved
    Workspace/Repository identity (no authenticated actor), resolves the
    configured installation route fail-closed, performs the authoritative
    GitHub reads with no database transaction open, revalidates the exact
    route under a row lock before writing, and returns the serialized
    before/current facts.

    Raises ``NotFoundError`` uniformly for an absent or foreign-Workspace
    repository, an unconfigured route, or a missing/foreign installation
    record; ``ConflictError`` for issue-number reuse against a different
    stable identity, an identity recorded under a different number, an
    unserializable write, or a pull-request-shaped number;
    ``StaleOperationError`` when the route moved during the reads;
    ``AuthorizationError`` for the normalized access-loss condition;
    ``ExternalOperationFailedError`` for other known provider failures;
    ``ExternalOperationUncertainError`` for unknown outcomes.
    """
    _require_uuid_command(workspace_id, "workspace_id")
    _require_uuid_command(repository_id, "repository_id")
    _require_issue_number_command(issue_number)
    with application_span(_SERVICE_TRACER_SCOPE, _RECONCILE_SPAN_NAME) as span:
        # Attach only after the caller-supplied identifiers proved valid: a
        # malformed command is classified without exporting its values.
        annotate_span(span, operation=_RECONCILE_SPAN_NAME, workspace_id=str(workspace_id))
        repository, installation_id, external_installation_id = (
            _resolve_system_repository_installation_route(
                pool, workspace_id=workspace_id, repository_id=repository_id
            )
        )
        try:
            repository_observation, issue_observation = _observe_authoritative_state(
                github,
                github_installation_id=external_installation_id,
                github_repository_id=repository.identity.github_repository_id,
                issue_number=issue_number,
            )
        except (
            GitHubAuthorizationRejectedError,
            GitHubRateLimitedError,
            GitHubAuthenticationRejectedError,
            GitHubRequestRejectedError,
            GitHubOutcomeUncertainError,
        ) as error:
            raise _translate_github_outcome(error) from error
        if issue_observation.is_pull_request:
            raise ConflictError("the addressed GitHub number is a pull request, not an issue")
        identity = GitHubIssueIdentity(github_issue_id=issue_observation.github_issue_id)
        state = GitHubIssueState(issue_observation.state)
        fingerprint = github_issue_requirements_fingerprint(
            issue_observation.title, issue_observation.body
        )
        return _apply_reconciled_state(
            pool,
            workspace_id=workspace_id,
            repository_id=repository_id,
            read_installation_id=installation_id,
            repository_observation=repository_observation,
            identity=identity,
            issue_number=issue_number,
            title=issue_observation.title,
            body=issue_observation.body,
            state=state,
            requirements_fingerprint=fingerprint,
            provider_updated_at=issue_observation.provider_updated_at,
        )


def reconcile_repository_issue_for_owner(
    pool: DatabasePool,
    github: GitHubAppClient,
    *,
    profile_id: UUID,
    workspace_id: UUID,
    repository_id: UUID,
    issue_number: int,
) -> RepositoryIssueReconciliation:
    """Reconcile one issue under the authenticated Owner authorization boundary.

    Composes the #52/#57 ownership gate (anti-probing, uniform not-found for
    an unowned Workspace, an absent repository, or an unconfigured route)
    and delegates to the trusted primitive. The account-deletion barrier is
    derived inside the primitive from durable Workspace ownership, so it is
    never duplicated here.
    """
    _require_uuid_command(profile_id, "profile_id")
    _require_uuid_command(workspace_id, "workspace_id")
    _require_uuid_command(repository_id, "repository_id")
    _require_issue_number_command(issue_number)
    with application_span(_SERVICE_TRACER_SCOPE, _OWNER_RECONCILE_SPAN_NAME) as span:
        annotate_span(span, operation=_OWNER_RECONCILE_SPAN_NAME, workspace_id=str(workspace_id))
        require_workspace_repository(
            pool, profile_id=profile_id, workspace_id=workspace_id, repository_id=repository_id
        )
        return reconcile_repository_issue(
            pool,
            github,
            workspace_id=workspace_id,
            repository_id=repository_id,
            issue_number=issue_number,
        )
