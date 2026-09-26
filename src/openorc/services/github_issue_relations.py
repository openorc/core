"""GitHub issue relationship synchronization services (issue #60).

Turn fresh authoritative GitHub relationship observations into the durable
presentation-only mirrors. Two independent reconciliation units share this
leaf; they share no transaction and no fate:

- ``synchronize_repository_issue_dependencies`` — the blocked-by dependency
  unit. This is AUTHORITATIVE INPUT TO ELIGIBILITY: the fresh observation is
  mirrored before any intake decision, and an unobservable blocking state
  fails closed. Same-repository edges reuse the local Repository's known
  stable GitHub repository ID with zero extra reads; distinct
  cross-repository ``repository_url`` references are deduplicated and
  resolved to stable numeric IDs through documented REST repository reads,
  all with no database transaction open. If a required stable identity
  cannot be established, the relationship observation classifies as
  unobservable and the unit fails closed — mutable owner/name/URL data is
  never treated as identity.
- ``synchronize_repository_issue_hierarchy`` — the parent + sub-issue unit.
  Hierarchy is deliberately NON-GATING: it is descriptive presentation
  state whose outcome never affects Task intake eligibility. The parent edge
  comes from the documented GraphQL nullable ``Issue.parent`` read — the
  only authoritative surface expressing both presence and absence (the REST
  parent endpoint documents only 200/301/404/410, so a REST 404 is an
  error, never ``NO_PARENT``).

Shared error contract: any GitHub error/uncertain outcome fails its unit
and leaves the prior mirror byte-identical — the last successfully observed
projection, never restamped as freshly reconciled truth. Both units use the
same system/Owner two-boundary pattern as B3 reconciliation; the
account-deletion barrier is derived inside the write phase from durable
Workspace ownership. Webhook payload relationship data is a routing hint,
never canonical input.

Event emission: ``task_dependency_synced`` / ``task_relationship_synced``
(actor GITHUB, minimal context) are recorded atomically with the mirror
write, only when the classified outcome is CHANGED.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from urllib.parse import urlparse
from uuid import UUID

from openorc.adapters.github import (
    GitHubAppClient,
    GitHubAuthenticationRejectedError,
    GitHubAuthorizationRejectedError,
    GitHubOutcomeUncertainError,
    GitHubRateLimitedError,
    GitHubRelatedIssueObservation,
    GitHubRequestRejectedError,
)
from openorc.domain.events import WorkflowEventActor, WorkflowEventType
from openorc.domain.github_issue_relations import RelatedIssueEndpoint
from openorc.domain.ownership import Repository
from openorc.observability import annotate_span, application_span
from openorc.persistence.events import record_workflow_event
from openorc.persistence.github_installations import get_github_installation
from openorc.persistence.github_issue_relations import (
    GitHubRelationReplaceOutcome,
    replace_github_issue_dependency_edges,
    replace_github_issue_parent_edge,
    replace_github_issue_sub_issue_edges,
)
from openorc.persistence.ownership import (
    get_repository,
    get_repository_for_update,
    get_workspace,
)
from openorc.persistence.pool import DatabasePool
from openorc.services.errors import (
    ApplicationError,
    AuthorizationError,
    ExternalOperationFailedError,
    ExternalOperationUncertainError,
    InvalidCommandError,
    NotFoundError,
    StaleOperationError,
)
from openorc.services.event_coordination import WorkflowActorContext
from openorc.services.profile_lifecycle_guard import require_account_operational
from openorc.services.transaction_composition import composed_transaction
from openorc.services.workspace_authorization import require_workspace_repository

__all__ = [
    "DependencySyncResult",
    "HierarchySyncResult",
    "synchronize_repository_issue_dependencies",
    "synchronize_repository_issue_dependencies_for_owner",
    "synchronize_repository_issue_hierarchy",
    "synchronize_repository_issue_hierarchy_for_owner",
]

logger = logging.getLogger(__name__)

_SERVICE_TRACER_SCOPE = "openorc.services.github_issue_relations"
_DEPENDENCY_SPAN_NAME = "github_issue_relations.synchronize_dependencies"
_HIERARCHY_SPAN_NAME = "github_issue_relations.synchronize_hierarchy"
_OWNER_DEPENDENCY_SPAN_NAME = "github_issue_relations.synchronize_dependencies_for_owner"
_OWNER_HIERARCHY_SPAN_NAME = "github_issue_relations.synchronize_hierarchy_for_owner"

_GITHUB_ACTOR = WorkflowActorContext(WorkflowEventActor.GITHUB, None)


@dataclass(frozen=True, slots=True)
class DependencySyncResult:
    """The classified durable outcome of one dependency-mirror synchronization.

    ``blocked`` is the derived current blocking state of the FRESH
    observation (an empty blocked-by edge set is not blocked) — the value
    the intake path relies on after this unit succeeds. ``changed`` is true
    exactly when this invocation durably advanced the mirror.
    """

    blocked: bool
    changed: bool


@dataclass(frozen=True, slots=True)
class HierarchySyncResult:
    """The classified durable outcome of one hierarchy-mirror synchronization.

    ``changed`` is true exactly when this invocation durably advanced the
    parent or sub-issue mirror. Hierarchy is non-gating presentation state:
    nothing here ever feeds a Task-eligibility decision.
    """

    changed: bool


def _require_uuid_command(value: object, name: str) -> None:
    if not isinstance(value, UUID):
        raise InvalidCommandError(f"{name} must be a UUID")


def _require_issue_number_command(value: object) -> None:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise InvalidCommandError("issue_number must be a positive integer")


def _translate_github_outcome(error: Exception) -> ApplicationError:
    """Translate the adapter's classified outcomes into the typed vocabulary."""
    if isinstance(error, GitHubAuthorizationRejectedError):
        return AuthorizationError(
            "the routed GitHub installation does not currently grant access "
            "to the addressed repository or its relationships"
        )
    if isinstance(error, GitHubRateLimitedError):
        return ExternalOperationFailedError("the GitHub relationship observation was rate limited")
    if isinstance(error, GitHubAuthenticationRejectedError):
        return ExternalOperationFailedError("GitHub rejected the OpenOrc GitHub App authentication")
    if isinstance(error, GitHubRequestRejectedError):
        return ExternalOperationFailedError("GitHub rejected the relationship observation")
    assert isinstance(error, GitHubOutcomeUncertainError)
    return ExternalOperationUncertainError(
        "the outcome of the GitHub relationship observation is unknown"
    )


def _resolve_system_repository_installation_route(
    pool: DatabasePool, *, workspace_id: UUID, repository_id: UUID
) -> tuple[Repository, UUID, int]:
    """Resolve the durable Workspace Repository → installation route (fail closed).

    The trusted system resolution: no authenticated actor participates, and
    the boundary still fails closed uniformly — an absent or
    foreign-Workspace repository, an unconfigured route, or a missing or
    foreign installation record is the uniform not-found outcome.
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


def _repository_address_from_url(repository_url: str) -> tuple[str, str] | None:
    """Extract the (owner, name) address from a documented repository_url.

    Returns ``None`` when the URL is not an https github.com repository
    reference: the caller then treats the relation as unresolvable rather
    than guessing. The URL is a transient resolution input inside one fresh
    observation — never identity, never persisted.
    """
    parsed = urlparse(repository_url)
    if parsed.scheme != "https" or parsed.hostname != "github.com":
        return None
    parts = [part for part in parsed.path.split("/") if part]
    if len(parts) != 2:
        return None
    return parts[0], parts[1]


def _resolve_related_endpoints(
    github: GitHubAppClient,
    *,
    github_installation_id: int,
    local_github_repository_id: int,
    related: list[GitHubRelatedIssueObservation],
) -> list[RelatedIssueEndpoint]:
    """Resolve related-issue observations into stable numeric endpoints.

    Same-repository references (the documented ``repository_url`` naming the
    local repository itself) reuse the known stable GitHub repository ID
    with zero extra reads. Each distinct cross-repository reference is
    resolved — deduplicated within the observation — through one documented
    REST repository read. A non-GitHub or unparseable reference fails the
    observation closed: mutable address data is never treated as identity.
    """
    resolved: list[RelatedIssueEndpoint] = []
    resolution_cache: dict[tuple[str, str], int] = {}
    for observation in related:
        address = _repository_address_from_url(observation.repository_url)
        if address is None:
            # An unparseable/non-GitHub reference cannot establish the stable
            # identity this mirror requires: fail the observation closed.
            raise ExternalOperationUncertainError(
                "the related issue's repository reference is not resolvable "
                "to a stable GitHub repository identity"
            )
        cached = resolution_cache.get(address)
        if cached is not None:
            github_repository_id = cached
        else:
            github_repository_id = github.get_repository_by_address(
                github_installation_id=github_installation_id,
                owner_login=address[0],
                repository_name=address[1],
            )
            resolution_cache[address] = github_repository_id
        resolved.append(
            RelatedIssueEndpoint(
                github_repository_id=github_repository_id,
                github_issue_id=observation.github_issue_id,
            )
        )
    return resolved


def _revalidate_write_phase(
    tx_pool: DatabasePool, *, workspace_id: UUID, repository_id: UUID, read_installation_id: UUID
) -> None:
    """Revalidate Workspace ownership, the account barrier, and the route under lock.

    The exact durable Repository/installation route used for the reads is
    reloaded under a row lock before any write, so an observation authorized
    through installation A is never committed after the route moved to B (a
    moved or unbound route is stale and applies nothing). The
    account-deletion Owner-mutation barrier (#97) is derived from durable
    Workspace ownership — the FIRST lock acquisition of this write phase.
    """
    workspace = get_workspace(tx_pool, workspace_id)
    if workspace is None:
        raise NotFoundError("the requested workspace is not available")
    require_account_operational(tx_pool, profile_id=workspace.owner_profile_id)
    reloaded = get_repository_for_update(tx_pool, repository_id)
    if reloaded is None or reloaded.workspace_id != workspace_id:
        raise NotFoundError("the requested repository is not available in this workspace")
    if reloaded.github_installation_id != read_installation_id:
        raise StaleOperationError(
            "the repository's github installation route changed during synchronization"
        )


def _record_relation_event(
    tx_pool: DatabasePool,
    *,
    workspace_id: UUID,
    repository_id: UUID,
    github_issue_id: int,
    event_type: WorkflowEventType,
) -> None:
    """Record one change-only relationship audit event (actor GITHUB).

    Context is minimal safe routing metadata — the local Repository UUID and
    the stable GitHub issue ID — never a duplicated canonical record, never
    provider URLs or bodies. The generic subject pair stays absent: this is
    a Workspace-scoped GitHub-relationship fact, not a Task-scoped one.
    """
    record_workflow_event(
        tx_pool,
        workspace_id=workspace_id,
        event_type=event_type,
        actor_type=_GITHUB_ACTOR.actor_type,
        actor_id=_GITHUB_ACTOR.actor_id,
        context={
            "repository_id": str(repository_id),
            "github_issue_id": github_issue_id,
        },
    )


def synchronize_repository_issue_dependencies(
    pool: DatabasePool,
    github: GitHubAppClient,
    *,
    workspace_id: UUID,
    repository_id: UUID,
    issue_number: int,
    github_issue_id: int,
) -> DependencySyncResult:
    """Synchronize the blocked-by dependency mirror from fresh authority.

    The authoritative-input-to-eligibility unit: resolve the route, observe
    the fresh blocked-by listing and resolve each related endpoint's stable
    identity with no database transaction open, then revalidate the route
    under a row lock and replace the mirror inside one short transaction
    (recording the change-only ``task_dependency_synced`` event when the
    mirror actually changed). The subject binds to the stable GitHub issue
    identity established by the fresh issue observation composed upstream
    (B3 reconcile) — never to the mutable issue number. An unobservable
    listing or an unresolvable related repository fails the unit closed and
    leaves the prior mirror byte-identical.
    """
    _require_uuid_command(workspace_id, "workspace_id")
    _require_uuid_command(repository_id, "repository_id")
    _require_issue_number_command(issue_number)
    if (
        isinstance(github_issue_id, bool)
        or not isinstance(github_issue_id, int)
        or github_issue_id <= 0
    ):
        raise InvalidCommandError("github_issue_id must be a positive integer")
    with application_span(_SERVICE_TRACER_SCOPE, _DEPENDENCY_SPAN_NAME) as span:
        annotate_span(span, operation=_DEPENDENCY_SPAN_NAME, workspace_id=str(workspace_id))
        repository, installation_id, external_installation_id = (
            _resolve_system_repository_installation_route(
                pool, workspace_id=workspace_id, repository_id=repository_id
            )
        )
        try:
            fresh_address = github.get_installation_repository(
                github_installation_id=external_installation_id,
                github_repository_id=repository.identity.github_repository_id,
            )
            related = github.get_issue_blocked_by(
                github_installation_id=external_installation_id,
                owner_login=fresh_address.owner_login,
                repository_name=fresh_address.name,
                issue_number=issue_number,
            )
            endpoints = _resolve_related_endpoints(
                github,
                github_installation_id=external_installation_id,
                local_github_repository_id=repository.identity.github_repository_id,
                related=related,
            )
        except (
            GitHubAuthorizationRejectedError,
            GitHubRateLimitedError,
            GitHubAuthenticationRejectedError,
            GitHubRequestRejectedError,
            GitHubOutcomeUncertainError,
        ) as error:
            raise _translate_github_outcome(error) from error
        outcome = _apply_dependency_mirror(
            pool,
            workspace_id=workspace_id,
            repository_id=repository_id,
            github_issue_id=github_issue_id,
            read_installation_id=installation_id,
            endpoints=endpoints,
        )
        return DependencySyncResult(
            blocked=bool(endpoints), changed=outcome is GitHubRelationReplaceOutcome.CHANGED
        )


def _apply_dependency_mirror(
    pool: DatabasePool,
    *,
    workspace_id: UUID,
    repository_id: UUID,
    github_issue_id: int,
    read_installation_id: UUID,
    endpoints: list[RelatedIssueEndpoint],
) -> GitHubRelationReplaceOutcome:
    """Apply the serialized dependency-mirror write inside one short transaction."""
    with composed_transaction(pool) as tx_pool:
        _revalidate_write_phase(
            tx_pool,
            workspace_id=workspace_id,
            repository_id=repository_id,
            read_installation_id=read_installation_id,
        )
        outcome = replace_github_issue_dependency_edges(
            tx_pool,
            workspace_id=workspace_id,
            repository_id=repository_id,
            github_issue_id=github_issue_id,
            blockers=endpoints,
        )
        if outcome is GitHubRelationReplaceOutcome.CHANGED:
            _record_relation_event(
                tx_pool,
                workspace_id=workspace_id,
                repository_id=repository_id,
                github_issue_id=github_issue_id,
                event_type=WorkflowEventType.TASK_DEPENDENCY_SYNCED,
            )
        return outcome


def _apply_hierarchy_mirror(
    pool: DatabasePool,
    *,
    workspace_id: UUID,
    repository_id: UUID,
    github_issue_id: int,
    read_installation_id: UUID,
    parent: RelatedIssueEndpoint | None,
    children: list[RelatedIssueEndpoint],
) -> GitHubRelationReplaceOutcome:
    """Apply the serialized hierarchy-mirror write inside one short transaction."""
    with composed_transaction(pool) as tx_pool:
        _revalidate_write_phase(
            tx_pool,
            workspace_id=workspace_id,
            repository_id=repository_id,
            read_installation_id=read_installation_id,
        )
        parent_outcome = replace_github_issue_parent_edge(
            tx_pool,
            workspace_id=workspace_id,
            repository_id=repository_id,
            github_issue_id=github_issue_id,
            parent=parent,
        )
        children_outcome = replace_github_issue_sub_issue_edges(
            tx_pool,
            workspace_id=workspace_id,
            repository_id=repository_id,
            github_issue_id=github_issue_id,
            children=children,
        )
        outcome = (
            GitHubRelationReplaceOutcome.CHANGED
            if GitHubRelationReplaceOutcome.CHANGED in (parent_outcome, children_outcome)
            else GitHubRelationReplaceOutcome.UNCHANGED
        )
        if outcome is GitHubRelationReplaceOutcome.CHANGED:
            _record_relation_event(
                tx_pool,
                workspace_id=workspace_id,
                repository_id=repository_id,
                github_issue_id=github_issue_id,
                event_type=WorkflowEventType.TASK_RELATIONSHIP_SYNCED,
            )
        return outcome


def synchronize_repository_issue_hierarchy(
    pool: DatabasePool,
    github: GitHubAppClient,
    *,
    workspace_id: UUID,
    repository_id: UUID,
    issue_number: int,
    github_issue_id: int,
) -> HierarchySyncResult:
    """Synchronize the parent + sub-issue hierarchy mirror from fresh authority.

    The NON-GATING presentation unit: the parent edge is observed through
    the documented GraphQL nullable ``Issue.parent`` read (authoritative
    presence AND absence) and the sub-issue listing through its documented
    REST read, with no database transaction open; the mirrors are then
    replaced inside one short transaction after route revalidation (the
    change-only ``task_relationship_synced`` event when anything changed).
    An unobservable read fails the unit and preserves the prior mirror —
    hierarchy failure never affects Task intake eligibility.
    """
    _require_uuid_command(workspace_id, "workspace_id")
    _require_uuid_command(repository_id, "repository_id")
    _require_issue_number_command(issue_number)
    with application_span(_SERVICE_TRACER_SCOPE, _HIERARCHY_SPAN_NAME) as span:
        annotate_span(span, operation=_HIERARCHY_SPAN_NAME, workspace_id=str(workspace_id))
        repository, installation_id, external_installation_id = (
            _resolve_system_repository_installation_route(
                pool, workspace_id=workspace_id, repository_id=repository_id
            )
        )
        try:
            fresh_address = github.get_installation_repository(
                github_installation_id=external_installation_id,
                github_repository_id=repository.identity.github_repository_id,
            )
            parent_observation = github.get_issue_parent(
                github_installation_id=external_installation_id,
                owner_login=fresh_address.owner_login,
                repository_name=fresh_address.name,
                issue_number=issue_number,
            )
            related_children = github.get_issue_sub_issues(
                github_installation_id=external_installation_id,
                owner_login=fresh_address.owner_login,
                repository_name=fresh_address.name,
                issue_number=issue_number,
            )
            child_endpoints = _resolve_related_endpoints(
                github,
                github_installation_id=external_installation_id,
                local_github_repository_id=repository.identity.github_repository_id,
                related=related_children,
            )
        except (
            GitHubAuthorizationRejectedError,
            GitHubRateLimitedError,
            GitHubAuthenticationRejectedError,
            GitHubRequestRejectedError,
            GitHubOutcomeUncertainError,
        ) as error:
            raise _translate_github_outcome(error) from error
        outcome = _apply_hierarchy_mirror(
            pool,
            workspace_id=workspace_id,
            repository_id=repository_id,
            github_issue_id=github_issue_id,
            read_installation_id=installation_id,
            parent=(
                RelatedIssueEndpoint(
                    github_repository_id=parent_observation.parent.github_repository_id,
                    github_issue_id=parent_observation.parent.github_issue_id,
                )
                if parent_observation.parent is not None
                else None
            ),
            children=child_endpoints,
        )
        return HierarchySyncResult(outcome is GitHubRelationReplaceOutcome.CHANGED)


def synchronize_repository_issue_dependencies_for_owner(
    pool: DatabasePool,
    github: GitHubAppClient,
    *,
    profile_id: UUID,
    workspace_id: UUID,
    repository_id: UUID,
    issue_number: int,
    github_issue_id: int,
) -> DependencySyncResult:
    """Synchronize dependencies under the authenticated Owner authorization boundary."""
    _require_uuid_command(profile_id, "profile_id")
    _require_uuid_command(workspace_id, "workspace_id")
    _require_uuid_command(repository_id, "repository_id")
    _require_issue_number_command(issue_number)
    with application_span(_SERVICE_TRACER_SCOPE, _OWNER_DEPENDENCY_SPAN_NAME) as span:
        annotate_span(span, operation=_OWNER_DEPENDENCY_SPAN_NAME, workspace_id=str(workspace_id))
        require_workspace_repository(
            pool, profile_id=profile_id, workspace_id=workspace_id, repository_id=repository_id
        )
        return synchronize_repository_issue_dependencies(
            pool,
            github,
            workspace_id=workspace_id,
            repository_id=repository_id,
            issue_number=issue_number,
            github_issue_id=github_issue_id,
        )


def synchronize_repository_issue_hierarchy_for_owner(
    pool: DatabasePool,
    github: GitHubAppClient,
    *,
    profile_id: UUID,
    workspace_id: UUID,
    repository_id: UUID,
    issue_number: int,
    github_issue_id: int,
) -> HierarchySyncResult:
    """Synchronize hierarchy under the authenticated Owner authorization boundary."""
    _require_uuid_command(profile_id, "profile_id")
    _require_uuid_command(workspace_id, "workspace_id")
    _require_uuid_command(repository_id, "repository_id")
    _require_issue_number_command(issue_number)
    with application_span(_SERVICE_TRACER_SCOPE, _OWNER_HIERARCHY_SPAN_NAME) as span:
        annotate_span(span, operation=_OWNER_HIERARCHY_SPAN_NAME, workspace_id=str(workspace_id))
        require_workspace_repository(
            pool, profile_id=profile_id, workspace_id=workspace_id, repository_id=repository_id
        )
        return synchronize_repository_issue_hierarchy(
            pool,
            github,
            workspace_id=workspace_id,
            repository_id=repository_id,
            issue_number=issue_number,
            github_issue_id=github_issue_id,
        )
