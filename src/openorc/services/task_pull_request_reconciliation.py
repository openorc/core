"""TaskPullRequest authoritative reconciliation (issue #63).

The trusted, system-capable reconciliation primitive for the Task's one
canonical pull request: it turns a fresh authoritative GitHub PR observation
into durable OpenOrc state.

- Authoritative reads only: every invocation obtains a fresh GitHub API
  observation — never a webhook payload, which is a notification that
  triggers this work, not canonical input. GitHub I/O occurs with no
  database transaction open.
- Stable identity: the record is addressed by its durable Workspace/Task
  scope and reconciled only when the addressed PR number still reports the
  record's stable ``github_pr_id``; a different stable identity under the
  repository-local number is a classified conflict, never a silent rebind
  and never a replacement-PR row. One Task has zero or one canonical
  TaskPullRequest in v1; reconciliation updates that one external object in
  place — there is no creation path here (the canonical record's only
  creator is the later race-safe PR publication operation) and no
  external-PR adoption path.
- Currentness: the exact durable Repository/installation route used for the
  reads is revalidated under a row lock before any write, so an observation
  authorized through installation A is never committed after the route has
  moved to installation B; the derived account-deletion Owner-mutation
  barrier is the first lock acquisition of the write phase, exactly as the
  #59 reconciliation composes it.
- Serialized facts: the observed-snapshot write returns the locked
  pre-image, so the returned before→current facts distinguish a changed PR
  head (the review subject changed) from a base-only change (which never
  invalidates OpenOrc exact-head acceptance) and lifecycle transitions,
  computed from durable serialized state rather than a racy re-read.
- Separation from workflow consequences: this leaf reconciles and reports
  facts. It emits no WorkflowEvent and decides no Task-state transition —
  the canonical consequences belong to the later Phase 2E workflow services.

Access loss / repository disappearance is the normalized integration
condition (typed ``AuthorizationError``) for later recovery; known provider
failures — including rate limits, deliberately not misread as lost
authorization — are ``ExternalOperationFailedError``; uncertain outcomes are
``ExternalOperationUncertainError`` and are never replayed here.
"""

from __future__ import annotations

from dataclasses import dataclass
from uuid import UUID

from openorc.adapters.github import (
    GitHubAppClient,
    GitHubAuthenticationRejectedError,
    GitHubAuthorizationRejectedError,
    GitHubOutcomeUncertainError,
    GitHubPullRequestObservation,
    GitHubRateLimitedError,
    GitHubRepositoryObservation,
    GitHubRequestRejectedError,
)
from openorc.domain.pull_requests import TaskPullRequest, TaskPullRequestState
from openorc.observability import annotate_span, application_span
from openorc.persistence.ownership import get_repository_for_update, get_workspace
from openorc.persistence.pool import DatabasePool
from openorc.persistence.pull_requests import (
    TaskPullRequestReconcileOutcome,
    reconcile_task_pull_request_observed,
)
from openorc.persistence.tasks import get_task
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
from openorc.services.github_installation_route import (
    resolve_system_repository_installation_route,
)
from openorc.services.profile_lifecycle_guard import require_account_operational
from openorc.services.task_subject_guards import require_canonical_task_pull_request
from openorc.services.transaction_composition import composed_transaction

__all__ = [
    "TaskPullRequestReconciliation",
    "reconcile_task_pull_request",
]

_SERVICE_TRACER_SCOPE = "openorc.services.task_pull_request_reconciliation"
_RECONCILE_SPAN_NAME = "task_pull_request_reconciliation.reconcile_task_pull_request"


@dataclass(frozen=True, slots=True)
class TaskPullRequestReconciliation:
    """The durable before→current facts of one canonical PR reconciliation.

    ``pull_request`` is the post-write durable record; ``updated`` says
    whether this invocation actually advanced the observed snapshot (an
    unchanged authoritative state is a true durable no-op).
    ``head_changed``/``base_changed``/``state_changed`` describe the
    serialized before→current transition: a changed head changes the exact
    review subject; a base-only change with an unchanged head does not.
    ``previous_*`` carry the locked pre-image facts for callers that need
    them.
    """

    pull_request: TaskPullRequest
    updated: bool
    head_changed: bool
    base_changed: bool
    state_changed: bool
    previous_head_sha: str | None
    previous_base_ref: str | None
    previous_state: TaskPullRequestState | None


def _translate_github_outcome(error: Exception) -> ApplicationError:
    """Translate the adapter's classified outcomes into the typed vocabulary.

    Authorization absence is the safe classified ``AuthorizationError`` —
    the normalized integration/access condition for later recovery, never a
    trigger for a human-credential fallback. A rate limit is a known
    provider condition, deliberately not an authorization absence. Uncertain
    outcomes stay uncertain.
    """
    if isinstance(error, GitHubAuthorizationRejectedError):
        return AuthorizationError(
            "the addressed repository or pull request is not accessible through the "
            "routed github installation"
        )
    if isinstance(error, GitHubOutcomeUncertainError):
        return ExternalOperationUncertainError(
            "the outcome of the authoritative GitHub pull request reconciliation read is unknown"
        )
    assert isinstance(
        error,
        (GitHubAuthenticationRejectedError, GitHubRateLimitedError, GitHubRequestRejectedError),
    )
    return ExternalOperationFailedError(
        "the authoritative GitHub pull request reconciliation read failed as a "
        "known provider condition"
    )


def _observe_authoritative_pull_request(
    github: GitHubAppClient,
    *,
    github_installation_id: int,
    github_repository_id: int,
    pull_number: int,
) -> tuple[GitHubRepositoryObservation, GitHubPullRequestObservation]:
    """Perform the authoritative GitHub reads with no database transaction open.

    The repository observation comes first (the documented stable-ID
    installation listing proves the access condition and observes the fresh
    repository address in one walk), and the PR read is addressed through
    that fresh owner/name — never stored mutable metadata — so a rename or
    ownership transfer is reconciled instead of breaking the read. The
    caller must hold no database transaction.
    """
    repository_observation = github.get_installation_repository(
        github_installation_id=github_installation_id,
        github_repository_id=github_repository_id,
    )
    pull_request_observation = github.get_repository_pull_request(
        github_installation_id=github_installation_id,
        owner_login=repository_observation.owner_login,
        repository_name=repository_observation.name,
        pull_number=pull_number,
    )
    return repository_observation, pull_request_observation


def reconcile_task_pull_request(
    pool: DatabasePool,
    github: GitHubAppClient,
    *,
    workspace_id: UUID,
    task_id: UUID,
) -> TaskPullRequestReconciliation:
    """Reconcile the Task's one canonical pull request against GitHub.

    Phase 1 (short database reads): the Task must exist in the Workspace and
    carry its one canonical TaskPullRequest record (both uniformly
    ``NotFoundError`` otherwise — this primitive never creates or adopts a
    record); the durable route is resolved.

    Phase 2 (no database transaction open): the repository access
    observation through the exact routed installation, then the documented
    PR read addressed by the freshly observed owner/name. The observation
    binds to the addressed record's stable ``github_pr_id``; a different
    stable identity under the repository-local number is a ``ConflictError``
    — never a silent rebind, never a replacement row.

    Phase 3 (one short write transaction): the derived account-deletion
    barrier is the first lock acquisition; the Repository row is then
    re-loaded under lock and the exact route revalidated (a moved route is
    stale and applies nothing); finally the strictly update-only serialized
    observed-snapshot write applies the fresh observation and yields the
    serialized before→current facts.
    """
    if not isinstance(workspace_id, UUID):
        raise InvalidCommandError("workspace_id must be a UUID")
    if not isinstance(task_id, UUID):
        raise InvalidCommandError("task_id must be a UUID")
    with application_span(_SERVICE_TRACER_SCOPE, _RECONCILE_SPAN_NAME) as span:
        # Attach only after the caller-supplied identifiers proved valid: a
        # malformed command is classified without exporting its values.
        annotate_span(
            span,
            operation=_RECONCILE_SPAN_NAME,
            workspace_id=str(workspace_id),
            task_id=str(task_id),
        )
        task = get_task(pool, task_id)
        if task is None or task.workspace_id != workspace_id:
            raise NotFoundError("the requested task is not available in this workspace")
        pull_request = require_canonical_task_pull_request(pool, task=task)
        route = resolve_system_repository_installation_route(
            pool, workspace_id=workspace_id, repository_id=task.repository_id
        )
        annotate_span(
            span,
            operation=_RECONCILE_SPAN_NAME,
            workspace_id=str(workspace_id),
            task_id=str(task_id),
            github_pull_request_number=pull_request.github_pr_number,
        )
        try:
            _, pull_request_observation = _observe_authoritative_pull_request(
                github,
                github_installation_id=route.github_installation_id,
                github_repository_id=route.repository.identity.github_repository_id,
                pull_number=pull_request.github_pr_number,
            )
        except (
            GitHubAuthorizationRejectedError,
            GitHubAuthenticationRejectedError,
            GitHubRateLimitedError,
            GitHubRequestRejectedError,
            GitHubOutcomeUncertainError,
        ) as error:
            raise _translate_github_outcome(error) from error
        if pull_request_observation.github_pr_id != pull_request.github_pr_id:
            # Stable identity is the reconciliation key: a different stable
            # identity under the addressed repository-local number is a
            # classified conflict, never a rebind and never a new row.
            raise ConflictError(
                "the addressed pull request number reports a different stable identity"
            )
        return _apply_reconciled_state(
            pool,
            workspace_id=workspace_id,
            task_id=task_id,
            read_installation_id=route.installation_id,
            read_repository_id=route.repository.id,
            task_pull_request_id=pull_request.id,
            observation=pull_request_observation,
        )


def _apply_reconciled_state(
    pool: DatabasePool,
    *,
    workspace_id: UUID,
    task_id: UUID,
    read_installation_id: UUID,
    read_repository_id: UUID,
    task_pull_request_id: UUID,
    observation: GitHubPullRequestObservation,
) -> TaskPullRequestReconciliation:
    """Apply the serialized durable write phase inside one short transaction.

    Order matters and is load-bearing: the derived account-deletion barrier
    is the first lock acquisition; the Repository row is then re-loaded
    under lock and the exact route revalidated, so an observation read
    through installation A can never be committed after the route moved to
    B (a moved or unbound route is stale and applies nothing); and the
    strictly update-only observed-snapshot write carries the locked
    pre-image the before→current facts are computed from.
    """
    observed_state = TaskPullRequestState(observation.state)
    with composed_transaction(pool) as tx_pool:
        workspace = get_workspace(tx_pool, workspace_id)
        if workspace is None:
            raise NotFoundError("the requested workspace is not available")
        # Account-wide Owner-mutation barrier (#97), derived from durable
        # Workspace ownership rather than an authenticated caller: the FIRST
        # lock acquisition of this write phase.
        require_account_operational(tx_pool, profile_id=workspace.owner_profile_id)
        reloaded = get_repository_for_update(tx_pool, read_repository_id)
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
        reconcile_result = reconcile_task_pull_request_observed(
            tx_pool,
            task_pull_request_id=task_pull_request_id,
            workspace_id=workspace_id,
            task_id=task_id,
            head_ref=observation.head_ref,
            base_ref=observation.base_ref,
            head_sha=observation.head_sha,
            state=observed_state,
            merged_at=observation.merged_at,
        )
        if reconcile_result.outcome is TaskPullRequestReconcileOutcome.MISSING:
            # The canonical record (or its scope) disappeared durably between
            # the read and the write phase: the uniform not-found.
            raise NotFoundError("the requested pull request record is not available for this task")
    assert reconcile_result.pull_request is not None
    return TaskPullRequestReconciliation(
        pull_request=reconcile_result.pull_request,
        updated=reconcile_result.outcome is TaskPullRequestReconcileOutcome.UPDATED,
        head_changed=reconcile_result.previous_head_sha != reconcile_result.pull_request.head_sha,
        base_changed=reconcile_result.previous_base_ref != reconcile_result.pull_request.base_ref,
        state_changed=reconcile_result.previous_state != reconcile_result.pull_request.state,
        previous_head_sha=reconcile_result.previous_head_sha,
        previous_base_ref=reconcile_result.previous_base_ref,
        previous_state=reconcile_result.previous_state,
    )
