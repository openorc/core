"""Exact-head merge request for the canonical TaskPullRequest (issue #63).

The trusted adapter/service primitive that requests the merge of the Task's
one canonical pull request using the exact reviewed or Owner-overridden
expected head SHA the later workflow service supplies:

- Before external mutation, one short transaction resolves the exact
  Workspace/Repository/TaskPullRequest scope and binds the merge's authority
  to the current reconciled PR head through the existing Phase 2A guard
  (``require_task_pull_request_head``): the caller's expected SHA must equal
  the durable reconciled head exactly. The transaction closes before any
  GitHub call — no database transaction ever spans a GitHub call.
- The GitHub request carries the documented ``sha`` expected-head parameter
  (the provider's own expected-head facility, merge authority deriving from
  ``contents: write`` validated at route/access time). A concurrent head
  change can never satisfy a stale merge request: GitHub answers the
  documented head-mismatch rejection, classified here as a stale operation.
- Known outcomes are distinct: known success (immediately confirmed by
  authoritative PR reconciliation supplying the durable merged facts),
  expected-head mismatch (``StaleOperationError``), and other known
  GitHub-owned policy/state rejection (``ExternalOperationFailedError`` —
  required checks, conflicts, branch protection; OpenOrc owns none of that
  policy). Authentication/access failure is the normalized integration
  condition; timeout/connection loss/ambiguous response is uncertain and is
  **never automatically replayed** — reconciliation and recovery for an
  uncertain mutating call belong to the later recovery layer.
- The later workflow service decides the Task ``COMPLETED`` transition and
  the Owner merge-decision policy; nothing of either lives here.
"""

from __future__ import annotations

from dataclasses import dataclass
from uuid import UUID

from openorc.adapters.github import (
    GitHubAppClient,
    GitHubAuthenticationRejectedError,
    GitHubAuthorizationRejectedError,
    GitHubMergeRequestOutcome,
    GitHubMergeRequestResult,
    GitHubOutcomeUncertainError,
    GitHubRateLimitedError,
    GitHubRepositoryObservation,
    GitHubRequestRejectedError,
)
from openorc.domain.pull_requests import TaskPullRequest
from openorc.observability import annotate_span, application_span
from openorc.persistence.pool import DatabasePool
from openorc.persistence.tasks import get_task
from openorc.services.errors import (
    ApplicationError,
    AuthorizationError,
    ExternalOperationFailedError,
    ExternalOperationUncertainError,
    InvalidCommandError,
    NotFoundError,
    StaleOperationError,
)
from openorc.services.github_installation_route import (
    resolve_system_repository_installation_route,
)
from openorc.services.task_pull_request_reconciliation import (
    TaskPullRequestReconciliation,
    reconcile_task_pull_request,
)
from openorc.services.task_subject_guards import require_task_pull_request_head

__all__ = [
    "PullRequestMergeResult",
    "request_pull_request_merge",
]

_SERVICE_TRACER_SCOPE = "openorc.services.pull_request_merge"
_MERGE_SPAN_NAME = "pull_request_merge.request_pull_request_merge"


@dataclass(frozen=True, slots=True)
class PullRequestMergeResult:
    """The authoritative merged facts after a known-successful merge request.

    ``merged_head_sha`` is the exact expected head the merge was bound to
    (the reviewed or Owner-overridden subject); ``merge_commit_sha`` is the
    merge commit SHA GitHub's documented successful answer reported;
    ``reconciliation`` carries the durable post-merge PR record (closed,
    carrying its merge instant) plus the serialized before→current facts.
    The Task ``COMPLETED`` transition belongs to the later workflow service.
    """

    pull_request: TaskPullRequest
    merged_head_sha: str
    merge_commit_sha: str
    reconciliation: TaskPullRequestReconciliation


def _translate_github_outcome(error: Exception) -> ApplicationError:
    """Translate the adapter's classified outcomes into the typed vocabulary.

    Authorization absence is the safe classified ``AuthorizationError`` —
    the normalized integration/access condition for later recovery, never a
    trigger for a human-credential fallback. A rate limit is a known
    provider condition, deliberately not an authorization absence. Uncertain
    outcomes stay uncertain and are never replayed here.
    """
    if isinstance(error, GitHubAuthorizationRejectedError):
        return AuthorizationError(
            "the addressed repository or pull request is not accessible through the "
            "routed github installation"
        )
    if isinstance(error, GitHubOutcomeUncertainError):
        return ExternalOperationUncertainError(
            "the outcome of the exact-head GitHub merge request is unknown"
        )
    assert isinstance(
        error,
        (GitHubAuthenticationRejectedError, GitHubRateLimitedError, GitHubRequestRejectedError),
    )
    return ExternalOperationFailedError(
        "the exact-head GitHub merge request failed as a known provider condition"
    )


def request_pull_request_merge(
    pool: DatabasePool,
    github: GitHubAppClient,
    *,
    workspace_id: UUID,
    task_id: UUID,
    expected_head_sha: str,
) -> PullRequestMergeResult:
    """Request the canonical PR's merge bound to the exact expected head.

    Phase 1 (one short transaction): the Task must exist in the Workspace;
    the canonical TaskPullRequest is resolved and the merge's authority is
    bound to its exact current reconciled head through the Phase 2A guard —
    a caller whose expected SHA does not match is stale before anything
    external happens.

    Phase 2 (no database transaction open): the repository access
    observation through the exact routed installation, then the documented
    exact-head merge request.

    Phase 3 (known-outcome classification): the documented head-mismatch
    answer is a stale operation; any other definitive rejection is a known
    GitHub-owned policy/state failure; on the known success the same
    authoritative reconciliation primitive immediately persists and returns
    the durable merged facts — the merge request itself never mutates Task
    workflow state.
    """
    if not isinstance(workspace_id, UUID):
        raise InvalidCommandError("workspace_id must be a UUID")
    if not isinstance(task_id, UUID):
        raise InvalidCommandError("task_id must be a UUID")
    if not isinstance(expected_head_sha, str) or not expected_head_sha.strip():
        raise InvalidCommandError("expected_head_sha must be a non-empty string")
    with application_span(_SERVICE_TRACER_SCOPE, _MERGE_SPAN_NAME) as span:
        # Attach only after the caller-supplied identifiers proved valid: a
        # malformed command is classified without exporting its values.
        annotate_span(
            span,
            operation=_MERGE_SPAN_NAME,
            workspace_id=str(workspace_id),
            task_id=str(task_id),
            github_head_sha=expected_head_sha,
        )
        task = get_task(pool, task_id)
        if task is None or task.workspace_id != workspace_id:
            raise NotFoundError("the requested task is not available in this workspace")
        pull_request = require_task_pull_request_head(
            pool, task=task, expected_head_sha=expected_head_sha
        )
        route = resolve_system_repository_installation_route(
            pool, workspace_id=workspace_id, repository_id=task.repository_id
        )
        annotate_span(
            span,
            operation=_MERGE_SPAN_NAME,
            workspace_id=str(workspace_id),
            task_id=str(task_id),
            github_head_sha=expected_head_sha,
            github_pull_request_number=pull_request.github_pr_number,
        )
        try:
            repository_observation: GitHubRepositoryObservation = (
                github.get_installation_repository(
                    github_installation_id=route.github_installation_id,
                    github_repository_id=route.repository.identity.github_repository_id,
                )
            )
            merge_result: GitHubMergeRequestResult = github.merge_pull_request(
                github_installation_id=route.github_installation_id,
                owner_login=repository_observation.owner_login,
                repository_name=repository_observation.name,
                pull_number=pull_request.github_pr_number,
                expected_head_sha=expected_head_sha,
            )
        except (
            GitHubAuthorizationRejectedError,
            GitHubAuthenticationRejectedError,
            GitHubRateLimitedError,
            GitHubRequestRejectedError,
            GitHubOutcomeUncertainError,
        ) as error:
            raise _translate_github_outcome(error) from error
        if merge_result.outcome is GitHubMergeRequestOutcome.HEAD_MISMATCH:
            # GitHub's documented expected-head guard refused: the head moved
            # concurrently and this merge request is stale. The operation is
            # never applied and never blindly retried.
            raise StaleOperationError(
                "the pull request head changed before the merge applied; the merge request is stale"
            )
        if merge_result.outcome is GitHubMergeRequestOutcome.REJECTED:
            # A known GitHub-owned policy/state rejection (required checks,
            # conflicts, branch protection): OpenOrc owns none of that
            # policy and only reports the known failure.
            raise ExternalOperationFailedError(
                "GitHub rejected the merge request: the pull request is not mergeable "
                "under GitHub's current merge policy or state"
            )
        assert (
            merge_result.outcome is GitHubMergeRequestOutcome.MERGED
            and merge_result.merge_commit_sha is not None
        )
        # Known success: authoritative reconciliation supplies the durable
        # merged facts. No Task-state transition is decided here.
        reconciliation = reconcile_task_pull_request(
            pool, github, workspace_id=workspace_id, task_id=task_id
        )
        return PullRequestMergeResult(
            pull_request=reconciliation.pull_request,
            merged_head_sha=expected_head_sha,
            merge_commit_sha=merge_result.merge_commit_sha,
            reconciliation=reconciliation,
        )
