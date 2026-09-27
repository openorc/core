"""GitHub checks/commit-status projection for the canonical PR's exact head
(issue #63).

The read-only projection service that surfaces GitHub's CI/status surfaces
for the Task's canonical pull request: both the check-run listing and the
combined commit status are read for the **exact relevant PR head SHA** —
the durable reconciled ``TaskPullRequest.head_sha``, never a caller-supplied
branch name or head — through the adapter's exhaustively paginated,
completeness-proven read operations.

The projection presents GitHub-owned facts and nothing more: it never
becomes an independent OpenOrc merge-policy engine and never synthesizes a
universal pass/fail gate that could disagree with GitHub branch protection.
It is not persisted in this leaf (no demonstrated cache/reconciliation need
yet) and decides no workflow consequence — later workflow services and
webhook dispatch consume the typed projection.

A Task without its one canonical TaskPullRequest record has nothing to
project (uniform ``NotFoundError`` — PR creation belongs to the later
race-safe publication capability). Access loss and known provider failures
classify exactly as the reconciliation boundary does; uncertain outcomes
stay uncertain.
"""

from __future__ import annotations

from dataclasses import dataclass
from uuid import UUID

from openorc.adapters.github import (
    GitHubAppClient,
    GitHubAuthenticationRejectedError,
    GitHubAuthorizationRejectedError,
    GitHubCheckRunObservation,
    GitHubCommitStatusesProjection,
    GitHubOutcomeUncertainError,
    GitHubRateLimitedError,
    GitHubRepositoryObservation,
    GitHubRequestRejectedError,
    GitHubStatusContextObservation,
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
)
from openorc.services.github_installation_route import (
    resolve_system_repository_installation_route,
)
from openorc.services.task_subject_guards import require_canonical_task_pull_request

__all__ = [
    "CommitChecksProjection",
    "project_commit_checks",
]

_SERVICE_TRACER_SCOPE = "openorc.services.commit_checks_projection"
_PROJECT_SPAN_NAME = "commit_checks_projection.project_commit_checks"


@dataclass(frozen=True, slots=True)
class CommitChecksProjection:
    """The Task's GitHub-owned CI/status facts for one exact head.

    ``head_sha`` is the exact durable reconciled head the projection was
    read for; ``check_runs`` and the combined-status surface
    (``combined_status_state`` plus the exhaustively collected
    ``status_contexts``) are GitHub-owned provider facts, presented only —
    never an OpenOrc merge-policy engine.
    """

    pull_request: TaskPullRequest
    head_sha: str
    check_runs: tuple[GitHubCheckRunObservation, ...]
    combined_status_state: str
    status_contexts: tuple[GitHubStatusContextObservation, ...]


def _translate_github_outcome(error: Exception) -> ApplicationError:
    """Translate the adapter's classified outcomes into the typed vocabulary.

    Authorization absence is the safe classified ``AuthorizationError`` —
    the normalized integration/access condition for later recovery. A rate
    limit is a known provider condition, deliberately not an authorization
    absence. Uncertain outcomes stay uncertain.
    """
    if isinstance(error, GitHubAuthorizationRejectedError):
        return AuthorizationError(
            "the addressed repository or commit checks are not accessible through "
            "the routed github installation"
        )
    if isinstance(error, GitHubOutcomeUncertainError):
        return ExternalOperationUncertainError(
            "the outcome of the GitHub checks/status projection read is unknown"
        )
    assert isinstance(
        error,
        (GitHubAuthenticationRejectedError, GitHubRateLimitedError, GitHubRequestRejectedError),
    )
    return ExternalOperationFailedError(
        "the GitHub checks/status projection read failed as a known provider condition"
    )


def project_commit_checks(
    pool: DatabasePool,
    github: GitHubAppClient,
    *,
    workspace_id: UUID,
    task_id: UUID,
) -> CommitChecksProjection:
    """Project GitHub's checks/commit-status surfaces for the exact PR head.

    Phase 1 (short database reads): the Task must exist in the Workspace and
    carry its one canonical TaskPullRequest record (uniform
    ``NotFoundError`` otherwise); the durable route is resolved. The
    projection head is the record's current reconciled ``head_sha``.

    Phase 2 (no database transaction open): the repository access
    observation through the exact routed installation, then the two
    documented reads addressed by that exact head — the exhaustively
    paginated check-run listing and the exhaustively paginated combined
    status. No database transaction is open at any point during the reads,
    and nothing durable is written: the projection is a typed read model.
    """
    if not isinstance(workspace_id, UUID):
        raise InvalidCommandError("workspace_id must be a UUID")
    if not isinstance(task_id, UUID):
        raise InvalidCommandError("task_id must be a UUID")
    with application_span(_SERVICE_TRACER_SCOPE, _PROJECT_SPAN_NAME) as span:
        # Attach only after the caller-supplied identifiers proved valid: a
        # malformed command is classified without exporting its values.
        annotate_span(
            span,
            operation=_PROJECT_SPAN_NAME,
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
        head_sha = pull_request.head_sha
        annotate_span(
            span,
            operation=_PROJECT_SPAN_NAME,
            workspace_id=str(workspace_id),
            task_id=str(task_id),
            github_pull_request_number=pull_request.github_pr_number,
            github_head_sha=head_sha,
        )
        try:
            repository_observation: GitHubRepositoryObservation = (
                github.get_installation_repository(
                    github_installation_id=route.github_installation_id,
                    github_repository_id=route.repository.identity.github_repository_id,
                )
            )
            check_runs = github.get_commit_check_runs(
                github_installation_id=route.github_installation_id,
                owner_login=repository_observation.owner_login,
                repository_name=repository_observation.name,
                head_sha=head_sha,
            )
            combined_status: GitHubCommitStatusesProjection = github.get_commit_combined_status(
                github_installation_id=route.github_installation_id,
                owner_login=repository_observation.owner_login,
                repository_name=repository_observation.name,
                head_sha=head_sha,
            )
        except (
            GitHubAuthorizationRejectedError,
            GitHubAuthenticationRejectedError,
            GitHubRateLimitedError,
            GitHubRequestRejectedError,
            GitHubOutcomeUncertainError,
        ) as error:
            raise _translate_github_outcome(error) from error
        return CommitChecksProjection(
            pull_request=pull_request,
            head_sha=head_sha,
            check_runs=tuple(check_runs),
            combined_status_state=combined_status.state,
            status_contexts=combined_status.statuses,
        )
