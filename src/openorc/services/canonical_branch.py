"""Canonical Task feature-branch verification and binding (issue #63).

The focused authoritative operation that turns a Producer-reported
feature-branch **claim** into canonical Task state:

- Runtime-local/worktree HEAD never establishes canonical committed state.
  The exact committed head SHA comes exclusively from the authoritative
  GitHub branch read for the Task's exact Repository, addressed through the
  freshly observed repository address (never stored mutable metadata).
- Branch names are routing/address facts; the exact commit SHA identifies
  code state. The claim establishes the branch only on first binding;
  binding goes through the existing Phase 2A exact-currentness/state-token
  mutation boundary
  (:func:`openorc.services.task_mutations.bind_canonical_branch`), which
  enforces bind-once, the current state token, and the repository-wide
  branch-ownership exclusivity.
- Once bound, later remediation continues on that canonical branch rather
  than replacing it: a claim naming a different branch is a classified
  conflict — the off-branch Producer is surfaced, never silently absorbed —
  and re-verification re-reads the canonical branch's current committed
  head from GitHub.
- A missing/deleted branch or an access failure is the normalized GitHub
  integration condition (typed ``AuthorizationError``), never permission to
  trust runtime-local state.
- GitHub reads occur with no database transaction open: the currentness
  guard completes its short read first, and the one-time binding (a
  database-only mutation) revalidates the exact currentness itself through
  the Phase 2A primitive.
- No Task-state transition is decided here: this leaf verifies and binds;
  the later Task workflow decides when the primitives may be invoked and
  what the verified head authorizes.
"""

from __future__ import annotations

from dataclasses import dataclass
from uuid import UUID

from openorc.adapters.github import (
    GitHubAppClient,
    GitHubAuthenticationRejectedError,
    GitHubAuthorizationRejectedError,
    GitHubOutcomeUncertainError,
    GitHubRateLimitedError,
    GitHubRepositoryObservation,
    GitHubRequestRejectedError,
)
from openorc.domain.tasks import Task
from openorc.observability import annotate_span, application_span
from openorc.persistence.pool import DatabasePool
from openorc.services.errors import (
    ApplicationError,
    AuthorizationError,
    ConflictError,
    ExternalOperationFailedError,
    ExternalOperationUncertainError,
    InvalidCommandError,
)
from openorc.services.github_installation_route import (
    resolve_system_repository_installation_route,
)
from openorc.services.task_mutations import bind_canonical_branch
from openorc.services.task_subject_guards import require_current_task

__all__ = [
    "CanonicalBranchVerification",
    "verify_canonical_task_branch",
]

_SERVICE_TRACER_SCOPE = "openorc.services.canonical_branch"
_VERIFY_SPAN_NAME = "canonical_branch.verify_canonical_task_branch"


@dataclass(frozen=True, slots=True)
class CanonicalBranchVerification:
    """The verified canonical branch facts of one Task.

    ``task`` is the durable post-invocation Task (carrying the newly rotated
    ``state_token`` when this invocation bound the branch; the unchanged
    current Task otherwise); ``branch`` is the canonical branch identity;
    ``verified_head_sha`` is the exact GitHub-committed head SHA — the code
    state later exact-head workflow authority binds to; ``bound_now`` says
    whether this invocation performed the one-time binding.
    """

    task: Task
    branch: str
    verified_head_sha: str
    bound_now: bool


def _translate_github_outcome(error: Exception) -> ApplicationError:
    """Translate the adapter's classified outcomes into the typed vocabulary.

    Authorization absence — lost repository access or the addressed branch's
    disappearance — is the normalized missing-branch/access integration
    condition (``AuthorizationError``) for later recovery; it is never
    permission to trust runtime-local Git state and never a trigger for a
    human-credential fallback. A rate limit is a known provider condition,
    deliberately not an authorization absence. Uncertain outcomes stay
    uncertain.
    """
    if isinstance(error, GitHubAuthorizationRejectedError):
        return AuthorizationError(
            "the addressed repository or branch is not accessible through the "
            "routed github installation"
        )
    if isinstance(error, GitHubOutcomeUncertainError):
        return ExternalOperationUncertainError(
            "the outcome of the authoritative GitHub branch verification is unknown"
        )
    assert isinstance(
        error,
        (GitHubAuthenticationRejectedError, GitHubRateLimitedError, GitHubRequestRejectedError),
    )
    return ExternalOperationFailedError(
        "the authoritative GitHub branch verification failed as a known provider condition"
    )


def verify_canonical_task_branch(
    pool: DatabasePool,
    github: GitHubAppClient,
    *,
    workspace_id: UUID,
    task_id: UUID,
    expected_state_token: UUID,
    claimed_branch: str,
) -> CanonicalBranchVerification:
    """Verify a Producer branch claim through GitHub and bind it exactly once.

    Phase 1 (short database read): the exact currentness guard — the Task
    must exist in the Workspace, be current, and carry the caller's expected
    ``state_token``. A Task whose canonical branch is already bound continues
    on that branch: a claim naming a different branch is a ``ConflictError``,
    and the canonical branch's current committed head is re-verified.

    Phase 2 (no database transaction open): the repository access
    observation through the exact routed installation, then the documented
    branch read addressed by the freshly observed owner/name. The verified
    head SHA comes from GitHub only.

    Phase 3 (one-time binding): an unbound Task binds the GitHub-verified
    branch through the Phase 2A exact-currentness primitive, which
    revalidates the caller's expected token under the write and returns the
    post-write Task with its rotated token. A claim that raced past a
    concurrent state change is a stale operation there — never applied,
    never retried blindly.
    """
    if not isinstance(workspace_id, UUID):
        raise InvalidCommandError("workspace_id must be a UUID")
    if not isinstance(task_id, UUID):
        raise InvalidCommandError("task_id must be a UUID")
    if not isinstance(expected_state_token, UUID):
        raise InvalidCommandError("expected_state_token must be a UUID")
    if not isinstance(claimed_branch, str) or not claimed_branch.strip():
        raise InvalidCommandError("claimed_branch must be a non-empty string")
    with application_span(_SERVICE_TRACER_SCOPE, _VERIFY_SPAN_NAME) as span:
        # Attach only after the caller-supplied identifiers proved valid: a
        # malformed command is classified without exporting its values.
        annotate_span(
            span,
            operation=_VERIFY_SPAN_NAME,
            workspace_id=str(workspace_id),
            task_id=str(task_id),
        )
        task = require_current_task(
            pool,
            workspace_id=workspace_id,
            task_id=task_id,
            expected_state_token=expected_state_token,
        )
        bound_branch = task.canonical_feature_branch
        if bound_branch is not None and bound_branch != claimed_branch:
            # Canonical branch ownership is a one-time fact; later remediation
            # continues on the canonical branch rather than replacing it.
            raise ConflictError(
                "the task's canonical feature branch is already bound; remediation "
                "continues on the canonical branch"
            )
        subject_branch = bound_branch if bound_branch is not None else claimed_branch
        route = resolve_system_repository_installation_route(
            pool, workspace_id=workspace_id, repository_id=task.repository_id
        )
        try:
            repository_observation: GitHubRepositoryObservation = (
                github.get_installation_repository(
                    github_installation_id=route.github_installation_id,
                    github_repository_id=route.repository.identity.github_repository_id,
                )
            )
            branch_observation = github.get_repository_branch(
                github_installation_id=route.github_installation_id,
                owner_login=repository_observation.owner_login,
                repository_name=repository_observation.name,
                branch_name=subject_branch,
            )
        except (
            GitHubAuthorizationRejectedError,
            GitHubAuthenticationRejectedError,
            GitHubRateLimitedError,
            GitHubRequestRejectedError,
            GitHubOutcomeUncertainError,
        ) as error:
            raise _translate_github_outcome(error) from error
        if bound_branch is None:
            verified_task = bind_canonical_branch(
                pool,
                workspace_id=workspace_id,
                task_id=task_id,
                expected_state_token=expected_state_token,
                canonical_feature_branch=branch_observation.branch_name,
            )
            bound_now = True
        else:
            verified_task = task
            bound_now = False
        return CanonicalBranchVerification(
            task=verified_task,
            branch=branch_observation.branch_name,
            verified_head_sha=branch_observation.head_sha,
            bound_now=bound_now,
        )
