"""Race-safe canonical PR publication and immediate post-create reconciliation
(issue #64).

The external-operation primitive the workflow layer invokes after explicit
``PR_AUTHORIZATION``: it turns an already-authorized, fully scoped publication
command into exactly one canonical GitHub pull request and the Task's one
durable TaskPullRequest record — closing the race window GitHub's
branch-addressed create leaves open (no atomic "create only if head is still
SHA X") with exact preflight validation and immediate authoritative
post-create reconciliation.

Boundaries this primitive owns and does NOT own:

- It owns: command-shape validation, the replay/currentness guard, the
  exact-head preflight branch read, the single create attempt, the
  immediate authoritative PR re-read, the canonical-record persistence, the
  post-create durable currentness recheck, and the deterministic
  classification of every outcome.
- It does NOT own: requesting or validating the Producer's ``pr_result``,
  resolving the ``PR_AUTHORIZATION`` OwnerGate, any Task-state transition,
  any WorkflowEvent emission, or Reviewer dispatch — the later workflow
  layer consumes the returned classification.

The central invariant: exact authorized head before creation, exact
authoritative head after creation, the exact durable Task authority context
both before and after, and no pretending the network is atomic.

Partial-success semantics are load-bearing:

- Once GitHub creation is known-successful, the created PR is an external
  fact that is never erased: never deleted/closed as a compensating action,
  never rolled back to "did not happen". Its stable identity and
  reconciled head are persisted as soon as known, even when the authorized
  head raced or the Task's authority context went stale in flight — those
  return distinct typed stale classifications, never clean success.
- A timeout/connection loss during the mutating create request is an
  ``ExternalOperationUncertainError``: never blindly replayed, never
  reclassified.
- A durable-persistence failure after known GitHub success propagates the
  typed persistence error as the recovery-required condition — authoritative
  reconciliation/recovery recovers through the canonical record or its
  documented absence; no automatic second create exists anywhere here.
- A GitHub 'already exists' answer while OpenOrc holds no canonical record
  is an explicit non-adoption conflict: v1 never adopts an externally
  created PR as the canonical TaskPullRequest.

All GitHub I/O occurs with no database transaction open; every durable
write is a short separate transaction that reloads and revalidates current
state under locks before applying. Telemetry annotations carry only the
sanctioned safe identifier vocabulary — never the PR title/body, issue
content, provider response bodies, tokens, or credentials.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from uuid import UUID

from openorc.adapters.github import (
    GitHubAppClient,
    GitHubAuthenticationRejectedError,
    GitHubAuthorizationRejectedError,
    GitHubOutcomeUncertainError,
    GitHubPullRequestExistsError,
    GitHubPullRequestFacts,
    GitHubPullRequestObservation,
    GitHubRateLimitedError,
    GitHubRepositoryObservation,
    GitHubRequestRejectedError,
)
from openorc.domain.pull_requests import TaskPullRequest, TaskPullRequestState
from openorc.observability import (
    annotate_span,
    application_span,
)
from openorc.persistence.ownership import get_repository_for_update, get_workspace
from openorc.persistence.pool import DatabasePool
from openorc.persistence.pull_requests import (
    create_task_pull_request,
    get_task_pull_request_for_task,
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
from openorc.services.transaction_composition import composed_transaction

__all__ = [
    "PublicationCommand",
    "TaskPullRequestPublication",
    "TaskPullRequestPublicationOutcome",
    "publish_task_pull_request",
]

_SERVICE_TRACER_SCOPE = "openorc.services.pull_request_publication"
_PUBLISH_SPAN_NAME = "pull_request_publication.publish_task_pull_request"

_OBSERVED_STATES: dict[str, TaskPullRequestState] = {
    "open": TaskPullRequestState.OPEN,
    "closed": TaskPullRequestState.CLOSED,
}


class TaskPullRequestPublicationOutcome(Enum):
    """The distinct classification of one publication invocation.

    - ``PUBLISHED`` — preflight head and authority matched, the create
      returned known success, the immediate authoritative re-read reported
      the same authorized head, and the durable authority context was still
      current: the canonical record is persisted and the returned PR/head
      facts are exact. Only this outcome may be read as review-safe by the
      workflow layer.
    - ``PREFLIGHT_STALE`` — the preflight authoritative branch head differed
      from the authorized head: no PR was created by this invocation.
    - ``POST_CREATE_HEAD_MISMATCH`` — GitHub creation was known-successful
      but the immediate authoritative re-read of the created PR reports a
      head differing from the authorized head: the created PR is preserved
      and persisted with its authoritative facts; the result is stale —
      never review-safe.
    - ``POST_CREATE_AUTHORITY_STALE`` — GitHub creation was known-successful
      and the reconciled head still matches the authorized head, but the
      Task's exact authority context (state token) moved on during the
      external calls: the created PR is preserved and persisted, and the
      result is stale — never review-safe.
    """

    PUBLISHED = "published"
    PREFLIGHT_STALE = "preflight_stale"
    POST_CREATE_HEAD_MISMATCH = "post_create_head_mismatch"
    POST_CREATE_AUTHORITY_STALE = "post_create_authority_stale"


@dataclass(frozen=True, slots=True)
class TaskPullRequestPublication:
    """The outcome and durable facts of one publication invocation.

    ``pull_request`` is present only when GitHub creation was known
    successful: it is the durable canonical TaskPullRequest carrying the
    GitHub-observed identity/head — persisted even for the stale
    post-create classifications. A ``PREFLIGHT_STALE`` invocation created
    nothing, so it carries none.
    """

    outcome: TaskPullRequestPublicationOutcome
    pull_request: TaskPullRequest | None


@dataclass(frozen=True, slots=True)
class PublicationCommand:
    """One fully authorized, fully scoped publication command.

    Every field is supplied and validated by the higher workflow layer —
    this primitive never derives repository/branch/identity facts from
    Producer prose, and the title/body are validated presentation content
    only: they can never override the Repository, branch, base, Task, or
    PR identity. ``state_token`` is the exact expected current Task
    authority context the command was prepared against.
    """

    workspace_id: UUID
    task_id: UUID
    authorized_head_sha: str
    canonical_branch: str
    base_ref: str
    state_token: UUID
    title: str
    body: str | None


def _require_uuid(value: object, name: str) -> None:
    if not isinstance(value, UUID):
        raise InvalidCommandError(f"{name} must be a UUID")


def _require_nonblank(value: object, name: str) -> None:
    if not isinstance(value, str) or not value.strip():
        raise InvalidCommandError(f"{name} must be a non-empty string")


def _require_command_shape(command: PublicationCommand) -> None:
    """Validate the caller command shape before any boundary is touched."""
    if not isinstance(command, PublicationCommand):
        raise InvalidCommandError("the publication command must be a PublicationCommand")
    _require_uuid(command.workspace_id, "workspace_id")
    _require_uuid(command.task_id, "task_id")
    _require_uuid(command.state_token, "state_token")
    _require_nonblank(command.authorized_head_sha, "authorized_head_sha")
    _require_nonblank(command.canonical_branch, "canonical_branch")
    _require_nonblank(command.base_ref, "base_ref")
    _require_nonblank(command.title, "title")
    if command.body is not None and not isinstance(command.body, str):
        raise InvalidCommandError("body must be None or a string")


def _translate_github_outcome(error: Exception) -> ApplicationError:
    """Translate the adapter's classified outcomes into the typed vocabulary.

    Authorization absence is the safe classified ``AuthorizationError``; a
    rate limit is a known provider condition, deliberately not an
    authorization absence; uncertain outcomes stay uncertain.
    """
    if isinstance(error, GitHubAuthorizationRejectedError):
        return AuthorizationError(
            "the addressed repository is not accessible through the routed github installation"
        )
    if isinstance(error, GitHubOutcomeUncertainError):
        return ExternalOperationUncertainError(
            "the outcome of the GitHub pull request publication operation is unknown"
        )
    assert isinstance(
        error,
        (
            GitHubAuthenticationRejectedError,
            GitHubPullRequestExistsError,
            GitHubRateLimitedError,
            GitHubRequestRejectedError,
        ),
    )
    return ExternalOperationFailedError(
        "the GitHub pull request publication operation failed as a known provider condition"
    )


_GITHUB_READ_ERRORS = (
    GitHubAuthorizationRejectedError,
    GitHubAuthenticationRejectedError,
    GitHubOutcomeUncertainError,
    GitHubRateLimitedError,
    GitHubRequestRejectedError,
)


def _observe_repository_and_branch(
    github: GitHubAppClient,
    *,
    github_installation_id: int,
    github_repository_id: int,
    branch_name: str,
) -> tuple[GitHubRepositoryObservation, str]:
    """The authoritative preflight branch-head read (no transaction open).

    The repository access observation proves the access condition through
    the exact routed installation and observes the fresh owner/name the
    branch read is addressed by — never stored mutable metadata.
    """
    repository_observation = github.get_installation_repository(
        github_installation_id=github_installation_id,
        github_repository_id=github_repository_id,
    )
    branch_observation = github.get_repository_branch(
        github_installation_id=github_installation_id,
        owner_login=repository_observation.owner_login,
        repository_name=repository_observation.name,
        branch_name=branch_name,
    )
    return repository_observation, branch_observation.head_sha


def _reobserve_created_pull_request(
    github: GitHubAppClient,
    *,
    github_installation_id: int,
    github_repository_id: int,
    pull_number: int,
) -> GitHubPullRequestObservation:
    """The authoritative post-create PR re-read (no transaction open)."""
    repository_observation = github.get_installation_repository(
        github_installation_id=github_installation_id,
        github_repository_id=github_repository_id,
    )
    return github.get_repository_pull_request(
        github_installation_id=github_installation_id,
        owner_login=repository_observation.owner_login,
        repository_name=repository_observation.name,
        pull_number=pull_number,
    )


@dataclass(frozen=True, slots=True)
class _WritePhaseResult:
    """The durable facts of one post-create write phase."""

    pull_request: TaskPullRequest
    authority_stale: bool


def _persist_created_pull_request(
    pool: DatabasePool,
    *,
    workspace_id: UUID,
    task_id: UUID,
    repository_id: UUID,
    read_installation_id: UUID,
    expected_state_token: UUID,
    observation: GitHubPullRequestObservation,
) -> _WritePhaseResult:
    """Persist the created canonical PR with its authoritative facts.

    One short database-only write transaction (the GitHub creation already
    happened and is never undone). Order is load-bearing:

    1. the derived account-deletion barrier is the first lock acquisition;
    2. the Task row is reloaded under lock and the exact ``state_token``
       revalidated — a moved token means the authority context went stale
       in flight, and the created PR is STILL persisted with its
       authoritative reconciled facts (the external fact is never erased)
       while the caller receives the stale classification;
    3. the Repository route is revalidated under lock exactly as the #63
       reconciliation composes it (a moved route is likewise stale in
       flight, with the same preservation rule);
    4. the canonical record is inserted with the reconciled head/base/state
       GitHub reported — or, when a concurrent/recovery write already
       persisted the record, the observed snapshot is reconciled into it,
       update-only: never a second create and never a rewrite.

    A persistence failure here (driver error, constraint violation) raises
    through: the recovery-required condition, never an automatic second
    create and never an erasure of the known external creation.
    """
    observed_state = _OBSERVED_STATES[observation.state]
    with composed_transaction(pool) as tx_pool:
        workspace = get_workspace(tx_pool, workspace_id)
        if workspace is None:
            raise NotFoundError("the requested workspace is not available")
        # Account-wide Owner-mutation barrier (#97), the FIRST lock
        # acquisition of this write phase.
        require_account_operational(tx_pool, profile_id=workspace.owner_profile_id)
        reloaded_task = get_task(tx_pool, task_id)
        if reloaded_task is None or reloaded_task.workspace_id != workspace_id:
            raise NotFoundError("the requested task is not available in this workspace")
        authority_stale = (
            reloaded_task.archived_at is not None
            or reloaded_task.state_token != expected_state_token
        )
        reloaded = get_repository_for_update(tx_pool, repository_id)
        if reloaded is None or reloaded.workspace_id != workspace_id:
            raise NotFoundError("the requested repository is not available in this workspace")
        if reloaded.github_installation_id != read_installation_id:
            authority_stale = True
        existing = get_task_pull_request_for_task(tx_pool, task_id=task_id)
        if existing is not None:
            # A concurrent publication (or a recovery replay after a
            # persistence failure) already persisted the canonical record:
            # never a second create and never a rewrite — the observed
            # snapshot is reconciled in place, update-only.
            reconcile_task_pull_request_observed(
                tx_pool,
                task_pull_request_id=existing.id,
                workspace_id=workspace_id,
                task_id=task_id,
                head_ref=observation.head_ref,
                base_ref=observation.base_ref,
                head_sha=observation.head_sha,
                state=observed_state,
                merged_at=observation.merged_at,
            )
            return _WritePhaseResult(pull_request=existing, authority_stale=authority_stale)
        created = create_task_pull_request(
            tx_pool,
            workspace_id=workspace_id,
            task_id=task_id,
            repository_id=repository_id,
            github_pr_id=observation.github_pr_id,
            github_pr_number=observation.pull_number,
            head_ref=observation.head_ref,
            base_ref=observation.base_ref,
            head_sha=observation.head_sha,
        )
    return _WritePhaseResult(pull_request=created, authority_stale=authority_stale)


def publish_task_pull_request(
    pool: DatabasePool,
    github: GitHubAppClient,
    command: PublicationCommand,
) -> TaskPullRequestPublication:
    """Publish the Task's canonical pull request, race-safe.

    Phase 1 (short database read): command-shape validation, then the
    durable Task/authority/route facts — the Task must be current with
    exactly the command's ``state_token`` and carry exactly the command's
    canonical branch. A canonical record that already exists is the replay
    conflict — never a second create.

    Phase 2 (no database transaction open): the preflight authoritative
    branch head through the exact routed installation. A head differing
    from the authorized SHA is ``PREFLIGHT_STALE`` — no create call was
    made and no PR exists because of this invocation.

    Phase 3 (external, no transaction): exactly one branch-addressed
    create with the validated presentation title/body. GitHub's
    'already exists' rejection is the explicit non-adoption conflict; an
    uncertain create raises ``ExternalOperationUncertainError`` with zero
    automatic resend.

    Phase 4 (no transaction): the immediate authoritative re-read of the
    created PR, bound to the created stable identity; its authoritative
    head is compared to the SAME authorized SHA.

    Phase 5 (one short write transaction): the durable write — the
    canonical record with the GitHub-observed facts, the exact authority
    currentness recheck inside it deciding clean success versus
    ``POST_CREATE_AUTHORITY_STALE``.
    """
    _require_command_shape(command)
    with application_span(_SERVICE_TRACER_SCOPE, _PUBLISH_SPAN_NAME) as span:
        annotate_span(
            span,
            operation=_PUBLISH_SPAN_NAME,
            workspace_id=str(command.workspace_id),
            task_id=str(command.task_id),
        )
        # --- Phase 1: durable preflight state (short read; authoritative
        # locks are re-taken inside the later write transaction) ---
        task = get_task(pool, command.task_id)
        if task is None or task.workspace_id != command.workspace_id:
            raise NotFoundError("the requested task is not available in this workspace")
        if task.archived_at is not None or task.state_token != command.state_token:
            raise StaleOperationError(
                "the task state has moved on since this publication was authorized"
            )
        canonical_branch = task.canonical_feature_branch
        if canonical_branch is None or canonical_branch != command.canonical_branch:
            raise ConflictError(
                "the publication command does not address the task's bound canonical branch"
            )
        route = resolve_system_repository_installation_route(
            pool, workspace_id=command.workspace_id, repository_id=task.repository_id
        )
        existing = get_task_pull_request_for_task(pool, task_id=command.task_id)
        if existing is not None:
            # Replay of an already-satisfied publication: one Task has one
            # canonical PR for its whole v1 lifetime — never a second PR.
            raise ConflictError(
                "the task already has a canonical pull request; publication is already satisfied"
            )
        # --- Phase 2: exact-head preflight (external, no transaction) ---
        try:
            preflight_repository, preflight_head_sha = _observe_repository_and_branch(
                github,
                github_installation_id=route.github_installation_id,
                github_repository_id=route.repository.identity.github_repository_id,
                branch_name=canonical_branch,
            )
        except _GITHUB_READ_ERRORS as error:
            raise _translate_github_outcome(error) from error
        if preflight_head_sha != command.authorized_head_sha:
            annotate_span(
                span,
                operation=_PUBLISH_SPAN_NAME,
                workspace_id=str(command.workspace_id),
                task_id=str(command.task_id),
                github_head_sha=preflight_head_sha,
            )
            return TaskPullRequestPublication(
                outcome=TaskPullRequestPublicationOutcome.PREFLIGHT_STALE,
                pull_request=None,
            )
        annotate_span(
            span,
            operation=_PUBLISH_SPAN_NAME,
            workspace_id=str(command.workspace_id),
            task_id=str(command.task_id),
            github_installation_id=str(route.github_installation_id),
        )

        # --- Phase 3: exactly one create attempt (external, no transaction)
        # ---
        try:
            created_facts: GitHubPullRequestFacts = github.create_pull_request(
                github_installation_id=route.github_installation_id,
                owner_login=preflight_repository.owner_login,
                repository_name=preflight_repository.name,
                head_ref=canonical_branch,
                base_ref=command.base_ref,
                title=command.title,
                body=command.body,
            )
        except GitHubPullRequestExistsError as error:
            # GitHub reports an open PR already exists for the head/base
            # while OpenOrc holds no canonical record: v1 never adopts an
            # externally created PR — an explicit conflict, never a silent
            # adoption and never a retry.
            raise ConflictError(
                "a pull request already exists for the canonical branch and base; "
                "external pull requests are never adopted"
            ) from error
        except _GITHUB_READ_ERRORS as error:
            raise _translate_github_outcome(error) from error
        # --- Phase 4: immediate authoritative reconciliation of the created
        # PR (external, no transaction; compared to the same authorized
        # head) ---
        try:
            observation = _reobserve_created_pull_request(
                github,
                github_installation_id=route.github_installation_id,
                github_repository_id=route.repository.identity.github_repository_id,
                pull_number=created_facts.pull_number,
            )
        except _GITHUB_READ_ERRORS as error:
            raise _translate_github_outcome(error) from error
        if observation.github_pr_id != created_facts.github_pr_id:
            # The re-read does not bind to the created PR's stable identity:
            # an uninterpretable reconciliation, never an accepted fact.
            raise ExternalOperationUncertainError(
                "the authoritative re-read of the created pull request does not bind "
                "to the created identity"
            )
        # --- Phase 5: durable persistence + currentness recheck (one short
        # write transaction) ---
        write_result = _persist_created_pull_request(
            pool,
            workspace_id=command.workspace_id,
            task_id=command.task_id,
            repository_id=task.repository_id,
            read_installation_id=route.installation_id,
            expected_state_token=command.state_token,
            observation=observation,
        )
        if write_result.authority_stale:
            # The authority context moved on during the external calls —
            # the created PR is preserved and persisted, but the result is
            # distinctly stale and can never be mistaken for review-safe
            # success. The workflow layer owns the recovery decision.
            return TaskPullRequestPublication(
                outcome=TaskPullRequestPublicationOutcome.POST_CREATE_AUTHORITY_STALE,
                pull_request=write_result.pull_request,
            )
        if observation.head_sha != command.authorized_head_sha:
            # The authorized head raced after preflight but before the
            # create took effect: the created PR is preserved and persisted
            # with its authoritative facts; the result is distinctly stale
            # and never review-safe.
            return TaskPullRequestPublication(
                outcome=TaskPullRequestPublicationOutcome.POST_CREATE_HEAD_MISMATCH,
                pull_request=write_result.pull_request,
            )
        return TaskPullRequestPublication(
            outcome=TaskPullRequestPublicationOutcome.PUBLISHED,
            pull_request=write_result.pull_request,
        )
