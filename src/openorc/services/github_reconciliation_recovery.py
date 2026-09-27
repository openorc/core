"""Repeatable GitHub reconciliation and missed-delivery recovery services
(issue #62).

The independent, repeatable recovery sweep that composes the authoritative
reconciliation capabilities (B3/B4/B7, issues #59/#60/#63) and the webhook
dispatch boundary (#120) so that missed, delayed, duplicated, or
out-of-order GitHub webhook delivery can never permanently strand OpenOrc's
mirrored GitHub state. The sweep is a correctness backstop, not a second
webhook path: every unit re-reads current authoritative GitHub state and
compares it to current durable OpenOrc state, so replayed, duplicated, and
out-of-order recovery work converges on the newest authority.

One operation — :func:`run_github_reconciliation_sweep` — serves periodic
scheduled reconciliation, operator/manual recovery, retry after a known-safe
reconciliation failure, and recovery of a recorded webhook delivery whose
downstream dispatch did not complete. The sweep:

- derives a bounded, current-work work set from durable Postgres only:
  repositories explicitly routed to an installation, open tracked issue
  projections unioned with the issues of current (non-archived) Tasks, the
  canonical branches/PR records of current Tasks, and delivery records whose
  processing is still unresolved. Historical/closed subjects with no current
  Task never enter the work set, and nothing is ever crawled on GitHub;
- bounds each invocation with explicit deterministic batching: every family
  partitions its eligible units into ``partitions`` buckets by stable durable
  identity, and one ``sweep_tick`` services one bucket per family. Bucket
  membership depends only on stable identities, so every eligible unit is
  serviced exactly once per ``partitions``-tick rotation — starvation-free
  eventual coverage with no persisted cursor, no queue-held correctness
  state, and no second scheduler state machine. ``sweep_tick=None`` is the
  operator's full sweep of the whole eligible set;
- performs no GitHub I/O itself: every unit composes an existing focused
  capability that owns its own short transactions around its external reads,
  so no database transaction ever spans a GitHub call;
- isolates unit failures: a known failure, an uncertain outcome, or an
  unexpected error in one repository/issue/Task unit never rolls back or
  blocks the independent units, and the sweep is safe to re-run after any
  partial completion;
- reports normalized durable fact changes to the later workflow layer and
  decides none of the Phase 2E consequences (``BLOCKED /
  GITHUB_SOURCE_CHANGED``, ``PR_CLOSED_UNMERGED``, ``COMPLETED``, Reviewer
  dispatch, merge authorization). Hierarchy changes are recovered for
  presentation only and never become blocking semantics; requirements drift
  is detected against the Task's immutable B4 source baseline without
  rewriting it; access loss is a normalized integration fact without any
  credential fallback.

Delivery recovery re-enters the untouched #120 dispatch boundary per
unresolved delivery — re-running routed reconciliation and marking only the
bounded ``processed_at`` recovery instant when every routed reconciliation
completed or typedly declined. Provider delivery identity is never
rewritten, and inbox status is never GitHub truth.
"""

from __future__ import annotations

import dataclasses
import logging
from dataclasses import dataclass
from enum import StrEnum
from uuid import UUID

from openorc.adapters.github import GitHubAppClient
from openorc.config import Settings
from openorc.domain.pull_requests import TaskPullRequestState
from openorc.domain.tasks import Task
from openorc.observability import annotate_span, application_span
from openorc.persistence.github_issues import list_open_repository_github_issues
from openorc.persistence.github_webhook_deliveries import (
    list_github_webhook_deliveries_requiring_recovery,
)
from openorc.persistence.ownership import list_github_routed_repositories
from openorc.persistence.pool import DatabasePool
from openorc.persistence.pull_requests import get_task_pull_request_for_task
from openorc.persistence.tasks import list_current_repository_tasks
from openorc.services import (
    canonical_branch,
    commit_checks_projection,
    github_issue_relations,
    github_reconciliation,
    github_webhook_dispatch,
    task_pull_request_reconciliation,
)
from openorc.services.errors import (
    ApplicationError,
    AuthorizationError,
    ExternalOperationUncertainError,
    InvalidCommandError,
    NotFoundError,
)
from openorc.services.github_webhook_dispatch import (
    GitHubWebhookDispatchResult,
    GitHubWebhookDispatchRouteStatus,
)

__all__ = [
    "GitHubReconciliationSweepFamily",
    "GitHubReconciliationSweepOutcome",
    "GitHubReconciliationSweepResult",
    "GitHubReconciliationSweepUnitStatus",
    "MAX_RECOVERY_DELIVERY_BATCH",
    "run_github_reconciliation_sweep",
]

logger = logging.getLogger(__name__)

# Application-service span boundary (issues #108/#109): one span per public
# use-case operation. Only the safe attribute vocabulary is attachable.
_SERVICE_TRACER_SCOPE = "openorc.services.github_reconciliation_recovery"
_SWEEP_SPAN_NAME = "github_reconciliation_recovery.run_github_reconciliation_sweep"

# The delivery-recovery batch cap: one sweep invocation re-dispatches at most
# this many unresolved deliveries, oldest first. Completed deliveries leave
# the eligible set through the bounded ``processed_at`` marking, so repeated
# sweeps drain any backlog; persistently failing deliveries stay visible in
# each sweep's outcomes. A bounded cap keeps one sweep's provider work
# bounded regardless of how long an outage lasted.
MAX_RECOVERY_DELIVERY_BATCH = 50


class GitHubReconciliationSweepFamily(StrEnum):
    """The bounded sweep unit families, in one invocation's execution order."""

    REPOSITORY = "repository"
    ISSUE = "issue"
    RELATIONS = "relations"
    BRANCH = "branch"
    PULL_REQUEST = "pull_request"
    CHECKS = "checks"
    DELIVERY = "delivery"


class GitHubReconciliationSweepUnitStatus(StrEnum):
    """The bounded outcome of one sweep unit.

    ``reconciled`` — the composed authoritative capability completed.
    ``declined`` — a typed, expected product outcome with nothing to
    reconcile (a canonical subject that vanished between the work-set read
    and the unit). ``failed_known`` — a typed known failure (including the
    durable route/installation conditions); recoverable and retried by the
    next rotation. ``failed_uncertain`` — an unknown outcome, surfaced
    explicitly and never destructive. ``failed_unexpected`` — an unexpected
    invariant failure, contained with a safe type classification only.
    ``access_lost`` — the normalized integration/authorization condition
    (lost installation/repository access, a missing/deleted canonical
    branch); mirrors stay byte-identical and no credential fallback exists.
    """

    RECONCILED = "reconciled"
    DECLINED = "declined"
    FAILED_KNOWN = "failed_known"
    FAILED_UNCERTAIN = "failed_uncertain"
    FAILED_UNEXPECTED = "failed_unexpected"
    ACCESS_LOST = "access_lost"


@dataclass(frozen=True, slots=True)
class GitHubReconciliationSweepOutcome:
    """One sweep unit's bounded outcome and its normalized fact deltas.

    Only the addressed unit's family fields carry values; every other field
    keeps its default ("nothing observed"). The facts are the durable
    before→current transitions the composed capabilities established, plus
    the pure comparisons the sweep performs for the later workflow layer —
    never workflow decisions.
    """

    family: GitHubReconciliationSweepFamily
    status: GitHubReconciliationSweepUnitStatus
    workspace_id: UUID | None = None
    repository_id: UUID | None = None
    task_id: UUID | None = None
    github_issue_id: int | None = None
    delivery_guid: str | None = None
    # Repository unit facts.
    repository_metadata_changed: bool = False
    # Issue unit facts: the reconciled projection's before→current transition
    # and the pure baseline-drift comparison against the immutable Task
    # source fingerprint (None when no current Task backs the issue).
    issue_created: bool = False
    issue_state_changed: bool = False
    issue_requirements_changed: bool = False
    issue_previous_fingerprint: str | None = None
    issue_current_fingerprint: str | None = None
    task_baseline_drift: bool | None = None
    # Relations unit facts: the dependency (blocking-sequencing) mirror and
    # the presentation-only hierarchy mirror transitions.
    dependency_blocked: bool | None = None
    dependency_mirror_changed: bool = False
    hierarchy_changed: bool = False
    # Branch unit facts: the observed bound-branch head (read model).
    branch_observed: bool = False
    branch_head_sha: str | None = None
    # Pull-request unit facts: the canonical PR's before→current observation
    # and the merged / closed-unmerged lifecycle classification.
    pull_request_updated: bool = False
    pull_request_head_changed: bool = False
    pull_request_base_changed: bool = False
    pull_request_state_changed: bool = False
    pull_request_previous_head_sha: str | None = None
    pull_request_previous_state: TaskPullRequestState | None = None
    pull_request_merged: bool = False
    pull_request_closed_unmerged: bool = False
    # Checks unit facts: the exact-head projection summary (read model; the
    # detailed check/status surfaces are re-projectable on demand).
    checks_projected: bool = False
    checks_head_sha: str | None = None
    checks_combined_status_state: str | None = None
    checks_check_run_count: int | None = None
    # Delivery unit facts: whether the re-dispatched delivery's routed
    # reconciliations all completed (the bounded processed_at marking).
    delivery_processed: bool | None = None


@dataclass(frozen=True, slots=True)
class GitHubReconciliationSweepResult:
    """The typed result of one sweep invocation.

    ``tick`` is the scheduling position that selected the serviced buckets
    (``None`` for the operator's full sweep) and ``partitions`` the stable
    rotation count the tick was taken modulo. ``outcomes`` is the
    deterministic, execution-ordered record of every serviced unit —
    including its failures — so partial sweeps stay visible and re-runnable.
    """

    tick: int | None
    partitions: int
    outcomes: tuple[GitHubReconciliationSweepOutcome, ...]


def _in_tick_bucket(stable: int, sweep_tick: int | None, partitions: int) -> bool:
    """Whether one stable-identity unit is serviced by this sweep tick.

    Bucket membership depends only on the stable identity and the rotation,
    never on the eligible set's size or order, so units can never be starved:
    every eligible identity is serviced exactly once per ``partitions``
    ticks, regardless of set growth, shrinkage, or prior failures. A
    ``None`` tick (the operator's full sweep) services every bucket.
    """
    if sweep_tick is None:
        return True
    return stable % partitions == sweep_tick % partitions


def _known_failure_outcome(
    family: GitHubReconciliationSweepFamily,
    *,
    workspace_id: UUID | None,
    repository_id: UUID | None,
    task_id: UUID | None,
    github_issue_id: int | None,
    delivery_guid: str | None,
    error: ApplicationError,
) -> GitHubReconciliationSweepOutcome:
    """Classify one unit's typed failure into the bounded sweep status."""
    status = (
        GitHubReconciliationSweepUnitStatus.FAILED_UNCERTAIN
        if isinstance(error, ExternalOperationUncertainError)
        else GitHubReconciliationSweepUnitStatus.FAILED_KNOWN
    )
    logger.info(
        "github reconciliation sweep unit did not complete (%s/%s; workspace %s); "
        "the unit remains recoverable by the next rotation",
        family.value,
        status.value,
        workspace_id,
    )
    return GitHubReconciliationSweepOutcome(
        family=family,
        status=status,
        workspace_id=workspace_id,
        repository_id=repository_id,
        task_id=task_id,
        github_issue_id=github_issue_id,
        delivery_guid=delivery_guid,
    )


def _unexpected_failure_outcome(
    family: GitHubReconciliationSweepFamily,
    *,
    workspace_id: UUID | None,
    repository_id: UUID | None,
    task_id: UUID | None,
    github_issue_id: int | None,
    delivery_guid: str | None,
    error: Exception,
) -> GitHubReconciliationSweepOutcome:
    """Contain one unit's unexpected failure with a safe type classification.

    Arbitrary exception content may carry provider bodies or customer
    content; only the exception type name is ever logged.
    """
    logger.warning(
        "github reconciliation sweep unit failed with an unexpected error of "
        "type %s (%s; workspace %s); the unit remains recoverable",
        type(error).__name__,
        family.value,
        workspace_id,
    )
    return GitHubReconciliationSweepOutcome(
        family=family,
        status=GitHubReconciliationSweepUnitStatus.FAILED_UNEXPECTED,
        workspace_id=workspace_id,
        repository_id=repository_id,
        task_id=task_id,
        github_issue_id=github_issue_id,
        delivery_guid=delivery_guid,
    )


def _access_lost_outcome(
    family: GitHubReconciliationSweepFamily,
    *,
    workspace_id: UUID | None,
    repository_id: UUID | None,
    task_id: UUID | None,
    github_issue_id: int | None,
    delivery_guid: str | None,
) -> GitHubReconciliationSweepOutcome:
    """Surface the normalized access-loss condition without any fallback.

    Lost installation/repository access (and a missing/deleted canonical
    branch, the same classified condition) is a durable integration fact the
    later workflow layer owns. The composed capabilities leave their mirrors
    byte-identical; the sweep records the fact and moves on.
    """
    logger.info(
        "github reconciliation sweep unit reported lost GitHub access "
        "(%s; workspace %s); no credential fallback exists",
        family.value,
        workspace_id,
    )
    return GitHubReconciliationSweepOutcome(
        family=family,
        status=GitHubReconciliationSweepUnitStatus.ACCESS_LOST,
        workspace_id=workspace_id,
        repository_id=repository_id,
        task_id=task_id,
        github_issue_id=github_issue_id,
        delivery_guid=delivery_guid,
    )


def _declined_outcome(
    family: GitHubReconciliationSweepFamily,
    *,
    workspace_id: UUID | None,
    repository_id: UUID | None,
    task_id: UUID | None,
    github_issue_id: int | None,
    delivery_guid: str | None,
) -> GitHubReconciliationSweepOutcome:
    """Record a typed nothing-outstanding decline for one unit."""
    return GitHubReconciliationSweepOutcome(
        family=family,
        status=GitHubReconciliationSweepUnitStatus.DECLINED,
        workspace_id=workspace_id,
        repository_id=repository_id,
        task_id=task_id,
        github_issue_id=github_issue_id,
        delivery_guid=delivery_guid,
    )


def _sweep_repository_unit(
    pool: DatabasePool, github: GitHubAppClient, *, workspace_id: UUID, repository_id: UUID
) -> GitHubReconciliationSweepOutcome:
    """Reconcile one configured repository's mutable metadata from fresh authority.

    A not-found route/installation condition is a known, recoverable
    configuration fact here — not a decline — so a repository whose durable
    route drifted stays visible and is retried by the next rotation.
    """
    try:
        result = github_reconciliation.reconcile_repository_observation(
            pool, github, workspace_id=workspace_id, repository_id=repository_id
        )
    except NotFoundError as error:
        return _known_failure_outcome(
            GitHubReconciliationSweepFamily.REPOSITORY,
            workspace_id=workspace_id,
            repository_id=repository_id,
            task_id=None,
            github_issue_id=None,
            delivery_guid=None,
            error=error,
        )
    except AuthorizationError:
        return _access_lost_outcome(
            GitHubReconciliationSweepFamily.REPOSITORY,
            workspace_id=workspace_id,
            repository_id=repository_id,
            task_id=None,
            github_issue_id=None,
            delivery_guid=None,
        )
    except ApplicationError as error:
        return _known_failure_outcome(
            GitHubReconciliationSweepFamily.REPOSITORY,
            workspace_id=workspace_id,
            repository_id=repository_id,
            task_id=None,
            github_issue_id=None,
            delivery_guid=None,
            error=error,
        )
    except Exception as error:  # noqa: BLE001 - contained with safe classification
        return _unexpected_failure_outcome(
            GitHubReconciliationSweepFamily.REPOSITORY,
            workspace_id=workspace_id,
            repository_id=repository_id,
            task_id=None,
            github_issue_id=None,
            delivery_guid=None,
            error=error,
        )
    return GitHubReconciliationSweepOutcome(
        family=GitHubReconciliationSweepFamily.REPOSITORY,
        status=GitHubReconciliationSweepUnitStatus.RECONCILED,
        workspace_id=workspace_id,
        repository_id=repository_id,
        repository_metadata_changed=result.metadata_changed,
    )


def _sweep_issue_unit(
    pool: DatabasePool,
    github: GitHubAppClient,
    *,
    workspace_id: UUID,
    repository_id: UUID,
    github_issue_id: int,
    issue_number: int,
    current_task: Task | None,
) -> GitHubReconciliationSweepOutcome:
    """Reconcile one work-set issue's projection from fresh authority.

    The composed capability performs the fresh authoritative read and the
    serialized change-only write; the sweep then reports the before→current
    facts and the pure requirements-baseline comparison against the backing
    current Task's immutable source fingerprint. The baseline is never
    rewritten and no Phase 2E consequence is decided here. A not-found
    route/installation condition is a known recoverable failure — the
    repository is otherwise durably configured and the subject itself cannot
    be absent through this primitive.
    """
    try:
        reconciliation = github_reconciliation.reconcile_repository_issue(
            pool,
            github,
            workspace_id=workspace_id,
            repository_id=repository_id,
            issue_number=issue_number,
        )
    except NotFoundError as error:
        return _known_failure_outcome(
            GitHubReconciliationSweepFamily.ISSUE,
            workspace_id=workspace_id,
            repository_id=repository_id,
            task_id=None,
            github_issue_id=github_issue_id,
            delivery_guid=None,
            error=error,
        )
    except AuthorizationError:
        return _access_lost_outcome(
            GitHubReconciliationSweepFamily.ISSUE,
            workspace_id=workspace_id,
            repository_id=repository_id,
            task_id=None,
            github_issue_id=github_issue_id,
            delivery_guid=None,
        )
    except ApplicationError as error:
        return _known_failure_outcome(
            GitHubReconciliationSweepFamily.ISSUE,
            workspace_id=workspace_id,
            repository_id=repository_id,
            task_id=None,
            github_issue_id=github_issue_id,
            delivery_guid=None,
            error=error,
        )
    except Exception as error:  # noqa: BLE001 - contained with safe classification
        return _unexpected_failure_outcome(
            GitHubReconciliationSweepFamily.ISSUE,
            workspace_id=workspace_id,
            repository_id=repository_id,
            task_id=None,
            github_issue_id=github_issue_id,
            delivery_guid=None,
            error=error,
        )
    drift: bool | None = (
        None
        if current_task is None
        else reconciliation.issue.requirements_fingerprint
        != current_task.source_requirements_fingerprint
    )
    return GitHubReconciliationSweepOutcome(
        family=GitHubReconciliationSweepFamily.ISSUE,
        status=GitHubReconciliationSweepUnitStatus.RECONCILED,
        workspace_id=workspace_id,
        repository_id=repository_id,
        github_issue_id=github_issue_id,
        issue_created=reconciliation.issue_created,
        issue_state_changed=reconciliation.issue_state_changed,
        issue_requirements_changed=reconciliation.requirements_changed,
        issue_previous_fingerprint=reconciliation.previous_fingerprint,
        issue_current_fingerprint=reconciliation.issue.requirements_fingerprint,
        task_baseline_drift=drift,
    )


def _sweep_relations_unit(
    pool: DatabasePool,
    github: GitHubAppClient,
    *,
    workspace_id: UUID,
    repository_id: UUID,
    github_issue_id: int,
    issue_number: int,
) -> GitHubReconciliationSweepOutcome:
    """Re-synchronize one work-set issue's dependency and hierarchy mirrors.

    The dependency unit (authoritative input to later sequencing/eligibility
    decisions) runs first; the hierarchy unit (presentation-only, never
    blocking/executability semantics) runs only after a completed dependency
    unit, and its own failure classifies the outcome without discarding the
    already-durable dependency facts. Every composed capability leaves its
    mirror byte-identical on failure.
    """
    outcome = GitHubReconciliationSweepOutcome(
        family=GitHubReconciliationSweepFamily.RELATIONS,
        status=GitHubReconciliationSweepUnitStatus.RECONCILED,
        workspace_id=workspace_id,
        repository_id=repository_id,
        github_issue_id=github_issue_id,
    )
    try:
        dependency_result = github_issue_relations.synchronize_repository_issue_dependencies(
            pool,
            github,
            workspace_id=workspace_id,
            repository_id=repository_id,
            issue_number=issue_number,
            github_issue_id=github_issue_id,
        )
    except AuthorizationError:
        return _access_lost_outcome(
            GitHubReconciliationSweepFamily.RELATIONS,
            workspace_id=workspace_id,
            repository_id=repository_id,
            task_id=None,
            github_issue_id=github_issue_id,
            delivery_guid=None,
        )
    except ApplicationError as error:
        return _known_failure_outcome(
            GitHubReconciliationSweepFamily.RELATIONS,
            workspace_id=workspace_id,
            repository_id=repository_id,
            task_id=None,
            github_issue_id=github_issue_id,
            delivery_guid=None,
            error=error,
        )
    except Exception as error:  # noqa: BLE001 - contained with safe classification
        return _unexpected_failure_outcome(
            GitHubReconciliationSweepFamily.RELATIONS,
            workspace_id=workspace_id,
            repository_id=repository_id,
            task_id=None,
            github_issue_id=github_issue_id,
            delivery_guid=None,
            error=error,
        )
    outcome = dataclasses.replace(
        outcome,
        dependency_blocked=dependency_result.blocked,
        dependency_mirror_changed=dependency_result.changed,
    )
    try:
        hierarchy_result = github_issue_relations.synchronize_repository_issue_hierarchy(
            pool,
            github,
            workspace_id=workspace_id,
            repository_id=repository_id,
            issue_number=issue_number,
            github_issue_id=github_issue_id,
        )
    except AuthorizationError:
        return _access_lost_outcome(
            GitHubReconciliationSweepFamily.RELATIONS,
            workspace_id=workspace_id,
            repository_id=repository_id,
            task_id=None,
            github_issue_id=github_issue_id,
            delivery_guid=None,
        )
    except ApplicationError as error:
        return _known_failure_outcome(
            GitHubReconciliationSweepFamily.RELATIONS,
            workspace_id=workspace_id,
            repository_id=repository_id,
            task_id=None,
            github_issue_id=github_issue_id,
            delivery_guid=None,
            error=error,
        )
    except Exception as error:  # noqa: BLE001 - contained with safe classification
        return _unexpected_failure_outcome(
            GitHubReconciliationSweepFamily.RELATIONS,
            workspace_id=workspace_id,
            repository_id=repository_id,
            task_id=None,
            github_issue_id=github_issue_id,
            delivery_guid=None,
            error=error,
        )
    return dataclasses.replace(outcome, hierarchy_changed=hierarchy_result.changed)


def _sweep_branch_unit(
    pool: DatabasePool, github: GitHubAppClient, *, workspace_id: UUID, task: Task
) -> GitHubReconciliationSweepOutcome:
    """Observe one current Task's bound canonical branch from fresh authority.

    A not-found outcome (the Task or its bound-branch fact vanished between
    the work-set read and the observation) is a typed decline — nothing
    remains to reconcile. A missing/deleted branch or lost access is the
    normalized access-loss condition, never runtime-local trust.
    """
    try:
        observation = canonical_branch.observe_bound_canonical_task_branch(
            pool, github, workspace_id=workspace_id, task_id=task.id
        )
    except NotFoundError:
        return _declined_outcome(
            GitHubReconciliationSweepFamily.BRANCH,
            workspace_id=workspace_id,
            repository_id=task.repository_id,
            task_id=task.id,
            github_issue_id=None,
            delivery_guid=None,
        )
    except AuthorizationError:
        return _access_lost_outcome(
            GitHubReconciliationSweepFamily.BRANCH,
            workspace_id=workspace_id,
            repository_id=task.repository_id,
            task_id=task.id,
            github_issue_id=None,
            delivery_guid=None,
        )
    except ApplicationError as error:
        return _known_failure_outcome(
            GitHubReconciliationSweepFamily.BRANCH,
            workspace_id=workspace_id,
            repository_id=task.repository_id,
            task_id=task.id,
            github_issue_id=None,
            delivery_guid=None,
            error=error,
        )
    except Exception as error:  # noqa: BLE001 - contained with safe classification
        return _unexpected_failure_outcome(
            GitHubReconciliationSweepFamily.BRANCH,
            workspace_id=workspace_id,
            repository_id=task.repository_id,
            task_id=task.id,
            github_issue_id=None,
            delivery_guid=None,
            error=error,
        )
    return GitHubReconciliationSweepOutcome(
        family=GitHubReconciliationSweepFamily.BRANCH,
        status=GitHubReconciliationSweepUnitStatus.RECONCILED,
        workspace_id=workspace_id,
        repository_id=task.repository_id,
        task_id=task.id,
        branch_observed=True,
        branch_head_sha=observation.head_sha,
    )


def _sweep_pull_request_unit(
    pool: DatabasePool,
    github: GitHubAppClient,
    *,
    workspace_id: UUID,
    repository_id: UUID,
    task_id: UUID,
) -> GitHubReconciliationSweepOutcome:
    """Reconcile one current Task's canonical pull request from fresh authority.

    The strictly update-only observed-snapshot write yields the serialized
    before→current facts; the sweep additionally classifies the merged and
    closed-unmerged lifecycle observations. No Phase 2E consequence (review
    dispatch, completion, ``PR_CLOSED_UNMERGED``) is decided here. A
    not-found outcome is a typed decline (the canonical subject vanished
    between the work-set read and the unit).
    """
    try:
        reconciliation = task_pull_request_reconciliation.reconcile_task_pull_request(
            pool, github, workspace_id=workspace_id, task_id=task_id
        )
    except NotFoundError:
        return _declined_outcome(
            GitHubReconciliationSweepFamily.PULL_REQUEST,
            workspace_id=workspace_id,
            repository_id=repository_id,
            task_id=task_id,
            github_issue_id=None,
            delivery_guid=None,
        )
    except AuthorizationError:
        return _access_lost_outcome(
            GitHubReconciliationSweepFamily.PULL_REQUEST,
            workspace_id=workspace_id,
            repository_id=repository_id,
            task_id=task_id,
            github_issue_id=None,
            delivery_guid=None,
        )
    except ApplicationError as error:
        return _known_failure_outcome(
            GitHubReconciliationSweepFamily.PULL_REQUEST,
            workspace_id=workspace_id,
            repository_id=repository_id,
            task_id=task_id,
            github_issue_id=None,
            delivery_guid=None,
            error=error,
        )
    except Exception as error:  # noqa: BLE001 - contained with safe classification
        return _unexpected_failure_outcome(
            GitHubReconciliationSweepFamily.PULL_REQUEST,
            workspace_id=workspace_id,
            repository_id=repository_id,
            task_id=task_id,
            github_issue_id=None,
            delivery_guid=None,
            error=error,
        )
    observed = reconciliation.pull_request
    return GitHubReconciliationSweepOutcome(
        family=GitHubReconciliationSweepFamily.PULL_REQUEST,
        status=GitHubReconciliationSweepUnitStatus.RECONCILED,
        workspace_id=workspace_id,
        repository_id=repository_id,
        task_id=task_id,
        pull_request_updated=reconciliation.updated,
        pull_request_head_changed=reconciliation.head_changed,
        pull_request_base_changed=reconciliation.base_changed,
        pull_request_state_changed=reconciliation.state_changed,
        pull_request_previous_head_sha=reconciliation.previous_head_sha,
        pull_request_previous_state=reconciliation.previous_state,
        pull_request_merged=(
            observed.state is TaskPullRequestState.CLOSED and observed.merged_at is not None
        ),
        pull_request_closed_unmerged=(
            observed.state is TaskPullRequestState.CLOSED and observed.merged_at is None
        ),
    )


def _sweep_checks_unit(
    pool: DatabasePool,
    github: GitHubAppClient,
    *,
    workspace_id: UUID,
    repository_id: UUID,
    task_id: UUID,
) -> GitHubReconciliationSweepOutcome:
    """Project one canonical PR's checks/status surfaces for its exact head.

    The projection is the typed read model mirroring the #120 CHECKS
    dispatch; the sweep records the bounded projection summary for the later
    workflow layer (the detailed check/status surfaces are re-projectable on
    demand). A not-found outcome is a typed decline.
    """
    try:
        projection = commit_checks_projection.project_commit_checks(
            pool, github, workspace_id=workspace_id, task_id=task_id
        )
    except NotFoundError:
        return _declined_outcome(
            GitHubReconciliationSweepFamily.CHECKS,
            workspace_id=workspace_id,
            repository_id=repository_id,
            task_id=task_id,
            github_issue_id=None,
            delivery_guid=None,
        )
    except AuthorizationError:
        return _access_lost_outcome(
            GitHubReconciliationSweepFamily.CHECKS,
            workspace_id=workspace_id,
            repository_id=repository_id,
            task_id=task_id,
            github_issue_id=None,
            delivery_guid=None,
        )
    except ApplicationError as error:
        return _known_failure_outcome(
            GitHubReconciliationSweepFamily.CHECKS,
            workspace_id=workspace_id,
            repository_id=repository_id,
            task_id=task_id,
            github_issue_id=None,
            delivery_guid=None,
            error=error,
        )
    except Exception as error:  # noqa: BLE001 - contained with safe classification
        return _unexpected_failure_outcome(
            GitHubReconciliationSweepFamily.CHECKS,
            workspace_id=workspace_id,
            repository_id=repository_id,
            task_id=task_id,
            github_issue_id=None,
            delivery_guid=None,
            error=error,
        )
    return GitHubReconciliationSweepOutcome(
        family=GitHubReconciliationSweepFamily.CHECKS,
        status=GitHubReconciliationSweepUnitStatus.RECONCILED,
        workspace_id=workspace_id,
        repository_id=repository_id,
        task_id=task_id,
        checks_projected=True,
        checks_head_sha=projection.head_sha,
        checks_combined_status_state=projection.combined_status_state,
        checks_check_run_count=len(projection.check_runs),
    )


def _delivery_dispatch_status(
    result: GitHubWebhookDispatchResult,
) -> GitHubReconciliationSweepUnitStatus:
    """Map a re-dispatched delivery's bounded outcomes to one sweep status.

    ``processed`` is the known success; otherwise the aggregate route
    classification is the sweep's fact, with uncertainty taking precedence so
    an unknown outcome is never reported as a clean known failure.
    """
    if result.processed:
        return GitHubReconciliationSweepUnitStatus.RECONCILED
    route_statuses = {outcome.status for outcome in result.route_outcomes}
    if GitHubWebhookDispatchRouteStatus.FAILED_UNCERTAIN in route_statuses:
        return GitHubReconciliationSweepUnitStatus.FAILED_UNCERTAIN
    if GitHubWebhookDispatchRouteStatus.FAILED_UNEXPECTED in route_statuses:
        return GitHubReconciliationSweepUnitStatus.FAILED_UNEXPECTED
    return GitHubReconciliationSweepUnitStatus.FAILED_KNOWN


def _sweep_delivery_units(
    pool: DatabasePool, github: GitHubAppClient
) -> list[GitHubReconciliationSweepOutcome]:
    """Recover recorded deliveries whose downstream dispatch did not complete.

    Each unresolved delivery re-enters the untouched #120 dispatch boundary:
    it reloads durable state by the delivery GUID, re-validates current
    routing, re-runs the routed authoritative reconciliation, and marks only
    the bounded ``processed_at`` recovery instant when every routed
    reconciliation completed or typedly declined. The provider delivery
    identity is never rewritten, inbox status is never GitHub truth, and the
    oldest-first batch cap keeps the recovery work bounded while the backlog
    drains across repeated sweeps.
    """
    outcomes: list[GitHubReconciliationSweepOutcome] = []
    deliveries = list_github_webhook_deliveries_requiring_recovery(
        pool, limit=MAX_RECOVERY_DELIVERY_BATCH
    )
    for delivery in deliveries:
        try:
            result = github_webhook_dispatch.dispatch_github_webhook_delivery(
                pool, github, delivery_guid=delivery.delivery_guid
            )
        except ApplicationError as error:
            outcomes.append(
                _known_failure_outcome(
                    GitHubReconciliationSweepFamily.DELIVERY,
                    workspace_id=None,
                    repository_id=None,
                    task_id=None,
                    github_issue_id=None,
                    delivery_guid=delivery.delivery_guid,
                    error=error,
                )
            )
            continue
        except Exception as error:  # noqa: BLE001 - contained with safe classification
            outcomes.append(
                _unexpected_failure_outcome(
                    GitHubReconciliationSweepFamily.DELIVERY,
                    workspace_id=None,
                    repository_id=None,
                    task_id=None,
                    github_issue_id=None,
                    delivery_guid=delivery.delivery_guid,
                    error=error,
                )
            )
            continue
        outcomes.append(
            GitHubReconciliationSweepOutcome(
                family=GitHubReconciliationSweepFamily.DELIVERY,
                status=_delivery_dispatch_status(result),
                delivery_guid=delivery.delivery_guid,
                delivery_processed=result.processed,
            )
        )
    return outcomes


def _repository_issue_work_set(
    pool: DatabasePool, *, workspace_id: UUID, repository_id: UUID
) -> list[tuple[int, int, Task | None]]:
    """Derive one repository's bounded issue work set from durable current state.

    The set is the union of the open tracked issue projections and the
    issues of the repository's current (non-archived) Tasks — the issues the
    current workflow actually needs reconciled — deduplicated, resolved to
    their durable address numbers (the projection's when tracked, otherwise
    the current Task's), ordered by stable issue identity, and paired with
    the backing current Task (for the immutable-baseline drift comparison).
    Closed historical issues with no current Task never enter the set, so
    sweep cost cannot grow with repository history.
    """
    open_projections = list_open_repository_github_issues(
        pool, workspace_id=workspace_id, repository_id=repository_id
    )
    current_tasks = list_current_repository_tasks(
        pool, workspace_id=workspace_id, repository_id=repository_id
    )
    issue_numbers: dict[int, int] = {}
    current_task_by_issue: dict[int, Task] = {}
    for projection in open_projections:
        issue_numbers.setdefault(projection.identity.github_issue_id, projection.issue_number)
    for task in current_tasks:
        issue_numbers.setdefault(task.github_issue_id, task.github_issue_number)
        current_task_by_issue[task.github_issue_id] = task
    return sorted(
        (
            (
                github_issue_id,
                issue_number,
                current_task_by_issue.get(github_issue_id),
            )
            for github_issue_id, issue_number in issue_numbers.items()
        ),
        key=lambda entry: entry[0],
    )


def run_github_reconciliation_sweep(
    pool: DatabasePool,
    github: GitHubAppClient,
    settings: Settings,
    *,
    sweep_tick: int | None = None,
) -> GitHubReconciliationSweepResult:
    """Run one bounded, repeatable GitHub reconciliation/recovery sweep.

    The single shared entry point for periodic scheduled reconciliation,
    operator/manual recovery, retry after a known-safe reconciliation
    failure, and recovery of recorded deliveries whose dispatch did not
    complete. ``sweep_tick`` selects the serviced stable-identity bucket of
    every family (``None`` services the whole eligible set); the partition
    count comes from the process settings and bounds the per-invocation
    provider-facing work. Every unit re-derives its subject from durable
    Postgres at its own start and re-reads fresh authoritative GitHub state,
    so any tick is safe to run, re-run, replay, or reorder, and the sweep
    converges on current GitHub truth without ever promoting webhook or
    queue state into authority.

    The sweep performs no GitHub I/O itself: each unit composes an existing
    focused capability that owns its short transactions around its external
    reads, and the work-set derivation is database-only. The execution order
    is the family order — repository metadata, then issues with their
    relations, then bound branches and canonical PRs with their checks
    projections, then the bounded delivery-recovery batch.
    """
    if sweep_tick is not None and (
        isinstance(sweep_tick, bool) or not isinstance(sweep_tick, int) or sweep_tick < 0
    ):
        raise InvalidCommandError("sweep_tick must be a non-negative integer or None")
    partitions = settings.github_reconciliation_sweep_partitions
    if isinstance(partitions, bool) or not isinstance(partitions, int) or partitions < 1:
        raise InvalidCommandError("the sweep partition count must be a positive integer")
    with application_span(_SERVICE_TRACER_SCOPE, _SWEEP_SPAN_NAME) as span:
        annotate_span(span, operation=_SWEEP_SPAN_NAME)
        outcomes: list[GitHubReconciliationSweepOutcome] = []
        repositories = list_github_routed_repositories(pool)

        # Family 1 — repository metadata (bucketed by repository identity).
        for repository in repositories:
            if not _in_tick_bucket(repository.id.int, sweep_tick, partitions):
                continue
            outcomes.append(
                _sweep_repository_unit(
                    pool,
                    github,
                    workspace_id=repository.workspace_id,
                    repository_id=repository.id,
                )
            )

        # Family 2 — issue + relation units (bucketed by stable issue
        # identity), over each configured repository's bounded current-work
        # issue set.
        for repository in repositories:
            for github_issue_id, issue_number, current_task in _repository_issue_work_set(
                pool, workspace_id=repository.workspace_id, repository_id=repository.id
            ):
                if not _in_tick_bucket(github_issue_id, sweep_tick, partitions):
                    continue
                outcomes.append(
                    _sweep_issue_unit(
                        pool,
                        github,
                        workspace_id=repository.workspace_id,
                        repository_id=repository.id,
                        github_issue_id=github_issue_id,
                        issue_number=issue_number,
                        current_task=current_task,
                    )
                )
                outcomes.append(
                    _sweep_relations_unit(
                        pool,
                        github,
                        workspace_id=repository.workspace_id,
                        repository_id=repository.id,
                        github_issue_id=github_issue_id,
                        issue_number=issue_number,
                    )
                )

        # Family 3 — branch / pull-request / checks units (bucketed by Task
        # identity), over each configured repository's current Tasks. The PR
        # unit runs before its checks unit so the projection addresses the
        # freshly reconciled head.
        for repository in repositories:
            for task in list_current_repository_tasks(
                pool, workspace_id=repository.workspace_id, repository_id=repository.id
            ):
                if not _in_tick_bucket(task.id.int, sweep_tick, partitions):
                    continue
                if task.canonical_feature_branch is not None:
                    outcomes.append(
                        _sweep_branch_unit(
                            pool,
                            github,
                            workspace_id=repository.workspace_id,
                            task=task,
                        )
                    )
                if get_task_pull_request_for_task(pool, task_id=task.id) is None:
                    continue
                outcomes.append(
                    _sweep_pull_request_unit(
                        pool,
                        github,
                        workspace_id=repository.workspace_id,
                        repository_id=repository.id,
                        task_id=task.id,
                    )
                )
                outcomes.append(
                    _sweep_checks_unit(
                        pool,
                        github,
                        workspace_id=repository.workspace_id,
                        repository_id=repository.id,
                        task_id=task.id,
                    )
                )

        # Family 4 — bounded delivery-recovery batch (oldest first).
        outcomes.extend(_sweep_delivery_units(pool, github))

        return GitHubReconciliationSweepResult(
            tick=sweep_tick,
            partitions=partitions,
            outcomes=tuple(outcomes),
        )
