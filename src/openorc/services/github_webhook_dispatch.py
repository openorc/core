"""GitHub webhook dispatch services (issue #120).

Dispatch accepted, durably deduplicated GitHub webhook notifications into the
existing authoritative reconciliation capabilities (B3/B4/B7). A webhook
delivery is a notification that triggers authoritative reconciliation —
never GitHub truth and never workflow authority: no webhook payload and no
queue state exists here at all. Dispatch reads only the durable delivery
record and its resolved Workspace routing linkages (recorded by #61 intake),
and every supported path invokes the relevant authoritative GitHub API
capability whose fresh reads — never the payload state — determine what
canonical OpenOrc state becomes.

Three operations:

- :func:`intake_and_dispatch_github_webhook` — the facade the thin FastAPI
  route calls. It composes the untouched #61 intake boundary with submission,
  so the transport owns no duplicate/relevance/processing semantics and no
  enqueue decision.
- :func:`submit_github_webhook_dispatch` — the enqueue boundary. Reloads the
  durable delivery by its provider delivery GUID, reads the persisted routes,
  marks the bounded ``processed_at`` recovery instant for terminal deliveries
  (nothing outstanding), and otherwise hands the delivery GUID — the only
  safe payload member — to the injected queue seam. Queue submission outcomes
  are explicit: known failure and uncertain outcome are distinct, and an
  uncertain outcome never causes a blind duplicate enqueue or any
  reconciliation here; unresolved processing remains recoverable by #62's
  independent reconciliation path.
- :func:`dispatch_github_webhook_delivery` — the worker-invoked
  reconciliation boundary. Reloads durable state by the delivery GUID, reads
  the persisted routes, fans out deterministically over them, and marks the
  delivery processed only when every routed reconciliation completed or
  typedly declined with nothing outstanding. Duplicate, replayed, delayed,
  and out-of-order notifications converge because every reconciliation
  re-reads current authoritative GitHub state.

No database transaction ever spans queue or GitHub I/O: the durable reads and
the processed-marking are short transactions, the enqueue is a queue call
with none open, and the invoked reconciliation services own their own short
transactions around their external reads.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from enum import StrEnum
from typing import Protocol
from uuid import UUID

from openorc.adapters.github import GitHubAppClient
from openorc.config import Settings
from openorc.domain.github_webhooks import (
    GitHubWebhookDelivery,
    GitHubWebhookDeliveryClassification,
    GitHubWebhookResolvedRoute,
    GitHubWebhookRoutingResolution,
    GitHubWebhookRoutingTarget,
)
from openorc.observability import annotate_span, application_span
from openorc.persistence.github_issues import list_repository_github_issues
from openorc.persistence.github_webhook_deliveries import (
    get_github_webhook_delivery,
    list_github_webhook_delivery_routes,
    mark_github_webhook_delivery_processed,
)
from openorc.persistence.pool import DatabasePool
from openorc.persistence.pull_requests import (
    find_task_pull_request_by_number,
    list_task_pull_requests_for_repository,
)
from openorc.services import (
    commit_checks_projection,
    github_issue_relations,
    github_reconciliation,
    task_intake,
    task_pull_request_reconciliation,
)
from openorc.services.errors import (
    ApplicationError,
    ExternalOperationUncertainError,
    InvalidCommandError,
    NotFoundError,
)
from openorc.services.github_webhook_intake import GitHubWebhookIntake, intake_github_webhook

__all__ = [
    "EnqueueOutcome",
    "GitHubWebhookDispatchResult",
    "GitHubWebhookDispatchRouteOutcome",
    "GitHubWebhookDispatchRouteStatus",
    "GitHubWebhookDispatchSubmissionResult",
    "GitHubWebhookIntakeAndDispatch",
    "WebhookDispatchSubmission",
    "dispatch_github_webhook_delivery",
    "intake_and_dispatch_github_webhook",
    "submit_github_webhook_dispatch",
]

logger = logging.getLogger(__name__)

# Application-service span boundaries (issues #108/#109): one span per public
# use-case operation. Only the safe attribute vocabulary is attachable.
_SERVICE_TRACER_SCOPE = "openorc.services.github_webhook_dispatch"
_SUBMIT_SPAN_NAME = "github_webhook_dispatch.submit_github_webhook_dispatch"
_DISPATCH_SPAN_NAME = "github_webhook_dispatch.dispatch_github_webhook_delivery"
_FACADE_SPAN_NAME = "github_webhook_dispatch.intake_and_dispatch_github_webhook"


class EnqueueOutcome(StrEnum):
    """The explicit queue-submission outcome for one accepted delivery.

    ``enqueued`` — known success. ``known_failed`` — a definitive enqueue
    failure, classified for safe recovery. ``uncertain`` — the enqueue
    outcome is unknown (a timeout after sending is indistinguishable from a
    connect failure): never re-enqueued blindly and never reconciled here;
    recovery (#62) re-derives from durable state.
    """

    ENQUEUED = "enqueued"
    KNOWN_FAILED = "known_failed"
    UNCERTAIN = "uncertain"


class GitHubWebhookDispatchRouteStatus(StrEnum):
    """The bounded outcome of one routed reconciliation invocation.

    ``reconciled`` — the authoritative capability completed. ``declined`` — a
    typed, expected product outcome with nothing to reconcile (a closed or
    blocked issue, an absent canonical subject): the route is terminal.
    ``failed_known`` — a known provider/authorization conflict; the route
    stays recoverable. ``failed_uncertain`` — an unknown outcome, never
    replayed blindly. ``failed_unexpected`` — an unexpected invariant
    failure, logged only as a safe type classification.
    """

    RECONCILED = "reconciled"
    DECLINED = "declined"
    FAILED_KNOWN = "failed_known"
    FAILED_UNCERTAIN = "failed_uncertain"
    FAILED_UNEXPECTED = "failed_unexpected"


class WebhookDispatchSubmission(Protocol):
    """The queue seam dispatch submission hands delivery identities to.

    Implemented by the RQ transport in :mod:`openorc.workers.jobs`; tests
    inject recording fakes. The seam owns the queue mechanics and classifies
    the provider outcome; the service owns the decision and its durable
    marking.
    """

    def enqueue(self, delivery_guid: str) -> EnqueueOutcome: ...


@dataclass(frozen=True, slots=True)
class GitHubWebhookDispatchRouteOutcome:
    """The bounded outcome of one routed reconciliation invocation."""

    workspace_id: UUID
    repository_id: UUID
    status: GitHubWebhookDispatchRouteStatus


@dataclass(frozen=True, slots=True)
class GitHubWebhookDispatchResult:
    """The typed result of one worker-invoked dispatch invocation.

    ``processed`` is true only when every routed reconciliation reconciled or
    typedly declined — the condition under which the bounded
    ``processed_at`` recovery instant is durably written.
    """

    delivery: GitHubWebhookDelivery | None
    processed: bool
    route_outcomes: tuple[GitHubWebhookDispatchRouteOutcome, ...]


@dataclass(frozen=True, slots=True)
class GitHubWebhookDispatchSubmissionResult:
    """The typed result of one dispatch submission invocation.

    ``enqueued`` is true exactly for the known-success outcome;
    ``enqueue_outcome`` carries the bounded known-failure/uncertain
    classification otherwise (``None`` when nothing was enqueued or the
    delivery identity was unknown).
    """

    delivery: GitHubWebhookDelivery | None
    enqueued: bool
    enqueue_outcome: EnqueueOutcome | None


@dataclass(frozen=True, slots=True)
class GitHubWebhookIntakeAndDispatch:
    """The composed intake + dispatch-submission outcome the thin route maps.

    The FastAPI router translates only this result's transport-relevant typed
    errors; every duplicate/relevance/processing decision happened here.
    """

    intake: GitHubWebhookIntake
    submission: GitHubWebhookDispatchSubmissionResult | None = None


def submit_github_webhook_dispatch(
    pool: DatabasePool,
    submission: WebhookDispatchSubmission,
    *,
    delivery_guid: str,
) -> GitHubWebhookDispatchSubmissionResult:
    """Submit one accepted delivery's dispatch, deciding only from durable state.

    Reloads the durable delivery record by the provider delivery GUID and its
    persisted routes — never the intake return object, never any payload
    content. Ignored/unusable deliveries require no enqueue and no recovery
    metadata; relevant deliveries whose routing resolved nothing are terminal
    and gain their bounded ``processed_at`` recovery instant; resolved
    deliveries are enqueued outside any database transaction, with the
    delivery GUID as the only payload member. Known enqueue failure and
    uncertain enqueue outcome are distinct typed results — an uncertain
    outcome is never re-enqueued blindly and never reconciled here, and
    unresolved processing stays recoverable by #62's independent path.
    """
    with application_span(_SERVICE_TRACER_SCOPE, _SUBMIT_SPAN_NAME) as span:
        annotate_span(span, operation=_SUBMIT_SPAN_NAME)
        delivery = get_github_webhook_delivery(pool, delivery_guid=delivery_guid)
        if delivery is None:
            # An unknown delivery identity is a bounded safe observation
            # (nothing durable to dispatch); no recovery metadata is written.
            return GitHubWebhookDispatchSubmissionResult(
                delivery=None, enqueued=False, enqueue_outcome=None
            )
        if delivery.classification is not GitHubWebhookDeliveryClassification.RELEVANT:
            # Ignored/unusable deliveries were terminal at intake: they
            # require neither enqueue nor recovery metadata.
            return GitHubWebhookDispatchSubmissionResult(
                delivery=delivery, enqueued=False, enqueue_outcome=None
            )
        if delivery.routing_resolution is not GitHubWebhookRoutingResolution.RESOLVED:
            # Relevant but unmapped/unconfigured/mismatched routing: no
            # Workspace route binds this delivery — terminal. The bounded
            # recovery instant records that nothing is outstanding.
            mark_github_webhook_delivery_processed(pool, delivery_id=delivery.id)
            return GitHubWebhookDispatchSubmissionResult(
                delivery=delivery, enqueued=False, enqueue_outcome=None
            )
        routes = list_github_webhook_delivery_routes(pool, delivery_id=delivery.id)
        if not routes:
            # A resolved delivery always carries persisted routes (the intake
            # invariant); an empty set is an unexpected durable-state
            # condition: no enqueue, the delivery stays recoverable.
            logger.warning(
                "github webhook dispatch submission found no persisted routes "
                "for a resolved delivery; the delivery remains recoverable"
            )
            return GitHubWebhookDispatchSubmissionResult(
                delivery=delivery, enqueued=False, enqueue_outcome=None
            )
        outcome = submission.enqueue(delivery_guid)
        if outcome is EnqueueOutcome.KNOWN_FAILED:
            logger.warning(
                "github webhook dispatch enqueue failed as a known queue "
                "condition; the delivery remains recoverable"
            )
        elif outcome is EnqueueOutcome.UNCERTAIN:
            # Uncertain enqueue outcome: no blind re-enqueue, no
            # reconciliation at submission — recovery re-derives.
            logger.warning(
                "github webhook dispatch enqueue outcome is unknown; "
                "the delivery remains recoverable"
            )
        return GitHubWebhookDispatchSubmissionResult(
            delivery=delivery,
            enqueued=outcome is EnqueueOutcome.ENQUEUED,
            enqueue_outcome=None if outcome is EnqueueOutcome.ENQUEUED else outcome,
        )


def dispatch_github_webhook_delivery(
    pool: DatabasePool,
    github: GitHubAppClient,
    *,
    delivery_guid: str,
) -> GitHubWebhookDispatchResult:
    """Dispatch one accepted delivery into authoritative reconciliation (#120).

    The worker-invoked boundary: reloads durable state by the provider
    delivery GUID, reads the persisted Workspace routing linkages, and fans
    out deterministically over them. Every routed invocation calls the
    target's authoritative B3/B4/B7 capability — whose fresh GitHub reads,
    never any payload state, determine canonical effects. The delivery is
    marked processed only when every route reconciled or typedly declined;
    known failures, uncertain outcomes, and unexpected conditions leave it
    recoverable for #62. Duplicate, replayed, delayed, and out-of-order jobs
    converge because every reconciliation re-reads current authoritative
    GitHub state.
    """
    with application_span(_SERVICE_TRACER_SCOPE, _DISPATCH_SPAN_NAME) as span:
        annotate_span(span, operation=_DISPATCH_SPAN_NAME)
        delivery = get_github_webhook_delivery(pool, delivery_guid=delivery_guid)
        if delivery is None:
            return GitHubWebhookDispatchResult(delivery=None, processed=False, route_outcomes=())
        if (
            delivery.classification is not GitHubWebhookDeliveryClassification.RELEVANT
            or delivery.routing_target is None
            or delivery.routing_resolution is not GitHubWebhookRoutingResolution.RESOLVED
        ):
            return _unexpected_delivery_shape(delivery)
        annotate_span(
            span,
            github_installation_id=(
                str(delivery.github_installation_id)
                if delivery.github_installation_id is not None
                else None
            ),
            github_repository=(
                str(delivery.github_repository_id)
                if delivery.github_repository_id is not None
                else None
            ),
            github_issue_number=delivery.github_issue_number,
            github_pull_request_number=delivery.github_pull_request_number,
        )
        routes = list_github_webhook_delivery_routes(pool, delivery_id=delivery.id)
        if not routes:
            return _unexpected_delivery_shape(delivery)
        route_outcomes = tuple(
            GitHubWebhookDispatchRouteOutcome(
                workspace_id=persisted_route.workspace_id,
                repository_id=persisted_route.repository_id,
                status=_dispatch_route(
                    pool,
                    github,
                    delivery=delivery,
                    route=GitHubWebhookResolvedRoute(
                        workspace_id=persisted_route.workspace_id,
                        repository_id=persisted_route.repository_id,
                    ),
                ),
            )
            for persisted_route in routes
        )
        processed = all(
            outcome.status
            in (
                GitHubWebhookDispatchRouteStatus.RECONCILED,
                GitHubWebhookDispatchRouteStatus.DECLINED,
            )
            for outcome in route_outcomes
        )
        if processed:
            mark_github_webhook_delivery_processed(pool, delivery_id=delivery.id)
        return GitHubWebhookDispatchResult(
            delivery=delivery, processed=processed, route_outcomes=route_outcomes
        )


def _unexpected_delivery_shape(
    delivery: GitHubWebhookDelivery,
) -> GitHubWebhookDispatchResult:
    """Classify an unexpected durable delivery shape (bounded, safe, recoverable).

    Submission never enqueues a non-relevant or unresolved delivery, so one
    reaching dispatch is an invariant condition: no reconciliation runs, and
    only the safe bounded classification is logged.
    """
    logger.warning(
        "github webhook dispatch observed an unexpected delivery shape "
        "(classification %s, resolution %s); the delivery remains recoverable",
        delivery.classification.value,
        delivery.routing_resolution.value if delivery.routing_resolution is not None else "none",
    )
    return GitHubWebhookDispatchResult(delivery=delivery, processed=False, route_outcomes=())


def _classify_route_failure(
    route: GitHubWebhookResolvedRoute, error: ApplicationError
) -> GitHubWebhookDispatchRouteStatus:
    """Map a typed application failure to the bounded route status, safely.

    Known failures (including authorization absence, which is the normalized
    integration condition — the invoked capabilities leave their mirrors
    byte-identical on it) and stale operations stay recoverable; uncertain
    outcomes stay explicitly uncertain and are never replayed blindly.
    """
    status = (
        GitHubWebhookDispatchRouteStatus.FAILED_UNCERTAIN
        if isinstance(error, ExternalOperationUncertainError)
        else GitHubWebhookDispatchRouteStatus.FAILED_KNOWN
    )
    logger.info(
        "github webhook dispatch route reconciliation did not complete (%s; "
        "workspace %s, repository %s); the delivery remains recoverable",
        status.value,
        route.workspace_id,
        route.repository_id,
    )
    return status


def _log_unexpected_route(
    route: GitHubWebhookResolvedRoute, error: Exception
) -> GitHubWebhookDispatchRouteStatus:
    """Contain an unexpected route failure with a safe type classification only.

    Arbitrary exception content may carry payload fragments or provider error
    bodies; only the exception type name is ever logged.
    """
    logger.warning(
        "github webhook dispatch route failed with an unexpected error of type %s "
        "(workspace %s, repository %s); the delivery remains recoverable",
        type(error).__name__,
        route.workspace_id,
        route.repository_id,
    )
    return GitHubWebhookDispatchRouteStatus.FAILED_UNEXPECTED


def _dispatch_route(
    pool: DatabasePool,
    github: GitHubAppClient,
    *,
    delivery: GitHubWebhookDelivery,
    route: GitHubWebhookResolvedRoute,
) -> GitHubWebhookDispatchRouteStatus:
    """Invoke the one authoritative capability the routing target names.

    The routing decision reads only the normalized semantic target recorded
    at intake — provider event names/actions never reach this layer.
    """
    target = delivery.routing_target
    if target is GitHubWebhookRoutingTarget.ISSUE_STATE:
        return _dispatch_issue_state(pool, github, delivery=delivery, route=route)
    if target is GitHubWebhookRoutingTarget.ISSUE_RELATIONS:
        return _dispatch_issue_relations(pool, github, route=route)
    if target is GitHubWebhookRoutingTarget.REPOSITORY_METADATA:
        return _dispatch_repository_metadata(pool, github, route=route)
    if target in (
        GitHubWebhookRoutingTarget.TASK_BRANCH_OR_PULL_REQUEST,
        GitHubWebhookRoutingTarget.PULL_REQUEST_STATE,
    ):
        return _dispatch_pull_request(pool, github, delivery=delivery, route=route)
    if target is GitHubWebhookRoutingTarget.CHECKS:
        return _dispatch_checks(pool, github, route=route)
    logger.warning(
        "github webhook dispatch observed an unknown routing target "
        "(workspace %s, repository %s); the delivery remains recoverable",
        route.workspace_id,
        route.repository_id,
    )
    return GitHubWebhookDispatchRouteStatus.FAILED_UNEXPECTED


def _dispatch_issue_state(
    pool: DatabasePool,
    github: GitHubAppClient,
    *,
    delivery: GitHubWebhookDelivery,
    route: GitHubWebhookResolvedRoute,
) -> GitHubWebhookDispatchRouteStatus:
    """Route an issue notification into the authoritative intake composition.

    ``intake_repository_task`` is the system-capable B3+B4 composition built
    for this layer: the fresh issue reconciliation, the fresh dependency
    observation, the eligibility check against those facts, the race-safe
    creation, and the non-gating hierarchy sync all read GitHub
    authoritatively. A closed issue, the typed blocked-issue eligibility
    outcome, or an absent Workspace subject is a typed expected decline (the
    fresh reconciliation and dependency mirror it performs were written
    inside the composition); a reconciliation/data-integrity conflict
    (stable identity or issue-number mismatch, a PR-shaped number, an
    unserializable write) is a known failure that stays recoverable; Task
    workflow consequences belong to Phase 2E.
    """
    if delivery.github_issue_number is None:
        logger.warning(
            "github webhook dispatch observed an issue-state delivery without "
            "an issue identity (workspace %s, repository %s); the delivery "
            "remains recoverable",
            route.workspace_id,
            route.repository_id,
        )
        return GitHubWebhookDispatchRouteStatus.FAILED_UNEXPECTED
    try:
        task_intake.intake_repository_task(
            pool,
            github,
            workspace_id=route.workspace_id,
            repository_id=route.repository_id,
            issue_number=delivery.github_issue_number,
        )
    except (NotFoundError, task_intake.IssueBlockedEligibilityError):
        # Typed expected product outcomes: a closed issue, the blocked-issue
        # eligibility outcome (the fresh reconciliation and dependency mirror
        # it performs were written inside the composition), or an absent
        # Workspace subject. Nothing remains outstanding. Generic
        # reconciliation conflicts (stable identity/number mismatches,
        # PR-shaped numbers, unserializable writes) do NOT decline here:
        # they stay known failures below and remain recoverable.
        return GitHubWebhookDispatchRouteStatus.DECLINED
    except ApplicationError as error:
        return _classify_route_failure(route, error)
    except Exception as error:
        return _log_unexpected_route(route, error)
    return GitHubWebhookDispatchRouteStatus.RECONCILED


def _dispatch_issue_relations(
    pool: DatabasePool,
    github: GitHubAppClient,
    *,
    route: GitHubWebhookResolvedRoute,
) -> GitHubWebhookDispatchRouteStatus:
    """Route a relation notification into authoritative B4 re-synchronization.

    ``sub_issues``/``issue_dependencies`` notifications carry no issue
    identity (their related issues may live in other repositories): the route
    re-synchronizes every tracked issue's authoritative relation state from
    its durable stable identity — fresh GitHub reads only, untracked issues
    have no subject, and a failure in any unit fails the route closed while
    leaving the prior mirror byte-identical.
    """
    try:
        projections = list_repository_github_issues(
            pool, workspace_id=route.workspace_id, repository_id=route.repository_id
        )
        for projection in projections:
            github_issue_relations.synchronize_repository_issue_dependencies(
                pool,
                github,
                workspace_id=route.workspace_id,
                repository_id=route.repository_id,
                issue_number=projection.issue_number,
                github_issue_id=projection.identity.github_issue_id,
            )
            github_issue_relations.synchronize_repository_issue_hierarchy(
                pool,
                github,
                workspace_id=route.workspace_id,
                repository_id=route.repository_id,
                issue_number=projection.issue_number,
                github_issue_id=projection.identity.github_issue_id,
            )
    except ApplicationError as error:
        return _classify_route_failure(route, error)
    except Exception as error:
        return _log_unexpected_route(route, error)
    return GitHubWebhookDispatchRouteStatus.RECONCILED


def _dispatch_repository_metadata(
    pool: DatabasePool,
    github: GitHubAppClient,
    *,
    route: GitHubWebhookResolvedRoute,
) -> GitHubWebhookDispatchRouteStatus:
    """Route a repository/access notification into authoritative re-observation."""
    try:
        github_reconciliation.reconcile_repository_observation(
            pool,
            github,
            workspace_id=route.workspace_id,
            repository_id=route.repository_id,
        )
    except ApplicationError as error:
        return _classify_route_failure(route, error)
    except Exception as error:
        return _log_unexpected_route(route, error)
    return GitHubWebhookDispatchRouteStatus.RECONCILED


def _dispatch_pull_request(
    pool: DatabasePool,
    github: GitHubAppClient,
    *,
    delivery: GitHubWebhookDelivery,
    route: GitHubWebhookResolvedRoute,
) -> GitHubWebhookDispatchRouteStatus:
    """Route a PR identity/head or PR-state notification into B7 reconciliation.

    The canonical record is addressed by the delivery's repository-local PR
    number; reconciliation binds to the record's stable ``github_pr_id`` and
    fails closed when the number reports a different stable identity. A
    number with no canonical record (an external/unrelated PR) is a typed
    expected decline; push notifications carry no PR identity and reconcile
    every canonical record in the routed repository.
    """
    try:
        if delivery.github_pull_request_number is not None:
            pull_request = find_task_pull_request_by_number(
                pool,
                workspace_id=route.workspace_id,
                repository_id=route.repository_id,
                github_pr_number=delivery.github_pull_request_number,
            )
            if pull_request is None:
                # An external/unrelated PR has no OpenOrc reconciliation
                # subject: a typed expected decline.
                return GitHubWebhookDispatchRouteStatus.DECLINED
            subjects: tuple[UUID, ...] = (pull_request.task_id,)
        else:
            subjects = tuple(
                record.task_id
                for record in list_task_pull_requests_for_repository(
                    pool, workspace_id=route.workspace_id, repository_id=route.repository_id
                )
            )
        for task_id in subjects:
            task_pull_request_reconciliation.reconcile_task_pull_request(
                pool,
                github,
                workspace_id=route.workspace_id,
                task_id=task_id,
            )
    except NotFoundError:
        # The canonical subject disappeared between the lookup and the
        # reconciliation: nothing to reconcile.
        return GitHubWebhookDispatchRouteStatus.DECLINED
    except ApplicationError as error:
        return _classify_route_failure(route, error)
    except Exception as error:
        return _log_unexpected_route(route, error)
    return GitHubWebhookDispatchRouteStatus.RECONCILED


def _dispatch_checks(
    pool: DatabasePool,
    github: GitHubAppClient,
    *,
    route: GitHubWebhookResolvedRoute,
) -> GitHubWebhookDispatchRouteStatus:
    """Route a check/status notification into the authoritative B7 projection."""
    try:
        for record in list_task_pull_requests_for_repository(
            pool, workspace_id=route.workspace_id, repository_id=route.repository_id
        ):
            commit_checks_projection.project_commit_checks(
                pool,
                github,
                workspace_id=route.workspace_id,
                task_id=record.task_id,
            )
    except ApplicationError as error:
        return _classify_route_failure(route, error)
    except Exception as error:
        return _log_unexpected_route(route, error)
    return GitHubWebhookDispatchRouteStatus.RECONCILED


def intake_and_dispatch_github_webhook(
    pool: DatabasePool,
    settings: Settings,
    submission: WebhookDispatchSubmission,
    *,
    raw_body: bytes,
    signature_header: str | None,
    event_name: str,
    delivery_guid: str,
) -> GitHubWebhookIntakeAndDispatch:
    """Compose #61 intake with #120 dispatch submission behind one service boundary.

    The thin FastAPI webhook route calls exactly this operation: intake
    verifies, classifies, durably records, and idempotently acknowledges the
    delivery; submission then decides — from durable state only — whether the
    accepted delivery is enqueued for reconciliation. The router owns no
    duplicate/relevance/processing semantics and no enqueue decision. Typed
    intake errors propagate unchanged to the transport mapping.
    """
    if not isinstance(delivery_guid, str) or not delivery_guid.strip():
        raise InvalidCommandError("the GitHub delivery identity header is missing or malformed")
    with application_span(_SERVICE_TRACER_SCOPE, _FACADE_SPAN_NAME) as span:
        annotate_span(span, operation=_FACADE_SPAN_NAME)
        intake = intake_github_webhook(
            pool,
            settings,
            raw_body=raw_body,
            signature_header=signature_header,
            event_name=event_name,
            delivery_guid=delivery_guid,
        )
        if intake.duplicate or intake.delivery is None:
            # A duplicate re-delivery was acknowledged idempotently by the
            # intake boundary; the original delivery's dispatch already
            # happened (or is recoverable) — no second submission.
            return GitHubWebhookIntakeAndDispatch(intake=intake, submission=None)
        submission_result = submit_github_webhook_dispatch(
            pool, submission, delivery_guid=intake.delivery.delivery_guid
        )
        return GitHubWebhookIntakeAndDispatch(intake=intake, submission=submission_result)
