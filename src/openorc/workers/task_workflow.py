"""F5a Task-workflow operation composition (issue #253).

The production handlers for the seven Task-workflow operation kinds and the
single application composition seam that owns their in-process sequencing.
Every handler invokes the existing E-series application services with
process-local dependencies — the handler never decides Task-state,
currentness, or authorization, and carries no workflow rules of its own.

Composition rules enforced by this module:

- Tightly coupled in-process sequences stay within one logical invocation
  exactly where an intermediate service-result DTO makes separate queued
  jobs unsound: the E1b→E2 bootstrap→initial-PLAN preparation handoff, the
  E3a→E3b dispatch→decision handoff (an unfinalized ``ReviewResult`` is not
  durable), and the PR remediation chain whose re-entered dispatch outcome
  embeds an unfinalized result. Interdependent service calls are never
  split across job boundaries, and no service-result DTO is ever serialized.
- Where the handoff is durably representable, a fresh logical invocation
  re-derives its exact subjects from committed state under the payload's
  exact ``state_token`` (the E2→E3a plan-production reload and the E8→E9
  published-subjects reload) instead of receiving a serialized DTO.
- Authorized continuations submit follow-up operations through the
  executing process's submission seam. A known refusal or uncertain
  submission never fails the already-completed operation, is never retried
  here, and is never converted into a known non-delivery — the workflow
  stays at its committed durable state, and later leaves own recovery.
  Queue IDs and submission outcomes are never Task truth.
- Gate presentations are ordinary authorized service continuations, never
  Owner approvals: ``open_implementation_authorization`` presents the
  exact-plan IMPLEMENTATION_AUTHORIZATION gate after an accepted planning
  review, and ``open_pr_authorization`` presents the exact-head
  PR_AUTHORIZATION gate after a finalized successful implementation. Both
  services' own contracts state that no Owner decision is required to
  present the gate; the explicit Owner approvals (E5/E7) and merge
  authorization (E10a) remain Owner commands outside this module.
- No operational retry, block, or Level 2 disposition logic lives here:
  this module provides the single narrow composition seam that the F5b
  coordinator (issue #262) wraps with typed failure handoff.
"""

from __future__ import annotations

import logging
from collections.abc import Callable, Iterator, Mapping
from contextlib import AbstractContextManager, contextmanager
from typing import Any, cast
from uuid import UUID

from openorc.adapters.agent_runtime.contract import AgentRuntimeAdapter
from openorc.adapters.github import HttpGitHubUserTokenClient
from openorc.config import ConfigurationError, Settings
from openorc.domain.connections import WorkflowRole
from openorc.domain.pending_interactions import PendingTaskRuntimeInteractionOperation
from openorc.domain.reviews import ReviewLoopPurpose, ReviewLoopStatus, ReviewOutcome
from openorc.domain.send_claims import (
    TaskRuntimeSendClaimKind,
    TaskRuntimeSendClaimStatus,
)
from openorc.domain.sessions import TaskAgentSession
from openorc.domain.tasks import TaskPlanningBootstrapStage, TaskStatus
from openorc.persistence import executions as execution_records
from openorc.persistence import reviews as review_records
from openorc.persistence import send_claims as send_claim_records
from openorc.persistence import tasks as task_records
from openorc.persistence.ownership import get_workspace
from openorc.persistence.pool import DatabasePool
from openorc.persistence.sessions import get_task_agent_session
from openorc.protocol.models import FormalResponse, PrResult
from openorc.services.cline_attachment import (
    ClineAttachmentPhase,
    attach_cline_runtime,
    settle_observed_detached_interrupt,
)
from openorc.services.cline_planning_bootstrap import ClineBootstrapRuntimeComposition
from openorc.services.errors import ConflictError, NotFoundError, StaleOperationError
from openorc.services.github_user_authorization import (
    GitHubUserAccessTokenResolver,
    ProfileUserAccessTokenResolver,
)
from openorc.services.operation_attempt_evidence import InvocationAttemptEvidence
from openorc.services.task_authorized_implementation_execution import (
    ProducerRuntimeAttachmentFactory,
    dispatch_authorized_implementation_detached,
)
from openorc.services.task_authorized_implementation_retry import (
    dispatch_authorized_implementation_retry_detached,
)
from openorc.services.task_authorized_merge_retry import (
    execute_authorized_merge_retry,
)
from openorc.services.task_authorized_pr_authorization_retry import (
    execute_authorized_pr_authorization_retry,
)
from openorc.services.task_completion_handoff import DetachedTurnCompletionHandoff
from openorc.services.task_implementation_authorization import (
    ImplementationAuthorization,
    open_implementation_authorization,
)
from openorc.services.task_owner_resume import (
    AuthorizedTaskResume,
    dispatch_authorized_task_resume,
)
from openorc.services.task_owner_reviewer_discussion import (
    admit_owner_reviewer_discussion_turn,
)
from openorc.services.task_plan_production import (
    TaskPlanProductionOutcome,
    dispatch_initial_plan_detached,
    dispatch_planning_revise_detached,
)
from openorc.services.task_planning_bootstrap import (
    ResumableBootstrapContinuation,
    TaskBootstrapRuntimeComposition,
    TaskPlanningBootstrapDispatchOutcome,
    TaskPlanningPreparationOutcome,
    _producer_snapshot_repository_base,
    complete_settled_bootstrap_prepare_transition,
    find_resumable_bootstrap_continuation,
    prepare_task_for_planning,
)
from openorc.services.task_planning_review_dispatch import (
    dispatch_planning_review_detached,
)
from openorc.services.task_pr_authorization import ApprovedPrAuthorization, open_pr_authorization
from openorc.services.task_pr_composition import (
    TaskPrPublished,
    dispatch_pr_compose_detached,
    publish_settled_task_pr_composition,
)
from openorc.services.task_pr_remediation import (
    dispatch_pr_remediation_detached,
    verify_settled_pr_remediation_push,
)
from openorc.services.task_pr_review_dispatch import (
    ReviewerRuntimeAttachmentFactory,
    dispatch_pr_review_detached,
)
from openorc.services.task_start import TaskStartOutcome, TaskStartStatus
from openorc.services.task_subject_guards import (
    require_canonical_task_pull_request,
    require_current_plan_revision,
    require_current_task,
)
from openorc.services.task_terminal_finalization import finalize_task_terminalization
from openorc.services.task_workflow_operation_coordinator import (
    coordinate_task_workflow_operation,
)
from openorc.workers.invocation import (
    InvalidInvocationError,
    OperationDependencies,
    OperationHandler,
    OperationInvocation,
    OperationKind,
    SubmissionOutcome,
    SubmissionResult,
    TaskAuthorizedImplementationPayload,
    TaskAuthorizedImplementationRetryPayload,
    TaskAuthorizedMergeRetryPayload,
    TaskOwnerReviewerDiscussionPayload,
    TaskPlanningBootstrapAndInitialPlanPayload,
    TaskPlanningReviewPayload,
    TaskPrAuthorizationRetryPayload,
    TaskPrCompositionPayload,
    TaskPrReviewPayload,
    TaskScope,
    TaskSessionResumePayload,
    WorkflowRoleRef,
)
from openorc.workers.task_cleanup_operations import (
    F1TerminalFinalizationSubmission,
    resolve_terminal_cleanup_dispatch,
)

__all__ = [
    "build_task_workflow_handlers",
    "execute_task_workflow_operation",
    "submit_authorized_implementation_after_approval",
    "submit_task_session_resume_after_authorization",
    "submit_planning_bootstrap_after_start",
    "submit_pr_composition_after_approval",
]

logger = logging.getLogger(__name__)

_TRACER_SCOPE = "openorc.workers.task_workflow"

# One per-kind composition callable: the in-process service sequencing a
# registered handler delegates to through the single seam. Each composition
# declares its own strictly typed payload; the construction-time
# kind/payload correspondence is re-proven by the handler's invocation.
TaskWorkflowComposition = Callable[..., None]


# --- process-local composition helpers --------------------------------------


def _require_external_session_id(session: TaskAgentSession, role_label: str) -> str:
    """Fail closed when a resolved binding carries no external session identity."""
    external_session_id = session.external_session_id
    if not external_session_id:
        raise ConflictError(
            f"the {role_label} session binding carries no external session identity"
        )
    return external_session_id


def _bound_session_external_id(
    pool: DatabasePool, *, workspace_id: UUID, task_id: UUID, role: WorkflowRole
) -> str:
    """Resolve the durable READY binding's external session identity.

    The BOUND attachment requires the exact external session identity; it is
    re-read and re-validated by the attachment composition and re-proven by
    the invoking service, so an interim lifecycle change fails closed there.
    """
    binding = get_task_agent_session(pool, task_id=task_id, role=role)
    if binding is None or binding.workspace_id != workspace_id:
        raise NotFoundError(
            "the requested task/role session binding is not available in this workspace"
        )
    return _require_external_session_id(binding, role.value)


def _producer_checkout_directory(pool: DatabasePool, *, workspace_id: UUID, task_id: UUID) -> str:
    """Derive the exact prepared Task checkout from the durable binding snapshot.

    The derivation rule is owned by the planning bootstrap; this composition
    reuses its fail-closed snapshot recovery so the workflow's Producer
    attachments address exactly the isolated checkout the bootstrap prepared
    (the original base's deterministic per-Task directory), never a
    substituted path.
    """
    binding = get_task_agent_session(pool, task_id=task_id, role=WorkflowRole.PRODUCER)
    if binding is None or binding.workspace_id != workspace_id:
        raise NotFoundError(
            "the requested producer session binding is not available in this workspace"
        )
    base = _producer_snapshot_repository_base(pool, binding, workspace_id=workspace_id)
    return f"{base}/{task_id}"


def _producer_runtime_attachment_factory(
    dependencies: OperationDependencies, *, workspace_id: UUID, task_id: UUID
) -> ProducerRuntimeAttachmentFactory:
    """Compose the production zero-arg BOUND Producer attachment factory.

    The deferred-attachment seam E6/E8 consume: the service decides WHEN the
    adapter is acquired (after its own admission transaction), and this
    composition owns HOW — the exact durable READY Producer binding at its
    prepared Task checkout.
    """
    pool = dependencies.database_pool()

    def factory() -> AbstractContextManager[AgentRuntimeAdapter]:

        @contextmanager
        def lease() -> Iterator[AgentRuntimeAdapter]:
            with attach_cline_runtime(
                pool,
                workspace_id=workspace_id,
                task_id=task_id,
                role=WorkflowRole.PRODUCER,
                phase=ClineAttachmentPhase.BOUND,
                external_session_id=_bound_session_external_id(
                    pool, workspace_id=workspace_id, task_id=task_id, role=WorkflowRole.PRODUCER
                ),
                working_directory=_producer_checkout_directory(
                    pool, workspace_id=workspace_id, task_id=task_id
                ),
            ) as attached:
                yield attached.adapter

        return lease()

    return factory


def _reviewer_runtime_attachment_factory(
    dependencies: OperationDependencies, *, workspace_id: UUID, task_id: UUID
) -> ReviewerRuntimeAttachmentFactory:
    """Compose the production session-addressed BOUND Reviewer factory (E9).

    The factory receives the service's resolved READY Reviewer binding and
    attaches its exact external session identity at the deployment's
    reviewer-neutral directory.
    """
    pool = dependencies.database_pool()
    neutral_directory = dependencies.reviewer_neutral_directory()

    def factory(session: TaskAgentSession) -> AbstractContextManager[AgentRuntimeAdapter]:

        @contextmanager
        def lease() -> Iterator[AgentRuntimeAdapter]:
            with attach_cline_runtime(
                pool,
                workspace_id=workspace_id,
                task_id=task_id,
                role=WorkflowRole.REVIEWER,
                phase=ClineAttachmentPhase.BOUND,
                external_session_id=_require_external_session_id(session, "reviewer"),
                working_directory=neutral_directory,
            ) as attached:
                yield attached.adapter

        return lease()

    return factory


# --- minimal durable reloads -------------------------------------------------


def _reload_plan_production_outcome(
    pool: DatabasePool, *, workspace_id: UUID, task_id: UUID, expected_state_token: UUID
) -> TaskPlanProductionOutcome:
    """Re-derive the exact current plan-production facts from committed state.

    The former in-memory E2→E3a handoff (:class:`TaskPlanProductionOutcome`)
    carries only durable facts — the persisted current PlanRevision and the
    post-update Task — so a fresh logical invocation reloads them instead of
    receiving a serialized DTO. The payload's exact ``state_token`` gates the
    reload: a Task that has moved on since submission is a stale operation.
    The executing dispatch service re-proves every subject under the Task
    row lock.
    """
    task = require_current_task(
        pool, workspace_id=workspace_id, task_id=task_id, expected_state_token=expected_state_token
    )
    if task.current_plan_revision_id is None:
        raise ConflictError(
            "the addressed task carries no current plan revision for review dispatch"
        )
    revision = require_current_plan_revision(
        pool, task=task, plan_revision_id=task.current_plan_revision_id
    )
    return TaskPlanProductionOutcome(plan_revision=revision, task=task)


def _reload_published_subjects(
    pool: DatabasePool,
    *,
    workspace_id: UUID,
    task_id: UUID,
    expected_state_token: UUID,
    task_pull_request_id: UUID,
) -> TaskPrPublished:
    """Re-derive the exact published-subject facts from committed state.

    The former in-memory E8→E9 handoff (:class:`TaskPrPublished`) carries
    only durable facts — the post-write Task and the one canonical
    ``TaskPullRequest`` — so a fresh logical invocation reloads them under
    the payload's exact token and canonical PR identity.
    """
    task = require_current_task(
        pool, workspace_id=workspace_id, task_id=task_id, expected_state_token=expected_state_token
    )
    pull_request = require_canonical_task_pull_request(pool, task=task)
    if pull_request.id != task_pull_request_id:
        raise StaleOperationError(
            "the addressed pull request record is not the task's canonical pull request; "
            "the operation is stale"
        )
    return TaskPrPublished(task=task, pull_request=pull_request)


# --- continuation submission -------------------------------------------------


def _submit_continuation(
    dependencies: OperationDependencies, invocation: OperationInvocation
) -> SubmissionResult:
    """Submit one authorized continuation for delivery in the executing process.

    Regular mode admits the follow-up into the serving process's own bounded
    executor; queue mode enqueues it. A known refusal or uncertain submission
    never fails the already-completed operation, is never retried here, and
    is never converted into a known non-delivery: the workflow stays at its
    committed durable state and later leaves own recovery. The submission
    outcome and any queue/job IDs are never Task truth.
    """
    submitter = dependencies.continuation_submitter()
    result = submitter.submit(invocation)
    if result.outcome is not SubmissionOutcome.ACCEPTED_FOR_DELIVERY:
        logger.warning(
            "operation continuation was not accepted for delivery (kind=%s, outcome=%s, reason=%s)",
            invocation.kind.value,
            result.outcome.value,
            result.refusal_reason.value if result.refusal_reason is not None else "none",
        )
    return result


# --- explicit workflow entry continuations -----------------------------------


def submit_planning_bootstrap_after_start(
    outcome: TaskStartOutcome,
    *,
    profile_id: UUID,
    dependencies: OperationDependencies,
) -> SubmissionResult | None:
    """Submit the planning bootstrap after one successful Task start (E1a to E1b).

    The explicitly invoked application continuation of the E1a start
    outcome: a ``PLANNING`` start carries the post-write Task with its
    rotated ``state_token`` and both admitted role bindings — the #203
    handoff — so this helper constructs the exact typed bootstrap invocation
    from that outcome and submits it for delivery. A ``QUEUED`` start has no
    continuation (``None``). The accountable Owner Profile is the starting
    caller's own identity, supplied explicitly and re-proven by the
    executing service. The returned submission outcome is transport
    information only, never Task truth: a known refusal or uncertain
    submission never fails the already-completed start and is never retried
    here.
    """
    if outcome.status is not TaskStartStatus.PLANNING:
        return None
    return _submit_continuation(
        dependencies,
        OperationInvocation(
            kind=OperationKind.TASK_PLANNING_BOOTSTRAP_AND_INITIAL_PLAN,
            payload=TaskPlanningBootstrapAndInitialPlanPayload(
                task=TaskScope(
                    workspace_id=str(outcome.task.workspace_id),
                    task_id=str(outcome.task.id),
                    role=WorkflowRoleRef.PRODUCER,
                ),
                profile_id=str(profile_id),
                expected_state_token=str(outcome.task.state_token),
            ),
        ),
    )


def submit_authorized_implementation_after_approval(
    authorization: ImplementationAuthorization,
    *,
    profile_id: UUID,
    dependencies: OperationDependencies,
) -> SubmissionResult:
    """Submit the authorized implementation after one E5 approval (E5 to E6).

    The explicitly invoked application continuation of the Owner's
    implementation approval: the ``ImplementationAuthorization`` carries the
    post-write ``implementing`` Task with its rotated ``state_token`` — the
    durable handoff E6 addresses — so this helper constructs the exact typed
    invocation from that outcome and submits it for delivery. The approved
    gate and its exact-plan subject are re-derived by the executing service
    and are never carried on the wire. The accountable Owner Profile is the
    approving caller's own identity, supplied explicitly and re-proven by
    the executing service. The returned submission outcome is transport
    information only, never Task truth.
    """
    return _submit_continuation(
        dependencies,
        OperationInvocation(
            kind=OperationKind.TASK_AUTHORIZED_IMPLEMENTATION,
            payload=TaskAuthorizedImplementationPayload(
                task=TaskScope(
                    workspace_id=str(authorization.task.workspace_id),
                    task_id=str(authorization.task.id),
                    role=WorkflowRoleRef.PRODUCER,
                ),
                profile_id=str(profile_id),
                expected_state_token=str(authorization.task.state_token),
            ),
        ),
    )


def submit_task_session_resume_after_authorization(
    authorized: AuthorizedTaskResume,
    *,
    dependencies: OperationDependencies,
) -> SubmissionResult:
    """Submit the authorized session-resume continuation after one Resume (F8g).

    The explicitly invoked application continuation of the Owner's Resume
    authorization: the ``AuthorizedTaskResume`` carries the restored Task
    with its rotated token and the committed one-shot session-resume record —
    the durable handoff the continuation addresses — so this helper
    constructs the exact typed invocation from that outcome and submits it
    for delivery. The record's routing context is re-derived by the executing
    continuation and is never carried on the wire. The returned submission
    outcome is transport information only, never Task truth: a known refusal
    or uncertain submission never fails the already-completed authorization
    and is never retried here.
    """
    return _submit_continuation(
        dependencies,
        OperationInvocation(
            kind=OperationKind.TASK_SESSION_RESUME,
            payload=TaskSessionResumePayload(
                task=TaskScope(
                    workspace_id=str(authorized.task.workspace_id),
                    task_id=str(authorized.task.id),
                    role=WorkflowRoleRef.PRODUCER,
                ),
                profile_id=str(authorized.resume.requested_by_profile_id),
                task_resume_intent_id=str(authorized.resume.id),
                restored_status=authorized.task.status.value,
                expected_state_token=str(authorized.task.state_token),
            ),
        ),
    )


def submit_pr_composition_after_approval(
    authorization: ApprovedPrAuthorization,
    *,
    profile_id: UUID,
    dependencies: OperationDependencies,
) -> SubmissionResult:
    """Submit PR composition/publish after one E7 approval (E7 to E8).

    The explicitly invoked application continuation of the Owner's
    publication approval: the ``ApprovedPrAuthorization`` carries the
    post-write Task with its rotated ``state_token`` and the exact approved
    ``PR_AUTHORIZATION`` gate, so this helper constructs the exact typed
    invocation from that outcome and submits it for delivery. The
    accountable Owner Profile is the approving caller's own identity,
    supplied explicitly and re-proven by the executing service. The returned
    submission outcome is transport information only, never Task truth.
    """
    return _submit_continuation(
        dependencies,
        OperationInvocation(
            kind=OperationKind.TASK_PR_COMPOSITION,
            payload=TaskPrCompositionPayload(
                task=TaskScope(
                    workspace_id=str(authorization.task.workspace_id),
                    task_id=str(authorization.task.id),
                    role=WorkflowRoleRef.PRODUCER,
                ),
                profile_id=str(profile_id),
                owner_gate_id=str(authorization.owner_gate.id),
                expected_state_token=str(authorization.task.state_token),
            ),
        ),
    )


# --- per-kind compositions ---------------------------------------------------


class _DetachedCompletionHandoff:
    """The ONE family-routed post-commit completion handoff (issue #333).

    An explicit narrow family route per settled operation family — never a
    plugin/callback framework. The initialization route is the #324
    readiness-resumed bootstrap submission; the initial-IMPLEMENT route
    presents the exact-head PR authorization gate through the existing F5f
    service under the F5b coordinator's typed failure handoff; the
    workspace-PREPARE route (issue #334) performs the settled bootstrap's
    same-session planning-context transition and arranges the next typed
    initial-plan invocation; the plan-result routes (issue #335) submit
    the existing typed planning REVIEW round from committed state after a
    settled initial-PLAN or REVISE result; the planning-REVIEW route
    (issue #336) owns the settled review decision's workflow continuation
    from committed state — the exact-plan IMPLEMENTATION_AUTHORIZATION
    presentation on acceptance and the detached Producer REVISE dispatch on
    an in-budget change request, with the exhausted REVIEW_RESOLUTION gate
    owning its own Owner-mediated pause; and the PR_COMPOSE route
    (issue #337) performs the settled composition's bounded publication
    through the existing owning service — the one place the settlement's
    in-memory validated ``pr_result`` is consumed, since the v1 persistence
    decision deliberately stores no title/body — and then submits the
    existing typed TASK_PR_REVIEW from committed state; the review-result
    route (issue #338) resolves the settled review decision's continuation
    by the reserved attempt's durable subject flavor — the planning
    continuation for a plan-subject iteration and the detached E9 remediation
    enqueue on the exact Task checkout for an in-budget PR/head change
    request — and the PR-remediation route re-proves the settled round,
    verifies the GitHub-confirmed new head, and submits the next typed
    TASK_PR_REVIEW through F1 from committed state; and the terminal-cleanup
    route (issue #345) re-invokes the ONE idempotent terminal finalization —
    the one-shot claim already settled atomically with the settled receipt,
    so the finalization proceeds archive-only to the mandatory atomic
    archival without ever redispatching cleanup. A settled family
    without a wired route is a fail-closed conflict that its own detached
    migration wires; no gate or workflow policy lives here beyond the
    routing and the committed-state re-derivation itself.
    """

    def __init__(self, dependencies: OperationDependencies) -> None:
        self._dependencies = dependencies

    def handoff_settled_completion(
        self,
        *,
        workspace_id: UUID,
        task_id: UUID,
        operation: PendingTaskRuntimeInteractionOperation,
        validated_result: FormalResponse | None = None,
    ) -> None:
        if validated_result is not None and (
            operation is not PendingTaskRuntimeInteractionOperation.PR_COMPOSE_DISPATCH
        ):
            # Fail-closed: only the PR_COMPOSE family consumes an in-memory
            # validated result (issue #337) — every other family re-derives
            # its continuation from committed durable state.
            raise ConflictError(
                "the settled operation family cannot carry an in-memory validated result"
            )
        if operation is PendingTaskRuntimeInteractionOperation.SESSION_INITIALIZATION:
            _handoff_initialization_completion(
                self._dependencies, workspace_id=workspace_id, task_id=task_id
            )
            return
        if operation is PendingTaskRuntimeInteractionOperation.IMPLEMENTATION_EXECUTION:
            _handoff_implementation_completion(
                self._dependencies, workspace_id=workspace_id, task_id=task_id
            )
            return
        if operation is PendingTaskRuntimeInteractionOperation.PLANNING_BOOTSTRAP_PREPARE:
            _handoff_bootstrap_prepare_completion(
                self._dependencies, workspace_id=workspace_id, task_id=task_id
            )
            return
        if operation in (
            PendingTaskRuntimeInteractionOperation.INITIAL_PLAN_DISPATCH,
            PendingTaskRuntimeInteractionOperation.PLANNING_REVISE_DISPATCH,
        ):
            _handoff_plan_result_completion(
                self._dependencies, workspace_id=workspace_id, task_id=task_id
            )
            return
        if operation is PendingTaskRuntimeInteractionOperation.REVIEWER_REVIEW:
            _handoff_review_result_completion(
                self._dependencies, workspace_id=workspace_id, task_id=task_id
            )
            return
        if operation is PendingTaskRuntimeInteractionOperation.PR_REMEDIATION_EXECUTION:
            _handoff_pr_remediation_completion(
                self._dependencies, workspace_id=workspace_id, task_id=task_id
            )
            return
        if operation is PendingTaskRuntimeInteractionOperation.PR_COMPOSE_DISPATCH:
            _handoff_pr_compose_completion(
                self._dependencies,
                workspace_id=workspace_id,
                task_id=task_id,
                validated_result=validated_result,
            )
            return
        if operation is PendingTaskRuntimeInteractionOperation.OWNER_REVIEWER_DISCUSSION:
            _handoff_discussion_completion(
                self._dependencies, workspace_id=workspace_id, task_id=task_id
            )
            return
        if operation is PendingTaskRuntimeInteractionOperation.TERMINAL_CLEANUP:
            _handoff_terminal_cleanup_completion(
                self._dependencies, workspace_id=workspace_id, task_id=task_id
            )
            return
        raise ConflictError(
            "the settled operation family has no detached completion handoff "
            "route yet; its own detached migration wires it"
        )

    def handoff_settled_interrupt(
        self,
        *,
        workspace_id: UUID,
        task_id: UUID,
        task_agent_session_id: UUID,
        external_session_id: str,
    ) -> None:
        """The #342 Interrupt-settlement route: contained best-effort.

        Re-derives the annotated unsettled Interrupt intent from committed
        durable state and, only when one correlates, releases the attached
        adapter's D6b guard discipline and records the intent's outcome
        through the shared E10b settlement machinery — strictly post-commit,
        with no database transaction across the adapter call. Never raises:
        a failed settlement leaves the durable annotated intent exactly as
        committed and the Owner-facing settlement branch remains the
        authority.
        """
        try:
            settle_observed_detached_interrupt(
                self._dependencies.database_pool(),
                workspace_id=workspace_id,
                task_id=task_id,
                task_agent_session_id=task_agent_session_id,
                external_session_id=external_session_id,
            )
        except Exception as error:  # noqa: BLE001 - contained settlement
            logger.warning(
                "the post-commit Interrupt settlement failed (%s) for task "
                "session %s; the durable annotated intent owns recovery",
                type(error).__name__,
                task_agent_session_id,
            )


def _handoff_terminal_cleanup_completion(
    dependencies: OperationDependencies, *, workspace_id: UUID, task_id: UUID
) -> None:
    """The TERMINAL_CLEANUP route (issue #345): the owning finalization.

    Re-invokes the ONE idempotent terminal finalization with the executing
    process's explicit cleanup dispatch selection. The settled completion
    already settled the one-shot claim atomically with its receipt — by the
    shared correlation seam's attributable routing or the acknowledgement
    attach's staged early completion — so the finalizer's re-derived claim
    classification proceeds archive-only: never a redispatch, never a second
    claim, never a re-run of the cleanup establishment. Its archival
    rechecks the one-shot window under the Task row lock; a concurrently
    pending window defers exactly as the #317 window guard always has.
    Contained by the handoff contract: a failed finalization leaves the
    durably settled attempt to a later explicitly invoked finalizer or the
    finite deadline disposition.
    """
    finalize_task_terminalization(
        dependencies.database_pool(),
        dependencies.terminal_cleanup_runtime(),
        workspace_id=workspace_id,
        task_id=task_id,
        dispatch=resolve_terminal_cleanup_dispatch(dependencies),
    )


def build_detached_completion_handoff(
    dependencies: OperationDependencies,
) -> DetachedTurnCompletionHandoff:
    """Build the executing process's ONE post-commit completion handoff (#333).

    The handoff owns the post-settlement resumption routing: it re-derives
    the next-step authority and inputs from committed state on every
    invocation and, only when the durable state proves it, submits typed F1
    follow-on work or invokes the existing owning service. A failed or
    uncertain follow-on submission is not success, is never retried here,
    and never permits a blind re-enqueue: the durable state owns recovery.
    Services never build invocation payloads — this worker-layer builder
    does. Routing outcomes are transport information only, never Task truth.
    """
    return _DetachedCompletionHandoff(dependencies)


def _handoff_initialization_completion(
    dependencies: OperationDependencies, *, workspace_id: UUID, task_id: UUID
) -> None:
    """The initialization route: the #324 readiness-resumed bootstrap submission.

    Re-derives the resumable classification from committed state and, only
    when the durable state proves it, submits the existing
    ``TASK_PLANNING_BOOTSTRAP_AND_INITIAL_PLAN`` continuation with the
    claim's rotated post-claim token through the executing process's
    submission seam.
    """
    resumable = find_resumable_bootstrap_continuation(
        dependencies.database_pool(),
        workspace_id=workspace_id,
        task_id=task_id,
    )
    if resumable is None:
        return
    _submit_readiness_resumed_bootstrap_continuation(dependencies, resumable)


def _handoff_implementation_completion(
    dependencies: OperationDependencies, *, workspace_id: UUID, task_id: UUID
) -> None:
    """The initial-IMPLEMENT route: the exact-head PR authorization presentation.

    Re-derives the presentation's authority from committed state — the fresh
    Task token seed and the durable Workspace Owner identity (the only
    Profile that can ever be authorized here) — and invokes the existing F5f
    service under the F5b coordinator's typed failure handoff, so a failing
    presentation triages exactly like the ordinary authorized continuation.
    The service's own Phase-A admission re-derives the qualifying successful
    Execution and every admission fact from durable state and refuses a
    moved-on subject with a typed error before any GitHub I/O; the handoff's
    callers contain it, and the durable state owns recovery. A successful
    run receipt is never treated as proof of the subsequent GitHub/gate
    effect: the presentation's own reconciliation is the only head source.
    """
    pool = dependencies.database_pool()
    task = task_records.get_task(pool, task_id=task_id)
    if task is None or task.workspace_id != workspace_id:
        return
    workspace = get_workspace(pool, workspace_id)
    if workspace is None:  # pragma: no cover - the Task's scope proved it
        return
    owner_profile_id = workspace.owner_profile_id
    expected_state_token = task.state_token

    def invoke_level_one(evidence: InvocationAttemptEvidence) -> None:
        open_pr_authorization(
            pool,
            dependencies.github_client(),
            profile_id=owner_profile_id,
            workspace_id=workspace_id,
            task_id=task_id,
            expected_state_token=expected_state_token,
            execution_id=None,
            attempt_evidence=evidence,
        )

    coordinate_task_workflow_operation(
        pool,
        expected_status=TaskStatus.IMPLEMENTING,
        workspace_id=workspace_id,
        task_id=task_id,
        profile_id=owner_profile_id,
        expected_state_token=expected_state_token,
        invoke_level_one=invoke_level_one,
    )


def _handoff_bootstrap_prepare_completion(
    dependencies: OperationDependencies, *, workspace_id: UUID, task_id: UUID
) -> None:
    """The workspace-PREPARE route: the planning-context transition continuation.

    Re-derives the continuation's authority from committed state — the
    settled ``prepared`` stage evidence is the only proof that the
    attributable workspace result validated and settled — and, only when
    the durable state proves it, invokes the owning bootstrap service's
    transition continuation under the F5b coordinator's typed failure
    handoff (the one same-session planning-context reconstruction into the
    exact Task-UUID checkout and the ``ready_for_planning`` settlement), so
    a positively detected reconstruction failure reports under the
    bootstrap claim's transition family exactly as the blocking path does —
    never an unreported failure stranded on a prepared stage. On success the
    route submits the next typed initial-plan invocation through the
    executing process's continuation seam with the fresh post-tail token.
    Every input is re-derived from committed durable state; a failed or
    uncertain routing or submission is transport information only, never
    Task truth, and never permits a blind re-enqueue — the durable state
    owns recovery.
    """
    pool = dependencies.database_pool()
    task = task_records.get_task(pool, task_id=task_id)
    if task is None or task.workspace_id != workspace_id:
        return
    if task.status is not TaskStatus.PLANNING or task.current_plan_revision_id is not None:
        return
    evidence = task_records.get_task_planning_bootstrap_evidence(pool, task_id=task_id)
    if evidence is None or evidence.stage is not TaskPlanningBootstrapStage.PREPARED:
        # The settled-and-untransitioned state is the only durable
        # continuation authority; anything else is already resolved or
        # owned by another path.
        return
    workspace = get_workspace(pool, workspace_id)
    if workspace is None:  # pragma: no cover - the Task's scope proved it
        return
    runtime_composition = dependencies.task_bootstrap_runtime_composition()
    expected_state_token = task.state_token

    def invoke_level_one(attempt_evidence: InvocationAttemptEvidence) -> None:
        complete_settled_bootstrap_prepare_transition(
            pool,
            runtime_composition,
            workspace_id=workspace_id,
            profile_id=workspace.owner_profile_id,
            task_id=task_id,
            expected_state_token=expected_state_token,
            attempt_evidence=attempt_evidence,
        )

    coordinate_task_workflow_operation(
        pool,
        expected_status=TaskStatus.PLANNING,
        workspace_id=workspace_id,
        task_id=task_id,
        profile_id=workspace.owner_profile_id,
        expected_state_token=expected_state_token,
        invoke_level_one=invoke_level_one,
    )
    _submit_continuation(
        dependencies,
        OperationInvocation(
            kind=OperationKind.TASK_PLANNING_BOOTSTRAP_AND_INITIAL_PLAN,
            payload=TaskPlanningBootstrapAndInitialPlanPayload(
                task=TaskScope(
                    workspace_id=str(task.workspace_id),
                    task_id=str(task.id),
                    role=WorkflowRoleRef.PRODUCER,
                ),
                profile_id=str(workspace.owner_profile_id),
                expected_state_token=str(task.state_token),
            ),
        ),
    )


def _handoff_plan_result_completion(
    dependencies: OperationDependencies, *, workspace_id: UUID, task_id: UUID
) -> None:
    """The plan-result routes: the next typed planning REVIEW round.

    Re-derives the continuation's authority from committed state — the
    Task's coarse planning state with its published current plan revision is
    the only proof that the attributable ``plan_result`` validated and
    settled (issue #335) — and, only when the durable state proves it,
    submits the existing typed ``TASK_PLANNING_REVIEW`` invocation through
    the executing process's continuation seam with the freshly reloaded
    post-revision token. This is exactly the submission the blocking
    compositions perform after a produced revision; every input is
    re-derived from committed durable state, a failed or uncertain submission
    is transport information only, never Task truth, and never permits a
    blind re-enqueue — the durable state owns recovery.
    """
    pool = dependencies.database_pool()
    task = task_records.get_task(pool, task_id=task_id)
    if task is None or task.workspace_id != workspace_id:
        return
    if task.status is not TaskStatus.PLANNING or task.current_plan_revision_id is None:
        # The settled plan revision is the only durable continuation
        # authority; anything else is already resolved or owned by another
        # path.
        return
    workspace = get_workspace(pool, workspace_id)
    if workspace is None:  # pragma: no cover - the Task's scope proved it
        return
    _submit_continuation(
        dependencies,
        OperationInvocation(
            kind=OperationKind.TASK_PLANNING_REVIEW,
            payload=TaskPlanningReviewPayload(
                task=TaskScope(
                    workspace_id=str(task.workspace_id),
                    task_id=str(task.id),
                    role=WorkflowRoleRef.REVIEWER,
                ),
                profile_id=str(workspace.owner_profile_id),
                expected_state_token=str(task.state_token),
            ),
        ),
    )


def _handoff_review_result_completion(
    dependencies: OperationDependencies, *, workspace_id: UUID, task_id: UUID
) -> None:
    """The planning-REVIEW route: the decision's workflow continuation.

    Re-derives the decision's durable continuation from committed state —
    never from a transient settlement object (issue #336). A Task already
    carrying a current owner gate is owned by that gate's own seam: the
    exhausted/sticky change request's REVIEW_RESOLUTION gate stands and its
    Owner-mediated resolution sends no Producer prompt. A still-``planning``
    Task with a published current plan revision routes on the OPEN planning
    loop's latest finalized iteration: an ACCEPTED review presents the
    exact-plan IMPLEMENTATION_AUTHORIZATION gate through the existing
    presentation service (never approved here), and an in-budget
    CHANGES_REQUESTED dispatches the new authorized Producer REVISE as one
    detached agent turn through the existing #335 service using the
    persisted iteration — after which the plan-result handoff route submits
    the next typed planning REVIEW from committed state. Every input is
    re-derived from committed durable state; a failed or uncertain
    continuation is transport information only, never Task truth, and never
    permits a blind re-enqueue — the durable state owns recovery.
    """
    pool = dependencies.database_pool()
    task = task_records.get_task(pool, task_id=task_id)
    if task is None or task.workspace_id != workspace_id:
        return
    if task.current_owner_gate_id is not None:
        # The exhausted/sticky change request's REVIEW_RESOLUTION gate (or
        # any other owner gate) owns the workflow: no Producer prompt is
        # sent and the Owner-mediated resolution is not a workflow operation.
        return
    if task.status is TaskStatus.REVIEWING:
        # The PR-review flavor (issue #338): the settled PR/head review
        # decision's in-budget change-request continuation from committed
        # state.
        _handoff_pr_change_request_remediation(
            dependencies, workspace_id=workspace_id, task_id=task_id
        )
        return
    if task.status is not TaskStatus.PLANNING or task.current_plan_revision_id is None:
        # The settled decision's own durable continuations are the only
        # authority; anything else is already resolved or owned by another
        # path.
        return
    loops = [
        loop
        for loop in review_records.list_task_review_loops(pool, task_id=task.id)
        if loop.workspace_id == workspace_id and loop.purpose is ReviewLoopPurpose.PLANNING
    ]
    if len(loops) != 1:
        return
    loop = loops[0]
    if loop.status is not ReviewLoopStatus.OPEN:
        return
    iterations = review_records.list_review_loop_iterations(pool, review_loop_id=loop.id)
    if not iterations or iterations[-1].outcome is None:
        return
    latest = iterations[-1]
    if latest.plan_revision_id != task.current_plan_revision_id:
        return
    workspace = get_workspace(pool, workspace_id)
    if workspace is None:  # pragma: no cover - the Task's scope proved it
        return
    owner_profile_id = workspace.owner_profile_id
    expected_state_token = task.state_token
    prior_revision_id = task.current_plan_revision_id

    if latest.outcome is ReviewOutcome.ACCEPTED:
        # The ordinary authorized continuation of accepted formal review
        # (issue #336): present the exact-plan IMPLEMENTATION_AUTHORIZATION
        # gate under the F5b coordinator's typed failure handoff — the
        # presentation's own durable attribution identity (the finalized
        # ACCEPTED iteration, recorded by the service before its external
        # source-requirements reconciliation) reaches the Level 2 triage, so
        # a detected presentation failure receives its durable escalation
        # disposition instead of stranding the accepted review. No Owner
        # decision is required to present it, and approval never follows
        # from Reviewer acceptance — the explicit E5 approval remains an
        # Owner command outside this module. The shared source-requirements
        # pre-check (issue #289) freshly verifies the authoritative issue
        # before the gate is presented; a confirmed requirements change
        # blocks the Task through the shared atomic drift-block path.
        def invoke_level_one(evidence: InvocationAttemptEvidence) -> None:
            open_implementation_authorization(
                pool,
                dependencies.github_client(),
                profile_id=owner_profile_id,
                workspace_id=workspace_id,
                task_id=task_id,
                expected_state_token=expected_state_token,
                attempt_evidence=evidence,
            )

        coordinate_task_workflow_operation(
            pool,
            expected_status=TaskStatus.PLANNING,
            workspace_id=workspace_id,
            task_id=task_id,
            profile_id=owner_profile_id,
            expected_state_token=expected_state_token,
            invoke_level_one=invoke_level_one,
        )
        return

    def invoke_level_one(evidence: InvocationAttemptEvidence) -> None:
        dispatch_planning_revise_detached(
            pool,
            dependencies.github_client(),
            workspace_id=workspace_id,
            task_id=task_id,
            profile_id=owner_profile_id,
            expected_state_token=expected_state_token,
            prior_plan_revision_id=prior_revision_id,
            review_iteration_id=latest.id,
            producer_runtime_factory=_producer_runtime_attachment_factory(
                dependencies, workspace_id=workspace_id, task_id=task_id
            ),
            observation_registrar=dependencies.detached_turn_observer(),
            completion_handoff=build_detached_completion_handoff(dependencies),
            attempt_evidence=evidence,
        )

    coordinate_task_workflow_operation(
        pool,
        expected_status=TaskStatus.PLANNING,
        workspace_id=workspace_id,
        task_id=task_id,
        profile_id=owner_profile_id,
        expected_state_token=expected_state_token,
        invoke_level_one=invoke_level_one,
    )


def _handoff_discussion_completion(
    dependencies: OperationDependencies, *, workspace_id: UUID, task_id: UUID
) -> None:
    """The discussion route: the settled exchange's workflow continuations.

    The discussion settlement's formal continuations commit the durable
    decision (the new finalized iteration, the retired or settled gate) and
    no Producer work; the ONE post-commit handoff re-derives every next-step
    authority from committed state (issue #339) through the same committed
    state derivations the ordinary review decisions use: a Task whose
    workflow resumed to ``planning`` presents the exact-plan
    IMPLEMENTATION_AUTHORIZATION on an accepted review or dispatches the new
    authorized Producer REVISE as one detached turn; a Task back in
    ``reviewing`` dispatches the in-budget PR remediation. A Task still
    carrying a current OwnerGate is owned by that gate's own seam and sends
    no Producer prompt.
    """
    _handoff_review_result_completion(dependencies, workspace_id=workspace_id, task_id=task_id)


def _handoff_pr_compose_completion(
    dependencies: OperationDependencies,
    *,
    workspace_id: UUID,
    task_id: UUID,
    validated_result: FormalResponse | None,
) -> None:
    """The PR_COMPOSE route: the settled composition's bounded publication.

    The settled PR_COMPOSE claim (exactly one ``settled`` claim — its
    validated ``pr_result`` was the only settlement authority, issue #337)
    is the durable proof that the attributable composition turn completed;
    the validated ``PrResult`` arrives in memory from the same settlement
    invocation (the v1 persistence decision deliberately stores no
    title/body, so the publication cannot be re-derived). The route
    re-derives every authority from committed state — the exact current
    ``waiting_for_owner`` Task (the claim and receipt settlements never
    rotated its token), no current OwnerGate, a bound canonical branch, no
    canonical record — and invokes the existing owning service's bounded
    publication continuation in this same completion-processing invocation,
    with no database transaction across the GitHub I/O. On the finalized
    publication it submits the existing typed ``TASK_PR_REVIEW`` invocation
    through the executing process's continuation seam with the published
    token, the canonical PR identity, and the re-derived approving Profile —
    exactly the submission the blocking compositions perform after a
    published PR. Every input is re-derived from committed durable state
    except the in-memory presentation; a failed or uncertain continuation
    is transport information only, never Task truth, and never permits a
    blind re-enqueue — the durable state owns recovery.
    """
    if not isinstance(validated_result, PrResult):
        # Fail-closed: the PR route's publication cannot run without the
        # settlement's validated presentation, and no title/body is ever
        # invented or re-derived.
        raise ConflictError(
            "the settled pr composition carries no in-memory validated "
            "pr_result; the publication continuation is not available"
        )
    pool = dependencies.database_pool()
    task = task_records.get_task(pool, task_id=task_id)
    if task is None or task.workspace_id != workspace_id:
        return
    if (
        task.status is not TaskStatus.WAITING_FOR_OWNER
        or task.current_owner_gate_id is not None
        or task.canonical_feature_branch is None
    ):
        # The settled composition's own durable continuations are the only
        # authority; anything else is already resolved or owned by another
        # path.
        return
    settled_claims = [
        claim
        for claim in send_claim_records.list_task_runtime_send_claims(pool, task_id=task_id)
        if claim.kind is TaskRuntimeSendClaimKind.PR_COMPOSE_DISPATCH
        and claim.status is TaskRuntimeSendClaimStatus.SETTLED
    ]
    if not settled_claims:
        return
    if len(settled_claims) > 1:
        # More than one settled PR_COMPOSE claim for one Task is a durable
        # invariant violation (the one-shot claim's exactly-once backstop):
        # fail closed, never guess.
        raise ConflictError(
            "the task carries multiple settled pr compose claims; the publication "
            "continuation cannot choose a subject"
        )
    settled_claim = settled_claims[0]
    owner_gate_id = settled_claim.owner_gate_id
    if owner_gate_id is None:  # pragma: no cover - the kind's subject form
        raise ConflictError("the settled pr compose claim carries no owner-gate subject")
    github = dependencies.github_client()
    user_token_resolver = dependencies.user_token_resolver()
    published = publish_settled_task_pr_composition(
        pool,
        github,
        user_token_resolver,
        workspace_id=workspace_id,
        task_id=task_id,
        owner_gate_id=owner_gate_id,
        expected_state_token=task.state_token,
        pr_result=validated_result,
        terminal_finalization_submission=F1TerminalFinalizationSubmission(
            dependencies.continuation_submitter()
        ),
    )
    _submit_continuation(
        dependencies,
        OperationInvocation(
            kind=OperationKind.TASK_PR_REVIEW,
            payload=TaskPrReviewPayload(
                task=TaskScope(
                    workspace_id=str(task.workspace_id),
                    task_id=str(task.id),
                    role=WorkflowRoleRef.REVIEWER,
                ),
                profile_id=str(published.approving_profile_id),
                task_pull_request_id=str(published.pull_request.id),
                expected_state_token=str(published.task.state_token),
            ),
        ),
    )


def _handoff_pr_change_request_remediation(
    dependencies: OperationDependencies, *, workspace_id: UUID, task_id: UUID
) -> None:
    """The PR-review route's in-budget change-request continuation (issue #338).

    Re-derives the durable remediation authority from committed state — the
    REVIEWING Task with no current OwnerGate, the single OPEN PR loop whose
    latest finalized PR/head iteration is exactly one in-budget
    ``CHANGES_REQUESTED`` result with no claimed remediation Execution yet —
    and, only when the durable state proves it, dispatches the new
    authorized E9 remediation as one detached agent turn on the exact Task
    checkout through the existing E9 service under the F5b coordinator's
    typed failure handoff. The ACCEPTED MERGE_DECISION and the exhausted or
    sticky REVIEW_RESOLUTION gates own their Owner seams with no Producer
    prompt; a failed or uncertain dispatch is transport information only,
    never Task truth, and never permits a blind re-enqueue — the durable
    state owns recovery.
    """
    pool = dependencies.database_pool()
    task = task_records.get_task(pool, task_id=task_id)
    if task is None or task.workspace_id != workspace_id:
        return
    if task.status is not TaskStatus.REVIEWING or task.current_owner_gate_id is not None:
        return
    loops = [
        loop
        for loop in review_records.list_task_review_loops(pool, task_id=task.id)
        if loop.workspace_id == workspace_id and loop.purpose is ReviewLoopPurpose.PR_REVIEW
    ]
    if len(loops) != 1 or loops[0].status is not ReviewLoopStatus.OPEN:
        return
    iterations = review_records.list_review_loop_iterations(pool, review_loop_id=loops[0].id)
    if not iterations:
        return
    latest = iterations[-1]
    if latest.outcome is not ReviewOutcome.CHANGES_REQUESTED or latest.task_pull_request_id is None:
        # The accepted merge decision and the exhausted/sticky resolution
        # gates own their Owner seams; a plan-subject attempt is never a PR
        # continuation.
        return
    claimed = [
        execution
        for execution in execution_records.list_task_executions(pool, task_id=task.id)
        if execution.review_iteration_id == latest.id
    ]
    if claimed:
        # The one-shot E9 claim association: a claimed iteration's
        # remediation is already owned by its own durable attempt.
        return
    workspace = get_workspace(pool, workspace_id)
    if workspace is None:  # pragma: no cover - the Task's scope proved it
        return
    owner_profile_id = workspace.owner_profile_id
    expected_state_token = task.state_token

    def invoke_level_one(evidence: InvocationAttemptEvidence) -> None:
        dispatch_pr_remediation_detached(
            pool,
            profile_id=owner_profile_id,
            workspace_id=workspace_id,
            task_id=task_id,
            expected_state_token=expected_state_token,
            addressed_iteration=latest,
            producer_runtime_factory=_producer_runtime_attachment_factory(
                dependencies, workspace_id=workspace_id, task_id=task_id
            ),
            observation_registrar=dependencies.detached_turn_observer(),
            completion_handoff=build_detached_completion_handoff(dependencies),
            attempt_evidence=evidence,
        )

    coordinate_task_workflow_operation(
        pool,
        expected_status=TaskStatus.REVIEWING,
        workspace_id=workspace_id,
        task_id=task_id,
        profile_id=owner_profile_id,
        expected_state_token=expected_state_token,
        invoke_level_one=invoke_level_one,
    )


def _handoff_pr_remediation_completion(
    dependencies: OperationDependencies, *, workspace_id: UUID, task_id: UUID
) -> None:
    """The E9-remediation route: the settled round's verified continuation.

    Re-derives the continuation's authority from committed state — the
    settled SUCCEEDED remediation Execution bound to the latest finalized
    CHANGES_REQUESTED iteration, its recorded branch claim, and the canonical
    PR — and invokes the existing owning verification directly. The
    verification is a direct control guard, not a Level 1 operation: a
    succeeded Execution is never a Level 2 failure identity (the
    execution-family contracts deliberately reject succeeded attempts), so a
    positively unverifiable round or a failed/uncertain reconciliation read
    installs the exact atomic Level 3 BLOCK_RESOLUTION block through the
    shared exceptional-blocking foundation under the escalation reason
    vocabulary instead of stranding the reviewing Task, and a GitHub-confirmed
    terminal pull-request state routes through the existing authoritative
    terminal-reconciliation ownership — the trusted #229 cancellation
    entrypoint for a confirmed closed-unmerged record, with this handoff's
    F1-backed terminal-finalization submission forwarded so the normal bounded
    checkout-cleanup arrangement survives, and zero durable mutation for a
    confirmed merge — instead of a competing recoverable block.
    Only the GitHub-confirmed NEW head submits the existing typed
    ``TASK_PR_REVIEW`` through the executing process's continuation seam with
    the reloaded post-reconciliation token; a ``None`` verification owns no
    continuation. Every input is re-derived from committed durable state; a
    failed or uncertain submission is transport information only, never Task
    truth, and never permits a blind re-enqueue — the durable state owns
    recovery.
    """
    pool = dependencies.database_pool()
    task = task_records.get_task(pool, task_id=task_id)
    if task is None or task.workspace_id != workspace_id:
        return
    if task.status is not TaskStatus.REVIEWING or task.current_owner_gate_id is not None:
        # The settled remediation's own durable continuations are the only
        # authority; anything else is already resolved or owned by another
        # path (a gate, a decision, or the Owner Resume routing).
        return
    workspace = get_workspace(pool, workspace_id)
    if workspace is None:  # pragma: no cover - the Task's scope proved it
        return
    owner_profile_id = workspace.owner_profile_id
    continuation = verify_settled_pr_remediation_push(
        pool,
        dependencies.github_client(),
        profile_id=owner_profile_id,
        workspace_id=workspace_id,
        task_id=task_id,
        expected_state_token=task.state_token,
        terminal_finalization_submission=F1TerminalFinalizationSubmission(
            dependencies.continuation_submitter()
        ),
    )
    if continuation is None:
        return
    _submit_continuation(
        dependencies,
        OperationInvocation(
            kind=OperationKind.TASK_PR_REVIEW,
            payload=TaskPrReviewPayload(
                task=TaskScope(
                    workspace_id=str(continuation.task.workspace_id),
                    task_id=str(continuation.task.id),
                    role=WorkflowRoleRef.REVIEWER,
                ),
                profile_id=str(owner_profile_id),
                task_pull_request_id=str(continuation.pull_request.id),
                expected_state_token=str(continuation.task.state_token),
            ),
        ),
    )


def _submit_readiness_resumed_bootstrap_continuation(
    dependencies: OperationDependencies,
    resumable: ResumableBootstrapContinuation,
) -> SubmissionResult:
    """Submit the readiness-resumed planning bootstrap continuation."""
    task = resumable.task
    return _submit_continuation(
        dependencies,
        OperationInvocation(
            kind=OperationKind.TASK_PLANNING_BOOTSTRAP_AND_INITIAL_PLAN,
            payload=TaskPlanningBootstrapAndInitialPlanPayload(
                task=TaskScope(
                    workspace_id=str(task.workspace_id),
                    task_id=str(task.id),
                    role=WorkflowRoleRef.PRODUCER,
                ),
                profile_id=str(resumable.owner_profile_id),
                expected_state_token=str(task.state_token),
            ),
        ),
    )


def _compose_task_planning_bootstrap_and_initial_plan(
    payload: TaskPlanningBootstrapAndInitialPlanPayload,
    *,
    dependencies: OperationDependencies,
    attempt_evidence: InvocationAttemptEvidence | None = None,
) -> None:
    """Run the planning bootstrap and produce the initial PLAN in one process.

    The E1b preparation and the E2 initial plan production are one logical
    invocation: the preparation outcome's process-local checkout facts make
    a separate queued E2 unsound, so the coupled sequence executes in-process
    and the payload's exact post-start token authorizes the whole operation.
    On success the exact post-E2 token authorizes the first review round.

    The mandatory initialization is dispatched as detached agent turns
    (issue #324): the preparation returns the dispatched suspension, this
    composition returns without waiting for ``session_ready``, and the
    observed completions settle readiness and resume the bootstrap through
    the post-commit continuation trigger. The initial PLAN is itself
    dispatched as one detached agent turn (issue #335): the composition
    returns after the durable admission — no ``send`` and no blocking wait —
    and the independently observed completion settles the validated plan
    result, with the ONE post-commit completion handoff submitting the typed
    planning REVIEW from committed state (issue #347: both execution modes
    run this same detached composition; queue mode resolves the remote
    registrar proxy over the #346 registration handshake).
    """
    pool = dependencies.database_pool()
    github = dependencies.github_client()
    runtime_composition = dependencies.task_bootstrap_runtime_composition()
    reviewer_neutral_directory = dependencies.reviewer_neutral_directory()
    workspace_id = UUID(payload.task.workspace_id)
    task_id = UUID(payload.task.task_id)
    profile_id = UUID(payload.profile_id)
    expected_state_token = UUID(payload.expected_state_token)

    # The detached establishment needs the executing process's detached-turn
    # observation registration seam — a process dependency in both execution
    # modes (issues #323/#346): regular mode resolves the serving process's
    # own observation host; queue mode resolves the remote registrar proxy
    # over the private authenticated registration handshake.
    observation_registrar = dependencies.detached_turn_observer()

    preparation = prepare_task_for_planning(
        pool,
        runtime_composition,
        workspace_id=workspace_id,
        task_id=task_id,
        profile_id=profile_id,
        expected_state_token=expected_state_token,
        reviewer_neutral_directory=reviewer_neutral_directory,
        attempt_evidence=attempt_evidence,
        observation_registrar=observation_registrar,
        completion_handoff=build_detached_completion_handoff(dependencies),
    )
    if isinstance(preparation, TaskPlanningBootstrapDispatchOutcome):
        # The detached establishment is durably dispatched: the worker
        # returns without waiting for ``session_ready``. The observed
        # completions own the resumption; submission outcomes are transport
        # information only and the durable resumable state owns recovery.
        return
    assert isinstance(preparation, TaskPlanningPreparationOutcome)
    # The initial PLAN is dispatched as one detached agent turn (issue
    # #335): the composition returns after the durable admission — no
    # ``send`` and no blocking wait — and the independently observed
    # completion settles the validated plan result, with the ONE post-commit
    # completion handoff submitting the typed planning REVIEW from committed
    # state.
    dispatch_initial_plan_detached(
        pool,
        github,
        workspace_id=workspace_id,
        task_id=task_id,
        profile_id=profile_id,
        preparation=preparation,
        producer_runtime_factory=_producer_runtime_attachment_factory(
            dependencies, workspace_id=workspace_id, task_id=task_id
        ),
        observation_registrar=observation_registrar,
        completion_handoff=build_detached_completion_handoff(dependencies),
        attempt_evidence=attempt_evidence,
    )
    # The detached dispatch is durably admitted: the worker returns without
    # waiting for the plan result. The observed completion owns the
    # settlement; submission outcomes are transport information only and the
    # durable state owns recovery.


def _compose_task_planning_review(
    payload: TaskPlanningReviewPayload,
    *,
    dependencies: OperationDependencies,
    attempt_evidence: InvocationAttemptEvidence | None = None,
) -> None:
    """Run one planning review round.

    The REVIEW dispatch is itself one detached agent turn (issue #336): the
    composition re-derives the round's exact plan-production subject from
    committed state under the payload's exact token, reserves exactly one
    next-numbered unfinalized ReviewIteration for that exact plan (the OPEN
    planning loop established on first review with the Workspace's
    snapshotted budget), commits the iteration's detached-turn receipt
    companion, and admits the composed canonical REVIEW message verbatim on
    the exact READY Reviewer session before returning. No ``send`` and no
    blocking wait exists here; the independently observed completion settles
    the validated ``review_result`` through the owning E3b decision
    settlement, and the ONE post-commit completion handoff owns the workflow
    continuation from committed state (the exact-plan
    IMPLEMENTATION_AUTHORIZATION presentation on acceptance, the detached
    Producer REVISE dispatch on an in-budget change request, and nothing on
    exhaustion — the REVIEW_RESOLUTION gate already stands). A stale,
    duplicate, or late REVIEW observation can never decide twice (issue
    #347: both execution modes run this same detached composition).
    """
    pool = dependencies.database_pool()
    workspace_id = UUID(payload.task.workspace_id)
    task_id = UUID(payload.task.task_id)
    profile_id = UUID(payload.profile_id)
    expected_state_token = UUID(payload.expected_state_token)

    production = _reload_plan_production_outcome(
        pool, workspace_id=workspace_id, task_id=task_id, expected_state_token=expected_state_token
    )
    # No Reviewer attachment is acquired in this composition and no decision
    # runs in-process — the reserved ReviewIteration, its detached-turn
    # receipt companion, and the verbatim admission commit here, and the
    # independently observed completion owns the decision settlement with
    # the ONE post-commit completion handoff owning the workflow continuation
    # from committed state.
    dispatch_planning_review_detached(
        pool,
        _reviewer_runtime_attachment_factory(
            dependencies, workspace_id=workspace_id, task_id=task_id
        ),
        profile_id=profile_id,
        workspace_id=workspace_id,
        task_id=task_id,
        plan_production=production,
        observation_registrar=dependencies.detached_turn_observer(),
        completion_handoff=build_detached_completion_handoff(dependencies),
        attempt_evidence=attempt_evidence,
    )
    # The detached dispatch is durably admitted: the worker returns without
    # waiting for the review result. The observed completion owns the
    # settlement; submission outcomes are transport information only and the
    # durable state owns recovery.


def _compose_task_authorized_implementation(
    payload: TaskAuthorizedImplementationPayload,
    *,
    dependencies: OperationDependencies,
    attempt_evidence: InvocationAttemptEvidence | None = None,
) -> None:
    """Execute the E5-authorized initial implementation and present the PR gate.

    The E6 service re-derives the exact approved gate/plan handoff from
    durable state (never caller-supplied) and reserves exactly one initial
    implementation attempt.

    The implementation is dispatched as one detached agent turn (issues
    #325/#333): the composition returns after the durable admission — no
    ``send`` and no blocking wait — and the independently observed completion
    settles the attempt through the shared finalize, with the ONE post-commit
    completion handoff presenting the exact-head PR_AUTHORIZATION gate from
    committed state — no Owner decision is required to present it, the
    service's existing GitHub authority checks (including the direct
    ``github_source_changed`` block) pass through unchanged, and the explicit
    E7 approval remains an Owner command outside this module (issue #347:
    both execution modes run this same detached composition).
    """
    pool = dependencies.database_pool()
    workspace_id = UUID(payload.task.workspace_id)
    task_id = UUID(payload.task.task_id)
    profile_id = UUID(payload.profile_id)
    expected_state_token = UUID(payload.expected_state_token)

    # The detached implementation dispatch needs the executing process's
    # detached-turn observation seam and the ONE post-commit completion
    # handoff — process dependencies in both execution modes (issues
    # #323/#333/#346).
    dispatch_authorized_implementation_detached(
        pool,
        profile_id=profile_id,
        workspace_id=workspace_id,
        task_id=task_id,
        expected_state_token=expected_state_token,
        producer_runtime_factory=_producer_runtime_attachment_factory(
            dependencies, workspace_id=workspace_id, task_id=task_id
        ),
        observation_registrar=dependencies.detached_turn_observer(),
        completion_handoff=build_detached_completion_handoff(dependencies),
        attempt_evidence=attempt_evidence,
    )
    # The detached dispatch is durably admitted: the worker returns without
    # waiting for the implementation result. The observed completion owns
    # the settlement; submission outcomes are transport information only and
    # the durable state owns recovery.


def _compose_task_authorized_implementation_retry(
    payload: TaskAuthorizedImplementationRetryPayload,
    *,
    dependencies: OperationDependencies,
    attempt_evidence: InvocationAttemptEvidence | None = None,
) -> None:
    """Execute the one F4-authorized implementation retry and present the PR gate.

    The F5c retry service atomically consumes the exact durably authorized
    F4 attempt for one new Execution (reserved, claimed, audited, and bound
    set-once before any external effect), then reuses the shared E6 external
    mechanics on the original Producer session. The authorized retry is
    dispatched as one detached agent turn (issues #325/#340) through the
    shared F5c/E6 detached external phase and the composition returns after
    the durable admission; the independently observed completion settles the
    NEW retry Execution and the completion handoff owns the exact-head
    PR_AUTHORIZATION presentation. F12 (issue #316) owns submitting due
    authorized retries through this registered operation in either mode
    (issue #347: both execution modes run this same detached composition).

    On the successful finalized execution, the ordinary authorized
    continuation presents the exact-head PR_AUTHORIZATION gate with the
    exact post-execution token and the NEW Execution identity — the same
    application path as the initial composition, with no new Owner approval
    or recovery policy here. A failed retry is reported to F4 by the F5b
    coordinator's existing durable binding lookup, never by this module.
    """
    pool = dependencies.database_pool()
    workspace_id = UUID(payload.task.workspace_id)
    task_id = UUID(payload.task.task_id)
    profile_id = UUID(payload.profile_id)
    expected_state_token = UUID(payload.expected_state_token)
    # The detached retry dispatch needs the executing process's detached-turn
    # observation seam and the ONE post-commit completion handoff — process
    # dependencies in both execution modes (issues #323/#333/#340/#346).
    dispatch_authorized_implementation_retry_detached(
        pool,
        profile_id=profile_id,
        workspace_id=workspace_id,
        task_id=task_id,
        expected_state_token=expected_state_token,
        retry_attempt_id=UUID(payload.retry_attempt_id),
        producer_runtime_factory=_producer_runtime_attachment_factory(
            dependencies, workspace_id=workspace_id, task_id=task_id
        ),
        observation_registrar=dependencies.detached_turn_observer(),
        completion_handoff=build_detached_completion_handoff(dependencies),
        attempt_evidence=attempt_evidence,
    )
    # The detached dispatch is durably admitted: the worker returns without
    # waiting for the implementation result. The observed completion owns
    # the settlement; submission outcomes are transport information only and
    # the durable state owns recovery.


def _compose_task_authorized_merge_retry(
    payload: TaskAuthorizedMergeRetryPayload,
    *,
    dependencies: OperationDependencies,
    attempt_evidence: InvocationAttemptEvidence | None = None,
) -> None:
    """Execute the one F4-authorized exact-head merge retry (F5d).

    The merge-retry service atomically consumes the exact durably authorized
    F4 attempt before its single outbound merge, settles every pre-send and
    post-send condition truthfully through the existing E10a semantics or the
    guarded F3/F4 escalation, and lets the merge send's own unknown outcome
    propagate for the F5b coordinator's typed Level 2 handoff. A successful
    merge completes the Task through the shared completion seam; no Owner
    approval, gate presentation, or wakeup scheduling exists here (F12 #316
    owns the due-time wakeups).
    """
    execute_authorized_merge_retry(
        dependencies.database_pool(),
        dependencies.github_client(),
        dependencies.user_token_resolver(),
        profile_id=UUID(payload.profile_id),
        workspace_id=UUID(payload.task.workspace_id),
        task_id=UUID(payload.task.task_id),
        expected_state_token=UUID(payload.expected_state_token),
        retry_attempt_id=UUID(payload.retry_attempt_id),
        attempt_evidence=attempt_evidence,
        terminal_finalization_submission=F1TerminalFinalizationSubmission(
            dependencies.continuation_submitter()
        ),
    )


def _compose_task_pr_authorization_retry(
    payload: TaskPrAuthorizationRetryPayload,
    *,
    dependencies: OperationDependencies,
    attempt_evidence: InvocationAttemptEvidence | None = None,
) -> None:
    """Execute the one F4-authorized presentation retry (F5f, issue #269).

    The presentation-retry seam atomically consumes the exact durably
    authorized F4 attempt and re-enters ONLY the guarded E7
    ``open_pr_authorization`` presentation — never the implementation effect,
    never a new Execution, never a second branch bind or gate. A failing
    re-presentation's operational error propagates for the F5b coordinator's
    typed Level 2 handoff with the consumed attempt as the PRIOR_AUTHORIZED
    reporting source; its workflow-semantic refusals are Level 1 outcomes.
    No wakeup scheduling exists here (F12 #316 owns the due-time wakeups).
    """
    execute_authorized_pr_authorization_retry(
        dependencies.database_pool(),
        dependencies.github_client(),
        profile_id=UUID(payload.profile_id),
        workspace_id=UUID(payload.task.workspace_id),
        task_id=UUID(payload.task.task_id),
        expected_state_token=UUID(payload.expected_state_token),
        retry_attempt_id=UUID(payload.retry_attempt_id),
        attempt_evidence=attempt_evidence,
    )


def _compose_task_pr_composition(
    payload: TaskPrCompositionPayload,
    *,
    dependencies: OperationDependencies,
    attempt_evidence: InvocationAttemptEvidence | None = None,
) -> None:
    """Compose and publish the canonical PR under the approving Profile.

    The composition turn is itself one detached agent turn (issue #337):
    the E8 admission, the one-shot PR_COMPOSE send claim with its
    detached-turn receipt companion, and the verbatim admission on the exact
    Producer session commit here, and the worker returns after the
    acknowledgement attach without waiting for any model turn. The
    independently observed completion settles the validated ``pr_result``
    through the owning settlement, and the ONE post-commit completion
    handoff owns the bounded publication and the PR-review submission from
    committed state. The narrow interrupted-publication continuation owns
    no detached turn: the dispatch reconciles and finalizes it inline, and
    this composition submits the PR review for the finalized publication
    (issue #347: both execution modes run this same detached composition).
    """
    pool = dependencies.database_pool()
    github = dependencies.github_client()
    user_token_resolver = dependencies.user_token_resolver()
    workspace_id = UUID(payload.task.workspace_id)
    task_id = UUID(payload.task.task_id)
    profile_id = UUID(payload.profile_id)
    owner_gate_id = UUID(payload.owner_gate_id)
    expected_state_token = UUID(payload.expected_state_token)

    dispatched = dispatch_pr_compose_detached(
        pool,
        github,
        user_token_resolver,
        _producer_runtime_attachment_factory(
            dependencies, workspace_id=workspace_id, task_id=task_id
        ),
        profile_id=profile_id,
        workspace_id=workspace_id,
        task_id=task_id,
        owner_gate_id=owner_gate_id,
        expected_state_token=expected_state_token,
        observation_registrar=dependencies.detached_turn_observer(),
        completion_handoff=build_detached_completion_handoff(dependencies),
        terminal_finalization_submission=F1TerminalFinalizationSubmission(
            dependencies.continuation_submitter()
        ),
        attempt_evidence=attempt_evidence,
    )
    if dispatched.published is None:
        # The fresh composition is durably admitted: the worker returns
        # without waiting for the pr_result. The observed completion owns
        # the settlement; submission outcomes are transport information
        # only and the durable state owns recovery.
        return
    # The narrow continuation finalized this invocation's own
    # interrupted publication (no composition ran): the same authorized
    # PR-review submission the clean published result always performs.
    published = dispatched.published
    _submit_continuation(
        dependencies,
        OperationInvocation(
            kind=OperationKind.TASK_PR_REVIEW,
            payload=TaskPrReviewPayload(
                task=TaskScope(
                    workspace_id=payload.task.workspace_id,
                    task_id=payload.task.task_id,
                    role=WorkflowRoleRef.REVIEWER,
                ),
                profile_id=payload.profile_id,
                task_pull_request_id=str(published.pull_request.id),
                expected_state_token=str(published.task.state_token),
            ),
        ),
    )


def _compose_task_pr_review(
    payload: TaskPrReviewPayload,
    *,
    dependencies: OperationDependencies,
    attempt_evidence: InvocationAttemptEvidence | None = None,
) -> None:
    """Run one PR review round.

    The REVIEW dispatch is itself one detached agent turn (issue #338): the
    composition re-derives the round's exact published subjects from
    committed state under the payload's exact token and canonical PR
    identity, and the owning service reserves exactly one next-numbered
    unfinalized PR/head ReviewIteration for the freshly reconciled head (the
    OPEN PR loop established on first review with the Workspace's
    snapshotted budget), commits the iteration's detached-turn receipt
    companion, and admits the composed canonical REVIEW message verbatim on
    the exact READY Reviewer session before returning. No ``send`` and no
    blocking wait exists here: the independently observed completion settles
    the validated ``review_result`` through the owning PR-review decision
    settlement, and the ONE post-commit completion handoff owns the workflow
    continuation from committed state — the detached E9 remediation enqueue
    on an in-budget change request (whose own observed completion re-proves
    the subjects, verifies the GitHub-confirmed new head, and submits the
    next typed ``TASK_PR_REVIEW`` through F1), with the MERGE_DECISION and
    REVIEW_RESOLUTION gates owning their Owner seams. A stale, duplicate, or
    late REVIEW observation can never decide twice, and duplicate/stale/
    ambiguous output never advances a newer head (issue #347: both execution
    modes run this same detached composition).
    """
    pool = dependencies.database_pool()
    github = dependencies.github_client()
    workspace_id = UUID(payload.task.workspace_id)
    task_id = UUID(payload.task.task_id)
    profile_id = UUID(payload.profile_id)
    reviewer_runtime_factory = _reviewer_runtime_attachment_factory(
        dependencies, workspace_id=workspace_id, task_id=task_id
    )

    published = _reload_published_subjects(
        pool,
        workspace_id=workspace_id,
        task_id=task_id,
        expected_state_token=UUID(payload.expected_state_token),
        task_pull_request_id=UUID(payload.task_pull_request_id),
    )
    # No Reviewer attachment is acquired in this composition, no decision
    # runs in-process, and the remediation chain never re-enters this worker
    # — the reserved PR/head iteration, its detached-turn receipt companion,
    # and the verbatim admission commit in the owning dispatch service, and
    # the independently observed completion owns the decision settlement
    # with the ONE post-commit completion handoff owning the remediation
    # enqueue and every later continuation from committed state.
    dispatch_pr_review_detached(
        pool,
        reviewer_runtime_factory,
        github,
        profile_id=profile_id,
        workspace_id=workspace_id,
        task_id=task_id,
        expected_state_token=published.task.state_token,
        observation_registrar=dependencies.detached_turn_observer(),
        completion_handoff=build_detached_completion_handoff(dependencies),
        attempt_evidence=attempt_evidence,
    )
    # The detached dispatch is durably admitted: the worker returns without
    # waiting for the review result. The observed completion owns the
    # settlement; submission outcomes are transport information only and the
    # durable state owns recovery.


def _compose_task_session_resume(
    payload: TaskSessionResumePayload,
    *,
    dependencies: OperationDependencies,
    attempt_evidence: InvocationAttemptEvidence | None = None,
) -> None:
    """Dispatch the authorized session-resume continuation (F8g, issue #292).

    The one-shot continuation the Owner Resume action's authorization
    committed: the durable record's routing context is re-derived from
    committed state, the record is claimed exactly once (or its proven
    never-dispatched supersession is recorded), and the fixed authorized
    resume instruction is admitted as one independent detached agent turn on
    the exact original session (issue #343) — the worker returns after the
    acknowledgement attach, the independently observed completion owns the
    settlement, and the ONE post-commit handoff owns the continuations (the
    original workflow operation's own family routes them). The handler owns
    no workflow rules of its own (issue #347: both execution modes run this
    same detached composition).
    """
    pool = dependencies.database_pool()
    workspace_id = UUID(payload.task.workspace_id)
    task_id = UUID(payload.task.task_id)
    dispatch_authorized_task_resume(
        pool,
        workspace_id=workspace_id,
        task_id=task_id,
        task_resume_intent_id=UUID(payload.task_resume_intent_id),
        profile_id=UUID(payload.profile_id),
        expected_state_token=UUID(payload.expected_state_token),
        producer_runtime_factory=_producer_runtime_attachment_factory(
            dependencies, workspace_id=workspace_id, task_id=task_id
        ),
        reviewer_runtime_factory=_reviewer_runtime_attachment_factory(
            dependencies, workspace_id=workspace_id, task_id=task_id
        ),
        attempt_evidence=attempt_evidence,
        observation_registrar=dependencies.detached_turn_observer(),
        completion_handoff=build_detached_completion_handoff(dependencies),
    )


# --- the single composition seam and the production handler table ------------


_TASK_WORKFLOW_COMPOSITIONS: Mapping[OperationKind, TaskWorkflowComposition] = {
    OperationKind.TASK_PLANNING_BOOTSTRAP_AND_INITIAL_PLAN: (
        _compose_task_planning_bootstrap_and_initial_plan
    ),
    OperationKind.TASK_PLANNING_REVIEW: _compose_task_planning_review,
    OperationKind.TASK_AUTHORIZED_IMPLEMENTATION: _compose_task_authorized_implementation,
    OperationKind.TASK_AUTHORIZED_IMPLEMENTATION_RETRY: (
        _compose_task_authorized_implementation_retry
    ),
    OperationKind.TASK_AUTHORIZED_MERGE_RETRY: _compose_task_authorized_merge_retry,
    OperationKind.TASK_PR_AUTHORIZATION_RETRY: _compose_task_pr_authorization_retry,
    OperationKind.TASK_PR_COMPOSITION: _compose_task_pr_composition,
    OperationKind.TASK_PR_REVIEW: _compose_task_pr_review,
    OperationKind.TASK_SESSION_RESUME: _compose_task_session_resume,
}


def execute_task_workflow_operation(
    invocation: OperationInvocation,
    *,
    dependencies: OperationDependencies,
    attempt_evidence: InvocationAttemptEvidence | None = None,
) -> None:
    """The single application composition seam for the F5a Task operations.

    Every registered production Task-workflow handler delegates here with
    its decoded invocation; the per-kind composition invokes the existing
    E-series services with process-local dependencies. The seam contains no
    Task-state, currentness, or authorization judgment of its own, and no
    operational retry/block/disposition logic: the F5b coordinator
    (issue #262) wraps this one seam with the typed failure handoff, and the
    compositions thread the coordinator's attempt-evidence collector into
    the services that produce durable identities.
    """
    composition = _TASK_WORKFLOW_COMPOSITIONS.get(invocation.kind)
    if composition is None:
        # Defensive fail-closed classification: the production handler table
        # registers exactly these kinds, so an unregistered kind never
        # reaches a Task-workflow handler.
        raise InvalidInvocationError(
            "no Task-workflow composition is registered for operation kind "
            f"{invocation.kind.value!r}"
        )
    composition(invocation.payload, dependencies=dependencies, attempt_evidence=attempt_evidence)


# The per-kind coarse Task status the operation addresses: the expected
# currentness input the F5b coordinator supplies to the Level 2 triage
# service (issue #262).
_TASK_KIND_EXPECTED_STATUS: Mapping[OperationKind, TaskStatus] = {
    OperationKind.TASK_PLANNING_BOOTSTRAP_AND_INITIAL_PLAN: TaskStatus.PLANNING,
    OperationKind.TASK_PLANNING_REVIEW: TaskStatus.PLANNING,
    OperationKind.TASK_AUTHORIZED_IMPLEMENTATION: TaskStatus.IMPLEMENTING,
    OperationKind.TASK_AUTHORIZED_IMPLEMENTATION_RETRY: TaskStatus.IMPLEMENTING,
    OperationKind.TASK_AUTHORIZED_MERGE_RETRY: TaskStatus.WAITING_FOR_OWNER,
    OperationKind.TASK_PR_AUTHORIZATION_RETRY: TaskStatus.IMPLEMENTING,
    OperationKind.TASK_PR_COMPOSITION: TaskStatus.WAITING_FOR_OWNER,
    OperationKind.TASK_PR_REVIEW: TaskStatus.REVIEWING,
}


def _task_workflow_handler(kind: OperationKind) -> OperationHandler:
    """Build the kind's registered handler around the single composition seam."""

    def handler(payload: Any, *, dependencies: OperationDependencies) -> None:
        # The F5b coordinator (issue #262) owns the typed failure handoff:
        # it invokes Level 1 through the single composition seam, preserves
        # ordinary success, and reports applicable operational failures to
        # the Level 2 triage service with the invocation's own Level 1
        # attempt identity and Level 1-established currentness. The handler
        # itself only decodes and delegates.
        if kind is OperationKind.TASK_SESSION_RESUME:
            # The F8g resume addresses the authorization-established restored
            # status carried on its payload (the one variable-status kind).
            resume_payload = cast(TaskSessionResumePayload, payload)
            expected_status = TaskStatus(resume_payload.restored_status)
        else:
            expected_status = _TASK_KIND_EXPECTED_STATUS[kind]
        coordinate_task_workflow_operation(
            dependencies.database_pool(),
            expected_status=expected_status,
            workspace_id=UUID(payload.task.workspace_id),
            task_id=UUID(payload.task.task_id),
            profile_id=UUID(payload.profile_id),
            expected_state_token=UUID(payload.expected_state_token),
            invoke_level_one=lambda evidence: execute_task_workflow_operation(
                # Reconstructing the invocation re-proves the construction-time
                # kind/payload correspondence before any composition runs.
                OperationInvocation(kind=kind, payload=payload),
                dependencies=dependencies,
                attempt_evidence=evidence,
            ),
        )

    return handler


def _handle_task_owner_reviewer_discussion(
    payload: Any, *, dependencies: OperationDependencies
) -> None:
    """Admit one prepared Owner ↔ Reviewer discussion turn (issue #339).

    The discussion admission is deliberately NOT enrolled in the F5b
    coordinator's Level 2 handoff: the discussion family carries no
    operational retry/block semantics (an Owner-initiated advisory
    conversation never reports operational failures or blocks the Task), so
    the handler invokes the admission service directly. The receipt
    lifecycle, the one-shot admission stamp, and the typed transport errors
    own every failure; the Owner-visible receipt-status projection reports
    them.
    """
    discussion_payload = cast(TaskOwnerReviewerDiscussionPayload, payload)
    workspace_id = UUID(discussion_payload.task.workspace_id)
    task_id = UUID(discussion_payload.task.task_id)
    admit_owner_reviewer_discussion_turn(
        dependencies.database_pool(),
        profile_id=UUID(discussion_payload.profile_id),
        workspace_id=workspace_id,
        task_id=task_id,
        owner_gate_id=UUID(discussion_payload.owner_gate_id),
        receipt_id=UUID(discussion_payload.receipt_id),
        message=discussion_payload.message,
        reviewer_runtime_factory=_reviewer_runtime_attachment_factory(
            dependencies, workspace_id=workspace_id, task_id=task_id
        ),
        observation_registrar=dependencies.detached_turn_observer(),
        completion_handoff=build_detached_completion_handoff(dependencies),
    )


def build_task_workflow_handlers() -> Mapping[OperationKind, OperationHandler]:
    """The explicit F5a Task-workflow kind-to-handler table.

    The Task-less GitHub delivery/reconciliation kinds are not Task-workflow
    operations: F9 (issue #255) registers them separately in
    ``openorc.workers.github_operations``. The detached Owner ↔ Reviewer
    discussion admission (issue #339) is registered beside them with its own
    non-coordinator handler.
    """
    handlers: dict[OperationKind, OperationHandler] = {
        kind: _task_workflow_handler(kind) for kind in _TASK_WORKFLOW_COMPOSITIONS
    }
    handlers[OperationKind.TASK_OWNER_REVIEWER_DISCUSSION] = _handle_task_owner_reviewer_discussion
    return handlers


# --- production dependency providers -----------------------------------------


def default_bootstrap_runtime_composition_provider(
    settings: Settings,
) -> TaskBootstrapRuntimeComposition:
    """The production Cline composition of the bootstrap runtime seam."""
    return ClineBootstrapRuntimeComposition()


def default_user_token_resolver_provider(settings: Settings) -> ProfileUserAccessTokenResolver:
    """The production durable Profile user-token resolver.

    Construction fails fast when the deployment's GitHub App user-flow
    credentials are missing: the component constructing the client is the
    one that must possess the credential, and Owner-accountable PR
    publication is unavailable without them.
    """
    if (
        not settings.github_app_user_flow_client_id
        or not settings.github_app_user_flow_client_secret
    ):
        raise ConfigurationError(
            "the GitHub App user-flow credentials are not configured; "
            "Owner-accountable PR publication is unavailable"
        )
    return GitHubUserAccessTokenResolver(
        token_client=HttpGitHubUserTokenClient(
            client_id=settings.github_app_user_flow_client_id,
            client_secret=settings.github_app_user_flow_client_secret,
        )
    )


def default_reviewer_neutral_directory_provider(settings: Settings) -> str:
    """The deployment's existing neutral non-repository Reviewer directory.

    The planning bootstrap establishes the Reviewer session in this
    pre-existing runtime-side directory and the workflow's Reviewer
    attachments reconstruct there; it is deployment configuration, never
    Workspace state, and it is required (fail-fast) when a Task-workflow
    operation executes without it.
    """
    configured = settings.reviewer_neutral_directory
    if configured is None or not configured.strip():
        raise ConfigurationError(
            "the reviewer-neutral directory is not configured; the planning "
            "bootstrap cannot establish the Reviewer session"
        )
    return configured
