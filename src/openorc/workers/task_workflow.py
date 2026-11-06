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
from openorc.domain.sessions import TaskAgentSession
from openorc.domain.tasks import TaskStatus
from openorc.persistence.pool import DatabasePool
from openorc.persistence.sessions import get_task_agent_session
from openorc.services.cline_attachment import ClineAttachmentPhase, attach_cline_runtime
from openorc.services.cline_planning_bootstrap import ClineBootstrapRuntimeComposition
from openorc.services.errors import ConflictError, NotFoundError, StaleOperationError
from openorc.services.github_user_authorization import (
    GitHubUserAccessTokenResolver,
    ProfileUserAccessTokenResolver,
)
from openorc.services.operation_attempt_evidence import InvocationAttemptEvidence
from openorc.services.task_authorized_implementation_execution import (
    ProducerRuntimeAttachmentFactory,
    execute_authorized_implementation,
)
from openorc.services.task_authorized_implementation_retry import (
    execute_authorized_implementation_retry,
)
from openorc.services.task_authorized_merge_retry import (
    execute_authorized_merge_retry,
)
from openorc.services.task_authorized_pr_authorization_retry import (
    execute_authorized_pr_authorization_retry,
)
from openorc.services.task_implementation_authorization import (
    ImplementationAuthorization,
    open_implementation_authorization,
)
from openorc.services.task_plan_production import (
    TaskPlanProductionOutcome,
    produce_initial_task_plan,
)
from openorc.services.task_planning_bootstrap import (
    TaskBootstrapRuntimeComposition,
    _producer_snapshot_repository_base,
    prepare_task_for_planning,
)
from openorc.services.task_planning_review_decision import (
    PlanningReviewDecision,
    finalize_planning_review_decision,
)
from openorc.services.task_planning_review_dispatch import dispatch_planning_review
from openorc.services.task_pr_authorization import ApprovedPrAuthorization, open_pr_authorization
from openorc.services.task_pr_composition import TaskPrPublished, compose_and_publish_task_pr
from openorc.services.task_pr_remediation import TaskPrRemediationOutcome
from openorc.services.task_pr_review_decision import finalize_pr_review_decision
from openorc.services.task_pr_review_dispatch import (
    ReviewerRuntimeAttachmentFactory,
    dispatch_pr_review,
)
from openorc.services.task_start import TaskStartOutcome, TaskStartStatus
from openorc.services.task_subject_guards import (
    require_canonical_task_pull_request,
    require_current_plan_revision,
    require_current_task,
)
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
    TaskPlanningBootstrapAndInitialPlanPayload,
    TaskPlanningReviewPayload,
    TaskPrAuthorizationRetryPayload,
    TaskPrCompositionPayload,
    TaskPrReviewPayload,
    TaskScope,
    WorkflowRoleRef,
)

__all__ = [
    "build_task_workflow_handlers",
    "execute_task_workflow_operation",
    "submit_authorized_implementation_after_approval",
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
    """
    pool = dependencies.database_pool()
    github = dependencies.github_client()
    runtime_composition = dependencies.task_bootstrap_runtime_composition()
    reviewer_neutral_directory = dependencies.reviewer_neutral_directory()
    workspace_id = UUID(payload.task.workspace_id)
    task_id = UUID(payload.task.task_id)
    profile_id = UUID(payload.profile_id)
    expected_state_token = UUID(payload.expected_state_token)

    preparation = prepare_task_for_planning(
        pool,
        runtime_composition,
        workspace_id=workspace_id,
        task_id=task_id,
        profile_id=profile_id,
        expected_state_token=expected_state_token,
        reviewer_neutral_directory=reviewer_neutral_directory,
        attempt_evidence=attempt_evidence,
    )
    # The initial PLAN is produced in the prepared Producer session's
    # planning checkout, using the same logical invocation's preparation
    # outcome (never re-derived, never serialized).
    with attach_cline_runtime(
        pool,
        workspace_id=workspace_id,
        task_id=task_id,
        role=WorkflowRole.PRODUCER,
        phase=ClineAttachmentPhase.BOUND,
        external_session_id=_require_external_session_id(preparation.producer_session, "producer"),
        working_directory=preparation.producer_checkout_directory,
    ) as attached:
        production = produce_initial_task_plan(
            pool,
            attached.adapter,
            github,
            workspace_id=workspace_id,
            task_id=task_id,
            profile_id=profile_id,
            preparation=preparation,
            attempt_evidence=attempt_evidence,
        )
    _submit_continuation(
        dependencies,
        OperationInvocation(
            kind=OperationKind.TASK_PLANNING_REVIEW,
            payload=TaskPlanningReviewPayload(
                task=TaskScope(
                    workspace_id=payload.task.workspace_id,
                    task_id=payload.task.task_id,
                    role=WorkflowRoleRef.REVIEWER,
                ),
                profile_id=payload.profile_id,
                expected_state_token=str(production.task.state_token),
            ),
        ),
    )


def _compose_task_planning_review(
    payload: TaskPlanningReviewPayload,
    *,
    dependencies: OperationDependencies,
    attempt_evidence: InvocationAttemptEvidence | None = None,
) -> None:
    """Run one planning review round: E3a dispatch → E3b decision in-process.

    The dispatch and its decision finalization are one logical invocation —
    the reserved iteration's unfinalized ``ReviewResult`` is never durable
    and never serialized. The round's exact plan-production subject is
    re-derived from committed state under the payload's exact token. E3a
    acquires the Reviewer runtime itself through the deferred session-
    addressed attachment factory, strictly after its atomic ReviewIteration
    reservation: an attachment failure is attributable to the exact reserved
    iteration and reaches the Level 2 handoff with its own identity instead
    of stranding the planning Task without a disposition (F5e, issue #268).
    On an in-budget change request the merged E2 revise already ran inside
    the decision, and the fresh post-revise token authorizes the next round;
    on acceptance the exact-plan implementation gate is presented (never
    approved); on exhaustion the REVIEW_RESOLUTION gate already stands.
    """
    pool = dependencies.database_pool()
    github = dependencies.github_client()
    workspace_id = UUID(payload.task.workspace_id)
    task_id = UUID(payload.task.task_id)
    profile_id = UUID(payload.profile_id)
    expected_state_token = UUID(payload.expected_state_token)

    production = _reload_plan_production_outcome(
        pool, workspace_id=workspace_id, task_id=task_id, expected_state_token=expected_state_token
    )
    dispatch_outcome = dispatch_planning_review(
        pool,
        _reviewer_runtime_attachment_factory(
            dependencies, workspace_id=workspace_id, task_id=task_id
        ),
        profile_id=profile_id,
        workspace_id=workspace_id,
        task_id=task_id,
        plan_production=production,
        attempt_evidence=attempt_evidence,
    )
    # The decision may route an in-budget revision to the merged E2 revise:
    # the Producer session continues that revision in its planning checkout.
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
        decision = finalize_planning_review_decision(
            pool,
            attached.adapter,
            github,
            profile_id=profile_id,
            workspace_id=workspace_id,
            task_id=task_id,
            expected_state_token=production.task.state_token,
            dispatch=dispatch_outcome,
            attempt_evidence=attempt_evidence,
        )
    if decision.decision is PlanningReviewDecision.REVISION_ROUTED:
        routed_production = decision.revision_production
        if routed_production is None:
            # Defensive invariant: a revision-routed decision always carries
            # the produced revision continuation.
            raise ConflictError(
                "the revision-routed planning decision carried no produced revision"
            )
        _submit_continuation(
            dependencies,
            OperationInvocation(
                kind=OperationKind.TASK_PLANNING_REVIEW,
                payload=TaskPlanningReviewPayload(
                    task=TaskScope(
                        workspace_id=payload.task.workspace_id,
                        task_id=payload.task.task_id,
                        role=WorkflowRoleRef.REVIEWER,
                    ),
                    profile_id=payload.profile_id,
                    expected_state_token=str(routed_production.task.state_token),
                ),
            ),
        )
    elif decision.decision is PlanningReviewDecision.ACCEPTED_CLEAR:
        # The ordinary authorized continuation of accepted formal review:
        # present the exact-plan IMPLEMENTATION_AUTHORIZATION gate. No Owner
        # decision is required to present it, and approval never follows from
        # Reviewer acceptance — the explicit E5 approval remains an Owner
        # command outside this module. The shared source-requirements
        # pre-check (issue #289) freshly verifies the authoritative issue
        # before the gate is presented; a confirmed requirements change
        # blocks the Task through the shared atomic drift-block path.
        open_implementation_authorization(
            pool,
            github,
            profile_id=profile_id,
            workspace_id=workspace_id,
            task_id=task_id,
            expected_state_token=decision.task.state_token,
        )
    # EXHAUSTED ends here: the exact-plan REVIEW_RESOLUTION gate is already
    # installed and its Owner-mediated resolution is not a workflow operation.


def _compose_task_authorized_implementation(
    payload: TaskAuthorizedImplementationPayload,
    *,
    dependencies: OperationDependencies,
    attempt_evidence: InvocationAttemptEvidence | None = None,
) -> None:
    """Execute the E5-authorized initial implementation and present the PR gate.

    The E6 service re-derives the exact approved gate/plan handoff from
    durable state (never caller-supplied) and reserves exactly one initial
    implementation attempt. On the successful finalized execution, the
    ordinary authorized continuation presents the exact-head
    PR_AUTHORIZATION gate with the exact post-execution token and the
    finalized Execution identity — no Owner decision is required to present
    it, the service's existing GitHub authority checks (including the
    direct ``github_source_changed`` block) pass through unchanged, and the
    explicit E7 approval remains an Owner command outside this module.
    """
    pool = dependencies.database_pool()
    github = dependencies.github_client()
    workspace_id = UUID(payload.task.workspace_id)
    task_id = UUID(payload.task.task_id)
    profile_id = UUID(payload.profile_id)
    expected_state_token = UUID(payload.expected_state_token)

    execution = execute_authorized_implementation(
        pool,
        profile_id=profile_id,
        workspace_id=workspace_id,
        task_id=task_id,
        expected_state_token=expected_state_token,
        producer_runtime_factory=_producer_runtime_attachment_factory(
            dependencies, workspace_id=workspace_id, task_id=task_id
        ),
        attempt_evidence=attempt_evidence,
    )
    open_pr_authorization(
        pool,
        github,
        profile_id=profile_id,
        workspace_id=workspace_id,
        task_id=task_id,
        expected_state_token=execution.task.state_token,
        execution_id=execution.execution.id,
        attempt_evidence=attempt_evidence,
    )


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
    mechanics on the original Producer session. On the successful finalized
    execution, the ordinary authorized continuation presents the exact-head
    PR_AUTHORIZATION gate with the exact post-execution token and the NEW
    Execution identity — the same application path as the initial
    composition, with no new Owner approval or recovery policy here. A
    failed retry is reported to F4 by the F5b coordinator's existing durable
    binding lookup, never by this module.
    """
    pool = dependencies.database_pool()
    github = dependencies.github_client()
    workspace_id = UUID(payload.task.workspace_id)
    task_id = UUID(payload.task.task_id)
    profile_id = UUID(payload.profile_id)
    expected_state_token = UUID(payload.expected_state_token)

    execution = execute_authorized_implementation_retry(
        pool,
        profile_id=profile_id,
        workspace_id=workspace_id,
        task_id=task_id,
        expected_state_token=expected_state_token,
        retry_attempt_id=UUID(payload.retry_attempt_id),
        producer_runtime_factory=_producer_runtime_attachment_factory(
            dependencies, workspace_id=workspace_id, task_id=task_id
        ),
        attempt_evidence=attempt_evidence,
    )
    open_pr_authorization(
        pool,
        github,
        profile_id=profile_id,
        workspace_id=workspace_id,
        task_id=task_id,
        expected_state_token=execution.task.state_token,
        execution_id=execution.execution.id,
        attempt_evidence=attempt_evidence,
    )


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
    approval, gate presentation, or wakeup scheduling exists here (F11 owns
    the due-time wakeups).
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
    No wakeup scheduling exists here (F11 owns the due-time wakeups).
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

    The E8 service re-derives the persisted approved PR_AUTHORIZATION gate,
    its exact head-SHA subject, and the OWNER-attributed approving Profile
    from durable state, and publishes through the approving Profile's own
    user-to-server authorization — the payload's accountable Profile is
    resolved through its existing user-token resolver seam, never
    substituted with installation authorization. On the clean published
    result the exact post-publication token and canonical PR identity
    authorize the PR review dispatch.
    """
    pool = dependencies.database_pool()
    github = dependencies.github_client()
    user_token_resolver = dependencies.user_token_resolver()
    workspace_id = UUID(payload.task.workspace_id)
    task_id = UUID(payload.task.task_id)
    profile_id = UUID(payload.profile_id)
    owner_gate_id = UUID(payload.owner_gate_id)
    expected_state_token = UUID(payload.expected_state_token)

    published = compose_and_publish_task_pr(
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
        attempt_evidence=attempt_evidence,
    )
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
    """Run the PR review: E9 dispatch → decision, with remediation in-process.

    The dispatch and its decision finalization are one logical invocation —
    the reserved PR/head iteration's unfinalized ``ReviewResult`` is never
    durable and never serialized. The published subjects are re-derived from
    committed state under the payload's exact token and canonical PR
    identity. When the decision routes an in-budget change request, the
    remediation chain the decision already executed internally re-enters the
    ordinary dispatch and returns its outcome; the chain must continue in
    this same process (the re-entered outcome embeds an unfinalized result
    that can never cross a job boundary) until a decision ends the loop with
    the MERGE_DECISION or REVIEW_RESOLUTION gate — both Owner seams.
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
    dispatch_outcome = dispatch_pr_review(
        pool,
        reviewer_runtime_factory,
        github,
        profile_id=profile_id,
        workspace_id=workspace_id,
        task_id=task_id,
        expected_state_token=published.task.state_token,
        published=published,
        attempt_evidence=attempt_evidence,
    )
    while True:
        # The decision may route remediation: the Producer session continues
        # the finalized change request in its Task checkout.
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
            decision = finalize_pr_review_decision(
                pool,
                attached.adapter,
                github,
                reviewer_runtime_factory,
                profile_id=profile_id,
                workspace_id=workspace_id,
                task_id=task_id,
                expected_state_token=dispatch_outcome.task.state_token,
                dispatch=dispatch_outcome,
                attempt_evidence=attempt_evidence,
            )
        remediation = decision.remediation
        if remediation is None:
            break
        # The decision module types the routed remediation loosely; its own
        # contract always returns the typed remediation outcome whose
        # re-entered dispatch carries the GitHub-confirmed new head.
        typed_remediation = cast(TaskPrRemediationOutcome, remediation)
        remediated_dispatch = typed_remediation.dispatch
        if remediated_dispatch is None:
            raise ConflictError(
                "the remediation-routed decision carried no re-entered dispatch outcome"
            )
        dispatch_outcome = remediated_dispatch


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
        coordinate_task_workflow_operation(
            dependencies.database_pool(),
            expected_status=_TASK_KIND_EXPECTED_STATUS[kind],
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


def build_task_workflow_handlers() -> Mapping[OperationKind, OperationHandler]:
    """The explicit F5a Task-workflow kind-to-handler table.

    The two GitHub delivery/reconciliation kinds are deliberately absent:
    they are registered by F9 (issue #255), so production dispatch fails
    closed for them.
    """
    return {kind: _task_workflow_handler(kind) for kind in _TASK_WORKFLOW_COMPOSITIONS}


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
