"""Producer plan production and immutable PlanRevision creation (issue #77).

E2 replaces the Producer half of the manual clipboard with two focused,
transport-independent application-service operations:

- ``produce_initial_task_plan`` consumes the exact successful E1b planning
  bootstrap outcome (#203), sends the ONE canonical PLAN interaction to the
  already-prepared Producer session, and turns the validated typed
  ``plan_result`` into the Task's first immutable PlanRevision — installed as
  the Task's current plan with its atomically coordinated
  ``PLAN_REVISION_CREATED`` audit event.
- ``revise_task_plan`` receives the E3-directed exact current prior
  PlanRevision and finalized planning ``CHANGES_REQUESTED`` ReviewIteration,
  routes that persisted review result verbatim to the same Producer session,
  and turns the new validated ``plan_result`` into the next immutable
  PlanRevision with its ``PLAN_REVISION_REVISED`` audit event.

Scope discipline (Phase 2E): OpenOrc coordinates messages, validated JSON,
persistence, authority, and safety; the agents make technical judgments.
These operations never assess plan semantics, never invoke the Reviewer,
never create ReviewLoops or ReviewIterations, never decide iteration limits
or exhaustion, never accept a plan, and never authorize implementation. The
planning ReviewLoop, Reviewer messaging, recorded review decisions, and
limit/exhaustion machinery are #78 (E3); implementation authorization is
#80 (E5); durable outbound logical-dispatch/replay and regular/RQ execution
are #87–#89; general blocking/recovery is #85.

Phase composition (both operations):

1. entry validation — one short database-only transaction: the account
   operational barrier, exact Owner Workspace/Task scope, the exact current
   ``planning`` Task under the caller's expected ``state_token``, the exact
   READY Producer binding (initial production additionally matches both
   bindings' identities against the bootstrap outcome and requires no
   already-current PlanRevision; revision production additionally proves the
   named prior PlanRevision is the Task's exact current revision and loads
   the finalized ``CHANGES_REQUESTED`` planning ReviewIteration addressed to
   it), and the durable Repository → installation route;
2. authoritative repository-base audit observation — installation-
   authenticated GitHub repository and default-branch reads with NO database
   transaction open; an inaccessible or missing base observation fails
   closed before any Producer dispatch;
3. the ONE interaction send — the established #129 interaction (PLAN renders
   the canonical ``controls/plan.md`` with optional subordinate Workspace
   guidance; REVISE routes the persisted validated ``review_result`` as the
   substantive Producer message with structural serialization only), sent
   once to the exact bound Producer through the #67
   ``realize_control(..., expected_family=PLAN_RESULT_FAMILY)`` funnel, with
   NO database transaction open;
4. atomic finalize — one short ``composed_transaction``: lock/recheck the
   exact Task subject (and, for revisions, that the prior revision is still
   exactly current) so a late response after external currentness moved
   publishes nothing, derive the next per-Task revision number from the
   existing history, insert the immutable PlanRevision carrying the exact
   plan text and the branch-observed base SHA, install it conditionally
   through the token-rotating pointer mutation, and record the coordinated
   WorkflowEvent — all committing or rolling back together.

The observed base SHA is audit context for the revision, never checkout
attestation and never a source-drift or branch-movement block: later base
movement alone neither invalidates the resulting plan nor forces replanning.
No agent-claimed SHA, checkout path, or plan text enters authority or audit
telemetry. Known runtime/GitHub failures and uncertain outcomes are distinct
typed conditions; an unresolved or uncertain send is never permission to
resend, and no outbox, job framework, or recovery engine exists here —
cross-process duplicate-send safety is #87–#89/F2/F4 work on top of the
logical invocation identity these operations provide.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from typing import NoReturn
from uuid import UUID

from psycopg.errors import UniqueViolation

from openorc.adapters.agent_runtime.contract import AgentRuntimeAdapter
from openorc.adapters.agent_runtime.errors import (
    AgentRuntimeError,
    AgentRuntimeUncertainOutcomeError,
)
from openorc.adapters.github import (
    GitHubAppClient,
    GitHubAuthenticationRejectedError,
    GitHubAuthorizationRejectedError,
    GitHubOutcomeUncertainError,
    GitHubRateLimitedError,
    GitHubRepositoryObservation,
    GitHubRequestRejectedError,
)
from openorc.domain.connections import WorkflowRole
from openorc.domain.events import WorkflowEventActor, WorkflowEventType
from openorc.domain.ownership import Workspace
from openorc.domain.reviews import ReviewIteration, ReviewLoopPurpose, ReviewOutcome
from openorc.domain.sessions import TaskAgentSession, TaskSessionLifecycleStatus
from openorc.domain.tasks import Task, TaskStatus
from openorc.observability import annotate_span, application_span
from openorc.persistence import planning as planning_records
from openorc.persistence import reviews as review_records
from openorc.persistence import tasks as task_records
from openorc.persistence.ownership import get_workspace
from openorc.persistence.pool import DatabasePool
from openorc.persistence.sessions import get_task_agent_session
from openorc.protocol.interaction import (
    REVIEW_OUTCOME_CHANGES_REQUESTED,
    PlanInteraction,
    PlanReviewSubject,
    ReviseInteraction,
)
from openorc.protocol.models import (
    PLAN_RESULT_FAMILY,
    REVIEW_RESULT_FAMILY,
    SCHEMA_VERSION,
    PlanResult,
    ReviewFinding,
    ReviewResult,
)
from openorc.services import event_coordination
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
    ResolvedRepositoryInstallationRoute,
    resolve_system_repository_installation_route,
)
from openorc.services.profile_lifecycle_guard import require_account_operational
from openorc.services.task_planning_bootstrap import TaskPlanningPreparationOutcome
from openorc.services.task_subject_guards import (
    require_current_plan_revision,
    require_current_task,
)
from openorc.services.transaction_composition import composed_transaction
from openorc.services.workspace_authorization import require_workspace_task

__all__ = [
    "TaskPlanProductionOutcome",
    "produce_initial_task_plan",
    "revise_task_plan",
]

_SERVICE_TRACER_SCOPE = "openorc.services.task_plan_production"
_PRODUCE_INITIAL_SPAN_NAME = "task_plan_production.produce_initial_task_plan"
_REVISE_PLAN_SPAN_NAME = "task_plan_production.revise_task_plan"

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class TaskPlanProductionOutcome:
    """The successful plan-production outcome for one Task continuation.

    ``plan_revision`` is the exact persisted immutable PlanRevision this
    invocation created; ``task`` is the post-pointer-update Task carrying
    the newly rotated ``state_token`` that the next continuation (an E3
    review dispatch or a further revision) must address.
    """

    plan_revision: planning_records.PlanRevision
    task: Task


# --- command validation -----------------------------------------------------


def _require_uuid_command(value: object, name: str) -> UUID:
    """Reject a malformed command identifier before any read or effect."""
    if not isinstance(value, UUID):
        raise InvalidCommandError(f"{name} must be a UUID")
    return value


def _require_runtime(value: object) -> AgentRuntimeAdapter:
    if not isinstance(value, AgentRuntimeAdapter):
        raise InvalidCommandError("runtime must be an AgentRuntimeAdapter")
    return value


def _require_preparation(value: object) -> TaskPlanningPreparationOutcome:
    """Require the exact successful E1b handoff, not coarse readiness facts.

    Coarse ``PLANNING`` state, READY sessions, or the one-shot bootstrap
    claim alone are never preparation proof: only the outcome object the
    successful bootstrap continuation returned — carrying the post-claim
    Task with its rotated ``state_token`` and both exact READY bindings —
    authorizes the first PLAN.
    """
    if not isinstance(value, TaskPlanningPreparationOutcome):
        raise InvalidCommandError(
            "preparation must be the successful TaskPlanningPreparationOutcome "
            "returned by the planning bootstrap"
        )
    return value


# --- session binding guards -------------------------------------------------


def _require_ready_binding(
    pool: DatabasePool, *, workspace_id: UUID, task_id: UUID, role: WorkflowRole
) -> TaskAgentSession:
    """Load the Task/role binding and require its exact READY readiness.

    Planning production reuses the original role binding only: a missing,
    cross-Workspace, or cross-Task binding is the uniform not-found outcome;
    a LOST/ENDED/CONNECTING binding is a terminal or not-yet-established
    conflict. No route rebinding, second session, or fresh initialization
    ever happens here.
    """
    binding = get_task_agent_session(pool, task_id=task_id, role=role)
    if binding is None or binding.workspace_id != workspace_id or binding.task_id != task_id:
        raise NotFoundError(
            f"the requested {role.value} session binding is not available for this task"
        )
    if binding.lifecycle_status is not TaskSessionLifecycleStatus.READY:
        raise ConflictError(
            f"the {role.value} session binding is not READY; plan production uses the "
            "exact prepared session only and never recreates or reinitializes one"
        )
    if binding.external_session_id is None:
        raise ConflictError(f"the {role.value} session binding has no external session identity")
    return binding


def _require_binding_identity_shape(expected: object, *, role: WorkflowRole) -> TaskAgentSession:
    """Reject a malformed outcome binding before any durable read."""
    if not isinstance(expected, TaskAgentSession):
        raise InvalidCommandError(
            f"the prepared {role.value} outcome binding must be a TaskAgentSession"
        )
    return expected


def _require_binding_identity(
    binding: TaskAgentSession, *, expected: TaskAgentSession, role: WorkflowRole
) -> None:
    """Match the durable binding against the bootstrap outcome's binding.

    The E1b outcome names the exact Task/role/Connection/external-session
    identities the planning dispatch must use; a durable binding that has
    moved to a different identity is a continuity conflict, and the prepared
    session is never substituted.
    """
    identity_matches = (
        expected.id == binding.id
        and expected.workspace_id == binding.workspace_id
        and expected.task_id == binding.task_id
        and expected.role is role
        and expected.connection_id == binding.connection_id
        and expected.external_session_id == binding.external_session_id
        and expected.lifecycle_status is TaskSessionLifecycleStatus.READY
    )
    if not identity_matches:
        raise ConflictError(
            f"the durable {role.value} session binding does not match the prepared "
            "planning outcome; the prepared session is never substituted"
        )


def _require_prepared_binding(
    pool: DatabasePool,
    *,
    workspace_id: UUID,
    task_id: UUID,
    role: WorkflowRole,
    expected: TaskAgentSession,
) -> TaskAgentSession:
    """Load the Task/role binding, require READY, and match the outcome identity."""
    _require_binding_identity_shape(expected, role=role)
    binding = _require_ready_binding(pool, workspace_id=workspace_id, task_id=task_id, role=role)
    _require_binding_identity(binding, expected=expected, role=role)
    return binding


def _producer_actor(producer: TaskAgentSession) -> event_coordination.WorkflowActorContext:
    """The safe bound Producer logical identity for plan audit events.

    The actor is the workflow role with its durable binding UUID as the
    logical actor identity — never plan text, prompts, transcripts, or
    runtime-owned identifiers.
    """
    return event_coordination.WorkflowActorContext(WorkflowEventActor.PRODUCER, str(producer.id))


# --- authoritative GitHub repository-base observation -----------------------


def _translate_github_observation_outcome(error: Exception) -> ApplicationError:
    """Translate the adapter's classified GitHub outcomes (issues #59/#63)."""
    if isinstance(error, GitHubAuthorizationRejectedError):
        return AuthorizationError(
            "the routed GitHub installation does not currently grant access to the "
            "addressed repository or its branches"
        )
    if isinstance(error, GitHubRateLimitedError):
        return ExternalOperationFailedError(
            "the GitHub repository-base observation was rate limited"
        )
    if isinstance(error, GitHubAuthenticationRejectedError):
        return ExternalOperationFailedError("GitHub rejected the OpenOrc GitHub App authentication")
    if isinstance(error, GitHubRequestRejectedError):
        return ExternalOperationFailedError("GitHub rejected the repository-base observation")
    assert isinstance(error, GitHubOutcomeUncertainError)
    return ExternalOperationUncertainError(
        "the outcome of the GitHub repository-base observation is unknown"
    )


def _observe_repository_base(
    github: GitHubAppClient, route: ResolvedRepositoryInstallationRoute
) -> tuple[GitHubRepositoryObservation, str]:
    """Observe the fresh repository address and its committed default-branch head.

    The installation-authenticated observation is the ONLY repository-base
    authority: agent text, a claimed checkout path, runtime-local HEAD, and
    the stored mutable repository address are never accepted. The caller
    holds no database transaction, so an observation failure cannot strand
    any answered-but-unpersisted plan and dispatch never begins without the
    audit context this returns.
    """
    try:
        repository_observation = github.get_installation_repository(
            github_installation_id=route.github_installation_id,
            github_repository_id=route.repository.identity.github_repository_id,
        )
    except GitHubOutcomeUncertainError as error:
        raise _translate_github_observation_outcome(error) from error
    except GitHubRequestRejectedError as error:
        raise _translate_github_observation_outcome(error) from error
    if (
        repository_observation.default_branch is None
        or not repository_observation.default_branch.strip()
    ):
        # An empty repository has no committed base to observe: the plan's
        # audit context cannot be established, so dispatch never begins.
        raise ExternalOperationFailedError(
            "the repository currently has no default branch to observe as the "
            "plan's repository-base context"
        )
    try:
        branch_observation = github.get_repository_branch(
            github_installation_id=route.github_installation_id,
            owner_login=repository_observation.owner_login,
            repository_name=repository_observation.name,
            branch_name=repository_observation.default_branch,
        )
    except GitHubOutcomeUncertainError as error:
        raise _translate_github_observation_outcome(error) from error
    except GitHubRequestRejectedError as error:
        raise _translate_github_observation_outcome(error) from error
    return repository_observation, branch_observation.head_sha


def _repository_full_name(observation: GitHubRepositoryObservation) -> str:
    """The freshly observed owner/name presentation address for one interaction."""
    return f"{observation.owner_login}/{observation.name}"


# --- interaction sends ------------------------------------------------------


def _realize_plan_control(
    runtime: AgentRuntimeAdapter,
    session_id: str,
    interaction: PlanInteraction | ReviseInteraction,
    *,
    message: str,
) -> PlanResult:
    """Send ONE established plan-family interaction to the exact bound Producer.

    The #67 shared funnel performs the formal validation; the service never
    adds an E-specific parser, inspects plan semantics, or manages runtime
    PLAN/ACT transitions. Known failure and uncertain outcome stay distinct,
    and neither is ever retried or resent here.
    """
    try:
        outcome = runtime.realize_control(
            session_id, interaction, message=message, expected_family=PLAN_RESULT_FAMILY
        )
    except AgentRuntimeUncertainOutcomeError as error:
        logger.warning(
            "plan production ended with an uncertain runtime outcome; no plan revision is published"
        )
        raise ExternalOperationUncertainError(
            "the outcome of the agent runtime plan interaction is unknown; "
            "no plan revision is published"
        ) from error
    except AgentRuntimeError as error:
        raise ExternalOperationFailedError(
            "the agent runtime plan interaction failed; no plan revision is published"
        ) from error
    if not isinstance(outcome, PlanResult):
        # A formal send with an expected family returns a typed formal
        # result or raises; anything else is a known non-answer.
        raise ExternalOperationFailedError(
            "the agent runtime did not return a typed plan result; no plan revision is published"
        )
    return outcome


def _send_initial_plan(
    runtime: AgentRuntimeAdapter,
    session_id: str,
    *,
    task: Task,
    workspace: Workspace,
    repository_observation: GitHubRepositoryObservation,
) -> PlanResult:
    """Construct and send the ONE canonical PLAN interaction (issue #129)."""
    interaction = PlanInteraction(
        repository_full_name=_repository_full_name(repository_observation),
        issue_number=task.github_issue_number,
        guidance=workspace.guidance,
    )
    return _realize_plan_control(runtime, session_id, interaction, message=interaction.render())


def _review_result_from_iteration(iteration: ReviewIteration) -> ReviewResult:
    """Rebuild the persisted protocol-validated review result verbatim.

    The finalized iteration's summary and findings are the revision input:
    values and ordering are preserved exactly, with no interpretation,
    rewriting, or free-text mutation. A persisted findings document that is
    no longer in the canonical ``review_result`` finding shape fails closed
    instead of being repaired or inferred.
    """
    findings: list[ReviewFinding] = []
    for item in iteration.findings or ():
        if (
            not isinstance(item, dict)
            or set(item.keys()) != {"summary", "details"}
            or not isinstance(item.get("summary"), str)
            or not str(item.get("summary", "")).strip()
            or not isinstance(item.get("details"), str)
            or not str(item.get("details", "")).strip()
        ):
            raise ConflictError(
                "the persisted review findings are not in the canonical review_result "
                "finding shape; no plan revision is produced"
            )
        findings.append(ReviewFinding(summary=item["summary"], details=item["details"]))
    return ReviewResult(
        outcome=REVIEW_OUTCOME_CHANGES_REQUESTED,
        summary=iteration.summary or "",
        findings=tuple(findings),
    )


def _serialize_review_result(review_result: ReviewResult) -> str:
    """The substantive Producer message: structural serialization only.

    The routed artifact is the exact canonical ``review_result`` formal
    envelope the Reviewer produced — no new remediation Markdown template,
    no ``producer.plan.revise`` control, no inferred instructions, and no
    rewriting of free-text fields.
    """
    payload = {
        "type": REVIEW_RESULT_FAMILY,
        "schema_version": SCHEMA_VERSION,
        "outcome": review_result.outcome,
        "summary": review_result.summary,
        "findings": [
            {"summary": finding.summary, "details": finding.details}
            for finding in review_result.findings
        ],
    }
    return json.dumps(payload)


def _send_revision_plan(
    runtime: AgentRuntimeAdapter,
    session_id: str,
    *,
    task: Task,
    prior_revision: planning_records.PlanRevision,
    review_result: ReviewResult,
    repository_observation: GitHubRepositoryObservation,
) -> PlanResult:
    """Construct and send the REVISE interaction for the exact review subject."""
    subject = PlanReviewSubject(
        plan_revision_id=str(prior_revision.id),
        repository_full_name=_repository_full_name(repository_observation),
        issue_number=task.github_issue_number,
        plan=prior_revision.content,
    )
    interaction = ReviseInteraction(subject=subject, review_result=review_result)
    return _realize_plan_control(
        runtime, session_id, interaction, message=_serialize_review_result(review_result)
    )


# --- atomic finalize --------------------------------------------------------


def _require_classified_plan_currentness(
    transaction_pool: DatabasePool, *, task_id: UUID, expected_state_token: UUID
) -> NoReturn:
    """Classify a rejected conditional pointer write from the locked row.

    The locked re-read observes the durable state at write time (a
    concurrent token rotation or archival serializes on the row lock), so
    the classification never comes from a racy re-read. Anything unexplained
    still fails closed as a stale operation.
    """
    current = task_records.get_task_for_update(transaction_pool, task_id)
    if current is None:
        raise NotFoundError("the requested task is not available in this workspace")
    if current.archived_at is not None:
        raise StaleOperationError("the task has already been archived; the operation is stale")
    if current.state_token != expected_state_token:
        raise StaleOperationError(
            "the task state has moved on since this operation was prepared; the operation is stale"
        )
    raise StaleOperationError(
        "the plan pointer mutation did not apply against the current task state; "
        "the operation is stale"
    )


def _commit_new_plan_revision(
    pool: DatabasePool,
    *,
    workspace_id: UUID,
    task_id: UUID,
    expected_state_token: UUID,
    plan_text: str,
    repository_base_sha: str,
    actor: event_coordination.WorkflowActorContext,
    event_type: WorkflowEventType,
    prior_revision: planning_records.PlanRevision | None,
) -> TaskPlanProductionOutcome:
    """Insert the immutable revision, install the pointer, and record the event.

    One short composed transaction: the post-send currentness recheck (the
    exact Task token, coarse planning state, and — per continuation kind —
    either still-no current revision or the exact prior revision still
    current) protects against late responses becoming authoritative; the
    per-Task revision-number UNIQUE constraint and the same-Task pointer
    foreign key are the durable backstops behind the derived next number.
    A rejected pointer write or a competing committed revision rolls the
    insert, pointer, and event back together — never two success outcomes.
    """
    with composed_transaction(pool) as transaction_pool:
        task = require_current_task(
            transaction_pool,
            workspace_id=workspace_id,
            task_id=task_id,
            expected_state_token=expected_state_token,
        )
        if task.status is not TaskStatus.PLANNING:
            raise StaleOperationError(
                "the task is no longer in its coarse planning state; the continuation is stale"
            )
        if prior_revision is None:
            if task.current_plan_revision_id is not None:
                raise StaleOperationError(
                    "the task already has a current plan revision; the first-plan "
                    "continuation is stale"
                )
        else:
            require_current_plan_revision(
                transaction_pool, task=task, plan_revision_id=prior_revision.id
            )
        revisions = planning_records.list_task_plan_revisions(transaction_pool, task_id=task_id)
        next_number = max((revision.revision_number for revision in revisions), default=0) + 1
        try:
            revision = planning_records.create_plan_revision(
                transaction_pool,
                workspace_id=workspace_id,
                task_id=task_id,
                revision_number=next_number,
                content=plan_text,
                repository_base_sha=repository_base_sha,
            )
        except UniqueViolation as exc:
            # The per-Task revision-number UNIQUE constraint rejected the
            # insert: a competing plan revision committed first, so this
            # continuation is stale. The composition rolls back and nothing
            # was applied.
            raise StaleOperationError(
                "a competing plan revision for this task committed first; the continuation is stale"
            ) from exc
        updated = task_records.set_current_plan_revision(
            transaction_pool,
            task_id,
            expected_state_token=expected_state_token,
            plan_revision_id=revision.id,
        )
        if updated is None:
            _require_classified_plan_currentness(
                transaction_pool, task_id=task_id, expected_state_token=expected_state_token
            )
        if event_type is WorkflowEventType.PLAN_REVISION_CREATED:
            event_coordination.record_plan_revision_created_event(
                transaction_pool,
                workspace_id=workspace_id,
                task_id=task_id,
                plan_revision_id=revision.id,
                actor=actor,
            )
        else:
            event_coordination.record_plan_revision_revised_event(
                transaction_pool,
                workspace_id=workspace_id,
                task_id=task_id,
                plan_revision_id=revision.id,
                actor=actor,
            )
    return TaskPlanProductionOutcome(plan_revision=revision, task=updated)


def produce_initial_task_plan(
    pool: DatabasePool,
    runtime: AgentRuntimeAdapter,
    github: GitHubAppClient,
    *,
    workspace_id: UUID,
    task_id: UUID,
    profile_id: UUID,
    preparation: TaskPlanningPreparationOutcome,
) -> TaskPlanProductionOutcome:
    """Send the first canonical PLAN to the prepared Producer and persist it.

    Consumes the exact successful E1b ``TaskPlanningPreparationOutcome``:
    the post-claim Task token authorizes the continuation, both durable
    role bindings must be READY and match the outcome's identities exactly,
    and the Task must carry no already-current PlanRevision. The canonical
    PLAN interaction is sent exactly once to the exact bound Producer
    session; the validated ``plan_result`` becomes revision 1 with the
    branch-observed repository-base SHA and the ``PLAN_REVISION_CREATED``
    audit event. Neither role session is recreated, reinitialized, or
    prepared again, the one-shot bootstrap claim is never consulted, and a
    later E3-directed revision never needs a second bootstrap outcome.
    """
    with application_span(_SERVICE_TRACER_SCOPE, _PRODUCE_INITIAL_SPAN_NAME) as span:
        _require_uuid_command(workspace_id, "workspace_id")
        _require_uuid_command(task_id, "task_id")
        _require_uuid_command(profile_id, "profile_id")
        runtime_adapter = _require_runtime(runtime)
        preparation = _require_preparation(preparation)
        annotate_span(
            span,
            operation=_PRODUCE_INITIAL_SPAN_NAME,
            workspace_id=str(workspace_id),
            task_id=str(task_id),
        )

        # --- Phase 1: entry subject/session validation (database only). ---
        with composed_transaction(pool) as transaction_pool:
            require_account_operational(transaction_pool, profile_id=profile_id)
            require_workspace_task(
                transaction_pool,
                profile_id=profile_id,
                workspace_id=workspace_id,
                task_id=task_id,
            )
            task = require_current_task(
                transaction_pool,
                workspace_id=workspace_id,
                task_id=task_id,
                expected_state_token=preparation.task.state_token,
            )
            if task.status is not TaskStatus.PLANNING:
                raise ConflictError("the addressed task is not in its coarse planning state")
            if task.current_plan_revision_id is not None:
                raise ConflictError(
                    "the task already has a current plan revision; initial plan "
                    "production is not repeatable"
                )
            producer = _require_prepared_binding(
                transaction_pool,
                workspace_id=workspace_id,
                task_id=task_id,
                role=WorkflowRole.PRODUCER,
                expected=preparation.producer_session,
            )
            _require_prepared_binding(
                transaction_pool,
                workspace_id=workspace_id,
                task_id=task_id,
                role=WorkflowRole.REVIEWER,
                expected=preparation.reviewer_session,
            )
            workspace = get_workspace(transaction_pool, workspace_id)
            if workspace is None:
                raise NotFoundError("the requested workspace is not available")
            route = resolve_system_repository_installation_route(
                transaction_pool,
                workspace_id=workspace_id,
                repository_id=task.repository_id,
            )

        # --- Phase 2: authoritative repository-base audit observation. ---
        repository_observation, base_sha = _observe_repository_base(github, route)

        # --- Phase 3: the ONE canonical PLAN send. ---
        plan_result = _send_initial_plan(
            runtime_adapter,
            producer.external_session_id or "",
            task=task,
            workspace=workspace,
            repository_observation=repository_observation,
        )

        # --- Phase 4: atomic finalize. ---
        return _commit_new_plan_revision(
            pool,
            workspace_id=workspace_id,
            task_id=task_id,
            expected_state_token=preparation.task.state_token,
            plan_text=plan_result.plan,
            repository_base_sha=base_sha,
            actor=_producer_actor(producer),
            event_type=WorkflowEventType.PLAN_REVISION_CREATED,
            prior_revision=None,
        )


def revise_task_plan(
    pool: DatabasePool,
    runtime: AgentRuntimeAdapter,
    github: GitHubAppClient,
    *,
    workspace_id: UUID,
    task_id: UUID,
    profile_id: UUID,
    expected_state_token: UUID,
    prior_plan_revision_id: UUID,
    review_iteration_id: UUID,
) -> TaskPlanProductionOutcome:
    """Route a finalized planning change request to the Producer and persist it.

    Receives the E3-directed exact current prior PlanRevision and finalized
    planning ``CHANGES_REQUESTED`` ReviewIteration: the iteration must
    belong to this Task's planning-purpose ReviewLoop and address exactly
    the current prior revision, and its persisted protocol-validated
    summary/findings are the revision input verbatim. The routed REVISE
    interaction continues the SAME prepared Producer session using the
    latest post-production Task token — it never demands a fresh bootstrap
    outcome and never reruns E1b startup/PREPARE/context switching (the
    one-shot claim deliberately prevents replay). E2 decides nothing about
    iteration limits, exhaustion, or Owner resolution: E3 owns eligibility.
    """
    with application_span(_SERVICE_TRACER_SCOPE, _REVISE_PLAN_SPAN_NAME) as span:
        _require_uuid_command(workspace_id, "workspace_id")
        _require_uuid_command(task_id, "task_id")
        _require_uuid_command(profile_id, "profile_id")
        _require_uuid_command(expected_state_token, "expected_state_token")
        _require_uuid_command(prior_plan_revision_id, "prior_plan_revision_id")
        _require_uuid_command(review_iteration_id, "review_iteration_id")
        runtime_adapter = _require_runtime(runtime)
        annotate_span(
            span,
            operation=_REVISE_PLAN_SPAN_NAME,
            workspace_id=str(workspace_id),
            task_id=str(task_id),
        )

        # --- Phase 1: entry subject/session validation (database only). ---
        with composed_transaction(pool) as transaction_pool:
            require_account_operational(transaction_pool, profile_id=profile_id)
            require_workspace_task(
                transaction_pool,
                profile_id=profile_id,
                workspace_id=workspace_id,
                task_id=task_id,
            )
            task = require_current_task(
                transaction_pool,
                workspace_id=workspace_id,
                task_id=task_id,
                expected_state_token=expected_state_token,
            )
            if task.status is not TaskStatus.PLANNING:
                raise ConflictError("the addressed task is not in its coarse planning state")
            prior_revision = require_current_plan_revision(
                transaction_pool, task=task, plan_revision_id=prior_plan_revision_id
            )
            iteration = _require_routed_review_iteration(
                transaction_pool,
                workspace_id=workspace_id,
                task_id=task_id,
                review_iteration_id=review_iteration_id,
                prior_revision=prior_revision,
            )
            producer = _require_ready_binding(
                transaction_pool,
                workspace_id=workspace_id,
                task_id=task_id,
                role=WorkflowRole.PRODUCER,
            )
            route = resolve_system_repository_installation_route(
                transaction_pool,
                workspace_id=workspace_id,
                repository_id=task.repository_id,
            )

        # --- Phase 2: authoritative repository-base audit observation. ---
        repository_observation, base_sha = _observe_repository_base(github, route)

        # --- Phase 3: the REVISE send to the same Producer session. ---
        plan_result = _send_revision_plan(
            runtime_adapter,
            producer.external_session_id or "",
            task=task,
            prior_revision=prior_revision,
            review_result=_review_result_from_iteration(iteration),
            repository_observation=repository_observation,
        )

        # --- Phase 4: atomic finalize (the prior revision must still be current). ---
        return _commit_new_plan_revision(
            pool,
            workspace_id=workspace_id,
            task_id=task_id,
            expected_state_token=expected_state_token,
            plan_text=plan_result.plan,
            repository_base_sha=base_sha,
            actor=_producer_actor(producer),
            event_type=WorkflowEventType.PLAN_REVISION_REVISED,
            prior_revision=prior_revision,
        )


def _require_routed_review_iteration(
    transaction_pool: DatabasePool,
    *,
    workspace_id: UUID,
    task_id: UUID,
    review_iteration_id: UUID,
    prior_revision: planning_records.PlanRevision,
) -> ReviewIteration:
    """Resolve the finalized planning change request addressed to the prior revision.

    The caller names the iteration; the persisted record decides its
    meaning. It must belong to this Task's planning-purpose ReviewLoop, be
    finalized ``CHANGES_REQUESTED`` (the complete result-fact set), and
    address exactly the Task's current prior revision — an ACCEPTED,
    unfinalized, non-planning, or superseded-subject iteration is never
    routed, and the findings are never taken from caller input.
    """
    iteration = review_records.get_review_iteration(
        transaction_pool, review_iteration_id=review_iteration_id
    )
    if iteration is None or iteration.workspace_id != workspace_id or iteration.task_id != task_id:
        raise NotFoundError("the requested review iteration is not available for this task")
    loop = review_records.get_review_loop(transaction_pool, review_loop_id=iteration.review_loop_id)
    if loop is None or loop.workspace_id != workspace_id or loop.task_id != task_id:
        raise NotFoundError("the requested review loop is not available for this task")
    if loop.purpose is not ReviewLoopPurpose.PLANNING:
        raise ConflictError(
            "the review iteration is not a planning review; revision production "
            "routes planning change requests only"
        )
    if (
        iteration.outcome is not ReviewOutcome.CHANGES_REQUESTED
        or iteration.summary is None
        or iteration.findings is None
        or iteration.decided_at is None
    ):
        raise ConflictError(
            "the review iteration is not a finalized CHANGES_REQUESTED planning "
            "review; revision production routes finalized change requests only"
        )
    if iteration.plan_revision_id != prior_revision.id:
        raise StaleOperationError(
            "the review iteration addresses a different plan revision than the "
            "task's current revision; the continuation is stale"
        )
    return iteration
