"""Producer plan production and immutable PlanRevision creation (issue #77).

E2 replaces the Producer half of the manual clipboard with focused,
transport-independent application-service operations, dispatched as one
detached agent turn per production (issues #335/#347):

- ``dispatch_initial_plan_detached`` consumes the exact successful E1b
  planning bootstrap outcome (#203), admits the ONE canonical PLAN
  interaction verbatim on the already-prepared Producer session as one
  independently observed detached turn, and the independently observed
  validated ``plan_result`` settles into the Task's first immutable
  PlanRevision — installed as the Task's current plan with its atomically
  coordinated ``PLAN_REVISION_CREATED`` audit event.
- ``dispatch_planning_revise_detached`` receives the E3-directed exact
  current prior PlanRevision and finalized planning ``CHANGES_REQUESTED``
  ReviewIteration that is still the latest-numbered iteration addressing
  that exact revision in the still-OPEN planning ReviewLoop (the #207
  supersession guard), admits that persisted review result verbatim to the
  same Producer session, and the settled ``plan_result`` becomes the next
  immutable PlanRevision with its ``PLAN_REVISION_REVISED`` audit event.
- ``settle_observed_plan_result`` is the owning settlement the correlation
  seam invokes for the attributable validated result of either family.

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
   it — the loop still OPEN and the iteration still the latest-numbered one
   for that exact revision, so a superseded result on the same plan is never
   dispatched (#207)), and the durable Repository → installation route;
2. authoritative repository-base audit observation — installation-
   authenticated GitHub repository and default-branch reads with NO database
   transaction open; an inaccessible or missing base observation fails
   closed before any Producer dispatch;
3. the durable pre-send dispatch claim (issue #87) — one short transaction
   under the Task row lock commits the exact subject (the one initial PLAN
   per Task/Producer session, or the planning REVISE per exact finalized
   ``CHANGES_REQUESTED`` planning ReviewIteration) with locked
   currentness/binding revalidation and NO token rotation; a replayed or
   previously claimed continuation never dispatches again;
4. the ONE interaction send — the established #129 interaction (PLAN renders
   the canonical ``controls/plan.md`` with optional subordinate Workspace
   guidance; REVISE routes the persisted validated ``review_result`` as the
   substantive Producer message with structural serialization only), sent
   once to the exact bound Producer through the #67
   ``realize_control(..., expected_family=PLAN_RESULT_FAMILY)`` funnel, with
   NO database transaction open;
5. atomic finalize — one short ``composed_transaction``: lock/recheck the
   exact Task subject, the exact expected session bindings (still READY and
   identity-matched — session loss and explicit ends do not rotate the Task
   token), and, for revisions, that the prior revision is still exactly
   current and the routed change request is still the latest-numbered one
   for that exact revision (the loop row re-read ``FOR UPDATE``, serializing
   against concurrent iteration creation — #207), so a late response or a
   result finalized during the send after any required subject moved on
   publishes nothing; then derive the next per-Task revision number from the
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

import logging
from collections.abc import Callable, Sequence
from contextlib import AbstractContextManager, ExitStack
from dataclasses import dataclass
from typing import Final, NoReturn, Protocol, cast
from uuid import UUID

from psycopg.errors import UniqueViolation

from openorc.adapters.agent_runtime.contract import AgentRuntimeAdapter
from openorc.adapters.agent_runtime.detached import AgentTurnAdmission
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
from openorc.domain.detached_turns import TaskDetachedTurnReceipt
from openorc.domain.events import WorkflowEventActor, WorkflowEventType
from openorc.domain.intents import TaskResumeIntentStatus
from openorc.domain.operational_failures import OriginalAttemptKind
from openorc.domain.ownership import Workspace
from openorc.domain.pending_interactions import PendingTaskRuntimeInteractionOperation
from openorc.domain.reviews import (
    ReviewIteration,
    ReviewLoopPurpose,
    ReviewLoopStatus,
    ReviewOutcome,
)
from openorc.domain.send_claims import (
    TaskRuntimeSendClaim,
    TaskRuntimeSendClaimKind,
)
from openorc.domain.sessions import TaskAgentSession, TaskSessionLifecycleStatus
from openorc.domain.tasks import Task, TaskStatus
from openorc.observability import annotate_span, application_span
from openorc.persistence import planning as planning_records
from openorc.persistence import reviews as review_records
from openorc.persistence import tasks as task_records
from openorc.persistence.detached_turns import refuse_task_detached_turn_receipt
from openorc.persistence.intents import record_task_resume_intent_outcome
from openorc.persistence.ownership import get_workspace
from openorc.persistence.pool import DatabasePool
from openorc.persistence.sessions import get_task_agent_session
from openorc.protocol.interaction import (
    REVIEW_OUTCOME_CHANGES_REQUESTED,
    PlanInteraction,
)
from openorc.protocol.models import (
    PLAN_RESULT_FAMILY,
    PlanResult,
    ReviewFinding,
    ReviewResult,
)
from openorc.services import event_coordination
from openorc.services import task_detached_turn_correlation as turn_correlation
from openorc.services.errors import (
    ApplicationError,
    AuthorizationError,
    ConflictError,
    ExternalOperationFailedError,
    ExternalOperationRejectedError,
    ExternalOperationUncertainError,
    InvalidCommandError,
    NotFoundError,
    ObservationRegistrationRefusedError,
    StaleOperationError,
)
from openorc.services.github_installation_route import (
    ResolvedRepositoryInstallationRoute,
    resolve_system_repository_installation_route,
)
from openorc.services.operation_attempt_evidence import InvocationAttemptEvidence
from openorc.services.profile_lifecycle_guard import require_account_operational
from openorc.services.review_result_envelope import serialize_review_result
from openorc.services.task_completion_handoff import DetachedTurnCompletionHandoff
from openorc.services.task_planning_bootstrap import TaskPlanningPreparationOutcome
from openorc.services.task_runtime_send_claims import (
    RUNTIME_PRE_EFFECT_FAILURE_KIND,
    claim_initial_plan_send,
    claim_planning_revise_send,
    record_send_claim_known_non_delivery,
    settle_send_claim_for_plan_revision,
)
from openorc.services.task_subject_guards import (
    require_current_plan_revision,
    require_current_task,
)
from openorc.services.transaction_composition import composed_transaction
from openorc.services.workspace_authorization import require_workspace_task

__all__ = [
    "TaskPlanProductionOutcome",
    "settle_resumed_task_record",
    "dispatch_initial_plan_detached",
    "dispatch_planning_revise_detached",
    "settle_observed_plan_result",
]

_SERVICE_TRACER_SCOPE = "openorc.services.task_plan_production"

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


def _initial_plan_message(
    task: Task,
    workspace: Workspace,
    repository_observation: GitHubRepositoryObservation,
) -> str:
    """Compose the ONE canonical PLAN instruction's message (issue #129).

    The single composer both the blocking send and the detached admission
    consume, so the detached turn carries the exact same canonical control
    verbatim (issue #335).
    """
    return PlanInteraction(
        repository_full_name=_repository_full_name(repository_observation),
        issue_number=task.github_issue_number,
        guidance=workspace.guidance,
    ).render()


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


def _commit_new_plan_revision_transaction(
    transaction_pool: DatabasePool,
    *,
    workspace_id: UUID,
    task_id: UUID,
    expected_state_token: UUID,
    plan_text: str,
    repository_base_sha: str,
    actor: event_coordination.WorkflowActorContext,
    event_type: WorkflowEventType,
    prior_revision: planning_records.PlanRevision | None,
    expected_review_iteration: ReviewIteration | None,
    expected_producer_session: TaskAgentSession,
    expected_reviewer_session: TaskAgentSession | None,
    send_claim: TaskRuntimeSendClaim,
) -> TaskPlanProductionOutcome:
    """The shared plan-revision commit composition (one open transaction).

    The post-send exact-subject recheck (the
    exact Task token and coarse planning state, the exact expected session
    bindings still READY and identity-matched — session loss and explicit
    ends do not rotate the Task token, so the token alone cannot prove the
    prepared sessions are still usable — and, per continuation kind, either
    still-no current revision or the exact prior revision still current
    together with the routed change request still the latest-numbered one
    for that exact revision, the loop row re-read ``FOR UPDATE`` so
    concurrent iteration creation serializes behind this composition —
    issue #207) protects against late responses becoming authoritative
    after any required subject moved on; the per-Task revision-number UNIQUE
    constraint and the same-Task pointer foreign key are the durable
    backstops behind the derived next number. A rejected pointer write or a
    competing committed revision rolls the insert, pointer, and event back
    together — never two success outcomes. Composed inside the caller's
    open transaction (a nested composition becomes a SAVEPOINT), so the
    F8g resume routing settles its record atomically with the commit.
    """
    with composed_transaction(transaction_pool) as transaction_pool:
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
        # The exact prepared Producer binding must still be READY and
        # identity-matched: session loss and explicit ends do not rotate the
        # Task token, so the token alone cannot prove the session usable. The
        # identity match proves the durable binding equals the expected
        # (entry) binding, so the caller-derived actor identity is the same
        # safe bound Producer logical identity at write time.
        _require_prepared_binding(
            transaction_pool,
            workspace_id=workspace_id,
            task_id=task_id,
            role=WorkflowRole.PRODUCER,
            expected=expected_producer_session,
        )
        if expected_reviewer_session is not None:
            _require_prepared_binding(
                transaction_pool,
                workspace_id=workspace_id,
                task_id=task_id,
                role=WorkflowRole.REVIEWER,
                expected=expected_reviewer_session,
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
            if expected_review_iteration is None:
                raise InvalidCommandError(
                    "revision finalization requires the routed review iteration"
                )
            _require_unsuperseded_review_iteration(
                transaction_pool,
                workspace_id=workspace_id,
                task_id=task_id,
                expected_review_iteration=expected_review_iteration,
                prior_revision=prior_revision,
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
        # The F2 claim settles exactly once with the atomic resolution: the
        # validated formal result completed the operation. A rejected
        # settlement rolls the whole composition back, leaving the claim
        # conservatively claimed.
        settle_send_claim_for_plan_revision(
            transaction_pool,
            claim=send_claim,
            workspace_id=workspace_id,
            task_id=task_id,
            plan_revision_id=revision.id,
        )
    return TaskPlanProductionOutcome(plan_revision=revision, task=updated)


def _validate_initial_production_subjects(
    transaction_pool: DatabasePool,
    *,
    workspace_id: UUID,
    task_id: UUID,
    profile_id: UUID,
    preparation: TaskPlanningPreparationOutcome,
) -> tuple[
    Task, TaskAgentSession, TaskAgentSession, Workspace, ResolvedRepositoryInstallationRoute
]:
    """The shared entry subject/session validation of the initial production.

    Both the blocking send and the detached dispatch (issue #335) re-derive
    the exact same subjects in one short database-only transaction: the
    account operational barrier, the exact Owner Workspace/Task scope, the
    exact current ``planning`` Task under the prepared outcome's post-claim
    token with no already-current PlanRevision, both exact READY prepared
    bindings identity-matched against the outcome, the Workspace (for the
    canonical instruction's guidance), and the durable Repository →
    installation route for the repository-base observation.
    """
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
    reviewer = _require_prepared_binding(
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
    return task, producer, reviewer, workspace, route


def _validate_revision_production_subjects(
    transaction_pool: DatabasePool,
    *,
    workspace_id: UUID,
    task_id: UUID,
    profile_id: UUID,
    expected_state_token: UUID,
    prior_plan_revision_id: UUID,
    review_iteration_id: UUID,
) -> tuple[
    Task,
    planning_records.PlanRevision,
    ReviewIteration,
    TaskAgentSession,
    ResolvedRepositoryInstallationRoute,
]:
    """The shared entry subject/session validation of the revision production.

    Both the blocking send and the detached dispatch (issue #335) re-derive
    the exact same subjects in one short database-only transaction: the
    account operational barrier, the exact Owner Workspace/Task scope, the
    exact current ``planning`` Task under the caller's token, the exact
    current prior PlanRevision, the finalized ``CHANGES_REQUESTED`` planning
    ReviewIteration still addressing it as its latest-numbered iteration
    (issue #207), the exact READY Producer binding, and the durable
    Repository → installation route for the repository-base observation.
    """
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
    return task, prior_revision, iteration, producer, route


def _require_latest_routed_iteration(
    iterations: Sequence[ReviewIteration],
    *,
    prior_revision: planning_records.PlanRevision,
    iteration: ReviewIteration,
) -> None:
    """Require the routed iteration to be the latest one on the exact revision.

    Supersession (issue #207) is decided by the loop's ordered, unique
    per-loop numbering: only the latest-numbered iteration bound to the
    exact prior PlanRevision may dispatch a Producer revision. A newer
    ACCEPTED result, a newer change request, or a newer unresolved attempt
    on the same plan supersedes an older finalized change request even
    though both records are individually valid and the caller's Task token
    is current; the superseded result fails closed with the typed stale
    error and can never reach the Producer or publish a PlanRevision.
    """
    for later in iterations:
        if (
            later.iteration_number > iteration.iteration_number
            and later.plan_revision_id == prior_revision.id
        ):
            raise StaleOperationError(
                "the review iteration has been superseded on the current plan "
                "revision by a newer review iteration; the continuation is stale"
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
    meaning. It must belong to this Task's still-OPEN planning-purpose
    ReviewLoop, be finalized ``CHANGES_REQUESTED`` (the complete result-fact
    set), and address exactly the Task's current prior revision — an
    ACCEPTED, unfinalized, non-planning, closed-loop, or superseded-subject
    iteration is never routed. It must additionally be the latest-numbered
    iteration addressing that exact revision in its loop (issue #207): a
    newer ACCEPTED result, a newer change request, or a newer unresolved
    attempt on the same plan supersedes an older finalized change request,
    and the superseded result is never dispatched to the Producer even when
    both results are individually valid and the caller's Task token is
    current. The findings are never taken from caller input.
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
    if loop.status is not ReviewLoopStatus.OPEN:
        raise ConflictError(
            "the planning review loop is no longer open; revision production "
            "routes change requests from the open planning loop only"
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
    _require_latest_routed_iteration(
        review_records.list_review_loop_iterations(transaction_pool, review_loop_id=loop.id),
        prior_revision=prior_revision,
        iteration=iteration,
    )
    return iteration


def _require_unsuperseded_review_iteration(
    transaction_pool: DatabasePool,
    *,
    workspace_id: UUID,
    task_id: UUID,
    expected_review_iteration: ReviewIteration,
    prior_revision: planning_records.PlanRevision,
) -> None:
    """Revalidate the routed change request inside the finalize transaction.

    The same condition proven at entry is rechecked after the external send
    (issue #207): the loop row is re-read ``FOR UPDATE`` — the lock
    ``create_review_iteration`` takes, so concurrent iteration creation
    serializes behind this composition instead of committing between the
    recheck and the revision publication — and the loop's ordered iterations
    must still name the routed change request the latest-numbered one for
    the exactly-current prior PlanRevision. A result finalized during the
    send fails closed with the typed stale error and publishes nothing.
    """
    loop = review_records.get_review_loop(
        transaction_pool,
        review_loop_id=expected_review_iteration.review_loop_id,
        for_update=True,
    )
    if loop is None or loop.workspace_id != workspace_id or loop.task_id != task_id:
        raise NotFoundError("the requested review loop is not available for this task")
    if loop.purpose is not ReviewLoopPurpose.PLANNING:
        raise ConflictError(
            "the review iteration is not a planning review; revision production "
            "routes planning change requests only"
        )
    if loop.status is not ReviewLoopStatus.OPEN:
        raise ConflictError(
            "the planning review loop is no longer open; revision production "
            "routes change requests from the open planning loop only"
        )
    _require_latest_routed_iteration(
        review_records.list_review_loop_iterations(transaction_pool, review_loop_id=loop.id),
        prior_revision=prior_revision,
        iteration=expected_review_iteration,
    )


def settle_resumed_task_record(
    transaction_pool: DatabasePool,
    *,
    task_resume_intent_id: UUID,
) -> None:
    """Settle the resumed operation's record atomically with its completion.

    The F8g one-shot discipline's final transition: the resumed turn's
    result was durably processed by its original workflow owner in this
    same composition, so the record settles exactly once here. A raced or
    already-terminal record is a durable invariant failure that rolls the
    completion back rather than fabricating a second outcome.
    """
    recorded = record_task_resume_intent_outcome(
        transaction_pool,
        task_resume_intent_id=task_resume_intent_id,
        status=TaskResumeIntentStatus.SETTLED,
        outcome={"result": "processed"},
    )
    if recorded is None:
        raise StaleOperationError(
            "the session-resume record is no longer awaiting its settled "
            "outcome; the completion composition is stale"
        )


# --- the detached plan-result production dispatch (issues #325/#335) -----------


ProducerRuntimeAttachmentFactory = Callable[[], AbstractContextManager[AgentRuntimeAdapter]]


class PlanProductionObservationRegistrar(Protocol):
    """The structural registration seam of the process's detached-turn observer.

    The same positively-required pre-admission gate the detached
    establishment and implementation dispatches consume (issues
    #323/#333/#334): the plan dispatch registers the exact bound external
    session before any admission, so an unreplayable terminal ``plan_result``
    can never be produced without active observation. Failures are the
    observer's typed refusals, routed through the pre-effect failure
    machinery.
    """

    def register_session(
        self,
        *,
        workspace_id: UUID,
        task_id: UUID,
        task_agent_session_id: UUID,
        connection_id: UUID,
        external_session_id: str,
    ) -> None:
        """Register one exact bound session for detached-turn observation."""
        ...


@dataclass(frozen=True, slots=True)
class DispatchedPlanProduction:
    """The typed detached plan-production dispatch outcome (issues #325/#335).

    The authoritative facts exactly as the durable state reads after the
    dispatch: the post-claim Task (its token unrotated by the claim), the
    committed one-shot send claim with its recorded repository-base context,
    the claimed detached-turn receipt companion, and the exact Producer
    binding the canonical instruction was admitted on. No runtime result, no
    validated ``plan_result``, and no published PlanRevision exists yet: the
    independently observed completion owns the settlement, and the ONE
    post-commit completion handoff owns the follow-on planning REVIEW.
    """

    task: Task
    send_claim: TaskRuntimeSendClaim
    receipt: TaskDetachedTurnReceipt
    producer_session: TaskAgentSession


# Safe refusal-kind tokens for a receipt's durable refusal outcome after a
# proven pre-effect detached-dispatch failure (no exception text or error
# content ever enters durable state).
_REGISTRATION_REFUSAL_KIND: Final[str] = "observation_registration_refused"
_ADMISSION_REFUSAL_KIND: Final[str] = "admission_refused"

_DISPATCH_INITIAL_SPAN_NAME: Final[str] = "task_plan_production.dispatch_initial_plan_detached"
_DISPATCH_REVISE_SPAN_NAME: Final[str] = "task_plan_production.dispatch_planning_revise_detached"


def _refuse_plan_dispatch_quietly(
    pool: DatabasePool,
    receipt: TaskDetachedTurnReceipt,
    send_claim: TaskRuntimeSendClaim,
    *,
    refusal_kind: str,
) -> None:
    """Mark a claimed plan receipt refused and its claim known-nondelivery.

    Best-effort containment after a positively proven pre-effect failure: a
    persistence failure here must never mask the typed refusal the caller
    raises. The claimed receipt stays the honest unresolved state under the
    accepted v1 recovery bounds and the failure is logged selectively.
    """
    try:
        refused = refuse_task_detached_turn_receipt(
            pool,
            receipt_id=receipt.id,
            task_id=receipt.task_id,
            workspace_id=receipt.workspace_id,
            refusal_kind=refusal_kind,
        )
    except Exception as error:  # noqa: BLE001 - containment, never masking
        logger.warning(
            "the refused plan-dispatch receipt could not be durably marked (%s); "
            "the claimed receipt stays the honest unresolved state",
            type(error).__name__,
        )
        refused = None
    if refused is None:  # pragma: no cover - a claimed receipt refuses once
        logger.warning(
            "the plan-dispatch receipt was no longer claimed when its pre-effect "
            "refusal was recorded"
        )
    try:
        record_send_claim_known_non_delivery(
            pool,
            claim=send_claim,
            workspace_id=send_claim.workspace_id,
            task_id=send_claim.task_id,
            failure_kind=RUNTIME_PRE_EFFECT_FAILURE_KIND,
        )
    except Exception as error:  # noqa: BLE001 - containment, never masking
        logger.warning(
            "the refused plan-dispatch claim could not be durably marked (%s); "
            "the claimed claim stays the honest unresolved state",
            type(error).__name__,
        )


def _dispatch_plan_turn(
    pool: DatabasePool,
    *,
    workspace_id: UUID,
    task_id: UUID,
    producer: TaskAgentSession,
    operation: PendingTaskRuntimeInteractionOperation,
    compose_claim_and_receipt: Callable[
        [DatabasePool], tuple[TaskRuntimeSendClaim, TaskDetachedTurnReceipt]
    ],
    message: str,
    producer_runtime_factory: ProducerRuntimeAttachmentFactory,
    observation_registrar: PlanProductionObservationRegistrar,
    completion_handoff: DetachedTurnCompletionHandoff,
    attempt_evidence: InvocationAttemptEvidence | None,
) -> DispatchedPlanProduction:
    """Admit one canonical plan instruction as one detached agent turn.

    The shared detached-dispatch mechanics of both plan families (issue
    #335), mirroring the established detached-dispatch shape: the runtime
    lease and the detached capability are acquired and duck-typed fail-closed
    BEFORE any durable dispatch effect, the exact one-shot claim (with its
    observed repository-base context) and the detached-turn receipt companion
    commit in ONE short transaction strictly before any runtime effect, one
    positively-required observation registration gates the admission, one
    detached-turn admission carries the composed canonical instruction
    verbatim, and the acknowledgement attach consumes any staged early
    completion exactly once. No ``send`` and no blocking wait exists in this
    path, and no database transaction is ever held across a runtime call.

    Proven pre-effect failures refuse the receipt and record the claim's
    known non-delivery, then raise the typed operational-failure classes the
    F5b coordinator triages under the claim's own family. An uncertain
    admission leaves the claim and receipt conservatively claimed and is
    never re-dispatched. The dispatching worker returns after the
    acknowledgement attach; the observed completion owns the settlement.
    """
    producer_external_session_id = producer.external_session_id
    if producer_external_session_id is None or not producer_external_session_id.strip():
        raise ConflictError("the producer session binding carries no external session identity")
    register = getattr(observation_registrar, "register_session", None)
    if not callable(register):
        raise InvalidCommandError(
            "the plan-production observation registrar does not implement the "
            "established-session registration seam"
        )
    with ExitStack() as attachment_stack:
        runtime = attachment_stack.enter_context(producer_runtime_factory())
        # The detached capability is duck-typed fail-closed BEFORE any durable
        # dispatch effect: a runtime without it cannot serve the detached
        # dispatch, and no claim or receipt was ever recorded.
        admit = getattr(runtime, "admit_detached_turn", None)
        if not callable(admit):
            raise ExternalOperationFailedError(
                "the producer runtime does not implement the detached turn "
                "capability; the plan dispatch failed before any effect"
            )
        admit_detached_turn = cast(Callable[[str, str], AgentTurnAdmission], admit)

        # --- The one short dispatch transaction: the exact pre-send claim
        # (with its observed repository-base context) and the detached-turn
        # receipt companion, committed atomically strictly before any runtime
        # effect. A replayed or raced continuation never claims or dispatches
        # again: the claim's one-shot subject guard and the
        # single-outstanding-receipt invariant are durable from here. ---
        with composed_transaction(pool) as transaction_pool:
            send_claim, receipt = compose_claim_and_receipt(transaction_pool)
        if attempt_evidence is not None:
            # The F5b coordinator's handoff fact: this invocation's committed
            # dispatch claim identity (#262).
            attempt_evidence.record_attempt(
                family=operation,
                original_attempt_kind=OriginalAttemptKind.RUNTIME_SEND_CLAIM,
                original_attempt_id=send_claim.id,
                phase=operation.value,
            )

        # --- The positive pre-admission observation registration: the
        # positively-required gate. A refusal is a proven pre-effect failure:
        # the just-recorded receipt is marked refused and the claim records
        # its known non-delivery, so a never-dispatched turn cannot hold the
        # session's single-outstanding slot. ---
        try:
            register(
                workspace_id=workspace_id,
                task_id=task_id,
                task_agent_session_id=producer.id,
                connection_id=producer.connection_id,
                external_session_id=producer_external_session_id,
            )
        except ObservationRegistrationRefusedError as error:
            _refuse_plan_dispatch_quietly(
                pool, receipt, send_claim, refusal_kind=_REGISTRATION_REFUSAL_KIND
            )
            raise ExternalOperationFailedError(
                "the plan-production observation registration was refused before "
                "any admission; the instruction never dispatched"
            ) from error
        except Exception as error:
            from openorc.services.task_detached_turn_observation import (  # noqa: PLC0415
                ObservationRegistrationRefusalReason,
            )

            _refuse_plan_dispatch_quietly(
                pool, receipt, send_claim, refusal_kind=_REGISTRATION_REFUSAL_KIND
            )
            raise ExternalOperationFailedError(
                "the plan-production observation registration failed "
                f"({ObservationRegistrationRefusalReason.OBSERVATION_REGISTRATION_FAILED.value}) "
                "before any admission; the instruction never dispatched"
            ) from error

        # --- The admission (external, no transaction held). One
        # detached-turn admission of the composed canonical instruction
        # verbatim. ---
        try:
            admission = admit_detached_turn(producer_external_session_id, message)
        except AgentRuntimeUncertainOutcomeError as error:
            logger.warning(
                "the detached plan-production admission ended with an uncertain "
                "outcome; the claim and receipt stay conservatively claimed "
                "and are never re-dispatched"
            )
            raise ExternalOperationUncertainError(
                "the outcome of the plan-production admission is unknown; the "
                "claim and receipt stay conservatively claimed and the "
                "instruction is never re-dispatched"
            ) from error
        except AgentRuntimeError as error:
            # A positively identified refusal (unreachable before effect,
            # runtime rejection, exact-session absence): proven pre-effect,
            # so the receipt is marked refused and the dispatch fails known.
            _refuse_plan_dispatch_quietly(
                pool, receipt, send_claim, refusal_kind=_ADMISSION_REFUSAL_KIND
            )
            raise ExternalOperationRejectedError(
                "the plan-production admission was refused before any effect; "
                "the receipt is refused and the instruction never dispatched"
            ) from error

        # --- The acknowledgement attach (one short transaction). A staged
        # early completion is consumed exactly once here and routed through
        # the ordinary settlement, so a validated plan result that finished
        # before the acknowledgement was persisted is correlated, never
        # ignored or duplicated. ---
        try:
            outcome: turn_correlation.TurnSettlementOutcome = (
                turn_correlation.attach_acknowledged_turn(
                    pool,
                    workspace_id=workspace_id,
                    task_id=task_id,
                    receipt_id=receipt.id,
                    admitted_turn_reference=admission.turn_reference,
                )
            )
        except ApplicationError:
            raise
        except Exception as error:
            raise ExternalOperationFailedError(
                "the plan-production acknowledgement could not be durably "
                "attached after its known admission; authoritative recovery "
                "is required and the instruction is never re-dispatched"
            ) from error

    # The attachment is released; the handoff is strictly post-dispatch and
    # never holds the runtime lease.
    if outcome.kind is turn_correlation.TurnSettlementKind.SETTLED_VALIDATED_RESULT:
        # A staged early completion settled the claim and the receipt within
        # this dispatch (issues #324/#333/#334): the ONE post-commit
        # completion handoff owns the family routing. Strictly post-commit,
        # contained, never a workflow authority; a failed routing leaves the
        # durable state to the existing recovery semantics.
        try:
            completion_handoff.handoff_settled_completion(
                workspace_id=workspace_id,
                task_id=task_id,
                operation=operation,
            )
        except Exception as error:  # noqa: BLE001 - contained handoff
            logger.warning(
                "the post-dispatch plan-production completion handoff failed "
                "(%s); the durable state owns recovery",
                type(error).__name__,
            )
    if (
        outcome.kind is turn_correlation.TurnSettlementKind.SETTLED_INTERRUPTED_TURN
        and outcome.receipt is not None
    ):
        # A staged interrupted completion settled the receipt within this
        # dispatch (issue #342): the ONE post-commit completion handoff owns
        # the Interrupt settlement — the attached adapter's D6b guard
        # release and the annotated intent's outcome recording — exactly as
        # it owns the validated families' continuations. Strictly
        # post-attach, contained best-effort inside the handoff, never a
        # workflow authority; a failed settlement leaves the durable
        # annotated intent exactly as committed.
        completion_handoff.handoff_settled_interrupt(
            workspace_id=workspace_id,
            task_id=task_id,
            task_agent_session_id=outcome.receipt.task_agent_session_id,
            external_session_id=producer_external_session_id,
        )
    reloaded_task = task_records.get_task(pool, task_id=task_id)
    if reloaded_task is None or reloaded_task.workspace_id != workspace_id:
        raise NotFoundError("the requested task is not available in this workspace")
    return DispatchedPlanProduction(
        task=reloaded_task,
        send_claim=send_claim,
        receipt=outcome.receipt if outcome.receipt is not None else receipt,
        producer_session=producer,
    )


def dispatch_initial_plan_detached(
    pool: DatabasePool,
    github: GitHubAppClient,
    *,
    workspace_id: UUID,
    task_id: UUID,
    profile_id: UUID,
    preparation: TaskPlanningPreparationOutcome,
    producer_runtime_factory: ProducerRuntimeAttachmentFactory,
    observation_registrar: PlanProductionObservationRegistrar,
    completion_handoff: DetachedTurnCompletionHandoff,
    attempt_evidence: InvocationAttemptEvidence | None = None,
) -> DispatchedPlanProduction:
    """Dispatch the first canonical PLAN as one detached agent turn.

    The detached counterpart of the ordinary initial plan production (issues
    #325/#335): the same entry validation, the same authoritative
    repository-base observation, and the same one-shot INITIAL_PLAN_DISPATCH
    claim — now committed together with its detached-turn receipt companion
    and the observed repository-base context — followed by the canonical
    ``PlanInteraction`` admitted verbatim as one independently completed
    agent turn on the exact prepared Producer session in its planning
    checkout. No ``send`` and no blocking wait exists in this path, and the
    dispatching worker returns after the acknowledgement attach. The
    independently observed completion settles the validated ``plan_result``
    through :func:`settle_observed_plan_result`, and the ONE post-commit
    completion handoff submits the existing typed planning REVIEW from
    committed state.
    """
    with application_span(_SERVICE_TRACER_SCOPE, _DISPATCH_INITIAL_SPAN_NAME) as span:
        _require_uuid_command(workspace_id, "workspace_id")
        _require_uuid_command(task_id, "task_id")
        _require_uuid_command(profile_id, "profile_id")
        preparation = _require_preparation(preparation)
        annotate_span(
            span,
            operation=_DISPATCH_INITIAL_SPAN_NAME,
            workspace_id=str(workspace_id),
            task_id=str(task_id),
        )

        # --- Phase 1: entry subject/session validation (database only). ---
        with composed_transaction(pool) as transaction_pool:
            task, producer, reviewer, workspace, route = _validate_initial_production_subjects(
                transaction_pool,
                workspace_id=workspace_id,
                task_id=task_id,
                profile_id=profile_id,
                preparation=preparation,
            )

        # --- Phase 2: authoritative repository-base audit observation. ---
        repository_observation, base_sha = _observe_repository_base(github, route)

        def compose_claim_and_receipt(
            transaction_pool: DatabasePool,
        ) -> tuple[TaskRuntimeSendClaim, TaskDetachedTurnReceipt]:
            send_claim = claim_initial_plan_send(
                transaction_pool,
                workspace_id=workspace_id,
                task_id=task_id,
                expected_state_token=task.state_token,
                producer=producer,
                repository_base_sha=base_sha,
            )
            receipt = turn_correlation.record_task_detached_turn_receipt(
                transaction_pool,
                workspace_id=workspace_id,
                task_id=task_id,
                task_agent_session_id=producer.id,
                operation=PendingTaskRuntimeInteractionOperation.INITIAL_PLAN_DISPATCH,
                original_attempt_kind=OriginalAttemptKind.RUNTIME_SEND_CLAIM,
                original_attempt_id=send_claim.id,
                expected_formal_family=PLAN_RESULT_FAMILY,
            )
            return send_claim, receipt

        return _dispatch_plan_turn(
            pool,
            workspace_id=workspace_id,
            task_id=task_id,
            producer=producer,
            operation=PendingTaskRuntimeInteractionOperation.INITIAL_PLAN_DISPATCH,
            compose_claim_and_receipt=compose_claim_and_receipt,
            message=_initial_plan_message(task, workspace, repository_observation),
            producer_runtime_factory=producer_runtime_factory,
            observation_registrar=observation_registrar,
            completion_handoff=completion_handoff,
            attempt_evidence=attempt_evidence,
        )


def dispatch_planning_revise_detached(
    pool: DatabasePool,
    github: GitHubAppClient,
    *,
    workspace_id: UUID,
    task_id: UUID,
    profile_id: UUID,
    expected_state_token: UUID,
    prior_plan_revision_id: UUID,
    review_iteration_id: UUID,
    producer_runtime_factory: ProducerRuntimeAttachmentFactory,
    observation_registrar: PlanProductionObservationRegistrar,
    completion_handoff: DetachedTurnCompletionHandoff,
    attempt_evidence: InvocationAttemptEvidence | None = None,
) -> DispatchedPlanProduction:
    """Dispatch one routed planning REVISE as one detached agent turn.

    The detached counterpart of the ordinary revision production (issues
    #325/#335): the same entry validation of the exact current prior
    PlanRevision and its finalized ``CHANGES_REQUESTED`` planning
    ReviewIteration (still the latest-numbered one addressing that exact
    revision in the still-OPEN loop, issue #207), the same authoritative
    repository-base observation, and the same one-shot
    PLANNING_REVISE_DISPATCH claim — now committed together with its
    detached-turn receipt companion and the observed repository-base
    context — followed by the Reviewer's **persisted** findings, rebuilt
    verbatim from durable state and structurally serialized, admitted as one
    independently completed agent turn on the same Producer session. No
    in-memory continuation object crosses the dispatch, no ``send`` and no
    blocking wait exists in this path, and the dispatching worker returns
    after the acknowledgement attach.
    """
    with application_span(_SERVICE_TRACER_SCOPE, _DISPATCH_REVISE_SPAN_NAME) as span:
        _require_uuid_command(workspace_id, "workspace_id")
        _require_uuid_command(task_id, "task_id")
        _require_uuid_command(profile_id, "profile_id")
        _require_uuid_command(expected_state_token, "expected_state_token")
        _require_uuid_command(prior_plan_revision_id, "prior_plan_revision_id")
        _require_uuid_command(review_iteration_id, "review_iteration_id")
        annotate_span(
            span,
            operation=_DISPATCH_REVISE_SPAN_NAME,
            workspace_id=str(workspace_id),
            task_id=str(task_id),
        )

        # --- Phase 1: entry subject/session validation (database only). ---
        with composed_transaction(pool) as transaction_pool:
            task, _prior_revision, iteration, producer, route = (
                _validate_revision_production_subjects(
                    transaction_pool,
                    workspace_id=workspace_id,
                    task_id=task_id,
                    profile_id=profile_id,
                    expected_state_token=expected_state_token,
                    prior_plan_revision_id=prior_plan_revision_id,
                    review_iteration_id=review_iteration_id,
                )
            )

        # --- Phase 2: authoritative repository-base audit observation. ---
        repository_observation, base_sha = _observe_repository_base(github, route)
        review_result = _review_result_from_iteration(iteration)
        message = serialize_review_result(review_result)

        def compose_claim_and_receipt(
            transaction_pool: DatabasePool,
        ) -> tuple[TaskRuntimeSendClaim, TaskDetachedTurnReceipt]:
            send_claim = claim_planning_revise_send(
                transaction_pool,
                workspace_id=workspace_id,
                task_id=task_id,
                expected_state_token=task.state_token,
                producer=producer,
                review_iteration=iteration,
                repository_base_sha=base_sha,
            )
            receipt = turn_correlation.record_task_detached_turn_receipt(
                transaction_pool,
                workspace_id=workspace_id,
                task_id=task_id,
                task_agent_session_id=producer.id,
                operation=PendingTaskRuntimeInteractionOperation.PLANNING_REVISE_DISPATCH,
                original_attempt_kind=OriginalAttemptKind.RUNTIME_SEND_CLAIM,
                original_attempt_id=send_claim.id,
                expected_formal_family=PLAN_RESULT_FAMILY,
            )
            return send_claim, receipt

        return _dispatch_plan_turn(
            pool,
            workspace_id=workspace_id,
            task_id=task_id,
            producer=producer,
            operation=PendingTaskRuntimeInteractionOperation.PLANNING_REVISE_DISPATCH,
            compose_claim_and_receipt=compose_claim_and_receipt,
            message=message,
            producer_runtime_factory=producer_runtime_factory,
            observation_registrar=observation_registrar,
            completion_handoff=completion_handoff,
            attempt_evidence=attempt_evidence,
        )


# --- the detached settlement's owning-service finalization (issue #335) -------


def settle_observed_plan_result(
    transaction_pool: DatabasePool,
    *,
    workspace_id: UUID,
    task_id: UUID,
    send_claim: TaskRuntimeSendClaim,
    result: PlanResult,
) -> TaskPlanProductionOutcome | None:
    """Finalize one observed plan result through the owning service.

    The detached-run settlement's owning-service composition (issue #335):
    the correlation seam funnels the attributed terminal observation through
    the shared formal contract and hands the validated ``PlanResult`` here
    inside its own settlement transaction. The settlement re-derives every
    subject from durable state — the exact current ``planning`` Task, the
    exact READY Producer binding the claim addressed (and, for the initial
    family, the exact READY Reviewer binding), and for the revise family the
    routed finalized change request still addressing the still-current prior
    revision as its latest-numbered iteration — and reuses the one shared
    commit composition: the next immutable PlanRevision created from the
    claim's recorded repository-base context, the current-pointer and token
    rotation, the audit event, and the send-claim settlement, all atomic.
    ``None`` means the subject set has moved on: the observation can no
    longer apply, nothing is rewritten, and the existing recovery semantics
    own the conservative residue.
    """
    if not isinstance(result, PlanResult):
        raise InvalidCommandError("settle_observed_plan_result requires a PlanResult")
    if not isinstance(send_claim, TaskRuntimeSendClaim):
        raise InvalidCommandError("settle_observed_plan_result requires a send claim")
    try:
        if send_claim.kind is TaskRuntimeSendClaimKind.INITIAL_PLAN_DISPATCH:
            _require_claim_dispatch_base_sha(send_claim)
            return _settle_observed_initial_plan_result(
                transaction_pool,
                workspace_id=workspace_id,
                task_id=task_id,
                send_claim=send_claim,
                result=result,
            )
        if send_claim.kind is TaskRuntimeSendClaimKind.PLANNING_REVISE_DISPATCH:
            _require_claim_dispatch_base_sha(send_claim)
            return _settle_observed_plan_revision_result(
                transaction_pool,
                workspace_id=workspace_id,
                task_id=task_id,
                send_claim=send_claim,
                result=result,
            )
        raise InvalidCommandError(
            "the addressed send claim kind carries no detached plan-result settlement"
        )
    except (NotFoundError, ConflictError, StaleOperationError):
        # The subject set moved on between the dispatch and the observed
        # completion (a rotated token, a lost session, a superseded change
        # request, a competing revision): the validated result can no longer
        # apply, so nothing is rewritten and the receipt settles as
        # result-not-applied. Never a fabricated revision, never a rewrite
        # of moved-on history.
        return None


def _require_claim_dispatch_base_sha(send_claim: TaskRuntimeSendClaim) -> str:
    """Require the claim's recorded repository-base dispatch context (#335).

    A claimed plan-kind claim without the fact is impossible for any receipt
    this settlement can attribute (the receipt is created only by the
    dispatch that recorded it) — an unexpected durable-state invariant
    failure that fails closed instead of fabricating audit context.
    """
    if send_claim.dispatch_repository_base_sha is None:
        raise InvalidCommandError(
            "the claimed plan dispatch carries no recorded repository-base "
            "context; its settlement cannot establish the revision's audit context"
        )
    return send_claim.dispatch_repository_base_sha


def _require_settlement_binding(
    transaction_pool: DatabasePool,
    *,
    workspace_id: UUID,
    task_id: UUID,
    role: WorkflowRole,
    task_agent_session_id: UUID | None = None,
) -> TaskAgentSession:
    """Re-derive one exact settlement binding from durable state.

    The binding must be the exact Task/role row — for the Producer, the
    exact claim-addressed session — still READY with an external identity:
    session loss and explicit ends do not rotate the Task token, so the
    token alone cannot prove the session usable at settlement time. A
    missing, moved, or non-READY binding is a moved-on subject (the
    caller's ``None`` translation), never a replacement session.
    """
    binding = get_task_agent_session(transaction_pool, task_id=task_id, role=role)
    if binding is None or binding.workspace_id != workspace_id or binding.task_id != task_id:
        raise NotFoundError(
            f"the claimed {role.value} session binding is not available for this task"
        )
    if task_agent_session_id is not None and binding.id != task_agent_session_id:
        raise ConflictError(
            f"the durable {role.value} session binding does not match the claimed "
            "dispatch subject; the observed plan result can no longer apply"
        )
    if binding.lifecycle_status is not TaskSessionLifecycleStatus.READY:
        raise ConflictError(
            f"the claimed {role.value} session binding is not READY; the observed "
            "plan result can no longer apply to the moved-on session"
        )
    if binding.external_session_id is None:
        raise ConflictError(f"the {role.value} session binding has no external session identity")
    return binding


def _settle_observed_initial_plan_result(
    transaction_pool: DatabasePool,
    *,
    workspace_id: UUID,
    task_id: UUID,
    send_claim: TaskRuntimeSendClaim,
    result: PlanResult,
) -> TaskPlanProductionOutcome:
    """Finalize the observed initial PLAN result as the Task's first revision.

    The exact current ``planning`` Task must still carry no current plan
    revision, and both the claimed Producer binding and the Task's current
    Reviewer binding must still be READY: the same prepared-session
    recheck the ordinary finalize composition performs.
    """
    task = task_records.get_task(transaction_pool, task_id=task_id)
    if task is None or task.workspace_id != workspace_id:
        raise NotFoundError("the requested task is not available in this workspace")
    if task.status is not TaskStatus.PLANNING or task.current_plan_revision_id is not None:
        raise StaleOperationError(
            "the task is no longer an unproduced planning subject; the observed "
            "initial plan result can no longer apply"
        )
    producer = _require_settlement_binding(
        transaction_pool,
        workspace_id=workspace_id,
        task_id=task_id,
        role=WorkflowRole.PRODUCER,
        task_agent_session_id=send_claim.task_agent_session_id,
    )
    reviewer = _require_settlement_binding(
        transaction_pool,
        workspace_id=workspace_id,
        task_id=task_id,
        role=WorkflowRole.REVIEWER,
    )
    return _commit_new_plan_revision_transaction(
        transaction_pool,
        workspace_id=workspace_id,
        task_id=task_id,
        expected_state_token=task.state_token,
        plan_text=result.plan,
        repository_base_sha=_require_claim_dispatch_base_sha(send_claim),
        actor=_producer_actor(producer),
        event_type=WorkflowEventType.PLAN_REVISION_CREATED,
        prior_revision=None,
        expected_review_iteration=None,
        expected_producer_session=producer,
        expected_reviewer_session=reviewer,
        send_claim=send_claim,
    )


def _settle_observed_plan_revision_result(
    transaction_pool: DatabasePool,
    *,
    workspace_id: UUID,
    task_id: UUID,
    send_claim: TaskRuntimeSendClaim,
    result: PlanResult,
) -> TaskPlanProductionOutcome:
    """Finalize the observed REVISE result as the Task's next revision.

    The claim's routed finalized change request is re-derived from durable
    state — never from any continuation object — and must still address the
    Task's exact current prior revision as its latest-numbered iteration in
    the still-OPEN planning loop (issue #207): a superseded result can never
    publish a revision.
    """
    task = task_records.get_task(transaction_pool, task_id=task_id)
    if task is None or task.workspace_id != workspace_id:
        raise NotFoundError("the requested task is not available in this workspace")
    if task.status is not TaskStatus.PLANNING:
        raise StaleOperationError(
            "the task is no longer in its coarse planning state; the observed "
            "revision result can no longer apply"
        )
    producer = _require_settlement_binding(
        transaction_pool,
        workspace_id=workspace_id,
        task_id=task_id,
        role=WorkflowRole.PRODUCER,
        task_agent_session_id=send_claim.task_agent_session_id,
    )
    assert send_claim.review_iteration_id is not None  # narrowed by the kind's subject form
    prior_revision = require_current_plan_revision(
        transaction_pool,
        task=task,
        plan_revision_id=_require_routed_iteration_revision(
            transaction_pool, send_claim=send_claim
        ),
    )
    iteration = _require_routed_review_iteration(
        transaction_pool,
        workspace_id=workspace_id,
        task_id=task_id,
        review_iteration_id=send_claim.review_iteration_id,
        prior_revision=prior_revision,
    )
    return _commit_new_plan_revision_transaction(
        transaction_pool,
        workspace_id=workspace_id,
        task_id=task_id,
        expected_state_token=task.state_token,
        plan_text=result.plan,
        repository_base_sha=_require_claim_dispatch_base_sha(send_claim),
        actor=_producer_actor(producer),
        event_type=WorkflowEventType.PLAN_REVISION_REVISED,
        prior_revision=prior_revision,
        expected_review_iteration=iteration,
        expected_producer_session=producer,
        expected_reviewer_session=None,
        send_claim=send_claim,
    )


def _require_routed_iteration_revision(
    transaction_pool: DatabasePool,
    *,
    send_claim: TaskRuntimeSendClaim,
) -> UUID:
    """Re-derive the prior revision the claimed change request addresses.

    The persisted ReviewIteration is the only authority for the revision its
    change request addresses (issue #335): the settlement never trusts any
    continuation object, and a moved or missing iteration is a moved-on
    subject the caller translates to the result-not-applied settlement.
    """
    assert send_claim.review_iteration_id is not None  # narrowed by the kind's subject form
    iteration = review_records.get_review_iteration(
        transaction_pool, review_iteration_id=send_claim.review_iteration_id
    )
    if (
        iteration is None
        or iteration.workspace_id != send_claim.workspace_id
        or iteration.task_id != send_claim.task_id
        or iteration.plan_revision_id is None
    ):
        raise NotFoundError(
            "the claimed change request's review iteration is not available for this task"
        )
    return iteration.plan_revision_id
