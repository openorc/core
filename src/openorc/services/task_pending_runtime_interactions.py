"""Runtime-neutral pending-interaction projection (F2, issue #87).

The small read-only projection of one Task's current outstanding
consequential automated runtime interactions — the exact facts the
stall-observation layer (F2a, issue #238) enumerates without any
runtime-status probe. The projection composes *existing* durable claim
identities plus the F2 runtime-send claims:

- the E1b planning-bootstrap claim with its F2 stage evidence (an
  outstanding PREPARE/control, an outstanding planning-context transition
  after the validated PREPARE result, vs a continuation waiting for E2);
- the F2 pre-send claims (initial PLAN, planning REVISE, PR_COMPOSE) still
  in their claimed state;
- the E3a reserved unfinalized ReviewIteration (the outstanding REVIEW
  dispatch);
- the E6/E9 RUNNING Execution claims (the outstanding implementation or
  remediation attempt).

Eligibility is deliberately conservative and current-work-only: an
operation is listed only while its exact subject is still the Task's
current work on a non-archived, non-terminal Task — a settled or
known-non-delivered claim, a superseded or resolved attempt, a moved-on
plan pointer, or a READY session merely waiting for an Owner approval or
another workflow step is never listed. The projection makes no
runtime-status probe, decides no block, and normalizes no activity
observation: ``WORKING``/``NOT_WORKING``/``UNKNOWN`` classification is
F2a's separate adapter-capability work, and block/recovery decisions are
F4's. Transport-independent by construction.
"""

from __future__ import annotations

from datetime import UTC, datetime
from uuid import UUID

from openorc.domain.connections import WorkflowRole
from openorc.domain.executions import ExecutionStatus
from openorc.domain.pending_interactions import (
    PendingTaskRuntimeInteraction,
    PendingTaskRuntimeInteractionOperation,
)
from openorc.domain.reviews import ReviewLoopPurpose, ReviewLoopStatus
from openorc.domain.send_claims import (
    TaskRuntimeSendClaim,
    TaskRuntimeSendClaimKind,
    TaskRuntimeSendClaimStatus,
)
from openorc.domain.tasks import Task, TaskPlanningBootstrapStage, TaskStatus
from openorc.persistence import reviews as review_records
from openorc.persistence import send_claims as send_claim_records
from openorc.persistence import tasks as task_records
from openorc.persistence.executions import list_task_executions
from openorc.persistence.pool import DatabasePool
from openorc.persistence.pull_requests import get_task_pull_request_for_task
from openorc.persistence.sessions import get_task_agent_session
from openorc.services.errors import InvalidCommandError, NotFoundError
from openorc.services.transaction_composition import composed_transaction

__all__ = ["list_pending_task_runtime_interactions"]

# Deterministic ordering fallback for rows whose committed start instant is
# honestly unknown (bootstrap residue claimed before the instant evidence
# existed). Ordering only — never displayed or persisted.
_UNSTATED_START_ORDERING = datetime(1970, 1, 1, tzinfo=UTC)


def _require_uuid_command(value: object, name: str) -> UUID:
    """Reject a malformed command identifier before any state is touched."""
    if not isinstance(value, UUID):
        raise InvalidCommandError(f"{name} must be a UUID")
    return value


def _bootstrap_interaction(
    pool: DatabasePool,
    *,
    workspace_id: UUID,
    task: Task,
) -> PendingTaskRuntimeInteraction | None:
    """The outstanding planning-bootstrap interaction, if still current.

    Exactly one of three forward-only shapes, read off the settled stage
    evidence (issue #87):

    - no settled stage and no published plan revision: the formal PREPARE
      control's validated workspace result is still outstanding — the
      conservatively outstanding establishment/preparation work;
    - stage ``prepared``: the PREPARE result already validated, but the
      separate effectful planning-context transition it authorizes has not
      settled — the outstanding ``planning_bootstrap_transition`` work. A
      settled ``prepared`` stage is never completion of the whole bootstrap
      interaction: the process can die or the runtime can stall between the
      stage settlement and the transition;
    - stage ``ready_for_planning``: the bootstrap resolved into its later
      continuations (the F2 send claims cover those) and the bootstrap
      itself is never listed again.

    A published plan revision means the bootstrap resolved at any stage and
    the bootstrap interaction is never listed. ``started_at`` is the
    database-stamped claim instant (for the outstanding PREPARE) or the
    stage settlement instant (for the outstanding transition — the instant
    the validated PREPARE result started the transition); residue claimed
    before the instant evidence existed carries ``None`` honestly.
    """
    evidence = task_records.get_task_planning_bootstrap_evidence(pool, task_id=task.id)
    if evidence is None:
        return None
    if task.current_plan_revision_id is not None:
        return None
    if evidence.stage is TaskPlanningBootstrapStage.READY_FOR_PLANNING:
        return None
    producer = get_task_agent_session(pool, task_id=task.id, role=WorkflowRole.PRODUCER)
    if evidence.stage is TaskPlanningBootstrapStage.PREPARED:
        # The formal PREPARE result was validated; the effectful
        # planning-context transition it authorized is still unsettled.
        return PendingTaskRuntimeInteraction(
            operation=PendingTaskRuntimeInteractionOperation.PLANNING_BOOTSTRAP_TRANSITION,
            workspace_id=workspace_id,
            task_id=task.id,
            role=WorkflowRole.PRODUCER,
            task_agent_session_id=None if producer is None else producer.id,
            subject={
                "planning_bootstrap_claim_token": str(evidence.claim_token),
                "formal_result_outstanding": False,
            },
            started_at=evidence.stage_settled_at,
        )
    # No settled stage: the formal PREPARE control itself is outstanding.
    return PendingTaskRuntimeInteraction(
        operation=PendingTaskRuntimeInteractionOperation.PLANNING_BOOTSTRAP_PREPARE,
        workspace_id=workspace_id,
        task_id=task.id,
        role=WorkflowRole.PRODUCER,
        task_agent_session_id=None if producer is None else producer.id,
        subject={
            "planning_bootstrap_claim_token": str(evidence.claim_token),
            "formal_result_outstanding": True,
        },
        started_at=evidence.claimed_at,
    )


def _send_claim_interaction(
    pool: DatabasePool,
    *,
    workspace_id: UUID,
    task: Task,
    claim_operation: PendingTaskRuntimeInteractionOperation,
    claim: TaskRuntimeSendClaim,
) -> PendingTaskRuntimeInteraction | None:
    """One still-claimed F2 send, if its subject is still current work.

    A claimed send whose subject moved on — a published plan revision, a
    superseded routed iteration, or an already-created canonical PR — is a
    resolved or superseded attempt, never current work: the finalize or
    continuation that owns the subject records the resolution. The safe
    subject identity and the committed claim instant travel on the row.
    """
    if claim.kind is TaskRuntimeSendClaimKind.INITIAL_PLAN_DISPATCH:
        if task.current_plan_revision_id is not None:
            return None
        subject: dict[str, object] = {"send_claim_id": str(claim.id)}
    elif claim.kind is TaskRuntimeSendClaimKind.PLANNING_REVISE_DISPATCH:
        if claim.review_iteration_id is None or task.current_plan_revision_id is None:
            return None
        iteration = review_records.get_review_iteration(
            pool, review_iteration_id=claim.review_iteration_id
        )
        if iteration is None or iteration.plan_revision_id != task.current_plan_revision_id:
            return None
        # Still the latest-numbered iteration addressing that exact
        # revision: a newer attempt supersedes this one.
        for later in review_records.list_review_loop_iterations(
            pool, review_loop_id=iteration.review_loop_id
        ):
            if (
                later.iteration_number > iteration.iteration_number
                and later.plan_revision_id == iteration.plan_revision_id
            ):
                return None
        subject = {
            "send_claim_id": str(claim.id),
            "review_iteration_id": str(claim.review_iteration_id),
        }
    else:
        if task.status is not TaskStatus.WAITING_FOR_OWNER:
            return None
        if get_task_pull_request_for_task(pool, task_id=task.id) is not None:
            return None
        subject = {
            "send_claim_id": str(claim.id),
            "owner_gate_id": str(claim.owner_gate_id),
            "subject_head_sha": claim.subject_head_sha,
        }
    return PendingTaskRuntimeInteraction(
        operation=claim_operation,
        workspace_id=workspace_id,
        task_id=task.id,
        role=WorkflowRole.PRODUCER,
        task_agent_session_id=claim.task_agent_session_id,
        subject=subject,
        started_at=claim.claimed_at,
    )


def _review_dispatch_interactions(
    pool: DatabasePool,
    *,
    workspace_id: UUID,
    task: Task,
) -> list[PendingTaskRuntimeInteraction]:
    """The outstanding REVIEW dispatches on the Task's open loops.

    An unfinalized iteration is the E3a/E3b reserved attempt whose REVIEW
    send already happened (or whose formal result is still outstanding).
    The projection is strictly current-work-only: an attempt is listed only
    while it is simultaneously

    - the latest-numbered iteration of its loop overall — any newer
      iteration, finalized or not, supersedes the older unfinalized attempt;
    - addressed to the Task's exact current workflow subject — a planning
      attempt must address the Task's current plan revision, a PR-review
      attempt must address the Task's current canonical PR at exactly the
      reviewed head — and
    - in the eligible active phase for that subject — ``PLANNING`` for a
      planning loop, ``REVIEWING`` with no current Owner gate for a PR
      review loop (mirroring the dispatch services' own currentness proofs).

    Anything else is superseded history, moved-on work, or Owner-gated
    waiting — never listed.
    """
    interactions: list[PendingTaskRuntimeInteraction] = []
    reviewer = get_task_agent_session(pool, task_id=task.id, role=WorkflowRole.REVIEWER)
    for loop in review_records.list_task_review_loops(
        pool, task_id=task.id, status=ReviewLoopStatus.OPEN
    ):
        iterations = review_records.list_review_loop_iterations(pool, review_loop_id=loop.id)
        unfinalized = [iteration for iteration in iterations if iteration.outcome is None]
        if not unfinalized:
            continue
        latest = max(unfinalized, key=lambda iteration: iteration.iteration_number)
        latest_overall = max(iterations, key=lambda iteration: iteration.iteration_number)
        if latest_overall.id != latest.id:
            # A newer attempt — finalized or not — has superseded this one.
            continue
        if loop.purpose is ReviewLoopPurpose.PLANNING:
            if task.status is not TaskStatus.PLANNING:
                continue
            if (
                latest.plan_revision_id is None
                or latest.plan_revision_id != task.current_plan_revision_id
            ):
                continue
        else:
            if task.status is not TaskStatus.REVIEWING:
                continue
            if task.current_owner_gate_id is not None:
                continue
            pull_request = get_task_pull_request_for_task(pool, task_id=task.id)
            if (
                pull_request is None
                or pull_request.id != latest.task_pull_request_id
                or pull_request.head_sha != latest.reviewed_head_sha
            ):
                continue
        subject: dict[str, object] = {
            "review_loop_id": str(loop.id),
            "review_iteration_id": str(latest.id),
            "iteration_number": latest.iteration_number,
            "purpose": loop.purpose.value,
        }
        if latest.plan_revision_id is not None:
            subject["plan_revision_id"] = str(latest.plan_revision_id)
        if latest.task_pull_request_id is not None:
            subject["task_pull_request_id"] = str(latest.task_pull_request_id)
        if latest.reviewed_head_sha is not None:
            subject["reviewed_head_sha"] = latest.reviewed_head_sha
        interactions.append(
            PendingTaskRuntimeInteraction(
                operation=PendingTaskRuntimeInteractionOperation.REVIEWER_REVIEW,
                workspace_id=workspace_id,
                task_id=task.id,
                role=WorkflowRole.REVIEWER,
                task_agent_session_id=None if reviewer is None else reviewer.id,
                subject=subject,
                started_at=latest.created_at,
            )
        )
    return interactions


def _execution_interactions(
    pool: DatabasePool,
    *,
    workspace_id: UUID,
    task_id: UUID,
) -> list[PendingTaskRuntimeInteraction]:
    """The outstanding implementation/remediation attempts.

    A ``RUNNING`` Execution is the E6/E9 potentially-dispatched claim record:
    it is current work by definition until it is truthfully finalized. Every
    other status (queued residue, paused-for-approval waits, terminal
    outcomes) is not an outstanding runtime interaction.
    """
    interactions: list[PendingTaskRuntimeInteraction] = []
    for execution in list_task_executions(pool, task_id=task_id):
        if execution.status is not ExecutionStatus.RUNNING:
            continue
        remediation = execution.review_iteration_id is not None
        subject: dict[str, object] = {
            "execution_id": str(execution.id),
            "execution_number": execution.execution_number,
        }
        if remediation:
            subject["review_iteration_id"] = str(execution.review_iteration_id)
        interactions.append(
            PendingTaskRuntimeInteraction(
                operation=(
                    PendingTaskRuntimeInteractionOperation.PR_REMEDIATION_EXECUTION
                    if remediation
                    else PendingTaskRuntimeInteractionOperation.IMPLEMENTATION_EXECUTION
                ),
                workspace_id=workspace_id,
                task_id=task_id,
                role=WorkflowRole.PRODUCER,
                task_agent_session_id=execution.producer_session_id,
                subject=subject,
                started_at=execution.created_at,
            )
        )
    return interactions


def list_pending_task_runtime_interactions(
    pool: DatabasePool,
    *,
    workspace_id: UUID,
    task_id: UUID,
) -> list[PendingTaskRuntimeInteraction]:
    """List the Task's current outstanding runtime interactions, read-only.

    The exact-Task-bound read: the addressed Task UUID must name a Task in
    the addressed Workspace (uniformly ``NotFoundError`` otherwise). An
    archived or terminal Task has no eligible active operations and lists
    empty — the projection never resurrects historical work. The result is
    deterministically ordered by committed start instant, then operation,
    then subject identity; rows whose start instant is honestly unknown
    order last, after every row with a known instant.
    """
    _require_uuid_command(workspace_id, "workspace_id")
    _require_uuid_command(task_id, "task_id")
    task = task_records.get_task(pool, task_id)
    if task is None or task.workspace_id != workspace_id:
        raise NotFoundError("the requested task is not available in this workspace")
    if task.archived_at is not None or task.status.is_terminal:
        return []
    # One short database-only composition: the projection is a consistent
    # snapshot of the claim facts, with no transaction open at return.
    with composed_transaction(pool) as transaction_pool:
        interactions: list[PendingTaskRuntimeInteraction] = []
        bootstrap = _bootstrap_interaction(transaction_pool, workspace_id=workspace_id, task=task)
        if bootstrap is not None:
            interactions.append(bootstrap)
        claim_operations = {
            TaskRuntimeSendClaimKind.INITIAL_PLAN_DISPATCH: (
                PendingTaskRuntimeInteractionOperation.INITIAL_PLAN_DISPATCH
            ),
            TaskRuntimeSendClaimKind.PLANNING_REVISE_DISPATCH: (
                PendingTaskRuntimeInteractionOperation.PLANNING_REVISE_DISPATCH
            ),
            TaskRuntimeSendClaimKind.PR_COMPOSE_DISPATCH: (
                PendingTaskRuntimeInteractionOperation.PR_COMPOSE_DISPATCH
            ),
        }
        for claim in send_claim_records.list_task_runtime_send_claims(
            transaction_pool, task_id=task_id
        ):
            if claim.status is not TaskRuntimeSendClaimStatus.CLAIMED:
                continue
            interaction = _send_claim_interaction(
                transaction_pool,
                workspace_id=workspace_id,
                task=task,
                claim_operation=claim_operations[claim.kind],
                claim=claim,
            )
            if interaction is not None:
                interactions.append(interaction)
        interactions.extend(
            _review_dispatch_interactions(transaction_pool, workspace_id=workspace_id, task=task)
        )
        interactions.extend(
            _execution_interactions(transaction_pool, workspace_id=workspace_id, task_id=task_id)
        )
    interactions.sort(
        key=lambda interaction: (
            0 if interaction.started_at is not None else 1,
            interaction.started_at or _UNSTATED_START_ORDERING,
            interaction.operation.value,
            str(interaction.subject),
        )
    )
    return interactions
