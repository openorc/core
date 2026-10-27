"""Durable pre-send runtime-dispatch claims for consequential automated
sends (F2, issue #87).

The three automated workflow sends without their own durable claim identity
— the initial PLAN dispatch, the planning REVISE dispatch, and the
PR_COMPOSE dispatch — claim their exact logical workflow subject here,
before any runtime send/control. The claim commits in one short transaction
with the Task row lock and a currentness revalidation, mirroring the R1
subject reproof the other dispatch services run after attachment: the exact
current Task at the caller's expected ``state_token``, the exact expected
READY Producer binding, and the per-kind source subject still current. The
account/Workspace authorization and, for plan production, the
authoritative repository-base observation stay with the calling service's
existing entry phases; this module re-proves the claim subject under the
lock only.

The claim semantics are deliberately narrow:

- a claimed subject is never re-dispatched — a duplicate, stale, or
  previously claimed invocation (including the residue of a worker that
  died after committing its claim) fails closed here and never reaches the
  adapter, whatever its outcome record says;
- no Task ``state_token`` is rotated to make a claim, and no second Owner
  authorization exists: the claim is one durable fact beside the existing
  currentness discipline, and the calling finalize keeps its own
  post-send subject rechecks;
- settlement is the validated-formal-result fact only (``settled``), a
  positively proven pre-effect failure records ``known_nondelivery``, and
  every uncertain or post-effect outcome leaves the claim conservatively
  claimed — never replayed, never reclassified, and never automatically
  recovered (F4b owns recovery later);
- E3a's reserved unfinalized ReviewIteration, the E6/E9 Execution claim,
  the E1b bootstrap claim, and the E10 one-shot intents keep their own
  existing identities and are deliberately not replicated here.

No prompt, plan text, review content, reasoning, formal response body,
credential, or SDK data ever enters a claim.
"""

from __future__ import annotations

from uuid import UUID

from openorc.domain.gates import OwnerGate, OwnerGateStatus, OwnerGateType
from openorc.domain.reviews import (
    ReviewIteration,
    ReviewLoopPurpose,
    ReviewLoopStatus,
    ReviewOutcome,
)
from openorc.domain.send_claims import (
    TaskRuntimeSendClaim,
    TaskRuntimeSendClaimKind,
    TaskRuntimeSendClaimStatus,
)
from openorc.domain.sessions import TaskAgentSession, TaskSessionLifecycleStatus
from openorc.domain.tasks import Task, TaskStatus
from openorc.persistence import gates as gate_records
from openorc.persistence import pull_requests as pull_request_records
from openorc.persistence import reviews as review_records
from openorc.persistence import send_claims as send_claim_records
from openorc.persistence import tasks as task_records
from openorc.persistence.pool import DatabasePool
from openorc.persistence.sessions import get_task_agent_session
from openorc.services.errors import (
    ConflictError,
    InvalidCommandError,
    NotFoundError,
    StaleOperationError,
)
from openorc.services.transaction_composition import composed_transaction

__all__ = [
    "RUNTIME_PRE_EFFECT_FAILURE_KIND",
    "claim_initial_plan_send",
    "claim_planning_revise_send",
    "claim_pr_compose_send",
    "record_send_claim_known_non_delivery",
    "settle_pr_compose_send_claim",
    "settle_send_claim_for_plan_revision",
]

# The safe failure-kind label shared with the E6/E9 pre-effect audit
# vocabulary: it records that the runtime error was positively proven to
# precede any effect, and never an outcome guess about a possibly-delivered
# send.
RUNTIME_PRE_EFFECT_FAILURE_KIND = "runtime_pre_effect"

# The Proven pre-effect runtime error classes (issue #87): the normalized
# taxonomy classes whose contracts establish that no effect occurred.
# AgentRuntimeTransportError and AgentRuntimeProtocolFailureError are
# deliberately absent: both can arise only after a send has done work, so
# they leave the claim conservatively claimed.
_PROVEN_PRE_EFFECT_ERROR_DOC = (
    "AgentRuntimeConfigurationRejectedError (rejected before the operation "
    "could take effect), AgentRuntimeUnavailableError (unavailable before "
    "any known effect), and AgentSessionNotFoundError (the addressed "
    "session is not live, so no delivery was possible)"
)

logger = None  # placeholder removed below


# --- command-shape validation ------------------------------------------------


def _require_uuid_command(value: object, name: str) -> UUID:
    """Reject a malformed command identifier before any state is touched."""
    if not isinstance(value, UUID):
        raise InvalidCommandError(f"{name} must be a UUID")
    return value


def _require_producer(value: object) -> TaskAgentSession:
    """Require the exact prepared Producer binding object from the caller."""
    if not isinstance(value, TaskAgentSession) or value.role.value != "producer":
        raise InvalidCommandError("producer must be the Task's producer TaskAgentSession")
    return value


def _require_routed_iteration(value: object) -> ReviewIteration:
    """Require a finalized planning change request as the REVISE claim subject."""
    if not isinstance(value, ReviewIteration):
        raise InvalidCommandError(
            "review_iteration must be the routed finalized planning ReviewIteration"
        )
    return value


def _require_approved_pr_authorization_gate(value: object) -> OwnerGate:
    """Require the durably approved PR_AUTHORIZATION gate as the claim subject."""
    if not isinstance(value, OwnerGate):
        raise InvalidCommandError("owner_gate must be an OwnerGate")
    if value.gate_type is not OwnerGateType.PR_AUTHORIZATION:
        raise InvalidCommandError(
            "owner_gate must be the approved PR_AUTHORIZATION gate of this publication"
        )
    if value.status is not OwnerGateStatus.APPROVED or value.subject_head_sha is None:
        raise InvalidCommandError(
            "owner_gate must be durably approved with its exact head-sha subject"
        )
    return value


# --- locked claim-transaction helpers ----------------------------------------


def _require_claim_current_task(
    transaction_pool: DatabasePool,
    *,
    workspace_id: UUID,
    task_id: UUID,
    expected_state_token: UUID,
    expected_status: TaskStatus,
) -> Task:
    """Re-prove the exact current Task under its row lock, without rotating it.

    The claim is one durable fact beside the existing currentness
    discipline: the locked re-read observes the durable state at claim time
    (a concurrent rotation, archival, or status move serializes on the row
    lock), and the claim commits only against the still-current subject.
    No token rotation and no workflow transition happen here.
    """
    locked = task_records.get_task_for_update(transaction_pool, task_id)
    if locked is None or locked.workspace_id != workspace_id:
        raise NotFoundError("the requested task is not available in this workspace")
    if locked.archived_at is not None:
        raise StaleOperationError("the task has already been archived; the operation is stale")
    if locked.state_token != expected_state_token:
        raise StaleOperationError(
            "the task state has moved on since this operation was prepared; the operation is stale"
        )
    if locked.status is not expected_status:
        raise StaleOperationError(
            "the task moved on during the operation; the dispatch claim is stale"
        )
    return locked


def _require_claim_producer_binding(
    transaction_pool: DatabasePool,
    *,
    task_id: UUID,
    workspace_id: UUID,
    expected: TaskAgentSession,
) -> TaskAgentSession:
    """Re-prove the exact prepared Producer binding under the Task row lock.

    The claim binds the original Producer session identity: the current
    (Task, role) binding must still be READY, carry the exact same
    internal identity and external session identity, and belong to the
    addressed Task/Workspace. Session loss and explicit ends do not rotate
    the Task token, so the token alone cannot prove the prepared binding —
    this reproof can. No replacement session, reinitialization, or
    redirection ever happens here.
    """
    binding = get_task_agent_session(transaction_pool, task_id=task_id, role=expected.role)
    if binding is None or binding.task_id != task_id or binding.workspace_id != workspace_id:
        raise NotFoundError("the requested producer session binding is not available for this task")
    if binding.lifecycle_status is not TaskSessionLifecycleStatus.READY:
        raise ConflictError(
            "the producer session binding is not READY; the dispatch claim binds the "
            "exact prepared session only and never recreates or reinitializes one"
        )
    if binding.id != expected.id or binding.external_session_id != expected.external_session_id:
        raise StaleOperationError(
            "the producer session binding moved on during the operation; "
            "the dispatch claim is stale"
        )
    if binding.external_session_id is None:
        raise ConflictError("the producer session binding has no external session identity")
    return binding


def _reject_already_claimed(existing: TaskRuntimeSendClaim, subject: str) -> StaleOperationError:
    """The typed rejection of a replayed/claimed subject: never re-dispatched."""
    if existing.status is TaskRuntimeSendClaimStatus.CLAIMED:
        return StaleOperationError(
            f"the {subject} was already claimed for dispatch and its formal result "
            "is still outstanding; the outcome is unresolved and is never re-dispatched"
        )
    return StaleOperationError(
        f"the {subject} was already claimed for dispatch and its outcome is durably "
        "recorded; the continuation is stale and is never re-dispatched"
    )


def _require_routed_revise_subject_still_current(
    transaction_pool: DatabasePool,
    *,
    task: Task,
    review_iteration: ReviewIteration,
) -> None:
    """Re-prove the routed REVISE subject under the Task row lock.

    The claim subject is the exact finalized ``CHANGES_REQUESTED`` planning
    ReviewIteration addressed to the Task's still-current prior revision,
    and it must still be the latest-numbered iteration addressing that
    revision in its still-OPEN planning loop (issue #207). A superseded or
    moved-on subject never claims a send: a newer ACCEPTED result, a newer
    change request, or a newer unresolved attempt on the same plan makes
    this continuation stale before any runtime effect.
    """
    stored = review_records.get_review_iteration(
        transaction_pool, review_iteration_id=review_iteration.id
    )
    if stored is None or stored.task_id != task.id or stored.workspace_id != task.workspace_id:
        raise NotFoundError("the requested review iteration is not available for this task")
    if stored.outcome is not ReviewOutcome.CHANGES_REQUESTED:
        raise ConflictError(
            "the routed review iteration is not a finalized changes request; "
            "only a finalized CHANGES_REQUESTED planning result claims a REVISE dispatch"
        )
    loop = review_records.get_review_loop(transaction_pool, review_loop_id=stored.review_loop_id)
    if (
        loop is None
        or loop.task_id != task.id
        or loop.workspace_id != task.workspace_id
        or loop.purpose is not ReviewLoopPurpose.PLANNING
        or loop.status is not ReviewLoopStatus.OPEN
    ):
        raise ConflictError(
            "the routed change request's planning review loop is not open; "
            "the REVISE dispatch claim applies nothing"
        )
    if stored.plan_revision_id is None or stored.plan_revision_id != task.current_plan_revision_id:
        raise StaleOperationError(
            "the routed change request no longer addresses the task's current plan "
            "revision; the continuation is stale"
        )
    iterations = review_records.list_review_loop_iterations(
        transaction_pool, review_loop_id=loop.id
    )
    for later in iterations:
        if (
            later.iteration_number > stored.iteration_number
            and later.plan_revision_id == stored.plan_revision_id
        ):
            raise StaleOperationError(
                "the review iteration has been superseded on the current plan revision "
                "by a newer review iteration; the continuation is stale"
            )


def _require_compose_subject_still_current(
    transaction_pool: DatabasePool,
    *,
    task: Task,
    workspace_id: UUID,
    owner_gate: OwnerGate,
) -> None:
    """Re-prove the PR_COMPOSE subject under the Task row lock.

    The claim subject is the exact durably approved ``PR_AUTHORIZATION``
    gate and its approved head, on the fresh composition path only: the
    Task must still carry no current gate and no canonical PR (an existing
    canonical record means the composition already resolved and the narrow
    publication continuation — never a second compose dispatch — owns the
    rest), and the addressed gate must still be the approved head-sha
    subject of this Task/Workspace.
    """
    if task.current_owner_gate_id is not None:
        raise StaleOperationError(
            "the task carries a current owner gate; the pr compose dispatch claim is stale"
        )
    existing_pr = pull_request_records.get_task_pull_request_for_task(
        transaction_pool, task_id=task.id
    )
    if existing_pr is not None:
        raise ConflictError(
            "the task already carries a canonical pull request; pr composition is "
            "not repeatable and the publication continuation owns the rest"
        )
    gate = gate_records.get_owner_gate(transaction_pool, owner_gate_id=owner_gate.id)
    if gate is None or gate.task_id != task.id or gate.workspace_id != workspace_id:
        raise NotFoundError("the requested owner gate is not available for this task")
    if (
        gate.gate_type is not OwnerGateType.PR_AUTHORIZATION
        or gate.status is not OwnerGateStatus.APPROVED
        or gate.subject_head_sha is None
        or gate.subject_head_sha != owner_gate.subject_head_sha
    ):
        raise StaleOperationError(
            "the approved pr authorization gate moved on during the operation; "
            "the pr compose dispatch claim is stale"
        )


# --- the one claim per send ---------------------------------------------------


def claim_initial_plan_send(
    pool: DatabasePool,
    *,
    workspace_id: UUID,
    task_id: UUID,
    expected_state_token: UUID,
    producer: TaskAgentSession,
) -> TaskRuntimeSendClaim:
    """Claim the one initial PLAN dispatch for the Task/Producer session.

    One short transaction under the Task row lock, composed after the
    calling service's authentication/currentness and repository-base checks
    and strictly before the PLAN send: the exact current ``planning`` Task
    at the caller's token (still no current plan revision), the exact
    prepared READY Producer binding, no existing claim for the subject, and
    the committed claim row. The partial unique index on the kind is the
    durable exactly-once backstop: a replayed or raced second claim for the
    Task — whatever its prior outcome, even one whose worker died after
    committing — fails closed here and never dispatches a runtime effect.
    """
    _require_uuid_command(workspace_id, "workspace_id")
    _require_uuid_command(task_id, "task_id")
    _require_uuid_command(expected_state_token, "expected_state_token")
    _require_producer(producer)
    with composed_transaction(pool) as transaction_pool:
        locked = _require_claim_current_task(
            transaction_pool,
            workspace_id=workspace_id,
            task_id=task_id,
            expected_state_token=expected_state_token,
            expected_status=TaskStatus.PLANNING,
        )
        if locked.current_plan_revision_id is not None:
            raise StaleOperationError(
                "the task already has a current plan revision; the first-plan continuation is stale"
            )
        _require_claim_producer_binding(
            transaction_pool,
            task_id=task_id,
            workspace_id=workspace_id,
            expected=producer,
        )
        existing = send_claim_records.get_task_runtime_send_claim_for_subject(
            transaction_pool,
            task_id=task_id,
            kind=TaskRuntimeSendClaimKind.INITIAL_PLAN_DISPATCH,
        )
        if existing is not None:
            raise _reject_already_claimed(existing, "initial plan dispatch")
        return send_claim_records.create_task_runtime_send_claim(
            transaction_pool,
            workspace_id=workspace_id,
            task_id=task_id,
            kind=TaskRuntimeSendClaimKind.INITIAL_PLAN_DISPATCH,
            task_agent_session_id=producer.id,
        )


def claim_planning_revise_send(
    pool: DatabasePool,
    *,
    workspace_id: UUID,
    task_id: UUID,
    expected_state_token: UUID,
    producer: TaskAgentSession,
    review_iteration: ReviewIteration,
) -> TaskRuntimeSendClaim:
    """Claim the planning REVISE dispatch for one exact finalized change request.

    One short transaction under the Task row lock, composed after the
    calling service's entry checks and repository-base observation and
    strictly before the REVISE send: the exact current ``planning`` Task at
    the caller's token, the routed finalized ``CHANGES_REQUESTED``
    planning ReviewIteration still addressing the still-current prior
    revision as its latest-numbered iteration (issue #207), the exact
    prepared READY Producer binding, no existing claim for the iteration,
    and the committed claim row. A distinct permitted next revision claims
    its own row; a replayed or raced claim for the same iteration never
    dispatches again.
    """
    _require_uuid_command(workspace_id, "workspace_id")
    _require_uuid_command(task_id, "task_id")
    _require_uuid_command(expected_state_token, "expected_state_token")
    _require_producer(producer)
    iteration = _require_routed_iteration(review_iteration)
    with composed_transaction(pool) as transaction_pool:
        locked = _require_claim_current_task(
            transaction_pool,
            workspace_id=workspace_id,
            task_id=task_id,
            expected_state_token=expected_state_token,
            expected_status=TaskStatus.PLANNING,
        )
        _require_routed_revise_subject_still_current(
            transaction_pool, task=locked, review_iteration=iteration
        )
        _require_claim_producer_binding(
            transaction_pool,
            task_id=task_id,
            workspace_id=workspace_id,
            expected=producer,
        )
        existing = send_claim_records.get_task_runtime_send_claim_for_subject(
            transaction_pool,
            task_id=task_id,
            kind=TaskRuntimeSendClaimKind.PLANNING_REVISE_DISPATCH,
            review_iteration_id=iteration.id,
        )
        if existing is not None:
            raise _reject_already_claimed(existing, "planning revise dispatch")
        return send_claim_records.create_task_runtime_send_claim(
            transaction_pool,
            workspace_id=workspace_id,
            task_id=task_id,
            kind=TaskRuntimeSendClaimKind.PLANNING_REVISE_DISPATCH,
            task_agent_session_id=producer.id,
            review_iteration_id=iteration.id,
        )


def claim_pr_compose_send(
    pool: DatabasePool,
    *,
    workspace_id: UUID,
    task_id: UUID,
    expected_state_token: UUID,
    owner_gate: OwnerGate,
    producer: TaskAgentSession,
) -> TaskRuntimeSendClaim:
    """Claim the PR_COMPOSE dispatch for one approved gate and approved head.

    One short transaction under the Task row lock, composed after the
    calling service's admission and strictly before the PR_COMPOSE send on
    the fresh path: the exact current ``waiting_for_owner`` Task at the
    caller's token, no current gate and no canonical PR, the addressed
    durably approved ``PR_AUTHORIZATION`` gate still carrying its exact
    approved head SHA, the exact prepared READY Producer binding, no
    existing claim for the gate, and the committed claim row. The narrow
    interrupted-publication continuation never claims or re-composes.
    """
    _require_uuid_command(workspace_id, "workspace_id")
    _require_uuid_command(task_id, "task_id")
    _require_uuid_command(expected_state_token, "expected_state_token")
    gate = _require_approved_pr_authorization_gate(owner_gate)
    _require_producer(producer)
    with composed_transaction(pool) as transaction_pool:
        locked = _require_claim_current_task(
            transaction_pool,
            workspace_id=workspace_id,
            task_id=task_id,
            expected_state_token=expected_state_token,
            expected_status=TaskStatus.WAITING_FOR_OWNER,
        )
        _require_compose_subject_still_current(
            transaction_pool, task=locked, workspace_id=workspace_id, owner_gate=gate
        )
        _require_claim_producer_binding(
            transaction_pool,
            task_id=task_id,
            workspace_id=workspace_id,
            expected=producer,
        )
        existing = send_claim_records.get_task_runtime_send_claim_for_subject(
            transaction_pool,
            task_id=task_id,
            kind=TaskRuntimeSendClaimKind.PR_COMPOSE_DISPATCH,
            owner_gate_id=gate.id,
        )
        if existing is not None:
            raise _reject_already_claimed(existing, "pr compose dispatch")
        return send_claim_records.create_task_runtime_send_claim(
            transaction_pool,
            workspace_id=workspace_id,
            task_id=task_id,
            kind=TaskRuntimeSendClaimKind.PR_COMPOSE_DISPATCH,
            task_agent_session_id=producer.id,
            owner_gate_id=gate.id,
            subject_head_sha=gate.subject_head_sha,
        )


# --- settlement and proven pre-effect non-delivery ----------------------------


def settle_send_claim_for_plan_revision(
    transaction_pool: DatabasePool,
    *,
    claim: TaskRuntimeSendClaim,
    workspace_id: UUID,
    task_id: UUID,
    plan_revision_id: UUID,
) -> None:
    """Settle one PLAN/REVISE claim inside the atomic finalize composition.

    Composed with the immutable PlanRevision insertion, the current-pointer
    installation, and the audit event: the validated formal result
    completed the operation, so the claim settles exactly once with only
    the safe revision identity as its outcome. A rejected settlement rolls
    the whole finalize composition back — the claim stays conservatively
    claimed and the resolution is never rewritten.
    """
    settled = send_claim_records.settle_task_runtime_send_claim(
        transaction_pool,
        claim_id=claim.id,
        task_id=task_id,
        workspace_id=workspace_id,
        outcome={"plan_revision_id": str(plan_revision_id)},
    )
    if settled is None:
        raise StaleOperationError(
            "the send claim could not be settled with the plan revision; the finalization is stale"
        )


def settle_pr_compose_send_claim(
    transaction_pool: DatabasePool,
    *,
    claim: TaskRuntimeSendClaim,
    workspace_id: UUID,
    task_id: UUID,
    subject_head_sha: str,
) -> None:
    """Settle one PR_COMPOSE claim before publication, truthfully.

    Composed with the post-interaction subject revalidation: the validated
    ``pr_result`` completed the composition, so the claim settles exactly
    once with only the safe approved head identity as its outcome — without
    pretending GitHub publication has completed. A rejected settlement
    rolls the revalidation transaction back; the claim stays conservatively
    claimed.
    """
    settled = send_claim_records.settle_task_runtime_send_claim(
        transaction_pool,
        claim_id=claim.id,
        task_id=task_id,
        workspace_id=workspace_id,
        outcome={"subject_head_sha": subject_head_sha},
    )
    if settled is None:
        raise StaleOperationError(
            "the send claim could not be settled with the validated pr result; "
            "the composition finalize is stale"
        )


def record_send_claim_known_non_delivery(
    pool: DatabasePool,
    *,
    claim: TaskRuntimeSendClaim,
    workspace_id: UUID,
    task_id: UUID,
    failure_kind: str,
) -> None:
    """Record a positively proven pre-effect failure on one claimed send.

    One short transaction after the failed send attempt: only an error
    positively proven to precede any effect reaches the
    ``known_nondelivery`` terminal record with the safe failure-kind label.
    Every uncertain or post-effect-capable outcome never calls this — the
    claim stays conservatively claimed. A rejected transition (a raced
    settlement) is a rejected no-op that is never retried blindly.
    """
    send_claim_records.record_task_runtime_send_claim_known_non_delivery(
        pool,
        claim_id=claim.id,
        task_id=task_id,
        workspace_id=workspace_id,
        failure_kind=failure_kind,
    )
