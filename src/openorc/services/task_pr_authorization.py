"""E7: the exact-head PR authorization gate (issue #82).

E7 completes exactly the next proven manual step after a successful Producer
implementation: the durably finalized successful E6 Execution is discovered
from durable state, the Producer's feature-branch claim is verified against
fresh authoritative GitHub state, and one exact-head ``PR_AUTHORIZATION``
OwnerGate is presented for the Owner's Approve/Cancel decision.

Authority boundaries (continuing the merged E5/E6 contracts):

- **The trigger is durable, never conversational.** E7's authoritative input
  is the persisted successful Execution with its validated
  ``implementation_result`` payload — never a GitHub push webhook, a raw
  transient model reply, or a caller-supplied branch/SHA/outcome. The E6
  one-shot finalize persisted ``type``/``schema_version``/``status``/
  ``branch``/``summary``/``changes``/``validation``/optional ``notes``
  (issue #81); E7 re-derives those persisted facts, takes ``branch`` only
  as a discovery/routing claim, and fails closed on missing or invalid
  persisted evidence. GitHub alone supplies the authoritative committed
  head SHA — runtime-local HEAD, webhook payloads, and Producer prose are
  never code state.
- **Gate presentation is an OpenOrc continuation, not an extra human
  decision.** ``open_pr_authorization`` derives every clearance fact from
  the canonical tables inside short composed transactions, verifies the
  source and branch through the established B3/B7 seams with no database
  transaction open, then binds the canonical feature branch (once),
  creates and installs one pending exact-head ``PR_AUTHORIZATION`` gate,
  and moves the Task ``implementing → waiting_for_owner`` atomically.
- **Source invalidity is reason-specific.** A confirmed requirements
  fingerprint change against the Task's immutable baseline is the one
  ``BLOCKED / GITHUB_SOURCE_CHANGED`` path: the TaskBlock, the
  ``implementing → blocked`` transition, and the ``task_blocked`` audit
  commit or roll back together, and no gate is created. A closed source
  issue, a stable-identity mismatch, access loss, known provider failure,
  and uncertain outcomes are the existing typed fail-closed refusals —
  never fabricated requirements drift, never a gate. Recovery from the
  blocked state belongs to later layers, not this leaf.
- **Approve and Cancel are the only Owner actions at this gate.**
  ``PR_AUTHORIZATION`` is the human accountability checkpoint for
  publishing this implementation — not another technical review, not a
  Producer feedback mechanism. The merged generic E4 discussion service may
  technically support the gate type, but this module neither exposes nor
  calls it here, and no Discuss, Decline/Reject, changes-request, formal
  Reviewer iteration, or pre-PR remediation routing exists. Approval
  resolves only the exact current gate with truthful OWNER provenance and
  never transitions the Task to ``reviewing`` and never creates a PR;
  cancellation archives the Task ``cancelled`` exactly like the E5 seam.
- **E8 recovers the approval durably.** The approving Profile identity and
  the approved head live on the gate's OWNER-attributed
  ``owner_gate_resolved`` audit event and the gate row; E8 (issue #83)
  re-verifies the fresh remote head against the approved SHA immediately
  before creation and performs composition, publication, postflight, and
  transition as its own leaf. This module never calls
  ``publish_task_pull_request``, GitHub create-PR, Reviewer PR review, or
  merge, and never calls the Producer again.

No universal dispatcher, operation outbox, branch scheduler, generic gate
engine, database transaction spanning GitHub/network I/O, or record of all
observed branch heads is built here. Telemetry carries only the safe
identifier vocabulary.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from uuid import UUID

from openorc.adapters.github import GitHubAppClient
from openorc.domain.blocks import TaskBlockReason
from openorc.domain.connections import WorkflowRole
from openorc.domain.events import WorkflowEventActor
from openorc.domain.executions import Execution, ExecutionStatus
from openorc.domain.gates import OwnerGate, OwnerGateStatus, OwnerGateType
from openorc.domain.github_issues import GitHubIssueState
from openorc.domain.planning import PlanRevision
from openorc.domain.reviews import (
    ReviewIteration,
    ReviewLoop,
    ReviewLoopPurpose,
    ReviewLoopStatus,
    ReviewOutcome,
)
from openorc.domain.sessions import TaskAgentSession
from openorc.domain.tasks import Task, TaskStatus
from openorc.observability import annotate_span, application_span
from openorc.persistence import blocks as block_records
from openorc.persistence import executions as execution_records
from openorc.persistence import gates as gate_records
from openorc.persistence import github_issues as issue_records
from openorc.persistence import pull_requests as pull_request_records
from openorc.persistence import reviews as review_records
from openorc.persistence import sessions as session_records
from openorc.persistence import tasks as task_records
from openorc.persistence.pool import DatabasePool
from openorc.protocol.models import IMPLEMENTATION_RESULT_FAMILY, SCHEMA_VERSION
from openorc.services import event_coordination
from openorc.services.canonical_branch import observe_routed_repository_branch
from openorc.services.errors import (
    ConflictError,
    InvalidCommandError,
    NotFoundError,
    StaleOperationError,
)
from openorc.services.github_reconciliation import reconcile_repository_issue
from openorc.services.profile_lifecycle_guard import require_account_operational
from openorc.services.task_subject_guards import (
    require_current_owner_gate,
    require_current_plan_revision,
    require_current_task,
)
from openorc.services.transaction_composition import composed_transaction
from openorc.services.workspace_authorization import require_workspace_task

__all__ = [
    "ApprovedPrAuthorization",
    "CancelledPrAuthorization",
    "PrAuthorizationGateOpened",
    "approve_pr_authorization",
    "cancel_pr_authorization",
    "open_pr_authorization",
]

_SERVICE_TRACER_SCOPE = "openorc.services.task_pr_authorization"
_OPEN_SPAN_NAME = "task_pr_authorization.open_pr_authorization"
_APPROVE_SPAN_NAME = "task_pr_authorization.approve_pr_authorization"
_CANCEL_SPAN_NAME = "task_pr_authorization.cancel_pr_authorization"

# The presentation and its audit are OpenOrc continuations, never an Owner
# approval: gate-creation provenance is OPENORC with no actor identity.
_OPENORC_ACTOR = event_coordination.WorkflowActorContext(WorkflowEventActor.OPENORC, None)


@dataclass(frozen=True, slots=True)
class PrAuthorizationGateOpened:
    """The durable presentation facts of one exact-head PR authorization gate.

    ``task`` is the post-write Task (``waiting_for_owner`` under its rotated
    ``state_token``); ``owner_gate`` is the pending exact-head gate; the
    canonical branch and the GitHub-verified head SHA are the presentation
    subject E8/Owner query surfaces consume.
    """

    task: Task
    owner_gate: OwnerGate
    canonical_branch: str
    verified_head_sha: str


@dataclass(frozen=True, slots=True)
class ApprovedPrAuthorization:
    """The durable approval facts E8 (issue #83) consumes.

    The gate is APPROVED with truthful OWNER provenance and the Task's
    current gate pointer is cleared under a rotated token. The Task stays
    ``waiting_for_owner`` — publication authorization is preserved for E8
    through the approved gate and its OWNER-attributed resolution audit;
    this approval is neither Reviewer acceptance nor merge authorization.
    """

    task: Task
    owner_gate: OwnerGate


@dataclass(frozen=True, slots=True)
class CancelledPrAuthorization:
    """The durable cancellation facts of one PR authorization decision.

    The gate resolved ``CANCELLED`` and the Task archived terminal
    ``cancelled`` in one atomic composition. No GitHub artifact is touched.
    """

    task: Task
    owner_gate: OwnerGate


@dataclass(frozen=True, slots=True)
class _ImplementationAuthority:
    """The E5/E6 implementation authority derived from durable state."""

    gate: OwnerGate
    revision: PlanRevision
    loop: ReviewLoop
    iteration: ReviewIteration
    producer_session: TaskAgentSession


@dataclass(frozen=True, slots=True)
class _ImplementationClaim:
    """The discovered implementation claim carried into the GitHub phase."""

    task: Task
    authority: _ImplementationAuthority
    execution: Execution
    claimed_branch: str
    expected_state_token: UUID


# --- command helpers ---------------------------------------------------------------


def _require_uuid_command(value: object, name: str) -> None:
    """Reject a malformed UUID command argument before any state is touched."""
    if not isinstance(value, UUID):
        raise InvalidCommandError(f"{name} must be a UUID")


def _require_pr_presentation_state(transaction_pool: DatabasePool, *, task: Task) -> None:
    """Prove the exact pre-gate implementing presentation state.

    A current non-archived ``implementing`` Task with no current OwnerGate
    and no canonical ``TaskPullRequest`` record: the post-E6 pre-gate shape.
    A bound canonical branch is explicitly allowed here (an already-bound
    Task continues on it); an unbound branch binds during presentation.
    """
    if task.archived_at is not None or task.status is not TaskStatus.IMPLEMENTING:
        raise ConflictError("the addressed task is not a non-archived implementing task")
    if task.current_owner_gate_id is not None:
        raise ConflictError(
            "the task carries a current owner gate; the pr authorization "
            "presentation is not available"
        )
    existing_pr = pull_request_records.get_task_pull_request_for_task(
        transaction_pool, task_id=task.id
    )
    if existing_pr is not None:
        raise ConflictError(
            "the task already carries a canonical pull request record; pr "
            "authorization is not available"
        )


def _resolve_approved_implementation_gate(
    transaction_pool: DatabasePool, *, task: Task, revision_id: UUID
) -> OwnerGate:
    """Resolve the E5-approved historical gate bound to the exact plan.

    The same durable derivation the E6 admission performs: the gate is
    resolved from Task gate history — never from a detached gate status —
    and must be the Owner-approved ``implementation_authorization`` gate
    whose subject is precisely the Task's current PlanRevision. Superseded
    pending history, cancelled gates, and gates for other revisions never
    authorize anything.
    """
    gates = gate_records.list_task_owner_gates(transaction_pool, task_id=task.id)
    approved = [
        gate
        for gate in gates
        if gate.gate_type is OwnerGateType.IMPLEMENTATION_AUTHORIZATION
        and gate.status is OwnerGateStatus.APPROVED
        and gate.plan_revision_id == revision_id
    ]
    if not approved:
        raise ConflictError(
            "no Owner-approved implementation authorization gate exists for the task's "
            "current plan; implementation is not authorized"
        )
    return max(approved, key=lambda gate: (gate.created_at, gate.id))


def _resolve_closed_planning_loop(transaction_pool: DatabasePool, *, task: Task) -> ReviewLoop:
    """Resolve the single CLOSED planning ReviewLoop following E5 approval."""
    closed = review_records.list_task_review_loops(
        transaction_pool, task_id=task.id, status=ReviewLoopStatus.CLOSED
    )
    planning = [loop for loop in closed if loop.purpose is ReviewLoopPurpose.PLANNING]
    if len(planning) != 1:
        raise ConflictError(
            "the task does not carry exactly one closed planning review loop following "
            "the Owner's implementation authorization"
        )
    return planning[0]


def _resolve_accepted_iteration(
    transaction_pool: DatabasePool, *, loop: ReviewLoop, revision_id: UUID
) -> ReviewIteration:
    """Prove the latest finalized iteration for the exact revision is ACCEPTED."""
    iterations = review_records.list_review_loop_iterations(
        transaction_pool, review_loop_id=loop.id
    )
    finalized = [
        iteration
        for iteration in iterations
        if iteration.decided_at is not None and iteration.plan_revision_id == revision_id
    ]
    if not finalized:
        raise ConflictError(
            "no finalized review iteration exists for the task's current plan revision"
        )
    latest = max(finalized, key=lambda iteration: iteration.iteration_number)
    if latest.outcome is not ReviewOutcome.ACCEPTED:
        raise ConflictError(
            "the latest finalized review iteration for the current plan is not a Reviewer "
            "acceptance; the persisted review evidence does not authorize implementation"
        )
    return latest


def _validated_persisted_branch(result: Mapping[str, object]) -> str:
    """Shape-check the persisted implementation result's routing claim.

    The E6 finalize persisted the validated canonical envelope; this is a
    durable-fact re-derivation, never a parser-matrix rerun and never a
    trust of an in-memory typed result. The envelope must be the
    ``implementation_result`` family at the pinned protocol version with a
    completed status (the schema's ``const``) and a usable branch claim;
    anything else fails closed.
    """
    if result.get("type") != IMPLEMENTATION_RESULT_FAMILY:
        raise ConflictError(
            "the persisted implementation result is not a validated implementation_result envelope"
        )
    if result.get("schema_version") != SCHEMA_VERSION:
        raise ConflictError(
            "the persisted implementation result carries an unsupported schema version"
        )
    if result.get("status") != "COMPLETED":
        raise ConflictError("the persisted implementation result is not a completed implementation")
    branch = result.get("branch")
    if not isinstance(branch, str) or not branch.strip():
        raise ConflictError("the persisted implementation result carries no usable branch claim")
    return branch


def _require_qualifying_execution(
    transaction_pool: DatabasePool, *, task: Task, execution_id: UUID | None
) -> tuple[Execution, str]:
    """Resolve the qualifying successful E6 completion from durable history.

    The Task's latest attempt must be a durably finalized successful
    Execution carrying a validated persisted result: an earlier success
    detached from a later conflicting attempt, an unresolved dispatch, a
    failed/terminal non-success, or a foreign named execution never opens
    the gate. The original Producer binding is re-proven against the
    attempt's immutable session reference.
    """
    history = execution_records.list_task_executions(transaction_pool, task_id=task.id)
    if not history:
        raise ConflictError(
            "the task carries no implementation attempt; there is no completed "
            "implementation to authorize"
        )
    latest = max(history, key=lambda attempt: attempt.execution_number)
    if execution_id is not None and execution_id != latest.id:
        raise ConflictError(
            "the addressed execution is not the task's latest implementation attempt"
        )
    if latest.status is not ExecutionStatus.SUCCEEDED or latest.implementation_result is None:
        raise ConflictError(
            "the task's latest implementation attempt is not a durably finalized "
            "successful completion"
        )
    producer_session = _resolve_producer_session(transaction_pool, task=task)
    if latest.producer_session_id != producer_session.id:
        raise ConflictError(
            "the successful implementation attempt does not belong to the task's "
            "original producer session"
        )
    claimed_branch = _validated_persisted_branch(latest.implementation_result)
    return latest, claimed_branch


def _require_unchanged_source_state(transaction_pool: DatabasePool, *, task: Task) -> None:
    """Re-prove the durable source projection against the Task's baseline.

    The composition re-checks read durable state only: the projection must
    exist under the Task's stable issue identity, be open, and carry the
    exact baseline fingerprint. A drift observed here durably is reported
    for a re-run that records the atomic ``github_source_changed`` block.
    """
    projection = issue_records.find_github_issue(
        transaction_pool,
        repository_id=task.repository_id,
        github_issue_id=task.github_issue_id,
    )
    if projection is None or projection.identity.github_issue_id != task.github_issue_id:
        raise ConflictError(
            "the task's source issue projection is no longer available under its stable identity"
        )
    if projection.state is GitHubIssueState.CLOSED:
        raise ConflictError("the task's source github issue is closed; the presentation is stale")
    if projection.requirements_fingerprint != task.source_requirements_fingerprint:
        raise ConflictError(
            "the task's source requirements changed during presentation; re-run the "
            "presentation to record the github_source_changed block"
        )


def _unexplained_rejection() -> StaleOperationError:
    """The conservative classification for an otherwise unexplained rejection."""
    return StaleOperationError("the pr authorization could not be applied; the operation is stale")


# --- the atomic source-drift block path ---------------------------------------------


def _block_source_requirements_drift(
    pool: DatabasePool, *, workspace_id: UUID, task_id: UUID, expected_state_token: UUID
) -> Task:
    """Atomically block the Task on confirmed requirements drift (issue #82).

    One short composed transaction re-proves the exact currentness under
    the Task row lock and the durable drift evidence against the freshly
    written projection, then creates the ``github_source_changed``
    TaskBlock, moves the Task ``implementing → blocked`` with a rotated
    token, and records the ``task_blocked`` audit — commit or roll back
    together. The distinct blocked path creates no PR gate; recovery from
    the blocked state belongs to later layers, not this leaf.
    """
    with composed_transaction(pool) as transaction_pool:
        locked = task_records.get_task_for_update(transaction_pool, task_id)
        if locked is None or locked.workspace_id != workspace_id:
            raise NotFoundError("the requested task is not available in this workspace")
        task = require_current_task(
            transaction_pool,
            workspace_id=workspace_id,
            task_id=task_id,
            expected_state_token=expected_state_token,
        )
        _require_pr_presentation_state(transaction_pool, task=task)
        projection = issue_records.find_github_issue(
            transaction_pool,
            repository_id=task.repository_id,
            github_issue_id=task.github_issue_id,
        )
        if projection is None or projection.identity.github_issue_id != task.github_issue_id:
            raise ConflictError(
                "the task's source issue projection is no longer available under its "
                "stable identity"
            )
        if projection.requirements_fingerprint == task.source_requirements_fingerprint:
            raise StaleOperationError(
                "the durable source requirements no longer differ from the task's "
                "baseline; the drift block is stale"
            )
        block = block_records.create_task_block(
            transaction_pool,
            workspace_id=workspace_id,
            task_id=task_id,
            reason=TaskBlockReason.GITHUB_SOURCE_CHANGED,
            context={
                "github_issue_id": task.github_issue_id,
                "expected_fingerprint": task.source_requirements_fingerprint,
                "observed_fingerprint": projection.requirements_fingerprint,
            },
        )
        blocked = task_records.update_task_status(
            transaction_pool,
            task_id,
            expected_state_token=task.state_token,
            status=TaskStatus.BLOCKED,
        )
        if blocked is None:
            raise StaleOperationError(
                "the task could not move to blocked; the drift block is stale"
            )
        event_coordination.record_task_blocked_event(
            transaction_pool,
            workspace_id=workspace_id,
            task_id=task_id,
            task_block_id=block.id,
            actor=_OPENORC_ACTOR,
        )
        return blocked


# --- the authoritative GitHub source/branch verification phase ----------------------


def _verify_source_authority(
    pool: DatabasePool, github: GitHubAppClient, *, claim: _ImplementationClaim
) -> None:
    """Verify the Task's source issue through fresh B3 reconciliation.

    The trusted B3 primitive performs the authoritative reads with no
    database transaction open and writes the refreshed projection. The
    classification is reason-specific (issue #82): only a confirmed
    requirements-fingerprint change against the Task's immutable baseline
    takes the atomic ``github_source_changed`` block path; a closed source
    issue and a stable-identity mismatch are typed fail-closed conflicts
    that fabricate no drift, and the B3 outcome vocabulary (access loss,
    known provider failure, uncertainty, stale route) passes through
    unchanged. Nothing here produces a gate.
    """
    task = claim.task
    reconciliation = reconcile_repository_issue(
        pool,
        github,
        workspace_id=task.workspace_id,
        repository_id=task.repository_id,
        issue_number=task.github_issue_number,
    )
    projection = reconciliation.issue
    if projection.identity.github_issue_id != task.github_issue_id:
        raise ConflictError(
            "the addressed github issue number resolves to a different stable issue identity"
        )
    if projection.state is GitHubIssueState.CLOSED:
        raise ConflictError(
            "the task's source github issue is closed; pr authorization is not available"
        )
    if projection.requirements_fingerprint != task.source_requirements_fingerprint:
        _block_source_requirements_drift(
            pool,
            workspace_id=task.workspace_id,
            task_id=task.id,
            expected_state_token=claim.expected_state_token,
        )
        raise ConflictError(
            "the source github issue's requirements changed; the task is blocked as "
            "github_source_changed pending owner recovery"
        )


def _require_unmoved_canonical_branch(claim: _ImplementationClaim) -> None:
    """Refuse a different-branch claim against an already-bound Task.

    Canonical branch ownership is a one-time fact: later remediation
    continues on the canonical branch rather than replacing it (the B7
    bind-once semantics), and the presentation re-observes the bound
    branch's current committed head.
    """
    bound_branch = claim.task.canonical_feature_branch
    if bound_branch is not None and bound_branch != claim.claimed_branch:
        raise ConflictError(
            "the task's canonical feature branch is already bound; remediation "
            "continues on the canonical branch"
        )


# --- the presentation service -------------------------------------------------------


def _discover_implementation_claim(
    transaction_pool: DatabasePool,
    *,
    profile_id: UUID,
    workspace_id: UUID,
    task_id: UUID,
    expected_state_token: UUID,
    execution_id: UUID | None,
) -> _ImplementationClaim:
    """Admission, authority derivation, and the discovered branch claim.

    One short composed transaction under the Task row lock: the account and
    Workspace/Task Owner barriers, the exact pre-gate implementing state,
    the E5-approved evidence still in force, and the qualifying successful
    E6 completion with its validated persisted branch claim — all derived
    from durable state, never from a caller-supplied outcome.
    """
    require_account_operational(transaction_pool, profile_id=profile_id)
    require_workspace_task(
        transaction_pool, profile_id=profile_id, workspace_id=workspace_id, task_id=task_id
    )
    locked = task_records.get_task_for_update(transaction_pool, task_id)
    if locked is None or locked.workspace_id != workspace_id:
        raise NotFoundError("the requested task is not available in this workspace")
    task = require_current_task(
        transaction_pool,
        workspace_id=workspace_id,
        task_id=task_id,
        expected_state_token=expected_state_token,
    )
    _require_pr_presentation_state(transaction_pool, task=task)
    authority = _load_implementation_authority(transaction_pool, task=task)
    execution, claimed_branch = _require_qualifying_execution(
        transaction_pool, task=task, execution_id=execution_id
    )
    return _ImplementationClaim(
        task=task,
        authority=authority,
        execution=execution,
        claimed_branch=claimed_branch,
        expected_state_token=expected_state_token,
    )


def _present_exact_head_gate(
    pool: DatabasePool,
    *,
    claim: _ImplementationClaim,
    verified_head_sha: str,
    observed_branch_name: str,
) -> PrAuthorizationGateOpened:
    """The atomic bind + exact-head gate presentation (one short transaction).

    Every admission fact is re-proven under the Task row lock — the exact
    token and pre-gate state, the identical authority subjects and
    qualifying execution, and the durable source projection — before the
    one-time canonical-branch bind, the exact-head gate creation and
    installation, the ``implementing → waiting_for_owner`` transition, and
    the safe OPENORC audit commit or roll back together.
    """
    workspace_id = claim.task.workspace_id
    task_id = claim.task.id
    with composed_transaction(pool) as transaction_pool:
        locked = task_records.get_task_for_update(transaction_pool, task_id)
        if locked is None or locked.workspace_id != workspace_id:
            raise NotFoundError("the requested task is not available in this workspace")
        task = require_current_task(
            transaction_pool,
            workspace_id=workspace_id,
            task_id=task_id,
            expected_state_token=claim.expected_state_token,
        )
        _require_pr_presentation_state(transaction_pool, task=task)
        authority = _load_implementation_authority(transaction_pool, task=task)
        execution, claimed_branch = _require_qualifying_execution(
            transaction_pool, task=task, execution_id=claim.execution.id
        )
        if (
            authority.gate.id != claim.authority.gate.id
            or authority.revision.id != claim.authority.revision.id
            or authority.loop.id != claim.authority.loop.id
            or authority.iteration.id != claim.authority.iteration.id
            or execution.id != claim.execution.id
            or claimed_branch != claim.claimed_branch
        ):
            raise StaleOperationError(
                "the authorized implementation subjects have moved on; the presentation is stale"
            )
        _require_unchanged_source_state(transaction_pool, task=task)
        bound_branch = task.canonical_feature_branch
        if bound_branch is not None and bound_branch != observed_branch_name:
            raise ConflictError(
                "the task's canonical feature branch is already bound; remediation "
                "continues on the canonical branch"
            )
        if bound_branch is None:
            bound_task = task_records.bind_canonical_branch(
                transaction_pool,
                task_id,
                expected_state_token=task.state_token,
                canonical_feature_branch=observed_branch_name,
            )
            if bound_task is None:
                raise StaleOperationError(
                    "the canonical feature branch could not be bound; the presentation is stale"
                )
        else:
            bound_task = task
        gate = gate_records.create_owner_gate(
            transaction_pool,
            workspace_id=workspace_id,
            task_id=task_id,
            gate_type=OwnerGateType.PR_AUTHORIZATION,
            subject_head_sha=verified_head_sha,
        )
        installed = task_records.set_current_owner_gate(
            transaction_pool,
            task_id,
            expected_state_token=bound_task.state_token,
            owner_gate_id=gate.id,
        )
        if installed is None:
            raise StaleOperationError(
                "the pr authorization gate could not be installed as the task's "
                "current gate; the presentation is stale"
            )
        waiting = task_records.update_task_status(
            transaction_pool,
            task_id,
            expected_state_token=installed.state_token,
            status=TaskStatus.WAITING_FOR_OWNER,
        )
        if waiting is None:
            raise StaleOperationError(
                "the task could not move to waiting_for_owner; the presentation is stale"
            )
        event_coordination.record_owner_gate_created_event(
            transaction_pool,
            workspace_id=workspace_id,
            task_id=task_id,
            owner_gate_id=gate.id,
            actor=_OPENORC_ACTOR,
        )
        return PrAuthorizationGateOpened(
            task=waiting,
            owner_gate=gate,
            canonical_branch=observed_branch_name,
            verified_head_sha=verified_head_sha,
        )


def open_pr_authorization(
    pool: DatabasePool,
    github: GitHubAppClient,
    *,
    profile_id: UUID,
    workspace_id: UUID,
    task_id: UUID,
    expected_state_token: UUID,
    execution_id: UUID | None = None,
) -> PrAuthorizationGateOpened:
    """Present the exact-head PR authorization gate for a completed implementation.

    The Owner-authorized OpenOrc continuation of a durably finalized
    successful implementation (no additional Owner decision is required to
    *present* the gate): one short composed transaction validates the
    command, the account operational barrier, and exact Workspace/Task
    Owner scope; locks the exact non-archived Task row; requires the
    expected Task token and the pre-gate ``implementing`` state (no current
    gate, no canonical PR); re-derives the E5-approved exact-plan evidence,
    the CLOSED planning loop with its Reviewer acceptance, and the
    qualifying successful E6 Execution with its validated persisted result
    — the ``branch`` claim being a routing claim only. Then, with no
    database transaction open, the source issue is reconciled through the
    trusted B3 primitive and the branch claim (or the bound canonical
    branch) is observed through the shared B7 read seam — GitHub's answer
    is the only head source. Finally one short composed transaction
    re-proves every admission fact and commits the one-time canonical-branch
    bind, one pending exact-head ``PR_AUTHORIZATION`` gate installed as the
    Task's current gate, the ``implementing → waiting_for_owner``
    transition, and the safe OPENORC audit together.

    A confirmed source-requirements change takes the distinct atomic
    ``github_source_changed`` block path (TaskBlock + ``blocked`` transition
    + audit in one transaction, no gate) and fails closed; a closed source
    issue, a stable-identity mismatch, lost access, known provider failure,
    and uncertain outcomes are typed fail-closed refusals that fabricate no
    drift, no block, and no gate. Replay/race can never create a second
    current gate or an orphaned branch bind.
    """
    _require_uuid_command(profile_id, "profile_id")
    _require_uuid_command(workspace_id, "workspace_id")
    _require_uuid_command(task_id, "task_id")
    _require_uuid_command(expected_state_token, "expected_state_token")
    if execution_id is not None:
        _require_uuid_command(execution_id, "execution_id")
    with application_span(_SERVICE_TRACER_SCOPE, _OPEN_SPAN_NAME) as span:
        # Attach only after the caller-supplied identifiers proved valid: a
        # malformed command is classified without exporting its values.
        annotate_span(
            span,
            operation=_OPEN_SPAN_NAME,
            workspace_id=str(workspace_id),
            task_id=str(task_id),
        )
        # --- Phase A: admission, authority derivation, and the claim. ---
        with composed_transaction(pool) as transaction_pool:
            claim = _discover_implementation_claim(
                transaction_pool,
                profile_id=profile_id,
                workspace_id=workspace_id,
                task_id=task_id,
                expected_state_token=expected_state_token,
                execution_id=execution_id,
            )
        # --- Phase B: fresh GitHub reconciliation (no transaction open). ---
        _require_unmoved_canonical_branch(claim)
        _verify_source_authority(pool, github, claim=claim)
        subject_branch = (
            claim.task.canonical_feature_branch
            if claim.task.canonical_feature_branch is not None
            else claim.claimed_branch
        )
        branch_observation = observe_routed_repository_branch(
            pool,
            github,
            workspace_id=workspace_id,
            repository_id=claim.task.repository_id,
            subject_branch=subject_branch,
        )
        # --- Phase C: the atomic bind + exact-head gate presentation. ---
        return _present_exact_head_gate(
            pool,
            claim=claim,
            verified_head_sha=branch_observation.head_sha,
            observed_branch_name=branch_observation.branch_name,
        )


def approve_pr_authorization(
    pool: DatabasePool,
    *,
    profile_id: UUID,
    workspace_id: UUID,
    task_id: UUID,
    owner_gate_id: UUID,
    expected_state_token: UUID,
) -> ApprovedPrAuthorization:
    """Approve publication of the exact gate head — the one irreversible seam.

    One short composed transaction proves the account operational barrier,
    the exact Workspace/Task Owner scope, the current non-archived
    ``waiting_for_owner`` Task under the caller's expected token, the exact
    current pending ``PR_AUTHORIZATION`` gate with its head-SHA subject, the
    bound canonical branch, and the coherent E6 success/approved-plan
    evidence; resolves **only that gate** ``APPROVED`` one-shot with
    truthful OWNER provenance (pointer clear + token rotation); and records
    the OWNER-attributed ``owner_gate_resolved`` audit atomically. The Task
    stays ``waiting_for_owner`` — this approval is neither a transition to
    ``reviewing`` nor a PR creation and never dispatches PR composition;
    E8 (issue #83) recovers the approving Profile identity durably from
    this gate's OWNER-attributed resolution audit and independently
    re-verifies the fresh remote head before any GitHub create. Replay,
    wrong Profile, stale token, replaced/resolved/cancelled gate, and
    currentness races produce no approval and no event.
    """
    _require_uuid_command(profile_id, "profile_id")
    _require_uuid_command(workspace_id, "workspace_id")
    _require_uuid_command(task_id, "task_id")
    _require_uuid_command(owner_gate_id, "owner_gate_id")
    _require_uuid_command(expected_state_token, "expected_state_token")
    with application_span(_SERVICE_TRACER_SCOPE, _APPROVE_SPAN_NAME) as span:
        annotate_span(
            span,
            operation=_APPROVE_SPAN_NAME,
            workspace_id=str(workspace_id),
            task_id=str(task_id),
        )
        with composed_transaction(pool) as transaction_pool:
            require_account_operational(transaction_pool, profile_id=profile_id)
            require_workspace_task(
                transaction_pool,
                profile_id=profile_id,
                workspace_id=workspace_id,
                task_id=task_id,
            )
            locked = task_records.get_task_for_update(transaction_pool, task_id)
            if locked is None or locked.workspace_id != workspace_id:
                raise NotFoundError("the requested task is not available in this workspace")
            task = require_current_task(
                transaction_pool,
                workspace_id=workspace_id,
                task_id=task_id,
                expected_state_token=expected_state_token,
            )
            if task.status is not TaskStatus.WAITING_FOR_OWNER:
                raise ConflictError("the addressed task is not waiting on its owner gate")
            gate = require_current_owner_gate(
                transaction_pool, task=task, owner_gate_id=owner_gate_id
            )
            if gate.gate_type is not OwnerGateType.PR_AUTHORIZATION:
                raise ConflictError(
                    "only the current pr authorization gate approves through this seam"
                )
            if (
                gate.subject_head_sha is None
                or gate.plan_revision_id is not None
                or gate.task_pull_request_id is not None
            ):
                raise ConflictError(
                    "the pr authorization gate does not carry the exact head-sha subject"
                )
            if task.canonical_feature_branch is None:
                raise ConflictError(
                    "the task carries no bound canonical feature branch; the pr "
                    "authorization subject is incomplete"
                )
            _load_implementation_authority(transaction_pool, task=task)
            _execution, claimed_branch = _require_qualifying_execution(
                transaction_pool, task=task, execution_id=None
            )
            if claimed_branch != task.canonical_feature_branch:
                raise ConflictError(
                    "the successful implementation's branch claim does not match the "
                    "task's bound canonical branch"
                )
            resolution = gate_records.resolve_owner_gate(
                transaction_pool,
                owner_gate_id=gate.id,
                outcome=OwnerGateStatus.APPROVED,
                expected_task_state_token=expected_state_token,
            )
            if resolution.outcome is gate_records.OwnerGateResolutionOutcome.RESOLVED:
                approved_gate = resolution.gate
                approved_task = resolution.task
                if approved_gate is None or approved_task is None:
                    raise _unexplained_rejection()
            elif resolution.gate is None:
                raise NotFoundError("the requested owner gate is not available for this task")
            elif resolution.outcome is gate_records.OwnerGateResolutionOutcome.NO_OP:
                raise StaleOperationError(
                    "the owner gate has already been resolved; the approval is stale"
                )
            else:
                raise StaleOperationError(
                    "the owner gate is no longer the task's current gate; the approval is stale"
                )
            event_coordination.record_owner_gate_resolved_event(
                transaction_pool,
                workspace_id=workspace_id,
                task_id=task_id,
                owner_gate_id=approved_gate.id,
                actor=event_coordination.owner_actor(profile_id),
                outcome=OwnerGateStatus.APPROVED,
            )
            return ApprovedPrAuthorization(task=approved_task, owner_gate=approved_gate)


def cancel_pr_authorization(
    pool: DatabasePool,
    *,
    profile_id: UUID,
    workspace_id: UUID,
    task_id: UUID,
    owner_gate_id: UUID,
    expected_state_token: UUID,
) -> CancelledPrAuthorization:
    """Cancel the Task at its exact current pending PR authorization gate.

    One short composed transaction (the established E5 cancellation
    pattern): the account operational barrier, the exact Workspace/Task
    Owner scope, the current non-archived ``waiting_for_owner`` Task under
    the caller's expected token, and the exact current pending
    ``PR_AUTHORIZATION`` gate; the gate resolves ``CANCELLED`` with truthful
    OWNER provenance and the Task archives terminal ``cancelled`` with its
    safe ``task_cancelled`` event atomically. Owner cancellation is a
    terminal Task outcome — never a rejection of the implementation and
    never an Owner ``REJECTED`` verdict — and no GitHub artifact (branch,
    commits, issue) is touched. Broad cross-stage cancellation and cleanup
    coordination belong to E10 (issue #85); this seam only guarantees an
    exact pending gate cannot survive its Task's cancellation.
    """
    _require_uuid_command(profile_id, "profile_id")
    _require_uuid_command(workspace_id, "workspace_id")
    _require_uuid_command(task_id, "task_id")
    _require_uuid_command(owner_gate_id, "owner_gate_id")
    _require_uuid_command(expected_state_token, "expected_state_token")
    with application_span(_SERVICE_TRACER_SCOPE, _CANCEL_SPAN_NAME) as span:
        annotate_span(
            span,
            operation=_CANCEL_SPAN_NAME,
            workspace_id=str(workspace_id),
            task_id=str(task_id),
        )
        with composed_transaction(pool) as transaction_pool:
            require_account_operational(transaction_pool, profile_id=profile_id)
            require_workspace_task(
                transaction_pool,
                profile_id=profile_id,
                workspace_id=workspace_id,
                task_id=task_id,
            )
            locked = task_records.get_task_for_update(transaction_pool, task_id)
            if locked is None or locked.workspace_id != workspace_id:
                raise NotFoundError("the requested task is not available in this workspace")
            task = require_current_task(
                transaction_pool,
                workspace_id=workspace_id,
                task_id=task_id,
                expected_state_token=expected_state_token,
            )
            if task.status is not TaskStatus.WAITING_FOR_OWNER:
                raise ConflictError("the addressed task is not waiting on its owner gate")
            gate = require_current_owner_gate(
                transaction_pool, task=task, owner_gate_id=owner_gate_id
            )
            if gate.gate_type is not OwnerGateType.PR_AUTHORIZATION:
                raise ConflictError(
                    "only the current pr authorization gate cancels through this seam"
                )
            resolution = gate_records.resolve_owner_gate(
                transaction_pool,
                owner_gate_id=gate.id,
                outcome=OwnerGateStatus.CANCELLED,
                expected_task_state_token=expected_state_token,
            )
            if resolution.outcome is gate_records.OwnerGateResolutionOutcome.RESOLVED:
                cancelled_gate = resolution.gate
                cancelled_task = resolution.task
                if cancelled_gate is None or cancelled_task is None:
                    raise _unexplained_rejection()
            elif resolution.gate is None:
                raise NotFoundError("the requested owner gate is not available for this task")
            elif resolution.outcome is gate_records.OwnerGateResolutionOutcome.NO_OP:
                raise StaleOperationError(
                    "the owner gate has already been resolved; the cancellation is stale"
                )
            else:
                raise StaleOperationError(
                    "the owner gate is no longer the task's current gate; the cancellation is stale"
                )
            owner_actor = event_coordination.owner_actor(profile_id)
            event_coordination.record_owner_gate_resolved_event(
                transaction_pool,
                workspace_id=workspace_id,
                task_id=task_id,
                owner_gate_id=cancelled_gate.id,
                actor=owner_actor,
                outcome=OwnerGateStatus.CANCELLED,
            )
            archived = task_records.archive_task(
                transaction_pool,
                task_id,
                expected_state_token=cancelled_task.state_token,
                terminal_status=TaskStatus.CANCELLED,
            )
            if archived is None:
                raise StaleOperationError(
                    "the task could not be cancelled; the cancellation is stale"
                )
            event_coordination.record_task_terminal_event(
                transaction_pool,
                workspace_id=workspace_id,
                task_id=task_id,
                actor=owner_actor,
                terminal_status=TaskStatus.CANCELLED,
            )
            return CancelledPrAuthorization(task=archived, owner_gate=cancelled_gate)


def _resolve_producer_session(transaction_pool: DatabasePool, *, task: Task) -> TaskAgentSession:
    """Resolve the Task's original Producer binding — never allocated or replaced.

    The binding identity alone is re-proven (E7 never dispatches the
    runtime): no READY requirement, because the finalize may legitimately
    leave the binding ended while its durable result remains the authority.
    """
    session = session_records.get_task_agent_session(
        transaction_pool, task_id=task.id, role=WorkflowRole.PRODUCER
    )
    if session is None or session.task_id != task.id or session.workspace_id != task.workspace_id:
        raise NotFoundError("the requested producer session is not available for this task")
    if session.external_session_id is None or not session.external_session_id.strip():
        raise ConflictError("the producer session binding carries no external session identity")
    return session


def _load_implementation_authority(
    transaction_pool: DatabasePool, *, task: Task
) -> _ImplementationAuthority:
    """Derive the complete implementation authority from canonical state.

    Every fact is resolved from durable Task-bound state — never from a
    caller-supplied acceptance, a detached gate status, or a submitted JSON
    document detached from its durable Task/plan relationships.
    """
    if task.current_plan_revision_id is None:
        raise ConflictError("the task carries no current plan revision")
    revision = require_current_plan_revision(
        transaction_pool, task=task, plan_revision_id=task.current_plan_revision_id
    )
    gate = _resolve_approved_implementation_gate(
        transaction_pool, task=task, revision_id=revision.id
    )
    loop = _resolve_closed_planning_loop(transaction_pool, task=task)
    iteration = _resolve_accepted_iteration(transaction_pool, loop=loop, revision_id=revision.id)
    producer_session = _resolve_producer_session(transaction_pool, task=task)
    return _ImplementationAuthority(
        gate=gate,
        revision=revision,
        loop=loop,
        iteration=iteration,
        producer_session=producer_session,
    )
