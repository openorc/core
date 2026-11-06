"""F8d: the Owner Ignore action for the exact GitHub issue change (issue #289).

``ignore_github_source_change`` is the ONE Owner-authorized Ignore command
for a Task blocked by an exactly-evidenced GitHub requirements change: the
Task's single ``source_requirements_fingerprint`` — its last Owner-accepted
comparison value, frozen at successful Start — is overwritten with the
exact observed fingerprint the resolved ``GITHUB_ISSUE_CHANGE`` gate's bound
TaskBlock carries, the exact current exceptional gate and its sole
unresolved ``GITHUB_SOURCE_CHANGED`` block resolve, and the interrupted E5
``IMPLEMENTATION_AUTHORIZATION`` or E7 ``PR_AUTHORIZATION`` approval
boundary presents its existing gate again — all in one all-or-nothing
composition.

The command has exactly three phases, each failing closed without any
write when a condition does not hold:

- **Verify** (one short composed transaction, zero effects): the
  authenticated Owner's account-operational barrier and exact
  Workspace/Task Owner scope, the locked non-archived ``blocked`` Task
  under the caller's expected token, the exact current pending
  ``GITHUB_ISSUE_CHANGE`` gate, its sole unresolved
  ``github_source_changed`` TaskBlock with coherent, digest-shaped
  expected/observed fingerprint evidence, and the interrupted approval
  boundary classified from durable state — the single planning ReviewLoop's
  status separates the two boundaries, and each candidate's full
  continuation derivation (the boundary's own E5/E7 authority checks) must
  also hold. An unclassifiable or unsafe boundary refuses the whole Ignore:
  unsafe or unverified conditions are never treated as successful recovery.
- **Fresh exactness** (no transaction open): the trusted B3 primitive
  freshly reconciles the authoritative GitHub issue, which must still be
  the same authorized source and carry exactly the block's observed
  fingerprint — a stale or moved-on observation changes nothing. The E7
  boundary additionally proves the existing canonical-branch authority
  guard (the bound branch must match the completed implementation's claim)
  and then observes the branch head through the shared B7 seam, the exact
  fact the interrupted presentation binds. Issue closure
  is deliberately not a refusal here: closure alone does not stop an
  active Task, and openness is checked where each boundary's own approval
  conditions place it.
- **Apply** (one short composed transaction): every verified fact is
  re-proven under the Task row lock together with the durable exactness of
  the freshly written projection; then — atomically — the exceptional gate
  resolves APPROVED with truthful OWNER provenance, the block resolves
  with its previous/observed evidence retained untouched, the guarded
  acknowledge statement overwrites the fingerprint and restores the
  derived pre-gate status, the truthful OWNER ``owner_gate_resolved`` audit
  commits, and the interrupted boundary's database-only presentation
  composition completes inside the same transaction, installing the
  existing approval gate. Any failure rolls the whole composition back,
  leaving the exact F7c-consistent blocked state (the pending Ignore gate
  and its unresolved block) — the durable, safely resumable continuation:
  the Owner re-issues the Ignore or cancels. A successful Ignore never
  leaves the Task between unblock and gate presentation.

No second Task field, no new gate type or status, no replanning,
re-implementation, or replayed external effect, no generic recovery
mechanism, and no client transport exists here.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from enum import StrEnum
from uuid import UUID

from openorc.adapters.github import GitHubAppClient
from openorc.domain.blocking_cases import (
    BlockingCaseDomainError,
    Level3BlockingCondition,
    validate_blocking_case_evidence,
)
from openorc.domain.blocks import TaskBlock, TaskBlockReason
from openorc.domain.gates import OwnerGate, OwnerGateStatus, OwnerGateType
from openorc.domain.reviews import ReviewLoopPurpose, ReviewLoopStatus
from openorc.domain.tasks import Task, TaskStatus
from openorc.observability import annotate_span, application_span
from openorc.persistence import blocks as block_records
from openorc.persistence import github_issues as issue_records
from openorc.persistence import reviews as review_records
from openorc.persistence import tasks as task_records
from openorc.persistence.gates import resolve_owner_gate as resolve_gate_records
from openorc.persistence.pool import DatabasePool
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
from openorc.services.task_implementation_authorization import (
    ImplementationGateOpened,
    present_implementation_authorization_gate,
    require_resolution_applied,
    resolve_implementation_authorization_continuation,
)
from openorc.services.task_pr_authorization import (
    PrAuthorizationGateOpened,
    present_pr_authorization_gate,
    require_canonical_branch_authority,
    resolve_pr_authorization_continuation,
)
from openorc.services.task_subject_guards import (
    require_current_owner_gate,
    require_current_task,
    require_owner_gate_unresolved_block_subject,
)
from openorc.services.transaction_composition import composed_transaction
from openorc.services.workspace_authorization import require_workspace_task

__all__ = [
    "IgnoredSourceChange",
    "IgnoredSourceChangeBoundary",
    "ignore_github_source_change",
]

_SERVICE_TRACER_SCOPE = "openorc.services.task_source_change_ignore"
_IGNORE_SPAN_NAME = "task_source_change_ignore.ignore_github_source_change"

# The block's observed fingerprint is the acknowledged comparison value; its
# shape is the same lowercase hex SHA-256 digest vocabulary the Task baseline
# carries (the database CHECK backstops the write).
_FINGERPRINT_SHAPE = re.compile(r"^[0-9a-f]{64}$")

_PR_AUTHORIZATION_CONTINUATION_STATUS = TaskStatus.IMPLEMENTING


def _require_uuid_command(value: object, name: str) -> None:
    """Reject a malformed UUID command argument before any state is touched."""
    if not isinstance(value, UUID):
        raise InvalidCommandError(f"{name} must be a UUID")


class IgnoredSourceChangeBoundary(StrEnum):
    """The interrupted Owner approval boundary the Ignore resumes (issue #289)."""

    IMPLEMENTATION_AUTHORIZATION = "implementation_authorization"
    PR_AUTHORIZATION = "pr_authorization"


@dataclass(frozen=True, slots=True)
class IgnoredSourceChange:
    """The one atomic effect of a successful Owner Ignore (issue #289).

    ``task`` is the post-presentation Task at the resumed approval gate
    (``waiting_for_owner`` under its rotated token); ``resolved_owner_gate``
    is the exceptional ``GITHUB_ISSUE_CHANGE`` gate resolved APPROVED with
    truthful OWNER provenance; ``resolved_task_block`` is the resolved
    blocking condition whose previous/observed fingerprint evidence stays
    retained untouched; ``presented`` is the interrupted boundary's own
    presentation handoff.
    """

    boundary: IgnoredSourceChangeBoundary
    task: Task
    resolved_owner_gate: OwnerGate
    resolved_task_block: TaskBlock
    acknowledged_fingerprint: str
    presented: ImplementationGateOpened | PrAuthorizationGateOpened


@dataclass(frozen=True, slots=True)
class _InterruptedBoundary:
    """The classified boundary, its restore status, and the E7 branch claim."""

    kind: IgnoredSourceChangeBoundary
    restore_status: TaskStatus
    claimed_branch: str | None


@dataclass(frozen=True, slots=True)
class _VerifiedIgnoreSubject:
    """The Phase-1 verified durable facts, carried into the fresh exactness."""

    task: Task
    gate: OwnerGate
    block: TaskBlock
    observed_fingerprint: str
    boundary: _InterruptedBoundary


@dataclass(frozen=True, slots=True)
class _FreshExactness:
    """The fresh GitHub facts: the exact fingerprint and, for E7, the head."""

    head_sha: str | None
    branch_name: str | None


def _classify_interrupted_boundary(
    transaction_pool: DatabasePool, *, task: Task
) -> _InterruptedBoundary:
    """Derive which approval boundary the drift interrupted (issue #289).

    The one planning ReviewLoop's status separates the two boundaries: an
    OPEN planning loop is the plan-approval boundary (E5, restore
    ``planning`` for the ordinary E3b shape or ``waiting_for_owner`` for the
    mediated E3c settled shape), and the single CLOSED loop following E5
    approval is the PR-authorization boundary (E7, restore ``implementing``).
    Each candidate must additionally hold its boundary's full continuation
    derivation — the exact E5/E7 authority checks — before the classification
    succeeds; anything unclassifiable or unsafe refuses the whole Ignore and
    leaves the Task blocked.
    """
    loops = review_records.list_task_review_loops(transaction_pool, task_id=task.id)
    planning = [loop for loop in loops if loop.purpose is ReviewLoopPurpose.PLANNING]
    open_planning = [loop for loop in planning if loop.status is ReviewLoopStatus.OPEN]
    closed_planning = [loop for loop in planning if loop.status is ReviewLoopStatus.CLOSED]
    if len(open_planning) == 1 and not closed_planning:
        restore_status = resolve_implementation_authorization_continuation(
            transaction_pool, task=task
        )
        return _InterruptedBoundary(
            kind=IgnoredSourceChangeBoundary.IMPLEMENTATION_AUTHORIZATION,
            restore_status=restore_status,
            claimed_branch=None,
        )
    if len(closed_planning) == 1 and not open_planning:
        claimed_branch = resolve_pr_authorization_continuation(transaction_pool, task=task)
        return _InterruptedBoundary(
            kind=IgnoredSourceChangeBoundary.PR_AUTHORIZATION,
            restore_status=_PR_AUTHORIZATION_CONTINUATION_STATUS,
            claimed_branch=claimed_branch,
        )
    raise ConflictError(
        "the interrupted approval boundary could not be established from durable state; "
        "the ignore action is refused"
    )


def _require_block_evidence(block: TaskBlock, *, task: Task) -> str:
    """Prove the block's exact requirements-change evidence and return it.

    The Level 3 requirements-change evidence coherence is re-proven against
    the block's own retained context, the observed fingerprint must be the
    digest-shaped comparison value, and the recorded expected fingerprint
    must still be the Task's stored baseline — the truthful overwrite
    replaces exactly that value.
    """
    if block.reason is not TaskBlockReason.GITHUB_SOURCE_CHANGED:
        raise ConflictError("the addressed gate's block subject is not a github source change")
    try:
        validate_blocking_case_evidence(
            Level3BlockingCondition.GITHUB_ISSUE_REQUIREMENTS_CHANGE, block.context
        )
    except BlockingCaseDomainError as error:
        raise ConflictError(
            "the github source change block carries incoherent fingerprint evidence"
        ) from error
    observed = block.context.get("observed_fingerprint")
    if not isinstance(observed, str) or not _FINGERPRINT_SHAPE.fullmatch(observed):
        raise ConflictError(
            "the github source change block's observed fingerprint is not a requirements digest"
        )
    if block.context.get("expected_fingerprint") != task.source_requirements_fingerprint:
        raise ConflictError(
            "the github source change block's expected fingerprint is no longer the "
            "task's stored comparison value; the ignore action is refused"
        )
    return observed


def _verify_ignore_subject(
    transaction_pool: DatabasePool,
    *,
    profile_id: UUID,
    workspace_id: UUID,
    task_id: UUID,
    owner_gate_id: UUID,
    expected_state_token: UUID,
) -> _VerifiedIgnoreSubject:
    """The Phase-1 verification transaction: every durable fact, zero effects."""
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
    if task.status is not TaskStatus.BLOCKED:
        raise ConflictError("the addressed task is not blocked; the ignore action is not available")
    gate = require_current_owner_gate(transaction_pool, task=task, owner_gate_id=owner_gate_id)
    if gate.gate_type is not OwnerGateType.GITHUB_ISSUE_CHANGE:
        raise ConflictError("only the current github issue change gate ignores through this seam")
    block = require_owner_gate_unresolved_block_subject(transaction_pool, task=task, gate=gate)
    observed_fingerprint = _require_block_evidence(block, task=task)
    boundary = _classify_interrupted_boundary(transaction_pool, task=task)
    return _VerifiedIgnoreSubject(
        task=task,
        gate=gate,
        block=block,
        observed_fingerprint=observed_fingerprint,
        boundary=boundary,
    )


def _verify_fresh_exactness(
    pool: DatabasePool,
    github: GitHubAppClient,
    *,
    verified: _VerifiedIgnoreSubject,
) -> _FreshExactness:
    """The Phase-2 fresh exactness verification (no transaction open).

    The trusted B3 primitive freshly reconciles the authoritative GitHub
    issue: it must still be the same authorized source and carry exactly
    the block's observed fingerprint — the only value the Ignore may
    acknowledge — so a stale or moved-on observation changes nothing. The
    E7 boundary additionally proves the existing canonical-branch
    authority guard — the bound canonical branch must match the completed
    implementation's branch claim, the exact refusal the normal
    presentation raises — before observing the branch head through the
    shared B7 seam: the exact fact the interrupted presentation binds.
    Issue closure is deliberately not a refusal: closure alone does not
    stop an active Task, and openness is checked where each boundary's own
    approval conditions place it.
    """
    task = verified.task
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
    if projection.requirements_fingerprint != verified.observed_fingerprint:
        raise ConflictError(
            "the github source issue's requirements moved on since the observed change; "
            "the ignore action cannot acknowledge a stale observation"
        )
    if verified.boundary.kind is not IgnoredSourceChangeBoundary.PR_AUTHORIZATION:
        return _FreshExactness(head_sha=None, branch_name=None)
    claimed_branch = verified.boundary.claimed_branch
    if claimed_branch is None:
        raise ConflictError(
            "the interrupted pr authorization boundary carries no branch claim to observe"
        )
    # The existing E7 canonical-branch authority guard: a bound canonical
    # branch that differs from the completed implementation's branch claim
    # is an unsafe continuation — the exact typed refusal the normal
    # presentation raises — proven here before any head read, and re-proven
    # inside the final atomic composition's fresh claim derivation.
    require_canonical_branch_authority(task=task, claimed_branch=claimed_branch)
    subject_branch = (
        task.canonical_feature_branch
        if task.canonical_feature_branch is not None
        else claimed_branch
    )
    branch_observation = observe_routed_repository_branch(
        pool,
        github,
        workspace_id=task.workspace_id,
        repository_id=task.repository_id,
        subject_branch=subject_branch,
    )
    return _FreshExactness(
        head_sha=branch_observation.head_sha,
        branch_name=branch_observation.branch_name,
    )


def _apply_ignore(
    pool: DatabasePool,
    *,
    profile_id: UUID,
    verified: _VerifiedIgnoreSubject,
    fresh: _FreshExactness,
) -> IgnoredSourceChange:
    """The Phase-3 commit: resolve, acknowledge, restore, and present — atomically.

    One short composed transaction re-proves every verified fact (the fresh
    GitHub reconciliation ran between the verification transaction and this
    commit) together with the durable exactness of the freshly written
    projection; then the exceptional gate resolves APPROVED with truthful
    OWNER provenance, the block resolves with its evidence retained, the
    guarded acknowledge statement overwrites the fingerprint and restores
    the derived pre-gate status, the truthful OWNER audit commits, and the
    interrupted boundary's database-only presentation composition completes
    inside the same transaction. Any failure rolls the whole composition
    back, leaving the exact F7c-consistent blocked state.
    """
    workspace_id = verified.task.workspace_id
    task_id = verified.task.id
    with composed_transaction(pool) as transaction_pool:
        require_account_operational(transaction_pool, profile_id=profile_id)
        locked = task_records.get_task_for_update(transaction_pool, task_id)
        if locked is None or locked.workspace_id != workspace_id:
            raise NotFoundError("the requested task is not available in this workspace")
        task = require_current_task(
            transaction_pool,
            workspace_id=workspace_id,
            task_id=task_id,
            expected_state_token=verified.task.state_token,
        )
        if task.status is not TaskStatus.BLOCKED:
            raise ConflictError(
                "the addressed task is no longer blocked; the ignore action is stale"
            )
        gate = require_current_owner_gate(
            transaction_pool, task=task, owner_gate_id=verified.gate.id
        )
        if gate.gate_type is not OwnerGateType.GITHUB_ISSUE_CHANGE:
            raise ConflictError(
                "only the current github issue change gate ignores through this seam"
            )
        block = require_owner_gate_unresolved_block_subject(transaction_pool, task=task, gate=gate)
        observed_fingerprint = _require_block_evidence(block, task=task)
        if observed_fingerprint != verified.observed_fingerprint:
            raise StaleOperationError(
                "the observed fingerprint evidence moved on; the ignore action is stale"
            )
        # The durable exactness re-proof: the freshly written projection must
        # still carry the observed drift and nothing else.
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
                "baseline; the ignore action is stale"
            )
        if projection.requirements_fingerprint != observed_fingerprint:
            raise ConflictError(
                "the durable source requirements moved on since the ignore was verified; "
                "the ignore action is stale"
            )
        return _commit_ignore(
            transaction_pool,
            profile_id=profile_id,
            workspace_id=workspace_id,
            task_id=task_id,
            gate=gate,
            block=block,
            observed_fingerprint=observed_fingerprint,
            verified=verified,
            fresh=fresh,
        )


def _commit_ignore(
    transaction_pool: DatabasePool,
    *,
    profile_id: UUID,
    workspace_id: UUID,
    task_id: UUID,
    gate: OwnerGate,
    block: TaskBlock,
    observed_fingerprint: str,
    verified: _VerifiedIgnoreSubject,
    fresh: _FreshExactness,
) -> IgnoredSourceChange:
    """The commit's resolution, acknowledge, audit, and presentation effects."""
    # The exact current exceptional gate resolves APPROVED with truthful
    # OWNER provenance: the pointer clears and the token rotates in the
    # same one-shot resolution.
    resolution = resolve_gate_records(
        transaction_pool,
        owner_gate_id=gate.id,
        outcome=OwnerGateStatus.APPROVED,
        expected_task_state_token=verified.task.state_token,
    )
    resolved_gate, gate_task = require_resolution_applied(resolution)
    resolved_block = block_records.resolve_task_block(transaction_pool, task_block_id=block.id)
    if resolved_block is None:
        raise StaleOperationError(
            "the github source change block has already resolved; the ignore action is stale"
        )
    # The acknowledged fingerprint becomes the Task's last Owner-accepted
    # comparison value and the Task leaves blocked for the interrupted
    # boundary's pre-gate status — one guarded statement, one rotation. The
    # resolved block's previous/observed evidence is retained untouched.
    restored = task_records.acknowledge_source_requirements_fingerprint(
        transaction_pool,
        task_id,
        expected_state_token=gate_task.state_token,
        fingerprint=observed_fingerprint,
        restored_status=verified.boundary.restore_status,
    )
    if restored is None:
        raise StaleOperationError(
            "the acknowledged fingerprint could not be applied; the ignore action is stale"
        )
    event_coordination.record_owner_gate_resolved_event(
        transaction_pool,
        workspace_id=workspace_id,
        task_id=task_id,
        owner_gate_id=resolved_gate.id,
        actor=event_coordination.owner_actor(profile_id),
        outcome=OwnerGateStatus.APPROVED,
    )
    # The interrupted boundary's presentation completes inside the same
    # transaction: no unblocked-but-unpresented state can ever exist.
    if verified.boundary.kind is IgnoredSourceChangeBoundary.IMPLEMENTATION_AUTHORIZATION:
        presented = present_implementation_authorization_gate(
            transaction_pool,
            profile_id=profile_id,
            workspace_id=workspace_id,
            task_id=task_id,
            expected_state_token=restored.state_token,
        )
    else:
        assert fresh.head_sha is not None and fresh.branch_name is not None
        presented = present_pr_authorization_gate(
            transaction_pool,
            profile_id=profile_id,
            workspace_id=workspace_id,
            task_id=task_id,
            expected_state_token=restored.state_token,
            execution_id=None,
            verified_head_sha=fresh.head_sha,
            observed_branch_name=fresh.branch_name,
        )
    return IgnoredSourceChange(
        boundary=verified.boundary.kind,
        task=presented.task,
        resolved_owner_gate=resolved_gate,
        resolved_task_block=resolved_block,
        acknowledged_fingerprint=observed_fingerprint,
        presented=presented,
    )


def ignore_github_source_change(
    pool: DatabasePool,
    github: GitHubAppClient,
    *,
    profile_id: UUID,
    workspace_id: UUID,
    task_id: UUID,
    owner_gate_id: UUID,
    expected_state_token: UUID,
) -> IgnoredSourceChange:
    """Ignore the exact observed GitHub issue change and resume the boundary.

    The Owner-authorized Ignore for the exact current pending
    ``GITHUB_ISSUE_CHANGE`` gate and its sole unresolved
    ``github_source_changed`` TaskBlock: the three phases — verify (zero
    effects), fresh exactness (no transaction open), and the one atomic
    commit — are each fail-closed. A successful Ignore overwrites the Task's
    single fingerprint with the exact observed value, resolves the gate
    APPROVED with truthful OWNER provenance and the block with its evidence
    retained, and reaches the interrupted boundary's existing approval gate
    in the same commit. Any refusal or failure leaves the Task exactly as it
    was: blocked at the pending gate, safe for the Owner to retry the Ignore
    or cancel.
    """
    _require_uuid_command(profile_id, "profile_id")
    _require_uuid_command(workspace_id, "workspace_id")
    _require_uuid_command(task_id, "task_id")
    _require_uuid_command(owner_gate_id, "owner_gate_id")
    _require_uuid_command(expected_state_token, "expected_state_token")
    with application_span(_SERVICE_TRACER_SCOPE, _IGNORE_SPAN_NAME) as span:
        annotate_span(
            span,
            operation=_IGNORE_SPAN_NAME,
            workspace_id=str(workspace_id),
            task_id=str(task_id),
        )
        # --- Phase 1: verification (zero effects). ---
        with composed_transaction(pool) as transaction_pool:
            verified = _verify_ignore_subject(
                transaction_pool,
                profile_id=profile_id,
                workspace_id=workspace_id,
                task_id=task_id,
                owner_gate_id=owner_gate_id,
                expected_state_token=expected_state_token,
            )
        # --- Phase 2: fresh exactness (no transaction open). ---
        fresh = _verify_fresh_exactness(pool, github, verified=verified)
        # --- Phase 3: the one atomic commit. ---
        return _apply_ignore(pool, profile_id=profile_id, verified=verified, fresh=fresh)
