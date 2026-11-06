"""The shared GitHub source-requirements fingerprint comparison (issue #289).

One comparison/blocking policy shared by the two Owner approval-gate
presentation boundaries — E5 ``IMPLEMENTATION_AUTHORIZATION`` (after
Reviewer plan acceptance) and E7 ``PR_AUTHORIZATION`` (after successful
implementation): the Task's single ``source_requirements_fingerprint`` —
its last Owner-accepted comparison value, frozen at successful Start — is
compared against the freshly reconciled authoritative GitHub issue, and a
confirmed requirements change blocks the Task through the existing F3
exceptional blocking foundation with its exact Level 3-selected
``GITHUB_ISSUE_CHANGE`` OwnerGate (issues #82, #258, #286).

- ``compare_source_fingerprint_or_block`` is the one shared comparison.
  The freshly reconciled projection must carry the Task's stable issue
  identity — a precondition of the comparison itself, never an approval
  condition — and the fingerprint must equal the Task's stored baseline.
  A confirmed change installs the atomic ``github_source_changed`` block
  with its pending ``GITHUB_ISSUE_CHANGE`` gate and raises the typed
  workflow conflict; a match returns the fresh fingerprint and the caller
  follows its existing gate-presentation path unchanged.
- Issue closure is deliberately not part of this policy: closure alone
  neither vetoes a presentation nor blocks a Task whose requirements are
  unchanged. Where a boundary's own approval conditions check openness
  (E7), they do so in their own modules; the grounding specification
  checks openness at merge, not at the approval-gate presentations.
- ``block_source_requirements_drift`` is the atomic drift-block
  composition, shared verbatim by both boundaries: one short composed
  transaction re-proves the account barrier and currentness under the
  Task row lock, re-proves the durable drift against the freshly written
  projection, and installs — atomically — the ``github_source_changed``
  TaskBlock with its exact issue identity and expected/observed
  fingerprint evidence, its pending ``GITHUB_ISSUE_CHANGE`` OwnerGate
  bound to it as the only subject, the guarded ``blocked`` transition
  with one rotated token, and the ``task_blocked`` / ``owner_gate_created``
  audits under the truthful OPENORC actor — commit or roll back together.
  A losing concurrent caller or a stale replay commits nothing at all.
- ``require_unchanged_durable_source_state`` re-proves, inside a
  presentation transaction, that the durable projection still exists under
  the Task's stable identity and still carries the exact baseline
  fingerprint — closing the window between the fresh external
  reconciliation and the presentation commit. It deliberately performs no
  closure check: openness is each boundary's own approval condition.

No polling, webhook detector, or additional checkpoint exists here: the
comparison runs exactly when a boundary presents its approval gate, and
the durable F7c invariant stays the only blocked-state authority.
"""

from __future__ import annotations

from uuid import UUID

from openorc.domain.blocking_cases import Level3BlockingCondition
from openorc.domain.blocks import TaskBlockReason
from openorc.domain.github_issues import GitHubIssueProjection
from openorc.domain.tasks import Task
from openorc.observability import application_span
from openorc.persistence import github_issues as issue_records
from openorc.persistence import tasks as task_records
from openorc.persistence.pool import DatabasePool
from openorc.services import event_coordination
from openorc.services.errors import ConflictError, NotFoundError, StaleOperationError
from openorc.services.profile_lifecycle_guard import require_account_operational
from openorc.services.task_exceptional_blocking import install_exceptional_block_gate
from openorc.services.task_level3_blocking_catalog import classify_blocking_case
from openorc.services.task_subject_guards import require_current_task
from openorc.services.transaction_composition import composed_transaction

__all__ = [
    "block_source_requirements_drift",
    "compare_source_fingerprint_or_block",
    "require_unchanged_durable_source_state",
]

_TRACER_SCOPE = "openorc.services.task_source_requirements"

# OpenOrc's own coordination fact — blocking the Task on a proven source
# change it observed while presenting an approval gate — is a safe
# control-plane fact attributed to OpenOrc, never an Owner decision.
_OPENORC_ACTOR = event_coordination.WorkflowActorContext(
    event_coordination.WorkflowEventActor.OPENORC, None
)


def require_unchanged_durable_source_state(
    transaction_pool: DatabasePool, *, task: Task
) -> GitHubIssueProjection:
    """Re-prove the durable source projection against the Task's baseline.

    The composition-level staleness re-proof shared by the presentation
    transactions: the projection must exist under the Task's stable issue
    identity and carry the exact baseline fingerprint. The freshly written
    projection is returned so a boundary can apply its own additional
    approval conditions to the same read. A drift observed here durably is
    reported for a re-run that records the atomic ``github_source_changed``
    block; no closure condition is applied (each boundary owns its own).
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
    if projection.requirements_fingerprint != task.source_requirements_fingerprint:
        raise ConflictError(
            "the task's source requirements changed during presentation; re-run the "
            "presentation to record the github_source_changed block"
        )
    return projection


def block_source_requirements_drift(
    pool: DatabasePool,
    *,
    profile_id: UUID,
    workspace_id: UUID,
    task_id: UUID,
    expected_state_token: UUID,
) -> Task:
    """Atomically block the Task on confirmed requirements drift.

    One short composed transaction re-proves the authenticated Owner's
    account-operational barrier FIRST (before any subject-row lock: the
    admission check does not remain authoritative across the intervening
    GitHub calls) and the durable drift evidence against the freshly
    written projection under the Task row lock, then the F3 exceptional
    blocking foundation (#258) installs — atomically — the
    ``github_source_changed`` TaskBlock with its exact GitHub issue
    identity and expected/observed fingerprint evidence, its pending
    Level 3-selected ``GITHUB_ISSUE_CHANGE`` OwnerGate bound to it as the
    only subject (F8b, issue #287), the guarded ``blocked`` transition
    with one rotated token, and the ``task_blocked`` /
    ``owner_gate_created`` audits under the truthful OPENORC actor —
    commit or roll back together. The caller's own boundary guards have
    already proven the exact pre-gate presentation state; the expected
    status and NULL-safe current-gate identity are re-proven here from the
    locked durable row. A moved-on Task applies nothing and is a typed
    stale operation. The distinct blocked path creates no gate
    presentation and no canonical pull request record; recovery from the
    blocked state belongs to the exceptional action services, not this
    comparison policy.
    """
    with (
        application_span(_TRACER_SCOPE, "task_source_requirements.block_source_requirements_drift"),
        composed_transaction(pool) as transaction_pool,
    ):
        require_account_operational(transaction_pool, profile_id=profile_id)
        locked = task_records.get_task_for_update(transaction_pool, task_id)
        if locked is None or locked.workspace_id != workspace_id:
            raise NotFoundError("the requested task is not available in this workspace")
        task = require_current_task(
            transaction_pool,
            workspace_id=workspace_id,
            task_id=task_id,
            expected_state_token=expected_state_token,
        )
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
        # The F3 exceptional blocking foundation (#258): one atomic block,
        # gate, transition, token rotation, and audit pair. The proven drift
        # is declared to the Level 3 blocking-case catalog (F8b, issue #287)
        # with its exact evidence — the authoritative policy, never this
        # caller, selects the exceptional OwnerGate case — and the
        # installer's guarded currentness re-proof is the durable backstop;
        # a raced or stale caller commits nothing at all.
        block_context = {
            "github_issue_id": task.github_issue_id,
            "expected_fingerprint": task.source_requirements_fingerprint,
            "observed_fingerprint": projection.requirements_fingerprint,
        }
        gate_type = classify_blocking_case(
            Level3BlockingCondition.GITHUB_ISSUE_REQUIREMENTS_CHANGE,
            block_context,
            TaskBlockReason.GITHUB_SOURCE_CHANGED,
        )
        installation = install_exceptional_block_gate(
            transaction_pool,
            workspace_id=workspace_id,
            task_id=task_id,
            expected_state_token=expected_state_token,
            expected_status=task.status,
            expected_current_owner_gate_id=task.current_owner_gate_id,
            gate_type=gate_type,
            block_reason=TaskBlockReason.GITHUB_SOURCE_CHANGED,
            block_context=block_context,
            actor=_OPENORC_ACTOR,
        )
        return installation.task


def compare_source_fingerprint_or_block(
    pool: DatabasePool,
    *,
    task: Task,
    projection: GitHubIssueProjection,
    profile_id: UUID,
    expected_state_token: UUID,
) -> str:
    """The one shared requirements-fingerprint comparison (issue #289).

    The freshly reconciled projection must carry the Task's stable issue
    identity — comparing a different issue's fingerprint would be
    meaningless — and its requirements fingerprint must equal the Task's
    stored ``source_requirements_fingerprint``. A match returns the fresh
    fingerprint and the caller follows its existing gate-presentation path
    unchanged. A confirmed change takes the shared atomic drift-block path
    and raises the typed workflow conflict: the Task is blocked as
    ``github_source_changed`` pending owner recovery, and no presentation
    proceeds. Issue closure is deliberately not a comparison outcome.
    """
    if projection.identity.github_issue_id != task.github_issue_id:
        raise ConflictError(
            "the addressed github issue number resolves to a different stable issue identity"
        )
    if projection.requirements_fingerprint == task.source_requirements_fingerprint:
        return projection.requirements_fingerprint
    block_source_requirements_drift(
        pool,
        profile_id=profile_id,
        workspace_id=task.workspace_id,
        task_id=task.id,
        expected_state_token=expected_state_token,
    )
    raise ConflictError(
        "the source github issue's requirements changed; the task is blocked as "
        "github_source_changed pending owner recovery"
    )
