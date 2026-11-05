"""Exceptional block-gate installation (issue #258, Phase 2F F3).

One guarded, reusable application service that installs the atomic
blocking foundation later capabilities consume: Level 2 operational-failure
triage (#251) escalates through it, the direct-blocking integration (#250)
converts the remaining legitimate direct blockers to it, and the Owner
action services (#254) resolve the gates it installs.

The service installs a pending exceptional OwnerGate for exactly one
reason/context-specific unresolved TaskBlock and atomically blocks the
Task under its currentness guard:

- The caller has already established its reason-specific exact
  operation/claim/GitHub subject and external evidence. This foundation
  revalidates the Task's currentness and the caller-observed
  status/current-gate identity, and the exceptional gate/reason pairing.
  It performs no GitHub/runtime reads, infers no external effect, and
  authorizes no Retry/Ignore/Cancel.
- One short ``composed_transaction``: the Task row is locked first; the
  exact token, non-archived currentness, caller-observed nonterminal
  status, and NULL-safe current-gate identity are verified under the lock;
  then one unresolved TaskBlock, one pending exceptional OwnerGate bound
  to it, the guarded current-gate pointer installation with the
  ``blocked`` transition and the ``queued_at`` clearance, and the two
  truthful audit events (``task_blocked``, ``owner_gate_created`` with the
  supplied actor) commit together — or roll back together. A losing
  concurrent caller or a stale replay commits nothing at all.
- A replaced prior current pending gate loses currency without any
  terminal stamp: it stays pending history with no manufactured Owner
  choice and no ``decided_at``, and no fictitious ``owner_gate_resolved``
  event is recorded. Only the current pointer makes a gate actionable.
- The gate/reason pairing is validated at this service boundary, kept
  consistent with the F8a Level 3 catalog (issue #286):
  ``GITHUB_ISSUE_CHANGE`` requires ``github_source_changed``;
  ``GITHUB_STATE_MISMATCH`` requires ``stale_operation`` (a stale_operation
  reason by itself is never proof of a GitHub mismatch — the caller owns
  its concrete origin/evidence); ``COST_LIMIT`` requires ``cost_limit``;
  ``OWNER_ABORT`` requires ``owner_action_required``;
  ``BLOCK_RESOLUTION`` accepts every settled reason except the terminal
  ``pr_closed_unmerged`` — including ``github_source_changed`` when the
  Level 3 classification policy cannot establish the specific Type A
  case and falls back to the Type B diagnostic gate. No generic
  reason-to-recovery policy is decided here.
- Since the F7c cutover (issue #282) the universal invariant is durably
  enforced in PostgreSQL: a committed current BLOCKED Task carries exactly
  one unresolved TaskBlock and one exact current pending exceptional
  OwnerGate bound to it, validated at commit by DEFERRABLE INITIALLY
  DEFERRED constraint triggers over Task, gate, and block mutations, with
  at most one unresolved TaskBlock per Task as the universal backstop.
  This composition's multitable sequence (block → gate → guarded Task
  mutation → audits) validates as one committed whole; a losing caller
  still commits nothing at all. The converse invariant is deliberately not
  imposed: a pending exceptional gate retained as history after archival,
  and a pending superseded ordinary gate, remain legal. Per the
  Owner-approved cutover scope (issue #282), no historical rows exist to
  convert — the enforced invariant governs every row from the start.

Expected failures use the typed application-error vocabulary: malformed
commands are ``InvalidCommandError`` (including ineligible source statuses
and pairing violations); missing or out-of-scope identity is
``NotFoundError``; stale tokens, moved-on status or current-gate identity,
and raced installations are ``StaleOperationError``. Command shape is
validated before any mutation or telemetry; no external call ever occurs
while the composition is open.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from types import MappingProxyType
from uuid import UUID

from openorc.domain.blocks import (
    TaskBlock,
    TaskBlockDomainError,
    TaskBlockReason,
    canonical_task_block_context,
)
from openorc.domain.gates import OwnerGate, OwnerGateType
from openorc.domain.tasks import Task, TaskStatus
from openorc.observability import annotate_span, application_span
from openorc.persistence import blocks as block_records
from openorc.persistence import gates as gate_records
from openorc.persistence import tasks as task_records
from openorc.persistence.pool import DatabasePool
from openorc.services import event_coordination
from openorc.services.errors import (
    InvalidCommandError,
    NotFoundError,
    StaleOperationError,
)
from openorc.services.event_coordination import WorkflowActorContext
from openorc.services.task_subject_guards import (
    require_current_owner_gate,
    require_current_task,
)
from openorc.services.transaction_composition import composed_transaction

__all__ = [
    "ExceptionalBlockInstallation",
    "install_exceptional_block_gate",
]

_TRACER_SCOPE = "openorc.services.task_exceptional_blocking"
_INSTALL_SPAN_NAME = "task_exceptional_blocking.install_exceptional_block_gate"


@dataclass(frozen=True, slots=True)
class ExceptionalBlockInstallation:
    """The one atomic effect of a successful exceptional block installation.

    ``task_block`` is the fresh unresolved blocking condition,
    ``owner_gate`` is the pending exceptional gate bound to it as its only
    subject, and ``task`` is the post-mutation Task carrying the newly
    rotated ``state_token``. Callers must continue from the returned
    token; a failed installation never fabricates a replacement token.
    """

    task_block: TaskBlock
    owner_gate: OwnerGate
    task: Task


# The eligible nonterminal source states of a new exceptional block
# installation: a ``ready_to_plan`` Task has not started autonomous work, a
# terminal Task has no recoverable work to block, and an already-``blocked``
# Task is not a new-block installation target (its recovery is the
# exceptional action services' concern, issue #254).
_ELIGIBLE_SOURCE_STATUSES: frozenset[TaskStatus] = frozenset(
    {
        TaskStatus.QUEUED,
        TaskStatus.PLANNING,
        TaskStatus.WAITING_FOR_OWNER,
        TaskStatus.IMPLEMENTING,
        TaskStatus.REVIEWING,
    }
)

# The closed exceptional gate/reason pairing: the gate type constrains
# which settled TaskBlock reasons may install it. The caller owns every
# reason's concrete origin/evidence; the pairing is only the domain-level
# coherence check, never a recovery-policy engine. Since the F8a Level 3
# catalog (issue #286), the pairing stays consistent with that catalog's
# every selectable outcome: BLOCK_RESOLUTION accepts every settled reason
# except the terminal closed-unmerged cancellation cause — including the
# github_source_changed fact when the classification policy cannot
# establish the specific GITHUB_ISSUE_CHANGE case and falls back to the
# Type B diagnostic gate — and COST_LIMIT/OWNER_ABORT accept exactly their
# own Owner-authorized stop reasons.
_EXCEPTIONAL_GATE_REASONS: Mapping[OwnerGateType, frozenset[TaskBlockReason]] = MappingProxyType(
    {
        OwnerGateType.GITHUB_ISSUE_CHANGE: frozenset({TaskBlockReason.GITHUB_SOURCE_CHANGED}),
        OwnerGateType.GITHUB_STATE_MISMATCH: frozenset({TaskBlockReason.STALE_OPERATION}),
        OwnerGateType.COST_LIMIT: frozenset({TaskBlockReason.COST_LIMIT}),
        OwnerGateType.OWNER_ABORT: frozenset({TaskBlockReason.OWNER_ACTION_REQUIRED}),
        OwnerGateType.BLOCK_RESOLUTION: frozenset(
            reason for reason in TaskBlockReason if reason is not TaskBlockReason.PR_CLOSED_UNMERGED
        ),
    }
)


def _require_uuid_command(value: object, name: str) -> None:
    """Reject a malformed UUID command argument before any state is touched."""
    if not isinstance(value, UUID):
        raise InvalidCommandError(f"{name} must be a UUID")


def install_exceptional_block_gate(
    pool: DatabasePool,
    *,
    workspace_id: UUID,
    task_id: UUID,
    expected_state_token: UUID,
    expected_status: TaskStatus,
    expected_current_owner_gate_id: UUID | None,
    gate_type: OwnerGateType,
    block_reason: TaskBlockReason,
    block_context: object,
    actor: WorkflowActorContext,
) -> ExceptionalBlockInstallation:
    """Install one exceptional block/gate and block the Task atomically.

    The caller supplies its observed Task status and current-gate identity
    (nullable) alongside the exact expected ``state_token``; all three are
    re-verified against the locked durable row. The caller has already
    established its reason-specific exact operation/claim/GitHub subject
    and external evidence; this foundation revalidates currency and the
    gate/reason pairing only.

    One short database transaction applies everything or nothing: the
    fresh unresolved TaskBlock, the pending exceptional OwnerGate bound to
    it, the current-gate pointer installation with the ``blocked``
    transition (``queued_at`` cleared when leaving ``queued``), one
    ``state_token`` rotation, and the ``task_blocked`` /
    ``owner_gate_created`` audit events under the supplied true actor. A
    replaced prior current pending gate stays pending history with no
    manufactured outcome and no ``owner_gate_resolved`` event.
    """
    with application_span(_TRACER_SCOPE, _INSTALL_SPAN_NAME) as span:
        # --- Command shape first: nothing about durable state is consulted.
        _require_uuid_command(workspace_id, "workspace_id")
        _require_uuid_command(task_id, "task_id")
        _require_uuid_command(expected_state_token, "expected_state_token")
        if expected_current_owner_gate_id is not None:
            _require_uuid_command(expected_current_owner_gate_id, "expected_current_owner_gate_id")
        if not isinstance(actor, WorkflowActorContext):
            raise InvalidCommandError("actor must be a WorkflowActorContext")
        if not isinstance(gate_type, OwnerGateType) or gate_type not in _EXCEPTIONAL_GATE_REASONS:
            raise InvalidCommandError(
                "install_exceptional_block_gate requires an exceptional "
                "OwnerGateType (block_resolution, github_issue_change, "
                "github_state_mismatch, cost_limit, or owner_abort)"
            )
        allowed_reasons = _EXCEPTIONAL_GATE_REASONS[gate_type]
        if not isinstance(block_reason, TaskBlockReason) or block_reason not in allowed_reasons:
            raise InvalidCommandError(
                f"a {gate_type.value} gate requires one of the applicable "
                "TaskBlock reasons for the exceptional gate/reason pairing"
            )
        if (
            not isinstance(expected_status, TaskStatus)
            or expected_status not in _ELIGIBLE_SOURCE_STATUSES
        ):
            raise InvalidCommandError(
                "expected_status must be an eligible nonterminal source "
                "status (queued, planning, waiting_for_owner, implementing, "
                "or reviewing); a ready_to_plan, terminal, or already-blocked "
                "Task is not a new-block installation target"
            )
        try:
            # Canonical-JSON-object shape validation only; the canonical
            # form is applied again by the persistence boundary.
            canonical_task_block_context(block_context)
        except TaskBlockDomainError as error:
            raise InvalidCommandError(
                "block_context must be a canonical JSON object "
                "(string keys at every level, JSON-representable values)"
            ) from error
        # Attach only after the caller-supplied identifiers proved valid: a
        # malformed command is classified without exporting its values.
        annotate_span(
            span,
            operation=_INSTALL_SPAN_NAME,
            workspace_id=str(workspace_id),
            task_id=str(task_id),
        )
        with composed_transaction(pool) as transaction_pool:
            # The Task row lock first (the #53 locked-read pattern): every
            # verification and write in this composition happens under this
            # lock, and the classification read observes the locked state.
            locked = task_records.get_task_for_update(transaction_pool, task_id)
            if locked is None or locked.workspace_id != workspace_id:
                raise NotFoundError("the requested task is not available in this workspace")
            task = require_current_task(
                transaction_pool,
                workspace_id=workspace_id,
                task_id=task_id,
                expected_state_token=expected_state_token,
            )
            # The caller-observed status and NULL-safe current-gate identity
            # must still match the locked durable state exactly.
            if task.status is not expected_status:
                raise StaleOperationError(
                    "the task status has moved on since this installation "
                    "was prepared; the operation is stale"
                )
            if task.current_owner_gate_id != expected_current_owner_gate_id:
                raise StaleOperationError(
                    "the task's current owner gate has moved on since this "
                    "installation was prepared; the operation is stale"
                )
            # A prior current gate must still be the exact pending,
            # same-Task/Workspace gate the caller observed (the guard also
            # proves the pointer identity; scope and pending-ness are
            # classified as NotFoundError/StaleOperationError there).
            if expected_current_owner_gate_id is not None:
                require_current_owner_gate(
                    transaction_pool,
                    task=task,
                    owner_gate_id=expected_current_owner_gate_id,
                )
            # One reason/context-specific unresolved TaskBlock, then the
            # pending exceptional gate bound to it as its only subject.
            block = block_records.create_task_block(
                transaction_pool,
                workspace_id=workspace_id,
                task_id=task_id,
                reason=block_reason,
                context=block_context,
            )
            gate = gate_records.create_owner_gate(
                transaction_pool,
                workspace_id=workspace_id,
                task_id=task_id,
                gate_type=gate_type,
                task_block_id=block.id,
            )
            # The one guarded Task mutation: pointer installation, blocked
            # transition, queued_at clearance, exactly one token rotation.
            blocked = task_records.block_task_with_exceptional_gate(
                transaction_pool,
                task_id,
                expected_state_token=expected_state_token,
                expected_status=expected_status,
                expected_current_owner_gate_id=expected_current_owner_gate_id,
                owner_gate_id=gate.id,
            )
            if blocked is None:
                # The durable backstop refused: the state moved on between
                # the locked read and the guarded write. Nothing applies.
                raise StaleOperationError(
                    "the task could not move to blocked; the exceptional "
                    "block installation is stale"
                )
            # The two truthful audit events, same composition, the supplied
            # true actor, no duplicated reason/subject payload.
            event_coordination.record_task_blocked_event(
                transaction_pool,
                workspace_id=workspace_id,
                task_id=task_id,
                task_block_id=block.id,
                actor=actor,
            )
            event_coordination.record_owner_gate_created_event(
                transaction_pool,
                workspace_id=workspace_id,
                task_id=task_id,
                owner_gate_id=gate.id,
                actor=actor,
            )
    return ExceptionalBlockInstallation(task_block=block, owner_gate=gate, task=blocked)
