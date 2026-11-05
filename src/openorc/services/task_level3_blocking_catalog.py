"""The authoritative Level 3 blocking-case catalog (issue #286, Phase 2F F8a).

One fixed, fail-closed classification policy for the blocked-Task authority
seam (issue #254 F8): a typed blocking condition plus its durable
reason/origin/evidence classifies into exactly one supported v1 exceptional
OwnerGate case, or into the Type B ``BLOCK_RESOLUTION`` diagnostic fallback.

- The policy is the only selector of an exceptional OwnerGate case.
  Originating services and Level 2 triage supply typed blocking facts and
  sanitized evidence; they never choose the gate type (F8b, issue #287,
  performs that caller cutover).
- The condition → gate-case catalog is fixed in code and never configured:
  GITHUB_ISSUE_REQUIREMENTS_CHANGE → GITHUB_ISSUE_CHANGE;
  PR_PUBLICATION_AUTHORITY_MISMATCH and MERGE_HEAD_AUTHORITY_MISMATCH →
  GITHUB_STATE_MISMATCH (two distinct typed conditions over the one
  settled gate type, with distinct evidence contracts);
  TASK_COST_LIMIT_REACHED → COST_LIMIT; OWNER_ABORT_ISSUED → OWNER_ABORT.
- Each condition carries its reason family: a declared condition whose
  durable TaskBlock reason is outside its family is not positively
  evidenced and classifies as the Type B fallback. A bare reason alone —
  ``stale_operation``, ``owner_action_required``, ``cost_limit``,
  ``github_source_changed`` — never establishes a Type A case.
- Fail-closed by construction: no declared condition, an evidence-coherence
  failure, or an out-of-family reason classifies BLOCK_RESOLUTION. The
  fallback is always atomically installable — the installer accepts
  ``BLOCK_RESOLUTION`` with every settled reason except the terminal
  ``pr_closed_unmerged`` cause.
- Policy-only: the classification reads no durable state, performs no
  I/O, chooses no recovery attempt, and executes no action. The F3
  installer's guarded currentness re-proof and the F7c invariant remain
  the durable backstops. Only malformed command shapes (a non-condition
  condition, a non-mapping evidence object, a non-reason block reason)
  raise the typed application error.
"""

from __future__ import annotations

from collections.abc import Mapping
from types import MappingProxyType

from openorc.domain.blocking_cases import (
    BlockingCaseDomainError,
    Level3BlockingCondition,
    validate_blocking_case_evidence,
)
from openorc.domain.blocks import TaskBlockReason
from openorc.domain.gates import OwnerGateType
from openorc.services.errors import InvalidCommandError

__all__ = [
    "LEVEL3_GATE_CASES",
    "LEVEL3_REASON_FAMILIES",
    "classify_blocking_case",
]

# The fixed condition → exceptional gate case catalog (issue #286). Never
# configured, never extended by callers: new supported cases arrive as
# deliberate vocabulary changes.
LEVEL3_GATE_CASES: Mapping[Level3BlockingCondition, OwnerGateType] = MappingProxyType(
    {
        Level3BlockingCondition.GITHUB_ISSUE_REQUIREMENTS_CHANGE: (
            OwnerGateType.GITHUB_ISSUE_CHANGE
        ),
        Level3BlockingCondition.PR_PUBLICATION_AUTHORITY_MISMATCH: (
            OwnerGateType.GITHUB_STATE_MISMATCH
        ),
        Level3BlockingCondition.MERGE_HEAD_AUTHORITY_MISMATCH: (
            OwnerGateType.GITHUB_STATE_MISMATCH
        ),
        Level3BlockingCondition.TASK_COST_LIMIT_REACHED: OwnerGateType.COST_LIMIT,
        Level3BlockingCondition.OWNER_ABORT_ISSUED: OwnerGateType.OWNER_ABORT,
    }
)

# The fixed condition → durable TaskBlock reason families. A declared
# condition is only positively evidenced together with its family reason;
# the installer's gate/reason pairing stays coherent by construction.
LEVEL3_REASON_FAMILIES: Mapping[Level3BlockingCondition, frozenset[TaskBlockReason]] = (
    MappingProxyType(
        {
            Level3BlockingCondition.GITHUB_ISSUE_REQUIREMENTS_CHANGE: frozenset(
                {TaskBlockReason.GITHUB_SOURCE_CHANGED}
            ),
            Level3BlockingCondition.PR_PUBLICATION_AUTHORITY_MISMATCH: frozenset(
                {TaskBlockReason.STALE_OPERATION}
            ),
            Level3BlockingCondition.MERGE_HEAD_AUTHORITY_MISMATCH: frozenset(
                {TaskBlockReason.STALE_OPERATION}
            ),
            Level3BlockingCondition.TASK_COST_LIMIT_REACHED: frozenset(
                {TaskBlockReason.COST_LIMIT}
            ),
            Level3BlockingCondition.OWNER_ABORT_ISSUED: frozenset(
                {TaskBlockReason.OWNER_ACTION_REQUIRED}
            ),
        }
    )
)


def classify_blocking_case(
    condition: Level3BlockingCondition | None,
    evidence: Mapping[str, object],
    block_reason: TaskBlockReason,
) -> OwnerGateType:
    """Classify one typed blocking condition into its exceptional gate case.

    ``condition`` is the declared supported blocking condition, or ``None``
    when the blocking fact is not one of the supported cases (a Level 2
    escalation, an operational failure, an uncertain outcome — the Type B
    diagnostic fallback territory). ``evidence`` is the sanitized,
    already-proven evidence the originator supplies; ``block_reason`` is
    the caller's durable TaskBlock reason, preserved unchanged into the
    installation.

    Returns the selected exceptional :class:`OwnerGateType`. Every
    fail-closed path — no declared condition, an evidence-coherence
    failure, or a reason outside the declared condition's family —
    returns ``OwnerGateType.BLOCK_RESOLUTION``. Only malformed command
    shapes raise :class:`InvalidCommandError`.
    """
    if not isinstance(evidence, Mapping):
        raise InvalidCommandError("blocking-case evidence must be a mapping")
    if not isinstance(block_reason, TaskBlockReason):
        raise InvalidCommandError("block_reason must be a TaskBlockReason")
    if condition is None:
        # The blocking fact is not a supported case: the Type B diagnostic
        # fallback owns it, whatever its evidence shape.
        return OwnerGateType.BLOCK_RESOLUTION
    if not isinstance(condition, Level3BlockingCondition):
        raise InvalidCommandError("condition must be a Level3BlockingCondition or None")
    if block_reason not in LEVEL3_REASON_FAMILIES[condition]:
        # A declared condition whose durable reason is outside its family
        # is not positively evidenced: fail closed to the diagnostic
        # fallback rather than trusting the declaration.
        return OwnerGateType.BLOCK_RESOLUTION
    try:
        validate_blocking_case_evidence(condition, evidence)
    except BlockingCaseDomainError:
        # Insufficient, malformed, or unrecognized evidence: the Type B
        # diagnostic fallback, never a guessed Type A case.
        return OwnerGateType.BLOCK_RESOLUTION
    return LEVEL3_GATE_CASES[condition]
