"""Level 3 blocking-case vocabulary (issue #286, Phase 2F F8a).

The typed conditions the authoritative Level 3 classification policy
(issue #254 F8) recognizes, with the minimum evidence coherence each
condition requires. Originating services and Level 2 triage supply these
typed conditions with sanitized, already-proven evidence; the policy —
never the caller — selects the exceptional OwnerGate case.

- The v1 condition vocabulary is exactly the five supported cases:
  GITHUB_ISSUE_REQUIREMENTS_CHANGE, PR_PUBLICATION_AUTHORITY_MISMATCH,
  MERGE_HEAD_AUTHORITY_MISMATCH, TASK_COST_LIMIT_REACHED, and
  OWNER_ABORT_ISSUED. Anything else — a Level 2 escalation, an operational
  failure, an uncertain outcome, a session loss — is not a supported case
  and is presented to the policy as no condition (the Type B diagnostic
  fallback).
- Minimum evidence is positive and minimal: each condition requires
  exactly the durable facts that prove it, and nothing a legitimate
  originating path cannot supply. The publication-mismatch condition
  recognizes the existing positively-evidenced variants (issue #280): the
  common authorization anchor — the exact Owner-authorized head SHA plus
  the originating OwnerGate identity — together with whatever canonical
  PR identity and observed head/ref facts that path positively carries,
  including the preflight form, which carries neither a PR identity nor
  an observed SHA because none exists. No PR identity or observed SHA is
  ever manufactured or required: the canonical PR identity is the
  OpenOrc UUID, while GitHub's own pull-request identity is a positive
  numeric value recorded as its decimal string. The merge-head condition
  keeps its distinct E10a contract (the approved MERGE_DECISION gate
  identity, the canonical TaskPullRequest, and the exact expected vs
  observed head).
- Evidence keys are closed: the catalog maps conditions to exact evidence
  shapes, not arbitrary diagnostic bags. A declared condition whose
  evidence carries unrecognized keys, malformed values, or a missing
  required fact is not positively evidenced.
- Validation is coherence only: key/type/value shape of the declared
  condition's evidence. It re-proves no external effect, performs no I/O,
  reads no durable state, and decides no recovery. The classification
  policy consumes these validators and fails closed to the Type B
  fallback when a declared condition's evidence does not satisfy its
  minimum coherence.

This module carries transport-independent validation only.
"""

from __future__ import annotations

from collections.abc import Mapping
from enum import StrEnum
from uuid import UUID

__all__ = [
    "BlockingCaseDomainError",
    "Level3BlockingCondition",
    "validate_blocking_case_evidence",
]


class BlockingCaseDomainError(Exception):
    """Raised when a declared blocking case's evidence is incoherent."""


class Level3BlockingCondition(StrEnum):
    """The settled v1 Level 3 blocking-condition vocabulary (issue #286).

    Each member is a typed blocking fact an originating service or Level 2
    triage can positively declare. There is deliberately no member for
    unsupported or operational conditions: those are presented to the
    policy as no condition and classify as the Type B diagnostic
    fallback.
    """

    GITHUB_ISSUE_REQUIREMENTS_CHANGE = "github_issue_requirements_change"
    PR_PUBLICATION_AUTHORITY_MISMATCH = "pr_publication_authority_mismatch"
    MERGE_HEAD_AUTHORITY_MISMATCH = "merge_head_authority_mismatch"
    TASK_COST_LIMIT_REACHED = "task_cost_limit_reached"
    OWNER_ABORT_ISSUED = "owner_abort_issued"


# The publication-mismatch evidence keys (issue #280): the authorization
# anchor is required; every other key is optional, positively-evidenced
# corroboration that varies by publication path. The preflight form
# carries only the anchor — no canonical PR exists to cite and no SHA was
# observed, so neither is required or manufactured. The OpenOrc-owned
# canonical PR identity is a UUID; GitHub's own pull-request identity is
# a positive numeric value recorded as its decimal string.
_PUBLICATION_ANCHOR_KEYS = ("authorized_head_sha", "owner_gate_id")
_PUBLICATION_UUID_KEYS = ("pull_request_id",)
_PUBLICATION_NUMERIC_IDENTITY_KEYS = ("github_pull_request_id",)
_PUBLICATION_OBSERVED_KEYS = (
    "github_pr_number",
    "observed_head_sha",
    "observed_head_ref",
    "observed_base_ref",
    "authorized_base_ref",
)
_PUBLICATION_KNOWN_KEYS = frozenset(
    (
        *_PUBLICATION_ANCHOR_KEYS,
        *_PUBLICATION_UUID_KEYS,
        *_PUBLICATION_NUMERIC_IDENTITY_KEYS,
        *_PUBLICATION_OBSERVED_KEYS,
    )
)

_MERGE_MISMATCH_KEYS = (
    "owner_gate_id",
    "task_pull_request_id",
    "expected_head_sha",
    "observed_head_sha",
)

_REQUIREMENTS_CHANGE_KEYS = ("github_issue_id", "expected_fingerprint", "observed_fingerprint")

_COST_LIMIT_KEYS = (
    "control",
    "reason_provenance",
    "observed_cost_usd",
    "authorized_budget_usd",
    "aggregate_includes_unobserved_sessions",
    "target_roles",
    "task_agent_session_ids",
)

_OWNER_ABORT_KEYS = ("control", "target_roles", "task_agent_session_ids")


def _require_non_empty_str(evidence: Mapping[str, object], key: str) -> str:
    value = evidence.get(key)
    if not isinstance(value, str) or not value.strip():
        raise BlockingCaseDomainError(
            f"the {key} blocking-case evidence must be a non-empty string"
        )
    return value


def _require_uuid_str(evidence: Mapping[str, object], key: str) -> str:
    value = _require_non_empty_str(evidence, key)
    try:
        UUID(value)
    except ValueError as error:
        raise BlockingCaseDomainError(
            f"the {key} blocking-case evidence must be a UUID string"
        ) from error
    return value


def _require_fixed_value(evidence: Mapping[str, object], key: str, expected: str) -> None:
    value = evidence.get(key)
    if value != expected:
        raise BlockingCaseDomainError(
            f"the {key} blocking-case evidence must be exactly {expected!r}"
        )


def _require_str_list(
    evidence: Mapping[str, object], key: str, *, allow_empty: bool = False
) -> None:
    value = evidence.get(key)
    if not isinstance(value, list) or (not value and not allow_empty):
        raise BlockingCaseDomainError(
            f"the {key} blocking-case evidence must be a non-empty list of strings"
        )
    if any(not isinstance(item, str) or not item.strip() for item in value):
        raise BlockingCaseDomainError(
            f"the {key} blocking-case evidence entries must be non-empty strings"
        )


def _require_positive_numeric_identity(evidence: Mapping[str, object], key: str) -> None:
    """A positive external numeric identity, recorded as its decimal string.

    GitHub's own numeric identities (the stable pull-request ID) are
    recorded in the E8 evidence as their decimal string form; they are
    never OpenOrc UUIDs.
    """
    value = evidence.get(key)
    if isinstance(value, int) and not isinstance(value, bool):
        if value <= 0:
            raise BlockingCaseDomainError(
                f"the {key} blocking-case evidence must be a positive numeric identity"
            )
        return
    if isinstance(value, str) and value.isascii() and value.isdigit():
        if int(value) <= 0:
            raise BlockingCaseDomainError(
                f"the {key} blocking-case evidence must be a positive numeric identity"
            )
        return
    raise BlockingCaseDomainError(
        f"the {key} blocking-case evidence must be a positive numeric identity "
        "(a positive integer or its decimal string form)"
    )


def _require_exact_keys(evidence: Mapping[str, object], keys: tuple[str, ...]) -> None:
    expected = frozenset(keys)
    observed = frozenset(evidence)
    if observed != expected:
        raise BlockingCaseDomainError(
            "the blocking-case evidence keys must be exactly "
            f"{sorted(expected)}; got {sorted(observed)}"
        )


def _validate_requirements_change_evidence(evidence: Mapping[str, object]) -> None:
    _require_exact_keys(evidence, _REQUIREMENTS_CHANGE_KEYS)
    # The stable GitHub issue identity is a positive int on the Task (the
    # exact shape the E7 drift context carries); a non-empty string form is
    # accepted as the equivalent canonical identity representation.
    issue_id = evidence.get("github_issue_id")
    if isinstance(issue_id, int) and not isinstance(issue_id, bool):
        if issue_id <= 0:
            raise BlockingCaseDomainError(
                "the github_issue_id blocking-case evidence must be a positive "
                "GitHub issue identity"
            )
    elif isinstance(issue_id, str):
        if not issue_id.strip():
            raise BlockingCaseDomainError(
                "the github_issue_id blocking-case evidence must be a "
                "non-empty GitHub issue identity"
            )
    else:
        raise BlockingCaseDomainError(
            "the github_issue_id blocking-case evidence must be a positive GitHub issue identity"
        )
    expected = _require_non_empty_str(evidence, "expected_fingerprint")
    observed = _require_non_empty_str(evidence, "observed_fingerprint")
    # The condition is the proven change: an equal fingerprint pair proves
    # no requirements change and is not positively evidenced.
    if observed == expected:
        raise BlockingCaseDomainError(
            "the observed requirements fingerprint must differ from the "
            "Task's immutable baseline for a proven requirements change"
        )


def _validate_publication_mismatch_evidence(evidence: Mapping[str, object]) -> None:
    unknown = frozenset(evidence) - _PUBLICATION_KNOWN_KEYS
    if unknown:
        raise BlockingCaseDomainError(
            f"the publication-mismatch evidence carries unrecognized keys: {sorted(unknown)}"
        )
    # The common authorization anchor every positively-evidenced variant
    # carries: the exact Owner-authorized head and the originating gate.
    _require_non_empty_str(evidence, "authorized_head_sha")
    _require_uuid_str(evidence, "owner_gate_id")
    # Optional corroboration is validated when present and never required:
    # the preflight form legitimately carries none of it. The OpenOrc-owned
    # canonical PR identity is a UUID; GitHub's own pull-request identity
    # is a positive numeric identity (issue #280 post-create outcomes).
    for key in _PUBLICATION_UUID_KEYS:
        if key in evidence:
            _require_uuid_str(evidence, key)
    for key in _PUBLICATION_NUMERIC_IDENTITY_KEYS:
        if key in evidence:
            _require_positive_numeric_identity(evidence, key)
    for key in _PUBLICATION_OBSERVED_KEYS:
        if key in evidence:
            _require_non_empty_str(evidence, key)


def _validate_merge_mismatch_evidence(evidence: Mapping[str, object]) -> None:
    _require_exact_keys(evidence, _MERGE_MISMATCH_KEYS)
    _require_uuid_str(evidence, "owner_gate_id")
    _require_uuid_str(evidence, "task_pull_request_id")
    _require_non_empty_str(evidence, "expected_head_sha")
    _require_non_empty_str(evidence, "observed_head_sha")


def _validate_cost_limit_evidence(evidence: Mapping[str, object]) -> None:
    _require_exact_keys(evidence, _COST_LIMIT_KEYS)
    _require_fixed_value(evidence, "control", "owner_abort")
    _require_fixed_value(evidence, "reason_provenance", "owner_authorized_task_budget")
    _require_non_empty_str(evidence, "observed_cost_usd")
    _require_non_empty_str(evidence, "authorized_budget_usd")
    if not isinstance(evidence.get("aggregate_includes_unobserved_sessions"), bool):
        raise BlockingCaseDomainError(
            "the aggregate_includes_unobserved_sessions blocking-case evidence must be a boolean"
        )
    # The interrupted-activity facts may legitimately be empty: the
    # cost-limit stop explicitly supports the idle form (BLOCKED_IDLE) where
    # no runtime activity requires interruption, and the context then
    # records empty role/session lists. Only their structure is enforced.
    _require_str_list(evidence, "target_roles", allow_empty=True)
    _require_str_list(evidence, "task_agent_session_ids", allow_empty=True)


def _validate_owner_abort_evidence(evidence: Mapping[str, object]) -> None:
    _require_exact_keys(evidence, _OWNER_ABORT_KEYS)
    _require_fixed_value(evidence, "control", "owner_abort")
    # The supported OWNER_ABORT origin is the specific Owner-issued Abort
    # with its exact session/Interrupt context: non-empty target facts are
    # part of that evidence (the idle cost-limit stop is the separate
    # TASK_COST_LIMIT_REACHED condition above).
    _require_str_list(evidence, "target_roles")
    _require_str_list(evidence, "task_agent_session_ids")


def validate_blocking_case_evidence(
    condition: Level3BlockingCondition,
    evidence: Mapping[str, object],
) -> None:
    """Validate one declared blocking condition's minimum evidence coherence.

    Raises :class:`BlockingCaseDomainError` when the declared condition's
    evidence does not positively satisfy the condition's minimum
    coherence. The classification policy treats every such failure as
    insufficient evidence and fails closed to the Type B diagnostic
    fallback; this validator performs no I/O and re-proves no external
    effect.
    """
    if not isinstance(condition, Level3BlockingCondition):
        raise BlockingCaseDomainError("condition must be a Level3BlockingCondition")
    if not isinstance(evidence, Mapping):
        raise BlockingCaseDomainError("blocking-case evidence must be a mapping")
    if condition is Level3BlockingCondition.GITHUB_ISSUE_REQUIREMENTS_CHANGE:
        _validate_requirements_change_evidence(evidence)
    elif condition is Level3BlockingCondition.PR_PUBLICATION_AUTHORITY_MISMATCH:
        _validate_publication_mismatch_evidence(evidence)
    elif condition is Level3BlockingCondition.MERGE_HEAD_AUTHORITY_MISMATCH:
        _validate_merge_mismatch_evidence(evidence)
    elif condition is Level3BlockingCondition.TASK_COST_LIMIT_REACHED:
        _validate_cost_limit_evidence(evidence)
    else:  # OWNER_ABORT_ISSUED
        _validate_owner_abort_evidence(evidence)
