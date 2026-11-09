"""Tests for the authoritative Level 3 blocking-case catalog (issue #286).

Ordinary deterministic tests: no database, no network (the installer fallback
consistency test drives the real installer against the scripted connection
machinery of the F3 service tests). These prove the accepted F8a contract:

- the fixed condition → gate-case catalog and reason families;
- every representative supported path classifies to its designated case —
  the proven GitHub issue-requirements change to its Type A case, and the
  proven publication and merge-head authority mismatches (preflight
  anchor-only and post-create forms alike) to the Type B
  BLOCK_RESOLUTION diagnostic gate since F8e (issue #290): the retired
  Try Again flow selects no gate any more;
- ambiguous same-reason paths, out-of-family reasons, and insufficient or
  unrecognized evidence fail closed to the Type B BLOCK_RESOLUTION
  fallback;
- the Type B fallback for an insufficiently evidenced
  github_source_changed condition remains atomically installable — the
  classification result pairs with the installer's gate/reason validation;
- malformed command shapes raise the typed application error.
"""

from __future__ import annotations

from typing import Any

import pytest

from openorc.domain.blocking_cases import Level3BlockingCondition
from openorc.domain.blocks import TaskBlockReason
from openorc.domain.gates import OwnerGateType
from openorc.services.errors import InvalidCommandError
from openorc.services.task_level3_blocking_catalog import (
    LEVEL3_GATE_CASES,
    LEVEL3_REASON_FAMILIES,
    classify_blocking_case,
)
from tests.test_services_task_exceptional_blocking import (
    ScriptedConnection,
    _install,
    _script_success,
)

_SHA = "0123456789abcdef0123456789abcdef01234567"
_OTHER_SHA = "fedcba9876543210fedcba9876543210fedcba98"
_GATE_ID = "1b234c67-8d90-4e1f-a234-56789abcdef0"
_PR_ID = "2c345d78-9e01-4f2f-b345-67890abcdef0"
_FINGERPRINT = "a" * 64
_OTHER_FINGERPRINT = "b" * 64


def _requirements_change(**overrides: Any) -> dict[str, Any]:
    evidence: dict[str, Any] = {
        "github_issue_id": 7703,
        "expected_fingerprint": _FINGERPRINT,
        "observed_fingerprint": _OTHER_FINGERPRINT,
    }
    evidence.update(overrides)
    return evidence


def _publication(**overrides: Any) -> dict[str, Any]:
    evidence: dict[str, Any] = {
        "authorized_head_sha": _SHA,
        "owner_gate_id": _GATE_ID,
    }
    evidence.update(overrides)
    return evidence


def _merge_mismatch(**overrides: Any) -> dict[str, Any]:
    evidence: dict[str, Any] = {
        "owner_gate_id": _GATE_ID,
        "task_pull_request_id": _PR_ID,
        "expected_head_sha": _SHA,
        "observed_head_sha": _OTHER_SHA,
    }
    evidence.update(overrides)
    return evidence


def _cost_limit(**overrides: Any) -> dict[str, Any]:
    evidence: dict[str, Any] = {
        "control": "owner_abort",
        "reason_provenance": "owner_authorized_task_budget",
        "observed_cost_usd": "12.50",
        "effective_threshold_usd": "10.00",
        "authorized_budget_usd": "10.00",
        "aggregate_includes_unobserved_sessions": False,
        "target_roles": ["producer"],
        "task_agent_session_ids": [_PR_ID],
    }
    evidence.update(overrides)
    return evidence


def _owner_abort(**overrides: Any) -> dict[str, Any]:
    evidence: dict[str, Any] = {
        "control": "owner_abort",
        "target_roles": ["producer", "reviewer"],
        "task_agent_session_ids": [_PR_ID],
    }
    evidence.update(overrides)
    return evidence


def test_the_catalog_maps_each_condition_to_its_settled_gate_case() -> None:
    # F8e (issue #290): the two proven GitHub authority-mismatch
    # conditions deliberately map to the Type B diagnostic fallback; no
    # supported case selects the legacy GITHUB_STATE_MISMATCH type.
    assert dict(LEVEL3_GATE_CASES) == {
        Level3BlockingCondition.GITHUB_ISSUE_REQUIREMENTS_CHANGE: (
            OwnerGateType.GITHUB_ISSUE_CHANGE
        ),
        Level3BlockingCondition.PR_PUBLICATION_AUTHORITY_MISMATCH: (OwnerGateType.BLOCK_RESOLUTION),
        Level3BlockingCondition.MERGE_HEAD_AUTHORITY_MISMATCH: (OwnerGateType.BLOCK_RESOLUTION),
        Level3BlockingCondition.TASK_COST_LIMIT_REACHED: OwnerGateType.COST_LIMIT,
        Level3BlockingCondition.OWNER_ABORT_ISSUED: OwnerGateType.OWNER_ABORT,
    }


def test_each_condition_is_evidenced_only_through_its_reason_family() -> None:
    assert dict(LEVEL3_REASON_FAMILIES) == {
        Level3BlockingCondition.GITHUB_ISSUE_REQUIREMENTS_CHANGE: frozenset(
            {TaskBlockReason.GITHUB_SOURCE_CHANGED}
        ),
        Level3BlockingCondition.PR_PUBLICATION_AUTHORITY_MISMATCH: frozenset(
            {TaskBlockReason.STALE_OPERATION}
        ),
        Level3BlockingCondition.MERGE_HEAD_AUTHORITY_MISMATCH: frozenset(
            {TaskBlockReason.STALE_OPERATION}
        ),
        Level3BlockingCondition.TASK_COST_LIMIT_REACHED: frozenset({TaskBlockReason.COST_LIMIT}),
        Level3BlockingCondition.OWNER_ABORT_ISSUED: frozenset(
            {TaskBlockReason.OWNER_ACTION_REQUIRED}
        ),
    }


def test_representative_supported_paths_classify_to_their_designated_cases() -> None:
    # E7: the proven GitHub issue-requirements change.
    assert (
        classify_blocking_case(
            Level3BlockingCondition.GITHUB_ISSUE_REQUIREMENTS_CHANGE,
            _requirements_change(),
            TaskBlockReason.GITHUB_SOURCE_CHANGED,
        )
        is OwnerGateType.GITHUB_ISSUE_CHANGE
    )
    # E8: the proven publication authority mismatch — the preflight
    # anchor-only form and the reconciled post-create form alike —
    # deliberately classifies to the Type B diagnostic gate (F8e, issue
    # #290): the Try Again flow is retired.
    assert (
        classify_blocking_case(
            Level3BlockingCondition.PR_PUBLICATION_AUTHORITY_MISMATCH,
            _publication(),
            TaskBlockReason.STALE_OPERATION,
        )
        is OwnerGateType.BLOCK_RESOLUTION
    )
    assert (
        classify_blocking_case(
            Level3BlockingCondition.PR_PUBLICATION_AUTHORITY_MISMATCH,
            _publication(pull_request_id=_PR_ID, observed_head_sha=_OTHER_SHA),
            TaskBlockReason.STALE_OPERATION,
        )
        is OwnerGateType.BLOCK_RESOLUTION
    )
    # The real E8 post-create evidence shape: GitHub's own numeric PR
    # identity (str(created.github_pr_id)) with no observed head.
    assert (
        classify_blocking_case(
            Level3BlockingCondition.PR_PUBLICATION_AUTHORITY_MISMATCH,
            _publication(
                pull_request_id=_PR_ID,
                github_pull_request_id="1770032834",
            ),
            TaskBlockReason.STALE_OPERATION,
        )
        is OwnerGateType.BLOCK_RESOLUTION
    )
    # E10a: the proven merge-head authority mismatch (its distinct
    # evidence contract) routes to the same Type B diagnostic gate (F8e,
    # issue #290).
    assert (
        classify_blocking_case(
            Level3BlockingCondition.MERGE_HEAD_AUTHORITY_MISMATCH,
            _merge_mismatch(),
            TaskBlockReason.STALE_OPERATION,
        )
        is OwnerGateType.BLOCK_RESOLUTION
    )
    # The Owner-authorized cost limit and the specific Owner abort.
    assert (
        classify_blocking_case(
            Level3BlockingCondition.TASK_COST_LIMIT_REACHED,
            _cost_limit(),
            TaskBlockReason.COST_LIMIT,
        )
        is OwnerGateType.COST_LIMIT
    )
    # An interrupted-activity-free evidence shape stays structurally valid:
    # the amended contract composes no block for a target-free crossing, but
    # the evidence validator still accepts the empty-list form for the
    # committed-history shapes.
    assert (
        classify_blocking_case(
            Level3BlockingCondition.TASK_COST_LIMIT_REACHED,
            _cost_limit(target_roles=[], task_agent_session_ids=[]),
            TaskBlockReason.COST_LIMIT,
        )
        is OwnerGateType.COST_LIMIT
    )
    assert (
        classify_blocking_case(
            Level3BlockingCondition.OWNER_ABORT_ISSUED,
            _owner_abort(),
            TaskBlockReason.OWNER_ACTION_REQUIRED,
        )
        is OwnerGateType.OWNER_ABORT
    )


def test_unsupported_blocking_facts_fail_closed_to_type_b() -> None:
    # A Level 2 escalation, an operational failure, an uncertain outcome —
    # no supported condition is declared, so the diagnostic fallback owns
    # the blocking fact whatever its reason or evidence shape.
    assert (
        classify_blocking_case(
            None,
            {"error_code": "runtime_unavailable", "effect_evidence": "effect_unclassified"},
            TaskBlockReason.RUNTIME_FAILURE,
        )
        is OwnerGateType.BLOCK_RESOLUTION
    )
    assert (
        classify_blocking_case(None, {}, TaskBlockReason.AGENT_SESSION_LOST)
        is OwnerGateType.BLOCK_RESOLUTION
    )


def test_ambiguous_same_reason_paths_fail_closed_to_type_b() -> None:
    # A bare stale_operation reason is never proof of a GitHub mismatch:
    # anchor-less publication evidence and an out-of-family reason both
    # fall back instead of trusting the declaration.
    assert (
        classify_blocking_case(
            Level3BlockingCondition.PR_PUBLICATION_AUTHORITY_MISMATCH,
            {"observed_head_sha": _OTHER_SHA},
            TaskBlockReason.STALE_OPERATION,
        )
        is OwnerGateType.BLOCK_RESOLUTION
    )
    assert (
        classify_blocking_case(
            Level3BlockingCondition.PR_PUBLICATION_AUTHORITY_MISMATCH,
            _publication(),
            TaskBlockReason.EXTERNAL_OPERATION_UNCERTAIN,
        )
        is OwnerGateType.BLOCK_RESOLUTION
    )
    # A bare owner_action_required reason without the exact owner-abort
    # session context, and a cost-limit declaration without its standing
    # budget facts, are equally insufficient.
    assert (
        classify_blocking_case(
            Level3BlockingCondition.OWNER_ABORT_ISSUED,
            {"origin": "unclear"},
            TaskBlockReason.OWNER_ACTION_REQUIRED,
        )
        is OwnerGateType.BLOCK_RESOLUTION
    )
    assert (
        classify_blocking_case(
            Level3BlockingCondition.TASK_COST_LIMIT_REACHED,
            {"observed_cost_usd": "12.50"},
            TaskBlockReason.COST_LIMIT,
        )
        is OwnerGateType.BLOCK_RESOLUTION
    )
    # A condition declared against another condition's family reason.
    assert (
        classify_blocking_case(
            Level3BlockingCondition.OWNER_ABORT_ISSUED,
            _owner_abort(),
            TaskBlockReason.COST_LIMIT,
        )
        is OwnerGateType.BLOCK_RESOLUTION
    )
    # Equal fingerprints prove no requirements change.
    assert (
        classify_blocking_case(
            Level3BlockingCondition.GITHUB_ISSUE_REQUIREMENTS_CHANGE,
            _requirements_change(observed_fingerprint=_FINGERPRINT),
            TaskBlockReason.GITHUB_SOURCE_CHANGED,
        )
        is OwnerGateType.BLOCK_RESOLUTION
    )
    # Unrecognized evidence keys are not positive evidence.
    assert (
        classify_blocking_case(
            Level3BlockingCondition.MERGE_HEAD_AUTHORITY_MISMATCH,
            {**_merge_mismatch(), "extra": "fact"},
            TaskBlockReason.STALE_OPERATION,
        )
        is OwnerGateType.BLOCK_RESOLUTION
    )


def test_the_type_b_fallback_for_github_source_changed_is_atomically_installable() -> None:
    # The acceptance-critical seam: an insufficiently evidenced
    # github_source_changed condition classifies to the Type B diagnostic
    # fallback, and the classification result pairs with the atomic
    # installer's gate/reason validation — F8b can commit it unchanged.
    selected = classify_blocking_case(
        Level3BlockingCondition.GITHUB_ISSUE_REQUIREMENTS_CHANGE,
        {"github_issue_id": 7703, "expected_fingerprint": _FINGERPRINT},
        TaskBlockReason.GITHUB_SOURCE_CHANGED,
    )
    assert selected is OwnerGateType.BLOCK_RESOLUTION

    conn = ScriptedConnection()
    _script_success(conn, gate_type=selected.value)
    result = _install(
        conn,
        gate_type=selected,
        block_reason=TaskBlockReason.GITHUB_SOURCE_CHANGED,
        block_context={"github_issue_id": 7703, "expected_fingerprint": _FINGERPRINT},
    )
    assert result.owner_gate.gate_type is OwnerGateType.BLOCK_RESOLUTION
    assert result.task.status.name == "BLOCKED"


def test_the_authority_mismatch_type_b_routing_is_atomically_installable() -> None:
    # F8e (issue #290): the proven E8 publication and E10a merge-head
    # authority mismatches classify to the Type B diagnostic fallback, and
    # the classification result pairs with the atomic installer's
    # gate/reason validation — the exact stale_operation evidence rides
    # the block unchanged.
    selected = classify_blocking_case(
        Level3BlockingCondition.MERGE_HEAD_AUTHORITY_MISMATCH,
        _merge_mismatch(),
        TaskBlockReason.STALE_OPERATION,
    )
    assert selected is OwnerGateType.BLOCK_RESOLUTION

    conn = ScriptedConnection()
    _script_success(conn, gate_type=selected.value)
    result = _install(
        conn,
        gate_type=selected,
        block_reason=TaskBlockReason.STALE_OPERATION,
        block_context=_merge_mismatch(),
    )
    assert result.owner_gate.gate_type is OwnerGateType.BLOCK_RESOLUTION
    assert result.task.status.name == "BLOCKED"


def test_malformed_command_shapes_raise_the_typed_application_error() -> None:
    with pytest.raises(InvalidCommandError):
        classify_blocking_case(
            Level3BlockingCondition.OWNER_ABORT_ISSUED,
            ["not", "a", "mapping"],  # type: ignore[arg-type]
            TaskBlockReason.OWNER_ACTION_REQUIRED,
        )
    with pytest.raises(InvalidCommandError):
        classify_blocking_case(
            Level3BlockingCondition.OWNER_ABORT_ISSUED,  # type: ignore[arg-type]
            _owner_abort(),
            "not_a_reason",  # type: ignore[arg-type]
        )
    with pytest.raises(InvalidCommandError):
        classify_blocking_case(
            "requirements_changed",  # type: ignore[arg-type]
            {},
            TaskBlockReason.GITHUB_SOURCE_CHANGED,
        )
