"""Domain tests for the Level 3 blocking-case vocabulary (issue #286).

Ordinary deterministic tests: no database, no network. These prove the
typed condition vocabulary and the minimum-evidence coherence the
classification policy consumes: the exact five-condition vocabulary, each
condition's positively-evidenced minimum facts, the publication-mismatch
variant acceptance (preflight anchor-only form; canonical PR identity with
that outcome's observed facts; the E8 head-ref, base-retarget, and
head-SHA drift variants of issue #280), the distinct E10a merge-head
contract, and closed evidence keys — no manufactured identities, no
arbitrary diagnostic bags.
"""

from __future__ import annotations

from typing import Any

import pytest

from openorc.domain.blocking_cases import (
    BlockingCaseDomainError,
    Level3BlockingCondition,
    validate_blocking_case_evidence,
)

_SHA = "0123456789abcdef0123456789abcdef01234567"
_OTHER_SHA = "fedcba9876543210fedcba9876543210fedcba98"
_GATE_ID = "1b234c67-8d90-4e1f-a234-56789abcdef0"
_PR_ID = "2c345d78-9e01-4f2f-b345-67890abcdef0"
_GITHUB_PR_ID = "3d456e89-af12-4a3f-c456-7890abcdef12"
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


def _preflight_publication(**overrides: Any) -> dict[str, Any]:
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


def test_the_condition_vocabulary_is_exactly_the_five_supported_cases() -> None:
    assert {member.value for member in Level3BlockingCondition} == {
        "github_issue_requirements_change",
        "pr_publication_authority_mismatch",
        "merge_head_authority_mismatch",
        "task_cost_limit_reached",
        "owner_abort_issued",
    }


def test_requirements_change_evidence_proves_the_fingerprint_delta() -> None:
    validate_blocking_case_evidence(
        Level3BlockingCondition.GITHUB_ISSUE_REQUIREMENTS_CHANGE,
        _requirements_change(),
    )
    # The string identity form is the equivalent canonical representation.
    validate_blocking_case_evidence(
        Level3BlockingCondition.GITHUB_ISSUE_REQUIREMENTS_CHANGE,
        _requirements_change(github_issue_id="7703"),
    )
    # An equal fingerprint pair proves no requirements change.
    with pytest.raises(BlockingCaseDomainError):
        validate_blocking_case_evidence(
            Level3BlockingCondition.GITHUB_ISSUE_REQUIREMENTS_CHANGE,
            _requirements_change(observed_fingerprint=_FINGERPRINT),
        )
    with pytest.raises(BlockingCaseDomainError):
        validate_blocking_case_evidence(
            Level3BlockingCondition.GITHUB_ISSUE_REQUIREMENTS_CHANGE,
            _requirements_change(github_issue_id=0),
        )
    with pytest.raises(BlockingCaseDomainError):
        validate_blocking_case_evidence(
            Level3BlockingCondition.GITHUB_ISSUE_REQUIREMENTS_CHANGE,
            _requirements_change(expected_fingerprint=None),
        )


def test_publication_mismatch_accepts_the_preflight_anchor_only_form() -> None:
    # The preflight stale form carries exactly the authorization anchor: no
    # canonical PR exists to cite and no SHA was observed, so neither is
    # required or manufactured.
    validate_blocking_case_evidence(
        Level3BlockingCondition.PR_PUBLICATION_AUTHORITY_MISMATCH,
        _preflight_publication(),
    )


def test_publication_mismatch_accepts_the_positively_evidenced_variants() -> None:
    # Finalize head-SHA drift (issue #280): canonical PR identity plus the
    # observed head.
    validate_blocking_case_evidence(
        Level3BlockingCondition.PR_PUBLICATION_AUTHORITY_MISMATCH,
        _preflight_publication(
            pull_request_id=_PR_ID,
            github_pr_number="43",
            observed_head_sha=_OTHER_SHA,
        ),
    )
    # Head-ref drift: the observed head ref and head.
    validate_blocking_case_evidence(
        Level3BlockingCondition.PR_PUBLICATION_AUTHORITY_MISMATCH,
        _preflight_publication(
            pull_request_id=_PR_ID,
            github_pr_number="43",
            observed_head_ref="other-branch",
            observed_head_sha=_OTHER_SHA,
        ),
    )
    # Base-retarget: the base moved while the head may be unchanged.
    validate_blocking_case_evidence(
        Level3BlockingCondition.PR_PUBLICATION_AUTHORITY_MISMATCH,
        _preflight_publication(
            pull_request_id=_PR_ID,
            github_pr_number="43",
            authorized_base_ref="main",
            observed_head_sha=_SHA,
            observed_base_ref="renamed-base",
        ),
    )
    # Post-create outcomes: the GitHub PR identity in place of an observed
    # head, per the outcome-specific evidence.
    validate_blocking_case_evidence(
        Level3BlockingCondition.PR_PUBLICATION_AUTHORITY_MISMATCH,
        _preflight_publication(
            pull_request_id=_PR_ID,
            github_pr_number="43",
            github_pull_request_id=_GITHUB_PR_ID,
        ),
    )


def test_publication_mismatch_rejects_unknown_keys_and_malformed_values() -> None:
    with pytest.raises(BlockingCaseDomainError):
        validate_blocking_case_evidence(
            Level3BlockingCondition.PR_PUBLICATION_AUTHORITY_MISMATCH,
            _preflight_publication(unrecognized_fact="nope"),
        )
    with pytest.raises(BlockingCaseDomainError):
        validate_blocking_case_evidence(
            Level3BlockingCondition.PR_PUBLICATION_AUTHORITY_MISMATCH,
            _preflight_publication(owner_gate_id="not-a-uuid"),
        )
    with pytest.raises(BlockingCaseDomainError):
        validate_blocking_case_evidence(
            Level3BlockingCondition.PR_PUBLICATION_AUTHORITY_MISMATCH,
            _preflight_publication(pull_request_id="not-a-uuid"),
        )
    with pytest.raises(BlockingCaseDomainError):
        validate_blocking_case_evidence(
            Level3BlockingCondition.PR_PUBLICATION_AUTHORITY_MISMATCH,
            _preflight_publication(authorized_head_sha=""),
        )


def test_merge_head_mismatch_keeps_its_distinct_e10a_contract() -> None:
    validate_blocking_case_evidence(
        Level3BlockingCondition.MERGE_HEAD_AUTHORITY_MISMATCH,
        _merge_mismatch(),
    )
    # The exact E10a key set: no publication-form keys, no extras.
    for extra in (
        {"authorized_head_sha": _SHA},
        {"merge_commit_sha": _OTHER_SHA},
    ):
        with pytest.raises(BlockingCaseDomainError):
            validate_blocking_case_evidence(
                Level3BlockingCondition.MERGE_HEAD_AUTHORITY_MISMATCH,
                _merge_mismatch(**extra),
            )
    with pytest.raises(BlockingCaseDomainError):
        validate_blocking_case_evidence(
            Level3BlockingCondition.MERGE_HEAD_AUTHORITY_MISMATCH,
            _merge_mismatch(observed_head_sha=None),
        )


def test_cost_limit_evidence_carries_the_standing_budget_facts() -> None:
    validate_blocking_case_evidence(
        Level3BlockingCondition.TASK_COST_LIMIT_REACHED,
        _cost_limit(),
    )
    with pytest.raises(BlockingCaseDomainError):
        validate_blocking_case_evidence(
            Level3BlockingCondition.TASK_COST_LIMIT_REACHED,
            _cost_limit(control="interactive_command"),
        )
    with pytest.raises(BlockingCaseDomainError):
        validate_blocking_case_evidence(
            Level3BlockingCondition.TASK_COST_LIMIT_REACHED,
            _cost_limit(reason_provenance="live_owner_command"),
        )
    with pytest.raises(BlockingCaseDomainError):
        validate_blocking_case_evidence(
            Level3BlockingCondition.TASK_COST_LIMIT_REACHED,
            _cost_limit(observed_cost_usd=""),
        )
    with pytest.raises(BlockingCaseDomainError):
        validate_blocking_case_evidence(
            Level3BlockingCondition.TASK_COST_LIMIT_REACHED,
            _cost_limit(aggregate_includes_unobserved_sessions="no"),
        )
    with pytest.raises(BlockingCaseDomainError):
        validate_blocking_case_evidence(
            Level3BlockingCondition.TASK_COST_LIMIT_REACHED,
            _cost_limit(target_roles=[]),
        )
    with pytest.raises(BlockingCaseDomainError):
        validate_blocking_case_evidence(
            Level3BlockingCondition.TASK_COST_LIMIT_REACHED,
            _cost_limit(extra_key="nope"),
        )


def test_owner_abort_evidence_carries_the_exact_session_context() -> None:
    validate_blocking_case_evidence(
        Level3BlockingCondition.OWNER_ABORT_ISSUED,
        _owner_abort(),
    )
    # The exact owner-abort control context: no cost-threshold keys, no
    # extras, non-empty session/role facts.
    with pytest.raises(BlockingCaseDomainError):
        validate_blocking_case_evidence(
            Level3BlockingCondition.OWNER_ABORT_ISSUED,
            _owner_abort(reason_provenance="owner_authorized_task_budget"),
        )
    with pytest.raises(BlockingCaseDomainError):
        validate_blocking_case_evidence(
            Level3BlockingCondition.OWNER_ABORT_ISSUED,
            _owner_abort(task_agent_session_ids=[]),
        )
    with pytest.raises(BlockingCaseDomainError):
        validate_blocking_case_evidence(
            Level3BlockingCondition.OWNER_ABORT_ISSUED,
            _owner_abort(target_roles=["producer", ""]),
        )


def test_the_validator_rejects_non_vocabulary_conditions() -> None:
    with pytest.raises(BlockingCaseDomainError):
        validate_blocking_case_evidence("requirements_changed", {})  # type: ignore[arg-type]
    with pytest.raises(BlockingCaseDomainError):
        validate_blocking_case_evidence(
            Level3BlockingCondition.OWNER_ABORT_ISSUED,
            ["not", "a", "mapping"],  # type: ignore[arg-type]
        )
