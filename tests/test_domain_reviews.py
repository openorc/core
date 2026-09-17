"""Domain tests for ReviewLoop and ReviewIteration (issue #23).

Ordinary deterministic tests: no database, no network. These prove the
review-history invariants later persistence and workflow-service behavior
inherit: the settled purpose/outcome/lifecycle vocabularies, the configured
default iteration limit, the four-fact atomic finalize coherence, the
settled outcome/findings coherence, findings canonicalization (plain-list
canonical JSON form, tuples never survive), and the exact field sets (no
``review_result`` wire-envelope fields, no outcomes on the loop, no
orchestration policy).
"""

from __future__ import annotations

from datetime import UTC, datetime
from math import inf, nan
from typing import Any
from uuid import uuid4

import pytest

from openorc.domain.reviews import (
    DEFAULT_REVIEW_LOOP_ITERATION_LIMIT,
    ReviewIteration,
    ReviewLoop,
    ReviewLoopDomainError,
    ReviewLoopPurpose,
    ReviewLoopStatus,
    ReviewOutcome,
    canonical_review_findings,
    review_iteration_field_names,
    review_loop_field_names,
)

_CREATED_AT = datetime(2026, 9, 17, 10, 0, 0, tzinfo=UTC)
_DECIDED_AT = datetime(2026, 9, 17, 11, 0, 0, tzinfo=UTC)


def _loop(**overrides: Any) -> ReviewLoop:
    """Build one valid OPEN ReviewLoop with optional field overrides."""
    values: dict[str, Any] = {
        "id": uuid4(),
        "workspace_id": uuid4(),
        "task_id": uuid4(),
        "purpose": ReviewLoopPurpose.PLANNING,
        "iteration_limit": DEFAULT_REVIEW_LOOP_ITERATION_LIMIT,
        "status": ReviewLoopStatus.OPEN,
        "closed_at": None,
        "created_at": _CREATED_AT,
    }
    values.update(overrides)
    return ReviewLoop(**values)


def _iteration(**overrides: Any) -> ReviewIteration:
    """Build one valid unfinalized ReviewIteration with optional overrides."""
    values: dict[str, Any] = {
        "id": uuid4(),
        "workspace_id": uuid4(),
        "task_id": uuid4(),
        "review_loop_id": uuid4(),
        "iteration_number": 1,
        "plan_revision_id": uuid4(),
        "outcome": None,
        "summary": None,
        "findings": None,
        "decided_at": None,
        "created_at": _CREATED_AT,
    }
    values.update(overrides)
    return ReviewIteration(**values)


def test_purpose_vocabulary_values_are_the_persisted_text_values() -> None:
    assert ReviewLoopPurpose.PLANNING == "planning"
    assert ReviewLoopPurpose.PR_REVIEW == "pr_review"
    assert [member.value for member in ReviewLoopPurpose] == ["planning", "pr_review"]


def test_outcome_vocabulary_values_are_the_persisted_text_values() -> None:
    assert ReviewOutcome.ACCEPTED == "accepted"
    assert ReviewOutcome.CHANGES_REQUESTED == "changes_requested"
    assert [member.value for member in ReviewOutcome] == ["accepted", "changes_requested"]


def test_status_vocabulary_values_are_the_persisted_text_values() -> None:
    assert ReviewLoopStatus.OPEN == "open"
    assert ReviewLoopStatus.CLOSED == "closed"
    assert [member.value for member in ReviewLoopStatus] == ["open", "closed"]


def test_default_iteration_limit_is_the_v1_configured_boundary_value() -> None:
    assert DEFAULT_REVIEW_LOOP_ITERATION_LIMIT == 5


def test_loop_requires_a_v1_purpose() -> None:
    # Exactly PLANNING and PR_REVIEW exist; any other value (including a
    # speculative implementation-review category) is rejected.
    for bad in ("implementation_review", "planning", "pr_review", None, 42):
        with pytest.raises(ReviewLoopDomainError):
            _loop(purpose=bad)


def test_loop_requires_a_positive_iteration_limit() -> None:
    for bad in (0, -1, 1.5, "5", True):
        with pytest.raises(ReviewLoopDomainError):
            _loop(iteration_limit=bad)


def test_loop_requires_a_v1_status() -> None:
    for bad in ("running", "exhausted", None, 42):
        with pytest.raises(ReviewLoopDomainError):
            _loop(status=bad)


def test_loop_closed_at_is_set_exactly_when_status_is_closed() -> None:
    closed = _loop(status=ReviewLoopStatus.CLOSED, closed_at=_DECIDED_AT)
    assert closed.closed_at == _DECIDED_AT
    # A CLOSED loop without the semantic timestamp is incoherent.
    with pytest.raises(ReviewLoopDomainError):
        _loop(status=ReviewLoopStatus.CLOSED, closed_at=None)
    # Non-CLOSED statuses must not carry it.
    with pytest.raises(ReviewLoopDomainError):
        _loop(closed_at=_DECIDED_AT)


def test_loop_is_frozen() -> None:
    loop = _loop()
    with pytest.raises(AttributeError):
        loop.iteration_limit = 99  # type: ignore[misc]


def test_iteration_requires_a_positive_iteration_number() -> None:
    for bad in (0, -1, 1.5, "1", True):
        with pytest.raises(ReviewLoopDomainError):
            _iteration(iteration_number=bad)


def test_iteration_requires_the_exact_subject() -> None:
    # v1 subjects are planning revisions: the exact PlanRevision under
    # review is required, never None or a loose value.
    for bad in (None, "not-a-uuid", 42):
        with pytest.raises(ReviewLoopDomainError):
            _iteration(plan_revision_id=bad)


def test_iteration_outcome_must_be_a_reviewer_judgment_or_none() -> None:
    # Provider/runtime/protocol failures are not reviewer judgments; the
    # vocabulary structurally excludes them.
    for bad in ("approved", "failed", "error", 42):
        with pytest.raises(ReviewLoopDomainError):
            _iteration(outcome=bad)


def test_iteration_summary_must_be_nonempty_when_set() -> None:
    for bad in ("", "   ", 42):
        with pytest.raises(ReviewLoopDomainError):
            _iteration(
                outcome=ReviewOutcome.ACCEPTED,
                summary=bad,
                findings=[],
                decided_at=_DECIDED_AT,
            )


def test_partially_set_result_facts_are_rejected() -> None:
    # From the all-None coherent form, setting any one result fact alone is
    # incoherent: the complete result finalizes atomically.
    partial_sets: list[dict[str, Any]] = [
        {"outcome": ReviewOutcome.ACCEPTED},
        {"summary": "clear"},
        {"findings": []},
        {"decided_at": _DECIDED_AT},
    ]
    for overrides in partial_sets:
        with pytest.raises(ReviewLoopDomainError):
            _iteration(**overrides)


def test_partially_null_result_facts_are_rejected() -> None:
    # From the all-set coherent form, nulling any one result fact alone is
    # incoherent: a finalized iteration carries its complete result.
    finalized: dict[str, Any] = {
        "outcome": ReviewOutcome.CHANGES_REQUESTED,
        "summary": "fix the loop",
        "findings": [{"summary": "s", "details": "d"}],
        "decided_at": _DECIDED_AT,
    }
    for name in ("outcome", "summary", "findings", "decided_at"):
        with pytest.raises(ReviewLoopDomainError):
            _iteration(**{**finalized, name: None})


def test_accepted_clears_with_zero_findings_and_changes_requested_retains_one() -> None:
    accepted = _iteration(
        outcome=ReviewOutcome.ACCEPTED,
        summary="clear",
        findings=[],
        decided_at=_DECIDED_AT,
    )
    assert accepted.findings == []
    changes = _iteration(
        outcome=ReviewOutcome.CHANGES_REQUESTED,
        summary="fix the loop",
        findings=[
            {"summary": "missing gate", "details": "no OwnerGate"},
            {"summary": "stale base", "details": "rebase first"},
        ],
        decided_at=_DECIDED_AT,
    )
    assert changes.findings is not None
    assert len(changes.findings) == 2
    # The settled coherence holds in both directions.
    with pytest.raises(ReviewLoopDomainError):
        _iteration(
            outcome=ReviewOutcome.ACCEPTED,
            summary="premature",
            findings=[{"summary": "s", "details": "d"}],
            decided_at=_DECIDED_AT,
        )
    with pytest.raises(ReviewLoopDomainError):
        _iteration(
            outcome=ReviewOutcome.CHANGES_REQUESTED,
            summary="empty",
            findings=[],
            decided_at=_DECIDED_AT,
        )


def test_findings_canonicalize_tuples_to_plain_lists_at_every_level() -> None:
    canonical = canonical_review_findings(
        ({"summary": "s", "details": "d", "tags": ("a", ("b",))}, (1, 2))
    )
    assert canonical == [{"summary": "s", "details": "d", "tags": ["a", ["b"]]}, [1, 2]]
    assert type(canonical) is list
    assert type(canonical[0]["tags"]) is list  # type: ignore[index]


def test_findings_canonicalization_matches_the_psycopg_read_back_shape() -> None:
    # The canonical form is exactly the shape jsonb stores and psycopg reads
    # back: a plain list. A reload re-canonicalizes to an identical form.
    original = [{"summary": "s", "details": "d", "nested": {"x": (1,)}}]
    canonical = canonical_review_findings(original)
    assert canonical_review_findings(canonical) == canonical


def test_findings_must_be_a_json_array() -> None:
    for bad in ({"summary": "s"}, "not-an-array", 42, None):
        with pytest.raises(ReviewLoopDomainError):
            canonical_review_findings(bad)


def test_findings_require_string_keys_at_every_level() -> None:
    with pytest.raises(ReviewLoopDomainError):
        canonical_review_findings([{1: "a"}])
    with pytest.raises(ReviewLoopDomainError):
        canonical_review_findings([{"nested": {2: "b"}}])


def test_findings_reject_non_json_value_types_and_nan() -> None:
    with pytest.raises(ReviewLoopDomainError):
        canonical_review_findings([b"bytes"])
    with pytest.raises(ReviewLoopDomainError):
        canonical_review_findings([nan])
    with pytest.raises(ReviewLoopDomainError):
        canonical_review_findings([inf])


def test_findings_are_canonicalized_in_place_on_the_iteration() -> None:
    iteration = _iteration(
        outcome=ReviewOutcome.CHANGES_REQUESTED,
        summary="fix",
        findings=({"summary": "s", "details": "d", "lines": (1, 2)},),
        decided_at=_DECIDED_AT,
    )
    assert iteration.findings == [{"summary": "s", "details": "d", "lines": [1, 2]}]


def test_per_item_protocol_schema_is_not_validated_in_phase_1() -> None:
    # Per-item review_result protocol validation (finding objects with
    # non-empty summary and details) belongs to Phase 2 protocol validation
    # in openorc.protocol. Phase 1 persists the caller-validated array
    # verbatim and validates canonical JSON form only.
    loose = _iteration(
        outcome=ReviewOutcome.CHANGES_REQUESTED,
        summary="fix",
        findings=["a loose string item", {"unknown_shape": True}],
        decided_at=_DECIDED_AT,
    )
    assert loose.findings == ["a loose string item", {"unknown_shape": True}]


def test_iteration_is_frozen() -> None:
    iteration = _iteration()
    with pytest.raises(AttributeError):
        iteration.outcome = ReviewOutcome.ACCEPTED  # type: ignore[misc]


def test_loop_field_set_carries_only_loop_facts() -> None:
    assert review_loop_field_names() == frozenset(
        {
            "id",
            "workspace_id",
            "task_id",
            "purpose",
            "iteration_limit",
            "status",
            "closed_at",
            "created_at",
        }
    )


def test_iteration_field_set_carries_the_complete_result_without_the_wire_envelope() -> None:
    # No ``type``/``schema_version`` wire-envelope fields (protocol concerns
    # for Phase 2), no loop status duplication, and no ``updated_at`` (the
    # finalize is stamped by the semantic ``decided_at``).
    assert review_iteration_field_names() == frozenset(
        {
            "id",
            "workspace_id",
            "task_id",
            "review_loop_id",
            "iteration_number",
            "plan_revision_id",
            "outcome",
            "summary",
            "findings",
            "decided_at",
            "created_at",
        }
    )
