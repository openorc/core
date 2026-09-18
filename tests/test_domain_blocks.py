"""Domain tests for TaskBlock (issue #24).

Ordinary deterministic tests: no database, no network. These prove the
blocking-record invariants later persistence and workflow-service behavior
inherit: the settled twelve-value reason vocabulary (with ReviewLoop
exhaustion structurally excluded), canonical-JSON context semantics, and
the exact field set (no universal blocked-to-next-state transition field).
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

import pytest

from openorc.domain.blocks import (
    TaskBlock,
    TaskBlockDomainError,
    TaskBlockReason,
    task_block_field_names,
)

_NOW = datetime(2026, 9, 17, 10, 0, 0, tzinfo=UTC)


def _block(**overrides: Any) -> TaskBlock:
    values: dict[str, Any] = {
        "id": uuid4(),
        "workspace_id": uuid4(),
        "task_id": uuid4(),
        "reason": TaskBlockReason.STALE_OPERATION,
        "context": {"expected_subject": "abc", "observed_subject": "def"},
        "resolved_at": None,
        "created_at": _NOW,
    }
    values.update(overrides)
    return TaskBlock(**values)


def test_reason_vocabulary_is_exactly_the_twelve_settled_values() -> None:
    assert {member.value for member in TaskBlockReason} == {
        "review_failure",
        "runtime_failure",
        "agent_session_lost",
        "connection_unavailable",
        "invalid_credentials",
        "external_operation_uncertain",
        "stale_operation",
        "github_source_changed",
        "runtime_request_rejected",
        "pr_closed_unmerged",
        "owner_action_required",
        "unknown",
    }


def test_review_loop_exhaustion_is_not_a_block_reason() -> None:
    # ReviewLoop iteration-limit exhaustion is represented through the
    # Task's waiting_for_owner status plus a REVIEW_RESOLUTION OwnerGate —
    # never as a TaskBlock reason.
    assert not any(
        "exhaust" in member.value or "iteration" in member.value or "limit" in member.value
        for member in TaskBlockReason
    )
    with pytest.raises(TaskBlockDomainError):
        _block(reason="review_loop_exhausted")  # type: ignore[arg-type]


def test_block_rejects_non_vocabulary_reason() -> None:
    with pytest.raises(TaskBlockDomainError):
        _block(reason="owner_chat")  # type: ignore[arg-type]


def test_block_requires_uuid_identity_fields() -> None:
    for name in ("id", "workspace_id", "task_id"):
        with pytest.raises(TaskBlockDomainError):
            _block(**{name: "not-a-uuid"})


def test_context_requires_a_mapping() -> None:
    for bad in ("not-a-mapping", 42, None, ["a"]):
        with pytest.raises(TaskBlockDomainError):
            _block(context=bad)  # type: ignore[arg-type]


def test_context_normalizes_sequences_and_requires_string_keys() -> None:
    block = _block(context={"steps": ("a", "b"), "nested": {"deep": (1, 2)}})
    assert block.context["steps"] == ["a", "b"]
    assert block.context["nested"] == {"deep": [1, 2]}
    for bad in ({1: "x"}, {"outer": {2: "x"}}):
        with pytest.raises(TaskBlockDomainError):
            _block(context=bad)  # type: ignore[misc]


def test_context_rejects_non_json_values_and_non_finite_numbers() -> None:
    with pytest.raises(TaskBlockDomainError):
        _block(context={"bad": object()})
    with pytest.raises(TaskBlockDomainError):
        _block(context={"bad": float("nan")})


def test_resolved_block_is_historical_and_retains_reason_and_context() -> None:
    resolved_at = datetime(2026, 9, 17, 11, 0, 0, tzinfo=UTC)
    block = _block(resolved_at=resolved_at)
    assert block.resolved_at == resolved_at
    assert block.reason is TaskBlockReason.STALE_OPERATION
    assert block.context == {"expected_subject": "abc", "observed_subject": "def"}


def test_block_is_frozen() -> None:
    block = _block()
    with pytest.raises(AttributeError):
        block.reason = TaskBlockReason.UNKNOWN  # type: ignore[misc]


def test_field_set_carries_no_universal_transition_field() -> None:
    # The exact field set is asserted so future drift cannot appear silently:
    # recovery is reason/context-specific and no universal
    # blocked-to-next-state transition is encoded in the record.
    assert task_block_field_names() == frozenset(
        {
            "id",
            "workspace_id",
            "task_id",
            "reason",
            "context",
            "resolved_at",
            "created_at",
        }
    )
