"""PromptTemplateOverride domain validation tests (issue #26).

Prove the override slot's own facts and the instruction-only boundary:
the record carries instruction text plus the built-in version it was
authored against — and its field set contains no protocol/authority/
session/workflow-semantics fields an override could redefine, no
built-in default content, and no rendered-template materialization.
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime
from typing import Any

import pytest

from openorc.domain.prompts import (
    PromptTemplateOverride,
    PromptTemplateOverrideDomainError,
    prompt_template_override_field_names,
)


def _override(**overrides: Any) -> PromptTemplateOverride:
    values: dict[str, Any] = {
        "id": uuid.uuid4(),
        "workspace_id": uuid.uuid4(),
        "template_key": "producer.plan_instructions",
        "base_template_version": "builtin-1.0.0",
        "instruction_text": "Plan the task step by step.",
        "created_at": datetime(2026, 9, 18, 12, 0, 0, tzinfo=UTC),
        "updated_at": datetime(2026, 9, 18, 12, 30, 0, tzinfo=UTC),
    }
    values.update(overrides)
    return PromptTemplateOverride(**values)


def test_field_set_is_exactly_the_override_slot() -> None:
    assert prompt_template_override_field_names() == frozenset(
        {
            "id",
            "workspace_id",
            "template_key",
            "base_template_version",
            "instruction_text",
            "created_at",
            "updated_at",
        }
    )


def test_the_record_is_not_a_complete_prompt_or_protocol_template() -> None:
    # The instruction-only boundary by construction: there is no field a
    # Workspace override could use to redefine protocol envelope, response
    # schema, review-subject identity, session semantics, authority rules,
    # communication topology, or workflow transitions.
    fields_ = prompt_template_override_field_names()
    assert "protocol" not in fields_
    assert "response_schema" not in fields_
    assert "authority" not in fields_
    assert "session" not in fields_
    assert "workflow" not in fields_
    assert "template" not in fields_  # instruction text, not a whole template


def test_a_valid_override_round_trips() -> None:
    override = _override()
    assert override.template_key == "producer.plan_instructions"
    assert override.base_template_version == "builtin-1.0.0"
    assert override.instruction_text == "Plan the task step by step."
    assert override.created_at.tzinfo is UTC


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("template_key", ""),
        ("template_key", "   "),
        ("base_template_version", ""),
        ("base_template_version", "  \t "),
        ("instruction_text", ""),
        ("instruction_text", "\n  "),
        ("template_key", 7),
        ("base_template_version", None),
        ("instruction_text", True),
    ],
)
def test_blank_or_invalid_text_is_rejected(field: str, value: object) -> None:
    with pytest.raises(PromptTemplateOverrideDomainError):
        _override(**{field: value})


@pytest.mark.parametrize("field", ["id", "workspace_id"])
def test_non_uuid_identity_is_rejected(field: str) -> None:
    with pytest.raises(PromptTemplateOverrideDomainError):
        _override(**{field: "not-a-uuid"})
