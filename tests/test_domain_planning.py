"""Domain tests for PlanRevision (issue #23).

Ordinary deterministic tests: no database, no network. These prove the
planning-artifact invariants later persistence and workflow-service behavior
inherit: per-Task versioning, exact-content and repository-base-context
validation, and the exact field set (no review outcomes, no Task status, no
optimistic-concurrency token, and no mutable last-change timestamp — an
immutable artifact has no update path).
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

import pytest

from openorc.domain.planning import (
    PlanRevision,
    PlanRevisionDomainError,
    plan_revision_field_names,
)

_CREATED_AT = datetime(2026, 9, 17, 10, 0, 0, tzinfo=UTC)


def _revision(**overrides: Any) -> PlanRevision:
    """Build one valid PlanRevision with optional field overrides."""
    values: dict[str, Any] = {
        "id": uuid4(),
        "workspace_id": uuid4(),
        "task_id": uuid4(),
        "revision_number": 1,
        "content": "# Plan\n\n1. Implement the thing.\n",
        "repository_base_sha": "0123456789abcdef0123456789abcdef01234567",
        "created_at": _CREATED_AT,
    }
    values.update(overrides)
    return PlanRevision(**values)


def test_revision_requires_positive_integer_versioning() -> None:
    for bad in (0, -1, 1.5, "1", True):
        with pytest.raises(PlanRevisionDomainError):
            _revision(revision_number=bad)


def test_revision_requires_nonempty_content() -> None:
    for bad in ("", "   \n\t  ", None, 42):
        with pytest.raises(PlanRevisionDomainError):
            _revision(content=bad)


def test_revision_requires_nonempty_repository_base_sha() -> None:
    for bad in ("", "   ", None, 42):
        with pytest.raises(PlanRevisionDomainError):
            _revision(repository_base_sha=bad)


def test_revision_requires_uuid_identity_fields() -> None:
    for name in ("id", "workspace_id", "task_id"):
        with pytest.raises(PlanRevisionDomainError):
            _revision(**{name: "not-a-uuid"})


def test_revisions_of_one_task_are_independent_immutable_artifacts() -> None:
    # Versioning is a per-Task sequence: a changed plan is a fresh revision.
    # Two revisions of one Task coexist, each carrying its own exact content
    # and base context; neither is a mutation of the other, and the planning
    # history stays complete.
    first = _revision(revision_number=1, content="v1 plan", repository_base_sha="base-a")
    second = _revision(
        task_id=first.task_id,
        workspace_id=first.workspace_id,
        revision_number=2,
        content="v2 plan",
        repository_base_sha="base-b",
    )
    assert first.task_id == second.task_id
    assert first.revision_number == 1 and second.revision_number == 2
    assert first.content == "v1 plan" and second.content == "v2 plan"
    assert first.repository_base_sha == "base-a" and second.repository_base_sha == "base-b"


def test_revision_is_frozen() -> None:
    revision = _revision()
    with pytest.raises(AttributeError):
        revision.content = "rewritten"  # type: ignore[misc]


def test_field_set_carries_only_planning_artifact_facts() -> None:
    # The exact field set is asserted so future drift cannot appear silently:
    # no review outcomes (they live on ReviewIteration), no Task status, no
    # optimistic-concurrency token, and no updated_at (an immutable artifact
    # has no update path).
    assert plan_revision_field_names() == frozenset(
        {
            "id",
            "workspace_id",
            "task_id",
            "revision_number",
            "content",
            "repository_base_sha",
            "created_at",
        }
    )
