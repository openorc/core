"""Domain tests for TaskPullRequest (issue #25).

Ordinary deterministic tests: no database, no network. These prove the
durable TaskPullRequest invariants later persistence and workflow-service
behavior inherit: the observed open/closed lifecycle vocabulary, the
separation between stable GitHub PR identity (``github_pr_id``) and the
repository-local address (``github_pr_number``), the merged-implies-closed
coherence, the exact field set (mutable observed reconciliation state and
no review outcomes on the PR), and record immutability.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

import pytest

from openorc.domain.pull_requests import (
    TaskPullRequest,
    TaskPullRequestDomainError,
    TaskPullRequestState,
    task_pull_request_field_names,
)

_CREATED_AT = datetime(2026, 9, 18, 8, 0, 0, tzinfo=UTC)
_MERGED_AT = datetime(2026, 9, 18, 9, 0, 0, tzinfo=UTC)


def _pull_request(**overrides: Any) -> TaskPullRequest:
    """Build one valid open TaskPullRequest with optional field overrides."""
    values: dict[str, Any] = {
        "id": uuid4(),
        "workspace_id": uuid4(),
        "task_id": uuid4(),
        "repository_id": uuid4(),
        "github_pr_id": 900_719_925_474_099,
        "github_pr_number": 42,
        "head_ref": "openorc/task-42",
        "base_ref": "main",
        "head_sha": "0123456789abcdef0123456789abcdef01234567",
        "state": TaskPullRequestState.OPEN,
        "merged_at": None,
        "created_at": _CREATED_AT,
        "updated_at": _CREATED_AT,
    }
    values.update(overrides)
    return TaskPullRequest(**values)


def test_state_vocabulary_is_the_observed_github_lifecycle() -> None:
    # The observed GitHub PR lifecycle is exactly open/closed; no richer
    # workflow lifecycle is modeled on the PR record.
    assert TaskPullRequestState.OPEN == "open"
    assert TaskPullRequestState.CLOSED == "closed"
    assert [member.value for member in TaskPullRequestState] == ["open", "closed"]


def test_record_requires_uuid_identity_fields() -> None:
    for name in ("id", "workspace_id", "task_id", "repository_id"):
        with pytest.raises(TaskPullRequestDomainError):
            _pull_request(**{name: "not-a-uuid"})  # type: ignore[arg-type]


def test_github_identity_fields_are_positive_integers() -> None:
    # Both GitHub PR identity and the repository-local address are positive
    # integers; booleans are structurally excluded (they are ints).
    for name in ("github_pr_id", "github_pr_number"):
        for bad in (0, -1, 1.5, "42", True):
            with pytest.raises(TaskPullRequestDomainError):
                _pull_request(**{name: bad})


def test_ref_and_head_fields_must_be_nonblank() -> None:
    for name in ("head_ref", "base_ref", "head_sha"):
        for bad in ("", "   ", None, 42):
            with pytest.raises(TaskPullRequestDomainError):
                _pull_request(**{name: bad})  # type: ignore[arg-type]


def test_state_must_be_the_observed_vocabulary() -> None:
    # "merged" and "draft" are not a richer persisted lifecycle: GitHub
    # owns PR truth and the record stores only observed open/closed state.
    for bad in ("merged", "draft", "open", None, 42):
        with pytest.raises(TaskPullRequestDomainError):
            _pull_request(state=bad)


def test_a_merged_pull_request_is_closed() -> None:
    # merged_at is the observed merge timestamp and exists only on a
    # closed PR: an open observation never carries it.
    merged = _pull_request(state=TaskPullRequestState.CLOSED, merged_at=_MERGED_AT)
    assert merged.state is TaskPullRequestState.CLOSED
    assert merged.merged_at == _MERGED_AT
    with pytest.raises(TaskPullRequestDomainError):
        _pull_request(state=TaskPullRequestState.OPEN, merged_at=_MERGED_AT)


def test_a_closed_unmerged_pull_request_carries_no_merged_at() -> None:
    # A closed-unmerged PR is a normal closed observation: merged_at stays
    # None, and the record remains the Task's canonical PR (issue #25).
    closed = _pull_request(state=TaskPullRequestState.CLOSED, merged_at=None)
    assert closed.state is TaskPullRequestState.CLOSED
    assert closed.merged_at is None


def test_record_is_frozen() -> None:
    record = _pull_request()
    with pytest.raises(AttributeError):
        record.head_sha = "ffffffffffffffffffffffffffffffffffffffff"  # type: ignore[misc]


def test_field_set_carries_only_the_pr_record_facts() -> None:
    # The exact field set is asserted so future drift cannot appear
    # silently: stable GitHub identity plus repository-local address, the
    # mutable observed reconciliation state, and no review outcomes (exact
    # reviewed heads live as immutable history on the review records).
    assert task_pull_request_field_names() == frozenset(
        {
            "id",
            "workspace_id",
            "task_id",
            "repository_id",
            "github_pr_id",
            "github_pr_number",
            "head_ref",
            "base_ref",
            "head_sha",
            "state",
            "merged_at",
            "created_at",
            "updated_at",
        }
    )
