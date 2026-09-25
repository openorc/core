"""Deterministic tests for the GitHub issue projection domain (issue #59).

The requirements fingerprint is the one deterministic requirements-identity
value OpenOrc treats as engineering intent for v1: derived only from the
canonical title + body representation, stable across processes (hashlib,
never Python's process-randomized ``hash()``), insensitive to comments
(including OpenOrc's future published plan comment), labels, assignees,
reactions, and timeline events — which have no fingerprint input at all —
and deliberately treating GitHub's nullable body and an empty body as the
same requirements content.
"""

from __future__ import annotations

import hashlib
import re
import uuid
from datetime import UTC, datetime

import pytest

from openorc.domain.github_issues import (
    GitHubIssueDomainError,
    GitHubIssueIdentity,
    GitHubIssueProjection,
    GitHubIssueState,
    github_issue_field_names,
    github_issue_requirements_fingerprint,
)

_FINGERPRINT_PATTERN = re.compile(r"^[0-9a-f]{64}$")


def _expected(title: str, body: str | None) -> str:
    return hashlib.sha256(
        title.encode("utf-8") + b"\x00" + ("" if body is None else body).encode("utf-8")
    ).hexdigest()


def test_the_fingerprint_is_the_documented_sha256_canonicalization() -> None:
    assert github_issue_requirements_fingerprint("Found a bug", "Steps: 1") == _expected(
        "Found a bug", "Steps: 1"
    )


def test_a_null_body_and_an_empty_body_carry_the_same_requirements_content() -> None:
    assert github_issue_requirements_fingerprint("Title", None) == (
        github_issue_requirements_fingerprint("Title", "")
    )


def test_title_only_body_only_and_combined_changes_move_the_fingerprint() -> None:
    base = github_issue_requirements_fingerprint("Title", "Body")
    title_only = github_issue_requirements_fingerprint("Title v2", "Body")
    body_only = github_issue_requirements_fingerprint("Title", "Body v2")
    both = github_issue_requirements_fingerprint("Title v2", "Body v2")
    assert base != title_only
    assert base != body_only
    assert base != both
    assert len({base, title_only, body_only, both}) == 4


def test_the_nul_separator_makes_the_concatenation_unambiguous() -> None:
    # "ab" + "c" and "a" + "bc" must never collide across the two fields.
    assert github_issue_requirements_fingerprint("ab", "c") != (
        github_issue_requirements_fingerprint("a", "bc")
    )


def test_comments_labels_and_assignees_have_no_fingerprint_input() -> None:
    # Only title and body exist as inputs; there is no API surface for any
    # other field to reach the digest.
    with pytest.raises(GitHubIssueDomainError):
        github_issue_requirements_fingerprint("Title", object())  # type: ignore[arg-type]
    with pytest.raises(GitHubIssueDomainError):
        github_issue_requirements_fingerprint("", None)
    with pytest.raises(GitHubIssueDomainError):
        github_issue_requirements_fingerprint("   ", "body")


def test_the_fingerprint_is_a_lowercase_hex_sha256_digest() -> None:
    digest = github_issue_requirements_fingerprint("Title", "Body")
    assert _FINGERPRINT_PATTERN.match(digest)
    assert hashlib.sha256(b"Title\x00Body").hexdigest() == digest


def test_issue_state_maps_the_documented_vocabulary_exactly() -> None:
    assert GitHubIssueState("open") is GitHubIssueState.OPEN
    assert GitHubIssueState("closed") is GitHubIssueState.CLOSED
    with pytest.raises(ValueError):
        GitHubIssueState("all")


def test_issue_identity_rejects_non_positive_or_non_integer_ids() -> None:
    assert GitHubIssueIdentity(1).github_issue_id == 1
    with pytest.raises(GitHubIssueDomainError):
        GitHubIssueIdentity(0)
    with pytest.raises(GitHubIssueDomainError):
        GitHubIssueIdentity(-1)
    with pytest.raises(GitHubIssueDomainError):
        GitHubIssueIdentity(True)  # type: ignore[arg-type]
    with pytest.raises(GitHubIssueDomainError):
        GitHubIssueIdentity("1")  # type: ignore[arg-type]


def _projection(**overrides: object) -> GitHubIssueProjection:
    values: dict[str, object] = {
        "id": uuid.uuid4(),
        "workspace_id": uuid.uuid4(),
        "repository_id": uuid.uuid4(),
        "identity": GitHubIssueIdentity(503),
        "issue_number": 42,
        "title": "Found a bug",
        "body": "Steps to reproduce",
        "state": GitHubIssueState.OPEN,
        "requirements_fingerprint": github_issue_requirements_fingerprint(
            "Found a bug", "Steps to reproduce"
        ),
        "provider_updated_at": datetime(2026, 9, 25, 12, 0, 0, tzinfo=UTC),
        "created_at": datetime(2026, 9, 25, 12, 0, 0, tzinfo=UTC),
        "updated_at": datetime(2026, 9, 25, 12, 0, 0, tzinfo=UTC),
    }
    values.update(overrides)
    return GitHubIssueProjection(**values)  # type: ignore[arg-type]


def test_the_projection_record_validates_its_durable_invariants() -> None:
    projection = _projection()
    assert projection.identity.github_issue_id == 503
    assert projection.state is GitHubIssueState.OPEN
    with pytest.raises(GitHubIssueDomainError):
        _projection(requirements_fingerprint="not-a-digest")
    with pytest.raises(GitHubIssueDomainError):
        _projection(issue_number=0)
    with pytest.raises(GitHubIssueDomainError):
        _projection(title="")
    with pytest.raises(GitHubIssueDomainError):
        _projection(state="open")  # type: ignore[arg-type]
    with pytest.raises(GitHubIssueDomainError):
        _projection(id="not-a-uuid")  # type: ignore[arg-type]


def test_the_projection_exposes_exactly_the_phase_2_issue_facts() -> None:
    # No credential-bearing field and no presentation surplus: comments,
    # labels, assignees, reactions, timeline events, and raw provider
    # payloads have no supported column on the projection.
    assert github_issue_field_names() == frozenset(
        {
            "id",
            "workspace_id",
            "repository_id",
            "identity",
            "issue_number",
            "title",
            "body",
            "state",
            "requirements_fingerprint",
            "provider_updated_at",
            "created_at",
            "updated_at",
        }
    )
