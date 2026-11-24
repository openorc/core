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
    GitHubIssueLabel,
    GitHubIssueMilestone,
    GitHubIssueProjection,
    GitHubIssueState,
    GitHubIssueUserRef,
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


def test_the_projection_exposes_exactly_the_documented_issue_facts() -> None:
    # No credential-bearing field and no raw-payload surplus: comments,
    # reactions, timeline events, and arbitrary provider navigation data
    # have no supported column on the projection. Issue #375 adds the agreed
    # bounded presentation facts: state_reason, the provider-supplied
    # html_url, provider created/closed instants, the nested user/labels/
    # assignees/milestone subsets, and the two relations observation-evidence
    # instants.
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
            "state_reason",
            "html_url",
            "provider_created_at",
            "provider_closed_at",
            "user",
            "labels",
            "assignees",
            "milestone",
            "hierarchy_observed_at",
            "dependencies_observed_at",
        }
    )


def test_presentation_facts_default_to_unobserved_none() -> None:
    # An unobserved presentation fact stays honest: nothing is fabricated
    # from defaults, and collections distinguish unobserved (None) from
    # observed empty (an empty tuple).
    projection = _projection()
    assert projection.state_reason is None
    assert projection.html_url is None
    assert projection.provider_created_at is None
    assert projection.provider_closed_at is None
    assert projection.user is None
    assert projection.labels is None
    assert projection.assignees is None
    assert projection.milestone is None
    assert projection.hierarchy_observed_at is None
    assert projection.dependencies_observed_at is None

    observed_empty = _projection(labels=(), assignees=())
    assert observed_empty.labels == ()
    assert observed_empty.assignees == ()


def test_the_projection_validates_the_presentation_fact_shapes() -> None:
    user = GitHubIssueUserRef(id=1, login="octocat")
    label = GitHubIssueLabel(id=2, name="bug", color="d73a4a", description=None)
    milestone = GitHubIssueMilestone(
        id=3,
        number=7,
        title="v1",
        state="open",
        html_url="https://github.com/example/repo/milestone/7",
        description=None,
        due_on=None,
    )
    projection = _projection(
        state_reason="reopened",
        html_url="https://github.com/example/repo/issues/42",
        user=user,
        labels=(label,),
        assignees=(user,),
        milestone=milestone,
    )
    assert projection.user == user
    assert projection.labels == (label,)
    assert projection.milestone == milestone

    with pytest.raises(GitHubIssueDomainError):
        _projection(user=("octocat",))  # type: ignore[arg-type]
    with pytest.raises(GitHubIssueDomainError):
        _projection(labels=(("bug",),))  # type: ignore[arg-type]
    with pytest.raises(GitHubIssueDomainError):
        _projection(milestone=("v1",))  # type: ignore[arg-type]
    with pytest.raises(GitHubIssueDomainError):
        _projection(html_url="")
    with pytest.raises(GitHubIssueDomainError):
        _projection(state_reason=42)  # type: ignore[arg-type]
    with pytest.raises(GitHubIssueDomainError):
        _projection(provider_created_at="2026-09-25")  # type: ignore[arg-type]
    with pytest.raises(GitHubIssueDomainError):
        _projection(dependencies_observed_at="2026-09-25")  # type: ignore[arg-type]


def test_the_presentation_value_objects_validate_their_shapes() -> None:
    with pytest.raises(GitHubIssueDomainError):
        GitHubIssueUserRef(id=0, login="octocat")
    with pytest.raises(GitHubIssueDomainError):
        GitHubIssueUserRef(id=1, login="")
    with pytest.raises(GitHubIssueDomainError):
        GitHubIssueLabel(id=0, name="bug")
    with pytest.raises(GitHubIssueDomainError):
        GitHubIssueLabel(id=1, name="")
    with pytest.raises(GitHubIssueDomainError):
        GitHubIssueMilestone(id=0, number=1, title="v1", state="open", html_url="https://x")
    with pytest.raises(GitHubIssueDomainError):
        GitHubIssueMilestone(id=1, number=0, title="v1", state="open", html_url="https://x")
    with pytest.raises(GitHubIssueDomainError):
        GitHubIssueMilestone(id=1, number=1, title="", state="open", html_url="https://x")
    with pytest.raises(GitHubIssueDomainError):
        GitHubIssueMilestone(id=1, number=1, title="v1", state="", html_url="https://x")
    with pytest.raises(GitHubIssueDomainError):
        GitHubIssueMilestone(id=1, number=1, title="v1", state="open", html_url="")
    with pytest.raises(GitHubIssueDomainError):
        GitHubIssueMilestone(
            id=1,
            number=1,
            title="v1",
            state="open",
            html_url="https://x",
            due_on="soon",  # type: ignore[arg-type]
        )
