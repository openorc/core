"""Domain tests for the GitHub issue relationship mirrors (issue #60).

These tests prove the transport-independent relationship-mirror semantics:
the two explicit identity spaces (local OpenOrc Repository UUID subject vs.
plain stable numeric GitHub related endpoints), the absence of any
Task-eligibility semantics from the mirror types, and the field-set
introspection contract. No database, no network.
"""

from __future__ import annotations

from uuid import uuid4

import pytest

from openorc.domain.github_issue_relations import (
    GitHubIssueDependencyEdge,
    GitHubIssueParentEdge,
    GitHubIssueRelationsDomainError,
    GitHubIssueSubIssueEdge,
    RelatedIssueEndpoint,
    dependency_edge_field_names,
    parent_edge_field_names,
    sub_issue_edge_field_names,
)

_WORKSPACE_ID = uuid4()
_REPOSITORY_ID = uuid4()
_ENDPOINT = RelatedIssueEndpoint(github_repository_id=555, github_issue_id=777)


def _parent_edge(**overrides: object) -> GitHubIssueParentEdge:
    values: dict[str, object] = {
        "workspace_id": _WORKSPACE_ID,
        "repository_id": _REPOSITORY_ID,
        "github_issue_id": 503,
        "parent": _ENDPOINT,
    }
    values.update(overrides)
    return GitHubIssueParentEdge(**values)  # type: ignore[arg-type]


def _sub_issue_edge(**overrides: object) -> GitHubIssueSubIssueEdge:
    values: dict[str, object] = {
        "workspace_id": _WORKSPACE_ID,
        "repository_id": _REPOSITORY_ID,
        "github_issue_id": 503,
        "child": _ENDPOINT,
    }
    values.update(overrides)
    return GitHubIssueSubIssueEdge(**values)  # type: ignore[arg-type]


def _dependency_edge(**overrides: object) -> GitHubIssueDependencyEdge:
    values: dict[str, object] = {
        "workspace_id": _WORKSPACE_ID,
        "repository_id": _REPOSITORY_ID,
        "github_issue_id": 503,
        "blocker": _ENDPOINT,
    }
    values.update(overrides)
    return GitHubIssueDependencyEdge(**values)  # type: ignore[arg-type]


@pytest.mark.parametrize(
    "factory",
    [_parent_edge, _sub_issue_edge, _dependency_edge],
)
def test_the_subject_side_is_the_local_openorc_scope(factory: object) -> None:
    edge = factory()  # type: ignore[operator]
    assert edge.workspace_id == _WORKSPACE_ID  # type: ignore[attr-defined]
    assert edge.repository_id == _REPOSITORY_ID  # type: ignore[attr-defined]
    assert edge.github_issue_id == 503  # type: ignore[attr-defined]


def test_the_related_endpoint_is_pure_stable_numeric_github_identity() -> None:
    assert _ENDPOINT.github_repository_id == 555
    assert _ENDPOINT.github_issue_id == 777
    with pytest.raises(GitHubIssueRelationsDomainError):
        RelatedIssueEndpoint(github_repository_id=0, github_issue_id=777)
    with pytest.raises(GitHubIssueRelationsDomainError):
        RelatedIssueEndpoint(github_repository_id=555, github_issue_id=-1)
    with pytest.raises(GitHubIssueRelationsDomainError):
        RelatedIssueEndpoint(github_repository_id=True, github_issue_id=777)


@pytest.mark.parametrize(
    ("factory", "bad"),
    [
        (_parent_edge, {"github_issue_id": 0}),
        (_parent_edge, {"workspace_id": "not-a-uuid"}),
        (_sub_issue_edge, {"repository_id": 42}),
        (_dependency_edge, {"github_issue_id": True}),
        (_parent_edge, {"parent": None}),
        (_dependency_edge, {"blocker": "nope"}),
    ],
)
def test_malformed_edges_are_rejected(factory: object, bad: dict[str, object]) -> None:
    with pytest.raises(GitHubIssueRelationsDomainError):
        factory(**bad)  # type: ignore[operator]


def test_the_mirror_field_sets_carry_no_eligibility_semantics() -> None:
    # The mirrors are presentation state: no Task reference, no eligibility
    # or blocking-outcome field can ever live on them.
    for names in (
        parent_edge_field_names(),
        sub_issue_edge_field_names(),
        dependency_edge_field_names(),
    ):
        assert "task_id" not in names
        assert "blocked" not in names
        assert "eligible" not in names
        assert "state" not in names
    assert parent_edge_field_names() == frozenset(
        {"workspace_id", "repository_id", "github_issue_id", "parent"}
    )
    assert sub_issue_edge_field_names() == frozenset(
        {"workspace_id", "repository_id", "github_issue_id", "child"}
    )
    assert dependency_edge_field_names() == frozenset(
        {"workspace_id", "repository_id", "github_issue_id", "blocker"}
    )
