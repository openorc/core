"""Deterministic tests for the GitHub relationship observations (issue #60).

Fail-closed parser contract for the blocked-by/sub-issue listings and the
GraphQL nullable-parent fact set: a well-formed related-issue object yields
its stable identity plus the repository reference; an authoritative empty
array is a first-class empty observation; a `parent: null` GraphQL answer is
the authoritative no-parent fact; and any uninterpretable/failed shape
classifies as an uncertain outcome — never as authoritative relationship
state. No live GitHub access.
"""

from __future__ import annotations

import pytest

from openorc.adapters.github.errors import GitHubOutcomeUncertainError
from openorc.adapters.github.observations_relations import (
    parse_graphql_issue_parent,
    parse_related_issue_payload,
    parse_related_issue_payloads,
)


def test_a_well_formed_related_issue_parses_to_stable_identity() -> None:
    observation = parse_related_issue_payload(
        {"id": 503, "repository_url": "https://api.github.com/repos/octocat/Hello-World"}
    )
    assert observation.github_issue_id == 503
    assert observation.repository_url == "https://api.github.com/repos/octocat/Hello-World"


@pytest.mark.parametrize(
    "payload",
    [
        None,
        "issue",
        {},
        {"repository_url": "https://api.github.com/repos/octocat/Hello-World"},
        {"id": 0, "repository_url": "https://api.github.com/repos/octocat/Hello-World"},
        {"id": True, "repository_url": "https://api.github.com/repos/octocat/Hello-World"},
        {"id": 503},
        {"id": 503, "repository_url": ""},
        {"id": 503, "repository_url": 7},
    ],
)
def test_malformed_related_issue_payloads_fail_closed(payload: object) -> None:
    with pytest.raises(GitHubOutcomeUncertainError):
        parse_related_issue_payload(payload)


def test_an_authoritative_empty_listing_is_a_first_class_empty_observation() -> None:
    assert parse_related_issue_payloads([]) == []


def test_a_non_array_listing_fails_closed() -> None:
    with pytest.raises(GitHubOutcomeUncertainError):
        parse_related_issue_payloads({"data": []})


def test_a_partially_interpretable_listing_fails_closed() -> None:
    entries = [
        {"id": 1, "repository_url": "https://api.github.com/repos/o/r"},
        {"id": "broken"},
    ]
    with pytest.raises(GitHubOutcomeUncertainError):
        parse_related_issue_payloads(entries)


def _parent_answer(parent: object) -> dict[str, object]:
    return {
        "data": {
            "repository": {
                "issue": {
                    "parent": parent,
                }
            }
        }
    }


def test_a_present_graphql_parent_yields_both_stable_ids() -> None:
    observation = parse_graphql_issue_parent(
        _parent_answer({"databaseId": 501, "repository": {"databaseId": 555}})
    )
    assert observation.parent is not None
    assert observation.parent.github_issue_id == 501
    assert observation.parent.github_repository_id == 555


def test_a_null_graphql_parent_is_the_authoritative_no_parent_fact() -> None:
    observation = parse_graphql_issue_parent(_parent_answer(None))
    assert observation.parent is None


@pytest.mark.parametrize(
    "payload",
    [
        None,
        "nope",
        {},
        {"data": None},
        {"data": {}},
        {"errors": [{"message": "boom"}], "data": {"repository": None}},
        {"data": {"errors": [{"message": "boom"}]}},
        {"data": {"repository": {"issue": {"parent": "broken"}}}},
        {"data": {"repository": {"issue": {"parent": {}}}}},
        {"data": {"repository": {"issue": {"parent": {"databaseId": 1}}}}},
        {"data": {"repository": {"issue": {"parent": {"databaseId": 501, "repository": {}}}}}},
        {
            "data": {
                "repository": {
                    "issue": {
                        "parent": {
                            "databaseId": -1,
                            "repository": {"databaseId": 2},
                        }
                    }
                }
            }
        },
    ],
)
def test_malformed_or_failing_graphql_answers_fail_closed(payload: object) -> None:
    # Errors, missing members, and malformed shapes are uncertain outcomes —
    # never reinterpreted as an authoritative parent or no-parent fact.
    with pytest.raises(GitHubOutcomeUncertainError):
        parse_graphql_issue_parent(payload)
