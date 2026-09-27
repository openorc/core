"""Deterministic tests for the GitHub webhook payload classification boundary (issue #61).

Covers the bounded classification contract over deterministic raw-byte
payload fixtures: relevant settled-v1 families map onto the normalized
semantic routing targets with stable identity; irrelevant/unsupported events
are safely ignored; structurally unusable payloads are safely classified;
customer content members are never extracted.
"""

from __future__ import annotations

import json

import pytest

from openorc.adapters.github.webhook_classification import (
    GitHubWebhookDeliveryFacts,
    classify_github_webhook_delivery,
)
from openorc.domain.github_webhooks import (
    GitHubWebhookDeliveryClassification,
    GitHubWebhookRoutingTarget,
)

_ISSUE_PAYLOAD = {
    "action": "edited",
    "installation": {"id": 12345678},
    "repository": {"id": 987654321, "name": "hello-world", "full_name": "octo/hello-world"},
    "issue": {"number": 42, "title": "SECRET-TITLE", "body": "SECRET-BODY"},
}


def _payload_bytes(payload: object) -> bytes:
    return json.dumps(payload).encode("utf-8")


def _classify(event_name: str, payload: object) -> GitHubWebhookDeliveryFacts:
    return classify_github_webhook_delivery(
        event_name=event_name, payload_bytes=_payload_bytes(payload)
    )


@pytest.mark.parametrize(
    ("event_name", "expected_target"),
    [
        ("issues", GitHubWebhookRoutingTarget.ISSUE_STATE),
        ("pull_request", GitHubWebhookRoutingTarget.TASK_BRANCH_OR_PULL_REQUEST),
        ("push", GitHubWebhookRoutingTarget.TASK_BRANCH_OR_PULL_REQUEST),
        ("status", GitHubWebhookRoutingTarget.CHECKS),
        ("check_run", GitHubWebhookRoutingTarget.CHECKS),
        ("check_suite", GitHubWebhookRoutingTarget.CHECKS),
        ("sub_issues", GitHubWebhookRoutingTarget.ISSUE_RELATIONS),
        ("issue_dependencies", GitHubWebhookRoutingTarget.ISSUE_RELATIONS),
        ("repository", GitHubWebhookRoutingTarget.REPOSITORY_METADATA),
    ],
)
def test_settled_v1_event_families_classify(
    event_name: str, expected_target: GitHubWebhookRoutingTarget | None
) -> None:
    payload: dict[str, object] = {
        "action": "opened",
        "installation": {"id": 1},
        "repository": {"id": 2},
        "issue": {"number": 3},
        "pull_request": {"number": 4},
    }
    facts = _classify(event_name, payload)

    expected_classification = (
        GitHubWebhookDeliveryClassification.RELEVANT
        if expected_target is not None
        else GitHubWebhookDeliveryClassification.IGNORED
    )
    assert facts.classification is expected_classification
    assert facts.routing_target is expected_target


def test_issue_delivery_classifies_relevant_with_stable_identity() -> None:
    facts = _classify("issues", _ISSUE_PAYLOAD)

    assert facts.classification is GitHubWebhookDeliveryClassification.RELEVANT
    assert facts.routing_target is GitHubWebhookRoutingTarget.ISSUE_STATE
    assert facts.github_installation_id == 12345678
    assert facts.github_repository_id == 987654321
    assert facts.github_issue_number == 42
    assert facts.github_pull_request_number is None


def test_pull_request_identity_action_routes_to_the_task_branch_surface() -> None:
    payload = dict(_ISSUE_PAYLOAD, action="opened", pull_request={"number": 7})

    facts = _classify("pull_request", payload)

    assert facts.classification is GitHubWebhookDeliveryClassification.RELEVANT
    assert facts.routing_target is GitHubWebhookRoutingTarget.TASK_BRANCH_OR_PULL_REQUEST
    assert facts.github_pull_request_number == 7
    assert facts.github_issue_number is None


def test_sub_issue_delivery_classifies_relevant_without_any_issue_identity() -> None:
    # issue #120: the parent or the sub-issue may live in another repository
    # (`parent_issue_repo`), so no issue number is extracted and the delivery
    # routes by the stable repository identity; customer content members are
    # never read.
    payload = {
        "action": "parent_issue_added",
        "installation": {"id": 12345678},
        "repository": {"id": 987654321},
        "sub_issue": {"number": 7, "title": "SECRET-SUB-TITLE"},
        "parent_issue": {"number": 5, "title": "SECRET-PARENT-TITLE"},
        "parent_issue_repo": {"id": 111, "name": "octo/other"},
    }

    facts = _classify("sub_issues", payload)

    assert facts.classification is GitHubWebhookDeliveryClassification.RELEVANT
    assert facts.routing_target is GitHubWebhookRoutingTarget.ISSUE_RELATIONS
    assert facts.github_installation_id == 12345678
    assert facts.github_repository_id == 987654321
    assert facts.github_issue_number is None
    assert facts.github_pull_request_number is None
    extracted = {getattr(facts, field) for field in dir(facts) if not field.startswith("_")}
    for forbidden in ("SECRET-SUB-TITLE", "SECRET-PARENT-TITLE", "octo/other"):
        assert forbidden not in extracted


def test_issue_dependency_delivery_classifies_relevant_without_any_issue_identity() -> None:
    # issue #120: the blocked and blocking issues may live in different
    # repositories (`blocking_issue_repo`), so no issue number is extracted
    # and the delivery routes by the stable repository identity.
    payload = {
        "action": "blocked_by_added",
        "installation": {"id": 12345678},
        "repository": {"id": 987654321},
        "blocked_issue_id": 7,
        "blocked_issue": {"number": 7, "title": "SECRET-BLOCKED-TITLE"},
        "blocking_issue": {"number": 5, "title": "SECRET-BLOCKING-TITLE"},
        "blocking_issue_repo": {"id": 111, "name": "octo/other"},
    }

    facts = _classify("issue_dependencies", payload)

    assert facts.classification is GitHubWebhookDeliveryClassification.RELEVANT
    assert facts.routing_target is GitHubWebhookRoutingTarget.ISSUE_RELATIONS
    assert facts.github_installation_id == 12345678
    assert facts.github_repository_id == 987654321
    assert facts.github_issue_number is None
    assert facts.github_pull_request_number is None
    extracted = {getattr(facts, field) for field in dir(facts) if not field.startswith("_")}
    for forbidden in ("SECRET-BLOCKED-TITLE", "SECRET-BLOCKING-TITLE", "octo/other"):
        assert forbidden not in extracted


def test_repository_delivery_classifies_relevant_with_stable_repository_identity() -> None:
    payload = {
        "action": "renamed",
        "installation": {"id": 12345678},
        "repository": {"id": 987654321, "name": "new-name"},
    }

    facts = _classify("repository", payload)

    assert facts.classification is GitHubWebhookDeliveryClassification.RELEVANT
    assert facts.routing_target is GitHubWebhookRoutingTarget.REPOSITORY_METADATA
    assert facts.github_installation_id == 12345678
    assert facts.github_repository_id == 987654321


@pytest.mark.parametrize("event_name", ["installation", "installation_repositories"])
def test_installation_deliveries_classify_installation_scoped(event_name: str) -> None:
    # issue #120: installation-level notifications affect repository sets or
    # the whole installation, never one singular repository: the delivery is
    # relevant with installation-only identity. The `repositories_added` /
    # `repositories_removed` arrays are deliberately not extracted — they
    # identify what may need reconciliation, never what canonical state
    # becomes.
    payload = {
        "action": "created" if event_name == "installation" else "added",
        "installation": {"id": 12345678},
        "repositories_added": [{"id": 111, "name": "octo/added"}],
        "repositories_removed": [{"id": 222, "name": "octo/removed"}],
        "repository_selection": "selected",
    }

    facts = _classify(event_name, payload)

    assert facts.classification is GitHubWebhookDeliveryClassification.RELEVANT
    assert facts.routing_target is GitHubWebhookRoutingTarget.REPOSITORY_METADATA
    assert facts.github_installation_id == 12345678
    assert facts.github_repository_id is None
    assert facts.github_issue_number is None
    assert facts.github_pull_request_number is None
    extracted = {getattr(facts, field) for field in dir(facts) if not field.startswith("_")}
    assert "octo/added" not in extracted
    assert "octo/removed" not in extracted


@pytest.mark.parametrize(
    ("event_name", "payload"),
    [
        ("installation", {"action": "created"}),  # missing installation identity
        ("installation_repositories", {"action": "added", "repositories_added": []}),
        ("sub_issues", {"installation": {"id": 1}}),  # missing repository identity
        ("issue_dependencies", {"repository": {"id": 2}}),  # missing installation identity
        ("repository", {"installation": {"id": 1}}),  # missing repository identity
    ],
)
def test_new_family_payloads_without_stable_identity_fail_closed(
    event_name: str, payload: dict[str, object]
) -> None:
    facts = _classify(event_name, payload)

    assert facts.classification is GitHubWebhookDeliveryClassification.UNUSABLE
    assert facts.routing_target is None


@pytest.mark.parametrize("action", ["closed", "labeled", "unlabeled", "edited"])
def test_pull_request_state_actions_route_to_the_state_surface(action: str) -> None:
    payload = dict(_ISSUE_PAYLOAD, action=action, pull_request={"number": 7})

    facts = _classify("pull_request", payload)

    assert facts.classification is GitHubWebhookDeliveryClassification.RELEVANT
    assert facts.routing_target is GitHubWebhookRoutingTarget.PULL_REQUEST_STATE


def test_issue_comment_is_not_a_settled_family_and_is_safely_ignored() -> None:
    # issue #122: the subscription is deliberately not required; a delivery
    # received anyway is valid-but-irrelevant and never extracts content.
    facts = _classify(
        "issue_comment",
        {"action": "created", "installation": {"id": 1}, "comment": {"body": "SECRET-BODY"}},
    )

    assert facts.classification is GitHubWebhookDeliveryClassification.IGNORED
    assert facts.routing_target is None
    fields = {field for field in dir(facts) if not field.startswith("_")}
    assert "comment" not in fields
    extracted = {
        getattr(facts, field) for field in fields if isinstance(getattr(facts, field), str)
    }
    assert "SECRET-BODY" not in extracted


def test_unsupported_events_are_safely_ignored() -> None:
    for event_name in ("ping", "fork", "watch", "star", "release"):
        facts = _classify(event_name, {"action": "anything", "zen": "zen"})

        assert facts.classification is GitHubWebhookDeliveryClassification.IGNORED
        assert facts.routing_target is None


def test_non_string_actions_are_treated_as_absent() -> None:
    facts = _classify("issues", dict(_ISSUE_PAYLOAD, action={"weird": True}))

    assert facts.action is None
    assert facts.classification is GitHubWebhookDeliveryClassification.RELEVANT


@pytest.mark.parametrize(
    "payload",
    [
        {"installation": {"id": 1}, "repository": {"id": 2}},  # missing issue identity
        {"installation": {"id": 1}, "issue": {"number": 3}},  # missing repository identity
        {"repository": {"id": 2}, "issue": {"number": 3}},  # missing installation identity
        {"installation": {"id": 0}, "repository": {"id": 2}, "issue": {"number": 3}},
        {"installation": {"id": True}, "repository": {"id": 2}, "issue": {"number": 3}},
        {"installation": {"id": "1"}, "repository": {"id": 2}, "issue": {"number": 3}},
    ],
)
def test_structurally_unusable_payloads_fail_closed(payload: dict[str, object]) -> None:
    facts = _classify("issues", payload)

    assert facts.classification is GitHubWebhookDeliveryClassification.UNUSABLE
    assert facts.routing_target is None


def test_uninterpretable_payload_bytes_classify_unusable() -> None:
    facts = classify_github_webhook_delivery(event_name="issues", payload_bytes=b"not-json-at-all")

    assert facts.classification is GitHubWebhookDeliveryClassification.UNUSABLE


def test_a_json_array_payload_classifies_unusable() -> None:
    facts = classify_github_webhook_delivery(event_name="issues", payload_bytes=b"[1, 2, 3]")

    assert facts.classification is GitHubWebhookDeliveryClassification.UNUSABLE


def test_customer_content_is_never_extracted() -> None:
    facts = _classify("issues", _ISSUE_PAYLOAD)
    fields = {field for field in dir(facts) if not field.startswith("_")}

    assert "title" not in fields
    assert "body" not in fields
    assert "payload" not in fields
    extracted = {
        getattr(facts, field) for field in fields if isinstance(getattr(facts, field), str)
    }
    assert "SECRET-TITLE" not in extracted
    assert "SECRET-BODY" not in extracted
    assert "octo/hello-world" not in extracted
