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
        ("issue_comment", None),
        ("pull_request", GitHubWebhookRoutingTarget.TASK_BRANCH_OR_PULL_REQUEST),
        ("push", GitHubWebhookRoutingTarget.TASK_BRANCH_OR_PULL_REQUEST),
        ("status", GitHubWebhookRoutingTarget.CHECKS),
        ("check_run", GitHubWebhookRoutingTarget.CHECKS),
        ("check_suite", GitHubWebhookRoutingTarget.CHECKS),
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


@pytest.mark.parametrize("action", ["closed", "labeled", "unlabeled", "edited"])
def test_pull_request_state_actions_route_to_the_state_surface(action: str) -> None:
    payload = dict(_ISSUE_PAYLOAD, action=action, pull_request={"number": 7})

    facts = _classify("pull_request", payload)

    assert facts.classification is GitHubWebhookDeliveryClassification.RELEVANT
    assert facts.routing_target is GitHubWebhookRoutingTarget.PULL_REQUEST_STATE


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
