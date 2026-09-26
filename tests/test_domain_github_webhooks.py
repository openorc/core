"""Deterministic tests for the GitHub webhook delivery domain models (issue #61).

Proves the intake-shape invariants (the classification/target/resolution
coupling the migration's CHECK constraints mirror), the positive-identity
checks, and the secret/content-leak proof surface: the delivery field sets
carry no payload, signature, or secret-bearing field.
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime

import pytest

from openorc.domain.github_webhooks import (
    GitHubWebhookDelivery,
    GitHubWebhookDeliveryClassification,
    GitHubWebhookDeliveryDomainError,
    GitHubWebhookDeliveryIntake,
    GitHubWebhookRouteResolution,
    GitHubWebhookRoutingResolution,
    GitHubWebhookRoutingTarget,
    github_webhook_delivery_field_names,
    github_webhook_delivery_intake_field_names,
    github_webhook_delivery_route_field_names,
)

_NOW = datetime(2026, 9, 26, 12, 0, 0, tzinfo=UTC)


def _intake(**overrides: object) -> GitHubWebhookDeliveryIntake:
    values: dict[str, object] = {
        "delivery_guid": "guid-1",
        "event_name": "issues",
        "action": "edited",
        "classification": GitHubWebhookDeliveryClassification.RELEVANT,
        "routing_target": GitHubWebhookRoutingTarget.ISSUE_STATE,
        "routing_resolution": GitHubWebhookRoutingResolution.RESOLVED,
        "github_installation_id": 123,
        "github_repository_id": 456,
        "github_issue_number": 42,
        "github_pull_request_number": None,
    }
    values.update(overrides)
    return GitHubWebhookDeliveryIntake(**values)  # type: ignore[arg-type]


def test_a_relevant_intake_validates_with_full_identity() -> None:
    intake = _intake()

    assert intake.classification is GitHubWebhookDeliveryClassification.RELEVANT


@pytest.mark.parametrize(
    "overrides",
    [
        {"routing_target": None},
        {"routing_resolution": None},
        {"github_installation_id": None},
        {"github_repository_id": None},
        {"classification": GitHubWebhookDeliveryClassification.IGNORED},
    ],
)
def test_relevant_intake_requires_target_resolution_and_identity(
    overrides: dict[str, object],
) -> None:
    with pytest.raises(GitHubWebhookDeliveryDomainError):
        _intake(**overrides)


def test_a_non_relevant_intake_carries_no_target_or_resolution() -> None:
    intake = _intake(
        delivery_guid="guid-2",
        event_name="ping",
        classification=GitHubWebhookDeliveryClassification.IGNORED,
        routing_target=None,
        routing_resolution=None,
        github_installation_id=None,
        github_repository_id=None,
        github_issue_number=None,
        github_pull_request_number=None,
    )

    assert intake.routing_target is None
    assert intake.routing_resolution is None


def test_issue_and_pull_request_numbers_are_mutually_exclusive() -> None:
    with pytest.raises(GitHubWebhookDeliveryDomainError):
        _intake(github_pull_request_number=7)


@pytest.mark.parametrize(
    ("field_name", "value"),
    [
        ("github_installation_id", 0),
        ("github_installation_id", -5),
        ("github_installation_id", True),
        ("github_repository_id", 0),
        ("github_issue_number", -1),
        ("github_pull_request_number", 0),
        ("delivery_guid", ""),
        ("delivery_guid", "   "),
        ("event_name", ""),
        ("action", ""),
    ],
)
def test_positive_int_and_non_empty_text_invariants(field_name: str, value: object) -> None:
    with pytest.raises(GitHubWebhookDeliveryDomainError):
        _intake(**{field_name: value})


def test_the_persisted_delivery_round_trips_the_intake_shape() -> None:
    intake = _intake()

    delivery = GitHubWebhookDelivery(
        id=uuid.uuid4(),
        delivery_guid=intake.delivery_guid,
        event_name=intake.event_name,
        action=intake.action,
        classification=intake.classification,
        routing_target=intake.routing_target,
        routing_resolution=intake.routing_resolution,
        github_installation_id=intake.github_installation_id,
        github_repository_id=intake.github_repository_id,
        github_issue_number=intake.github_issue_number,
        github_pull_request_number=intake.github_pull_request_number,
        received_at=_NOW,
    )

    assert delivery.received_at == _NOW


def test_route_resolution_defaults_to_no_routes() -> None:
    resolution = GitHubWebhookRouteResolution(
        resolution=GitHubWebhookRoutingResolution.UNMAPPED_INSTALLATION
    )

    assert resolution.routes == ()


def test_delivery_field_sets_carry_no_payload_or_secret_field() -> None:
    for field_names in (
        github_webhook_delivery_field_names(),
        github_webhook_delivery_intake_field_names(),
    ):
        forbidden = (
            "payload",
            "raw_body",
            "body",
            "signature",
            "secret",
            "token",
            "credential",
            "title",
            "content",
        )
        for name in forbidden:
            assert name not in field_names


def test_route_field_names_are_the_persisted_linkage_shape() -> None:
    field_names = github_webhook_delivery_route_field_names()

    assert field_names == frozenset(
        {"id", "delivery_id", "workspace_id", "repository_id", "created_at"}
    )
