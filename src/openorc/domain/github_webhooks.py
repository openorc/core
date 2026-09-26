"""GitHub webhook delivery domain models (issue #61).

The normalized, transport-independent vocabulary and records of the
authenticated GitHub webhook intake boundary:

- ``GitHubWebhookDelivery`` is the durable, GUID-keyed record of one VERIFIED
  webhook delivery: safe metadata only (event name, optional action, the
  bounded classification, the normalized semantic routing target, the bounded
  routing resolution, stable provider identifiers, and the received instant).
  Raw webhook bodies, signature material, secrets, and customer content are
  categorically absent from these models.
- ``GitHubWebhookDeliveryIntake`` is the pre-persistence candidate the intake
  service composes (no OpenOrc identity or received instant yet — the
  database assigns both durably).
- ``GitHubWebhookDeliveryRoute`` is one persisted resolved Workspace routing
  linkage of an accepted delivery; ``GitHubWebhookResolvedRoute`` is one
  exact route match produced by the B1 routing resolution. Fan-out over
  several Workspaces is representable; the linkage is
  notification/recovery metadata, never workflow authority.
- The bounded vocabularies mirror the database CHECK constraints exactly:
  ``GitHubWebhookDeliveryClassification`` (relevant/ignored/unusable),
  ``GitHubWebhookRoutingTarget`` (the semantic reconciliation-target space
  dispatch #120 maps onto), and ``GitHubWebhookRoutingResolution``
  (resolved/unmapped_installation/unconfigured_repository/route_mismatch).

A delivery record is notification/recovery metadata, not workflow authority
and not an event-sourced copy of GitHub.
"""

from __future__ import annotations

from dataclasses import dataclass, fields
from datetime import datetime
from enum import StrEnum
from uuid import UUID

__all__ = [
    "GitHubWebhookDelivery",
    "GitHubWebhookDeliveryClassification",
    "GitHubWebhookDeliveryDomainError",
    "GitHubWebhookDeliveryIntake",
    "GitHubWebhookDeliveryRoute",
    "GitHubWebhookResolvedRoute",
    "GitHubWebhookRouteResolution",
    "GitHubWebhookRoutingResolution",
    "GitHubWebhookRoutingTarget",
    "github_webhook_delivery_field_names",
    "github_webhook_delivery_intake_field_names",
    "github_webhook_delivery_route_field_names",
]


class GitHubWebhookDeliveryDomainError(Exception):
    """Raised when a GitHub webhook delivery domain invariant is violated."""


class GitHubWebhookDeliveryClassification(StrEnum):
    """The bounded intake classification of one verified delivery.

    ``relevant`` — a settled v1 GitHub event family whose payload carries
    sufficient stable routing identity; it persists a normalized semantic
    routing target for dispatch. ``ignored`` — valid but irrelevant or
    unsupported; safely acknowledged. ``unusable`` — structurally unusable
    for routing; safely classified without inventing authority.
    """

    RELEVANT = "relevant"
    IGNORED = "ignored"
    UNUSABLE = "unusable"


class GitHubWebhookRoutingTarget(StrEnum):
    """The normalized semantic reconciliation targets dispatch (#120) maps onto.

    The vocabulary mirrors the settled v1 notification categories: mutable
    repository/installation metadata; issue state/requirements; sub-issue
    hierarchy and dependencies; canonical Task branch / pull-request identity
    or head; PR merged/closed/open state; checks and commit statuses.
    """

    REPOSITORY_METADATA = "repository_metadata"
    ISSUE_STATE = "issue_state"
    ISSUE_RELATIONS = "issue_relations"
    TASK_BRANCH_OR_PULL_REQUEST = "task_branch_or_pull_request"
    PULL_REQUEST_STATE = "pull_request_state"
    CHECKS = "checks"


class GitHubWebhookRoutingResolution(StrEnum):
    """The bounded routing-resolution outcome of one relevant delivery.

    ``resolved`` — at least one exact Workspace route match was retained.
    ``unmapped_installation`` — the delivery's installation has no OpenOrc
    Workspace installation record. ``unconfigured_repository`` — the
    installation is known, but the affected stable repository identity is not
    configured as an OpenOrc Repository in its Workspaces. ``route_mismatch``
    — an OpenOrc Repository record exists for the affected identity, but none
    is explicitly routed to this delivery's installation (unrouted or routed
    elsewhere, including another Workspace). Zero/mismatched routes are
    non-authoritative safe integration observations.
    """

    RESOLVED = "resolved"
    UNMAPPED_INSTALLATION = "unmapped_installation"
    UNCONFIGURED_REPOSITORY = "unconfigured_repository"
    ROUTE_MISMATCH = "route_mismatch"


def _require_positive_int(value: object, field_name: str) -> None:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise GitHubWebhookDeliveryDomainError(
            f"{field_name} must be a positive integer, got {value!r}"
        )


def _require_non_empty_text(value: object, field_name: str) -> None:
    if not isinstance(value, str) or not value.strip():
        raise GitHubWebhookDeliveryDomainError(f"{field_name} must be a non-empty string")


def _validate_intake_shape(
    *,
    delivery_guid: object,
    event_name: object,
    action: object,
    classification: object,
    routing_target: object,
    routing_resolution: object,
    github_installation_id: object,
    github_repository_id: object,
    github_issue_number: object,
    github_pull_request_number: object,
) -> None:
    """Validate the intake facts shared by the candidate and persisted records.

    Mirrors the migration's CHECK constraints exactly: a ``relevant``
    classification requires both the semantic routing target and the bounded
    routing resolution (and, per the classification contract, both stable
    installation/repository identities); non-relevant classifications carry
    neither; issue and pull-request numbers are mutually exclusive.
    """
    _require_non_empty_text(delivery_guid, "delivery_guid")
    _require_non_empty_text(event_name, "event_name")
    if action is not None:
        _require_non_empty_text(action, "action")
    if not isinstance(classification, GitHubWebhookDeliveryClassification):
        raise GitHubWebhookDeliveryDomainError(
            "classification must be a GitHubWebhookDeliveryClassification"
        )
    if routing_target is not None and not isinstance(routing_target, GitHubWebhookRoutingTarget):
        raise GitHubWebhookDeliveryDomainError(
            "routing_target must be a GitHubWebhookRoutingTarget or None"
        )
    if routing_resolution is not None and not isinstance(
        routing_resolution, GitHubWebhookRoutingResolution
    ):
        raise GitHubWebhookDeliveryDomainError(
            "routing_resolution must be a GitHubWebhookRoutingResolution or None"
        )
    relevant = classification is GitHubWebhookDeliveryClassification.RELEVANT
    if relevant != (routing_target is not None):
        raise GitHubWebhookDeliveryDomainError(
            "a relevant delivery carries a routing target; every other classification carries none"
        )
    if relevant != (routing_resolution is not None):
        raise GitHubWebhookDeliveryDomainError(
            "a relevant delivery carries a routing resolution; every other "
            "classification carries none"
        )
    for field_name, value in (
        ("github_installation_id", github_installation_id),
        ("github_repository_id", github_repository_id),
        ("github_issue_number", github_issue_number),
        ("github_pull_request_number", github_pull_request_number),
    ):
        if value is not None:
            _require_positive_int(value, field_name)
    if relevant and (github_installation_id is None or github_repository_id is None):
        raise GitHubWebhookDeliveryDomainError(
            "a relevant delivery requires both stable installation and repository identity"
        )
    if github_issue_number is not None and github_pull_request_number is not None:
        raise GitHubWebhookDeliveryDomainError(
            "a delivery never addresses an issue and a pull request at once"
        )


@dataclass(frozen=True, slots=True)
class GitHubWebhookDeliveryIntake:
    """The pre-persistence candidate of one verified delivery (issue #61).

    Composed by the intake service from the adapter's verified-payload facts
    and the B1 routing resolution; OpenOrc identity and the received instant
    are assigned durably by the database.
    """

    delivery_guid: str
    event_name: str
    action: str | None
    classification: GitHubWebhookDeliveryClassification
    routing_target: GitHubWebhookRoutingTarget | None
    routing_resolution: GitHubWebhookRoutingResolution | None
    github_installation_id: int | None
    github_repository_id: int | None
    github_issue_number: int | None
    github_pull_request_number: int | None

    def __post_init__(self) -> None:
        _validate_intake_shape(
            delivery_guid=self.delivery_guid,
            event_name=self.event_name,
            action=self.action,
            classification=self.classification,
            routing_target=self.routing_target,
            routing_resolution=self.routing_resolution,
            github_installation_id=self.github_installation_id,
            github_repository_id=self.github_repository_id,
            github_issue_number=self.github_issue_number,
            github_pull_request_number=self.github_pull_request_number,
        )


@dataclass(frozen=True, slots=True)
class GitHubWebhookDelivery:
    """The durable, GUID-keyed record of one verified webhook delivery.

    Safe metadata only; the record is the durable intake/deduplication fact
    later dispatch/recovery reads. The record never carries workflow
    authority: invoking reconciliation is dispatch work (#120).
    """

    id: UUID
    delivery_guid: str
    event_name: str
    action: str | None
    classification: GitHubWebhookDeliveryClassification
    routing_target: GitHubWebhookRoutingTarget | None
    routing_resolution: GitHubWebhookRoutingResolution | None
    github_installation_id: int | None
    github_repository_id: int | None
    github_issue_number: int | None
    github_pull_request_number: int | None
    received_at: datetime

    def __post_init__(self) -> None:
        _validate_intake_shape(
            delivery_guid=self.delivery_guid,
            event_name=self.event_name,
            action=self.action,
            classification=self.classification,
            routing_target=self.routing_target,
            routing_resolution=self.routing_resolution,
            github_installation_id=self.github_installation_id,
            github_repository_id=self.github_repository_id,
            github_issue_number=self.github_issue_number,
            github_pull_request_number=self.github_pull_request_number,
        )


@dataclass(frozen=True, slots=True)
class GitHubWebhookResolvedRoute:
    """One exact route match of a relevant delivery's B1 routing resolution.

    The Workspace and the OpenOrc Repository record explicitly routed to the
    delivery's installation for the affected stable repository identity.
    Never authorization, never inferred from mutable names.
    """

    workspace_id: UUID
    repository_id: UUID


@dataclass(frozen=True, slots=True)
class GitHubWebhookDeliveryRoute:
    """One persisted resolved Workspace routing linkage of an accepted delivery."""

    id: UUID
    delivery_id: UUID
    workspace_id: UUID
    repository_id: UUID
    created_at: datetime


def github_webhook_delivery_field_names() -> frozenset[str]:
    """Return the exact field set a persisted delivery record exposes.

    Lets tests prove the record carries no payload/signature/secret-bearing
    field: raw webhook bodies, signature material, and customer content never
    appear in domain or persistence objects.
    """
    return frozenset(field.name for field in fields(GitHubWebhookDelivery))


def github_webhook_delivery_intake_field_names() -> frozenset[str]:
    """Return the exact field set a delivery intake candidate exposes."""
    return frozenset(field.name for field in fields(GitHubWebhookDeliveryIntake))


def github_webhook_delivery_route_field_names() -> frozenset[str]:
    """Return the exact field set a persisted routing linkage exposes."""
    return frozenset(field.name for field in fields(GitHubWebhookDeliveryRoute))


@dataclass(frozen=True, slots=True)
class GitHubWebhookRouteResolution:
    """The classified result of one relevant delivery's B1 routing resolution.

    ``resolution`` classifies the outcome against the explicit Workspace
    routing (``resolved`` when ``routes`` is non-empty; the bounded
    fail-closed observations otherwise). Routes are ordered by Workspace, so
    repeated resolutions converge on the same facts.
    """

    resolution: GitHubWebhookRoutingResolution
    routes: tuple[GitHubWebhookResolvedRoute, ...] = ()
