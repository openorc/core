"""GitHub webhook payload classification boundary (issue #61).

Provider payload semantics stay inside this GitHub adapter boundary. The
classifier parses a VERIFIED payload only far enough to classify it and to
extract safe stable routing identity — never issue/PR titles or bodies, never
repository content, never comments:

- ``relevant`` — one of the settled v1 GitHub event families (the same set
  the deployed App is required to subscribe to,
  ``REQUIRED_V1_WEBHOOK_EVENTS`` in ``capabilities.py``) whose payload
  carries sufficient stable routing identity: the stable installation ID,
  the stable repository ID, and — where the family addresses a provider
  object — the issue or pull-request number. Relevant deliveries are mapped
  onto the normalized semantic reconciliation-target vocabulary
  (:mod:`openorc.domain.github_webhooks`) that dispatch (#120) dispatches
  from; the concrete provider event names/actions never leak above this
  boundary.
- ``ignored`` — valid but irrelevant or unsupported (anything outside the
  settled v1 families, including ``issue_comment``: no v1 reconciliation
  surface reads comments); safely acknowledged.
- ``unusable`` — a settled-family payload that is structurally unusable for
  safe routing (uninterpretable JSON, missing/malformed stable identity
  members); safely classified without inventing authority.

Non-string ``action`` members are treated as absent (the action is
presentation metadata, not routing identity). No member other than the
listed stable identity paths is ever read.
"""

from __future__ import annotations

import json
from dataclasses import dataclass

from openorc.adapters.github.capabilities import REQUIRED_V1_WEBHOOK_EVENTS
from openorc.domain.github_webhooks import (
    GitHubWebhookDeliveryClassification,
    GitHubWebhookRoutingTarget,
)

__all__ = ["GitHubWebhookDeliveryFacts", "classify_github_webhook_delivery"]

# The settled v1 GitHub webhook event families this boundary classifies.
_SETTLED_V1_EVENT_FAMILIES = REQUIRED_V1_WEBHOOK_EVENTS

# Pull-request actions that move the canonical Task branch / PR identity or
# head; every other pull-request action classifies to the PR state surface.
_PR_IDENTITY_ACTIONS = frozenset({"opened", "synchronize", "reopened"})


@dataclass(frozen=True, slots=True)
class GitHubWebhookDeliveryFacts:
    """The safe, normalized facts extracted from one verified delivery.

    Identity and classification only: stable provider identifiers, the
    bounded classification, and (when relevant) the normalized semantic
    routing target. Titles, bodies, comments, and all other payload content
    have no field and no path into this type.
    """

    event_name: str
    action: str | None
    classification: GitHubWebhookDeliveryClassification
    routing_target: GitHubWebhookRoutingTarget | None
    github_installation_id: int | None
    github_repository_id: int | None
    github_issue_number: int | None
    github_pull_request_number: int | None


def _facts(
    event_name: str,
    action: str | None,
    classification: GitHubWebhookDeliveryClassification,
    *,
    routing_target: GitHubWebhookRoutingTarget | None = None,
    github_installation_id: int | None = None,
    github_repository_id: int | None = None,
    github_issue_number: int | None = None,
    github_pull_request_number: int | None = None,
) -> GitHubWebhookDeliveryFacts:
    return GitHubWebhookDeliveryFacts(
        event_name=event_name,
        action=action,
        classification=classification,
        routing_target=routing_target,
        github_installation_id=github_installation_id,
        github_repository_id=github_repository_id,
        github_issue_number=github_issue_number,
        github_pull_request_number=github_pull_request_number,
    )


def _positive_int(value: object) -> int | None:
    """Return the value when it is a well-formed positive integer, else ``None``."""
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        return None
    return value


def _stable_identity_member(payload: object, *path: str) -> int | None:
    """Read one stable identity member along an explicit path, or ``None``."""
    current: object = payload
    for key in path:
        if not isinstance(current, dict):
            return None
        current = current.get(key)
    return _positive_int(current)


def _classify_issues(
    payload: dict[str, object], action: str | None, event_name: str
) -> GitHubWebhookDeliveryFacts:
    installation_id = _stable_identity_member(payload, "installation", "id")
    repository_id = _stable_identity_member(payload, "repository", "id")
    issue_number = _stable_identity_member(payload, "issue", "number")
    if installation_id is None or repository_id is None or issue_number is None:
        return _facts(event_name, action, GitHubWebhookDeliveryClassification.UNUSABLE)
    return _facts(
        event_name,
        action,
        GitHubWebhookDeliveryClassification.RELEVANT,
        routing_target=GitHubWebhookRoutingTarget.ISSUE_STATE,
        github_installation_id=installation_id,
        github_repository_id=repository_id,
        github_issue_number=issue_number,
    )


def _classify_pull_request(
    payload: dict[str, object], action: str | None, event_name: str
) -> GitHubWebhookDeliveryFacts:
    installation_id = _stable_identity_member(payload, "installation", "id")
    repository_id = _stable_identity_member(payload, "repository", "id")
    pull_request_number = _stable_identity_member(payload, "pull_request", "number")
    if installation_id is None or repository_id is None or pull_request_number is None:
        return _facts(event_name, action, GitHubWebhookDeliveryClassification.UNUSABLE)
    # Head/identity-affecting actions route to the canonical branch/PR-identity
    # surface; every other action (closed and presentation changes) routes to
    # the PR merged/closed/open-state surface.
    routing_target = (
        GitHubWebhookRoutingTarget.TASK_BRANCH_OR_PULL_REQUEST
        if action in _PR_IDENTITY_ACTIONS
        else GitHubWebhookRoutingTarget.PULL_REQUEST_STATE
    )
    return _facts(
        event_name,
        action,
        GitHubWebhookDeliveryClassification.RELEVANT,
        routing_target=routing_target,
        github_installation_id=installation_id,
        github_repository_id=repository_id,
        github_pull_request_number=pull_request_number,
    )


def _classify_push(
    payload: dict[str, object], action: str | None, event_name: str
) -> GitHubWebhookDeliveryFacts:
    installation_id = _stable_identity_member(payload, "installation", "id")
    repository_id = _stable_identity_member(payload, "repository", "id")
    if installation_id is None or repository_id is None:
        return _facts(event_name, action, GitHubWebhookDeliveryClassification.UNUSABLE)
    # Branch names are mutable presentation, never durable identity: the
    # delivery routes by the stable repository identity and dispatch
    # re-observes branch/head state authoritatively.
    return _facts(
        event_name,
        action,
        GitHubWebhookDeliveryClassification.RELEVANT,
        routing_target=GitHubWebhookRoutingTarget.TASK_BRANCH_OR_PULL_REQUEST,
        github_installation_id=installation_id,
        github_repository_id=repository_id,
    )


def _classify_checks(
    payload: dict[str, object], action: str | None, event_name: str
) -> GitHubWebhookDeliveryFacts:
    installation_id = _stable_identity_member(payload, "installation", "id")
    repository_id = _stable_identity_member(payload, "repository", "id")
    if installation_id is None or repository_id is None:
        return _facts(event_name, action, GitHubWebhookDeliveryClassification.UNUSABLE)
    # Check-run/suite and commit-status observations route by the stable
    # repository identity; dispatch re-observes the fresh authoritative check
    # state, so individual check object IDs are deliberately not extracted.
    return _facts(
        event_name,
        action,
        GitHubWebhookDeliveryClassification.RELEVANT,
        routing_target=GitHubWebhookRoutingTarget.CHECKS,
        github_installation_id=installation_id,
        github_repository_id=repository_id,
    )


_FAMILY_CLASSIFIERS = {
    "issues": _classify_issues,
    "pull_request": _classify_pull_request,
    "push": _classify_push,
    "status": _classify_checks,
    "check_run": _classify_checks,
    "check_suite": _classify_checks,
}


def classify_github_webhook_delivery(
    *, event_name: str, payload_bytes: bytes
) -> GitHubWebhookDeliveryFacts:
    """Classify one verified delivery into bounded, safe routing facts.

    ``payload_bytes`` are the exact verified raw bytes. The payload is parsed
    only after signature verification succeeded upstream and only into the
    safe identity/classification members this boundary defines.
    """
    try:
        payload = json.loads(payload_bytes.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        return _facts(event_name, None, GitHubWebhookDeliveryClassification.UNUSABLE)
    if not isinstance(payload, dict):
        return _facts(event_name, None, GitHubWebhookDeliveryClassification.UNUSABLE)
    raw_action = payload.get("action")
    action = raw_action if isinstance(raw_action, str) and raw_action.strip() else None
    if event_name not in _SETTLED_V1_EVENT_FAMILIES:
        return _facts(event_name, action, GitHubWebhookDeliveryClassification.IGNORED)
    classifier = _FAMILY_CLASSIFIERS.get(event_name)
    if classifier is None:
        # A settled family with no v1 reconciliation surface (issue_comment:
        # no v1 capability reads comments) is safely ignored.
        return _facts(event_name, action, GitHubWebhookDeliveryClassification.IGNORED)
    return classifier(payload, action, event_name)
