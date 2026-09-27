"""GitHub webhook payload classification boundary (issues #61 and #120).

Provider payload semantics stay inside this GitHub adapter boundary. The
classifier parses a VERIFIED payload only far enough to classify it and to
extract safe stable routing identity — never issue/PR titles or bodies, never
repository content, never comments:

- ``relevant`` — one of the classified v1 GitHub event families
  (``V1_CLASSIFIED_WEBHOOK_EVENTS`` in ``capabilities.py``, a strict superset
  of the deployed App's subscription contract
  ``REQUIRED_V1_WEBHOOK_EVENTS``, adding the default-delivered,
  non-configurable ``installation`` and ``installation_repositories``
  families) whose payload carries sufficient stable routing identity. Two
  identity shapes exist (issue #120): repository-scoped deliveries carry the
  stable installation ID, the stable repository ID, and — where the family
  addresses a provider object — the issue or pull-request number;
  installation-scoped deliveries (the installation families, whose
  notifications affect repository sets or the whole installation rather than
  one singular repository) carry only the stable installation ID. Relevant
  deliveries are mapped onto the normalized semantic reconciliation-target
  vocabulary (:mod:`openorc.domain.github_webhooks`) that dispatch (#120)
  dispatches from; the concrete provider event names/actions never leak
  above this boundary.
- ``ignored`` — valid but irrelevant or unsupported (anything outside the
  classified v1 families, including ``issue_comment``, which is deliberately
  not a required subscription and has no v1 reconciliation surface); safely
  acknowledged.
- ``unusable`` — a classified-family payload that is structurally unusable
  for safe routing (uninterpretable JSON, missing/malformed stable identity
  members); safely classified without inventing authority.

Sub-issue and issue-dependency notifications (``sub_issues``,
``issue_dependencies``) deliberately extract NO issue number: their related
issues may live in other repositories (the payloads carry
``parent_issue_repo`` / ``blocking_issue_repo``), so the delivery routes by
the stable repository identity and dispatch re-synchronizes the repository's
tracked issue relations authoritatively. Non-string ``action`` members are
treated as absent (the action is presentation metadata, not routing
identity). No member other than the listed stable identity paths is ever
read.
"""

from __future__ import annotations

import json
from dataclasses import dataclass

from openorc.adapters.github.capabilities import V1_CLASSIFIED_WEBHOOK_EVENTS
from openorc.domain.github_webhooks import (
    GitHubWebhookDeliveryClassification,
    GitHubWebhookRoutingTarget,
)

__all__ = ["GitHubWebhookDeliveryFacts", "classify_github_webhook_delivery"]

# The classified v1 GitHub webhook event families this boundary classifies.
_CLASSIFIED_V1_EVENT_FAMILIES = V1_CLASSIFIED_WEBHOOK_EVENTS

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


def _classify_sub_issues(
    payload: dict[str, object], action: str | None, event_name: str
) -> GitHubWebhookDeliveryFacts:
    installation_id = _stable_identity_member(payload, "installation", "id")
    repository_id = _stable_identity_member(payload, "repository", "id")
    if installation_id is None or repository_id is None:
        return _facts(event_name, action, GitHubWebhookDeliveryClassification.UNUSABLE)
    # Sub-issue membership changes route by the stable repository identity:
    # the sub-issue or its parent may live in another repository (the payload
    # carries ``parent_issue_repo``), so no issue number is ever extracted
    # and dispatch re-synchronizes the repository's tracked issue relations
    # authoritatively.
    return _facts(
        event_name,
        action,
        GitHubWebhookDeliveryClassification.RELEVANT,
        routing_target=GitHubWebhookRoutingTarget.ISSUE_RELATIONS,
        github_installation_id=installation_id,
        github_repository_id=repository_id,
    )


def _classify_issue_dependencies(
    payload: dict[str, object], action: str | None, event_name: str
) -> GitHubWebhookDeliveryFacts:
    installation_id = _stable_identity_member(payload, "installation", "id")
    repository_id = _stable_identity_member(payload, "repository", "id")
    if installation_id is None or repository_id is None:
        return _facts(event_name, action, GitHubWebhookDeliveryClassification.UNUSABLE)
    # Blocked-by dependency changes route by the stable repository identity:
    # the blocked and blocking issues may live in different repositories (the
    # payload carries ``blocking_issue_repo``), so no issue number is ever
    # extracted and dispatch re-synchronizes the repository's tracked issue
    # relations authoritatively.
    return _facts(
        event_name,
        action,
        GitHubWebhookDeliveryClassification.RELEVANT,
        routing_target=GitHubWebhookRoutingTarget.ISSUE_RELATIONS,
        github_installation_id=installation_id,
        github_repository_id=repository_id,
    )


def _classify_repository(
    payload: dict[str, object], action: str | None, event_name: str
) -> GitHubWebhookDeliveryFacts:
    installation_id = _stable_identity_member(payload, "installation", "id")
    repository_id = _stable_identity_member(payload, "repository", "id")
    if installation_id is None or repository_id is None:
        return _facts(event_name, action, GitHubWebhookDeliveryClassification.UNUSABLE)
    # Mutable repository metadata (rename, transfer, archival, ...) routes by
    # the stable repository identity; dispatch re-observes the fresh
    # authoritative repository state through the routed installation.
    return _facts(
        event_name,
        action,
        GitHubWebhookDeliveryClassification.RELEVANT,
        routing_target=GitHubWebhookRoutingTarget.REPOSITORY_METADATA,
        github_installation_id=installation_id,
        github_repository_id=repository_id,
    )


def _classify_installation_scoped(
    payload: dict[str, object], action: str | None, event_name: str
) -> GitHubWebhookDeliveryFacts:
    installation_id = _stable_identity_member(payload, "installation", "id")
    if installation_id is None:
        return _facts(event_name, action, GitHubWebhookDeliveryClassification.UNUSABLE)
    # Installation-level notifications (the installation lifecycle and the
    # repositories an installation gained/lost access to) affect repository
    # sets or the whole installation rather than one singular repository:
    # the delivery is installation-scoped (no repository identity) and
    # dispatch fans out over the installation's routed repositories. The
    # payload's added/removed repository arrays are deliberately not
    # extracted — they identify what may need reconciliation, never what
    # canonical state becomes.
    return _facts(
        event_name,
        action,
        GitHubWebhookDeliveryClassification.RELEVANT,
        routing_target=GitHubWebhookRoutingTarget.REPOSITORY_METADATA,
        github_installation_id=installation_id,
    )


_FAMILY_CLASSIFIERS = {
    "issues": _classify_issues,
    "pull_request": _classify_pull_request,
    "push": _classify_push,
    "status": _classify_checks,
    "check_run": _classify_checks,
    "check_suite": _classify_checks,
    "sub_issues": _classify_sub_issues,
    "issue_dependencies": _classify_issue_dependencies,
    "repository": _classify_repository,
    "installation": _classify_installation_scoped,
    "installation_repositories": _classify_installation_scoped,
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
    if event_name not in _CLASSIFIED_V1_EVENT_FAMILIES:
        return _facts(event_name, action, GitHubWebhookDeliveryClassification.IGNORED)
    classifier = _FAMILY_CLASSIFIERS.get(event_name)
    if classifier is None:
        # A settled family with no v1 reconciliation surface is safely
        # ignored; outside-settled families (issue_comment included) take
        # the ignored branch above.
        return _facts(event_name, action, GitHubWebhookDeliveryClassification.IGNORED)
    return classifier(payload, action, event_name)
