"""The GitHub workflow capability contract (issue #58).

The semantic capability model for the settled v1 GitHub workflow. All
GitHub permission-name and API-version details live inside this adapter
module: higher-level workflow code asks the semantic question — "can this
installation currently perform the required OpenOrc repository operations
for this repository?" — through the typed facts and helpers here, and never
inspects provider-native permission dictionaries.

Two strictly separated validation fact sources (verified against GitHub's
documented REST surface):

1. **Repository membership/access** — the documented
   ``GET /installation/repositories`` operation under the installation access
   token. Its paginated listing proves the installation currently grants
   access to the expected repository by matching each entry's numeric stable
   ``id``. The per-entry ``permissions`` member is the ordinary repository
   access shape and is deliberately NEVER consumed as capability authority:
   the GitHub App installation permission dictionary lives only on the
   installation object.
2. **Installation permission/event/suspension facts** — the JWT-authenticated
   documented installation lookup ``GET /app/installations/{installation_id}``
   (REST API endpoints for GitHub Apps). The installation object carries the
   fine-grained ``permissions`` dictionary (semantic keys such as
   ``contents``, ``issues``, ``pull_requests``, ``checks``, ``metadata``),
   the subscribed ``events`` array, and the ``suspended_at`` suspension
   state. These are the inputs the v1 capability validator consumes.

Required v1 capabilities use least privilege — no broad "admin" assumption:

- ``contents: write`` is required because the documented exact-head PR merge
  contract (``PUT /repos/{owner}/{repo}/pulls/{pull_number}/merge``, whose
  ``sha`` parameter is the expected-head guard) requires the Contents
  repository permission at write level; ``Pull requests: write`` supplies PR
  creation/reconciliation authority but never merge authority.
- ``issues`` write covers issue state, sub-issues, dependencies, and issue
  comments; ``pull_requests`` write covers PR read/create/reconciliation;
  ``checks`` read covers check runs and suites, while commit-status reads
  require GitHub's separate ``statuses`` (Commit statuses) permission;
  ``metadata`` read is GitHub's mandatory baseline.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Any

from openorc.adapters.github.errors import GitHubOutcomeUncertainError

__all__ = [
    "GITHUB_INSTALLATION_PATH",
    "GITHUB_INSTALLATION_REPOSITORIES_PATH",
    "GITHUB_MINT_INSTALLATION_TOKEN_PATH",
    "GITHUB_REPOSITORY_ISSUE_PATH",
    "REQUIRED_V1_WEBHOOK_EVENTS",
    "REQUIRED_V1_WORKFLOW_CAPABILITIES",
    "GitHubAccessValidation",
    "GitHubInstallationCapabilities",
    "GitHubWorkflowCapability",
    "map_installation_permissions",
    "missing_required_capabilities",
    "missing_required_webhook_events",
    "parse_instant",
    "parse_installation_payload",
    "parse_installation_repositories_page",
    "require_positive_int",
]


class GitHubWorkflowCapability(Enum):
    """The semantic capability facts an OpenOrc GitHub workflow consumes."""

    ISSUE_READ = "issue_read"
    ISSUE_WRITE = "issue_write"
    REPOSITORY_READ = "repository_read"
    CONTENTS_WRITE = "contents_write"
    PULL_REQUEST_READ = "pull_request_read"
    PULL_REQUEST_WRITE = "pull_request_write"
    CHECKS_READ = "checks_read"
    COMMIT_STATUS_READ = "commit_status_read"
    METADATA_READ = "metadata_read"


# The settled v1 OpenOrc GitHub workflow capability requirement (least
# privilege): issue state/sub-issues/dependencies/comments, repository
# refs/committed state, PR read/create/reconciliation, check runs/suites,
# commit statuses, and exact-head merge authority. Exact-head merge derives
# from ``contents: write`` (the documented merge contract) plus the merge
# endpoint's exact-head ``sha`` parameter — never from the pull-request
# permission. Commit-status reads require GitHub's separate Commit statuses
# permission (exposed under ``statuses`` in the installation object), not
# the Checks permission. ``metadata: read`` is GitHub's mandatory baseline.
REQUIRED_V1_WORKFLOW_CAPABILITIES = frozenset(
    {
        GitHubWorkflowCapability.ISSUE_WRITE,
        GitHubWorkflowCapability.REPOSITORY_READ,
        GitHubWorkflowCapability.CONTENTS_WRITE,
        GitHubWorkflowCapability.PULL_REQUEST_WRITE,
        GitHubWorkflowCapability.CHECKS_READ,
        GitHubWorkflowCapability.COMMIT_STATUS_READ,
        GitHubWorkflowCapability.METADATA_READ,
    }
)

# The subscribed webhook events the settled v1 workflow relies on for
# notification-triggered reconciliation (GitHub remains authoritative;
# reconciliation never depends on deliveries alone). The set is a single,
# explicit configuration point: later Phase 2B leaves adjust it here,
# deliberately, rather than scattering event names through services.
REQUIRED_V1_WEBHOOK_EVENTS = frozenset(
    {"issues", "issue_comment", "pull_request", "push", "status", "check_run", "check_suite"}
)

# Documented GitHub REST paths owned by this adapter (issues #58 and #59).
GITHUB_MINT_INSTALLATION_TOKEN_PATH = "/app/installations/{installation_id}/access_tokens"
GITHUB_INSTALLATION_PATH = "/app/installations/{installation_id}"
GITHUB_INSTALLATION_REPOSITORIES_PATH = "/installation/repositories"
GITHUB_REPOSITORY_ISSUE_PATH = "/repos/{owner}/{repo}/issues/{issue_number}"


def require_positive_int(value: object, name: str) -> int:
    """Validate a caller-supplied stable external identifier (fail closed).

    GitHub stable identifiers are positive integers; booleans and other
    non-int values are caller-contract violations, not provider outcomes.
    """
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ValueError(f"{name} must be a positive integer")
    return value


@dataclass(frozen=True, slots=True)
class GitHubInstallationCapabilities:
    """Normalized capability facts for one GitHub App installation.

    Derived only from the JWT-authenticated installation object: the
    semantic capability set mapped from the fine-grained ``permissions``
    dictionary, the subscribed webhook ``events``, and the observed
    ``suspended_at`` state. Carries no credential material.
    """

    github_installation_id: int
    capabilities: frozenset[GitHubWorkflowCapability]
    subscribed_events: frozenset[str]
    suspended_at: datetime | None


@dataclass(frozen=True, slots=True)
class GitHubAccessValidation:
    """The normalized result of one successful installation/repository validation.

    Produced only when the routed installation currently grants access to the
    expected stable repository identity AND satisfies the required v1
    capability set: any authorization absence (suspension, capability or
    event shortfall, or absence of the stable repository identity from the
    installation's accessible set) is raised by the adapter as the
    classified authorization rejection instead.
    """

    github_installation_id: int
    github_repository_id: int
    capabilities: frozenset[GitHubWorkflowCapability]
    subscribed_events: frozenset[str]


# GitHub installation permission dictionary -> semantic capabilities.
# Values follow GitHub's fine-grained permission vocabulary ("read"/"write");
# a "write" grant implies the read-level facts. Permission keys GitHub
# reports that OpenOrc's v1 workflow does not consume are deliberately
# ignored: capabilities are never inferred beyond what the dictionary grants.
_CapabilitySet = frozenset[GitHubWorkflowCapability]
_INSTALLATION_PERMISSION_TO_CAPABILITIES: dict[tuple[str, str], _CapabilitySet] = {
    ("issues", "read"): frozenset({GitHubWorkflowCapability.ISSUE_READ}),
    ("issues", "write"): frozenset(
        {GitHubWorkflowCapability.ISSUE_READ, GitHubWorkflowCapability.ISSUE_WRITE}
    ),
    ("contents", "read"): frozenset({GitHubWorkflowCapability.REPOSITORY_READ}),
    ("contents", "write"): frozenset(
        {GitHubWorkflowCapability.REPOSITORY_READ, GitHubWorkflowCapability.CONTENTS_WRITE}
    ),
    ("pull_requests", "read"): frozenset({GitHubWorkflowCapability.PULL_REQUEST_READ}),
    ("pull_requests", "write"): frozenset(
        {
            GitHubWorkflowCapability.PULL_REQUEST_READ,
            GitHubWorkflowCapability.PULL_REQUEST_WRITE,
        }
    ),
    ("checks", "read"): frozenset({GitHubWorkflowCapability.CHECKS_READ}),
    ("checks", "write"): frozenset({GitHubWorkflowCapability.CHECKS_READ}),
    ("statuses", "read"): frozenset({GitHubWorkflowCapability.COMMIT_STATUS_READ}),
    ("statuses", "write"): frozenset({GitHubWorkflowCapability.COMMIT_STATUS_READ}),
    ("metadata", "read"): frozenset({GitHubWorkflowCapability.METADATA_READ}),
}


def map_installation_permissions(
    permissions: Mapping[str, Any],
) -> frozenset[GitHubWorkflowCapability]:
    """Map one installation object's permission dictionary to semantic capabilities.

    This mapping is the GitHub-permission-name boundary: unknown permission
    keys are ignored (forward-compatible); a value outside GitHub's
    read/write vocabulary grants nothing; a non-string key or value is an
    uninterpretable response (uncertain outcome). Exact-head merge authority
    derives only from ``contents: write``.
    """
    mapped: set[GitHubWorkflowCapability] = set()
    for key, value in permissions.items():
        if not isinstance(key, str) or not isinstance(value, str):
            raise GitHubOutcomeUncertainError(
                "the installation permission dictionary is not interpretable"
            )
        if value not in ("read", "write"):
            continue
        mapped |= _INSTALLATION_PERMISSION_TO_CAPABILITIES.get((key, value), frozenset())
    return frozenset(mapped)


def missing_required_capabilities(
    capabilities: frozenset[GitHubWorkflowCapability],
) -> frozenset[GitHubWorkflowCapability]:
    """Return the required v1 capabilities the installation does not grant."""
    return REQUIRED_V1_WORKFLOW_CAPABILITIES - capabilities


def missing_required_webhook_events(subscribed_events: frozenset[str]) -> frozenset[str]:
    """Return the required v1 webhook events the app does not subscribe to."""
    return REQUIRED_V1_WEBHOOK_EVENTS - subscribed_events


def parse_instant(value: object, description: str) -> datetime:
    """Parse a provider ISO-8601 instant; malformed shapes are uninterpretable."""
    if not isinstance(value, str):
        raise GitHubOutcomeUncertainError(
            f"the installation response is not interpretable: {description} is malformed"
        )
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise GitHubOutcomeUncertainError(
            f"the installation response is not interpretable: {description} is malformed"
        ) from exc
    if parsed.tzinfo is None:
        raise GitHubOutcomeUncertainError(
            f"the installation response is not interpretable: {description} is naive"
        )
    return parsed


def parse_installation_payload(
    payload: Mapping[str, Any], *, github_installation_id: int
) -> GitHubInstallationCapabilities:
    """Normalize a JWT-authenticated installation object into capability facts.

    The installation object (GitHub REST documentation, "Get an app
    installation") is the documented source of the fine-grained
    ``permissions`` dictionary, the subscribed ``events`` array, and the
    ``suspended_at`` observation. A response whose numeric ``id`` does not
    match the addressed installation is an uninterpretable outcome — never a
    silently accepted fact: installation facts bind to the exact stable
    identity the adapter addressed.
    """
    reported_id = payload.get("id")
    if isinstance(reported_id, bool) or not isinstance(reported_id, int) or reported_id <= 0:
        raise GitHubOutcomeUncertainError(
            "the installation response is not interpretable: the installation identity is missing"
        )
    if reported_id != github_installation_id:
        raise GitHubOutcomeUncertainError(
            "the installation response is not interpretable: it reports a different installation"
        )
    permissions = payload.get("permissions")
    if not isinstance(permissions, dict):
        raise GitHubOutcomeUncertainError(
            "the installation response is not interpretable: the permissions dictionary is missing"
        )
    events = payload.get("events")
    if not isinstance(events, list) or not all(isinstance(event, str) for event in events):
        raise GitHubOutcomeUncertainError(
            "the installation response is not interpretable: the events subscription is missing"
        )
    suspended_raw = payload.get("suspended_at")
    suspended_at = (
        None
        if suspended_raw is None
        else parse_instant(suspended_raw, "the suspension observation")
    )
    return GitHubInstallationCapabilities(
        github_installation_id=reported_id,
        capabilities=map_installation_permissions(permissions),
        subscribed_events=frozenset(events),
        suspended_at=suspended_at,
    )


def parse_installation_repositories_page(payload: Mapping[str, Any]) -> list[int]:
    """Normalize one page of the installation repository listing to stable IDs.

    Only the numeric stable ``id`` values are consumed: the listing proves
    repository membership by stable identity. The per-entry ``permissions``
    member is the ordinary repository access shape — NOT the GitHub App
    installation permission dictionary — and is deliberately ignored so it
    can never be mistaken for capability authority.
    """
    repositories = payload.get("repositories")
    if not isinstance(repositories, list):
        raise GitHubOutcomeUncertainError(
            "the installation repository listing is not interpretable: "
            "the repositories array is missing"
        )
    repository_ids: list[int] = []
    for entry in repositories:
        if not isinstance(entry, dict):
            raise GitHubOutcomeUncertainError(
                "the installation repository listing is not interpretable: "
                "a repository entry is malformed"
            )
        repository_id = entry.get("id")
        if (
            isinstance(repository_id, bool)
            or not isinstance(repository_id, int)
            or repository_id <= 0
        ):
            raise GitHubOutcomeUncertainError(
                "the installation repository listing is not interpretable: "
                "a repository identity is malformed"
            )
        repository_ids.append(repository_id)
    return repository_ids
