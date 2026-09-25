"""Normalized GitHub observation facts for authoritative reconciliation
(issue #59).

Adapter-local normalized results for the two documented GitHub reads the
reconciliation boundary performs:

1. **Repository observation** — parsed from the documented
   ``GET /installation/repositories`` listing entry that matches the exact
   stable repository ID: the same documented operation that proves
   membership (issue #58) also carries the authoritative repository object.
   There is no documented REST operation to fetch a repository by stable
   ID, and addressing by stored owner/name breaks exactly when a rename or
   ownership transfer must be reconciled, so the stable-ID listing entry is
   both the access proof and the observation source.
2. **Issue observation** — parsed from the documented
   ``GET /repos/{owner}/{repo}/issues/{issue_number}`` response, bound to
   the addressed subject by the response's own ``number`` and
   ``repository_url`` members.

Both parsers fail closed: any shape that cannot be interpreted as the
documented fact set classifies as an uncertain outcome, never as silently
accepted partial truth. The documented ``pull_request`` member is GitHub's
own discriminator marking the addressed number as a pull request viewed
through the issues endpoint; the parser records it as a typed fact and
application services decide the workflow consequence. Error messages never
contain provider URLs, response bodies, or credential material.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from openorc.adapters.github.capabilities import parse_instant
from openorc.adapters.github.errors import GitHubOutcomeUncertainError

__all__ = [
    "GitHubIssueObservation",
    "GitHubRepositoryObservation",
    "parse_installation_repository_entry",
    "parse_issue_payload",
]

_DOCUMENTED_ISSUE_STATES = frozenset({"open", "closed"})


def _require_non_empty_str(value: object, description: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise GitHubOutcomeUncertainError(
            f"the GitHub response is not interpretable: {description} is malformed"
        )
    return value


def _require_positive_int(value: object, description: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise GitHubOutcomeUncertainError(
            f"the GitHub response is not interpretable: {description} is malformed"
        )
    return value


@dataclass(frozen=True, slots=True)
class GitHubRepositoryObservation:
    """Normalized current observation of one installation repository.

    Parsed from the documented installation-repositories listing entry
    matched by the exact stable repository ID. All fields except the stable
    ID are mutable observed metadata (owner login, name, URL, visibility,
    default branch); none of them is ever identity. ``default_branch`` is
    ``None`` for an empty repository that has no branch yet.
    """

    github_repository_id: int
    owner_login: str
    name: str
    html_url: str
    is_private: bool
    default_branch: str | None


def parse_installation_repository_entry(
    entry: object, *, github_repository_id: int
) -> GitHubRepositoryObservation:
    """Normalize one installation-repositories listing entry.

    The entry binds to the exact stable repository identity the adapter
    addressed: an entry reporting a different numeric ``id`` is an
    uninterpretable outcome — never a silently accepted fact. Any malformed
    documented field classifies as an uncertain outcome because the
    repository observation cannot be trusted as a fact.
    """
    if not isinstance(entry, dict):
        raise GitHubOutcomeUncertainError(
            "the installation repository listing is not interpretable: "
            "the repository entry is malformed"
        )
    reported_id = _require_positive_int(entry.get("id"), "the repository identity")
    if reported_id != github_repository_id:
        raise GitHubOutcomeUncertainError(
            "the installation repository listing is not interpretable: "
            "it reports a different repository"
        )
    owner = entry.get("owner")
    if not isinstance(owner, dict):
        raise GitHubOutcomeUncertainError(
            "the installation repository listing is not interpretable: "
            "the repository owner is malformed"
        )
    owner_login = _require_non_empty_str(owner.get("login"), "the repository owner login")
    name = _require_non_empty_str(entry.get("name"), "the repository name")
    html_url = _require_non_empty_str(entry.get("html_url"), "the repository URL")
    is_private = entry.get("private")
    if not isinstance(is_private, bool):
        raise GitHubOutcomeUncertainError(
            "the installation repository listing is not interpretable: "
            "the repository visibility is malformed"
        )
    default_branch_raw = entry.get("default_branch")
    if default_branch_raw is None:
        default_branch: str | None = None
    elif isinstance(default_branch_raw, str) and default_branch_raw.strip():
        default_branch = default_branch_raw
    else:
        raise GitHubOutcomeUncertainError(
            "the installation repository listing is not interpretable: "
            "the default branch is malformed"
        )
    return GitHubRepositoryObservation(
        github_repository_id=reported_id,
        owner_login=owner_login,
        name=name,
        html_url=html_url,
        is_private=is_private,
        default_branch=default_branch,
    )


@dataclass(frozen=True, slots=True)
class GitHubIssueObservation:
    """Normalized current observation of one GitHub issue.

    Parsed from the documented ``GET /repos/{owner}/{repo}/issues/{number}``
    response. ``github_issue_id`` is the stable identity; ``issue_number``
    is the repository-local address. ``body`` is GitHub's documented
    nullable body verbatim. ``is_pull_request`` is GitHub's documented
    ``pull_request`` member — the response's own discriminator that the
    addressed number is a pull request viewed through the issues endpoint —
    carried as a typed fact; application services decide the consequence.
    """

    github_issue_id: int
    issue_number: int
    title: str
    body: str | None
    state: str
    provider_updated_at: datetime
    is_pull_request: bool


def parse_issue_payload(
    payload: object, *, owner_login: str, repository_name: str, issue_number: int
) -> GitHubIssueObservation:
    """Normalize one documented issue response, bound to the addressed subject.

    The response binds to the exact subject the adapter addressed: its
    ``number`` must equal the addressed number and its ``repository_url``
    must name the addressed owner/repository (case-insensitively, matching
    GitHub's documented case-insensitive name handling). A mismatch means
    the answer does not bind to the addressed subject — for example a
    transfer racing the two reconciliation reads — and classifies as an
    uninterpretable outcome rather than a silently accepted fact.
    """
    if not isinstance(payload, dict):
        raise GitHubOutcomeUncertainError(
            "the GitHub response is not interpretable: the object shape is unexpected"
        )
    reported_id = _require_positive_int(payload.get("id"), "the issue identity")
    reported_number = _require_positive_int(payload.get("number"), "the issue number")
    if reported_number != issue_number:
        raise GitHubOutcomeUncertainError(
            "the GitHub response is not interpretable: it reports a different issue number"
        )
    title = _require_non_empty_str(payload.get("title"), "the issue title")
    body_raw = payload.get("body")
    if body_raw is not None and not isinstance(body_raw, str):
        raise GitHubOutcomeUncertainError(
            "the GitHub response is not interpretable: the issue body is malformed"
        )
    state = _require_non_empty_str(payload.get("state"), "the issue state")
    if state not in _DOCUMENTED_ISSUE_STATES:
        raise GitHubOutcomeUncertainError(
            "the GitHub response is not interpretable: the issue state is unexpected"
        )
    provider_updated_at = parse_instant(payload.get("updated_at"), "the issue update instant")
    repository_url = _require_non_empty_str(
        payload.get("repository_url"), "the issue repository reference"
    )
    addressed = f"{owner_login}/{repository_name}".lower()
    # The documented repository_url ends with /repos/{owner}/{repo}; anything
    # else — including a URL without the documented marker — fails the
    # comparison and classifies as uninterpretable.
    reported_address = repository_url.rsplit("/repos/", 1)[-1]
    if reported_address.lower() != addressed:
        raise GitHubOutcomeUncertainError(
            "the GitHub response is not interpretable: it does not bind to the addressed repository"
        )
    is_pull_request = payload.get("pull_request") is not None
    return GitHubIssueObservation(
        github_issue_id=reported_id,
        issue_number=reported_number,
        title=title,
        body=body_raw,
        state=state,
        provider_updated_at=provider_updated_at,
        is_pull_request=is_pull_request,
    )
