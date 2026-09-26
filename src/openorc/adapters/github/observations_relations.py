"""GitHub adapter observations for issue relationships (issue #60).

Adapter-local normalized results for the relationship reads B4 adds to the
GitHub boundary. All of them fail closed: any shape that cannot be
interpreted as the documented fact set classifies as an uncertain outcome,
never as silently accepted partial truth, and a failed/ambiguous GitHub
answer is never reinterpreted as authoritative relationship state.

- **Blocked-by dependency listing** — parsed from the documented
  ``GET /repos/{owner}/{repo}/issues/{issue_number}/dependencies/blocked_by``
  array of documented issue objects. Each edge carries the related issue's
  stable numeric ``id`` plus its documented ``repository_url``; the related
  repository's numeric stable ID is resolved by the application service (REST
  repository-metadata reads, deduplicated), never guessed from mutable URL
  text.
- **Sub-issue listing** — parsed from the documented
  ``GET /repos/{owner}/{repo}/issues/{issue_number}/sub_issues`` array with
  the same related-issue fact shape.
- **GraphQL parent observation** — the documented nullable ``Issue.parent``
  field (GitHub GraphQL schema) is the one authoritative surface that can
  express BOTH parent presence and parent absence: the REST parent endpoint
  documents only 200/301/404/410 with no distinct successful no-parent
  response, so a REST 404 is an authorization-shaped error — never an
  authoritative ``NO_PARENT`` fact. ``parent`` present ⇒ the parent's stable
  endpoint facts; ``parent`` null ⇒ the authoritative no-parent outcome.
  A transport/permission/malformed outcome stays a classified error and
  leaves any durable mirror untouched.

Error messages never contain provider URLs, response bodies, or credential
material.
"""

from __future__ import annotations

from dataclasses import dataclass

from openorc.adapters.github.errors import GitHubOutcomeUncertainError

__all__ = [
    "GitHubIssueParentObservation",
    "GitHubRelatedIssueEndpoint",
    "GitHubRelatedIssueObservation",
    "parse_graphql_issue_parent",
    "parse_related_issue_payload",
    "parse_related_issue_payloads",
]


def _require_positive_int(value: object, description: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise GitHubOutcomeUncertainError(
            f"the GitHub response is not interpretable: {description} is malformed"
        )
    return value


def _require_non_empty_str(value: object, description: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise GitHubOutcomeUncertainError(
            f"the GitHub response is not interpretable: {description} is malformed"
        )
    return value


@dataclass(frozen=True, slots=True)
class GitHubRelatedIssueObservation:
    """One related issue as GitHub documents it in relationship listings.

    ``github_issue_id`` is the stable numeric issue identity;
    ``repository_url`` is GitHub's documented repository reference of the
    related issue, carried verbatim as a RESOLUTION INPUT ONLY fact: the
    application service uses it — within the same fresh observation unit —
    to resolve the related repository's stable numeric ID. It is never
    persisted as identity and never treated as authoritative identity by
    itself.
    """

    github_issue_id: int
    repository_url: str


def parse_related_issue_payload(payload: object) -> GitHubRelatedIssueObservation:
    """Normalize one documented related-issue object from a relationship listing.

    Any shape that cannot be interpreted as the documented fact set — a
    missing/malformed stable issue ``id``, or a missing/malformed
    ``repository_url`` — classifies as an uninterpretable outcome rather
    than a silently accepted partial fact.
    """
    if not isinstance(payload, dict):
        raise GitHubOutcomeUncertainError(
            "the GitHub response is not interpretable: the object shape is unexpected"
        )
    return GitHubRelatedIssueObservation(
        github_issue_id=_require_positive_int(payload.get("id"), "the related issue identity"),
        repository_url=_require_non_empty_str(
            payload.get("repository_url"), "the related issue repository reference"
        ),
    )


def parse_related_issue_payloads(payload: object) -> list[GitHubRelatedIssueObservation]:
    """Normalize a documented relationship-listing array.

    The documented blocked-by and sub-issue endpoints answer a JSON array of
    issue objects; any non-array, or any element that fails related-issue
    parsing, classifies as an uninterpretable outcome. The full observation
    is all-or-nothing: a partially interpretable listing is never silently
    truncated into accepted truth.
    """
    if not isinstance(payload, list):
        raise GitHubOutcomeUncertainError(
            "the GitHub response is not interpretable: the listing shape is unexpected"
        )
    return [parse_related_issue_payload(entry) for entry in payload]


@dataclass(frozen=True, slots=True)
class GitHubRelatedIssueEndpoint:
    """The stable numeric GitHub identity pair of one related issue.

    Used where the authoritative answer exposes BOTH stable numeric
    identifiers directly (the GraphQL parent fact set, whose documented
    ``databaseId`` fields supply the parent issue and parent repository
    identities without any resolution step).
    """

    github_repository_id: int
    github_issue_id: int


@dataclass(frozen=True, slots=True)
class GitHubIssueParentObservation:
    """The authoritative parent fact of one issue from the GraphQL surface.

    ``parent`` is ``None`` exactly when GitHub's documented nullable
    ``Issue.parent`` field is null — the authoritative no-parent fact — and
    carries the parent's stable numeric endpoint otherwise. There is no
    third meaning: an uninterpretable or failed GraphQL answer raises the
    classified error instead of manufacturing a parent fact.
    """

    parent: GitHubRelatedIssueEndpoint | None


def parse_graphql_issue_parent(payload: object) -> GitHubIssueParentObservation:
    """Normalize the documented nullable ``Issue.parent`` GraphQL answer.

    The expected shape is ``{"data": {"repository": {"issue": {"parent":
    <issue object or null>}}}}`` where the parent object exposes the stable
    numeric identifiers through the documented fields (``databaseId`` on the
    parent issue and on its ``repository``). A ``null`` parent inside a
    well-formed answer is the authoritative no-parent fact. Errors in the
    GraphQL answer, or any other uninterpretable shape, raise the classified
    uncertain outcome — never a parent fact.
    """
    if not isinstance(payload, dict):
        raise GitHubOutcomeUncertainError(
            "the GitHub response is not interpretable: the object shape is unexpected"
        )
    data = payload.get("data")
    if not isinstance(data, dict):
        raise GitHubOutcomeUncertainError(
            "the GitHub response is not interpretable: the GraphQL data member is missing"
        )
    if isinstance(data.get("errors"), list) and data["errors"]:
        raise GitHubOutcomeUncertainError(
            "the GitHub response is not interpretable: the GraphQL answer reports errors"
        )
    repository = data.get("repository")
    if not isinstance(repository, dict):
        raise GitHubOutcomeUncertainError(
            "the GitHub response is not interpretable: the repository member is missing"
        )
    issue = repository.get("issue")
    if not isinstance(issue, dict):
        raise GitHubOutcomeUncertainError(
            "the GitHub response is not interpretable: the issue member is missing"
        )
    parent = issue.get("parent")
    if parent is None:
        # The documented nullable field: the authoritative no-parent fact.
        return GitHubIssueParentObservation(parent=None)
    if not isinstance(parent, dict):
        raise GitHubOutcomeUncertainError(
            "the GitHub response is not interpretable: the parent member is malformed"
        )
    parent_issue_id = _require_positive_int(parent.get("databaseId"), "the parent issue identity")
    parent_repository = parent.get("repository")
    if not isinstance(parent_repository, dict):
        raise GitHubOutcomeUncertainError(
            "the GitHub response is not interpretable: the parent repository member is missing"
        )
    parent_repository_id = _require_positive_int(
        parent_repository.get("databaseId"), "the parent repository identity"
    )
    return GitHubIssueParentObservation(
        parent=GitHubRelatedIssueEndpoint(
            # The GraphQL parent answer carries the parent repository's stable
            # numeric identity directly through the documented databaseId
            # fields: no URL-based resolution step exists for this fact set.
            github_repository_id=parent_repository_id,
            github_issue_id=parent_issue_id,
        )
    )
