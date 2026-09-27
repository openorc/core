"""Normalized GitHub pull-request observation and exact-head merge-request
facts (issue #63).

The documented ``GET /repos/{owner}/{repo}/pulls/{pull_number}`` response is
normalized into the stable PR identity, the repository-local PR number, the
mutable head/base observations, and the observed lifecycle facts the
canonical TaskPullRequest model carries. The parser binds the response to
the addressed subject through the response's own documented ``number``
member and its GitHub-returned ``url`` self-reference validated as the
addressed pull request's documented opaque API address (the same
self-reference binding discipline the issue observation uses; issue #122:
the URL is never decomposed into owner/repo/pull-number parts) and fails
closed: any shape that cannot be interpreted as
the documented fact set classifies as an uncertain outcome, never silently
accepted partial truth. The documented merge coherence is enforced at the
parse boundary — a merged PR is a closed PR and carries its merge instant —
mirroring the durable model's CHECK.

The normalized exact-head merge-request result carries the documented
response classes of ``PUT /repos/{owner}/{repo}/pulls/{pull_number}/merge``:
the successful merge (with the merge commit SHA GitHub reports), the
documented 409 expected-head mismatch — the merge endpoint's own ``sha``
guard refusing a stale head — and any other definitive policy/state
rejection GitHub owns (required checks, merge conflicts, branch
protection). Authentication/access absence, rate limits, and uncertain
outcomes raise the adapter's classified errors instead. Error messages
never contain provider URLs, response bodies, or credential material.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from enum import Enum

from openorc.adapters.github.capabilities import parse_instant
from openorc.adapters.github.errors import GitHubOutcomeUncertainError
from openorc.adapters.github.transport import github_repository_api_address, is_github_api_origin

__all__ = [
    "GitHubMergeRequestOutcome",
    "GitHubMergeRequestResult",
    "GitHubPullRequestFacts",
    "GitHubPullRequestObservation",
    "body_reports_pull_request_already_exists",
    "parse_merge_response_payload",
    "parse_pull_request_facts",
    "parse_pull_request_payload",
]

_DOCUMENTED_PR_STATES = frozenset({"open", "closed"})

# The stable documented 422 error members GitHub uses to report the specific
# 'pull request already exists' validation failure of the create-pull-request
# operation (GitHub REST create-pull-request documentation: a 422 carries a
# message plus an errors array whose members identify the failing field).
# Matched case-insensitively over a bounded body prefix; no provider content
# is ever stored, echoed into errors, or exported (the same content-safe
# bounded-classification discipline as the transport's secondary-limit
# marker check).
_ALREADY_EXISTS_BODY_MARKERS = ("a pull request already exists",)
_ALREADY_EXISTS_ERROR_FIELD = "base"
_ALREADY_EXISTS_MESSAGE_MARKER = "validation failed"
_ALREADY_EXISTS_BOUND_BYTES = 4096


def body_reports_pull_request_already_exists(body: bytes) -> bool:
    """Whether a bounded 422 body carries the documented already-exists failure.

    The create-pull-request endpoint documents 422 for ANY validation
    failure (invalid base/head, malformed title, endpoint abuse), so the
    bare status alone must never classify as the duplicate condition. The
    body is decoded over a bounded prefix and matched only against the
    documented stable markers: GitHub's explicit 'a pull request already
    exists' validation message, or the documented validation-failed shape
    whose errors array names the ``base`` field (the duplicate-PR
    rejection's documented failing field). Returns a boolean; no provider
    content ever crosses this boundary.
    """
    if not body:
        return False
    text = body[:_ALREADY_EXISTS_BOUND_BYTES].decode("utf-8", "ignore").lower()
    if any(marker in text for marker in _ALREADY_EXISTS_BODY_MARKERS):
        return True
    try:
        payload = json.loads(text) if text.lstrip().startswith("{") else None
    except ValueError:
        return False
    if not isinstance(payload, dict):
        return False
    message = payload.get("message")
    if isinstance(message, str) and _ALREADY_EXISTS_MESSAGE_MARKER in message.lower():
        errors = payload.get("errors")
        if isinstance(errors, list) and any(
            isinstance(entry, dict) and entry.get("field") == _ALREADY_EXISTS_ERROR_FIELD
            for entry in errors
        ):
            return True
    return False


@dataclass(frozen=True, slots=True)
class GitHubPullRequestObservation:
    """Normalized current observation of one repository pull request.

    ``github_pr_id`` is the stable external GitHub PR identity used for
    reconciliation; ``pull_number`` is the repository-local address, never
    identity. The mutable observed reconciliation facts — head ref/SHA, base
    ref, the open/closed state, and the merge observation — change in place
    while the identity stays stable. ``merged_at`` is present only for a
    merged (closed) PR, mirroring the durable model's CHECK.
    """

    github_pr_id: int
    pull_number: int
    head_ref: str
    head_sha: str
    base_ref: str
    state: str
    merged: bool
    merged_at: datetime | None


def parse_pull_request_payload(
    payload: object, *, owner_login: str, repository_name: str, pull_number: int
) -> GitHubPullRequestObservation:
    """Normalize one documented pull-request response, bound to the subject.

    The response binds to the exact subject the adapter addressed: its
    documented ``number`` member must equal the addressed pull number and
    its documented ``url`` member must be the addressed pull request's
    documented opaque API address on the exact trusted GitHub API origin
    (case-insensitively, matching GitHub's documented case-insensitive name
    handling; never decomposed into owner/repo/pull-number parts —
    issue #122). A mismatch means the answer does not bind to the addressed
    subject and classifies as an uninterpretable outcome rather than a
    silently accepted fact.
    """
    if not isinstance(payload, dict):
        raise GitHubOutcomeUncertainError(
            "the GitHub response is not interpretable: the object shape is unexpected"
        )
    reported_id = payload.get("id")
    if isinstance(reported_id, bool) or not isinstance(reported_id, int) or reported_id <= 0:
        raise GitHubOutcomeUncertainError(
            "the GitHub response is not interpretable: the pull request identity is malformed"
        )
    reported_number = payload.get("number")
    if (
        isinstance(reported_number, bool)
        or not isinstance(reported_number, int)
        or reported_number <= 0
    ):
        raise GitHubOutcomeUncertainError(
            "the GitHub response is not interpretable: the pull request number is malformed"
        )
    if reported_number != pull_number:
        raise GitHubOutcomeUncertainError(
            "the GitHub response is not interpretable: it reports a different pull request number"
        )
    reported_url = payload.get("url")
    if not isinstance(reported_url, str) or not reported_url.strip():
        raise GitHubOutcomeUncertainError(
            "the GitHub response is not interpretable: the pull request reference is malformed"
        )
    # The GitHub-returned self-reference is an opaque navigation fact
    # (issue #122): it must target the exact trusted HTTPS GitHub API origin
    # and equal the addressed pull request's documented API address in full
    # (case-insensitively). The URL is never decomposed into
    # owner/repo/pull-number parts.
    if not is_github_api_origin(reported_url):
        raise GitHubOutcomeUncertainError(
            "the GitHub response is not interpretable: the pull request reference "
            "does not target the exact trusted GitHub API origin"
        )
    addressed = (
        github_repository_api_address(owner_login=owner_login, repository_name=repository_name)
        + f"/pulls/{pull_number}"
    ).lower()
    if reported_url.lower() != addressed:
        raise GitHubOutcomeUncertainError(
            "the GitHub response is not interpretable: it does not bind to the addressed repository"
        )
    head = payload.get("head")
    if not isinstance(head, dict):
        raise GitHubOutcomeUncertainError(
            "the GitHub response is not interpretable: the pull request head is missing"
        )
    head_ref = head.get("ref")
    if not isinstance(head_ref, str) or not head_ref.strip():
        raise GitHubOutcomeUncertainError(
            "the GitHub response is not interpretable: the pull request head ref is malformed"
        )
    head_sha = head.get("sha")
    if not isinstance(head_sha, str) or not head_sha.strip():
        raise GitHubOutcomeUncertainError(
            "the GitHub response is not interpretable: the pull request head SHA is malformed"
        )
    base = payload.get("base")
    if not isinstance(base, dict):
        raise GitHubOutcomeUncertainError(
            "the GitHub response is not interpretable: the pull request base is missing"
        )
    base_ref = base.get("ref")
    if not isinstance(base_ref, str) or not base_ref.strip():
        raise GitHubOutcomeUncertainError(
            "the GitHub response is not interpretable: the pull request base ref is malformed"
        )
    state = payload.get("state")
    if not isinstance(state, str) or state not in _DOCUMENTED_PR_STATES:
        raise GitHubOutcomeUncertainError(
            "the GitHub response is not interpretable: the pull request state is unexpected"
        )
    merged = payload.get("merged")
    if not isinstance(merged, bool):
        raise GitHubOutcomeUncertainError(
            "the GitHub response is not interpretable: the merge observation is malformed"
        )
    merged_at_raw = payload.get("merged_at")
    merged_at = (
        None
        if merged_at_raw is None
        else parse_instant(merged_at_raw, "the pull request merge instant")
    )
    # The documented merge coherence, mirrored by the durable model's CHECK:
    # a merged PR is a closed PR carrying its merge instant; an unmerged PR
    # carries none.
    if merged and (state != "closed" or merged_at is None):
        raise GitHubOutcomeUncertainError(
            "the GitHub response is not interpretable: a merged pull request is closed "
            "and carries its merge instant"
        )
    if not merged and merged_at is not None:
        raise GitHubOutcomeUncertainError(
            "the GitHub response is not interpretable: an unmerged pull request carries "
            "no merge instant"
        )
    return GitHubPullRequestObservation(
        github_pr_id=reported_id,
        pull_number=reported_number,
        head_ref=head_ref,
        head_sha=head_sha,
        base_ref=base_ref,
        state=state,
        merged=merged,
        merged_at=merged_at,
    )


@dataclass(frozen=True, slots=True)
class GitHubPullRequestFacts:
    """The stable identity and mutable facts one PR response reports.

    The identity/fact extraction shared by the PR read and the PR-create
    response: the create response carries the same documented fact set but
    cannot bind through an addressed ``number``/``url`` (the number is the
    response's own output, not a caller-supplied address), so the shared
    extraction validates only what is provider-authoritative in both
    responses: the stable ``id``, a positive ``number``, the head/base
    facts, and the documented state/merge coherence. A shape that cannot
    be interpreted classifies as an uncertain outcome.
    """

    github_pr_id: int
    pull_number: int
    head_ref: str
    head_sha: str
    base_ref: str
    state: str
    merged: bool
    merged_at: datetime | None


def _extract_required_string(payload: dict[str, object], member: str, description: str) -> str:
    value = payload.get(member)
    if not isinstance(value, str) or not value.strip():
        raise GitHubOutcomeUncertainError(
            f"the GitHub response is not interpretable: the {description} is malformed"
        )
    return value


def parse_pull_request_facts(payload: object) -> GitHubPullRequestFacts:
    """Extract the documented PR fact set from one PR response payload."""
    if not isinstance(payload, dict):
        raise GitHubOutcomeUncertainError(
            "the GitHub response is not interpretable: the object shape is unexpected"
        )
    reported_id = payload.get("id")
    if isinstance(reported_id, bool) or not isinstance(reported_id, int) or reported_id <= 0:
        raise GitHubOutcomeUncertainError(
            "the GitHub response is not interpretable: the pull request identity is malformed"
        )
    reported_number = payload.get("number")
    if (
        isinstance(reported_number, bool)
        or not isinstance(reported_number, int)
        or reported_number <= 0
    ):
        raise GitHubOutcomeUncertainError(
            "the GitHub response is not interpretable: the pull request number is malformed"
        )
    head = payload.get("head")
    if not isinstance(head, dict):
        raise GitHubOutcomeUncertainError(
            "the GitHub response is not interpretable: the pull request head is missing"
        )
    head_ref = _extract_required_string(head, "ref", "pull request head ref")
    head_sha = _extract_required_string(head, "sha", "pull request head SHA")
    base = payload.get("base")
    if not isinstance(base, dict):
        raise GitHubOutcomeUncertainError(
            "the GitHub response is not interpretable: the pull request base is missing"
        )
    base_ref = _extract_required_string(base, "ref", "pull request base ref")
    state = payload.get("state")
    if not isinstance(state, str) or state not in _DOCUMENTED_PR_STATES:
        raise GitHubOutcomeUncertainError(
            "the GitHub response is not interpretable: the pull request state is unexpected"
        )
    merged = payload.get("merged")
    if not isinstance(merged, bool):
        raise GitHubOutcomeUncertainError(
            "the GitHub response is not interpretable: the merge observation is malformed"
        )
    merged_at_raw = payload.get("merged_at")
    merged_at = (
        None
        if merged_at_raw is None
        else parse_instant(merged_at_raw, "the pull request merge instant")
    )
    # The documented merge coherence, mirrored by the durable model's CHECK:
    # a merged PR is a closed PR carrying its merge instant; an unmerged PR
    # carries none.
    if merged and (state != "closed" or merged_at is None):
        raise GitHubOutcomeUncertainError(
            "the GitHub response is not interpretable: a merged pull request is closed "
            "and carries its merge instant"
        )
    if not merged and merged_at is not None:
        raise GitHubOutcomeUncertainError(
            "the GitHub response is not interpretable: an unmerged pull request carries "
            "no merge instant"
        )
    return GitHubPullRequestFacts(
        github_pr_id=reported_id,
        pull_number=reported_number,
        head_ref=head_ref,
        head_sha=head_sha,
        base_ref=base_ref,
        state=state,
        merged=merged,
        merged_at=merged_at,
    )


class GitHubMergeRequestOutcome(Enum):
    """The documented response classes of one exact-head merge request.

    - ``MERGED`` — the documented successful merge; GitHub reports the merge
      commit SHA and immediate authoritative reconciliation confirms it.
    - ``HEAD_MISMATCH`` — the merge endpoint's documented 409 expected-head
      answer: the ``sha`` guard refused because the pull request head is no
      longer the expected head. A concurrent head change can never satisfy a
      stale merge request.
    - ``REJECTED`` — any other definitive policy/state rejection GitHub owns
      (required checks, merge conflicts, branch protection): GitHub owns the
      policy, OpenOrc only reports the known rejection.
    """

    MERGED = "merged"
    HEAD_MISMATCH = "head_mismatch"
    REJECTED = "rejected"


@dataclass(frozen=True, slots=True)
class GitHubMergeRequestResult:
    """Normalized result of one exact-head merge request.

    ``merge_commit_sha`` is present only for the documented successful
    merge; the known rejections carry none because GitHub applied nothing.
    Authentication/access absence, rate limits, and uncertain outcomes raise
    the adapter's classified errors instead of a result.
    """

    outcome: GitHubMergeRequestOutcome
    merge_commit_sha: str | None


def parse_merge_response_payload(payload: object) -> GitHubMergeRequestResult:
    """Normalize the documented 200 merge response (known success).

    The documented successful merge reports ``merged: true`` and the merge
    commit SHA. Any other 2xx shape is uninterpretable — an uncertain
    outcome, never a silently accepted success.
    """
    if not isinstance(payload, dict):
        raise GitHubOutcomeUncertainError(
            "the GitHub response is not interpretable: the object shape is unexpected"
        )
    merged = payload.get("merged")
    if not isinstance(merged, bool) or not merged:
        raise GitHubOutcomeUncertainError(
            "the GitHub response is not interpretable: the merge response does not "
            "report a successful merge"
        )
    merge_commit_sha = payload.get("sha")
    if not isinstance(merge_commit_sha, str) or not merge_commit_sha.strip():
        raise GitHubOutcomeUncertainError(
            "the GitHub response is not interpretable: the merge commit SHA is malformed"
        )
    return GitHubMergeRequestResult(
        outcome=GitHubMergeRequestOutcome.MERGED, merge_commit_sha=merge_commit_sha
    )
