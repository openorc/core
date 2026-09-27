"""Normalized GitHub pull-request observation and exact-head merge-request
facts (issue #63).

The documented ``GET /repos/{owner}/{repo}/pulls/{pull_number}`` response is
normalized into the stable PR identity, the repository-local PR number, the
mutable head/base observations, and the observed lifecycle facts the
canonical TaskPullRequest model carries. The parser binds the response to
the addressed subject through the response's own documented ``number`` and
``url`` members (the same self-reference binding discipline the issue
observation uses) and fails closed: any shape that cannot be interpreted as
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

from dataclasses import dataclass
from datetime import datetime
from enum import Enum

from openorc.adapters.github.capabilities import parse_instant
from openorc.adapters.github.errors import GitHubOutcomeUncertainError

__all__ = [
    "GitHubMergeRequestOutcome",
    "GitHubMergeRequestResult",
    "GitHubPullRequestObservation",
    "parse_merge_response_payload",
    "parse_pull_request_payload",
]

_DOCUMENTED_PR_STATES = frozenset({"open", "closed"})


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
    its documented ``url`` member must name the addressed owner/repository
    (case-insensitively, matching GitHub's documented case-insensitive name
    handling). A mismatch means the answer does not bind to the addressed
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
    # The documented url member ends with /repos/{owner}/{repo}/pulls/{number};
    # anything else — including a URL without the documented marker — fails
    # the comparison and classifies as uninterpretable.
    addressed = f"{owner_login}/{repository_name}/pulls/{pull_number}".lower()
    reported_address = reported_url.rsplit("/repos/", 1)[-1].lower()
    if reported_address != addressed:
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
