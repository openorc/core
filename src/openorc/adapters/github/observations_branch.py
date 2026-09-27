"""Normalized GitHub branch observation for canonical branch verification
(issue #63).

The documented ``GET /repos/{owner}/{repo}/branches/{branch}`` response is
normalized into the stable branch/ref identity and the exact committed head
SHA the canonical-branch verification boundary needs. The parser binds the
response to the addressed subject — its documented ``name`` member must
equal the addressed branch name exactly, because git branch names are
case-sensitive — and fails closed: any shape that cannot be interpreted as
the documented fact set classifies as an uncertain outcome, never silently
accepted partial truth.

A 404 answer (the addressed branch does not exist, or access to the
addressed repository is absent) is the transport's classified authorization
absence: the normalized missing-branch/access integration condition, never
permission to trust runtime-local Git state. Error messages never contain
provider URLs, response bodies, or credential material.
"""

from __future__ import annotations

from dataclasses import dataclass

from openorc.adapters.github.errors import GitHubOutcomeUncertainError

__all__ = [
    "GitHubBranchObservation",
    "parse_branch_payload",
]


@dataclass(frozen=True, slots=True)
class GitHubBranchObservation:
    """Normalized current observation of one repository branch.

    ``branch_name`` is the branch/ref identity, bound to the exact name the
    adapter addressed; ``head_sha`` is the exact GitHub-committed head SHA —
    the fact that identifies the branch's committed code state. Runtime-local
    and worktree HEAD never establish this fact: only the authoritative
    GitHub answer does.
    """

    branch_name: str
    head_sha: str


def parse_branch_payload(payload: object, *, branch_name: str) -> GitHubBranchObservation:
    """Normalize one documented branch response, bound to the addressed name.

    The response binds to the exact subject the adapter addressed: its
    documented ``name`` member must equal the addressed branch name exactly.
    A mismatch means the answer does not bind to the addressed branch and
    classifies as an uninterpretable outcome rather than a silently accepted
    fact.
    """
    if not isinstance(payload, dict):
        raise GitHubOutcomeUncertainError(
            "the GitHub response is not interpretable: the object shape is unexpected"
        )
    reported_name = payload.get("name")
    if not isinstance(reported_name, str) or not reported_name.strip():
        raise GitHubOutcomeUncertainError(
            "the GitHub response is not interpretable: the branch name is malformed"
        )
    if reported_name != branch_name:
        raise GitHubOutcomeUncertainError(
            "the GitHub response is not interpretable: it reports a different branch"
        )
    commit = payload.get("commit")
    if not isinstance(commit, dict):
        raise GitHubOutcomeUncertainError(
            "the GitHub response is not interpretable: the branch commit is missing"
        )
    head_sha = commit.get("sha")
    if not isinstance(head_sha, str) or not head_sha.strip():
        raise GitHubOutcomeUncertainError(
            "the GitHub response is not interpretable: the branch head commit SHA is malformed"
        )
    return GitHubBranchObservation(branch_name=reported_name, head_sha=head_sha)
