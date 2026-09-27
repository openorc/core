"""Normalized GitHub checks/commit-status projection for one exact head
(issue #63).

Two documented reads of GitHub's CI/status surfaces, both addressed by the
exact head SHA — never a branch name — and both exhaustively paginated
under the bounded ``Link``-header discipline the installation-repositories
listing established:

1. ``GET /repos/{owner}/{repo}/commits/{ref}/check-runs`` — check runs for
   the reference (``checks`` read); an object page carrying the documented
   ``total_count`` and the ``check_runs`` array.
2. ``GET /repos/{owner}/{repo}/commits/{ref}/status`` — the combined commit
   status: GitHub's provider-owned aggregate ``state`` plus the paginated
   ``statuses`` array (the combined-status listing is itself paginated with
   the documented ``per_page``/``page`` parameters).

Both parsers fail closed: any shape that cannot be interpreted as the
documented fact set classifies as an uncertain outcome, never silently
accepted partial truth. Completeness is proven, never assumed: the combined
status response binds its documented ``sha`` member to the addressed exact
head, and the paginated walks cross-check the collected entries against the
documented ``total_count`` — a bound overrun, a broken pagination chain, or
a count mismatch is an uncertain outcome rather than an accepted partial
read.

The projection is a typed normalized read model of GitHub-owned facts. It
never becomes an OpenOrc merge-policy engine, never synthesizes a universal
pass/fail gate that would disagree with GitHub branch protection, and is
not persisted by this leaf. Error messages never contain provider URLs,
response bodies, or credential material.
"""

from __future__ import annotations

from dataclasses import dataclass

from openorc.adapters.github.errors import GitHubOutcomeUncertainError

__all__ = [
    "GitHubCheckRunObservation",
    "GitHubCommitStatusesProjection",
    "GitHubStatusContextObservation",
    "parse_check_runs_page",
    "parse_combined_status_page",
]

_DOCUMENTED_CHECK_RUN_STATUSES = frozenset({"queued", "in_progress", "completed"})
_DOCUMENTED_COMMIT_STATUS_STATES = frozenset({"error", "failure", "pending", "success"})


@dataclass(frozen=True, slots=True)
class GitHubCheckRunObservation:
    """Normalized current observation of one check run on the exact head.

    ``status`` is GitHub's documented check-run lifecycle value;
    ``conclusion`` is the documented completion conclusion, present only for
    a completed run. The facts are GitHub-owned and display-only: OpenOrc
    derives no workflow meaning or merge policy from them.
    """

    name: str
    status: str
    conclusion: str | None


@dataclass(frozen=True, slots=True)
class GitHubStatusContextObservation:
    """Normalized current observation of one commit-status context."""

    context: str
    state: str


@dataclass(frozen=True, slots=True)
class GitHubCommitStatusesProjection:
    """The normalized combined-status read model for one exact head.

    ``state`` is GitHub's provider-owned combined aggregate state; the
    ``statuses`` tuple is the exhaustively paginated per-context listing
    (completeness proven against the documented ``total_count`` inside the
    adapter). The aggregate deliberately stays GitHub's own answer: it is
    presented, never reinterpreted as an OpenOrc gate.
    """

    state: str
    statuses: tuple[GitHubStatusContextObservation, ...]


def parse_check_runs_page(payload: object) -> tuple[int, list[GitHubCheckRunObservation]]:
    """Normalize one documented check-runs page: ``(total_count, check_runs)``.

    ``total_count`` is the documented page-level total the pagination walk
    proves completeness against. A malformed entry is an uninterpretable
    outcome — never a silently skipped fact.
    """
    if not isinstance(payload, dict):
        raise GitHubOutcomeUncertainError(
            "the GitHub response is not interpretable: the object shape is unexpected"
        )
    total_count = payload.get("total_count")
    if isinstance(total_count, bool) or not isinstance(total_count, int) or total_count < 0:
        raise GitHubOutcomeUncertainError(
            "the GitHub response is not interpretable: the check runs total is malformed"
        )
    check_runs = payload.get("check_runs")
    if not isinstance(check_runs, list):
        raise GitHubOutcomeUncertainError(
            "the GitHub response is not interpretable: the check runs listing is missing"
        )
    observations: list[GitHubCheckRunObservation] = []
    for entry in check_runs:
        if not isinstance(entry, dict):
            raise GitHubOutcomeUncertainError(
                "the GitHub response is not interpretable: a check run entry is malformed"
            )
        name = entry.get("name")
        if not isinstance(name, str) or not name.strip():
            raise GitHubOutcomeUncertainError(
                "the GitHub response is not interpretable: a check run name is malformed"
            )
        status = entry.get("status")
        if not isinstance(status, str) or status not in _DOCUMENTED_CHECK_RUN_STATUSES:
            raise GitHubOutcomeUncertainError(
                "the GitHub response is not interpretable: a check run status is unexpected"
            )
        conclusion = entry.get("conclusion")
        if conclusion is not None and (not isinstance(conclusion, str) or not conclusion.strip()):
            raise GitHubOutcomeUncertainError(
                "the GitHub response is not interpretable: a check run conclusion is malformed"
            )
        observations.append(
            GitHubCheckRunObservation(name=name, status=status, conclusion=conclusion)
        )
    return total_count, observations


def parse_combined_status_page(
    payload: object, *, head_sha: str
) -> tuple[str, int, list[GitHubStatusContextObservation]]:
    """Normalize one documented combined-status page, bound to the addressed head.

    Returns the provider-owned aggregate ``state``, the documented
    ``total_count`` the pagination walk proves completeness against, and the
    page's status contexts. The response's documented ``sha`` member must
    equal the exact head SHA the adapter addressed; anything else does not
    bind to the addressed subject and classifies as uninterpretable.
    """
    if not isinstance(payload, dict):
        raise GitHubOutcomeUncertainError(
            "the GitHub response is not interpretable: the object shape is unexpected"
        )
    state = payload.get("state")
    if not isinstance(state, str) or state not in _DOCUMENTED_COMMIT_STATUS_STATES:
        raise GitHubOutcomeUncertainError(
            "the GitHub response is not interpretable: the combined status state is unexpected"
        )
    reported_sha = payload.get("sha")
    if not isinstance(reported_sha, str) or reported_sha != head_sha:
        raise GitHubOutcomeUncertainError(
            "the GitHub response is not interpretable: it does not bind to the addressed head"
        )
    total_count = payload.get("total_count")
    if isinstance(total_count, bool) or not isinstance(total_count, int) or total_count < 0:
        raise GitHubOutcomeUncertainError(
            "the GitHub response is not interpretable: the combined status total is malformed"
        )
    statuses = payload.get("statuses")
    if not isinstance(statuses, list):
        raise GitHubOutcomeUncertainError(
            "the GitHub response is not interpretable: the status contexts listing is missing"
        )
    observations: list[GitHubStatusContextObservation] = []
    for entry in statuses:
        if not isinstance(entry, dict):
            raise GitHubOutcomeUncertainError(
                "the GitHub response is not interpretable: a status context entry is malformed"
            )
        context = entry.get("context")
        if not isinstance(context, str) or not context.strip():
            raise GitHubOutcomeUncertainError(
                "the GitHub response is not interpretable: a status context is malformed"
            )
        context_state = entry.get("state")
        if (
            not isinstance(context_state, str)
            or context_state not in _DOCUMENTED_COMMIT_STATUS_STATES
        ):
            raise GitHubOutcomeUncertainError(
                "the GitHub response is not interpretable: a status context state is unexpected"
            )
        observations.append(GitHubStatusContextObservation(context=context, state=context_state))
    return state, total_count, observations
