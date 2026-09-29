"""Deterministic tests for the B7 GitHub client operations (issue #63).

The concrete ``HttpGitHubAppClient`` operations B7 adds —
``get_repository_branch`` (the documented branch read carrying the exact
committed head SHA), ``get_repository_pull_request`` (the documented PR read
bound to the addressed subject), ``get_commit_check_runs`` /
``get_commit_combined_status`` (the exhaustively paginated, completeness-
proven CI/status projections for one exact head), and ``merge_pull_request``
(the documented exact-head merge with GitHub's ``sha`` guard) — exercised
through the injectable fetch seam with a real JWT authenticator. No live
GitHub access.

Proven here: exact documented paths (including the percent-encoded branch
segment), response binding failures as uncertain outcomes, exhaustive
bounded pagination with ``total_count`` completeness proofs for both
projection surfaces, the documented merge response classes (success, 409
head mismatch, other definitive rejection), and auth/access/rate-limit/5xx
classifications — an uncertain merge outcome is never a result.

Credential mode (issue #143): the Owner-accountable write operations
(``create_pull_request``, ``merge_pull_request``) authenticate with the
exact Profile-bound user-to-server credential — no installation token is
ever minted for them — and a definitive 401 under the user token propagates
for the service-side bounded recovery instead of any adapter-side retry.
"""

from __future__ import annotations

import json
import uuid
from collections.abc import Mapping
from datetime import UTC, datetime
from typing import Any

import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa

from openorc.adapters.github.authentication import GitHubAppAuthenticator
from openorc.adapters.github.client import HttpGitHubAppClient
from openorc.adapters.github.errors import (
    GitHubAuthenticationRejectedError,
    GitHubAuthorizationRejectedError,
    GitHubOutcomeUncertainError,
    GitHubRateLimitedError,
)
from openorc.adapters.github.observations_branch import GitHubBranchObservation
from openorc.adapters.github.observations_checks import (
    GitHubCommitStatusesProjection,
    parse_check_runs_page,
    parse_combined_status_page,
)
from openorc.adapters.github.observations_pull_request import (
    GitHubMergeRequestOutcome,
    GitHubPullRequestFacts,
    GitHubPullRequestObservation,
    parse_merge_response_payload,
    parse_pull_request_payload,
)
from openorc.adapters.github.transport import GITHUB_API_BASE_URL, HttpGitHubRestClient
from openorc.adapters.github.user_tokens import (
    GitHubProfileUserAccessToken,
    GitHubUserAccessToken,
)

_NOW = 1_790_000_000.0
_REPOSITORY_ID = 987654321
_PULL_NUMBER = 77
_HEAD_SHA = "0123456789abcdef0123456789abcdef01234567"
_OTHER_SHA = "fedcba9876543210fedcba9876543210fedcba98"


class FakeClock:
    def __init__(self, start: float = _NOW) -> None:
        self.now = start

    def __call__(self) -> float:
        return self.now


class FakeFetcher:
    """Scripted transport seam recording calls; serves queued results."""

    def __init__(self, results: list[tuple[int, Mapping[str, str], bytes] | Exception]) -> None:
        self.results = list(results)
        self.calls: list[tuple[str, str, dict[str, str], float, bytes | None]] = []

    def __call__(
        self,
        url: str,
        method: str,
        headers: Mapping[str, str],
        timeout_seconds: float,
        body: bytes | None,
    ) -> tuple[int, Mapping[str, str], bytes]:
        self.calls.append((url, method, dict(headers), timeout_seconds, body))
        result = self.results.pop(0)
        if isinstance(result, Exception):
            raise result
        return result


def _key_pem() -> str:
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    return key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    ).decode()


def _json(
    status: int, headers: Mapping[str, str], payload: Any
) -> tuple[int, Mapping[str, str], bytes]:
    return status, dict(headers), json.dumps(payload).encode()


def _mint_response(token: str = "ghs_pull_request_token") -> tuple[int, Mapping[str, str], bytes]:
    expires_at = datetime.fromtimestamp(_NOW + 3000, tz=UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
    return _json(201, {}, {"token": token, "expires_at": expires_at})


def _client(fetch: FakeFetcher) -> HttpGitHubAppClient:
    authenticator = GitHubAppAuthenticator(
        app_id=12345, private_key_pem=_key_pem(), clock=FakeClock(), fetch=fetch
    )
    transport = HttpGitHubRestClient(fetch=fetch)
    return HttpGitHubAppClient(authenticator=authenticator, transport=transport)


_USER_TOKEN_VALUE = "ghu_owner_user_token"


def _user_credential() -> GitHubProfileUserAccessToken:
    """One accountable Profile's resolved user credential for the write operations."""
    return GitHubProfileUserAccessToken(
        profile_id=uuid.UUID(int=42),
        access_token=GitHubUserAccessToken(
            value=_USER_TOKEN_VALUE, expires_at=datetime.fromtimestamp(_NOW + 3600, tz=UTC)
        ),
    )


def _assert_owner_write_auth(fetch: FakeFetcher, write_call_index: int = 0) -> None:
    """The write presented the Profile-bound user token and never minted one."""
    assert (
        fetch.calls[write_call_index][2]["Authorization"] == f"Bearer {_USER_TOKEN_VALUE}"
    )
    assert not any("access_tokens" in call[0] for call in fetch.calls)


def _branch_payload(name: str = "openorc/task-42", sha: str = _HEAD_SHA) -> dict[str, Any]:
    return {"name": name, "commit": {"sha": sha}, "protected": False}


def _pr_payload(
    *,
    pr_id: int = 900_719_925_474_099,
    number: int = _PULL_NUMBER,
    head_ref: str = "openorc/task-42",
    head_sha: str = _HEAD_SHA,
    base_ref: str = "main",
    state: str = "open",
    merged: bool = False,
    merged_at: str | None = None,
    url: str | None = None,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "id": pr_id,
        "number": number,
        "url": url or f"{GITHUB_API_BASE_URL}/repos/octocat/hello-world/pulls/{number}",
        "head": {"ref": head_ref, "sha": head_sha},
        "base": {"ref": base_ref, "sha": _OTHER_SHA},
        "state": state,
        "merged": merged,
        "merged_at": merged_at,
    }
    return payload


def _check_run(
    name: str, *, status: str = "completed", conclusion: str | None = "success"
) -> dict[str, Any]:
    return {"id": 4, "name": name, "status": status, "conclusion": conclusion}


def _check_runs_payload(runs: list[dict[str, Any]]) -> dict[str, Any]:
    return {"total_count": len(runs), "check_runs": runs}


def _status_entry(context: str, state: str) -> dict[str, Any]:
    return {"id": 1, "context": context, "state": state, "target_url": None}


def _combined_status_payload(
    *,
    state: str = "success",
    total_count: int | None = None,
    statuses: list[dict[str, Any]] | None = None,
    sha: str = _HEAD_SHA,
) -> dict[str, Any]:
    entries = statuses if statuses is not None else []
    return {
        "state": state,
        "sha": sha,
        "total_count": len(entries) if total_count is None else total_count,
        "statuses": entries,
    }


def test_branch_observation_carries_the_exact_committed_head_sha() -> None:
    fetch = FakeFetcher([_mint_response(), _json(200, {}, _branch_payload())])

    observation = _client(fetch).get_repository_branch(
        github_installation_id=4242,
        owner_login="octocat",
        repository_name="hello-world",
        branch_name="openorc/task-42",
    )

    assert isinstance(observation, GitHubBranchObservation)
    assert observation.branch_name == "openorc/task-42"
    # The exact committed SHA comes from GitHub, not from the caller or any
    # runtime-local source.
    assert observation.head_sha == _HEAD_SHA
    assert fetch.calls[0][0].endswith("/app/installations/4242/access_tokens")
    assert (
        fetch.calls[1][0]
        == f"{GITHUB_API_BASE_URL}/repos/octocat/hello-world/branches/openorc%2Ftask-42"
    )
    assert fetch.calls[1][1] == "GET"
    assert fetch.calls[1][2]["X-GitHub-Api-Version"] == "2026-03-10"


def test_branch_names_with_slashes_are_one_encoded_path_segment() -> None:
    fetch = FakeFetcher([_mint_response(), _json(200, {}, _branch_payload(name="feature/task 42"))])

    _client(fetch).get_repository_branch(
        github_installation_id=4242,
        owner_login="octocat",
        repository_name="hello-world",
        branch_name="feature/task 42",
    )

    assert fetch.calls[1][0].endswith("/branches/feature%2Ftask%2042")


def test_a_missing_branch_is_the_classified_access_condition() -> None:
    fetch = FakeFetcher([_mint_response(), _json(404, {}, {"message": "Not Found"})])

    with pytest.raises(GitHubAuthorizationRejectedError):
        _client(fetch).get_repository_branch(
            github_installation_id=4242,
            owner_login="octocat",
            repository_name="hello-world",
            branch_name="deleted-branch",
        )
    assert len(fetch.calls) == 2


def test_a_branch_answer_for_a_different_branch_does_not_bind() -> None:
    fetch = FakeFetcher([_mint_response(), _json(200, {}, _branch_payload(name="other-branch"))])

    with pytest.raises(GitHubOutcomeUncertainError):
        _client(fetch).get_repository_branch(
            github_installation_id=4242,
            owner_login="octocat",
            repository_name="hello-world",
            branch_name="openorc/task-42",
        )


def test_a_branch_answer_without_an_interpretable_commit_is_uncertain() -> None:
    fetch = FakeFetcher([_mint_response(), _json(200, {}, _branch_payload(sha=""))])

    with pytest.raises(GitHubOutcomeUncertainError):
        _client(fetch).get_repository_branch(
            github_installation_id=4242,
            owner_login="octocat",
            repository_name="hello-world",
            branch_name="openorc/task-42",
        )


def test_pull_request_observation_normalizes_the_documented_facts() -> None:
    fetch = FakeFetcher([_mint_response(), _json(200, {}, _pr_payload())])

    observation = _client(fetch).get_repository_pull_request(
        github_installation_id=4242,
        owner_login="octocat",
        repository_name="hello-world",
        pull_number=_PULL_NUMBER,
    )

    assert isinstance(observation, GitHubPullRequestObservation)
    assert observation.github_pr_id == 900_719_925_474_099
    assert observation.pull_number == _PULL_NUMBER
    assert observation.head_ref == "openorc/task-42"
    assert observation.head_sha == _HEAD_SHA
    assert observation.base_ref == "main"
    assert observation.state == "open"
    assert observation.merged is False
    assert observation.merged_at is None
    assert fetch.calls[1][0] == f"{GITHUB_API_BASE_URL}/repos/octocat/hello-world/pulls/77"


def test_a_merged_pull_request_observation_carries_its_instant() -> None:
    fetch = FakeFetcher(
        [
            _mint_response(),
            _json(
                200,
                {},
                _pr_payload(state="closed", merged=True, merged_at="2026-09-26T12:00:00Z"),
            ),
        ]
    )

    observation = _client(fetch).get_repository_pull_request(
        github_installation_id=4242,
        owner_login="octocat",
        repository_name="hello-world",
        pull_number=_PULL_NUMBER,
    )

    assert observation.state == "closed"
    assert observation.merged is True
    assert observation.merged_at == datetime(2026, 9, 26, 12, 0, 0, tzinfo=UTC)


def test_a_pull_request_answer_for_a_different_number_does_not_bind() -> None:
    fetch = FakeFetcher([_mint_response(), _json(200, {}, _pr_payload(number=78))])

    with pytest.raises(GitHubOutcomeUncertainError):
        _client(fetch).get_repository_pull_request(
            github_installation_id=4242,
            owner_login="octocat",
            repository_name="hello-world",
            pull_number=_PULL_NUMBER,
        )


def test_a_pull_request_answer_for_a_different_repository_does_not_bind() -> None:
    fetch = FakeFetcher(
        [
            _mint_response(),
            _json(
                200,
                {},
                _pr_payload(url=f"{GITHUB_API_BASE_URL}/repos/other/repo/pulls/77"),
            ),
        ]
    )

    with pytest.raises(GitHubOutcomeUncertainError):
        _client(fetch).get_repository_pull_request(
            github_installation_id=4242,
            owner_login="octocat",
            repository_name="hello-world",
            pull_number=_PULL_NUMBER,
        )


def test_a_pull_request_answer_with_a_malformed_head_is_uncertain() -> None:
    fetch = FakeFetcher([_mint_response(), _json(200, {}, _pr_payload(head_sha=""))])

    with pytest.raises(GitHubOutcomeUncertainError):
        _client(fetch).get_repository_pull_request(
            github_installation_id=4242,
            owner_login="octocat",
            repository_name="hello-world",
            pull_number=_PULL_NUMBER,
        )


def test_merge_coherence_violations_are_uninterpretable_observations() -> None:
    # A merged PR is a closed PR carrying its instant.
    incoherent = _pr_payload(state="open", merged=True, merged_at="2026-09-26T12:00:00Z")
    fetch = FakeFetcher([_mint_response(), _json(200, {}, incoherent)])
    with pytest.raises(GitHubOutcomeUncertainError):
        _client(fetch).get_repository_pull_request(
            github_installation_id=4242,
            owner_login="octocat",
            repository_name="hello-world",
            pull_number=_PULL_NUMBER,
        )
    # An unmerged PR carries no merge instant.
    phantom_instant = _pr_payload(merged=False, merged_at="2026-09-26T12:00:00Z")
    fetch = FakeFetcher([_mint_response(), _json(200, {}, phantom_instant)])
    with pytest.raises(GitHubOutcomeUncertainError):
        _client(fetch).get_repository_pull_request(
            github_installation_id=4242,
            owner_login="octocat",
            repository_name="hello-world",
            pull_number=_PULL_NUMBER,
        )


def test_check_runs_projection_walks_the_documented_link_pagination() -> None:
    page_two = (
        f"{GITHUB_API_BASE_URL}/repos/octocat/hello-world/commits/{_HEAD_SHA}"
        "/check-runs?per_page=100&page=2"
    )
    fetch = FakeFetcher(
        [
            _mint_response(),
            _json(
                200,
                {"Link": f'<{page_two}>; rel="next"'},
                _check_runs_payload([_check_run("ci_a")]) | {"total_count": 2},
            ),
            _json(
                200,
                {},
                _check_runs_payload([_check_run("ci_b", conclusion="failure")])
                | {"total_count": 2},
            ),
        ]
    )

    runs = _client(fetch).get_commit_check_runs(
        github_installation_id=4242,
        owner_login="octocat",
        repository_name="hello-world",
        head_sha=_HEAD_SHA,
    )

    assert [(run.name, run.status, run.conclusion) for run in runs] == [
        ("ci_a", "completed", "success"),
        ("ci_b", "completed", "failure"),
    ]
    assert fetch.calls[1][0].endswith(f"/commits/{_HEAD_SHA}/check-runs?per_page=100")
    assert fetch.calls[2][0] == page_two


def test_check_runs_projection_pending_observations_are_carried() -> None:
    fetch = FakeFetcher(
        [
            _mint_response(),
            _json(
                200,
                {},
                _check_runs_payload([_check_run("ci", status="in_progress", conclusion=None)]),
            ),
        ]
    )

    runs = _client(fetch).get_commit_check_runs(
        github_installation_id=4242,
        owner_login="octocat",
        repository_name="hello-world",
        head_sha=_HEAD_SHA,
    )

    assert runs[0].status == "in_progress"
    assert runs[0].conclusion is None


def test_an_incomplete_check_runs_listing_is_uncertain_never_partial() -> None:
    # The documented total is larger than the collected pages: a silent
    # partial projection must never be returned.
    fetch = FakeFetcher(
        [
            _mint_response(),
            _json(
                200,
                {},
                _check_runs_payload([_check_run("ci_a")]) | {"total_count": 3},
            ),
        ]
    )

    with pytest.raises(GitHubOutcomeUncertainError):
        _client(fetch).get_commit_check_runs(
            github_installation_id=4242,
            owner_login="octocat",
            repository_name="hello-world",
            head_sha=_HEAD_SHA,
        )


def test_check_runs_pages_reporting_different_totals_are_uninterpretable() -> None:
    page_two = (
        f"{GITHUB_API_BASE_URL}/repos/octocat/hello-world/commits/{_HEAD_SHA}"
        "/check-runs?per_page=100&page=2"
    )
    fetch = FakeFetcher(
        [
            _mint_response(),
            _json(
                200,
                {"Link": f'<{page_two}>; rel="next"'},
                _check_runs_payload([_check_run("ci_a")]),
            ),
            _json(200, {}, _check_runs_payload([_check_run("ci_b")]) | {"total_count": 5}),
        ]
    )

    with pytest.raises(GitHubOutcomeUncertainError):
        _client(fetch).get_commit_check_runs(
            github_installation_id=4242,
            owner_login="octocat",
            repository_name="hello-world",
            head_sha=_HEAD_SHA,
        )


def test_combined_status_projection_collects_every_context() -> None:
    page_two = (
        f"{GITHUB_API_BASE_URL}/repos/octocat/hello-world/commits/{_HEAD_SHA}"
        "/status?per_page=100&page=2"
    )
    fetch = FakeFetcher(
        [
            _mint_response(),
            _json(
                200,
                {"Link": f'<{page_two}>; rel="next"'},
                _combined_status_payload(
                    state="pending",
                    total_count=2,
                    statuses=[_status_entry("ci/a", "pending")],
                ),
            ),
            _json(
                200,
                {},
                _combined_status_payload(
                    state="pending",
                    total_count=2,
                    statuses=[_status_entry("ci/b", "success")],
                ),
            ),
        ]
    )

    projection = _client(fetch).get_commit_combined_status(
        github_installation_id=4242,
        owner_login="octocat",
        repository_name="hello-world",
        head_sha=_HEAD_SHA,
    )

    assert isinstance(projection, GitHubCommitStatusesProjection)
    # The aggregate state is GitHub's own answer, returned together with the
    # proven-complete per-context listing.
    assert projection.state == "pending"
    assert [(context.context, context.state) for context in projection.statuses] == [
        ("ci/a", "pending"),
        ("ci/b", "success"),
    ]
    assert fetch.calls[1][0].endswith(f"/commits/{_HEAD_SHA}/status?per_page=100")
    assert fetch.calls[2][0] == page_two


def test_a_combined_status_answer_for_a_different_head_does_not_bind() -> None:
    fetch = FakeFetcher(
        [_mint_response(), _json(200, {}, _combined_status_payload(sha=_OTHER_SHA))]
    )

    with pytest.raises(GitHubOutcomeUncertainError):
        _client(fetch).get_commit_combined_status(
            github_installation_id=4242,
            owner_login="octocat",
            repository_name="hello-world",
            head_sha=_HEAD_SHA,
        )


def test_an_incomplete_combined_status_listing_is_uncertain_never_partial() -> None:
    # GitHub paginates the combined-status array (per_page/page): a listing
    # whose collected contexts do not reach the documented total must never
    # be returned as a complete projection.
    fetch = FakeFetcher(
        [
            _mint_response(),
            _json(
                200,
                {},
                _combined_status_payload(
                    state="failure",
                    total_count=4,
                    statuses=[_status_entry("ci/a", "failure")],
                ),
            ),
        ]
    )

    with pytest.raises(GitHubOutcomeUncertainError):
        _client(fetch).get_commit_combined_status(
            github_installation_id=4242,
            owner_login="octocat",
            repository_name="hello-world",
            head_sha=_HEAD_SHA,
        )


def test_combined_status_pages_reporting_different_aggregates_are_uninterpretable() -> None:
    page_two = (
        f"{GITHUB_API_BASE_URL}/repos/octocat/hello-world/commits/{_HEAD_SHA}"
        "/status?per_page=100&page=2"
    )
    fetch = FakeFetcher(
        [
            _mint_response(),
            _json(
                200,
                {"Link": f'<{page_two}>; rel="next"'},
                _combined_status_payload(
                    state="pending", statuses=[_status_entry("ci/a", "pending")]
                ),
            ),
            _json(
                200,
                {},
                _combined_status_payload(
                    state="success", statuses=[_status_entry("ci/b", "success")]
                ),
            ),
        ]
    )

    with pytest.raises(GitHubOutcomeUncertainError):
        _client(fetch).get_commit_combined_status(
            github_installation_id=4242,
            owner_login="octocat",
            repository_name="hello-world",
            head_sha=_HEAD_SHA,
        )


def test_a_pagination_walk_beyond_the_bounded_page_count_is_uncertain(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("openorc.adapters.github.client._MAX_LISTING_PAGES", 2)
    pages = [
        _json(
            200,
            {
                "Link": (
                    f"<{GITHUB_API_BASE_URL}/repos/octocat/hello-world/commits/"
                    f'{_HEAD_SHA}/status?per_page=100&page={page + 1}>; rel="next"'
                )
            },
            _combined_status_payload(
                state="pending", statuses=[_status_entry(f"ci/{page}", "pending")]
            ),
        )
        for page in range(3)
    ]
    fetch = FakeFetcher([_mint_response(), *pages])

    with pytest.raises(GitHubOutcomeUncertainError):
        _client(fetch).get_commit_combined_status(
            github_installation_id=4242,
            owner_login="octocat",
            repository_name="hello-world",
            head_sha=_HEAD_SHA,
        )


def test_merge_request_success_reports_the_merge_commit_sha() -> None:
    fetch = FakeFetcher(
        [
            _json(
                200,
                {},
                {"sha": _OTHER_SHA, "merged": True, "message": "Pull Request successfully merged"},
            ),
        ]
    )

    result = _client(fetch).merge_pull_request(
        credential=_user_credential(),
        github_installation_id=4242,
        owner_login="octocat",
        repository_name="hello-world",
        pull_number=_PULL_NUMBER,
        expected_head_sha=_HEAD_SHA,
    )

    assert result.outcome is GitHubMergeRequestOutcome.MERGED
    assert result.merge_commit_sha == _OTHER_SHA
    # The documented merge operation: PUT with the sha expected-head guard.
    assert fetch.calls[0][0] == f"{GITHUB_API_BASE_URL}/repos/octocat/hello-world/pulls/77/merge"
    assert fetch.calls[0][1] == "PUT"
    assert json.loads(fetch.calls[0][4] or b"{}") == {"sha": _HEAD_SHA}
    _assert_owner_write_auth(fetch)


def test_merge_request_expected_head_mismatch_is_the_documented_409_outcome() -> None:
    fetch = FakeFetcher(
        [_json(409, {}, {"message": "Head branch was modified or is invalid"})]
    )

    result = _client(fetch).merge_pull_request(
        credential=_user_credential(),
        github_installation_id=4242,
        owner_login="octocat",
        repository_name="hello-world",
        pull_number=_PULL_NUMBER,
        expected_head_sha=_HEAD_SHA,
    )

    assert result.outcome is GitHubMergeRequestOutcome.HEAD_MISMATCH
    assert result.merge_commit_sha is None


def test_merge_request_other_definitive_rejections_are_github_owned_policy() -> None:
    # 405 (not mergeable) and 422 (validation) are documented definitive
    # rejections GitHub owns; the merge applied nothing in either case.
    for status in (405, 422):
        fetch = FakeFetcher([_json(status, {}, {"message": "rejected"})])

        result = _client(fetch).merge_pull_request(
            credential=_user_credential(),
            github_installation_id=4242,
            owner_login="octocat",
            repository_name="hello-world",
            pull_number=_PULL_NUMBER,
            expected_head_sha=_HEAD_SHA,
        )

        assert result.outcome is GitHubMergeRequestOutcome.REJECTED
        assert result.merge_commit_sha is None


def test_merge_request_access_rejections_are_classified() -> None:
    fetch = FakeFetcher([_json(403, {}, {"message": "Forbidden"})])
    with pytest.raises(GitHubAuthorizationRejectedError):
        _client(fetch).merge_pull_request(
            credential=_user_credential(),
            github_installation_id=4242,
            owner_login="octocat",
            repository_name="hello-world",
            pull_number=_PULL_NUMBER,
            expected_head_sha=_HEAD_SHA,
        )


def test_a_definitive_user_token_401_propagates_without_any_adapter_retry() -> None:
    # The adapter never resolves, stores, or refreshes a user token: a
    # definitive 401-style rejection under the Profile-bound credential
    # propagates for the service's single bounded recovery pass (issue
    # #142/#143). Exactly one transport call — no adapter-side eviction or
    # replay — for both Owner-accountable write operations.
    fetch = FakeFetcher([_json(401, {}, {"message": "Bad credentials"})])
    with pytest.raises(GitHubAuthenticationRejectedError):
        _client(fetch).merge_pull_request(
            credential=_user_credential(),
            github_installation_id=4242,
            owner_login="octocat",
            repository_name="hello-world",
            pull_number=_PULL_NUMBER,
            expected_head_sha=_HEAD_SHA,
        )
    assert len(fetch.calls) == 1

    fetch = FakeFetcher([_json(401, {}, {"message": "Bad credentials"})])
    with pytest.raises(GitHubAuthenticationRejectedError):
        _client(fetch).create_pull_request(
            credential=_user_credential(),
            github_installation_id=4242,
            owner_login="octocat",
            repository_name="hello-world",
            head_ref="openorc/task-42",
            base_ref="main",
            title="feat: task 42",
            body=None,
        )
    assert len(fetch.calls) == 1


def test_merge_request_rate_limits_and_uncertain_outcomes_are_classified() -> None:
    fetch = FakeFetcher([_json(429, {"Retry-After": "60"}, {"message": "abuse"})])
    with pytest.raises(GitHubRateLimitedError):
        _client(fetch).merge_pull_request(
            credential=_user_credential(),
            github_installation_id=4242,
            owner_login="octocat",
            repository_name="hello-world",
            pull_number=_PULL_NUMBER,
            expected_head_sha=_HEAD_SHA,
        )
    fetch = FakeFetcher([_json(500, {}, {"message": "boom"})])
    with pytest.raises(GitHubOutcomeUncertainError):
        _client(fetch).merge_pull_request(
            credential=_user_credential(),
            github_installation_id=4242,
            owner_login="octocat",
            repository_name="hello-world",
            pull_number=_PULL_NUMBER,
            expected_head_sha=_HEAD_SHA,
        )
    # A connection loss is an uncertain outcome, never a non-delivery.
    fetch = FakeFetcher([TimeoutError("connection lost")])
    with pytest.raises(GitHubOutcomeUncertainError):
        _client(fetch).merge_pull_request(
            credential=_user_credential(),
            github_installation_id=4242,
            owner_login="octocat",
            repository_name="hello-world",
            pull_number=_PULL_NUMBER,
            expected_head_sha=_HEAD_SHA,
        )


def test_a_merge_response_that_does_not_report_a_successful_merge_is_uncertain() -> None:
    fetch = FakeFetcher([_json(200, {}, {"merged": False, "sha": None})])

    with pytest.raises(GitHubOutcomeUncertainError):
        _client(fetch).merge_pull_request(
            credential=_user_credential(),
            github_installation_id=4242,
            owner_login="octocat",
            repository_name="hello-world",
            pull_number=_PULL_NUMBER,
            expected_head_sha=_HEAD_SHA,
        )


def test_parser_surface_rejects_uninterpretable_shapes() -> None:
    with pytest.raises(GitHubOutcomeUncertainError):
        parse_check_runs_page({"total_count": 1, "check_runs": [{"name": "x", "status": "bogus"}]})
    with pytest.raises(GitHubOutcomeUncertainError):
        parse_combined_status_page(
            {"state": "bogus", "sha": _HEAD_SHA, "total_count": 0, "statuses": []},
            head_sha=_HEAD_SHA,
        )
    with pytest.raises(GitHubOutcomeUncertainError):
        parse_pull_request_payload({"id": 1}, owner_login="o", repository_name="r", pull_number=1)
    with pytest.raises(GitHubOutcomeUncertainError):
        parse_merge_response_payload({"merged": True})


def test_create_pull_request_uses_the_documented_collection_operation() -> None:
    fetch = FakeFetcher([_json(201, {}, _pr_payload())])

    facts = _client(fetch).create_pull_request(
        credential=_user_credential(),
        github_installation_id=4242,
        owner_login="octocat",
        repository_name="hello-world",
        head_ref="openorc/task-42",
        base_ref="main",
        title="feat: task 42",
        body="Closes #42",
    )

    assert isinstance(facts, GitHubPullRequestFacts)
    assert facts.github_pr_id == 900_719_925_474_099
    assert facts.pull_number == _PULL_NUMBER
    assert facts.head_sha == _HEAD_SHA
    assert fetch.calls[0][0] == f"{GITHUB_API_BASE_URL}/repos/octocat/hello-world/pulls"
    assert fetch.calls[0][1] == "POST"
    _assert_owner_write_auth(fetch)
    sent = json.loads(fetch.calls[0][4] or b"{}")
    assert sent == {
        "head": "openorc/task-42",
        "base": "main",
        "title": "feat: task 42",
        "body": "Closes #42",
    }


def test_create_pull_request_omits_an_absent_body() -> None:
    fetch = FakeFetcher([_json(201, {}, _pr_payload())])

    _client(fetch).create_pull_request(
        credential=_user_credential(),
        github_installation_id=4242,
        owner_login="octocat",
        repository_name="hello-world",
        head_ref="openorc/task-42",
        base_ref="main",
        title="feat: task 42",
        body=None,
    )

    sent = json.loads(fetch.calls[0][4] or b"{}")
    assert "body" not in sent
    _assert_owner_write_auth(fetch)


def test_create_pull_request_already_exists_is_a_typed_classified_rejection() -> None:
    from openorc.adapters.github import GitHubPullRequestExistsError

    # The documented duplicate-PR 422 carries the validation-failed shape
    # whose errors entry names the base field with the documented
    # already_exists code — the structured form that itself proves
    # duplication.
    fetch = FakeFetcher(
        [
            _json(
                422,
                {},
                {
                    "message": "Validation Failed",
                    "errors": [
                        {
                            "resource": "PullRequest",
                            "field": "base",
                            "code": "already_exists",
                        }
                    ],
                },
            ),
        ]
    )

    with pytest.raises(GitHubPullRequestExistsError) as exc_info:
        _client(fetch).create_pull_request(
            credential=_user_credential(),
            github_installation_id=4242,
            owner_login="octocat",
            repository_name="hello-world",
            head_ref="openorc/task-42",
            base_ref="main",
            title="feat: task 42",
            body=None,
        )

    # Only the bare status is carried; provider content never leaks.
    assert exc_info.value.status_code == 422


def test_create_pull_request_already_exists_message_form_is_classified() -> None:
    from openorc.adapters.github import GitHubPullRequestExistsError

    # The documented explicit 'a pull request already exists' message form.
    fetch = FakeFetcher(
        [
            _json(
                422,
                {},
                {"message": "A pull request already exists for octocat/hello-world."},
            ),
        ]
    )

    with pytest.raises(GitHubPullRequestExistsError):
        _client(fetch).create_pull_request(
            credential=_user_credential(),
            github_installation_id=4242,
            owner_login="octocat",
            repository_name="hello-world",
            head_ref="openorc/task-42",
            base_ref="main",
            title="feat: task 42",
            body=None,
        )


def test_create_pull_request_unrelated_422_is_not_an_already_exists_conflict() -> None:
    from openorc.adapters.github import (
        GitHubPullRequestExistsError,
        GitHubRequestRejectedError,
    )

    # A generic validation failure (e.g. an invalid head) is the endpoint's
    # general 422: an ordinary definitive rejection, NEVER the duplicate-PR
    # classification — a non-duplicate validation refusal must never become
    # the workflow's existing-PR conflict.
    fetch = FakeFetcher([_json(422, {}, {"message": "Validation Failed"})])

    with pytest.raises(GitHubRequestRejectedError) as exc_info:
        _client(fetch).create_pull_request(
            credential=_user_credential(),
            github_installation_id=4242,
            owner_login="octocat",
            repository_name="hello-world",
            head_ref="openorc/task-42",
            base_ref="main",
            title="feat: task 42",
            body=None,
        )

    assert not isinstance(exc_info.value, GitHubPullRequestExistsError)
    assert exc_info.value.status_code == 422


def test_create_pull_request_unrelated_422_error_field_is_not_a_conflict() -> None:
    from openorc.adapters.github import (
        GitHubPullRequestExistsError,
        GitHubRequestRejectedError,
    )

    # A validation failure naming a different field (title) is not the
    # duplicate-PR condition.
    fetch = FakeFetcher(
        [
            _json(
                422,
                {},
                {
                    "message": "Validation Failed",
                    "errors": [{"resource": "PullRequest", "field": "title", "code": "missing"}],
                },
            ),
        ]
    )

    with pytest.raises(GitHubRequestRejectedError) as exc_info:
        _client(fetch).create_pull_request(
            credential=_user_credential(),
            github_installation_id=4242,
            owner_login="octocat",
            repository_name="hello-world",
            head_ref="openorc/task-42",
            base_ref="main",
            title="feat: task 42",
            body=None,
        )

    assert not isinstance(exc_info.value, GitHubPullRequestExistsError)


def test_create_pull_request_base_field_with_invalid_code_is_not_a_conflict() -> None:
    from openorc.adapters.github import (
        GitHubPullRequestExistsError,
        GitHubRequestRejectedError,
    )

    # An actually invalid/nonexistent base documents as field=base with the
    # ordinary `invalid` code: the `code` vocabulary is what distinguishes
    # meanings, so this stays an ordinary definitive rejection and must
    # never become the workflow's non-adoption conflict.
    fetch = FakeFetcher(
        [
            _json(
                422,
                {},
                {
                    "message": "Validation Failed",
                    "errors": [{"resource": "PullRequest", "field": "base", "code": "invalid"}],
                },
            ),
        ]
    )

    with pytest.raises(GitHubRequestRejectedError) as exc_info:
        _client(fetch).create_pull_request(
            credential=_user_credential(),
            github_installation_id=4242,
            owner_login="octocat",
            repository_name="hello-world",
            head_ref="openorc/task-42",
            base_ref="nonexistent-base",
            title="feat: task 42",
            body=None,
        )

    assert not isinstance(exc_info.value, GitHubPullRequestExistsError)


def test_create_pull_request_other_definitive_rejections_stay_classified() -> None:
    fetch = FakeFetcher([_json(403, {}, {"message": "denied"})])

    with pytest.raises(GitHubAuthorizationRejectedError):
        _client(fetch).create_pull_request(
            credential=_user_credential(),
            github_installation_id=4242,
            owner_login="octocat",
            repository_name="hello-world",
            head_ref="openorc/task-42",
            base_ref="main",
            title="feat: task 42",
            body=None,
        )


def test_create_pull_request_connection_loss_is_uncertain() -> None:
    fetch = FakeFetcher([ConnectionResetError("lost")])

    with pytest.raises(GitHubOutcomeUncertainError):
        _client(fetch).create_pull_request(
            credential=_user_credential(),
            github_installation_id=4242,
            owner_login="octocat",
            repository_name="hello-world",
            head_ref="openorc/task-42",
            base_ref="main",
            title="feat: task 42",
            body=None,
        )


def test_create_pull_request_uninterpretable_success_is_uncertain() -> None:
    fetch = FakeFetcher([_json(201, {}, {"id": "not-an-int"})])

    with pytest.raises(GitHubOutcomeUncertainError):
        _client(fetch).create_pull_request(
            credential=_user_credential(),
            github_installation_id=4242,
            owner_login="octocat",
            repository_name="hello-world",
            head_ref="openorc/task-42",
            base_ref="main",
            title="feat: task 42",
            body=None,
        )
