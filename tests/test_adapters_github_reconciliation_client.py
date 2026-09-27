"""Deterministic tests for the reconciliation client operations (issue #59).

The concrete ``HttpGitHubAppClient`` operations B3 adds —
``get_installation_repository`` (the full v1 access validation folded into
the single bounded walk of the documented installation-repositories
listing, returning the stable-ID-matched repository observation) and
``get_repository_issue`` (the documented issue read addressed by the fresh
owner/name) — exercised through the injectable fetch seam with a real JWT
authenticator. No live GitHub access.

Proven here: exact documented paths, the observed repository metadata
preserving stable identity, absence of the stable ID as a classified
authorization denial, malformed observations as uncertain outcomes, the
documented pull-request discriminator carried as a typed fact, the response
binding checks (issue number, repository reference), and — issue #122 — the
adapter-owned related-reference resolution (references validated against the
trusted origin and followed verbatim; stable identity from the returned
object's documented fields) and the normalized rate-limit timing facts.
"""

from __future__ import annotations

import json
from collections.abc import Mapping
from datetime import UTC, datetime
from typing import Any

import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa

from openorc.adapters.github.authentication import GitHubAppAuthenticator
from openorc.adapters.github.client import HttpGitHubAppClient
from openorc.adapters.github.errors import (
    GitHubAuthorizationRejectedError,
    GitHubOutcomeUncertainError,
    GitHubRateLimitedError,
)
from openorc.adapters.github.observations_relations import GitHubRelatedIssueObservation
from openorc.adapters.github.transport import GITHUB_API_BASE_URL, HttpGitHubRestClient
from openorc.domain.github_issue_relations import RelatedIssueEndpoint

_NOW = 1_790_000_000.0
_REPOSITORY_ID = 987654321
_ISSUE_NUMBER = 42


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


def _mint_response(token: str = "ghs_reconcile_token") -> tuple[int, Mapping[str, str], bytes]:
    expires_at = datetime.fromtimestamp(_NOW + 3000, tz=UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
    return _json(201, {}, {"token": token, "expires_at": expires_at})


_FULL_V1_PERMISSIONS = {
    "issues": "write",
    "contents": "write",
    "pull_requests": "write",
    "checks": "read",
    "statuses": "read",
    "metadata": "read",
}
_FULL_V1_EVENTS = [
    "issues",
    "pull_request",
    "push",
    "status",
    "check_run",
    "check_suite",
]


def _installation_payload() -> dict[str, Any]:
    return {"id": 4242, "permissions": _FULL_V1_PERMISSIONS, "events": _FULL_V1_EVENTS}


def _repository_entry(
    repository_id: int = _REPOSITORY_ID,
    *,
    owner: str = "octocat",
    name: str = "hello-world",
) -> dict[str, Any]:
    return {
        "id": repository_id,
        "node_id": "R_dummy",
        "name": name,
        "owner": {"login": owner},
        "html_url": f"https://github.com/{owner}/{name}",
        "private": False,
        "default_branch": "main",
        "permissions": {"admin": False, "push": True, "pull": True},
    }


def _listing_payload(entries: list[dict[str, Any]]) -> dict[str, Any]:
    return {"total_count": len(entries), "repositories": entries}


def _issue_payload(
    *,
    issue_id: int = 503,
    number: int = _ISSUE_NUMBER,
    title: str = "Found a bug",
    body: str | None = "Steps to reproduce",
    state: str = "open",
    repository_url: str | None = None,
    updated_at: str = "2026-09-24T01:02:03Z",
    is_pull_request: bool = False,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "id": issue_id,
        "number": number,
        "title": title,
        "body": body,
        "state": state,
        "updated_at": updated_at,
        "repository_url": repository_url or f"{GITHUB_API_BASE_URL}/repos/octocat/hello-world",
        "labels": [],
        "assignees": [],
    }
    if is_pull_request:
        payload["pull_request"] = {"merged_at": None}
    return payload


def _client(fetch: FakeFetcher) -> HttpGitHubAppClient:
    authenticator = GitHubAppAuthenticator(
        app_id=12345, private_key_pem=_key_pem(), clock=FakeClock(), fetch=fetch
    )
    transport = HttpGitHubRestClient(fetch=fetch)
    return HttpGitHubAppClient(authenticator=authenticator, transport=transport)


def _reconcile_repository_fetch() -> FakeFetcher:
    return FakeFetcher(
        [
            _json(200, {}, _installation_payload()),
            _mint_response(),
            _json(200, {}, _listing_payload([_repository_entry()])),
        ]
    )


def test_repository_observation_comes_from_the_documented_stable_id_listing() -> None:
    fetch = _reconcile_repository_fetch()

    observation = _client(fetch).get_installation_repository(
        github_installation_id=4242, github_repository_id=_REPOSITORY_ID
    )

    assert observation.github_repository_id == _REPOSITORY_ID
    assert observation.owner_login == "octocat"
    assert observation.name == "hello-world"
    assert observation.html_url == "https://github.com/octocat/hello-world"
    assert observation.is_private is False
    assert observation.default_branch == "main"
    # Exact documented call sequence: App-JWT installation lookup, token
    # mint for the routed installation, then the bounded listing walk.
    assert fetch.calls[0][0] == f"{GITHUB_API_BASE_URL}/app/installations/4242"
    assert fetch.calls[1][0].endswith("/app/installations/4242/access_tokens")
    assert fetch.calls[2][0] == f"{GITHUB_API_BASE_URL}/installation/repositories?per_page=100"
    assert fetch.calls[2][2]["Authorization"] == "Bearer ghs_reconcile_token"


def test_repository_observation_walks_the_documented_link_pagination() -> None:
    page_two = f"{GITHUB_API_BASE_URL}/installation/repositories?per_page=100&page=2"
    fetch = FakeFetcher(
        [
            _json(200, {}, _installation_payload()),
            _mint_response(),
            _json(
                200,
                {"Link": f'<{page_two}>; rel="next"'},
                _listing_payload([_repository_entry(repository_id=111)]),
            ),
            _json(200, {}, _listing_payload([_repository_entry()])),
        ]
    )

    observation = _client(fetch).get_installation_repository(
        github_installation_id=4242, github_repository_id=_REPOSITORY_ID
    )

    assert observation.github_repository_id == _REPOSITORY_ID
    assert fetch.calls[3][0] == page_two


def test_repository_access_absence_is_a_classified_denial() -> None:
    fetch = FakeFetcher(
        [
            _json(200, {}, _installation_payload()),
            _mint_response(),
            _json(200, {}, _listing_payload([_repository_entry(repository_id=111)])),
        ]
    )

    with pytest.raises(GitHubAuthorizationRejectedError):
        _client(fetch).get_installation_repository(
            github_installation_id=4242, github_repository_id=_REPOSITORY_ID
        )


def test_a_suspended_installation_is_denied_before_any_listing() -> None:
    suspended = _installation_payload() | {"suspended_at": "2026-09-24T01:02:03Z"}
    fetch = FakeFetcher([_json(200, {}, suspended)])

    with pytest.raises(GitHubAuthorizationRejectedError):
        _client(fetch).get_installation_repository(
            github_installation_id=4242, github_repository_id=_REPOSITORY_ID
        )

    assert len(fetch.calls) == 1


def test_a_malformed_matched_entry_classifies_as_uncertain() -> None:
    malformed = _repository_entry() | {"name": ""}
    fetch = FakeFetcher(
        [
            _json(200, {}, _installation_payload()),
            _mint_response(),
            _json(200, {}, _listing_payload([malformed])),
        ]
    )

    with pytest.raises(GitHubOutcomeUncertainError):
        _client(fetch).get_installation_repository(
            github_installation_id=4242, github_repository_id=_REPOSITORY_ID
        )


def test_the_issue_read_targets_the_documented_path_and_returns_the_observation() -> None:
    fetch = FakeFetcher([_mint_response(), _json(200, {}, _issue_payload())])

    observation = _client(fetch).get_repository_issue(
        github_installation_id=4242,
        owner_login="octocat",
        repository_name="hello-world",
        issue_number=_ISSUE_NUMBER,
    )

    assert fetch.calls[0][0].endswith("/app/installations/4242/access_tokens")
    assert fetch.calls[1][0] == f"{GITHUB_API_BASE_URL}/repos/octocat/hello-world/issues/42"
    assert fetch.calls[1][2]["Authorization"] == "Bearer ghs_reconcile_token"
    assert observation.github_issue_id == 503
    assert observation.issue_number == _ISSUE_NUMBER
    assert observation.title == "Found a bug"
    assert observation.body == "Steps to reproduce"
    assert observation.state == "open"
    assert observation.provider_updated_at == datetime(2026, 9, 24, 1, 2, 3, tzinfo=UTC)
    assert observation.is_pull_request is False


def test_a_null_body_is_preserved_verbatim() -> None:
    fetch = FakeFetcher([_mint_response(), _json(200, {}, _issue_payload(body=None))])

    observation = _client(fetch).get_repository_issue(
        github_installation_id=4242,
        owner_login="octocat",
        repository_name="hello-world",
        issue_number=_ISSUE_NUMBER,
    )

    assert observation.body is None


def test_the_documented_pull_request_member_is_a_typed_discriminator() -> None:
    fetch = FakeFetcher([_mint_response(), _json(200, {}, _issue_payload(is_pull_request=True))])

    observation = _client(fetch).get_repository_issue(
        github_installation_id=4242,
        owner_login="octocat",
        repository_name="hello-world",
        issue_number=_ISSUE_NUMBER,
    )

    assert observation.is_pull_request is True


def test_a_lost_issue_answer_is_a_classified_access_denial() -> None:
    fetch = FakeFetcher([_mint_response(), _json(404, {}, {"message": "Not Found"})])

    with pytest.raises(GitHubAuthorizationRejectedError):
        _client(fetch).get_repository_issue(
            github_installation_id=4242,
            owner_login="octocat",
            repository_name="hello-world",
            issue_number=_ISSUE_NUMBER,
        )


def test_a_rate_limited_issue_read_is_a_known_failure_not_lost_access() -> None:
    fetch = FakeFetcher([_mint_response(), _json(429, {"Retry-After": "60"}, {})])

    with pytest.raises(GitHubRateLimitedError):
        _client(fetch).get_repository_issue(
            github_installation_id=4242,
            owner_login="octocat",
            repository_name="hello-world",
            issue_number=_ISSUE_NUMBER,
        )


def test_an_unfollowed_redirect_is_an_uncertain_outcome() -> None:
    fetch = FakeFetcher([_mint_response(), _json(301, {"Location": "https://evil.example/"}, {})])

    with pytest.raises(GitHubOutcomeUncertainError):
        _client(fetch).get_repository_issue(
            github_installation_id=4242,
            owner_login="octocat",
            repository_name="hello-world",
            issue_number=_ISSUE_NUMBER,
        )


def test_the_issue_response_must_bind_to_the_addressed_number() -> None:
    fetch = FakeFetcher(
        [_mint_response(), _json(200, {}, _issue_payload(number=_ISSUE_NUMBER + 1))]
    )

    with pytest.raises(GitHubOutcomeUncertainError):
        _client(fetch).get_repository_issue(
            github_installation_id=4242,
            owner_login="octocat",
            repository_name="hello-world",
            issue_number=_ISSUE_NUMBER,
        )


def test_the_issue_response_must_bind_to_the_addressed_repository() -> None:
    fetch = FakeFetcher(
        [
            _mint_response(),
            _json(
                200,
                {},
                _issue_payload(repository_url=f"{GITHUB_API_BASE_URL}/repos/other/repo"),
            ),
        ]
    )

    with pytest.raises(GitHubOutcomeUncertainError):
        _client(fetch).get_repository_issue(
            github_installation_id=4242,
            owner_login="octocat",
            repository_name="hello-world",
            issue_number=_ISSUE_NUMBER,
        )


def test_the_issue_response_binds_case_insensitively_to_the_addressed_repository() -> None:
    # GitHub's documented case-insensitive name handling: the returned
    # reference is compared whole against the documented address — never
    # decomposed into owner/name parts.
    fetch = FakeFetcher(
        [
            _mint_response(),
            _json(
                200,
                {},
                _issue_payload(repository_url=f"{GITHUB_API_BASE_URL}/repos/OCTOCAT/Hello-World"),
            ),
        ]
    )

    observation = _client(fetch).get_repository_issue(
        github_installation_id=4242,
        owner_login="octocat",
        repository_name="hello-world",
        issue_number=_ISSUE_NUMBER,
    )

    assert observation.github_issue_id == 503


def test_a_non_api_origin_issue_repository_reference_fails_closed() -> None:
    # Regression (issue #122): a URL that merely carries a
    # /repos/{owner}/{repo} suffix on a foreign origin used to satisfy the
    # tail-splitting binding check; parsed-origin validation fails it closed.
    fetch = FakeFetcher(
        [
            _mint_response(),
            _json(
                200,
                {},
                _issue_payload(repository_url="https://evil.example/repos/octocat/hello-world"),
            ),
        ]
    )

    with pytest.raises(GitHubOutcomeUncertainError):
        _client(fetch).get_repository_issue(
            github_installation_id=4242,
            owner_login="octocat",
            repository_name="hello-world",
            issue_number=_ISSUE_NUMBER,
        )


def test_rate_limit_timing_facts_normalize_through_the_client_boundary() -> None:
    fetch = FakeFetcher(
        [
            _mint_response(),
            _json(429, {"Retry-After": "17", "X-RateLimit-Reset": "1893456000"}, {}),
        ]
    )

    with pytest.raises(GitHubRateLimitedError) as caught:
        _client(fetch).get_repository_issue(
            github_installation_id=4242,
            owner_login="octocat",
            repository_name="hello-world",
            issue_number=_ISSUE_NUMBER,
        )

    assert caught.value.retry_after_seconds == 17.0
    assert caught.value.rate_limit_reset_at == datetime.fromtimestamp(1893456000, tz=UTC)


# --- Related-issue endpoint resolution (issue #122) --------------------------


def test_cross_repository_resolution_follows_the_exact_returned_reference() -> None:
    reference = f"{GITHUB_API_BASE_URL}/repos/other/repo"
    fetch = FakeFetcher(
        [
            _mint_response(),
            _json(200, {}, {"id": 555, "name": "repo", "owner": {"login": "other"}}),
        ]
    )

    resolved = _client(fetch).resolve_related_issue_endpoints(
        github_installation_id=4242,
        local_github_repository_id=_REPOSITORY_ID,
        fresh_owner_login="octocat",
        fresh_repository_name="hello-world",
        related=[GitHubRelatedIssueObservation(github_issue_id=7, repository_url=reference)],
    )

    # The exact GitHub-returned reference is followed verbatim — never
    # reconstructed from dissected path parts — and the stable identity comes
    # from the returned object's documented numeric id.
    assert fetch.calls[0][0].endswith("/app/installations/4242/access_tokens")
    assert fetch.calls[1][0] == reference
    assert resolved == [RelatedIssueEndpoint(github_repository_id=555, github_issue_id=7)]


def test_same_repository_references_reuse_the_local_stable_id_without_reads() -> None:
    fetch = FakeFetcher([_mint_response()])

    resolved = _client(fetch).resolve_related_issue_endpoints(
        github_installation_id=4242,
        local_github_repository_id=_REPOSITORY_ID,
        fresh_owner_login="octocat",
        fresh_repository_name="hello-world",
        related=[
            GitHubRelatedIssueObservation(
                github_issue_id=7,
                repository_url=f"{GITHUB_API_BASE_URL}/repos/OCTOCAT/Hello-World",
            )
        ],
    )

    # Case-insensitive documented-address match; zero extra reads — not even
    # an installation-token mint, because no provider request is made.
    assert fetch.calls == []
    assert resolved == [
        RelatedIssueEndpoint(github_repository_id=_REPOSITORY_ID, github_issue_id=7)
    ]


def test_each_distinct_reference_resolves_at_most_once_per_call() -> None:
    reference = f"{GITHUB_API_BASE_URL}/repos/other/repo"
    fetch = FakeFetcher([_mint_response(), _json(200, {}, {"id": 555})])

    resolved = _client(fetch).resolve_related_issue_endpoints(
        github_installation_id=4242,
        local_github_repository_id=_REPOSITORY_ID,
        fresh_owner_login="octocat",
        fresh_repository_name="hello-world",
        related=[
            GitHubRelatedIssueObservation(github_issue_id=7, repository_url=reference),
            GitHubRelatedIssueObservation(github_issue_id=8, repository_url=reference),
            GitHubRelatedIssueObservation(
                github_issue_id=9,
                repository_url=f"{GITHUB_API_BASE_URL}/repos/octocat/hello-world",
            ),
        ],
    )

    assert len(fetch.calls) == 2  # token mint + exactly one follow of the reference
    assert resolved == [
        RelatedIssueEndpoint(github_repository_id=555, github_issue_id=7),
        RelatedIssueEndpoint(github_repository_id=555, github_issue_id=8),
        RelatedIssueEndpoint(github_repository_id=_REPOSITORY_ID, github_issue_id=9),
    ]


def test_a_non_api_origin_reference_cannot_establish_identity() -> None:
    fetch = FakeFetcher([])

    with pytest.raises(GitHubOutcomeUncertainError):
        _client(fetch).resolve_related_issue_endpoints(
            github_installation_id=4242,
            local_github_repository_id=_REPOSITORY_ID,
            fresh_owner_login="octocat",
            fresh_repository_name="hello-world",
            related=[
                GitHubRelatedIssueObservation(
                    github_issue_id=7,
                    repository_url="https://github.com/octocat/other",
                )
            ],
        )

    assert fetch.calls == []


def test_an_empty_related_listing_resolves_without_reads() -> None:
    fetch = FakeFetcher([])

    resolved = _client(fetch).resolve_related_issue_endpoints(
        github_installation_id=4242,
        local_github_repository_id=_REPOSITORY_ID,
        fresh_owner_login="octocat",
        fresh_repository_name="hello-world",
        related=[],
    )

    assert resolved == []
    assert fetch.calls == []


def test_a_malformed_returned_repository_object_classifies_uncertain() -> None:
    fetch = FakeFetcher(
        [
            _mint_response(),
            _json(200, {}, {"name": "repo", "owner": {"login": "other"}}),
        ]
    )

    with pytest.raises(GitHubOutcomeUncertainError):
        _client(fetch).resolve_related_issue_endpoints(
            github_installation_id=4242,
            local_github_repository_id=_REPOSITORY_ID,
            fresh_owner_login="octocat",
            fresh_repository_name="hello-world",
            related=[
                GitHubRelatedIssueObservation(
                    github_issue_id=7,
                    repository_url=f"{GITHUB_API_BASE_URL}/repos/other/repo",
                )
            ],
        )
