"""Deterministic tests for the Profile-bound user-to-server credential path (issue #143).

The adapter surface #143 adds for Owner-accountable GitHub writes: the
typed ``GitHubProfileUserAccessToken`` carrier (the exact accountable
Profile identity bound to that Profile's resolved user access token) and
the authoritative user × App installation × repository intersection proof
(``validate_user_installation_repository_access``) through the documented
App-scoped user-to-server listings under the user access token.

Proven here: exact documented paths, the Bearer user token on every proof
request (no installation token is ever minted on this path), stable-ID
binding of both routed identities, the documented ``Link`` pagination with
the bounded page count, the classified absence/401/403/rate-limit/5xx
outcomes, uncertainty for uninterpretable answers, and the carrier's
redacted representation — the token value is reachable only through the
carrier at the request-construction boundary. No live GitHub access.
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
from openorc.adapters.github.capabilities import (
    parse_installation_repositories_page,
    parse_user_installations_page,
)
from openorc.adapters.github.client import HttpGitHubAppClient
from openorc.adapters.github.errors import (
    GitHubAuthenticationRejectedError,
    GitHubAuthorizationRejectedError,
    GitHubOutcomeUncertainError,
    GitHubRateLimitedError,
)
from openorc.adapters.github.transport import (
    GITHUB_API_BASE_URL,
    HttpGitHubRestClient,
)
from openorc.adapters.github.user_tokens import (
    GitHubProfileUserAccessToken,
    GitHubUserAccessToken,
)

_NOW = 1_790_000_000.0
_INSTALLATION_ID = 4242
_REPOSITORY_ID = 987654321
_USER_TOKEN_VALUE = "ghu_owner_user_token"


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


class FakeClock:
    def __init__(self, start: float = _NOW) -> None:
        self.now = start

    def __call__(self) -> float:
        return self.now


def _json(
    status: int, headers: Mapping[str, str], payload: Any
) -> tuple[int, Mapping[str, str], bytes]:
    return status, dict(headers), json.dumps(payload).encode()


def _client(fetch: FakeFetcher) -> HttpGitHubAppClient:
    authenticator = GitHubAppAuthenticator(
        app_id=12345,
        private_key_pem=_key_pem(),
        clock=FakeClock(),
        fetch=fetch,
    )
    transport = HttpGitHubRestClient(fetch=fetch)
    return HttpGitHubAppClient(authenticator=authenticator, transport=transport)


def _credential() -> GitHubProfileUserAccessToken:
    return GitHubProfileUserAccessToken(
        profile_id=uuid.UUID(int=42),
        access_token=GitHubUserAccessToken(
            value=_USER_TOKEN_VALUE, expires_at=datetime.fromtimestamp(_NOW + 3600, tz=UTC)
        ),
    )


def _key_pem() -> str:
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    return key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    ).decode()


def _installations_payload(*installation_ids: int) -> dict[str, Any]:
    return {
        "total_count": len(installation_ids),
        "installations": [{"id": installation_id} for installation_id in installation_ids],
    }


def _repositories_payload(*repository_ids: int) -> dict[str, Any]:
    return {
        "total_count": len(repository_ids),
        "repositories": [{"id": repository_id} for repository_id in repository_ids],
    }


def test_the_intersection_proof_walks_both_documented_user_listings() -> None:
    fetch = FakeFetcher(
        [
            _json(200, {}, _installations_payload(_INSTALLATION_ID)),
            _json(200, {}, _repositories_payload(_REPOSITORY_ID)),
        ]
    )

    validation = _client(fetch).validate_user_installation_repository_access(
        credential=_credential(),
        github_installation_id=_INSTALLATION_ID,
        github_repository_id=_REPOSITORY_ID,
    )

    assert validation.github_installation_id == _INSTALLATION_ID
    assert validation.github_repository_id == _REPOSITORY_ID
    # Both proof requests are GETs under the user access token: no
    # installation token is ever minted on this path.
    assert fetch.calls[0][0] == f"{GITHUB_API_BASE_URL}/user/installations?per_page=100"
    assert fetch.calls[0][1] == "GET"
    assert fetch.calls[0][2]["Authorization"] == f"Bearer {_USER_TOKEN_VALUE}"
    assert fetch.calls[1][0] == (
        f"{GITHUB_API_BASE_URL}/user/installations/{_INSTALLATION_ID}/repositories?per_page=100"
    )
    assert fetch.calls[1][1] == "GET"
    assert fetch.calls[1][2]["Authorization"] == f"Bearer {_USER_TOKEN_VALUE}"
    assert not any("access_tokens" in call[0] for call in fetch.calls)


def test_a_routed_installation_absent_from_the_user_listing_is_the_classified_rejection() -> None:
    fetch = FakeFetcher([_json(200, {}, _installations_payload(111))])

    with pytest.raises(GitHubAuthorizationRejectedError):
        _client(fetch).validate_user_installation_repository_access(
            credential=_credential(),
            github_installation_id=_INSTALLATION_ID,
            github_repository_id=_REPOSITORY_ID,
        )
    # The complete listing ran once; the repositories listing was never
    # reached and nothing was retried.
    assert len(fetch.calls) == 1


def test_a_routed_repository_absent_from_the_installation_listing_is_the_classified_rejection() -> (
    None
):
    fetch = FakeFetcher(
        [
            _json(200, {}, _installations_payload(_INSTALLATION_ID)),
            _json(200, {}, _repositories_payload(111)),
        ]
    )

    with pytest.raises(GitHubAuthorizationRejectedError):
        _client(fetch).validate_user_installation_repository_access(
            credential=_credential(),
            github_installation_id=_INSTALLATION_ID,
            github_repository_id=_REPOSITORY_ID,
        )
    assert len(fetch.calls) == 2


def test_both_user_listings_walk_the_documented_link_pagination() -> None:
    page_two = f"{GITHUB_API_BASE_URL}/user/installations?per_page=100&page=2"
    repo_page_two = (
        f"{GITHUB_API_BASE_URL}/user/installations/{_INSTALLATION_ID}/repositories"
        "?per_page=100&page=2"
    )
    fetch = FakeFetcher(
        [
            _json(200, {"Link": f'<{page_two}>; rel="next"'}, _installations_payload(111)),
            _json(200, {}, _installations_payload(_INSTALLATION_ID)),
            _json(
                200,
                {"Link": f'<{repo_page_two}>; rel="next"'},
                _repositories_payload(111),
            ),
            _json(200, {}, _repositories_payload(_REPOSITORY_ID)),
        ]
    )

    validation = _client(fetch).validate_user_installation_repository_access(
        credential=_credential(),
        github_installation_id=_INSTALLATION_ID,
        github_repository_id=_REPOSITORY_ID,
    )

    assert validation.github_repository_id == _REPOSITORY_ID
    assert fetch.calls[1][0] == page_two
    assert fetch.calls[3][0] == repo_page_two


def test_a_pagination_walk_beyond_the_bounded_page_count_is_uncertain(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("openorc.adapters.github.client._MAX_LISTING_PAGES", 2)
    pages = [
        _json(
            200,
            {
                "Link": (
                    f"<{GITHUB_API_BASE_URL}/user/installations?per_page=100&page={page + 1}>"
                    '; rel="next"'
                )
            },
            _installations_payload(111),
        )
        for page in range(3)
    ]
    fetch = FakeFetcher(pages)

    with pytest.raises(GitHubOutcomeUncertainError):
        _client(fetch).validate_user_installation_repository_access(
            credential=_credential(),
            github_installation_id=_INSTALLATION_ID,
            github_repository_id=_REPOSITORY_ID,
        )


def test_a_pagination_target_leaving_the_github_origin_is_uncertain() -> None:
    fetch = FakeFetcher(
        [
            _json(
                200,
                {"Link": '<https://evil.example/user/installations?page=2>; rel="next"'},
                _installations_payload(111),
            ),
        ]
    )

    with pytest.raises(GitHubOutcomeUncertainError):
        _client(fetch).validate_user_installation_repository_access(
            credential=_credential(),
            github_installation_id=_INSTALLATION_ID,
            github_repository_id=_REPOSITORY_ID,
        )


def test_a_definitive_user_token_401_during_the_proof_propagates_without_retry() -> None:
    fetch = FakeFetcher([_json(401, {}, {"message": "Bad credentials"})])

    with pytest.raises(GitHubAuthenticationRejectedError):
        _client(fetch).validate_user_installation_repository_access(
            credential=_credential(),
            github_installation_id=_INSTALLATION_ID,
            github_repository_id=_REPOSITORY_ID,
        )
    # Exactly one transport call: the adapter never refreshes a user token;
    # the service owns the bounded recovery.
    assert len(fetch.calls) == 1


def test_rate_limited_uncertain_and_uninterpretable_proof_outcomes_are_classified() -> None:
    fetch = FakeFetcher([_json(429, {"Retry-After": "60"}, {"message": "abuse"})])
    with pytest.raises(GitHubRateLimitedError):
        _client(fetch).validate_user_installation_repository_access(
            credential=_credential(),
            github_installation_id=_INSTALLATION_ID,
            github_repository_id=_REPOSITORY_ID,
        )

    fetch = FakeFetcher([TimeoutError("connection lost")])
    with pytest.raises(GitHubOutcomeUncertainError):
        _client(fetch).validate_user_installation_repository_access(
            credential=_credential(),
            github_installation_id=_INSTALLATION_ID,
            github_repository_id=_REPOSITORY_ID,
        )

    fetch = FakeFetcher([_json(500, {}, {"message": "boom"})])
    with pytest.raises(GitHubOutcomeUncertainError):
        _client(fetch).validate_user_installation_repository_access(
            credential=_credential(),
            github_installation_id=_INSTALLATION_ID,
            github_repository_id=_REPOSITORY_ID,
        )

    # An uninterpretable listings answer is never a proof.
    fetch = FakeFetcher([_json(200, {}, {"no_installations_here": True})])
    with pytest.raises(GitHubOutcomeUncertainError):
        _client(fetch).validate_user_installation_repository_access(
            credential=_credential(),
            github_installation_id=_INSTALLATION_ID,
            github_repository_id=_REPOSITORY_ID,
        )


def test_the_profile_bound_credential_carrier_is_redacted_and_strictly_composed() -> None:
    credential = _credential()

    # Ordinary representation is redacted; the value is reachable only
    # through the carrier at the request-construction boundary.
    assert repr(credential) == "GitHubProfileUserAccessToken(<redacted>)"
    assert str(credential) == "GitHubProfileUserAccessToken(<redacted>)"
    assert credential.access_token.token_value() == _USER_TOKEN_VALUE
    assert repr(credential.access_token) == "GitHubUserAccessToken(<redacted>)"

    with pytest.raises(ValueError):
        GitHubProfileUserAccessToken(
            profile_id="not-a-uuid",  # type: ignore[arg-type]
            access_token=GitHubUserAccessToken(
                value=_USER_TOKEN_VALUE,
                expires_at=datetime.fromtimestamp(_NOW + 3600, tz=UTC),
            ),
        )
    with pytest.raises(ValueError):
        GitHubProfileUserAccessToken(
            profile_id=uuid.UUID(int=42),
            access_token="raw-string-is-never-a-carrier",  # type: ignore[arg-type]
        )


def test_the_user_installations_page_parser_consumes_only_stable_ids() -> None:
    assert parse_user_installations_page(_installations_payload(4242, 111)) == [4242, 111]
    with pytest.raises(GitHubOutcomeUncertainError):
        parse_user_installations_page({"installations": "not-a-list"})
    with pytest.raises(GitHubOutcomeUncertainError):
        parse_user_installations_page({"installations": [{"id": "not-an-int"}]})
    with pytest.raises(GitHubOutcomeUncertainError):
        parse_user_installations_page({"installations": [{"id": 0}]})

    # The repositories listing parser is reused for the user-scoped shape.
    assert parse_installation_repositories_page(_repositories_payload(987654321)) == [987654321]