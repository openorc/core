"""Deterministic tests for the GitHub user-token web-flow mechanics (issue #142).

An injected fetch seam and clock prove the normalized expiring-token grant
parsing, the outcome classification (known rejection — including GitHub's
documented 200-status error body — vs. the missing refresh capability vs.
unknown outcome), the fixed token-endpoint request contract with credentials
only in the form-encoded body, the authoritative current-user normalization,
the fail-fast construction rules, and the redacted secret-bearing carriers.
No live GitHub network access.
"""

from __future__ import annotations

import urllib.parse
from collections.abc import Mapping
from datetime import UTC, datetime, timedelta

import pytest

from openorc.adapters.github.errors import (
    GitHubAuthenticationRejectedError,
    GitHubOutcomeUncertainError,
    GitHubUserTokenRefreshCapabilityMissingError,
    GitHubUserTokenRejectedError,
)
from openorc.adapters.github.transport import GITHUB_API_BASE_URL
from openorc.adapters.github.user_tokens import (
    GITHUB_TOKEN_ENDPOINT_URL,
    GitHubUserAccessToken,
    HttpGitHubUserTokenClient,
)
from openorc.config import ConfigurationError

_FIXED_NOW = datetime(2026, 9, 29, 12, 0, 0, tzinfo=UTC)
_CLIENT_ID = "Iv1.client-id-value"
_CLIENT_SECRET = "s3cr3t-client-secret"
_CODE = "authorization-code-value"
_REFRESH = "ghr_s3cr3t-refresh-value"


class FakeGitHubTransport:
    """Injectable transport recording calls; serves queued responses."""

    def __init__(self, results: list[tuple[int, Mapping[str, str], bytes] | Exception]) -> None:
        self.results = list(results)
        self.calls: list[tuple[str, str, dict[str, str], float, bytes | None]] = []

    def __call__(
        self,
        url: str,
        method: str,
        headers: Mapping[str, str],
        timeout: float,
        body: bytes | None,
    ) -> tuple[int, Mapping[str, str], bytes]:
        self.calls.append((url, method, dict(headers), timeout, body))
        result = self.results.pop(0)
        if isinstance(result, Exception):
            raise result
        return result


def _client(transport: FakeGitHubTransport) -> HttpGitHubUserTokenClient:
    return HttpGitHubUserTokenClient(
        client_id=_CLIENT_ID,
        client_secret=_CLIENT_SECRET,
        fetch=transport,
        clock=lambda: _FIXED_NOW,
    )


def _grant_body() -> bytes:
    return (
        b'{"access_token": "ghu_s3cr3t-access", "expires_in": 28800,'
        b' "refresh_token": "' + _REFRESH.encode() + b'",'
        b' "refresh_token_expires_in": 15897600, "scope": "", "token_type": "bearer"}'
    )


def test_exchange_posts_credentials_in_the_body_to_the_fixed_token_endpoint() -> None:
    transport = FakeGitHubTransport([(200, {}, _grant_body())])

    _client(transport).exchange_authorization_code(_CODE)

    ((url, method, headers, _timeout, body),) = transport.calls
    assert url == GITHUB_TOKEN_ENDPOINT_URL
    assert method == "POST"
    assert headers["Accept"] == "application/json"
    assert headers["Content-Type"] == "application/x-www-form-urlencoded"
    form = dict(urllib.parse.parse_qsl((body or b"").decode()))
    assert form["client_id"] == _CLIENT_ID
    assert form["client_secret"] == _CLIENT_SECRET
    assert form["code"] == _CODE
    assert "refresh_token" not in form


def test_exchange_normalizes_the_expiring_token_grant_with_absolute_expiries() -> None:
    transport = FakeGitHubTransport([(200, {}, _grant_body())])

    grant = _client(transport).exchange_authorization_code(_CODE)

    assert grant.access_token.token_value() == "ghu_s3cr3t-access"
    assert grant.access_token.expires_at == _FIXED_NOW + timedelta(seconds=28800)
    assert grant.refresh_token.secret_value() == _REFRESH
    assert grant.refresh_token.expires_at == _FIXED_NOW + timedelta(seconds=15897600)
    # Secret-bearing carriers are redacted on every ordinary representation.
    assert "ghu_s3cr3t-access" not in repr(grant.access_token)
    assert "ghu_s3cr3t-access" not in str(grant.access_token)
    assert "ghr_s3cr3t" not in repr(grant.refresh_token)
    assert "ghr_s3cr3t" not in str(grant.refresh_token)


def test_refresh_exchange_sends_the_documented_refresh_grant() -> None:
    transport = FakeGitHubTransport([(200, {}, _grant_body())])

    _client(transport).refresh_user_token(_REFRESH)

    ((url, method, _headers, _timeout, body),) = transport.calls
    assert url == GITHUB_TOKEN_ENDPOINT_URL
    assert method == "POST"
    form = dict(urllib.parse.parse_qsl((body or b"").decode()))
    assert form["grant_type"] == "refresh_token"
    assert form["refresh_token"] == _REFRESH
    assert form["client_id"] == _CLIENT_ID
    assert form["client_secret"] == _CLIENT_SECRET
    assert "code" not in form


def test_documented_oauth_error_body_under_a_200_status_is_a_known_rejection() -> None:
    # GitHub's OAuth token endpoint delivers its error shape as a JSON body
    # that may arrive even under a 200 status: the parsed error member — not
    # the status alone — decides the known rejection.
    transport = FakeGitHubTransport(
        [
            (
                200,
                {},
                b'{"error": "bad_verification_code", "error_description": "..."}',
            )
        ]
    )

    with pytest.raises(GitHubUserTokenRejectedError):
        _client(transport).exchange_authorization_code(_CODE)


def test_definitive_answer_without_the_refresh_capability_fails_closed() -> None:
    # The documented non-expiring shape: OpenOrc never persists a long-lived
    # user access token as a second credential model.
    transport = FakeGitHubTransport(
        [(200, {}, b'{"access_token": "ghu_longlived", "token_type": "bearer"}')]
    )

    with pytest.raises(GitHubUserTokenRefreshCapabilityMissingError):
        _client(transport).exchange_authorization_code(_CODE)


@pytest.mark.parametrize("status", [400, 401, 403, 422])
def test_definitive_4xx_answers_are_known_rejections(status: int) -> None:
    transport = FakeGitHubTransport([(status, {}, b'{"error": "incorrect_client_credentials"}')])

    with pytest.raises(GitHubUserTokenRejectedError) as error:
        _client(transport).exchange_authorization_code(_CODE)

    assert error.value.status_code == status
    assert _CLIENT_SECRET not in str(error.value)


@pytest.mark.parametrize("status", [500, 502, 503])
def test_server_errors_leave_the_outcome_unknown(status: int) -> None:
    transport = FakeGitHubTransport([(status, {}, b"")])

    with pytest.raises(GitHubOutcomeUncertainError):
        _client(transport).exchange_authorization_code(_CODE)


def test_transport_failure_leaves_the_outcome_unknown() -> None:
    transport = FakeGitHubTransport([TimeoutError("timed out")])

    with pytest.raises(GitHubOutcomeUncertainError):
        _client(transport).exchange_authorization_code(_CODE)


@pytest.mark.parametrize(
    "body",
    [
        b"",
        b"not json",
        b"[]",
        b'{"access_token": 5, "expires_in": 1, "refresh_token": "g",'
        b' "refresh_token_expires_in": 1, "token_type": "bearer"}',
        b'{"access_token": "t", "expires_in": "soon", "refresh_token": "g",'
        b' "refresh_token_expires_in": 1, "token_type": "bearer"}',
        b'{"access_token": "t", "expires_in": 0, "refresh_token": "g",'
        b' "refresh_token_expires_in": 1, "token_type": "bearer"}',
        b'{"access_token": "t", "expires_in": 28800, "refresh_token": "g",'
        b' "refresh_token_expires_in": 1, "token_type": "mac"}',
        b'{"access_token": "t", "expires_in": 28800, "refresh_token": "g",'
        b' "refresh_token_expires_in": -5, "token_type": "bearer"}',
        b'{"access_token": "t", "expires_in": 28800, "refresh_token": "g",'
        b' "refresh_token_expires_in": 1, "token_type": "mac"}',
    ],
)
def test_uninterpretable_successful_answers_leave_the_outcome_unknown(body: bytes) -> None:
    transport = FakeGitHubTransport([(200, {}, body)])

    with pytest.raises(GitHubOutcomeUncertainError):
        _client(transport).exchange_authorization_code(_CODE)


@pytest.mark.parametrize(
    "kwargs",
    [
        {"client_id": "", "client_secret": _CLIENT_SECRET},
        {"client_id": _CLIENT_ID, "client_secret": ""},
        {"client_id": _CLIENT_ID, "client_secret": None},
    ],
)
def test_construction_fails_fast_on_missing_deployment_credentials(kwargs: dict) -> None:
    with pytest.raises(ConfigurationError):
        HttpGitHubUserTokenClient(
            clock=lambda: _FIXED_NOW,
            fetch=FakeGitHubTransport([]),
            **kwargs,
        )


def test_authenticated_user_lookup_normalizes_the_stable_identity() -> None:
    transport = FakeGitHubTransport(
        [(200, {}, b'{"id": 5432, "login": "octocat", "email": "e@x"}')]
    )
    token = GitHubUserAccessToken(value="ghu_s3cr3t-access", expires_at=_FIXED_NOW)

    current_user = _client(transport).fetch_authenticated_user(token)

    assert current_user.github_user_id == 5432
    assert current_user.login == "octocat"
    ((url, method, headers, _timeout, body),) = transport.calls
    assert url == f"{GITHUB_API_BASE_URL}/user"
    assert method == "GET"
    # The user access token is presented as the Bearer credential.
    assert headers["Authorization"] == "Bearer ghu_s3cr3t-access"
    assert body is None


def test_authenticated_user_lookup_classifies_a_rejected_token() -> None:
    transport = FakeGitHubTransport([(401, {}, b"")])
    token = GitHubUserAccessToken(value="ghu_expired", expires_at=_FIXED_NOW)

    with pytest.raises(GitHubAuthenticationRejectedError):
        _client(transport).fetch_authenticated_user(token)


def test_authenticated_user_lookup_fails_closed_on_malformed_payload() -> None:
    transport = FakeGitHubTransport([(200, {}, b'{"id": "not-a-number"}')])
    token = GitHubUserAccessToken(value="ghu_x", expires_at=_FIXED_NOW)

    with pytest.raises(GitHubOutcomeUncertainError):
        _client(transport).fetch_authenticated_user(token)


def test_client_ordinary_representation_is_redacted() -> None:
    client = _client(FakeGitHubTransport([]))

    assert _CLIENT_SECRET not in repr(client)
    assert _CLIENT_SECRET not in str(client)
