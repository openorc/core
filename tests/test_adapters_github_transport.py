"""Deterministic tests for the GitHub REST transport boundary (issue #58).

An injected fetch seam proves the centralized header contract (the pinned
REST API version, the JSON Accept header, the presented Authorization
credential, the mandatory User-Agent), the bounded timeout propagation,
outcome classification (2xx success; definitive 401/403/404 rejections; rate
limits classified apart from authorization; 3xx/5xx and transport failures as
uncertain outcomes), body-discipline on every failure, and the path-boundary
rules. No live GitHub network access.
"""

from __future__ import annotations

import urllib.error
from collections.abc import Mapping
from typing import Any

import pytest

from openorc.adapters.github.errors import (
    GitHubAuthenticationRejectedError,
    GitHubAuthorizationRejectedError,
    GitHubOutcomeUncertainError,
    GitHubRateLimitedError,
    GitHubRequestRejectedError,
)
from openorc.adapters.github.transport import (
    DEFAULT_GITHUB_REQUEST_TIMEOUT_SECONDS,
    GITHUB_API_BASE_URL,
    GITHUB_JSON_ACCEPT_HEADER,
    SUPPORTED_GITHUB_API_VERSION,
    HttpGitHubRestClient,
)
from openorc.config import ConfigurationError

_SECRET_BODY = b'{"message": "ghs_secret_token_value_do_not_leak"}'


class FakeFetcher:
    """Injectable transport seam recording calls; serves queued results.

    Each queued entry is either a ``(status, headers, body)`` tuple or an
    exception instance to raise once (transport-level failures the
    classification maps to uncertain outcomes).
    """

    def __init__(self, results: list[tuple[int, Mapping[str, str], bytes] | Exception]) -> None:
        self.results = list(results)
        self.calls: list[tuple[str, str, dict[str, str], float]] = []

    def __call__(
        self, url: str, method: str, headers: Mapping[str, str], timeout_seconds: float
    ) -> tuple[int, Mapping[str, str], bytes]:
        self.calls.append((url, method, dict(headers), timeout_seconds))
        result = self.results.pop(0)
        if isinstance(result, Exception):
            raise result
        return result


def _client(fetch: FakeFetcher) -> HttpGitHubRestClient:
    return HttpGitHubRestClient(fetch=fetch)


def test_requests_carry_the_centralized_header_and_timeout_contract() -> None:
    fetch = FakeFetcher([(200, {}, b"{}")])

    _client(fetch).request("/test/path", method="GET", authorization="Bearer test-token")

    url, method, headers, timeout = fetch.calls[0]
    assert url == GITHUB_API_BASE_URL + "/test/path"
    assert method == "GET"
    assert headers["Accept"] == GITHUB_JSON_ACCEPT_HEADER
    # The supported GitHub REST API version is pinned in one adapter location.
    assert headers["X-GitHub-Api-Version"] == SUPPORTED_GITHUB_API_VERSION
    assert headers["Authorization"] == "Bearer test-token"
    assert headers["User-Agent"]
    assert timeout == DEFAULT_GITHUB_REQUEST_TIMEOUT_SECONDS


def test_json_request_body_sets_the_content_type() -> None:
    fetch = FakeFetcher([(201, {}, b"{}")])

    _client(fetch).request(
        "/test/path", method="POST", authorization="Bearer t", body=b'{"k": "v"}'
    )

    _, _, headers, _ = fetch.calls[0]
    assert headers["Content-Type"] == "application/json"


def test_successful_answers_return_the_header_retaining_response() -> None:
    fetch = FakeFetcher([(200, {"Link": "<next>"}, b'{"ok": true}')])

    response = _client(fetch).request("/x", method="GET", authorization="Bearer t")

    assert response.status == 200
    assert response.headers["Link"] == "<next>"
    assert response.body == b'{"ok": true}'


@pytest.mark.parametrize(
    ("status", "expected_error"),
    [
        (401, GitHubAuthenticationRejectedError),
        (403, GitHubAuthorizationRejectedError),
        (404, GitHubAuthorizationRejectedError),
        (400, GitHubRequestRejectedError),
        (422, GitHubRequestRejectedError),
        (429, GitHubRequestRejectedError),
        (302, GitHubOutcomeUncertainError),
        (304, GitHubOutcomeUncertainError),
        (500, GitHubOutcomeUncertainError),
        (503, GitHubOutcomeUncertainError),
    ],
)
def test_outcome_classification(status: int, expected_error: type[Exception]) -> None:
    fetch = FakeFetcher([(status, {}, _SECRET_BODY)])

    with pytest.raises(expected_error):
        _client(fetch).request("/x", method="GET", authorization="Bearer t")


@pytest.mark.parametrize(
    ("status", "header_name"),
    [
        (403, "X-RateLimit-Remaining"),
        (403, "x-ratelimit-remaining"),
        (429, "X-RateLimit-Remaining"),
    ],
)
def test_exhausted_rate_limits_classify_apart_from_authorization(
    status: int, header_name: str
) -> None:
    fetch = FakeFetcher([(status, {header_name: "0"}, _SECRET_BODY)])

    with pytest.raises(GitHubRateLimitedError):
        _client(fetch).request("/x", method="GET", authorization="Bearer t")


def test_non_exhausted_rate_limit_header_is_not_rate_limited() -> None:
    fetch = FakeFetcher([(403, {"X-RateLimit-Remaining": "42"}, _SECRET_BODY)])

    with pytest.raises(GitHubAuthorizationRejectedError):
        _client(fetch).request("/x", method="GET", authorization="Bearer t")


@pytest.mark.parametrize(
    "status",
    [302, 401, 403, 404, 422, 429, 500, 503],
)
def test_error_messages_never_contain_the_response_body(status: int) -> None:
    fetch = FakeFetcher([(status, {}, _SECRET_BODY)])

    with pytest.raises(Exception) as caught:  # noqa: PT011 - classification tested above
        _client(fetch).request("/x", method="GET", authorization="Bearer t")

    assert "ghs_secret_token_value" not in str(caught.value)
    assert "ghs_secret_token_value" not in repr(caught.value)
    # The response content itself is never carried into the error boundary.
    assert '{"message"' not in str(caught.value)


@pytest.mark.parametrize(
    "transport_error",
    [
        TimeoutError("timed out"),
        urllib.error.URLError("connection lost"),
        ConnectionResetError("reset"),
        OSError("network unreachable"),
        RuntimeError("unexpected seam failure"),
    ],
)
def test_transport_failures_classify_as_uncertain_outcomes(
    transport_error: Exception,
) -> None:
    fetch = FakeFetcher([transport_error])

    with pytest.raises(GitHubOutcomeUncertainError):
        _client(fetch).request("/x", method="GET", authorization="Bearer t")


def test_seam_level_uncertain_errors_pass_through_unchanged() -> None:
    fetch = FakeFetcher([GitHubOutcomeUncertainError("already classified")])

    with pytest.raises(GitHubOutcomeUncertainError, match="already classified"):
        _client(fetch).request("/x", method="GET", authorization="Bearer t")


def test_absolute_api_urls_are_accepted_for_link_pagination_targets() -> None:
    fetch = FakeFetcher([(200, {}, b"{}")])
    absolute = GITHUB_API_BASE_URL + "/installation/repositories?per_page=100&page=2"

    _client(fetch).request(absolute, method="GET", authorization="Bearer t")

    assert fetch.calls[0][0] == absolute


@pytest.mark.parametrize("path", ["https://evil.example/steal", "relative/path", ""])
def test_targets_outside_the_api_boundary_are_rejected(path: str) -> None:
    fetch = FakeFetcher([(200, {}, b"{}")])

    with pytest.raises(ConfigurationError):
        _client(fetch).request(path, method="GET", authorization="Bearer t")
    assert fetch.calls == []


@pytest.mark.parametrize("timeout", [0, -1, "10", True, None])
def test_invalid_timeouts_fail_fast_at_construction(timeout: Any) -> None:
    with pytest.raises(ConfigurationError):
        HttpGitHubRestClient(timeout_seconds=timeout)  # type: ignore[arg-type]
