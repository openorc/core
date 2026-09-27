"""Deterministic tests for the GitHub REST transport boundary (issue #58).

An injected fetch seam proves the centralized header contract (the pinned
REST API version, the JSON Accept header, the presented Authorization
credential, the mandatory User-Agent), the bounded timeout propagation,
outcome classification (2xx success; definitive 401/403/404 rejections; rate
limits classified apart from authorization; 3xx/5xx and transport failures as
uncertain outcomes), body-discipline on every failure, and the path-boundary
rules. No live GitHub network access.

Issue #122 adds: the safe bounded redirect policy (same-trusted-origin,
read-method, bounded — exercised by driving the redirect handler over
scripted responses with no network) and the normalized rate-limit scheduling
facts on the classified error boundary.
"""

from __future__ import annotations

import urllib.error
import urllib.request
from collections.abc import Mapping
from datetime import UTC, datetime
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
    _github_request_for,
    _GitHubApiRedirectHandler,
    is_github_api_origin,
)
from openorc.config import ConfigurationError

_SECRET_BODY = b'{"message": "ghs_secret_token_value_do_not_leak"}'


class FakeFetcher:
    """Injectable transport seam recording calls; serves queued results.

    Each queued entry is either a ``(status, headers, body)`` tuple or an
    exception instance to raise once (transport-level failures the
    classification maps to uncertain outcomes). The recorded calls include
    the request body the transport presented to the seam.
    """

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


def _client(fetch: FakeFetcher) -> HttpGitHubRestClient:
    return HttpGitHubRestClient(fetch=fetch)


def test_requests_carry_the_centralized_header_and_timeout_contract() -> None:
    fetch = FakeFetcher([(200, {}, b"{}")])

    _client(fetch).request("/test/path", method="GET", authorization="Bearer test-token")

    url, method, headers, timeout, _ = fetch.calls[0]
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

    _, _, headers, _, _ = fetch.calls[0]
    assert headers["Content-Type"] == "application/json"


def test_request_bodies_reach_the_fetch_seam_verbatim() -> None:
    # Regression guard: the seam carries the caller's request body — a
    # POST/PUT/PATCH operation must not silently send an empty request.
    fetch = FakeFetcher([(201, {}, b"{}")])
    payload = b'{"title": "the exact bytes reach the seam"}'

    _client(fetch).request("/test/path", method="POST", authorization="Bearer t", body=payload)

    assert fetch.calls[0][4] == payload


def test_bodyless_calls_present_no_body_to_the_fetch_seam() -> None:
    fetch = FakeFetcher([(200, {}, b"{}")])

    _client(fetch).request("/test/path", method="GET", authorization="Bearer t")

    assert fetch.calls[0][4] is None


def test_the_real_fetch_construction_carries_the_request_body() -> None:
    # Regression guard against the real urllib wiring: the constructed
    # request transmits the supplied body as its data.
    request = _github_request_for(
        "https://api.github.com/test", "POST", {"Accept": "application/json"}, b'{"a": 1}'
    )

    assert request.data == b'{"a": 1}'
    assert request.get_method() == "POST"


def test_the_real_fetch_construction_is_bodyless_without_a_body() -> None:
    request = _github_request_for("https://api.github.com/test", "GET", {}, None)

    assert request.data is None
    assert request.get_method() == "GET"


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
        (429, GitHubRateLimitedError),
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


@pytest.mark.parametrize(
    ("status", "headers"),
    [
        (403, {"Retry-After": "60"}),
        (429, {"Retry-After": "60"}),
        (429, {"X-RateLimit-Remaining": "42", "Retry-After": "30"}),
        (403, {"retry-after": "30"}),
    ],
)
def test_secondary_rate_limits_classify_apart_from_authorization(
    status: int, headers: dict[str, str]
) -> None:
    # Regression guard: a documented secondary rate limit answers with a
    # Retry-After header while the primary budget is not exhausted — it must
    # classify as a rate limit, never as lost authorization.
    fetch = FakeFetcher([(status, headers, _SECRET_BODY)])

    with pytest.raises(GitHubRateLimitedError):
        _client(fetch).request("/x", method="GET", authorization="Bearer t")


def test_secondary_403_without_retry_after_is_recognized_from_the_documented_marker() -> None:
    # Regression guard for the remaining documented form: a secondary rate
    # limit can answer 403 with neither Retry-After nor an exhausted primary
    # budget, documented only by the error message. The bounded body-marker
    # check recognizes it, so it can never be promoted to authorization loss.
    fetch = FakeFetcher(
        [
            (
                403,
                {"X-RateLimit-Remaining": "4747"},
                b'{"message": "You have exceeded a secondary rate limit"}',
            )
        ]
    )

    with pytest.raises(GitHubRateLimitedError):
        _client(fetch).request("/x", method="GET", authorization="Bearer t")


def test_abuse_detection_marker_is_recognized_as_a_rate_limit() -> None:
    fetch = FakeFetcher(
        [
            (
                403,
                {},
                b'{"message": "You have exceeded an abuse detection mechanism"}',
            )
        ]
    )

    with pytest.raises(GitHubRateLimitedError):
        _client(fetch).request("/x", method="GET", authorization="Bearer t")


def test_secondary_limit_marker_content_never_enters_the_error_message() -> None:
    # The marker check is boolean-only: the provider body (secret-looking or
    # not) is never echoed into the raised error.
    body = b'{"message": "You have exceeded a secondary rate limit ghs_secret_value"}'
    fetch = FakeFetcher([(403, {"X-RateLimit-Remaining": "4747"}, body)])

    with pytest.raises(GitHubRateLimitedError) as caught:
        _client(fetch).request("/x", method="GET", authorization="Bearer t")

    assert "ghs_secret_value" not in str(caught.value)


def test_a_standard_403_denial_body_remains_an_authorization_rejection() -> None:
    # A 403 carrying GitHub's standard authorization-denial message (no
    # secondary-limit marker, no rate-limit headers) is affirmative evidence
    # of denial and keeps the classified authorization rejection.
    fetch = FakeFetcher(
        [
            (
                403,
                {"X-RateLimit-Remaining": "4747"},
                b'{"message": "Resource not accessible by integration"}',
            )
        ]
    )

    with pytest.raises(GitHubAuthorizationRejectedError):
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


@pytest.mark.parametrize(
    "path",
    [
        # Hostname-prefix confusion: the string shares the base-URL prefix
        # but resolves to a foreign origin.
        "https://api.github.com.evil.example/steal",
        # Foreign userinfo embedding the GitHub hostname.
        "https://api.github.com@evil.example/steal",
        # Downgraded scheme.
        "http://api.github.com/steal",
        # Foreign port.
        "https://api.github.com:8443/steal",
    ],
)
def test_origin_confusion_targets_never_reach_the_fetch_seam(path: str) -> None:
    # Regression guard: parsed-origin validation replaces the string-prefix
    # check, so no URL that merely shares the base-URL prefix — and no URL
    # with foreign userinfo — can ever receive the Authorization credential.
    fetch = FakeFetcher([(200, {}, b"{}")])

    with pytest.raises(ConfigurationError):
        _client(fetch).request(path, method="GET", authorization="Bearer t")

    assert fetch.calls == []


def test_same_origin_absolute_targets_are_accepted() -> None:
    # An explicit default port is the same origin as the implicit one.
    fetch = FakeFetcher([(200, {}, b"{}")])

    _client(fetch).request(
        "https://api.github.com:443/installation/repositories?page=2",
        method="GET",
        authorization="Bearer t",
    )

    assert fetch.calls[0][0] == "https://api.github.com:443/installation/repositories?page=2"


@pytest.mark.parametrize(
    "url",
    [
        "https://api.github.com/x",
        "https://api.github.com:443/x",
        "https://api.github.com.evil.example/steal",
        "https://api.github.com@evil.example/steal",
        "https://api.github.com:not-a-port/steal",
        "https://api.github.com:99999/steal",
        "https://[::1/steal",
        "https://[::1]:99999/steal",
        "",
    ],
)
def test_is_github_api_origin_is_total_and_never_raises(url: str) -> None:
    # Regression guard: URL parsing and port access raise ValueError for
    # malformed authorities; the origin predicate must classify them as
    # "not the GitHub origin" instead of escaping a raw parser error.
    assert isinstance(is_github_api_origin(url), bool)


@pytest.mark.parametrize(
    "path",
    [
        "https://api.github.com:not-a-port/steal",
        "https://api.github.com:99999/steal",
        "https://[::1/steal",
        "https://[::1]:99999/steal",
    ],
)
def test_malformed_target_authorities_are_rejected_without_reaching_the_seam(
    path: str,
) -> None:
    # Regression guard: a malformed direct target classifies as
    # ConfigurationError and the fetch seam is never invoked.
    fetch = FakeFetcher([(200, {}, b"{}")])

    with pytest.raises(ConfigurationError):
        _client(fetch).request(path, method="GET", authorization="Bearer t")

    assert fetch.calls == []


@pytest.mark.parametrize("timeout", [0, -1, "10", True, None])
def test_invalid_timeouts_fail_fast_at_construction(timeout: Any) -> None:
    with pytest.raises(ConfigurationError):
        HttpGitHubRestClient(timeout_seconds=timeout)  # type: ignore[arg-type]


# --- Safe bounded redirect policy (issue #122) -------------------------------


class _FakeFp:
    """Minimal file-like object for urllib's redirect machinery."""

    def read(self, *args: object) -> bytes:
        return b""

    def close(self) -> None:
        return None


class _RedirectChainOpener:
    """Drive urllib's redirect handler over scripted responses — no network.

    Mimics the two opener behaviors the handler interacts with: every request
    the handler constructs is recorded (so destination/credential assertions
    are exact), and a 3xx the handler refuses surfaces as HTTPError exactly
    like HTTPErrorProcessor raises it in the real opener. The seam arguments
    urllib types strictly (``IO[bytes]``, ``HTTPMessage``) stand in as
    ``Any`` — the policy decision and the loop mechanics never read them.
    """

    def __init__(
        self,
        handler: _GitHubApiRedirectHandler,
        responses: list[tuple[int, Any]],
    ) -> None:
        handler.add_parent(self)  # type: ignore[arg-type]  # the fake stands in for OpenerDirector
        self.handler = handler
        self.responses = list(responses)
        self.requests: list[urllib.request.Request] = []

    def open(self, req: urllib.request.Request, timeout: float | None = None) -> object:
        req.timeout = timeout  # OpenerDirector.open sets this before handlers run.
        self.requests.append(req)
        status, headers = self.responses.pop(0)
        if status in (301, 302, 303, 307, 308):
            fp: Any = _FakeFp()
            result = self.handler.http_error_302(req, fp, status, "Moved", headers)
            if result is None:
                raise urllib.error.HTTPError(req.full_url, status, "Moved", headers, fp)
            return result
        return ("answered", status, headers)


def _policy_decision(
    handler: _GitHubApiRedirectHandler,
    request: urllib.request.Request,
    code: int,
    location: str,
) -> object:
    """Invoke the redirect policy decision point with stand-in seam arguments.

    The policy decision (and urllib's own ``redirect_request``) reads neither
    the file-like ``fp`` nor the ``headers`` mapping — only the request and
    the resolved target — so the stand-ins stay honest.
    """
    fp: Any = _FakeFp()
    headers: Any = {"Location": location}
    return handler.redirect_request(request, fp, code, "Moved", headers, location)


def test_a_same_origin_redirect_policy_returns_the_redirected_request() -> None:
    request = _github_request_for(
        f"{GITHUB_API_BASE_URL}/repos/octocat/Hello-World",
        "GET",
        {"Authorization": "Bearer t"},
        None,
    )
    handler = _GitHubApiRedirectHandler()
    location = f"{GITHUB_API_BASE_URL}/repos/octocat/hello-world"

    redirected = _policy_decision(handler, request, 301, location)

    assert isinstance(redirected, urllib.request.Request)
    assert redirected.full_url == location
    assert redirected.get_method() == "GET"
    # urllib copies request headers into the redirected request; the policy
    # makes that safe by refusing every target off the trusted origin.
    assert redirected.headers["Authorization"] == "Bearer t"


def test_a_same_origin_308_redirect_is_followed_for_reads() -> None:
    # The pinned stdlib supports 308 for GET/HEAD; the policy allows it on
    # the trusted origin like the other documented redirect statuses.
    request = _github_request_for(f"{GITHUB_API_BASE_URL}/x", "GET", {}, None)
    handler = _GitHubApiRedirectHandler()
    location = f"{GITHUB_API_BASE_URL}/moved"

    redirected = _policy_decision(handler, request, 308, location)

    assert isinstance(redirected, urllib.request.Request)
    assert redirected.full_url == location


def test_a_same_origin_redirect_is_followed_with_the_credential_intact() -> None:
    handler = _GitHubApiRedirectHandler()
    opener = _RedirectChainOpener(
        handler,
        [
            (302, {"location": f"{GITHUB_API_BASE_URL}/repos/octocat/hello-world?before=move"}),
            (200, {}),
        ],
    )
    request = _github_request_for(
        f"{GITHUB_API_BASE_URL}/repos/octocat/Hello-World",
        "GET",
        {"Authorization": "Bearer ghs_token", "Accept": GITHUB_JSON_ACCEPT_HEADER},
        None,
    )

    result = opener.open(request, timeout=5.0)

    assert result == ("answered", 200, {})
    followed = opener.requests[1]
    assert followed.full_url == f"{GITHUB_API_BASE_URL}/repos/octocat/hello-world?before=move"
    assert followed.get_method() == "GET"
    assert followed.headers["Authorization"] == "Bearer ghs_token"


def test_a_head_request_redirect_preserves_the_head_method() -> None:
    handler = _GitHubApiRedirectHandler()
    opener = _RedirectChainOpener(
        handler, [(301, {"location": f"{GITHUB_API_BASE_URL}/x"}), (200, {})]
    )
    request = _github_request_for(f"{GITHUB_API_BASE_URL}/x", "HEAD", {}, None)

    result = opener.open(request)

    assert result == ("answered", 200, {})
    assert opener.requests[1].get_method() == "HEAD"


def test_bounded_multi_hop_same_origin_redirects_are_followed() -> None:
    handler = _GitHubApiRedirectHandler()
    second = f"{GITHUB_API_BASE_URL}/installation/repositories?page=2"
    third = f"{GITHUB_API_BASE_URL}/installation/repositories?page=3"
    opener = _RedirectChainOpener(
        handler,
        [(302, {"location": second}), (302, {"location": third}), (200, {})],
    )
    request = _github_request_for(
        f"{GITHUB_API_BASE_URL}/installation/repositories", "GET", {}, None
    )

    result = opener.open(request)

    assert result == ("answered", 200, {})
    assert [req.full_url for req in opener.requests] == [
        f"{GITHUB_API_BASE_URL}/installation/repositories",
        second,
        third,
    ]


def test_excess_redirects_fail_closed_as_an_uncertain_answer() -> None:
    handler = _GitHubApiRedirectHandler()
    targets = [f"{GITHUB_API_BASE_URL}/hop/{n}" for n in range(2, 7)]
    responses = [(302, {"location": target}) for target in targets] + [(200, {})]
    opener = _RedirectChainOpener(handler, responses)
    request = _github_request_for(f"{GITHUB_API_BASE_URL}/hop/1", "GET", {}, None)

    with pytest.raises(urllib.error.HTTPError):
        opener.open(request)

    # The followed-redirect bound (3) tripped before the scripted terminal
    # answer; the excess 3xx surfaces for the uncertain classification.
    assert len(opener.requests) == 4


def test_a_redirect_loop_fails_closed() -> None:
    handler = _GitHubApiRedirectHandler()
    repeat = f"{GITHUB_API_BASE_URL}/loop"
    opener = _RedirectChainOpener(handler, [(302, {"location": repeat})] * 3 + [(200, {})])
    request = _github_request_for(f"{GITHUB_API_BASE_URL}/loop", "GET", {}, None)

    with pytest.raises(urllib.error.HTTPError) as caught:
        opener.open(request)

    assert "infinite loop" in str(caught.value)
    assert len(opener.requests) == 3


def test_a_cross_origin_redirect_never_constructs_a_redirected_request() -> None:
    handler = _GitHubApiRedirectHandler()
    opener = _RedirectChainOpener(
        handler,
        [(302, {"location": "https://evil.example/steal"}), (200, {})],
    )
    request = _github_request_for(
        f"{GITHUB_API_BASE_URL}/x", "GET", {"Authorization": "Bearer ghs_token"}, None
    )

    with pytest.raises(urllib.error.HTTPError) as caught:
        opener.open(request)

    assert caught.value.code == 302
    # No redirected request was ever constructed: the credential never left
    # the trusted origin.
    assert len(opener.requests) == 1


@pytest.mark.parametrize(
    "location",
    [
        "https://api.github.com.evil.example/steal",
        "https://api.github.com@evil.example/steal",
        "http://api.github.com/steal",
        "https://api.github.com:8443/steal",
        "https://evil.example/steal",
        "relative/path",
        "",
        "https://[::1/steal",
    ],
)
def test_disallowed_redirect_targets_refuse_without_a_redirected_request(
    location: str,
) -> None:
    request = _github_request_for(
        f"{GITHUB_API_BASE_URL}/x", "GET", {"Authorization": "Bearer t"}, None
    )
    handler = _GitHubApiRedirectHandler()

    assert _policy_decision(handler, request, 302, location) is None


@pytest.mark.parametrize("method", ["POST", "PUT", "PATCH", "DELETE"])
def test_mutating_requests_never_follow_redirects(method: str) -> None:
    # A redirect answer to a consequential operation is an uncertain outcome,
    # never an automatic replay of the mutation against a new target.
    request = _github_request_for(f"{GITHUB_API_BASE_URL}/x", method, {}, None)
    handler = _GitHubApiRedirectHandler()
    location = f"{GITHUB_API_BASE_URL}/moved"

    assert _policy_decision(handler, request, 302, location) is None


def test_the_redirect_bounds_are_tighter_than_urllib_defaults() -> None:
    assert (
        _GitHubApiRedirectHandler.max_redirections
        < urllib.request.HTTPRedirectHandler.max_redirections
    )
    assert _GitHubApiRedirectHandler.max_repeats < urllib.request.HTTPRedirectHandler.max_repeats


# --- Normalized rate-limit scheduling facts (issue #122) ---------------------


def test_retry_after_normalizes_onto_the_rate_limit_error() -> None:
    fetch = FakeFetcher([(429, {"Retry-After": "17"}, _SECRET_BODY)])

    with pytest.raises(GitHubRateLimitedError) as caught:
        _client(fetch).request("/x", method="GET", authorization="Bearer t")

    assert caught.value.retry_after_seconds == 17.0
    assert caught.value.rate_limit_reset_at is None


def test_x_rate_limit_reset_normalizes_onto_the_rate_limit_error() -> None:
    fetch = FakeFetcher(
        [(403, {"X-RateLimit-Remaining": "0", "X-RateLimit-Reset": "1893456000"}, _SECRET_BODY)]
    )

    with pytest.raises(GitHubRateLimitedError) as caught:
        _client(fetch).request("/x", method="GET", authorization="Bearer t")

    assert caught.value.rate_limit_reset_at == datetime.fromtimestamp(1893456000, tz=UTC)
    assert caught.value.retry_after_seconds is None


def test_rate_limit_without_timing_headers_normalizes_to_none() -> None:
    fetch = FakeFetcher([(429, {"X-RateLimit-Remaining": "0"}, _SECRET_BODY)])

    with pytest.raises(GitHubRateLimitedError) as caught:
        _client(fetch).request("/x", method="GET", authorization="Bearer t")

    assert caught.value.retry_after_seconds is None
    assert caught.value.rate_limit_reset_at is None


@pytest.mark.parametrize(
    "value",
    [
        "soon",
        "Wed, 21 Oct 2015 07:28:00 GMT",  # the HTTP-date form: absent, not guessed
        "-5",
        "1.5",
        "1e3",
        "",
    ],
)
def test_malformed_retry_after_values_normalize_to_none(value: str) -> None:
    fetch = FakeFetcher([(429, {"Retry-After": value}, _SECRET_BODY)])

    with pytest.raises(GitHubRateLimitedError) as caught:
        _client(fetch).request("/x", method="GET", authorization="Bearer t")

    assert caught.value.retry_after_seconds is None
    assert caught.value.rate_limit_reset_at is None


def test_an_absurd_retry_after_horizon_normalizes_to_none() -> None:
    fetch = FakeFetcher([(429, {"Retry-After": "9" * 25}, _SECRET_BODY)])

    with pytest.raises(GitHubRateLimitedError) as caught:
        _client(fetch).request("/x", method="GET", authorization="Bearer t")

    assert caught.value.retry_after_seconds is None


@pytest.mark.parametrize(
    "value", ["abc", "2026-09-26T00:00:00Z", "-1", "1.5", "1e10", "", "9" * 25]
)
def test_malformed_or_absurd_reset_values_normalize_to_none(value: str) -> None:
    fetch = FakeFetcher([(429, {"X-RateLimit-Reset": value}, _SECRET_BODY)])

    with pytest.raises(GitHubRateLimitedError) as caught:
        _client(fetch).request("/x", method="GET", authorization="Bearer t")

    assert caught.value.rate_limit_reset_at is None
    assert caught.value.retry_after_seconds is None


def test_rate_limit_timing_headers_never_enter_the_error_text() -> None:
    fetch = FakeFetcher(
        [(429, {"Retry-After": "17", "X-RateLimit-Reset": "1893456000"}, _SECRET_BODY)]
    )

    with pytest.raises(GitHubRateLimitedError) as caught:
        _client(fetch).request("/x", method="GET", authorization="Bearer t")

    assert "17" not in str(caught.value)
    assert "1893456000" not in str(caught.value)
    assert "1893456000" not in repr(caught.value)
    assert "ghs_secret_token_value" not in str(caught.value)


def test_a_rate_limited_request_performs_exactly_one_fetch_call() -> None:
    # The adapter never sleeps, queues, or automatically retries: the
    # classified answer surfaces once through the seam and scheduling policy
    # stays above the adapter.
    fetch = FakeFetcher([(429, {"Retry-After": "17"}, _SECRET_BODY)])

    with pytest.raises(GitHubRateLimitedError):
        _client(fetch).request("/x", method="GET", authorization="Bearer t")

    assert len(fetch.calls) == 1
