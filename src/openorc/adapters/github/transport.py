"""GitHub REST transport mechanics (issue #58).

The focused HTTP transport for the OpenOrc GitHub App adapter boundary. It
owns the provider-native request mechanics — URL construction, the
``Accept``/``X-GitHub-Api-Version``/``Authorization``/``User-Agent`` header
contract, bounded timeouts, redirect refusal, and outcome classification —
and nothing else: normalization into OpenOrc facts, capability semantics,
installation routing, and all workflow meaning belong to the modules above
and the services above the adapter.

Centralized API versioning: :data:`SUPPORTED_GITHUB_API_VERSION` is the single
location pinning the supported GitHub REST API version, sent on every request
through the ``X-GitHub-Api-Version`` header (GitHub REST documentation). The
value is the most recent GA REST API version at implementation time,
verified against GitHub's API-versions documentation.

Transport hardening:

- Redirects are never followed. ``urllib`` copies non-content headers into
  redirected requests, so a followed redirect could forward the
  ``Authorization`` credential to another host. A redirect that is not
  followed surfaces as an unclassified 3xx and classifies as an uncertain
  outcome — never success, never a safe replay.
- Every request carries a bounded timeout; the fetch seam receives it so a
  stalled connection cannot hang the calling process.
- Failure classification uses the status code, rate-limit response headers,
  and — solely to recognize GitHub's documented secondary-rate-limit
  markers — a bounded prefix of the response body. The body content itself
  is never stored, echoed into errors, logs, or span attributes, or
  exported in any form; the marker check returns a boolean only.

The low-level fetch seam is deliberately header-retaining: a
``(status, headers, body)`` result is required by the documented
``Link``-header pagination used by the concrete installation-repositories
listing operation.
"""

from __future__ import annotations

import logging
import urllib.error
import urllib.request
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from urllib.parse import urlparse

from openorc.adapters.github.errors import (
    GitHubAuthenticationRejectedError,
    GitHubAuthorizationRejectedError,
    GitHubOutcomeUncertainError,
    GitHubRateLimitedError,
    GitHubRequestRejectedError,
)
from openorc.config import ConfigurationError

__all__ = [
    "DEFAULT_GITHUB_REQUEST_TIMEOUT_SECONDS",
    "GITHUB_API_BASE_URL",
    "GITHUB_JSON_ACCEPT_HEADER",
    "SUPPORTED_GITHUB_API_VERSION",
    "GitHubFetcher",
    "GitHubHttpResponse",
    "HttpGitHubRestClient",
    "http_fetch",
    "is_github_api_origin",
]

logger = logging.getLogger(__name__)
# The GitHub REST API root. GHES deployments are not a v1 OpenOrc surface.
GITHUB_API_BASE_URL = "https://api.github.com"

# The supported GitHub REST API version, pinned in exactly this one location
# and sent on every request through the X-GitHub-Api-Version header. The
# value is the most recent GA REST API version at implementation time,
# verified against GitHub's API-versions documentation.
SUPPORTED_GITHUB_API_VERSION = "2026-03-10"

# GitHub recommends the JSON media type on every REST request.
GITHUB_JSON_ACCEPT_HEADER = "application/vnd.github+json"

# A GitHub API request must always carry a User-Agent; the value identifies
# the integration and carries no secret material.
GITHUB_USER_AGENT = "OpenOrc"

# Bounded timeout for one GitHub HTTP request.
DEFAULT_GITHUB_REQUEST_TIMEOUT_SECONDS = 10.0

# The fetch seam: one bounded HTTP call carrying an optional request body,
# returning status, response headers, and body. Response headers are part of
# the seam because documented pagination (Link headers) and rate-limit
# exhaustion surface as headers; the request body is part of the seam
# because later GitHub operations send JSON payloads.
GitHubFetcher = Callable[
    [str, str, Mapping[str, str], float, "bytes | None"],
    tuple[int, Mapping[str, str], bytes],
]

# Rate-limit exhaustion is signaled by a 403/429 answer with an exhausted
# X-RateLimit-Remaining budget (the primary limit) or with a Retry-After
# header (the documented secondary-limit signal, which may be present while
# the primary budget is not exhausted) — GitHub rate-limit documentation.
# A secondary limit can also answer without Retry-After, documented only by
# the response error message; the bounded body-marker check below recognizes
# that form without exposing any provider content.
_RATE_LIMITED_STATUSES = frozenset({403, 429})

# A bounded prefix of a non-2xx body is retained solely for the classifier
# to recognize GitHub's documented secondary-rate-limit markers. The content
# is never stored, echoed, or exported.
_ERROR_BODY_CLASSIFICATION_LIMIT_BYTES = 4096

# The stable documented phrases GitHub uses for secondary rate limits
# (GitHub rate-limit documentation). Matched case-insensitively; no other
# body content is ever inspected or retained.
_SECONDARY_LIMIT_BODY_MARKERS = ("secondary rate limit", "abuse detection mechanism")


@dataclass(frozen=True, slots=True)
class GitHubHttpResponse:
    """One answered GitHub HTTP response (status, headers, body).

    The body is retained only for 2xx-answer parsing by the modules above;
    failure classification deliberately discards it so no provider content
    can leak into errors or logs.
    """

    status: int
    headers: Mapping[str, str]
    body: bytes


class _NoRedirectHandler(urllib.request.HTTPRedirectHandler):
    """Refuse all redirects: never forward Authorization across hosts."""

    def redirect_request(self, req, fp, code, msg, headers, newurl):  # type: ignore[no-untyped-def]
        return None


def _github_request_for(
    url: str, method: str, headers: Mapping[str, str], body: bytes | None
) -> urllib.request.Request:
    """Construct the urllib request for one GitHub call, carrying the body.

    The request body — when the caller supplied one — is transmitted as the
    request data; a body-less call transmits nothing.
    """
    request = urllib.request.Request(url, data=body, method=method)
    for name, value in headers.items():
        request.add_header(name, value)
    return request


def http_fetch(
    url: str,
    method: str,
    headers: Mapping[str, str],
    timeout_seconds: float,
    body: bytes | None,
) -> tuple[int, Mapping[str, str], bytes]:
    """The real GitHub fetch seam: one bounded urllib request.

    The caller-supplied request body (when present) is transmitted verbatim
    as the request data. Non-2xx answers return with a bounded error-body
    prefix so the caller's classifier can recognize GitHub's documented
    secondary-rate-limit markers; error content is never stored, echoed, or
    exported beyond that boolean check. Transport-level failures (timeout,
    connection loss) propagate as exceptions the caller maps to the
    uncertain-outcome classification.
    """
    request = _github_request_for(url, method, headers, body)
    opener = urllib.request.build_opener(_NoRedirectHandler)
    try:
        with opener.open(request, timeout=timeout_seconds) as response:
            return response.status, dict(response.headers.items()), response.read()
    except urllib.error.HTTPError as exc:
        # A definitive HTTP answer, including 3xx (the handler refuses to
        # follow redirects, surfacing them here): classify from the status.
        return exc.code, dict(exc.headers.items()), exc.read(_ERROR_BODY_CLASSIFICATION_LIMIT_BYTES)


def _body_reports_secondary_rate_limit(body: bytes) -> bool:
    """Whether a bounded non-2xx body carries GitHub's documented secondary-limit marker.

    Content-safe by construction: the check decodes a bounded prefix, matches
    only the stable documented phrases, and returns a boolean — no provider
    content is ever stored, echoed into errors, or exported.
    """
    if not body:
        return False
    text = body[:_ERROR_BODY_CLASSIFICATION_LIMIT_BYTES].decode("utf-8", "ignore").lower()
    return any(marker in text for marker in _SECONDARY_LIMIT_BODY_MARKERS)


def _header_value(headers: Mapping[str, str], name: str) -> str | None:
    """Case-insensitive header lookup for provider response headers."""
    lowered = name.lower()
    for key, value in headers.items():
        if key.lower() == lowered:
            return value
    return None


def is_github_api_origin(url: str) -> bool:
    """Whether an absolute URL targets the exact HTTPS GitHub API origin.

    Total for arbitrary strings: URL parsing and port access can raise
    ``ValueError`` for malformed authorities (an unparseable port, an
    out-of-range port, a malformed bracketed host) — a malformed target is
    not the GitHub API origin. Parsed-origin validation, never a
    string-prefix check: a hostname that merely shares the base-URL prefix
    (``https://api.github.com.evil.example``) or embeds foreign userinfo
    (``https://api.github.com@evil.example``) is not the GitHub API origin
    and can never carry the Authorization credential across the credential
    boundary.
    """
    try:
        parsed = urlparse(url)
        # ``port`` parses lazily: an unparseable or out-of-range port raises
        # ``ValueError`` here, which classifies as "not the GitHub origin".
        port = parsed.port
    except ValueError:
        return False
    return (
        parsed.scheme == "https"
        and parsed.hostname == "api.github.com"
        and parsed.username is None
        and parsed.password is None
        and port in (None, 443)
    )


class HttpGitHubRestClient:
    """Low-level GitHub REST transport with centralized outcome classification.

    Constructed with the bounded request timeout and an injectable fetch seam
    (tests substitute deterministic fakes; production uses :func:`http_fetch`).
    Every call presents a full ``Authorization`` header value supplied by the
    caller — the client never owns or derives credentials.
    """

    def __init__(
        self,
        *,
        timeout_seconds: float = DEFAULT_GITHUB_REQUEST_TIMEOUT_SECONDS,
        fetch: GitHubFetcher | None = None,
    ) -> None:
        if isinstance(timeout_seconds, bool) or not isinstance(timeout_seconds, (int, float)):
            raise ConfigurationError("the GitHub request timeout must be a positive number")
        if timeout_seconds <= 0:
            raise ConfigurationError("the GitHub request timeout must be a positive number")
        self._timeout_seconds = float(timeout_seconds)
        self._fetch = fetch if fetch is not None else http_fetch

    @property
    def request_timeout_seconds(self) -> float:
        """Bounded timeout of one GitHub HTTP request."""
        return self._timeout_seconds

    def request(
        self,
        path: str,
        *,
        method: str,
        authorization: str,
        body: bytes | None = None,
    ) -> GitHubHttpResponse:
        """Perform one classified GitHub REST request.

        ``path`` is either an absolute path under :data:`GITHUB_API_BASE_URL`
        or a full API URL (documented ``Link`` pagination targets); a target
        outside the API base classifies as an uncertain outcome rather than
        silently moving the credential boundary. 2xx answers return the
        header-retaining response; definitive rejections and uncertain
        outcomes raise the adapter-local error boundary.
        """
        url = self._url_for(path)
        headers: dict[str, str] = {
            "Accept": GITHUB_JSON_ACCEPT_HEADER,
            "Authorization": authorization,
            "User-Agent": GITHUB_USER_AGENT,
            "X-GitHub-Api-Version": SUPPORTED_GITHUB_API_VERSION,
        }
        if body is not None:
            headers["Content-Type"] = "application/json"
        try:
            status, response_headers, response_body = self._fetch(
                url, method, headers, self._timeout_seconds, body
            )
        except GitHubOutcomeUncertainError:
            raise
        except Exception as exc:  # transport-level failure: the outcome is unknown
            raise GitHubOutcomeUncertainError(
                "the outcome of the GitHub request is unknown: the transport failed"
            ) from exc

        if 200 <= status < 300:
            return GitHubHttpResponse(status=status, headers=response_headers, body=response_body)
        # GitHub signals the primary rate limit with an exhausted
        # X-RateLimit-Remaining budget and secondary rate limits with a
        # Retry-After header or a documented secondary-limit error message
        # (which can be present while the primary budget is not exhausted and
        # Retry-After is absent). Any of these on a 403/429 — and the 429
        # status itself — is a known rate limit, deliberately NOT an
        # authorization absence.
        if status in _RATE_LIMITED_STATUSES and (
            status == 429
            or _header_value(response_headers, "X-RateLimit-Remaining") == "0"
            or _header_value(response_headers, "Retry-After") is not None
            or _body_reports_secondary_rate_limit(response_body)
        ):
            raise GitHubRateLimitedError(f"the GitHub request was rate limited (status {status})")
        if status == 401:
            raise GitHubAuthenticationRejectedError(
                f"GitHub rejected the presented credential (status {status})"
            )
        if status in (403, 404):
            raise GitHubAuthorizationRejectedError(
                f"GitHub denied access to the addressed resource (status {status})"
            )
        if 400 <= status < 500:
            raise GitHubRequestRejectedError(f"GitHub rejected the request (status {status})")
        # 3xx (redirects are never followed) and 5xx: the outcome is unknown.
        raise GitHubOutcomeUncertainError(
            f"the outcome of the GitHub request is unknown (status {status})"
        )

    def _url_for(self, path: str) -> str:
        if not isinstance(path, str) or not path:
            raise ConfigurationError("a GitHub request path must be a non-empty string")
        if path.startswith("/"):
            return GITHUB_API_BASE_URL + path
        # An absolute target (a documented Link pagination target) must
        # target the exact HTTPS GitHub API origin — validated by parsing,
        # never by string prefix — so the Authorization credential can never
        # cross the credential boundary.
        if is_github_api_origin(path):
            return path
        raise ConfigurationError(
            "a GitHub request target outside the exact HTTPS GitHub API "
            "origin is rejected: the credential boundary is never crossed"
        )
