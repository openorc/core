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
- The response body is never echoed into errors, logs, or span attributes;
  classification uses the status code and (for rate limits) response
  headers only.

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

# The fetch seam: one bounded HTTP call returning status, response headers,
# and body. Response headers are part of the seam because documented
# pagination (Link headers) and rate-limit exhaustion surface as headers.
GitHubFetcher = Callable[[str, str, Mapping[str, str], float], tuple[int, Mapping[str, str], bytes]]

# Rate-limit exhaustion is signaled by a 403/429 answer with an exhausted
# X-RateLimit-Remaining budget (GitHub rate-limit documentation).
_RATE_LIMITED_STATUSES = frozenset({403, 429})


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


def http_fetch(
    url: str, method: str, headers: Mapping[str, str], timeout_seconds: float
) -> tuple[int, Mapping[str, str], bytes]:
    """The real GitHub fetch seam: one bounded urllib request.

    Non-2xx answers return as ``(status, headers, b"")`` so the caller
    classifies the outcome; the error body is deliberately discarded so no
    provider content can leak into errors or logs. Transport-level failures
    (timeout, connection loss) propagate as exceptions the caller maps to
    the uncertain-outcome classification.
    """
    request = urllib.request.Request(url, data=None, method=method)
    for name, value in headers.items():
        request.add_header(name, value)
    opener = urllib.request.build_opener(_NoRedirectHandler)
    try:
        with opener.open(request, timeout=timeout_seconds) as response:
            return response.status, dict(response.headers.items()), response.read()
    except urllib.error.HTTPError as exc:
        # A definitive HTTP answer, including 3xx (the handler refuses to
        # follow redirects, surfacing them here): classify from the status.
        return exc.code, dict(exc.headers.items()), b""


def _header_value(headers: Mapping[str, str], name: str) -> str | None:
    """Case-insensitive header lookup for provider response headers."""
    lowered = name.lower()
    for key, value in headers.items():
        if key.lower() == lowered:
            return value
    return None


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
                url, method, headers, self._timeout_seconds
            )
        except GitHubOutcomeUncertainError:
            raise
        except Exception as exc:  # transport-level failure: the outcome is unknown
            raise GitHubOutcomeUncertainError(
                "the outcome of the GitHub request is unknown: the transport failed"
            ) from exc

        if 200 <= status < 300:
            return GitHubHttpResponse(status=status, headers=response_headers, body=response_body)
        remaining = _header_value(response_headers, "X-RateLimit-Remaining")
        if status in _RATE_LIMITED_STATUSES and remaining == "0":
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
        if path.startswith(GITHUB_API_BASE_URL):
            return path
        if not path.startswith("/"):
            raise ConfigurationError(
                "a GitHub request path must be an absolute path under the API base URL"
            )
        return GITHUB_API_BASE_URL + path
