"""GitHub App user-to-server token mechanics (issue #142).

The focused web-flow mechanics for the Profile-scoped GitHub App user
authorization: authorization-code exchange, refresh-token exchange, and the
authoritative current-user lookup under a user access token. Application
services own every workflow decision — the same-human binding proof, durable
refresh persistence, generation compare-and-swap — while this boundary owns
the provider-native request mechanics and outcome classification only.

Scope and safety:

- The exchange API accepts only post-correlation callback inputs. OAuth
  ``state`` correlation and session lifecycle against the authenticated
  Profile are the browser/API callback layer's responsibility (outside this
  leaf); an authorization code is never treated here as proof of Profile
  identity. The GitHub App web application flow supports PKCE: where the
  correlated authorization request carried ``code_challenge``/
  ``code_challenge_method=S256``, the exchange forwards the original
  ``code_verifier`` (GitHub's documented ``missing or incorrect
  code_verifier`` rejection classifies as a known rejection); challenge
  generation and verifier lifecycle remain callback-layer concerns.
- The deployment's GitHub App user-flow client ID and client secret are
  deployment/bootstrap secret material: supplied at construction, validated
  once (fail fast on missing/blank values), held in private fields with
  redacted ordinary representations, and never exposed through errors,
  logs, span attributes, or returned objects.
- The token endpoint is fixed to the documented HTTPS
  ``https://github.com/login/oauth/access_token`` URL; credentials travel in
  the form-encoded request body, never in a URL or query string, and the
  POST never follows redirects (credentials would cross the origin
  boundary). Every request carries a bounded timeout.
- Outcome classification is the discipline the services rely on: a parsed
  ``error`` payload or a 4xx answer is a known
  :class:`GitHubUserTokenRejectedError`; a definitive answer without the
  expiring-token capability is a known
  :class:`GitHubUserTokenRefreshCapabilityMissingError` (OpenOrc never
  persists a long-lived user access token as a second credential model);
  5xx, timeout/connection loss, and uninterpretable responses are
  :class:`GitHubOutcomeUncertainError`. Token and refresh values never
  enter errors, logs, or telemetry.
- The current-user lookup is the documented ``GET /user`` under the user
  access token through the shared REST transport (same safe redirect
  policy, API-version header, and outcome taxonomy as every other
  authenticated GitHub read).
"""

from __future__ import annotations

import json
import logging
import urllib.parse
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Protocol

from openorc.adapters.github.errors import (
    GitHubOutcomeUncertainError,
    GitHubUserTokenRefreshCapabilityMissingError,
    GitHubUserTokenRejectedError,
)
from openorc.adapters.github.transport import (
    DEFAULT_GITHUB_REQUEST_TIMEOUT_SECONDS,
    GitHubFetcher,
    HttpGitHubRestClient,
    http_fetch,
)
from openorc.config import ConfigurationError
from openorc.observability import annotate_span, application_span

__all__ = [
    "GITHUB_TOKEN_ENDPOINT_URL",
    "GitHubCurrentUser",
    "GitHubUserAccessToken",
    "GitHubUserRefreshSecret",
    "GitHubUserTokenClient",
    "GitHubUserTokenGrant",
    "HttpGitHubUserTokenClient",
]

# The documented GitHub App web-flow token endpoint (authorization-code
# exchange and refresh exchange). Credentials travel only in the request
# body; the URL is a fixed HTTPS constant, never caller-supplied.
GITHUB_TOKEN_ENDPOINT_URL = "https://github.com/login/oauth/access_token"


class GitHubUserAccessToken:
    """Secret-bearing in-memory carrier of one GitHub user access token.

    Ordinary representation is redacted; the value is reachable only through
    :meth:`token_value`, reserved for the trusted GitHub API call that needs
    it. The token lives only in the service's bounded in-memory cache —
    never in domain objects, persistence, events, logs, or telemetry.
    """

    __slots__ = ("_value", "_expires_at")

    def __init__(self, *, value: str, expires_at: datetime) -> None:
        self._value = value
        self._expires_at = expires_at

    def token_value(self) -> str:
        """Return the user access token for the trusted GitHub API call."""
        return self._value

    @property
    def expires_at(self) -> datetime:
        """The parsed absolute expiry instant of this token."""
        return self._expires_at

    def __repr__(self) -> str:
        return "GitHubUserAccessToken(<redacted>)"

    def __str__(self) -> str:
        return "GitHubUserAccessToken(<redacted>)"


class GitHubUserRefreshSecret:
    """Secret-bearing in-memory carrier of one GitHub refresh credential.

    Exists only inside trusted flows: between the adapter's normalized
    exchange answer and the trusted Vault write, and between the trusted
    Vault read and the refresh-exchange request construction. Ordinary
    representation is redacted; the value is reachable only through
    :meth:`secret_value`, reserved for those trusted calls. It is never
    copied into domain objects, DTOs, events, logs, telemetry, or any other
    structure.
    """

    __slots__ = ("_value", "_expires_at")

    def __init__(self, *, value: str, expires_at: datetime) -> None:
        self._value = value
        self._expires_at = expires_at

    def secret_value(self) -> str:
        """Return the refresh credential for the trusted Vault persistence call."""
        return self._value

    @property
    def expires_at(self) -> datetime:
        """The parsed absolute expiry instant of this refresh credential."""
        return self._expires_at

    def __repr__(self) -> str:
        return "GitHubUserRefreshSecret(<redacted>)"

    def __str__(self) -> str:
        return "GitHubUserRefreshSecret(<redacted>)"


@dataclass(frozen=True, slots=True)
class GitHubUserTokenGrant:
    """One normalized expiring user-token answer (initial or refreshed).

    The grant carries the rotating secret-bearing pair plus their parsed
    absolute expiry instants. It exists only inside the trusted service flow
    that either persists the refresh credential to Vault or installs a
    rotation.
    """

    access_token: GitHubUserAccessToken
    refresh_token: GitHubUserRefreshSecret


@dataclass(frozen=True, slots=True)
class GitHubCurrentUser:
    """The authoritative current-user observation under a user access token.

    ``github_user_id`` is the stable numeric identity the same-human binding
    proof compares against; ``login`` is mutable presentation metadata only.
    """

    github_user_id: int
    login: str | None


class GitHubUserTokenClient(Protocol):
    """Structural contract of the GitHub user-token web-flow boundary."""

    def exchange_authorization_code(
        self,
        code: str,
        *,
        redirect_uri: str | None = None,
        code_verifier: str | None = None,
    ) -> GitHubUserTokenGrant:
        """Exchange one post-correlation authorization code for a token grant.

        ``code_verifier`` is forwarded when the correlated authorization
        request used PKCE.
        """
        ...

    def refresh_user_token(self, refresh_token: GitHubUserRefreshSecret) -> GitHubUserTokenGrant:
        """Exchange one refresh credential for a fresh, rotated token grant.

        The secret-bearing carrier keeps the credential out of ordinary
        representations; the raw value is read only at request construction.
        """
        ...

    def fetch_authenticated_user(self, access_token: GitHubUserAccessToken) -> GitHubCurrentUser:
        """Resolve the authoritative current user under the user access token."""
        ...


# The clock seam used to convert GitHub's relative expiry durations into
# absolute instants; injectable for deterministic tests.
Clock = Callable[[], datetime]

_TOKEN_TRACER_SCOPE = "openorc.adapters.github.user_tokens"
_EXCHANGE_SPAN_NAME = "github.user_token_exchange"
_REFRESH_SPAN_NAME = "github.user_token_refresh"
_FETCH_USER_SPAN_NAME = "github.user_token_fetch_authenticated_user"

logger = logging.getLogger(__name__)


def _require_str(value: object, description: str) -> str:
    """Fail fast on a missing/blank deployment credential value."""
    if not isinstance(value, str) or value == "":
        raise ConfigurationError(f"the GitHub App user-flow {description} must be configured")
    return value


def _require_expiry_seconds(value: object, member: str) -> int:
    """Normalize one documented relative expiry duration (fail closed)."""
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise GitHubOutcomeUncertainError(
            f"the GitHub user token response is not interpretable: {member} is malformed"
        )
    return value


def _normalize_token_payload(payload: dict[str, object], now: datetime) -> GitHubUserTokenGrant:
    """Normalize one definitive, successful token answer into the typed grant.

    The documented expiring-token response carries the token pair plus both
    relative expiry durations. A definitive answer without the refresh
    capability is the known misconfiguration condition (the GitHub App has
    opted out of expiring user-to-server tokens), never a silently accepted
    long-lived token.
    """
    access_value = payload.get("access_token")
    refresh_value = payload.get("refresh_token")
    if not isinstance(access_value, str) or access_value == "":
        raise GitHubOutcomeUncertainError(
            "the GitHub user token response is not interpretable: the access token is missing"
        )
    if not isinstance(refresh_value, str) or refresh_value == "":
        # The documented non-expiring shape omits the refresh capability
        # (and its expiry) together: the known misconfiguration condition,
        # never a silently accepted long-lived token.
        raise GitHubUserTokenRefreshCapabilityMissingError(
            "the GitHub user token answer does not carry the required refresh capability"
        )
    access_expires_in = _require_expiry_seconds(payload.get("expires_in"), "expires_in")
    refresh_expires_in = _require_expiry_seconds(
        payload.get("refresh_token_expires_in"), "refresh_token_expires_in"
    )
    token_type = payload.get("token_type")
    if token_type != "bearer":
        raise GitHubOutcomeUncertainError(
            "the GitHub user token response is not interpretable: the token type is malformed"
        )
    return GitHubUserTokenGrant(
        access_token=GitHubUserAccessToken(
            value=access_value, expires_at=now + timedelta(seconds=access_expires_in)
        ),
        refresh_token=GitHubUserRefreshSecret(
            value=refresh_value, expires_at=now + timedelta(seconds=refresh_expires_in)
        ),
    )


def _classify_token_response(status: int, body: bytes, *, now: datetime) -> GitHubUserTokenGrant:
    """Classify one token-endpoint answer (see the module contract).

    GitHub's OAuth token endpoint documents its error shape as a JSON body
    that may be delivered even under a 200 status, so the parsed ``error``
    member — not the status alone — decides the known rejection; a 2xx
    answer without it must parse the full expiring-token grant or the
    outcome is unknown.
    """
    if 200 <= status < 300:
        try:
            payload = json.loads(body)
        except (ValueError, UnicodeDecodeError) as exc:
            raise GitHubOutcomeUncertainError(
                "the GitHub user token response is not interpretable"
            ) from exc
        if not isinstance(payload, dict):
            raise GitHubOutcomeUncertainError("the GitHub user token response is not interpretable")
        if "error" in payload:
            logger.warning(
                "the GitHub user token request was definitively rejected by the token endpoint"
            )
            raise GitHubUserTokenRejectedError(
                "the GitHub user token request was rejected",
                status_code=status,
            )
        return _normalize_token_payload(payload, now)
    if 400 <= status < 500:
        logger.warning(
            "the GitHub user token request was definitively rejected by the token endpoint"
        )
        raise GitHubUserTokenRejectedError(
            f"the GitHub user token request was rejected (status {status})",
            status_code=status,
        )
    raise GitHubOutcomeUncertainError(
        f"the outcome of the GitHub user token request is unknown (status {status})"
    )


def _parse_current_user_payload(body: bytes) -> GitHubCurrentUser:
    """Normalize the documented ``GET /user`` answer into the typed fact."""
    try:
        payload = json.loads(body)
    except (ValueError, UnicodeDecodeError) as exc:
        raise GitHubOutcomeUncertainError(
            "the GitHub current-user response is not interpretable"
        ) from exc
    if not isinstance(payload, dict):
        raise GitHubOutcomeUncertainError("the GitHub current-user response is not interpretable")
    user_id = payload.get("id")
    if isinstance(user_id, bool) or not isinstance(user_id, int) or user_id <= 0:
        raise GitHubOutcomeUncertainError(
            "the GitHub current-user response is not interpretable: the user id is malformed"
        )
    login = payload.get("login")
    return GitHubCurrentUser(
        github_user_id=user_id, login=login if isinstance(login, str) and login else None
    )


class HttpGitHubUserTokenClient:
    """HTTP GitHub user-token client for the web-flow boundary.

    Transport-neutral by construction: no FastAPI/RQ machinery, callable
    directly by the authorization service. The deployment's user-flow client
    ID and client secret are held in private fields with redacted ordinary
    representations; construction fails fast when either is missing or
    blank — the component that constructs the client is the one that must
    possess the credential.
    """

    def __init__(
        self,
        *,
        client_id: str,
        client_secret: str,
        fetch: GitHubFetcher | None = None,
        clock: Clock | None = None,
        request_timeout_seconds: float = DEFAULT_GITHUB_REQUEST_TIMEOUT_SECONDS,
    ) -> None:
        self._client_id = _require_str(client_id, "client ID")
        self._client_secret = _require_str(client_secret, "client secret")
        self._clock: Clock = clock if clock is not None else (lambda: datetime.now(UTC))
        self._rest = HttpGitHubRestClient(timeout_seconds=request_timeout_seconds, fetch=fetch)
        self._timeout_seconds = float(request_timeout_seconds)
        # The shared GitHub fetch seam: the token endpoint POST uses it with
        # its fixed documented URL and form-encoded body; the default stdlib
        # path never follows redirects for a mutating request carrying
        # credentials, and network failures classify as unknown outcomes at
        # the call site.
        self._fetch = fetch if fetch is not None else http_fetch

    @property
    def request_timeout_seconds(self) -> float:
        """Bounded timeout of one GitHub HTTP request."""
        return self._timeout_seconds

    def exchange_authorization_code(
        self,
        code: str,
        *,
        redirect_uri: str | None = None,
        code_verifier: str | None = None,
    ) -> GitHubUserTokenGrant:
        """Exchange one post-correlation authorization code (see the module contract)."""
        with application_span(_TOKEN_TRACER_SCOPE, _EXCHANGE_SPAN_NAME) as span:
            annotate_span(span, operation=_EXCHANGE_SPAN_NAME)
            body = self._token_request_body(
                {
                    "code": code,
                    "redirect_uri": redirect_uri,
                    "code_verifier": code_verifier,
                }
            )
            status, response_body = self._post_token_request(body)
            return _classify_token_response(status, response_body, now=self._clock())

    def refresh_user_token(self, refresh_token: GitHubUserRefreshSecret) -> GitHubUserTokenGrant:
        """Exchange one refresh credential for a fresh, rotated token grant."""
        with application_span(_TOKEN_TRACER_SCOPE, _REFRESH_SPAN_NAME) as span:
            annotate_span(span, operation=_REFRESH_SPAN_NAME)
            # The raw credential value is read only here, at the final HTTP
            # request-construction boundary — never copied elsewhere.
            body = self._token_request_body(
                {
                    "grant_type": "refresh_token",
                    "refresh_token": refresh_token.secret_value(),
                }
            )
            status, response_body = self._post_token_request(body)
            return _classify_token_response(status, response_body, now=self._clock())

    def fetch_authenticated_user(self, access_token: GitHubUserAccessToken) -> GitHubCurrentUser:
        """Resolve the authoritative current user under the user access token."""
        with application_span(_TOKEN_TRACER_SCOPE, _FETCH_USER_SPAN_NAME) as span:
            annotate_span(span, operation=_FETCH_USER_SPAN_NAME)
            response = self._rest.request(
                "/user",
                method="GET",
                authorization=f"Bearer {access_token.token_value()}",
            )
            return _parse_current_user_payload(response.body)

    def _token_request_body(self, fields: dict[str, str | None]) -> bytes:
        """Compose the form-encoded token-endpoint request body.

        Credentials travel only in the body, never in a URL or query string.
        Absent optional members are omitted.
        """
        encoded = urllib.parse.urlencode(
            {
                "client_id": self._client_id,
                "client_secret": self._client_secret,
                **{key: value for key, value in fields.items() if value is not None},
            }
        )
        return encoded.encode("utf-8")

    def _post_token_request(self, body: bytes) -> tuple[int, bytes]:
        """Perform the fixed-URL HTTPS POST; classify transport failures.

        The POST never follows a redirect: urllib copies request headers
        into redirected requests, and a token request carrying credentials
        must never be replayed onto another origin. A refused redirect and
        every 5xx answer leave the outcome unknown; the bounded body is
        never echoed into errors, logs, or telemetry.
        """
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/x-www-form-urlencoded",
            "User-Agent": "OpenOrc",
        }
        try:
            status, _headers, response_body = self._fetch(
                GITHUB_TOKEN_ENDPOINT_URL,
                "POST",
                headers,
                self._timeout_seconds,
                body,
            )
        except GitHubOutcomeUncertainError:
            raise
        except Exception as exc:  # transport-level failure: the outcome is unknown
            logger.warning(
                "the outcome of the GitHub user token request is unknown: the transport failed"
            )
            raise GitHubOutcomeUncertainError(
                "the outcome of the GitHub user token request is unknown: the transport failed"
            ) from exc
        return status, response_body

    def __repr__(self) -> str:
        return "HttpGitHubUserTokenClient(<redacted>)"

    def __str__(self) -> str:
        return "HttpGitHubUserTokenClient(<redacted>)"
