"""GitHub App authentication mechanics (issue #58).

The OpenOrc GitHub App authentication boundary: GitHub App JWT creation and
short-lived installation access-token minting for the exact installation the
#57 route resolution selected. Human GitHub sign-in is identity only — this
boundary is the sole credential authority for durable repository automation,
and no PAT or human OAuth token fallback exists anywhere in this adapter.

Deployment-held identity:

- the GitHub App ID and private key are deployment/bootstrap secret material;
  they are supplied at construction (typically via
  :meth:`HttpGitHubAppClient.from_settings`), validated once, and never
  exposed through repr/str, errors, logs, or span attributes;
- construction fails fast on a missing/blank App ID or a private key that is
  not a parseable PEM key — the component that constructs the authenticator
  is the one that must possess the credential.

Installation access tokens:

- minted through the documented
  ``POST /app/installations/{installation_id}/access_tokens`` endpoint under
  a freshly created App JWT, for the exact installation ID the caller
  resolved from the #57 route — never guessed, never routed through owner
  logins or URLs;
- held in memory only: a bounded cache keyed by the stable external
  installation ID honors GitHub's token expiry semantics (tokens expire
  after at most one hour; the cache treats a token as unusable a safety
  margin before its documented ``expires_at``). Nothing persists or returns
  the token beyond the immediate adapter/client lifetime — no domain object,
  persistence row, event, log, or telemetry attribute ever carries it;
- expiry/re-minting is adapter/authentication mechanics, deliberately not
  token-refresh persistence. Eviction on an authentication rejection is the
  caller's bounded recovery path (one re-mint), never a workflow retry.
"""

from __future__ import annotations

import json
import logging
import threading
import time
from collections import OrderedDict
from collections.abc import Callable
from datetime import datetime

import jwt

from openorc.adapters.github.capabilities import (
    GITHUB_MINT_INSTALLATION_TOKEN_PATH,
    require_positive_int,
)
from openorc.adapters.github.errors import GitHubOutcomeUncertainError
from openorc.adapters.github.transport import (
    DEFAULT_GITHUB_REQUEST_TIMEOUT_SECONDS,
    GitHubFetcher,
    HttpGitHubRestClient,
)
from openorc.config import ConfigurationError
from openorc.observability import annotate_span, application_span

__all__ = [
    "INSTALLATION_TOKEN_CACHE_MAX_ENTRIES",
    "INSTALLATION_TOKEN_EXPIRY_SAFETY_MARGIN_SECONDS",
    "GitHubAppAuthenticator",
    "InstallationAccessToken",
]

logger = logging.getLogger(__name__)

# The GitHub App JWT contract (GitHub App authentication documentation):
# signed with the App's private RSA key, ``iss`` is the App ID, and the
# lifetime is at most 10 minutes. OpenOrc mints short 9-minute JWTs with a
# one-minute backward ``iat`` for clock drift, and a fresh JWT is created for
# every authenticated call — there is no persisted JWT state.
_JWT_LIFETIME_SECONDS = 540
_JWT_IAT_BACKWARD_SECONDS = 60

# A cached installation token is treated as unusable this many seconds
# before its documented ``expires_at``: GitHub installation tokens live at
# most one hour, and the margin keeps a live request from presenting a token
# that expires mid-flight. The cache is process-memory only.
INSTALLATION_TOKEN_EXPIRY_SAFETY_MARGIN_SECONDS = 60.0

# The cache is genuinely bounded: at most this many installation entries are
# retained per process, with expired entries pruned opportunistically and
# least-recently-used entries evicted when the bound would be exceeded. A
# long-running process therefore never retains arbitrarily many — or
# arbitrarily old — installation-token secret values.
INSTALLATION_TOKEN_CACHE_MAX_ENTRIES = 128

# Representative external-adapter span boundary (issues #108/#109): one
# instrumented external operation per token mint. Only the operation name
# and the stable installation identifier are attachable; the App JWT, the
# minted token, and all provider content have no supported path into
# telemetry.
_TRACER_SCOPE = "openorc.adapters.github.authentication"
_MINT_SPAN_NAME = "github.mint_installation_access_token"


class InstallationAccessToken:
    """Secret-bearing value for one minted GitHub installation access token.

    The token necessarily exists in backend memory while the adapter
    authenticates GitHub operations. Ordinary representation is redacted:
    ``repr()`` and ``str()`` never expose the value. The value is reachable
    only through :meth:`token_value`, which is reserved for the trusted
    transport call that needs it. The token is never part of a domain
    object, persistence row, event, DTO, or telemetry attribute.
    """

    __slots__ = ("_expires_at", "_value")

    def __init__(self, *, value: str, expires_at: datetime) -> None:
        if not isinstance(value, str) or not value:
            raise GitHubOutcomeUncertainError(
                "the installation token response is not interpretable: the token value is missing"
            )
        if not isinstance(expires_at, datetime) or expires_at.tzinfo is None:
            raise GitHubOutcomeUncertainError(
                "the installation token response is not interpretable: "
                "the expiry is not a timezone-aware instant"
            )
        self._value = value
        self._expires_at = expires_at

    def token_value(self) -> str:
        """Return the token value for the trusted transport call only."""
        return self._value

    @property
    def expires_at(self) -> datetime:
        """The documented token expiry (timezone-aware)."""
        return self._expires_at

    def usable_at(self, now_epoch: float) -> bool:
        """Whether the token remains usable at ``now_epoch`` (with safety margin)."""
        usable_until = (
            self._expires_at.timestamp() - INSTALLATION_TOKEN_EXPIRY_SAFETY_MARGIN_SECONDS
        )
        return now_epoch < usable_until

    def __repr__(self) -> str:
        return "InstallationAccessToken(<redacted>)"

    def __str__(self) -> str:
        return "InstallationAccessToken(<redacted>)"


class GitHubAppAuthenticator:
    """GitHub App JWT creation and installation access-token minting.

    Constructed with the deployment-held GitHub App identity (the App ID and
    the App private key), which are validated once and held privately; the
    ordinary representation is redacted. The token cache is process memory
    keyed by the stable external installation ID and is genuinely bounded:
    expired entries are pruned opportunistically under the cache lock, and
    retention is LRU-bounded at
    :data:`INSTALLATION_TOKEN_CACHE_MAX_ENTRIES` entries (configurable at
    construction), so a long-running process never retains arbitrarily many
    or arbitrarily old installation-token secret values.
    """

    def __init__(
        self,
        *,
        app_id: int,
        private_key_pem: str,
        clock: Callable[[], float] | None = None,
        fetch: GitHubFetcher | None = None,
        timeout_seconds: float = DEFAULT_GITHUB_REQUEST_TIMEOUT_SECONDS,
        cache_max_entries: int = INSTALLATION_TOKEN_CACHE_MAX_ENTRIES,
    ) -> None:
        if isinstance(app_id, bool) or not isinstance(app_id, int) or app_id <= 0:
            raise ConfigurationError("the GitHub App ID must be a positive integer")
        if not isinstance(private_key_pem, str) or not private_key_pem.strip():
            raise ConfigurationError("the GitHub App private key must be a non-empty PEM string")
        if (
            isinstance(cache_max_entries, bool)
            or not isinstance(cache_max_entries, int)
            or (cache_max_entries <= 0)
        ):
            raise ConfigurationError(
                "the installation token cache bound must be a positive integer"
            )
        try:
            # Validate the key once by signing a throwaway payload: a private
            # key that cannot produce an RS256 signature fails fast here. The
            # failure detail is never echoed — it is derived from secret
            # material.
            jwt.encode({"exp": 0}, private_key_pem, algorithm="RS256")
        except (TypeError, ValueError, jwt.PyJWTError) as exc:
            raise ConfigurationError(
                "the GitHub App private key is not a usable PEM private key"
            ) from exc
        self._private_key_pem = private_key_pem
        self._app_id = app_id
        self._clock = clock if clock is not None else time.time
        self._transport = HttpGitHubRestClient(timeout_seconds=timeout_seconds, fetch=fetch)
        self._cache: OrderedDict[int, InstallationAccessToken] = OrderedDict()
        self._cache_lock = threading.Lock()
        self._cache_max_entries = cache_max_entries

    @property
    def app_id(self) -> int:
        """The deployed OpenOrc GitHub App ID (not secret material)."""
        return self._app_id

    def app_jwt(self) -> str:
        """Create a fresh GitHub App JWT for one authenticated call.

        Signed with the deployment-held private key under RS256: ``iss`` is
        the App ID, ``iat`` carries a one-minute clock-drift allowance, and
        ``exp`` stays inside GitHub's 10-minute maximum. A new JWT is created
        for every authenticated call; JWT state is never cached or persisted.
        """
        now = int(self._clock())
        payload = {
            "iat": now - _JWT_IAT_BACKWARD_SECONDS,
            "exp": now + _JWT_LIFETIME_SECONDS,
            # JWT ``iss`` is a string-typed claim (GitHub accepts the App ID
            # as its string form; PyJWT enforces the string type).
            "iss": str(self._app_id),
        }
        return jwt.encode(payload, self._private_key_pem, algorithm="RS256")

    def installation_token(self, github_installation_id: int) -> InstallationAccessToken:
        """Return a usable installation token for the exact installation.

        Serves the bounded in-memory cache when a minted token remains usable
        at the current clock reading; otherwise mints a fresh token through
        the documented endpoint. The cache key is the stable external
        installation ID resolved from the #57 route — never a login, URL, or
        other mutable address. Retention is bounded: expired/unusable entries
        are pruned opportunistically under the cache lock, hits refresh the
        LRU position, and storing beyond the configured bound evicts the
        least-recently-used entry.
        """
        require_positive_int(github_installation_id, "github_installation_id")
        now = self._clock()
        with self._cache_lock:
            self._prune_expired_locked(now)
            cached = self._cache.get(github_installation_id)
            if cached is not None:
                self._cache.move_to_end(github_installation_id)
                return cached
        fresh = self.mint_installation_token(github_installation_id)
        with self._cache_lock:
            self._prune_expired_locked(self._clock())
            self._cache[github_installation_id] = fresh
            self._cache.move_to_end(github_installation_id)
            while len(self._cache) > self._cache_max_entries:
                self._cache.popitem(last=False)
        return fresh

    def _prune_expired_locked(self, now_epoch: float) -> None:
        """Remove expired/unusable entries; the caller holds the cache lock.

        An entry whose token is past its expiry minus the safety margin can
        never be served again, so retaining it would only retain a dead
        secret value; such entries are dropped whenever the cache is touched.
        """
        expired = [key for key, token in self._cache.items() if not token.usable_at(now_epoch)]
        for key in expired:
            del self._cache[key]

    def invalidate_installation_token(self, github_installation_id: int) -> None:
        """Evict the cached token for the exact installation.

        The recovery path for a GitHub 401 authentication rejection: the
        caller evicts once and re-mints a bounded number of times. Uncertain
        outcomes are never an eviction trigger.
        """
        require_positive_int(github_installation_id, "github_installation_id")
        with self._cache_lock:
            self._cache.pop(github_installation_id, None)

    def mint_installation_token(self, github_installation_id: int) -> InstallationAccessToken:
        """Mint one short-lived installation token for the exact installation."""
        require_positive_int(github_installation_id, "github_installation_id")
        with application_span(_TRACER_SCOPE, _MINT_SPAN_NAME) as span:
            annotate_span(
                span,
                operation=_MINT_SPAN_NAME,
                github_installation_id=str(github_installation_id),
            )
            response = self._transport.request(
                GITHUB_MINT_INSTALLATION_TOKEN_PATH.format(installation_id=github_installation_id),
                method="POST",
                authorization=f"Bearer {self.app_jwt()}",
            )
            return _parse_installation_token_response(response.body)

    def __repr__(self) -> str:
        return "GitHubAppAuthenticator(<redacted>)"

    def __str__(self) -> str:
        return "GitHubAppAuthenticator(<redacted>)"


def _parse_installation_token_response(body: bytes) -> InstallationAccessToken:
    """Normalize a mint response into the secret-bearing token value.

    The documented response carries the token value and its expiry instant;
    an uninterpretable shape classifies as an uncertain outcome. No response
    content ever enters errors or logs.
    """
    try:
        payload = json.loads(body)
    except (ValueError, UnicodeDecodeError) as exc:
        raise GitHubOutcomeUncertainError(
            "the installation token response is not interpretable"
        ) from exc
    if not isinstance(payload, dict):
        raise GitHubOutcomeUncertainError("the installation token response is not interpretable")
    value = payload.get("token")
    expires_raw = payload.get("expires_at")
    if not isinstance(value, str) or not isinstance(expires_raw, str):
        raise GitHubOutcomeUncertainError(
            "the installation token response is not interpretable: the token or expiry is missing"
        )
    try:
        expires_at = datetime.fromisoformat(expires_raw.replace("Z", "+00:00"))
    except ValueError as exc:
        raise GitHubOutcomeUncertainError(
            "the installation token response is not interpretable: the expiry is malformed"
        ) from exc
    return InstallationAccessToken(value=value, expires_at=expires_at)
