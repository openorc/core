"""Transport-neutral Supabase Auth access-token verification.

The verifier is the Supabase adapter's authentication mechanics boundary: it
takes a raw bearer-token value and returns the authenticated principal (the
JWT ``sub`` UUID) or a normalized, adapter-local rejection. It owns JWKS
fetching/caching, signature verification, and Supabase claim-level token
validation. It decides nothing about OpenOrc workflow meaning and knows
nothing about FastAPI, RQ, or persistence: later API/worker surfaces call the
``openorc.services.authentication`` service, which composes this verifier
with Profile resolution and translates these adapter errors into the typed
application error vocabulary.

Dependency direction: adapters never import ``openorc.services``. The
adapter-local error types below are the normalized boundary; the service
translates them (token rejection → ``AuthenticationError``; JWKS retrieval
failure → the external-operation vocabulary — a JWKS outage is never
reported as invalid caller credentials).

Token contract (v1 GitHub-only sign-in):

- asymmetric Supabase signing keys only (ES256/RS256 allowlist; the token
  header ``alg`` is validated against the allowlist and never trusted alone;
  no symmetric/HS256 shared-secret path exists);
- issuer must equal the configured project auth issuer
  (``<project-url>/auth/v1``); JWKS is fetched from
  ``<issuer>/.well-known/jwks.json``;
- ``exp`` is required and validated; ``nbf`` is validated when present;
- audience must equal the configured expected audience (default
  ``authenticated``) and ``role`` must equal ``authenticated``;
- the trusted ``is_anonymous`` claim must not be true — Supabase anonymous
  users can carry the authenticated role, so anonymity is rejected explicitly,
  not inferred from the role;
- trusted ``app_metadata`` must identify the account as GitHub-originated
  (``provider == "github"`` and ``"github" in providers``). Editable
  ``user_metadata`` is never consulted for this decision.

The GitHub-only guarantee is primarily a Supabase Auth deployment
configuration rule (only GitHub OAuth enabled for sign-in); these trusted
metadata checks are defense-in-depth account-origin validation, not proof of
which linked provider minted an individual session. Error messages here are
deliberately safe: they never contain the bearer token, signature, or JWKS
contents.
"""

from __future__ import annotations

import json
import threading
import time
import urllib.error
import urllib.request
from collections.abc import Callable
from typing import Protocol
from urllib.parse import urlparse
from uuid import UUID

import jwt
from jwt import PyJWK

from openorc.config import ConfigurationError
from openorc.domain.identity import AuthenticatedPrincipal

__all__ = [
    "EXPECTED_TOKEN_ROLE",
    "GITHUB_PROVIDER",
    "SUPPORTED_ASYMMETRIC_ALGORITHMS",
    "HttpJwksClient",
    "JwksClient",
    "SupabaseAccessTokenRejectedError",
    "SupabaseAccessTokenVerifier",
    "SupabaseJwksOutcomeUnknownError",
    "SupabaseJwksUnavailableError",
]

# Supabase Auth issuer path under the project URL; appending the JWKS path
# exposes the project's public signing keys (Supabase JWT documentation).
AUTH_ISSUER_PATH = "/auth/v1"
JWKS_PATH = "/.well-known/jwks.json"

# Explicit asymmetric algorithm allowlist. Verification never trusts the
# token header to choose an arbitrary algorithm; HS256 shared-secret
# verification is deliberately not supported (projects still on the legacy
# symmetric JWT secret fail closed here).
SUPPORTED_ASYMMETRIC_ALGORITHMS = frozenset({"ES256", "RS256"})

# Supabase Auth mints user access tokens with the "authenticated" audience and
# role; anon/service-role keys are not user sessions.
EXPECTED_TOKEN_ROLE = "authenticated"

GITHUB_PROVIDER = "github"

DEFAULT_JWKS_TIMEOUT_SECONDS = 5.0
DEFAULT_JWKS_CACHE_TTL_SECONDS = 300.0


class SupabaseAccessTokenRejectedError(Exception):
    """The token itself is invalid: malformed, expired, wrong project/claims,
    unknown key, or not GitHub-originated per trusted metadata.

    Safe normalized rejection: messages never contain the bearer token,
    signature, or JWKS contents. Translated by the authentication service into
    the typed application ``AuthenticationError``.
    """


class SupabaseJwksUnavailableError(Exception):
    """A known failure of JWKS retrieval: verification could not be performed.

    The JWKS source answered with an error (HTTP failure) or returned an
    unusable signing-key document. This is an external-operation failure of
    the verification infrastructure — never an invalid caller credential —
    and is translated by the authentication service into the external-operation
    error vocabulary.
    """


class SupabaseJwksOutcomeUnknownError(Exception):
    """The JWKS retrieval outcome is unknown (timeout, connection loss).

    Distinct from a known failure: the retrieval outcome is neither success
    nor known failure, so the caller must treat verification as unavailable,
    not as token rejection.
    """


class JwksClient(Protocol):
    """Source of the project's current asymmetric signing keys (JWKS)."""

    def get_signing_key(self, kid: str | None) -> PyJWK:
        """Return the signing key for the token's ``kid``.

        Raises :class:`SupabaseAccessTokenRejectedError` when no current key
        matches, or one of the JWKS-retrieval failures when the key source
        itself cannot be consulted.
        """
        ...


_JwksFetcher = Callable[[str, float], bytes]
_Clock = Callable[[], float]


def _fetch_jwks_document(jwks_url: str, timeout_seconds: float) -> bytes:
    """Fetch the raw JWKS document over HTTP."""
    with urllib.request.urlopen(jwks_url, timeout=timeout_seconds) as response:
        return response.read()


def _fetch_jwks_keys(
    fetch: _JwksFetcher, jwks_url: str, timeout_seconds: float
) -> list[dict[str, object]]:
    """Fetch and classify: transport errors classify here, at the call site.

    HTTP failure is a known non-success of the JWKS retrieval; timeout or
    connection loss leaves the outcome unknown. Both are normalized to the
    adapter-local retrieval errors regardless of the underlying fetch
    implementation, so verification infrastructure failures are never
    confused with token rejections.
    """
    try:
        raw = fetch(jwks_url, timeout_seconds)
    except urllib.error.HTTPError as exc:
        # The source answered with a definitive failure: known non-success.
        raise SupabaseJwksUnavailableError(
            "the Supabase Auth signing-key source rejected the JWKS lookup"
        ) from exc
    except (urllib.error.URLError, TimeoutError, OSError) as exc:
        # Timeout or connection loss leaves the outcome unknown; never
        # reclassified as a known failure or success.
        raise SupabaseJwksOutcomeUnknownError(
            "the Supabase Auth signing-key source could not be reached"
        ) from exc
    return _parse_jwks_document(raw)


def _select_signing_key(
    keys: list[dict[str, object]] | None, kid: str | None
) -> dict[str, object] | None:
    """Select the JWK entry matching the token header ``kid``.

    ``kid`` is optional in the JWT header: a token without ``kid`` matches a
    single-key JWKS unambiguously; a multi-key set is never guessed. No keys,
    no match, or an ambiguous match returns ``None`` (a token-selection
    problem, not a retrieval failure).
    """
    if not keys:
        return None
    if kid is None:
        return keys[0] if len(keys) == 1 else None
    matches = [key for key in keys if key.get("kid") == kid]
    return matches[0] if len(matches) == 1 else None


class HttpJwksClient:
    """JWKS client over HTTP with a small TTL cache.

    Encapsulates the only network dependency of token verification, so a
    verification path does not require a live Supabase call per request when
    asymmetric signing keys are in use. Deterministic :class:`JwksClient`
    fakes replace this class entirely in ordinary tests; the injected
    ``fetch``/``clock`` seams exist for testing this implementation without
    network access.

    Two clocks are tracked separately:

    - ``_document_fetched_at`` is when the last JWKS fetch SUCCEEDED. It is
      the only gate for serving known keys from the cached document: failed
      fetches never extend it, so last-good signing keys stay usable only
      within their own cache lifetime and stale keys cannot survive a
      prolonged outage (key-rotation/revocation safety).
    - ``_epoch_attempted_at`` is when the last load attempt happened, success
      or failure. It governs only the bounded backoff window during which a
      failed load attempt is re-reported without issuing further fetches.
    """

    def __init__(
        self,
        jwks_url: str,
        *,
        timeout_seconds: float = DEFAULT_JWKS_TIMEOUT_SECONDS,
        cache_ttl_seconds: float = DEFAULT_JWKS_CACHE_TTL_SECONDS,
        clock: _Clock = time.monotonic,
        fetch: _JwksFetcher | None = None,
    ) -> None:
        if not jwks_url:
            raise ConfigurationError("a JWKS URL is required to fetch signing keys")
        if timeout_seconds <= 0:
            raise ConfigurationError("the JWKS timeout must be greater than 0 seconds")
        if cache_ttl_seconds <= 0:
            raise ConfigurationError("the JWKS cache TTL must be greater than 0 seconds")
        self._jwks_url = jwks_url
        self._timeout_seconds = timeout_seconds
        self._cache_ttl_seconds = cache_ttl_seconds
        self._clock = clock
        self._fetch: _JwksFetcher = fetch if fetch is not None else _fetch_jwks_document
        self._lock = threading.Lock()
        self._cached_keys: list[dict[str, object]] | None = None
        self._document_fetched_at: float | None = None
        self._epoch_attempted_at: float | None = None
        self._forced_refresh_used = False
        self._epoch_failure: (
            SupabaseJwksUnavailableError | SupabaseJwksOutcomeUnknownError | None
        ) = None

    def get_signing_key(self, kid: str | None) -> PyJWK:
        """Return the current signing key matching ``kid``.

        Known keys are served from the last successfully fetched document
        only while that document is within its own cache lifetime — failed
        fetches never renew that gate. One refresh is forced per document
        epoch when the document does not contain the key (stale-cache safety
        during signing-key rotation), consumed by the attempt itself. A
        failed load — natural or forced — starts a bounded backoff epoch
        during which lookups needing the key source re-report the classified
        outcome without issuing further fetches; a retrieval failure is never
        reclassified as an invalid credential, and a still-missing ``kid``
        after a bounded refresh is a token rejection. Once the last good
        document expires, lookups fail closed until a JWKS fetch succeeds.
        """
        with self._lock:
            now = self._clock()
            document_fresh = (
                self._document_fetched_at is not None
                and now - self._document_fetched_at < self._cache_ttl_seconds
            )
            backoff_active = (
                self._epoch_attempted_at is not None
                and now - self._epoch_attempted_at < self._cache_ttl_seconds
            )

            # 1) Known keys: served from the last successfully fetched
            #    document, only while that document is within its own cache
            #    lifetime. Failed fetches never renew this gate.
            if document_fresh and self._cached_keys is not None:
                key_data = _select_signing_key(self._cached_keys, kid)
                if key_data is not None:
                    return self._build_signing_key(key_data)

            # 2) The key source is needed. Within an active backoff epoch the
            #    previous attempt's classified outcome is re-reported without
            #    another fetch, preserving the external-operation
            #    classification.
            if backoff_active and self._epoch_failure is not None:
                raise self._epoch_failure

            if document_fresh:
                # Unknown kid against a still-fresh document: exactly one
                # forced refresh per document epoch, consumed by the attempt
                # itself whether it succeeds or fails.
                if self._forced_refresh_used:
                    raise SupabaseAccessTokenRejectedError(
                        "authentication failed: the access token was not signed by "
                        "a known project signing key"
                    )
                self._forced_refresh_used = True
                keys = self._load_keys(force_refresh=True)
            else:
                # The last good document is stale (or absent): one natural
                # load per backoff epoch. Expired keys are never revived by
                # failed-attempt epochs — lookups fail closed until a fetch
                # succeeds.
                keys = self._load_keys(force_refresh=False)
            key_data = _select_signing_key(keys, kid)
            if key_data is None:
                raise SupabaseAccessTokenRejectedError(
                    "authentication failed: the access token was not signed by "
                    "a known project signing key"
                )
            return self._build_signing_key(key_data)

    def _build_signing_key(self, key_data: dict[str, object]) -> PyJWK:
        try:
            return PyJWK(key_data)
        except jwt.PyJWTError as exc:
            raise SupabaseJwksUnavailableError(
                "the Supabase Auth signing-key source published an unusable key"
            ) from exc

    def _load_keys(self, *, force_refresh: bool) -> list[dict[str, object]]:
        """Attempt one JWKS load, bounded by the backoff epoch.

        A load attempt — successful or failed — consumes the current backoff
        epoch: within the TTL window a failed attempt is re-reported without
        issuing another fetch. The attempt timestamp is tracked separately
        from the successful-document timestamp: a failed attempt never
        extends the freshness (and therefore the servable lifetime) of the
        last successfully fetched signing-key document.
        """
        now = self._clock()
        backoff_active = (
            self._epoch_attempted_at is not None
            and now - self._epoch_attempted_at < self._cache_ttl_seconds
        )
        if backoff_active and self._epoch_failure is not None:
            raise self._epoch_failure

        try:
            keys = _fetch_jwks_keys(self._fetch, self._jwks_url, self._timeout_seconds)
        except (SupabaseJwksUnavailableError, SupabaseJwksOutcomeUnknownError) as exc:
            # The failed attempt consumes its backoff epoch: within the TTL
            # window, repeated lookups report this classified outcome instead
            # of issuing repeated live fetches. The timestamp of the last
            # SUCCESSFUL fetch is deliberately untouched.
            self._epoch_failure = exc
            self._epoch_attempted_at = self._clock()
            raise
        self._cached_keys = keys
        self._epoch_failure = None
        self._document_fetched_at = self._clock()
        self._epoch_attempted_at = self._document_fetched_at
        # A naturally loaded document starts a fresh document epoch with one
        # forced-refresh slot available for unknown-kid lookups; a forced
        # refresh keeps its own slot consumed.
        if not force_refresh:
            self._forced_refresh_used = False
        return keys


def _parse_jwks_document(raw: bytes) -> list[dict[str, object]]:
    """Parse the raw JWKS document into validated JWK entries."""
    try:
        document = json.loads(raw)
    except (TypeError, ValueError) as exc:
        raise SupabaseJwksUnavailableError(
            "the Supabase Auth signing-key source returned an unusable JWKS document"
        ) from exc
    if not isinstance(document, dict) or not isinstance(document.get("keys"), list):
        raise SupabaseJwksUnavailableError(
            "the Supabase Auth signing-key source returned an unusable JWKS document"
        )
    keys = document["keys"]
    if not keys:
        raise SupabaseJwksUnavailableError(
            "the Supabase Auth signing-key source published no signing keys"
        )
    for key_data in keys:
        if not isinstance(key_data, dict):
            raise SupabaseJwksUnavailableError(
                "the Supabase Auth signing-key source returned an unusable JWKS document"
            )
    return keys


def _require_project_url(project_url: str) -> None:
    parsed = urlparse(project_url)
    if parsed.scheme not in ("http", "https") or not parsed.netloc:
        raise ConfigurationError("the Supabase project URL must be an http(s) URL with a host")


class SupabaseAccessTokenVerifier:
    """Verify Supabase Auth access tokens into authenticated principals.

    Transport-neutral by construction: no FastAPI/RQ machinery, callable
    directly by later API/worker code through the authentication service.
    JWKS fetching/caching is encapsulated behind the injected
    :class:`JwksClient`, so ordinary tests use deterministic fakes and the
    verification path performs no live Supabase call per request.
    """

    def __init__(
        self,
        *,
        project_url: str,
        audience: str,
        jwks_client: JwksClient | None = None,
    ) -> None:
        _require_project_url(project_url)
        if not audience:
            raise ConfigurationError(
                "the expected Supabase Auth audience must be a non-empty string"
            )
        self._issuer = project_url.rstrip("/") + AUTH_ISSUER_PATH
        self._audience = audience
        self._jwks_client: JwksClient = (
            jwks_client if jwks_client is not None else HttpJwksClient(self._issuer + JWKS_PATH)
        )

    def verify(self, token: str) -> AuthenticatedPrincipal:
        """Verify a raw bearer-token value and return the authenticated principal.

        Every invalid-token failure raises
        :class:`SupabaseAccessTokenRejectedError` — invalid signature, unknown
        key, wrong issuer/audience/role, expiry, malformed subject, anonymous
        or non-GitHub-originated accounts. JWKS-retrieval failures raise their
        own normalized errors: verification could not be performed and the
        caller's credential is not implicated. Messages never contain token or
        key contents.
        """
        if not isinstance(token, str) or not token:
            raise SupabaseAccessTokenRejectedError(
                "authentication failed: the access token is missing or empty"
            )

        try:
            header = jwt.get_unverified_header(token)
        except jwt.PyJWTError as exc:
            raise SupabaseAccessTokenRejectedError(
                "authentication failed: the access token could not be parsed"
            ) from exc

        algorithm = header.get("alg")
        if not isinstance(algorithm, str) or algorithm not in SUPPORTED_ASYMMETRIC_ALGORITHMS:
            raise SupabaseAccessTokenRejectedError(
                "authentication failed: the access token does not use a supported "
                "asymmetric signing algorithm"
            )
        kid = header.get("kid")
        if not isinstance(kid, str) or not kid:
            raise SupabaseAccessTokenRejectedError(
                "authentication failed: the access token does not identify its signing key"
            )

        try:
            signing_key = self._jwks_client.get_signing_key(kid)
        except (SupabaseJwksUnavailableError, SupabaseJwksOutcomeUnknownError):
            raise
        except SupabaseAccessTokenRejectedError:
            raise

        claims = self._decode_claims(token, signing_key, algorithm)
        return self._resolve_principal(claims)

    def _decode_claims(self, token: str, signing_key: PyJWK, algorithm: str) -> dict[str, object]:
        """Decode and standard-validate the verified token claims."""
        try:
            claims: dict[str, object] = jwt.decode(
                token,
                key=signing_key.key,
                algorithms=[algorithm],
                audience=self._audience,
                issuer=self._issuer,
                options={"require": ["exp", "sub", "aud"]},
            )
        except jwt.ExpiredSignatureError as exc:
            raise SupabaseAccessTokenRejectedError(
                "authentication failed: the access token has expired"
            ) from exc
        except jwt.ImmatureSignatureError as exc:
            raise SupabaseAccessTokenRejectedError(
                "authentication failed: the access token is not yet valid"
            ) from exc
        except jwt.InvalidSignatureError as exc:
            raise SupabaseAccessTokenRejectedError(
                "authentication failed: the access token signature is invalid"
            ) from exc
        except jwt.InvalidAudienceError as exc:
            raise SupabaseAccessTokenRejectedError(
                "authentication failed: the access token audience is not valid for this project"
            ) from exc
        except jwt.InvalidIssuerError as exc:
            raise SupabaseAccessTokenRejectedError(
                "authentication failed: the access token issuer is not this project"
            ) from exc
        except jwt.MissingRequiredClaimError as exc:
            raise SupabaseAccessTokenRejectedError(
                "authentication failed: the access token is missing a required claim"
            ) from exc
        except jwt.PyJWTError as exc:
            raise SupabaseAccessTokenRejectedError(
                "authentication failed: the access token is invalid"
            ) from exc
        return claims

    def _resolve_principal(self, claims: dict[str, object]) -> AuthenticatedPrincipal:
        """Apply the v1 GitHub-only account-origin policy to trusted claims."""
        if claims.get("role") != EXPECTED_TOKEN_ROLE:
            raise SupabaseAccessTokenRejectedError(
                "authentication failed: the access token does not carry the authenticated role"
            )
        if claims.get("is_anonymous") is True:
            # Supabase anonymous users can carry the authenticated role, so
            # anonymity is rejected explicitly, not inferred from other claims.
            raise SupabaseAccessTokenRejectedError(
                "authentication failed: anonymous accounts cannot access OpenOrc"
            )

        app_metadata = claims.get("app_metadata")
        if not isinstance(app_metadata, dict):
            raise SupabaseAccessTokenRejectedError(
                "authentication failed: the access token does not carry trusted "
                "account-origin metadata"
            )
        providers = app_metadata.get("providers")
        if (
            app_metadata.get("provider") != GITHUB_PROVIDER
            or not isinstance(providers, list)
            or GITHUB_PROVIDER not in providers
        ):
            # Trusted app_metadata only; editable user_metadata is never a
            # source of account origin.
            raise SupabaseAccessTokenRejectedError(
                "authentication failed: the account is not GitHub-originated"
            )

        subject = claims.get("sub")
        try:
            user_id = UUID(str(subject))
        except (TypeError, ValueError) as exc:
            raise SupabaseAccessTokenRejectedError(
                "authentication failed: the access token subject is not a valid account identifier"
            ) from exc

        return AuthenticatedPrincipal(user_id=user_id)
