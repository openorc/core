"""Transport-neutral Supabase Auth Admin boundary for account deletion.

The server-side administrative boundary for permanent Supabase Auth user
deletion (Phase 2A, issue #97) and the trusted identity lookup the
Profile-scoped GitHub user authorization binding proof needs (issue #142): a
focused client for the supported Admin operations — permanent user deletion,
the read surface used to reconcile uncertain deletion outcomes, and the
multiplicity-preserving GitHub provider-identity read. It owns the Supabase
Auth Admin transport mechanics and nothing else: workflow meaning,
reconciliation policy, authorization binding, and error translation belong to
``openorc.services.account_lifecycle`` and
``openorc.services.github_user_authorization``, which compose this client and
translate these adapter errors into the typed application vocabulary.

Dependency direction: adapters never import ``openorc.services``. The
adapter-local error types below are the normalized boundary; the service
translates them (definitive rejection → known external-operation failure;
timeout/connection loss/uninterpretable response → external-operation
uncertainty; a confirmed-absent user is a classified end state, never a
failure).

Credential contract (verified against the current Supabase API-keys
documentation): publishable and secret keys are NOT JWTs — the deployment's
secret API key travels on the ``apikey`` request header, never as
``Authorization: Bearer`` and never in a URL or query parameter. Only the
``apikey`` header carries credential material on these calls.

Privileged-transport hardening: the project URL must be HTTPS (the
administrative credential is never sent over plaintext HTTP; a narrowly-
constrained local development exception permits http only for loopback
hosts), and redirects are never followed — ``urllib`` copies non-content
headers into redirected requests, so a followed redirect could forward the
credential to another host. A redirect that is not followed surfaces as an
unclassified 3xx and is classified as an unknown outcome, never success and
never a safe replay.

Key hygiene: the secret key is process bootstrap material supplied at
construction. It is held in a private field, its ordinary representation is
redacted, and it never appears in errors, logs, span attributes, or returned
objects. Construction fails fast on a missing or blank key: the component
that owns account deletion is the one that must possess the credential.

Outcome classification (the boundary the service relies on):

- ``delete_user``: any 2xx is success; 404 means the user is already absent —
  the desired end state, surfaced as a distinct classified outcome rather
  than a failure; other 4xx answers are definitive rejections (known
  non-success); 5xx answers, timeouts, connection loss, and uninterpretable
  transports leave the outcome unknown — deliberately NOT known failures,
  because a destructive write's effect can be masked by an intermediary and
  the caller reconciles through the read surface before any replay.
- ``fetch_user``: 200 reports the user present, 404 reports absent; the same
  failure taxonomy applies.

Error messages here are deliberately safe: they never contain the secret
key, the project URL (it carries the project reference), or user-record
contents.
"""

from __future__ import annotations

import json
import logging
import urllib.error
import urllib.request
from collections.abc import Callable, Mapping
from typing import Protocol
from urllib.parse import urlparse
from uuid import UUID

from openorc.adapters.supabase.auth import GITHUB_PROVIDER, _require_project_url
from openorc.config import ConfigurationError
from openorc.observability import annotate_span, application_span

__all__ = [
    "AUTH_ADMIN_USERS_PATH",
    "DEFAULT_ADMIN_REQUEST_TIMEOUT_SECONDS",
    "HttpSupabaseAuthAdminClient",
    "SupabaseAuthAdminClient",
    "SupabaseAuthAdminOutcomeUnknownError",
    "SupabaseAuthAdminRejectedError",
    "SupabaseAuthAdminUserAbsentError",
]

# Auth Admin user-management path under the Supabase project URL; the exact
# user UUID is appended per call (Supabase Auth Admin API).
AUTH_ADMIN_USERS_PATH = "/auth/v1/admin/users"

# Bounded request timeout for one Admin HTTP call. The account-deletion
# service derives its durable active-attempt lease from this bound (plus a
# documented safety margin), so the lease provably outlives any live request.
DEFAULT_ADMIN_REQUEST_TIMEOUT_SECONDS = 5.0

# The privileged Admin transport requires HTTPS: the deployment's secret API
# key is never sent over plaintext HTTP. A narrowly-constrained local
# development exception permits http only for loopback hosts.
_ADMIN_LOOPBACK_HOSTS = frozenset({"localhost", "127.0.0.1", "::1"})

# Representative external-adapter span boundaries (issues #108/#109): one
# instrumented external operation per Admin call, mirroring the JWKS
# retrieval span. The request URL carries the Supabase project reference and
# the apikey header carries credential material, so only the operation name
# is attached.
_ADMIN_TRACER_SCOPE = "openorc.adapters.supabase.admin"
_DELETE_USER_SPAN_NAME = "supabase.auth_admin_delete_user"
_FETCH_USER_SPAN_NAME = "supabase.auth_admin_fetch_user"
_FETCH_GITHUB_PROVIDER_IDS_SPAN_NAME = "supabase.auth_admin_fetch_user_github_provider_ids"

logger = logging.getLogger(__name__)


class SupabaseAuthAdminUserAbsentError(Exception):
    """The addressed Auth user is confirmed absent.

    For :meth:`SupabaseAuthAdminClient.delete_user` this is the desired end
    state already true (a second deletion after prior success classifies as
    already absent, never as a failure); for
    :meth:`SupabaseAuthAdminClient.fetch_user` it is a confirmed-absent
    lookup. Messages carry no user-record contents.
    """


def _require_admin_project_url(project_url: str) -> None:
    """Validate the project URL for the privileged Admin transport.

    Builds on the well-formed http(s) URL check and then requires HTTPS: the
    deployment's secret API key is privileged administrative credential
    material and is never sent over plaintext HTTP. A narrowly-constrained
    local development exception permits http only for loopback hosts. The
    error message is safe by construction (it never echoes the URL or any
    credential material).
    """
    _require_project_url(project_url)
    if urlparse(project_url).scheme == "https":
        return
    host = (urlparse(project_url).hostname or "").lower()
    if host in _ADMIN_LOOPBACK_HOSTS:
        return
    raise ConfigurationError(
        "the Supabase Auth Admin project URL must use https: the "
        "administrative credential is never sent over plaintext HTTP"
    )


class _NoRedirectHandler(urllib.request.HTTPRedirectHandler):
    """Admin-transport hardening: never follow redirects.

    ``urllib`` copies non-content request headers into redirected requests,
    so following a cross-origin redirect would forward the ``apikey``
    credential (the administrative secret) to another host. A redirect that
    is not followed surfaces as an HTTPError carrying the redirect status,
    which the client classifies as an unknown outcome — never followed with
    credentials, never reclassified as success or as a safe replay.
    """

    def redirect_request(self, req, fp, code, msg, headers, newurl):  # noqa: ANN001
        return None


# One module-level opener with redirect following disabled for the default
# (stdlib) Admin transport path. Injectable fetchers bypass it.
_ADMIN_HTTP_OPENER = urllib.request.build_opener(_NoRedirectHandler())


class SupabaseAuthAdminRejectedError(Exception):
    """A known failure of the Auth Admin request: the API definitively
    rejected it.

    The endpoint answered with a definitive non-success (for example an
    invalid administrative credential or an unacceptable request). This is an
    external-operation failure — never silently reclassified as success or as
    a safe replay. Messages never contain the secret key, project URL, or
    user-record contents.
    """


class SupabaseAuthAdminOutcomeUnknownError(Exception):
    """The Auth Admin request outcome is unknown.

    Timeout, connection loss, a 5xx answer, or an uninterpretable transport
    response leaves a destructive write's effect neither known success nor
    known failure. The caller must treat it as neither: reconcile
    authoritatively through the read surface where possible, and never
    blindly replay the delete. Messages never contain the secret key, project
    URL, or user-record contents.
    """


class SupabaseAuthAdminClient(Protocol):
    """Structural contract of the server-side Supabase Auth Admin boundary.

    The account-deletion lifecycle consumes exactly these two operations plus
    the bounded request timeout its durable active-attempt lease is derived
    from. Implementations own the transport mechanics — credential header
    handling, request/response classification, and key redaction — and decide
    nothing about OpenOrc workflow meaning.
    """

    @property
    def request_timeout_seconds(self) -> float:
        """Bounded timeout of one Admin HTTP request (the lease basis)."""
        ...

    def delete_user(self, user_id: UUID) -> None:
        """Permanently delete the exact Auth user (hard-delete semantics).

        Returns on success. Raises :class:`SupabaseAuthAdminUserAbsentError`
        when the user is confirmed absent (the end state is already true),
        :class:`SupabaseAuthAdminRejectedError` on a definitive rejection,
        and :class:`SupabaseAuthAdminOutcomeUnknownError` when the outcome
        cannot be classified.
        """
        ...

    def fetch_user(self, user_id: UUID) -> bool:
        """Report whether the exact Auth user currently exists.

        ``True`` when confirmed present, ``False`` when confirmed absent.
        Raises :class:`SupabaseAuthAdminRejectedError` on a definitive
        rejection and :class:`SupabaseAuthAdminOutcomeUnknownError` when
        existence cannot be determined.
        """
        ...

    def fetch_user_github_provider_ids(self, user_id: UUID) -> tuple[str | None, ...]:
        """Return the stable GitHub provider facts of the exact Auth user.

        The narrowly normalized, multiplicity-preserving read surface for
        the same-human authorization binding proof (issue #142): one entry
        per identity in the user's trusted ``identities`` collection whose
        ``provider`` is exactly ``"github"``, in collection order. Each
        entry carries the raw ``identity_data.provider_id`` string — the
        stable provider-side GitHub user fact — or ``None`` when that
        entry's provider_id member is missing or malformed. The collection
        shape is preserved exactly (zero, one, or many entries): collapsing
        or normalizing it here would silently resolve ambiguity that only
        the authorization service may classify. Mutable identity metadata
        (login, email, ``user_metadata``) is never read into the result.

        Raises :class:`SupabaseAuthAdminUserAbsentError` when the user is
        confirmed absent, :class:`SupabaseAuthAdminRejectedError` on a
        definitive rejection, and
        :class:`SupabaseAuthAdminOutcomeUnknownError` when the outcome or
        the trusted user-record shape cannot be determined.
        """
        ...


# Injectable transport seam: one HTTP call returning (status, raw body).
# Network-level failures (timeout, connection loss, DNS) raise; the HTTP
# status is returned so classification stays at one deliberate place.
_AdminFetcher = Callable[[str, str, Mapping[str, str], float], tuple[int, bytes]]


def _fetch_admin_response(
    fetch: _AdminFetcher | None,
    url: str,
    method: str,
    headers: Mapping[str, str],
    timeout_seconds: float,
) -> tuple[int, bytes]:
    """Perform one Admin HTTP request through the injected (or stdlib) fetcher.

    The default stdlib path uses the module opener with redirect following
    disabled: a redirect is never followed with the ``apikey`` credential and
    surfaces as an HTTPError carrying the redirect status, which the caller
    classifies as an unknown outcome.
    """
    if fetch is not None:
        return fetch(url, method, dict(headers), timeout_seconds)
    request = urllib.request.Request(url, method=method, headers=dict(headers))
    try:
        with _ADMIN_HTTP_OPENER.open(request, timeout=timeout_seconds) as response:
            return response.status, response.read()
    except urllib.error.HTTPError as exc:
        # The endpoint answered with an HTTP error status — including a
        # redirect that was deliberately not followed. Return it for
        # status-driven classification at the call site. The error body is
        # never needed for the outcome classification and is deliberately
        # discarded so no response content can leak into errors or logs.
        return exc.code, b""


def _parse_github_provider_ids(body: bytes) -> tuple[str | None, ...]:
    """Normalize the trusted user record into multiplicity-preserving facts.

    One entry per ``identities`` member whose ``provider`` is exactly the
    GitHub provider, in collection order; each entry carries the raw
    ``identity_data.provider_id`` string, or ``None`` when that member is
    missing or malformed. A non-dict identities entry is trusted durable
    state that cannot be interpreted, so it contributes ``None`` — the
    service fails closed on it. A response whose trusted shape cannot be
    interpreted at all (not JSON, not an object, ``identities`` not a list)
    is an unknown outcome, never a provable fact. No response content ever
    enters the exception messages.
    """
    try:
        payload = json.loads(body)
    except (ValueError, UnicodeDecodeError) as exc:
        raise SupabaseAuthAdminOutcomeUnknownError(
            "the Supabase Auth Admin GitHub identity lookup response is not interpretable"
        ) from exc
    if not isinstance(payload, dict):
        raise SupabaseAuthAdminOutcomeUnknownError(
            "the Supabase Auth Admin GitHub identity lookup response is not interpretable"
        )
    identities = payload.get("identities")
    if not isinstance(identities, list):
        raise SupabaseAuthAdminOutcomeUnknownError(
            "the Supabase Auth Admin GitHub identity lookup response is not interpretable"
        )
    provider_ids: list[str | None] = []
    for entry in identities:
        if not isinstance(entry, dict) or entry.get("provider") != GITHUB_PROVIDER:
            if not isinstance(entry, dict):
                provider_ids.append(None)
            continue
        identity_data = entry.get("identity_data")
        if not isinstance(identity_data, dict):
            provider_ids.append(None)
            continue
        provider_id = identity_data.get("provider_id")
        provider_ids.append(provider_id if isinstance(provider_id, str) else None)
    return tuple(provider_ids)


class HttpSupabaseAuthAdminClient:
    """HTTP Supabase Auth Admin client for the permanent account-deletion boundary.

    Transport-neutral by construction: no FastAPI/RQ machinery, callable
    directly by the account-deletion service. The deployment's secret API key
    is supplied at construction, carried on the ``apikey`` request header
    only (publishable/secret keys are not JWTs — they never travel as
    ``Authorization: Bearer`` and never in a URL), and never exposed through
    repr, str, errors, logs, or span attributes.
    """

    def __init__(
        self,
        *,
        project_url: str,
        secret_key: str,
        timeout_seconds: float = DEFAULT_ADMIN_REQUEST_TIMEOUT_SECONDS,
        fetch: _AdminFetcher | None = None,
    ) -> None:
        _require_admin_project_url(project_url)
        if not isinstance(secret_key, str) or not secret_key.strip():
            raise ConfigurationError(
                "the Supabase Auth Admin secret key must be a non-empty string"
            )
        if isinstance(timeout_seconds, bool) or not isinstance(timeout_seconds, (int, float)):
            raise ConfigurationError(
                "the Supabase Auth Admin request timeout must be a positive number"
            )
        if timeout_seconds <= 0:
            raise ConfigurationError(
                "the Supabase Auth Admin request timeout must be a positive number"
            )
        self._base_url = project_url.rstrip("/")
        self._secret_key = secret_key
        self._timeout_seconds = float(timeout_seconds)
        self._fetch = fetch

    @property
    def request_timeout_seconds(self) -> float:
        """Bounded timeout of one Admin HTTP request (the durable-lease basis)."""
        return self._timeout_seconds

    def delete_user(self, user_id: UUID) -> None:
        """Permanently delete the exact Auth user (see the module contract)."""
        with application_span(_ADMIN_TRACER_SCOPE, _DELETE_USER_SPAN_NAME) as span:
            annotate_span(span, operation=_DELETE_USER_SPAN_NAME)
            status = self._perform("DELETE", user_id)
            if 200 <= status < 300:
                return
            if status == 404:
                raise SupabaseAuthAdminUserAbsentError(
                    "the Supabase Auth user addressed for permanent deletion is already absent"
                )
            if 400 <= status < 500:
                logger.warning(
                    "Supabase Auth Admin permanent-deletion request was definitively "
                    "rejected by the administrative endpoint"
                )
                raise SupabaseAuthAdminRejectedError(
                    "the Supabase Auth Admin boundary rejected the permanent deletion request"
                )
            logger.warning(
                "Supabase Auth Admin request outcome is unknown: the administrative "
                "endpoint reported an unclassified failure"
            )
            raise SupabaseAuthAdminOutcomeUnknownError(
                "the Supabase Auth Admin permanent-deletion outcome is unknown"
            )

    def fetch_user(self, user_id: UUID) -> bool:
        """Report whether the exact Auth user exists (see the module contract)."""
        with application_span(_ADMIN_TRACER_SCOPE, _FETCH_USER_SPAN_NAME) as span:
            annotate_span(span, operation=_FETCH_USER_SPAN_NAME)
            status = self._perform("GET", user_id)
            if 200 <= status < 300:
                return True
            if status == 404:
                return False
            if 400 <= status < 500:
                logger.warning(
                    "Supabase Auth Admin account lookup was definitively rejected "
                    "by the administrative endpoint"
                )
                raise SupabaseAuthAdminRejectedError(
                    "the Supabase Auth Admin boundary rejected the account lookup"
                )
            logger.warning(
                "Supabase Auth Admin request outcome is unknown: the administrative "
                "endpoint reported an unclassified failure"
            )
            raise SupabaseAuthAdminOutcomeUnknownError(
                "the Supabase Auth Admin account-lookup outcome is unknown"
            )

    def _perform(self, method: str, user_id: UUID) -> int:
        """Run one Admin request; return the HTTP status for classification.

        The request URL carries the Supabase project reference and the
        apikey header carries credential material, so neither ever reaches a
        log record, span attribute, or exception message.
        """
        status, _body = self._perform_read(method, user_id)
        return status

    def _perform_read(self, method: str, user_id: UUID) -> tuple[int, bytes]:
        """Run one Admin request; return (status, raw body) for classification.

        The body is retained only for 2xx reads whose documented operation
        parses it (the GitHub identity lookup); failure classification
        deliberately discards it. Transport-level failures (timeout,
        connection loss, DNS) leave the outcome unknown — never reclassified
        as a known failure or success. Fixed safe message only — never the
        URL, the key, or any user-record content.
        """
        url = self._base_url + AUTH_ADMIN_USERS_PATH + f"/{user_id}"
        try:
            status, body = _fetch_admin_response(
                self._fetch,
                url,
                method,
                {"apikey": self._secret_key},
                self._timeout_seconds,
            )
        except (urllib.error.URLError, TimeoutError, OSError) as exc:
            # Timeout or connection loss leaves the outcome unknown; never
            # reclassified as a known failure or success. Fixed safe message
            # only — never the URL, the key, or any user-record content.
            logger.warning(
                "Supabase Auth Admin request outcome is unknown: the administrative "
                "endpoint could not be reached"
            )
            raise SupabaseAuthAdminOutcomeUnknownError(
                "the Supabase Auth Admin endpoint could not be reached"
            ) from exc
        return status, body

    def fetch_user_github_provider_ids(self, user_id: UUID) -> tuple[str | None, ...]:
        """Return the user's GitHub provider facts (see the module contract)."""
        with application_span(_ADMIN_TRACER_SCOPE, _FETCH_GITHUB_PROVIDER_IDS_SPAN_NAME) as span:
            annotate_span(span, operation=_FETCH_GITHUB_PROVIDER_IDS_SPAN_NAME)
            status, body = self._perform_read("GET", user_id)
            if 200 <= status < 300:
                return _parse_github_provider_ids(body)
            if status == 404:
                raise SupabaseAuthAdminUserAbsentError(
                    "the Supabase Auth user addressed for the GitHub identity lookup is absent"
                )
            if 400 <= status < 500:
                logger.warning(
                    "Supabase Auth Admin GitHub identity lookup was definitively "
                    "rejected by the administrative endpoint"
                )
                raise SupabaseAuthAdminRejectedError(
                    "the Supabase Auth Admin boundary rejected the GitHub identity lookup"
                )
            logger.warning(
                "Supabase Auth Admin request outcome is unknown: the administrative "
                "endpoint reported an unclassified failure"
            )
            raise SupabaseAuthAdminOutcomeUnknownError(
                "the Supabase Auth Admin GitHub identity lookup outcome is unknown"
            )

    def __repr__(self) -> str:
        return "HttpSupabaseAuthAdminClient(<redacted>)"

    def __str__(self) -> str:
        return "HttpSupabaseAuthAdminClient(<redacted>)"
