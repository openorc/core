"""Normalized GitHub adapter error boundary (issue #58).

The adapter-local error types are the normalized boundary between the GitHub
REST transport/authentication mechanics and the application services above.
Services translate them into the typed application-error vocabulary
(:mod:`openorc.services.errors`) — adapters never import services and never
create a GitHub-shaped workflow exception hierarchy.

Outcome classification (the discipline the services rely on):

- ``GitHubAuthenticationRejectedError`` — GitHub answered 401: the presented
  App JWT or installation access token was rejected. A known failure that
  never means the underlying operation's effect is known.
- ``GitHubAuthorizationRejectedError`` — GitHub answered 403/404 where access
  to the addressed resource is the question, or the installation fails the
  capability/suspension validation: authorization absence. A known,
  safe-to-classify condition. It never triggers a fallback to a human
  credential — human GitHub sign-in is identity only, and no PAT or human
  OAuth token fallback exists anywhere in this adapter.
- ``GitHubRateLimitedError`` — GitHub answered 403/429 with an exhausted
  rate-limit budget. A known failure that is deliberately NOT an
  authorization absence: misclassifying a rate limit as lost repository
  access would let a workflow conclude wrongly that a Workspace lost
  GitHub authorization. When GitHub supplies them, the bounded safe
  scheduling facts (``Retry-After``, ``X-RateLimit-Reset``) are normalized
  onto the error boundary as typed fields; provider headers never leak
  above the adapter, and the adapter never sleeps, queues, or retries.
- ``GitHubRequestRejectedError`` — any other definitive non-success answer.
- ``GitHubOutcomeUncertainError`` — timeout, connection loss, a redirect the
  safe redirect policy refuses to follow (cross-origin, malformed, loop,
  excess, or a non-read method), 5xx, or an otherwise uninterpretable
  response. The outcome is neither success nor known failure; callers
  reconcile, they do not blindly replay.

Error messages are deliberately safe by authoring: they never contain the App
private key, installation access tokens, provider URLs, or response bodies.
"""

from __future__ import annotations

from datetime import datetime

__all__ = [
    "GitHubAuthenticationRejectedError",
    "GitHubAuthorizationRejectedError",
    "GitHubOutcomeUncertainError",
    "GitHubPullRequestExistsError",
    "GitHubRateLimitedError",
    "GitHubRequestRejectedError",
    "GitHubUserTokenRefreshCapabilityMissingError",
    "GitHubUserTokenRejectedError",
]


class GitHubRequestRejectedError(Exception):
    """GitHub answered a definitive non-success (known failure).

    ``status_code`` carries the bare HTTP status of the definitive answer so
    a documented operation can apply its own finer documented response
    classification inside the adapter (for example the merge endpoint's
    documented 409 expected-head-mismatch answer). ``response_body`` carries
    the transport's already-bounded failure body for the same adapter-
    internal finer classification only (the create-pull-request operation's
    documented duplicate-PR 422 members) — it is never echoed into error
    messages, logs, or telemetry, and application services never see it: the
    attribute is adapter-internal request mechanics.
    """

    def __init__(
        self,
        message: str,
        *,
        status_code: int | None = None,
        response_body: bytes | None = None,
    ) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.response_body = response_body


class GitHubAuthenticationRejectedError(GitHubRequestRejectedError):
    """GitHub rejected the presented App JWT or installation token (401)."""


class GitHubUserTokenRejectedError(GitHubRequestRejectedError):
    """The GitHub user-token endpoint definitively rejected the grant (issue #142).

    A known failure of the web-flow authorization-code exchange or the
    refresh exchange: GitHub answered with its documented OAuth error shape
    (``error``/``error_description`` — which the token endpoint may deliver
    even under a 200 status) or a definitive 4xx answer. The known rejection
    is never reclassified as uncertainty, and the caller never blindly
    replays it: an uncertain token answer (timeout, connection loss,
    uninterpretable response) is a :class:`GitHubOutcomeUncertainError`
    instead — deliberately distinct, because a refresh token GitHub may have
    consumed is not safe to reuse after an unknown outcome.
    """


class GitHubUserTokenRefreshCapabilityMissingError(GitHubRequestRejectedError):
    """The token answer cannot establish the required expiring-token lifecycle.

    GitHub answered definitively but without the expiring user access token
    capability OpenOrc requires (no refresh token/expiry in the response —
    the documented shape when a GitHub App has opted out of expiring
    user-to-server tokens). A known, safe-to-classify misconfiguration
    condition: OpenOrc never persists a long-lived user access token as a
    second credential model.
    """


class GitHubPullRequestExistsError(GitHubRequestRejectedError):
    """GitHub answered the documented 'pull request already exists' rejection.

    The PR-create operation's own documented finer classification of a
    definitive rejection (a branch already has an open PR toward the base).
    Carries only the bare HTTP status like every definitive rejection —
    never provider content. Whether this means a silent adoption, a
    conflict, or a replay condition is workflow meaning decided strictly
    above the adapter.
    """


class GitHubAuthorizationRejectedError(GitHubRequestRejectedError):
    """GitHub denied access to the addressed installation or repository.

    Authorization absence is a known integration condition — never a trigger
    for a human-credential fallback.
    """


class GitHubRateLimitedError(GitHubRequestRejectedError):
    """GitHub rate limited the presented credential (403/429).

    Either documented signal classifies here: an exhausted
    ``X-RateLimit-Remaining`` budget (the primary limit) or a
    ``Retry-After`` header (the documented secondary-limit signal, which
    may be present while the primary budget is not exhausted).
    Deliberately distinct from :class:`GitHubAuthorizationRejectedError`:
    a rate limit is not lost repository access.

    Normalized safe scheduling facts (issue #122): when GitHub supplies
    them, ``retry_after_seconds`` carries the documented ``Retry-After``
    delay-seconds value and ``rate_limit_reset_at`` the documented
    ``X-RateLimit-Reset`` epoch value as a UTC datetime. Absent, malformed,
    or absurd values normalize to ``None`` without changing the
    classification, raw header values never enter messages or telemetry,
    and the adapter never sleeps, queues, or retries — scheduling policy
    belongs above the adapter.
    """

    def __init__(
        self,
        message: str,
        *,
        status_code: int | None = None,
        retry_after_seconds: float | None = None,
        rate_limit_reset_at: datetime | None = None,
    ) -> None:
        super().__init__(message, status_code=status_code)
        self.retry_after_seconds = retry_after_seconds
        self.rate_limit_reset_at = rate_limit_reset_at


class GitHubOutcomeUncertainError(Exception):
    """The outcome of a GitHub operation is unknown.

    Timeout, connection loss, a redirect the transport refuses to follow,
    5xx answers, or an uninterpretable response leave the result neither
    known success nor known failure. Callers reconcile; uncertain outcomes
    are never silently replayed by this adapter.
    """
