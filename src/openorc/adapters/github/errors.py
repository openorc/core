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
    "GitHubRateLimitedError",
    "GitHubRequestRejectedError",
]


class GitHubRequestRejectedError(Exception):
    """GitHub answered a definitive non-success (known failure).

    ``status_code`` carries the bare HTTP status of the definitive answer so
    a documented operation can apply its own finer documented response
    classification inside the adapter (for example the merge endpoint's
    documented 409 expected-head-mismatch answer). It is adapter-internal
    request mechanics: application services consume only the normalized
    adapter outcomes and never branch on raw provider status codes, and the
    attribute never carries provider content beyond the numeric status.
    """

    def __init__(self, message: str, *, status_code: int | None = None) -> None:
        super().__init__(message)
        self.status_code = status_code


class GitHubAuthenticationRejectedError(GitHubRequestRejectedError):
    """GitHub rejected the presented App JWT or installation token (401)."""


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
