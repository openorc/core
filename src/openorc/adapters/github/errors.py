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
  GitHub authorization.
- ``GitHubRequestRejectedError`` — any other definitive non-success answer.
- ``GitHubOutcomeUncertainError`` — timeout, connection loss, a 3xx the
  transport deliberately does not follow, 5xx, or an otherwise uninterpretable
  response. The outcome is neither success nor known failure; callers
  reconcile, they do not blindly replay.

Error messages are deliberately safe by authoring: they never contain the App
private key, installation access tokens, provider URLs, or response bodies.
"""

from __future__ import annotations

__all__ = [
    "GitHubAuthenticationRejectedError",
    "GitHubAuthorizationRejectedError",
    "GitHubOutcomeUncertainError",
    "GitHubRateLimitedError",
    "GitHubRequestRejectedError",
]


class GitHubRequestRejectedError(Exception):
    """GitHub answered a definitive non-success (known failure)."""


class GitHubAuthenticationRejectedError(GitHubRequestRejectedError):
    """GitHub rejected the presented App JWT or installation token (401)."""


class GitHubAuthorizationRejectedError(GitHubRequestRejectedError):
    """GitHub denied access to the addressed installation or repository.

    Authorization absence is a known integration condition — never a trigger
    for a human-credential fallback.
    """


class GitHubRateLimitedError(GitHubRequestRejectedError):
    """GitHub exhausted the rate-limit budget for the presented credential.

    Deliberately distinct from :class:`GitHubAuthorizationRejectedError`:
    a rate limit is not lost repository access.
    """


class GitHubOutcomeUncertainError(Exception):
    """The outcome of a GitHub operation is unknown.

    Timeout, connection loss, a redirect the transport refuses to follow,
    5xx answers, or an uninterpretable response leave the result neither
    known success nor known failure. Callers reconcile; uncertain outcomes
    are never silently replayed by this adapter.
    """
