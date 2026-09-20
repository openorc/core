"""Transport-neutral typed application errors for expected failures.

Application services raise these errors for expected failures; the caller
that owns a transport (FastAPI router, RQ job wrapper, ...) translates them
into transport behavior. The vocabulary is deliberately transport-neutral:
it imports no FastAPI, RQ, adapter, or provider machinery and carries no
status-code semantics of its own.

The vocabulary is minimal by design — only the categories demonstrated by
Phase 2A needs. It is not an exception registry, a result-monad layer, or a
command bus. Error instances are message-typed; when a later capability
demonstrates the need, it adds explicit safe keyword fields — never
credentials, tokens, or a second copy of canonical domain state — so errors
remain safe to surface to callers and logs.
"""

from __future__ import annotations

__all__ = [
    "ApplicationError",
    "AuthenticationError",
    "AuthorizationError",
    "ConflictError",
    "ExternalOperationFailedError",
    "ExternalOperationUncertainError",
    "InvalidCommandError",
    "NotFoundError",
    "StaleOperationError",
]


class ApplicationError(Exception):
    """Base of the expected application-failure vocabulary.

    Transport-neutral by construction: translated into transport behavior
    only by the transport that catches it, and safe to log — errors never
    contain credentials, tokens, or a second copy of domain state.
    """


class AuthenticationError(ApplicationError):
    """The caller could not be authenticated.

    Identity could not be established or verified (invalid or expired
    credential, or an identity whose backing account no longer exists).
    Never reveals token or credential contents.
    """


class AuthorizationError(ApplicationError):
    """The authenticated caller is not permitted to perform the operation."""


class NotFoundError(ApplicationError):
    """The addressed subject does not exist."""


class InvalidCommandError(ApplicationError):
    """The requested operation is not a valid command or input.

    The command cannot be interpreted on its own terms, independent of
    current durable state.
    """


class ConflictError(ApplicationError):
    """The operation conflicts with current durable state."""


class StaleOperationError(ConflictError):
    """The operation addressed state that has since moved on.

    A distinct kind of conflict: the operation's expected subject or
    authority context (state token, current plan revision, gate,
    execution, PR head, runtime request, or equivalent) no longer matches
    current durable state. The operation is stale: it is never applied and
    never blindly retried; recovery context is surfaced instead.
    """


class ExternalOperationFailedError(ApplicationError):
    """A known failure of a named external operation.

    The external system answered — or failed — in a way that establishes
    the outcome as a known non-success. This is distinct from uncertainty:
    a timeout, connection loss, or uninterpretable response is an
    :class:`ExternalOperationUncertainError`, never silently reclassified
    as success or as a safe retry.
    """


class ExternalOperationUncertainError(ApplicationError):
    """The outcome of an external operation is unknown.

    Timeout, connection loss, or an uninterpretable response leaves the
    outcome neither known success nor known failure. Treat it as neither:
    reconcile authoritatively where possible, retry only when provably
    safe, or block for recovery — the LLM is never the idempotency
    mechanism.
    """
