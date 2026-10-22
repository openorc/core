"""Transport-neutral typed application errors for expected failures.

Application services raise these errors for expected failures; the caller
that owns a transport (FastAPI router, RQ job wrapper, ...) translates them
into transport behavior. The vocabulary is deliberately transport-neutral:
it imports no FastAPI, RQ, adapter, or provider machinery and carries no
status-code semantics of its own.

The vocabulary is minimal by design — only the categories demonstrated by
Phase 2A needs. It is not an exception registry, a result-monad layer, or a
command bus. Error instances are message-typed, and the types do not
sanitize message content: keeping errors safe to surface is an authoring
obligation. Service authors must provide safe application-level messages
and must never place raw credentials, tokens, or a second copy of canonical
domain state in an exception message; explicit keyword fields added by
later capabilities are likewise required to hold only safe values.
"""

from __future__ import annotations

__all__ = [
    "ApplicationError",
    "AuthenticationError",
    "AuthorizationError",
    "ConflictError",
    "ExternalOperationFailedError",
    "ExternalOperationRejectedError",
    "ExternalOperationUncertainError",
    "IntegrationNotConfiguredError",
    "InvalidCommandError",
    "MergeConfirmedReconciliationPendingError",
    "NotFoundError",
    "StaleOperationError",
]


class ApplicationError(Exception):
    """Base of the expected application-failure vocabulary.

    Transport-neutral by construction: translated into transport behavior
    only by the transport that catches it. The types do not sanitize
    message content — service authors must provide safe application-level
    messages and must never place credentials, tokens, or a second copy of
    domain state in an error message or in any explicit field.
    """


class AuthenticationError(ApplicationError):
    """The caller could not be authenticated.

    Identity could not be established or verified (invalid or expired
    credential, or an identity whose backing account no longer exists).
    Failure messages must describe the failure without revealing token or
    credential contents.
    """


class AuthorizationError(ApplicationError):
    """The authenticated caller is not permitted to perform the operation."""


class NotFoundError(ApplicationError):
    """The addressed subject does not exist."""


class IntegrationNotConfiguredError(ApplicationError):
    """The operation's required integration configuration is absent.

    The caller addressed a boundary whose required deployment/bootstrap
    configuration or secret material is not configured for this process. The
    operation fails closed: it never proceeds on a best-effort basis and
    never manufactures a substitute. Messages must describe the absence
    without carrying the missing configuration's value.
    """


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


class ExternalOperationRejectedError(ExternalOperationFailedError):
    """A known failure the external system decided itself: it received and
    refused the addressed operation under its own policy or state.

    A machine-readable subtype of :class:`ExternalOperationFailedError` for
    callers that must separate a provider-owned refusal of the operation
    itself from the other known failures of the same operation
    (authentication/authorization rejections, rate limits, pre-delivery
    credential-resolution failures). The refused operation provably did not
    apply, and the refusal provenance is the provider's own policy decision
    — never an access or delivery condition. Callers that do not need the
    distinction keep handling the superclass unchanged; the distinction is
    made by type only, never by inspecting messages or chained provider
    exceptions.
    """


class MergeConfirmedReconciliationPendingError(ApplicationError):
    """An external mutation is confirmed applied, but its authoritative
    durable reconciliation is unconfirmed.

    Raised when the external system answered a known success and the
    follow-up authoritative reconciliation that persists the durable facts
    failed — or succeeded without yet confirming the mutation durably (for
    example, the observed snapshot still reads unmerged): the mutation
    phase and its outcome evidence are preserved for the caller instead of
    being lost behind a reconciliation error or misread as a bare success.
    The caller must never repeat the mutation; it re-attempts
    reconciliation and fails closed while the durable facts stay
    unconfirmed. The explicit fields carry only safe canonical identifiers.
    """

    def __init__(self, message: str, *, merge_commit_sha: str, expected_head_sha: str) -> None:
        super().__init__(message)
        self.merge_commit_sha = merge_commit_sha
        self.expected_head_sha = expected_head_sha
