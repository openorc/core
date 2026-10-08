"""Compact backend-local failure taxonomy of the Cline SDK backend (issue #71).

These typed errors are the failure vocabulary of the language-neutral
``ClineSdkBackend`` seam (:mod:`openorc.adapters.cline.contract`): the
Python transport/process layer (#73) reports them and the Python Cline
adapter (#74) and downstream reconciliation (D6) classify them. They are
deliberately compact — four machine-distinguishable classifications plus the
SDK rejection carrier — and never mirror provider status codes as dozens of
enum values. Mapping onto the normalized universal
``openorc.adapters.agent_runtime`` error taxonomy is the adapter's job
(D5a); this module creates no second public domain taxonomy.

Outcome classification (the discipline downstream code relies on):

- ``ClineRemoteAttachmentRejectedError`` — the remote attachment or its
  configuration was rejected: a known failure decided before any effect.
  Deliberately distinct from unavailability.
- ``ClineRemoteAttachmentUnavailableError`` — the remote could not be
  reached or attached with no knowledge of any effect: unavailable strictly
  before known effect.
- ``ClineBackendUncertainOutcomeError`` — an operation's transport/bridge
  failure or timeout with uncertain effect: an in-flight send/start/control
  might already have reached the Hub. Deliberately not equated with
  non-delivery; callers reconcile against current durable state instead of
  blindly replaying.
- ``ClineSessionNotFoundError`` — positively identified exact-session
  absence: the supplied opaque session ID does not address a session on
  this backend. Distinct from unavailability (the backend answered), and
  never a trigger for session replacement, redirection, or replay.
- ``ClineSdkOperationRejectedError`` — the SDK rejected the operation with
  an available safe machine code (for example the qualified
  ``hub_connection_closed`` / ``hub_connect_failed``) and bounded safe
  structured details (for example a numeric close code). A bare
  empty-message ``hub_connect_failed`` must not be interpreted as
  identifying bad credentials versus a down Hub: the code is preserved for
  downstream classification, never interpreted here.

Non-inferences: terminal session loss is never inferred from ``stop``,
``abort``, bridge death, WebSocket 1006, or a session record's ``status``.
Public-evidence classification and reconciliation belong to D6; no recovery
algorithm lives in this module.

Safety by authoring: error messages, reprs, and the ``details`` carrier
never contain raw SDK/provider exception text, stack traces, URLs with
credentials, tokens, prompts, responses, session transcripts, or arbitrary
JS errors. Native exceptions are discarded at the boundary — constructors
and raisers never chain them (traceback/telemetry handling can traverse
``__cause__``/``__context__``), so concrete implementations convert native
failures into these sanitized errors after the failing handler exits.
``ClineSdkOperationRejectedError.details`` additionally never appears in
the exception's string or representation.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from openorc.adapters.cline.values import JsonObject

__all__ = [
    "ClineBackendUncertainOutcomeError",
    "ClineRemoteAttachmentRejectedError",
    "ClineRemoteAttachmentUnavailableError",
    "ClineSdkBackendError",
    "ClineSdkOperationRejectedError",
    "ClineSessionNotFoundError",
]


class ClineSdkBackendError(Exception):
    """Base of the Cline SDK backend failure taxonomy."""


class ClineRemoteAttachmentRejectedError(ClineSdkBackendError):
    """The remote attachment/configuration was rejected before any effect.

    A known failure with no effect: the presented attachment inputs or
    configuration were rejected before the operation could take effect.
    """


class ClineRemoteAttachmentUnavailableError(ClineSdkBackendError):
    """The remote was unreachable or unattachable before any known effect."""


class ClineBackendUncertainOutcomeError(ClineSdkBackendError):
    """The outcome is unknown whether the operation took effect.

    A transport/bridge failure or timeout of an operation whose effect — an
    in-flight send/start/control — might already have reached the Hub.
    Neither success nor known failure: callers reconcile against current
    durable state instead of blindly replaying the operation.
    """


class ClineSessionNotFoundError(ClineSdkBackendError):
    """The exact addressed opaque session ID does not address a session here.

    Positively identified exact-session absence — distinct from remote
    unavailability (the backend answered), and never a trigger for creating
    a replacement session, redirecting to another conversation, or
    replaying the operation.
    """


class ClineSdkOperationRejectedError(ClineSdkBackendError):
    """The SDK rejected the operation with a safe machine code.

    ``code`` is a safe machine identifier from the qualified baseline (for
    example ``hub_connection_closed`` / ``hub_connect_failed``), preserved
    for downstream classification. ``details`` carries optional bounded
    safe structured details (for example a numeric close code) and is
    excluded from the exception's string and representation. Neither value
    is interpreted here: a bare empty-message ``hub_connect_failed`` does
    not identify bad credentials versus a down Hub.
    """

    def __init__(self, code: str, details: JsonObject | None = None) -> None:
        if not isinstance(code, str) or not code.strip():
            raise ValueError("ClineSdkOperationRejectedError.code must be a non-empty string")
        if details is not None and (
            not isinstance(details, dict) or any(not isinstance(key, str) for key in details)
        ):
            raise ValueError("ClineSdkOperationRejectedError.details must be None or a JSON object")
        self.code = code
        self.details = details
        super().__init__(f"cline sdk operation rejected with machine code {code!r}")

    def __repr__(self) -> str:
        return f"ClineSdkOperationRejectedError(code={self.code!r})"
