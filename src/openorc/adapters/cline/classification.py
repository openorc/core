"""Documented safe classification of #71 backend failures into the #67 taxonomy.

This module carries the one authoritative translation from the
``openorc.adapters.cline.errors`` backend failure taxonomy onto the normalized
universal ``openorc.adapters.agent_runtime`` error taxonomy. It was promoted
from the Cline adapter (issue #74) so the adapter and the attachment
composition (``services/cline_attachment.py``, issue #190) classify the same
backend failure identically from one source of truth; the adapter's behavior
is unchanged.

Documented safe classification grounded in the D2 taxonomy and the qualified
baseline (R2b/R3/R4). The backend error object itself is discarded by the
caller after this classification; only these safe machine-identifier messages
survive. Known pre-effect classifications are preserved only where positive
code/phase evidence supports them: the two qualified hub transport codes
classify attachment state, and a pure read can never take an external effect.
Any other SDK machine code is forwarded by the pinned bridge without
establishing whether the operation took effect, so an unproven effectful
``start``/``send``/``stop`` outcome is conservatively uncertain — never a
known pre-effect failure, never a safe retry. Deliberate non-inferences: bad
credentials are never inferred from ``hub_connect_failed`` alone, exact-session
loss is never inferred from connection loss, and an uncertain
``start``/``send``/``stop`` is never downgraded to a safe retry. The backend
taxonomy currently carries no positive timeout marker, so every uncertain
backend outcome maps to delivery uncertainty.
"""

from __future__ import annotations

from typing import Final

from openorc.adapters.agent_runtime.errors import (
    AgentRuntimeConfigurationRejectedError,
    AgentRuntimeDeliveryUncertainError,
    AgentRuntimeError,
    AgentRuntimeTransportError,
    AgentRuntimeUnavailableError,
    AgentRuntimeUncertainOutcomeError,
    AgentSessionNotFoundError,
)
from openorc.adapters.cline.errors import (
    ClineBackendUncertainOutcomeError,
    ClineBridgeProtocolError,
    ClineRemoteAttachmentRejectedError,
    ClineRemoteAttachmentUnavailableError,
    ClineSdkBackendError,
    ClineSdkOperationRejectedError,
    ClineSessionNotFoundError,
)

__all__ = ["EFFECTFUL_OPERATIONS", "classify_backend_failure"]


# Safe machine classifications of the qualified baseline's connection-level
# SDK rejection codes (R2b/R4). They classify transport state only — never
# credentials, never exact-session loss.
_HUB_CONNECT_FAILED: Final = "hub_connect_failed"
_HUB_CONNECTION_CLOSED: Final = "hub_connection_closed"

# D3's bridge wire-protocol rejection for operations attempted before a
# successful attachment: positively pre-effect unavailability.
_NOT_ATTACHED: Final = "not_attached"

# Operations whose effect is an external session allocation, an incarnation
# change, or an activity interruption: a connection-level breakage during
# them leaves the effect unknown.
EFFECTFUL_OPERATIONS: Final[frozenset[str]] = frozenset({"start", "send", "stop", "abort"})


def classify_backend_failure(error: ClineSdkBackendError, *, operation: str) -> AgentRuntimeError:
    """Translate one #71 backend failure into the normalized #67 taxonomy."""
    if isinstance(error, ClineSessionNotFoundError):
        return AgentSessionNotFoundError(
            "the exact addressed external session is not live on this runtime"
        )
    if isinstance(error, ClineBackendUncertainOutcomeError):
        return AgentRuntimeDeliveryUncertainError(
            f"the {operation} outcome is unknown whether it took effect"
        )
    if isinstance(error, ClineRemoteAttachmentRejectedError):
        return AgentRuntimeConfigurationRejectedError(
            f"the runtime rejected the {operation} attachment configuration"
        )
    if isinstance(error, ClineRemoteAttachmentUnavailableError):
        return AgentRuntimeUnavailableError(
            f"the runtime was unreachable for the {operation} before any known effect"
        )
    if isinstance(error, ClineBridgeProtocolError):
        if error.reason == _NOT_ATTACHED:
            return AgentRuntimeUnavailableError(
                f"the bridge attachment was not established for the {operation}"
            )
        return AgentRuntimeConfigurationRejectedError(
            f"the bridge wire protocol rejected the {operation} ({error.reason!r})"
        )
    if isinstance(error, ClineSdkOperationRejectedError):
        if error.code == _HUB_CONNECT_FAILED:
            return AgentRuntimeUnavailableError(
                f"the remote hub could not be reached for the {operation} ({error.code!r})"
            )
        if error.code == _HUB_CONNECTION_CLOSED:
            if operation in EFFECTFUL_OPERATIONS:
                return AgentRuntimeUncertainOutcomeError(
                    f"the connection closed around the {operation}; its effect cannot be classified"
                )
            return AgentRuntimeUnavailableError(
                f"the connection closed before the {operation} could be served ({error.code!r})"
            )
        # Any other code is forwarded by the pinned bridge without
        # establishing whether the operation took effect. For effectful
        # operations there is no positive evidence of non-effect, so the
        # outcome stays conservatively uncertain (and latches same-ID
        # reconstruction); a pure read can never take an external effect,
        # so its answered-but-unusable response is a transport-class failure.
        if operation in EFFECTFUL_OPERATIONS:
            return AgentRuntimeUncertainOutcomeError(
                f"the {operation} was rejected with an unproven effect ({error.code!r})"
            )
        return AgentRuntimeTransportError(
            f"the {operation} read was answered without a usable response ({error.code!r})"
        )
    raise AssertionError(  # pragma: no cover - the D2 taxonomy is closed
        f"unclassified cline backend failure: {type(error).__name__}"
    )
