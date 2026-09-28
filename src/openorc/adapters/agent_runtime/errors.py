"""Normalized Agent Runtime adapter failure taxonomy (issue #67).

These typed errors are the normalized boundary between concrete Agent Runtime
adapters and the application services above. Every failure of the universal
session operations classifies as exactly one of them; services translate them
into the typed application-error vocabulary (:mod:`openorc.services.errors`).
The shared contract never imports services and never creates a runtime-shaped
workflow exception hierarchy, and the taxonomy never mirrors provider status
codes as dozens of enum values.

Outcome classification (the discipline downstream code relies on):

- ``AgentRuntimeConfigurationRejectedError`` — authentication/configuration
  rejection: a known failure decided before any effect. Deliberately distinct
  from runtime unavailability.
- ``AgentRuntimeUnavailableError`` — the runtime was unreachable or
  unavailable with no knowledge of any effect, because the operation never
  got that far (unavailable strictly before known effect).
- ``AgentRuntimeUncertainOutcomeError`` — the outcome is unknown whether the
  operation took effect. This is deliberately not equated with non-delivery:
  callers reconcile against current durable state instead of blindly
  replaying, and the adapter never silently replays an uncertain operation.
  - ``AgentRuntimeTimeoutError`` — the operation exceeded its deadline, so
    the effect may exist even though no answer arrived.
  - ``AgentRuntimeDeliveryUncertainError`` — the transport/connection broke
    mid-operation, so delivery/effect cannot be classified.
- ``AgentSessionNotFoundError`` — the exact supplied opaque session ID does
  not address a live session on this runtime: it never existed there, was
  never successfully created there, or is genuinely lost. Distinct from
  runtime unavailability: the runtime answered, and the addressed session is
  gone. This never triggers session replacement, redirection, or replay.
- ``AgentRuntimeTransportError`` — the runtime answered, but its native
  response could not be extracted from the transport/framing layer before
  any formal-object work.
- ``AgentRuntimeProtocolFailureError`` — the extracted candidate response
  failed the shared canonical v1 formal-response contract (issue #65). The
  original typed ``ProtocolError`` stays chained as ``__cause__``; the
  adapter never repairs, reinterprets, manufactures a formal result, or
  silently retries it.

Error messages are safe by authoring: they never contain credentials/tokens,
Workspace guidance, prompt/initialization bodies, raw model/runtime
responses, transcripts, or provider-native exception text, because such
payloads may carry sensitive content.
"""

from __future__ import annotations

__all__ = [
    "AgentRuntimeConfigurationRejectedError",
    "AgentRuntimeDeliveryUncertainError",
    "AgentRuntimeError",
    "AgentRuntimeProtocolFailureError",
    "AgentRuntimeTimeoutError",
    "AgentRuntimeTransportError",
    "AgentRuntimeUnavailableError",
    "AgentRuntimeUncertainOutcomeError",
    "AgentSessionNotFoundError",
]


class AgentRuntimeError(Exception):
    """Base of the normalized Agent Runtime adapter failure taxonomy."""


class AgentRuntimeConfigurationRejectedError(AgentRuntimeError):
    """The runtime rejected the presented authentication/configuration.

    A known failure with no effect: the presented credentials or
    configuration were rejected before the operation could take effect.
    """


class AgentRuntimeUnavailableError(AgentRuntimeError):
    """The runtime was unreachable or unavailable before any known effect."""


class AgentRuntimeUncertainOutcomeError(AgentRuntimeError):
    """The outcome is unknown whether the operation took effect.

    Neither success nor known failure: callers reconcile against current
    durable state instead of blindly replaying the operation.
    """


class AgentRuntimeTimeoutError(AgentRuntimeUncertainOutcomeError):
    """The operation exceeded its deadline.

    A timeout is an uncertain outcome, never a known non-delivery: the
    effect may exist even though no answer arrived.
    """


class AgentRuntimeDeliveryUncertainError(AgentRuntimeUncertainOutcomeError):
    """Delivery/effect became unclassifiable mid-operation.

    The transport or connection broke during the operation, so neither a
    known answer nor a known pre-effect failure is available.
    """


class AgentSessionNotFoundError(AgentRuntimeError):
    """The exact addressed opaque session ID does not address a live session.

    Covers both a session that never existed or was never successfully
    created on this runtime and a session that is genuinely lost.
    Deliberately distinct from runtime unavailability, and never a trigger
    for creating a replacement session, redirecting to another conversation,
    or replaying the operation.
    """


class AgentRuntimeTransportError(AgentRuntimeError):
    """The runtime answered, but its native response could not be extracted.

    A framing/transport failure of the runtime-native response envelope,
    surfaced before any formal-object extraction is attempted.
    """


class AgentRuntimeProtocolFailureError(AgentRuntimeError):
    """The extracted candidate response failed the shared v1 formal contract.

    The shared canonical protocol parser (``openorc.protocol``, issue #65)
    rejected the runtime-envelope-stripped candidate response. The original
    typed protocol error stays chained as ``__cause__``; the adapter never
    repairs, reinterprets, manufactures a formal result, or silently
    retries. The wrapper message carries no response content: the shared
    protocol errors are already safe by authoring, and raw responses never
    enter adapter error messages.
    """
