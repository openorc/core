"""The minimal runtime-neutral Agent Runtime adapter contract (issue #67).

Concrete Agent Runtime adapters — the deterministic fake runtime (#69), the
official Cline adapter (#73), and any later runtime — implement this ABC;
application services consume its typed results and normalized error
taxonomy. The universal surface carries exactly the three demonstrated v1
operations:

- ``create_session``: one logical readiness handshake per exact Task/role;
- ``send``: one exact-session interaction, formal or non-formal;
- ``realize_control``: realization of one already-authorized semantic #129
  workflow control.

There is deliberately no universal ``close_session``/``end_session``
operation and no session-status polling: runtime-specific cancel/resume,
inspection, telemetry, and lifecycle mechanics remain optional local adapter
capabilities that never expand this interface. Provider/model selection,
Cline modes, telemetry streams, approvals, filesystem/Git operations, and
other provider-native capabilities are likewise never exposed here, and
there is no capability marketplace.

Authority and meaning stay above this boundary. Adapters own runtime/provider
mechanics and normalization only: they never decide workflow meaning, never
mutate durable OpenOrc persistence, and never decide whether a workflow
control is authorized. Formal-response validation stays in the shared
``openorc.protocol`` package (issue #65): a concrete adapter strips its own
runtime-native outer envelope and funnels the candidate through
``_parse_formal_candidate`` — formal OpenOrc schemas are never duplicated in
adapters. Session operations are always addressed to the exact opaque
external session ID supplied by the durable binding and never replace,
redirect, merge, or reuse sessions.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, cast

from openorc.adapters.agent_runtime.errors import AgentRuntimeProtocolFailureError
from openorc.adapters.agent_runtime.results import (
    AgentSessionCreated,
    AgentSessionCreationRequest,
    AgentTextResponse,
)
from openorc.protocol.errors import ProtocolError
from openorc.protocol.interaction import Interaction
from openorc.protocol.models import FORMAL_RESPONSE_FAMILIES, FormalResponse
from openorc.protocol.parsing import parse_formal_response

__all__ = ["AgentRuntimeAdapter"]


class AgentRuntimeAdapter(ABC):
    """The minimal runtime-neutral Agent Runtime session contract.

    Implementation obligations: every failure surfaces as exactly one of the
    normalized errors of :mod:`openorc.adapters.agent_runtime.errors`
    (``AgentRuntimeError`` subtypes); every success returns the typed
    normalized results of :mod:`openorc.adapters.agent_runtime.results`; and
    normalized messages never echo credentials, Workspace guidance,
    prompt/initialization bodies, raw responses, transcripts, or
    provider-native exception text. The same discipline holds for the whole
    exception chain: traceback/telemetry handling can traverse
    ``__cause__``/``__context__``, so provider/runtime-native exceptions are
    discarded at the boundary and never retained anywhere reachable by that
    traversal. Because ``raise ... from None`` inside a failing handler
    suppresses only the displayed chain and still populates ``__context__``,
    concrete adapters raise the normalized error after the handler exits
    (or otherwise convert the native failure to a separately sanitized
    summary). The one deliberate exception is the shared #65
    ``ProtocolError`` chained inside ``AgentRuntimeProtocolFailureError`` —
    that error vocabulary is itself safe by authoring (it names only the
    response family, the failing schema location, and the failing keyword).
    """

    @abstractmethod
    def create_session(self, request: AgentSessionCreationRequest) -> AgentSessionCreated:
        """Establish one ready isolated external session for the exact Task/role.

        One logical readiness handshake, not merely external-context
        allocation: establish a fresh isolated external conversational
        context for the exact Task/role, deliver the request's composed
        initialization content, extract and normalize the agent's response,
        validate it as a canonical ``session_ready`` formal response through
        the shared parser, and return the exact opaque external session ID
        with only safe normalized observations.

        An externally allocated session ID whose readiness response is
        invalid, or whose creation outcome is known-failed or uncertain, is
        an internal runtime fact: it is never returned as a successful
        result. The failure surfaces as the matching normalized error — a
        normalized protocol failure for invalid readiness, a configuration
        rejection for rejected authentication/configuration, an unavailable
        error strictly before any effect, and an uncertain outcome where the
        effect cannot be classified. Creation itself mutates no durable
        OpenOrc persistence: the durable CONNECTING/READY transition belongs
        to the application service above.
        """

    @abstractmethod
    def send(
        self,
        session_id: str,
        message: str,
        *,
        expected_family: str | None = None,
    ) -> FormalResponse | AgentTextResponse:
        """Send one composed message to the exact addressed external session.

        The operation is always addressed to the exact supplied opaque
        session ID. Adapters never create a replacement session when the
        exact ID is missing, never redirect to a latest conversation, never
        merge Producer/Reviewer contexts, and never reuse a session across
        Tasks/roles; an exact-session loss surfaces as
        ``AgentSessionNotFoundError``, deliberately distinct from runtime
        unavailability.

        When ``expected_family`` names a canonical v1 response family, the
        send is a formal interaction: the adapter extracts the runtime-native
        candidate response, strips its own runtime-specific outer
        envelope/completion wrapper, and funnels the candidate through
        ``_parse_formal_candidate`` with the expected family, returning the
        typed formal result. Parsing failures surface as normalized protocol
        failures — never repaired, reinterpreted, manufactured, or retried;
        surrounding prose never supplies semantics.

        When ``expected_family`` is None, the send is an ordinary
        exact-session interaction (for example advisory Owner ↔ Reviewer
        discussion routed through the existing Reviewer session): the adapter
        returns the extracted, envelope-stripped reply as an
        ``AgentTextResponse`` without invoking formal-response parsing. A
        reply that resembles or contains a formal JSON object stays ordinary
        text: the caller's expected family is the only trigger for #65
        parsing.

        ``message`` is composed interaction prose supplied by the application
        layer; the adapter delivers it verbatim and never authors or
        re-renders prompt content.
        """

    def realize_control(
        self,
        session_id: str,
        interaction: Interaction,
        *,
        message: str | None = None,
        expected_family: str | None = None,
    ) -> FormalResponse | None:
        """Realize one already-authorized semantic #129 workflow control.

        The semantic control arrives distinctly from its rendered prose:
        ``interaction`` is the semantic control, and ``message`` is its
        already-composed interaction prose and ``expected_family`` its
        expected formal response family, both supplied by the application
        layer (the interaction-to-family mapping stays with the caller).

        The shared default realization is the common prose-only path: send
        the composed message and return its typed formal result. Concrete
        adapters may override realization where runtime-native action is
        required in addition to/instead of prose, dispatching locally on the
        semantic control kind; the shared contract never hard-codes any
        runtime's native action vocabulary, and no runtime-specific symbol
        appears here.

        Returns the typed formal response when the realization includes a
        formal send, and None when the control was realized without a formal
        response (runtime-native action only). The adapter never decides
        whether the control is authorized: it is handed an
        already-authorized semantic control.
        """
        if message is None:
            raise ValueError(
                "the shared prose-only control realization requires the composed "
                "interaction message; runtime-native realization without prose "
                "belongs to the concrete adapter override"
            )
        if not message.strip():
            raise ValueError("the composed control message must be a non-empty string")
        if expected_family is None:
            raise ValueError(
                "the composed control must declare its expected formal response family"
            )
        result = self.send(
            session_id,
            message,
            expected_family=expected_family,
        )
        # A send with a non-None expected family always produces a typed
        # formal result or raises; the union's text arm is unreachable here.
        return cast("FormalResponse", result)

    def _parse_formal_candidate(
        self,
        candidate: str | dict[str, Any],
        *,
        expected_family: str | None,
    ) -> FormalResponse:
        """Shared formal-interaction validation funnel for concrete adapters.

        Concrete adapters extract and unwrap their runtime-native candidate
        response first, then funnel it through this helper so every adapter
        shares the same deterministic formal-object extraction (including
        isolating exactly one explicit JSON object from harmless surrounding
        prose), family/version classification, canonical-schema validation,
        and normalized protocol-failure wrapping. Protocol schemas stay in
        ``openorc.protocol`` — never adapter-local copies.
        """
        if expected_family is not None and expected_family not in FORMAL_RESPONSE_FAMILIES:
            raise ValueError(
                f"expected_family does not name a canonical v1 response family: {expected_family!r}"
            )
        try:
            return parse_formal_response(candidate, expected_family=expected_family)
        except ProtocolError as error:
            raise AgentRuntimeProtocolFailureError(
                "formal response failed the shared canonical v1 protocol contract"
            ) from error
