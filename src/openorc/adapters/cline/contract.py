"""The language-neutral Cline SDK backend contract (issue #71).

``ClineSdkBackend`` is the small Python-facing seam between OpenOrc's future
Python Cline adapter (#74) and whatever executes the qualified v1
``@cline/sdk`` / ``ClineCore`` remote operations for it — initially the Node
JSON-RPC bridge process (#72/#73), later possibly a native Python SDK. The
interface is defined so replacing the bridge with a native Python SDK does
not change ``ClineAdapter``, Task services, or workflow semantics. It
deliberately covers only the operations OpenOrc actually needs — no
wholesale ClineCore mirror, no SDK/private Hub protocol, no provider
catalog, no capability registry — and it defines no SDK/Hub client,
subprocess, or JSON-RPC transport: D3 maps these names to public ClineCore
calls, and D4 implements the Python transport/process and correlation
mechanics.

Authority and meaning stay above this boundary. Data crossing the eventual
stdio bridge is JSON-compatible Python values only
(:mod:`openorc.adapters.cline.values`); SDK-native observations and results
are forwarded as opaque JSON objects without interpretation. Formal OpenOrc
response parsing never happens here: the backend does not parse or
manufacture formal OpenOrc meaning, and ``send`` results stay unparsed
SDK-native candidates. The backend never imports application services,
persistence, or Cloud, and never touches the universal
``openorc.adapters.agent_runtime.AgentRuntimeAdapter`` contract — the
Python adapter implements that contract on top of this seam and funnels
formal candidates through the shared #67 → #65 validation funnel.

Operation semantics (exactly the qualified v1 surface):

- ``connect`` establishes/re-establishes the client attachment to the
  already-running remote Cline Hub described by ``ClineRemoteConfig``.
  Cloud — not this backend — resolves/reissues managed-Hub attach
  credentials and manages the host/tunnel.
- ``start`` is the SDK's allocation call, **not** a ready OpenOrc Task
  session: the exact returned external ``session_id`` is retained, but
  ``session_ready`` is not validated, the optional SDK ``result`` is not
  assumed to have executed an initial turn, and no ``AgentSessionCreated``
  is produced. The complete v1 construction bundle arrives as explicit
  ``ClineStartRequest`` fields, forwarded unchanged; a same-ID start
  carries the same complete inputs plus the raw persisted message array as
  ``initial_messages``. The backend neither decides when to stop/start nor
  runs the read → stop → start reconstruction sequence; the adapter owns
  that serialized policy and checks the exact returned ID.
- ``send`` addresses only the supplied exact ID and sends the supplied
  prompt verbatim. Its result is the SDK-native plain-JSON ``AgentResult``
  (or None for the SDK's legitimate undefined response), never parsed into
  OpenOrc meaning here.
- ``stop`` releases the runtime incarnation for serialized same-ID
  reconstruction; ``abort`` interrupts active work. They are distinct
  operations, and neither implies deleting or replacing an OpenOrc Task
  session. No user-facing ``stop`` action and no ``delete`` operation exist
  on this interface.
- ``get``, ``read_messages``, ``list_history``, and
  ``get_accumulated_usage`` expose the actual public read results,
  including absent/null values where the SDK permits them. Record/status
  and usage observations are non-authoritative: interpretation belongs to
  reconciliation/observation work, and usage may reset across Hub
  lifetimes.
- ``subscribe`` receives session-scoped or unfiltered ``CoreSessionEvent``
  observations, retaining the session ID, event kind, and JSON payload.
  The returned handle supports explicit idempotent unsubscribe. This is an
  event-forwarding contract, not a durable replay guarantee: on the pinned
  baseline a fresh client did not replay an already-running turn's missed
  mid-flight stream.
- ``dispose`` releases local SDK/client/bridge attachment resources; it
  must not delete or replace an external Cline Task session and is not a
  universal runtime ``close_session``.

Synchrony and lifecycle: the backend is synchronous from the perspective of
the universal adapter contract; subscribed callbacks may be delivered
asynchronously by a concrete implementation, so subscription lifecycle is
explicit (returned handle, idempotent unsubscribe, no replay promise). A
backend instance is associated with one remote client attachment; no
general connection pool or discovery framework exists here.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Protocol, runtime_checkable

from openorc.adapters.cline.values import (
    ClineRemoteConfig,
    ClineSessionEvent,
    ClineStartRequest,
    ClineStartResult,
    JsonObject,
    Subscription,
)

__all__ = ["ClineSdkBackend"]


@runtime_checkable
class ClineSdkBackend(Protocol):
    """Structural contract implemented by the bridge process and the fake.

    Implementations return the declared result shapes on success and raise
    exactly one :mod:`openorc.adapters.cline.errors` backend error on
    failure. Implementations never log tokens, prompts, responses,
    transcripts, or payload contents, and never expose SDK-native objects,
    JS classes, Node handles, or process objects above this boundary.
    """

    def connect(self, remote: ClineRemoteConfig) -> None:
        """Establish/re-establish the client attachment to the remote Hub."""
        ...

    def start(self, request: ClineStartRequest) -> ClineStartResult:
        """Allocate/bind one external session; allocation, not readiness."""
        ...

    def send(self, session_id: str, prompt: str) -> JsonObject | None:
        """Send the prompt verbatim to the exact supplied session ID.

        Returns the SDK-native plain-JSON ``AgentResult``, or None when the
        SDK's response is legitimately undefined. Never parsed into OpenOrc
        meaning here.
        """
        ...

    def stop(self, session_id: str) -> None:
        """Release the runtime incarnation for serialized same-ID
        reconstruction. Distinct from ``abort``; no session deletion."""
        ...

    def abort(self, session_id: str, reason: str | None = None) -> None:
        """Interrupt active work on the exact supplied session. Distinct
        from ``stop``; no session deletion."""
        ...

    def get(self, session_id: str) -> JsonObject | None:
        """Read the session's public record/status observation, or None
        where the SDK permits absence. Non-authoritative."""
        ...

    def read_messages(self, session_id: str) -> list[JsonObject]:
        """Read the session's raw public message array. Losslessly
        round-trips into a later same-ID ``start``."""
        ...

    def list_history(self) -> list[JsonObject]:
        """List the attachment's public session-history observations.
        Non-authoritative."""
        ...

    def get_accumulated_usage(self, session_id: str) -> JsonObject | None:
        """Read the session's accumulated usage observation, or None where
        the SDK permits absence. May reset across Hub lifetimes."""
        ...

    def subscribe(
        self,
        session_id: str | None,
        listener: Callable[[ClineSessionEvent], None],
    ) -> Subscription:
        """Forward session-scoped (or, with None, unfiltered) session
        events to the listener; explicit idempotent unsubscribe via the
        returned handle; no replay promise."""
        ...

    def dispose(self) -> None:
        """Release local SDK/client/bridge attachment resources; never
        deletes or replaces an external session."""
        ...
