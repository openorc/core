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
  ``ClineStartRequest`` fields, forwarded unchanged. Fresh construction
  only: same-ID reconstruction is ``resume_session`` (issue #408), never a
  same-ID re-start carrying a transcript. The backend neither decides when
  to stop/resume nor runs the stop → resume reconstruction sequence; the
  adapter owns that serialized policy and checks the exact restored ID.
- ``resume_session`` (issue #408) is the native same-ID reconstruction: one
  public ``NodeHubClient.command("session.resume", …)`` call carrying the
  complete ``ClineResumeRequest`` construction bundle for the exact
  existing session, with Hub-side saved-history loading — no transcript is
  ever uploaded, and no fallback reconstruction mechanism exists. The
  consequential dispatch passes the documented public
  ``retryAfterDispatch: false`` command option authoritatively at the real
  call site. The only success value is the verified exact restored session
  identity (:class:`ClineResumeResult`); a positively identified Hub
  refusal (resident session, absent saved record, empty/unavailable saved
  conversation, unsupported command) raises
  :class:`ClineSdkOperationRejectedError` carrying the Hub's machine code
  (exact saved-record absence maps to :class:`ClineSessionNotFoundError`),
  and every unproven outcome — transport loss, dispatch timeout, lost or
  mismatched reply — raises :class:`ClineBackendUncertainOutcomeError`.
  A lost reply never authorizes another resume.
- ``send`` addresses only the supplied exact ID and sends the supplied
  prompt verbatim. Its result is the SDK-native plain-JSON ``AgentResult``
  (or None for the SDK's legitimate undefined response), never parsed into
  OpenOrc meaning here.
- ``enqueue`` (issue #321) admits one nonblocking Hub-native run through the
  public SDK Hub command surface and returns the verified acknowledgement.
  It is transport evidence for later exact correlation, never a workflow
  decision: admission is neither delivery nor execution, and an uncertain
  outcome never authorizes a resend.
- ``stop`` releases the runtime incarnation for serialized native-resume
  reconstruction; ``abort`` interrupts active work. They are distinct
  operations, and neither implies deleting or replacing an OpenOrc Task
  session. No user-facing ``stop`` action and no ``delete`` operation exist
  on this interface.
- ``get``, ``read_messages``, ``list_history``, and
  ``get_accumulated_usage`` expose the actual public read results,
  including absent/null values where the SDK permits them. Record/status
  and usage observations are non-authoritative: interpretation belongs to
  reconciliation/observation work, and usage may reset across Hub
  lifetimes. ``read_messages`` serves the conversation-read/reconciliation
  capabilities and positive continuity evidence — never a transcript
  upload.
- ``list_runs`` (issue #321) exposes the attachment's status-only Hub run
  records as opaque observations, and ``subscribe_hub`` (issue #321)
  forwards Hub event envelopes with envelope-owned session identity, event
  name, durable-log sequence, and unchanged payload. Both are transport
  observations only.
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
    ClineHubEvent,
    ClineRemoteConfig,
    ClineResumeRequest,
    ClineResumeResult,
    ClineRunAdmission,
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
        """Allocate/bind one fresh external session; allocation, not
        readiness. Same-ID reconstruction is ``resume_session``."""
        ...

    def resume_session(self, request: ClineResumeRequest) -> ClineResumeResult:
        """Restore the exact existing external session through the Hub's
        native ``session.resume`` with Hub-side saved-history loading.

        One consequential Hub command (issue #408) carrying the complete
        :class:`ClineResumeRequest` construction bundle verbatim, with the
        documented public ``retryAfterDispatch: false`` option applied
        authoritatively at the real call site. Returns the verified exact
        restored session identity; a positively identified Hub refusal
        raises :class:`ClineSdkOperationRejectedError` carrying the Hub's
        machine code (exact saved-record absence maps to
        :class:`ClineSessionNotFoundError`), and every unproven outcome —
        transport loss, dispatch timeout, lost or mismatched reply — raises
        :class:`ClineBackendUncertainOutcomeError`. Never a transcript
        upload, never a resend, and no fallback reconstruction mechanism.
        """
        ...

    def send(self, session_id: str, prompt: str) -> JsonObject | None:
        """Send the prompt verbatim to the exact supplied session ID.

        Returns the SDK-native plain-JSON ``AgentResult``, or None when the
        SDK's response is legitimately undefined. Never parsed into OpenOrc
        meaning here.
        """
        ...

    def enqueue(self, session_id: str, prompt: str) -> ClineRunAdmission:
        """Admit one nonblocking Hub-native run on the exact session ID.

        Returns the verified Hub acknowledgement (:class:`ClineRunAdmission`)
        — transport evidence for later exact correlation, never a workflow
        decision. Admission is neither delivery nor execution: accepted
        failed runs may emit no terminal event, and a lost acknowledgement or
        timeout is never proof of non-delivery or permission to resend. A
        positively identified Hub refusal raises
        :class:`ClineSdkOperationRejectedError` carrying the Hub's machine
        code; uncertain outcomes stay
        :class:`ClineBackendUncertainOutcomeError`. Deliberately distinct
        from ``send`` and from any OpenOrc queue mode.
        """
        ...

    def stop(self, session_id: str) -> None:
        """Release the runtime incarnation for serialized native-resume
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
        """Read the session's raw public message array. Serves the
        conversation-read/reconciliation capabilities and positive
        continuity evidence — never a transcript upload."""
        ...

    def list_history(self) -> list[JsonObject]:
        """List the attachment's public session-history observations.
        Non-authoritative."""
        ...

    def list_runs(self, session_id: str | None) -> list[JsonObject]:
        """Observe the attachment's status-only Hub run records, opaque and
        uninterpreted.

        ``session_id`` filters to the exact session, or None lists across
        the attachment. Records are status observations, never workflow
        evidence by themselves; a missing or malformed observation fails as
        an uncertain outcome instead of returning an empty list, because an
        empty list must never become false "no runs" evidence downstream.
        """
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

    def subscribe_hub(
        self,
        session_id: str | None,
        listener: Callable[[ClineHubEvent], None],
    ) -> Subscription:
        """Forward Hub event envelopes — session-scoped or, with None,
        unfiltered — to the listener.

        Every delivered :class:`ClineHubEvent` carries the Hub envelope's
        own session identity, event name, durable-log sequence when present,
        and unchanged payload; identity is never synthesized. Same explicit
        idempotent unsubscribe via the returned handle and no replay promise
        as ``subscribe``.
        """
        ...

    def dispose(self) -> None:
        """Release local SDK/client/bridge attachment resources; never
        deletes or replaces an external session."""
        ...
