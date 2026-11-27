"""Deterministic fake Cline SDK backend for ordinary tests (issue #71).

``FakeClineSdkBackend`` implements the same #71 ``ClineSdkBackend``
interface as the future Node bridge (#72/#73) while remaining deliberately
dumb about OpenOrc workflow state: it carries no Task, ReviewLoop,
OwnerGate, Execution, RuntimeRequest, or role semantics, performs no OpenOrc
readiness parsing, prompt interpretation, workflow authorization, automatic
same-ID reconstruction, session recovery, retry, provider
authentication/catalog, or tool execution, and produces only explicitly
scripted outcomes. It is test infrastructure, not a production API, and
deliberately distinct from the #69 ``FakeAgentRuntimeAdapter``: that fake
exercises the runtime-neutral ``AgentRuntimeAdapter`` and the shared
#67 → #65 formal-parser funnel, while this fake exercises only the lower
Cline SDK backend the real adapter will call. The two fakes are never
combined and no second formal parser exists here — backend ``send`` results
stay unparsed.

Faithfulness and determinism guarantees:

- Implements the exact #71 interface; data crossing it is JSON-compatible
  Python values only. No live SDK, Node, network, sleeps, timers,
  randomness, or Postgres; everything is synchronous and in-process.
- Scripting model: outcomes are queued FIFO per operation and consumed
  exactly once. A scripted outcome is either a return value for
  value-returning operations (including the legitimate ``None`` read/send
  results) or one backend-local error instance
  (:mod:`openorc.adapters.cline.errors`) to raise as-is — the backend
  taxonomy is the scripting surface, never arbitrary SDK/provider-native
  exception objects, and scripted error messages stay safe by authoring.
  Session-addressed scripting is keyed by exact session ID. Unscripted
  session-addressed calls on an ID this instance does not know raise
  ``ClineSessionNotFoundError`` (positively identified absence for this
  instance); unscripted value operations return their dumb defaults
  (``None``/empty reads).
- Stable opaque session IDs: fresh starts allocate ``cline-session-1``,
  ``cline-session-2``, ... in call order per instance; a scripted start
  result binds its own exact ID. The native ``resume_session`` (issue
  #408) binds the requested exact ID, preserves the stored conversation
  (Hub-side saved loading), and records the complete construction request
  verbatim for assertions.
- Deterministic event delivery: ``emit`` delivers synchronously to active
  subscriptions in subscription order, filtered by exact session ID (a
  ``None`` filter receives everything). Unsubscribe is explicit and
  idempotent; there is no replay or background delivery.
- Recording: every operation is appended to one ordered in-memory call log
  (exact target IDs, verbatim prompts/requests/reasons/config) for test
  assertions. Recording is in memory only; the fake never logs and never
  persists anything, and separate instances share no mutable state.
  Representations withhold prompt/request content, so recorded values stay
  verbatim for assertions without entering reprs, assertion-failure text,
  or telemetry.
- Optional deterministic operation gates: ``gate_operation`` registers a
  FIFO ``(operation, session_id)`` gate whose next matching call — after
  its recording — signals ``entered`` and blocks on ``release`` before
  producing its scripted outcome. Gates exist for concurrency tests: the
  bounded release wait is a deadlock guard, never a timing assertion, and
  no sleeps or timers are introduced for synchronization.

The fake does not enforce attachment state, call ordering, or transport
policy — scripts own outcomes. Its ``stop``/``abort``/``dispose`` are
record-only releases: none of them deletes or replaces session state,
allocates a replacement ID, or implies an OpenOrc Task-session lifecycle
decision. ``dispose`` additionally deactivates the local subscriptions (a
local attachment resource) while leaving session state untouched.
"""

from __future__ import annotations

import copy
import threading
from collections import deque
from collections.abc import Callable
from dataclasses import dataclass

from openorc.adapters.cline.errors import ClineSdkBackendError, ClineSessionNotFoundError
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

__all__ = [
    "FakeBackendCall",
    "FakeClineSdkBackend",
]

OP_ABORT = "abort"
OP_CONNECT = "connect"
OP_DISPOSE = "dispose"
OP_ENQUEUE = "enqueue"
OP_GET = "get"
OP_GET_ACCUMULATED_USAGE = "get_accumulated_usage"
OP_LIST_HISTORY = "list_history"
OP_LIST_RUNS = "list_runs"
OP_READ_MESSAGES = "read_messages"
OP_RESUME = "resume"
OP_SEND = "send"
OP_START = "start"
OP_STOP = "stop"
OP_SUBSCRIBE = "subscribe"
OP_SUBSCRIBE_HUB = "subscribe_hub"

# Bounded release wait of one registered operation gate. It exists purely as
# a deadlock guard so a misbehaving test fails on its own assertions instead
# of hanging the suite; it is never a timing assertion.
_GATE_RELEASE_TIMEOUT_SECONDS = 10.0

# Deterministic admission-timestamp base for unscripted ``enqueue`` defaults,
# mirroring the epoch-millisecond shape the qualified Hub acknowledges.
_FAKE_ACCEPTED_AT_EPOCH_MS = 1794712612934


@dataclass(frozen=True)
class FakeBackendCall:
    """One recorded backend operation, in call order.

    ``session_id`` is the exact addressed target ID (None for operations
    with no session target), and the remaining fields hold the operation's
    raw inputs verbatim for test assertions: ``prompt``/``reason`` for
    sends and aborts, ``request`` for starts, and ``remote`` for connects.
    Recording is in memory only; recorded payloads are never logged, and
    the representation withholds prompt and request content — recorded
    values stay verbatim for assertions.
    """

    operation: str
    session_id: str | None = None
    prompt: str | None = None
    reason: str | None = None
    request: ClineStartRequest | ClineResumeRequest | None = None
    remote: ClineRemoteConfig | None = None

    def __repr__(self) -> str:
        parts = [f"operation={self.operation!r}"]
        if self.session_id is not None:
            parts.append(f"session_id={self.session_id!r}")
        if self.reason is not None:
            parts.append(f"reason={self.reason!r}")
        if self.prompt is not None:
            parts.append("prompt=<withheld>")
        if self.request is not None:
            parts.append("request=<withheld>")
        if self.remote is not None:
            parts.append(f"remote={self.remote!r}")
        return f"FakeBackendCall({', '.join(parts)})"


@dataclass
class _SubscriptionEntry:
    """One active session-event subscription tracked by the fake."""

    session_id: str | None
    listener: Callable[[ClineSessionEvent], None]
    active: bool = True

    def cancel(self) -> None:
        self.active = False


@dataclass
class _HubSubscriptionEntry:
    """One active Hub event-envelope subscription tracked by the fake."""

    session_id: str | None
    listener: Callable[[ClineHubEvent], None]
    active: bool = True

    def cancel(self) -> None:
        self.active = False


@dataclass
class _GateEntry:
    """One registered operation gate, consumed by the next matching call."""

    operation: str
    session_id: str | None
    entered: threading.Event
    release: threading.Event


class FakeClineSdkBackend:
    """In-memory deterministic fake of the exact #71 backend contract."""

    def __init__(self) -> None:
        self._allocated = 0
        self._admitted = 0
        self._sessions: dict[str, list[JsonObject]] = {}
        self._runs: list[JsonObject] = []
        self._calls: list[FakeBackendCall] = []
        self._subscriptions: list[_SubscriptionEntry] = []
        self._hub_subscriptions: list[_HubSubscriptionEntry] = []
        self._connect_scripts: deque[ClineSdkBackendError] = deque()
        self._start_scripts: deque[ClineStartResult | ClineSdkBackendError] = deque()
        self._resume_scripts: dict[str, deque[ClineResumeResult | ClineSdkBackendError]] = {}
        self._send_scripts: dict[str, deque[JsonObject | None | ClineSdkBackendError]] = {}
        self._enqueue_scripts: dict[str, deque[ClineRunAdmission | ClineSdkBackendError]] = {}
        self._stop_scripts: dict[str, deque[ClineSdkBackendError]] = {}
        self._abort_scripts: dict[str, deque[ClineSdkBackendError]] = {}
        self._get_scripts: dict[str, deque[JsonObject | None | ClineSdkBackendError]] = {}
        self._read_scripts: dict[str, deque[list[JsonObject] | ClineSdkBackendError]] = {}
        self._list_history_scripts: deque[list[JsonObject] | ClineSdkBackendError] = deque()
        self._list_runs_scripts: dict[
            str | None, deque[list[JsonObject] | ClineSdkBackendError]
        ] = {}
        self._usage_scripts: dict[str, deque[JsonObject | None | ClineSdkBackendError]] = {}
        self._gates: deque[_GateEntry] = deque()

    # -- scripting surface (test-only, FIFO per operation) -----------------

    def queue_connect(self, outcome: ClineSdkBackendError) -> None:
        """Script one FIFO ``connect`` outcome (success returns None)."""
        self._connect_scripts.append(outcome)

    def queue_start(self, outcome: ClineStartResult | ClineSdkBackendError) -> None:
        """Script one FIFO ``start`` outcome."""
        self._start_scripts.append(outcome)

    def queue_resume(
        self, session_id: str, outcome: ClineResumeResult | ClineSdkBackendError
    ) -> None:
        """Script one FIFO ``resume_session`` outcome for the exact session ID."""
        self._resume_scripts.setdefault(session_id, deque()).append(outcome)

    def queue_send(
        self, session_id: str, outcome: JsonObject | None | ClineSdkBackendError
    ) -> None:
        """Script one FIFO ``send`` outcome for the exact session ID."""
        self._send_scripts.setdefault(session_id, deque()).append(outcome)

    def queue_stop(self, session_id: str, outcome: ClineSdkBackendError) -> None:
        """Script one FIFO ``stop`` outcome (success returns None)."""
        self._stop_scripts.setdefault(session_id, deque()).append(outcome)

    def queue_abort(self, session_id: str, outcome: ClineSdkBackendError) -> None:
        """Script one FIFO ``abort`` outcome (success returns None)."""
        self._abort_scripts.setdefault(session_id, deque()).append(outcome)

    def queue_get(self, session_id: str, outcome: JsonObject | None | ClineSdkBackendError) -> None:
        """Script one FIFO ``get`` outcome for the exact session ID."""
        self._get_scripts.setdefault(session_id, deque()).append(outcome)

    def queue_read_messages(
        self, session_id: str, outcome: list[JsonObject] | ClineSdkBackendError
    ) -> None:
        """Script one FIFO ``read_messages`` outcome for the exact session ID."""
        self._read_scripts.setdefault(session_id, deque()).append(outcome)

    def queue_list_history(self, outcome: list[JsonObject] | ClineSdkBackendError) -> None:
        """Script one FIFO ``list_history`` outcome."""
        self._list_history_scripts.append(outcome)

    def queue_get_accumulated_usage(
        self, session_id: str, outcome: JsonObject | None | ClineSdkBackendError
    ) -> None:
        """Script one FIFO usage outcome for the exact session ID."""
        self._usage_scripts.setdefault(session_id, deque()).append(outcome)

    def queue_enqueue(
        self, session_id: str, outcome: ClineRunAdmission | ClineSdkBackendError
    ) -> None:
        """Script one FIFO ``enqueue`` outcome for the exact session ID."""
        self._enqueue_scripts.setdefault(session_id, deque()).append(outcome)

    def queue_list_runs(
        self, session_id: str | None, outcome: list[JsonObject] | ClineSdkBackendError
    ) -> None:
        """Script one FIFO ``list_runs`` outcome for the exact filter."""
        self._list_runs_scripts.setdefault(session_id, deque()).append(outcome)

    # -- deterministic operation gates (test-only, FIFO) --------------------

    def gate_operation(
        self,
        operation: str,
        session_id: str | None,
        *,
        entered: threading.Event,
        release: threading.Event,
    ) -> None:
        """Register one FIFO gate for the next matching operation call.

        The next call to ``operation`` targeting ``session_id`` — after its
        recording — signals ``entered`` and blocks on ``release`` before
        producing its scripted outcome. Entries are consumed FIFO like
        scripts. The synchronization surface is events only: the bounded
        release wait is a deadlock guard, never a timing assertion.
        """
        self._gates.append(
            _GateEntry(
                operation=operation,
                session_id=session_id,
                entered=entered,
                release=release,
            )
        )

    def _apply_gate(self, operation: str, session_id: str | None) -> None:
        """Hold the caller on the next matching registered gate, if any."""
        for index, entry in enumerate(self._gates):
            if entry.operation == operation and entry.session_id == session_id:
                del self._gates[index]
                entry.entered.set()
                if not entry.release.wait(timeout=_GATE_RELEASE_TIMEOUT_SECONDS):
                    # Deadlock-guard expiry: proceed so a misbehaving test
                    # fails on its own recorded-order assertions instead of
                    # hanging the suite.
                    return
                return

    # -- deterministic event input -----------------------------------------

    def emit(self, event: ClineSessionEvent) -> None:
        """Deliver one deterministic session event to active subscriptions.

        Synchronous, in subscription order, filtered by exact session ID
        (an unfiltered subscription receives every event).
        """
        for entry in tuple(self._subscriptions):
            if entry.active and (entry.session_id is None or entry.session_id == event.session_id):
                entry.listener(event)

    def emit_hub(self, event: ClineHubEvent) -> None:
        """Deliver one deterministic Hub event envelope to active hub
        subscriptions (issue #321).

        Synchronous, in subscription order, filtered by the envelope's own
        session identity (a None filter receives every envelope, including
        ones without session identity).
        """
        for entry in tuple(self._hub_subscriptions):
            if entry.active and (entry.session_id is None or entry.session_id == event.session_id):
                entry.listener(event)

    # -- introspection for test assertions ---------------------------------

    def calls(self) -> tuple[FakeBackendCall, ...]:
        """All recorded operations in exact call order."""
        return tuple(self._calls)

    def known_session_ids(self) -> tuple[str, ...]:
        """Session IDs this instance knows, in binding order."""
        return tuple(self._sessions)

    def stored_messages(self, session_id: str) -> list[JsonObject]:
        """The raw message array currently bound to the session (deep copy)."""
        return copy.deepcopy(self._sessions[session_id])

    def seed_messages(self, session_id: str, messages: list[JsonObject]) -> None:
        """Seed the session's Hub-side saved conversation (test-only).

        The fake's stored per-session array models what the qualified Hub
        persists and what native ``resume_session`` loads Hub-side: reads
        serve it unscripted, and a reconstruction preserves it verbatim.
        Seeding records no call; it only establishes the deterministic
        saved state a test needs.
        """
        if session_id not in self._sessions:
            raise ClineSessionNotFoundError(
                f"session {session_id!r} is not known to this backend instance"
            )
        self._sessions[session_id] = copy.deepcopy(messages)

        # -- the #71 ClineSdkBackend operations ---------------------------------

    def connect(self, remote: ClineRemoteConfig) -> None:
        self._calls.append(FakeBackendCall(OP_CONNECT, remote=remote))
        if self._connect_scripts:
            raise self._connect_scripts.popleft()

    def start(self, request: ClineStartRequest) -> ClineStartResult:
        self._calls.append(FakeBackendCall(OP_START, request=request))
        if self._start_scripts:
            outcome = self._start_scripts.popleft()
            if isinstance(outcome, ClineSdkBackendError):
                raise outcome
            result = outcome
        else:
            self._allocated += 1
            result = ClineStartResult(session_id=f"cline-session-{self._allocated}")
        session_id = result.session_id
        if session_id not in self._sessions:
            self._sessions[session_id] = []
        return result

    def resume_session(self, request: ClineResumeRequest) -> ClineResumeResult:
        """Restore the exact requested session with Hub-side saved loading.

        Unscripted default behavior mirrors the qualified native seam: a
        session unknown to this instance raises the saved-record absence
        (:class:`ClineSessionNotFoundError`), and a known session restores
        with its stored conversation preserved verbatim and the requested
        exact identity. Scripted outcomes (including native refusals) are
        raised or returned as queued.
        """
        self._calls.append(
            FakeBackendCall(OP_RESUME, session_id=request.session_id, request=request)
        )
        self._apply_gate(OP_RESUME, request.session_id)
        scripted = self._resume_scripts.get(request.session_id)
        if scripted:
            outcome = scripted.popleft()
            if isinstance(outcome, ClineSdkBackendError):
                raise outcome
            return outcome
        if request.session_id not in self._sessions:
            raise ClineSessionNotFoundError(
                f"session {request.session_id!r} is not known to this backend instance"
            )
        return ClineResumeResult(session_id=request.session_id)

    def send(self, session_id: str, prompt: str) -> JsonObject | None:
        self._calls.append(FakeBackendCall(OP_SEND, session_id=session_id, prompt=prompt))
        self._apply_gate(OP_SEND, session_id)
        scripted = self._send_scripts.get(session_id)
        if scripted:
            outcome = scripted.popleft()
            if isinstance(outcome, ClineSdkBackendError):
                raise outcome
            return copy.deepcopy(outcome)
        if session_id not in self._sessions:
            raise ClineSessionNotFoundError(
                f"session {session_id!r} is not known to this backend instance"
            )
        return None

    def enqueue(self, session_id: str, prompt: str) -> ClineRunAdmission:
        self._calls.append(FakeBackendCall(OP_ENQUEUE, session_id=session_id, prompt=prompt))
        scripted = self._enqueue_scripts.get(session_id)
        if scripted:
            outcome = scripted.popleft()
            if isinstance(outcome, ClineSdkBackendError):
                raise outcome
            admission = outcome
        else:
            if session_id not in self._sessions:
                raise ClineSessionNotFoundError(
                    f"session {session_id!r} is not known to this backend instance"
                )
            self._admitted += 1
            admission = ClineRunAdmission(
                run_id=f"hub-run-{self._admitted}",
                accepted_at=_FAKE_ACCEPTED_AT_EPOCH_MS + self._admitted,
                queue_position=0,
            )
        # The fake's own status-only bookkeeping of admitted runs; status
        # records stay opaque observations for ``list_runs`` defaults.
        self._runs.append(
            {
                "runId": admission.run_id,
                "sessionId": session_id,
                "state": "queued",
                "acceptedAt": admission.accepted_at,
                "queuePosition": admission.queue_position,
            }
        )
        return admission

    def stop(self, session_id: str) -> None:
        self._calls.append(FakeBackendCall(OP_STOP, session_id=session_id))
        self._apply_gate(OP_STOP, session_id)
        scripted = self._stop_scripts.get(session_id)
        if scripted:
            raise scripted.popleft()
        if session_id not in self._sessions:
            raise ClineSessionNotFoundError(
                f"session {session_id!r} is not known to this backend instance"
            )

    def abort(self, session_id: str, reason: str | None = None) -> None:
        self._calls.append(FakeBackendCall(OP_ABORT, session_id=session_id, reason=reason))
        self._apply_gate(OP_ABORT, session_id)
        scripted = self._abort_scripts.get(session_id)
        if scripted:
            raise scripted.popleft()
        if session_id not in self._sessions:
            raise ClineSessionNotFoundError(
                f"session {session_id!r} is not known to this backend instance"
            )

    def get(self, session_id: str) -> JsonObject | None:
        self._calls.append(FakeBackendCall(OP_GET, session_id=session_id))
        scripted = self._get_scripts.get(session_id)
        if scripted:
            outcome = scripted.popleft()
            if isinstance(outcome, ClineSdkBackendError):
                raise outcome
            return copy.deepcopy(outcome)
        if session_id not in self._sessions:
            raise ClineSessionNotFoundError(
                f"session {session_id!r} is not known to this backend instance"
            )
        return None

    def read_messages(self, session_id: str) -> list[JsonObject]:
        self._calls.append(FakeBackendCall(OP_READ_MESSAGES, session_id=session_id))
        self._apply_gate(OP_READ_MESSAGES, session_id)
        scripted = self._read_scripts.get(session_id)
        if scripted:
            outcome = scripted.popleft()
            if isinstance(outcome, ClineSdkBackendError):
                raise outcome
            return copy.deepcopy(outcome)
        if session_id not in self._sessions:
            raise ClineSessionNotFoundError(
                f"session {session_id!r} is not known to this backend instance"
            )
        return copy.deepcopy(self._sessions[session_id])

    def list_history(self) -> list[JsonObject]:
        self._calls.append(FakeBackendCall(OP_LIST_HISTORY))
        if self._list_history_scripts:
            outcome = self._list_history_scripts.popleft()
            if isinstance(outcome, ClineSdkBackendError):
                raise outcome
            return copy.deepcopy(outcome)
        return []

    def list_runs(self, session_id: str | None) -> list[JsonObject]:
        self._calls.append(FakeBackendCall(OP_LIST_RUNS, session_id=session_id))
        scripted = self._list_runs_scripts.get(session_id)
        if scripted:
            outcome = scripted.popleft()
            if isinstance(outcome, ClineSdkBackendError):
                raise outcome
            return copy.deepcopy(outcome)
        if session_id is None:
            return copy.deepcopy(self._runs)
        return copy.deepcopy([run for run in self._runs if run.get("sessionId") == session_id])

    def get_accumulated_usage(self, session_id: str) -> JsonObject | None:
        self._calls.append(FakeBackendCall(OP_GET_ACCUMULATED_USAGE, session_id=session_id))
        scripted = self._usage_scripts.get(session_id)
        if scripted:
            outcome = scripted.popleft()
            if isinstance(outcome, ClineSdkBackendError):
                raise outcome
            return copy.deepcopy(outcome)
        if session_id not in self._sessions:
            raise ClineSessionNotFoundError(
                f"session {session_id!r} is not known to this backend instance"
            )
        return None

    def subscribe(
        self, session_id: str | None, listener: Callable[[ClineSessionEvent], None]
    ) -> Subscription:
        self._calls.append(FakeBackendCall(OP_SUBSCRIBE, session_id=session_id))
        entry = _SubscriptionEntry(session_id=session_id, listener=listener)
        self._subscriptions.append(entry)
        return Subscription(entry.cancel)

    def subscribe_hub(
        self, session_id: str | None, listener: Callable[[ClineHubEvent], None]
    ) -> Subscription:
        self._calls.append(FakeBackendCall(OP_SUBSCRIBE_HUB, session_id=session_id))
        entry = _HubSubscriptionEntry(session_id=session_id, listener=listener)
        self._hub_subscriptions.append(entry)
        return Subscription(entry.cancel)

    def dispose(self) -> None:
        self._calls.append(FakeBackendCall(OP_DISPOSE))
        for entry in self._subscriptions:
            entry.active = False
        for entry in self._hub_subscriptions:
            entry.active = False
