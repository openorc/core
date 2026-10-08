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
  ``cline-session-2``, ... in call order per instance; same-ID starts bind
  the supplied exact ID, and a scripted start result binds its own exact
  ID. The raw ``initial_messages`` array is deep-copied on the way in and
  deep-copied on the way out, so the exact nested JSON round-trips across
  ``read_messages`` → next same-ID ``start`` and callers can never mutate
  recorded state.
- Deterministic event delivery: ``emit`` delivers synchronously to active
  subscriptions in subscription order, filtered by exact session ID (a
  ``None`` filter receives everything). Unsubscribe is explicit and
  idempotent; there is no replay or background delivery.
- Recording: every operation is appended to one ordered in-memory call log
  (exact target IDs, verbatim prompts/requests/reasons/config) for test
  assertions. Recording is in memory only; the fake never logs and never
  persists anything, and separate instances share no mutable state.

The fake does not enforce attachment state, call ordering, or transport
policy — scripts own outcomes. Its ``stop``/``abort``/``dispose`` are
record-only releases: none of them deletes or replaces session state,
allocates a replacement ID, or implies an OpenOrc Task-session lifecycle
decision. ``dispose`` additionally deactivates the local subscriptions (a
local attachment resource) while leaving session state untouched.
"""

from __future__ import annotations

import copy
from collections import deque
from collections.abc import Callable
from dataclasses import dataclass

from openorc.adapters.cline.errors import ClineSdkBackendError, ClineSessionNotFoundError
from openorc.adapters.cline.values import (
    ClineRemoteConfig,
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
OP_GET = "get"
OP_GET_ACCUMULATED_USAGE = "get_accumulated_usage"
OP_LIST_HISTORY = "list_history"
OP_READ_MESSAGES = "read_messages"
OP_SEND = "send"
OP_START = "start"
OP_STOP = "stop"
OP_SUBSCRIBE = "subscribe"


@dataclass(frozen=True)
class FakeBackendCall:
    """One recorded backend operation, in call order.

    ``session_id`` is the exact addressed target ID (None for operations
    with no session target), and the remaining fields hold the operation's
    raw inputs verbatim for test assertions: ``prompt``/``reason`` for
    sends and aborts, ``request`` for starts, and ``remote`` for connects.
    Recording is in memory only; recorded payloads are never logged.
    """

    operation: str
    session_id: str | None = None
    prompt: str | None = None
    reason: str | None = None
    request: ClineStartRequest | None = None
    remote: ClineRemoteConfig | None = None


@dataclass
class _SubscriptionEntry:
    """One active event subscription tracked by the fake."""

    session_id: str | None
    listener: Callable[[ClineSessionEvent], None]
    active: bool = True

    def cancel(self) -> None:
        self.active = False


class FakeClineSdkBackend:
    """In-memory deterministic fake of the exact #71 backend contract."""

    def __init__(self) -> None:
        self._allocated = 0
        self._sessions: dict[str, list[JsonObject]] = {}
        self._calls: list[FakeBackendCall] = []
        self._subscriptions: list[_SubscriptionEntry] = []
        self._connect_scripts: deque[ClineSdkBackendError] = deque()
        self._start_scripts: deque[ClineStartResult | ClineSdkBackendError] = deque()
        self._send_scripts: dict[str, deque[JsonObject | None | ClineSdkBackendError]] = {}
        self._stop_scripts: dict[str, deque[ClineSdkBackendError]] = {}
        self._abort_scripts: dict[str, deque[ClineSdkBackendError]] = {}
        self._get_scripts: dict[str, deque[JsonObject | None | ClineSdkBackendError]] = {}
        self._read_scripts: dict[str, deque[list[JsonObject] | ClineSdkBackendError]] = {}
        self._list_history_scripts: deque[list[JsonObject] | ClineSdkBackendError] = deque()
        self._usage_scripts: dict[str, deque[JsonObject | None | ClineSdkBackendError]] = {}

    # -- scripting surface (test-only, FIFO per operation) -----------------

    def queue_connect(self, outcome: ClineSdkBackendError) -> None:
        """Script one FIFO ``connect`` outcome (success returns None)."""
        self._connect_scripts.append(outcome)

    def queue_start(self, outcome: ClineStartResult | ClineSdkBackendError) -> None:
        """Script one FIFO ``start`` outcome."""
        self._start_scripts.append(outcome)

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

    # -- deterministic event input -----------------------------------------

    def emit(self, event: ClineSessionEvent) -> None:
        """Deliver one deterministic session event to active subscriptions.

        Synchronous, in subscription order, filtered by exact session ID
        (an unfiltered subscription receives every event).
        """
        for entry in tuple(self._subscriptions):
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

        # -- the #71 ClineSdkBackend operations ---------------------------------

    def connect(self, remote: ClineRemoteConfig) -> None:
        self._calls.append(FakeBackendCall(OP_CONNECT, remote=remote))
        if self._connect_scripts:
            raise self._connect_scripts.popleft()

    def start(self, request: ClineStartRequest) -> ClineStartResult:
        self._calls.append(
            FakeBackendCall(OP_START, session_id=request.session_id, request=request)
        )
        if self._start_scripts:
            outcome = self._start_scripts.popleft()
            if isinstance(outcome, ClineSdkBackendError):
                raise outcome
            result = outcome
        elif request.session_id is not None:
            result = ClineStartResult(session_id=request.session_id)
        else:
            self._allocated += 1
            result = ClineStartResult(session_id=f"cline-session-{self._allocated}")
        session_id = result.session_id
        if request.initial_messages is not None:
            self._sessions[session_id] = copy.deepcopy(request.initial_messages)
        elif session_id not in self._sessions:
            self._sessions[session_id] = []
        return result

    def send(self, session_id: str, prompt: str) -> JsonObject | None:
        self._calls.append(FakeBackendCall(OP_SEND, session_id=session_id, prompt=prompt))
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

    def stop(self, session_id: str) -> None:
        self._calls.append(FakeBackendCall(OP_STOP, session_id=session_id))
        scripted = self._stop_scripts.get(session_id)
        if scripted:
            raise scripted.popleft()
        if session_id not in self._sessions:
            raise ClineSessionNotFoundError(
                f"session {session_id!r} is not known to this backend instance"
            )

    def abort(self, session_id: str, reason: str | None = None) -> None:
        self._calls.append(FakeBackendCall(OP_ABORT, session_id=session_id, reason=reason))
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

    def dispose(self) -> None:
        self._calls.append(FakeBackendCall(OP_DISPOSE))
        for entry in self._subscriptions:
            entry.active = False
