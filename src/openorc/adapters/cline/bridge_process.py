"""Concrete bridge-process implementation of the ``ClineSdkBackend`` (issue #73).

This module is the D4 transport half of the language-neutral Cline SDK
backend seam (:mod:`openorc.adapters.cline.contract`): one concrete,
synchronous Python backend that lazily starts, owns, and supervises a
long-lived **local** Node subprocess running D3's built stdio entrypoint
(``packages/cline-sdk-bridge/dist/cli.js``), and speaks D3's fixed
newline-delimited JSON-RPC 2.0 wire contract with it.

The child process is a replaceable language shim, not a service: it uses
piped stdin/stdout only — no TCP port, daemon, database, or service
discovery — and one backend instance owns at most one child-process
attachment at a time. The backend introduces no workflow, domain, task,
session-readiness, or persistence semantics: it never decides that a
returned session ID is a usable OpenOrc Task session, never replaces or
reconstructs an external Cline session, never replays an uncertain send,
and never issues Cline ``delete`` or an implicit ``stop``/``abort``. It
transports the caller-supplied ``ClineRemoteConfig`` and
``ClineStartRequest`` verbatim, returns the declared D2 typed results, and
converts wire failures into exactly one sanitized
:mod:`openorc.adapters.cline.errors` backend error.

Failure discipline (conservative outcome classification):

- A positively received wire ``error.data`` classification maps one-to-one
  onto the existing D2 error classes. D3's explicit ``wire_protocol``
  rejections map onto the narrowly scoped :class:`ClineBridgeProtocolError`;
  the same error is raised locally for ordinary operations attempted before
  a successful ``connect`` attachment (``not_attached``).
- A timeout, process crash/exit, broken pipe, or malformed wire output
  after a request may have been dispatched resolves that call as
  :class:`ClineBackendUncertainOutcomeError` — never as known
  non-delivery — and invalidates the affected attachment. A demonstrable
  failure before dispatch (missing Node runtime, missing built entrypoint,
  unserializable caller inputs) remains a known failure.
- Native transport exceptions are discarded, never chained: the sanitized
  backend error is raised after the failing handler exits, so ``__cause__``
  and ``__context__`` stay empty and no raw error text, prompt, transcript,
  token, or payload content can leak through exception strings, ``repr``,
  spans, or logs. stdout frames are never logged; the child's stderr is
  discarded (``/dev/null``) and never treated as diagnostic text.

Concurrency discipline: request frames are written atomically under a
short write lock (never held while awaiting a reply), a dedicated reader
thread matches replies to their exact per-generation request IDs as they
complete out of order, and ``cline.event`` notifications dispatch to their
subscription listener through one small bounded dispatcher (listener code
never runs under transport locks or on the reader thread). Lifecycle
changes — ``connect`` replacement, ``dispose``, explicit reconnection —
are serialized against the process generation and outstanding requests.
A replaced child's late replies can never satisfy the new attachment's
calls, and old subscription handles never resurrect onto a new attachment.
"""

from __future__ import annotations

import json
import os
import queue
import re
import shutil
import subprocess
import threading
from collections.abc import Callable, Mapping, Sequence
from contextlib import suppress
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from openorc.adapters.cline.errors import (
    ClineBackendUncertainOutcomeError,
    ClineBridgeProtocolError,
    ClineRemoteAttachmentRejectedError,
    ClineRemoteAttachmentUnavailableError,
    ClineSdkBackendError,
    ClineSdkOperationRejectedError,
    ClineSessionNotFoundError,
)
from openorc.adapters.cline.values import (
    ClineRemoteConfig,
    ClineSessionEvent,
    ClineStartRequest,
    ClineStartResult,
    JsonObject,
    Subscription,
)
from openorc.observability import annotate_span, application_span

__all__ = ["ClineBridgeProcessBackend"]

_TRACER_SCOPE = "openorc.adapters.cline.bridge_process"

_CONNECT_SPAN_NAME = "cline_bridge.connect"
_START_SPAN_NAME = "cline_bridge.start"
_SEND_SPAN_NAME = "cline_bridge.send"
_STOP_SPAN_NAME = "cline_bridge.stop"
_ABORT_SPAN_NAME = "cline_bridge.abort"
_GET_SPAN_NAME = "cline_bridge.get"
_READ_MESSAGES_SPAN_NAME = "cline_bridge.read_messages"
_LIST_HISTORY_SPAN_NAME = "cline_bridge.list_history"
_GET_ACCUMULATED_USAGE_SPAN_NAME = "cline_bridge.get_accumulated_usage"
_SUBSCRIBE_SPAN_NAME = "cline_bridge.subscribe"
_UNSUBSCRIBE_SPAN_NAME = "cline_bridge.unsubscribe"
_DISPOSE_SPAN_NAME = "cline_bridge.dispose"

# Deployment configuration of the default launch command. The defaults are
# deterministic for the source-available repository layout (the built D3
# entrypoint inside this repository, Node resolved from PATH); the
# environment overrides exist for deployments that keep the bridge package
# elsewhere. Test doubles inject the full command through the constructor.
_NODE_OVERRIDE_ENV = "OPENORC_CLINE_BRIDGE_NODE"
_ENTRYPOINT_OVERRIDE_ENV = "OPENORC_CLINE_BRIDGE_ENTRYPOINT"

# Bounded request waits appropriate to potentially long model-backed sends;
# both knobs are constructor-configurable so tests can run tight bounds.
_DEFAULT_REQUEST_TIMEOUT_SECONDS = 900.0
_DEFAULT_TEARDOWN_GRACE_SECONDS = 2.0
_REAP_WAIT_SECONDS = 5.0
_DISPATCH_JOIN_SECONDS = 5.0

# One newline-delimited stdout line is one frame. A longer "line" is wire
# corruption, not a frame, and invalidates the attachment.
_MAX_FRAME_BYTES = 64 * 1024 * 1024

# One small bounded event dispatcher per attachment. Events beyond the bound
# are dropped observations — telemetry loss, never workflow authority — so a
# slow listener can never stall the reader or an independent RPC.
_DISPATCH_QUEUE_BOUND = 256

_WIRE_PROTOCOL_KIND = "wire_protocol"
_NOT_ATTACHED_REASON = "not_attached"

# The safe machine-identifier shape shared with the D2 error boundary:
# lowercase snake_case of bounded length. Arbitrary provider text cannot
# satisfy the shape and is therefore never embedded in raised errors.
_MACHINE_CODE_PATTERN = re.compile(r"[a-z][a-z0-9_]*")
_MAX_MACHINE_CODE_LENGTH = 64


def _is_machine_identifier(value: object) -> bool:
    return (
        isinstance(value, str)
        and len(value) <= _MAX_MACHINE_CODE_LENGTH
        and _MACHINE_CODE_PATTERN.fullmatch(value) is not None
    )


@dataclass(frozen=True)
class _Failure:
    """Bounded, sanitized wire-failure classification; no payload content."""

    kind: str
    reason: str | None = None
    code: str | None = None
    close_code: int | None = None


_UNCERTAIN_FAILURE = _Failure(kind="uncertain_outcome")

# Marker returned by result decoding when a reply violates the declared D2
# Python shape for its operation. The stream is still aligned, so only the
# affected call is resolved — conservatively as an uncertain outcome.
_INVALID_RESULT = object()


class _RequestSerializationError(Exception):
    """A caller-supplied parameter object could not be serialized to wire JSON."""


def _classify_error(error: Any) -> _Failure:
    """Map one wire ``error`` body onto the bounded failure classification.

    Unknown or malformed classifications resolve conservatively as an
    uncertain outcome; nothing received from the wire is ever embedded in a
    raised error.
    """
    if not isinstance(error, dict):
        return _UNCERTAIN_FAILURE
    data = error.get("data")
    if not isinstance(data, dict):
        return _UNCERTAIN_FAILURE
    kind = data.get("kind")
    if kind in (
        "remote_attachment_rejected",
        "remote_attachment_unavailable",
        "uncertain_outcome",
        "session_not_found",
    ):
        return _Failure(kind=kind)
    if kind == "sdk_operation_rejected":
        code = data.get("code")
        if not _is_machine_identifier(code):
            return _UNCERTAIN_FAILURE
        close_code = data.get("close_code")
        if close_code is not None and type(close_code) is not int:
            close_code = None
        return _Failure(kind=kind, code=code, close_code=close_code)
    if kind == _WIRE_PROTOCOL_KIND:
        reason = data.get("reason")
        if not _is_machine_identifier(reason):
            return _UNCERTAIN_FAILURE
        return _Failure(kind=kind, reason=reason)
    return _UNCERTAIN_FAILURE


def _failure_to_error(failure: _Failure) -> ClineSdkBackendError:
    """Convert one classified wire failure into exactly one D2 backend error."""
    if failure.kind == "remote_attachment_rejected":
        return ClineRemoteAttachmentRejectedError()
    if failure.kind == "remote_attachment_unavailable":
        return ClineRemoteAttachmentUnavailableError()
    if failure.kind == "session_not_found":
        return ClineSessionNotFoundError()
    if failure.kind == "sdk_operation_rejected":
        code = failure.code
        assert code is not None  # classification guarantees a validated machine code
        details = None if failure.close_code is None else {"close_code": failure.close_code}
        return ClineSdkOperationRejectedError(code, details)
    if failure.kind == _WIRE_PROTOCOL_KIND:
        reason = failure.reason
        assert reason is not None  # classification guarantees a validated machine reason
        return ClineBridgeProtocolError(reason)
    return ClineBackendUncertainOutcomeError()


def _decode_result(method: str, value: Any) -> Any:
    """Structurally validate one reply against the declared D2 Python type.

    Only the declared shape is checked; contents are never interpreted.
    Returns the decoded D2 value or the ``_INVALID_RESULT`` marker.
    """
    if method in ("connect", "stop", "abort", "unsubscribe"):
        return None if value is None else _INVALID_RESULT
    if method in ("send", "get", "get_accumulated_usage"):
        if value is None:
            return None
        return value if isinstance(value, dict) else _INVALID_RESULT
    if method in ("read_messages", "list_history"):
        if isinstance(value, list) and all(isinstance(item, dict) for item in value):
            return list(value)
        return _INVALID_RESULT
    if method == "start":
        if not isinstance(value, dict):
            return _INVALID_RESULT
        session_id = value.get("session_id")
        result = value.get("result")
        if not isinstance(session_id, str) or not session_id.strip():
            return _INVALID_RESULT
        if result is not None and not isinstance(result, dict):
            return _INVALID_RESULT
        return ClineStartResult(session_id=session_id, result=result)
    if method == "subscribe":
        if isinstance(value, dict):
            subscription_id = value.get("subscription_id")
            if isinstance(subscription_id, str) and subscription_id.strip():
                return subscription_id
        return _INVALID_RESULT
    return _INVALID_RESULT


def _remote_params(remote: ClineRemoteConfig) -> JsonObject:
    """Flatten the remote attachment inputs exactly as D3 expects them."""
    params: JsonObject = {"endpoint": remote.endpoint, "client_identity": remote.client_identity}
    if remote.auth_token is not None:
        params["auth_token"] = remote.auth_token
    if remote.remote_options is not None:
        params["remote_options"] = remote.remote_options
    return params


def _start_params(request: ClineStartRequest) -> JsonObject:
    """Flatten the complete construction bundle verbatim; nothing synthesized."""
    params: JsonObject = {
        "provider_id": request.provider_id,
        "model_id": request.model_id,
        "mode": request.mode.value,
        "rules": request.rules,
        "system_prompt": request.system_prompt,
        "cwd": request.cwd,
        "workspace_root": request.workspace_root,
        "enable_tools": request.enable_tools,
        "interactive": request.interactive,
        "tool_policies": request.tool_policies,
    }
    if request.session_id is not None:
        params["session_id"] = request.session_id
    if request.initial_messages is not None:
        params["initial_messages"] = request.initial_messages
    return params


def _default_entrypoint_path() -> Path:
    """Repository-local built entrypoint of the D3 bridge package."""
    repository_root = Path(__file__).resolve().parents[4]
    return repository_root / "packages" / "cline-sdk-bridge" / "dist" / "cli.js"


class _SubscriptionEntry:
    """One local registration of a wire subscription ID to its listener."""

    __slots__ = ("active", "filter_session_id", "listener", "subscription_id")

    def __init__(
        self,
        subscription_id: str,
        filter_session_id: str | None,
        listener: Callable[[ClineSessionEvent], None],
    ) -> None:
        self.subscription_id = subscription_id
        self.filter_session_id = filter_session_id
        self.listener = listener
        self.active = True


class _PendingCall:
    """One in-flight request awaiting its exact reply (or a safe failure)."""

    __slots__ = ("event", "method", "outcome", "request_id")

    def __init__(self, request_id: str, method: str) -> None:
        self.request_id = request_id
        self.method = method
        self.event = threading.Event()
        self.outcome: tuple[bool, Any] | None = None


class _BridgeAttachment:
    """One local child-process attachment owned by one backend instance.

    Owns exactly one child process, its piped stdio, the per-generation
    pending-call map, the local subscription registry, the dedicated stdout
    reader, and one small event dispatcher. No connection pool, no service
    identity, no durable state.
    """

    def __init__(
        self,
        generation: int,
        process: subprocess.Popen[bytes],
        backend: ClineBridgeProcessBackend,
        request_timeout: float,
        teardown_grace: float,
    ) -> None:
        self.generation = generation
        self.process = process
        self._backend = backend
        self._request_timeout = request_timeout
        self._teardown_grace = teardown_grace
        self._lock = threading.Lock()
        self._write_lock = threading.Lock()
        self._finalization_lock = threading.Lock()
        self._child_lock = threading.Lock()
        self._finalized = False
        self._child_shutdown = False
        self._next_request_number = 0
        self._pending: dict[str, _PendingCall] = {}
        self._subscriptions: dict[str, _SubscriptionEntry] = {}
        self._closing = threading.Event()
        self._dispatch_queue: queue.Queue[tuple[_SubscriptionEntry, ClineSessionEvent]] = (
            queue.Queue(maxsize=_DISPATCH_QUEUE_BOUND)
        )
        self._reader = threading.Thread(
            target=self._reader_loop, name=f"openorc-cline-bridge-reader-{generation}", daemon=True
        )
        self._dispatcher = threading.Thread(
            target=self._dispatcher_loop,
            name=f"openorc-cline-bridge-dispatcher-{generation}",
            daemon=True,
        )
        self._reader.start()
        self._dispatcher.start()

    def dispatch(self, method: str, params: JsonObject) -> _PendingCall:
        """Register one pending call, then write its frame atomically.

        The write lock is short — held only for the serialized frame write,
        never while awaiting the reply.
        """
        with self._lock:
            self._next_request_number += 1
            request_id = f"{self.generation}.{self._next_request_number}"
        frame = {"jsonrpc": "2.0", "id": request_id, "method": method, "params": params}
        try:
            payload = json.dumps(frame).encode("utf-8") + b"\n"
        except (TypeError, ValueError):
            raise _RequestSerializationError from None
        pending = _PendingCall(request_id, method)
        with self._lock:
            self._pending[request_id] = pending
        try:
            with self._write_lock:
                stdin = self.process.stdin
                assert stdin is not None  # guaranteed by _launch before the attachment exists
                stdin.write(payload)
                stdin.flush()
        except (OSError, ValueError):
            # OSError: broken pipe (the child is gone). ValueError: the pipe
            # was closed concurrently by teardown. Either way the frame was
            # not reliably delivered — the caller classifies conservatively.
            with self._lock:
                self._pending.pop(request_id, None)
            raise
        return pending

    def await_reply(self, pending: _PendingCall) -> Any:
        """Wait bounded for the exact reply; convert the outcome safely.

        A timeout resolves the call conservatively as an uncertain outcome;
        its late reply can never satisfy any other call (IDs are unique per
        process generation and the timed-out entry is removed).
        """
        if not pending.event.wait(self._request_timeout):
            with self._lock:
                if pending.outcome is None:
                    pending.outcome = (False, _UNCERTAIN_FAILURE)
                    self._pending.pop(pending.request_id, None)
                    pending.event.set()
        settled = pending.outcome
        assert settled is not None  # set by the resolver or the timeout path above
        ok, outcome = settled
        if ok:
            return outcome
        if isinstance(outcome, _Failure):
            raise _failure_to_error(outcome)
        raise ClineBackendUncertainOutcomeError()

    def resolve(self, pending: _PendingCall, outcome: tuple[bool, Any]) -> None:
        """Resolve one pending call exactly once."""
        with self._lock:
            if pending.outcome is not None:
                return
            pending.outcome = outcome
            self._pending.pop(pending.request_id, None)
            pending.event.set()

    def fail_pending(self) -> None:
        """Resolve every outstanding call conservatively; idempotent."""
        with self._lock:
            outstanding = list(self._pending.values())
            self._pending.clear()
        for pending in outstanding:
            if pending.outcome is None:
                pending.outcome = (False, _UNCERTAIN_FAILURE)
                pending.event.set()

    def register_subscription(self, entry: _SubscriptionEntry) -> None:
        with self._lock:
            self._subscriptions[entry.subscription_id] = entry

    def remove_subscription(self, subscription_id: str) -> None:
        with self._lock:
            entry = self._subscriptions.pop(subscription_id, None)
        if entry is not None:
            entry.active = False

    def deactivate_subscriptions(self) -> None:
        with self._lock:
            entries = list(self._subscriptions.values())
            self._subscriptions.clear()
        for entry in entries:
            entry.active = False

    def offer_event(self, entry: _SubscriptionEntry, event: ClineSessionEvent) -> None:
        """Hand one event to the bounded dispatcher; never block the reader."""
        # Bounded dispatch: a dropped event is lost telemetry, never authority.
        with suppress(queue.Full):
            self._dispatch_queue.put_nowait((entry, event))

    def _reader_loop(self) -> None:
        """Consume stdout continuously so replies match out of order while
        notifications interleave. This is the sole stdout-reading path."""
        stdout = self.process.stdout
        assert stdout is not None  # guaranteed by _launch before the attachment exists
        try:
            while True:
                try:
                    line = stdout.readline(_MAX_FRAME_BYTES + 1)
                except (OSError, ValueError):
                    break
                if not line:
                    break  # EOF: the child exited (or closed its stream)
                if not line.endswith(b"\n"):
                    if len(line) > _MAX_FRAME_BYTES:
                        self.shutdown_child()  # oversized: corruption, not a frame
                    break  # final partial line at EOF — never a frame
                try:
                    frame_text = line[:-1].decode("utf-8")
                except UnicodeDecodeError:
                    self.shutdown_child()
                    break
                if frame_text.endswith("\r"):
                    frame_text = frame_text[:-1]
                if not self._handle_line(frame_text):
                    self.shutdown_child()
                    break
        finally:
            self._reader_finished()

    def _handle_line(self, frame_text: str) -> bool:
        """Parse and route one complete stdout line.

        Returns ``False`` when the stream can no longer be trusted
        (unparseable content), which invalidates the attachment
        conservatively. Unknown-but-well-delimited frames are discarded,
        never accepted as responses.
        """
        try:
            frame = json.loads(frame_text)
        except ValueError:
            return False
        if not isinstance(frame, dict) or frame.get("jsonrpc") != "2.0":
            return True
        if "id" not in frame:
            self._handle_notification(frame)
            return True
        request_id = frame.get("id")
        if isinstance(request_id, bool) or not isinstance(request_id, str):
            return True  # uncorrelatable response-shaped frame: discarded
        pending = self._pending.get(request_id)
        if pending is None:
            return True  # late reply for a timed-out/abandoned call: satisfies nothing
        has_result = "result" in frame
        has_error = "error" in frame
        if has_result == has_error:
            self.resolve(pending, (False, _UNCERTAIN_FAILURE))
            return True
        if has_error:
            self.resolve(pending, (False, _classify_error(frame["error"])))
            return True
        decoded = _decode_result(pending.method, frame["result"])
        if decoded is _INVALID_RESULT:
            self.resolve(pending, (False, _UNCERTAIN_FAILURE))
            return True
        self.resolve(pending, (True, decoded))
        return True

    def _handle_notification(self, frame: dict[str, Any]) -> None:
        """Route one ``cline.event`` notification to its exact subscription."""
        if frame.get("method") != "cline.event":
            return
        params = frame.get("params")
        if not isinstance(params, dict):
            return
        subscription_id = params.get("subscription_id")
        session_id = params.get("session_id")
        kind = params.get("kind")
        payload = params.get("payload")
        if not (
            isinstance(subscription_id, str)
            and subscription_id.strip()
            and isinstance(session_id, str)
            and session_id.strip()
            and isinstance(kind, str)
            and kind.strip()
            and isinstance(payload, dict)
        ):
            return
        with self._lock:
            entry = self._subscriptions.get(subscription_id)
        if entry is not None and entry.active:
            self.offer_event(
                entry, ClineSessionEvent(session_id=session_id, kind=kind, payload=payload)
            )

    def _dispatcher_loop(self) -> None:
        """The one small event-dispatch boundary: listener code never runs
        under transport locks, on the reader, or one thread per event."""
        while True:
            try:
                item = self._dispatch_queue.get(timeout=0.1)
            except queue.Empty:
                if self._closing.is_set():
                    break
                continue
            if item is None or self._closing.is_set():
                break
            entry, event = item
            if not entry.active:
                continue
            try:
                entry.listener(event)
            except BaseException:
                # Listener failures are contained at this boundary: they must
                # never destroy the dispatcher, stall an independent RPC, or
                # leak event contents into errors or logs.
                continue

    def unsubscribe(self, subscription_id: str) -> None:
        """Best-effort wire-only unsubscribe; failures are contained.

        The wire unsubscribe is idempotent on the bridge side and stops only
        its own handle; a dead or replaced attachment is simply gone.
        """
        if self._finalized or self.process.poll() is not None:
            return
        with application_span(_TRACER_SCOPE, _UNSUBSCRIBE_SPAN_NAME) as span:
            annotate_span(span, operation=_UNSUBSCRIBE_SPAN_NAME)
            try:
                pending = self.dispatch("unsubscribe", {"subscription_id": subscription_id})
                self.await_reply(pending)
            except (ClineSdkBackendError, OSError, ValueError):
                return  # contained: wire-only, idempotent, never a session decision

    def shutdown_child(self) -> None:
        """Bounded child teardown: graceful stdin close, then terminate/kill.

        Never sends any protocol operation: bridge teardown implies neither
        external-session ``stop``, ``abort``, nor ``delete``.
        """
        with self._child_lock:
            if self._child_shutdown:
                return
            self._child_shutdown = True
        stdin = self.process.stdin
        try:
            if stdin is not None and not stdin.closed:
                stdin.close()
        except (OSError, ValueError):
            pass
        with suppress(subprocess.TimeoutExpired):
            self.process.wait(timeout=self._teardown_grace)
        if self.process.poll() is None:
            self.process.terminate()
            with suppress(subprocess.TimeoutExpired):
                self.process.wait(timeout=self._teardown_grace)
            if self.process.poll() is None:
                self.process.kill()
                with suppress(subprocess.TimeoutExpired):
                    self.process.wait(timeout=self._teardown_grace)

    def _reader_finished(self) -> None:
        """Reader exit path (EOF or corruption): invalidate conservatively."""
        with suppress(subprocess.TimeoutExpired):
            self.process.wait(timeout=_REAP_WAIT_SECONDS)
        self._backend._attachment_died(self)
        self.fail_pending()
        self.deactivate_subscriptions()
        self._closing.set()
        self._close_handles()

    def finalize(self, *, join_reader: bool) -> None:
        """Idempotent bounded teardown of the whole attachment."""
        with self._finalization_lock:
            if self._finalized:
                return
            self._finalized = True
        self.shutdown_child()
        self._closing.set()
        if join_reader:
            self._reader.join(_REAP_WAIT_SECONDS + 3 * self._teardown_grace + 1.0)
        self.fail_pending()
        self.deactivate_subscriptions()
        self._dispatcher.join(_DISPATCH_JOIN_SECONDS)
        self._close_handles()
        self._backend._attachment_died(self)

    def _close_handles(self) -> None:
        for handle in (self.process.stdin, self.process.stdout):
            try:
                if handle is not None and not handle.closed:
                    handle.close()
            except (OSError, ValueError):
                pass


class ClineBridgeProcessBackend:
    """Concrete bridge-process implementation of the D2 ``ClineSdkBackend``.

    Lazily launches and owns one long-lived local Node child running D3's
    built stdio entrypoint, speaks D3's fixed newline-delimited JSON-RPC 2.0
    wire contract, and exposes the exact D2 operation surface with the
    declared typed results and sanitized backend errors. It is a replaceable
    language shim: application code depends only on ``ClineSdkBackend``, and
    a future native Python backend can replace this class without changes
    above that seam.

    The launch command and child environment are injectable for tests
    (``command`` / ``child_env``); the deterministic default launches the
    supported Node executable with this repository's built
    ``packages/cline-sdk-bridge/dist/cli.js`` through a direct argument
    vector (no shell). Request waits and teardown grace are bounded and
    constructor-configurable.
    """

    def __init__(
        self,
        *,
        command: Sequence[str] | None = None,
        child_env: Mapping[str, str] | None = None,
        request_timeout_seconds: float = _DEFAULT_REQUEST_TIMEOUT_SECONDS,
        teardown_grace_seconds: float = _DEFAULT_TEARDOWN_GRACE_SECONDS,
    ) -> None:
        if request_timeout_seconds <= 0:
            raise ValueError("request_timeout_seconds must be positive")
        if teardown_grace_seconds <= 0:
            raise ValueError("teardown_grace_seconds must be positive")
        if command is not None and (
            not command or any(not isinstance(part, str) or not part for part in command)
        ):
            raise ValueError("command must be a non-empty sequence of non-empty strings")
        self._command_override = tuple(command) if command is not None else None
        self._child_env = dict(child_env) if child_env is not None else None
        self._request_timeout = float(request_timeout_seconds)
        self._teardown_grace = float(teardown_grace_seconds)
        self._lifecycle_lock = threading.RLock()
        self._state_lock = threading.Lock()
        self._current: _BridgeAttachment | None = None
        self._attached = False
        self._generation = 0

    def connect(self, remote: ClineRemoteConfig) -> None:
        """Establish/re-establish the client attachment through the bridge.

        Lazily launches the child, then issues D3's ``connect`` RPC with the
        supplied current remote inputs unchanged. Any prior attachment is
        torn down first (replacement); ordinary operations before a
        successful attachment fail cleanly. A missing Node runtime or built
        entrypoint is a safe pre-dispatch failure, never session loss.
        """
        with application_span(_TRACER_SCOPE, _CONNECT_SPAN_NAME) as span:
            annotate_span(span, operation=_CONNECT_SPAN_NAME)
            with self._lifecycle_lock:
                with self._state_lock:
                    previous = self._current
                    self._current = None
                    self._attached = False
                    self._generation += 1
                    generation = self._generation
                if previous is not None:
                    previous.finalize(join_reader=True)
                command = self._resolve_launch_command()
                process = self._launch(command)
                attachment = _BridgeAttachment(
                    generation, process, self, self._request_timeout, self._teardown_grace
                )
                with self._state_lock:
                    self._current = attachment
                try:
                    self._rpc(attachment, "connect", _remote_params(remote))
                except BaseException:
                    attachment.finalize(join_reader=True)
                    raise
                with self._state_lock:
                    if self._current is attachment:
                        self._attached = True

    def _resolve_launch_command(self) -> list[str]:
        if self._command_override is not None:
            return list(self._command_override)
        node = os.environ.get(_NODE_OVERRIDE_ENV) or "node"
        entrypoint = os.environ.get(_ENTRYPOINT_OVERRIDE_ENV) or str(_default_entrypoint_path())
        if shutil.which(node) is None:
            raise ClineRemoteAttachmentUnavailableError()
        if not Path(entrypoint).is_file():
            raise ClineRemoteAttachmentUnavailableError()
        return [node, entrypoint]

    def _launch(self, command: Sequence[str]) -> subprocess.Popen[bytes]:
        child_env = self._child_env
        environment: dict[str, str] | None = None
        if child_env:
            environment = dict(os.environ)
            environment.update(child_env)
        process: subprocess.Popen[bytes] | None = None
        try:
            process = subprocess.Popen(
                list(command),
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.DEVNULL,
                env=environment,
            )
        except OSError:
            process = None
        if process is None:
            # Raised after the failing handler exits: no native chaining.
            raise ClineRemoteAttachmentUnavailableError()
        if process.stdin is None or process.stdout is None:  # pragma: no cover - PIPE guarantees
            process.kill()
            raise ClineRemoteAttachmentUnavailableError()
        return process

    def _require_attached(self) -> _BridgeAttachment:
        with self._state_lock:
            if self._attached and self._current is not None:
                return self._current
        raise ClineBridgeProtocolError(_NOT_ATTACHED_REASON)

    def _rpc(self, attachment: _BridgeAttachment, method: str, params: JsonObject) -> Any:
        """Dispatch one request on the attachment and await its typed reply.

        Unserializable caller inputs fail before dispatch (known, sanitized
        ``ValueError``); a broken pipe after registration resolves
        conservatively as an uncertain outcome and invalidates the
        attachment.
        """
        try:
            pending = attachment.dispatch(method, params)
        except _RequestSerializationError:
            serialization_failed: Exception | None = ValueError(
                "the bridge request parameters could not be serialized to the wire format"
            )
        except (OSError, ValueError):
            self._attachment_died(attachment)
            serialization_failed = ClineBackendUncertainOutcomeError()
        else:
            serialization_failed = None
        if serialization_failed is not None:
            # Raised after the failing handler exits: __cause__ and
            # __context__ stay empty, so no native error text can be chained
            # into the sanitized backend error.
            raise serialization_failed
        assert pending is not None  # the failure branches above returned instead
        return attachment.await_reply(pending)

    def _attachment_died(self, attachment: _BridgeAttachment) -> None:
        with self._state_lock:
            if self._current is attachment:
                self._current = None
                self._attached = False

    def start(self, request: ClineStartRequest) -> ClineStartResult:
        """Allocate/bind one external session; allocation, not readiness."""
        with application_span(_TRACER_SCOPE, _START_SPAN_NAME) as span:
            annotate_span(span, operation=_START_SPAN_NAME)
            attachment = self._require_attached()
            outcome = self._rpc(attachment, "start", _start_params(request))
            assert isinstance(outcome, ClineStartResult)  # reader-validated invariant
            return outcome

    def send(self, session_id: str, prompt: str) -> JsonObject | None:
        """Send the prompt verbatim to the exact supplied session ID."""
        with application_span(_TRACER_SCOPE, _SEND_SPAN_NAME) as span:
            annotate_span(span, operation=_SEND_SPAN_NAME)
            attachment = self._require_attached()
            outcome = self._rpc(attachment, "send", {"session_id": session_id, "prompt": prompt})
            assert outcome is None or isinstance(outcome, dict)  # reader-validated invariant
            return outcome

    def stop(self, session_id: str) -> None:
        """Release the runtime incarnation for same-ID reconstruction."""
        with application_span(_TRACER_SCOPE, _STOP_SPAN_NAME) as span:
            annotate_span(span, operation=_STOP_SPAN_NAME)
            attachment = self._require_attached()
            self._rpc(attachment, "stop", {"session_id": session_id})

    def abort(self, session_id: str, reason: str | None = None) -> None:
        """Interrupt active work on the exact supplied session."""
        with application_span(_TRACER_SCOPE, _ABORT_SPAN_NAME) as span:
            annotate_span(span, operation=_ABORT_SPAN_NAME)
            attachment = self._require_attached()
            params: JsonObject = {"session_id": session_id}
            if reason is not None:
                params["reason"] = reason
            self._rpc(attachment, "abort", params)

    def get(self, session_id: str) -> JsonObject | None:
        """Read the session's public record/status observation, or None."""
        with application_span(_TRACER_SCOPE, _GET_SPAN_NAME) as span:
            annotate_span(span, operation=_GET_SPAN_NAME)
            attachment = self._require_attached()
            outcome = self._rpc(attachment, "get", {"session_id": session_id})
            assert outcome is None or isinstance(outcome, dict)  # reader-validated invariant
            return outcome

    def read_messages(self, session_id: str) -> list[JsonObject]:
        """Read the session's raw public message array, losslessly."""
        with application_span(_TRACER_SCOPE, _READ_MESSAGES_SPAN_NAME) as span:
            annotate_span(span, operation=_READ_MESSAGES_SPAN_NAME)
            attachment = self._require_attached()
            outcome = self._rpc(attachment, "read_messages", {"session_id": session_id})
            assert isinstance(outcome, list)  # reader-validated invariant
            return outcome

    def list_history(self) -> list[JsonObject]:
        """List the attachment's public session-history observations."""
        with application_span(_TRACER_SCOPE, _LIST_HISTORY_SPAN_NAME) as span:
            annotate_span(span, operation=_LIST_HISTORY_SPAN_NAME)
            attachment = self._require_attached()
            outcome = self._rpc(attachment, "list_history", {})
            assert isinstance(outcome, list)  # reader-validated invariant
            return outcome

    def get_accumulated_usage(self, session_id: str) -> JsonObject | None:
        """Read the session's accumulated usage observation, or None."""
        with application_span(_TRACER_SCOPE, _GET_ACCUMULATED_USAGE_SPAN_NAME) as span:
            annotate_span(span, operation=_GET_ACCUMULATED_USAGE_SPAN_NAME)
            attachment = self._require_attached()
            outcome = self._rpc(attachment, "get_accumulated_usage", {"session_id": session_id})
            assert outcome is None or isinstance(outcome, dict)  # reader-validated invariant
            return outcome

    def subscribe(
        self,
        session_id: str | None,
        listener: Callable[[ClineSessionEvent], None],
    ) -> Subscription:
        """Subscribe to session-scoped (or unfiltered) forwarded events.

        Returns the D2 ``Subscription`` handle whose idempotent
        ``unsubscribe`` issues D3's wire-only ``unsubscribe``. Old handles
        never resurrect onto a replaced attachment; fresh explicit
        subscription is required after reconnection.
        """
        with application_span(_TRACER_SCOPE, _SUBSCRIBE_SPAN_NAME) as span:
            annotate_span(span, operation=_SUBSCRIBE_SPAN_NAME)
            attachment = self._require_attached()
            subscription_id = self._rpc(attachment, "subscribe", {"session_id": session_id})
            assert isinstance(subscription_id, str)  # reader-validated invariant
            entry = _SubscriptionEntry(subscription_id, session_id, listener)
            attachment.register_subscription(entry)
            return Subscription(cancel=lambda: attachment.unsubscribe(subscription_id))

    def dispose(self) -> None:
        """Release only local attachment/process resources; idempotent.

        Bounded graceful stdin-close/exit with terminate/kill fallback;
        pending callers are unblocked. Never deletes or replaces an external
        session and never implicitly issues Cline ``stop``/``abort``.
        """
        with application_span(_TRACER_SCOPE, _DISPOSE_SPAN_NAME) as span:
            annotate_span(span, operation=_DISPOSE_SPAN_NAME)
            with self._lifecycle_lock:
                with self._state_lock:
                    current = self._current
                    self._current = None
                    self._attached = False
                if current is not None:
                    current.finalize(join_reader=True)
