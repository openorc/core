"""Deterministic tests for the bridge-process ClineSdkBackend (issue #73).

The concrete D4 backend (``src/openorc/adapters/cline/bridge_process.py``)
is exercised through a scripted Python child that speaks the same fixed
newline-delimited JSON-RPC 2.0 contract as D3's bridge, plus one small real
Python-to-Node seam test over D3's ``tests/spawn-harness.mjs``. No remote
Hub, model inference, Supabase, Valkey, or database is involved anywhere;
the seam test runs only when ``node`` and the built bridge ``dist/`` are
available, and with ``OPENORC_BRIDGE_SEAM_REQUIRED=1`` (set by CI's
``python-checks`` job, which builds the bridge first) a missing
prerequisite is a hard failure instead of a silent skip.
"""

from __future__ import annotations

import json
import os
import shutil
import sys
import threading
import time
from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any

import pytest

from openorc.adapters.cline import (
    ClineBackendUncertainOutcomeError,
    ClineBridgeProcessBackend,
    ClineBridgeProtocolError,
    ClineConstructionMode,
    ClineRemoteAttachmentRejectedError,
    ClineRemoteAttachmentUnavailableError,
    ClineRemoteConfig,
    ClineSdkBackend,
    ClineSdkOperationRejectedError,
    ClineSessionEvent,
    ClineSessionNotFoundError,
    ClineStartRequest,
    ClineStartResult,
)
from openorc.adapters.cline.bridge_process import _default_entrypoint_path

_REPO_ROOT = Path(__file__).resolve().parents[1]
_BRIDGE_PACKAGE = _REPO_ROOT / "packages" / "cline-sdk-bridge"
_SPAWN_HARNESS = _BRIDGE_PACKAGE / "tests" / "spawn-harness.mjs"
_BUILT_BRIDGE_CORE = _BRIDGE_PACKAGE / "dist" / "bridge.js"

_SEAM_REQUIRED_ENV = "OPENORC_BRIDGE_SEAM_REQUIRED"
_FAKE_SCRIPT_ENV = "OPENORC_BRIDGE_FAKE_SCRIPT"
_FAKE_RECORD_ENV = "OPENORC_BRIDGE_FAKE_RECORD"

# Scripted JSON-RPC child standing in for the D3 bridge process. It mirrors
# the fixed wire contract (method surface, error envelope, `cline.event`
# notifications) so the backend's transport, correlation, event dispatch,
# and failure classification are exercised deterministically without Node.
# Scenario script keys: `outcomes` (per-method FIFO of {ok|error|delay_ms|
# crash|garbage}), `events` (after_subscribe-keyed notifications), `sentinel`
# (sensitive-text marker inside the harmless error message).
_FAKE_BRIDGE_CHILD_SOURCE = '''
"""Scripted JSON-RPC child used only by the bridge-process backend tests."""
import json
import os
import sys
import threading
import time

with open(os.environ["OPENORC_BRIDGE_FAKE_SCRIPT"], "r", encoding="utf-8") as handle:
    script = json.load(handle)
record_path = os.environ.get("OPENORC_BRIDGE_FAKE_RECORD")
write_lock = threading.Lock()


def record(call):
    if not record_path:
        return
    with write_lock, open(record_path, "a", encoding="utf-8") as handle:
        handle.write(json.dumps(call) + "\\n")


def write_frame(frame):
    with write_lock:
        sys.stdout.write(json.dumps(frame) + "\\n")
        sys.stdout.flush()


def write_raw(text):
    with write_lock:
        sys.stdout.write(text + "\\n")
        sys.stdout.flush()


queues = {name: list(items) for name, items in script.get("outcomes", {}).items()}


def next_outcome(method):
    items = queues.get(method)
    if not items:
        return None
    return items.pop(0)


sessions = {}
counters = {"session": 0, "subscription": 0}
subscriptions = []
NOT_FOUND = {"error": {"kind": "session_not_found"}}


def respond(request_id, *, result=None, error=None):
    frame = {"jsonrpc": "2.0", "id": request_id}
    if error is not None:
        message = "cline sdk backend operation failed"
        if script.get("sentinel"):
            message += " SECRET-SENTINEL"
        frame["error"] = {"code": -32000, "message": message, "data": error}
    else:
        frame["result"] = result
    write_frame(frame)


def session_value(session_id, value):
    if session_id in sessions:
        return value
    return NOT_FOUND


def deliver(entry, event):
    if not entry["active"]:
        return
    if entry["filter"] is not None and entry["filter"] != event["session_id"]:
        return
    write_frame({
        "jsonrpc": "2.0",
        "method": "cline.event",
        "params": {
            "subscription_id": entry["id"],
            "session_id": event["session_id"],
            "kind": event["kind"],
            "payload": event["payload"],
        },
    })


def broadcast(event):
    for entry in list(subscriptions):
        deliver(entry, event)


def apply(method, request_id, fallback):
    outcome = next_outcome(method)
    if outcome is None:
        value = fallback()
    else:
        if outcome.get("delay_ms"):
            time.sleep(outcome["delay_ms"] / 1000.0)
        if outcome.get("crash"):
            sys.stdout.flush()
            os._exit(101)
        if outcome.get("garbage"):
            write_raw(outcome["garbage"])
        if "error" in outcome:
            respond(request_id, error=outcome["error"])
            return
        value = outcome.get("ok")
    if isinstance(value, dict) and "error" in value:
        respond(request_id, error=value["error"])
    else:
        respond(request_id, result=value)

def handle(method, params, request_id):
    if method == "connect":
        record({"method": "connect", "args": params})
        apply(method, request_id, lambda: None)
        return
    if method == "start":
        record({"method": "start", "args": params})

        def start_fallback():
            if params.get("session_id"):
                session_id = params["session_id"]
            else:
                counters["session"] += 1
                session_id = "cline-session-%d" % counters["session"]
            sessions[session_id] = list(params.get("initial_messages") or [])
            return {"session_id": session_id, "result": None}

        apply(method, request_id, start_fallback)
        return
    if method == "send":
        record({"method": "send", "args": params})
        apply(method, request_id, lambda: session_value(params["session_id"], None))
        return
    if method == "stop":
        record({"method": "stop", "args": params})
        apply(method, request_id, lambda: session_value(params["session_id"], None))
        return
    if method == "abort":
        record({"method": "abort", "args": params})
        apply(method, request_id, lambda: session_value(params["session_id"], None))
        return
    if method == "get":
        record({"method": "get", "args": params})
        apply(method, request_id, lambda: session_value(params["session_id"], None))
        return
    if method == "read_messages":
        record({"method": "read_messages", "args": params})
        fallback_messages = sessions.get(params["session_id"], [])
        apply(method, request_id, lambda: session_value(params["session_id"], fallback_messages))
        return
    if method == "list_history":
        record({"method": "list_history", "args": params})
        apply(method, request_id, lambda: [])
        return
    if method == "get_accumulated_usage":
        record({"method": "get_accumulated_usage", "args": params})
        apply(method, request_id, lambda: session_value(params["session_id"], None))
        return
    if method == "subscribe":
        record({"method": "subscribe", "args": params})
        counters["subscription"] += 1
        subscription_id = "sub-%d" % counters["subscription"]
        entry = {"id": subscription_id, "filter": params.get("session_id"), "active": True}
        subscriptions.append(entry)
        # D3 harness semantics: a scripted event fires at the SDK level and
        # is delivered to every active matching subscription, each through
        # its own bridge subscription ID.
        for event in script.get("events", []):
            if event.get("after_subscribe") != counters["subscription"]:
                continue
            delay = (event.get("delay_ms") or 10) / 1000.0
            timer = threading.Timer(delay, broadcast, args=(event,))
            timer.daemon = True
            timer.start()
        respond(request_id, result={"subscription_id": subscription_id})
        return
    if method == "unsubscribe":
        record({"method": "unsubscribe", "args": params})
        for entry in subscriptions:
            if entry["id"] == params.get("subscription_id"):
                entry["active"] = False
        respond(request_id, result=None)
        return
    if method == "dispose":
        record({"method": "dispose", "args": params})
        for entry in subscriptions:
            entry["active"] = False
        respond(request_id, result=None)
        return
    respond(request_id, error={"kind": "wire_protocol", "reason": "method_not_found"})


for raw in sys.stdin:
    line = raw.strip()
    if not line:
        continue
    request = json.loads(line)
    args = (request["method"], request.get("params") or {}, request["id"])
    worker = threading.Thread(target=handle, args=args)
    worker.daemon = True
    worker.start()
'''


def _scripted_child(
    tmp_path: Path, name: str, script: dict[str, Any]
) -> tuple[list[str], dict[str, str], Path]:
    """Write the scripted child plus its scenario script; return the launch
    command, the child environment, and the call-record path."""
    child = tmp_path / f"{name}-child.py"
    child.write_text(_FAKE_BRIDGE_CHILD_SOURCE, encoding="utf-8")
    script_path = tmp_path / f"{name}-script.json"
    script_path.write_text(json.dumps(script), encoding="utf-8")
    record_path = tmp_path / f"{name}-record.jsonl"
    command = [sys.executable, str(child)]
    child_env = {_FAKE_SCRIPT_ENV: str(script_path), _FAKE_RECORD_ENV: str(record_path)}
    return command, child_env, record_path


def _make_backend(
    command: list[str],
    child_env: dict[str, str],
    *,
    request_timeout: float = 5.0,
    teardown_grace: float = 1.0,
) -> ClineBridgeProcessBackend:
    return ClineBridgeProcessBackend(
        command=command,
        child_env=child_env,
        request_timeout_seconds=request_timeout,
        teardown_grace_seconds=teardown_grace,
    )


def _remote_config() -> ClineRemoteConfig:
    return ClineRemoteConfig(
        endpoint="https://hub.example.test",
        client_identity="openorc-test",
        auth_token="TOKEN-SENTINEL",
        remote_options={"region": "test"},
    )


def _start_request(**overrides: Any) -> ClineStartRequest:
    values: dict[str, Any] = {
        "provider_id": "provider-x",
        "model_id": "model-y",
        "mode": ClineConstructionMode.PLAN,
        "rules": "# Role",
        "system_prompt": "",
        "cwd": "/workspace/producer",
        "workspace_root": "/workspace",
        "enable_tools": True,
        "interactive": False,
        "tool_policies": {"write": {"allowed": True}},
    }
    values.update(overrides)
    return ClineStartRequest(**values)


def _recorded_calls(record_path: Path) -> list[dict[str, Any]]:
    if not record_path.exists():
        return []
    text = record_path.read_text(encoding="utf-8")
    return [json.loads(line) for line in text.splitlines() if line.strip()]


def _wait_until(predicate: Callable[[], bool], timeout: float = 5.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.02)
    raise AssertionError("timed out waiting for the expected condition")


def test_backend_satisfies_the_d2_contract() -> None:
    backend = ClineBridgeProcessBackend(command=[sys.executable, "-c", "pass"])
    assert isinstance(backend, ClineSdkBackend)


def test_connect_round_trip_returns_declared_typed_results(tmp_path: Path) -> None:
    script = {
        "outcomes": {
            "start": [{"ok": {"session_id": "sess-1", "result": {"turn": 1}}}],
            "send": [{"ok": {"answer": "sdk-native"}}, {"ok": None}],
            "get": [{"ok": {"status": "running"}}],
            "read_messages": [{"ok": []}],
            "list_history": [{"ok": [{"session_id": "sess-1"}]}],
            "get_accumulated_usage": [{"ok": None}],
            "stop": [{}],
            "abort": [{}, {}],
        }
    }
    command, child_env, record_path = _scripted_child(tmp_path, "trip", script)
    backend = _make_backend(command, child_env)
    backend.connect(_remote_config())
    started = backend.start(_start_request())
    assert started == ClineStartResult(session_id="sess-1", result={"turn": 1})
    assert backend.send("sess-1", "first prompt") == {"answer": "sdk-native"}
    assert backend.send("sess-1", "second prompt") is None
    assert backend.get("sess-1") == {"status": "running"}
    assert backend.read_messages("sess-1") == []
    assert backend.list_history() == [{"session_id": "sess-1"}]
    assert backend.get_accumulated_usage("sess-1") is None
    backend.stop("sess-1")
    backend.abort("sess-1")
    backend.abort("sess-1", reason="owner interrupted")
    backend.dispose()

    calls = _recorded_calls(record_path)
    assert [call["method"] for call in calls] == [
        "connect",
        "start",
        "send",
        "send",
        "get",
        "read_messages",
        "list_history",
        "get_accumulated_usage",
        "stop",
        "abort",
        "abort",
    ]
    assert calls[0]["args"] == {
        "endpoint": "https://hub.example.test",
        "client_identity": "openorc-test",
        "auth_token": "TOKEN-SENTINEL",
        "remote_options": {"region": "test"},
    }
    assert calls[1]["args"] == {
        "provider_id": "provider-x",
        "model_id": "model-y",
        "mode": "plan",
        "rules": "# Role",
        "system_prompt": "",
        "cwd": "/workspace/producer",
        "workspace_root": "/workspace",
        "enable_tools": True,
        "interactive": False,
        "tool_policies": {"write": {"allowed": True}},
    }
    assert calls[2]["args"] == {"session_id": "sess-1", "prompt": "first prompt"}
    assert calls[3]["args"] == {"session_id": "sess-1", "prompt": "second prompt"}
    assert calls[9]["args"] == {"session_id": "sess-1"}  # an absent abort reason stays absent
    assert calls[10]["args"] == {"session_id": "sess-1", "reason": "owner interrupted"}
    assert not any(call["method"] in ("dispose", "unsubscribe") for call in calls)


def test_same_id_reconstruction_round_trip_is_lossless(tmp_path: Path) -> None:
    messages: list[dict[str, Any]] = [
        {
            "role": "user",
            "parts": [
                {"type": "text", "text": "héllo", "meta": {"nested": [1, 2, {"deep": True}]}}
            ],
        },
        {"role": "assistant", "parts": []},
    ]
    script = {"outcomes": {"read_messages": [{"ok": messages}]}}
    command, child_env, record_path = _scripted_child(tmp_path, "lossless", script)
    backend = _make_backend(command, child_env)
    backend.connect(_remote_config())
    started = backend.start(_start_request())
    session_id = started.session_id
    assert backend.read_messages(session_id) == messages
    backend.start(_start_request(session_id=session_id, initial_messages=messages))
    assert backend.read_messages(session_id) == messages
    backend.dispose()

    starts = [call for call in _recorded_calls(record_path) if call["method"] == "start"]
    assert starts[1]["args"]["session_id"] == session_id
    assert starts[1]["args"]["initial_messages"] == messages  # byte-exact nested round-trip


def test_independent_calls_progress_during_a_long_send(tmp_path: Path) -> None:
    script = {
        "outcomes": {
            "send": [{"delay_ms": 900, "ok": None}],
            "abort": [{"ok": None}],
            "get": [{"ok": {"status": "running"}}],
            "read_messages": [{"ok": []}],
        }
    }
    command, child_env, _ = _scripted_child(tmp_path, "concurrent", script)
    backend = _make_backend(command, child_env)
    backend.connect(_remote_config())
    backend.start(_start_request())
    send_done = threading.Event()

    def run_send() -> None:
        backend.send("cline-session-1", "long prompt")
        send_done.set()

    worker = threading.Thread(target=run_send)
    worker.start()
    try:
        assert backend.abort("cline-session-1", reason="interrupt") is None
        assert backend.get("cline-session-1") == {"status": "running"}
        assert backend.read_messages("cline-session-1") == []
        assert not send_done.is_set()  # the long send was still in flight
    finally:
        assert send_done.wait(10.0)
        worker.join(10.0)
    backend.dispose()


def test_out_of_order_replies_bind_to_exact_ids(tmp_path: Path) -> None:
    script = {
        "outcomes": {
            "get": [{"delay_ms": 400, "ok": {"from": "get"}}],
            "read_messages": [{"delay_ms": 100, "ok": [{"m": 1}]}],
        }
    }
    command, child_env, _ = _scripted_child(tmp_path, "ordering", script)
    backend = _make_backend(command, child_env)
    backend.connect(_remote_config())
    backend.start(_start_request())
    read_done = threading.Event()
    get_done = threading.Event()
    read_result: list[Any] = []
    get_result: list[Any] = []

    def run_read() -> None:
        read_result.append(backend.read_messages("cline-session-1"))
        read_done.set()

    def run_get() -> None:
        get_result.append(backend.get("cline-session-1"))
        get_done.set()

    get_worker = threading.Thread(target=run_get)
    read_worker = threading.Thread(target=run_read)
    get_worker.start()
    read_worker.start()
    assert read_done.wait(5.0)
    assert not get_done.is_set()  # the read completed first, out of order
    assert get_done.wait(5.0)
    get_worker.join(5.0)
    read_worker.join(5.0)
    assert get_result == [{"from": "get"}]
    assert read_result == [[{"m": 1}]]
    backend.dispose()


def test_event_filtering_and_payload_fidelity(tmp_path: Path) -> None:
    payload = {"sessionId": "s1", "turn": {"cost": 1.5, "tokens": {"input": 10}}}
    script = {
        "events": [
            {
                "after_subscribe": 1,
                "delay_ms": 30,
                "session_id": "s1",
                "kind": "message",
                "payload": payload,
            },
            {
                "after_subscribe": 1,
                "delay_ms": 60,
                "session_id": "s2",
                "kind": "message",
                "payload": {"other": True},
            },
            {
                "after_subscribe": 1,
                "delay_ms": 90,
                "session_id": "s1",
                "kind": "state",
                "payload": {"mode": "act"},
            },
            {
                "after_subscribe": 2,
                "delay_ms": 200,
                "session_id": "s1",
                "kind": "message",
                "payload": payload,
            },
            {
                "after_subscribe": 2,
                "delay_ms": 230,
                "session_id": "s2",
                "kind": "message",
                "payload": {"other": True},
            },
            {
                "after_subscribe": 2,
                "delay_ms": 260,
                "session_id": "s1",
                "kind": "state",
                "payload": {"mode": "act"},
            },
        ]
    }
    command, child_env, _ = _scripted_child(tmp_path, "events", script)
    backend = _make_backend(command, child_env)
    backend.connect(_remote_config())
    filtered: list[ClineSessionEvent] = []
    unfiltered: list[ClineSessionEvent] = []
    first = backend.subscribe("s1", filtered.append)
    second = backend.subscribe(None, unfiltered.append)
    # A scripted event is delivered to every active matching subscription.
    _wait_until(lambda: len(filtered) >= 4 and len(unfiltered) >= 6)
    assert [(event.session_id, event.kind) for event in filtered] == [
        ("s1", "message"),
        ("s1", "state"),
        ("s1", "message"),
        ("s1", "state"),
    ]
    assert [event.payload for event in unfiltered] == [
        payload,
        {"other": True},
        {"mode": "act"},
        payload,
        {"other": True},
        {"mode": "act"},
    ]
    assert json.dumps(payload) not in repr(
        filtered[0]
    )  # event payloads never enter representations
    first.unsubscribe()
    second.unsubscribe()
    backend.dispose()


def test_listener_failure_reentrancy_and_independent_progress(tmp_path: Path) -> None:
    script = {
        "outcomes": {
            "get": [
                {"ok": {"status": "ok"}},
                {"ok": {"status": "ok"}},
                {"ok": {"status": "ok"}},
                {"ok": {"status": "ok"}},
            ]
        },
        "events": [
            {
                "after_subscribe": 1,
                "delay_ms": 30,
                "session_id": "s1",
                "kind": "message",
                "payload": {"n": 1},
            },
            {
                "after_subscribe": 1,
                "delay_ms": 160,
                "session_id": "s1",
                "kind": "message",
                "payload": {"n": 2},
            },
        ],
    }
    command, child_env, _ = _scripted_child(tmp_path, "listener", script)
    backend = _make_backend(command, child_env)
    backend.connect(_remote_config())
    backend.start(_start_request())
    seen: list[Any] = []
    reentrant_results: list[Any] = []

    def failing_listener(event: ClineSessionEvent) -> None:
        seen.append(event.payload["n"])
        raise RuntimeError("deliberate listener failure")

    def reentrant_listener(event: ClineSessionEvent) -> None:
        reentrant_results.append(backend.get("cline-session-1"))

    backend.subscribe("s1", failing_listener)
    backend.subscribe("s1", reentrant_listener)
    independent = backend.get("cline-session-1")  # completes while listeners dispatch
    assert independent == {"status": "ok"}
    _wait_until(lambda: len(seen) >= 2 and len(reentrant_results) >= 2)
    assert seen == [1, 2]  # a throwing listener did not stop later deliveries
    assert reentrant_results == [{"status": "ok"}, {"status": "ok"}]  # reentrant calls were served
    backend.dispose()


def test_unsubscribe_is_idempotent_and_isolated(tmp_path: Path) -> None:
    script = {
        "events": [
            {
                "after_subscribe": 1,
                "delay_ms": 30,
                "session_id": "s1",
                "kind": "message",
                "payload": {"n": 1},
            },
            {
                "after_subscribe": 2,
                "delay_ms": 400,
                "session_id": "s1",
                "kind": "message",
                "payload": {"n": 2},
            },
        ]
    }
    command, child_env, record_path = _scripted_child(tmp_path, "unsub", script)
    backend = _make_backend(command, child_env)
    backend.connect(_remote_config())
    first: list[ClineSessionEvent] = []
    second: list[ClineSessionEvent] = []
    first_handle = backend.subscribe("s1", first.append)
    second_handle = backend.subscribe("s1", second.append)
    _wait_until(lambda: len(first) >= 1 and len(second) >= 1)
    first_handle.unsubscribe()
    first_handle.unsubscribe()  # idempotent: exactly one wire unsubscribe below
    _wait_until(lambda: len(second) >= 2)
    assert [event.payload["n"] for event in first] == [1]  # no delivery after unsubscribe
    assert [event.payload["n"] for event in second] == [1, 2]  # the other handle is isolated
    second_handle.unsubscribe()
    backend.dispose()
    assert (
        len([call for call in _recorded_calls(record_path) if call["method"] == "unsubscribe"]) == 2
    )


def test_queued_events_cannot_reach_an_unsubscribed_listener(tmp_path: Path) -> None:
    script = {
        "events": [
            {
                "after_subscribe": 1,
                "delay_ms": 30,
                "session_id": "s1",
                "kind": "message",
                "payload": {"n": 1},
            },
            {
                "after_subscribe": 1,
                "delay_ms": 30,
                "session_id": "s1",
                "kind": "message",
                "payload": {"n": 2},
            },
        ]
    }
    command, child_env, record_path = _scripted_child(tmp_path, "queued", script)
    backend = _make_backend(command, child_env)
    backend.connect(_remote_config())
    seen: list[Any] = []
    started = threading.Event()
    gate = threading.Event()

    def blocking_listener(event: ClineSessionEvent) -> None:
        seen.append(event.payload["n"])
        started.set()
        assert gate.wait(5.0)  # hold the single dispatcher busy; event 2 stays queued

    subscription = backend.subscribe("s1", blocking_listener)
    assert started.wait(5.0)  # event 1 is executing; event 2 is queued behind it
    subscription.unsubscribe()  # removes the local registration synchronously
    assert subscription.active is False
    gate.set()  # release the in-execution callback
    time.sleep(0.3)  # the dispatcher drains: the queued event must be dropped
    assert seen == [1]  # the queued event never reached the unsubscribed listener
    subscription.unsubscribe()  # idempotent
    backend.dispose()
    assert (
        len([call for call in _recorded_calls(record_path) if call["method"] == "unsubscribe"]) == 1
    )


@pytest.mark.parametrize(
    ("method", "error", "expected_type", "expected"),
    [
        ("connect", {"kind": "remote_attachment_rejected"}, ClineRemoteAttachmentRejectedError, {}),
        (
            "connect",
            {"kind": "remote_attachment_unavailable"},
            ClineRemoteAttachmentUnavailableError,
            {},
        ),
        ("send", {"kind": "uncertain_outcome"}, ClineBackendUncertainOutcomeError, {}),
        ("send", {"kind": "session_not_found"}, ClineSessionNotFoundError, {}),
        (
            "get",
            {"kind": "sdk_operation_rejected", "code": "hub_connection_closed", "close_code": 1006},
            ClineSdkOperationRejectedError,
            {"code": "hub_connection_closed", "details": {"close_code": 1006}},
        ),
        (
            "get",
            {"kind": "wire_protocol", "reason": "invalid_params"},
            ClineBridgeProtocolError,
            {"reason": "invalid_params"},
        ),
        ("get", {"kind": "mystery"}, ClineBackendUncertainOutcomeError, {}),
    ],
)
def test_wire_error_classification_is_conservative_and_sanitized(
    tmp_path: Path,
    method: str,
    error: dict[str, Any],
    expected_type: type[Exception],
    expected: dict[str, Any],
) -> None:
    script = {"sentinel": True, "outcomes": {method: [{"error": error}]}}
    command, child_env, _ = _scripted_child(tmp_path, "err", script)
    backend = _make_backend(command, child_env)
    if method == "connect":
        with pytest.raises(expected_type) as raised:
            backend.connect(_remote_config())
    else:
        backend.connect(_remote_config())
        backend.start(_start_request())
        with pytest.raises(expected_type) as raised:
            if method == "send":
                backend.send("cline-session-1", "probe prompt")
            else:
                backend.get("cline-session-1")
    failure = raised.value
    for text in (str(failure), repr(failure)):
        assert "SECRET-SENTINEL" not in text
        assert "probe prompt" not in text
    for attribute, value in expected.items():
        assert getattr(failure, attribute) == value
    if isinstance(failure, ClineBridgeProtocolError):
        assert (
            failure.reason != "session_not_found"
        )  # wire reasons stay distinct from session absence
    backend.dispose()


def test_operations_before_connect_fail_not_attached_without_spawning(tmp_path: Path) -> None:
    marker = tmp_path / "spawned.marker"
    child = tmp_path / "marker-child.py"
    child.write_text(
        f"from pathlib import Path\nPath({str(marker)!r}).write_text('spawned')\n", encoding="utf-8"
    )
    backend = ClineBridgeProcessBackend(
        command=[sys.executable, str(child)], request_timeout_seconds=2.0
    )
    with pytest.raises(ClineBridgeProtocolError) as raised:
        backend.start(_start_request())
    assert raised.value.reason == "not_attached"
    with pytest.raises(ClineBridgeProtocolError):
        backend.send("s", "probe prompt")
    with pytest.raises(ClineBridgeProtocolError):
        backend.get("s")
    with pytest.raises(ClineBridgeProtocolError):
        backend.subscribe("s", lambda event: None)
    backend.dispose()  # no-op without an attachment
    assert not marker.exists()  # no child was ever launched


def test_timeout_invalidates_attachment_until_explicit_reconnect(tmp_path: Path) -> None:
    script = {
        "outcomes": {
            "send": [{"delay_ms": 30000}],
            "read_messages": [{"ok": [{"role": "user", "parts": []}]}],
        }
    }
    command, child_env, record_path = _scripted_child(tmp_path, "timeout", script)
    backend = _make_backend(command, child_env, request_timeout=0.3)
    backend.connect(_remote_config())
    backend.start(_start_request())
    started_at = time.monotonic()
    with pytest.raises(ClineBackendUncertainOutcomeError):
        backend.send("cline-session-1", "probe prompt")  # bounded wait, not the scripted delay
    assert time.monotonic() - started_at < 3.0
    with pytest.raises(ClineBridgeProtocolError) as raised:
        backend.get("cline-session-1")
    assert raised.value.reason == "not_attached"  # further use requires an explicit reconnect
    with pytest.raises(ClineBridgeProtocolError):
        backend.start(_start_request())

    backend.connect(_remote_config())  # explicit reattachment: a fresh child
    messages = backend.read_messages("cline-session-1")  # same explicitly supplied session ID
    assert messages == [{"role": "user", "parts": []}]
    backend.dispose()

    calls = _recorded_calls(record_path)
    assert len([call for call in calls if call["method"] == "connect"]) == 2
    assert len([call for call in calls if call["method"] == "start"]) == 1  # no implicit start
    assert len([call for call in calls if call["method"] == "send"]) == 1  # no replay


def test_invalid_result_shape_invalidates_attachment_conservatively(tmp_path: Path) -> None:
    script = {
        "outcomes": {
            "get": [{"ok": "not-a-json-object"}],
            "read_messages": [{"ok": [{"role": "user", "parts": []}]}],
        }
    }
    command, child_env, _ = _scripted_child(tmp_path, "shape", script)
    backend = _make_backend(command, child_env)
    backend.connect(_remote_config())
    backend.start(_start_request())
    with pytest.raises(ClineBackendUncertainOutcomeError):
        backend.get("cline-session-1")  # not evidence the effect never happened
    with pytest.raises(ClineBridgeProtocolError) as raised:
        backend.send("cline-session-1", "probe prompt")
    assert raised.value.reason == "not_attached"
    backend.connect(_remote_config())  # explicit reattachment serves the same supplied ID
    assert backend.read_messages("cline-session-1") == [{"role": "user", "parts": []}]
    backend.dispose()


def test_invalid_envelope_invalidates_attachment_conservatively(tmp_path: Path) -> None:
    script = {
        "outcomes": {
            "get": [{"garbage": '{"id": "1.2", "result": 1}', "ok": {"status": "running"}}],
        }
    }
    command, child_env, _ = _scripted_child(tmp_path, "envelope", script)
    backend = _make_backend(command, child_env)
    backend.connect(_remote_config())
    backend.start(_start_request())
    with pytest.raises(ClineBackendUncertainOutcomeError):
        backend.get("cline-session-1")  # the corrupt frame invalidated the attachment
    with pytest.raises(ClineBridgeProtocolError) as raised:
        backend.send("cline-session-1", "probe prompt")
    assert raised.value.reason == "not_attached"
    backend.dispose()


def test_child_crash_invalidates_and_reconnect_reads_exact_session(tmp_path: Path) -> None:
    script = {
        "outcomes": {
            "send": [{"delay_ms": 100, "crash": True}],
            "read_messages": [{"ok": [{"role": "user", "parts": []}]}],
        }
    }
    command, child_env, record_path = _scripted_child(tmp_path, "reconnect", script)
    backend = _make_backend(command, child_env)
    backend.connect(_remote_config())
    started = backend.start(_start_request())
    session_id = started.session_id
    old_handle = backend.subscribe(session_id, lambda event: None)
    with pytest.raises(ClineBackendUncertainOutcomeError):
        backend.send(session_id, "probe prompt")
    with pytest.raises(ClineBridgeProtocolError) as raised:
        backend.get(session_id)
    assert raised.value.reason == "not_attached"  # the crashed attachment is invalidated

    backend.connect(_remote_config())  # explicit reattachment: a fresh child
    messages = backend.read_messages(session_id)  # same explicitly supplied session ID
    assert messages == [{"role": "user", "parts": []}]
    old_handle.unsubscribe()  # must not touch the new attachment
    backend.dispose()

    calls = _recorded_calls(record_path)
    starts = [call for call in calls if call["method"] == "start"]
    sends = [call for call in calls if call["method"] == "send"]
    unsubscribes = [call for call in calls if call["method"] == "unsubscribe"]
    reads = [call for call in calls if call["method"] == "read_messages"]
    assert len(starts) == 1  # no implicit start or session replacement
    assert len(sends) == 1  # no implicit send or replay
    assert unsubscribes == []  # old handles never reach a new attachment
    assert reads[0]["args"] == {"session_id": session_id}


def test_malformed_frame_invalidates_attachment_conservatively(tmp_path: Path) -> None:
    script = {
        "outcomes": {"get": [{"garbage": "definitely not json", "ok": {"status": "running"}}]}
    }
    command, child_env, _ = _scripted_child(tmp_path, "corrupt", script)
    backend = _make_backend(command, child_env)
    backend.connect(_remote_config())
    backend.start(_start_request())
    with pytest.raises(ClineBackendUncertainOutcomeError):
        backend.get("cline-session-1")
    with pytest.raises(ClineBridgeProtocolError) as raised:
        backend.send("cline-session-1", "probe prompt")
    assert raised.value.reason == "not_attached"
    backend.dispose()


def test_dispose_unblocks_waiters_repeatable_and_leaves_no_workers(tmp_path: Path) -> None:
    script = {"outcomes": {"send": [{"delay_ms": 30000}]}}
    command, child_env, record_path = _scripted_child(tmp_path, "dispose", script)
    backend = _make_backend(command, child_env, request_timeout=30.0)
    backend.connect(_remote_config())
    backend.start(_start_request())
    outcomes: list[BaseException | None] = []

    def run_send() -> None:
        try:
            backend.send("cline-session-1", "probe prompt")
            outcomes.append(None)
        except Exception as error:  # the test records the classified failure
            outcomes.append(error)

    worker = threading.Thread(target=run_send)
    worker.start()
    time.sleep(0.15)  # let the send dispatch
    disposed_at = time.monotonic()
    backend.dispose()
    worker.join(5.0)
    assert not worker.is_alive()
    assert time.monotonic() - disposed_at < 5.0  # bounded, not the scripted 30s
    assert outcomes and isinstance(outcomes[0], ClineBackendUncertainOutcomeError)
    backend.dispose()  # idempotent repeat
    assert [
        thread
        for thread in threading.enumerate()
        if thread.name.startswith("openorc-cline-bridge-")
    ] == []
    assert not any(
        call["method"] in ("dispose", "stop", "abort") for call in _recorded_calls(record_path)
    )


def test_pre_dispatch_launch_failures_are_known_failures(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    backend = ClineBridgeProcessBackend(command=[str(tmp_path / "missing-node")])
    with pytest.raises(ClineRemoteAttachmentUnavailableError) as raised:
        backend.connect(_remote_config())
    assert raised.value.__cause__ is None and raised.value.__context__ is None
    monkeypatch.setenv("OPENORC_CLINE_BRIDGE_ENTRYPOINT", str(tmp_path / "missing-cli.js"))
    with pytest.raises(ClineRemoteAttachmentUnavailableError):
        ClineBridgeProcessBackend().connect(_remote_config())
    monkeypatch.setenv("OPENORC_CLINE_BRIDGE_NODE", str(tmp_path / "missing-node"))
    with pytest.raises(ClineRemoteAttachmentUnavailableError):
        ClineBridgeProcessBackend().connect(_remote_config())


def test_default_launch_resolution_targets_the_built_entrypoint() -> None:
    entrypoint = _default_entrypoint_path()
    assert entrypoint.name == "cli.js"
    if (entrypoint.parents[1] / "package.json").is_file():  # the bridge package is present
        assert entrypoint.is_file()


def test_unserializable_inputs_fail_before_dispatch_sanitized(tmp_path: Path) -> None:
    command, child_env, record_path = _scripted_child(tmp_path, "serial", {})
    backend = _make_backend(command, child_env)
    backend.connect(_remote_config())
    with pytest.raises(ValueError) as raised:
        backend.start(_start_request(tool_policies={"policy": {"bad": object()}}))
    assert raised.value.__cause__ is None and raised.value.__context__ is None
    assert [call["method"] for call in _recorded_calls(record_path)] == [
        "connect"
    ]  # never dispatched
    backend.dispose()


def test_sensitive_values_never_enter_errors_or_diagnostics(tmp_path: Path) -> None:
    script = {
        "outcomes": {
            "send": [{"error": {"kind": "uncertain_outcome"}}],
            "get": [{"error": {"kind": "session_not_found"}}],
        }
    }
    command, child_env, _ = _scripted_child(tmp_path, "hygiene", script)
    backend = _make_backend(command, child_env)
    backend.connect(_remote_config())  # carries TOKEN-SENTINEL
    backend.start(_start_request(rules="PROMPT-SENTINEL rules"))
    failures: list[Exception] = []
    with pytest.raises(ClineBackendUncertainOutcomeError) as first:
        backend.send("cline-session-1", "PROMPT-SENTINEL prompt")
    failures.append(first.value)
    with pytest.raises(ClineSessionNotFoundError) as second:
        backend.get("missing-session")
    failures.append(second.value)
    for failure in failures:
        for text in (str(failure), repr(failure)):
            assert "TOKEN-SENTINEL" not in text
            assert "PROMPT-SENTINEL" not in text
    backend.dispose()


def _seam_missing_reason() -> str | None:
    if shutil.which("node") is None:
        return "the node executable is not on PATH"
    if not _BUILT_BRIDGE_CORE.is_file():
        return "the built bridge dist/bridge.js is missing (build packages/cline-sdk-bridge first)"
    if not _SPAWN_HARNESS.is_file():
        return "the D3 spawn harness is missing"
    return None


@pytest.fixture
def node_seam(tmp_path: Path) -> Iterator[tuple[list[str], dict[str, str], Path]]:
    """The real Python-to-Node seam launch: D3's scripted spawn harness.

    Skips when prerequisites are unavailable in ordinary Python-only
    environments; with OPENORC_BRIDGE_SEAM_REQUIRED=1 (CI's python-checks
    job builds the bridge first) a missing prerequisite is a hard failure.
    """
    reason = _seam_missing_reason()
    if reason is not None:
        if os.environ.get(_SEAM_REQUIRED_ENV) == "1":
            pytest.fail(f"required Python-to-Node bridge seam test could not execute: {reason}")
        pytest.skip(f"bridge seam prerequisites unavailable: {reason}")
    harness_script = {
        "events": [
            {
                "afterSubscribe": 1,
                "delayMs": 50,
                "sessionId": "cline-session-1",
                "kind": "message",
                "payload": {"sessionId": "cline-session-1", "extra": {"deep": [1, 2, 3]}},
            }
        ],
        "outcomes": {
            "read_messages": [
                {"ok": [{"role": "user", "parts": [{"type": "text", "text": "persisted"}]}]}
            ]
        },
    }
    script_path = tmp_path / "seam-script.json"
    script_path.write_text(json.dumps(harness_script), encoding="utf-8")
    record_path = tmp_path / "seam-record.jsonl"
    command = ["node", str(_SPAWN_HARNESS)]
    child_env = {
        "OPENORC_BRIDGE_FAKE_SCRIPT": str(script_path),
        "OPENORC_BRIDGE_FAKE_RECORD": str(record_path),
    }
    yield command, child_env, record_path


def test_real_python_to_node_bridge_seam(node_seam: tuple[list[str], dict[str, str], Path]) -> None:
    command, child_env, record_path = node_seam
    backend = _make_backend(command, child_env)
    backend.connect(_remote_config())
    started = backend.start(_start_request())
    assert started == ClineStartResult(session_id="cline-session-1", result=None)
    assert backend.send("cline-session-1", "probe prompt") is None  # SDK undefined → JSON null
    assert backend.get("cline-session-1") is None
    messages = [{"role": "user", "parts": [{"type": "text", "text": "persisted"}]}]
    assert backend.read_messages("cline-session-1") == messages
    backend.start(_start_request(session_id="cline-session-1", initial_messages=messages))
    received: list[ClineSessionEvent] = []
    subscription = backend.subscribe("cline-session-1", received.append)
    _wait_until(lambda: len(received) >= 1)
    assert received[0].session_id == "cline-session-1"
    assert received[0].kind == "message"
    assert received[0].payload == {"sessionId": "cline-session-1", "extra": {"deep": [1, 2, 3]}}
    subscription.unsubscribe()
    backend.stop("cline-session-1")
    backend.dispose()

    calls = _recorded_calls(record_path)
    by_method = {call["method"]: call["args"] for call in calls}
    assert by_method["factory"] == {  # the real child received the flattened remote inputs
        "endpoint": "https://hub.example.test",
        "client_identity": "openorc-test",
        "auth_token": "TOKEN-SENTINEL",
        "remote_options": {"region": "test"},
    }
    starts = [call for call in calls if call["method"] == "start"]
    first_config = starts[0]["args"]["config"]
    assert first_config["providerId"] == "provider-x"  # the D2 bundle mapped verbatim
    assert first_config["mode"] == "plan"
    assert "sessionId" not in first_config  # fresh construction carries no session ID
    assert starts[1]["args"]["config"]["sessionId"] == "cline-session-1"  # same exact external ID
    assert (
        starts[1]["args"]["initialMessages"] == messages
    )  # nested raw payload round-trips losslessly through the real child
    assert not any(call["method"] in ("dispose", "unsubscribe") for call in calls)
