"""Deterministic self-tests for the fake Cline SDK backend instrument (#71).

These tests prove only D2's own seam: that a Python client can consume the
declared ``ClineSdkBackend`` contract against the fake without Node, a
subprocess, or ``@cline/sdk``; that construction/reconstruction inputs and
raw message arrays round-trip losslessly with exact session identity; that
``send``/read observations pass through unparsed; that ``abort``/``stop``/
``dispose`` stay distinct without implicit delete/replacement; that event
forwarding, filtering, and unsubscribe are deterministic; that the backend
error classifications stay machine-distinguishable without leaking a
sensitive sentinel through text/representation/exception chains; and that
fake instances and scripted FIFO outcomes are isolated. The behavioral
matrices of #65/#67/#69/#130 and the R-series qualification experiments are
deliberately not repeated here.
"""

from __future__ import annotations

import copy
from collections.abc import Callable
from typing import Any

import pytest
from tests.fakes.cline_sdk_backend import (
    OP_ABORT,
    OP_CONNECT,
    OP_DISPOSE,
    OP_GET,
    OP_GET_ACCUMULATED_USAGE,
    OP_LIST_HISTORY,
    OP_READ_MESSAGES,
    OP_SEND,
    OP_START,
    OP_STOP,
    OP_SUBSCRIBE,
    FakeBackendCall,
    FakeClineSdkBackend,
)

from openorc.adapters.cline import (
    ClineBackendUncertainOutcomeError,
    ClineConstructionMode,
    ClineRemoteAttachmentRejectedError,
    ClineRemoteAttachmentUnavailableError,
    ClineRemoteConfig,
    ClineSdkBackend,
    ClineSdkBackendError,
    ClineSdkOperationRejectedError,
    ClineSessionEvent,
    ClineSessionNotFoundError,
    ClineStartRequest,
    ClineStartResult,
)


def _remote_config() -> ClineRemoteConfig:
    return ClineRemoteConfig(
        endpoint="https://hub.example.test",
        client_identity="openorc-core/test",
        auth_token="SENSITIVE-ATTACH-TOKEN-SENTINEL",
        remote_options={"workspaceRoot": "/workspace"},
    )


def _start_request(**overrides: Any) -> ClineStartRequest:
    fields: dict[str, Any] = {
        "provider_id": "opaque-provider-id",
        "model_id": "opaque-model-id",
        "mode": ClineConstructionMode.PLAN,
        "rules": "# Producer role rules\n\nMarkdown guidance.",
        "system_prompt": "",
        "cwd": "/workspace/producer",
        "workspace_root": "/workspace",
        "enable_tools": True,
        "interactive": True,
        "tool_policies": {
            "*": {"autoApprove": True},
            "ask_question": {"enabled": False},
        },
    }
    fields.update(overrides)
    return ClineStartRequest(**fields)


def _raw_messages() -> list[dict[str, Any]]:
    return [
        {
            "sessionId": "raw-session-id",
            "ts": 1728000000000,
            "messages": [
                {
                    "role": "user",
                    "content": [{"type": "text", "text": "composed turn text"}],
                    "metadata": {"nested": {"deep": [1, 2, {"key": "value"}]}},
                },
                {
                    "role": "assistant",
                    "content": [{"type": "tool_use", "name": "read_file"}],
                },
            ],
        }
    ]


def _sample_client_session(backend: ClineSdkBackend) -> str:
    """Drive one full session through only the declared contract type."""
    backend.connect(_remote_config())
    started = backend.start(_start_request())
    backend.send(started.session_id, "composed producer prompt")
    messages = backend.read_messages(started.session_id)
    assert isinstance(messages, list)
    assert backend.get_accumulated_usage(started.session_id) is None
    backend.abort(started.session_id, reason="owner-cancelled")
    return started.session_id


def test_sample_client_consumes_the_declared_contract_without_runtime_dependencies() -> None:
    fake = FakeClineSdkBackend()
    # Structural conformance to the #71 contract, without Node, a subprocess,
    # or @cline/sdk:
    assert isinstance(fake, ClineSdkBackend)

    session_id = _sample_client_session(fake)

    assert session_id == "cline-session-1"
    assert [call.operation for call in fake.calls()] == [
        OP_CONNECT,
        OP_START,
        OP_SEND,
        OP_READ_MESSAGES,
        OP_GET_ACCUMULATED_USAGE,
        OP_ABORT,
    ]
    # Sensitive attachment inputs never enter representation:
    config_repr = repr(_remote_config())
    assert "SENSITIVE-ATTACH-TOKEN-SENTINEL" not in config_repr
    assert "workspaceRoot" not in config_repr


def test_fresh_start_preserves_the_construction_bundle_and_allocates_exact_ids() -> None:
    backend = FakeClineSdkBackend()
    request = _start_request()

    started = backend.start(request)

    assert started.session_id == "cline-session-1"
    assert started.result is None
    assert backend.calls()[0] == FakeBackendCall(OP_START, session_id=None, request=request)

    # A second fresh start allocates the next deterministic opaque ID:
    assert backend.start(_start_request()).session_id == "cline-session-2"


def test_same_id_start_round_trips_the_raw_message_array_losslessly() -> None:
    backend = FakeClineSdkBackend()
    session_id = backend.start(_start_request()).session_id

    raw = _raw_messages()
    backend.queue_read_messages(session_id, raw)
    read = backend.read_messages(session_id)
    assert read == raw
    assert read is not raw

    # What a caller received is an isolated deep copy: mutating it cannot
    # touch what the fake recorded or will return next.
    read[0]["messages"][0]["metadata"]["nested"]["deep"][2]["key"] = "mutated"

    pristine = copy.deepcopy(raw)
    reconstruction = _start_request(session_id=session_id, initial_messages=pristine)
    restarted = backend.start(reconstruction)

    # The exact external session identity stays intact:
    assert restarted.session_id == session_id == "cline-session-1"
    assert restarted.result is None
    start_calls = [call for call in backend.calls() if call.operation == OP_START]
    assert start_calls[1] == FakeBackendCall(
        OP_START, session_id=session_id, request=reconstruction
    )
    assert start_calls[1].request is not None
    # The nested raw array arrived unchanged, and reads still serve it:
    assert start_calls[1].request.initial_messages == raw
    assert backend.read_messages(session_id) == raw

    # The stored round-trip array is itself an isolated deep copy:
    stored = backend.stored_messages(session_id)
    stored[0]["messages"][0]["metadata"]["nested"]["deep"][2]["key"] = "mutated-again"
    assert backend.read_messages(session_id) == raw


def test_send_carries_exact_identity_and_prompt_and_returns_unparsed_results() -> None:
    backend = FakeClineSdkBackend()
    session_id = backend.start(_start_request()).session_id

    native_result = {
        "isError": False,
        "sessionMetadata": {"id": "raw-session-id"},
        "textChunks": [{"plainText": "native candidate text"}],
    }
    backend.queue_send(session_id, native_result)
    assert backend.send(session_id, "composed exact prompt") == native_result

    # The SDK's legitimate undefined response stays the None case, unparsed:
    assert backend.send(session_id, "second composed prompt") is None

    send_calls = [call for call in backend.calls() if call.operation == OP_SEND]
    assert send_calls == [
        FakeBackendCall(OP_SEND, session_id=session_id, prompt="composed exact prompt"),
        FakeBackendCall(OP_SEND, session_id=session_id, prompt="second composed prompt"),
    ]


def test_reads_pass_through_public_shapes_without_deriving_state() -> None:
    backend = FakeClineSdkBackend()
    session_id = backend.start(_start_request()).session_id

    # Unscripted defaults where the SDK permits absence:
    assert backend.get(session_id) is None
    assert backend.list_history() == []
    assert backend.get_accumulated_usage(session_id) is None

    record = {"status": "active", "sessionId": session_id}
    history = [{"sessionId": session_id, "ts": 1728000000000}]
    usage = {"tokensIn": 11, "tokensOut": 7, "cost": 0.25}
    backend.queue_get(session_id, record)
    backend.queue_list_history(history)
    backend.queue_get_accumulated_usage(session_id, usage)

    # Scripted observations pass through verbatim; nothing is derived:
    assert backend.get(session_id) == record
    assert backend.list_history() == history
    assert backend.get_accumulated_usage(session_id) == usage

    assert [call.operation for call in backend.calls()[1:]] == [
        OP_GET,
        OP_LIST_HISTORY,
        OP_GET_ACCUMULATED_USAGE,
        OP_GET,
        OP_LIST_HISTORY,
        OP_GET_ACCUMULATED_USAGE,
    ]


@pytest.mark.parametrize(
    ("operation", "exercise"),
    [
        pytest.param(
            OP_ABORT,
            lambda backend, session_id: backend.abort(session_id, reason="owner-cancelled"),
            id="abort",
        ),
        pytest.param(
            OP_STOP,
            lambda backend, session_id: backend.stop(session_id),
            id="stop",
        ),
        pytest.param(
            OP_DISPOSE,
            lambda backend, session_id: backend.dispose(),
            id="dispose",
        ),
    ],
)
def test_release_operations_stay_distinct_without_implicit_deletion_or_replacement(
    operation: str, exercise: Callable[[FakeClineSdkBackend, str], None]
) -> None:
    backend = FakeClineSdkBackend()
    session_id = backend.start(_start_request(initial_messages=_raw_messages())).session_id
    recorded_before = len(backend.calls())

    exercise(backend, session_id)

    # Exactly the one release operation was recorded, with its exact target:
    release_call = backend.calls()[recorded_before]
    assert release_call.operation == operation
    if operation == OP_DISPOSE:
        assert release_call.session_id is None
    else:
        assert release_call.session_id == session_id
        if operation == OP_ABORT:
            assert release_call.reason == "owner-cancelled"

    # No implicit deletion or replacement: the session stays known with its
    # state intact, and the next fresh start allocates the next ID.
    assert backend.known_session_ids() == (session_id,)
    assert backend.read_messages(session_id) == _raw_messages()
    assert backend.start(_start_request()).session_id == "cline-session-2"


def test_event_forwarding_filtering_and_unsubscribe_are_deterministic() -> None:
    backend = FakeClineSdkBackend()
    unfiltered: list[ClineSessionEvent] = []
    filtered: list[ClineSessionEvent] = []
    backend.subscribe(None, unfiltered.append)
    filtered_subscription = backend.subscribe("cline-session-1", filtered.append)

    first = ClineSessionEvent(session_id="cline-session-1", kind="message", payload={"n": 1})
    second = ClineSessionEvent(session_id="cline-session-2", kind="state", payload={"n": 2})
    third = ClineSessionEvent(session_id="cline-session-1", kind="message", payload={"n": 3})
    backend.emit(first)
    backend.emit(second)
    filtered_subscription.unsubscribe()
    backend.emit(third)
    filtered_subscription.unsubscribe()  # idempotent

    # The session-filtered subscription saw only its exact session, before
    # unsubscribing; the unfiltered subscription saw everything, in order:
    assert filtered == [first]
    assert unfiltered == [first, second, third]

    subscribe_calls = [call for call in backend.calls() if call.operation == OP_SUBSCRIBE]
    assert subscribe_calls == [
        FakeBackendCall(OP_SUBSCRIBE, session_id=None),
        FakeBackendCall(OP_SUBSCRIBE, session_id="cline-session-1"),
    ]

    # Opaque payload contents never enter representation:
    assert repr(first) == (
        "ClineSessionEvent(session_id='cline-session-1', kind='message', payload=<withheld>)"
    )


def test_representative_backend_errors_stay_machine_distinguishable() -> None:
    # Known pre-effect failure: remote attachment/configuration rejection.
    rejected = FakeClineSdkBackend()
    rejected.queue_connect(
        ClineRemoteAttachmentRejectedError("remote rejected the presented attachment")
    )
    with pytest.raises(ClineRemoteAttachmentRejectedError):
        rejected.connect(_remote_config())

    # Pre-effect unavailability scripts on start:
    unavailable = FakeClineSdkBackend()
    unavailable.queue_start(
        ClineRemoteAttachmentUnavailableError("remote not attachable before any effect")
    )
    with pytest.raises(ClineRemoteAttachmentUnavailableError):
        unavailable.start(_start_request())

    # Uncertain effect: an in-flight send over a broken bridge.
    uncertain = FakeClineSdkBackend()
    session_id = uncertain.start(_start_request()).session_id
    uncertain.queue_send(
        session_id,
        ClineBackendUncertainOutcomeError("bridge failed mid-send; effect unknown"),
    )
    with pytest.raises(ClineBackendUncertainOutcomeError):
        uncertain.send(session_id, "composed prompt")

    # Positively identified exact-session absence needs no script: this
    # instance positively does not know the addressed ID.
    missing = FakeClineSdkBackend()
    with pytest.raises(ClineSessionNotFoundError):
        missing.send("cline-session-404", "composed prompt")
    with pytest.raises(ClineSessionNotFoundError):
        missing.get("cline-session-404")

    # The classifications are distinct types under the one backend base:
    for error_type in (
        ClineRemoteAttachmentRejectedError,
        ClineRemoteAttachmentUnavailableError,
        ClineBackendUncertainOutcomeError,
        ClineSessionNotFoundError,
        ClineSdkOperationRejectedError,
    ):
        assert issubclass(error_type, ClineSdkBackendError)


def test_backend_errors_never_leak_a_sensitive_sentinel_through_text_or_chains() -> None:
    sentinel = "SENSITIVE-NATIVE-SENTINEL"

    def raise_after_handler_exit(error: ClineSdkBackendError) -> ClineSdkBackendError:
        """Sanctioned conversion path: the native failure is discarded in the
        handler and the backend error is raised after the handler exits, so
        no cause/context chain can reach the native exception."""
        try:
            try:
                raise RuntimeError(f"native failure containing {sentinel}")
            except RuntimeError:
                pass
            raise error
        except ClineSdkBackendError as caught:
            return caught

    known = raise_after_handler_exit(
        ClineRemoteAttachmentRejectedError("remote rejected the presented attachment")
    )
    uncertain = raise_after_handler_exit(
        ClineBackendUncertainOutcomeError("bridge failed mid-send; effect unknown")
    )
    missing = raise_after_handler_exit(ClineSessionNotFoundError("session not found"))
    rejected_code = raise_after_handler_exit(
        ClineSdkOperationRejectedError("hub_connect_failed", details={"note": sentinel})
    )

    for error in (known, uncertain, missing, rejected_code):
        assert sentinel not in str(error)
        assert sentinel not in repr(error)
        assert error.__cause__ is None
        assert error.__context__ is None

    # The machine code stays available for downstream classification while
    # the bounded details never enter text or representation:
    assert isinstance(rejected_code, ClineSdkOperationRejectedError)
    assert rejected_code.code == "hub_connect_failed"
    assert rejected_code.details == {"note": sentinel}
    assert str(rejected_code) == (
        "cline sdk operation rejected with machine code 'hub_connect_failed'"
    )
    assert repr(rejected_code) == ("ClineSdkOperationRejectedError(code='hub_connect_failed')")


def test_fake_instances_and_scripted_outcomes_are_isolated_and_deterministic() -> None:
    first = FakeClineSdkBackend()
    second = FakeClineSdkBackend()
    raw = _raw_messages()

    for backend in (first, second):
        session_id = backend.start(_start_request(initial_messages=raw)).session_id
        backend.queue_send(session_id, {"echo": "scripted-once"})
        assert backend.send(session_id, "same prompt") == {"echo": "scripted-once"}
        # FIFO scripts are consumed exactly once; the next send falls back to
        # the legitimate unscripted None:
        assert backend.send(session_id, "same prompt") is None
        assert backend.stored_messages(session_id) == raw
        assert [call.operation for call in backend.calls()] == [
            OP_START,
            OP_SEND,
            OP_SEND,
        ]

    # Separate instances share no mutable state: identical scripting yields
    # identical deterministic outcomes over independent recordings.
    assert first.known_session_ids() == ("cline-session-1",)
    assert second.known_session_ids() == ("cline-session-1",)
    assert first.calls() == second.calls()

    # Scripted start outcomes keep their scripted exact ID, and explicit
    # None read results are scripted outcomes too:
    third = FakeClineSdkBackend()
    third.queue_start(ClineStartResult(session_id="scripted-opaque-id"))
    assert third.start(_start_request()).session_id == "scripted-opaque-id"
    assert third.known_session_ids() == ("scripted-opaque-id",)
    third.queue_get("scripted-opaque-id", None)
    assert third.get("scripted-opaque-id") is None
    third.queue_read_messages("scripted-opaque-id", [])
    assert third.read_messages("scripted-opaque-id") == []
