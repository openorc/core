"""Deterministic adapter tests for the official Cline Agent Runtime adapter
(issue #74, D5a).

These tests exercise the behavior the Cline adapter owns over the #71
``ClineSdkBackend`` fake: fresh construction in the qualified native modes
with the canonical readiness handshake, verbatim exact-session interactions
through the shared #65 funnel, the #181 workspace controls and the native
IMPLEMENT realization, the serialized same-ID reconstruction seam (including
cross-instance continuity over the resolver-established durable binding),
and the normalized backend-failure classification with whole-chain secret
safety. They reuse the already-covered #65/#66/#181 protocol behavior
instead of re-testing it and require no database, network, or live runtime.
"""

from __future__ import annotations

import json
from pathlib import Path
from uuid import UUID

import pytest
from tests.fakes.cline_sdk_backend import FakeClineSdkBackend

import openorc.protocol
from openorc.adapters.agent_runtime.errors import (
    AgentRuntimeConfigurationRejectedError,
    AgentRuntimeDeliveryUncertainError,
    AgentRuntimeError,
    AgentRuntimeProtocolFailureError,
    AgentRuntimeTransportError,
    AgentRuntimeUnavailableError,
    AgentRuntimeUncertainOutcomeError,
    AgentSessionNotFoundError,
)
from openorc.adapters.agent_runtime.results import AgentSessionCreationRequest
from openorc.adapters.cline import (
    ClineAdapter,
    ClineConstructionContext,
    ClineConstructionContextResolver,
    ClineConstructionMode,
    ClineSessionIdentity,
)
from openorc.adapters.cline.errors import (
    ClineBackendUncertainOutcomeError,
    ClineBridgeProtocolError,
    ClineRemoteAttachmentRejectedError,
    ClineRemoteAttachmentUnavailableError,
    ClineSdkOperationRejectedError,
    ClineSessionNotFoundError,
)
from openorc.adapters.cline.values import ClineStartResult
from openorc.domain.connections import WorkflowRole
from openorc.protocol.interaction import (
    CleanupWorkspaceInteraction,
    ImplementInteraction,
    PrepareWorkspaceInteraction,
)
from openorc.protocol.models import PLAN_RESULT_FAMILY, WORKSPACE_RESULT_FAMILY

# --- shared test constants and helpers ---

_SECRET_INITIALIZATION_MARKER = "SECRET-INIT-MARKER"
_SECRET_RESPONSE_MARKER = "SECRET-RESPONSE-MARKER"

_WORKSPACE_ID = UUID("00000000-0000-0000-0000-0000000000a1")
_TASK_ID = UUID("00000000-0000-0000-0000-0000000000a2")
_OTHER_TASK_ID = UUID("00000000-0000-0000-0000-0000000000a3")
_CONNECTION_ID = UUID("00000000-0000-0000-0000-0000000000a4")

_REPOSITORY_BASE = "/srv/openorc/repositories"
_TASK_CHECKOUT = f"{_REPOSITORY_BASE}/{_TASK_ID}"
_NEUTRAL_DIRECTORY = "/srv/openorc/neutral"

_READY_PAYLOAD = json.dumps({"type": "session_ready", "schema_version": 1, "status": "READY"})
_INITIALIZATION = "You are the Producer role in an OpenOrc Task session."

_RAW_MESSAGES = [
    {"id": "m1", "role": "user", "content": [{"type": "text", "text": _INITIALIZATION}], "ts": 1},
    {
        "id": "m2",
        "role": "assistant",
        "content": [{"type": "text", "text": _READY_PAYLOAD}],
        "ts": 2,
        "metadata": {"nested": {"keep": True}},
    },
]

_PACKAGE_DIR = Path(openorc.protocol.__file__).resolve().parent


def _packaged_role_prompt(role: str) -> str:
    return (_PACKAGE_DIR / "role_prompts" / f"{role}.md").read_text(encoding="utf-8")


def _completed(text: str) -> dict[str, object]:
    """The pinned SDK-native successful AgentResult shape (R2b)."""
    return {"finishReason": "completed", "text": text}


def _failed(text: str, finish_reason: str = "error") -> dict[str, object]:
    return {"finishReason": finish_reason, "text": text, "error": {"message": text}}


def _identity(
    role: WorkflowRole = WorkflowRole.PRODUCER,
    *,
    workspace_id: UUID = _WORKSPACE_ID,
    task_id: UUID = _TASK_ID,
    connection_id: UUID = _CONNECTION_ID,
) -> ClineSessionIdentity:
    return ClineSessionIdentity(
        workspace_id=workspace_id,
        task_id=task_id,
        role=role,
        connection_id=connection_id,
    )


def _context(
    identity: ClineSessionIdentity | None = None,
    *,
    provider: str | None = "configured-provider",
    model: str | None = "configured-model",
    override: str | None = None,
    cwd: str | None = _REPOSITORY_BASE,
    workspace_root: str | None = _REPOSITORY_BASE,
) -> ClineConstructionContext:
    return ClineConstructionContext(
        identity=identity if identity is not None else _identity(),
        configured_provider=provider,
        configured_model=model,
        role_prompt_override=override,
        cwd=cwd,
        workspace_root=workspace_root,
    )


def _creation_request(
    role: WorkflowRole = WorkflowRole.PRODUCER,
    *,
    initialization: str = _INITIALIZATION,
) -> AgentSessionCreationRequest:
    return AgentSessionCreationRequest(
        workspace_id=_WORKSPACE_ID,
        task_id=_TASK_ID,
        role=role,
        initialization=initialization,
    )


def _workspace_interaction(
    kind: str,
) -> PrepareWorkspaceInteraction | CleanupWorkspaceInteraction:
    model = PrepareWorkspaceInteraction if kind == "prepare" else CleanupWorkspaceInteraction
    return model(  # type: ignore[operator]
        repository_full_name="openorc/core",
        issue_number=74,
        task_uuid=_TASK_ID,
        target_directory=_TASK_CHECKOUT,
    )


def _workspace_result_payload(operation: str, status: str) -> str:
    return json.dumps(
        {
            "type": "workspace_result",
            "schema_version": 1,
            "operation": operation,
            "status": status,
            "summary": f"{operation} reported {status}.",
        }
    )


class ScriptedResolver(ClineConstructionContextResolver):
    """Read-only resolver double: canned creation/bound contexts plus queries."""

    def __init__(
        self,
        *,
        creation: ClineConstructionContext | None = None,
        bound: dict[str, ClineConstructionContext | None] | None = None,
    ) -> None:
        self.creation_queries: list[tuple[UUID, UUID, WorkflowRole]] = []
        self.bound_queries: list[str] = []
        self._creation = creation
        self._bound: dict[str, ClineConstructionContext | None] = dict(bound or {})

    def set_bound(self, session_id: str, context: ClineConstructionContext | None) -> None:
        self._bound[session_id] = context

    def resolve_construction_context(
        self, workspace_id: UUID, task_id: UUID, role: WorkflowRole
    ) -> ClineConstructionContext:
        self.creation_queries.append((workspace_id, task_id, role))
        assert self._creation is not None, "no scripted creation context"
        return self._creation

    def resolve_bound_session_context(self, session_id: str) -> ClineConstructionContext | None:
        self.bound_queries.append(session_id)
        return self._bound.get(session_id)


def _script_ready_session(
    backend: FakeClineSdkBackend,
    *,
    session_id: str = "cline-session-1",
    text: str = _READY_PAYLOAD,
) -> str:
    backend.queue_start(ClineStartResult(session_id=session_id))
    backend.queue_send(session_id, _completed(text))
    return session_id


def _ready_adapter(
    *,
    backend: FakeClineSdkBackend | None = None,
    context: ClineConstructionContext | None = None,
) -> tuple[ClineAdapter, FakeClineSdkBackend, ScriptedResolver, str]:
    backend = backend if backend is not None else FakeClineSdkBackend()
    context = context if context is not None else _context()
    resolver = ScriptedResolver(creation=context)
    session_id = _script_ready_session(backend)
    adapter = ClineAdapter(backend, resolver)
    created = adapter.create_session(_creation_request(role=context.identity.role))
    assert created.external_session_id == session_id
    assert created.reported_provider is None
    assert created.reported_model is None
    assert created.reported_runtime_version is None
    return adapter, backend, resolver, session_id


# --- fresh construction and asset selection ---


def test_fresh_producer_constructs_native_act_at_the_repository_base() -> None:
    backend = FakeClineSdkBackend()
    resolver = ScriptedResolver(creation=_context())
    _script_ready_session(backend)
    adapter = ClineAdapter(backend, resolver)

    created = adapter.create_session(_creation_request())

    assert created.external_session_id == "cline-session-1"
    assert created.reported_provider is None
    assert created.reported_model is None
    assert created.reported_runtime_version is None
    start_calls = [call for call in backend.calls() if call.operation == "start"]
    assert len(start_calls) == 1
    request = start_calls[0].request
    assert request is not None
    assert request.mode is ClineConstructionMode.ACT
    assert request.provider_id == "configured-provider"
    assert request.model_id == "configured-model"
    assert request.cwd == _REPOSITORY_BASE
    assert request.workspace_root == _REPOSITORY_BASE
    assert request.rules == _packaged_role_prompt("producer")
    assert request.system_prompt == ""
    assert request.enable_tools is True
    assert request.interactive is True
    assert request.tool_policies == {
        "*": {"autoApprove": True},
        "ask_question": {"enabled": False},
    }
    assert request.session_id is None
    assert request.initial_messages is None
    sends = [call for call in backend.calls() if call.operation == "send"]
    assert len(sends) == 1
    assert sends[0].prompt == _INITIALIZATION
    assert resolver.creation_queries == [(_WORKSPACE_ID, _TASK_ID, WorkflowRole.PRODUCER)]


def test_reviewer_is_constructed_native_plan_at_a_neutral_directory() -> None:
    backend = FakeClineSdkBackend()
    context = _context(
        _identity(role=WorkflowRole.REVIEWER),
        cwd=_NEUTRAL_DIRECTORY,
        workspace_root=_NEUTRAL_DIRECTORY,
    )
    resolver = ScriptedResolver(creation=context)
    _script_ready_session(backend)
    adapter = ClineAdapter(backend, resolver)

    created = adapter.create_session(
        _creation_request(role=WorkflowRole.REVIEWER, initialization="Reviewer init.")
    )

    assert created.external_session_id == "cline-session-1"
    request = [c for c in backend.calls() if c.operation == "start"][0].request
    assert request is not None
    assert request.mode is ClineConstructionMode.PLAN
    assert request.cwd == _NEUTRAL_DIRECTORY
    assert request.workspace_root == _NEUTRAL_DIRECTORY
    assert request.rules == _packaged_role_prompt("reviewer")


def test_role_prompt_override_passes_verbatim_including_empty() -> None:
    for override in ("Owner verbatim role Markdown.", ""):
        backend = FakeClineSdkBackend()
        resolver = ScriptedResolver(creation=_context(override=override))
        _script_ready_session(backend)
        adapter = ClineAdapter(backend, resolver)

        adapter.create_session(_creation_request())

        request = [c for c in backend.calls() if c.operation == "start"][0].request
        assert request is not None
        assert request.rules == override


def test_missing_provider_model_pair_fails_closed_before_start() -> None:
    backend = FakeClineSdkBackend()
    resolver = ScriptedResolver(creation=_context(provider=None, model=None))
    adapter = ClineAdapter(backend, resolver)

    with pytest.raises(AgentRuntimeConfigurationRejectedError):
        adapter.create_session(_creation_request())

    assert backend.calls() == ()


def test_missing_working_directory_fails_closed_before_start() -> None:
    backend = FakeClineSdkBackend()
    resolver = ScriptedResolver(creation=_context(cwd=None, workspace_root=None))
    adapter = ClineAdapter(backend, resolver)

    with pytest.raises(AgentRuntimeConfigurationRejectedError):
        adapter.create_session(_creation_request())

    assert backend.calls() == ()


def test_identity_mismatch_fails_closed_before_start() -> None:
    backend = FakeClineSdkBackend()
    resolver = ScriptedResolver(creation=_context(_identity(task_id=_OTHER_TASK_ID)))
    adapter = ClineAdapter(backend, resolver)

    with pytest.raises(AgentRuntimeConfigurationRejectedError):
        adapter.create_session(_creation_request())

    assert backend.calls() == ()


# --- readiness handshake and response extraction ---


def test_allocation_alone_is_never_a_ready_session() -> None:
    backend = FakeClineSdkBackend()
    backend.queue_start(ClineStartResult(session_id="cline-session-1"))
    backend.queue_send("cline-session-1", _completed("still working, no formal object yet"))
    adapter = ClineAdapter(backend, ScriptedResolver(creation=_context()))

    with pytest.raises(AgentRuntimeProtocolFailureError):
        adapter.create_session(_creation_request())

    # Exactly one first initialization send happened; no second send exists.
    sends = [call for call in backend.calls() if call.operation == "send"]
    assert len(sends) == 1
    assert sends[0].prompt == _INITIALIZATION


def test_missing_native_result_is_a_transport_failure() -> None:
    backend = FakeClineSdkBackend()
    backend.queue_start(ClineStartResult(session_id="cline-session-1"))
    backend.queue_send("cline-session-1", None)
    adapter = ClineAdapter(backend, ScriptedResolver(creation=_context()))

    with pytest.raises(AgentRuntimeTransportError):
        adapter.create_session(_creation_request())


def test_result_without_completion_envelope_is_a_transport_failure() -> None:
    backend = FakeClineSdkBackend()
    backend.queue_start(ClineStartResult(session_id="cline-session-1"))
    backend.queue_send("cline-session-1", {"unexpected": "shape"})
    adapter = ClineAdapter(backend, ScriptedResolver(creation=_context()))

    with pytest.raises(AgentRuntimeTransportError):
        adapter.create_session(_creation_request())


def test_completed_result_without_reply_text_is_a_transport_failure() -> None:
    backend = FakeClineSdkBackend()
    backend.queue_start(ClineStartResult(session_id="cline-session-1"))
    backend.queue_send("cline-session-1", {"finishReason": "completed"})
    adapter = ClineAdapter(backend, ScriptedResolver(creation=_context()))

    with pytest.raises(AgentRuntimeTransportError):
        adapter.create_session(_creation_request())


def test_first_turn_non_success_finish_cannot_report_ready() -> None:
    backend = FakeClineSdkBackend()
    backend.queue_start(ClineStartResult(session_id="cline-session-1"))
    backend.queue_send("cline-session-1", _failed("Unknown or disabled provider."))
    adapter = ClineAdapter(backend, ScriptedResolver(creation=_context()))

    with pytest.raises(AgentRuntimeConfigurationRejectedError):
        adapter.create_session(_creation_request())


def test_uncertain_readiness_send_is_delivery_uncertain() -> None:
    backend = FakeClineSdkBackend()
    backend.queue_start(ClineStartResult(session_id="cline-session-1"))
    backend.queue_send("cline-session-1", ClineBackendUncertainOutcomeError())
    adapter = ClineAdapter(backend, ScriptedResolver(creation=_context()))

    with pytest.raises(AgentRuntimeDeliveryUncertainError):
        adapter.create_session(_creation_request())


# --- exact-session interactions ---


def test_formal_send_returns_the_typed_result_and_sends_verbatim() -> None:
    adapter, backend, _, session_id = _ready_adapter()
    plan_payload = json.dumps(
        {"type": "plan_result", "schema_version": 1, "plan": "Plan the flow."}
    )
    backend.queue_send(session_id, _completed(plan_payload))

    result = adapter.send(session_id, "Compose the plan.", expected_family=PLAN_RESULT_FAMILY)

    from openorc.protocol.models import PlanResult

    assert isinstance(result, PlanResult)
    assert result.plan == "Plan the flow."
    sends = [call for call in backend.calls() if call.operation == "send"]
    assert sends[-1].session_id == session_id
    assert sends[-1].prompt == "Compose the plan."


def test_nonformal_send_returns_plain_text_without_parsing() -> None:
    adapter, backend, _, session_id = _ready_adapter()
    reply = 'ordinary prose resembling {"type": "session_ready"} JSON'
    backend.queue_send(session_id, _completed(reply))

    result = adapter.send(session_id, "Advisory question.")

    from openorc.adapters.agent_runtime.results import AgentTextResponse

    assert isinstance(result, AgentTextResponse)
    assert result.text == reply


def test_send_to_a_lost_exact_session_is_session_not_found() -> None:
    adapter, backend, _, session_id = _ready_adapter()
    backend.queue_send(session_id, ClineSessionNotFoundError("session gone"))

    with pytest.raises(AgentSessionNotFoundError):
        adapter.send(session_id, "Hello?")


# --- workspace lifecycle controls (#181) ---


def test_prepare_workspace_uses_the_shared_prose_path_without_auto_reconstruction() -> None:
    adapter, backend, resolver, session_id = _ready_adapter()
    resolver.set_bound(session_id, _context())
    backend.queue_send(
        session_id,
        _completed(_workspace_result_payload("PREPARE", "COMPLETED")),
    )

    result = adapter.realize_control(
        session_id,
        _workspace_interaction("prepare"),
        message="PREPARE_WORKSPACE prose rendered by the caller.",
        expected_family=WORKSPACE_RESULT_FAMILY,
    )

    from openorc.protocol.models import WorkspaceResult

    assert isinstance(result, WorkspaceResult)
    assert result.operation == "PREPARE"
    assert result.status == "COMPLETED"
    operations = [call.operation for call in backend.calls()]
    # Only the prose send happened: no read/stop/start reconstruction and no
    # second initialization send was triggered by the reported result.
    assert operations == ["start", "send", "send"]


def test_cleanup_workspace_failed_result_is_a_typed_result_not_an_error() -> None:
    adapter, backend, resolver, session_id = _ready_adapter()
    resolver.set_bound(session_id, _context())
    backend.queue_send(
        session_id,
        _completed(_workspace_result_payload("CLEANUP", "FAILED")),
    )

    result = adapter.realize_control(
        session_id,
        _workspace_interaction("cleanup"),
        message="CLEANUP_WORKSPACE prose rendered by the caller.",
        expected_family=WORKSPACE_RESULT_FAMILY,
    )

    from openorc.protocol.models import WorkspaceResult

    assert isinstance(result, WorkspaceResult)
    assert result.operation == "CLEANUP"
    assert result.status == "FAILED"


def test_workspace_control_on_a_reviewer_session_is_rejected_before_effects() -> None:
    adapter, backend, resolver, session_id = _ready_adapter(
        context=_context(
            _identity(role=WorkflowRole.REVIEWER),
            cwd=_NEUTRAL_DIRECTORY,
            workspace_root=_NEUTRAL_DIRECTORY,
        )
    )
    resolver.set_bound(
        session_id,
        _context(
            _identity(role=WorkflowRole.REVIEWER),
            cwd=_NEUTRAL_DIRECTORY,
            workspace_root=_NEUTRAL_DIRECTORY,
        ),
    )

    with pytest.raises(ValueError, match="Producer-only"):
        adapter.realize_control(
            session_id,
            _workspace_interaction("prepare"),
            message="caller prose",
            expected_family=WORKSPACE_RESULT_FAMILY,
        )

    assert [call.operation for call in backend.calls()] == ["start", "send"]


def test_workspace_control_with_unestablishable_context_fails_closed() -> None:
    adapter, backend, _, _ = _ready_adapter()
    # The addressed session was established by another instance: this
    # instance holds no memory and the resolver cannot establish the durable
    # context, so the control fails closed before any runtime effect.
    session_id = "cline-session-foreign"
    with pytest.raises(AgentRuntimeConfigurationRejectedError):
        adapter.realize_control(
            session_id,
            _workspace_interaction("prepare"),
            message="caller prose",
            expected_family=WORKSPACE_RESULT_FAMILY,
        )

    assert [call.operation for call in backend.calls()] == ["start", "send"]


# --- IMPLEMENT: authorized native PLAN->ACT realization ---


def _bind_checkout(resolver: ScriptedResolver, session_id: str) -> None:
    resolver.set_bound(
        session_id,
        _context(cwd=_TASK_CHECKOUT, workspace_root=_TASK_CHECKOUT),
    )


def test_implement_realizes_authorized_plan_to_act_at_the_task_checkout() -> None:
    adapter, backend, resolver, session_id = _ready_adapter()
    _bind_checkout(resolver, session_id)
    backend.queue_read_messages(session_id, list(_RAW_MESSAGES))

    result = adapter.realize_control(session_id, ImplementInteraction())

    assert result is None
    operations = [call.operation for call in backend.calls()]
    assert operations == ["start", "send", "read_messages", "stop", "start"]
    rebuild = backend.calls()[-1].request
    assert rebuild is not None
    assert rebuild.mode is ClineConstructionMode.ACT
    assert rebuild.session_id == session_id
    assert rebuild.cwd == _TASK_CHECKOUT
    assert rebuild.workspace_root == _TASK_CHECKOUT
    assert rebuild.initial_messages == _RAW_MESSAGES
    # No implementation prompt and no second initialization was ever sent.
    assert [call.operation for call in backend.calls()].count("send") == 1


def test_implement_on_a_reviewer_session_is_rejected_before_effects() -> None:
    adapter, backend, resolver, session_id = _ready_adapter(
        context=_context(
            _identity(role=WorkflowRole.REVIEWER),
            cwd=_NEUTRAL_DIRECTORY,
            workspace_root=_NEUTRAL_DIRECTORY,
        )
    )
    resolver.set_bound(
        session_id,
        _context(
            _identity(role=WorkflowRole.REVIEWER),
            cwd=_NEUTRAL_DIRECTORY,
            workspace_root=_NEUTRAL_DIRECTORY,
        ),
    )

    with pytest.raises(ValueError, match="Producer-only"):
        adapter.realize_control(session_id, ImplementInteraction())

    assert [call.operation for call in backend.calls()] == ["start", "send"]


# --- same-ID reconstruction seam ---


def _reconstruction_setup() -> tuple[ClineAdapter, FakeClineSdkBackend, ScriptedResolver, str]:
    adapter, backend, resolver, session_id = _ready_adapter()
    _bind_checkout(resolver, session_id)
    backend.queue_read_messages(session_id, list(_RAW_MESSAGES))
    return adapter, backend, resolver, session_id


def test_reconstruction_sequence_preserves_the_exact_id_and_raw_messages() -> None:
    adapter, backend, _, session_id = _reconstruction_setup()

    adapter.reconstruct_session(session_id, target_mode=ClineConstructionMode.PLAN)

    operations = [call.operation for call in backend.calls()]
    assert operations[-3:] == ["read_messages", "stop", "start"]
    rebuild = backend.calls()[-1].request
    assert rebuild is not None
    assert rebuild.session_id == session_id
    assert rebuild.mode is ClineConstructionMode.PLAN
    assert rebuild.provider_id == "configured-provider"
    assert rebuild.model_id == "configured-model"
    assert rebuild.rules == _packaged_role_prompt("producer")
    assert rebuild.system_prompt == ""
    assert rebuild.cwd == _TASK_CHECKOUT
    assert rebuild.workspace_root == _TASK_CHECKOUT
    assert rebuild.enable_tools is True
    assert rebuild.interactive is True
    assert rebuild.tool_policies == {
        "*": {"autoApprove": True},
        "ask_question": {"enabled": False},
    }
    # The raw nested persisted messages round-trip unmodified: ordering,
    # nested objects, and metadata intact. No initialization resend.
    assert rebuild.initial_messages == _RAW_MESSAGES
    assert [call.operation for call in backend.calls()].count("send") == 1


def test_the_three_authorized_transitions_reconstruct_the_same_id() -> None:
    adapter, backend, resolver, session_id = _reconstruction_setup()
    base_context = _context()
    checkout_context = _context(cwd=_TASK_CHECKOUT, workspace_root=_TASK_CHECKOUT)

    # Bootstrap ACT/base -> PLAN/Task checkout (planning).
    resolver.set_bound(session_id, checkout_context)
    backend.queue_read_messages(session_id, list(_RAW_MESSAGES))
    adapter.reconstruct_session(session_id, target_mode=ClineConstructionMode.PLAN)

    # IMPLEMENT PLAN/checkout -> ACT/same checkout.
    backend.queue_read_messages(session_id, list(_RAW_MESSAGES))
    adapter.reconstruct_session(session_id, target_mode=ClineConstructionMode.ACT)

    # Authorized cleanup ACT/checkout -> ACT/base (outside the checkout).
    resolver.set_bound(session_id, base_context)
    backend.queue_read_messages(session_id, list(_RAW_MESSAGES))
    adapter.reconstruct_session(session_id, target_mode=ClineConstructionMode.ACT)

    rebuilds = [
        call.request
        for call in backend.calls()
        if call.operation == "start" and call.request is not None and call.request.session_id
    ]
    assert [rebuild.mode for rebuild in rebuilds] == [
        ClineConstructionMode.PLAN,
        ClineConstructionMode.ACT,
        ClineConstructionMode.ACT,
    ]
    assert [rebuild.cwd for rebuild in rebuilds] == [
        _TASK_CHECKOUT,
        _TASK_CHECKOUT,
        _REPOSITORY_BASE,
    ]
    assert all(rebuild.session_id == session_id for rebuild in rebuilds)


def test_repeat_reconstruction_reapplies_the_same_construction() -> None:
    adapter, backend, _, session_id = _reconstruction_setup()

    adapter.reconstruct_session(session_id, target_mode=ClineConstructionMode.ACT)
    backend.queue_read_messages(session_id, list(_RAW_MESSAGES))
    adapter.reconstruct_session(session_id, target_mode=ClineConstructionMode.ACT)

    starts = [call for call in backend.calls() if call.operation == "start"]
    assert len(starts) == 3  # fresh + two same-ID reapplications


def test_reconstruction_fails_closed_on_a_wrong_returned_session_id() -> None:
    adapter, backend, _, session_id = _reconstruction_setup()
    backend.queue_start(ClineStartResult(session_id="cline-session-other"))

    with pytest.raises(AgentRuntimeConfigurationRejectedError):
        adapter.reconstruct_session(session_id, target_mode=ClineConstructionMode.PLAN)

    operations = [call.operation for call in backend.calls()]
    assert operations[-3:] == ["read_messages", "stop", "start"]


def test_reconstruction_stop_failure_never_reaches_start() -> None:
    adapter, backend, _, session_id = _reconstruction_setup()
    backend.queue_stop(session_id, ClineBackendUncertainOutcomeError())

    with pytest.raises(AgentRuntimeDeliveryUncertainError):
        adapter.reconstruct_session(session_id, target_mode=ClineConstructionMode.PLAN)

    assert "start" not in [call.operation for call in backend.calls()[2:]]


def test_reconstruction_refuses_after_an_uncertain_send() -> None:
    adapter, backend, resolver, session_id = _ready_adapter()
    _bind_checkout(resolver, session_id)
    backend.queue_send(session_id, ClineBackendUncertainOutcomeError())

    with pytest.raises(AgentRuntimeDeliveryUncertainError):
        adapter.send(session_id, "Hello?")

    # Reconstruction is refused without touching the backend at all: an
    # uncertain send is never silently displaced by a stop/rebuild.
    with pytest.raises(AgentRuntimeUncertainOutcomeError):
        adapter.reconstruct_session(session_id, target_mode=ClineConstructionMode.PLAN)

    assert "read_messages" not in [call.operation for call in backend.calls()]
    assert "stop" not in [call.operation for call in backend.calls()]


def test_reconstruction_without_an_establishable_context_fails_closed() -> None:
    adapter, backend, _, session_id = _ready_adapter()

    with pytest.raises(AgentRuntimeConfigurationRejectedError):
        adapter.reconstruct_session(session_id, target_mode=ClineConstructionMode.PLAN)

    assert [call.operation for call in backend.calls()] == ["start", "send"]


def test_fresh_instance_reconstructs_and_uses_an_existing_bound_session() -> None:
    """Durable continuity: a NEW adapter instance (no create_session in that
    instance) reconstructs the existing bound external session from the
    resolver-established durable binding and continues using it."""
    backend = FakeClineSdkBackend()
    resolver_a = ScriptedResolver(creation=_context())
    session_id = _script_ready_session(backend)
    adapter_a = ClineAdapter(backend, resolver_a)
    adapter_a.create_session(_creation_request())

    # A fresh instance over the same connected backend, resolving the durable
    # binding of the exact external session: no initialization is repeated.
    resolver_b = ScriptedResolver()
    resolver_b.set_bound(
        session_id,
        _context(cwd=_TASK_CHECKOUT, workspace_root=_TASK_CHECKOUT),
    )
    adapter_b = ClineAdapter(backend, resolver_b)
    backend.queue_read_messages(session_id, list(_RAW_MESSAGES))

    adapter_b.reconstruct_session(session_id, target_mode=ClineConstructionMode.PLAN)

    rebuild = backend.calls()[-1].request
    assert rebuild is not None
    assert rebuild.session_id == session_id
    assert rebuild.initial_messages == _RAW_MESSAGES

    # The fresh instance continues to operate on the exact bound session:
    # a Producer-only control resolves through the resolver-established
    # binding and a send addresses the exact ID.
    backend.queue_send(
        session_id,
        _completed(_workspace_result_payload("PREPARE", "COMPLETED")),
    )
    result = adapter_b.realize_control(
        session_id,
        _workspace_interaction("prepare"),
        message="caller prose",
        expected_family=WORKSPACE_RESULT_FAMILY,
    )
    assert result is not None
    backend.queue_send(session_id, _completed("ordinary reply"))
    response = adapter_b.send(session_id, "Advisory question.")
    from openorc.adapters.agent_runtime.results import AgentTextResponse

    assert isinstance(response, AgentTextResponse)
    assert response.text == "ordinary reply"


# --- normalized backend-failure classification ---


def test_normalized_error_chains_stay_clean() -> None:
    adapter, backend, _, session_id = _ready_adapter()
    backend.queue_send(session_id, ClineBackendUncertainOutcomeError())

    with pytest.raises(AgentRuntimeUncertainOutcomeError) as excinfo:
        adapter.send(session_id, "Hello?")

    error = excinfo.value
    assert error.__cause__ is None
    assert error.__context__ is None


@pytest.mark.parametrize(
    ("script", "operation", "expected"),
    [
        ("attachment_rejected_start", "start", AgentRuntimeConfigurationRejectedError),
        ("attachment_unavailable_start", "start", AgentRuntimeUnavailableError),
        ("hub_connect_failed_start", "start", AgentRuntimeUnavailableError),
        ("hub_closed_send", "send", AgentRuntimeUncertainOutcomeError),
        ("hub_closed_read", "read_messages", AgentRuntimeUnavailableError),
        ("other_code_start", "start", AgentRuntimeUncertainOutcomeError),
        ("unknown_code_send", "send", AgentRuntimeUncertainOutcomeError),
        ("not_attached_start", "start", AgentRuntimeUnavailableError),
        ("invalid_params_start", "start", AgentRuntimeConfigurationRejectedError),
    ],
)
def test_backend_failures_classify_into_the_normalized_taxonomy(
    script: str, operation: str, expected: type[AgentRuntimeError]
) -> None:
    adapter, backend, resolver, session_id = _ready_adapter()
    _bind_checkout(resolver, session_id)

    if script == "attachment_rejected_start":
        backend.queue_start(ClineRemoteAttachmentRejectedError())
    elif script == "attachment_unavailable_start":
        backend.queue_start(ClineRemoteAttachmentUnavailableError())
    elif script == "hub_connect_failed_start":
        backend.queue_start(ClineSdkOperationRejectedError("hub_connect_failed"))
    elif script == "hub_closed_send":
        backend.queue_send(session_id, ClineSdkOperationRejectedError("hub_connection_closed"))
    elif script == "hub_closed_read":
        backend.queue_read_messages(
            session_id, ClineSdkOperationRejectedError("hub_connection_closed")
        )
    elif script == "other_code_start":
        backend.queue_start(ClineSdkOperationRejectedError("operation_invalid"))
    elif script == "not_attached_start":
        backend.queue_start(ClineBridgeProtocolError("not_attached"))
    elif script == "invalid_params_start":
        backend.queue_start(ClineBridgeProtocolError("invalid_params"))
    elif script == "unknown_code_send":
        backend.queue_send(session_id, ClineSdkOperationRejectedError("mystery_failure"))

    if operation == "send":
        with pytest.raises(expected):
            adapter.send(session_id, "Hello?")
    elif operation == "start":
        with pytest.raises(expected):
            adapter.reconstruct_session(session_id, target_mode=ClineConstructionMode.ACT)
    else:
        with pytest.raises(expected):
            adapter.reconstruct_session(session_id, target_mode=ClineConstructionMode.ACT)


def test_secret_material_never_enters_normalized_errors_or_chains() -> None:
    backend = FakeClineSdkBackend()
    resolver = ScriptedResolver(creation=_context())
    session_id = "cline-session-1"
    backend.queue_start(ClineStartResult(session_id=session_id))
    backend.queue_send(
        session_id,
        _completed(f"junk reply {_SECRET_RESPONSE_MARKER}"),
    )
    adapter = ClineAdapter(backend, resolver)
    initialization = f"init prose {_SECRET_INITIALIZATION_MARKER}"

    with pytest.raises(AgentRuntimeProtocolFailureError) as excinfo:
        adapter.create_session(_creation_request(initialization=initialization))

    error = excinfo.value
    surfaces = [str(error), repr(error)]
    cause = error.__cause__
    while cause is not None:
        surfaces.extend([str(cause), repr(cause)])
        cause = getattr(cause, "__cause__", None)
    for surface in surfaces:
        assert _SECRET_INITIALIZATION_MARKER not in surface
        assert _SECRET_RESPONSE_MARKER not in surface


# --- review remediations: authority, native construction invariants, and
# --- conservative unknown-code classification ---


def test_established_session_losing_its_resolver_binding_fails_closed() -> None:
    """The in-memory record of a session this instance established is never a
    substitute for the authoritative durable binding: when the resolver can
    no longer establish the binding, a Producer-only control fails closed
    before any runtime effect without claiming session loss."""
    adapter, backend, resolver, session_id = _ready_adapter()
    resolver.set_bound(session_id, None)

    with pytest.raises(AgentRuntimeConfigurationRejectedError):
        adapter.realize_control(
            session_id,
            _workspace_interaction("prepare"),
            message="caller prose",
            expected_family=WORKSPACE_RESULT_FAMILY,
        )

    assert [call.operation for call in backend.calls()] == ["start", "send"]


def test_reconstruction_into_reviewer_act_is_refused_before_any_effect() -> None:
    """The Reviewer remains native PLAN for the Task lifetime: a forbidden
    ACT reconstruction is refused before any backend effect."""
    reviewer_context = _context(
        _identity(role=WorkflowRole.REVIEWER),
        cwd=_NEUTRAL_DIRECTORY,
        workspace_root=_NEUTRAL_DIRECTORY,
    )
    adapter, backend, resolver, session_id = _ready_adapter(context=reviewer_context)
    resolver.set_bound(session_id, reviewer_context)

    with pytest.raises(ValueError, match="native PLAN"):
        adapter.reconstruct_session(session_id, target_mode=ClineConstructionMode.ACT)

    assert [call.operation for call in backend.calls()] == ["start", "send"]


def test_mismatched_working_roots_fail_closed_before_start_and_reconstruction() -> None:
    backend = FakeClineSdkBackend()
    resolver = ScriptedResolver(
        creation=_context(cwd=_REPOSITORY_BASE, workspace_root="/srv/openorc/other")
    )
    adapter = ClineAdapter(backend, resolver)

    with pytest.raises(AgentRuntimeConfigurationRejectedError):
        adapter.create_session(_creation_request())
    assert backend.calls() == ()

    ready_adapter, ready_backend, ready_resolver, session_id = _ready_adapter()
    ready_resolver.set_bound(
        session_id, _context(cwd=_TASK_CHECKOUT, workspace_root="/srv/openorc/other")
    )

    with pytest.raises(AgentRuntimeConfigurationRejectedError):
        ready_adapter.reconstruct_session(session_id, target_mode=ClineConstructionMode.PLAN)

    assert [call.operation for call in ready_backend.calls()] == ["start", "send"]


def test_unknown_code_send_failure_remains_uncertain_and_blocks_reconstruction() -> None:
    """An SDK code the pinned bridge forwards without establishing effect
    gives no positive evidence of non-effect for a send: the outcome stays
    conservatively uncertain and same-ID reconstruction cannot follow."""
    adapter, backend, resolver, session_id = _ready_adapter()
    _bind_checkout(resolver, session_id)
    backend.queue_send(session_id, ClineSdkOperationRejectedError("mystery_failure"))

    with pytest.raises(AgentRuntimeUncertainOutcomeError):
        adapter.send(session_id, "Hello?")

    with pytest.raises(AgentRuntimeUncertainOutcomeError):
        adapter.reconstruct_session(session_id, target_mode=ClineConstructionMode.PLAN)

    operations = [call.operation for call in backend.calls()]
    assert "read_messages" not in operations
    assert "stop" not in operations
