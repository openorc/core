"""Deterministic adapter tests for the official Cline Agent Runtime adapter
(issue #74, D5a).

These tests exercise the behavior the Cline adapter owns over the #71
``ClineSdkBackend`` fake: fresh construction in the qualified native modes
with the canonical readiness handshake, verbatim exact-session interactions
through the shared #65 funnel, the #181 workspace controls and the native
IMPLEMENT realization, the serialized same-ID reconstruction seam (including
cross-instance continuity over the resolver-established durable binding),
the normalized backend-failure classification with whole-chain secret
safety, and the #148 D6b interruption-reconciliation and recovery seam —
public-state reconciliation with conservative continuity classification,
caller-attested per-kind guard settlement, and authorized same-ID recovery
over an already-established attachment — and the #164 D6c activity/usage
observation seam: safe typed event projection, exact-binding observation
subscriptions, and the aggregated-usage snapshot read. They reuse the
already-covered #65/#66/#181 protocol behavior instead of re-testing it
and require no database, network, or live runtime.
"""

from __future__ import annotations

import json
import threading
from collections.abc import Callable
from dataclasses import replace
from pathlib import Path
from uuid import UUID

import pytest

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
    ClineDispatchCheckpoint,
    ClineReviewSubjectUnverifiedError,
    ClineSessionContinuity,
    ClineSessionIdentity,
    ClineSettlementOutcome,
    ClineSettlementRefusal,
)
from openorc.adapters.cline.activity import (
    ClineAccumulatedUsage,
    ClineActivityKind,
    ClineActivityObservation,
    ClineCommentaryActivity,
    ClineFinishActivity,
    ClineIterationActivity,
    ClineNoticeActivity,
    ClineNoticeCategory,
    ClineRuntimeEndedActivity,
    ClineRuntimeErrorActivity,
    ClineRuntimeStatusActivity,
    ClineToolActivity,
    ClineToolCategory,
    ClineTurnFinishReason,
    ClineUsageActivity,
    project_session_event,
    project_usage_summary,
)
from openorc.adapters.cline.errors import (
    ClineBackendUncertainOutcomeError,
    ClineBridgeProtocolError,
    ClineRemoteAttachmentRejectedError,
    ClineRemoteAttachmentUnavailableError,
    ClineSdkOperationRejectedError,
    ClineSessionNotFoundError,
)
from openorc.adapters.cline.reconciliation import (
    ClineSessionEvidence,
    classify_session_evidence,
)
from openorc.adapters.cline.values import ClineSessionEvent, ClineStartResult
from openorc.domain.connections import WorkflowRole
from openorc.protocol.interaction import (
    CleanupWorkspaceInteraction,
    ImplementInteraction,
    PlanReviewSubject,
    PrepareWorkspaceInteraction,
    PrReviewSubject,
    ReviewInteraction,
)
from openorc.protocol.models import (
    PLAN_RESULT_FAMILY,
    REVIEW_RESULT_FAMILY,
    WORKSPACE_RESULT_FAMILY,
)
from tests.fakes.cline_sdk_backend import FakeClineSdkBackend

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
        ("attachment_rejected_abort", "abort", AgentRuntimeConfigurationRejectedError),
        ("attachment_unavailable_abort", "abort", AgentRuntimeUnavailableError),
        ("hub_connect_failed_abort", "abort", AgentRuntimeUnavailableError),
        ("hub_closed_abort", "abort", AgentRuntimeUncertainOutcomeError),
        ("not_attached_abort", "abort", AgentRuntimeUnavailableError),
        ("unknown_code_abort", "abort", AgentRuntimeUncertainOutcomeError),
        ("session_not_found_abort", "abort", AgentSessionNotFoundError),
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
    elif script == "attachment_rejected_abort":
        backend.queue_abort(session_id, ClineRemoteAttachmentRejectedError())
    elif script == "attachment_unavailable_abort":
        backend.queue_abort(session_id, ClineRemoteAttachmentUnavailableError())
    elif script == "hub_connect_failed_abort":
        backend.queue_abort(session_id, ClineSdkOperationRejectedError("hub_connect_failed"))
    elif script == "hub_closed_abort":
        backend.queue_abort(session_id, ClineSdkOperationRejectedError("hub_connection_closed"))
    elif script == "not_attached_abort":
        backend.queue_abort(session_id, ClineBridgeProtocolError("not_attached"))
    elif script == "unknown_code_abort":
        backend.queue_abort(session_id, ClineSdkOperationRejectedError("mystery_failure"))
    elif script == "session_not_found_abort":
        backend.queue_abort(session_id, ClineSessionNotFoundError("gone"))

    if operation == "send":
        with pytest.raises(expected):
            adapter.send(session_id, "Hello?")
    elif operation == "abort":
        with pytest.raises(expected):
            adapter.abort_activity(session_id)
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


# --- D5b: PR-review subject verification (issue #163) ---
#
# Representative sanitized tool-block fixtures shaped on the Owner
# qualification of 2026-10-13 (openorc/core#163 prerequisite): the qualified
# native GitHub MCP shapes with synthetic repository/PR/SHA values. Tool
# inputs and results mirror the qualified read_messages message shapes.

_REVIEW_OWNER = "openorc"
_REVIEW_REPO_NAME = "review-fixture"
_REVIEW_REPO = f"{_REVIEW_OWNER}/{_REVIEW_REPO_NAME}"
_REVIEW_PR_NUMBER = 7
_REVIEW_ISSUE_NUMBER = 163
_REVIEW_FILE = "src/feature.py"

_ASSIGNED_HEAD = "a" * 40
_OTHER_HEAD = "b" * 40
_BASE_HEAD = "c" * 40
_BLOB_SHA = "d" * 40

_PR_READ_TOOL = "Github__pull_request_read"
_FILE_READ_TOOL = "Github__get_file_contents"


def _pr_subject(head_sha: str = _ASSIGNED_HEAD) -> PrReviewSubject:
    return PrReviewSubject(
        repository_full_name=_REVIEW_REPO,
        pull_request_number=_REVIEW_PR_NUMBER,
        head_sha=head_sha,
        issue_number=_REVIEW_ISSUE_NUMBER,
    )


def _pr_review_interaction(head_sha: str = _ASSIGNED_HEAD) -> ReviewInteraction:
    return ReviewInteraction(subject=_pr_subject(head_sha))


def _review_message() -> str:
    return (
        f"REVIEW assignment: inspect {_REVIEW_REPO} PR #{_REVIEW_PR_NUMBER} at "
        f"exact head {_ASSIGNED_HEAD} with your native GitHub MCP; read code at "
        "that SHA with commit-addressed get_file_contents reads."
    )


def _review_result_payload() -> str:
    return json.dumps(
        {
            "type": "review_result",
            "schema_version": 1,
            "outcome": "ACCEPTED",
            "summary": "Implementation matches the plan.",
            "findings": [],
        }
    )


def _tool_use(call_id: str, name: str, tool_input: dict[str, object]) -> dict[str, object]:
    return {"type": "tool_use", "id": call_id, "name": name, "input": tool_input}


def _tool_result(
    call_id: str, name: str, content: object, *, is_error: bool = False
) -> dict[str, object]:
    block: dict[str, object] = {
        "type": "tool_result",
        "tool_use_id": call_id,
        "name": name,
        "content": content,
    }
    if is_error:
        block["is_error"] = True
    return block


def _pr_read_pair(
    call_id: str,
    head_sha: str,
    *,
    repo: str = _REVIEW_REPO,
    pr_number: int = _REVIEW_PR_NUMBER,
) -> tuple[dict[str, object], dict[str, object]]:
    owner, _, name = repo.partition("/")
    tool_use = _tool_use(
        call_id,
        _PR_READ_TOOL,
        {"method": "get", "owner": owner, "repo": name, "pullNumber": pr_number},
    )
    pr_payload = {
        "number": pr_number,
        "state": "open",
        "head": {"ref": "feature", "sha": head_sha},
        "base": {"ref": "main", "sha": _BASE_HEAD},
    }
    return tool_use, _tool_result(call_id, _PR_READ_TOOL, json.dumps(pr_payload))


def _file_read_pair(
    call_id: str,
    ref: str,
    *,
    repo: str = _REVIEW_REPO,
    path: str = _REVIEW_FILE,
    with_uri: bool = True,
    is_error: bool = False,
) -> tuple[dict[str, object], dict[str, object]]:
    owner, _, name = repo.partition("/")
    tool_use = _tool_use(
        call_id,
        _FILE_READ_TOOL,
        {"owner": owner, "repo": name, "path": path, "ref": ref, "sha": ""},
    )
    if is_error:
        content: object = {"content": [{"type": "text", "text": "request failed"}]}
    elif with_uri:
        content = {
            "content": [
                {
                    "type": "text",
                    "text": f"successfully downloaded text file (SHA: {_BLOB_SHA})",
                },
                {
                    "type": "resource",
                    "resource": {
                        "uri": f"repo://{owner}/{name}/sha/{ref}/contents/{path}",
                        "mimeType": "text/plain",
                    },
                },
            ]
        }
    else:
        content = {
            "content": [
                {
                    "type": "text",
                    "text": f"successfully downloaded text file (SHA: {_BLOB_SHA})",
                }
            ]
        }
    return tool_use, _tool_result(call_id, _FILE_READ_TOOL, content, is_error=is_error)


def _turn_messages(*pairs: tuple[dict[str, object], dict[str, object]]) -> list[dict[str, object]]:
    """One assistant tool-use message plus the paired user tool-result message."""
    messages: list[dict[str, object]] = []
    uses = [pair[0] for pair in pairs]
    results = [pair[1] for pair in pairs]
    if uses:
        messages.append({"role": "assistant", "content": uses, "ts": len(messages) + 10})
    if results:
        messages.append({"role": "user", "content": results, "ts": len(messages) + 10})
    return messages


def _text_message(text: str) -> dict[str, object]:
    return {"role": "assistant", "content": [{"type": "text", "text": text}], "ts": 99}


def _ready_reviewer_adapter() -> tuple[ClineAdapter, FakeClineSdkBackend, ScriptedResolver, str]:
    context = _context(
        _identity(role=WorkflowRole.REVIEWER),
        cwd=_NEUTRAL_DIRECTORY,
        workspace_root=_NEUTRAL_DIRECTORY,
    )
    return _ready_adapter(context=context)


def _queue_verified_review_turn(
    backend: FakeClineSdkBackend, session_id: str, suffix: list[dict[str, object]]
) -> None:
    """Script one fully verified review turn: prior transcript, send, suffix."""
    backend.queue_read_messages(session_id, list(_RAW_MESSAGES))
    backend.queue_send(session_id, _completed(_review_result_payload()))
    backend.queue_read_messages(session_id, list(_RAW_MESSAGES) + suffix)

    backend.queue_read_messages(session_id, list(_RAW_MESSAGES))
    backend.queue_send(session_id, _completed(_review_result_payload()))
    backend.queue_read_messages(session_id, list(_RAW_MESSAGES) + suffix)


def test_verified_pr_review_returns_the_typed_result_without_new_sessions() -> None:
    """The exact assigned PR/head with a successful SHA-addressed read releases
    the existing typed ReviewResult: one snapshot, one send, one post-read,
    no new session and no reconstruction."""
    from openorc.protocol.models import ReviewResult

    adapter, backend, resolver, session_id = _ready_reviewer_adapter()
    _queue_verified_review_turn(
        backend,
        session_id,
        _turn_messages(
            _pr_read_pair("call-1", _ASSIGNED_HEAD),
            _file_read_pair("call-2", _ASSIGNED_HEAD),
        ),
    )

    result = adapter.realize_control(
        session_id,
        _pr_review_interaction(),
        message=_review_message(),
        expected_family=REVIEW_RESULT_FAMILY,
    )

    assert isinstance(result, ReviewResult)
    assert result.outcome == "ACCEPTED"
    operations = [call.operation for call in backend.calls()]
    assert operations == ["start", "send", "read_messages", "send", "read_messages"]
    sends = [call for call in backend.calls() if call.operation == "send"]
    assert sends[-1].prompt == _review_message()
    assert sends[-1].session_id == session_id


def test_verified_pr_review_tolerates_corrected_stale_observations_in_one_turn() -> None:
    """Earlier stale metadata or wrong-ref reads followed by matching successful
    reads in the same turn verify the assignment (qualified T3 pattern)."""
    from openorc.protocol.models import ReviewResult

    adapter, backend, resolver, session_id = _ready_reviewer_adapter()
    _queue_verified_review_turn(
        backend,
        session_id,
        _turn_messages(
            _pr_read_pair("call-1", _OTHER_HEAD),  # stale head read, corrected below
            _pr_read_pair("call-2", _ASSIGNED_HEAD),
            _file_read_pair("call-3", _OTHER_HEAD),  # wrong-ref read, corrected below
            _file_read_pair("call-4", _ASSIGNED_HEAD),
        ),
    )

    result = adapter.realize_control(
        session_id,
        _pr_review_interaction(),
        message=_review_message(),
        expected_family=REVIEW_RESULT_FAMILY,
    )

    assert isinstance(result, ReviewResult)


def test_prior_turn_evidence_cannot_verify_a_new_review_assignment() -> None:
    """A matching earlier review revision's tool evidence lives only in the
    pre-send snapshot: a new assignment with no current-turn tool evidence —
    model claims included — fails closed."""
    adapter, backend, resolver, session_id = _ready_reviewer_adapter()
    prior_turn = _turn_messages(
        _pr_read_pair("call-1", _ASSIGNED_HEAD),
        _file_read_pair("call-2", _ASSIGNED_HEAD),
    )
    backend.queue_read_messages(session_id, list(_RAW_MESSAGES) + prior_turn)
    backend.queue_send(session_id, _completed(_review_result_payload()))
    backend.queue_read_messages(
        session_id,
        list(_RAW_MESSAGES)
        + prior_turn
        + [_text_message(f"OBSERVED_HEAD={_ASSIGNED_HEAD} COMPARISON=MATCH")],
    )

    with pytest.raises(ClineReviewSubjectUnverifiedError):
        adapter.realize_control(
            session_id,
            _pr_review_interaction(),
            message=_review_message(),
            expected_family=REVIEW_RESULT_FAMILY,
        )


def test_pr_review_without_any_tool_evidence_fails_closed() -> None:
    """Model-only claims — however affirmative — are never evidence: a turn
    that read nothing through the qualified GitHub MCP cannot verify."""
    adapter, backend, resolver, session_id = _ready_reviewer_adapter()
    _queue_verified_review_turn(
        backend,
        session_id,
        [_text_message(f"OBSERVED_HEAD={_ASSIGNED_HEAD} COMPARISON=MATCH")],
    )

    with pytest.raises(ClineReviewSubjectUnverifiedError):
        adapter.realize_control(
            session_id,
            _pr_review_interaction(),
            message=_review_message(),
            expected_family=REVIEW_RESULT_FAMILY,
        )


def test_pr_review_with_wrong_repository_evidence_fails_closed() -> None:
    """Wrong-PR observations are rejected evidence: reads of another repository
    neither verify nor contradict the assigned subject."""
    adapter, backend, resolver, session_id = _ready_reviewer_adapter()
    _queue_verified_review_turn(
        backend,
        session_id,
        _turn_messages(
            _pr_read_pair("call-1", _ASSIGNED_HEAD, repo="openorc/other-fixture"),
            _file_read_pair("call-2", _ASSIGNED_HEAD, repo="openorc/other-fixture"),
        ),
    )

    with pytest.raises(ClineReviewSubjectUnverifiedError):
        adapter.realize_control(
            session_id,
            _pr_review_interaction(),
            message=_review_message(),
            expected_family=REVIEW_RESULT_FAMILY,
        )


def test_pr_review_with_wrong_pr_number_evidence_fails_closed() -> None:
    adapter, backend, resolver, session_id = _ready_reviewer_adapter()
    _queue_verified_review_turn(
        backend,
        session_id,
        _turn_messages(
            _pr_read_pair("call-1", _ASSIGNED_HEAD, pr_number=99),
            _file_read_pair("call-2", _ASSIGNED_HEAD),
        ),
    )

    with pytest.raises(ClineReviewSubjectUnverifiedError):
        adapter.realize_control(
            session_id,
            _pr_review_interaction(),
            message=_review_message(),
            expected_family=REVIEW_RESULT_FAMILY,
        )


def test_pr_review_with_unresolved_head_mismatch_fails_closed() -> None:
    """The final relevant PR read contradicts the assignment and nothing later
    corrects it (qualified T2 pattern): the review fails closed even though an
    assigned-SHA file read succeeded."""
    adapter, backend, resolver, session_id = _ready_reviewer_adapter()
    _queue_verified_review_turn(
        backend,
        session_id,
        _turn_messages(
            _pr_read_pair("call-1", _ASSIGNED_HEAD),
            _pr_read_pair("call-2", _OTHER_HEAD),
            _file_read_pair("call-3", _ASSIGNED_HEAD),
        ),
    )

    with pytest.raises(ClineReviewSubjectUnverifiedError):
        adapter.realize_control(
            session_id,
            _pr_review_interaction(),
            message=_review_message(),
            expected_family=REVIEW_RESULT_FAMILY,
        )

    with pytest.raises(ClineReviewSubjectUnverifiedError):
        adapter.realize_control(
            session_id,
            _pr_review_interaction(),
            message=_review_message(),
            expected_family=REVIEW_RESULT_FAMILY,
        )


def test_pr_review_with_failed_or_malformed_tool_results_fails_closed() -> None:
    """Failed and malformed reads are rejected evidence: an error-flagged PR
    read, a non-JSON PR result, and an errored file read verify nothing."""
    adapter, backend, resolver, session_id = _ready_reviewer_adapter()
    error_pr = _pr_read_pair("call-1", _ASSIGNED_HEAD)
    malformed_pr = _pr_read_pair("call-2", _ASSIGNED_HEAD)
    error_file = _file_read_pair("call-3", _ASSIGNED_HEAD, is_error=True)
    error_pr = (
        error_pr[0],
        _tool_result("call-1", _PR_READ_TOOL, "gateway timeout", is_error=True),
    )
    malformed_pr = (malformed_pr[0], _tool_result("call-2", _PR_READ_TOOL, "not json"))
    _queue_verified_review_turn(
        backend, session_id, _turn_messages(error_pr, malformed_pr, error_file)
    )

    with pytest.raises(ClineReviewSubjectUnverifiedError):
        adapter.realize_control(
            session_id,
            _pr_review_interaction(),
            message=_review_message(),
            expected_family=REVIEW_RESULT_FAMILY,
        )


def test_pr_review_without_a_commit_addressed_file_read_fails_closed() -> None:
    """File reads at the wrong ref — and reads at the assigned ref whose result
    carries no explicit commit binding — never satisfy the file check."""
    adapter, backend, resolver, session_id = _ready_reviewer_adapter()
    _queue_verified_review_turn(
        backend,
        session_id,
        _turn_messages(
            _pr_read_pair("call-1", _ASSIGNED_HEAD),
            _file_read_pair("call-2", _OTHER_HEAD),  # wrong ref
            _file_read_pair("call-3", _ASSIGNED_HEAD, with_uri=False),  # no binding
        ),
    )

    with pytest.raises(ClineReviewSubjectUnverifiedError):
        adapter.realize_control(
            session_id,
            _pr_review_interaction(),
            message=_review_message(),
            expected_family=REVIEW_RESULT_FAMILY,
        )


def test_pr_review_with_mismatched_commit_uri_binding_fails_closed() -> None:
    """A URI whose repository or commit disagrees with the assignment is not a
    binding for the assigned subject."""
    adapter, backend, resolver, session_id = _ready_reviewer_adapter()
    foreign_repo_pair = _file_read_pair("call-2", _ASSIGNED_HEAD, repo="openorc/other-fixture")
    _queue_verified_review_turn(
        backend,
        session_id,
        _turn_messages(_pr_read_pair("call-1", _ASSIGNED_HEAD), foreign_repo_pair),
    )

    with pytest.raises(ClineReviewSubjectUnverifiedError):
        adapter.realize_control(
            session_id,
            _pr_review_interaction(),
            message=_review_message(),
            expected_family=REVIEW_RESULT_FAMILY,
        )


def test_unsafe_transcript_boundary_fails_the_review_closed() -> None:
    """A post-send transcript that is not an exact extension of the snapshot
    cannot establish the current-turn boundary: the review is not verified."""
    adapter, backend, resolver, session_id = _ready_reviewer_adapter()
    good_turn = _turn_messages(
        _pr_read_pair("call-1", _ASSIGNED_HEAD),
        _file_read_pair("call-2", _ASSIGNED_HEAD),
    )
    backend.queue_read_messages(session_id, list(_RAW_MESSAGES))
    backend.queue_send(session_id, _completed(_review_result_payload()))
    backend.queue_read_messages(session_id, good_turn)

    with pytest.raises(ClineReviewSubjectUnverifiedError):
        adapter.realize_control(
            session_id,
            _pr_review_interaction(),
            message=_review_message(),
            expected_family=REVIEW_RESULT_FAMILY,
        )


def test_review_subject_error_never_carries_transcript_content() -> None:
    """The unverified-review failure is safe by authoring: a static machine
    reason only, with no fixture content and no exception chain."""
    adapter, backend, resolver, session_id = _ready_reviewer_adapter()
    _queue_verified_review_turn(
        backend,
        session_id,
        [_text_message(f"OBSERVED_HEAD={_ASSIGNED_HEAD} COMPARISON=MATCH")],
    )

    with pytest.raises(ClineReviewSubjectUnverifiedError) as exc_info:
        adapter.realize_control(
            session_id,
            _pr_review_interaction(),
            message=_review_message(),
            expected_family=REVIEW_RESULT_FAMILY,
        )

    error = exc_info.value
    assert "review-fixture" not in str(error)
    assert _ASSIGNED_HEAD not in str(error) and _ASSIGNED_HEAD not in repr(error)
    assert _OTHER_HEAD not in str(error)
    assert "OBSERVED_HEAD" not in str(error) and "MATCH" not in str(error)
    assert error.__cause__ is None
    assert error.__context__ is None

    assert error.__cause__ is None
    assert error.__context__ is None


def test_planning_review_keeps_the_shared_prose_path() -> None:
    """A planning REVIEW (PlanReviewSubject) is unchanged: one prose send with
    the shared funnel, no transcript reads, no verification."""
    from openorc.protocol.models import ReviewResult

    adapter, backend, resolver, session_id = _ready_reviewer_adapter()
    backend.queue_send(session_id, _completed(_review_result_payload()))
    interaction = ReviewInteraction(
        subject=PlanReviewSubject(
            plan_revision_id="plan-rev-1",
            repository_full_name=_REVIEW_REPO,
            issue_number=_REVIEW_ISSUE_NUMBER,
            plan="# Plan\n\n1. Implement the thing.\n",
        )
    )

    result = adapter.realize_control(
        session_id,
        interaction,
        message="composed planning review prose",
        expected_family=REVIEW_RESULT_FAMILY,
    )

    assert isinstance(result, ReviewResult)
    operations = [call.operation for call in backend.calls()]
    assert operations == ["start", "send", "send"]


def test_verified_pr_review_composition_is_a_caller_obligation() -> None:
    """The verified path requires the composed message and the review_result
    family, exactly like the shared prose path, and fails before any effect."""
    adapter, backend, resolver, session_id = _ready_reviewer_adapter()

    with pytest.raises(ValueError):
        adapter.realize_control(session_id, _pr_review_interaction())
    with pytest.raises(ValueError):
        adapter.realize_control(
            session_id,
            _pr_review_interaction(),
            message="   ",
            expected_family=REVIEW_RESULT_FAMILY,
        )
    with pytest.raises(ValueError):
        adapter.realize_control(
            session_id,
            _pr_review_interaction(),
            message=_review_message(),
            expected_family=PLAN_RESULT_FAMILY,
        )

    operations = [call.operation for call in backend.calls()]
    assert operations == ["start", "send"]


def test_failed_pre_send_read_prevents_the_review_send() -> None:
    """A failed snapshot read means the current-turn boundary cannot be
    attempted: the normalized error surfaces before any review is delivered."""
    adapter, backend, resolver, session_id = _ready_reviewer_adapter()
    backend.queue_read_messages(session_id, ClineSessionNotFoundError("gone"))

    with pytest.raises(AgentSessionNotFoundError):
        adapter.realize_control(
            session_id,
            _pr_review_interaction(),
            message=_review_message(),
            expected_family=REVIEW_RESULT_FAMILY,
        )

    operations = [call.operation for call in backend.calls()]
    assert operations == ["start", "send", "read_messages"]


def test_uncertain_review_send_latches_the_session_like_ordinary_sends() -> None:
    """An uncertain review send keeps the ordinary classification and latch:
    the outcome stays uncertain and same-ID reconstruction is refused."""
    context = _context(
        _identity(role=WorkflowRole.REVIEWER),
        cwd=_NEUTRAL_DIRECTORY,
        workspace_root=_NEUTRAL_DIRECTORY,
    )
    adapter, backend, resolver, session_id = _ready_adapter(context=context)
    resolver.set_bound(session_id, context)
    backend.queue_read_messages(session_id, list(_RAW_MESSAGES))
    backend.queue_send(session_id, ClineBackendUncertainOutcomeError("dropped"))

    with pytest.raises(AgentRuntimeDeliveryUncertainError):
        adapter.realize_control(
            session_id,
            _pr_review_interaction(),
            message=_review_message(),
            expected_family=REVIEW_RESULT_FAMILY,
        )

    with pytest.raises(AgentRuntimeUncertainOutcomeError):
        adapter.reconstruct_session(session_id, target_mode=ClineConstructionMode.PLAN)


def test_failed_post_send_read_withholds_the_review_without_claiming_non_delivery() -> None:
    """A failed evidence read after the completed review send withholds the
    review result as unverified: no pre-effect unavailability classification
    may escape and imply the send never occurred."""
    adapter, backend, resolver, session_id = _ready_reviewer_adapter()
    backend.queue_read_messages(session_id, list(_RAW_MESSAGES))
    backend.queue_send(session_id, _completed(_review_result_payload()))
    backend.queue_read_messages(session_id, ClineRemoteAttachmentUnavailableError("down"))

    with pytest.raises(ClineReviewSubjectUnverifiedError) as exc_info:
        adapter.realize_control(
            session_id,
            _pr_review_interaction(),
            message=_review_message(),
            expected_family=REVIEW_RESULT_FAMILY,
        )

    error = exc_info.value
    assert "down" not in str(error)
    assert error.__cause__ is None
    assert error.__context__ is None
    operations = [call.operation for call in backend.calls()]
    assert operations == ["start", "send", "read_messages", "send", "read_messages"]


def test_uri_in_returned_text_never_binds_the_commit() -> None:
    """A matching repo:// URI appearing only as ordinary returned text — for
    example inside repository-controlled file content — is not structured tool
    evidence: the commit binding is read from the resource.uri field only."""
    owner, name = _REVIEW_OWNER, _REVIEW_REPO_NAME
    adapter, backend, resolver, session_id = _ready_reviewer_adapter()
    tool_use = _tool_use(
        "call-2",
        _FILE_READ_TOOL,
        {"owner": owner, "repo": name, "path": _REVIEW_FILE, "ref": _ASSIGNED_HEAD, "sha": ""},
    )
    smuggled = {
        "content": [
            {
                "type": "text",
                "text": (
                    f"successfully downloaded text file (SHA: {_BLOB_SHA}) — "
                    f"binding: repo://{owner}/{name}/sha/{_ASSIGNED_HEAD}/contents/{_REVIEW_FILE}"
                ),
            }
        ]
    }
    _queue_verified_review_turn(
        backend,
        session_id,
        _turn_messages(
            _pr_read_pair("call-1", _ASSIGNED_HEAD),
            (tool_use, _tool_result("call-2", _FILE_READ_TOOL, smuggled)),
        ),
    )

    with pytest.raises(ClineReviewSubjectUnverifiedError):
        adapter.realize_control(
            session_id,
            _pr_review_interaction(),
            message=_review_message(),
            expected_family=REVIEW_RESULT_FAMILY,
        )


def test_string_encoded_mcp_results_verify_like_structured_ones() -> None:
    """The pinned baseline encodes each MCP result wrapper as a JSON string:
    the structured resource URI and head.sha inside such wrappers still
    verify, while text items inside them stay unscanned."""
    from openorc.protocol.models import ReviewResult

    adapter, backend, resolver, session_id = _ready_reviewer_adapter()
    pr_pair = _pr_read_pair("call-1", _ASSIGNED_HEAD)
    string_pr = (
        pr_pair[0],
        _tool_result(
            "call-1",
            _PR_READ_TOOL,
            json.dumps({"content": [{"type": "text", "text": pr_pair[1]["content"]}]}),
        ),
    )
    file_pair = _file_read_pair("call-2", _ASSIGNED_HEAD)
    string_file = (
        file_pair[0],
        _tool_result("call-2", _FILE_READ_TOOL, json.dumps(file_pair[1]["content"])),
    )
    _queue_verified_review_turn(backend, session_id, _turn_messages(string_pr, string_file))

    result = adapter.realize_control(
        session_id,
        _pr_review_interaction(),
        message=_review_message(),
        expected_family=REVIEW_RESULT_FAMILY,
    )

    assert isinstance(result, ReviewResult)


# --- D6a: activity abort (issue #75) ---

_EVENT_TIMEOUT_SECONDS = 10.0


def _call_in_thread(target: Callable[[], object]) -> tuple[threading.Thread, list[Exception]]:
    """Run target on a daemon worker, capturing a raised exception."""
    captured: list[Exception] = []

    def wrapper() -> None:
        try:
            target()
        except Exception as error:
            captured.append(error)

    thread = threading.Thread(target=wrapper, daemon=True)
    thread.start()
    return thread, captured


class _PairedRoleResolver(ClineConstructionContextResolver):
    """Resolver double serving one Producer and one Reviewer context."""

    def __init__(
        self, producer: ClineConstructionContext, reviewer: ClineConstructionContext
    ) -> None:
        self._contexts = {
            (producer.identity.task_id, producer.identity.role): producer,
            (reviewer.identity.task_id, reviewer.identity.role): reviewer,
        }
        self._bound: dict[str, ClineConstructionContext] = {}

    def resolve_construction_context(
        self, workspace_id: UUID, task_id: UUID, role: WorkflowRole
    ) -> ClineConstructionContext:
        return self._contexts[(task_id, role)]

    def resolve_bound_session_context(self, session_id: str) -> ClineConstructionContext | None:
        return self._bound.get(session_id)

    def bind(self, session_id: str, context: ClineConstructionContext) -> None:
        self._bound[session_id] = context


def test_abort_activity_dispatches_one_native_abort_for_a_bound_producer_session() -> None:
    """The exact bound ID dispatches one public abort: no stop/start/send,
    no replacement, no new prompt, and no invented abort reason."""
    adapter, backend, resolver, session_id = _ready_adapter()
    _bind_checkout(resolver, session_id)

    adapter.abort_activity(session_id)

    operations = [call.operation for call in backend.calls()]
    assert operations == ["start", "send", "abort"]
    abort_calls = [call for call in backend.calls() if call.operation == "abort"]
    assert len(abort_calls) == 1
    assert abort_calls[0].session_id == session_id
    assert abort_calls[0].reason is None


def test_abort_activity_serves_a_fresh_instance_bound_reviewer_session() -> None:
    """A fresh adapter instance with only the resolver-established binding
    aborts the exact bound Reviewer session: _known_identity is never the
    authority and both roles are eligible at this seam."""
    adapter, backend, resolver, session_id = _ready_reviewer_adapter()
    resolver.set_bound(
        session_id,
        _context(
            _identity(role=WorkflowRole.REVIEWER),
            cwd=_NEUTRAL_DIRECTORY,
            workspace_root=_NEUTRAL_DIRECTORY,
        ),
    )
    fresh = ClineAdapter(backend, resolver)

    fresh.abort_activity(session_id)

    operations = [call.operation for call in backend.calls()]
    assert operations == ["start", "send", "abort"]
    abort_calls = [call for call in backend.calls() if call.operation == "abort"]
    assert abort_calls[0].session_id == session_id


def test_abort_activity_rejects_a_blank_session_id_before_any_effect() -> None:
    adapter, backend, _, _ = _ready_adapter()

    with pytest.raises(ValueError):
        adapter.abort_activity("")
    with pytest.raises(ValueError):
        adapter.abort_activity("   ")

    assert [call.operation for call in backend.calls()] == ["start", "send"]


def test_abort_activity_fails_closed_without_an_establishable_binding() -> None:
    """A missing durable binding fails closed before any runtime dispatch."""
    adapter, backend, resolver, session_id = _ready_adapter()

    with pytest.raises(AgentRuntimeConfigurationRejectedError):
        adapter.abort_activity(session_id)

    assert [call.operation for call in backend.calls()] == ["start", "send"]
    assert resolver.bound_queries == [session_id]


def test_abort_reaches_the_backend_while_an_ordinary_send_is_in_flight() -> None:
    """The abort dispatches without the send/reconstruction lock: it reaches
    the backend while the send is still held open, and the send completes
    afterwards."""
    adapter, backend, resolver, session_id = _ready_adapter()
    _bind_checkout(resolver, session_id)
    backend.queue_send(session_id, _completed("slow reply"))
    entered, release = threading.Event(), threading.Event()
    backend.gate_operation("send", session_id, entered=entered, release=release)

    send_thread, send_errors = _call_in_thread(
        lambda: adapter.send(session_id, "long running turn")
    )
    assert entered.wait(timeout=_EVENT_TIMEOUT_SECONDS)

    adapter.abort_activity(session_id)

    assert [call.operation for call in backend.calls()] == ["start", "send", "send", "abort"]
    assert send_thread.is_alive()

    release.set()
    send_thread.join(timeout=_EVENT_TIMEOUT_SECONDS)
    assert not send_thread.is_alive()
    assert send_errors == []


def test_an_abort_on_one_session_leaves_another_sessions_send_independent() -> None:
    """Abort coordination is per session: an abort dispatched for a held-open
    session A does not serialize or disturb a concurrent send on session B."""
    backend = FakeClineSdkBackend()
    producer_context = _context()
    reviewer_context = _context(
        _identity(role=WorkflowRole.REVIEWER),
        cwd=_NEUTRAL_DIRECTORY,
        workspace_root=_NEUTRAL_DIRECTORY,
    )
    resolver = _PairedRoleResolver(producer_context, reviewer_context)
    adapter = ClineAdapter(backend, resolver)
    _script_ready_session(backend)
    producer_id = adapter.create_session(_creation_request()).external_session_id
    _script_ready_session(backend, session_id="cline-session-2")
    reviewer_id = adapter.create_session(
        _creation_request(role=WorkflowRole.REVIEWER, initialization="Reviewer init.")
    ).external_session_id
    assert (producer_id, reviewer_id) == ("cline-session-1", "cline-session-2")
    resolver.bind(producer_id, producer_context)
    resolver.bind(reviewer_id, reviewer_context)

    entered, release = threading.Event(), threading.Event()
    backend.gate_operation("send", producer_id, entered=entered, release=release)
    backend.queue_send(producer_id, _completed("slow producer reply"))
    backend.queue_send(reviewer_id, _completed("fast reviewer reply"))

    producer_thread, producer_errors = _call_in_thread(
        lambda: adapter.send(producer_id, "long running producer turn")
    )
    assert entered.wait(timeout=_EVENT_TIMEOUT_SECONDS)

    adapter.abort_activity(producer_id)

    reviewer_thread, reviewer_errors = _call_in_thread(
        lambda: adapter.send(reviewer_id, "reviewer question")
    )
    reviewer_thread.join(timeout=_EVENT_TIMEOUT_SECONDS)
    assert reviewer_errors == []
    assert producer_thread.is_alive()

    release.set()
    producer_thread.join(timeout=_EVENT_TIMEOUT_SECONDS)
    assert not producer_thread.is_alive()
    assert producer_errors == []


def test_abort_reaches_the_backend_during_a_verified_pr_review_send() -> None:
    """The D5b verified-review path holds the per-session lock across its
    pre-send read, send, and post-send evidence read; the abort still
    reaches the backend while the review send is in flight."""
    from openorc.protocol.models import ReviewResult

    adapter, backend, resolver, session_id = _ready_reviewer_adapter()
    resolver.set_bound(
        session_id,
        _context(
            _identity(role=WorkflowRole.REVIEWER),
            cwd=_NEUTRAL_DIRECTORY,
            workspace_root=_NEUTRAL_DIRECTORY,
        ),
    )
    suffix = _turn_messages(
        _pr_read_pair("call-1", _ASSIGNED_HEAD),
        _file_read_pair("call-2", _ASSIGNED_HEAD),
    )
    backend.queue_read_messages(session_id, list(_RAW_MESSAGES))
    backend.queue_send(session_id, _completed(_review_result_payload()))
    backend.queue_read_messages(session_id, list(_RAW_MESSAGES) + suffix)
    entered, release = threading.Event(), threading.Event()
    backend.gate_operation("send", session_id, entered=entered, release=release)

    outcomes: list[object] = []
    review_thread, review_errors = _call_in_thread(
        lambda: outcomes.append(
            adapter.realize_control(
                session_id,
                _pr_review_interaction(),
                message=_review_message(),
                expected_family=REVIEW_RESULT_FAMILY,
            )
        )
    )
    assert entered.wait(timeout=_EVENT_TIMEOUT_SECONDS)

    adapter.abort_activity(session_id)

    operations = [call.operation for call in backend.calls()]
    assert operations == ["start", "send", "read_messages", "send", "abort"]
    assert review_thread.is_alive()

    release.set()
    review_thread.join(timeout=_EVENT_TIMEOUT_SECONDS)
    assert not review_thread.is_alive()
    assert review_errors == []
    assert len(outcomes) == 1
    assert isinstance(outcomes[0], ReviewResult)


def test_abort_declines_while_reconstruction_holds_the_incarnation_section() -> None:
    """A reconstruction that already entered its read_messages -> stop ->
    start section makes the abort decline locally before any dispatch —
    never a queued interruption of the possibly new incarnation."""
    adapter, backend, resolver, session_id = _ready_adapter()
    _bind_checkout(resolver, session_id)
    entered, release = threading.Event(), threading.Event()
    backend.gate_operation("read_messages", session_id, entered=entered, release=release)

    reconstruct_thread, reconstruct_errors = _call_in_thread(
        lambda: adapter.reconstruct_session(session_id, target_mode=ClineConstructionMode.ACT)
    )
    assert entered.wait(timeout=_EVENT_TIMEOUT_SECONDS)

    with pytest.raises(AgentRuntimeUnavailableError):
        adapter.abort_activity(session_id)

    assert "abort" not in [call.operation for call in backend.calls()]

    release.set()
    reconstruct_thread.join(timeout=_EVENT_TIMEOUT_SECONDS)
    assert not reconstruct_thread.is_alive()
    assert reconstruct_errors == []
    assert [call.operation for call in backend.calls()] == [
        "start",
        "send",
        "read_messages",
        "stop",
        "start",
    ]

    # With the reconstruction complete, a later abort dispatches normally.
    adapter.abort_activity(session_id)
    assert [call.operation for call in backend.calls()][-1] == "abort"


def test_a_pending_abort_makes_a_waiting_reconstruction_fail_closed() -> None:
    """A reconstruction queued behind a running send must not starve the
    abort: the abort dispatches while the send is in flight, and after the
    send completes the reconstruction refuses before any read/stop effect."""
    adapter, backend, resolver, session_id = _ready_adapter()
    _bind_checkout(resolver, session_id)
    backend.queue_send(session_id, _completed("slow reply"))
    entered, release = threading.Event(), threading.Event()
    backend.gate_operation("send", session_id, entered=entered, release=release)

    send_thread, send_errors = _call_in_thread(
        lambda: adapter.send(session_id, "long running turn")
    )
    assert entered.wait(timeout=_EVENT_TIMEOUT_SECONDS)

    # Started while the gate holds the send open: the reconstruction cannot
    # acquire the send lock yet, so the abort below is registered first.
    reconstruct_thread, reconstruct_errors = _call_in_thread(
        lambda: adapter.reconstruct_session(session_id, target_mode=ClineConstructionMode.ACT)
    )

    adapter.abort_activity(session_id)
    assert [call.operation for call in backend.calls()] == ["start", "send", "send", "abort"]

    release.set()
    send_thread.join(timeout=_EVENT_TIMEOUT_SECONDS)
    assert not send_thread.is_alive()
    assert send_errors == []
    reconstruct_thread.join(timeout=_EVENT_TIMEOUT_SECONDS)
    assert not reconstruct_thread.is_alive()
    assert len(reconstruct_errors) == 1
    assert isinstance(reconstruct_errors[0], AgentRuntimeUncertainOutcomeError)
    assert [call.operation for call in backend.calls()] == ["start", "send", "send", "abort"]


def test_an_acknowledged_abort_does_not_clear_the_reconstruction_guard() -> None:
    """An acknowledged abort is unresolved interruption evidence, not
    quiescence: same-ID reconstruction stays refused for #148."""
    adapter, backend, resolver, session_id = _ready_adapter()
    _bind_checkout(resolver, session_id)

    adapter.abort_activity(session_id)

    with pytest.raises(AgentRuntimeUncertainOutcomeError):
        adapter.reconstruct_session(session_id, target_mode=ClineConstructionMode.ACT)

    assert [call.operation for call in backend.calls()] == ["start", "send", "abort"]


def test_overlapping_aborts_keep_the_reconstruction_guard_until_all_settle() -> None:
    """Regression: one abort's known pre-effect failure must not release the
    reconstruction guard while another abort for the same session is still
    outstanding. Only when the last outstanding abort settles with a known
    no-effect outcome does the baseline reconstruction safety return."""
    adapter, backend, resolver, session_id = _ready_adapter()
    _bind_checkout(resolver, session_id)
    first_entered, first_release = threading.Event(), threading.Event()
    second_entered, second_release = threading.Event(), threading.Event()
    backend.gate_operation("abort", session_id, entered=first_entered, release=first_release)
    backend.gate_operation("abort", session_id, entered=second_entered, release=second_release)
    backend.queue_abort(session_id, ClineRemoteAttachmentUnavailableError("down"))
    backend.queue_abort(session_id, ClineRemoteAttachmentUnavailableError("down"))

    first_thread, first_errors = _call_in_thread(lambda: adapter.abort_activity(session_id))
    assert first_entered.wait(timeout=_EVENT_TIMEOUT_SECONDS)
    second_thread, second_errors = _call_in_thread(lambda: adapter.abort_activity(session_id))
    assert second_entered.wait(timeout=_EVENT_TIMEOUT_SECONDS)

    # The second abort settles first with a known pre-effect failure while
    # the first abort is still outstanding:
    second_release.set()
    second_thread.join(timeout=_EVENT_TIMEOUT_SECONDS)
    assert not second_thread.is_alive()
    assert len(second_errors) == 1
    assert isinstance(second_errors[0], AgentRuntimeUnavailableError)
    assert second_errors[0].__cause__ is None
    assert second_errors[0].__context__ is None

    # The guard still holds with one abort outstanding: reconstruction is
    # refused even though one abort already settled without any effect.
    with pytest.raises(AgentRuntimeUncertainOutcomeError):
        adapter.reconstruct_session(session_id, target_mode=ClineConstructionMode.ACT)
    assert "read_messages" not in [call.operation for call in backend.calls()]

    first_release.set()
    first_thread.join(timeout=_EVENT_TIMEOUT_SECONDS)
    assert not first_thread.is_alive()
    assert len(first_errors) == 1
    assert isinstance(first_errors[0], AgentRuntimeUnavailableError)

    adapter.reconstruct_session(session_id, target_mode=ClineConstructionMode.ACT)
    operations = [call.operation for call in backend.calls()]
    assert operations.count("abort") == 2
    assert operations[-3:] == ["read_messages", "stop", "start"]


def test_an_uncertain_abort_latches_the_session_against_reconstruction() -> None:
    """An abort whose effect could not be classified keeps the D5a uncertain
    classification and latch: same-ID reconstruction is refused."""
    adapter, backend, resolver, session_id = _ready_adapter()
    _bind_checkout(resolver, session_id)
    backend.queue_abort(session_id, ClineBackendUncertainOutcomeError("dropped"))

    with pytest.raises(AgentRuntimeDeliveryUncertainError) as excinfo:
        adapter.abort_activity(session_id)

    error = excinfo.value
    assert error.__cause__ is None
    assert error.__context__ is None

    with pytest.raises(AgentRuntimeUncertainOutcomeError):
        adapter.reconstruct_session(session_id, target_mode=ClineConstructionMode.ACT)

    assert [call.operation for call in backend.calls()] == ["start", "send", "abort"]


def test_a_known_pre_effect_abort_failure_releases_the_guard_when_settled() -> None:
    """A verifiably pre-effect abort never reached the runtime: with no abort
    left outstanding, the D5a baseline reconstruction safety applies again
    and the normalized taxonomy surfaces unchanged."""
    adapter, backend, resolver, session_id = _ready_adapter()
    _bind_checkout(resolver, session_id)
    backend.queue_abort(session_id, ClineRemoteAttachmentUnavailableError("down"))

    with pytest.raises(AgentRuntimeUnavailableError) as excinfo:
        adapter.abort_activity(session_id)

    assert excinfo.value.__cause__ is None
    assert excinfo.value.__context__ is None

    adapter.reconstruct_session(session_id, target_mode=ClineConstructionMode.ACT)
    assert [call.operation for call in backend.calls()] == [
        "start",
        "send",
        "abort",
        "read_messages",
        "stop",
        "start",
    ]


# --- D6b: interruption reconciliation and recovery (issue #148) ---


def _reconcile_instance(
    backend: FakeClineSdkBackend, session_id: str
) -> tuple[ClineAdapter, ScriptedResolver]:
    """A fresh adapter instance over an established binding: process-local
    caches are empty and the resolver's durable binding is the authority."""
    resolver = ScriptedResolver()
    resolver.set_bound(
        session_id,
        _context(cwd=_TASK_CHECKOUT, workspace_root=_TASK_CHECKOUT),
    )
    return ClineAdapter(backend, resolver), resolver


def test_reconciliation_reads_the_exact_bound_transcript_without_runtime_effects() -> None:
    """A newly instantiated adapter with a resolver-proven binding inspects
    the exact persisted transcript through public reads only: no start, no
    initialization or send, no stop, no subscription, and no connect."""
    backend = FakeClineSdkBackend()
    session_id = _script_ready_session(backend)
    adapter, _ = _reconcile_instance(backend, session_id)
    backend.queue_get(session_id, {"sessionId": session_id, "status": "running"})
    backend.queue_read_messages(session_id, list(_RAW_MESSAGES))
    backend.queue_get_accumulated_usage(session_id, {"totalTokens": 5})

    result = adapter.reconcile_session(session_id)

    assert result.continuity is ClineSessionContinuity.ACCESSIBLE
    assert result.transcript_message_count == len(_RAW_MESSAGES)
    assert result.messages_beyond_checkpoint is None
    assert result.record_observed is True
    assert result.usage_observed is True
    operations = [call.operation for call in backend.calls()]
    assert operations == ["get", "read_messages", "get_accumulated_usage"]
    assert "connect" not in operations


def test_reconciliation_fails_closed_without_a_durable_binding() -> None:
    adapter, backend, _, session_id = _ready_adapter()
    fresh = ClineAdapter(backend, ScriptedResolver())

    with pytest.raises(AgentRuntimeConfigurationRejectedError):
        fresh.reconcile_session(session_id)

    assert [call.operation for call in backend.calls()] == ["start", "send"]


def test_reconciliation_surviving_transcript_with_non_addressable_record_is_recoverable() -> None:
    """A readable persisted transcript without an addressable record is the
    recoverable-incarnation distinction, never a terminal-loss claim."""
    backend = FakeClineSdkBackend()
    session_id = _script_ready_session(backend)
    adapter, _ = _reconcile_instance(backend, session_id)
    backend.queue_read_messages(session_id, list(_RAW_MESSAGES))

    result = adapter.reconcile_session(session_id)

    assert result.continuity is ClineSessionContinuity.RECOVERABLE
    assert result.transcript_message_count == len(_RAW_MESSAGES)
    assert result.record_observed is False
    assert "list_history" not in [call.operation for call in backend.calls()]


def test_reconciliation_enumeration_absence_is_unknown_without_an_exhaustive_history_contract() -> (
    None
):
    """Coherent absence across reads stays unknown without a demonstrated
    exhaustive public-history contract: a readable enumeration proves
    readability, never that every persisted session was covered, so an
    omitted older session is never classified terminally lost."""
    backend = FakeClineSdkBackend()
    session_id = _script_ready_session(backend)
    adapter, _ = _reconcile_instance(backend, session_id)
    backend.queue_get(session_id, ClineSessionNotFoundError("gone"))
    backend.queue_read_messages(session_id, ClineSessionNotFoundError("gone"))
    backend.queue_list_history([{"sessionId": "cline-session-other"}])

    result = adapter.reconcile_session(session_id)

    assert result.continuity is ClineSessionContinuity.UNKNOWN
    assert result.transcript_message_count is None


def test_reconciliation_absence_without_coherent_history_is_unknown_not_lost() -> None:
    """Absence without coherent corroboration stays unknown: an empty or
    non-identifying history enumeration cannot establish terminal loss, and
    a history that still lists the session is contradictory."""
    backend = FakeClineSdkBackend()
    session_id = _script_ready_session(backend)
    adapter, _ = _reconcile_instance(backend, session_id)
    backend.queue_get(session_id, ClineSessionNotFoundError("gone"))
    backend.queue_read_messages(session_id, ClineSessionNotFoundError("gone"))
    backend.queue_list_history([])

    result = adapter.reconcile_session(session_id)
    assert result.continuity is ClineSessionContinuity.UNKNOWN

    backend.queue_list_history([{"sessionId": session_id}])
    result = adapter.reconcile_session(session_id)
    assert result.continuity is ClineSessionContinuity.UNKNOWN


def test_reconciliation_attachment_failures_are_never_terminal_claims() -> None:
    """An unavailable attachment or a mid-read connection loss fails closed
    through the normalized taxonomy without any session-loss or replay
    claim, and without latching the otherwise safe session."""
    adapter, backend, resolver, session_id = _ready_adapter()
    _bind_checkout(resolver, session_id)
    backend.queue_get(session_id, ClineRemoteAttachmentUnavailableError("down"))

    with pytest.raises(AgentRuntimeUnavailableError):
        adapter.reconcile_session(session_id)

    backend.queue_get(
        session_id,
        ClineSdkOperationRejectedError("hub_connection_closed", {"close_code": 1006}),
    )
    with pytest.raises(AgentRuntimeUnavailableError):
        adapter.reconcile_session(session_id)

    backend.queue_read_messages(session_id, list(_RAW_MESSAGES))
    adapter.reconstruct_session(session_id, target_mode=ClineConstructionMode.ACT)
    assert [call.operation for call in backend.calls()][-3:] == [
        "read_messages",
        "stop",
        "start",
    ]


def test_reconciliation_checkpoint_observations_stay_bounded_and_claim_nothing() -> None:
    """A caller-supplied pre-dispatch checkpoint yields only a bounded
    new-turn observation count: it never claims delivery, never triggers a
    duplicate send, never reconstitutes a formal result, and never clears
    the uncertain-send latch — even with a completed record status."""
    adapter, backend, resolver, session_id = _ready_adapter()
    _bind_checkout(resolver, session_id)
    backend.queue_send(session_id, ClineBackendUncertainOutcomeError("dropped"))
    with pytest.raises(AgentRuntimeDeliveryUncertainError):
        adapter.send(session_id, "interrupted turn")

    backend.queue_get(session_id, {"sessionId": session_id, "status": "completed"})
    backend.queue_read_messages(session_id, list(_RAW_MESSAGES) + [{"id": "m3"}])
    result = adapter.reconcile_session(
        session_id,
        checkpoint=ClineDispatchCheckpoint(transcript_message_count=len(_RAW_MESSAGES)),
    )

    assert result.continuity is ClineSessionContinuity.ACCESSIBLE
    assert result.messages_beyond_checkpoint == 1
    # The interrupted send plus the readiness send: reconciliation added
    # no duplicate send and no synthetic formal result.
    assert [call.operation for call in backend.calls()].count("send") == 2

    with pytest.raises(AgentRuntimeUncertainOutcomeError):
        adapter.reconstruct_session(session_id, target_mode=ClineConstructionMode.ACT)


def test_reconciliation_shrunk_transcript_is_contradictory() -> None:
    """A transcript that shrank below the caller's pre-dispatch observation
    is contradictory evidence: unknown continuity, no delta observation."""
    adapter, backend, resolver, session_id = _ready_adapter()
    _bind_checkout(resolver, session_id)
    backend.queue_read_messages(session_id, list(_RAW_MESSAGES))

    result = adapter.reconcile_session(
        session_id,
        checkpoint=ClineDispatchCheckpoint(transcript_message_count=len(_RAW_MESSAGES) + 3),
    )

    assert result.continuity is ClineSessionContinuity.UNKNOWN
    assert result.messages_beyond_checkpoint is None


def test_settlement_requires_positive_caller_evidence() -> None:
    """Settlement is caller-attested: an unsatisfied attestation raises
    before any effect and leaves every guard latched."""
    adapter, backend, resolver, session_id = _ready_adapter()
    _bind_checkout(resolver, session_id)
    backend.queue_send(session_id, ClineBackendUncertainOutcomeError("dropped"))
    with pytest.raises(AgentRuntimeDeliveryUncertainError):
        adapter.send(session_id, "interrupted turn")

    with pytest.raises(ValueError, match="positive caller evidence"):
        adapter.settle_uncertain_send(
            session_id,
            runtime_work_settled=False,
            operation_accounted_for=True,
        )
    with pytest.raises(ValueError, match="positive caller evidence"):
        adapter.settle_uncertain_send(
            session_id,
            runtime_work_settled=True,
            operation_accounted_for=False,
        )

    with pytest.raises(AgentRuntimeUncertainOutcomeError):
        adapter.reconstruct_session(session_id, target_mode=ClineConstructionMode.ACT)
    assert [call.operation for call in backend.calls()] == ["start", "send", "send"]


def test_settling_an_accounted_uncertain_send_releases_only_the_send_kind() -> None:
    """A provably accounted-for settled send releases the send-kind latch
    and reconstruction becomes possible again — without equating record
    status with completion and without clearing unrelated kinds."""
    adapter, backend, resolver, session_id = _ready_adapter()
    _bind_checkout(resolver, session_id)
    backend.queue_send(session_id, ClineBackendUncertainOutcomeError("dropped"))
    with pytest.raises(AgentRuntimeDeliveryUncertainError):
        adapter.send(session_id, "interrupted turn")

    outcome = adapter.settle_uncertain_send(
        session_id,
        runtime_work_settled=True,
        operation_accounted_for=True,
    )
    assert outcome.settled is True
    assert outcome.refusal is None

    backend.queue_read_messages(session_id, list(_RAW_MESSAGES))
    adapter.reconstruct_session(session_id, target_mode=ClineConstructionMode.ACT)
    assert [call.operation for call in backend.calls()][-3:] == [
        "read_messages",
        "stop",
        "start",
    ]


def test_settlement_never_clears_an_unrelated_uncertain_stop() -> None:
    """Settling the send kind leaves an uncertain stop latched: reconstruction
    stays refused until every applicable uncertain effect is accounted for,
    and a later send settlement never clears the stop kind either."""
    adapter, backend, resolver, session_id = _ready_adapter()
    _bind_checkout(resolver, session_id)
    backend.queue_send(session_id, ClineBackendUncertainOutcomeError("dropped"))
    with pytest.raises(AgentRuntimeDeliveryUncertainError):
        adapter.send(session_id, "interrupted turn")

    outcome = adapter.settle_uncertain_send(
        session_id,
        runtime_work_settled=True,
        operation_accounted_for=True,
    )
    assert outcome.settled is True

    # With the send settled, an authorized reconstruction may run — and an
    # uncertain stop inside it latches the stop kind.
    backend.queue_read_messages(session_id, list(_RAW_MESSAGES))
    backend.queue_stop(session_id, ClineBackendUncertainOutcomeError("dropped"))
    with pytest.raises(AgentRuntimeDeliveryUncertainError):
        adapter.reconstruct_session(session_id, target_mode=ClineConstructionMode.ACT)

    # A new interrupted send latches the send kind again; settling it must
    # not clear the unrelated stop-kind uncertainty.
    backend.queue_send(session_id, ClineBackendUncertainOutcomeError("dropped"))
    with pytest.raises(AgentRuntimeDeliveryUncertainError):
        adapter.send(session_id, "interrupted again")

    outcome = adapter.settle_uncertain_send(
        session_id,
        runtime_work_settled=True,
        operation_accounted_for=True,
    )
    assert outcome.settled is True

    with pytest.raises(AgentRuntimeUncertainOutcomeError):
        adapter.reconstruct_session(session_id, target_mode=ClineConstructionMode.ACT)
    assert [call.operation for call in backend.calls()].count("read_messages") == 1


def test_settlement_vetoes_while_an_abort_is_outstanding_or_unresolved() -> None:
    """An outstanding or acknowledged-but-unsettled abort is always a veto:
    settlement returns a typed refusal and keeps every guard latched."""
    adapter, backend, resolver, session_id = _ready_adapter()
    _bind_checkout(resolver, session_id)
    backend.queue_send(session_id, ClineBackendUncertainOutcomeError("dropped"))
    with pytest.raises(AgentRuntimeDeliveryUncertainError):
        adapter.send(session_id, "interrupted turn")

    entered, release = threading.Event(), threading.Event()
    backend.gate_operation("abort", session_id, entered=entered, release=release)
    abort_thread, abort_errors = _call_in_thread(lambda: adapter.abort_activity(session_id))
    assert entered.wait(timeout=_EVENT_TIMEOUT_SECONDS)

    outcome = adapter.settle_uncertain_send(
        session_id,
        runtime_work_settled=True,
        operation_accounted_for=True,
    )
    assert outcome.settled is False
    assert outcome.refusal is ClineSettlementRefusal.OUTSTANDING_ABORT

    release.set()
    abort_thread.join(timeout=_EVENT_TIMEOUT_SECONDS)
    assert not abort_thread.is_alive()
    assert abort_errors == []

    outcome = adapter.settle_uncertain_send(
        session_id,
        runtime_work_settled=True,
        operation_accounted_for=True,
    )
    assert outcome.settled is False
    assert outcome.refusal is ClineSettlementRefusal.UNRESOLVED_ABORT

    outcome = adapter.settle_abort(
        session_id,
        runtime_work_settled=True,
        operation_accounted_for=True,
    )
    assert outcome.settled is True

    outcome = adapter.settle_uncertain_send(
        session_id,
        runtime_work_settled=True,
        operation_accounted_for=True,
    )
    assert outcome.settled is True

    backend.queue_read_messages(session_id, list(_RAW_MESSAGES))
    adapter.reconstruct_session(session_id, target_mode=ClineConstructionMode.ACT)
    assert [call.operation for call in backend.calls()][-3:] == [
        "read_messages",
        "stop",
        "start",
    ]


def test_settlement_cannot_slip_past_an_active_reconstruction() -> None:
    """Guard settlement serializes behind the same reconstruction section:
    it cannot clear anything while a reconstruction holds the incarnation
    section, and only proceeds after the section ends."""
    adapter, backend, resolver, session_id = _ready_adapter()
    _bind_checkout(resolver, session_id)
    backend.queue_read_messages(session_id, list(_RAW_MESSAGES))
    entered, release = threading.Event(), threading.Event()
    backend.gate_operation("read_messages", session_id, entered=entered, release=release)
    reconstruction_thread, reconstruction_errors = _call_in_thread(
        lambda: adapter.reconstruct_session(session_id, target_mode=ClineConstructionMode.ACT)
    )
    assert entered.wait(timeout=_EVENT_TIMEOUT_SECONDS)

    outcomes: list[object] = []
    settle_thread, settle_errors = _call_in_thread(
        lambda: outcomes.append(
            adapter.settle_abort(
                session_id,
                runtime_work_settled=True,
                operation_accounted_for=True,
            )
        )
    )
    assert settle_thread.is_alive()

    release.set()
    reconstruction_thread.join(timeout=_EVENT_TIMEOUT_SECONDS)
    assert not reconstruction_thread.is_alive()
    assert reconstruction_errors == []
    settle_thread.join(timeout=_EVENT_TIMEOUT_SECONDS)
    assert not settle_thread.is_alive()
    assert settle_errors == []
    assert len(outcomes) == 1
    outcome = outcomes[0]
    assert isinstance(outcome, ClineSettlementOutcome)
    assert outcome.settled is False
    assert outcome.refusal is ClineSettlementRefusal.NOTHING_TO_SETTLE


def test_settling_an_uncertain_abort_releases_guard_and_latch_atomically() -> None:
    """An uncertain abort keeps both its D6a guard and its abort-kind
    uncertain effect; one caller-attested settlement clears both, and the
    authorized reconstruction then succeeds — no residual generic latch."""
    adapter, backend, resolver, session_id = _ready_adapter()
    _bind_checkout(resolver, session_id)
    backend.queue_abort(session_id, ClineBackendUncertainOutcomeError("dropped"))
    with pytest.raises(AgentRuntimeDeliveryUncertainError):
        adapter.abort_activity(session_id)

    with pytest.raises(AgentRuntimeUncertainOutcomeError):
        adapter.reconstruct_session(session_id, target_mode=ClineConstructionMode.ACT)

    outcome = adapter.settle_abort(
        session_id,
        runtime_work_settled=True,
        operation_accounted_for=True,
    )
    assert outcome.settled is True
    assert outcome.refusal is None

    backend.queue_read_messages(session_id, list(_RAW_MESSAGES))
    adapter.reconstruct_session(session_id, target_mode=ClineConstructionMode.ACT)
    assert [call.operation for call in backend.calls()][-3:] == [
        "read_messages",
        "stop",
        "start",
    ]


def test_abort_settlement_never_clears_an_uncertain_send() -> None:
    """Settling the abort guard leaves an uncertain send latched: both
    interruptions must be accounted for before reconstruction."""
    adapter, backend, resolver, session_id = _ready_adapter()
    _bind_checkout(resolver, session_id)
    backend.queue_abort(session_id, ClineBackendUncertainOutcomeError("dropped"))
    with pytest.raises(AgentRuntimeDeliveryUncertainError):
        adapter.abort_activity(session_id)
    backend.queue_send(session_id, ClineBackendUncertainOutcomeError("dropped"))
    with pytest.raises(AgentRuntimeDeliveryUncertainError):
        adapter.send(session_id, "interrupted turn")

    outcome = adapter.settle_abort(
        session_id,
        runtime_work_settled=True,
        operation_accounted_for=True,
    )
    assert outcome.settled is True

    with pytest.raises(AgentRuntimeUncertainOutcomeError):
        adapter.reconstruct_session(session_id, target_mode=ClineConstructionMode.ACT)

    outcome = adapter.settle_uncertain_send(
        session_id,
        runtime_work_settled=True,
        operation_accounted_for=True,
    )
    assert outcome.settled is True

    backend.queue_read_messages(session_id, list(_RAW_MESSAGES))
    adapter.reconstruct_session(session_id, target_mode=ClineConstructionMode.ACT)
    assert [call.operation for call in backend.calls()][-3:] == [
        "read_messages",
        "stop",
        "start",
    ]


def test_failed_reconciliation_reads_never_latch_an_otherwise_safe_session() -> None:
    """A pure observational read can never take an external effect: even an
    uncertain-classified transport failure on a reconciliation read leaves
    no unresolved runtime-effect guard behind."""
    adapter, backend, resolver, session_id = _ready_adapter()
    _bind_checkout(resolver, session_id)
    backend.queue_get(session_id, ClineBackendUncertainOutcomeError("timeout"))

    with pytest.raises(AgentRuntimeDeliveryUncertainError):
        adapter.reconcile_session(session_id)

    backend.queue_read_messages(session_id, list(_RAW_MESSAGES))
    adapter.reconstruct_session(session_id, target_mode=ClineConstructionMode.ACT)
    assert [call.operation for call in backend.calls()][-3:] == [
        "read_messages",
        "stop",
        "start",
    ]


def test_recovery_reconstructs_the_same_id_after_a_qualified_restart() -> None:
    """Surviving transcript with a non-addressable incarnation: the
    authorized recovery runs the single same-ID seam — read_messages, a
    positively-absent stop, and a same-ID start carrying the raw transcript
    verbatim in the workflow-selected mode at the resolver-bound original
    checkout, with no connect and no initialization resend."""
    backend = FakeClineSdkBackend()
    session_id = _script_ready_session(backend)
    resolver = ScriptedResolver(
        creation=_context(cwd="/elsewhere/edited", workspace_root="/elsewhere/edited")
    )
    resolver.set_bound(
        session_id,
        _context(cwd=_TASK_CHECKOUT, workspace_root=_TASK_CHECKOUT),
    )
    adapter = ClineAdapter(backend, resolver)
    backend.queue_read_messages(session_id, list(_RAW_MESSAGES))
    backend.queue_stop(session_id, ClineSessionNotFoundError("no live incarnation"))

    adapter.recover_session(session_id, target_mode=ClineConstructionMode.ACT)

    operations = [call.operation for call in backend.calls()]
    assert operations[-3:] == ["read_messages", "stop", "start"]
    assert "connect" not in operations
    rebuild = backend.calls()[-1].request
    assert rebuild is not None
    assert rebuild.session_id == session_id
    assert rebuild.mode is ClineConstructionMode.ACT
    assert rebuild.cwd == _TASK_CHECKOUT
    assert rebuild.workspace_root == _TASK_CHECKOUT
    assert rebuild.initial_messages == _RAW_MESSAGES


def test_recovery_refuses_an_ambiguous_stop_failure() -> None:
    """An uncertain stop never permits a same-ID start: the recovery fails
    closed and the stop-kind uncertainty keeps the session guarded."""
    adapter, backend, resolver, session_id = _ready_adapter()
    _bind_checkout(resolver, session_id)
    backend.queue_read_messages(session_id, list(_RAW_MESSAGES))
    backend.queue_stop(session_id, ClineBackendUncertainOutcomeError("dropped"))

    with pytest.raises(AgentRuntimeDeliveryUncertainError):
        adapter.recover_session(session_id, target_mode=ClineConstructionMode.ACT)

    operations = [call.operation for call in backend.calls()]
    assert "start" not in operations[2:]
    with pytest.raises(AgentRuntimeUncertainOutcomeError):
        adapter.reconstruct_session(session_id, target_mode=ClineConstructionMode.ACT)
    assert [call.operation for call in backend.calls()].count("read_messages") == 1


def test_recovery_refuses_without_surviving_transcript_evidence() -> None:
    """Recovery requires positively established surviving transcript
    evidence: a positively absent read and an empty read on an established
    bound session both refuse before any stop/start effect."""
    adapter, backend, resolver, session_id = _ready_adapter()
    _bind_checkout(resolver, session_id)
    backend.queue_read_messages(session_id, ClineSessionNotFoundError("gone"))

    with pytest.raises(AgentSessionNotFoundError):
        adapter.recover_session(session_id, target_mode=ClineConstructionMode.ACT)
    operations = [call.operation for call in backend.calls()]
    assert "stop" not in operations[2:]
    assert "start" not in operations[2:]

    empty = FakeClineSdkBackend()
    empty_session = _script_ready_session(empty)
    empty_resolver = ScriptedResolver()
    empty_resolver.set_bound(
        empty_session,
        _context(cwd=_TASK_CHECKOUT, workspace_root=_TASK_CHECKOUT),
    )
    fresh = ClineAdapter(empty, empty_resolver)
    empty.queue_read_messages(empty_session, [])

    with pytest.raises(AgentRuntimeUncertainOutcomeError):
        fresh.recover_session(empty_session, target_mode=ClineConstructionMode.ACT)
    empty_operations = [call.operation for call in empty.calls()]
    assert "stop" not in empty_operations
    assert "start" not in empty_operations


def test_recovery_refuses_while_uncertainty_or_abort_guards_remain() -> None:
    """Recovery never displaces possibly active or unresolved work: an
    unresolved uncertain effect or an acknowledged abort refuses the
    recovery before the first effect until the caller settles them."""
    adapter, backend, resolver, session_id = _ready_adapter()
    _bind_checkout(resolver, session_id)
    backend.queue_send(session_id, ClineBackendUncertainOutcomeError("dropped"))
    with pytest.raises(AgentRuntimeDeliveryUncertainError):
        adapter.send(session_id, "interrupted turn")

    with pytest.raises(AgentRuntimeUncertainOutcomeError):
        adapter.recover_session(session_id, target_mode=ClineConstructionMode.ACT)
    assert "read_messages" not in [call.operation for call in backend.calls()]

    adapter.abort_activity(session_id)
    with pytest.raises(AgentRuntimeUncertainOutcomeError):
        adapter.recover_session(session_id, target_mode=ClineConstructionMode.ACT)
    assert "read_messages" not in [call.operation for call in backend.calls()]

    outcome = adapter.settle_abort(
        session_id,
        runtime_work_settled=True,
        operation_accounted_for=True,
    )
    assert outcome.settled is True
    outcome = adapter.settle_uncertain_send(
        session_id,
        runtime_work_settled=True,
        operation_accounted_for=True,
    )
    assert outcome.settled is True

    backend.queue_read_messages(session_id, list(_RAW_MESSAGES))
    adapter.recover_session(session_id, target_mode=ClineConstructionMode.ACT)
    assert [call.operation for call in backend.calls()][-3:] == [
        "read_messages",
        "stop",
        "start",
    ]


def test_recovery_into_reviewer_act_is_refused_before_any_effect() -> None:
    adapter, backend, resolver, session_id = _ready_reviewer_adapter()
    resolver.set_bound(
        session_id,
        _context(
            _identity(role=WorkflowRole.REVIEWER),
            cwd=_NEUTRAL_DIRECTORY,
            workspace_root=_NEUTRAL_DIRECTORY,
        ),
    )

    with pytest.raises(ValueError, match="native PLAN"):
        adapter.recover_session(session_id, target_mode=ClineConstructionMode.ACT)

    assert [call.operation for call in backend.calls()] == ["start", "send"]


def test_reconciliation_and_recovery_preserve_the_shared_attachment() -> None:
    """Reconciliation and recovery consume the existing attachment: no
    connect is ever recorded and an in-flight send on another session is
    not disturbed or invalidated."""
    backend = FakeClineSdkBackend()
    producer_context = _context()
    reviewer_context = _context(
        _identity(role=WorkflowRole.REVIEWER),
        cwd=_NEUTRAL_DIRECTORY,
        workspace_root=_NEUTRAL_DIRECTORY,
    )
    resolver = _PairedRoleResolver(producer_context, reviewer_context)
    producer_id = _script_ready_session(backend)
    backend.queue_start(ClineStartResult(session_id="cline-session-2"))
    backend.queue_send("cline-session-2", _completed(_READY_PAYLOAD))
    adapter = ClineAdapter(backend, resolver)
    adapter.create_session(_creation_request())
    adapter.create_session(_creation_request(role=WorkflowRole.REVIEWER))
    reviewer_id = "cline-session-2"
    resolver.bind(producer_id, _context(cwd=_TASK_CHECKOUT, workspace_root=_TASK_CHECKOUT))
    resolver.bind(reviewer_id, reviewer_context)

    backend.queue_send(reviewer_id, _completed("slow reply"))
    entered, release = threading.Event(), threading.Event()
    backend.gate_operation("send", reviewer_id, entered=entered, release=release)
    send_thread, send_errors = _call_in_thread(lambda: adapter.send(reviewer_id, "long turn"))
    assert entered.wait(timeout=_EVENT_TIMEOUT_SECONDS)

    backend.queue_get(producer_id, {"sessionId": producer_id, "status": "running"})
    backend.queue_read_messages(producer_id, list(_RAW_MESSAGES))
    result = adapter.reconcile_session(producer_id)
    assert result.continuity is ClineSessionContinuity.ACCESSIBLE
    assert send_thread.is_alive()

    backend.queue_read_messages(producer_id, list(_RAW_MESSAGES))
    backend.queue_stop(producer_id, ClineSessionNotFoundError("no live incarnation"))
    adapter.recover_session(producer_id, target_mode=ClineConstructionMode.ACT)
    assert send_thread.is_alive()

    release.set()
    send_thread.join(timeout=_EVENT_TIMEOUT_SECONDS)
    assert not send_thread.is_alive()
    assert send_errors == []

    operations = [call.operation for call in backend.calls()]
    assert "connect" not in operations
    assert operations.count("send") == 3


def test_classification_requires_a_demonstrated_exhaustive_history_contract_for_terminal_loss() -> (
    None
):
    """The pure classification policy: terminal loss requires positive
    transcript absence AND positive record absence AND a coherent
    identity-carrying history absence AND a demonstrated exhaustive
    public-history contract. Without that contract — the pinned #71
    baseline — even the fully coherent absence combination stays unknown,
    and every weaker combination does too."""
    absent = ClineSessionEvidence(
        record_present=False,
        transcript=None,
        transcript_absent=True,
        history_presence=False,
    )
    assert (
        classify_session_evidence(absent, checkpoint=None).continuity
        is ClineSessionContinuity.UNKNOWN
    )

    demonstrated = replace(absent, history_exhaustive=True)
    assert (
        classify_session_evidence(demonstrated, checkpoint=None).continuity
        is ClineSessionContinuity.LOST
    )

    for history_presence in (None, True):
        weakened = replace(demonstrated, history_presence=history_presence)
        assert (
            classify_session_evidence(weakened, checkpoint=None).continuity
            is ClineSessionContinuity.UNKNOWN
        )

    contradictory_record = replace(demonstrated, record_present=True)
    assert (
        classify_session_evidence(contradictory_record, checkpoint=None).continuity
        is ClineSessionContinuity.UNKNOWN
    )

    record_only_absence = replace(demonstrated, transcript_absent=False, transcript=None)
    assert (
        classify_session_evidence(record_only_absence, checkpoint=None).continuity
        is ClineSessionContinuity.UNKNOWN
    )


def test_classification_marks_a_shrunk_transcript_contradictory() -> None:
    evidence = ClineSessionEvidence(
        record_present=True,
        transcript=[{"id": "m1"}, {"id": "m2"}, {"id": "m3"}],
        transcript_absent=False,
        history_presence=None,
    )
    result = classify_session_evidence(
        evidence,
        checkpoint=ClineDispatchCheckpoint(transcript_message_count=5),
    )
    assert result.continuity is ClineSessionContinuity.UNKNOWN
    assert result.messages_beyond_checkpoint is None

    consistent = classify_session_evidence(
        evidence,
        checkpoint=ClineDispatchCheckpoint(transcript_message_count=2),
    )
    assert consistent.continuity is ClineSessionContinuity.ACCESSIBLE
    assert consistent.messages_beyond_checkpoint == 1


def test_session_evidence_representation_withholds_the_transcript() -> None:
    """The evidence carrier is representation-safe: the raw transcript it
    holds transiently never enters its str or repr."""
    evidence = ClineSessionEvidence(
        record_present=True,
        transcript=[
            {
                "id": "m1",
                "content": [{"type": "text", "text": "SECRET-EVIDENCE-MARKER"}],
            }
        ],
        transcript_absent=False,
        history_presence=None,
    )

    assert "SECRET-EVIDENCE-MARKER" not in repr(evidence)
    assert "SECRET-EVIDENCE-MARKER" not in str(evidence)


def test_reconciliation_failures_stay_secret_safe() -> None:
    """Reconciliation and recovery failures surface only the normalized
    safe classifications: no scripted backend text, prompt, transcript, or
    token material enters the error string, repr, or exception chain."""
    adapter, backend, resolver, session_id = _ready_adapter()
    _bind_checkout(resolver, session_id)
    backend.queue_get(
        session_id,
        ClineBackendUncertainOutcomeError("SECRET-RECON-MARKER prompt transcript token"),
    )

    with pytest.raises(AgentRuntimeDeliveryUncertainError) as excinfo:
        adapter.reconcile_session(session_id)

    error = excinfo.value
    assert "SECRET-RECON-MARKER" not in str(error)
    assert "SECRET-RECON-MARKER" not in repr(error)
    assert error.__cause__ is None
    assert error.__context__ is None

    backend.queue_read_messages(session_id, list(_RAW_MESSAGES))
    backend.queue_stop(
        session_id,
        ClineBackendUncertainOutcomeError("SECRET-RECON-MARKER transcript"),
    )
    with pytest.raises(AgentRuntimeDeliveryUncertainError) as excinfo:
        adapter.recover_session(session_id, target_mode=ClineConstructionMode.ACT)

    error = excinfo.value
    assert "SECRET-RECON-MARKER" not in str(error)
    assert "SECRET-RECON-MARKER" not in repr(error)
    assert error.__cause__ is None
    assert error.__context__ is None


# --- D6c: activity and usage observation (issue #164) ---

_REASONING_SENTINEL = "SECRET-REASONING-MARKER"
_PROMPT_SENTINEL = "SECRET-PROMPT-MARKER"
_COMMENTARY_TEXT = "Implemented the parser and updated the failing tests."
_TOOL_INPUT_SENTINEL = "SECRET-TOOL-INPUT-MARKER"
_TOOL_OUTPUT_SENTINEL = "SECRET-TOOL-OUTPUT-MARKER"
_TOOL_ERROR_SENTINEL = "SECRET-TOOL-ERROR-MARKER"
_NOTICE_SENTINEL = "SECRET-NOTICE-MARKER"
_DONE_TEXT_SENTINEL = "SECRET-DONE-TEXT-MARKER"
_ERROR_TEXT_SENTINEL = "SECRET-ERROR-TEXT-MARKER"
_SNAPSHOT_SENTINEL = "SECRET-SNAPSHOT-MARKER"

_EXCLUDED_SENTINELS = (
    _REASONING_SENTINEL,
    _PROMPT_SENTINEL,
    _TOOL_INPUT_SENTINEL,
    _TOOL_OUTPUT_SENTINEL,
    _TOOL_ERROR_SENTINEL,
    _NOTICE_SENTINEL,
    _DONE_TEXT_SENTINEL,
    _ERROR_TEXT_SENTINEL,
    _SNAPSHOT_SENTINEL,
)


def _agent_event(session_id: str, event: dict[str, object]) -> ClineSessionEvent:
    return ClineSessionEvent(
        session_id=session_id,
        kind="agent_event",
        payload={"sessionId": session_id, "event": event},
    )


def _status_event(session_id: str, status: object) -> ClineSessionEvent:
    return ClineSessionEvent(
        session_id=session_id,
        kind="status",
        payload={"sessionId": session_id, "status": status},
    )


def _ended_event(session_id: str, reason: object) -> ClineSessionEvent:
    return ClineSessionEvent(
        session_id=session_id,
        kind="ended",
        payload={"sessionId": session_id, "reason": reason, "ts": 1},
    )


def _snapshot_event(session_id: str, snapshot: dict[str, object]) -> ClineSessionEvent:
    return ClineSessionEvent(
        session_id=session_id,
        kind="session_snapshot",
        payload={"sessionId": session_id, "snapshot": snapshot},
    )


def _chunk_event(session_id: str, chunk: object) -> ClineSessionEvent:
    return ClineSessionEvent(
        session_id=session_id,
        kind="chunk",
        payload={"sessionId": session_id, "stream": "agent", "chunk": chunk, "ts": 1},
    )


def _collector() -> tuple[
    list[ClineActivityObservation], Callable[[ClineActivityObservation], None]
]:
    """A recording listener and its observation list for one subscription."""
    observations: list[ClineActivityObservation] = []
    return observations, observations.append


def _bound_adapter(
    *,
    session_id: str = "cline-session-1",
    context: ClineConstructionContext | None = None,
) -> tuple[ClineAdapter, FakeClineSdkBackend, ScriptedResolver, str]:
    """A fresh adapter over a durably bound session with no construction.

    The resolver carries the bound Task/role context; the backend knows no
    session, so any construction or session-addressed effectful call would
    fail loudly — observation seams must work without them.
    """
    context = context if context is not None else _context()
    resolver = ScriptedResolver(bound={session_id: context})
    backend = FakeClineSdkBackend()
    return ClineAdapter(backend, resolver), backend, resolver, session_id


def _kinds(observations: list[ClineActivityObservation]) -> list[ClineActivityKind]:
    return [observation.kind for observation in observations]


def _assert_sentinels_absent(observations: list[ClineActivityObservation]) -> None:
    """No excluded sentinel appears in any observation representation."""
    rendered = repr(observations)
    for sentinel in _EXCLUDED_SENTINELS:
        assert sentinel not in rendered
        for observation in observations:
            assert sentinel not in repr(observation)
            assert sentinel not in str(observation)


def test_subscribe_activity_resolves_the_durable_binding_without_construction_effects() -> None:
    adapter, backend, resolver, session_id = _bound_adapter()
    observations, listener = _collector()

    subscription = adapter.subscribe_activity(session_id, listener)
    backend.emit(
        _agent_event(session_id, {"type": "iteration_start", "iteration": 1}),
    )

    assert subscription.active is True
    assert resolver.bound_queries == [session_id]
    assert [call.operation for call in backend.calls()] == ["subscribe"]
    assert backend.calls()[0].session_id == session_id
    assert _kinds(observations) == [ClineActivityKind.ITERATION_STARTED]
    assert observations[0].session_id == session_id
    assert isinstance(observations[0].detail, ClineIterationActivity)
    assert observations[0].detail.iteration == 1


def test_read_activity_snapshot_reads_public_usage_without_construction_effects() -> None:
    adapter, backend, resolver, session_id = _bound_adapter()
    resolver.bound_queries.clear()

    backend.queue_get_accumulated_usage(
        session_id,
        {
            "usage": {"inputTokens": 10, "outputTokens": 20, "totalCost": 0.1},
            "aggregateUsage": {"inputTokens": 15, "outputTokens": 25, "totalCost": 0.2},
        },
    )
    snapshot = adapter.read_activity_snapshot(session_id)

    assert resolver.bound_queries == [session_id]
    assert [call.operation for call in backend.calls()] == ["get_accumulated_usage"]
    assert backend.calls()[0].session_id == session_id
    assert snapshot.session_id == session_id
    assert snapshot.usage is not None
    assert snapshot.usage.input_tokens == 10
    assert snapshot.aggregate_usage is not None
    assert snapshot.aggregate_usage.input_tokens == 15


def test_subscribe_activity_refuses_an_unbound_session_before_backend_effects() -> None:
    adapter, backend, _resolver, _session_id = _bound_adapter()
    observations, listener = _collector()

    with pytest.raises(AgentRuntimeConfigurationRejectedError):
        adapter.subscribe_activity("unbound-session", listener)

    assert backend.calls() == ()
    assert observations == []


def test_read_activity_snapshot_refuses_an_unbound_session_before_backend_effects() -> None:
    adapter, backend, _resolver, _session_id = _bound_adapter()

    with pytest.raises(AgentRuntimeConfigurationRejectedError):
        adapter.read_activity_snapshot("unbound-session")

    assert backend.calls() == ()


def test_activity_subscriptions_do_not_cross_feed_between_bound_sessions() -> None:
    adapter, backend, resolver, session_id = _bound_adapter()
    other_session = "cline-session-2"
    resolver.set_bound(other_session, _context(_identity(task_id=_OTHER_TASK_ID)))
    producer_observations, producer_listener = _collector()
    reviewer_observations, reviewer_listener = _collector()
    adapter.subscribe_activity(session_id, producer_listener)
    adapter.subscribe_activity(other_session, reviewer_listener)

    backend.emit(_agent_event(session_id, {"type": "iteration_start", "iteration": 1}))
    backend.emit(_agent_event(other_session, {"type": "iteration_start", "iteration": 2}))

    assert _kinds(producer_observations) == [ClineActivityKind.ITERATION_STARTED]
    assert producer_observations[0].session_id == session_id
    assert _kinds(reviewer_observations) == [ClineActivityKind.ITERATION_STARTED]
    assert reviewer_observations[0].session_id == other_session


def test_project_session_event_drops_events_addressed_to_another_session() -> None:
    event = _agent_event("cline-session-1", {"type": "iteration_start", "iteration": 1})

    assert project_session_event(event, expected_session_id="cline-session-2") == []


def test_reasoning_content_projects_indicators_without_content() -> None:
    adapter, backend, _resolver, session_id = _bound_adapter()
    observations, listener = _collector()
    adapter.subscribe_activity(session_id, listener)

    backend.emit(
        _agent_event(
            session_id,
            {
                "type": "content_start",
                "contentType": "reasoning",
                "text": _REASONING_SENTINEL,
                "reasoning": _REASONING_SENTINEL,
                "accumulated": _REASONING_SENTINEL,
                "redacted": False,
            },
        )
    )
    backend.emit(
        _agent_event(
            session_id,
            {
                "type": "content_end",
                "contentType": "reasoning",
                "reasoning": _REASONING_SENTINEL,
                "text": _REASONING_SENTINEL,
            },
        )
    )

    assert _kinds(observations) == [
        ClineActivityKind.THINKING_STARTED,
        ClineActivityKind.THINKING_ENDED,
    ]
    assert all(observation.detail is None for observation in observations)
    _assert_sentinels_absent(observations)


def test_completed_assistant_text_reaches_only_its_typed_commentary_field() -> None:
    adapter, backend, _resolver, session_id = _bound_adapter()
    observations, listener = _collector()
    adapter.subscribe_activity(session_id, listener)

    backend.emit(
        _agent_event(
            session_id,
            {"type": "content_start", "contentType": "text", "text": _COMMENTARY_TEXT},
        )
    )
    backend.emit(
        _agent_event(
            session_id,
            {"type": "content_end", "contentType": "text", "text": _COMMENTARY_TEXT},
        )
    )

    assert _kinds(observations) == [ClineActivityKind.COMMENTARY]
    detail = observations[0].detail
    assert isinstance(detail, ClineCommentaryActivity)
    assert detail.text == _COMMENTARY_TEXT
    assert detail.truncated is False


def test_formal_response_envelopes_are_not_relabelled_as_commentary() -> None:
    adapter, backend, _resolver, session_id = _bound_adapter()
    observations, listener = _collector()
    adapter.subscribe_activity(session_id, listener)

    for family in (
        "session_ready",
        "plan_result",
        "review_result",
        "implementation_result",
        "workspace_result",
        "pr_result",
    ):
        payload = json.dumps({"type": family, "schema_version": 1, "status": "READY"})
        backend.emit(
            _agent_event(
                session_id,
                {"type": "content_end", "contentType": "text", "text": payload},
            )
        )

    assert observations == []


def test_fenced_formal_responses_are_not_relabelled_as_commentary() -> None:
    adapter, backend, _resolver, session_id = _bound_adapter()
    observations, listener = _collector()
    adapter.subscribe_activity(session_id, listener)

    for family in ("plan_result", "review_result", "implementation_result"):
        payload = json.dumps({"type": family, "schema_version": 1})
        backend.emit(
            _agent_event(
                session_id,
                {
                    "type": "content_end",
                    "contentType": "text",
                    "text": f"```json\n{payload}\n```",
                },
            )
        )

    assert observations == []


def test_fenced_ordinary_json_still_projects_as_commentary() -> None:
    adapter, backend, _resolver, session_id = _bound_adapter()
    observations, listener = _collector()
    adapter.subscribe_activity(session_id, listener)

    fenced = f"```json\n{json.dumps({'type': 'unrelated_note', 'count': 2})}\n```"
    backend.emit(
        _agent_event(
            session_id,
            {"type": "content_end", "contentType": "text", "text": fenced},
        )
    )

    assert _kinds(observations) == [ClineActivityKind.COMMENTARY]
    detail = observations[0].detail
    assert isinstance(detail, ClineCommentaryActivity)
    assert detail.text == fenced
    assert detail.truncated is False


def test_non_formal_json_and_prose_still_project_as_commentary() -> None:
    adapter, backend, _resolver, session_id = _bound_adapter()
    observations, listener = _collector()
    adapter.subscribe_activity(session_id, listener)

    backend.emit(
        _agent_event(
            session_id,
            {
                "type": "content_end",
                "contentType": "text",
                "text": json.dumps({"type": "unrelated_note", "count": 2}),
            },
        )
    )
    backend.emit(
        _agent_event(
            session_id,
            {"type": "content_end", "contentType": "text", "text": _COMMENTARY_TEXT},
        )
    )

    assert _kinds(observations) == [ClineActivityKind.COMMENTARY, ClineActivityKind.COMMENTARY]
    details = [observation.detail for observation in observations]
    assert all(isinstance(detail, ClineCommentaryActivity) for detail in details)


def test_commentary_is_bounded_with_explicit_truncation() -> None:
    adapter, backend, _resolver, session_id = _bound_adapter()
    observations, listener = _collector()
    adapter.subscribe_activity(session_id, listener)

    backend.emit(
        _agent_event(
            session_id,
            {"type": "content_end", "contentType": "text", "text": "x" * 2500},
        )
    )

    detail = observations[0].detail
    assert isinstance(detail, ClineCommentaryActivity)
    assert detail.truncated is True
    assert len(detail.text) == 2000
    assert detail.text == "x" * 2000


def test_unknown_and_malformed_events_project_nothing() -> None:
    adapter, backend, _resolver, session_id = _bound_adapter()
    observations, listener = _collector()
    adapter.subscribe_activity(session_id, listener)

    events = [
        ClineSessionEvent(session_id, "agent_event", {"sessionId": session_id}),
        ClineSessionEvent(
            session_id,
            "agent_event",
            {"sessionId": session_id, "event": "not-a-dict"},
        ),
        ClineSessionEvent(
            session_id,
            "agent_event",
            {"sessionId": session_id, "event": {"type": "mystery_event"}},
        ),
        ClineSessionEvent(
            session_id,
            "agent_event",
            {"sessionId": session_id, "event": {"type": "content_start"}},
        ),
        ClineSessionEvent(
            session_id,
            "agent_event",
            {"sessionId": session_id, "event": {"type": "iteration_start", "iteration": 0}},
        ),
        ClineSessionEvent(
            session_id,
            "agent_event",
            {
                "sessionId": session_id,
                "event": {
                    "type": "content_update",
                    "contentType": "text",
                    "text": _PROMPT_SENTINEL,
                },
            },
        ),
        _agent_event(session_id, {"type": "content_start", "contentType": "media"}),
        _chunk_event(session_id, _PROMPT_SENTINEL),
        ClineSessionEvent(
            session_id, "hook", {"sessionId": session_id, "hookEventName": "tool_call"}
        ),
        ClineSessionEvent(
            session_id,
            "team_progress",
            {"sessionId": session_id, "teamName": "t", "lifecycle": {}, "summary": {}},
        ),
        ClineSessionEvent(
            session_id,
            "pending_prompts",
            {"sessionId": session_id, "prompts": []},
        ),
        ClineSessionEvent(session_id, "made_up_kind", {"sessionId": session_id}),
    ]
    for event in events:
        backend.emit(event)

    assert observations == []


def test_runtime_status_and_ended_events_project_bounded_bookkeeping() -> None:
    adapter, backend, _resolver, session_id = _bound_adapter()
    observations, listener = _collector()
    adapter.subscribe_activity(session_id, listener)

    backend.emit(_status_event(session_id, "running"))
    backend.emit(_status_event(session_id, "not a machine identifier"))
    backend.emit(_ended_event(session_id, "completed"))
    backend.emit(_ended_event(session_id, "arbitrary runtime prose"))
    backend.emit(
        _snapshot_event(
            session_id,
            {
                "status": "running",
                "prompt": _PROMPT_SENTINEL,
                "messages": [_PROMPT_SENTINEL],
                "workspace": {"cwd": _SNAPSHOT_SENTINEL},
                "metadata": {"nested": _SNAPSHOT_SENTINEL},
            },
        )
    )
    backend.emit(_snapshot_event(session_id, {"prompt": _PROMPT_SENTINEL}))

    assert _kinds(observations) == [
        ClineActivityKind.RUNTIME_STATUS,
        ClineActivityKind.RUNTIME_ENDED,
        ClineActivityKind.RUNTIME_ENDED,
        ClineActivityKind.RUNTIME_STATUS,
    ]
    assert isinstance(observations[0].detail, ClineRuntimeStatusActivity)
    assert observations[0].detail.status == "running"
    assert isinstance(observations[1].detail, ClineRuntimeEndedActivity)
    assert observations[1].detail.reason == "completed"
    assert isinstance(observations[2].detail, ClineRuntimeEndedActivity)
    assert observations[2].detail.reason is None
    assert isinstance(observations[3].detail, ClineRuntimeStatusActivity)
    _assert_sentinels_absent(observations)


def test_command_tool_lifecycle_projects_safe_details_without_content() -> None:
    adapter, backend, _resolver, session_id = _bound_adapter()
    observations, listener = _collector()
    adapter.subscribe_activity(session_id, listener)

    backend.emit(
        _agent_event(
            session_id,
            {
                "type": "content_start",
                "contentType": "tool",
                "toolName": "run_commands",
                "toolCallId": "call-1",
                "input": {"command": _TOOL_INPUT_SENTINEL},
            },
        )
    )
    backend.emit(
        _agent_event(
            session_id,
            {
                "type": "content_update",
                "contentType": "tool",
                "toolName": "run_commands",
                "toolCallId": "call-1",
                "update": _TOOL_OUTPUT_SENTINEL,
            },
        )
    )
    backend.emit(
        _agent_event(
            session_id,
            {
                "type": "content_end",
                "contentType": "tool",
                "toolName": "run_commands",
                "toolCallId": "call-1",
                "durationMs": 120,
                "output": _TOOL_OUTPUT_SENTINEL,
            },
        )
    )

    assert _kinds(observations) == [
        ClineActivityKind.TOOL_STARTED,
        ClineActivityKind.TOOL_PROGRESS,
        ClineActivityKind.TOOL_COMPLETED,
    ]
    started = observations[0].detail
    assert isinstance(started, ClineToolActivity)
    assert started.category is ClineToolCategory.COMMAND
    assert started.tool_name == "run_commands"
    assert started.tool_call_id == "call-1"
    assert started.duration_ms is None
    completed = observations[2].detail
    assert isinstance(completed, ClineToolActivity)
    assert completed.duration_ms == 120
    _assert_sentinels_absent(observations)


def test_file_and_mcp_tool_names_classify_conservatively() -> None:
    adapter, backend, _resolver, session_id = _bound_adapter()
    observations, listener = _collector()
    adapter.subscribe_activity(session_id, listener)

    backend.emit(
        _agent_event(
            session_id,
            {
                "type": "content_start",
                "contentType": "tool",
                "toolName": "apply_patch",
                "toolCallId": "call-2",
                "input": {"path": "src/app.py"},
            },
        )
    )
    backend.emit(
        _agent_event(
            session_id,
            {
                "type": "content_start",
                "contentType": "tool",
                "toolName": "Github__pull_request_read",
                "toolCallId": "call-3",
                "input": {"method": "get", "extras": _TOOL_INPUT_SENTINEL},
            },
        )
    )
    backend.emit(
        _agent_event(
            session_id,
            {"type": "content_start", "contentType": "tool", "toolName": "some_custom_tool"},
        )
    )
    backend.emit(_agent_event(session_id, {"type": "content_start", "contentType": "tool"}))

    assert _kinds(observations) == [ClineActivityKind.TOOL_STARTED] * 4
    first, second, third, fourth = (observation.detail for observation in observations)
    assert isinstance(first, ClineToolActivity)
    assert isinstance(second, ClineToolActivity)
    assert isinstance(third, ClineToolActivity)
    assert isinstance(fourth, ClineToolActivity)
    assert first.category is ClineToolCategory.FILE
    assert first.tool_name == "apply_patch"
    assert second.category is ClineToolCategory.MCP
    assert second.tool_name == "Github__pull_request_read"
    assert third.category is ClineToolCategory.OTHER
    assert third.tool_name == "some_custom_tool"
    assert fourth.category is ClineToolCategory.OTHER
    assert fourth.tool_name is None
    _assert_sentinels_absent(observations)


def test_failed_tool_end_projects_failure_without_error_text() -> None:
    adapter, backend, _resolver, session_id = _bound_adapter()
    observations, listener = _collector()
    adapter.subscribe_activity(session_id, listener)

    backend.emit(
        _agent_event(
            session_id,
            {
                "type": "content_start",
                "contentType": "tool",
                "toolName": "run_commands",
                "toolCallId": "call-1",
            },
        )
    )
    backend.emit(
        _agent_event(
            session_id,
            {
                "type": "content_end",
                "contentType": "tool",
                "toolName": "run_commands",
                "toolCallId": "call-1",
                "durationMs": 30,
                "error": _TOOL_ERROR_SENTINEL,
                "output": _TOOL_OUTPUT_SENTINEL,
            },
        )
    )

    assert _kinds(observations) == [ClineActivityKind.TOOL_STARTED, ClineActivityKind.TOOL_FAILED]
    _assert_sentinels_absent(observations)


def test_missing_tool_end_never_fabricates_a_completion() -> None:
    adapter, backend, _resolver, session_id = _bound_adapter()
    observations, listener = _collector()
    adapter.subscribe_activity(session_id, listener)

    backend.emit(
        _agent_event(
            session_id,
            {
                "type": "content_start",
                "contentType": "tool",
                "toolName": "run_commands",
                "toolCallId": "call-1",
            },
        )
    )

    assert _kinds(observations) == [ClineActivityKind.TOOL_STARTED]


def test_notice_done_and_error_events_project_safe_classifications_only() -> None:
    adapter, backend, _resolver, session_id = _bound_adapter()
    observations, listener = _collector()
    adapter.subscribe_activity(session_id, listener)

    backend.emit(
        _agent_event(
            session_id,
            {
                "type": "notice",
                "noticeType": "recovery",
                "reason": "api_error",
                "message": _NOTICE_SENTINEL,
                "metadata": {"nested": _NOTICE_SENTINEL},
            },
        )
    )
    backend.emit(
        _agent_event(
            session_id,
            {"type": "notice", "noticeType": "mystery", "message": _NOTICE_SENTINEL},
        )
    )
    backend.emit(
        _agent_event(
            session_id,
            {
                "type": "done",
                "reason": "completed",
                "text": _DONE_TEXT_SENTINEL,
                "iterations": 3,
            },
        )
    )
    backend.emit(
        _agent_event(
            session_id,
            {
                "type": "done",
                "reason": "something_unrecognized",
                "text": _DONE_TEXT_SENTINEL,
                "iterations": 1,
            },
        )
    )
    backend.emit(
        _agent_event(
            session_id,
            {
                "type": "error",
                "error": {"message": _ERROR_TEXT_SENTINEL},
                "errorClass": "provider_overloaded",
                "recoverable": True,
                "iteration": 2,
            },
        )
    )

    assert _kinds(observations) == [
        ClineActivityKind.NOTICE,
        ClineActivityKind.TURN_FINISHED,
        ClineActivityKind.TURN_FINISHED,
        ClineActivityKind.RUNTIME_ERROR,
    ]
    notice = observations[0].detail
    assert isinstance(notice, ClineNoticeActivity)
    assert notice.category is ClineNoticeCategory.RECOVERY
    assert notice.reason == "api_error"
    finished = observations[1].detail
    assert isinstance(finished, ClineFinishActivity)
    assert finished.reason is ClineTurnFinishReason.COMPLETED
    assert finished.iterations == 3
    unknown_finish = observations[2].detail
    assert isinstance(unknown_finish, ClineFinishActivity)
    assert unknown_finish.reason is ClineTurnFinishReason.UNKNOWN
    runtime_error = observations[3].detail
    assert isinstance(runtime_error, ClineRuntimeErrorActivity)
    assert runtime_error.error_class == "provider_overloaded"
    assert runtime_error.recoverable is True
    assert runtime_error.iteration == 2
    _assert_sentinels_absent(observations)


def test_live_usage_event_projects_per_turn_figures_and_totals() -> None:
    adapter, backend, _resolver, session_id = _bound_adapter()
    observations, listener = _collector()
    adapter.subscribe_activity(session_id, listener)

    backend.emit(
        _agent_event(
            session_id,
            {
                "type": "usage",
                "inputTokens": 10,
                "outputTokens": 20,
                "cacheReadTokens": 5,
                "cacheWriteTokens": 6,
                "cost": 0.01,
                "totalInputTokens": 100,
                "totalOutputTokens": 200,
                "totalCacheReadTokens": 50,
                "totalCacheWriteTokens": 60,
                "totalCost": 0.5,
            },
        )
    )

    assert _kinds(observations) == [ClineActivityKind.USAGE_OBSERVED]
    detail = observations[0].detail
    assert isinstance(detail, ClineUsageActivity)
    assert detail.input_tokens == 10
    assert detail.output_tokens == 20
    assert detail.cache_read_tokens == 5
    assert detail.cache_write_tokens == 6
    assert detail.cost == 0.01
    assert detail.total_input_tokens == 100
    assert detail.total_output_tokens == 200
    assert detail.total_cache_read_tokens == 50
    assert detail.total_cache_write_tokens == 60
    assert detail.total_cost == 0.5


def test_live_usage_event_leaves_unusable_fields_unreported() -> None:
    adapter, backend, _resolver, session_id = _bound_adapter()
    observations, listener = _collector()
    adapter.subscribe_activity(session_id, listener)

    backend.emit(
        _agent_event(
            session_id,
            {
                "type": "usage",
                "inputTokens": "many",
                "outputTokens": float("nan"),
                "cacheWriteTokens": -1,
                "cost": -0.5,
                "totalInputTokens": True,
                "totalOutputTokens": 30,
            },
        )
    )

    detail = observations[0].detail
    assert isinstance(detail, ClineUsageActivity)
    assert detail.input_tokens is None
    assert detail.output_tokens is None
    assert detail.cache_write_tokens is None
    assert detail.cost is None
    assert detail.total_input_tokens is None
    assert detail.total_output_tokens == 30


def test_snapshot_usage_parses_both_aggregation_scopes() -> None:
    adapter, backend, _resolver, session_id = _bound_adapter()

    backend.queue_get_accumulated_usage(
        session_id,
        {
            "usage": {
                "inputTokens": 10,
                "outputTokens": 20,
                "cacheReadTokens": 5,
                "cacheWriteTokens": 6,
                "totalCost": 0.1,
            },
            "aggregateUsage": {
                "inputTokens": 15,
                "outputTokens": 25,
                "cacheReadTokens": 7,
                "cacheWriteTokens": 8,
                "totalCost": 0.2,
            },
        },
    )
    snapshot = adapter.read_activity_snapshot(session_id)

    assert isinstance(snapshot.usage, ClineAccumulatedUsage)
    assert isinstance(snapshot.aggregate_usage, ClineAccumulatedUsage)
    assert snapshot.usage is not snapshot.aggregate_usage
    assert snapshot.usage.input_tokens == 10
    assert snapshot.usage.output_tokens == 20
    assert snapshot.usage.total_cost == 0.1
    assert snapshot.aggregate_usage.input_tokens == 15
    assert snapshot.aggregate_usage.output_tokens == 25
    assert snapshot.aggregate_usage.total_cost == 0.2


def test_snapshot_usage_preserves_scope_absence_without_substitution() -> None:
    adapter, backend, _resolver, session_id = _bound_adapter()
    usage_scope = {"inputTokens": 10, "outputTokens": 20, "totalCost": 0.1}
    aggregate_scope = {"inputTokens": 15, "outputTokens": 25, "totalCost": 0.2}

    backend.queue_get_accumulated_usage(session_id, {"usage": usage_scope})
    root_only = adapter.read_activity_snapshot(session_id)
    backend.queue_get_accumulated_usage(session_id, {"aggregateUsage": aggregate_scope})
    aggregate_only = adapter.read_activity_snapshot(session_id)
    backend.queue_get_accumulated_usage(session_id, None)
    undefined = adapter.read_activity_snapshot(session_id)
    backend.queue_get_accumulated_usage(session_id, {})
    empty = adapter.read_activity_snapshot(session_id)
    backend.queue_get_accumulated_usage(session_id, {"usage": "not-an-object"})
    unusable = adapter.read_activity_snapshot(session_id)

    assert isinstance(root_only.usage, ClineAccumulatedUsage)
    assert root_only.usage.input_tokens == 10
    assert root_only.aggregate_usage is None
    assert aggregate_only.usage is None
    assert isinstance(aggregate_only.aggregate_usage, ClineAccumulatedUsage)
    assert aggregate_only.aggregate_usage.input_tokens == 15
    assert undefined.usage is None
    assert undefined.aggregate_usage is None
    assert empty.usage is None
    assert empty.aggregate_usage is None
    assert unusable.usage is None
    assert unusable.aggregate_usage is None


def test_snapshot_usage_leaves_unusable_fields_unreported() -> None:
    adapter, backend, _resolver, session_id = _bound_adapter()

    backend.queue_get_accumulated_usage(
        session_id,
        {
            "aggregateUsage": {
                "inputTokens": float("nan"),
                "outputTokens": "many",
                "cacheReadTokens": True,
                "cacheWriteTokens": -2,
                "totalCost": 0.3,
            }
        },
    )
    snapshot = adapter.read_activity_snapshot(session_id)

    assert snapshot.usage is None
    assert snapshot.aggregate_usage is not None
    assert snapshot.aggregate_usage.input_tokens is None
    assert snapshot.aggregate_usage.output_tokens is None
    assert snapshot.aggregate_usage.cache_read_tokens is None
    assert snapshot.aggregate_usage.cache_write_tokens is None
    assert snapshot.aggregate_usage.total_cost == 0.3

    backend.queue_get_accumulated_usage(
        session_id,
        {"usage": {"inputTokens": -100, "outputTokens": 20}},
    )
    negative_tokens = adapter.read_activity_snapshot(session_id)

    assert isinstance(negative_tokens.usage, ClineAccumulatedUsage)
    assert negative_tokens.usage.input_tokens is None
    assert negative_tokens.usage.output_tokens == 20


def test_snapshot_usage_after_a_hub_restart_is_a_new_observation() -> None:
    adapter, backend, _resolver, session_id = _bound_adapter()

    backend.queue_get_accumulated_usage(
        session_id,
        {"aggregateUsage": {"inputTokens": 1000, "outputTokens": 2000, "totalCost": 5.0}},
    )
    before = adapter.read_activity_snapshot(session_id)
    backend.queue_get_accumulated_usage(
        session_id,
        {"aggregateUsage": {"inputTokens": 40, "outputTokens": 80, "totalCost": 0.2}},
    )
    after = adapter.read_activity_snapshot(session_id)

    assert before.aggregate_usage is not None
    assert after.aggregate_usage is not None
    assert before.aggregate_usage.total_cost == 5.0
    assert before.aggregate_usage.input_tokens == 1000
    assert after.aggregate_usage.total_cost == 0.2
    assert after.aggregate_usage.input_tokens == 40


def test_snapshot_read_failures_use_normalized_classifications() -> None:
    adapter, backend, _resolver, session_id = _bound_adapter()

    backend.queue_get_accumulated_usage(
        session_id,
        ClineSessionNotFoundError("session 'x' is not known to this backend instance"),
    )
    with pytest.raises(AgentSessionNotFoundError):
        adapter.read_activity_snapshot(session_id)

    backend.queue_get_accumulated_usage(
        session_id,
        ClineBackendUncertainOutcomeError("SECRET-USAGE-MARKER"),
    )
    with pytest.raises(AgentRuntimeDeliveryUncertainError) as excinfo:
        adapter.read_activity_snapshot(session_id)

    assert "SECRET-USAGE-MARKER" not in str(excinfo.value)
    assert "SECRET-USAGE-MARKER" not in repr(excinfo.value)
    assert excinfo.value.__cause__ is None
    assert excinfo.value.__context__ is None


def test_project_usage_summary_parses_the_pinned_wrapper_conservatively() -> None:
    usage_scope = {"inputTokens": 10, "outputTokens": 20, "totalCost": 0.1}
    aggregate_scope = {"inputTokens": 15, "outputTokens": 25, "totalCost": 0.2}

    usage, aggregate = project_usage_summary(
        {"usage": usage_scope, "aggregateUsage": aggregate_scope}
    )
    assert usage is not None and aggregate is not None
    assert usage.input_tokens == 10 and aggregate.input_tokens == 15

    usage, aggregate = project_usage_summary({"usage": usage_scope})
    assert usage is not None and usage.output_tokens == 20
    assert aggregate is None

    usage, aggregate = project_usage_summary({"aggregateUsage": aggregate_scope})
    assert usage is None
    assert aggregate is not None and aggregate.total_cost == 0.2

    assert project_usage_summary(None) == (None, None)
    assert project_usage_summary("not-an-object") == (None, None)  # type: ignore[arg-type]
    assert project_usage_summary({}) == (None, None)
    assert project_usage_summary({"usage": 5}) == (None, None)
    nan_scope = {"inputTokens": float("nan"), "outputTokens": True}
    assert project_usage_summary({"usage": nan_scope}) == (None, None)


def test_unsubscribe_is_explicit_idempotent_and_stops_only_its_delivery() -> None:
    adapter, backend, _resolver, session_id = _bound_adapter()
    observations, listener = _collector()
    subscription = adapter.subscribe_activity(session_id, listener)

    subscription.unsubscribe()
    subscription.unsubscribe()
    backend.emit(_agent_event(session_id, {"type": "iteration_start", "iteration": 1}))

    assert subscription.active is False
    assert observations == []
    assert [call.operation for call in backend.calls()].count("subscribe") == 1


def test_a_disposed_attachment_invalidates_subscriptions_without_replacement() -> None:
    adapter, backend, _resolver, session_id = _bound_adapter()
    observations, listener = _collector()
    adapter.subscribe_activity(session_id, listener)

    backend.dispose()
    backend.emit(_agent_event(session_id, {"type": "iteration_start", "iteration": 1}))

    assert observations == []
    assert [call.operation for call in backend.calls()].count("subscribe") == 1


def test_observations_flow_during_a_parked_send() -> None:
    adapter, backend, resolver, session_id = _ready_adapter()
    resolver.set_bound(session_id, _context())
    entered, release = threading.Event(), threading.Event()
    backend.gate_operation("send", session_id, entered=entered, release=release)
    backend.queue_send(session_id, _completed("Continued the authorized work."))
    send_errors: list[BaseException] = []

    def _parked_send() -> None:
        try:
            adapter.send(session_id, "Continue the work.")
        except BaseException as error:  # noqa: BLE001 - captured for assertion
            send_errors.append(error)

    sender = threading.Thread(target=_parked_send)
    sender.start()
    assert entered.wait(timeout=5)

    observations, listener = _collector()
    adapter.subscribe_activity(session_id, listener)
    backend.emit(_agent_event(session_id, {"type": "iteration_start", "iteration": 1}))
    backend.queue_get_accumulated_usage(session_id, {"usage": {"inputTokens": 1}})
    snapshot = adapter.read_activity_snapshot(session_id)

    assert _kinds(observations) == [ClineActivityKind.ITERATION_STARTED]
    assert snapshot.usage is not None
    assert snapshot.usage.input_tokens == 1

    release.set()
    sender.join(timeout=5)

    assert not sender.is_alive()
    assert send_errors == []


def test_projected_activity_carries_only_the_permitted_content() -> None:
    adapter, backend, _resolver, session_id = _bound_adapter()
    observations, listener = _collector()
    adapter.subscribe_activity(session_id, listener)

    backend.emit(
        _agent_event(
            session_id,
            {
                "type": "content_start",
                "contentType": "reasoning",
                "text": _REASONING_SENTINEL,
                "reasoning": _REASONING_SENTINEL,
                "accumulated": _REASONING_SENTINEL,
            },
        )
    )
    backend.emit(
        _agent_event(
            session_id,
            {"type": "content_end", "contentType": "text", "text": _COMMENTARY_TEXT},
        )
    )
    backend.emit(
        _agent_event(
            session_id,
            {
                "type": "content_start",
                "contentType": "tool",
                "toolName": "run_commands",
                "toolCallId": "call-1",
                "input": {"command": _TOOL_INPUT_SENTINEL},
            },
        )
    )
    backend.emit(
        _agent_event(
            session_id,
            {
                "type": "content_update",
                "contentType": "tool",
                "toolName": "run_commands",
                "toolCallId": "call-1",
                "update": _TOOL_OUTPUT_SENTINEL,
            },
        )
    )
    backend.emit(
        _agent_event(
            session_id,
            {
                "type": "content_end",
                "contentType": "tool",
                "toolName": "run_commands",
                "toolCallId": "call-1",
                "durationMs": 5,
                "output": _TOOL_OUTPUT_SENTINEL,
                "error": _TOOL_ERROR_SENTINEL,
            },
        )
    )
    backend.emit(
        _agent_event(
            session_id,
            {"type": "notice", "noticeType": "status", "message": _NOTICE_SENTINEL},
        )
    )
    backend.emit(
        _agent_event(
            session_id,
            {"type": "done", "reason": "completed", "text": _DONE_TEXT_SENTINEL, "iterations": 3},
        )
    )
    backend.emit(
        _agent_event(
            session_id,
            {
                "type": "error",
                "error": {"message": _ERROR_TEXT_SENTINEL},
                "recoverable": False,
                "iteration": 2,
            },
        )
    )
    backend.emit(
        _snapshot_event(
            session_id,
            {
                "status": "running",
                "prompt": _PROMPT_SENTINEL,
                "messages": [_PROMPT_SENTINEL],
                "workspace": {"cwd": _SNAPSHOT_SENTINEL},
            },
        )
    )

    assert _kinds(observations) == [
        ClineActivityKind.THINKING_STARTED,
        ClineActivityKind.COMMENTARY,
        ClineActivityKind.TOOL_STARTED,
        ClineActivityKind.TOOL_PROGRESS,
        ClineActivityKind.TOOL_FAILED,
        ClineActivityKind.NOTICE,
        ClineActivityKind.TURN_FINISHED,
        ClineActivityKind.RUNTIME_ERROR,
        ClineActivityKind.RUNTIME_STATUS,
    ]
    commentary = observations[1].detail
    assert isinstance(commentary, ClineCommentaryActivity)
    assert commentary.text == _COMMENTARY_TEXT
    _assert_sentinels_absent(observations)


def test_activity_surface_validates_its_inputs() -> None:
    adapter, _backend, _resolver, session_id = _bound_adapter()
    _observations, listener = _collector()

    with pytest.raises(ValueError):
        adapter.subscribe_activity("", listener)
    with pytest.raises(ValueError):
        adapter.subscribe_activity("   ", listener)
    with pytest.raises(ValueError):
        adapter.subscribe_activity(session_id, "not-callable")  # type: ignore[arg-type]
    with pytest.raises(ValueError):
        adapter.read_activity_snapshot("")
    with pytest.raises(ValueError):
        adapter.read_activity_snapshot("   ")
