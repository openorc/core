"""Deterministic adapter-contract tests for the shared runtime-neutral Agent
Runtime session contract (issue #67).

These tests exercise the behavior owned by the adapter boundary through a
minimal scripted adapter that satisfies the shared ABC without any Cline
types. They reuse the already-covered #65 parser/validator behavior and #129
control-model behavior instead of re-testing their full matrices, and they
require no database, network, or live runtime.
"""

from __future__ import annotations

import ast
import json
from collections.abc import Callable
from pathlib import Path
from typing import cast
from uuid import uuid4

import pytest

from openorc.adapters.agent_runtime import (
    AgentRuntimeAdapter,
    AgentRuntimeConfigurationRejectedError,
    AgentRuntimeDeliveryUncertainError,
    AgentRuntimeError,
    AgentRuntimeProtocolFailureError,
    AgentRuntimeTimeoutError,
    AgentRuntimeTransportError,
    AgentRuntimeUnavailableError,
    AgentRuntimeUncertainOutcomeError,
    AgentSessionCreated,
    AgentSessionCreationRequest,
    AgentSessionNotFoundError,
    AgentTextResponse,
)
from openorc.adapters.agent_runtime import errors as agent_runtime_errors
from openorc.domain.connections import WorkflowRole
from openorc.protocol.errors import (
    FormalObjectNotFoundError,
    MalformedFormalObjectError,
    ProtocolError,
    SchemaInvalidPayloadError,
    UnexpectedResponseTypeError,
)
from openorc.protocol.interaction import (
    ImplementInteraction,
    Interaction,
    InteractionKind,
    PlanInteraction,
    PrComposeInteraction,
)
from openorc.protocol.models import (
    IMPLEMENTATION_RESULT_FAMILY,
    PLAN_RESULT_FAMILY,
    PR_RESULT_FAMILY,
    SESSION_READY_FAMILY,
    FormalResponse,
    ImplementationResult,
    PlanResult,
    PrResult,
)

# A runtime-style outer "task completed" envelope: runtime-native mechanics
# that the concrete adapter must strip before any shared protocol parsing.
_ENVELOPE_PREFIX = "task-completed<<"
_ENVELOPE_SUFFIX = ">>end"

# Sensitive marker values planted into payload/diagnostic surfaces that
# normalized messages must never echo.
_SECRET_INITIALIZATION_MARKER = "SECRET-INIT-MARKER"
_SECRET_RESPONSE_MARKER = "SECRET-RESPONSE-MARKER"
_SECRET_PROVIDER_TEXT_MARKER = "SECRET-PROVIDER-MARKER"


def _session_ready_payload() -> str:
    return json.dumps({"type": "session_ready", "schema_version": 1, "status": "READY"})


def _plan_result_payload() -> str:
    return json.dumps(
        {"type": "plan_result", "schema_version": 1, "plan": "Plan the session flow."}
    )


def _implementation_result_payload() -> str:
    return json.dumps(
        {
            "type": "implementation_result",
            "schema_version": 1,
            "status": "COMPLETED",
            "branch": "feat/session-flow",
            "summary": "Implemented the session flow.",
            "changes": ["Added the flow"],
            "validation": ["Ran the tests"],
        }
    )


def _pr_result_payload() -> str:
    return json.dumps({"type": "pr_result", "schema_version": 1, "title": "Title", "body": "Body"})


class _ScriptedAgentRuntime(AgentRuntimeAdapter):
    """Minimal concrete adapter satisfying the shared contract without any
    Cline types.

    Scripted outcomes drive the normalized failure model. Every candidate
    response arrives wrapped in the runtime-style outer envelope, which this
    adapter strips before funnelling through the shared formal validation —
    exactly the mechanics a real runtime adapter owns.
    """

    def __init__(self) -> None:
        self.creation_outcome = "ready"
        self.send_outcome = "formal"
        self.formal_reply = _plan_result_payload()
        self.nonformal_reply = "Advisory reply from the reviewer session."
        self.reported_provider: str | None = "reported-provider"
        self.reported_model: str | None = None
        self.reported_runtime_version: str | None = None

        # Internal runtime facts and addressable sessions.
        self.allocated_ids: list[str] = []
        self.ready_ids: list[str] = []
        self.lost_ids: set[str] = set()
        self.received: list[tuple[str, str]] = []
        self.native_actions: list[tuple[str, str]] = []

    @staticmethod
    def _wrap(candidate: str) -> str:
        return f"{_ENVELOPE_PREFIX}{candidate}{_ENVELOPE_SUFFIX}"

    @staticmethod
    def _unwrap(wrapped: str) -> str:
        if not (wrapped.startswith(_ENVELOPE_PREFIX) and wrapped.endswith(_ENVELOPE_SUFFIX)):
            raise AgentRuntimeTransportError(
                "the runtime-native response envelope could not be extracted"
            )
        return wrapped[len(_ENVELOPE_PREFIX) : -len(_ENVELOPE_SUFFIX)]

    def create_session(self, request: AgentSessionCreationRequest) -> AgentSessionCreated:
        if self.creation_outcome == "config_rejected":
            provider_error = RuntimeError(
                f"hub rejected the configuration {_SECRET_PROVIDER_TEXT_MARKER}"
            )
            raise AgentRuntimeConfigurationRejectedError(
                "the runtime rejected the presented configuration"
            ) from provider_error
        if self.creation_outcome == "unavailable":
            raise AgentRuntimeUnavailableError("the runtime was unreachable before any effect")
        if self.creation_outcome == "uncertain":
            raise AgentRuntimeUncertainOutcomeError(
                "the creation outcome is unknown whether it took effect"
            )
        if self.creation_outcome == "timeout":
            raise AgentRuntimeTimeoutError("the creation exceeded its deadline")
        # The runtime allocates an internal external context first; the
        # readiness response is delivered through the runtime envelope and
        # validated through the shared parser before the context becomes a
        # successful session.
        external_id = f"external-{len(self.allocated_ids) + 1}"
        self.allocated_ids.append(external_id)
        ready_candidate = (
            "still working on it, no formal object yet"
            if self.creation_outcome == "invalid_ready"
            else _session_ready_payload()
        )
        self._parse_formal_candidate(
            self._unwrap(self._wrap(ready_candidate)),
            expected_family=SESSION_READY_FAMILY,
        )
        self.ready_ids.append(external_id)
        return AgentSessionCreated(
            external_session_id=external_id,
            reported_provider=self.reported_provider,
            reported_model=self.reported_model,
            reported_runtime_version=self.reported_runtime_version,
        )

    def send(
        self,
        session_id: str,
        message: str,
        *,
        expected_family: str | None = None,
    ) -> FormalResponse | AgentTextResponse:
        if session_id not in self.ready_ids or session_id in self.lost_ids:
            raise AgentSessionNotFoundError(
                "the exact addressed session is not live on this runtime"
            )
        self.received.append((session_id, message))
        if self.send_outcome == "config_rejected":
            raise AgentRuntimeConfigurationRejectedError(
                "the runtime rejected the presented configuration"
            )
        if self.send_outcome == "unavailable":
            raise AgentRuntimeUnavailableError("the runtime was unreachable before any effect")
        if self.send_outcome == "uncertain":
            raise AgentRuntimeUncertainOutcomeError(
                "the send outcome is unknown whether it took effect"
            )
        if self.send_outcome == "timeout":
            raise AgentRuntimeTimeoutError("the send exceeded its deadline")
        if self.send_outcome == "delivery_uncertain":
            raise AgentRuntimeDeliveryUncertainError("the connection broke during the send")
        content = self.formal_reply if expected_family is not None else self.nonformal_reply
        wrapped = "no envelope at all" if self.send_outcome == "transport" else self._wrap(content)
        candidate = self._unwrap(wrapped)
        if expected_family is None:
            return AgentTextResponse(text=candidate)
        return self._parse_formal_candidate(candidate, expected_family=expected_family)


class _NativeActionAgentRuntime(_ScriptedAgentRuntime):
    """Concrete adapter that realizes IMPLEMENT through a scripted
    runtime-native action, dispatched locally on the semantic control kind.
    """

    def __init__(self) -> None:
        super().__init__()
        self.native_then_send = False

    def realize_control(
        self,
        session_id: str,
        interaction: Interaction,
        *,
        message: str | None = None,
        expected_family: str | None = None,
    ) -> FormalResponse | None:
        if interaction.kind is InteractionKind.IMPLEMENT:
            self.native_actions.append((session_id, interaction.kind.value))
            if not self.native_then_send:
                return None
            assert message is not None
            assert expected_family is not None
            result = self.send(session_id, message, expected_family=expected_family)
            # A formal send always produces the typed result or raises.
            return cast("FormalResponse", result)
        return super().realize_control(
            session_id,
            interaction,
            message=message,
            expected_family=expected_family,
        )


def _creation_request() -> AgentSessionCreationRequest:
    return AgentSessionCreationRequest(
        workspace_id=uuid4(),
        task_id=uuid4(),
        role=WorkflowRole.PRODUCER,
        initialization=(
            "canonical producer initialization\n\n---\n\n"
            f"Workspace guidance {_SECRET_INITIALIZATION_MARKER}"
        ),
    )


def _created_session_on(runtime: _ScriptedAgentRuntime) -> str:
    return runtime.create_session(_creation_request()).external_session_id


def _created_session() -> tuple[_ScriptedAgentRuntime, str]:
    runtime = _ScriptedAgentRuntime()
    return runtime, _created_session_on(runtime)


def test_create_session_returns_the_exact_opaque_id_only_after_valid_session_ready() -> None:
    runtime = _ScriptedAgentRuntime()
    created = runtime.create_session(_creation_request())

    assert isinstance(created, AgentSessionCreated)
    # Success is the exact ready context: the same external identity the
    # runtime allocated internally, now addressable.
    assert created.external_session_id == runtime.allocated_ids[0]
    assert runtime.ready_ids == [created.external_session_id]


def test_create_session_returns_only_safe_normalized_observations() -> None:
    runtime = _ScriptedAgentRuntime()
    runtime.reported_provider = "reported-provider"
    runtime.reported_model = "reported-model"
    runtime.reported_runtime_version = "reported-version"
    created = runtime.create_session(_creation_request())

    assert created.reported_provider == "reported-provider"
    assert created.reported_model == "reported-model"
    assert created.reported_runtime_version == "reported-version"
    # Initialization content never becomes a normalized result field.
    assert _SECRET_INITIALIZATION_MARKER not in repr(created)

    runtime = _ScriptedAgentRuntime()
    runtime.reported_provider = None
    runtime.reported_model = None
    runtime.reported_runtime_version = None
    created = runtime.create_session(_creation_request())
    assert created.reported_provider is None
    assert created.reported_model is None
    assert created.reported_runtime_version is None


def test_invalid_readiness_is_a_normalized_protocol_failure_never_a_session() -> None:
    runtime = _ScriptedAgentRuntime()
    runtime.creation_outcome = "invalid_ready"

    with pytest.raises(AgentRuntimeProtocolFailureError) as exc_info:
        runtime.create_session(_creation_request())

    # The shared #65 failure stays chained; the wrapper message carries no
    # response content.
    assert isinstance(exc_info.value.__cause__, FormalObjectNotFoundError)
    assert isinstance(exc_info.value.__cause__, ProtocolError)
    # The allocated external context stayed an internal runtime fact: it is
    # never returned as a successful session and never becomes addressable.
    assert len(runtime.allocated_ids) == 1
    assert runtime.ready_ids == []
    unconfirmed_id = runtime.allocated_ids[0]
    with pytest.raises(AgentSessionNotFoundError):
        runtime.send(unconfirmed_id, "hello", expected_family=PLAN_RESULT_FAMILY)


@pytest.mark.parametrize(
    ("outcome", "expected_error"),
    [
        ("config_rejected", AgentRuntimeConfigurationRejectedError),
        ("unavailable", AgentRuntimeUnavailableError),
        ("uncertain", AgentRuntimeUncertainOutcomeError),
        ("timeout", AgentRuntimeTimeoutError),
    ],
)
def test_creation_outcomes_classify_into_the_normalized_taxonomy(
    outcome: str,
    expected_error: type[Exception],
) -> None:
    runtime = _ScriptedAgentRuntime()
    runtime.creation_outcome = outcome

    with pytest.raises(expected_error):
        runtime.create_session(_creation_request())

    # Known-failure and pre-effect/uncertain creation outcomes produce no
    # successful session result at all.
    assert runtime.allocated_ids == []
    assert runtime.ready_ids == []


def test_send_addresses_the_exact_session_and_returns_the_typed_formal_result() -> None:
    runtime, session_id = _created_session()

    result = runtime.send(session_id, "Plan this task.", expected_family=PLAN_RESULT_FAMILY)

    assert isinstance(result, PlanResult)
    assert result.plan == "Plan the session flow."
    # The exact supplied opaque ID was addressed, and the composed message
    # was delivered verbatim.
    assert runtime.received == [(session_id, "Plan this task.")]


def test_send_strips_the_runtime_envelope_before_shared_parsing() -> None:
    runtime, session_id = _created_session()
    # A model reply surrounded by harmless prose, delivered inside the
    # runtime-native "task completed" wrapper the adapter must strip.
    runtime.formal_reply = "Here is my plan.\n\n" + _plan_result_payload() + "\n\nLet me know."

    result = runtime.send(session_id, "message", expected_family=PLAN_RESULT_FAMILY)

    assert isinstance(result, PlanResult)
    # The envelope never entered shared parsing; the surrounding prose never
    # supplies semantics, and the one explicit JSON object was isolated.
    assert result.plan == "Plan the session flow."


@pytest.mark.parametrize(
    ("formal_reply", "expected_cause"),
    [
        ("no formal object in this reply at all", FormalObjectNotFoundError),
        ("{ not json }", MalformedFormalObjectError),
        (_pr_result_payload(), UnexpectedResponseTypeError),
        ('{"type": "plan_result", "schema_version": 1, "plan": 5}', SchemaInvalidPayloadError),
    ],
)
def test_formal_parsing_failures_propagate_as_normalized_protocol_failures(
    formal_reply: str,
    expected_cause: type[ProtocolError],
) -> None:
    runtime, session_id = _created_session()
    runtime.formal_reply = formal_reply

    with pytest.raises(AgentRuntimeProtocolFailureError) as exc_info:
        runtime.send(session_id, "message", expected_family=PLAN_RESULT_FAMILY)

    cause = exc_info.value.__cause__
    assert isinstance(cause, expected_cause)
    assert isinstance(cause, ProtocolError)
    # The adapter neither repaired nor retried: exactly one send attempt.
    assert len(runtime.received) == 1


def test_nonformal_send_returns_text_without_protocol_parsing() -> None:
    runtime, session_id = _created_session()
    # Advisory Owner ↔ Reviewer discussion: the reply contains free prose
    # AND a well-formed formal JSON object that parsing would accept.
    runtime.nonformal_reply = "Sure — focusing on the plan. " + _plan_result_payload()

    result = runtime.send(session_id, "How is it going?", expected_family=None)

    # No expected family means no #65 parsing: the reply is returned
    # verbatim as normalized text and never upgraded into a formal result.
    assert isinstance(result, AgentTextResponse)
    assert result.text == runtime.nonformal_reply
    assert not isinstance(result, PlanResult)


def test_exact_session_loss_is_distinct_and_never_replaces_or_redirects() -> None:
    runtime, session_id = _created_session()
    runtime.lost_ids.add(session_id)

    with pytest.raises(AgentSessionNotFoundError):
        runtime.send(session_id, "message", expected_family=PLAN_RESULT_FAMILY)

    # Loss never triggers replacement: no new context was allocated, the
    # ready set is unchanged, and nothing was delivered elsewhere.
    assert len(runtime.allocated_ids) == 1
    assert runtime.ready_ids == [session_id]
    assert runtime.received == []
    # Exact-session loss is distinguishable from runtime unavailability.
    assert not issubclass(AgentSessionNotFoundError, AgentRuntimeUnavailableError)


@pytest.mark.parametrize(
    ("outcome", "expected_error"),
    [
        ("config_rejected", AgentRuntimeConfigurationRejectedError),
        ("unavailable", AgentRuntimeUnavailableError),
        ("uncertain", AgentRuntimeUncertainOutcomeError),
        ("timeout", AgentRuntimeTimeoutError),
        ("delivery_uncertain", AgentRuntimeDeliveryUncertainError),
        ("transport", AgentRuntimeTransportError),
    ],
)
def test_send_outcomes_classify_into_the_normalized_taxonomy(
    outcome: str,
    expected_error: type[Exception],
) -> None:
    runtime, session_id = _created_session()
    runtime.send_outcome = outcome

    with pytest.raises(expected_error):
        runtime.send(session_id, "message", expected_family=PLAN_RESULT_FAMILY)


def test_uncertain_outcomes_stay_catchable_as_one_class() -> None:
    # Unknown-whether-applied is one catchable class: timeout and mid-send
    # delivery loss are its leaves, and known-failure categories are not.
    assert issubclass(AgentRuntimeTimeoutError, AgentRuntimeUncertainOutcomeError)
    assert issubclass(AgentRuntimeDeliveryUncertainError, AgentRuntimeUncertainOutcomeError)
    for known_failure in (
        AgentRuntimeConfigurationRejectedError,
        AgentRuntimeUnavailableError,
        AgentSessionNotFoundError,
        AgentRuntimeTransportError,
        AgentRuntimeProtocolFailureError,
    ):
        assert not issubclass(known_failure, AgentRuntimeUncertainOutcomeError)
    for error in agent_runtime_errors.__all__:
        assert issubclass(getattr(agent_runtime_errors, error), AgentRuntimeError)
    # The taxonomy is compact: no provider-status mirror enums.
    assert len(agent_runtime_errors.__all__) == 9


@pytest.mark.parametrize(
    ("interaction_factory", "expected_family", "reply_payload", "result_type"),
    [
        (lambda: PlanInteraction(), PLAN_RESULT_FAMILY, _plan_result_payload(), PlanResult),
        (
            lambda: PrComposeInteraction(repository_full_name="org/repo", issue_number=7),
            PR_RESULT_FAMILY,
            _pr_result_payload(),
            PrResult,
        ),
    ],
)
def test_realize_control_default_is_prose_only_realization(
    interaction_factory: Callable[[], Interaction],
    expected_family: str,
    reply_payload: str,
    result_type: type[PlanResult] | type[PrResult],
) -> None:
    runtime, session_id = _created_session()
    runtime.formal_reply = reply_payload
    interaction = interaction_factory()
    composed = "composed interaction prose from the application layer"

    result = runtime.realize_control(
        session_id,
        interaction,
        message=composed,
        expected_family=expected_family,
    )

    # The semantic control arrived distinctly from its rendered prose, the
    # exact session was addressed with the composed message, and the typed
    # formal result is returned.
    assert isinstance(result, result_type)
    assert runtime.received == [(session_id, composed)]


def test_realize_control_default_requires_composed_prose_and_family() -> None:
    runtime, session_id = _created_session()
    interaction = PlanInteraction()

    with pytest.raises(ValueError):
        runtime.realize_control(session_id, interaction)
    with pytest.raises(ValueError):
        runtime.realize_control(session_id, interaction, message="composed")
    with pytest.raises(ValueError):
        runtime.realize_control(session_id, interaction, message="   ")
    # Fail closed: no silent success and nothing was delivered.
    assert runtime.received == []


def test_native_only_realization_performs_the_native_action_without_prose() -> None:
    runtime = _NativeActionAgentRuntime()
    created = runtime.create_session(_creation_request())
    session_id = created.external_session_id
    interaction = ImplementInteraction()

    result = runtime.realize_control(session_id, interaction)

    # Runtime-native action only: the control is realized with no formal
    # response and no prose send.
    assert result is None
    assert runtime.native_actions == [(session_id, InteractionKind.IMPLEMENT.value)]
    assert runtime.received == []


def test_combined_realization_performs_the_native_action_then_the_formal_send() -> None:
    runtime = _NativeActionAgentRuntime()
    runtime.native_then_send = True
    runtime.formal_reply = _implementation_result_payload()
    created = runtime.create_session(_creation_request())
    session_id = created.external_session_id
    interaction = ImplementInteraction()
    composed = "implementation authorization interaction"

    result = runtime.realize_control(
        session_id,
        interaction,
        message=composed,
        expected_family=IMPLEMENTATION_RESULT_FAMILY,
    )

    # Native action plus the composed send reinforcement, in one
    # realization: the action happened exactly once and the typed formal
    # result reaches the caller.
    assert isinstance(result, ImplementationResult)
    assert runtime.native_actions == [(session_id, InteractionKind.IMPLEMENT.value)]
    assert runtime.received == [(session_id, composed)]


def test_shared_contract_never_hardcodes_native_control_dispatch() -> None:
    # The shared default performs no runtime-native action for any semantic
    # control kind; native dispatch is a concrete adapter override.
    runtime, session_id = _created_session()
    for interaction in (PlanInteraction(), PrComposeInteraction(), ImplementInteraction()):
        with pytest.raises(ValueError):
            runtime.realize_control(session_id, interaction)
    assert runtime.native_actions == []
    assert runtime.received == []


def _fail_creation_config_rejected() -> None:
    runtime = _ScriptedAgentRuntime()
    runtime.creation_outcome = "config_rejected"
    runtime.create_session(_creation_request())


def _fail_creation_uncertain() -> None:
    runtime = _ScriptedAgentRuntime()
    runtime.creation_outcome = "uncertain"
    runtime.create_session(_creation_request())


def _fail_send_protocol_failure() -> None:
    runtime = _ScriptedAgentRuntime()
    session_id = _created_session_on(runtime)
    runtime.formal_reply = f"mentioning {_SECRET_RESPONSE_MARKER} with no formal object"
    runtime.send(session_id, "message", expected_family=PLAN_RESULT_FAMILY)


def _fail_send_transport() -> None:
    runtime = _ScriptedAgentRuntime()
    session_id = _created_session_on(runtime)
    runtime.formal_reply = f"content {_SECRET_RESPONSE_MARKER}"
    runtime.send_outcome = "transport"
    runtime.send(session_id, "message", expected_family=PLAN_RESULT_FAMILY)


@pytest.mark.parametrize(
    ("trigger", "expected_error", "planted_marker"),
    [
        (
            _fail_creation_config_rejected,
            AgentRuntimeConfigurationRejectedError,
            _SECRET_PROVIDER_TEXT_MARKER,
        ),
        (
            _fail_creation_uncertain,
            AgentRuntimeUncertainOutcomeError,
            _SECRET_INITIALIZATION_MARKER,
        ),
        (_fail_send_protocol_failure, AgentRuntimeProtocolFailureError, _SECRET_RESPONSE_MARKER),
        (_fail_send_transport, AgentRuntimeTransportError, _SECRET_RESPONSE_MARKER),
    ],
)
def test_normalized_diagnostics_never_echo_sensitive_payloads(
    trigger: Callable[[], None],
    expected_error: type[Exception],
    planted_marker: str,
) -> None:
    with pytest.raises(expected_error) as exc_info:
        trigger()

    message = str(exc_info.value)
    # No normalized message echoes credentials, guidance/initialization
    # content, raw runtime responses, or provider exception text.
    for marker in (
        _SECRET_INITIALIZATION_MARKER,
        _SECRET_RESPONSE_MARKER,
        _SECRET_PROVIDER_TEXT_MARKER,
    ):
        assert marker not in message


_ALLOWED_OPENORC_ROOTS = (
    "openorc.protocol",
    "openorc.adapters.agent_runtime",
    "openorc.domain.connections",
)

# Cline PLAN/ACT mode symbols and any runtime-native action vocabulary have
# no place in the runtime-neutral public contract.
_CLINE_MODE_TOKENS = (
    "PLAN_TO_ACT",
    "plan_to_act",
    "PLAN_MODE",
    "ACT_MODE",
    "plan_mode",
    "act_mode",
    "switch_to_plan",
    "switch_to_act",
)


def test_agent_runtime_contract_package_stays_runtime_neutral() -> None:
    import openorc.adapters.agent_runtime as agent_runtime_package

    package_dir = Path(agent_runtime_package.__file__).resolve().parent
    files = sorted(package_dir.rglob("*.py"))
    assert files, "agent_runtime package source must be discoverable"

    for path in files:
        source = path.read_text(encoding="utf-8")
        for token in _CLINE_MODE_TOKENS:
            assert token not in source
        tree = ast.parse(source, filename=str(path))
        for node in ast.walk(tree):
            targets: list[str] = []
            if isinstance(node, ast.Import):
                targets = [alias.name for alias in node.names]
            elif isinstance(node, ast.ImportFrom) and node.level == 0 and node.module:
                targets = [node.module]
            for target in targets:
                assert "cline" not in target.lower(), (
                    f"{path.name} imports runtime-specific module {target}"
                )
                if not target.startswith("openorc."):
                    continue
                allowed = any(
                    target == root or target.startswith(root + ".")
                    for root in _ALLOWED_OPENORC_ROOTS
                )
                assert allowed, f"{path.name} imports out-of-boundary module {target}"


def test_universal_surface_is_exactly_the_documented_operations() -> None:
    # Exactly the three documented universal operations; only creation and
    # send are abstract, so the shared prose-only control realization is
    # real, and no universal close/end/polling operation exists.
    assert AgentRuntimeAdapter.__abstractmethods__ == frozenset({"create_session", "send"})
    public_methods = {
        name
        for name in dir(AgentRuntimeAdapter)
        if not name.startswith("_") and callable(getattr(AgentRuntimeAdapter, name))
    }
    assert public_methods == {"create_session", "send", "realize_control"}
