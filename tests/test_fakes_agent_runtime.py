"""Deterministic self-tests for the fake Agent Runtime test instrument (#69).

These tests prove only the fake's own programmable behavior and isolation
guarantees: scripted creation/readiness identity handling, FIFO outcome
queues, session/instance isolation, every v1 formal family traveling through
the shared #67 → #65 funnel, non-formal text behavior, normalized failure
scripting, workflow-control realization modes, and that explicit scripting —
never received initialization/message/guidance content — determines fake
behavior. The behavioral matrices of #65, #67, #68, and #130 already have
dedicated coverage and are deliberately not repeated here; the one required
cross-boundary establishment scenario lives with the #130 service seam tests.
"""

from __future__ import annotations

import json
import uuid
from typing import Any

import pytest
from tests.fakes.agent_runtime import (
    ControlRealization,
    FakeAgentRuntimeAdapter,
    accepted_review_result_candidate,
    changes_requested_review_result_candidate,
    implementation_result_candidate,
    plan_result_candidate,
    pr_result_candidate,
    session_ready_candidate,
)

from openorc.adapters.agent_runtime.errors import (
    AgentRuntimeConfigurationRejectedError,
    AgentRuntimeDeliveryUncertainError,
    AgentRuntimeError,
    AgentRuntimeProtocolFailureError,
    AgentRuntimeTimeoutError,
    AgentRuntimeTransportError,
    AgentRuntimeUnavailableError,
    AgentSessionNotFoundError,
)
from openorc.adapters.agent_runtime.results import (
    AgentSessionCreated,
    AgentSessionCreationRequest,
    AgentTextResponse,
)
from openorc.domain.connections import WorkflowRole
from openorc.protocol.errors import ProtocolError
from openorc.protocol.initialization_assets import compose_initialization
from openorc.protocol.interaction import ImplementInteraction, PlanInteraction
from openorc.protocol.models import (
    IMPLEMENTATION_RESULT_FAMILY,
    PLAN_RESULT_FAMILY,
    PR_RESULT_FAMILY,
    REVIEW_RESULT_FAMILY,
    SESSION_READY_FAMILY,
    FormalResponse,
    ImplementationResult,
    PlanResult,
    PrResult,
    ReviewFinding,
    ReviewResult,
    SessionReady,
)


def _creation_request(
    *,
    role: WorkflowRole = WorkflowRole.PRODUCER,
    initialization: str = "composed canonical initialization",
) -> AgentSessionCreationRequest:
    return AgentSessionCreationRequest(
        workspace_id=uuid.uuid4(),
        task_id=uuid.uuid4(),
        role=role,
        initialization=initialization,
    )


def _created_session(
    adapter: FakeAgentRuntimeAdapter,
    *,
    role: WorkflowRole = WorkflowRole.PRODUCER,
) -> AgentSessionCreated:
    adapter.queue_creation(session_ready_candidate())
    return adapter.create_session(_creation_request(role=role))


def test_creation_records_the_exact_request_and_returns_stable_isolated_identities() -> None:
    adapter = FakeAgentRuntimeAdapter(
        reported_provider="fake-provider",
        reported_model="fake-model",
        reported_runtime_version="fake-1.2.3",
    )
    request = _creation_request(initialization="composed producer initialization")
    adapter.queue_creation(session_ready_candidate())

    created = adapter.create_session(request)

    assert adapter.creation_requests() == (request,)
    assert created.external_session_id == "fake-session-1"
    assert created.reported_provider == "fake-provider"
    assert created.reported_model == "fake-model"
    assert created.reported_runtime_version == "fake-1.2.3"
    context = adapter.session_context("fake-session-1")
    assert context.role is WorkflowRole.PRODUCER
    assert context.workspace_id == request.workspace_id
    assert context.task_id == request.task_id
    assert context.initialization == "composed producer initialization"
    assert not context.lost

    # The next successful creation gets its own stable identity; a scripted
    # JSON-string candidate follows the same shared funnel as a dict
    # candidate.
    adapter.queue_creation(json.dumps(session_ready_candidate()))
    reviewer_request = _creation_request(
        role=WorkflowRole.REVIEWER, initialization="composed reviewer initialization"
    )
    second = adapter.create_session(reviewer_request)
    assert second.external_session_id == "fake-session-2"
    assert adapter.created_session_ids() == ("fake-session-1", "fake-session-2")
    assert adapter.session_context("fake-session-2").role is WorkflowRole.REVIEWER

    # Safe provenance observations are optional and scripted, never invented.
    bare = FakeAgentRuntimeAdapter()
    bare.queue_creation(session_ready_candidate())
    unprovenanced = bare.create_session(_creation_request())
    assert unprovenanced.reported_provider is None
    assert unprovenanced.reported_model is None
    assert unprovenanced.reported_runtime_version is None


@pytest.mark.parametrize(
    "failure",
    [
        AgentRuntimeConfigurationRejectedError("configuration rejected before any effect"),
        AgentRuntimeUnavailableError("unavailable before any effect"),
        AgentRuntimeTimeoutError("deadline exceeded; effect unclassified"),
        AgentRuntimeDeliveryUncertainError("delivery broke mid-operation"),
    ],
    ids=["config-rejected", "unavailable", "timeout", "delivery-uncertain"],
)
def test_failed_or_uncertain_creation_never_exposes_a_session(failure: AgentRuntimeError) -> None:
    adapter = FakeAgentRuntimeAdapter()
    request = _creation_request()
    adapter.queue_creation(session_ready_candidate())
    adapter.queue_creation(failure)

    ready = adapter.create_session(request)
    assert ready.external_session_id == "fake-session-1"

    with pytest.raises(type(failure)):
        adapter.create_session(request)
    assert adapter.creation_requests() == (request, request)
    assert adapter.created_session_ids() == ("fake-session-1",)
    # The failed/uncertain attempt left no addressable session behind.
    with pytest.raises(AgentSessionNotFoundError):
        adapter.send("fake-session-2", "hello")


def test_invalid_readiness_candidate_never_exposes_a_session() -> None:
    adapter = FakeAgentRuntimeAdapter()
    request = _creation_request()
    adapter.queue_creation(plan_result_candidate(plan="a plan, not a readiness response"))

    with pytest.raises(AgentRuntimeProtocolFailureError):
        adapter.create_session(request)

    assert adapter.creation_requests() == (request,)
    assert adapter.created_session_ids() == ()
    with pytest.raises(AgentSessionNotFoundError):
        adapter.send("fake-session-1", "hello")


def test_separate_sessions_and_instances_share_nothing_and_outcomes_run_fifo() -> None:
    adapter = FakeAgentRuntimeAdapter()
    producer_request = _creation_request()
    reviewer_request = _creation_request(
        role=WorkflowRole.REVIEWER, initialization="composed reviewer initialization"
    )
    adapter.queue_creation(session_ready_candidate())
    adapter.queue_creation(session_ready_candidate())
    producer = adapter.create_session(producer_request)
    reviewer = adapter.create_session(reviewer_request)

    adapter.queue_send(producer.external_session_id, plan_result_candidate("first plan"))
    adapter.queue_send(producer.external_session_id, plan_result_candidate("second plan"))
    adapter.queue_send(reviewer.external_session_id, "plain reviewer text")

    first = adapter.send(
        producer.external_session_id, "plan this", expected_family=PLAN_RESULT_FAMILY
    )
    second = adapter.send(
        producer.external_session_id, "plan again", expected_family=PLAN_RESULT_FAMILY
    )
    reviewer_reply = adapter.send(reviewer.external_session_id, "advisory note")

    assert first == PlanResult(plan="first plan")
    assert second == PlanResult(plan="second plan")
    assert reviewer_reply == AgentTextResponse(text="plain reviewer text")
    sends = adapter.submitted_sends(producer.external_session_id)
    assert [record.message for record in sends] == ["plan this", "plan again"]
    assert all(record.formal for record in sends)
    assert adapter.submitted_sends(reviewer.external_session_id)[0].formal is False

    # A separate adapter instance starts from a clean slate despite identical
    # scripted inputs: no sessions, outcomes, or recordings cross instances.
    other = FakeAgentRuntimeAdapter()
    other.queue_creation(session_ready_candidate())
    twin = other.create_session(producer_request)
    assert twin.external_session_id == "fake-session-1"
    assert other.session_context("fake-session-1").initialization == (
        producer_request.initialization
    )
    assert other.creation_requests() == (producer_request,)
    assert other.submitted_sends() == ()
    assert other.realized_controls() == ()
    # The first adapter's reviewer session does not exist on the other
    # instance: no session state crossed the instance boundary.
    with pytest.raises(AgentSessionNotFoundError):
        other.send(reviewer.external_session_id, "hello")


@pytest.mark.parametrize(
    ("expected_family", "candidate", "expected"),
    [
        (SESSION_READY_FAMILY, session_ready_candidate(), SessionReady(status="READY")),
        (
            PLAN_RESULT_FAMILY,
            plan_result_candidate("A deterministic plan."),
            PlanResult(plan="A deterministic plan."),
        ),
        (
            REVIEW_RESULT_FAMILY,
            accepted_review_result_candidate("Everything checks out."),
            ReviewResult(outcome="ACCEPTED", summary="Everything checks out.", findings=()),
        ),
        (
            REVIEW_RESULT_FAMILY,
            changes_requested_review_result_candidate(
                "Please address the gaps.", [("Naming", "Rename the helper.")]
            ),
            ReviewResult(
                outcome="CHANGES_REQUESTED",
                summary="Please address the gaps.",
                findings=(ReviewFinding(summary="Naming", details="Rename the helper."),),
            ),
        ),
        (
            IMPLEMENTATION_RESULT_FAMILY,
            implementation_result_candidate(
                branch="feat/fake-runtime",
                summary="Implemented the fake runtime.",
                changes=["Added the fake"],
                validation=["Ran the tests"],
                notes="Follow-up pending",
            ),
            ImplementationResult(
                status="COMPLETED",
                branch="feat/fake-runtime",
                summary="Implemented the fake runtime.",
                changes=("Added the fake",),
                validation=("Ran the tests",),
                notes="Follow-up pending",
            ),
        ),
        (
            PR_RESULT_FAMILY,
            pr_result_candidate(title="Title", body="Body"),
            PrResult(title="Title", body="Body"),
        ),
    ],
    ids=[
        "session-ready",
        "plan",
        "review-accepted",
        "review-changes-requested",
        "implementation",
        "pr",
    ],
)
def test_valid_formal_candidates_reach_the_shared_funnel_for_every_family(
    expected_family: str,
    candidate: dict[str, Any],
    expected: FormalResponse,
) -> None:
    adapter = FakeAgentRuntimeAdapter()
    session = _created_session(adapter)
    adapter.queue_send(session.external_session_id, candidate)

    result = adapter.send(
        session.external_session_id, "composed prose", expected_family=expected_family
    )

    assert result == expected


def test_invalid_formal_candidate_surfaces_the_normalized_protocol_failure() -> None:
    adapter = FakeAgentRuntimeAdapter()
    session = _created_session(adapter)
    adapter.queue_send(session.external_session_id, "definitely not a formal JSON object")

    with pytest.raises(AgentRuntimeProtocolFailureError) as excinfo:
        adapter.send(session.external_session_id, "plan this", expected_family=PLAN_RESULT_FAMILY)

    # The normalized wrapper comes from the shared path with the typed #65
    # protocol error chained; the wrapper carries no response content.
    assert isinstance(excinfo.value.__cause__, ProtocolError)
    assert "definitely not a formal JSON object" not in str(excinfo.value)


def test_nonformal_replies_stay_ordinary_text_even_when_formal_looking() -> None:
    adapter = FakeAgentRuntimeAdapter()
    session = _created_session(adapter)
    formal_shaped = session_ready_candidate()
    adapter.queue_send(session.external_session_id, formal_shaped)
    adapter.queue_send(session.external_session_id, json.dumps(formal_shaped))
    adapter.queue_send(
        session.external_session_id,
        'ordinary prose around {"type": "plan_result", "schema_version": 1, "plan": "x"}',
    )

    replies = [
        adapter.send(session.external_session_id, "advisory note"),
        adapter.send(session.external_session_id, "advisory note"),
        adapter.send(session.external_session_id, "advisory note"),
    ]

    assert replies[0] == AgentTextResponse(text=json.dumps(formal_shaped))
    assert replies[1] == AgentTextResponse(text=json.dumps(formal_shaped))
    assert replies[2] == AgentTextResponse(
        text='ordinary prose around {"type": "plan_result", "schema_version": 1, "plan": "x"}'
    )
    # No expected family was supplied, so no formal parsing happened: the
    # recorded sends are all non-formal and the formal-looking content stayed
    # ordinary reply text.
    assert all(not record.formal for record in adapter.submitted_sends(session.external_session_id))


@pytest.mark.parametrize(
    "failure",
    [
        AgentRuntimeConfigurationRejectedError("configuration rejected"),
        AgentRuntimeUnavailableError("unavailable before any effect"),
        AgentRuntimeTimeoutError("deadline exceeded"),
        AgentRuntimeDeliveryUncertainError("delivery unclassifiable"),
        AgentRuntimeTransportError("native envelope could not be extracted"),
    ],
    ids=["config-rejected", "unavailable", "timeout", "delivery-uncertain", "transport"],
)
def test_normalized_failure_scripting_covers_the_taxonomy(failure: AgentRuntimeError) -> None:
    adapter = FakeAgentRuntimeAdapter()
    session = _created_session(adapter)
    adapter.queue_send(session.external_session_id, failure)

    with pytest.raises(type(failure)) as excinfo:
        adapter.send(
            session.external_session_id, "composed prose", expected_family=PLAN_RESULT_FAMILY
        )

    assert excinfo.value is failure


def test_session_loss_raises_the_normalized_exact_session_error_without_replacement() -> None:
    adapter = FakeAgentRuntimeAdapter()
    session = _created_session(adapter)
    adapter.queue_send(session.external_session_id, plan_result_candidate("before loss"))
    assert adapter.send(
        session.external_session_id, "first", expected_family=PLAN_RESULT_FAMILY
    ) == PlanResult(plan="before loss")

    adapter.lose_session(session.external_session_id)

    with pytest.raises(AgentSessionNotFoundError):
        adapter.send(session.external_session_id, "after loss", expected_family=PLAN_RESULT_FAMILY)
    with pytest.raises(AgentSessionNotFoundError):
        adapter.send(session.external_session_id, "after loss")
    assert adapter.session_context(session.external_session_id).lost
    # Unknown session IDs are likewise never silently replaced.
    with pytest.raises(AgentSessionNotFoundError):
        adapter.send("fake-session-never-created", "hello")
    assert adapter.created_session_ids() == (session.external_session_id,)


@pytest.mark.parametrize(
    ("mode", "expected_native", "realizes_formally"),
    [
        (ControlRealization.PROSE_ONLY, False, True),
        (ControlRealization.NATIVE_ONLY, True, False),
        (ControlRealization.NATIVE_THEN_PROSE, True, True),
    ],
    ids=["prose-only", "native-only", "native-then-prose"],
)
def test_control_realization_records_the_exact_contract_inputs(
    mode: ControlRealization,
    expected_native: bool,
    realizes_formally: bool,
) -> None:
    adapter = FakeAgentRuntimeAdapter()
    session = _created_session(adapter)
    adapter.script_realization(session.external_session_id, mode)
    adapter.queue_send(session.external_session_id, plan_result_candidate("The scripted plan."))
    interaction = PlanInteraction()

    result = adapter.realize_control(
        session.external_session_id,
        interaction,
        message="composed plan prose",
        expected_family=PLAN_RESULT_FAMILY,
    )

    controls = adapter.realized_controls(session.external_session_id)
    assert len(controls) == 1
    assert controls[0].interaction is interaction
    assert controls[0].message == "composed plan prose"
    assert controls[0].expected_family == PLAN_RESULT_FAMILY
    assert controls[0].native_action is expected_native

    if realizes_formally:
        # A formal send inside the realization uses the ordinary shared
        # parsing path and is recorded like any exact-session send.
        assert result == PlanResult(plan="The scripted plan.")
        sends = adapter.submitted_sends(session.external_session_id)
        assert [record.message for record in sends] == ["composed plan prose"]
        assert [record.expected_family for record in sends] == [PLAN_RESULT_FAMILY]
    else:
        # Native-only realization invents no formal result and sends nothing.
        assert result is None
        assert adapter.submitted_sends(session.external_session_id) == ()


def test_unscripted_realization_uses_the_shared_prose_only_default() -> None:
    adapter = FakeAgentRuntimeAdapter()
    session = _created_session(adapter)
    adapter.queue_send(
        session.external_session_id,
        implementation_result_candidate(
            branch="feat/fake-runtime",
            summary="Implemented.",
            changes=["change"],
            validation=["check"],
        ),
    )

    result = adapter.realize_control(
        session.external_session_id,
        ImplementInteraction(),
        message="composed implement prose",
        expected_family=IMPLEMENTATION_RESULT_FAMILY,
    )

    assert result == ImplementationResult(
        status="COMPLETED",
        branch="feat/fake-runtime",
        summary="Implemented.",
        changes=("change",),
        validation=("check",),
        notes=None,
    )
    assert adapter.realized_controls(session.external_session_id)[0].native_action is False


def test_explicit_scripting_not_received_content_determines_behavior() -> None:
    adapter = FakeAgentRuntimeAdapter()
    guidance = "Owner-authored guidance prose"
    producer_request = _creation_request(
        initialization=compose_initialization("producer", guidance)
    )
    reviewer_request = _creation_request(
        role=WorkflowRole.REVIEWER,
        initialization="an entirely different composed initialization",
    )
    adapter.queue_creation(session_ready_candidate())
    adapter.queue_creation(session_ready_candidate())
    producer = adapter.create_session(producer_request)
    reviewer = adapter.create_session(reviewer_request)
    for session_id in (producer.external_session_id, reviewer.external_session_id):
        adapter.queue_send(session_id, plan_result_candidate("The same scripted plan."))

    # Identical scripting over different received content yields identical
    # behavior: the fake never interprets initialization/message/guidance
    # prose.
    assert adapter.send(
        producer.external_session_id,
        "guidance-laden composed prose",
        expected_family=PLAN_RESULT_FAMILY,
    ) == PlanResult(plan="The same scripted plan.")
    assert adapter.send(
        reviewer.external_session_id,
        "different composed prose",
        expected_family=PLAN_RESULT_FAMILY,
    ) == PlanResult(plan="The same scripted plan.")

    # Changing the scripting — not the content — changes the outcome.
    adapter.queue_send(producer.external_session_id, AgentRuntimeTimeoutError("scripted"))
    adapter.queue_send(
        reviewer.external_session_id, plan_result_candidate("The same scripted plan.")
    )
    with pytest.raises(AgentRuntimeTimeoutError):
        adapter.send(
            producer.external_session_id,
            "guidance-laden composed prose",
            expected_family=PLAN_RESULT_FAMILY,
        )
    assert adapter.send(
        reviewer.external_session_id,
        "different composed prose",
        expected_family=PLAN_RESULT_FAMILY,
    ) == PlanResult(plan="The same scripted plan.")

    # Received content is inspectable verbatim exactly as composed upstream:
    # guidance appears only because the upstream composition included it.
    assert adapter.session_context(producer.external_session_id).initialization == (
        compose_initialization("producer", guidance)
    )
    assert adapter.submitted_sends(producer.external_session_id)[0].message == (
        "guidance-laden composed prose"
    )
