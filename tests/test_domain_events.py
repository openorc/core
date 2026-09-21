"""WorkflowEvent domain validation tests (issue #26).

Prove the locked vocabularies (including the explicit absence of the
deliberately excluded CRUD-ish event types and of any HUMAN actor), the
subject-pair rules, the canonical context form, and the audit-vs-state
boundary: the event record carries only its own audit facts and no
duplicated canonical domain state fields.
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime
from types import MappingProxyType
from typing import Any

import pytest

from openorc.domain.events import (
    WorkflowEvent,
    WorkflowEventActor,
    WorkflowEventDomainError,
    WorkflowEventType,
    workflow_event_field_names,
)


def _event(**overrides: Any) -> WorkflowEvent:
    values: dict[str, Any] = {
        "id": uuid.uuid4(),
        "workspace_id": uuid.uuid4(),
        "task_id": None,
        "event_type": WorkflowEventType.TASK_CREATED,
        "actor_type": WorkflowEventActor.OPENORC,
        "actor_id": None,
        "subject_type": None,
        "subject_id": None,
        "context": {},
        "created_at": datetime(2026, 9, 18, 12, 0, 0, tzinfo=UTC),
    }
    values.update(overrides)
    return WorkflowEvent(**values)


def test_event_type_vocabulary_is_exactly_the_locked_32() -> None:
    locked = {
        "task_created",
        "agent_session_created",
        "agent_session_bound",
        "agent_session_lost",
        "planning_started",
        "plan_revision_created",
        "plan_reviewed",
        "review_limit_reached",
        "plan_revision_revised",
        "plan_ready",
        "implementation_authorized",
        "execution_started",
        "execution_completed",
        "execution_failed",
        "runtime_request_created",
        "runtime_request_resolved",
        "runtime_request_cancelled",
        "retry_started",
        "task_blocked",
        "owner_gate_created",
        "owner_gate_resolved",
        "owner_reviewer_discussion_message",
        "pr_created",
        "pr_reviewed",
        "task_relationship_synced",
        "task_dependency_synced",
        "pr_head_changed",
        "merge_requested",
        "merge_rejected_by_github",
        "pr_merged",
        "task_cancelled",
        "task_completed",
    }
    assert {member.value for member in WorkflowEventType} == locked
    assert len(WorkflowEventType) == 32


def test_excluded_crud_event_types_do_not_exist() -> None:
    # Archival/currentness, branch binding, session lifecycle fields, and
    # TaskPullRequest reconciliation already have canonical storage homes;
    # the audit vocabulary is not a shadow copy of persistence mutations.
    values = {member.value for member in WorkflowEventType}
    for excluded in (
        "task_archived",
        "canonical_branch_bound",
        "agent_session_ended",
        "task_pull_request_created",
        "task_pull_request_updated",
        "prompt_override_set",
        "prompt_override_reset",
    ):
        assert excluded not in values
        assert not hasattr(WorkflowEventType, excluded.upper())


def test_actor_vocabulary_is_exactly_the_locked_six_with_owner_and_no_human() -> None:
    assert {member.value for member in WorkflowEventActor} == {
        "owner",
        "openorc",
        "producer",
        "reviewer",
        "runtime",
        "github",
    }
    # OWNER is the human-authority actor terminology; HUMAN is not an
    # OpenOrc actor anywhere.
    assert WorkflowEventActor.OWNER.value == "owner"
    assert not hasattr(WorkflowEventActor, "HUMAN")
    assert "human" not in {member.value for member in WorkflowEventActor}


def test_field_set_carries_no_duplicated_canonical_domain_state() -> None:
    assert workflow_event_field_names() == frozenset(
        {
            "id",
            "workspace_id",
            "task_id",
            "event_type",
            "actor_type",
            "actor_id",
            "subject_type",
            "subject_id",
            "context",
            "created_at",
        }
    )
    # No canonical domain state is restated on the event: Task status,
    # review results, gate decisions, Execution/RuntimeRequest state, PR
    # state, and prompt bodies live on their canonical records.
    fields_ = workflow_event_field_names()
    for duplicated in (
        "task_status",
        "archived_at",
        "canonical_feature_branch",
        "review_outcome",
        "gate_status",
        "execution_status",
        "request_status",
        "pr_state",
        "head_sha",
        "instruction_text",
        "updated_at",
    ):
        assert duplicated not in fields_


def test_a_valid_workspace_level_event_round_trips() -> None:
    event = _event()
    assert event.task_id is None
    assert event.event_type is WorkflowEventType.TASK_CREATED
    assert event.actor_type is WorkflowEventActor.OPENORC
    assert event.context == MappingProxyType({})


def test_a_task_related_event_carries_optional_task_scope() -> None:
    task_id = uuid.uuid4()
    event = _event(
        task_id=task_id,
        event_type=WorkflowEventType.EXECUTION_STARTED,
        actor_type=WorkflowEventActor.PRODUCER,
    )
    assert event.task_id == task_id


def test_actor_id_is_optional_opaque_identity_and_blank_values_are_rejected() -> None:
    owner_id = uuid.uuid4()
    event = _event(
        event_type=WorkflowEventType.IMPLEMENTATION_AUTHORIZED,
        actor_type=WorkflowEventActor.OWNER,
        actor_id=str(owner_id),
    )
    assert event.actor_id == str(owner_id)
    for blank in ("", "   ", "\t\n", 42, True):
        with pytest.raises(WorkflowEventDomainError):
            _event(actor_id=blank)


def test_the_subject_reference_is_a_pair() -> None:
    subject_id = uuid.uuid4()
    event = _event(
        event_type=WorkflowEventType.OWNER_GATE_CREATED,
        actor_type=WorkflowEventActor.OPENORC,
        subject_type="owner_gate",
        subject_id=subject_id,
    )
    assert event.subject_type == "owner_gate"
    assert event.subject_id == subject_id


def test_a_half_present_subject_reference_is_rejected() -> None:
    with pytest.raises(WorkflowEventDomainError):
        _event(subject_type="owner_gate")
    with pytest.raises(WorkflowEventDomainError):
        _event(subject_id=uuid.uuid4())


def test_subject_type_is_open_text_and_must_be_nonblank_when_present() -> None:
    # Deliberately open: a new auditable subject kind needs no schema
    # migration, so subject_type is plain validated text, not an enum.
    for subject_type in ("plan_revision", "owner_gate", "some_future_subject"):
        event = _event(subject_type=subject_type, subject_id=uuid.uuid4())
        assert event.subject_type == subject_type
    for blank in ("", "   ", 9, True):
        with pytest.raises(WorkflowEventDomainError):
            _event(subject_type=blank, subject_id=uuid.uuid4())


def test_subject_id_must_be_a_uuid_when_present() -> None:
    with pytest.raises(WorkflowEventDomainError):
        _event(subject_type="owner_gate", subject_id="not-a-uuid")


def test_event_type_and_actor_type_must_be_locked_enum_members() -> None:
    with pytest.raises(WorkflowEventDomainError):
        _event(event_type="task_created")  # type: ignore[arg-type]
    with pytest.raises(WorkflowEventDomainError):
        _event(event_type="task_archived")  # type: ignore[arg-type]
    with pytest.raises(WorkflowEventDomainError):
        _event(actor_type="owner")  # type: ignore[arg-type]
    with pytest.raises(WorkflowEventDomainError):
        _event(actor_type="human")  # type: ignore[arg-type]


def test_context_is_canonicalized_and_frozen() -> None:
    event = _event(context={"reason": "capacity", "tags": ("a", "b"), "meta": {"k": 1}})
    assert event.context == MappingProxyType(
        {"reason": "capacity", "tags": ["a", "b"], "meta": {"k": 1}}
    )
    assert isinstance(event.context, MappingProxyType)
    with pytest.raises(WorkflowEventDomainError):
        _event(context=["not", "a", "mapping"])
    with pytest.raises(WorkflowEventDomainError):
        _event(context={7: "non-string key"})
    with pytest.raises(WorkflowEventDomainError):
        _event(context={"x": float("nan")})
    with pytest.raises(WorkflowEventDomainError):
        _event(context={"x": float("inf")})


def test_an_empty_context_is_valid() -> None:
    assert _event(context={}).context == MappingProxyType({})
