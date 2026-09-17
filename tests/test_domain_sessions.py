"""Domain tests for TaskAgentSession (issue #22).

Ordinary deterministic tests: no database, no network. These prove the
continuity invariants that later persistence and workflow-service behavior
inherit: the lifecycle vocabulary and transition rules, the
CONNECTING-is-not-a-bound-session coherence rule, the ENDED-either-form rule,
``ended_at`` as the semantic ENDED timestamp, non-secret snapshot
canonicalization, opaque provenance, and the exact field set (no replacement
machinery, no authentication material, no runtime health/telemetry, no
conversational content).
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

import pytest

from openorc.domain.connections import WorkflowRole
from openorc.domain.sessions import (
    TASK_SESSION_LIFECYCLE_TRANSITIONS,
    TaskAgentSession,
    TaskAgentSessionDomainError,
    TaskSessionLifecycleStatus,
    canonical_effective_config_snapshot,
    task_agent_session_field_names,
)

_INITIALIZED_AT = datetime(2026, 9, 17, 12, 0, 0, tzinfo=UTC)
_CREATED_AT = datetime(2026, 9, 17, 11, 0, 0, tzinfo=UTC)


def _session(**overrides: Any) -> TaskAgentSession:
    """Build one valid READY TaskAgentSession with optional field overrides."""
    values: dict[str, Any] = {
        "id": uuid4(),
        "workspace_id": uuid4(),
        "task_id": uuid4(),
        "role": WorkflowRole.PRODUCER,
        "connection_id": uuid4(),
        "external_session_id": "ext-session-1",
        "lifecycle_status": TaskSessionLifecycleStatus.READY,
        "initialization_protocol_version": "1",
        "effective_config_snapshot": {"stage": "plan"},
        "reported_provider": None,
        "reported_model": None,
        "reported_runtime_version": None,
        "initialized_at": _INITIALIZED_AT,
        "ended_at": None,
        "created_at": _CREATED_AT,
        "updated_at": _INITIALIZED_AT,
    }
    values.update(overrides)
    return TaskAgentSession(**values)


def test_lifecycle_vocabulary_values_are_the_persisted_text_values() -> None:
    assert TaskSessionLifecycleStatus.CONNECTING == "connecting"
    assert TaskSessionLifecycleStatus.READY == "ready"
    assert TaskSessionLifecycleStatus.LOST == "lost"
    assert TaskSessionLifecycleStatus.ENDED == "ended"
    assert [member.value for member in TaskSessionLifecycleStatus] == [
        "connecting",
        "ready",
        "lost",
        "ended",
    ]


def test_transition_map_is_exactly_the_settled_v1_lifecycle() -> None:
    # CONNECTING has not bound an external session yet, so it cannot become
    # LOST; READY can genuinely lose the exact session or end normally; LOST
    # and ENDED are absorbing.
    assert {
        TaskSessionLifecycleStatus.CONNECTING: frozenset(
            {TaskSessionLifecycleStatus.READY, TaskSessionLifecycleStatus.ENDED}
        ),
        TaskSessionLifecycleStatus.READY: frozenset(
            {TaskSessionLifecycleStatus.LOST, TaskSessionLifecycleStatus.ENDED}
        ),
        TaskSessionLifecycleStatus.LOST: frozenset(),
        TaskSessionLifecycleStatus.ENDED: frozenset(),
    } == TASK_SESSION_LIFECYCLE_TRANSITIONS


def test_role_requires_a_workflow_role() -> None:
    with pytest.raises(TaskAgentSessionDomainError):
        _session(role="producer")  # type: ignore[arg-type]


def test_lifecycle_status_requires_the_vocabulary_enum() -> None:
    with pytest.raises(TaskAgentSessionDomainError):
        _session(lifecycle_status="ready")  # type: ignore[arg-type]


@pytest.mark.parametrize("bad_identity", ["", "   "])
def test_external_session_id_rejects_blank_strings(bad_identity: str) -> None:
    with pytest.raises(TaskAgentSessionDomainError):
        _session(external_session_id=bad_identity)


@pytest.mark.parametrize("bad_version", ["", "   "])
def test_initialization_protocol_version_rejects_blank_strings(bad_version: str) -> None:
    with pytest.raises(TaskAgentSessionDomainError):
        _session(initialization_protocol_version=bad_version)


def test_absence_is_valid_for_protocol_version_and_provenance() -> None:
    session = _session(
        initialization_protocol_version=None,
        reported_provider=None,
        reported_model=None,
        reported_runtime_version=None,
    )
    assert session.initialization_protocol_version is None
    assert session.reported_provider is None
    assert session.reported_model is None
    assert session.reported_runtime_version is None


@pytest.mark.parametrize("bad_provenance", [1, True, 2.5])
def test_reported_provenance_accepts_only_strings_or_none(bad_provenance: Any) -> None:
    # Opaque runtime-reported observations: any string, or None. No
    # vocabulary, no enum, no normalization — never configuration authority.
    with pytest.raises(TaskAgentSessionDomainError):
        _session(reported_provider=bad_provenance)


def test_reported_provenance_accepts_arbitrary_opaque_strings() -> None:
    session = _session(
        reported_provider="some-runtime/anthropic",
        reported_model="claude-opus-4-6",
        reported_runtime_version="2026.09.12+build",
    )
    assert session.reported_provider == "some-runtime/anthropic"
    assert session.reported_model == "claude-opus-4-6"
    assert session.reported_runtime_version == "2026.09.12+build"


def test_connecting_requires_no_external_session_identity() -> None:
    connecting = _session(
        lifecycle_status=TaskSessionLifecycleStatus.CONNECTING,
        external_session_id=None,
        initialization_protocol_version=None,
        effective_config_snapshot=None,
        initialized_at=None,
    )
    assert connecting.external_session_id is None
    assert connecting.initialized_at is None
    # CONNECTING is by definition not a bound session: an identity or an
    # initialization instant cannot exist yet.
    with pytest.raises(TaskAgentSessionDomainError):
        _session(
            lifecycle_status=TaskSessionLifecycleStatus.CONNECTING,
            external_session_id=None,
            initialized_at=_INITIALIZED_AT,
        )
    with pytest.raises(TaskAgentSessionDomainError):
        _session(
            lifecycle_status=TaskSessionLifecycleStatus.CONNECTING,
            external_session_id="ext-session-1",
            initialized_at=None,
        )


def test_ready_and_lost_require_a_coherent_initialized_identity() -> None:
    for status in (TaskSessionLifecycleStatus.READY, TaskSessionLifecycleStatus.LOST):
        session = _session(lifecycle_status=status)
        assert session.external_session_id is not None
        assert session.initialized_at is not None
        with pytest.raises(TaskAgentSessionDomainError):
            _session(lifecycle_status=status, external_session_id=None)
        with pytest.raises(TaskAgentSessionDomainError):
            _session(lifecycle_status=status, initialized_at=None)


def test_ended_permits_both_coherent_initialization_forms() -> None:
    # Ended before initialization: the establishment attempt ended without
    # ever binding an external session — the binding stays valid with a NULL
    # identity.
    before = _session(
        lifecycle_status=TaskSessionLifecycleStatus.ENDED,
        external_session_id=None,
        initialization_protocol_version=None,
        effective_config_snapshot=None,
        initialized_at=None,
        ended_at=_INITIALIZED_AT,
    )
    assert before.external_session_id is None
    assert before.initialized_at is None
    assert before.ended_at is not None
    # Ended after a successful initialization: the bound identity is
    # preserved on the same binding.
    after = _session(
        lifecycle_status=TaskSessionLifecycleStatus.ENDED,
        ended_at=_INITIALIZED_AT,
    )
    assert after.external_session_id == "ext-session-1"
    assert after.initialized_at == _INITIALIZED_AT
    assert after.ended_at is not None


@pytest.mark.parametrize("status", list(TaskSessionLifecycleStatus))
def test_mixed_initialization_state_is_rejected_for_every_status(
    status: TaskSessionLifecycleStatus,
) -> None:
    # One NULL and one non-NULL is incoherent no matter the lifecycle status.
    # ended_at is kept consistent with the status so the mixed-state rule is
    # the violation that surfaces.
    ended_at = _INITIALIZED_AT if status is TaskSessionLifecycleStatus.ENDED else None
    with pytest.raises(TaskAgentSessionDomainError):
        _session(
            lifecycle_status=status,
            external_session_id=None,
            initialized_at=_INITIALIZED_AT,
            ended_at=ended_at,
        )
    with pytest.raises(TaskAgentSessionDomainError):
        _session(
            lifecycle_status=status,
            external_session_id="ext-session-1",
            initialized_at=None,
            ended_at=ended_at,
        )


def test_ended_at_is_set_exactly_when_status_is_ended() -> None:
    # ENDED requires the semantic timestamp — generic updated_at never
    # substitutes for it.
    with pytest.raises(TaskAgentSessionDomainError):
        _session(
            lifecycle_status=TaskSessionLifecycleStatus.ENDED,
            external_session_id=None,
            initialized_at=None,
            ended_at=None,
        )
    # Non-ENDED statuses must not carry it.
    with pytest.raises(TaskAgentSessionDomainError):
        _session(ended_at=_INITIALIZED_AT)
    with pytest.raises(TaskAgentSessionDomainError):
        _session(lifecycle_status=TaskSessionLifecycleStatus.LOST, ended_at=_INITIALIZED_AT)


def test_snapshot_is_canonicalized_in_place() -> None:
    session = _session(effective_config_snapshot={"flags": (1, 2), "nested": {"more": (3,)}})
    assert dict(session.effective_config_snapshot) == {  # type: ignore[arg-type]
        "flags": [1, 2],
        "nested": {"more": [3]},
    }


def test_snapshot_must_be_a_mapping() -> None:
    with pytest.raises(TaskAgentSessionDomainError):
        _session(effective_config_snapshot=["not", "a", "mapping"])  # type: ignore[arg-type]


@pytest.mark.parametrize("bad_snapshot", [{1: "a"}, {"nested": {2: "b"}}])
def test_snapshot_requires_string_keys_at_every_level(bad_snapshot: dict[Any, Any]) -> None:
    with pytest.raises(TaskAgentSessionDomainError):
        _session(effective_config_snapshot=bad_snapshot)


def test_snapshot_rejects_non_json_value_types() -> None:
    with pytest.raises(TaskAgentSessionDomainError):
        _session(effective_config_snapshot={"raw": b"bytes"})


def test_snapshot_rejects_nan_and_infinity() -> None:
    with pytest.raises(TaskAgentSessionDomainError):
        _session(effective_config_snapshot={"ratio": float("nan")})
    with pytest.raises(TaskAgentSessionDomainError):
        _session(effective_config_snapshot={"ratio": float("inf")})


def test_snapshot_view_is_immutable() -> None:
    session = _session(effective_config_snapshot={"a": 1})
    with pytest.raises(TypeError):
        session.effective_config_snapshot["a"] = 2  # type: ignore[index]


def test_canonical_effective_config_snapshot_stands_alone() -> None:
    canonical = canonical_effective_config_snapshot({"a": (1, {"b": [True]})})
    assert dict(canonical) == {"a": [1, {"b": [True]}]}


def test_no_replacement_secret_or_telemetry_fields_exist() -> None:
    # The exact field set is asserted so future drift cannot appear silently:
    # no replacement machinery (a successor-session pointer would defeat the
    # no-replacement invariant), no authentication material (OpenOrc-owned
    # auth stays on the Connection behind the opaque auth_reference boundary;
    # raw credentials/tokens never enter the snapshot), no runtime
    # health/telemetry, and no conversational content.
    assert task_agent_session_field_names() == frozenset(
        {
            "id",
            "workspace_id",
            "task_id",
            "role",
            "connection_id",
            "external_session_id",
            "lifecycle_status",
            "initialization_protocol_version",
            "effective_config_snapshot",
            "reported_provider",
            "reported_model",
            "reported_runtime_version",
            "initialized_at",
            "ended_at",
            "created_at",
            "updated_at",
        }
    )


def test_producer_and_reviewer_bindings_are_independent_rows() -> None:
    task_id = uuid4()
    connection_id = uuid4()
    producer = _session(role=WorkflowRole.PRODUCER, task_id=task_id, connection_id=connection_id)
    reviewer = _session(role=WorkflowRole.REVIEWER, task_id=task_id, connection_id=connection_id)
    assert producer.task_id == reviewer.task_id
    assert producer.connection_id == reviewer.connection_id
    assert producer.role != reviewer.role
    assert producer.id != reviewer.id
