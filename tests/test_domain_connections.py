"""Domain tests for Connection and WorkflowRoleBinding (issue #20).

Ordinary deterministic tests: no database, no network. These prove the
domain invariants that later persistence/API behavior inherits: Owner-configured
capacity admission, the boolean eligibility switch, the opaque nullable
authentication-reference boundary, nullable opaque runtime-reported provider/
model strings, structural-only safe-config validation, and the honest binding
shape.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

import pytest

from openorc.domain.connections import (
    AdapterType,
    Connection,
    ConnectionDomainError,
    WorkflowRole,
    WorkflowRoleBinding,
    connection_field_names,
    workflow_role_binding_field_names,
)


def _connection(**overrides: Any) -> Connection:
    values: dict[str, Any] = {
        "id": uuid4(),
        "workspace_id": uuid4(),
        "adapter": AdapterType("cline"),
        "name": "primary cline hub",
        "safe_config": {"base_url": "https://hub.example.com"},
        "session_capacity": 1,
        "enabled": True,
        "auth_reference": None,
        "reported_provider": None,
        "reported_model": None,
        "created_at": datetime(2026, 9, 17, 8, 0, 0, tzinfo=UTC),
        "updated_at": datetime(2026, 9, 17, 8, 0, 0, tzinfo=UTC),
    }
    values.update(overrides)
    return Connection(**values)


def test_vocabulary_values_are_the_persisted_text_values() -> None:
    assert WorkflowRole.PRODUCER == "producer"
    assert WorkflowRole.REVIEWER == "reviewer"
    # AdapterType carries exactly the one v1 adapter vocabulary value.
    assert AdapterType("cline").value == "cline"
    assert [member.value for member in AdapterType] == ["cline"]


@pytest.mark.parametrize("bad_capacity", [0, -1, -100])
def test_session_capacity_must_be_positive(bad_capacity: int) -> None:
    with pytest.raises(ConnectionDomainError):
        _connection(session_capacity=bad_capacity)


@pytest.mark.parametrize("bad_capacity", [True, False, 1.0, "2", None])
def test_session_capacity_must_be_an_integer_not_bool_or_other(bad_capacity: Any) -> None:
    # Owner-configured admission control is a plain integer; booleans are
    # rejected even though bool subclasses int.
    with pytest.raises(ConnectionDomainError):
        _connection(session_capacity=bad_capacity)


def test_capacity_beyond_the_default_is_explicit_owner_configuration() -> None:
    connection = _connection(session_capacity=4)
    assert connection.session_capacity == 4


def test_name_must_be_non_empty() -> None:
    with pytest.raises(ConnectionDomainError):
        _connection(name="   ")


@pytest.mark.parametrize("bad_enabled", [1, 0, "true", None])
def test_enabled_must_be_a_boolean(bad_enabled: Any) -> None:
    # The eligibility switch is a plain boolean: it never encodes runtime
    # reachability, health, or lifecycle state.
    with pytest.raises(ConnectionDomainError):
        _connection(enabled=bad_enabled)


def test_auth_reference_is_none_or_a_non_empty_opaque_string() -> None:
    assert _connection().auth_reference is None
    opaque = "vault://openorc/connection-auth/9f1c"
    assert _connection(auth_reference=opaque).auth_reference == opaque


@pytest.mark.parametrize("bad_reference", ["", "   "])
def test_auth_reference_rejects_blank_strings(bad_reference: str) -> None:
    # NULL is the "none configured" sentinel; a blank string would blur it.
    with pytest.raises(ConnectionDomainError):
        _connection(auth_reference=bad_reference)


def _binding(role: WorkflowRole = WorkflowRole.PRODUCER, **overrides: Any) -> WorkflowRoleBinding:
    values: dict[str, Any] = {
        "id": uuid4(),
        "workspace_id": uuid4(),
        "role": role,
        "connection_id": uuid4(),
        "created_at": datetime(2026, 9, 17, 8, 0, 0, tzinfo=UTC),
        "updated_at": datetime(2026, 9, 17, 8, 0, 0, tzinfo=UTC),
    }
    values.update(overrides)
    return WorkflowRoleBinding(**values)


@pytest.mark.parametrize(
    ("field_name", "value"),
    [
        ("reported_provider", "whatever-runtime-said-1"),
        ("reported_provider", ""),
        ("reported_model", "gpt-4o-mini"),
        ("reported_model", "opaque-token-with-ünicode-⚡"),
    ],
)
def test_reported_provider_model_accept_arbitrary_strings(field_name: str, value: str) -> None:
    # Runtime-reported observations accept arbitrary strings; they are not
    # vocabularies, not enums, and never configuration authority.
    connection = _connection(**{field_name: value})
    assert getattr(connection, field_name) == value


def test_reported_provider_model_accept_null() -> None:
    connection = _connection(reported_provider=None, reported_model=None)
    assert connection.reported_provider is None
    assert connection.reported_model is None


@pytest.mark.parametrize(
    ("field_name", "value"), [("reported_provider", 7), ("reported_model", [])]
)
def test_reported_provider_model_reject_non_strings(field_name: str, value: Any) -> None:
    with pytest.raises(ConnectionDomainError):
        _connection(**{field_name: value})


def test_safe_config_accepts_structurally_valid_json_content() -> None:
    config = {"base_url": "https://hub.example.com", "nested": {"flags": [1, 2, 3]}}
    connection = _connection(safe_config=config)
    assert dict(connection.safe_config) == config


def test_safe_config_rejects_non_json_serializable_content() -> None:
    with pytest.raises(ConnectionDomainError):
        _connection(safe_config={"bad": {1, 2}})


def test_safe_config_rejects_non_json_numbers() -> None:
    # jsonb storage does not accept NaN/Infinity; strict serialization rejects
    # them here rather than at the database boundary.
    with pytest.raises(ConnectionDomainError):
        _connection(safe_config={"ratio": float("nan")})


def test_safe_config_must_be_a_mapping() -> None:
    with pytest.raises(ConnectionDomainError):
        _connection(safe_config=["not", "a", "mapping"])  # type: ignore[arg-type]


def test_safe_config_view_is_immutable() -> None:
    connection = _connection(safe_config={"a": 1})
    with pytest.raises(TypeError):
        connection.safe_config["a"] = 2  # type: ignore[index]


def test_no_raw_secret_field_exists_on_domain_objects() -> None:
    # Ordinary persistence objects expose no raw-secret field. OpenOrc-owned
    # authentication is represented only through the opaque nullable
    # ``auth_reference`` boundary; everything else is routing/configuration/
    # observation metadata. The exact field sets are asserted so a future
    # raw-credential field cannot appear silently.
    assert connection_field_names() == frozenset(
        {
            "id",
            "workspace_id",
            "adapter",
            "name",
            "safe_config",
            "session_capacity",
            "enabled",
            "auth_reference",
            "reported_provider",
            "reported_model",
            "created_at",
            "updated_at",
        }
    )
    assert workflow_role_binding_field_names() == frozenset(
        {"id", "workspace_id", "role", "connection_id", "created_at", "updated_at"}
    )


def test_producer_and_reviewer_bindings_may_share_one_connection() -> None:
    workspace_id = uuid4()
    connection_id = uuid4()
    producer = _binding(
        WorkflowRole.PRODUCER, workspace_id=workspace_id, connection_id=connection_id
    )
    reviewer = _binding(
        WorkflowRole.REVIEWER, workspace_id=workspace_id, connection_id=connection_id
    )
    assert producer.connection_id == reviewer.connection_id
    assert producer.id != reviewer.id
    assert producer.role != reviewer.role


def test_producer_and_reviewer_bindings_may_use_separate_connections() -> None:
    workspace_id = uuid4()
    producer = _binding(WorkflowRole.PRODUCER, workspace_id=workspace_id, connection_id=uuid4())
    reviewer = _binding(WorkflowRole.REVIEWER, workspace_id=workspace_id, connection_id=uuid4())
    assert producer.connection_id != reviewer.connection_id


def test_binding_requires_a_workflow_role() -> None:
    with pytest.raises(ConnectionDomainError):
        _binding(role="producer")  # type: ignore[arg-type]
