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
        "configured_provider": None,
        "configured_model": None,
        "role_prompt_override": None,
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
    # jsonb storage does not accept NaN/Infinity; canonical JSON rejects
    # them here rather than at the database boundary.
    with pytest.raises(ConnectionDomainError):
        _connection(safe_config={"ratio": float("nan")})


@pytest.mark.parametrize("bad_config", [{1: "a"}, {None: "a"}, {"nested": {2: "b"}}])
def test_safe_config_requires_string_keys_at_every_level(bad_config: dict[Any, Any]) -> None:
    # Canonical JSON-object semantics: keys are strings everywhere. Nothing
    # relies on json's silent key coercion ({"1": ...} would change what the
    # caller sent).
    with pytest.raises(ConnectionDomainError):
        _connection(safe_config=bad_config)


def test_safe_config_normalizes_sequences_to_lists() -> None:
    # Canonical representation: tuples do not silently survive as tuples;
    # what is stored is exactly what a reload reads back.
    connection = _connection(safe_config={"flags": (1, 2), "nested": {"more": (3,)}})
    assert dict(connection.safe_config) == {"flags": [1, 2], "nested": {"more": [3]}}


def test_safe_config_rejects_non_json_value_types() -> None:
    with pytest.raises(ConnectionDomainError):
        _connection(safe_config={"raw": b"bytes"})


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
        {
            "id",
            "workspace_id",
            "role",
            "connection_id",
            "configured_provider",
            "configured_model",
            "role_prompt_override",
            "created_at",
            "updated_at",
        }
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


# --- concrete per-role runtime session configuration (issue #162) -------------


def test_binding_provider_model_pair_may_be_fully_configured() -> None:
    binding = _binding(configured_provider="provider-id-1", configured_model="model-id-x")
    assert binding.configured_provider == "provider-id-1"
    assert binding.configured_model == "model-id-x"


def test_binding_provider_model_pair_may_remain_unconfigured() -> None:
    # Existing/legacy bindings migrate as unconfigured: both values absent is
    # the honest, representable state with no invented defaults.
    binding = _binding()
    assert binding.configured_provider is None
    assert binding.configured_model is None


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("configured_provider", "provider-id-1"),
        ("configured_model", "model-id-x"),
    ],
)
def test_binding_partial_provider_model_pair_is_rejected(field: str, value: str) -> None:
    # Configuration is complete only when both values are present; a partial
    # one-value pair is an impossible configuration and cannot be represented.
    overrides: dict[str, Any] = {field: value}
    with pytest.raises(ConnectionDomainError):
        _binding(**overrides)


@pytest.mark.parametrize("blank", ["", "   ", "\t\n"])
def test_binding_blank_pair_members_are_rejected(blank: str) -> None:
    with pytest.raises(ConnectionDomainError):
        _binding(configured_provider=blank, configured_model="model-id-x")
    with pytest.raises(ConnectionDomainError):
        _binding(configured_provider="provider-id-1", configured_model=blank)


@pytest.mark.parametrize("bad", [1, True, 1.5])
def test_binding_non_string_pair_members_are_rejected(bad: Any) -> None:
    with pytest.raises(ConnectionDomainError):
        _binding(configured_provider=bad, configured_model="model-id-x")  # type: ignore[arg-type]


def test_binding_role_prompt_override_is_none_or_any_verbatim_string() -> None:
    # NULL means the current shipped default applies. ANY non-NULL string —
    # including an empty or whitespace-only one — is the Owner's explicit
    # override, stored and passed verbatim; Core does not parse, classify,
    # or reason about the prose.
    assert _binding().role_prompt_override is None
    markdown = "# Producer instructions\n\n第二段落 ⚡"
    assert _binding(role_prompt_override=markdown).role_prompt_override == markdown
    assert _binding(role_prompt_override="").role_prompt_override == ""
    assert _binding(role_prompt_override="   ").role_prompt_override == "   "


def test_binding_role_prompt_override_non_string_is_rejected() -> None:
    with pytest.raises(ConnectionDomainError):
        _binding(role_prompt_override=123)  # type: ignore[arg-type]
