"""Deterministic tests for the Connection control-credential service (issue #55).

The ordinary suite cannot execute Postgres: canned rows and a scripted fake
connection seam prove the validation-before-mutation rule, the Owner
authorization composition, the atomic configure/rotate flows at the SQL
level, the fail-closed resolution states, the secret-bearing redaction, and
that application errors never contain credential material. Durable Vault
behavior is proven against a real Supabase branch by the integration-marked
suite.
"""

from __future__ import annotations

import logging
import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime
from typing import Any, cast

import pytest
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

from openorc.observability import (
    CONNECTION_ID,
    OPERATION,
    WORKSPACE_ID,
    injected_tracer_source,
)
from openorc.persistence import runtime_control_secrets
from openorc.persistence.pool import DatabasePool
from openorc.services import connection_credentials
from openorc.services.connection_credentials import ControlEndpointSecret
from openorc.services.errors import ConflictError, InvalidCommandError, NotFoundError

_OBSERVED = datetime(2026, 9, 21, 12, 0, 0, tzinfo=UTC)

_CREDENTIAL = "s3cr3t-credential-value"
_ROTATED_CREDENTIAL = "n3w-creden7i4l-v4lu3"


class FakeCursor:
    """Returns one canned row, like a psycopg cursor."""

    def __init__(self, row: tuple[Any, ...] | None) -> None:
        self._row = row

    def fetchone(self) -> tuple[Any, ...] | None:
        return self._row


class ScriptedConnection:
    """Plays back canned statement results in order, recording executed SQL."""

    def __init__(self, results: list[tuple[Any, ...] | None]) -> None:
        self.results = list(results)
        self.executed: list[tuple[str, tuple[Any, ...] | None]] = []

    def execute(self, sql: str, params: tuple[Any, ...] | None = None) -> FakeCursor:
        self.executed.append((sql, params))
        return FakeCursor(self.results.pop(0))

    @contextmanager
    def transaction(self) -> Iterator[None]:
        # Supports composed_transaction's outer transaction entry and the
        # nested repository scopes under it.
        yield


class FakePool:
    """Emulates psycopg_pool ConnectionPool.connection() semantics."""

    def __init__(self, conn: ScriptedConnection) -> None:
        self._conn = conn

    def connection(self) -> Any:
        @contextmanager
        def managed() -> Iterator[ScriptedConnection]:
            yield self._conn

        return managed()

    def close(self) -> None:
        raise AssertionError("credential service tests never close pools")


def _pool(conn: ScriptedConnection) -> DatabasePool:
    return cast(DatabasePool, FakePool(conn))


def _ws_row(owner_profile_id: Any) -> tuple[Any, ...]:
    return (uuid.uuid4(), owner_profile_id, "platform", _OBSERVED, _OBSERVED, 5, "")


def _connection_row(
    workspace_id: Any,
    *,
    connection_id: Any = None,
    auth_reference: str | None = None,
    enabled: bool = True,
) -> tuple[Any, ...]:
    return (
        uuid.uuid4() if connection_id is None else connection_id,
        workspace_id,
        "cline",
        "primary cline hub",
        {},
        1,
        enabled,
        auth_reference,
        None,
        None,
        _OBSERVED,
        _OBSERVED,
    )


def test_configure_rejects_invalid_credentials_before_any_mutation() -> None:
    profile_id = uuid.uuid4()

    for bad in (None, 123, b"s3cr3t", ""):
        conn = ScriptedConnection([])
        with pytest.raises(InvalidCommandError):
            connection_credentials.configure_connection_credential(
                _pool(conn),
                profile_id=profile_id,
                workspace_id=uuid.uuid4(),
                connection_id=uuid.uuid4(),
                credential=bad,  # type: ignore[arg-type]
            )
        # Validation precedes every mutation: no ownership read, no row lock,
        # no Vault call, and no Connection write happened at all.
        assert conn.executed == []


def test_rotate_rejects_invalid_credentials_before_any_mutation() -> None:
    profile_id = uuid.uuid4()

    for bad in (None, 123, b"s3cr3t", ""):
        conn = ScriptedConnection([])
        with pytest.raises(InvalidCommandError):
            connection_credentials.rotate_connection_credential(
                _pool(conn),
                profile_id=profile_id,
                workspace_id=uuid.uuid4(),
                connection_id=uuid.uuid4(),
                credential=bad,  # type: ignore[arg-type]
            )
        assert conn.executed == []


def test_configure_preserves_non_empty_values_verbatim() -> None:
    profile_id = uuid.uuid4()
    workspace_id = uuid.uuid4()
    secret_id = uuid.uuid4()
    locked = _connection_row(workspace_id)
    reference = runtime_control_secrets.encode_connection_auth_reference(secret_id)
    updated = _connection_row(workspace_id, auth_reference=reference)
    conn = ScriptedConnection([_ws_row(profile_id), locked, (secret_id,), updated])

    connection_credentials.configure_connection_credential(
        _pool(conn),
        profile_id=profile_id,
        workspace_id=workspace_id,
        connection_id=locked[0],
        credential="  spaced value  ",
    )

    create_sql, create_params = conn.executed[2]
    assert create_sql == "select vault.create_secret(%s, null, %s)"
    assert create_params is not None
    # No stripping or normalization: credential bytes are meaningful.
    assert create_params[0] == "  spaced value  "


def test_configure_creates_the_secret_and_installs_the_reference_atomically() -> None:
    profile_id = uuid.uuid4()
    workspace_id = uuid.uuid4()
    secret_id = uuid.uuid4()
    locked = _connection_row(workspace_id)
    reference = runtime_control_secrets.encode_connection_auth_reference(secret_id)
    updated = _connection_row(workspace_id, auth_reference=reference)
    conn = ScriptedConnection([_ws_row(profile_id), locked, (secret_id,), updated])

    result = connection_credentials.configure_connection_credential(
        _pool(conn),
        profile_id=profile_id,
        workspace_id=workspace_id,
        connection_id=locked[0],
        credential=_CREDENTIAL,
    )

    assert result.auth_reference == reference
    # One composed transaction: ownership gate, row-locked read, Vault secret
    # creation, then the opaque reference install.
    ownership_sql, _ = conn.executed[0]
    assert "from openorc.workspaces where id = %s" in ownership_sql
    lock_sql, _ = conn.executed[1]
    assert "for update" in lock_sql
    create_sql, create_params = conn.executed[2]
    assert create_sql == "select vault.create_secret(%s, null, %s)"
    assert create_params is not None
    assert create_params[0] == _CREDENTIAL
    install_sql, install_params = conn.executed[3]
    assert "update openorc.connections" in install_sql
    assert "set auth_reference = %s" in install_sql
    assert install_params == (reference, locked[0])


def test_configure_conflicts_when_a_credential_is_already_configured() -> None:
    profile_id = uuid.uuid4()
    workspace_id = uuid.uuid4()
    locked = _connection_row(
        workspace_id,
        auth_reference=runtime_control_secrets.encode_connection_auth_reference(uuid.uuid4()),
    )
    conn = ScriptedConnection([_ws_row(profile_id), locked])

    with pytest.raises(ConflictError):
        connection_credentials.configure_connection_credential(
            _pool(conn),
            profile_id=profile_id,
            workspace_id=workspace_id,
            connection_id=locked[0],
            credential=_CREDENTIAL,
        )

    # Ownership gate + locked read ran; no Vault secret was created.
    assert len(conn.executed) == 2
    assert all("vault.create_secret" not in sql for sql, _ in conn.executed)


def test_configure_fails_closed_for_missing_and_cross_workspace_connections() -> None:
    profile_id = uuid.uuid4()
    workspace_id = uuid.uuid4()
    other_workspace = uuid.uuid4()

    missing = ScriptedConnection([_ws_row(profile_id), None])
    with pytest.raises(NotFoundError):
        connection_credentials.configure_connection_credential(
            _pool(missing),
            profile_id=profile_id,
            workspace_id=workspace_id,
            connection_id=uuid.uuid4(),
            credential=_CREDENTIAL,
        )
    assert len(missing.executed) == 2
    assert all("vault" not in sql for sql, _ in missing.executed)

    other_row = _connection_row(other_workspace)
    cross = ScriptedConnection([_ws_row(profile_id), other_row])
    with pytest.raises(NotFoundError):
        connection_credentials.configure_connection_credential(
            _pool(cross),
            profile_id=profile_id,
            workspace_id=workspace_id,
            connection_id=other_row[0],
            credential=_CREDENTIAL,
        )
    assert len(cross.executed) == 2
    assert all("vault" not in sql for sql, _ in cross.executed)


def test_configure_fails_closed_for_non_owners() -> None:
    conn = ScriptedConnection([_ws_row(uuid.uuid4())])

    with pytest.raises(NotFoundError):
        connection_credentials.configure_connection_credential(
            _pool(conn),
            profile_id=uuid.uuid4(),
            workspace_id=uuid.uuid4(),
            connection_id=uuid.uuid4(),
            credential=_CREDENTIAL,
        )

    # Only the ownership gate ran; nothing else was attempted.
    assert len(conn.executed) == 1


def test_rotate_updates_the_secret_in_place_and_keeps_the_reference() -> None:
    profile_id = uuid.uuid4()
    workspace_id = uuid.uuid4()
    secret_id = uuid.uuid4()
    reference = runtime_control_secrets.encode_connection_auth_reference(secret_id)
    locked = _connection_row(workspace_id, auth_reference=reference)
    conn = ScriptedConnection([_ws_row(profile_id), locked, (1,), (None,)])

    result = connection_credentials.rotate_connection_credential(
        _pool(conn),
        profile_id=profile_id,
        workspace_id=workspace_id,
        connection_id=locked[0],
        credential=_ROTATED_CREDENTIAL,
    )

    assert result.auth_reference == reference
    # In-place update: the Vault function is called with the exact secret id
    # and the new value; the Connection row is never rewritten.
    assert len(conn.executed) == 4
    existence_sql, _ = conn.executed[2]
    assert existence_sql == "select 1 from vault.secrets where id = %s"
    update_sql, update_params = conn.executed[3]
    assert update_sql == "select vault.update_secret(%s, %s)"
    assert update_params == (secret_id, _ROTATED_CREDENTIAL)
    assert all("update openorc.connections" not in sql for sql, _ in conn.executed)


def test_rotate_conflicts_without_a_configured_credential() -> None:
    profile_id = uuid.uuid4()
    workspace_id = uuid.uuid4()
    locked = _connection_row(workspace_id, auth_reference=None)
    conn = ScriptedConnection([_ws_row(profile_id), locked])

    with pytest.raises(ConflictError):
        connection_credentials.rotate_connection_credential(
            _pool(conn),
            profile_id=profile_id,
            workspace_id=workspace_id,
            connection_id=locked[0],
            credential=_ROTATED_CREDENTIAL,
        )

    assert len(conn.executed) == 2
    assert all("vault.update_secret" not in sql for sql, _ in conn.executed)


def test_rotate_conflicts_on_a_malformed_reference() -> None:
    profile_id = uuid.uuid4()
    workspace_id = uuid.uuid4()
    locked = _connection_row(workspace_id, auth_reference="vault://openorc/connection-auth/abc")
    conn = ScriptedConnection([_ws_row(profile_id), locked])

    with pytest.raises(ConflictError):
        connection_credentials.rotate_connection_credential(
            _pool(conn),
            profile_id=profile_id,
            workspace_id=workspace_id,
            connection_id=locked[0],
            credential=_ROTATED_CREDENTIAL,
        )

    assert len(conn.executed) == 2
    assert all("vault.update_secret" not in sql for sql, _ in conn.executed)


def test_rotate_conflicts_on_a_dangling_secret() -> None:
    profile_id = uuid.uuid4()
    workspace_id = uuid.uuid4()
    locked = _connection_row(
        workspace_id,
        auth_reference=runtime_control_secrets.encode_connection_auth_reference(uuid.uuid4()),
    )
    conn = ScriptedConnection([_ws_row(profile_id), locked, None])

    with pytest.raises(ConflictError):
        connection_credentials.rotate_connection_credential(
            _pool(conn),
            profile_id=profile_id,
            workspace_id=workspace_id,
            connection_id=locked[0],
            credential=_ROTATED_CREDENTIAL,
        )

    # Existence check ran; the in-place update did not.
    assert len(conn.executed) == 3
    assert all("vault.update_secret" not in sql for sql, _ in conn.executed)


def test_rotate_failure_messages_never_contain_the_credential() -> None:
    profile_id = uuid.uuid4()
    workspace_id = uuid.uuid4()
    locked = _connection_row(
        workspace_id,
        auth_reference=runtime_control_secrets.encode_connection_auth_reference(uuid.uuid4()),
    )
    conn = ScriptedConnection([_ws_row(profile_id), locked, None])

    with pytest.raises(ConflictError) as error:
        connection_credentials.rotate_connection_credential(
            _pool(conn),
            profile_id=profile_id,
            workspace_id=workspace_id,
            connection_id=locked[0],
            credential=_ROTATED_CREDENTIAL,
        )

    assert _ROTATED_CREDENTIAL not in str(error.value)


def test_resolution_wraps_the_exact_secret_in_the_secret_boundary() -> None:
    workspace_id = uuid.uuid4()
    secret_id = uuid.uuid4()
    reference = runtime_control_secrets.encode_connection_auth_reference(secret_id)
    row = _connection_row(workspace_id, auth_reference=reference)
    conn = ScriptedConnection([row, (_CREDENTIAL,)])

    connection, secret = connection_credentials.resolve_connection_runtime_credential(
        _pool(conn), workspace_id=workspace_id, connection_id=row[0]
    )

    assert connection.id == row[0]
    # The Connection object carries the opaque reference only — the secret
    # value lives solely inside the secret-bearing boundary.
    assert connection.auth_reference == reference
    assert secret.secret_value() == _CREDENTIAL
    sql, params = conn.executed[1]
    assert sql == "select decrypted_secret from vault.decrypted_secrets where id = %s"
    assert params == (secret_id,)


def test_control_endpoint_secret_never_exposes_the_value_ordinarily() -> None:
    secret = ControlEndpointSecret(_CREDENTIAL)

    assert secret.secret_value() == _CREDENTIAL
    # Ordinary representation is redacted: the value cannot leak through
    # repr/str into logs, debugging output, or exception formatting.
    assert _CREDENTIAL not in repr(secret)
    assert _CREDENTIAL not in str(secret)
    assert repr(secret) == "ControlEndpointSecret(<redacted>)"
    assert str(secret) == "ControlEndpointSecret(<redacted>)"
    assert f"{secret} {secret!r}" == (
        "ControlEndpointSecret(<redacted>) ControlEndpointSecret(<redacted>)"
    )
    # No dataclass/attribute surface: the value stays behind the explicit
    # accessor, and the type is not an ordinary DTO-like object.
    assert not hasattr(secret, "__dict__")
    assert not hasattr(secret, "__dataclass_fields__")


def test_resolution_fails_closed_on_a_disabled_connection() -> None:
    workspace_id = uuid.uuid4()
    row = _connection_row(
        workspace_id,
        enabled=False,
        auth_reference=runtime_control_secrets.encode_connection_auth_reference(uuid.uuid4()),
    )
    conn = ScriptedConnection([row])

    with pytest.raises(ConflictError):
        connection_credentials.resolve_connection_runtime_credential(
            _pool(conn), workspace_id=workspace_id, connection_id=row[0]
        )

    assert len(conn.executed) == 1


def test_resolution_fails_closed_without_a_configured_credential() -> None:
    workspace_id = uuid.uuid4()
    row = _connection_row(workspace_id, auth_reference=None)
    conn = ScriptedConnection([row])

    with pytest.raises(ConflictError):
        connection_credentials.resolve_connection_runtime_credential(
            _pool(conn), workspace_id=workspace_id, connection_id=row[0]
        )

    assert len(conn.executed) == 1


def test_resolution_fails_closed_on_a_malformed_reference() -> None:
    workspace_id = uuid.uuid4()
    row = _connection_row(workspace_id, auth_reference="openorc:connection-auth:v1:notvault:x")
    conn = ScriptedConnection([row])

    with pytest.raises(ConflictError):
        connection_credentials.resolve_connection_runtime_credential(
            _pool(conn), workspace_id=workspace_id, connection_id=row[0]
        )

    assert len(conn.executed) == 1


def test_resolution_fails_closed_on_a_dangling_secret() -> None:
    workspace_id = uuid.uuid4()
    row = _connection_row(
        workspace_id,
        auth_reference=runtime_control_secrets.encode_connection_auth_reference(uuid.uuid4()),
    )
    conn = ScriptedConnection([row, None])

    with pytest.raises(ConflictError):
        connection_credentials.resolve_connection_runtime_credential(
            _pool(conn), workspace_id=workspace_id, connection_id=row[0]
        )

    assert len(conn.executed) == 2


def test_resolution_fails_closed_for_missing_and_cross_workspace_connections() -> None:
    workspace_id = uuid.uuid4()

    missing = ScriptedConnection([None])
    with pytest.raises(NotFoundError):
        connection_credentials.resolve_connection_runtime_credential(
            _pool(missing), workspace_id=workspace_id, connection_id=uuid.uuid4()
        )
    assert len(missing.executed) == 1

    other_row = _connection_row(uuid.uuid4())
    cross = ScriptedConnection([other_row])
    with pytest.raises(NotFoundError):
        connection_credentials.resolve_connection_runtime_credential(
            _pool(cross), workspace_id=workspace_id, connection_id=other_row[0]
        )
    assert len(cross.executed) == 1


def test_resolution_failure_messages_never_contain_the_secret() -> None:
    workspace_id = uuid.uuid4()
    row = _connection_row(
        workspace_id,
        auth_reference=runtime_control_secrets.encode_connection_auth_reference(uuid.uuid4()),
    )
    conn = ScriptedConnection([row, None])

    with pytest.raises(ConflictError) as error:
        connection_credentials.resolve_connection_runtime_credential(
            _pool(conn), workspace_id=workspace_id, connection_id=row[0]
        )

    assert _CREDENTIAL not in str(error.value)


def _local_provider_with_exporter() -> tuple[TracerProvider, InMemorySpanExporter]:
    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    return provider, exporter


def test_configure_opens_its_service_span_without_credential_material() -> None:
    provider, exporter = _local_provider_with_exporter()
    profile_id = uuid.uuid4()
    workspace_id = uuid.uuid4()
    secret_id = uuid.uuid4()
    locked = _connection_row(workspace_id)
    reference = runtime_control_secrets.encode_connection_auth_reference(secret_id)
    updated = _connection_row(workspace_id, auth_reference=reference)
    conn = ScriptedConnection([_ws_row(profile_id), locked, (secret_id,), updated])

    with injected_tracer_source(lambda name: provider.get_tracer(name)):
        connection_credentials.configure_connection_credential(
            _pool(conn),
            profile_id=profile_id,
            workspace_id=workspace_id,
            connection_id=locked[0],
            credential=_CREDENTIAL,
        )

    (exported,) = exporter.get_finished_spans()
    assert exported.name == "connection_credentials.configure_connection_credential"
    attributes = exported.attributes
    assert attributes is not None
    assert attributes[OPERATION] == "connection_credentials.configure_connection_credential"
    assert attributes[WORKSPACE_ID] == str(workspace_id)
    assert attributes[CONNECTION_ID] == str(locked[0])
    # The credential value and the opaque reference have no supported path
    # into telemetry attributes.
    exported_blob = str(attributes)
    assert _CREDENTIAL not in exported_blob
    assert reference not in exported_blob


def test_rotate_opens_its_service_span_without_credential_material() -> None:
    provider, exporter = _local_provider_with_exporter()
    profile_id = uuid.uuid4()
    workspace_id = uuid.uuid4()
    reference = runtime_control_secrets.encode_connection_auth_reference(uuid.uuid4())
    locked = _connection_row(workspace_id, auth_reference=reference)
    conn = ScriptedConnection([_ws_row(profile_id), locked, (1,), (None,)])

    with injected_tracer_source(lambda name: provider.get_tracer(name)):
        connection_credentials.rotate_connection_credential(
            _pool(conn),
            profile_id=profile_id,
            workspace_id=workspace_id,
            connection_id=locked[0],
            credential=_ROTATED_CREDENTIAL,
        )

    (exported,) = exporter.get_finished_spans()
    assert exported.name == "connection_credentials.rotate_connection_credential"
    attributes = exported.attributes
    assert attributes is not None
    assert attributes[WORKSPACE_ID] == str(workspace_id)
    assert attributes[CONNECTION_ID] == str(locked[0])
    exported_blob = str(attributes)
    assert _ROTATED_CREDENTIAL not in exported_blob
    assert reference not in exported_blob


def test_resolution_opens_its_service_span_without_credential_material() -> None:
    provider, exporter = _local_provider_with_exporter()
    workspace_id = uuid.uuid4()
    row = _connection_row(
        workspace_id,
        auth_reference=runtime_control_secrets.encode_connection_auth_reference(uuid.uuid4()),
    )
    conn = ScriptedConnection([row, (_CREDENTIAL,)])

    with injected_tracer_source(lambda name: provider.get_tracer(name)):
        _, secret = connection_credentials.resolve_connection_runtime_credential(
            _pool(conn), workspace_id=workspace_id, connection_id=row[0]
        )

    assert secret.secret_value() == _CREDENTIAL
    (exported,) = exporter.get_finished_spans()
    assert exported.name == "connection_credentials.resolve_connection_runtime_credential"
    attributes = exported.attributes
    assert attributes is not None
    assert attributes[WORKSPACE_ID] == str(workspace_id)
    assert attributes[CONNECTION_ID] == str(row[0])
    assert _CREDENTIAL not in str(attributes)


def test_resolution_dangling_reference_logs_a_safe_warning(
    caplog: pytest.LogCaptureFixture,
) -> None:
    workspace_id = uuid.uuid4()
    secret_id = uuid.uuid4()
    reference = runtime_control_secrets.encode_connection_auth_reference(secret_id)
    row = _connection_row(workspace_id, auth_reference=reference)
    conn = ScriptedConnection([row, None])

    with (
        caplog.at_level(logging.WARNING, logger="openorc.services.connection_credentials"),
        pytest.raises(ConflictError),
    ):
        connection_credentials.resolve_connection_runtime_credential(
            _pool(conn), workspace_id=workspace_id, connection_id=row[0]
        )

    warnings = [
        record
        for record in caplog.records
        if record.name == "openorc.services.connection_credentials"
    ]
    assert len(warnings) == 1
    assert warnings[0].levelno == logging.WARNING
    # Fixed safe message only: never the reference value or credential material.
    message = warnings[0].getMessage()
    assert "dangling auth reference" in message
    assert _CREDENTIAL not in message
    assert reference not in message
    assert str(secret_id) not in message


def test_resolution_unrecognized_reference_logs_a_safe_warning(
    caplog: pytest.LogCaptureFixture,
) -> None:
    workspace_id = uuid.uuid4()
    row = _connection_row(workspace_id, auth_reference="openorc:connection-auth:v1:notvault:x")
    conn = ScriptedConnection([row])

    with (
        caplog.at_level(logging.WARNING, logger="openorc.services.connection_credentials"),
        pytest.raises(ConflictError),
    ):
        connection_credentials.resolve_connection_runtime_credential(
            _pool(conn), workspace_id=workspace_id, connection_id=row[0]
        )

    warnings = [
        record
        for record in caplog.records
        if record.name == "openorc.services.connection_credentials"
    ]
    assert len(warnings) == 1
    assert "unrecognized auth reference" in warnings[0].getMessage()


def test_rotation_dangling_reference_logs_a_safe_warning(
    caplog: pytest.LogCaptureFixture,
) -> None:
    profile_id = uuid.uuid4()
    workspace_id = uuid.uuid4()
    secret_id = uuid.uuid4()
    reference = runtime_control_secrets.encode_connection_auth_reference(secret_id)
    row = _connection_row(workspace_id, auth_reference=reference)
    conn = ScriptedConnection([_ws_row(profile_id), row, None])

    with (
        caplog.at_level(logging.WARNING, logger="openorc.services.connection_credentials"),
        pytest.raises(ConflictError),
    ):
        connection_credentials.rotate_connection_credential(
            _pool(conn),
            profile_id=profile_id,
            workspace_id=workspace_id,
            connection_id=row[0],
            credential=_ROTATED_CREDENTIAL,
        )

    warnings = [
        record
        for record in caplog.records
        if record.name == "openorc.services.connection_credentials"
    ]
    assert len(warnings) == 1
    message = warnings[0].getMessage()
    assert "dangling auth reference" in message
    assert _ROTATED_CREDENTIAL not in message
    assert reference not in message
    assert str(secret_id) not in message


def test_rotation_unrecognized_reference_logs_a_safe_warning(
    caplog: pytest.LogCaptureFixture,
) -> None:
    profile_id = uuid.uuid4()
    workspace_id = uuid.uuid4()
    row = _connection_row(workspace_id, auth_reference="vault://openorc/connection-auth/abc")
    conn = ScriptedConnection([_ws_row(profile_id), row])

    with (
        caplog.at_level(logging.WARNING, logger="openorc.services.connection_credentials"),
        pytest.raises(ConflictError),
    ):
        connection_credentials.rotate_connection_credential(
            _pool(conn),
            profile_id=profile_id,
            workspace_id=workspace_id,
            connection_id=row[0],
            credential=_ROTATED_CREDENTIAL,
        )

    warnings = [
        record
        for record in caplog.records
        if record.name == "openorc.services.connection_credentials"
    ]
    assert len(warnings) == 1
    message = warnings[0].getMessage()
    assert "unrecognized auth reference" in message
    assert _ROTATED_CREDENTIAL not in message
