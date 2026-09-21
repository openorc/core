"""Integration tests for the secure Connection credential boundary (issue #55).

These tests run against the explicitly supplied non-production Supabase
branch database (the existing conftest path) and prove the real Vault
behavior of the credential boundary: extension availability, the effective
backend-only privilege posture, create/resolve/update-in-place/delete by
exact UUID, and the atomic configure/rotate compositions — including that a
forced second-write failure leaves neither an orphaned Vault secret nor a
Connection reference pointing at one. They are excluded from the ordinary
deterministic baseline by the repository pytest configuration.

Run explicitly when a target has been made available:

    OPENORC_TEST_DATABASE_URL=<supplied non-production branch database URL> \
      .venv/bin/python -m pytest -m integration \
      tests/integration/test_connection_credential_services.py

The suite consumes the database it is given and never provisions one;
provisioning and teardown of the target stay with the Owner-run
``devserver.sh --testdb`` flow. Agents never invoke that tooling. The module
borrows the fixture connection through a passthrough pool adapter that adds
no transaction or lifetime behavior of its own, so
``composed_transaction``'s explicit outer block is the only composition-wide
transaction scope (see tests/integration/test_service_transaction_composition.py
for the full regression rationale).
"""

from __future__ import annotations

import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any, cast

import pytest
from psycopg import Connection
from psycopg.types.json import Jsonb

from openorc.persistence import runtime_control_secrets
from openorc.persistence.pool import DatabasePool
from openorc.services import connection_credentials
from openorc.services.errors import ConflictError, InvalidCommandError

pytestmark = pytest.mark.integration

_CREDENTIAL = "s3cr3t-credential-value"
_ROTATED_CREDENTIAL = "n3w-creden7i4l-v4lu3"


class _BorrowedConnectionPool:
    """Borrows the fixture-owned connection; adds no transaction or lifetime behavior.

    The fixture owns the connection's lifetime and its implicit transaction.
    This adapter only lends the connection to the production primitives — no
    ``with conn:`` wrapping, no checkout bookkeeping, nothing to close.
    """

    def __init__(self, connection: Connection[Any]) -> None:
        self._connection = connection

    @contextmanager
    def connection(self) -> Iterator[Connection[Any]]:
        yield self._connection

    def close(self) -> None:
        raise AssertionError("the test fixture owns the connection lifetime")


def _pool(conn: Connection[Any]) -> DatabasePool:
    return cast(DatabasePool, _BorrowedConnectionPool(conn))


def _insert_profile(conn: Connection[Any]) -> uuid.UUID:
    profile_id = uuid.uuid4()
    # profiles.id references auth.users (id) ON DELETE CASCADE (issue #27):
    # every Profile needs its backing Auth user row. The inserts roll back
    # with the test transaction.
    conn.execute("insert into auth.users (id) values (%s)", (profile_id,))
    conn.execute("insert into openorc.profiles (id) values (%s)", (profile_id,))
    return profile_id


def _insert_workspace(conn: Connection[Any], owner_profile_id: uuid.UUID) -> uuid.UUID:
    workspace_id = uuid.uuid4()
    conn.execute(
        "insert into openorc.workspaces (id, owner_profile_id, name) values (%s, %s, %s)",
        (workspace_id, owner_profile_id, "workspace"),
    )
    return workspace_id


def _insert_connection(
    conn: Connection[Any],
    *,
    workspace_id: uuid.UUID,
    enabled: bool = True,
    auth_reference: str | None = None,
) -> uuid.UUID:
    connection_id = uuid.uuid4()
    conn.execute(
        "insert into openorc.connections "
        "(id, workspace_id, adapter_type, name, safe_config, session_capacity, enabled, "
        "auth_reference) "
        "values (%s, %s, %s, %s, %s, %s, %s, %s)",
        (
            connection_id,
            workspace_id,
            "cline",
            "primary cline hub",
            Jsonb({}),
            1,
            enabled,
            auth_reference,
        ),
    )
    return connection_id


def _secret_rows(conn: Connection[Any], *, connection_id: uuid.UUID) -> int:
    row = conn.execute(
        "select count(*) from vault.secrets where description like %s",
        (f"%{connection_id}%",),
    ).fetchone()
    assert row is not None
    return row[0]


def _auth_reference(conn: Connection[Any], connection_id: uuid.UUID) -> str | None:
    row = conn.execute(
        "select auth_reference from openorc.connections where id = %s",
        (connection_id,),
    ).fetchone()
    assert row is not None
    return row[0]


def test_vault_extension_is_available(conn: Connection[Any]) -> None:
    row = conn.execute(
        "select exists(select 1 from pg_extension where extname = 'supabase_vault'), "
        "exists(select 1 from pg_namespace where nspname = 'vault')"
    ).fetchone()

    assert row is not None
    assert row == (True, True)


def test_vault_privilege_posture_is_backend_only(conn: Connection[Any]) -> None:
    """Prove the effective privilege posture on the real branch, not the text.

    Every non-backend role (anon, authenticated, service_role) must have no
    path into the vault schema, the secret table/view, or the
    secret-management functions, while the backend — the connecting, owning
    role the credential services actually run as — keeps the full direct
    Postgres path.
    """
    non_backend_roles = ("anon", "authenticated", "service_role")

    for role in non_backend_roles:
        schema_usable = conn.execute(
            "select has_schema_privilege(%s, 'vault', 'USAGE')", (role,)
        ).fetchone()
        assert schema_usable is not None and schema_usable[0] is False

    for table in ("vault.secrets", "vault.decrypted_secrets"):
        for privilege in ("SELECT", "INSERT", "UPDATE", "DELETE"):
            for role in non_backend_roles:
                granted = conn.execute(
                    "select has_table_privilege(%s, %s, %s)", (role, table, privilege)
                ).fetchone()
                assert granted is not None and granted[0] is False, (table, privilege, role)

    # Vault secret-management and decryption functions, resolved by exact
    # signature. The decryption helper is included deliberately: current
    # platform images may grant it to service_role, and the migration's
    # guarded revoke must have removed that grant. A signature absent from an
    # older platform image is vacuous no-access and is skipped gracefully;
    # the create/update round trips pin the supported surface regardless.
    for signature in (
        "vault.create_secret(text,text,text,uuid)",
        "vault.update_secret(uuid,text,text,text,uuid)",
        "vault._crypto_aead_det_decrypt(bytea,bytea,bigint,bytea,bytea)",
    ):
        function = conn.execute("select to_regprocedure(%s)", (signature,)).fetchone()
        if function is None or function[0] is None:
            continue
        for role in non_backend_roles:
            granted = conn.execute(
                "select has_function_privilege(%s, %s, 'EXECUTE')", (role, function[0])
            ).fetchone()
            assert granted is not None and granted[0] is False, (signature, role)
        backend_granted = conn.execute(
            "select has_function_privilege(current_user, %s, 'EXECUTE')", (function[0],)
        ).fetchone()
        assert backend_granted is not None and backend_granted[0] is True

    # Backend positive controls.
    backend = conn.execute(
        "select has_schema_privilege(current_user, 'vault', 'USAGE'), "
        "has_table_privilege(current_user, 'vault.secrets', 'SELECT'), "
        "has_table_privilege(current_user, 'vault.secrets', 'DELETE'), "
        "has_table_privilege(current_user, 'vault.decrypted_secrets', 'SELECT')"
    ).fetchone()
    assert backend is not None
    assert backend == (True, True, True, True)


def test_secret_create_resolve_update_delete_round_trip(conn: Connection[Any]) -> None:
    pool = _pool(conn)
    secret_id = runtime_control_secrets.create_runtime_control_secret(
        pool, secret=_CREDENTIAL, connection_id=uuid.uuid4(), workspace_id=uuid.uuid4()
    )

    resolved = runtime_control_secrets.read_runtime_control_secret(pool, secret_id=secret_id)
    assert resolved == _CREDENTIAL
    assert runtime_control_secrets.runtime_control_secret_exists(pool, secret_id=secret_id) is True

    # The Vault row carries safe metadata only: NULL name (the UNIQUE name is
    # deliberately avoided) and no plaintext outside the encrypted column.
    row = conn.execute(
        "select name, secret from vault.secrets where id = %s", (secret_id,)
    ).fetchone()
    assert row is not None
    assert row[0] is None
    assert _CREDENTIAL not in str(row[1])

    # In-place update preserves the exact UUID — and therefore any opaque
    # reference pointing at it.
    assert (
        runtime_control_secrets.update_runtime_control_secret(
            pool, secret_id=secret_id, secret=_ROTATED_CREDENTIAL
        )
        is True
    )
    updated = runtime_control_secrets.read_runtime_control_secret(pool, secret_id=secret_id)
    assert updated == _ROTATED_CREDENTIAL

    # The opaque reference round-trips to exactly the stored secret.
    reference = runtime_control_secrets.encode_connection_auth_reference(secret_id)
    assert runtime_control_secrets.parse_connection_auth_reference(reference) == secret_id

    # Delete by exact UUID; the second delete reports nothing removed.
    assert runtime_control_secrets.delete_runtime_control_secret(pool, secret_id=secret_id) is True
    assert runtime_control_secrets.read_runtime_control_secret(pool, secret_id=secret_id) is None
    assert runtime_control_secrets.delete_runtime_control_secret(pool, secret_id=secret_id) is False


def test_configure_rotate_and_resolution_round_trip(conn: Connection[Any]) -> None:
    profile_id = _insert_profile(conn)
    workspace_id = _insert_workspace(conn, profile_id)
    connection_id = _insert_connection(conn, workspace_id=workspace_id)
    pool = _pool(conn)

    configured = connection_credentials.configure_connection_credential(
        pool,
        profile_id=profile_id,
        workspace_id=workspace_id,
        connection_id=connection_id,
        credential=_CREDENTIAL,
    )
    assert configured.auth_reference is not None
    assert _CREDENTIAL not in configured.auth_reference
    reference = _auth_reference(conn, connection_id)
    assert reference == configured.auth_reference

    resolved_connection, secret = connection_credentials.resolve_connection_runtime_credential(
        pool, workspace_id=workspace_id, connection_id=connection_id
    )
    assert resolved_connection.id == connection_id
    assert secret.secret_value() == _CREDENTIAL

    rotated = connection_credentials.rotate_connection_credential(
        pool,
        profile_id=profile_id,
        workspace_id=workspace_id,
        connection_id=connection_id,
        credential=_ROTATED_CREDENTIAL,
    )
    # Rotation preserves the opaque reference exactly.
    assert rotated.auth_reference == configured.auth_reference
    assert _auth_reference(conn, connection_id) == reference
    _, rotated_secret = connection_credentials.resolve_connection_runtime_credential(
        pool, workspace_id=workspace_id, connection_id=connection_id
    )
    assert rotated_secret.secret_value() == _ROTATED_CREDENTIAL

    # Configuring over an existing credential conflicts; nothing is created.
    with pytest.raises(ConflictError):
        connection_credentials.configure_connection_credential(
            pool,
            profile_id=profile_id,
            workspace_id=workspace_id,
            connection_id=connection_id,
            credential=_CREDENTIAL,
        )
    assert _secret_rows(conn, connection_id=connection_id) == 1

    # A disabled Connection fails closed at resolution.
    conn.execute("update openorc.connections set enabled = false where id = %s", (connection_id,))
    with pytest.raises(ConflictError):
        connection_credentials.resolve_connection_runtime_credential(
            pool, workspace_id=workspace_id, connection_id=connection_id
        )


def test_invalid_credentials_persist_nothing(conn: Connection[Any]) -> None:
    profile_id = _insert_profile(conn)
    workspace_id = _insert_workspace(conn, profile_id)
    connection_id = _insert_connection(conn, workspace_id=workspace_id)
    pool = _pool(conn)

    for bad in (None, 123, b"s3cr3t", ""):
        with pytest.raises(InvalidCommandError):
            connection_credentials.configure_connection_credential(
                pool,
                profile_id=profile_id,
                workspace_id=workspace_id,
                connection_id=connection_id,
                credential=bad,  # type: ignore[arg-type]
            )
        # No Vault secret was persisted and no reference was installed.
        assert _secret_rows(conn, connection_id=connection_id) == 0
        assert _auth_reference(conn, connection_id) is None


def test_configure_rolls_back_secret_and_reference_when_the_second_write_fails(
    conn: Connection[Any], monkeypatch: pytest.MonkeyPatch
) -> None:
    profile_id = _insert_profile(conn)
    workspace_id = _insert_workspace(conn, profile_id)
    connection_id = _insert_connection(conn, workspace_id=workspace_id)
    pool = _pool(conn)

    def force_second_write_failure(*args: Any, **kwargs: Any) -> Any:
        raise RuntimeError("forced second-write failure")

    monkeypatch.setattr(
        connection_credentials, "set_connection_auth_reference", force_second_write_failure
    )

    with pytest.raises(RuntimeError, match="forced second-write failure"):
        connection_credentials.configure_connection_credential(
            pool,
            profile_id=profile_id,
            workspace_id=workspace_id,
            connection_id=connection_id,
            credential=_CREDENTIAL,
        )

    # Neither side committed: no orphaned Vault secret, no reference split.
    assert _secret_rows(conn, connection_id=connection_id) == 0
    assert _auth_reference(conn, connection_id) is None


def test_failed_rotation_leaves_the_prior_credential_resolvable(
    conn: Connection[Any], monkeypatch: pytest.MonkeyPatch
) -> None:
    profile_id = _insert_profile(conn)
    workspace_id = _insert_workspace(conn, profile_id)
    connection_id = _insert_connection(conn, workspace_id=workspace_id)
    pool = _pool(conn)

    configured = connection_credentials.configure_connection_credential(
        pool,
        profile_id=profile_id,
        workspace_id=workspace_id,
        connection_id=connection_id,
        credential=_CREDENTIAL,
    )
    reference = configured.auth_reference

    def force_secret_update_failure(*args: Any, **kwargs: Any) -> Any:
        raise RuntimeError("forced vault update failure")

    monkeypatch.setattr(
        connection_credentials.runtime_control_secrets,
        "update_runtime_control_secret",
        force_secret_update_failure,
    )

    with pytest.raises(RuntimeError, match="forced vault update failure"):
        connection_credentials.rotate_connection_credential(
            pool,
            profile_id=profile_id,
            workspace_id=workspace_id,
            connection_id=connection_id,
            credential=_ROTATED_CREDENTIAL,
        )

    # The prior credential is untouched and still resolves exactly.
    assert _auth_reference(conn, connection_id) == reference
    _, secret = connection_credentials.resolve_connection_runtime_credential(
        pool, workspace_id=workspace_id, connection_id=connection_id
    )
    assert secret.secret_value() == _CREDENTIAL
