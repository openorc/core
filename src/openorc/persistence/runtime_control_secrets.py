"""Runtime-control secret storage for OpenOrc-owned Connection credentials.

OpenOrc-owned Agent Runtime control-endpoint credentials are stored encrypted
at rest in Supabase Vault (Phase 2A, issue #55). This module is the one
focused persistence boundary for that storage; no other persistence, domain,
or service module queries ``vault.*``. Ordinary ``openorc.*`` tables carry
only the opaque v1 ``Connection.auth_reference`` boundary — never credential
material — and runtime-owned provider/MCP/tool credentials remain entirely
outside this module.

Two concerns live here, deliberately together because they co-evolve with the
storage format:

- The opaque v1 ``auth_reference`` format and its fail-closed parse. The
  reference is a stable, version-tagged pointer to one Vault secret UUID —
  never the secret value itself. Anything that is not exactly the v1 shape
  raises :class:`RuntimeControlSecretReferenceError`; services translate that
  into the typed application vocabulary.
- The Vault SQL surface, by exact UUID only: create, decrypt-on-read,
  decrypt-free existence lookup, in-place value update (preserving the secret
  UUID), and targeted delete. There is deliberately no generic Vault
  browsing/search API, and decryption happens only in
  :func:`read_runtime_control_secret` — existence checks never decrypt.

Vault notes (verified against the supported ``supabase_vault`` surface):
``vault.update_secret`` preserves the secret's UUID when updating in place but
silently no-ops on an unknown UUID, so :func:`update_runtime_control_secret`
checks existence first and reports a dangling pointer as ``False``;
``vault.secrets.name`` is UNIQUE when non-NULL, so created secrets carry a
NULL name and only safe identifiers (connection/workspace UUIDs) in the
description — avoiding a unique-name collision when a Connection is
re-configured before administrative cleanup lands (issue #97); and
``supabase_vault`` 0.3.x has no ``remove_secret`` function, so deletion is a
direct parameterized delete by exact id, which is version-stable for the
owning role.

Administrative disconnect/delete cleanup of Vault secrets belongs to the
deletion lifecycle (issue #97), not to the credential services.
"""

from __future__ import annotations

from uuid import UUID

from openorc.persistence.pool import DatabasePool
from openorc.persistence.transactions import transaction

__all__ = [
    "RuntimeControlSecretReferenceError",
    "create_runtime_control_secret",
    "delete_runtime_control_secret",
    "encode_connection_auth_reference",
    "parse_connection_auth_reference",
    "read_runtime_control_secret",
    "runtime_control_secret_exists",
    "update_runtime_control_secret",
]

# The opaque v1 reference shape: five colon-separated segments with exact
# scheme, purpose, version, and backend tags, carrying one canonically
# rendered Vault secret UUID. The reference is durable representation of the
# secret's storage locator — it is never the secret value, and parsing it
# belongs to this secure credential boundary alone.
_REFERENCE_SEGMENTS = ("openorc", "connection-auth", "v1", "vault")


class RuntimeControlSecretReferenceError(Exception):
    """A Connection auth_reference is malformed or of an unrecognized format.

    Raised fail-closed by :func:`parse_connection_auth_reference` for any
    value that is not a string, not exactly the v1 shape, or whose payload is
    not a canonically rendered UUID. The exception carries no credential
    material — only the reference boundary's own safe description of the
    failure.
    """


def encode_connection_auth_reference(secret_id: UUID) -> str:
    """Return the opaque v1 auth_reference pointing at one Vault secret UUID.

    The encoding is total over valid UUIDs and always renders the canonical
    lowercase hyphenated form, so every reference OpenOrc writes is exactly
    what :func:`parse_connection_auth_reference` accepts.
    """
    return ":".join((*_REFERENCE_SEGMENTS, str(secret_id)))


def parse_connection_auth_reference(value: str) -> UUID:
    """Parse one opaque v1 auth_reference to its Vault secret UUID.

    Fail closed: a non-string value, an unrecognized format, an unknown
    version or backend tag, or a payload that is not the canonical UUID
    rendering raises :class:`RuntimeControlSecretReferenceError`. The strict
    canonical check means only references this boundary's own encoder (or an
    identical future writer) produce can resolve; loose UUID forms are
    treated as malformed rather than opportunistically accepted.
    """
    if not isinstance(value, str):
        raise RuntimeControlSecretReferenceError("connection auth_reference must be a string")
    segments = value.split(":")
    if len(segments) != len(_REFERENCE_SEGMENTS) + 1 or tuple(segments[:-1]) != _REFERENCE_SEGMENTS:
        raise RuntimeControlSecretReferenceError(
            "connection auth_reference format is not recognized"
        )
    payload = segments[-1]
    try:
        secret_id = UUID(payload)
    except ValueError as exc:
        raise RuntimeControlSecretReferenceError(
            "connection auth_reference payload is not a valid secret identifier"
        ) from exc
    if str(secret_id) != payload:
        raise RuntimeControlSecretReferenceError(
            "connection auth_reference payload is not a canonical secret identifier"
        )
    return secret_id


def create_runtime_control_secret(
    pool: DatabasePool, *, secret: str, connection_id: UUID, workspace_id: UUID
) -> UUID:
    """Store one encrypted Vault secret; return its UUID.

    The caller is the credential service inside one composed transaction, so
    the returned UUID can be installed as the Connection's opaque
    ``auth_reference`` atomically (a failure there rolls the secret back with
    everything else).

    The Vault row carries a NULL ``name`` (``vault.secrets.name`` is UNIQUE
    when non-NULL; a NULL name never collides when a Connection is
    re-configured before administrative cleanup) and a ``description`` of
    safe identifiers only — the connection and workspace UUIDs, never the
    credential value.
    """
    with transaction(pool) as conn:
        row = conn.execute(
            "select vault.create_secret(%s, null, %s)",
            (
                secret,
                (
                    "openorc connection credential "
                    f"(workspace {workspace_id}, connection {connection_id})"
                ),
            ),
        ).fetchone()
    assert row is not None
    return row[0]


def read_runtime_control_secret(pool: DatabasePool, *, secret_id: UUID) -> str | None:
    """Return the decrypted secret value, or None when the UUID is unknown.

    Decryption happens only here, only on explicit resolution. The returned
    string is the credential material itself: callers (the credential
    services) must wrap it in the secret-bearing boundary immediately and
    must never copy it into ordinary structures, DTOs, configuration
    snapshots, or logs.
    """
    with transaction(pool) as conn:
        row = conn.execute(
            "select decrypted_secret from vault.decrypted_secrets where id = %s",
            (secret_id,),
        ).fetchone()
    return None if row is None else row[0]


def runtime_control_secret_exists(pool: DatabasePool, *, secret_id: UUID) -> bool:
    """Report whether one exact Vault secret UUID exists — without decrypting.

    The lookup reads the encrypted storage table directly by exact id; the
    decrypt-on-read view is deliberately not involved.
    """
    with transaction(pool) as conn:
        row = conn.execute(
            "select 1 from vault.secrets where id = %s",
            (secret_id,),
        ).fetchone()
    return row is not None


def update_runtime_control_secret(pool: DatabasePool, *, secret_id: UUID, secret: str) -> bool:
    """Replace one secret's value in place, preserving its UUID; fail closed.

    ``vault.update_secret`` updates the encrypted value in place — the
    secret's UUID (and therefore the Connection's opaque ``auth_reference``)
    remains stable across rotations — but it silently no-ops on an unknown
    UUID. Existence is therefore checked first (decrypt-free), and ``False``
    tells the caller the reference is dangling. ``True`` means the value was
    replaced.

    A concurrent administrative deletion between the existence check and the
    update is a dangling-reference condition for the next resolution, not a
    silent false success; the credential service fails closed on it.
    """
    with transaction(pool) as conn:
        exists = conn.execute(
            "select 1 from vault.secrets where id = %s",
            (secret_id,),
        ).fetchone()
        if exists is None:
            return False
        conn.execute(
            "select vault.update_secret(%s, %s)",
            (secret_id, secret),
        )
    return True


def delete_runtime_control_secret(pool: DatabasePool, *, secret_id: UUID) -> bool:
    """Delete one secret by exact UUID; return whether a row was removed.

    A direct targeted delete (not a named helper) is version-stable across
    supported Vault releases: ``supabase_vault`` 0.3.x has no
    ``remove_secret`` function. Intended for the administrative lifecycle
    boundary (issue #97).
    """
    with transaction(pool) as conn:
        row = conn.execute(
            "delete from vault.secrets where id = %s returning 1",
            (secret_id,),
        ).fetchone()
    return row is not None
