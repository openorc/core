"""Connection-purpose secret boundary for OpenOrc-owned runtime credentials.

OpenOrc-owned Agent Runtime control-endpoint credentials are stored encrypted
at rest in Supabase Vault (Phase 2A, issue #55). This module is the one
focused Connection-purpose boundary for that storage; ordinary ``openorc.*``
tables carry only the opaque v1 ``Connection.auth_reference`` boundary —
never credential material — and runtime-owned provider/MCP/tool credentials
remain entirely outside this module.

Two concerns live here, deliberately together because they co-evolve with the
reference format:

- The opaque v1 ``auth_reference`` format and its fail-closed parse. The
  reference is a stable, version-tagged pointer to one Vault secret UUID —
  never the secret value itself. Anything that is not exactly the v1 shape
  raises :class:`RuntimeControlSecretReferenceError`; services translate that
  into the typed application vocabulary.
- The Connection-purpose operations over the shared exact-ID Vault storage
  primitive (:mod:`openorc.persistence.vault_exact_id`): create with the
  safe connection/workspace description, decrypt-on-read resolution,
  decrypt-free existence lookup, in-place value update preserving the secret
  UUID, and targeted delete.

The shared primitive owns the exact-ID Vault mechanics and is the only module
that queries ``vault.*``. Purpose-specific modules own their reference
formats and service semantics: this module owns the v1 Connection
``auth_reference`` today, and a later Profile-scoped GitHub user-refresh
credential module will define its own distinct opaque reference format over
the same primitive (issue #141). Purpose references are never
cross-parseable, and there is deliberately no generic Vault browsing/search
API. Decryption happens only on explicit resolution; existence checks never
decrypt.

Administrative disconnect/delete cleanup of Vault secrets belongs to the
deletion lifecycle (issue #97), not to the credential services.
"""

from __future__ import annotations

from uuid import UUID

from openorc.persistence.pool import DatabasePool
from openorc.persistence.vault_exact_id import (
    create_vault_secret,
    delete_vault_secret,
    read_vault_secret,
    update_vault_secret,
    vault_secret_exists,
)

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
    return create_vault_secret(
        pool,
        secret=secret,
        description=(
            f"openorc connection credential (workspace {workspace_id}, connection {connection_id})"
        ),
    )


def read_runtime_control_secret(pool: DatabasePool, *, secret_id: UUID) -> str | None:
    """Return the decrypted secret value, or None when the UUID is unknown.

    Resolution is explicit and the decryption is owned by the shared
    primitive. The returned string is the credential material itself:
    callers (the credential services) must wrap it in the secret-bearing
    boundary immediately and must never copy it into ordinary structures,
    DTOs, configuration snapshots, or logs.
    """
    return read_vault_secret(pool, secret_id=secret_id)


def runtime_control_secret_exists(pool: DatabasePool, *, secret_id: UUID) -> bool:
    """Report whether one exact Vault secret UUID exists — without decrypting.

    The shared primitive reads the encrypted storage table directly by exact
    id; the decrypt-on-read view is deliberately not involved.
    """
    return vault_secret_exists(pool, secret_id=secret_id)


def update_runtime_control_secret(pool: DatabasePool, *, secret_id: UUID, secret: str) -> bool:
    """Replace one secret's value in place, preserving its UUID; fail closed.

    The in-place update keeps the secret's UUID — and therefore the
    Connection's opaque ``auth_reference`` — stable across rotations. The
    shared primitive checks existence first (decrypt-free) and reports a
    dangling pointer as ``False``; ``True`` means the value was replaced.

    A concurrent administrative deletion between the existence check and the
    update is a dangling-reference condition for the next resolution, not a
    silent false success; the credential service fails closed on it.
    """
    return update_vault_secret(pool, secret_id=secret_id, secret=secret)


def delete_runtime_control_secret(pool: DatabasePool, *, secret_id: UUID) -> bool:
    """Delete one secret by exact UUID; return whether a row was removed.

    The shared primitive performs the version-stable targeted delete.
    Intended for the administrative lifecycle boundary (issue #97).
    """
    return delete_vault_secret(pool, secret_id=secret_id)
