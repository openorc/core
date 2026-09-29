"""Exact-ID Supabase Vault storage primitive.

One focused persistence boundary for the exact-ID mechanics of the supported
Supabase Vault extension: create, decrypt-on-read, decrypt-free existence
lookup, in-place value update, and targeted delete — each addressed by one
exact secret UUID.

This primitive deliberately knows nothing about secret purposes. It owns no
reference format, resolves no workflow meaning, and exposes no generic
browsing or search surface. Purpose-specific persistence modules call this
primitive for the underlying mechanics and own their own opaque reference
codecs and service semantics: the Connection runtime-control credential
boundary (``openorc.persistence.runtime_control_secrets``, issue #55) today,
and a later Profile-scoped GitHub user-refresh credential module over the
same primitive (issue #141). Purpose references are never cross-parseable.

Supabase Vault is the concrete store for multiple OpenOrc-owned secret
purposes; ordinary ``openorc.*`` tables never carry raw secret material, and
browser-facing roles have no Vault access (``vault`` remains outside exposed
Data API schemas).

Vault notes (verified against the supported ``supabase_vault`` surface):
``vault.update_secret`` preserves the secret's UUID when updating in place
but silently no-ops on an unknown UUID, so :func:`update_vault_secret` checks
existence first and reports a dangling pointer as ``False``;
``vault.secrets.name`` is UNIQUE when non-NULL, so created secrets carry a
NULL name and the caller supplies a description of safe identifiers only
(never the secret value); and ``supabase_vault`` 0.3.x has no
``remove_secret`` function, so deletion is a direct parameterized delete by
exact id, which is version-stable for the owning role.

Existence checks do not decrypt. A decrypted value crosses this boundary only
on explicit read, only for immediate wrapping by the trusted secret-bearing
service/adapter boundary — never into ordinary structures, logs, events, or
persistence.
"""

from __future__ import annotations

from uuid import UUID

from openorc.persistence.pool import DatabasePool
from openorc.persistence.transactions import transaction

__all__ = [
    "create_vault_secret",
    "delete_vault_secret",
    "read_vault_secret",
    "update_vault_secret",
    "vault_secret_exists",
]


def create_vault_secret(pool: DatabasePool, *, secret: str, description: str) -> UUID:
    """Store one encrypted Vault secret; return its UUID.

    The Vault row carries a NULL ``name`` (``vault.secrets.name`` is UNIQUE
    when non-NULL) and the caller-composed ``description``, which must be
    safe metadata — stable identifiers only, never the secret value. The
    caller is a purpose-specific boundary acting inside its own transaction
    composition, so the returned UUID can be installed as the purpose's
    opaque reference atomically (a failure there rolls the secret back with
    everything else).
    """
    with transaction(pool) as conn:
        row = conn.execute(
            "select vault.create_secret(%s, null, %s)",
            (secret, description),
        ).fetchone()
    assert row is not None
    return row[0]


def read_vault_secret(pool: DatabasePool, *, secret_id: UUID) -> str | None:
    """Return the decrypted secret value, or None when the UUID is unknown.

    Decryption happens only here, only on explicit resolution. The returned
    string is the secret material itself: callers must wrap it in the
    trusted secret-bearing boundary immediately and must never copy it into
    ordinary structures, DTOs, configuration snapshots, or logs.
    """
    with transaction(pool) as conn:
        row = conn.execute(
            "select decrypted_secret from vault.decrypted_secrets where id = %s",
            (secret_id,),
        ).fetchone()
    return None if row is None else row[0]


def vault_secret_exists(pool: DatabasePool, *, secret_id: UUID) -> bool:
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


def update_vault_secret(pool: DatabasePool, *, secret_id: UUID, secret: str) -> bool:
    """Replace one secret's value in place, preserving its UUID; fail closed.

    ``vault.update_secret`` updates the encrypted value in place — the
    secret's UUID (and therefore any durable purpose reference pointing at
    it) remains stable across rotations — but it silently no-ops on an
    unknown UUID. Existence is therefore checked first (decrypt-free), and
    ``False`` tells the caller the pointer is dangling. ``True`` means the
    value was replaced.

    A concurrent administrative deletion between the existence check and the
    update is a dangling-reference condition for the next resolution, not a
    silent false success; the calling purpose boundary fails closed on it.
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


def delete_vault_secret(pool: DatabasePool, *, secret_id: UUID) -> bool:
    """Delete one secret by exact UUID; return whether a row was removed.

    A direct targeted delete (not a named helper) is version-stable across
    supported Vault releases: ``supabase_vault`` 0.3.x has no
    ``remove_secret`` function.
    """
    with transaction(pool) as conn:
        row = conn.execute(
            "delete from vault.secrets where id = %s returning 1",
            (secret_id,),
        ).fetchone()
    return row is not None
