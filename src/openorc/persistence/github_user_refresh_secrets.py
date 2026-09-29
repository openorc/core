"""GitHub-user-purpose secret boundary for the Profile's refresh credential.

OpenOrc-owned GitHub App user-to-server refresh credentials are stored
encrypted at rest in Supabase Vault (issue #142). This module is the one
focused GitHub-user-purpose boundary for that storage; ordinary ``openorc.*``
tables — in particular ``openorc.github_user_authorizations`` — carry only
the opaque ``refresh_secret_reference`` boundary, never credential material.

Two concerns live here, deliberately together because they co-evolve with
the reference format:

- The opaque v1 ``refresh_secret_reference`` format and its fail-closed
  parse. The reference is a stable, version-tagged pointer to one Vault
  secret UUID — never the secret value itself. Anything that is not exactly
  the v1 GitHub-user shape raises
  :class:`GitHubUserRefreshSecretReferenceError`; services translate that
  into the typed application vocabulary. The format is deliberately not
  cross-parseable with the Connection-purpose ``auth_reference`` format
  (``persistence/runtime_control_secrets.py``, issue #55): purpose
  references never cross-parse.
- The GitHub-user-purpose operations over the shared exact-ID Vault storage
  primitive (:mod:`openorc.persistence.vault_exact_id`): create with the
  safe Profile description, decrypt-on-read resolution, decrypt-free
  existence lookup, in-place value update preserving the secret UUID across
  refresh rotations, and targeted delete.

The shared primitive owns the exact-ID Vault mechanics and is the only
module that queries ``vault.*``. Purpose-specific modules own their
reference formats and service semantics. Decryption happens only on
explicit resolution; existence checks never decrypt. Account-deletion
cleanup of these secrets belongs to the account lifecycle (issue #97
revocation transaction), not to the authorization services.
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
    "GitHubUserRefreshSecretReferenceError",
    "create_github_user_refresh_secret",
    "delete_github_user_refresh_secret",
    "encode_github_user_refresh_reference",
    "github_user_refresh_secret_exists",
    "parse_github_user_refresh_reference",
    "read_github_user_refresh_secret",
    "update_github_user_refresh_secret",
]

# The opaque v1 reference shape: five colon-separated segments with exact
# scheme, purpose, version, and backend tags, carrying one canonically
# rendered Vault secret UUID. The reference is the durable representation of
# the secret's storage locator — never the secret value — and parsing it
# belongs to this secure credential boundary alone. The purpose tag differs
# from every other purpose reference OpenOrc defines.
_REFERENCE_SEGMENTS = ("openorc", "github-user-refresh", "v1", "vault")


class GitHubUserRefreshSecretReferenceError(Exception):
    """A GitHub user refresh_secret_reference is malformed or unrecognized.

    Raised fail-closed by :func:`parse_github_user_refresh_reference` for
    any value that is not a string, not exactly the v1 GitHub-user shape, or
    whose payload is not a canonically rendered UUID. The exception carries
    no credential material — only the reference boundary's own safe
    description of the failure.
    """


def encode_github_user_refresh_reference(secret_id: UUID) -> str:
    """Render one Vault secret UUID as the opaque v1 GitHub-user reference."""
    return ":".join((*_REFERENCE_SEGMENTS, str(secret_id)))


def parse_github_user_refresh_reference(value: str) -> UUID:
    """Parse the exact Vault secret UUID out of one opaque reference.

    Anything that is not exactly the v1 GitHub-user-refresh shape — including
    a Connection-purpose ``auth_reference`` — raises
    :class:`GitHubUserRefreshSecretReferenceError`; purpose references are
    never cross-parseable.
    """
    if not isinstance(value, str):
        raise GitHubUserRefreshSecretReferenceError(
            "a GitHub user refresh_secret_reference must be a string"
        )
    segments = value.split(":")
    if len(segments) != len(_REFERENCE_SEGMENTS) + 1 or tuple(segments[:-1]) != _REFERENCE_SEGMENTS:
        raise GitHubUserRefreshSecretReferenceError(
            "the value is not a recognized v1 GitHub user refresh reference"
        )
    payload = segments[-1]
    try:
        secret_id = UUID(payload)
    except ValueError as exc:
        raise GitHubUserRefreshSecretReferenceError(
            "the GitHub user refresh reference payload is not a valid secret identifier"
        ) from exc
    if str(secret_id) != payload:
        raise GitHubUserRefreshSecretReferenceError(
            "the GitHub user refresh reference payload is not a canonical secret identifier"
        )
    return secret_id


def create_github_user_refresh_secret(pool: DatabasePool, *, secret: str, profile_id: UUID) -> UUID:
    """Store one encrypted Vault secret; return its UUID.

    The caller is the authorization service inside one composed transaction,
    so the returned UUID can be installed as the Profile authorization's
    opaque reference atomically (a failure there rolls the secret back with
    everything else). The Vault row carries a NULL ``name``
    (``vault.secrets.name`` is UNIQUE when non-NULL) and a description of
    safe identifiers only — the Profile UUID, never the secret value.
    """
    return create_vault_secret(
        pool,
        secret=secret,
        description=f"openorc github user refresh credential (profile {profile_id})",
    )


def read_github_user_refresh_secret(pool: DatabasePool, *, secret_id: UUID) -> str | None:
    """Return the decrypted refresh credential, or None when the UUID is unknown.

    Resolution is explicit and the decryption is owned by the shared
    primitive. The returned string is the credential material itself:
    callers (the authorization service) must wrap it in the secret-bearing
    boundary immediately and must never copy it into ordinary structures,
    DTOs, logs, or persistence.
    """
    return read_vault_secret(pool, secret_id=secret_id)


def github_user_refresh_secret_exists(pool: DatabasePool, *, secret_id: UUID) -> bool:
    """Report whether one exact Vault secret UUID exists — without decrypting."""
    return vault_secret_exists(pool, secret_id=secret_id)


def update_github_user_refresh_secret(pool: DatabasePool, *, secret_id: UUID, secret: str) -> bool:
    """Replace one refresh credential in place, preserving its UUID; fail closed.

    The in-place update keeps the secret's UUID — and therefore the Profile
    authorization's opaque reference — stable across refresh rotations. The
    shared primitive checks existence first (decrypt-free) and reports a
    dangling pointer as ``False``; ``True`` means the value was replaced.
    """
    return update_vault_secret(pool, secret_id=secret_id, secret=secret)


def delete_github_user_refresh_secret(pool: DatabasePool, *, secret_id: UUID) -> bool:
    """Delete one refresh credential by exact UUID; return whether it existed.

    The shared primitive performs the version-stable targeted delete.
    Intended for explicit revocation and the account-deletion revocation
    transaction (issue #97/#142).
    """
    return delete_vault_secret(pool, secret_id=secret_id)
