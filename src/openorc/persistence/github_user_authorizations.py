"""Repositories for Profile-scoped GitHub user authorization persistence.

Explicit SQL repositories over the ``openorc`` schema for the one current
GitHub App user authorization per Profile (issue #142). Rows map to
transport-independent domain objects from
:mod:`openorc.domain.github_user_authorization`; instants returned from
Postgres are normalized to timezone-aware UTC at this boundary.

Durable lifecycle expressed here (the issue #142 stale-write contract):

- The row is keyed by ``profile_id`` and created once per Profile. It is
  updated in place for its whole Profile lifetime; the only removal is the
  sanctioned ``auth.users -> profiles`` account-root cascade.
- ``reauthorize_github_user_authorization`` advances ``refresh_generation``
  monotonically in place (never delete-and-reinsert, never a generation
  reset), so a stale process's cached credential facts or an earlier
  generation can never become apparently current again.
- ``try_install_rotated_refresh`` is the cross-process compare-and-swap
  guard for refresh rotation: the rotated expiry/currentness metadata is
  installed and the generation incremented only when the durable generation
  still matches AND the row is still active. A zero-row outcome is the
  stale/not-current condition — the caller reloads and reclassifies from
  durable state, never overwrites.
- ``revoke_github_user_authorization_row`` marks the row revoked, removes
  the usable refresh reference and its expiry, and advances the generation
  in the same statement, so no (Profile, generation) cache key outlives a
  revocation. Idempotency for account-deletion retries is the caller's
  composition (a revoked row with no reference has nothing to revoke).

Raw credential material has no column here and no path through this module:
only the opaque ``refresh_secret_reference`` pointer is persisted. Violated
database invariants surface as driver exceptions; translating them into
typed application errors is a service-layer concern.
"""

from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime
from typing import Any
from uuid import UUID

from openorc.domain.github_user_authorization import (
    GitHubUserAuthorization,
    GitHubUserAuthorizationStatus,
)
from openorc.persistence.pool import DatabasePool
from openorc.persistence.time import normalize_utc
from openorc.persistence.transactions import transaction

__all__ = [
    "get_github_user_authorization",
    "get_github_user_authorization_for_update",
    "insert_active_github_user_authorization",
    "reauthorize_github_user_authorization",
    "revoke_github_user_authorization_row",
    "try_install_rotated_refresh",
]

# The full column list, in the stable mapping order consumed by
# _authorization_from_row.
_AUTHORIZATION_COLUMNS = (
    "profile_id, github_user_id, github_login, status, refresh_secret_reference, "
    "refresh_expires_at, refresh_generation, authorized_at, revoked_at, "
    "created_at, updated_at"
)


def _authorization_from_row(row: Sequence[Any]) -> GitHubUserAuthorization:
    return GitHubUserAuthorization(
        profile_id=row[0],
        github_user_id=row[1],
        github_login=row[2],
        status=GitHubUserAuthorizationStatus(row[3]),
        refresh_secret_reference=row[4],
        refresh_expires_at=None if row[5] is None else normalize_utc(row[5]),
        refresh_generation=row[6],
        authorized_at=normalize_utc(row[7]),
        revoked_at=None if row[8] is None else normalize_utc(row[8]),
        created_at=normalize_utc(row[9]),
        updated_at=normalize_utc(row[10]),
    )


def get_github_user_authorization(
    pool: DatabasePool, *, profile_id: UUID
) -> GitHubUserAuthorization | None:
    """Return the Profile's current authorization, or None when absent."""
    with transaction(pool) as conn:
        row = conn.execute(
            f"select {_AUTHORIZATION_COLUMNS} from openorc.github_user_authorizations "
            "where profile_id = %s",
            (profile_id,),
        ).fetchone()
    return None if row is None else _authorization_from_row(row)


def get_github_user_authorization_for_update(
    pool: DatabasePool, *, profile_id: UUID
) -> GitHubUserAuthorization | None:
    """Read the Profile's authorization under its row lock.

    The deliberate lock acquisition for every lifecycle mutation
    (establishment/replace, refresh install, revocation, account-deletion
    cleanup): the caller composes the account-operational barrier BEFORE
    taking this lock, then serializes the row's mutation under it.
    """
    with transaction(pool) as conn:
        row = conn.execute(
            f"select {_AUTHORIZATION_COLUMNS} from openorc.github_user_authorizations "
            "where profile_id = %s for update",
            (profile_id,),
        ).fetchone()
    return None if row is None else _authorization_from_row(row)


def insert_active_github_user_authorization(
    pool: DatabasePool,
    *,
    profile_id: UUID,
    github_user_id: int,
    github_login: str | None,
    refresh_secret_reference: str,
    refresh_expires_at: datetime,
) -> GitHubUserAuthorization:
    """Insert the Profile's first active authorization (generation 1).

    Called only when the locked read proved the row absent. The opaque
    reference was already created in the same composed transaction, so a
    failure rolls the Vault secret back with the row.
    """
    with transaction(pool) as conn:
        row = conn.execute(
            "insert into openorc.github_user_authorizations "
            "(profile_id, github_user_id, github_login, status, "
            "refresh_secret_reference, refresh_expires_at, refresh_generation, "
            "authorized_at, revoked_at, created_at, updated_at) "
            "values (%s, %s, %s, 'active', %s, %s, 1, now(), null, now(), now()) "
            f"returning {_AUTHORIZATION_COLUMNS}",
            (
                profile_id,
                github_user_id,
                github_login,
                refresh_secret_reference,
                refresh_expires_at,
            ),
        ).fetchone()
    assert row is not None
    return _authorization_from_row(row)


def reauthorize_github_user_authorization(
    pool: DatabasePool,
    *,
    profile_id: UUID,
    github_user_id: int,
    github_login: str | None,
    refresh_secret_reference: str,
    refresh_expires_at: datetime,
) -> GitHubUserAuthorization | None:
    """Replace an existing authorization's facts in place, advancing its generation.

    The row is never delete-and-reinserted: the same durable row stays, its
    generation advances monotonically (so no earlier in-memory cache key can
    resurface), and the new refresh credential facts install atomically with
    the currentness reset to active. The caller holds the row lock (the
    for-update read) and has already deleted the superseded Vault secret.
    Returns None only when the row vanished (the caller inserts instead).
    """
    with transaction(pool) as conn:
        row = conn.execute(
            "update openorc.github_user_authorizations set "
            "github_user_id = %s, github_login = %s, status = 'active', "
            "refresh_secret_reference = %s, refresh_expires_at = %s, "
            "revoked_at = null, authorized_at = now(), "
            "refresh_generation = refresh_generation + 1, updated_at = now() "
            "where profile_id = %s "
            f"returning {_AUTHORIZATION_COLUMNS}",
            (
                github_user_id,
                github_login,
                refresh_secret_reference,
                refresh_expires_at,
                profile_id,
            ),
        ).fetchone()
    return None if row is None else _authorization_from_row(row)


def try_install_rotated_refresh(
    pool: DatabasePool,
    *,
    profile_id: UUID,
    expected_generation: int,
    refresh_expires_at: datetime,
) -> GitHubUserAuthorization | None:
    """Compare-and-swap one successful refresh rotation into durable state.

    Installs the rotated refresh credential's expiry and increments the
    generation atomically ONLY when the durable generation still equals
    ``expected_generation`` and the row is still active. The rotated Vault
    secret value itself is updated in place by the caller inside the same
    composition (its UUID — and therefore the opaque reference — stays
    stable). A zero-row return means the durable state moved on (another
    process rotated, or the authorization became revoked): the caller
    discards its returned token pair, reloads, and reclassifies — it never
    overwrites.
    """
    with transaction(pool) as conn:
        row = conn.execute(
            "update openorc.github_user_authorizations set "
            "refresh_expires_at = %s, "
            "refresh_generation = refresh_generation + 1, updated_at = now() "
            "where profile_id = %s and refresh_generation = %s and status = 'active' "
            f"returning {_AUTHORIZATION_COLUMNS}",
            (refresh_expires_at, profile_id, expected_generation),
        ).fetchone()
    return None if row is None else _authorization_from_row(row)


def revoke_github_user_authorization_row(
    pool: DatabasePool, *, profile_id: UUID
) -> GitHubUserAuthorization | None:
    """Mark the Profile's authorization revoked, advancing its generation.

    One statement removes the usable refresh reference and expiry and
    advances the generation atomically, so no (Profile, generation) cache
    key survives the revocation. The caller holds the row lock, has already
    deleted the referenced Vault secret (or the row was found revoked), and
    treats a zero-row return as the row being absent. The row persists —
    revocation is durable currentness state, deliberately distinct from
    never-authorized — until the sanctioned account-root cascade removes it.
    """
    with transaction(pool) as conn:
        row = conn.execute(
            "update openorc.github_user_authorizations set "
            "status = 'revoked', refresh_secret_reference = null, "
            "refresh_expires_at = null, revoked_at = now(), "
            "refresh_generation = refresh_generation + 1, updated_at = now() "
            "where profile_id = %s "
            f"returning {_AUTHORIZATION_COLUMNS}",
            (profile_id,),
        ).fetchone()
    return None if row is None else _authorization_from_row(row)
