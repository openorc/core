"""Repositories for pending GitHub user authorization flow persistence.

Explicit SQL repositories over the ``openorc`` schema for the one PENDING
OAuth start-to-callback correlation per Profile (issue #373). Rows map to
transport-independent domain objects from
:mod:`openorc.domain.github_user_authorization_flows`; instants returned
from Postgres are normalized to timezone-aware UTC at this boundary.

Durable properties expressed here:

- The table holds PENDING rows only, keyed by the Profile itself (one
  pending flow per Profile, enforced by the primary key). The row is
  deleted — never terminalized — at exactly three moments: the callback's
  atomic consume (the delete IS the single-use enforcement, conditional on
  the exact state and an unexpired row), the start operation's supersession
  of the Profile's prior pending flow, and Owner-revocation /
  account-deletion invalidation — and every one of those deletes removes
  the verifier's Vault secret in the same transaction, so no consumed,
  declined, superseded, or invalidated flow ever outlives its encrypted
  verifier material. An expired pending row is simply never consumable;
  the accumulation stays bounded by one row per Profile and the sanctioned
  account-root cascade removes the row with the Profile graph.
- The PKCE verifier is secret-bearing exchange material: it lives
  exclusively in Supabase Vault behind the opaque
  ``pkce_verifier_reference`` (this module's own purpose-specific reference
  codec — never cross-parseable with the refresh-credential or Connection
  formats) and is decrypted only by the atomic consume. The decrypted
  verifier crosses this boundary exclusively as
  :func:`consume_pending_flow`'s return value, for immediate use by the
  trusted completion service in the same call chain — never into ordinary
  structures, DTOs, logs, or persistence.
- Malformed or dangling verifier references fail closed and roll the whole
  operation back (the caller translates the module error into its typed
  conflict vocabulary).
- ``observed_revocation_count`` is the fence evidence captured at start;
  the completion service compares it against the durable authorization
  count under the authorization row lock. No fence semantics live here.

Raw verifier material has no column here and no path through the returned
flow facts: only the opaque reference is persisted and projected.
"""

from __future__ import annotations

from uuid import UUID

from openorc.domain.github_user_authorization_flows import (
    GitHubUserAuthorizationFlow,
    GitHubUserAuthorizationFlowPurpose,
)
from openorc.persistence.pool import DatabasePool
from openorc.persistence.time import normalize_utc
from openorc.persistence.transactions import transaction
from openorc.persistence.vault_exact_id import (
    create_vault_secret,
    delete_vault_secret,
    read_vault_secret,
)

__all__ = [
    "GitHubUserFlowVerifierReferenceError",
    "consume_pending_flow",
    "create_pending_flow",
    "delete_pending_flow_with_verifier_secret",
    "discard_pending_flow",
    "get_pending_flow",
    "parse_github_user_flow_verifier_reference",
]

# --- opaque reference codec --------------------------------------------------

# The opaque v1 reference shape: five colon-separated segments with exact
# scheme, purpose, version, and backend tags, carrying one canonically
# rendered Vault secret UUID. The purpose tag differs from every other
# purpose reference OpenOrc defines (the GitHub-user refresh credential, the
# Connection runtime-control credential): purpose references are never
# cross-parseable.
_REFERENCE_SEGMENTS = ("openorc", "github-user-flow-verifier", "v1", "vault")


class GitHubUserFlowVerifierReferenceError(Exception):
    """A pending flow's pkce_verifier_reference is malformed or unrecognized.

    Raised fail-closed by :func:`parse_github_user_flow_verifier_reference`
    and by the consuming/deleting operations for any reference that is not a
    string, not exactly the v1 flow-verifier shape, not a canonically
    rendered UUID, or whose secret is absent (dangling). The exception
    carries no verifier material — only the reference boundary's own safe
    description of the failure.
    """


def encode_github_user_flow_verifier_reference(secret_id: UUID) -> str:
    """Render one Vault secret UUID as the opaque v1 flow-verifier reference."""
    return ":".join((*_REFERENCE_SEGMENTS, str(secret_id)))


def parse_github_user_flow_verifier_reference(value: str) -> UUID:
    """Parse the exact Vault secret UUID out of one opaque reference.

    Anything that is not exactly the v1 GitHub-user-flow-verifier shape —
    including a refresh-credential or Connection-purpose reference — raises
    :class:`GitHubUserFlowVerifierReferenceError`; purpose references are
    never cross-parseable.
    """
    if not isinstance(value, str):
        raise GitHubUserFlowVerifierReferenceError(
            "a GitHub user flow pkce_verifier_reference must be a string"
        )
    segments = value.split(":")
    if len(segments) != len(_REFERENCE_SEGMENTS) + 1 or tuple(segments[:-1]) != _REFERENCE_SEGMENTS:
        raise GitHubUserFlowVerifierReferenceError(
            "the value is not a recognized v1 GitHub user flow verifier reference"
        )
    payload = segments[-1]
    try:
        secret_id = UUID(payload)
    except ValueError as exc:
        raise GitHubUserFlowVerifierReferenceError(
            "the GitHub user flow verifier reference payload is not a valid secret identifier"
        ) from exc
    if str(secret_id) != payload:
        raise GitHubUserFlowVerifierReferenceError(
            "the GitHub user flow verifier reference payload is not a canonical secret identifier"
        )
    return secret_id


# --- row mapping -------------------------------------------------------------

# The full column list, in the stable mapping order consumed by _flow_from_row.
_FLOW_COLUMNS = (
    "profile_id, purpose, state, pkce_verifier_reference, "
    "observed_revocation_count, started_at, expires_at"
)


def _flow_from_row(row: tuple[object, ...]) -> GitHubUserAuthorizationFlow:
    return GitHubUserAuthorizationFlow(
        profile_id=row[0],  # type: ignore[arg-type]
        purpose=GitHubUserAuthorizationFlowPurpose(row[1]),  # type: ignore[arg-type]
        state=row[2],  # type: ignore[arg-type]
        pkce_verifier_reference=row[3],  # type: ignore[arg-type]
        observed_revocation_count=row[4],  # type: ignore[arg-type]
        started_at=normalize_utc(row[5]),  # type: ignore[arg-type]
        expires_at=normalize_utc(row[6]),  # type: ignore[arg-type]
    )


# --- flow operations ---------------------------------------------------------


def create_pending_flow(
    pool: DatabasePool,
    *,
    profile_id: UUID,
    purpose: GitHubUserAuthorizationFlowPurpose,
    state: str,
    verifier: str,
    observed_revocation_count: int,
    expires_at: object,
) -> GitHubUserAuthorizationFlow:
    """Store the PKCE verifier in Vault and insert the pending flow.

    The caller is the flow service inside one composed transaction, so the
    Vault secret and the row install atomically (a failure anywhere rolls
    both back with the composition). The Vault row carries a NULL ``name``
    and a description of safe identifiers only — the Profile UUID, never the
    verifier value. The verifier parameter is secret-bearing exchange
    material accepted only at this trusted storage boundary; it is never
    persisted anywhere else.
    """
    secret_id = create_vault_secret(
        pool,
        secret=verifier,
        description=(
            f"openorc github user authorization flow PKCE verifier (profile {profile_id})"
        ),
    )
    reference = encode_github_user_flow_verifier_reference(secret_id)
    with transaction(pool) as conn:
        row = conn.execute(
            "insert into openorc.github_user_authorization_flows "
            "(profile_id, purpose, state, pkce_verifier_reference, "
            "observed_revocation_count, started_at, expires_at) "
            "values (%s, %s, %s, %s, %s, now(), %s) "
            f"returning {_FLOW_COLUMNS}",
            (
                profile_id,
                purpose.value,
                state,
                reference,
                observed_revocation_count,
                expires_at,
            ),
        ).fetchone()
    assert row is not None
    return _flow_from_row(row)


def get_pending_flow(pool: DatabasePool, *, profile_id: UUID) -> GitHubUserAuthorizationFlow | None:
    """Return the Profile's pending flow, or None when none is pending."""
    with transaction(pool) as conn:
        row = conn.execute(
            f"select {_FLOW_COLUMNS} from openorc.github_user_authorization_flows "
            "where profile_id = %s",
            (profile_id,),
        ).fetchone()
    return None if row is None else _flow_from_row(row)


def consume_pending_flow(
    pool: DatabasePool, *, state: str
) -> tuple[GitHubUserAuthorizationFlow, str] | None:
    """Atomically consume one pending flow by its exact state (single use).

    The conditional delete matches only a still-pending, unexpired row, so a
    replayed, expired, or unknown state returns None without any effect —
    the delete IS the single-use enforcement. On a match the correlation row
    is deleted, the Vault verifier is decrypted, and the verifier secret
    itself is deleted inside the same transaction: a malformed or dangling
    verifier reference — including a secret that turns out to be absent at
    cleanup time — raises :class:`GitHubUserFlowVerifierReferenceError` and
    rolls the whole consumption back, leaving the pending flow intact and
    retryable. The row and its secret commit together, so no consumed flow
    ever orphans its encrypted verifier material.

    The returned verifier string is secret-bearing exchange material: the
    completion service must use it immediately in its completion call chain
    and must never copy it into ordinary structures, DTOs, logs, or
    persistence.
    """
    with transaction(pool) as conn:
        row = conn.execute(
            "delete from openorc.github_user_authorization_flows "
            "where state = %s and expires_at > now() "
            f"returning {_FLOW_COLUMNS}",
            (state,),
        ).fetchone()
        if row is None:
            return None
        flow = _flow_from_row(row)
        secret_id = parse_github_user_flow_verifier_reference(flow.pkce_verifier_reference)
        verifier = read_vault_secret(pool, secret_id=secret_id)
        if not isinstance(verifier, str):
            raise GitHubUserFlowVerifierReferenceError(
                "the flow verifier reference does not point at an existing secret"
            )
        if not delete_vault_secret(pool, secret_id=secret_id):
            raise GitHubUserFlowVerifierReferenceError(
                "the flow verifier reference does not point at an existing secret"
            )
    return flow, verifier


def discard_pending_flow(pool: DatabasePool, *, state: str) -> bool:
    """Delete one provider-terminated pending flow and its verifier secret.

    The callback arrived with a GitHub ``error`` answer (for example, the
    Owner denied access at the consent step): the correlation is deleted so
    it can never be completed later. No verifier decryption is needed, but
    the secret itself is deleted inside the same transaction as the row — a
    malformed or dangling verifier reference raises
    :class:`GitHubUserFlowVerifierReferenceError` and rolls the whole discard
    back — so a declined flow never orphans its encrypted verifier material.
    Returns whether a pending flow was removed.
    """
    with transaction(pool) as conn:
        row = conn.execute(
            "delete from openorc.github_user_authorization_flows "
            "where state = %s returning pkce_verifier_reference",
            (state,),
        ).fetchone()
        if row is None:
            return False
        secret_id = parse_github_user_flow_verifier_reference(row[0])  # type: ignore[arg-type]
        if not delete_vault_secret(pool, secret_id=secret_id):
            raise GitHubUserFlowVerifierReferenceError(
                "the flow verifier reference does not point at an existing secret"
            )
    return True


def delete_pending_flow_with_verifier_secret(pool: DatabasePool, *, profile_id: UUID) -> bool:
    """Delete the Profile's pending flow row and its Vault verifier secret.

    One transaction: the row delete and the exact-ID Vault secret delete
    commit together, so no superseded or invalidated pending flow outlives
    its verifier secret and no Vault secret outlives its row. Returns False
    when no pending flow exists (the natural no-op for revocation and
    supersession compositions). A malformed or dangling verifier reference
    raises :class:`GitHubUserFlowVerifierReferenceError` and rolls the whole
    operation back — the caller translates that into its typed conflict
    vocabulary and fails closed rather than orphaning or trusting the flow.
    """
    with transaction(pool) as conn:
        row = conn.execute(
            "delete from openorc.github_user_authorization_flows "
            "where profile_id = %s returning pkce_verifier_reference",
            (profile_id,),
        ).fetchone()
        if row is None:
            return False
        secret_id = parse_github_user_flow_verifier_reference(row[0])  # type: ignore[arg-type]
        deleted = delete_vault_secret(pool, secret_id=secret_id)
        if not deleted:
            raise GitHubUserFlowVerifierReferenceError(
                "the flow verifier reference does not point at an existing secret"
            )
    return True
