"""Permanent account-deletion application service (issue #97).

The one administrative operation that crosses the Supabase Auth network
boundary: permanent deletion of the authenticated OpenOrc account. The
sequence is deliberate:

1. authenticate/authorize that the acting Profile is deleting its own
   account (the authenticated Profile UUID is the only authority; there is no
   Workspace addressing and no delegate);
2. in ONE short transaction, revoke every OpenOrc-owned runtime-control
   credential: enumerate every Connection across the Profile's Workspaces
   under deterministic row locks, delete every referenced Vault secret (a
   malformed or dangling reference fails closed with nothing written), and
   disconnect every Connection (``enabled = false`` with
   ``auth_reference = null``) — and claim the durable attempt state, the
   account-wide Owner-mutation barrier that fails closed every ordinary
   Owner mutation from this commit until the deletion resolves;
3. with NO database transaction open, request permanent deletion of the
   exact Supabase Auth user UUID through the supported Admin boundary;
4. rely on the existing ``auth.users -> profiles -> ...`` cascade to remove
   the OpenOrc application rows — no manual Profile/Workspace graph deletion
   competes with it, and no parallel session blacklist exists for the
   outstanding-JWT problem (a still-unexpired JWT cannot recreate the Profile
   through the #52 fail-closed identity boundary).

The Supabase Auth Admin client is the transport boundary supplied by the
calling component (which alone owns the deployment's secret API key); this
service composes it and classifies its outcomes:

- success → the account is deleted (the cascade removes the Profile, its
  graph, and the attempt state with it);
- confirmed absent → ``already_absent``: the idempotent end state. A second
  invocation after a prior success is classified here, never manufactured —
  including the case where the Profile row is already gone (see below);
- definitive rejection → known failure: the account stays present, its
  runtime credentials stay revoked, a later explicit retry is safe, and the
  attempt state is cleared attempt-scoped so normal account use resumes;
- unknown outcome → reconcile FIRST through the Admin read surface — absent
  completes as deleted; present reports the not-applied operation (known
  failure, attempt state cleared, later explicit retry safe); existence that
  cannot be determined marks the attempt ``uncertain`` (fail closed) and
  never replays the delete inside the same attempt.

Durable attempt/recovery state machine (the ``account_deletion_state`` /
``account_deletion_attempt_id`` / ``account_deletion_started_at`` tuple on
``openorc.profiles``, composite-CHECK-bound at the database boundary):

- no state: normal operation; an entry claim establishes ``active`` with a
  fresh attempt UUID composed with the revocation.
- ``active``, lease fresh: single-flight — another invocation is rejected
  without any external call.
- ``active``, lease EXPIRED: an abandoned attempt (for example a process
  crash between the revocation commit and the Auth call or its outcome
  persistence). Recovery reconciles through the read surface before any
  replay: absent → already absent; present → compare-and-swap the exact
  expired attempt into a fresh one and replay; undeterminable → attempt-
  scoped transition to ``uncertain``.
- ``uncertain``: the next explicit invocation reconciles BEFORE any replay.
  A reconciliation result is bound to the EXACT attempt UUID it reconciled:
  the replay claim compare-and-swaps that exact attempt, so a stale
  reconciliation (another retrier consumed the uncertain attempt and stored a
  newer one) matches zero rows and the caller reloads and reclassifies
  instead of authorizing a replay with a stale result.

The active-attempt lease is derived from the Admin client's bounded request
timeout (each of the two external calls) plus a documented safety margin, and
its expiry is compared against the DATABASE clock inside the persistence
statement — never the application clock. The lease makes abandoned attempts
recoverable while fresh attempts keep their single-flight protection.

Profile-absent idempotency: when the exact Profile row is absent at entry
(a prior successful Auth deletion cascade-removed it), there is no OpenOrc
credential graph or barrier left to revoke — the service reconciles the exact
Auth user UUID through the read surface with NO database transaction open
and NO destructive call: confirmed absent → ``already_absent``; confirmed
present → fail closed as an inconsistent durable state (never a delete for an
identity whose authorization anchor is gone, and never a manufactured
Profile); undeterminable → uncertainty. The normal transport may never reach
this path because #52 fails closed on the old JWT, but the application
service carries the idempotent semantics itself.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from uuid import UUID, uuid4

from openorc.adapters.supabase import (
    SupabaseAuthAdminClient,
    SupabaseAuthAdminOutcomeUnknownError,
    SupabaseAuthAdminRejectedError,
    SupabaseAuthAdminUserAbsentError,
)
from openorc.observability import annotate_span, application_span
from openorc.persistence import runtime_control_secrets
from openorc.persistence.connections import list_profile_connections_for_update
from openorc.persistence.deletion import disconnect_connection
from openorc.persistence.ownership import (
    claim_account_deletion_attempt,
    clear_account_deletion_attempt,
    get_profile_for_account_deletion,
    mark_account_deletion_attempt_uncertain,
    reclaim_expired_account_deletion_attempt,
    reclaim_uncertain_account_deletion_attempt,
)
from openorc.persistence.pool import DatabasePool
from openorc.persistence.runtime_control_secrets import RuntimeControlSecretReferenceError
from openorc.services.errors import (
    ConflictError,
    ExternalOperationFailedError,
    ExternalOperationUncertainError,
)
from openorc.services.transaction_composition import composed_transaction

__all__ = [
    "AccountDeletionOutcome",
    "RESULT_ALREADY_ABSENT",
    "RESULT_DELETED",
    "delete_account",
]

# Typed end states of the account-deletion lifecycle.
RESULT_DELETED = "deleted"
RESULT_ALREADY_ABSENT = "already_absent"

# Active-attempt lease margin: the lease must outlive a live attempt's two
# bounded Admin HTTP calls (delete_user and, when its outcome is unknown, the
# reconcile-only fetch_user) plus an explicit safety margin for the short
# database transactions between them. The lease interval is compared against
# the DATABASE clock inside the persistence statement — never the application
# clock — so an abandoned attempt is recoverable while a fresh one keeps its
# durable single-flight protection.
ACTIVE_ATTEMPT_LEASE_MARGIN_SECONDS = 5.0

# Bound on durable-state reclassification passes per invocation: a failed
# compare-and-swap means another invocation moved the state and this
# invocation reloads and reclassifies without reusing its stale
# reconciliation. Pathological persistent contention fails closed here rather
# than looping indefinitely.
_MAX_STATE_PASSES = 4

# Application-service span boundary (issues #108/#109): the account-deletion
# use case participates in OpenTelemetry traces. Only the safe attribute
# vocabulary is attachable, so no credential, key, or user-record content has
# a supported path into telemetry.
_ACCOUNT_LIFECYCLE_TRACER_SCOPE = "openorc.services.account_lifecycle"
_DELETE_ACCOUNT_SPAN_NAME = "account_lifecycle.delete_account"

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class AccountDeletionOutcome:
    """Typed, secret-free end state of one permanent account deletion."""

    profile_id: UUID
    result: str


def _active_attempt_lease_seconds(admin_client: SupabaseAuthAdminClient) -> float:
    """Derive the durable active-attempt lease from the bounded Admin timeouts.

    The lease must provably outlive any live attempt's external work: the two
    bounded Admin calls (the delete and, when its outcome is unknown, the
    reconciliation lookup) plus an explicit safety margin for the short
    database transactions between them.
    """
    return 2.0 * admin_client.request_timeout_seconds + ACTIVE_ATTEMPT_LEASE_MARGIN_SECONDS


def _revoke_profile_runtime_credentials(transaction_pool, *, profile_id: UUID) -> None:
    """Revoke every OpenOrc-owned runtime-control credential of one Profile.

    Composed inside the caller's one short transaction: every Connection
    across the Profile's Workspaces is enumerated under deterministic row
    locks (the same locks configure/rotate take), every referenced Vault
    secret is deleted, and every Connection is disconnected
    (``enabled = false``, ``auth_reference = null``) — the revocation barrier
    that outlives the transaction even when the external Auth deletion later
    fails. A malformed or dangling reference fails closed: nothing is
    claimed, nothing is revoked, and no external call is issued.
    """
    connections = list_profile_connections_for_update(transaction_pool, owner_profile_id=profile_id)
    for connection in connections:
        if connection.auth_reference is not None:
            try:
                secret_id = runtime_control_secrets.parse_connection_auth_reference(
                    connection.auth_reference
                )
            except RuntimeControlSecretReferenceError as exc:
                logger.warning(
                    "account deletion revocation rejected an unrecognized auth reference"
                )
                raise ConflictError(
                    "the account's credential references could not be reconciled; "
                    "nothing was revoked"
                ) from exc
            if not runtime_control_secrets.delete_runtime_control_secret(
                transaction_pool, secret_id=secret_id
            ):
                logger.warning("account deletion revocation rejected a dangling auth reference")
                raise ConflictError(
                    "the account's credential references could not be reconciled; "
                    "nothing was revoked"
                )
        disconnect_connection(transaction_pool, connection.id)


def _clear_own_attempt(pool: DatabasePool, *, profile_id: UUID, attempt_id: UUID) -> None:
    """Attempt-scoped clear of this invocation's own 'active' attempt.

    Conditional on the exact attempt UUID, so a foreign or moved-on attempt
    state is never overwritten. A conditional clear matching zero rows is an
    unexpected durable-state invariant (single-flight should have prevented
    it); the state is left untouched and surfaced operationally.
    """
    if not clear_account_deletion_attempt(pool, profile_id=profile_id, attempt_id=attempt_id):
        logger.warning(
            "account deletion recovery found the durable attempt state already moved on; "
            "it was left untouched"
        )


def _reconcile_absent_profile(
    admin_client: SupabaseAuthAdminClient, *, profile_id: UUID
) -> AccountDeletionOutcome:
    """Reconcile a Profile-absent entry through the Admin read surface only.

    The OpenOrc authorization anchor is gone: there is nothing to revoke and
    no destructive call is issued on this path. Confirmed absent completes as
    ``already_absent``; confirmed present is an inconsistent durable state
    (an Auth user with no Profile anchor) that fails closed without any
    delete and without manufacturing a Profile; an undeterminable lookup
    surfaces external-operation uncertainty.
    """
    try:
        present = admin_client.fetch_user(profile_id)
    except SupabaseAuthAdminRejectedError as exc:
        raise ExternalOperationUncertainError(
            "the account state could not be reconciled: the identity provider "
            "rejected the administrative lookup"
        ) from exc
    except SupabaseAuthAdminOutcomeUnknownError as exc:
        raise ExternalOperationUncertainError(
            "the account state could not be reconciled: the identity provider "
            "lookup outcome is unknown"
        ) from exc
    if not present:
        return AccountDeletionOutcome(profile_id=profile_id, result=RESULT_ALREADY_ABSENT)
    raise ConflictError(
        "the account is in an inconsistent state: its OpenOrc identity is gone "
        "but the identity-provider account still exists"
    )


def _classify_delete_outcome(
    pool: DatabasePool,
    admin_client: SupabaseAuthAdminClient,
    *,
    profile_id: UUID,
    attempt_id: UUID,
) -> AccountDeletionOutcome:
    """Issue the destructive Auth delete and classify its outcome.

    Called with NO database transaction open. Success and confirmed absence
    are end states (the sanctioned cascade removes the Profile, its graph,
    and the attempt state with it). A definitive rejection is a known failure:
    the account stays present, its credentials stay revoked, a later explicit
    retry is safe, and this attempt is cleared attempt-scoped so normal
    account use resumes. An unknown outcome reconciles through the read
    surface before anything else.
    """
    try:
        admin_client.delete_user(profile_id)
    except SupabaseAuthAdminUserAbsentError:
        # Already absent: the end state is true. If the user was removed
        # outside this attempt, the sanctioned cascade removed the Profile
        # and its attempt-state row with it — nothing to clear.
        return AccountDeletionOutcome(profile_id=profile_id, result=RESULT_ALREADY_ABSENT)
    except SupabaseAuthAdminRejectedError as exc:
        _clear_own_attempt(pool, profile_id=profile_id, attempt_id=attempt_id)
        raise ExternalOperationFailedError(
            "the account could not be deleted: the identity provider rejected "
            "the permanent deletion request"
        ) from exc
    except SupabaseAuthAdminOutcomeUnknownError as exc:
        return _classify_unknown_delete_outcome(
            pool,
            admin_client,
            profile_id=profile_id,
            attempt_id=attempt_id,
            original=exc,
        )
    return AccountDeletionOutcome(profile_id=profile_id, result=RESULT_DELETED)


def _classify_unknown_delete_outcome(
    pool: DatabasePool,
    admin_client: SupabaseAuthAdminClient,
    *,
    profile_id: UUID,
    attempt_id: UUID,
    original: Exception,
) -> AccountDeletionOutcome:
    """Reconcile an unknown destructive outcome; never replay inside the attempt.

    The reconciliation runs with NO database transaction open. Confirmed
    absent completes as deleted (the cascade has removed the OpenOrc graph).
    Confirmed present is the known not-applied outcome: the attempt is
    cleared attempt-scoped and the failure is reported for a later explicit
    retry. Existence that cannot be determined marks this attempt
    ``uncertain`` (attempt-scoped, fail closed) — the next explicit
    invocation reconciles before any replay.
    """
    try:
        present = admin_client.fetch_user(profile_id)
    except (SupabaseAuthAdminRejectedError, SupabaseAuthAdminOutcomeUnknownError):
        if not mark_account_deletion_attempt_uncertain(
            pool, profile_id=profile_id, attempt_id=attempt_id
        ):
            # The durable state moved on (an invariant single-flight should
            # have prevented); it stands as-is and is never overwritten.
            logger.warning(
                "account deletion uncertainty handling found the durable attempt state "
                "already moved on; it was left untouched"
            )
        raise ExternalOperationUncertainError(
            "the account deletion outcome is unknown and could not be reconciled; "
            "the attempt is recorded as uncertain"
        ) from original
    if not present:
        # The delete actually applied: the sanctioned cascade removed the
        # Profile and its attempt-state row.
        return AccountDeletionOutcome(profile_id=profile_id, result=RESULT_ALREADY_ABSENT)
    # The delete provably did not apply: a known failure of this attempt. The
    # account stays present, its credentials stay revoked, and a later
    # explicit retry is safe; the attempt state is cleared attempt-scoped so
    # normal account use resumes.
    _clear_own_attempt(pool, profile_id=profile_id, attempt_id=attempt_id)
    raise ExternalOperationFailedError(
        "the account was not deleted: the identity provider did not apply the permanent deletion"
    ) from original


def delete_account(
    pool: DatabasePool,
    admin_client: SupabaseAuthAdminClient,
    *,
    profile_id: UUID,
) -> AccountDeletionOutcome:
    """Permanently delete the authenticated Profile's own OpenOrc account.

    Transport-neutral and callable without FastAPI/RQ objects. The durable
    attempt state machine (documented in the module docstring) drives the
    whole lifecycle: revocation-then-claim in one short transaction, the
    external Auth Admin delete with NO transaction open, reconciliation
    before any replay, attempt-scoped recovery transitions, and a bounded
    lease distinguishing live single-flight attempts from abandoned ones. The
    returned outcome is a typed, secret-free end state.
    """
    with application_span(_ACCOUNT_LIFECYCLE_TRACER_SCOPE, _DELETE_ACCOUNT_SPAN_NAME) as span:
        annotate_span(span, operation=_DELETE_ACCOUNT_SPAN_NAME)
        lease_seconds = _active_attempt_lease_seconds(admin_client)
        for _pass in range(_MAX_STATE_PASSES):
            outcome = _attempt_account_deletion_pass(
                pool, admin_client, profile_id=profile_id, lease_seconds=lease_seconds
            )
            if outcome is not None:
                return outcome
        # Every pass found the durable state moved on after a failed
        # compare-and-swap: persistent contention fails closed without ever
        # authorizing a replay from a stale reconciliation.
        raise ConflictError(
            "the account deletion attempt state changed repeatedly during this "
            "invocation; no destructive replay was issued"
        )


def _attempt_account_deletion_pass(
    pool: DatabasePool,
    admin_client: SupabaseAuthAdminClient,
    *,
    profile_id: UUID,
    lease_seconds: float,
) -> AccountDeletionOutcome | None:
    """One pass over the durable account-deletion state machine.

    ``None`` means a compare-and-swap matched zero rows because the durable
    state moved on after this pass reconciled it (another invocation consumed
    it first): the caller reloads and reclassifies, never authorizing a
    replay from the stale reconciliation result.
    """
    claimed_attempt_id: UUID | None = None
    reconciled_attempt_id: UUID | None = None
    mode = ""

    # Stage 1: short entry transaction under the Profile-root FOR UPDATE.
    with composed_transaction(pool) as transaction_pool:
        locked = get_profile_for_account_deletion(
            transaction_pool, profile_id=profile_id, lease_seconds=lease_seconds
        )
        if locked is None:
            # Profile absent: no OpenOrc credential graph or barrier left to
            # revoke. Reconcile through the read surface only — never a blind
            # destructive call, never a manufactured Profile.
            mode = "absent_profile"
        else:
            _profile, state, lease_expired = locked
            if state is None:
                # Normal operation: claim the fresh attempt composed with the
                # revocation; the barrier is durable from this commit.
                attempt_id = uuid4()
                if not claim_account_deletion_attempt(
                    transaction_pool, profile_id=profile_id, attempt_id=attempt_id
                ):
                    # Unreachable while the composition holds the root lock.
                    raise ConflictError(
                        "the account deletion attempt state changed during the attempt"
                    )
                _revoke_profile_runtime_credentials(transaction_pool, profile_id=profile_id)
                claimed_attempt_id = attempt_id
            elif state.state == "active" and not lease_expired:
                # Durable single-flight: a live concurrent attempt owns the claim.
                raise ConflictError(
                    "an account deletion attempt is already in progress for this account"
                )
            else:
                # 'uncertain', or an EXPIRED 'active' (abandoned) attempt:
                # reconcile externally BEFORE any replay, with no transaction
                # open during the lookup.
                mode = state.state
                reconciled_attempt_id = state.attempt_id
    # Entry transaction committed (or rolled back) — no external call above.

    if mode == "absent_profile":
        return _reconcile_absent_profile(admin_client, profile_id=profile_id)
    if claimed_attempt_id is not None:
        return _classify_delete_outcome(
            pool, admin_client, profile_id=profile_id, attempt_id=claimed_attempt_id
        )
    if reconciled_attempt_id is None:  # pragma: no cover - unreachable by construction
        raise ConflictError(
            "the account deletion attempt state could not be interpreted on its own terms"
        )

    # Stage 2: reconcile the EXACT attempt this pass observed — with NO
    # database transaction open — before any destructive replay.
    try:
        present = admin_client.fetch_user(profile_id)
    except (SupabaseAuthAdminRejectedError, SupabaseAuthAdminOutcomeUnknownError) as exc:
        # Adopt uncertainty for the exact abandoned attempt, attempt-scoped.
        if mode == "active" and not mark_account_deletion_attempt_uncertain(
            pool, profile_id=profile_id, attempt_id=reconciled_attempt_id
        ):
            logger.warning(
                "account deletion recovery found the durable attempt state already "
                "moved on; it was left untouched"
            )
        raise ExternalOperationUncertainError(
            "the account deletion state could not be reconciled: the identity "
            "provider lookup could not determine whether the account still exists"
        ) from exc
    if not present:
        # The reconciled attempt's deletion actually applied: the sanctioned
        # cascade removed the Profile and its attempt-state row.
        return AccountDeletionOutcome(profile_id=profile_id, result=RESULT_ALREADY_ABSENT)

    # Present: the reconciled attempt provably did not apply. Compare-and-swap
    # THE EXACT reconciled attempt into a fresh active attempt composed with
    # the idempotent revocation re-run, then replay the delete.
    new_attempt_id = uuid4()
    claimed = False
    with composed_transaction(pool) as transaction_pool:
        if mode == "uncertain":
            claimed = reclaim_uncertain_account_deletion_attempt(
                transaction_pool,
                profile_id=profile_id,
                reconciled_attempt_id=reconciled_attempt_id,
                new_attempt_id=new_attempt_id,
            )
        else:
            claimed = reclaim_expired_account_deletion_attempt(
                transaction_pool,
                profile_id=profile_id,
                expired_attempt_id=reconciled_attempt_id,
                new_attempt_id=new_attempt_id,
                lease_seconds=lease_seconds,
            )
        if claimed:
            _revoke_profile_runtime_credentials(transaction_pool, profile_id=profile_id)
    if not claimed:
        # The compare-and-swap matched zero rows: another invocation consumed
        # or moved the durable state after this pass reconciled it. The stale
        # reconciliation is never used to authorize a replay; reload and
        # reclassify.
        return None
    return _classify_delete_outcome(
        pool, admin_client, profile_id=profile_id, attempt_id=new_attempt_id
    )
