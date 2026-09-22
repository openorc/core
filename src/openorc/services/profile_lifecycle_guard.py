"""Account-wide Owner-mutation barrier for the account-deletion lifecycle.

The durable, database-backed write barrier established by the permanent
account-deletion service (issue #97): while the Profile's
``account_deletion_state`` attempt state exists (``active`` or ``uncertain``),
every ordinary authenticated Owner mutation fails closed. This closes the
account-deletion external-call window: after the revocation transaction
commits and before the Supabase Auth Admin deletion resolves, the account and
its still-valid JWT remain usable, and a concurrent Owner mutation could
otherwise create a fresh Workspace/Connection and install a new Vault secret
that the Auth-root cascade would orphan (Vault secrets are not foreign-keyed
rows).

The guard is the FIRST lock acquisition of every guarded mutation — before
any subject-row lock — so a guarded mutation never holds a Connection/Task
lock while waiting on the Profile. It composes with the account-deletion
transaction's Profile-root ``FOR UPDATE`` through the deliberate
``FOR KEY SHARE`` read: concurrent guarded mutations either commit before
that transaction's revocation enumeration (their effects are then visible and
cleaned) or block and re-read the committed state after its commit. Guards
are never individually spanned; they run inside the instrumented use-case
span of the operation composing them (issue #109 service-span contract).

Scope discipline: the guard is composed ONLY by authenticated Owner mutation
paths that carry the authenticated Profile UUID — Workspace configuration
(#53), Connection credential administration (#55), and the #97 administrative
deletion operations. The #54 Task mutation primitives are deliberately NOT
guarded: they are authority-neutral foundational workflow services whose
signatures carry workspace/task/currentness facts rather than an authenticated
Profile, and later Owner-facing workflow capabilities compose this barrier at
their Owner authorization boundary. Deleting an account composes no guard
(it must stay retryable); authentication and ordinary reads are unaffected.

When the durable state resolves — attempt-scoped clear after a confirmed
known-not-applied outcome, Auth-root cascade removal on success — guarded
mutations resume failing closed only while a state row actually exists.
"""

from __future__ import annotations

from uuid import UUID

from openorc.persistence.ownership import read_account_deletion_state_for_key_share
from openorc.persistence.pool import DatabasePool
from openorc.services.errors import ConflictError, NotFoundError

__all__ = ["require_account_operational"]


def require_account_operational(pool: DatabasePool, *, profile_id: UUID) -> None:
    """Fail closed while the Profile's durable deletion-attempt state exists.

    Composed as the FIRST authorization read of every guarded Owner mutation,
    inside the caller's composed transaction: the ``FOR KEY SHARE`` read on
    the Profile row conflicts with the account-deletion transaction's
    Profile-root ``FOR UPDATE``, serializing in-flight guarded mutations
    against the revocation commit, and any existing attempt state rejects the
    mutation outright. A missing Profile fails closed as the uniform
    not-found outcome (the #53 anti-probing rule at the account level).
    """
    exists, state = read_account_deletion_state_for_key_share(pool, profile_id=profile_id)
    if not exists:
        raise NotFoundError("the requested account is not available to this Profile")
    if state is not None:
        raise ConflictError(
            "the account is being deleted: changes are rejected until the deletion attempt resolves"
        )
