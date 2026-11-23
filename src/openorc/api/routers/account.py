"""Authenticated permanent account-deletion transport (issue #374).

Public path composes under the versioned ``/api/v1`` namespace (issue #369).

This router owns HTTP concerns only: authenticated Profile extraction (the
#52 dependency), the per-request deployment composition of the Supabase
Auth Admin boundary (the credential-bearing component, issue #97), and the
typed outcome-to-wire mapping. It owns no deletion policy, no attempt-state
semantics, and no identity-provider mechanics — those belong to the service
and adapter layers. Blocking database and external Admin work runs off the
event loop (sync route handler; the v1 persistence surface is synchronous).
The addressed errors are the typed application vocabulary, mapped to HTTP
by the application-level application-error handler.

The authenticated Profile is the only authority: there is no Workspace
addressing, no delegate, and no caller-supplied accountable actor id — the
deletion always addresses the caller's own account. Guarded refusals (an
unresolved account-deletion attempt holding the account-wide Owner-mutation
barrier, or repeated durable-state contention) are typed conflicts; known
identity-provider failures and unknown external outcomes are honest 503s —
never reported as success, never replayed inside the same attempt.
"""

from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends, Request

from openorc.api.dependencies.account_admin_client import resolve_account_admin_client
from openorc.api.dependencies.application_errors import (
    DOCUMENTED_AUTH_401,
    DOCUMENTED_CONFLICT_409,
    DOCUMENTED_UNAVAILABLE_503,
)
from openorc.api.dependencies.authentication import authenticated_profile
from openorc.api.schemas.account import AccountDeletionResult
from openorc.config import Settings
from openorc.domain.ownership import Profile
from openorc.persistence.pool import get_database_pool
from openorc.services import account_lifecycle
from openorc.services.account_lifecycle import AccountDeletionOutcome

router = APIRouter(tags=["account"])

AuthenticatedProfile = Annotated[Profile, Depends(authenticated_profile)]

# The typed outcome vocabulary is closed; the wire mapping is a fixed table.
_WIRE_RESULT_BY_OUTCOME: dict[str, str] = {
    account_lifecycle.RESULT_DELETED: "deleted",
    account_lifecycle.RESULT_ALREADY_ABSENT: "already_absent",
}


@router.delete(
    "/profile/account",
    response_model=AccountDeletionResult,
    operation_id="delete_account",
    responses={
        401: DOCUMENTED_AUTH_401,
        409: DOCUMENTED_CONFLICT_409,
        503: DOCUMENTED_UNAVAILABLE_503,
    },
)
def delete_account(
    request: Request,
    profile: AuthenticatedProfile,
) -> AccountDeletionResult:
    """Permanently delete the authenticated Profile's own OpenOrc account.

    In one short transaction every OpenOrc-owned runtime-control credential
    is revoked and the durable deletion attempt is claimed — the
    account-wide barrier that fails closed every ordinary Owner mutation
    until the deletion resolves — then the supported Supabase Auth Admin
    boundary is asked to delete the exact Auth user with no database
    transaction open. ``deleted`` and ``already_absent`` are the truthful
    end states; a known identity-provider refusal leaves the account
    present (its OpenOrc-owned credentials stay revoked) and is a 503; an
    outcome that cannot be determined is honest uncertainty, also a 503,
    reconciled through the Admin read surface before any replay.
    """
    settings: Settings = request.app.state.settings
    admin_client = resolve_account_admin_client(settings)
    outcome = account_lifecycle.delete_account(
        get_database_pool(settings),
        admin_client,
        profile_id=profile.id,
    )
    return _to_wire(outcome)


def _to_wire(outcome: AccountDeletionOutcome) -> AccountDeletionResult:
    """Map the typed outcome onto the wire DTO (closed vocabulary only)."""
    wire_result = _WIRE_RESULT_BY_OUTCOME.get(outcome.result)
    if wire_result is None:
        # The service outcome vocabulary is closed by construction; an
        # unknown member is an invariant failure that fails closed rather
        # than ever being reported as a success state.
        raise RuntimeError("unknown account deletion outcome")
    return AccountDeletionResult(profile_id=outcome.profile_id, result=wire_result)
