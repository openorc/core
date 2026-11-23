"""Wire DTOs for the authenticated account-deletion surface (issue #374).

Thin transport models only: the account-lifecycle application service owns
every semantic rule (the durable attempt state machine, revocation fencing,
truthful known/unknown outcome classification) and these models never
duplicate service validation. The outcome is the typed, secret-free end
state the service returns — a guarded refusal, a known identity-provider
failure, or an unknown external outcome is a typed application error mapped
to HTTP, never a success body.
"""

from __future__ import annotations

from typing import Literal
from uuid import UUID

from pydantic import BaseModel

__all__ = ["AccountDeletionResult"]


class AccountDeletionResult(BaseModel):
    """The typed, secret-free end state of one permanent account deletion.

    ``deleted`` means the identity provider applied the permanent deletion
    (the sanctioned cascade removes the OpenOrc graph with it);
    ``already_absent`` is the idempotent end state, including a second
    invocation after a prior success. No credential, key, or identity-provider
    record content is ever carried on this surface.
    """

    profile_id: UUID
    result: Literal["deleted", "already_absent"]
