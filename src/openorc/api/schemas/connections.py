"""Wire DTOs for the Workspace Connection administration surface (issue #371).

Thin transport models only: the Owner-authorized application service owns
every semantic rule (ownership, the account-mutation barrier, safe-config
validation per the #188 adapter contract, capacity validity, same-value
no-op and audit-on-change) and these models never duplicate service/domain
validation — FastAPI parses the declared shapes and the router delegates.

The read model exposes configured/available inventory state only and is
structurally incapable of carrying credential material: the opaque
``auth_reference`` and any credential value have no field at all, and
``credential_configured`` is the only credential fact. Read status never
claims runtime Hub health, session READY, or active-run state — configured
inventory is not proven runtime state.
"""

from __future__ import annotations

from datetime import datetime
from uuid import UUID

from pydantic import BaseModel, StrictInt

from openorc.domain.connections import AdapterType

__all__ = [
    "ConnectionCreate",
    "ConnectionCredentialValue",
    "ConnectionRead",
    "ConnectionReplace",
]


class ConnectionCreate(BaseModel):
    """The creation command for one Workspace Connection.

    Unprovided optional fields take their durable defaults (session
    capacity 2, enabled) or remain unset (repository base, control
    endpoint). The adapter type is carried so the contract names the
    adapter dimension; v1 supports only Cline and the service rejects any
    other value. The name/safe-config/capacity semantic rules are the
    service's, never wire duplicates.
    """

    name: str
    adapter_type: AdapterType = AdapterType.CLINE
    repository_base: str | None = None
    control_endpoint: str | None = None
    session_capacity: StrictInt | None = None
    enabled: bool = True


class ConnectionReplace(BaseModel):
    """The full desired Connection record.

    Full-record replacement semantics: EVERY field is required in the
    request and carries the desired current value — omitting a field is a
    wire-validation error, never a silent clear, so existing configuration
    can never be erased through omitted write arguments. Explicit ``null``
    is the only reset value (for the repository base and the control
    endpoint: back to the unset/stored-but-not-configurable state). The
    adapter type is immutable identity and is deliberately absent.
    """

    name: str
    repository_base: str | None
    control_endpoint: str | None
    session_capacity: StrictInt
    enabled: bool


class ConnectionCredentialValue(BaseModel):
    """The credential value for one Connection's control credential.

    The value is opaque; the non-empty rule is the #55 service's, never a
    wire duplicate. The value is never echoed back by any response.
    """

    credential: str


class ConnectionRead(BaseModel):
    """One Connection's durable identity and configured-inventory state.

    Safe configuration fields only: the validated repository base and
    control endpoint (``null`` when unset), the Owner-controlled enabled
    switch and admission capacity, and the credential presence boolean.
    ``credential_configured`` reports only that an OpenOrc-owned control
    credential reference exists — never a value, reference text, expiry,
    or health fact. This model deliberately carries no runtime Hub health,
    session READY, or active-run state.
    """

    id: UUID
    workspace_id: UUID
    adapter_type: AdapterType
    name: str
    repository_base: str | None
    control_endpoint: str | None
    session_capacity: int
    enabled: bool
    credential_configured: bool
    created_at: datetime
    updated_at: datetime
