"""Wire DTOs for the role-binding configuration surface (issue #162).

Thin transport models only: the Owner-authorized application service owns
every semantic rule (configuration authority, pair completeness, ownership,
same-value no-op, identity semantics) and these models never duplicate
service/domain validation — FastAPI parses the declared shapes and the
router delegates.
"""

from __future__ import annotations

from datetime import datetime
from uuid import UUID

from pydantic import BaseModel

from openorc.domain.connections import WorkflowRole

__all__ = [
    "RoleBindingConfigurationRead",
    "RoleBindingConfigurationUpdate",
]


class RoleBindingConfigurationUpdate(BaseModel):
    """The full desired role-binding configuration record.

    Full-record semantics: every field is the desired current value, so a
    ``null`` role-prompt override is the explicit reset to the shipped
    default. The configured provider/model values are opaque Owner-supplied
    configuration; the pair-completeness rule is the service's, never a
    wire-level duplicate.
    """

    connection_id: UUID
    configured_provider: str | None = None
    configured_model: str | None = None
    role_prompt_override: str | None = None


class RoleBindingConfigurationRead(BaseModel):
    """One role binding's durable configuration state."""

    id: UUID
    workspace_id: UUID
    role: WorkflowRole
    connection_id: UUID
    configured_provider: str | None
    configured_model: str | None
    role_prompt_override: str | None
    created_at: datetime
    updated_at: datetime
