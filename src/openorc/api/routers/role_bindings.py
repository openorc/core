"""Thin role-binding configuration transport (issue #162).

This router owns HTTP concerns only: authenticated Profile extraction (the
#52 dependency), request path/body parsing, delegation to the shared
Owner-authorized application service, and the domain-to-wire response
mapping. It owns no authorization policy, no configuration semantics, and no
persistence behavior — those belong to the service layer. Blocking database
work runs off the event loop (sync route handlers; the v1 persistence
surface is synchronous). The addressed errors are the typed application
vocabulary, mapped to HTTP by the application-level application-error
handler.
"""

from __future__ import annotations

from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, Request

from openorc.api.dependencies.authentication import authenticated_profile
from openorc.api.schemas.role_bindings import (
    RoleBindingConfigurationRead,
    RoleBindingConfigurationUpdate,
)
from openorc.config import Settings
from openorc.domain.connections import WorkflowRole, WorkflowRoleBinding
from openorc.domain.ownership import Profile
from openorc.persistence.pool import get_database_pool
from openorc.services.role_binding_configuration import (
    get_role_binding_configuration,
    set_role_binding_configuration,
)

router = APIRouter(tags=["role-bindings"])

AuthenticatedProfile = Annotated[Profile, Depends(authenticated_profile)]


@router.get(
    "/api/workspaces/{workspace_id}/role-bindings/{role}",
    response_model=RoleBindingConfigurationRead,
)
def read_role_binding_configuration(
    workspace_id: UUID,
    role: WorkflowRole,
    request: Request,
    profile: AuthenticatedProfile,
) -> RoleBindingConfigurationRead:
    """Read one role binding's current runtime-session configuration."""
    settings: Settings = request.app.state.settings
    binding = get_role_binding_configuration(
        get_database_pool(settings),
        profile_id=profile.id,
        workspace_id=workspace_id,
        role=role,
    )
    return _to_wire(binding)


@router.put(
    "/api/workspaces/{workspace_id}/role-bindings/{role}",
    response_model=RoleBindingConfigurationRead,
)
def update_role_binding_configuration(
    workspace_id: UUID,
    role: WorkflowRole,
    payload: RoleBindingConfigurationUpdate,
    request: Request,
    profile: AuthenticatedProfile,
) -> RoleBindingConfigurationRead:
    """Replace one role binding's full runtime-session configuration."""
    settings: Settings = request.app.state.settings
    result = set_role_binding_configuration(
        get_database_pool(settings),
        profile_id=profile.id,
        workspace_id=workspace_id,
        role=role,
        connection_id=payload.connection_id,
        configured_provider=payload.configured_provider,
        configured_model=payload.configured_model,
        role_prompt_override=payload.role_prompt_override,
    )
    return _to_wire(result.binding)


def _to_wire(binding: WorkflowRoleBinding) -> RoleBindingConfigurationRead:
    """Map the domain binding onto the wire DTO (safe configuration fields only)."""
    return RoleBindingConfigurationRead(
        id=binding.id,
        workspace_id=binding.workspace_id,
        role=binding.role,
        connection_id=binding.connection_id,
        configured_provider=binding.configured_provider,
        configured_model=binding.configured_model,
        role_prompt_override=binding.role_prompt_override,
        created_at=binding.created_at,
        updated_at=binding.updated_at,
    )
