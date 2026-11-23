"""Thin Workspace Connection administration transport (issue #371).

Public paths compose under the versioned ``/api/v1`` namespace (issue #369).

This router owns HTTP concerns only: authenticated Profile extraction (the
#52 dependency), request path/body parsing, delegation to the shared
Owner-authorized application services, and the domain-to-wire response
mapping. It owns no authorization policy, no safe-config semantics, and no
persistence behavior — those belong to the service layer. Connection
administration delegates to the issue #371 configuration service; credential
setup and rotation delegate through its thin composition of the issue #55
credential service, so every projected response passes the same
read-boundary validation. The issue #374 administrative disconnect and
deletion routes delegate to the issue #97 administrative-deletion service
unchanged. Blocking database work runs off the event loop
(sync route handlers; the v1 persistence surface is synchronous). The
addressed errors are the typed application vocabulary, mapped to HTTP by the
application-level application-error handler.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, Request

from openorc.adapters.cline.connection_endpoint import (
    CONNECTION_CONTROL_ENDPOINT_CONFIG_KEY,
    CONNECTION_REPOSITORY_BASE_CONFIG_KEY,
)
from openorc.api.dependencies.application_errors import (
    DOCUMENTED_AUTH_401,
    DOCUMENTED_CONFLICT_409,
    DOCUMENTED_INVALID_COMMAND_422,
    DOCUMENTED_NOT_FOUND_404,
    DOCUMENTED_UNAVAILABLE_503,
)
from openorc.api.dependencies.authentication import authenticated_profile
from openorc.api.schemas.connections import (
    ConnectionCreate,
    ConnectionCredentialValue,
    ConnectionRead,
    ConnectionReplace,
)
from openorc.config import Settings
from openorc.domain.connections import Connection
from openorc.domain.ownership import Profile
from openorc.persistence.pool import get_database_pool
from openorc.services import administrative_deletion, connection_configuration

router = APIRouter(tags=["connections"])

AuthenticatedProfile = Annotated[Profile, Depends(authenticated_profile)]

# The documented responses shared by the administrative disconnect/deletion
# routes (issue #374): guarded refusals are conflicts, never successes.
_DELETION_RESPONSES = {
    401: DOCUMENTED_AUTH_401,
    404: DOCUMENTED_NOT_FOUND_404,
    409: DOCUMENTED_CONFLICT_409,
    503: DOCUMENTED_UNAVAILABLE_503,
}


@router.get(
    "/workspaces/{workspace_id}/connections",
    response_model=list[ConnectionRead],
    operation_id="list_workspace_connections",
    responses={
        401: DOCUMENTED_AUTH_401,
        404: DOCUMENTED_NOT_FOUND_404,
        409: DOCUMENTED_CONFLICT_409,
        503: DOCUMENTED_UNAVAILABLE_503,
    },
)
def list_connections(
    workspace_id: UUID,
    request: Request,
    profile: AuthenticatedProfile,
) -> list[ConnectionRead]:
    """List the Workspace's configured Agent Runtime Connections."""
    settings: Settings = request.app.state.settings
    connections = connection_configuration.list_workspace_connections(
        get_database_pool(settings), profile_id=profile.id, workspace_id=workspace_id
    )
    return [_to_wire(connection) for connection in connections]


@router.post(
    "/workspaces/{workspace_id}/connections",
    response_model=ConnectionRead,
    status_code=201,
    operation_id="create_workspace_connection",
    responses={
        401: DOCUMENTED_AUTH_401,
        404: DOCUMENTED_NOT_FOUND_404,
        409: DOCUMENTED_CONFLICT_409,
        422: DOCUMENTED_INVALID_COMMAND_422,
        503: DOCUMENTED_UNAVAILABLE_503,
    },
)
def create_connection(
    workspace_id: UUID,
    payload: ConnectionCreate,
    request: Request,
    profile: AuthenticatedProfile,
) -> ConnectionRead:
    """Create one Agent Runtime Connection in the Workspace."""
    settings: Settings = request.app.state.settings
    connection = connection_configuration.create_workspace_connection(
        get_database_pool(settings),
        profile_id=profile.id,
        workspace_id=workspace_id,
        adapter_type=payload.adapter_type,
        name=payload.name,
        repository_base=payload.repository_base,
        control_endpoint=payload.control_endpoint,
        session_capacity=payload.session_capacity,
        enabled=payload.enabled,
    )
    return _to_wire(connection)


@router.get(
    "/workspaces/{workspace_id}/connections/{connection_id}",
    response_model=ConnectionRead,
    operation_id="read_workspace_connection",
    responses={
        401: DOCUMENTED_AUTH_401,
        404: DOCUMENTED_NOT_FOUND_404,
        409: DOCUMENTED_CONFLICT_409,
        503: DOCUMENTED_UNAVAILABLE_503,
    },
)
def read_connection(
    workspace_id: UUID,
    connection_id: UUID,
    request: Request,
    profile: AuthenticatedProfile,
) -> ConnectionRead:
    """Read one Connection's configured-inventory state."""
    settings: Settings = request.app.state.settings
    connection = connection_configuration.get_workspace_connection(
        get_database_pool(settings),
        profile_id=profile.id,
        workspace_id=workspace_id,
        connection_id=connection_id,
    )
    return _to_wire(connection)


@router.put(
    "/workspaces/{workspace_id}/connections/{connection_id}",
    response_model=ConnectionRead,
    operation_id="replace_workspace_connection",
    responses={
        401: DOCUMENTED_AUTH_401,
        404: DOCUMENTED_NOT_FOUND_404,
        409: DOCUMENTED_CONFLICT_409,
        422: DOCUMENTED_INVALID_COMMAND_422,
        503: DOCUMENTED_UNAVAILABLE_503,
    },
)
def replace_connection(
    workspace_id: UUID,
    connection_id: UUID,
    payload: ConnectionReplace,
    request: Request,
    profile: AuthenticatedProfile,
) -> ConnectionRead:
    """Replace one Connection's full mutable Owner configuration record."""
    settings: Settings = request.app.state.settings
    connection = connection_configuration.replace_workspace_connection(
        get_database_pool(settings),
        profile_id=profile.id,
        workspace_id=workspace_id,
        connection_id=connection_id,
        name=payload.name,
        repository_base=payload.repository_base,
        control_endpoint=payload.control_endpoint,
        session_capacity=payload.session_capacity,
        enabled=payload.enabled,
    )
    return _to_wire(connection)


@router.post(
    "/workspaces/{workspace_id}/connections/{connection_id}/credentials",
    response_model=ConnectionRead,
    status_code=201,
    operation_id="configure_connection_credential",
    responses={
        401: DOCUMENTED_AUTH_401,
        404: DOCUMENTED_NOT_FOUND_404,
        409: DOCUMENTED_CONFLICT_409,
        422: DOCUMENTED_INVALID_COMMAND_422,
        503: DOCUMENTED_UNAVAILABLE_503,
    },
)
def configure_credential(
    workspace_id: UUID,
    connection_id: UUID,
    payload: ConnectionCredentialValue,
    request: Request,
    profile: AuthenticatedProfile,
) -> ConnectionRead:
    """Configure the Connection's control credential (issue #55 composition).

    Configuring over an existing credential is a conflict: rotate instead.
    The credential value is never echoed back by any response, and the
    response passes the service's read-boundary validation — a legacy or
    malformed persisted safe config fails closed instead of being projected.
    """
    settings: Settings = request.app.state.settings
    connection = connection_configuration.setup_connection_credential(
        get_database_pool(settings),
        profile_id=profile.id,
        workspace_id=workspace_id,
        connection_id=connection_id,
        credential=payload.credential,
    )
    return _to_wire(connection)


@router.put(
    "/workspaces/{workspace_id}/connections/{connection_id}/credentials",
    response_model=ConnectionRead,
    operation_id="rotate_connection_credential",
    responses={
        401: DOCUMENTED_AUTH_401,
        404: DOCUMENTED_NOT_FOUND_404,
        409: DOCUMENTED_CONFLICT_409,
        422: DOCUMENTED_INVALID_COMMAND_422,
        503: DOCUMENTED_UNAVAILABLE_503,
    },
)
def rotate_credential(
    workspace_id: UUID,
    connection_id: UUID,
    payload: ConnectionCredentialValue,
    request: Request,
    profile: AuthenticatedProfile,
) -> ConnectionRead:
    """Rotate the Connection's control credential in place (issue #55 composition).

    Rotating without a configured credential is a conflict: set one up
    first. The credential value is never echoed back by any response, and
    the response passes the service's read-boundary validation — a legacy
    or malformed persisted safe config fails closed instead of being
    projected.
    """
    settings: Settings = request.app.state.settings
    connection = connection_configuration.rotate_connection_credential(
        get_database_pool(settings),
        profile_id=profile.id,
        workspace_id=workspace_id,
        connection_id=connection_id,
        credential=payload.credential,
    )
    return _to_wire(connection)


@router.delete(
    "/workspaces/{workspace_id}/connections/{connection_id}/credentials",
    response_model=ConnectionRead,
    operation_id="disconnect_connection_credential",
    responses=_DELETION_RESPONSES,
)
def disconnect_credential(
    workspace_id: UUID,
    connection_id: UUID,
    request: Request,
    profile: AuthenticatedProfile,
) -> ConnectionRead:
    """Disconnect the Connection's control credential (issue #374).

    One atomic administrative composition: the account-wide Owner-mutation
    barrier, the Workspace ownership gate, the row-locked Connection read,
    the referenced OpenOrc-owned Vault secret's deletion, and the
    disconnect semantics (``enabled = false`` with ``auth_reference =
    null``). Distinct from disabling the Connection (the replace operation
    with ``enabled = false`` keeps its credential) and from deleting the
    Connection's record (the DELETE on the Connection itself). A malformed
    or dangling credential reference is a conflict — never a reported
    success — and runtime-owned credentials and provider state are never
    touched.
    """
    settings: Settings = request.app.state.settings
    disconnected = administrative_deletion.disconnect_connection_credential(
        get_database_pool(settings),
        profile_id=profile.id,
        workspace_id=workspace_id,
        connection_id=connection_id,
    )
    return _to_wire(disconnected)


@router.delete(
    "/workspaces/{workspace_id}/connections/{connection_id}",
    response_model=ConnectionRead,
    operation_id="delete_workspace_connection",
    responses=_DELETION_RESPONSES,
)
def delete_connection(
    workspace_id: UUID,
    connection_id: UUID,
    request: Request,
    profile: AuthenticatedProfile,
) -> ConnectionRead:
    """Hard-delete an unreferenced Connection with its credential cleanup.

    The OpenOrc-owned Vault secret cleanup runs first; the Phase 1 delete
    primitive then forces its historical-reference constraints immediate —
    a Connection still referenced by historical TaskAgentSessions or
    workflow role bindings is a typed conflict (history is never cascaded
    away to make the delete succeed). The response is the deleted
    Connection's final configured-inventory state; no runtime Hub or
    filesystem state is ever touched.
    """
    settings: Settings = request.app.state.settings
    deleted = administrative_deletion.delete_connection_record(
        get_database_pool(settings),
        profile_id=profile.id,
        workspace_id=workspace_id,
        connection_id=connection_id,
    )
    return _to_wire(deleted)


def _permitted_value(safe_config: Mapping[str, object], key: str) -> str | None:
    """Project one permitted safe-config value.

    The service boundary has validated every returned value against the
    established contracts (the read-boundary rule), so the permitted fields
    are mapped directly and unknown persisted keys are never projected. The
    isinstance narrowing expresses that validated guarantee.
    """
    value = safe_config.get(key)
    return value if isinstance(value, str) else None


def _to_wire(connection: Connection) -> ConnectionRead:
    """Map the domain Connection onto the wire DTO (safe configured-inventory fields only).

    The opaque ``auth_reference`` appears only as the presence boolean; no
    credential value, reference text, or runtime-URL assertion is ever
    carried, and the exposed state is configured inventory — never runtime
    Hub health, session READY, or active-run state.
    """
    return ConnectionRead(
        id=connection.id,
        workspace_id=connection.workspace_id,
        adapter_type=connection.adapter,
        name=connection.name,
        repository_base=_permitted_value(
            connection.safe_config, CONNECTION_REPOSITORY_BASE_CONFIG_KEY
        ),
        control_endpoint=_permitted_value(
            connection.safe_config, CONNECTION_CONTROL_ENDPOINT_CONFIG_KEY
        ),
        session_capacity=connection.session_capacity,
        enabled=connection.enabled,
        credential_configured=connection.auth_reference is not None,
        created_at=connection.created_at,
        updated_at=connection.updated_at,
    )
