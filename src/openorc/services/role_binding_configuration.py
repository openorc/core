"""Workflow role-binding configuration application service (issue #162).

The concrete per-role runtime-session configuration established by the
managed-Cline R3/R5 contract — the routed ``connection_id``, the
Owner-supplied opaque configured provider/model pair, and the nullable
Owner-authored role-prompt Markdown override — as ownership-gated use cases
over the durable, typed columns on ``openorc.workflow_role_bindings``.

These are live Workspace configuration, not historical evidence and not
runtime-session state:

- Editing them never mutates an already-resident runtime incarnation and
  never rewrites an existing ``TaskAgentSession``, its admitted Connection
  boundary, or its historical effective configuration snapshot; later
  legitimate runtime construction or same-ID reconstruction resolves the
  then-current configuration. This module composes no session persistence
  at all.
- The configured provider/model pair is configuration authority only:
  OpenOrc never validates it through a provider/model catalog,
  configured-provider enumeration, private runtime state, or required
  effective-identity readback. The pair is complete only when both values
  are present; a partial one-value command is rejected before persistence.
- ``role_prompt_override`` is ``None`` (use OpenOrc's current shipped default
  for the role) or the Owner's verbatim Markdown override — any non-NULL
  string, including an empty one, is an explicit override; Core does not
  parse, classify, or reason about its prose, and no shipped default is ever
  materialized into persistence.

Every public operation takes the authenticated Profile UUID from the #52
authentication boundary and composes ownership through the shared
Workspace-authorization resolvers — the only place Workspace ownership
policy is written — so authentication alone never establishes Workspace
access, and missing, foreign-Workspace, and non-owner subjects are uniformly
the same ``NotFoundError`` for reads and mutations alike. Database-only
validation and mutation compose inside one short ``composed_transaction``
(#51 external-I/O rule). A role-binding configuration change is an ordinary
Workspace configuration mutation: ownership-gated, account-barrier-gated,
same-value no-op, and audited (``WORKSPACE_CONFIGURATION_CHANGED``) only for
actual changes — with no active-Task admission lock, prompt-edit
prohibition, or shipped-default deployment gate introduced.
"""

from __future__ import annotations

from dataclasses import dataclass
from uuid import UUID

from psycopg import errors as psycopg_errors

from openorc.domain.connections import WorkflowRole, WorkflowRoleBinding
from openorc.observability import annotate_span, application_span
from openorc.persistence.connections import set_role_binding
from openorc.persistence.pool import DatabasePool
from openorc.services import event_coordination
from openorc.services.errors import InvalidCommandError, NotFoundError
from openorc.services.profile_lifecycle_guard import require_account_operational
from openorc.services.transaction_composition import composed_transaction
from openorc.services.workspace_authorization import (
    require_profile_workspace,
    require_workspace_role_binding,
)

__all__ = [
    "RoleBindingConfigurationUpdate",
    "get_role_binding_configuration",
    "set_role_binding_configuration",
]

# Application-service span boundaries (issues #108/#109): every public
# use-case operation of this module opens one span at the established service
# boundary. Only safe vocabulary attributes are attachable, so the Owner's
# provider/model identifiers and role-prompt prose have no supported path
# into telemetry.
_SERVICE_TRACER_SCOPE = "openorc.services.role_binding_configuration"
_GET_CONFIGURATION_SPAN_NAME = "role_binding_configuration.get_role_binding_configuration"
_SET_CONFIGURATION_SPAN_NAME = "role_binding_configuration.set_role_binding_configuration"


@dataclass(frozen=True, slots=True)
class RoleBindingConfigurationUpdate:
    """The resulting role-binding configuration plus its change fact.

    ``changed`` is the statement-level durable-change fact (an actual
    creation or field change). It is the only audit-handoff input: the
    recorded event context carries the setting and role, never the
    configuration values.
    """

    binding: WorkflowRoleBinding
    changed: bool


def get_role_binding_configuration(
    pool: DatabasePool, *, profile_id: UUID, workspace_id: UUID, role: WorkflowRole
) -> WorkflowRoleBinding:
    """Read one role's current runtime-session configuration, failing closed.

    The shared ``require_workspace_role_binding`` resolver establishes the
    authenticated Profile's exact Workspace ownership before any subject
    resolution: authentication alone never grants Workspace access, and a
    missing or foreign-Workspace binding is uniformly ``NotFoundError``.
    """
    with application_span(_SERVICE_TRACER_SCOPE, _GET_CONFIGURATION_SPAN_NAME) as span:
        _require_uuid_command(workspace_id, "workspace_id")
        _require_workflow_role(role)
        annotate_span(
            span,
            operation=_GET_CONFIGURATION_SPAN_NAME,
            workspace_id=str(workspace_id),
            workflow_role=role.value,
        )
        return require_workspace_role_binding(
            pool, profile_id=profile_id, workspace_id=workspace_id, role=role
        )


def set_role_binding_configuration(
    pool: DatabasePool,
    *,
    profile_id: UUID,
    workspace_id: UUID,
    role: WorkflowRole,
    connection_id: UUID,
    configured_provider: str | None,
    configured_model: str | None,
    role_prompt_override: str | None,
) -> RoleBindingConfigurationUpdate:
    """Replace one role binding's full runtime configuration, failing closed.

    The command is the full desired configuration record (whole-record
    semantics): ``role_prompt_override=None`` is the explicit reset to the
    shipped default. The one-binding-per-``(Workspace, role)`` identity and
    upsert semantics are preserved — the conditional upsert keeps the
    binding's ``id``/``created_at`` across updates, treats an identical
    write as a no-op, and reports ``changed`` from the durable statement
    outcome. A missing or cross-Workspace Connection is the uniform
    ``NotFoundError`` (the composite foreign key surfaces as
    ``ForeignKeyViolation`` and is classified here, never leaked).
    """
    with application_span(_SERVICE_TRACER_SCOPE, _SET_CONFIGURATION_SPAN_NAME) as span:
        # Command-shape validation runs INSIDE the use-case span and BEFORE
        # telemetry annotation, authorization, or persistence: malformed
        # caller-supplied values raise InvalidCommandError with their raw
        # values never entering telemetry.
        _require_uuid_command(workspace_id, "workspace_id")
        _require_uuid_command(connection_id, "connection_id")
        _require_workflow_role(role)
        _require_valid_configuration(configured_provider, configured_model, role_prompt_override)
        annotate_span(
            span,
            operation=_SET_CONFIGURATION_SPAN_NAME,
            workspace_id=str(workspace_id),
            workflow_role=role.value,
        )
        with composed_transaction(pool) as transaction_pool:
            # The account-wide Owner-mutation barrier first (issue #97): the
            # Profile FOR KEY SHARE read is the first lock acquisition and
            # fails closed while an account deletion attempt is unresolved.
            require_account_operational(transaction_pool, profile_id=profile_id)
            require_profile_workspace(
                transaction_pool, profile_id=profile_id, workspace_id=workspace_id
            )
            try:
                binding, changed = set_role_binding(
                    transaction_pool,
                    workspace_id=workspace_id,
                    role=role,
                    connection_id=connection_id,
                    configured_provider=configured_provider,
                    configured_model=configured_model,
                    role_prompt_override=role_prompt_override,
                )
            except psycopg_errors.ForeignKeyViolation as error:
                # The addressed Connection is missing or belongs to another
                # Workspace: the uniform fail-closed subject outcome, never a
                # probing oracle.
                raise NotFoundError(
                    "the requested connection is not available in this workspace"
                ) from error
            if changed:
                # An actual durable change only: identical writes record no
                # event, and the event insert commits (or rolls back) with the
                # canonical mutation as one transaction.
                event_coordination.record_role_binding_configuration_changed_event(
                    transaction_pool,
                    workspace_id=workspace_id,
                    actor=event_coordination.owner_actor(profile_id),
                    role=role,
                )
    return RoleBindingConfigurationUpdate(binding=binding, changed=changed)


def _require_uuid_command(value: object, name: str) -> None:
    """Reject a malformed UUID argument before any authorization or write."""
    if not isinstance(value, UUID):
        raise InvalidCommandError(f"{name} must be a UUID")


def _require_workflow_role(role: object) -> WorkflowRole:
    """Reject a malformed workflow-role argument before any authorization."""
    if not isinstance(role, WorkflowRole):
        raise InvalidCommandError("workflow role must be the producer or reviewer role")
    return role


def _require_valid_configuration(
    configured_provider: object, configured_model: object, role_prompt_override: object
) -> None:
    """Validate the configuration command shape before authorization/persistence.

    The provider/model pair is complete only when both values are present,
    and a present value is a nonblank opaque string. The role-prompt override
    is ``None`` (shipped default) or any verbatim string — including an
    empty/whitespace-only one, which is a deliberate explicit override Core
    never interprets.
    """
    if (configured_provider is None) != (configured_model is None):
        raise InvalidCommandError(
            "role provider/model configuration is complete only when both values are present"
        )
    if configured_provider is not None:
        if not isinstance(configured_provider, str) or not configured_provider.strip():
            raise InvalidCommandError(
                "the configured provider ID must be a nonblank opaque string when present"
            )
        if not isinstance(configured_model, str) or not configured_model.strip():
            raise InvalidCommandError(
                "the configured model ID must be a nonblank opaque string when present"
            )
    if role_prompt_override is not None and not isinstance(role_prompt_override, str):
        raise InvalidCommandError("the role-prompt override must be a string or null")
