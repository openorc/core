"""Workspace Connection administration application service (issue #371).

The G1b Owner-facing Connection administration use cases over the existing
Connection persistence (:mod:`openorc.persistence.connections`): the
authenticated Owner lists, reads, creates, and reconfigures the Workspace
Agent Runtime Connections. This module is deliberately narrow — the
configuration boundary only. The trusted control-credential lifecycle stays
in :mod:`openorc.services.connection_credentials` (issue #55), the role
bindings stay in :mod:`openorc.services.role_binding_configuration`
(issue #162), and disconnect/deletion stays with the administrative deletion
services. Nothing here touches ``task_agent_sessions`` or
``workflow_role_bindings``: existing Task/session bindings never migrate
merely from configuration edits, and Connection configuration is live
Workspace configuration, not runtime state.

Safe configuration (the #188 adapter contract): the v1 Cline Connection
``safe_config`` may carry exactly the permitted non-secret Owner fields —
``repository_base`` and the optional ``control_endpoint``. The command
surface is field-based, so no other key can ever be written.
``repository_base`` is the runtime-side repository base the runtime
construction composes: a canonical absolute runtime-side path (starts with
``/`` and carries no empty, ``.``, or ``..`` segments) validated by shape
only — Core never probes any filesystem. The ``control_endpoint`` is
validated by composing the adapter's single #188 parsing/validation
primitive
(:func:`openorc.adapters.cline.resolve_connection_control_endpoint`); the
adapter's rejection messages are safe by contract and never echo the
configured value. An absent endpoint is a valid stored-but-not-attachable
Connection; no value is ever invented.

Read-boundary validation: the read paths project persisted safe config only
after re-validating the stored values against the same established
contracts, so a legacy or otherwise malformed persisted value can never be
exposed as approved safe configuration. A violation is a durable-state
invariant failure: the read fails closed with ``ConflictError`` carrying a
fixed safe message (never the stored value) plus a safe warning — mirroring
the credential service's malformed-reference handling. Unknown keys are
never projected (only the permitted fields are exposed); they are not a
read failure.

Credentials never appear here: the domain Connection carries only the
opaque nullable ``auth_reference``, and this service neither accepts nor
returns any credential value. The configured state this module exposes
(enabled, endpoint, credential presence) is inventory configuration only
and is deliberately distinct from proven runtime Hub health, session READY,
or active native run state.

Authorization and audit: every public operation takes the authenticated
Profile UUID from the #52 authentication boundary and composes ownership
through the shared Workspace-authorization resolvers; missing and
cross-Workspace Connections are uniformly ``NotFoundError``. Mutations are
guarded Owner mutations: the account-wide deletion barrier (issue #97) is
the first lock acquisition inside one short ``composed_transaction``, and a
changed full-record replacement records one
``WORKSPACE_CONFIGURATION_CHANGED`` audit event inside the same transaction
— a same-value no-op records no event and does not churn the row. Creation
is not audited: no creation-time event vocabulary exists in the durable
vocabulary, so none is invented (the #370 Workspace-creation precedent).
"""

from __future__ import annotations

import logging
from uuid import UUID

from openorc.adapters.cline.connection_endpoint import (
    CONNECTION_CONTROL_ENDPOINT_CONFIG_KEY,
    CONNECTION_REPOSITORY_BASE_CONFIG_KEY,
    ConnectionControlEndpointError,
    resolve_connection_control_endpoint,
)
from openorc.domain.connections import AdapterType, Connection
from openorc.observability import annotate_span, application_span
from openorc.persistence import connections as connection_records
from openorc.persistence.pool import DatabasePool
from openorc.services import event_coordination
from openorc.services.errors import ConflictError, InvalidCommandError, NotFoundError
from openorc.services.profile_lifecycle_guard import require_account_operational
from openorc.services.transaction_composition import composed_transaction
from openorc.services.workspace_authorization import require_profile_workspace

__all__ = [
    "create_workspace_connection",
    "get_workspace_connection",
    "list_workspace_connections",
    "replace_workspace_connection",
]

# Application-service span boundaries (issues #108/#109): every public
# use-case operation of this module opens one span at the established service
# boundary. Only the safe attribute vocabulary is attachable, so safe-config
# values have no supported path into telemetry.
_SERVICE_TRACER_SCOPE = "openorc.services.connection_configuration"
_LIST_SPAN_NAME = "connection_configuration.list_workspace_connections"
_GET_SPAN_NAME = "connection_configuration.get_workspace_connection"
_CREATE_SPAN_NAME = "connection_configuration.create_workspace_connection"
_REPLACE_SPAN_NAME = "connection_configuration.replace_workspace_connection"

# The durable default (issue #200): one shared Connection serves both Task
# roles, whose two session reservations consume two slots. An unprovided
# capacity takes this default; explicit positive values pass through.
_DEFAULT_SESSION_CAPACITY = 2

# The Connection ``name`` column is durable data the domain validates only
# for nonblankness, so the bounded-length rule is service-owned, like the
# Workspace name rule.
_MAX_CONNECTION_NAME_LENGTH = 200

# Fixed safe message for a persisted safe_config that violates the
# established contracts: never the stored value (the issue #109 durable-state
# invariant handling, mirroring the credential service's reference checks).
_UNSAFE_STORED_CONFIG_MESSAGE = (
    "the connection's stored configuration does not satisfy the established "
    "safe-configuration contract"
)

logger = logging.getLogger(__name__)


def _require_uuid(value: object, name: str) -> UUID:
    """Reject a malformed subject identifier before any authorization."""
    if not isinstance(value, UUID):
        raise InvalidCommandError(f"the addressed {name} must be a UUID")
    return value


def _validate_connection_name(name: object) -> str:
    """Reject an invalid Connection name before any database work."""
    if not isinstance(name, str) or not name.strip() or len(name) > _MAX_CONNECTION_NAME_LENGTH:
        raise InvalidCommandError(
            "Connection name must be a non-blank string of at most "
            f"{_MAX_CONNECTION_NAME_LENGTH} characters"
        )
    return name


def _validate_session_capacity(value: object) -> int:
    """Reject an invalid admission capacity before any database work.

    The capacity is Owner-configured OpenOrc admission control: a strictly
    positive integer, where a boolean is never an integer (the domain and
    the durable CHECK constraint agree).
    """
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise InvalidCommandError(
            "Connection session capacity must be a positive integer "
            "(Owner-configured admission control)"
        )
    return value


def _validate_enabled(value: object) -> bool:
    """Reject a non-boolean eligibility switch before any database work."""
    if not isinstance(value, bool):
        raise InvalidCommandError("Connection enabled must be a boolean")
    return value


def _validate_adapter_type(value: object) -> AdapterType:
    """Reject any adapter type other than the one supported v1 adapter."""
    if value is not AdapterType.CLINE:
        raise InvalidCommandError(
            "the connection adapter type must be 'cline' (the only supported v1 adapter)"
        )
    return value


def _canonical_absolute_path_failure(value: object) -> str | None:
    """Return a safe failure description for a non-canonical absolute path.

    The repository base is an existing runtime-side absolute path (a parent
    directory, not a shared clone); Core validates shape only and never
    probes any filesystem. A canonical absolute path starts with ``/`` and
    carries no empty, ``.``, or ``..`` segments — the same established rule
    the construction/selection boundaries enforce.
    """
    if not isinstance(value, str) or not value.strip():
        return "must be a non-empty absolute runtime-side path"
    if not value.startswith("/"):
        return "must be an absolute runtime-side path"
    segments = value.split("/")[1:]
    if any(segment in ("", ".", "..") for segment in segments):
        return "must be canonical (no empty, '.', or '..' path segments)"
    return None


def _build_safe_config(repository_base: object, control_endpoint: object) -> dict[str, object]:
    """Build the v1 safe-config mapping from the commanded Owner fields.

    The v1 Cline Connection safe_config carries exactly the permitted
    non-secret fields; an absent field is simply absent (the
    stored-but-not-configurable / stored-but-not-attachable states). Both
    values are validated before anything persists, and the validated values
    are preserved verbatim (#188: the parse never normalizes or rewrites).
    """
    config: dict[str, object] = {}
    if repository_base is not None:
        failure = _canonical_absolute_path_failure(repository_base)
        if failure is not None:
            raise InvalidCommandError(f"the connection repository base {failure}")
        config[CONNECTION_REPOSITORY_BASE_CONFIG_KEY] = repository_base
    if control_endpoint is not None:
        config[CONNECTION_CONTROL_ENDPOINT_CONFIG_KEY] = control_endpoint
        try:
            resolve_connection_control_endpoint(config)
        except ConnectionControlEndpointError as error:
            # The adapter's rejection messages are safe by contract: they
            # name the failing aspect and never echo the configured value.
            raise InvalidCommandError(str(error)) from None
    return config


def _validate_persisted_safe_config(connection: Connection) -> None:
    """Fail closed on persisted safe config that violates the contracts.

    A read exposes a stored value only after it re-passes the same
    established shape/URL contracts the mutation boundary enforces, so a
    legacy or otherwise malformed persisted value can never be projected as
    approved safe configuration. Unknown keys are never projected and are
    not a read failure. A violation is a durable-state invariant failure:
    ``ConflictError`` with a fixed safe message that never carries any
    stored value, plus a safe warning naming no configuration content.
    """
    if CONNECTION_REPOSITORY_BASE_CONFIG_KEY in connection.safe_config:
        failure = _canonical_absolute_path_failure(
            connection.safe_config[CONNECTION_REPOSITORY_BASE_CONFIG_KEY]
        )
        if failure is not None:
            logger.warning(
                "connection read rejected a persisted repository base that violates "
                "the established safe-configuration contract"
            )
            raise ConflictError(_UNSAFE_STORED_CONFIG_MESSAGE)
    if CONNECTION_CONTROL_ENDPOINT_CONFIG_KEY in connection.safe_config:
        try:
            resolve_connection_control_endpoint(connection.safe_config)
        except ConnectionControlEndpointError:
            logger.warning(
                "connection read rejected a persisted control endpoint that violates "
                "the established safe-configuration contract"
            )
            raise ConflictError(_UNSAFE_STORED_CONFIG_MESSAGE) from None


def _same_configuration(
    connection: Connection,
    *,
    name: str,
    safe_config: dict[str, object],
    session_capacity: int,
    enabled: bool,
) -> bool:
    """Whether the commanded full record is identical to the stored one.

    The stored safe config is the canonical form the domain normalized and
    the commanded mapping was built through the same validation, so a plain
    value comparison is the honest change fact.
    """
    return (
        connection.name == name
        and dict(connection.safe_config) == safe_config
        and connection.session_capacity == session_capacity
        and connection.enabled == enabled
    )


def list_workspace_connections(
    pool: DatabasePool, *, profile_id: UUID, workspace_id: UUID
) -> list[Connection]:
    """List the Connections of the authorized Workspace, failing closed.

    Ownership is established first; the owner-scoped SQL filter is the
    isolation boundary. Every projected row re-validates its persisted safe
    config at the read boundary, so the listing exposes only values the
    established contracts accept — one malformed persisted row fails the
    whole listing closed rather than surfacing prohibited material as
    approved safe configuration. Read-only, like every other configuration
    read: no account-mutation barrier is composed.
    """
    with application_span(_SERVICE_TRACER_SCOPE, _LIST_SPAN_NAME) as span:
        annotate_span(span, operation=_LIST_SPAN_NAME, workspace_id=str(workspace_id))
        _require_uuid(workspace_id, "workspace id")
        require_profile_workspace(pool, profile_id=profile_id, workspace_id=workspace_id)
        connections = connection_records.list_workspace_connections(pool, workspace_id=workspace_id)
        for connection in connections:
            _validate_persisted_safe_config(connection)
        return connections


def get_workspace_connection(
    pool: DatabasePool, *, profile_id: UUID, workspace_id: UUID, connection_id: UUID
) -> Connection:
    """Read one Connection of the authorized Workspace, failing closed.

    A missing or cross-Workspace Connection is uniformly ``NotFoundError``
    so probing internal UUIDs across Workspaces is indistinguishable from
    addressing something absent. The stored safe config re-validates at the
    read boundary before the Connection is returned.
    """
    with application_span(_SERVICE_TRACER_SCOPE, _GET_SPAN_NAME) as span:
        annotate_span(span, operation=_GET_SPAN_NAME, workspace_id=str(workspace_id))
        _require_uuid(workspace_id, "workspace id")
        _require_uuid(connection_id, "connection id")
        require_profile_workspace(pool, profile_id=profile_id, workspace_id=workspace_id)
        connection = connection_records.get_connection(pool, connection_id)
        if connection is None or connection.workspace_id != workspace_id:
            # Uniform fail-closed subject outcome, never a probing oracle.
            raise NotFoundError("the requested connection is not available in this workspace")
        annotate_span(span, connection_id=str(connection_id))
        _validate_persisted_safe_config(connection)
        return connection


def create_workspace_connection(
    pool: DatabasePool,
    *,
    profile_id: UUID,
    workspace_id: UUID,
    adapter_type: AdapterType,
    name: str,
    repository_base: str | None = None,
    control_endpoint: str | None = None,
    session_capacity: int | None = None,
    enabled: bool = True,
) -> Connection:
    """Create one Connection in the authorized Workspace, failing closed.

    Command shape is validated before any authorization or database work.
    The account-wide Owner-mutation barrier (issue #97) is the first lock
    acquisition inside the one short composed transaction. The unprovided
    capacity takes the durable default (issue #200); unprovided safe-config
    fields remain unset. No ``auth_reference`` is installed at creation —
    credential setup is the issue #55 service's separate Owner operation.
    """
    with application_span(_SERVICE_TRACER_SCOPE, _CREATE_SPAN_NAME) as span:
        annotate_span(span, operation=_CREATE_SPAN_NAME, workspace_id=str(workspace_id))
        _require_uuid(workspace_id, "workspace id")
        _validate_adapter_type(adapter_type)
        _validate_connection_name(name)
        capacity = (
            _DEFAULT_SESSION_CAPACITY
            if session_capacity is None
            else _validate_session_capacity(session_capacity)
        )
        _validate_enabled(enabled)
        safe_config = _build_safe_config(repository_base, control_endpoint)
        with composed_transaction(pool) as transaction_pool:
            # The account-wide Owner-mutation barrier first (issue #97): the
            # Profile FOR KEY SHARE read is the first lock acquisition and
            # fails closed while an account deletion attempt is unresolved.
            require_account_operational(transaction_pool, profile_id=profile_id)
            require_profile_workspace(
                transaction_pool, profile_id=profile_id, workspace_id=workspace_id
            )
            connection = connection_records.create_connection(
                transaction_pool,
                workspace_id=workspace_id,
                adapter=adapter_type,
                name=name,
                safe_config=safe_config,
                session_capacity=capacity,
                enabled=enabled,
            )
        annotate_span(span, connection_id=str(connection.id))
        return connection


def replace_workspace_connection(
    pool: DatabasePool,
    *,
    profile_id: UUID,
    workspace_id: UUID,
    connection_id: UUID,
    name: str,
    repository_base: str | None,
    control_endpoint: str | None,
    session_capacity: int,
    enabled: bool,
) -> Connection:
    """Replace one Connection's full mutable Owner record, failing closed.

    Full-record replacement semantics: the commanded record carries every
    mutable field's desired value; the opaque ``auth_reference`` is not a
    command input and is preserved verbatim from the row-locked read.
    Identity, Workspace scope, and adapter type are never touched, and no
    existing Task/session binding or role binding is rewritten. A same-value
    write is a no-op (no row churn, no audit); an actual change records one
    ``WORKSPACE_CONFIGURATION_CHANGED`` event inside the same composed
    transaction.
    """
    with application_span(_SERVICE_TRACER_SCOPE, _REPLACE_SPAN_NAME) as span:
        annotate_span(span, operation=_REPLACE_SPAN_NAME, workspace_id=str(workspace_id))
        _require_uuid(workspace_id, "workspace id")
        _require_uuid(connection_id, "connection id")
        _validate_connection_name(name)
        capacity = _validate_session_capacity(session_capacity)
        _validate_enabled(enabled)
        safe_config = _build_safe_config(repository_base, control_endpoint)
        with composed_transaction(pool) as transaction_pool:
            # The account-wide Owner-mutation barrier first (issue #97).
            require_account_operational(transaction_pool, profile_id=profile_id)
            require_profile_workspace(
                transaction_pool, profile_id=profile_id, workspace_id=workspace_id
            )
            current = connection_records.get_connection_for_update(transaction_pool, connection_id)
            if current is None or current.workspace_id != workspace_id:
                # Uniform fail-closed subject outcome under the row lock,
                # never a probing oracle.
                raise NotFoundError("the requested connection is not available in this workspace")
            annotate_span(span, connection_id=str(connection_id))
            if _same_configuration(
                current,
                name=name,
                safe_config=safe_config,
                session_capacity=capacity,
                enabled=enabled,
            ):
                # Identical stored configuration: the row was locked but not
                # updated, so the locked read is the honest result of this
                # no-op write and no audit event is fabricated.
                return current
            updated = connection_records.update_connection(
                transaction_pool,
                connection_id,
                name=name,
                safe_config=safe_config,
                session_capacity=capacity,
                enabled=enabled,
                auth_reference=current.auth_reference,
            )
            if updated is None:
                # Unreachable under the row lock held in this same
                # transaction; the uniform subject outcome, never a guess.
                raise NotFoundError("the requested connection is not available in this workspace")
            event_coordination.record_connection_configuration_changed_event(
                transaction_pool,
                workspace_id=workspace_id,
                actor=event_coordination.owner_actor(profile_id),
                connection_id=connection_id,
            )
            return updated
    raise AssertionError("unreachable: the replacement flow returns inside its transaction")
