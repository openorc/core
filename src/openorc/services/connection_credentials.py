"""Connection control-credential application service (issue #55).

The secure credential boundary for OpenOrc-owned Agent Runtime
control-endpoint credentials: Owner-authorized configure/rotate over the
opaque ``Connection.auth_reference`` with encrypted Supabase Vault storage
(``persistence/runtime_control_secrets.py``), plus the trusted resolution
path OpenOrc's own runtime invocation uses when it must authenticate to the
Agent Runtime.

Authorization composition: configure and rotate take the authenticated
Profile UUID from the #52 authentication boundary and compose ownership
through ``require_profile_workspace`` (#53) inside the composition — the
addressed Connection must belong to the authorized Workspace, and a missing
or cross-Workspace Connection is uniformly ``NotFoundError`` (the #53
anti-probing rule). Resolution is the background path: an OpenOrc worker
acting on an already-authorized workflow needs no Owner authorization, but
the durable Workspace scope is always enforced, and resolution fails closed
on disabled connections, missing references, unrecognized reference formats,
and dangling secret pointers.

Validation precedes every mutation: the credential must be a non-empty
string, rejected before authorization reads, before the composed
transaction, and before any Vault call — an invalid command can never
persist an empty credential. Every non-empty value is preserved verbatim (no
stripping or normalization; credential bytes are meaningful).

Atomicity (#51): the Vault secret write and the ``Connection.auth_reference``
mutation compose in one short ``composed_transaction`` over a row-locked
Connection read, so a failed transaction commits neither side — no orphaned
Vault secret and no reference to a secret that did not commit. Rotation
updates the existing Vault secret in place, so the opaque reference (and the
Vault UUID behind it) stays stable and the Connection row is deliberately
untouched.

Secret-bearing boundary: ``ControlEndpointSecret`` is the narrow in-memory
carrier of the decrypted value. Its ordinary representation is redacted, and
its value is reachable only through ``secret_value()``, reserved for the
trusted adapter/authentication call that needs it. The value is never part of
domain dataclasses, WorkflowEvent context, configuration snapshots, return
DTOs, logs, or persistence, and every application error raised along these
paths contains only safe content.
"""

from __future__ import annotations

import logging
from uuid import UUID

from openorc.domain.connections import Connection
from openorc.observability import annotate_span, application_span
from openorc.persistence import runtime_control_secrets
from openorc.persistence.connections import (
    get_connection,
    get_connection_for_update,
    set_connection_auth_reference,
)
from openorc.persistence.pool import DatabasePool
from openorc.persistence.runtime_control_secrets import RuntimeControlSecretReferenceError
from openorc.services.errors import ConflictError, InvalidCommandError, NotFoundError
from openorc.services.profile_lifecycle_guard import require_account_operational
from openorc.services.transaction_composition import composed_transaction
from openorc.services.workspace_authorization import require_profile_workspace

__all__ = [
    "ControlEndpointSecret",
    "configure_connection_credential",
    "resolve_connection_runtime_credential",
    "rotate_connection_credential",
]

# Application-service span boundaries (issues #108/#109): configure, rotate,
# and trusted resolution each open one span at their use-case boundary. The
# credential value and its opaque reference have no supported path into
# telemetry; the safe vocabulary carries the Workspace/Connection identity.
_CONNECTION_CREDENTIALS_TRACER_SCOPE = "openorc.services.connection_credentials"
_CONFIGURE_SPAN_NAME = "connection_credentials.configure_connection_credential"
_ROTATE_SPAN_NAME = "connection_credentials.rotate_connection_credential"
_RESOLVE_SPAN_NAME = "connection_credentials.resolve_connection_runtime_credential"

logger = logging.getLogger(__name__)


class ControlEndpointSecret:
    """Narrow secret-bearing value for one Connection's control credential.

    The decrypted credential necessarily exists briefly in backend memory
    when OpenOrc authenticates to an Agent Runtime. This type is the explicit
    in-memory boundary around that value:

    - ordinary representation is redacted: ``repr()`` and ``str()`` never
      expose the value, so it cannot leak through logs, debugging output, or
      exception formatting;
    - the value is reachable only through :meth:`secret_value`, which is
      reserved for the trusted adapter/authentication call that needs it;
    - the type is never part of domain dataclasses, WorkflowEvent context,
      configuration snapshots, return DTOs, or persistence.
    """

    __slots__ = ("_value",)

    def __init__(self, value: str) -> None:
        self._value = value

    def secret_value(self) -> str:
        """Return the credential value for the trusted adapter authentication call."""
        return self._value

    def __repr__(self) -> str:
        return "ControlEndpointSecret(<redacted>)"

    def __str__(self) -> str:
        return "ControlEndpointSecret(<redacted>)"


def _require_credential_value(credential: object) -> str:
    """Validate a credential command value before any mutation (fail closed).

    The credential must be a string and must not be exactly empty; every
    non-empty value is accepted verbatim — no stripping or normalization, a
    credential's bytes are meaningful. Rejection happens before authorization
    reads, before the composed transaction, and before any Vault call, so an
    invalid command can never persist an empty credential.
    """
    if not isinstance(credential, str) or credential == "":
        raise InvalidCommandError("a Connection control credential must be a non-empty string")
    return credential


def _require_owner_authorized_connection(
    transaction_pool: DatabasePool, *, profile_id: UUID, workspace_id: UUID, connection_id: UUID
) -> Connection:
    """Row-locked, Owner-authorized Connection read shared by configure/rotate.

    Composes the #53 ownership gate, then takes the deliberate row lock and
    validates the Connection's durable Workspace scope under it. A missing or
    cross-Workspace Connection is uniformly ``NotFoundError`` so probing
    internal UUIDs across Workspaces is indistinguishable from addressing
    something absent.
    """
    require_profile_workspace(transaction_pool, profile_id=profile_id, workspace_id=workspace_id)
    connection = get_connection_for_update(transaction_pool, connection_id)
    if connection is None or connection.workspace_id != workspace_id:
        raise NotFoundError("the requested connection is not available in this workspace")
    return connection


def configure_connection_credential(
    pool: DatabasePool,
    *,
    profile_id: UUID,
    workspace_id: UUID,
    connection_id: UUID,
    credential: str,
) -> Connection:
    """Configure the control-endpoint credential of one Connection (Owner-authorized).

    The credential is validated before any mutation. Inside one short
    ``composed_transaction`` the operation composes the Owner authorization
    gate, a row-locked Connection read, the Vault secret creation, and the
    opaque ``auth_reference`` install, so a failure commits neither side — no
    orphaned Vault secret and no reference to a secret that did not commit.
    Configuring over an existing credential is a conflict: rotate instead.
    """
    with application_span(_CONNECTION_CREDENTIALS_TRACER_SCOPE, _CONFIGURE_SPAN_NAME) as span:
        annotate_span(
            span,
            operation=_CONFIGURE_SPAN_NAME,
            workspace_id=str(workspace_id),
            connection_id=str(connection_id),
        )
        _require_credential_value(credential)
        with composed_transaction(pool) as transaction_pool:
            # The account-wide Owner-mutation barrier first (issue #97): the
            # Profile FOR KEY SHARE read is the first lock acquisition, before
            # any Connection row lock, and fails closed while an account
            # deletion attempt is unresolved.
            require_account_operational(transaction_pool, profile_id=profile_id)
            connection = _require_owner_authorized_connection(
                transaction_pool,
                profile_id=profile_id,
                workspace_id=workspace_id,
                connection_id=connection_id,
            )
            if not connection.enabled:
                # Disconnection is the revocation barrier: a disabled
                # Connection can never acquire a new Vault secret (issue #97
                # defense in depth — the account-wide deletion barrier is the
                # primary guard).
                raise ConflictError(
                    "a disabled connection cannot be configured with a new credential"
                )
            if connection.auth_reference is not None:
                raise ConflictError(
                    "the connection already has a configured credential; rotate it instead"
                )
            secret_id = runtime_control_secrets.create_runtime_control_secret(
                transaction_pool,
                secret=credential,
                connection_id=connection_id,
                workspace_id=workspace_id,
            )
            updated = set_connection_auth_reference(
                transaction_pool,
                connection_id,
                auth_reference=runtime_control_secrets.encode_connection_auth_reference(secret_id),
            )
            if updated is None:
                # Unreachable while the composition holds the row lock; treated
                # as failure so the created Vault secret rolls back with the
                # write.
                raise NotFoundError("the requested connection is not available in this workspace")
    return updated


def rotate_connection_credential(
    pool: DatabasePool,
    *,
    profile_id: UUID,
    workspace_id: UUID,
    connection_id: UUID,
    credential: str,
) -> Connection:
    """Replace the control-endpoint credential of one Connection (Owner-authorized).

    Rotation updates the existing Vault secret in place, so the opaque
    ``auth_reference`` — and the Vault UUID behind it — remains stable, and
    the Connection row is deliberately untouched. The credential is validated
    before any mutation; a Connection with no configured credential, an
    unrecognized reference format, or a dangling secret pointer fails closed
    inside the composition with nothing written.
    """
    with application_span(_CONNECTION_CREDENTIALS_TRACER_SCOPE, _ROTATE_SPAN_NAME) as span:
        annotate_span(
            span,
            operation=_ROTATE_SPAN_NAME,
            workspace_id=str(workspace_id),
            connection_id=str(connection_id),
        )
        _require_credential_value(credential)
        with composed_transaction(pool) as transaction_pool:
            # The account-wide Owner-mutation barrier first (issue #97): the
            # Profile FOR KEY SHARE read precedes any Connection row lock.
            require_account_operational(transaction_pool, profile_id=profile_id)
            connection = _require_owner_authorized_connection(
                transaction_pool,
                profile_id=profile_id,
                workspace_id=workspace_id,
                connection_id=connection_id,
            )
            if connection.auth_reference is None:
                raise ConflictError("the connection has no configured credential to rotate")
            try:
                secret_id = runtime_control_secrets.parse_connection_auth_reference(
                    connection.auth_reference
                )
            except RuntimeControlSecretReferenceError as exc:
                # Invariant state: configure writes only recognized v1
                # references, so an unrecognized reference is a durable-state
                # inconsistency worth operational visibility (issue #109).
                logger.warning(
                    "connection credential rotation rejected an unrecognized auth reference"
                )
                raise ConflictError(
                    "the connection's credential reference is not a recognized v1 reference"
                ) from exc
            if not runtime_control_secrets.update_runtime_control_secret(
                transaction_pool, secret_id=secret_id, secret=credential
            ):
                # Dangling invariant: the opaque reference points at a secret
                # that does not exist. Fixed safe message — never the reference
                # value or any credential material.
                logger.warning("connection credential rotation rejected a dangling auth reference")
                raise ConflictError(
                    "the connection's credential reference does not point at an existing secret"
                )
    # The Connection row is deliberately unchanged by rotation: the reference
    # stays stable, so the locked row read is the post-rotation state.
    return connection


def resolve_connection_runtime_credential(
    pool: DatabasePool, *, workspace_id: UUID, connection_id: UUID
) -> tuple[Connection, ControlEndpointSecret]:
    """Resolve one Connection's control credential for the trusted runtime path.

    The runtime-adapter invocation boundary: background OpenOrc workers
    acting on an already-authorized workflow need no Owner authorization, but
    the durable Workspace scope is always enforced — a missing or
    cross-Workspace Connection is uniformly ``NotFoundError``. Resolution
    fails closed with ``ConflictError`` on a disabled Connection, a missing
    reference, an unrecognized reference format, or a dangling secret
    pointer. The decrypted value is returned only wrapped in
    :class:`ControlEndpointSecret` — never in an ordinary return or
    configuration object.
    """
    with application_span(_CONNECTION_CREDENTIALS_TRACER_SCOPE, _RESOLVE_SPAN_NAME) as span:
        annotate_span(
            span,
            operation=_RESOLVE_SPAN_NAME,
            workspace_id=str(workspace_id),
            connection_id=str(connection_id),
        )
        connection = get_connection(pool, connection_id)
        if connection is None or connection.workspace_id != workspace_id:
            raise NotFoundError("the requested connection is not available in this workspace")
        if not connection.enabled:
            raise ConflictError("the connection is disabled; its credential cannot be used")
        if connection.auth_reference is None:
            raise ConflictError("the connection has no configured credential")
        try:
            secret_id = runtime_control_secrets.parse_connection_auth_reference(
                connection.auth_reference
            )
        except RuntimeControlSecretReferenceError as exc:
            # Invariant: only configure writes references, so an unrecognized
            # reference is a durable-state inconsistency (issue #109).
            logger.warning(
                "connection credential resolution rejected an unrecognized auth reference"
            )
            raise ConflictError(
                "the connection's credential reference is not a recognized v1 reference"
            ) from exc
        secret = runtime_control_secrets.read_runtime_control_secret(pool, secret_id=secret_id)
        if secret is None:
            # Dangling invariant: the reference points at a nonexistent secret.
            logger.warning("connection credential resolution rejected a dangling auth reference")
            raise ConflictError(
                "the connection's credential reference does not point at an existing secret"
            )
        return connection, ControlEndpointSecret(secret)
