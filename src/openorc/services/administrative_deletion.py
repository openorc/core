"""Authorized administrative deletion application services (issue #97).

The Owner-authorized aggregate administration above the Phase 1 destructive
persistence primitives: disconnect an Agent Runtime Connection with its
secure-secret cleanup, hard-delete an unreferenced Connection, purge an
archived Task attempt, delete a Repository mapping / Project / Workspace.
The permanent account deletion lives in
:mod:`openorc.services.account_lifecycle`; this module owns the ordinary
aggregate-deletion use cases.

Every operation is transport-neutral — no FastAPI/RQ objects cross this
boundary — and composes the same fail-closed authorization sequence: the
account-wide Owner-mutation barrier
(:func:`require_account_operational`, whose Profile ``FOR KEY SHARE`` read is
the FIRST lock acquisition) then the #53 Workspace ownership gate, and only
then the destructive effect. A missing or cross-Workspace subject is
uniformly ``NotFoundError`` so probing internal UUIDs across Workspaces is
indistinguishable from addressing something absent.

External-authority boundary: deleting OpenOrc state never deletes, closes,
reverts, or mutates GitHub repositories, issues, branches, commits, pull
requests, checks/statuses, runtime-owned provider/MCP/Git/tool credentials,
or runtime filesystem/sandbox state. Repository/Project/Workspace deletion
removes OpenOrc's mapping/control state only; no GitHub or runtime adapter
call exists on any of these paths, and this module imports no adapter.

Connection credentials: disconnect/delete additionally remove ONLY the
OpenOrc-owned control credential managed by issue #55 — the Vault secret
referenced by the opaque ``auth_reference`` — inside the same short
``composed_transaction`` (#51) as the row mutation, with every Connection
row-locked before its current reference is inspected (the same locks
configure/rotate use, so a concurrent rotation can neither escape cleanup nor
repoint after the read). Malformed and dangling references fail closed: a
cleanup that cannot be reconciled never reports success.

Workspace deletion takes the explicit Workspace-root ``FOR UPDATE`` before
enumerating Connections: a concurrent child INSERT (whose foreign-key check
takes a conflicting ``FOR KEY SHARE`` lock on the parent row) blocks until
the aggregate deletion commits, so no Connection can enter the aggregate
after the cleanup set is established — a Workspace with zero Connections
still carries the root lock. Project/Repository deletion touches no
Workspace-level Connection credential.

Normal Task cancellation is not an administrative purge: only archived
(terminal) attempts may be purged, and the phase-1 primitive rejects a
current Task; callers use the normal workflow cancellation first. The purge
remains internal-state deletion only and never touches the backing GitHub
issue/branch/PR.
"""

from __future__ import annotations

import logging
from uuid import UUID

from psycopg import errors as psycopg_errors

from openorc.domain.connections import Connection
from openorc.domain.ownership import Project, Repository, Workspace
from openorc.domain.tasks import Task, TaskDomainError
from openorc.observability import (
    annotate_span,
    application_span,
)
from openorc.persistence import runtime_control_secrets
from openorc.persistence.connections import (
    get_connection_for_update,
    list_workspace_connections_for_update,
)
from openorc.persistence.deletion import (
    delete_connection,
    delete_project,
    delete_repository,
    delete_workspace,
    disconnect_connection,
    purge_archived_task,
)
from openorc.persistence.ownership import get_workspace_for_update
from openorc.persistence.pool import DatabasePool
from openorc.persistence.runtime_control_secrets import RuntimeControlSecretReferenceError
from openorc.services.errors import ConflictError, NotFoundError
from openorc.services.profile_lifecycle_guard import require_account_operational
from openorc.services.transaction_composition import composed_transaction
from openorc.services.workspace_authorization import (
    require_profile_workspace,
    require_workspace_project,
    require_workspace_repository,
    require_workspace_task,
)

__all__ = [
    "delete_connection_record",
    "delete_project_record",
    "delete_repository_record",
    "delete_workspace_record",
    "disconnect_connection_credential",
    "purge_archived_task_record",
]

# Application-service span boundaries (issues #108/#109): every public
# use-case operation of this module opens one span at the established service
# boundary, attaching only the safe attribute vocabulary. Guards, scope
# resolvers, and persistence calls run inside the use-case span.
_SERVICE_TRACER_SCOPE = "openorc.services.administrative_deletion"
_DISCONNECT_SPAN_NAME = "administrative_deletion.disconnect_connection_credential"
_DELETE_CONNECTION_SPAN_NAME = "administrative_deletion.delete_connection_record"
_PURGE_TASK_SPAN_NAME = "administrative_deletion.purge_archived_task_record"
_DELETE_REPOSITORY_SPAN_NAME = "administrative_deletion.delete_repository_record"
_DELETE_PROJECT_SPAN_NAME = "administrative_deletion.delete_project_record"
_DELETE_WORKSPACE_SPAN_NAME = "administrative_deletion.delete_workspace_record"

logger = logging.getLogger(__name__)


def _cleanup_connection_credential(transaction_pool, connection: Connection) -> None:
    """Delete the OpenOrc-owned Vault secret referenced by a locked Connection.

    Called under the caller's composed transaction with the Connection row
    lock held (by :func:`get_connection_for_update` or a locked enumeration),
    so the reference it inspects is the same one the follow-up mutation
    commits against. A Connection with no OpenOrc-owned auth reference has no
    secret to clean. Malformed and dangling references fail closed through
    the typed application vocabulary — credential cleanup never reports
    success when the persisted reference cannot be reconciled — and the
    surrounding transaction rolls back so no partial cleanup commits.
    """
    if connection.auth_reference is None:
        return
    try:
        secret_id = runtime_control_secrets.parse_connection_auth_reference(
            connection.auth_reference
        )
    except RuntimeControlSecretReferenceError as exc:
        # Invariant: only credential configuration writes references, so an
        # unrecognized reference is a durable-state inconsistency worth
        # operational visibility. Fixed safe message only.
        logger.warning("administrative credential cleanup rejected an unrecognized auth reference")
        raise ConflictError(
            "the connection's credential reference is not a recognized v1 reference"
        ) from exc
    if not runtime_control_secrets.delete_runtime_control_secret(
        transaction_pool, secret_id=secret_id
    ):
        logger.warning("administrative credential cleanup rejected a dangling auth reference")
        raise ConflictError(
            "the connection's credential reference does not point at an existing secret"
        )


def disconnect_connection_credential(
    pool: DatabasePool, *, profile_id: UUID, workspace_id: UUID, connection_id: UUID
) -> Connection:
    """Disconnect one Connection and remove its OpenOrc-owned Vault secret.

    One atomic configuration change in a single short transaction: the
    account-wide Owner-mutation barrier, the #53 ownership gate, a row-locked
    Connection read (serialized with configure/rotate), the referenced Vault
    secret's deletion, and the Phase 1 disconnect semantics
    (``enabled = false`` with ``auth_reference = null``). A Connection with no
    configured credential disconnects without touching Vault. Malformed or
    dangling references fail closed with nothing written. Disconnect never
    deletes the Connection row, and runtime-owned credentials, provider
    configuration, and filesystem state are never touched.
    """
    with application_span(_SERVICE_TRACER_SCOPE, _DISCONNECT_SPAN_NAME) as span:
        annotate_span(
            span,
            operation=_DISCONNECT_SPAN_NAME,
            workspace_id=str(workspace_id),
            connection_id=str(connection_id),
        )
        with composed_transaction(pool) as transaction_pool:
            require_account_operational(transaction_pool, profile_id=profile_id)
            require_profile_workspace(
                transaction_pool, profile_id=profile_id, workspace_id=workspace_id
            )
            connection = get_connection_for_update(transaction_pool, connection_id)
            if connection is None or connection.workspace_id != workspace_id:
                raise NotFoundError("the requested connection is not available in this workspace")
            _cleanup_connection_credential(transaction_pool, connection)
            disconnected = disconnect_connection(transaction_pool, connection_id)
            if disconnected is None:
                # Unreachable while the composition holds the row lock; treated
                # as failure so nothing partial commits.
                raise NotFoundError("the requested connection is not available in this workspace")
    return disconnected


def delete_connection_record(
    pool: DatabasePool, *, profile_id: UUID, workspace_id: UUID, connection_id: UUID
) -> Connection:
    """Hard-delete an unreferenced Connection with its secure-secret cleanup.

    The OpenOrc-owned Vault secret cleanup runs first, then the restrictive
    Phase 1 delete primitive forces its historical-reference constraints
    immediate: a Connection still referenced by historical TaskAgentSessions
    or workflow role bindings is rejected — history is never cascaded away to
    make the delete succeed — and that rejection surfaces as a typed
    conflict. Zero GitHub/runtime adapter calls.
    """
    with application_span(_SERVICE_TRACER_SCOPE, _DELETE_CONNECTION_SPAN_NAME) as span:
        annotate_span(
            span,
            operation=_DELETE_CONNECTION_SPAN_NAME,
            workspace_id=str(workspace_id),
            connection_id=str(connection_id),
        )
        try:
            with composed_transaction(pool) as transaction_pool:
                require_account_operational(transaction_pool, profile_id=profile_id)
                require_profile_workspace(
                    transaction_pool, profile_id=profile_id, workspace_id=workspace_id
                )
                connection = get_connection_for_update(transaction_pool, connection_id)
                if connection is None or connection.workspace_id != workspace_id:
                    raise NotFoundError(
                        "the requested connection is not available in this workspace"
                    )
                _cleanup_connection_credential(transaction_pool, connection)
                deleted = delete_connection(transaction_pool, connection_id)
                if deleted is None:
                    # Unreachable while the composition holds the row lock.
                    raise NotFoundError(
                        "the requested connection is not available in this workspace"
                    )
        except psycopg_errors.ForeignKeyViolation as exc:
            # The Phase 1 primitive's forced-immediate constraint check: the
            # Connection is still referenced by historical workflow state.
            raise ConflictError(
                "the connection is still referenced by historical workflow state "
                "and cannot be deleted"
            ) from exc
    return deleted


def purge_archived_task_record(
    pool: DatabasePool, *, profile_id: UUID, workspace_id: UUID, task_id: UUID
) -> Task:
    """Purge one archived Task attempt with its complete OpenOrc aggregate.

    Only archived (terminal) attempts may be purged through the Phase 1
    primitive: a current (non-archived) Task is rejected as a conflict —
    normal Task cancellation is the caller's route, never this purge. The
    purge is internal-state deletion only: the backing GitHub issue, branch,
    and PR are never touched, and no external call exists on the path.
    """
    with application_span(_SERVICE_TRACER_SCOPE, _PURGE_TASK_SPAN_NAME) as span:
        annotate_span(
            span,
            operation=_PURGE_TASK_SPAN_NAME,
            workspace_id=str(workspace_id),
            task_id=str(task_id),
        )
        try:
            with composed_transaction(pool) as transaction_pool:
                require_account_operational(transaction_pool, profile_id=profile_id)
                require_workspace_task(
                    transaction_pool,
                    profile_id=profile_id,
                    workspace_id=workspace_id,
                    task_id=task_id,
                )
                purged = purge_archived_task(transaction_pool, task_id)
                if purged is None:
                    # The Task vanished between the scope resolution and the
                    # purge; the uniform not-found outcome applies.
                    raise NotFoundError("the requested task is not available in this workspace")
        except TaskDomainError as exc:
            # The Phase 1 purge primitive rejects a current (non-archived)
            # Task: normal workflow cancellation comes first.
            raise ConflictError(
                "the task is not archived; cancel the task before purging it"
            ) from exc
    return purged


def delete_repository_record(
    pool: DatabasePool, *, profile_id: UUID, workspace_id: UUID, repository_id: UUID
) -> Repository:
    """Delete one OpenOrc Repository mapping within the authorized Workspace.

    Removes OpenOrc's own persisted mapping for an external GitHub repository
    — never the GitHub repository, its issues, branches, commits, pull
    requests, or checks. No GitHub or runtime adapter call exists on this
    path, and no Workspace-level Connection credential is touched.
    """
    with application_span(_SERVICE_TRACER_SCOPE, _DELETE_REPOSITORY_SPAN_NAME) as span:
        annotate_span(
            span,
            operation=_DELETE_REPOSITORY_SPAN_NAME,
            workspace_id=str(workspace_id),
        )
        with composed_transaction(pool) as transaction_pool:
            require_account_operational(transaction_pool, profile_id=profile_id)
            require_workspace_repository(
                transaction_pool,
                profile_id=profile_id,
                workspace_id=workspace_id,
                repository_id=repository_id,
            )
            deleted = delete_repository(transaction_pool, repository_id)
            if deleted is None:
                # The subject vanished between the scope check and the
                # deletion; the uniform not-found outcome covers it.
                raise NotFoundError("the requested repository is not available in this workspace")
    return deleted


def delete_project_record(
    pool: DatabasePool, *, profile_id: UUID, workspace_id: UUID, project_id: UUID
) -> Project:
    """Delete one Project (with its Task subtrees) within the authorized Workspace.

    Removes OpenOrc's Project aggregate only: the Project's Repositories and
    their Task subtrees go with it, while the Workspace, sibling Projects,
    Workspace-level Connections, and their credentials are untouched. No
    GitHub or runtime adapter call exists on this path.
    """
    with application_span(_SERVICE_TRACER_SCOPE, _DELETE_PROJECT_SPAN_NAME) as span:
        annotate_span(
            span,
            operation=_DELETE_PROJECT_SPAN_NAME,
            workspace_id=str(workspace_id),
        )
        with composed_transaction(pool) as transaction_pool:
            require_account_operational(transaction_pool, profile_id=profile_id)
            require_workspace_project(
                transaction_pool,
                profile_id=profile_id,
                workspace_id=workspace_id,
                project_id=project_id,
            )
            deleted = delete_project(transaction_pool, project_id)
            if deleted is None:
                raise NotFoundError("the requested project is not available in this workspace")
    return deleted


def delete_workspace_record(
    pool: DatabasePool, *, profile_id: UUID, workspace_id: UUID
) -> Workspace:
    """Delete one Workspace with every OpenOrc-owned Vault secret it references.

    The explicit Workspace-root ``FOR UPDATE`` (re-validated under the lock)
    is held across the whole composition: a concurrent child INSERT into any
    Workspace-owned table takes a conflicting ``FOR KEY SHARE`` lock on this
    row during its foreign-key check, so no Connection can enter the
    aggregate after the cleanup set is established. Every enumerated
    Connection is row-locked before its current ``auth_reference`` is
    inspected, and every referenced OpenOrc-owned Vault secret is deleted in
    the same transaction as the Phase 1 aggregate deletion. Project and
    Repository deletion elsewhere in this module never touch these
    Workspace-level credentials. No GitHub or runtime adapter call exists on
    this path.
    """
    with application_span(_SERVICE_TRACER_SCOPE, _DELETE_WORKSPACE_SPAN_NAME) as span:
        annotate_span(
            span,
            operation=_DELETE_WORKSPACE_SPAN_NAME,
            workspace_id=str(workspace_id),
        )
        with composed_transaction(pool) as transaction_pool:
            require_account_operational(transaction_pool, profile_id=profile_id)
            require_profile_workspace(
                transaction_pool, profile_id=profile_id, workspace_id=workspace_id
            )
            workspace = get_workspace_for_update(transaction_pool, workspace_id)
            if workspace is None or workspace.owner_profile_id != profile_id:
                raise NotFoundError("the requested workspace is not available to this Profile")
            connections = list_workspace_connections_for_update(
                transaction_pool, workspace_id=workspace_id
            )
            for connection in connections:
                _cleanup_connection_credential(transaction_pool, connection)
            deleted = delete_workspace(transaction_pool, workspace_id)
            if deleted is None:
                # Unreachable while the composition holds the root lock.
                raise NotFoundError("the requested workspace is not available to this Profile")
    return deleted
