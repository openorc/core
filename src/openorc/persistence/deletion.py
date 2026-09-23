"""Destructive aggregate-deletion repositories (Phase 1, issue #27).

Explicit SQL repositories over the ``openorc`` schema for the destructive
actions that are meaningful at Phase 1, each one short transaction that removes
exactly the OpenOrc-owned aggregate it names. Deleting OpenOrc state never
deletes or mutates external engineering artifacts (GitHub repositories,
issues, branches, commits, pull requests, checks, statuses, comments) or
runtime-owned state (Cline provider/MCP credentials, runtime configuration,
filesystem state): OpenOrc's ``Repository`` record is OpenOrc's own persisted
mapping for an external GitHub repository, and deleting it — like every other
deletion here — is pure OpenOrc-database work. No operation in this module
calls or depends on any external system, and no database action in the
deletion-ownership migration touches one.

Deletion follows the ownership graph enforced by the deletion-ownership
migration (issue #27):

    auth.users
        | on delete cascade   (sanctioned Supabase Auth boundary — the
        |                      canonical account root; deleting the Auth user
        |                      removes the complete OpenOrc-owned graph)
    openorc.profiles
        | on delete cascade
    workspaces -> projects -> repositories -> tasks -> (task-owned graph)
        + connections, workflow_role_bindings, workspace-level workflow_events
        + github_installations (issue #57 Workspace-scoped GitHub App
          installation records)

True ownership edges cascade in the database; restrictive edges are
``NO ACTION DEFERRABLE INITIALLY DEFERRED`` and block deletion of a referenced
row while references exist — historical/config cross-references (for example
a Connection referenced by historical TaskAgentSessions, or
PlanRevisions/Sessions/TaskPullRequests referenced by gates and iterations)
are never cascaded through. Because one deliberate root deletion can reach a
row through several ownership paths and Postgres fires sibling referential
triggers in an undefined order, the operations below perform explicit, ordered
deletion inside one short transaction rather than relying on cascade/check
ordering.

Task archive-vs-purge: normal Task lifecycle archives attempts
(:func:`openorc.persistence.tasks.archive_task`); only archived Tasks may be
purged, and every Task-deleting operation explicitly clears the Task's
current-object pointers (``current_plan_revision_id`` /
``current_owner_gate_id``) inside the same transaction before deleting the
Task rows — those rows are about to be destroyed, so preserving current
pointers during deletion has no value. The Auth-root cascade cannot run
OpenOrc code; the deferred pointer foreign keys backstop that path.

Connection deletion is deliberately restrictive: a Connection row is removed
only when no role binding or TaskAgentSession still references it. Disconnect
is the explicit operation that revokes OpenOrc use (``enabled = false``) and
drops the OpenOrc-owned authentication reference (``auth_reference = null``)
in one atomic configuration change; runtime-owned credentials and
configuration remain runtime-owned and are never touched here.

Violated durable invariants surface as driver exceptions (for example
``psycopg.errors.ForeignKeyViolation``); translating them into typed
application errors is a service-layer concern, not a persistence one.
"""

from __future__ import annotations

from typing import TYPE_CHECKING
from uuid import UUID

from openorc.domain.tasks import Task, TaskDomainError
from openorc.persistence.connections import _CONNECTION_COLUMNS, _connection_from_row
from openorc.persistence.ownership import (
    _REPOSITORY_COLUMNS,
    _WORKSPACE_COLUMNS,
    _project_from_row,
    _repository_from_row,
    _workspace_from_row,
)
from openorc.persistence.pool import DatabasePool
from openorc.persistence.tasks import _TASK_COLUMNS, _task_from_row
from openorc.persistence.transactions import transaction

__all__ = [
    "delete_connection",
    "delete_project",
    "delete_repository",
    "delete_workspace",
    "disconnect_connection",
    "purge_archived_task",
]

# Row-to-domain mapping and column lists are imported from the sibling
# repository modules on purpose: this package keeps one source of truth for
# row -> domain-object mapping at the persistence boundary. The quoted return
# annotations below name the same domain objects those helpers build.
if TYPE_CHECKING:  # pragma: no cover - domain types are runtime-constructed
    from openorc.domain.connections import Connection
    from openorc.domain.ownership import Project, Repository, Workspace

# The Connection-reference foreign keys (historical TaskAgentSession bindings
# and workflow role bindings) are DEFERRABLE INITIALLY DEFERRED so a
# deliberate root deletion — above all the ``auth.users`` account root, which
# cascades through every ownership path in one statement — can settle before
# the referential check. Releasing a repository-level transaction (or a
# SAVEPOINT, as repository calls run under in tests) does not check deferred
# constraints, so hard Connection deletion forces exactly these constraints
# IMMEDIATE inside its own transaction: a referenced Connection is rejected
# before the operation returns, deterministically, in every caller context.
_CONNECTION_REFERENCE_CONSTRAINTS = (
    "openorc.task_agent_sessions_connection_id_workspace_id_fkey",
    "openorc.workflow_role_bindings_connection_id_workspace_id_fkey",
)


def delete_workspace(pool: DatabasePool, workspace_id: UUID) -> Workspace | None:
    """Delete one Workspace and everything exclusively owned by it.

    Deliberate aggregate deletion in dependency order inside one transaction:

    1. clear the current-object pointers of the Workspace's Tasks and delete
       the Tasks (cascading the full Task-owned graph: sessions, plan
       revisions, review loops/iterations, gates, executions, runtime
       requests, blocks, pull requests, review history, Task-scoped events);
    2. delete the Workspace's workflow role bindings;
    3. delete the Workspace's Connections (no session or binding references
       remain);
    4. delete the Workspace's GitHub installation records (issue #57): the
       Workspace row has not been removed yet, but no Repository still routes
       through the deferred route foreign key at commit time once Repositories
       are cascaded away with the Workspace row, so the deferred check
       settles; the GitHub App itself is never uninstalled and no external
       GitHub artifact is touched;
    5. delete the Workspace row (cascading Projects, Repositories, prompt
       overrides, and Workspace-level workflow events).

    Returns the deleted Workspace, or ``None`` when it does not exist. The
    owning Profile, sibling Workspaces, other Profiles' data, GitHub
    repositories, and runtime-owned state are untouched.
    """
    with transaction(pool) as conn:
        conn.execute(
            "update openorc.tasks "
            "set current_plan_revision_id = null, current_owner_gate_id = null "
            "where workspace_id = %s",
            (workspace_id,),
        )
        conn.execute("delete from openorc.tasks where workspace_id = %s", (workspace_id,))
        conn.execute(
            "delete from openorc.workflow_role_bindings where workspace_id = %s",
            (workspace_id,),
        )
        conn.execute(
            "delete from openorc.connections where workspace_id = %s",
            (workspace_id,),
        )
        conn.execute(
            "delete from openorc.github_installations where workspace_id = %s",
            (workspace_id,),
        )
        row = conn.execute(
            f"delete from openorc.workspaces where id = %s returning {_WORKSPACE_COLUMNS}",
            (workspace_id,),
        ).fetchone()
    return None if row is None else _workspace_from_row(row)


def delete_project(pool: DatabasePool, project_id: UUID) -> Project | None:
    """Delete one Project and its OpenOrc Repository/Task subtree.

    Deliberate aggregate deletion in dependency order inside one transaction:
    the Project's Repositories' Tasks are pointer-cleared and deleted
    (cascading their Task-owned graphs), then the Repository mappings, then
    the Project row. Returns the deleted Project, or ``None`` when it does
    not exist. The Workspace, sibling Projects, Workspace-level configuration
    (Connections, role bindings, prompt overrides, events), all GitHub
    repositories, and runtime-owned state are untouched.
    """
    with transaction(pool) as conn:
        conn.execute(
            "update openorc.tasks "
            "set current_plan_revision_id = null, current_owner_gate_id = null "
            "where repository_id in "
            "(select id from openorc.repositories where project_id = %s)",
            (project_id,),
        )
        conn.execute(
            "delete from openorc.tasks "
            "where repository_id in "
            "(select id from openorc.repositories where project_id = %s)",
            (project_id,),
        )
        conn.execute(
            "delete from openorc.repositories where project_id = %s",
            (project_id,),
        )
        row = conn.execute(
            "delete from openorc.projects where id = %s "
            "returning id, workspace_id, name, created_at, updated_at",
            (project_id,),
        ).fetchone()
    return None if row is None else _project_from_row(row)


def delete_repository(pool: DatabasePool, repository_id: UUID) -> Repository | None:
    """Delete one OpenOrc Repository mapping and its OpenOrc Task subtree.

    This removes OpenOrc's persisted relationship/state for the external
    GitHub repository and the Task aggregates owned beneath it. It explicitly
    does NOT delete or mutate the GitHub repository itself or any GitHub
    engineering artifact: no persistence primitive here may call or depend on
    a GitHub deletion API, and none does — the persistence layer has no
    authority over external artifacts.

    Deliberate aggregate deletion in dependency order inside one transaction:
    the Repository's Tasks are pointer-cleared and deleted (cascading their
    Task-owned graphs), then the Repository mapping row. Returns the deleted
    Repository, or ``None`` when it does not exist. Sibling Repository
    mappings, the Project, the Workspace, and runtime-owned state are
    untouched.
    """
    with transaction(pool) as conn:
        conn.execute(
            "update openorc.tasks "
            "set current_plan_revision_id = null, current_owner_gate_id = null "
            "where repository_id = %s",
            (repository_id,),
        )
        conn.execute("delete from openorc.tasks where repository_id = %s", (repository_id,))
        row = conn.execute(
            f"delete from openorc.repositories where id = %s returning {_REPOSITORY_COLUMNS}",
            (repository_id,),
        ).fetchone()
    return None if row is None else _repository_from_row(row)


def purge_archived_task(pool: DatabasePool, task_id: UUID) -> Task | None:
    """Purge one archived Task attempt with its complete OpenOrc aggregate.

    Explicit administrative primitive for archived Tasks only. Normal Task
    lifecycle uses archival, never deletion: a current (non-archived) Task
    must never be silently deleted through this primitive — the normal
    active lifecycle cancels first, and this operation rejects a
    non-archived Task with :class:`TaskDomainError`.

    Inside one transaction: lock the Task row (``SELECT ... FOR UPDATE``),
    reject a missing row with ``None``, reject a current (non-archived) Task,
    explicitly clear the Task's current-object pointers (the rows are about
    to be destroyed), and delete the Task — the true Task-owned descendants
    (sessions, plan revisions, review loops/iterations, gates, executions,
    runtime requests, blocks, pull requests, review history, Task-scoped
    events) are removed by the ownership cascades. Sibling Tasks, the
    Repository mapping, the Project, the Workspace, Connections, and the
    Profile are untouched. GitHub issue, branch, commit, PR, CI/check state,
    and other GitHub artifacts are never deleted or mutated: no external
    call exists on any deletion path.

    Returns the purged Task, or ``None`` when the Task does not exist.
    """
    with transaction(pool) as conn:
        row = conn.execute(
            f"select {_TASK_COLUMNS} from openorc.tasks where id = %s for update",
            (task_id,),
        ).fetchone()
        if row is None:
            return None
        task = _task_from_row(row)
        if task.archived_at is None:
            raise TaskDomainError(
                "purge_archived_task only purges archived Task attempts; "
                "a current (non-archived) Task is rejected — the normal "
                "active lifecycle cancels the Task first"
            )
        conn.execute(
            "update openorc.tasks "
            "set current_plan_revision_id = null, current_owner_gate_id = null "
            "where id = %s",
            (task_id,),
        )
        conn.execute("delete from openorc.tasks where id = %s", (task_id,))
    return task


def delete_connection(pool: DatabasePool, connection_id: UUID) -> Connection | None:
    """Hard-delete one Connection under deliberately restrictive semantics.

    A Connection is Workspace-owned configuration, but historical
    TaskAgentSessions and role bindings reference the Connection identity as
    retained historical/config evidence. The Connection-reference foreign
    keys are ``NO ACTION DEFERRABLE INITIALLY DEFERRED`` (never cascading):
    they are checked here explicitly so a referenced Connection is rejected
    with ``ForeignKeyViolation`` before this operation returns — deleting one
    Connection can never erase Task/session history or other Workspace
    configuration, and an unreferenced (never-used) Connection is removed.
    Deleting a Workspace removes its Connections through the ordered
    Workspace-aggregate deletion (:func:`delete_workspace`) after Task/session
    descendants and role bindings are gone.

    Disconnect (revoke OpenOrc use and drop the OpenOrc-owned authentication
    reference) is :func:`disconnect_connection`; runtime-owned credentials
    and configuration remain outside OpenOrc deletion authority and are never
    touched. Returns the deleted Connection, or ``None`` when it does not
    exist.
    """
    with transaction(pool) as conn:
        row = conn.execute(
            f"delete from openorc.connections where id = %s returning {_CONNECTION_COLUMNS}",
            (connection_id,),
        ).fetchone()
        if row is None:
            return None
        # Check the (deferred) Connection-reference constraints now, narrowly:
        # a referenced Connection must be rejected before this operation
        # returns, both in production transactions and under the
        # rollback-based integration harness where no commit ever fires the
        # deferred check.
        conn.execute(
            "set constraints " + ", ".join(_CONNECTION_REFERENCE_CONSTRAINTS) + " immediate"
        )
    return _connection_from_row(row)


def disconnect_connection(pool: DatabasePool, connection_id: UUID) -> Connection | None:
    """Disconnect one Connection: revoke OpenOrc use and drop its auth reference.

    Explicit persistence operation for the settled disconnect semantics
    (issue #27): one atomic configuration change setting ``enabled = false``
    and ``auth_reference = null`` together. Clearing ``auth_reference`` alone
    would not be sufficient — an enabled Connection without an OpenOrc-owned
    authentication reference can still be a valid configured Connection — so
    both facts move together. Runtime-owned credentials, provider
    configuration, and filesystem state are never OpenOrc deletion authority
    and are never touched. Row identity is preserved; this is a
    configuration change, not a tombstone and not a deletion.

    Returns the disconnected Connection, or ``None`` when it does not exist.
    """
    with transaction(pool) as conn:
        row = conn.execute(
            "update openorc.connections "
            "set enabled = false, auth_reference = null, updated_at = now() "
            f"where id = %s returning {_CONNECTION_COLUMNS}",
            (connection_id,),
        ).fetchone()
    return None if row is None else _connection_from_row(row)
