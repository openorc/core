"""Workspace authorization application service (issue #53).

The v1 Workspace isolation boundary: one Workspace has exactly one owning
Profile (``workspace.owner_profile_id == authenticated_profile.id``), and
membership, invitation, and RBAC concepts do not exist. This module is the
shared authorization boundary later services compose with when operating on
any Workspace- or Task-owned record.

Authorization policy lives only here — never in persistence, never in SQL
repositories, never in the database composite foreign keys (those are durable
scope backstops). The public surface is transport-neutral and independent of
FastAPI/RQ.

Caller-ownership composition is explicit and gapless: every public resolver
takes the authenticated Profile UUID (the ``AuthenticatedUser.profile.id``
resolved by the #52 authentication boundary) and re-enforces ownership
through :func:`require_profile_workspace` before any scope check. There is
deliberately no public resolver that accepts a
:class:`~openorc.domain.ownership.Workspace` object, a bare ``workspace_id``
alone, or any pre-authorized object as authority — a ``Workspace`` is
ordinary durable data that persistence can load and application code can
construct, so passing one proves only object shape, never that the
authenticated Profile was checked. Plain Workspace objects returned by these
helpers are resource data, never reusable authority.

Failure normalization: the caller-facing outcome for an inaccessible subject
is uniformly :class:`NotFoundError` — whether the UUID does not exist,
belongs to another Profile's Workspace, or is mislinked — so probing exact
internal UUIDs across Workspaces is indistinguishable from addressing
something that is absent. Authorization is never inferred from GitHub
identity, repository metadata, Connection identity, object existence alone,
or client-supplied Workspace claims; where a record carries a direct
``workspace_id`` that scope is validated, never a join through mutable
external metadata.
"""

from __future__ import annotations

from uuid import UUID

from openorc.domain.blocks import TaskBlock
from openorc.domain.connections import Connection, WorkflowRole, WorkflowRoleBinding
from openorc.domain.events import WorkflowEvent
from openorc.domain.executions import Execution
from openorc.domain.gates import OwnerGate
from openorc.domain.ownership import Project, Repository, Workspace
from openorc.domain.planning import PlanRevision
from openorc.domain.pull_requests import TaskPullRequest
from openorc.domain.reviews import ReviewIteration, ReviewLoop
from openorc.domain.runtime_requests import RuntimeRequest
from openorc.domain.sessions import TaskAgentSession
from openorc.domain.tasks import Task
from openorc.persistence.blocks import get_task_block
from openorc.persistence.connections import get_connection, get_role_binding
from openorc.persistence.events import get_workflow_event
from openorc.persistence.executions import get_execution
from openorc.persistence.gates import get_owner_gate
from openorc.persistence.ownership import get_project, get_repository, get_workspace
from openorc.persistence.planning import get_plan_revision
from openorc.persistence.pool import DatabasePool
from openorc.persistence.pull_requests import get_task_pull_request
from openorc.persistence.reviews import get_review_iteration, get_review_loop
from openorc.persistence.runtime_requests import get_runtime_request
from openorc.persistence.sessions import get_task_agent_session
from openorc.persistence.tasks import get_task
from openorc.services.errors import NotFoundError

__all__ = [
    "require_profile_workspace",
    "require_task_agent_session",
    "require_task_block",
    "require_task_execution",
    "require_task_owner_gate",
    "require_task_plan_revision",
    "require_task_pull_request",
    "require_task_review_iteration",
    "require_task_review_loop",
    "require_task_runtime_request",
    "require_workspace_connection",
    "require_workspace_event",
    "require_workspace_project",
    "require_workspace_repository",
    "require_workspace_role_binding",
    "require_workspace_task",
]


def _require_owned_workspace(
    pool: DatabasePool, *, profile_id: UUID, workspace_id: UUID
) -> Workspace:
    """Load one Workspace and fail closed unless the Profile owns it.

    The canonical v1 rule is exact: ``workspace.owner_profile_id ==
    profile_id``. A missing Workspace and a Workspace owned by another
    Profile are the same caller-facing outcome (not found) so existence is
    never leaked. This is the only place ownership policy is written; every
    public resolver composes through it.
    """
    workspace = get_workspace(pool, workspace_id)
    if workspace is None or workspace.owner_profile_id != profile_id:
        raise NotFoundError("the requested workspace is not available to this Profile")
    return workspace


def _require_task_in_workspace(pool: DatabasePool, workspace: Workspace, task_id: UUID) -> Task:
    """Resolve one Task of the already-authorized Workspace, failing closed.

    Task-owned record linkage is validated against this Task: the record's
    direct ``workspace_id`` plus its ``task_id`` pointing at a Task of the
    authorized Workspace. The database composite foreign keys
    ``(task_id, workspace_id) → tasks (id, workspace_id)`` are the durable
    backstop for exactly this agreement; the checks here are the
    authorization mechanism.
    """
    task = get_task(pool, task_id)
    if task is None or task.workspace_id != workspace.id:
        raise NotFoundError("the requested task is not available in this workspace")
    return task


def require_profile_workspace(
    pool: DatabasePool, *, profile_id: UUID, workspace_id: UUID
) -> Workspace:
    """Resolve the authenticated Profile's owned Workspace, failing closed.

    The entry point of the v1 Workspace boundary: the returned Workspace is
    the requested resource for the caller — it is ordinary domain data and
    never itself an authorization capability for any other resolver.
    """
    return _require_owned_workspace(pool, profile_id=profile_id, workspace_id=workspace_id)


def require_workspace_project(
    pool: DatabasePool, *, profile_id: UUID, workspace_id: UUID, project_id: UUID
) -> Project:
    """Resolve one Project within the Profile's authorized Workspace."""
    workspace = _require_owned_workspace(pool, profile_id=profile_id, workspace_id=workspace_id)
    project = get_project(pool, project_id)
    if project is None or project.workspace_id != workspace.id:
        raise NotFoundError("the requested project is not available in this workspace")
    return project


def require_workspace_repository(
    pool: DatabasePool, *, profile_id: UUID, workspace_id: UUID, repository_id: UUID
) -> Repository:
    """Resolve one Repository record within the Profile's authorized Workspace.

    The Repository record's direct ``workspace_id`` scope is validated —
    never a GitHub identity or repository-metadata join; the same external
    GitHub repository may legitimately exist in other Workspaces.
    """
    workspace = _require_owned_workspace(pool, profile_id=profile_id, workspace_id=workspace_id)
    repository = get_repository(pool, repository_id)
    if repository is None or repository.workspace_id != workspace.id:
        raise NotFoundError("the requested repository is not available in this workspace")
    return repository


def require_workspace_connection(
    pool: DatabasePool, *, profile_id: UUID, workspace_id: UUID, connection_id: UUID
) -> Connection:
    """Resolve one Connection within the Profile's authorized Workspace."""
    workspace = _require_owned_workspace(pool, profile_id=profile_id, workspace_id=workspace_id)
    connection = get_connection(pool, connection_id)
    if connection is None or connection.workspace_id != workspace.id:
        raise NotFoundError("the requested connection is not available in this workspace")
    return connection


def require_workspace_role_binding(
    pool: DatabasePool, *, profile_id: UUID, workspace_id: UUID, role: WorkflowRole
) -> WorkflowRoleBinding:
    """Resolve one per-role binding within the Profile's authorized Workspace.

    Bindings are addressed by ``(Workspace, role)`` in v1, so the lookup is
    already workspace-scoped; ownership is still enforced here and the
    binding's direct scope is validated for uniformity.
    """
    workspace = _require_owned_workspace(pool, profile_id=profile_id, workspace_id=workspace_id)
    binding = get_role_binding(pool, workspace_id=workspace.id, role=role)
    if binding is None or binding.workspace_id != workspace.id:
        raise NotFoundError(
            "the requested workflow role binding is not available in this workspace"
        )
    return binding


def require_workspace_task(
    pool: DatabasePool, *, profile_id: UUID, workspace_id: UUID, task_id: UUID
) -> Task:
    """Resolve one Task within the Profile's authorized Workspace.

    The gateway resolver for every Task-owned record: later services anchor
    Task-owned operations through this Task scope.
    """
    workspace = _require_owned_workspace(pool, profile_id=profile_id, workspace_id=workspace_id)
    return _require_task_in_workspace(pool, workspace, task_id)


def require_task_agent_session(
    pool: DatabasePool,
    *,
    profile_id: UUID,
    workspace_id: UUID,
    task_id: UUID,
    role: WorkflowRole,
) -> TaskAgentSession:
    """Resolve one Task/role session binding inside the authorized Task.

    Sessions are addressed by ``(Task, role)``, not by session UUID; the
    binding's direct workspace/task scope is validated against the
    authorized Workspace and Task.
    """
    workspace = _require_owned_workspace(pool, profile_id=profile_id, workspace_id=workspace_id)
    task = _require_task_in_workspace(pool, workspace, task_id)
    session = get_task_agent_session(pool, task_id=task.id, role=role)
    if session is None or session.workspace_id != workspace.id or session.task_id != task.id:
        raise NotFoundError("the requested agent session is not available in this workspace")
    return session


def require_task_plan_revision(
    pool: DatabasePool, *, profile_id: UUID, workspace_id: UUID, plan_revision_id: UUID
) -> PlanRevision:
    """Resolve one PlanRevision of the authorized Workspace and its Task."""
    workspace = _require_owned_workspace(pool, profile_id=profile_id, workspace_id=workspace_id)
    revision = get_plan_revision(pool, plan_revision_id=plan_revision_id)
    if revision is None or revision.workspace_id != workspace.id:
        raise NotFoundError("the requested plan revision is not available in this workspace")
    _require_task_in_workspace(pool, workspace, revision.task_id)
    return revision


def require_task_review_loop(
    pool: DatabasePool, *, profile_id: UUID, workspace_id: UUID, review_loop_id: UUID
) -> ReviewLoop:
    """Resolve one ReviewLoop of the authorized Workspace and its Task."""
    workspace = _require_owned_workspace(pool, profile_id=profile_id, workspace_id=workspace_id)
    loop = get_review_loop(pool, review_loop_id=review_loop_id)
    if loop is None or loop.workspace_id != workspace.id:
        raise NotFoundError("the requested review loop is not available in this workspace")
    _require_task_in_workspace(pool, workspace, loop.task_id)
    return loop


def require_task_review_iteration(
    pool: DatabasePool, *, profile_id: UUID, workspace_id: UUID, review_iteration_id: UUID
) -> ReviewIteration:
    """Resolve one ReviewIteration of the authorized Workspace and its Task."""
    workspace = _require_owned_workspace(pool, profile_id=profile_id, workspace_id=workspace_id)
    iteration = get_review_iteration(pool, review_iteration_id=review_iteration_id)
    if iteration is None or iteration.workspace_id != workspace.id:
        raise NotFoundError("the requested review iteration is not available in this workspace")
    _require_task_in_workspace(pool, workspace, iteration.task_id)
    return iteration


def require_task_owner_gate(
    pool: DatabasePool, *, profile_id: UUID, workspace_id: UUID, owner_gate_id: UUID
) -> OwnerGate:
    """Resolve one OwnerGate of the authorized Workspace and its Task."""
    workspace = _require_owned_workspace(pool, profile_id=profile_id, workspace_id=workspace_id)
    gate = get_owner_gate(pool, owner_gate_id=owner_gate_id)
    if gate is None or gate.workspace_id != workspace.id:
        raise NotFoundError("the requested owner gate is not available in this workspace")
    _require_task_in_workspace(pool, workspace, gate.task_id)
    return gate


def require_task_execution(
    pool: DatabasePool, *, profile_id: UUID, workspace_id: UUID, execution_id: UUID
) -> Execution:
    """Resolve one Execution of the authorized Workspace and its Task."""
    workspace = _require_owned_workspace(pool, profile_id=profile_id, workspace_id=workspace_id)
    execution = get_execution(pool, execution_id=execution_id)
    if execution is None or execution.workspace_id != workspace.id:
        raise NotFoundError("the requested execution is not available in this workspace")
    _require_task_in_workspace(pool, workspace, execution.task_id)
    return execution


def require_task_runtime_request(
    pool: DatabasePool, *, profile_id: UUID, workspace_id: UUID, runtime_request_id: UUID
) -> RuntimeRequest:
    """Resolve one RuntimeRequest of the authorized Workspace and its Task."""
    workspace = _require_owned_workspace(pool, profile_id=profile_id, workspace_id=workspace_id)
    request = get_runtime_request(pool, runtime_request_id=runtime_request_id)
    if request is None or request.workspace_id != workspace.id:
        raise NotFoundError("the requested runtime request is not available in this workspace")
    _require_task_in_workspace(pool, workspace, request.task_id)
    return request


def require_task_block(
    pool: DatabasePool, *, profile_id: UUID, workspace_id: UUID, block_id: UUID
) -> TaskBlock:
    """Resolve one TaskBlock of the authorized Workspace and its Task."""
    workspace = _require_owned_workspace(pool, profile_id=profile_id, workspace_id=workspace_id)
    block = get_task_block(pool, task_block_id=block_id)
    if block is None or block.workspace_id != workspace.id:
        raise NotFoundError("the requested task block is not available in this workspace")
    _require_task_in_workspace(pool, workspace, block.task_id)
    return block


def require_task_pull_request(
    pool: DatabasePool, *, profile_id: UUID, workspace_id: UUID, task_pull_request_id: UUID
) -> TaskPullRequest:
    """Resolve one TaskPullRequest of the authorized Workspace and its Task."""
    workspace = _require_owned_workspace(pool, profile_id=profile_id, workspace_id=workspace_id)
    pull_request = get_task_pull_request(pool, task_pull_request_id=task_pull_request_id)
    if pull_request is None or pull_request.workspace_id != workspace.id:
        raise NotFoundError("the requested pull request record is not available in this workspace")
    _require_task_in_workspace(pool, workspace, pull_request.task_id)
    return pull_request


def require_workspace_event(
    pool: DatabasePool, *, profile_id: UUID, workspace_id: UUID, workflow_event_id: UUID
) -> WorkflowEvent:
    """Resolve one WorkflowEvent for reading within the authorized Workspace.

    Workspace scope is the authorization boundary for event reads (a
    Workspace-level event legitimately has no Task). A present ``task_id``
    is validated explicitly through the private Task-scope resolver — the
    referenced Task must belong to the authorized Workspace — rather than
    relying on the durable
    ``(task_id, workspace_id) → tasks (id, workspace_id)`` composite foreign
    key: those keys are integrity backstops, never the service
    authorization mechanism. A Workspace-level event (``task_id=None``)
    resolves without any Task lookup.
    """
    workspace = _require_owned_workspace(pool, profile_id=profile_id, workspace_id=workspace_id)
    event = get_workflow_event(pool, workflow_event_id=workflow_event_id)
    if event is None or event.workspace_id != workspace.id:
        raise NotFoundError("the requested workflow event is not available in this workspace")
    if event.task_id is not None:
        _require_task_in_workspace(pool, workspace, event.task_id)
    return event
