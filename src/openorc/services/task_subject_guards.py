"""Task-subject guards for authoritative workflow commands (issue #54).

Focused, typed resolvers for the exact current facts later Phase 2 workflow
commands must prove before applying any workflow-changing effect. A
caller-supplied UUID or SHA is never trusted merely because the record
exists: every resolver here reloads authoritative durable state and proves
the named subject is still the exact current subject for the Task and
Workspace the command addresses. Nothing here is a generic "subject"
dictionary or a reflection-based framework — each guard is a typed function
around the existing domain objects.

Three deliberate layers:

- scope-only resolvers prove a caller-named subject belongs to the
  addressed Task and Workspace with no currentness requirement — that is
  what installation mutations need, because the subject they install is
  not current yet (``require_plan_revision_in_task``,
  ``require_pending_owner_gate_in_task``);
- currentness guards build on the scope-only resolvers and additionally
  require the exact current fact (``require_current_task`` with its
  expected ``state_token``, ``require_current_plan_revision``,
  ``require_current_owner_gate``);
- pull-request guards resolve the canonical TaskPullRequest and bind
  authority to its exact reconciled head SHA
  (``require_task_pull_request_head``).

Failure vocabulary (#51): a missing subject — or one that belongs to a
different Task or Workspace — is uniformly ``NotFoundError``, so probing
exact internal UUIDs is indistinguishable from addressing something absent.
A subject that has moved on (stale ``state_token``, archived Task,
superseded revision, superseded or resolved gate, changed PR head) is a
``StaleOperationError``: the operation is never applied and never retried
blindly. A malformed command is an ``InvalidCommandError``. No guard calls
GitHub or any other external system: PR-head binding is checked against the
durable reconciled ``TaskPullRequest.head_sha``; authoritative GitHub
reconciliation (Phase 2B) happens before these guards are used for
externally consequential actions.
"""

from __future__ import annotations

from uuid import UUID

from openorc.domain.gates import OwnerGate, OwnerGateStatus
from openorc.domain.planning import PlanRevision
from openorc.domain.pull_requests import TaskPullRequest
from openorc.domain.tasks import Task
from openorc.persistence.gates import get_owner_gate
from openorc.persistence.planning import get_plan_revision
from openorc.persistence.pool import DatabasePool
from openorc.persistence.pull_requests import get_task_pull_request_for_task
from openorc.persistence.tasks import get_task
from openorc.services.errors import InvalidCommandError, NotFoundError, StaleOperationError

__all__ = [
    "require_canonical_task_pull_request",
    "require_current_owner_gate",
    "require_current_plan_revision",
    "require_current_task",
    "require_pending_owner_gate_in_task",
    "require_plan_revision_in_task",
    "require_task_pull_request_head",
]


def _require_uuid_command(value: object, name: str) -> None:
    """Reject a malformed UUID command argument before any state is touched."""
    if not isinstance(value, UUID):
        raise InvalidCommandError(f"{name} must be a UUID")


def _require_nonblank_command(value: object, name: str) -> None:
    """Reject a malformed string command argument before any state is touched."""
    if not isinstance(value, str) or not value.strip():
        raise InvalidCommandError(f"{name} must be a non-empty string")


def require_current_task(
    pool: DatabasePool, *, workspace_id: UUID, task_id: UUID, expected_state_token: UUID
) -> Task:
    """Resolve the current non-archived Task of the addressed Workspace.

    The one Task-identity guard every authoritative command composes first:
    the addressed Task UUID must name a Task that exists in the Workspace
    the command addresses (otherwise uniformly ``NotFoundError``), that is
    still current (non-archived), and that carries exactly the caller's
    expected ``state_token``. An archived Task or a token mismatch is a
    stale operation: nothing may be applied against it, and the typed
    ``StaleOperationError`` is never retried blindly.
    """
    _require_uuid_command(workspace_id, "workspace_id")
    _require_uuid_command(task_id, "task_id")
    _require_uuid_command(expected_state_token, "expected_state_token")
    task = get_task(pool, task_id)
    if task is None or task.workspace_id != workspace_id:
        raise NotFoundError("the requested task is not available in this workspace")
    if task.archived_at is not None:
        raise StaleOperationError("the task has already been archived; the operation is stale")
    if task.state_token != expected_state_token:
        raise StaleOperationError(
            "the task state has moved on since this operation was prepared; the operation is stale"
        )
    return task


def require_plan_revision_in_task(
    pool: DatabasePool, *, task: Task, plan_revision_id: UUID
) -> PlanRevision:
    """Resolve one PlanRevision that belongs to the addressed Task (scope-only).

    Installation-safe resolution: the revision need not be the Task's
    current revision yet — installing the current-plan pointer is a separate
    authoritative mutation (:mod:`openorc.services.task_mutations`). A
    caller-supplied revision UUID is never trusted merely because the row
    exists: it must carry exactly the addressed Task's ``task_id`` and
    ``workspace_id``, and a missing, cross-Task, or cross-Workspace revision
    is uniformly ``NotFoundError``.
    """
    _require_uuid_command(plan_revision_id, "plan_revision_id")
    revision = get_plan_revision(pool, plan_revision_id=plan_revision_id)
    if (
        revision is None
        or revision.task_id != task.id
        or revision.workspace_id != task.workspace_id
    ):
        raise NotFoundError("the requested plan revision is not available for this task")
    return revision


def require_pending_owner_gate_in_task(
    pool: DatabasePool, *, task: Task, owner_gate_id: UUID
) -> OwnerGate:
    """Resolve one pending OwnerGate that belongs to the addressed Task (scope-only).

    Installation-safe resolution: the gate need not be the Task's current
    gate yet — installing it as current is a separate authoritative Task
    mutation (:mod:`openorc.services.task_mutations`). A caller-supplied
    gate UUID is never trusted merely because the row exists: it must carry
    exactly the addressed Task's ``task_id`` and ``workspace_id``
    (uniformly ``NotFoundError``), and it must still be pending — a gate
    that has already been resolved has moved on (``StaleOperationError``),
    which is also how the resolution mutation classifies a raced outcome.
    """
    _require_uuid_command(owner_gate_id, "owner_gate_id")
    gate = get_owner_gate(pool, owner_gate_id=owner_gate_id)
    if gate is None or gate.task_id != task.id or gate.workspace_id != task.workspace_id:
        raise NotFoundError("the requested owner gate is not available for this task")
    if gate.status is not OwnerGateStatus.PENDING:
        raise StaleOperationError("the owner gate is no longer pending; the operation is stale")
    return gate


def require_current_plan_revision(
    pool: DatabasePool, *, task: Task, plan_revision_id: UUID
) -> PlanRevision:
    """Resolve the exact current PlanRevision of the addressed Task.

    Builds on the scope-only resolver: the named revision must belong to
    the addressed Task and Workspace, and additionally be exactly the
    Task's current-plan pointer. A revision that exists but is no longer
    current — superseded by a newer same-Task revision or simply a named
    older revision — is a stale operation and applies no workflow effect.
    """
    revision = require_plan_revision_in_task(pool, task=task, plan_revision_id=plan_revision_id)
    if task.current_plan_revision_id != revision.id:
        raise StaleOperationError(
            "the plan revision is no longer the task's current revision; the operation is stale"
        )
    return revision


def require_current_owner_gate(pool: DatabasePool, *, task: Task, owner_gate_id: UUID) -> OwnerGate:
    """Resolve the exact current OwnerGate of the addressed Task.

    Builds on the scope-only resolver: the named gate must belong to the
    addressed Task and Workspace, still be pending, and additionally be
    exactly the Task's current-gate pointer. A gate that is no longer
    current — resolved, superseded, or never installed — is a stale
    operation and applies no workflow effect.
    """
    gate = require_pending_owner_gate_in_task(pool, task=task, owner_gate_id=owner_gate_id)
    if task.current_owner_gate_id != gate.id:
        raise StaleOperationError(
            "the owner gate is no longer the task's current gate; the operation is stale"
        )
    return gate


def require_canonical_task_pull_request(pool: DatabasePool, *, task: Task) -> TaskPullRequest:
    """Resolve the Task's canonical TaskPullRequest record.

    ``unique (task_id)`` means one Task has exactly one canonical PR record
    for its whole v1 lifetime — there is no replacement-PR row to consider.
    A Task without its canonical PR record yet has nothing to address
    (``NotFoundError``), and the record's direct Workspace scope is
    validated explicitly: the durable constraint is a backstop, never the
    resolution mechanism.
    """
    pull_request = get_task_pull_request_for_task(pool, task_id=task.id)
    if pull_request is None or pull_request.workspace_id != task.workspace_id:
        raise NotFoundError("the requested pull request record is not available for this task")
    return pull_request


def require_task_pull_request_head(
    pool: DatabasePool, *, task: Task, expected_head_sha: str
) -> TaskPullRequest:
    """Resolve the canonical TaskPullRequest and bind authority to its exact head.

    The reusable exact-head check for commands whose authority is bound to
    a GitHub PR head: the caller's expected SHA must equal the current
    reconciled ``head_sha`` exactly. A changed head invalidates the
    head-bound command as a stale operation that applies no workflow effect;
    base-branch movement alone is deliberately never consulted and is not a
    stale PR-review subject — review and merge authority is head-SHA bound,
    exactly as the Phase 1 identity established. The comparison reads
    durable reconciliation state only: this guard never calls GitHub, and
    authoritative reconciliation (Phase 2B) happens before these guards are
    used for externally consequential actions.
    """
    _require_nonblank_command(expected_head_sha, "expected_head_sha")
    pull_request = require_canonical_task_pull_request(pool, task=task)
    if pull_request.head_sha != expected_head_sha:
        raise StaleOperationError(
            "the pull request head has changed since this operation was prepared; "
            "the operation is stale"
        )
    return pull_request
