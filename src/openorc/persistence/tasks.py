"""Repositories for Task persistence.

Explicit SQL repositories over the ``openorc`` schema for the Task aggregate
root (Phase 1, issue #21). Rows map to transport-independent domain objects
from :mod:`openorc.domain.tasks`; instants returned from Postgres are
normalized to timezone-aware UTC at this boundary.

Identity and lifecycle: ``github_issue_id`` is the stable external identity
backing a Task; one current (non-archived) Task exists per
``(repository_id, github_issue_id)`` and the partial unique index keeps it
that way. Archival is a separate fact from the terminal outcome, so
``archive_task`` — not ``update_task_status`` — is the only terminal
transition path: it atomically sets the terminal ``status``, stamps
``archived_at``, and rotates ``state_token``.

Optimistic concurrency: every authoritative Task-state mutation is
conditional on the caller's expected ``state_token`` and replaces the token
in the same statement. A mutation returns ``None`` when the expected token no
longer matches (or the row is missing/already archived): the operation is
stale and must not be retried blindly. Translating that outcome into typed
application errors is a service-layer concern, not a persistence one.

Violated database invariants (uniqueness, foreign keys, CHECK constraints)
surface as driver exceptions (for example ``psycopg.errors.UniqueViolation``,
``ForeignKeyViolation``, and ``CheckViolation``).
"""

from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime
from decimal import Decimal
from typing import Any
from uuid import UUID

from openorc.domain.tasks import (
    Task,
    TaskDomainError,
    TaskPlanningBootstrapEvidence,
    TaskPlanningBootstrapStage,
    TaskStatus,
    TaskTerminalCause,
)
from openorc.persistence.pool import DatabasePool
from openorc.persistence.time import normalize_utc
from openorc.persistence.transactions import transaction

__all__ = [
    "archive_task",
    "bind_canonical_branch",
    "create_task",
    "claim_task_planning_bootstrap",
    "find_current_task_by_branch",
    "find_current_task_for_issue",
    "get_task",
    "get_task_for_update",
    "get_task_planning_bootstrap_claim",
    "get_task_planning_bootstrap_evidence",
    "list_issue_attempts",
    "list_current_repository_tasks",
    "list_queued_tasks",
    "list_workspace_tasks",
    "lock_workspace_task_arbitration",
    "record_planning_bootstrap_stage",
    "set_current_plan_revision",
    "set_current_owner_gate",
    "update_task_status",
]

_TASK_COLUMNS = (
    "id, workspace_id, repository_id, github_issue_id, github_issue_number, "
    "status, archived_at, canonical_feature_branch, state_token, "
    "current_plan_revision_id, current_owner_gate_id, "
    "source_requirements_fingerprint, queued_at, created_at, updated_at, "
    "terminal_cause, authorized_task_budget_usd"
)


def _task_from_row(row: Sequence[Any]) -> Task:
    archived_at = row[6]
    queued_at = row[12]
    return Task(
        id=row[0],
        workspace_id=row[1],
        repository_id=row[2],
        github_issue_id=row[3],
        github_issue_number=row[4],
        status=TaskStatus(row[5]),
        archived_at=None if archived_at is None else normalize_utc(archived_at),
        queued_at=None if queued_at is None else normalize_utc(queued_at),
        canonical_feature_branch=row[7],
        state_token=row[8],
        current_plan_revision_id=row[9],
        current_owner_gate_id=row[10],
        source_requirements_fingerprint=row[11],
        created_at=normalize_utc(row[13]),
        updated_at=normalize_utc(row[14]),
        terminal_cause=None if row[15] is None else TaskTerminalCause(row[15]),
        authorized_task_budget_usd=row[16],
    )


def create_task(
    pool: DatabasePool,
    *,
    workspace_id: UUID,
    repository_id: UUID,
    github_issue_id: int,
    github_issue_number: int,
    source_requirements_fingerprint: str | None = None,
    status: TaskStatus = TaskStatus.READY_TO_PLAN,
) -> Task:
    """Insert one Task attempt for a stable GitHub issue within a Repository.

    A fresh Task starts nonterminal (default ``ready_to_plan``) with a NULL
    canonical branch and NO source baseline: issue discovery and explicit
    Owner intake do not freeze requirements (issue #60, corrected by issue
    #233). ``source_requirements_fingerprint`` stays NULL until a successful
    Owner Start freezes it atomically with the ``ready_to_plan`` transition
    (:func:`update_task_status` capture). Terminal attempts only arise through
    :func:`archive_task`, the canonical branch is only bound once later
    workflow/runtime logic has verified it (:func:`bind_canonical_branch`),
    and at most one non-archived Task may exist per
    ``(repository_id, github_issue_id)`` (the database partial unique index
    enforces it).
    """
    if status.is_terminal:
        raise TaskDomainError(
            "a Task cannot be created with a terminal status; use archive_task on an existing Task"
        )
    with transaction(pool) as conn:
        row = conn.execute(
            "insert into openorc.tasks "
            "(workspace_id, repository_id, github_issue_id, github_issue_number, status, "
            "source_requirements_fingerprint) "
            "values (%s, %s, %s, %s, %s, %s) "
            f"returning {_TASK_COLUMNS}",
            (
                workspace_id,
                repository_id,
                github_issue_id,
                github_issue_number,
                status.value,
                source_requirements_fingerprint,
            ),
        ).fetchone()
    assert row is not None
    return _task_from_row(row)


def get_task(pool: DatabasePool, task_id: UUID) -> Task | None:
    """Return one Task by id, or ``None`` when it does not exist."""
    with transaction(pool) as conn:
        row = conn.execute(
            f"select {_TASK_COLUMNS} from openorc.tasks where id = %s", (task_id,)
        ).fetchone()
    return None if row is None else _task_from_row(row)


def get_task_for_update(pool: DatabasePool, task_id: UUID) -> Task | None:
    """Read one Task row under an exclusive row lock.

    Serves service-layer stale/conflict classification (issue #54) after a
    rejected conditional Task write: composed inside the same outer
    transaction as the failed write, the locked re-read observes the durable
    state at write time — a concurrent token rotation or archival serializes
    on the row lock — so the application-level classification never comes
    from a racy re-read (the same deliberate locked-read-plus-conditional-
    write pattern as Workspace configuration). Outside a composition the
    lock is held only for this function's own short transaction.
    Translating the reloaded state into typed application outcomes is a
    service-layer concern, never a persistence one.
    """
    with transaction(pool) as conn:
        row = conn.execute(
            f"select {_TASK_COLUMNS} from openorc.tasks where id = %s for update",
            (task_id,),
        ).fetchone()
    return None if row is None else _task_from_row(row)


def get_task_planning_bootstrap_claim(pool: DatabasePool, *, task_id: UUID) -> UUID | None:
    """Return the Task's one-shot planning-bootstrap claim token, or ``None``.

    The focused read of the E1b replay-safety fact (issue #203): NULL means
    the Task's bootstrap establishment has never been claimed (and the
    Task exists — the calling service resolves subject existence first); a
    non-NULL token means a bootstrap claim was durably written exactly once,
    so no later continuation may re-dispatch session creation or workspace
    preparation. Representation only: classifying the fact into workflow
    outcomes is a service-layer concern.
    """
    if not isinstance(task_id, UUID):
        raise TaskDomainError("task_id must be a UUID")
    with transaction(pool) as conn:
        row = conn.execute(
            "select planning_bootstrap_claim_token from openorc.tasks where id = %s",
            (task_id,),
        ).fetchone()
    return None if row is None else row[0]


def claim_task_planning_bootstrap(
    pool: DatabasePool,
    *,
    task_id: UUID,
    expected_state_token: UUID,
    claim_token: UUID,
) -> UUID | None:
    """Write the one-shot planning-bootstrap claim exactly once (issue #203).

    The E1b entry transaction's replay-safety write, composed immediately
    BEFORE the first runtime session-creation dispatch: the claim token is
    installed only while the row is current (exact ``expected_state_token``),
    non-archived, and still unclaimed (``planning_bootstrap_claim_token IS
    NULL``), and the write rotates ``state_token`` like every other
    authoritative Task-state mutation. Returns the post-claim
    ``state_token``, or ``None`` when the token no longer matches, the Task
    is missing/already archived, or the bootstrap was already claimed — a
    rejected claim is a stale or already-claimed operation that must never be
    retried blindly and never dispatches a runtime effect.
    """
    if not isinstance(task_id, UUID):
        raise TaskDomainError("task_id must be a UUID")
    if not isinstance(expected_state_token, UUID):
        raise TaskDomainError("expected_state_token must be a UUID")
    if not isinstance(claim_token, UUID):
        raise TaskDomainError("claim_token must be a UUID")
    with transaction(pool) as conn:
        row = conn.execute(
            "update openorc.tasks "
            "set planning_bootstrap_claim_token = %s, "
            "planning_bootstrap_claimed_at = now(), "
            "state_token = gen_random_uuid(), updated_at = now() "
            "where id = %s and state_token = %s and archived_at is null "
            "and planning_bootstrap_claim_token is null "
            "returning state_token",
            (claim_token, task_id, expected_state_token),
        ).fetchone()
    return None if row is None else row[0]


def get_task_planning_bootstrap_evidence(
    pool: DatabasePool, *, task_id: UUID
) -> TaskPlanningBootstrapEvidence | None:
    """Return the Task's bootstrap claim and stage evidence, or ``None``.

    The focused read of the E1b replay-safety fact (issue #203) plus the F2
    stage/settlement evidence (issue #87): ``None`` means the Task's
    bootstrap establishment has never been claimed (or the Task is absent —
    the calling service resolves subject existence first). A non-``None``
    evidence carries the exactly-once claim token, its database-stamped
    claim instant (``None`` is residue claimed before the instant evidence
    existed), the forward-only settled stage, and that stage's settlement
    instant. Representation only: classifying the facts into workflow
    outcomes is a service-layer concern.
    """
    if not isinstance(task_id, UUID):
        raise TaskDomainError("task_id must be a UUID")
    with transaction(pool) as conn:
        row = conn.execute(
            "select planning_bootstrap_claim_token, planning_bootstrap_claimed_at, "
            "planning_bootstrap_stage, planning_bootstrap_stage_settled_at "
            "from openorc.tasks where id = %s",
            (task_id,),
        ).fetchone()
    if row is None or row[0] is None:
        return None
    return TaskPlanningBootstrapEvidence(
        claim_token=row[0],
        claimed_at=None if row[1] is None else normalize_utc(row[1]),
        stage=None if row[2] is None else TaskPlanningBootstrapStage(row[2]),
        stage_settled_at=None if row[3] is None else normalize_utc(row[3]),
    )


def record_planning_bootstrap_stage(
    pool: DatabasePool,
    *,
    task_id: UUID,
    claim_token: UUID,
    stage: TaskPlanningBootstrapStage,
) -> bool:
    """Advance the bootstrap stage evidence forward-only, exactly once per stage.

    The F2 stage/settlement write (issue #87): observational evidence only,
    never authoritative workflow state — the write does not rotate
    ``state_token`` and composes no workflow transition. It applies only
    while the row is current (non-archived), claimed by the exact
    ``claim_token``, and still at the required prior stage (``None`` for
    ``prepared``; ``prepared`` for ``ready_for_planning``), so a raced or
    stale write is a rejected no-op (``False``) and stage history never
    regresses. ``None`` means the caller classifies the rejection.
    """
    if not isinstance(task_id, UUID):
        raise TaskDomainError("task_id must be a UUID")
    if not isinstance(claim_token, UUID):
        raise TaskDomainError("claim_token must be a UUID")
    if not isinstance(stage, TaskPlanningBootstrapStage):
        raise TaskDomainError("stage must be a TaskPlanningBootstrapStage")
    expected_prior_stage: str | None
    if stage is TaskPlanningBootstrapStage.PREPARED:
        expected_prior_stage = None
    elif stage is TaskPlanningBootstrapStage.READY_FOR_PLANNING:
        expected_prior_stage = TaskPlanningBootstrapStage.PREPARED.value
    else:
        raise TaskDomainError(f"unknown planning bootstrap stage: {stage!r}")
    with transaction(pool) as conn:
        row = conn.execute(
            "update openorc.tasks "
            "set planning_bootstrap_stage = %s, "
            "planning_bootstrap_stage_settled_at = now(), updated_at = now() "
            "where id = %s and planning_bootstrap_claim_token = %s "
            "and archived_at is null "
            "and planning_bootstrap_stage is not distinct from %s "
            "returning planning_bootstrap_stage",
            (stage.value, task_id, claim_token, expected_prior_stage),
        ).fetchone()
    return row is not None


def list_workspace_tasks(
    pool: DatabasePool, *, workspace_id: UUID, include_archived: bool = False
) -> list[Task]:
    """List a Workspace's Tasks.

    Defaults to the current (non-archived) attempts only; pass
    ``include_archived=True`` for full history. Serves the active-Workspace
    and status query shapes behind the ``tasks_workspace_*`` indexes.
    """
    predicate = "" if include_archived else " and archived_at is null"
    with transaction(pool) as conn:
        rows = conn.execute(
            f"select {_TASK_COLUMNS} from openorc.tasks "
            f"where workspace_id = %s{predicate} order by created_at, id",
            (workspace_id,),
        ).fetchall()
    return [_task_from_row(row) for row in rows]


def list_issue_attempts(
    pool: DatabasePool, *, repository_id: UUID, github_issue_id: int
) -> list[Task]:
    """List every attempt (archived and current) for one stable issue."""
    with transaction(pool) as conn:
        rows = conn.execute(
            f"select {_TASK_COLUMNS} from openorc.tasks "
            "where repository_id = %s and github_issue_id = %s "
            "order by created_at, id",
            (repository_id, github_issue_id),
        ).fetchall()
    return [_task_from_row(row) for row in rows]


def list_current_repository_tasks(
    pool: DatabasePool, *, workspace_id: UUID, repository_id: UUID
) -> list[Task]:
    """List one Workspace Repository's current (non-archived) Tasks.

    The bounded current-work Task set for the #62 repeatable reconciliation
    sweep: only nonterminal attempts participate, deterministically ordered by
    stable issue identity then record id. Database-only; never attempt
    history.
    """
    with transaction(pool) as conn:
        rows = conn.execute(
            f"select {_TASK_COLUMNS} from openorc.tasks "
            "where workspace_id = %s and repository_id = %s and archived_at is null "
            "order by github_issue_id, id",
            (workspace_id, repository_id),
        ).fetchall()
    return [_task_from_row(row) for row in rows]


def find_current_task_for_issue(
    pool: DatabasePool, *, repository_id: UUID, github_issue_id: int
) -> Task | None:
    """Return the one current Task for a stable issue, or ``None``.

    Serves current-Task resolution through the partial unique index
    ``tasks_repository_issue_current_uniq``.
    """
    with transaction(pool) as conn:
        row = conn.execute(
            f"select {_TASK_COLUMNS} from openorc.tasks "
            "where repository_id = %s and github_issue_id = %s "
            "and archived_at is null",
            (repository_id, github_issue_id),
        ).fetchone()
    return None if row is None else _task_from_row(row)


def list_queued_tasks(
    pool: DatabasePool,
    *,
    workspace_id: UUID,
    after_queued_at: datetime | None = None,
    after_task_id: UUID | None = None,
    limit: int,
) -> list[Task]:
    """Return one bounded page of the Workspace's queued Tasks in FIFO order.

    The one canonical queue-order read (issue #76): current queued Tasks
    ordered by the durable ``(queued_at, id)`` key. Pagination is keyset
    based — each page continues strictly after the caller's last examined
    ``(queued_at, task_id)`` key — so admitted candidates leaving the queued
    set never shift a page boundary and a caller scanning repeatedly with
    the returned last key examines every candidate exactly once, oldest
    first. ``limit`` bounds ONE page of a scan; the caller that needs the
    full queue paginates with the cursor. A queue wait is a scheduling
    state: this read decides nothing about admission.
    """
    if not isinstance(workspace_id, UUID):
        raise TaskDomainError("workspace_id must be a UUID")
    if isinstance(limit, bool) or not isinstance(limit, int) or limit <= 0:
        raise TaskDomainError("limit must be a positive integer")
    params: list[Any] = [workspace_id]
    if after_queued_at is None:
        predicate = ""
    else:
        if not isinstance(after_task_id, UUID):
            raise TaskDomainError("the queue page cursor must carry both key parts")
        if after_queued_at.tzinfo is None:
            raise TaskDomainError("the queue page cursor timestamp must be timezone-aware")
        predicate = " and (queued_at, id) > (%s, %s)"
        params.extend([normalize_utc(after_queued_at), after_task_id])
    params.append(limit)
    with transaction(pool) as conn:
        rows = conn.execute(
            f"select {_TASK_COLUMNS} from openorc.tasks "
            f"where workspace_id = %s and status = 'queued'{predicate} "
            "order by queued_at, id limit %s",
            tuple(params),
        ).fetchall()
    return [_task_from_row(row) for row in rows]


def lock_workspace_task_arbitration(pool: DatabasePool, *, workspace_id: UUID) -> None:
    """Take the per-Workspace Task-capacity arbitration advisory lock.

    The serialization boundary for Task start/FIFO admission decisions
    (issue #76): every transaction that arbitrates capacity — a competing
    new start's decision, a queued-candidates wakeup sweep, or a lifecycle
    transition that changes active (CONNECTING/READY) Connection occupancy —
    composes this same transaction-scoped ``pg_advisory_xact_lock`` FIRST,
    before any occupancy read or row lock it must observe consistently. The
    lock is released at commit/rollback; transactions stay short. The key is
    deterministically derived from the Workspace UUID (the low 63 bits, the
    single-bigint advisory-key form): collisions can only add serialization
    between unrelated Workspaces' arbitrations, never weaken a decision.
    """
    if not isinstance(workspace_id, UUID):
        raise TaskDomainError("workspace_id must be a UUID")
    advisory_key = workspace_id.int & ((1 << 63) - 1)
    with transaction(pool) as conn:
        conn.execute("select pg_advisory_xact_lock(%s)", (advisory_key,))


def find_current_task_by_branch(
    pool: DatabasePool, *, repository_id: UUID, canonical_feature_branch: str
) -> Task | None:
    """Return the current Task owning a canonical branch, or ``None``.

    Serves canonical-branch ownership lookup through the partial unique
    index ``tasks_repository_canonical_branch_current_uniq``.
    """
    with transaction(pool) as conn:
        row = conn.execute(
            f"select {_TASK_COLUMNS} from openorc.tasks "
            "where repository_id = %s and canonical_feature_branch = %s "
            "and archived_at is null",
            (repository_id, canonical_feature_branch),
        ).fetchone()
    return None if row is None else _task_from_row(row)


def update_task_status(
    pool: DatabasePool,
    task_id: UUID,
    *,
    expected_state_token: UUID,
    status: TaskStatus,
    capture_source_requirements_fingerprint: str | None = None,
    capture_authorized_task_budget_usd: Decimal | None = None,
) -> Task | None:
    """Move a current Task to a nonterminal status, rotating ``state_token``.

    Nonterminal transitions only: terminal statuses (``cancelled``/
    ``completed``) raise :class:`TaskDomainError` — terminal transitions go
    exclusively through :func:`archive_task`, which atomically sets the
    terminal status, stamps ``archived_at``, and rotates the token.

    The Owner Start arbitration (issue #233) freezes the freshly verified
    requirements fingerprint in the SAME guarded statement that transitions
    the Task: ``capture_source_requirements_fingerprint`` is set only where
    the baseline is still NULL (``coalesce``), so the capture is atomic with
    the ``ready_to_plan`` -> ``queued``/``planning`` transition and can never
    rewrite an existing frozen baseline. The default ``None`` is a no-op for
    every other caller.

    The same statement captures the Owner-authorized USD Task budget
    (issue #240): ``capture_authorized_task_budget_usd`` is set only where
    the authorization is still NULL (``coalesce``), so a successful start
    copies the then-current Workspace default atomically with the
    transition, a queued Task keeps the authorization it queued with when
    capacity later admits it, a queued-wakeup claim (which passes ``None``)
    and any later Workspace default edit never rewrite it. ``None`` means no
    spending policy is authorized — disabled, never zero.

    The update is conditional on ``expected_state_token`` and replaces the
    token atomically. Returns the updated Task, or ``None`` when the token
    no longer matches or the Task is missing/already archived (a stale
    operation that must not be retried blindly).
    """
    if not isinstance(status, TaskStatus) or status.is_terminal:
        raise TaskDomainError(
            "update_task_status only accepts nonterminal Task statuses; "
            "terminal transitions go exclusively through archive_task"
        )
    if capture_authorized_task_budget_usd is not None and (
        isinstance(capture_authorized_task_budget_usd, bool)
        or not isinstance(capture_authorized_task_budget_usd, Decimal)
        or not capture_authorized_task_budget_usd.is_finite()
        or capture_authorized_task_budget_usd <= 0
    ):
        raise TaskDomainError(
            "capture_authorized_task_budget_usd must be None or a positive finite USD amount"
        )
    with transaction(pool) as conn:
        row = conn.execute(
            "update openorc.tasks "
            "set status = %s, "
            # The Owner Start commitment capture (issue #233): the freshly
            # verified requirements fingerprint is frozen in this same
            # statement, and only where no baseline exists yet — coalesce can
            # never rewrite an existing frozen baseline. The status-aware
            # CHECK is the durable backstop: queued/planning and every later
            # status reject a NULL baseline.
            "source_requirements_fingerprint = "
            "coalesce(source_requirements_fingerprint, %s), "
            # The Owner spending-authorization capture (issue #240): the
            # then-current Workspace default is frozen in this same
            # statement, and only where no authorization exists yet —
            # coalesce can never rewrite a captured authorization. The
            # status-aware CHECK is the durable backstop: a ready_to_plan
            # row rejects a present authorization.
            "authorized_task_budget_usd = "
            "coalesce(authorized_task_budget_usd, %s), "
            # The durable queue-entry timestamp (issue #76): stamped by the
            # DB clock exactly once when a Task first moves into queued,
            # preserved unchanged while it stays queued (repeated capacity
            # checks never rewrite it), and cleared when it leaves queued by
            # any later authorized path. The iff-QUEUED CHECK is the durable
            # backstop; the condition binding is still the exact expected
            # state_token.
            "queued_at = case "
            "when %s = 'queued' and status <> 'queued' then now() "
            "when status = 'queued' and %s <> 'queued' then null "
            "else queued_at end, "
            "state_token = gen_random_uuid(), updated_at = now() "
            "where id = %s and state_token = %s and archived_at is null "
            f"returning {_TASK_COLUMNS}",
            (
                status.value,
                capture_source_requirements_fingerprint,
                capture_authorized_task_budget_usd,
                status.value,
                status.value,
                task_id,
                expected_state_token,
            ),
        ).fetchone()
    return None if row is None else _task_from_row(row)


def archive_task(
    pool: DatabasePool,
    task_id: UUID,
    *,
    expected_state_token: UUID,
    terminal_status: TaskStatus,
    terminal_cause: TaskTerminalCause | None = None,
) -> Task | None:
    """Archive a Task attempt with its terminal outcome, atomically.

    The only terminal transition path: sets ``terminal_status`` (must be
    ``cancelled`` or ``completed``), stamps ``archived_at``, and rotates
    ``state_token`` in one statement. Archival is independent from terminal
    outcome — the outcome stays distinguishable through ``status`` — and both
    CANCELLED and COMPLETED attempts become archived history. Archival also
    clears the Task's ``current_owner_gate_id`` pointer (issue #227): an
    archived Task has no actionable current gate, while the gate row itself
    stays untouched pending/resolved history with its own provenance.

    ``terminal_cause`` (issue #229) is the optional canonical terminal cause
    of an externally established cancellation, stamped in the same guarded
    statement and never rewritten or cleared afterwards. It is admitted only
    with the ``cancelled`` terminal status; an Owner-attributed cancellation
    and every COMPLETED archival carry no cause.

    The update is conditional on ``expected_state_token``. Returns the
    updated Task, or ``None`` when the token no longer matches or the Task
    is missing/already archived (a stale operation that must not be retried
    blindly).
    """
    if not isinstance(terminal_status, TaskStatus) or not terminal_status.is_terminal:
        raise TaskDomainError(
            "archive_task requires a terminal status (cancelled or completed); "
            "nonterminal transitions use update_task_status"
        )
    if terminal_cause is not None and not isinstance(terminal_cause, TaskTerminalCause):
        raise TaskDomainError("archive_task terminal_cause must be None or a TaskTerminalCause")
    if terminal_cause is not None and terminal_status is not TaskStatus.CANCELLED:
        raise TaskDomainError("a terminal cause applies only to a cancelled archival")
    with transaction(pool) as conn:
        row = conn.execute(
            "update openorc.tasks "
            "set status = %s, archived_at = now(), queued_at = null, "
            "terminal_cause = %s, "
            "current_owner_gate_id = null, "
            "state_token = gen_random_uuid(), updated_at = now() "
            "where id = %s and state_token = %s and archived_at is null "
            f"returning {_TASK_COLUMNS}",
            (
                terminal_status.value,
                None if terminal_cause is None else terminal_cause.value,
                task_id,
                expected_state_token,
            ),
        ).fetchone()
    return None if row is None else _task_from_row(row)


def bind_canonical_branch(
    pool: DatabasePool,
    task_id: UUID,
    *,
    expected_state_token: UUID,
    canonical_feature_branch: str,
) -> Task | None:
    """Bind a Task's canonical feature branch once, rotating the token.

    Canonical branch ownership is a one-time, Task-level fact: the branch is
    created NULL and bound exactly once, after later workflow/runtime logic
    has verified the Producer-created branch. The conditional UPDATE applies
    only while ``canonical_feature_branch IS NULL``, so a rebinding attempt
    is a rejected no-op — a current Task never releases or switches its
    canonical branch. A Task's branch is exclusive within its Repository
    (enforced by the partial unique index); retries, later Executions, and
    PR remediation for the Task continue on the same branch. Archival is
    what releases branch ownership for future Tasks.

    The update is conditional on ``expected_state_token``. Returns the
    updated Task, or ``None`` when the token no longer matches, the Task is
    missing/already archived, or the branch is already bound (none of these
    outcomes may be retried blindly; stale-branch translation is a
    service-layer concern).
    """
    if not isinstance(canonical_feature_branch, str) or not canonical_feature_branch.strip():
        raise TaskDomainError("canonical_feature_branch must be a non-empty string")
    with transaction(pool) as conn:
        row = conn.execute(
            "update openorc.tasks "
            "set canonical_feature_branch = %s, "
            "state_token = gen_random_uuid(), updated_at = now() "
            "where id = %s and state_token = %s and archived_at is null "
            "and canonical_feature_branch is null "
            f"returning {_TASK_COLUMNS}",
            (canonical_feature_branch, task_id, expected_state_token),
        ).fetchone()
    return None if row is None else _task_from_row(row)


def set_current_plan_revision(
    pool: DatabasePool,
    task_id: UUID,
    *,
    expected_state_token: UUID,
    plan_revision_id: UUID,
) -> Task | None:
    """Point the Task's current-plan pointer at one exact PlanRevision.

    The current-plan pointer is a nullable Task-level fact: it identifies
    the authoritative current PlanRevision without duplicating the
    revision's content or review outcomes on the Task row (one fact, one
    home). The composite foreign key ``tasks_current_plan_revision_fk``
    durably enforces same-Task/Workspace consistency: the pointer may only
    reference a PlanRevision whose ``(id, task_id, workspace_id)`` matches
    the Task, so cross-Task or cross-Workspace corruption attempts raise
    ``ForeignKeyViolation`` (translating driver exceptions into typed
    application errors is a service-layer concern). Moving the pointer to a
    newer same-Task revision leaves every older revision intact; the
    pointer is never cleared implicitly (archival is what ends a Task's
    attempt).

    The update is conditional on ``expected_state_token`` and rotates the
    token like every other authoritative Task-state mutation. Returns the
    updated Task, or ``None`` when the token no longer matches or the Task
    is missing/already archived (a stale operation that must not be retried
    blindly).
    """
    if not isinstance(plan_revision_id, UUID):
        raise TaskDomainError("plan_revision_id must be a UUID")
    with transaction(pool) as conn:
        row = conn.execute(
            "update openorc.tasks "
            "set current_plan_revision_id = %s, "
            "state_token = gen_random_uuid(), updated_at = now() "
            "where id = %s and state_token = %s and archived_at is null "
            f"returning {_TASK_COLUMNS}",
            (plan_revision_id, task_id, expected_state_token),
        ).fetchone()
    return None if row is None else _task_from_row(row)


def set_current_owner_gate(
    pool: DatabasePool,
    task_id: UUID,
    *,
    expected_state_token: UUID,
    owner_gate_id: UUID,
) -> Task | None:
    """Install a pending same-Task/Workspace OwnerGate as the current gate.

    A pending OwnerGate should never be superseded by another current gate:
    the normal lifecycle installs one pending gate as current, resolves it
    while current (which clears the pointer and rotates ``state_token``
    atomically), and only then installs another pending gate. The update is
    therefore conditional on ``current_owner_gate_id IS NULL`` — it can
    never replace an existing current pending gate — in addition to the
    usual ``expected_state_token``, non-archived, and one-fact-one-home
    conditions. The subquery requires the referenced gate to be ``pending``
    and to belong to the same Task and Workspace (the composite foreign key
    ``tasks_current_owner_gate_fk`` durably backstops the same-Task/
    Workspace scope; the pointer never duplicates the gate's type, subject,
    or outcome on the Task row).

    The update rotates the token like every other authoritative Task-state
    mutation. Returns the updated Task, or ``None`` when the token no
    longer matches, the Task is missing/already archived, a current gate is
    already installed, or the gate is missing/not pending (a stale
    operation that must not be retried blindly).
    """
    if not isinstance(owner_gate_id, UUID):
        raise TaskDomainError("owner_gate_id must be a UUID")
    with transaction(pool) as conn:
        row = conn.execute(
            "update openorc.tasks "
            "set current_owner_gate_id = %s, "
            "state_token = gen_random_uuid(), updated_at = now() "
            "where id = %s and state_token = %s and archived_at is null "
            "and current_owner_gate_id is null "
            "and exists ("
            "select 1 from openorc.owner_gates g "
            "where g.id = %s and g.task_id = tasks.id "
            "and g.workspace_id = tasks.workspace_id and g.status = 'pending') "
            f"returning {_TASK_COLUMNS}",
            (owner_gate_id, task_id, expected_state_token, owner_gate_id),
        ).fetchone()
    return None if row is None else _task_from_row(row)
