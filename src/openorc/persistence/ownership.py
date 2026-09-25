"""Repositories for the ownership and repository-identity foundation.

Explicit SQL repositories over the ``openorc`` schema for Profile, Workspace,
Project, and Repository (Phase 1). Rows map to transport-independent domain
objects from :mod:`openorc.domain.ownership`; instants returned from Postgres
are normalized to timezone-aware UTC at this boundary.

Repository routing (issue #57): ``set_repository_installation_route`` sets or
clears one Repository's explicit route to a Workspace-scoped GitHubInstallation
record. The composite route foreign key makes cross-Workspace routing a
driver-level impossibility, and this module stays policy-free — durable
representation, not product semantics.

Violated database invariants surface as driver exceptions (for example
``psycopg.errors.UniqueViolation`` and ``ForeignKeyViolation``); translating
them into typed application errors is a service-layer concern, not a
persistence one.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any
from uuid import UUID

from openorc.domain.ownership import (
    AccountDeletionAttemptState,
    GitHubRepositoryIdentity,
    Profile,
    Project,
    Repository,
    RepositoryMetadata,
    Workspace,
)
from openorc.persistence.pool import DatabasePool
from openorc.persistence.time import normalize_utc
from openorc.persistence.transactions import transaction

__all__ = [
    "clear_account_deletion_attempt",
    "claim_account_deletion_attempt",
    "create_profile",
    "create_project",
    "create_repository",
    "create_workspace",
    "ensure_profile",
    "find_repository_by_github_identity",
    "get_profile",
    "get_profile_for_account_deletion",
    "get_project",
    "get_repository",
    "get_workspace",
    "get_workspace_for_update",
    "mark_account_deletion_attempt_uncertain",
    "read_account_deletion_state_for_key_share",
    "reclaim_expired_account_deletion_attempt",
    "reclaim_uncertain_account_deletion_attempt",
    "set_repository_installation_route",
    "update_repository_metadata",
    "update_workspace_guidance",
    "update_workspace_review_iteration_limit",
]

# The full Workspace column list, including the first-class configuration
# settings added by issue #53 (review_iteration_limit, guidance).
_WORKSPACE_COLUMNS = (
    "id, owner_profile_id, name, created_at, updated_at, review_iteration_limit, guidance"
)

# The full Repository column list, including the issue #57 explicit
# GitHub-installation route (nullable: unconfigured historical records).
_REPOSITORY_COLUMNS = (
    "id, project_id, workspace_id, github_repository_id, owner_login, name, "
    "html_url, is_private, default_branch, created_at, updated_at, "
    "github_installation_id"
)


def _profile_from_row(row: Sequence[Any]) -> Profile:
    return Profile(id=row[0], created_at=normalize_utc(row[1]))


def _workspace_from_row(row: Sequence[Any]) -> Workspace:
    return Workspace(
        id=row[0],
        owner_profile_id=row[1],
        name=row[2],
        created_at=normalize_utc(row[3]),
        updated_at=normalize_utc(row[4]),
        review_iteration_limit=row[5],
        guidance=row[6],
    )


def _project_from_row(row: Sequence[Any]) -> Project:
    return Project(
        id=row[0],
        workspace_id=row[1],
        name=row[2],
        created_at=normalize_utc(row[3]),
        updated_at=normalize_utc(row[4]),
    )


def _repository_from_row(row: Sequence[Any]) -> Repository:
    return Repository(
        id=row[0],
        project_id=row[1],
        workspace_id=row[2],
        identity=GitHubRepositoryIdentity(github_repository_id=row[3]),
        metadata=RepositoryMetadata(
            owner_login=row[4],
            name=row[5],
            html_url=row[6],
            is_private=row[7],
            default_branch=row[8],
        ),
        created_at=normalize_utc(row[9]),
        updated_at=normalize_utc(row[10]),
        github_installation_id=row[11],
    )


def create_profile(pool: DatabasePool, *, profile_id: UUID) -> Profile:
    """Insert a Profile whose id is the corresponding Supabase Auth user UUID.

    The caller supplies the identity; the database never generates one.
    Inserting the same UUID twice violates the primary key (Profile/Auth is
    1:1).
    """
    with transaction(pool) as conn:
        row = conn.execute(
            "insert into openorc.profiles (id) values (%s) returning id, created_at",
            (profile_id,),
        ).fetchone()
    assert row is not None
    return _profile_from_row(row)


def get_profile(pool: DatabasePool, profile_id: UUID) -> Profile | None:
    """Return the Profile with the given Supabase Auth user UUID, or ``None``."""
    with transaction(pool) as conn:
        row = conn.execute(
            "select id, created_at from openorc.profiles where id = %s",
            (profile_id,),
        ).fetchone()
    return None if row is None else _profile_from_row(row)


def ensure_profile(pool: DatabasePool, *, profile_id: UUID) -> Profile:
    """Resolve or idempotently bootstrap the Profile for one Auth user UUID.

    ``insert ... on conflict (id) do nothing`` converges concurrent first
    requests for the same valid Supabase user onto exactly one Profile: the
    winner's insert commits, the loser's insert no-ops, and the fallback
    ``select`` (a fresh statement snapshot under ``READ COMMITTED``) returns
    the winner's row. Repeated calls are idempotent and never surface a
    duplicate-key error.

    Fail-closed account-identity boundary: ``openorc.profiles.id`` references
    ``auth.users (id) ON DELETE CASCADE`` (the single sanctioned Auth
    boundary, issue #27). If the backing Auth user row does not exist — for
    example, a previously issued but still cryptographically valid JWT whose
    Auth user has been permanently deleted — the insert raises
    ``psycopg.errors.ForeignKeyViolation``. Translating that into a typed
    authentication failure (never re-creating the identity) is the service
    layer's concern, not a persistence one.
    """
    with transaction(pool) as conn:
        row = conn.execute(
            "insert into openorc.profiles (id) values (%s) "
            "on conflict (id) do nothing "
            "returning id, created_at",
            (profile_id,),
        ).fetchone()
        if row is None:
            # Another request created (or is committing) this Profile; the
            # on-conflict insert lost the race, so read the existing row.
            row = conn.execute(
                "select id, created_at from openorc.profiles where id = %s",
                (profile_id,),
            ).fetchone()
    assert row is not None
    return _profile_from_row(row)


def create_workspace(pool: DatabasePool, *, owner_profile_id: UUID, name: str) -> Workspace:
    """Insert a Workspace owned by exactly one Profile.

    The first-class Workspace configuration settings are not insert inputs:
    the durable column defaults supply them, so a fresh Workspace carries the
    configured review-iteration boundary (``review_iteration_limit`` default
    5) and blank guidance unless explicitly changed afterwards.
    """
    with transaction(pool) as conn:
        row = conn.execute(
            "insert into openorc.workspaces (owner_profile_id, name) "
            "values (%s, %s) "
            f"returning {_WORKSPACE_COLUMNS}",
            (owner_profile_id, name),
        ).fetchone()
    assert row is not None
    return _workspace_from_row(row)


def get_workspace(pool: DatabasePool, workspace_id: UUID) -> Workspace | None:
    """Return one Workspace with its configuration settings, or ``None``."""
    with transaction(pool) as conn:
        row = conn.execute(
            f"select {_WORKSPACE_COLUMNS} from openorc.workspaces where id = %s",
            (workspace_id,),
        ).fetchone()
    return None if row is None else _workspace_from_row(row)


def get_project(pool: DatabasePool, project_id: UUID) -> Project | None:
    """Return one Project by id, or ``None`` when it does not exist."""
    with transaction(pool) as conn:
        row = conn.execute(
            "select id, workspace_id, name, created_at, updated_at "
            "from openorc.projects where id = %s",
            (project_id,),
        ).fetchone()
    return None if row is None else _project_from_row(row)


def update_workspace_review_iteration_limit(
    pool: DatabasePool, workspace_id: UUID, *, review_iteration_limit: int
) -> tuple[Workspace, int, bool] | None:
    """Set the Workspace review-loop iteration limit (issue #53).

    One deliberate ``SELECT ... FOR UPDATE`` inside one short transaction
    captures the exact before-state, then the write happens only when the
    value actually differs. The locked same-transaction previous/new facts
    are the safe audit handoff for a consequential configuration-change
    event (#56) — the caller never re-reads a racy before-state. The change
    affects future ReviewLoops only: ``ReviewLoop.iteration_limit`` values
    stored on existing loops are immutable historical configuration and are
    never touched.

    Returns ``(updated Workspace, previous limit, changed)``; a no-op
    returns the unchanged Workspace with ``changed=False`` and does not
    advance ``updated_at``. Returns ``None`` when the Workspace does not
    exist.
    """
    with transaction(pool) as conn:
        row = conn.execute(
            f"select {_WORKSPACE_COLUMNS} from openorc.workspaces where id = %s for update",
            (workspace_id,),
        ).fetchone()
        if row is None:
            return None
        previous = _workspace_from_row(row)
        if previous.review_iteration_limit == review_iteration_limit:
            return previous, previous.review_iteration_limit, False
        updated_row = conn.execute(
            "update openorc.workspaces "
            "set review_iteration_limit = %s, updated_at = now() "
            "where id = %s "
            f"returning {_WORKSPACE_COLUMNS}",
            (review_iteration_limit, workspace_id),
        ).fetchone()
    assert updated_row is not None
    return _workspace_from_row(updated_row), previous.review_iteration_limit, True


def update_workspace_guidance(
    pool: DatabasePool, workspace_id: UUID, *, guidance: str
) -> tuple[Workspace, str, bool] | None:
    """Set the Workspace guidance prose (issue #53).

    Guidance is one current, Owner-authored value: the write replaces the
    current value and creates no history, hash, snapshot, or revision. The
    same deliberate ``SELECT ... FOR UPDATE`` plus conditional-write shape as
    the review-limit update provides the exact locked before-state for the
    #56 audit handoff — the previous prose is returned to the caller but the
    guidance-change event needs only the semantic fact that the setting
    changed, never the prose itself. Returns ``(updated Workspace, previous
    guidance, changed)``; a no-op returns the unchanged Workspace with
    ``changed=False``. Returns ``None`` when the Workspace does not exist.
    """
    with transaction(pool) as conn:
        row = conn.execute(
            f"select {_WORKSPACE_COLUMNS} from openorc.workspaces where id = %s for update",
            (workspace_id,),
        ).fetchone()
        if row is None:
            return None
        previous = _workspace_from_row(row)
        if previous.guidance == guidance:
            return previous, previous.guidance, False
        updated_row = conn.execute(
            "update openorc.workspaces "
            "set guidance = %s, updated_at = now() "
            "where id = %s "
            f"returning {_WORKSPACE_COLUMNS}",
            (guidance, workspace_id),
        ).fetchone()
    assert updated_row is not None
    return _workspace_from_row(updated_row), previous.guidance, True


def create_project(pool: DatabasePool, *, workspace_id: UUID, name: str) -> Project:
    """Insert a Project belonging to one Workspace."""
    with transaction(pool) as conn:
        row = conn.execute(
            "insert into openorc.projects (workspace_id, name) "
            "values (%s, %s) "
            "returning id, workspace_id, name, created_at, updated_at",
            (workspace_id, name),
        ).fetchone()
    assert row is not None
    return _project_from_row(row)


def create_repository(
    pool: DatabasePool,
    *,
    project_id: UUID,
    workspace_id: UUID,
    identity: GitHubRepositoryIdentity,
    metadata: RepositoryMetadata,
) -> Repository:
    """Insert one canonical Repository record for a GitHub repository identity.

    A second record binding the same GitHub repository identity to the same
    Workspace violates the ``(workspace_id, github_repository_id)`` unique
    constraint; the same identity in a different Workspace is a distinct,
    independent record.
    """
    with transaction(pool) as conn:
        row = conn.execute(
            "insert into openorc.repositories "
            "(project_id, workspace_id, github_repository_id, owner_login, name, "
            "html_url, is_private, default_branch) "
            "values (%s, %s, %s, %s, %s, %s, %s, %s) "
            f"returning {_REPOSITORY_COLUMNS}",
            (
                project_id,
                workspace_id,
                identity.github_repository_id,
                metadata.owner_login,
                metadata.name,
                metadata.html_url,
                metadata.is_private,
                metadata.default_branch,
            ),
        ).fetchone()
    assert row is not None
    return _repository_from_row(row)


def get_repository(pool: DatabasePool, repository_id: UUID) -> Repository | None:
    with transaction(pool) as conn:
        row = conn.execute(
            f"select {_REPOSITORY_COLUMNS} from openorc.repositories where id = %s",
            (repository_id,),
        ).fetchone()
    return None if row is None else _repository_from_row(row)


def get_repository_for_update(pool: DatabasePool, repository_id: UUID) -> Repository | None:
    """Return one Repository row locked ``FOR UPDATE``, or ``None``.

    The deliberate row lock is the serialized-reconciliation read (issue
    #59): the current durable route and metadata must be inspected under
    lock before any consequential write, so a concurrent route rebind or
    metadata change either precedes the locked read (its effect is visible)
    or follows it (blocked until the caller's transaction commits).
    """
    with transaction(pool) as conn:
        row = conn.execute(
            f"select {_REPOSITORY_COLUMNS} from openorc.repositories where id = %s for update",
            (repository_id,),
        ).fetchone()
    return None if row is None else _repository_from_row(row)


def find_repository_by_github_identity(
    pool: DatabasePool, *, workspace_id: UUID, identity: GitHubRepositoryIdentity
) -> Repository | None:
    with transaction(pool) as conn:
        row = conn.execute(
            f"select {_REPOSITORY_COLUMNS} "
            "from openorc.repositories "
            "where workspace_id = %s and github_repository_id = %s",
            (workspace_id, identity.github_repository_id),
        ).fetchone()
    return None if row is None else _repository_from_row(row)


def update_repository_metadata(
    pool: DatabasePool, repository_id: UUID, *, metadata: RepositoryMetadata
) -> Repository | None:
    """Replace the mutable observed metadata of one Repository record.

    Identity is never touched: the record keeps its OpenOrc UUID and its stable
    external GitHub repository identity, and ``updated_at`` advances to the
    database clock. Returns ``None`` when the repository does not exist.
    """
    with transaction(pool) as conn:
        row = conn.execute(
            "update openorc.repositories "
            "set owner_login = %s, name = %s, html_url = %s, is_private = %s, "
            "default_branch = %s, updated_at = now() "
            "where id = %s "
            f"returning {_REPOSITORY_COLUMNS}",
            (
                metadata.owner_login,
                metadata.name,
                metadata.html_url,
                metadata.is_private,
                metadata.default_branch,
                repository_id,
            ),
        ).fetchone()
    return None if row is None else _repository_from_row(row)


def set_repository_installation_route(
    pool: DatabasePool,
    *,
    repository_id: UUID,
    workspace_id: UUID,
    github_installation_id: UUID | None,
) -> Repository | None:
    """Set or clear one Repository's explicit GitHub installation route (issue #57).

    ``github_installation_id`` is the OpenOrc GitHubInstallation record UUID
    the Repository is routed to; ``None`` clears the route — valid historical /
    configuration state that is not usable for GitHub operations. A single
    nullable column makes multiple competing routes unrepresentable; the
    composite route foreign key durably rejects a route into another Workspace
    as ``ForeignKeyViolation``, and translating driver exceptions into typed
    application errors is a service-layer concern, not a persistence one. The
    Repository's stable GitHub identity is never touched by this write.
    Returns the updated Repository, or ``None`` when the Repository does not
    exist within the given direct Workspace scope.
    """
    with transaction(pool) as conn:
        row = conn.execute(
            "update openorc.repositories "
            "set github_installation_id = %s, updated_at = now() "
            "where id = %s and workspace_id = %s "
            f"returning {_REPOSITORY_COLUMNS}",
            (github_installation_id, repository_id, workspace_id),
        ).fetchone()
    return None if row is None else _repository_from_row(row)


def get_workspace_for_update(pool: DatabasePool, workspace_id: UUID) -> Workspace | None:
    """Row-locked read of one Workspace (issue #97 administrative deletion).

    The deliberate ``SELECT ... FOR UPDATE`` on the Workspace root is the
    aggregate-deletion barrier: a concurrent child ``INSERT`` into any table
    foreign-keyed to this Workspace (connections, projects, repositories,
    tasks) takes a conflicting ``FOR KEY SHARE`` lock on this row during its
    foreign-key check, so no child row can enter the aggregate after the
    locked read — and before the aggregate deletion — commits. The lock is
    held within the caller's composed transaction only.
    """
    with transaction(pool) as conn:
        row = conn.execute(
            f"select {_WORKSPACE_COLUMNS} from openorc.workspaces where id = %s for update",
            (workspace_id,),
        ).fetchone()
    return None if row is None else _workspace_from_row(row)


# Durable account-deletion attempt state (issue #97): three nullable Profile
# columns bound together by the migration's composite CHECK — either every
# column is NULL (normal operation) or the state is 'active'/'uncertain' with
# both the attempt UUID and the database-clock establishment time non-NULL.
# Every mutation below is a conditional write guarded by the exact attempt
# UUID it owns (or the exact state tuple it transitions), so one invocation
# can never clear or move another invocation's protection. All comparisons
# involving time use the database clock, never the application clock.
_ATTEMPT_STATE_COLUMNS = (
    "account_deletion_state, account_deletion_attempt_id, account_deletion_started_at"
)


def _attempt_state_from_row(
    profile_id: UUID, row: Sequence[Any]
) -> AccountDeletionAttemptState | None:
    if row[0] is None:
        # Normal operation: the composite CHECK keeps the whole tuple NULL.
        return None
    return AccountDeletionAttemptState(
        profile_id=profile_id,
        state=row[0],
        attempt_id=row[1],
        started_at=normalize_utc(row[2]),
    )


def read_account_deletion_state_for_key_share(
    pool: DatabasePool, *, profile_id: UUID
) -> tuple[bool, AccountDeletionAttemptState | None]:
    """Locked-but-compatible read of one Profile's deletion-attempt state.

    ``SELECT ... FOR KEY SHARE`` is the Owner-mutation barrier read (issue
    #97): it conflicts with the account-deletion transaction's Profile-root
    ``FOR UPDATE`` — so a guarded mutation concurrent with that transaction
    blocks until the revocation commit and then re-reads the committed state
    under its lock — while remaining compatible with other guarded mutations
    and with every ordinary read. Returns ``(profile_exists, state)``; a
    present state row carries the attempt identity the caller's fail-closed
    decision is made on.
    """
    with transaction(pool) as conn:
        row = conn.execute(
            f"select {_ATTEMPT_STATE_COLUMNS} from openorc.profiles where id = %s for key share",
            (profile_id,),
        ).fetchone()
    if row is None:
        return False, None
    return True, _attempt_state_from_row(profile_id, row)


def get_profile_for_account_deletion(
    pool: DatabasePool, *, profile_id: UUID, lease_seconds: float
) -> tuple[Profile, AccountDeletionAttemptState | None, bool] | None:
    """Profile-root locked read of the deletion-attempt state and its lease.

    ``SELECT ... FOR UPDATE`` on the Profile row is the account-deletion
    transaction's anchor: it conflicts with the guard's ``FOR KEY SHARE``
    reads, so a concurrent guarded Owner mutation either commits before this
    read (its effects are then visible to the revocation below) or blocks and
    re-reads the committed state after the revocation commits. The
    ``lease_expired`` flag compares ``account_deletion_started_at`` plus the
    documented lease interval against the database clock inside the same
    statement — an expired 'active' attempt is an abandoned attempt to be
    recovered through reconciliation, never replayed blindly. Returns ``None``
    when the Profile does not exist (the caller decides absent semantics).
    """
    with transaction(pool) as conn:
        row = conn.execute(
            "select id, created_at, "
            "account_deletion_state, account_deletion_attempt_id, account_deletion_started_at, "
            "(account_deletion_state is not null and account_deletion_state = 'active' "
            "and account_deletion_started_at + make_interval(secs => %s) < now()) "
            "as lease_expired "
            "from openorc.profiles where id = %s for update",
            (lease_seconds, profile_id),
        ).fetchone()
    if row is None:
        return None
    profile = _profile_from_row((row[0], row[1]))
    state = _attempt_state_from_row(profile_id, (row[2], row[3], row[4]))
    return profile, state, bool(row[5])


def claim_account_deletion_attempt(
    pool: DatabasePool, *, profile_id: UUID, attempt_id: UUID
) -> bool:
    """Claim the normal→'active' transition with a fresh attempt UUID.

    Conditional on a completely NULL state tuple (normal operation), so a
    concurrent claim that won the race is never overwritten. Returns whether
    the claim applied.
    """
    with transaction(pool) as conn:
        row = conn.execute(
            "update openorc.profiles "
            "set account_deletion_state = 'active', account_deletion_attempt_id = %s, "
            "account_deletion_started_at = now() "
            "where id = %s and account_deletion_state is null "
            "and account_deletion_attempt_id is null and account_deletion_started_at is null "
            "returning 1",
            (attempt_id, profile_id),
        ).fetchone()
    return row is not None


def reclaim_expired_account_deletion_attempt(
    pool: DatabasePool,
    *,
    profile_id: UUID,
    expired_attempt_id: UUID,
    new_attempt_id: UUID,
    lease_seconds: float,
) -> bool:
    """Compare-and-swap an EXPIRED 'active' attempt into a fresh 'active' one.

    Conditional on the exact abandoned attempt UUID, the 'active' state, and
    the database-clock lease expiry — a fresh concurrent attempt is never
    displaced and a state that moved on matches zero rows (the caller then
    reloads and reclassifies). Composed with the revocation re-run inside one
    short transaction after reconciliation confirmed the Auth user present.
    """
    with transaction(pool) as conn:
        row = conn.execute(
            "update openorc.profiles "
            "set account_deletion_attempt_id = %s, account_deletion_started_at = now() "
            "where id = %s and account_deletion_state = 'active' "
            "and account_deletion_attempt_id = %s "
            "and account_deletion_started_at + make_interval(secs => %s) < now() "
            "returning 1",
            (new_attempt_id, profile_id, expired_attempt_id, lease_seconds),
        ).fetchone()
    return row is not None


def reclaim_uncertain_account_deletion_attempt(
    pool: DatabasePool, *, profile_id: UUID, reconciled_attempt_id: UUID, new_attempt_id: UUID
) -> bool:
    """Compare-and-swap the EXACT reconciled 'uncertain' attempt into 'active'.

    Conditional on ``account_deletion_state = 'uncertain'`` AND the exact
    attempt UUID that was reconciled through the Admin read surface: a
    reconciliation result is bound to the attempt it reconciled, and only
    that attempt may be replaced by a fresh replay claim — a second retrier
    that reconciled an older attempt must never consume a newer attempt's
    'uncertain' state. Returns whether the compare-and-swap applied; zero
    rows means the durable state moved on and the caller reloads and
    reclassifies without using the stale reconciliation.
    """
    with transaction(pool) as conn:
        row = conn.execute(
            "update openorc.profiles "
            "set account_deletion_state = 'active', account_deletion_attempt_id = %s, "
            "account_deletion_started_at = now() "
            "where id = %s and account_deletion_state = 'uncertain' "
            "and account_deletion_attempt_id = %s "
            "returning 1",
            (new_attempt_id, profile_id, reconciled_attempt_id),
        ).fetchone()
    return row is not None


def mark_account_deletion_attempt_uncertain(
    pool: DatabasePool, *, profile_id: UUID, attempt_id: UUID
) -> bool:
    """Attempt-scoped transition of the owning 'active' attempt to 'uncertain'.

    Persisted only when reconciliation of an unknown Auth outcome itself
    cannot determine existence. Conditional on the exact attempt UUID so a
    foreign or moved-on attempt is never overwritten. Returns whether the
    transition applied.
    """
    with transaction(pool) as conn:
        row = conn.execute(
            "update openorc.profiles "
            "set account_deletion_state = 'uncertain' "
            "where id = %s and account_deletion_state = 'active' "
            "and account_deletion_attempt_id = %s "
            "returning 1",
            (profile_id, attempt_id),
        ).fetchone()
    return row is not None


def clear_account_deletion_attempt(
    pool: DatabasePool, *, profile_id: UUID, attempt_id: UUID
) -> bool:
    """Attempt-scoped clear of the owning 'active' attempt back to normal.

    Conditional on the exact attempt UUID and the 'active' state, so one
    invocation can never clear another invocation's protection (and an
    'uncertain' state is never cleared by a stale failure handler). Returns
    whether the clear applied.
    """
    with transaction(pool) as conn:
        row = conn.execute(
            "update openorc.profiles "
            "set account_deletion_state = null, account_deletion_attempt_id = null, "
            "account_deletion_started_at = null "
            "where id = %s and account_deletion_state = 'active' "
            "and account_deletion_attempt_id = %s "
            "returning 1",
            (profile_id, attempt_id),
        ).fetchone()
    return row is not None
