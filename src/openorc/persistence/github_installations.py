"""Repositories for GitHub App installation persistence (issue #57).

Explicit SQL repositories over the ``openorc`` schema for the Workspace-scoped
GitHub App installation records and their observation facts. Rows map to
transport-independent domain objects from
:mod:`openorc.domain.github_installations`; instants returned from Postgres are
normalized to timezone-aware UTC at this boundary.

Routing, not authorization: these repositories persist the deterministic
Workspace -> installation facts and the explicit Repository route. They make no
claim about current repository access or effective permissions — later GitHub
reconciliation/capability work establishes those. No credential material
(private keys, installation access tokens, PATs, human OAuth tokens) is ever
written or read here; GitHub App credentials are not OpenOrc application-table
state at all.

Violated database invariants surface as driver exceptions (for example
``psycopg.errors.UniqueViolation``, ``ForeignKeyViolation``, and
``CheckViolation``); translating them into typed application errors is a
service-layer concern, not a persistence one.
"""

from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime
from typing import Any
from uuid import UUID

from openorc.domain.github_installations import (
    GitHubInstallation,
    GitHubInstallationAccount,
    GitHubInstallationIdentity,
)
from openorc.persistence.pool import DatabasePool
from openorc.persistence.time import normalize_utc
from openorc.persistence.transactions import transaction

__all__ = [
    "claim_github_installation",
    "create_or_reconcile_github_installation",
    "delete_github_installation",
    "get_github_installation",
    "get_workspace_github_installation",
    "list_workspace_installations",
]

_GITHUB_INSTALLATION_COLUMNS = (
    "id, workspace_id, github_installation_id, github_account_id, account_login, "
    "account_type, suspended_at, created_at, updated_at"
)

# The Repository route foreign key (issue #57) is DEFERRABLE INITIALLY DEFERRED
# so deliberate root deletions (above all the ``auth.users`` account root,
# which cascades through every ownership path in one statement) can settle
# before the referential check. Releasing a repository-level transaction (or a
# SAVEPOINT, as repository calls run under in tests) does not check deferred
# constraints, so hard installation deletion forces exactly this constraint
# IMMEDIATE inside its own transaction: an installation still routed by a
# Repository is rejected before the operation returns, deterministically, in
# every caller context.
_ROUTE_REFERENCE_CONSTRAINTS = ("openorc.repositories_github_installation_id_workspace_id_fkey",)


def _github_installation_from_row(row: Sequence[Any]) -> GitHubInstallation:
    return GitHubInstallation(
        id=row[0],
        workspace_id=row[1],
        identity=GitHubInstallationIdentity(github_installation_id=row[2]),
        # ``account`` is the observed account identity block: a NULL account
        # triple is the issue #372 claim state (the Owner-submitted stable
        # installation identity recorded before any trusted GitHub
        # observation exists). The observation columns are written together
        # or not at all, so the triple is all-NULL or fully populated.
        account=(
            None
            if row[3] is None
            else GitHubInstallationAccount(
                github_account_id=row[3],
                login=row[4],
                type=row[5],
            )
        ),
        suspended_at=None if row[6] is None else normalize_utc(row[6]),
        created_at=normalize_utc(row[7]),
        updated_at=normalize_utc(row[8]),
    )


def create_or_reconcile_github_installation(
    pool: DatabasePool,
    *,
    workspace_id: UUID,
    identity: GitHubInstallationIdentity,
    account: GitHubInstallationAccount,
    suspended_at: datetime | None,
) -> GitHubInstallation:
    """Create or reconcile one Workspace installation record from trusted facts.

    The durable record identity is the ``(workspace_id, github_installation_id)``
    pair, and both stable external identifiers — the GitHub installation ID and
    the GitHub account ID — are never rewritten by reconciliation once
    established: an existing record keeps its OpenOrc UUID, creation time, and
    stored account ID, and receives only the mutable reported observations
    (login/type, ``suspended_at``). The one establishment exception is the
    issue #372 claim state: a claim-state record carries a NULL stable account
    identity until trusted facts arrive, and the upsert populates it then
    (coalesce-preserved for already-established rows) atomically with the
    first observation write. A reconcile that reports a different account ID
    than an established record leaves the stored stable identity intact;
    deciding the conflict belongs to the service boundary, not to persistence.
    No credential material is accepted or stored.

    ``suspended_at`` crosses into the TIMESTAMPTZ column only through the
    established UTC-normalization boundary: naive datetimes are rejected
    (:class:`~openorc.persistence.time.NaiveDatetimeError`) and aware values
    are normalized to UTC, so no session-timezone semantics can apply.

    The single ``insert ... on conflict do update`` statement is atomic and
    serializes concurrent reconciles on the unique index — a losing writer
    takes the update path after the winner commits — so no separate
    ``SELECT ... FOR UPDATE`` read is needed.
    """
    suspended_at = None if suspended_at is None else normalize_utc(suspended_at)
    with transaction(pool) as conn:
        row = conn.execute(
            "insert into openorc.github_installations "
            "(workspace_id, github_installation_id, github_account_id, "
            "account_login, account_type, suspended_at) "
            "values (%s, %s, %s, %s, %s, %s) "
            "on conflict (workspace_id, github_installation_id) do update "
            "set github_account_id = coalesce("
            "openorc.github_installations.github_account_id, "
            "excluded.github_account_id), "
            "account_login = excluded.account_login, "
            "account_type = excluded.account_type, "
            "suspended_at = excluded.suspended_at, "
            "updated_at = now() "
            f"returning {_GITHUB_INSTALLATION_COLUMNS}",
            (
                workspace_id,
                identity.github_installation_id,
                account.github_account_id,
                account.login,
                account.type,
                suspended_at,
            ),
        ).fetchone()
    assert row is not None
    return _github_installation_from_row(row)


def claim_github_installation(
    pool: DatabasePool, *, workspace_id: UUID, identity: GitHubInstallationIdentity
) -> GitHubInstallation:
    """Record one Owner installation claim as unverified configuration state.

    The claim command is the Owner-facing configuration entry (issue #372): it
    carries the stable external installation identity alone and can never
    carry or write observation facts. The row is created in claim state —
    every observation column NULL — or, when the ``(workspace_id,
    github_installation_id)`` record already exists (a re-asserted claim, a
    committed concurrent claim, or an already-established installation), the
    durable row is returned unchanged: a claim never updates an existing
    row's stable identity or observations. Only trusted GitHub reconciliation
    (:func:`create_or_reconcile_github_installation`) later populates the
    observation columns, atomically establishing the claim's stable account
    identity.
    """
    with transaction(pool) as conn:
        row = conn.execute(
            "insert into openorc.github_installations "
            "(workspace_id, github_installation_id, github_account_id, "
            "account_login, account_type, suspended_at) "
            "values (%s, %s, null, null, null, null) "
            "on conflict (workspace_id, github_installation_id) do nothing "
            f"returning {_GITHUB_INSTALLATION_COLUMNS}",
            (workspace_id, identity.github_installation_id),
        ).fetchone()
        if row is None:
            # The record already exists: the claim is idempotent, and the
            # durable row — in whatever state trusted reconciliation has
            # established — is the returned truth.
            row = conn.execute(
                f"select {_GITHUB_INSTALLATION_COLUMNS} "
                "from openorc.github_installations "
                "where workspace_id = %s and github_installation_id = %s",
                (workspace_id, identity.github_installation_id),
            ).fetchone()
    assert row is not None
    return _github_installation_from_row(row)


def get_github_installation(pool: DatabasePool, installation_id: UUID) -> GitHubInstallation | None:
    """Return one installation record by OpenOrc UUID, or ``None``."""
    with transaction(pool) as conn:
        row = conn.execute(
            f"select {_GITHUB_INSTALLATION_COLUMNS} "
            "from openorc.github_installations where id = %s",
            (installation_id,),
        ).fetchone()
    return None if row is None else _github_installation_from_row(row)


def get_workspace_github_installation(
    pool: DatabasePool, *, workspace_id: UUID, github_installation_id: int
) -> GitHubInstallation | None:
    """Return one Workspace installation record by stable external identity, or ``None``."""
    with transaction(pool) as conn:
        row = conn.execute(
            f"select {_GITHUB_INSTALLATION_COLUMNS} "
            "from openorc.github_installations "
            "where workspace_id = %s and github_installation_id = %s",
            (workspace_id, github_installation_id),
        ).fetchone()
    return None if row is None else _github_installation_from_row(row)


def list_workspace_installations(
    pool: DatabasePool, *, workspace_id: UUID
) -> list[GitHubInstallation]:
    """List the installation records of one Workspace in stable order."""
    with transaction(pool) as conn:
        rows = conn.execute(
            f"select {_GITHUB_INSTALLATION_COLUMNS} "
            "from openorc.github_installations "
            "where workspace_id = %s order by created_at, id",
            (workspace_id,),
        ).fetchall()
    return [_github_installation_from_row(row) for row in rows]


def delete_github_installation(
    pool: DatabasePool, installation_id: UUID
) -> GitHubInstallation | None:
    """Hard-delete one installation record under deliberately restrictive semantics.

    The installation record is Workspace-owned configuration, but a Repository
    route references it while the route is configured. The composite route
    foreign key is ``NO ACTION DEFERRABLE INITIALLY DEFERRED`` (never
    cascading): it is checked here explicitly so a routed installation is
    rejected with ``ForeignKeyViolation`` before this operation returns —
    removing an installation record can never silently un-route Repositories
    (unbinding the route is the explicit operation). Deleting a Workspace
    removes its installation records through the ordered Workspace-aggregate
    deletion after Repositories are gone.

    Returns the deleted installation, or ``None`` when it does not exist.
    """
    with transaction(pool) as conn:
        row = conn.execute(
            f"delete from openorc.github_installations where id = %s "
            f"returning {_GITHUB_INSTALLATION_COLUMNS}",
            (installation_id,),
        ).fetchone()
        if row is None:
            return None
        # Check the (deferred) route constraint now, narrowly: an installation
        # still referenced by a Repository route must be rejected before this
        # operation returns, both in production transactions and under the
        # rollback-based integration harness where no commit ever fires the
        # deferred check.
        conn.execute("set constraints " + ", ".join(_ROUTE_REFERENCE_CONSTRAINTS) + " immediate")
    return _github_installation_from_row(row)
