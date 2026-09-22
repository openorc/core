"""Deterministic tests for the permanent account-deletion lifecycle (issue #97).

The ordinary suite cannot execute Postgres or call live Supabase Auth: canned
rows, a scripted fake connection seam, and a fully scripted fake Auth Admin
client prove the durable attempt state machine — the revocation-then-claim
composition, the no-transaction external-call boundary,
reconcile-before-replay, compare-and-swap on the exact reconciled attempt,
attempt-scoped recovery transitions, the bounded active-attempt lease, the
Profile-absent idempotent path, and the post-commit/pre-Auth-call revocation
window. Durable cascade and constraint behavior is proven by the
integration-marked suite.
"""

from __future__ import annotations

import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime
from typing import Any, cast
from uuid import UUID

import pytest

from openorc.adapters.supabase import (
    SupabaseAuthAdminOutcomeUnknownError,
    SupabaseAuthAdminRejectedError,
    SupabaseAuthAdminUserAbsentError,
)
from openorc.persistence import runtime_control_secrets
from openorc.persistence.connections import get_connection_for_update
from openorc.persistence.pool import DatabasePool
from openorc.services import account_lifecycle
from openorc.services.account_lifecycle import AccountDeletionOutcome
from openorc.services.errors import (
    ConflictError,
    ExternalOperationFailedError,
    ExternalOperationUncertainError,
    NotFoundError,
)
from openorc.services.profile_lifecycle_guard import require_account_operational
from openorc.services.transaction_composition import composed_transaction
from openorc.services.workspace_authorization import require_profile_workspace

_OBSERVED = datetime(2026, 9, 22, 12, 0, 0, tzinfo=UTC)


class FakeCursor:
    """Returns one canned row (or canned rows), like a psycopg cursor."""

    def __init__(self, row: tuple[Any, ...] | None, rows: list[tuple[Any, ...]] | None = None):
        self._row = row
        self._rows = rows

    def fetchone(self) -> tuple[Any, ...] | None:
        return self._row

    def fetchall(self) -> list[tuple[Any, ...]]:
        if self._rows is None:
            return [] if self._row is None else [self._row]
        return self._rows


class ScriptedConnection:
    """Plays back canned statement results in order, recording executed SQL.

    ``transaction_depth`` lets tests prove no database transaction is open
    while an external Auth Admin call runs (the #51 external-I/O rule).
    """

    def __init__(self, results: list[tuple[Any, ...] | None | list[Any] | Exception]) -> None:
        self.results = list(results)
        self.executed: list[tuple[str, tuple[Any, ...] | None]] = []
        self.transaction_depth = 0

    def execute(self, sql: str, params: tuple[Any, ...] | None = None) -> FakeCursor:
        self.executed.append((sql, params))
        result = self.results.pop(0)
        if isinstance(result, Exception):
            raise result
        if isinstance(result, list):
            return FakeCursor(None, result)
        return FakeCursor(result)  # type: ignore[arg-type]

    @contextmanager
    def transaction(self) -> Iterator[None]:
        self.transaction_depth += 1
        try:
            yield
        finally:
            self.transaction_depth -= 1


class FakePool:
    """Emulates psycopg_pool ConnectionPool.connection() semantics."""

    def __init__(self, conn: ScriptedConnection) -> None:
        self._conn = conn

    def connection(self) -> Any:
        @contextmanager
        def managed() -> Iterator[ScriptedConnection]:
            yield self._conn

        return managed()

    def close(self) -> None:
        raise AssertionError("account lifecycle tests never close pools")


def fetch_user(self, user_id: UUID) -> bool:
    self.fetch_calls.append(user_id)
    outcome = self.fetch_results.pop(0)
    if isinstance(outcome, Exception):
        raise outcome
    return cast(bool, outcome)


def _pool(conn: ScriptedConnection) -> DatabasePool:
    return cast(DatabasePool, FakePool(conn))


def _connection_row(
    workspace_id: Any, *, auth_reference: str | None, enabled: bool = True
) -> tuple[Any, ...]:
    return (
        uuid.uuid4(),
        workspace_id,
        "cline",
        "runtime",
        {},
        1,
        enabled,
        auth_reference,
        None,
        None,
        _OBSERVED,
        _OBSERVED,
    )


def _profile_attempt_row(
    profile_id: Any,
    *,
    state: str | None = None,
    attempt_id: Any = None,
    lease_expired: bool = False,
) -> tuple[Any, ...]:
    # The locked account-deletion read: id, created_at, state, attempt UUID,
    # establishment time, and the database-clock lease-expiry flag.
    return (
        profile_id,
        _OBSERVED,
        state,
        attempt_id,
        _OBSERVED if state is not None else None,
        lease_expired,
    )


def _reference() -> str:
    return runtime_control_secrets.encode_connection_auth_reference(uuid.uuid4())


class FakeAdminClient:
    """Scripted Supabase Auth Admin boundary for the deletion lifecycle.

    Queued outcomes drive each call: ``None`` is success, an exception
    instance is raised once, and a ``False`` delete outcome raises the
    adapter's confirmed-absent error. ``on_delete`` runs inside
    ``delete_user`` so tests can exercise the post-revocation-commit window.
    """

    def __init__(self, *, timeout_seconds: float = 5.0) -> None:
        self._timeout_seconds = timeout_seconds
        self.delete_results: list[None | bool | Exception] = []
        self.fetch_results: list[bool | Exception] = []
        self.delete_calls: list[UUID] = []
        self.fetch_calls: list[UUID] = []
        self.on_delete: Any = None

    @property
    def request_timeout_seconds(self) -> float:
        return self._timeout_seconds

    def delete_user(self, user_id: UUID) -> None:
        self.delete_calls.append(user_id)
        if self.on_delete is not None:
            self.on_delete()
        outcome = self.delete_results.pop(0)
        if isinstance(outcome, Exception):
            raise outcome
        if outcome is False:
            raise SupabaseAuthAdminUserAbsentError("the user is already absent")

    def fetch_user(self, user_id: UUID) -> bool:
        self.fetch_calls.append(user_id)
        outcome = self.fetch_results.pop(0)
        if isinstance(outcome, Exception):
            raise outcome
        return cast(bool, outcome)


def test_first_invocation_revokes_credentials_then_deletes_the_account() -> None:
    profile_id = uuid.uuid4()
    workspace_id = uuid.uuid4()
    reference = _reference()
    conn = ScriptedConnection(
        [
            _profile_attempt_row(profile_id),  # locked read: no attempt state
            (1,),  # attempt claim
            [_connection_row(workspace_id, auth_reference=reference)],  # enumeration
            (1,),  # Vault secret delete
            _connection_row(workspace_id, auth_reference=None),  # disconnect
        ]
    )
    admin = FakeAdminClient()
    admin.delete_results.append(None)
    depth_at_delete: list[int] = []
    sql_count_at_delete: list[int] = []

    def on_delete() -> None:
        # The external call runs with NO database transaction open, after the
        # full revocation composition.
        depth_at_delete.append(conn.transaction_depth)
        sql_count_at_delete.append(len(conn.executed))

    admin.on_delete = on_delete

    outcome = account_lifecycle.delete_account(_pool(conn), admin, profile_id=profile_id)

    assert outcome == AccountDeletionOutcome(profile_id=profile_id, result="deleted")
    assert depth_at_delete == [0]
    # Revocation-before-Auth-call ordering: the claim, Vault delete, and
    # disconnect all ran before the external delete was issued.
    assert sql_count_at_delete == [5]
    sqls = [sql for sql, _ in conn.executed]
    assert "for update" in sqls[0] and "openorc.profiles" in sqls[0]
    claim_sql, claim_params = conn.executed[1]
    assert "account_deletion_state = 'active'" in claim_sql
    assert all(
        "vault.secrets" in sql or "update openorc.connections" in sql
        for sql, _ in conn.executed[3:5]
    )
    assert admin.fetch_calls == []
    assert admin.delete_calls == [profile_id]


def test_first_invocation_with_no_connections_still_claims_and_deletes() -> None:
    profile_id = uuid.uuid4()
    conn = ScriptedConnection(
        [
            _profile_attempt_row(profile_id),
            (1,),
            [],  # zero Connections across the Profile's Workspaces
        ]
    )
    admin = FakeAdminClient()
    admin.delete_results.append(None)

    outcome = account_lifecycle.delete_account(_pool(conn), admin, profile_id=profile_id)

    assert outcome == AccountDeletionOutcome(profile_id=profile_id, result="deleted")
    assert all("vault" not in sql for sql, _ in conn.executed)
    assert admin.delete_calls == [profile_id]


def test_revocation_fails_closed_on_an_unrecognized_reference() -> None:
    profile_id = uuid.uuid4()
    workspace_id = uuid.uuid4()
    malformed = "vault://openorc/connection-auth/abc"
    conn = ScriptedConnection(
        [
            _profile_attempt_row(profile_id),
            (1,),  # claim
            [_connection_row(workspace_id, auth_reference=malformed)],
        ]
    )
    admin = FakeAdminClient()

    with pytest.raises(ConflictError) as error:
        account_lifecycle.delete_account(_pool(conn), admin, profile_id=profile_id)

    # Fail closed: nothing external was issued, and the error message carries
    # no reference or credential material.
    assert admin.delete_calls == []
    assert admin.fetch_calls == []
    assert malformed not in str(error.value)


def test_profile_absent_entry_reconciles_through_the_read_surface_only() -> None:
    profile_id = uuid.uuid4()
    # A prior successful Auth deletion cascade-removed the Profile: no
    # revocation, no barrier, no destructive call — reconcile first.
    conn = ScriptedConnection([None])  # locked read: Profile absent
    admin = FakeAdminClient()
    admin.fetch_results.append(False)  # confirmed absent

    outcome = account_lifecycle.delete_account(_pool(conn), admin, profile_id=profile_id)

    assert outcome == AccountDeletionOutcome(profile_id=profile_id, result="already_absent")
    assert admin.fetch_calls == [profile_id]
    assert admin.delete_calls == []  # never a blind destructive call
    assert len(conn.executed) == 1


def test_profile_absent_entry_fails_closed_when_the_auth_user_is_present() -> None:
    profile_id = uuid.uuid4()
    conn = ScriptedConnection([None])  # locked read: Profile absent
    admin = FakeAdminClient()
    admin.fetch_results.append(True)  # confirmed present

    with pytest.raises(ConflictError, match="inconsistent state"):
        account_lifecycle.delete_account(_pool(conn), admin, profile_id=profile_id)

    assert admin.delete_calls == []  # never delete an anchor-less identity
    assert admin.fetch_calls == [profile_id]


def test_profile_absent_entry_surfaces_undeterminable_existence() -> None:
    for lookup_failure in [
        SupabaseAuthAdminRejectedError("rejected"),
        SupabaseAuthAdminOutcomeUnknownError("unknown"),
    ]:
        conn = ScriptedConnection([None])
        admin = FakeAdminClient()
        admin.fetch_results.append(lookup_failure)

        with pytest.raises(ExternalOperationUncertainError):
            account_lifecycle.delete_account(_pool(conn), admin, profile_id=uuid.uuid4())
        assert admin.delete_calls == []


def test_a_fresh_active_attempt_is_single_flight_without_any_external_call() -> None:
    profile_id = uuid.uuid4()
    live_attempt = uuid.uuid4()
    conn = ScriptedConnection(
        [
            _profile_attempt_row(
                profile_id, state="active", attempt_id=live_attempt, lease_expired=False
            ),
        ]
    )
    admin = FakeAdminClient()

    with pytest.raises(ConflictError, match="already in progress"):
        account_lifecycle.delete_account(_pool(conn), admin, profile_id=profile_id)

    assert admin.delete_calls == []
    assert admin.fetch_calls == []
    assert len(conn.executed) == 1


def test_expired_active_attempt_recovers_through_reconciliation_before_replay() -> None:
    profile_id = uuid.uuid4()
    abandoned_attempt = uuid.uuid4()
    conn = ScriptedConnection(
        [
            # Pass 1 entry: expired 'active' attempt.
            _profile_attempt_row(
                profile_id, state="active", attempt_id=abandoned_attempt, lease_expired=True
            ),
            # Recovery: compare-and-swap the exact expired attempt into a fresh
            # active attempt, composed with the idempotent revocation re-run.
            (1,),
            [],  # revocation re-run enumeration: credentials already revoked
            # The replay delete_user resolves.
        ]
    )
    admin = FakeAdminClient()
    admin.fetch_results.append(True)  # reconcile: the abandoned delete did not apply
    admin.delete_results.append(None)

    outcome = account_lifecycle.delete_account(_pool(conn), admin, profile_id=profile_id)

    assert outcome == AccountDeletionOutcome(profile_id=profile_id, result="deleted")
    # Reconcile BEFORE replay: the fetch precedes the destructive call.
    assert len(admin.fetch_calls) == 1
    assert len(admin.delete_calls) == 1
    reclaim_sql, reclaim_params = conn.executed[1]
    assert "account_deletion_attempt_id = %s" in reclaim_sql
    assert "account_deletion_started_at + make_interval(secs => %s) < now()" in reclaim_sql
    assert reclaim_params is not None
    assert reclaim_params[1] == profile_id
    assert reclaim_params[2] == abandoned_attempt


def test_expired_active_attempt_confirmed_absent_completes_without_replay() -> None:
    profile_id = uuid.uuid4()
    abandoned_attempt = uuid.uuid4()
    conn = ScriptedConnection(
        [
            _profile_attempt_row(
                profile_id, state="active", attempt_id=abandoned_attempt, lease_expired=True
            ),
        ]
    )
    admin = FakeAdminClient()
    admin.fetch_results.append(False)  # the abandoned delete actually applied

    outcome = account_lifecycle.delete_account(_pool(conn), admin, profile_id=profile_id)

    assert outcome == AccountDeletionOutcome(profile_id=profile_id, result="already_absent")
    assert admin.delete_calls == []  # no replay: the end state is already true


def test_expired_active_attempt_with_undeterminable_existence_adopts_uncertainty() -> None:
    profile_id = uuid.uuid4()
    abandoned_attempt = uuid.uuid4()
    conn = ScriptedConnection(
        [
            _profile_attempt_row(
                profile_id, state="active", attempt_id=abandoned_attempt, lease_expired=True
            ),
            (1,),  # attempt-scoped 'active' -> 'uncertain' transition
        ]
    )
    admin = FakeAdminClient()
    admin.fetch_results.append(SupabaseAuthAdminOutcomeUnknownError("lookup unknown"))

    with pytest.raises(ExternalOperationUncertainError):
        account_lifecycle.delete_account(_pool(conn), admin, profile_id=profile_id)

    assert admin.delete_calls == []
    mark_sql, mark_params = conn.executed[1]
    assert "account_deletion_state = 'uncertain'" in mark_sql
    assert mark_params == (profile_id, abandoned_attempt)


def test_uncertain_state_reconciles_before_any_replay() -> None:
    profile_id = uuid.uuid4()
    uncertain_attempt = uuid.uuid4()
    conn = ScriptedConnection(
        [
            _profile_attempt_row(profile_id, state="uncertain", attempt_id=uncertain_attempt),
            (1,),  # CAS: the exact reconciled 'uncertain' attempt -> fresh 'active'
            [],  # idempotent revocation re-run
        ]
    )
    admin = FakeAdminClient()
    admin.fetch_results.append(True)  # reconcile: the uncertain delete did not apply
    admin.delete_results.append(None)

    outcome = account_lifecycle.delete_account(_pool(conn), admin, profile_id=profile_id)

    assert outcome == AccountDeletionOutcome(profile_id=profile_id, result="deleted")
    assert len(admin.fetch_calls) == 1 and len(admin.delete_calls) == 1
    cas_sql, cas_params = conn.executed[1]
    assert "account_deletion_state = 'uncertain'" in cas_sql
    assert cas_params is not None
    assert cas_params[1] == profile_id
    assert cas_params[2] == uncertain_attempt


def test_uncertain_state_confirmed_absent_completes_without_replay() -> None:
    profile_id = uuid.uuid4()
    conn = ScriptedConnection(
        [
            _profile_attempt_row(profile_id, state="uncertain", attempt_id=uuid.uuid4()),
        ]
    )
    admin = FakeAdminClient()
    admin.fetch_results.append(False)

    outcome = account_lifecycle.delete_account(_pool(conn), admin, profile_id=profile_id)

    assert outcome == AccountDeletionOutcome(profile_id=profile_id, result="already_absent")
    assert admin.delete_calls == []


def test_uncertain_state_with_undeterminable_existence_stays_fail_closed() -> None:
    profile_id = uuid.uuid4()
    conn = ScriptedConnection(
        [
            _profile_attempt_row(profile_id, state="uncertain", attempt_id=uuid.uuid4()),
        ]
    )
    admin = FakeAdminClient()
    admin.fetch_results.append(SupabaseAuthAdminOutcomeUnknownError("lookup unknown"))

    with pytest.raises(ExternalOperationUncertainError):
        account_lifecycle.delete_account(_pool(conn), admin, profile_id=profile_id)

    # The state stands as-is ('uncertain' already); no destructive call.
    assert admin.delete_calls == []
    assert len(conn.executed) == 1


def test_stale_reconciliation_can_never_authorize_a_replay_two_retrier_race() -> None:
    # The two-retrier race: A and B both observe uncertain U1 and reconcile
    # it; B claims a fresh attempt and reaches a NEW uncertain state U2. A's
    # compare-and-swap against U1's UUID must match zero rows — A then
    # reloads and reclassifies (reconciling U2) instead of replaying with
    # U1's stale reconciliation result.
    profile_id = uuid.uuid4()
    attempt_u1 = uuid.uuid4()
    attempt_u2 = uuid.uuid4()
    conn = ScriptedConnection(
        [
            # Pass 1 (caller A): reconcile U1.
            _profile_attempt_row(
                profile_id, state="uncertain", attempt_id=attempt_u1, lease_expired=False
            ),
            None,  # A's CAS against U1 matches zero rows: B consumed U1 into U2
            # Pass 2: reload and reclassify — the current state is U2.
            _profile_attempt_row(
                profile_id, state="uncertain", attempt_id=attempt_u2, lease_expired=False
            ),
            (1,),  # A's CAS against the EXACT U2 applies
            [],  # idempotent revocation re-run
        ]
    )
    admin = FakeAdminClient()
    admin.fetch_results.extend([True, True])  # reconcile U1, then reconcile U2
    admin.delete_results.append(None)

    outcome = account_lifecycle.delete_account(_pool(conn), admin, profile_id=profile_id)

    assert outcome == AccountDeletionOutcome(profile_id=profile_id, result="deleted")
    # Two reconciliations (U1, then U2) before exactly one authorized replay:
    # the stale U1 reconciliation never authorized a replay.
    assert admin.fetch_calls == [profile_id, profile_id]
    assert admin.delete_calls == [profile_id]
    first_cas_sql, first_cas_params = conn.executed[1]
    assert first_cas_params is not None
    assert first_cas_params[2] == attempt_u1  # CAS bound to the reconciled attempt
    second_cas_sql, second_cas_params = conn.executed[3]
    assert second_cas_params is not None
    assert second_cas_params[2] == attempt_u2


def test_definitive_rejection_leaves_the_account_present_with_credentials_revoked() -> None:
    profile_id = uuid.uuid4()
    reference = _reference()
    conn = ScriptedConnection(
        [
            _profile_attempt_row(profile_id),
            (1,),  # claim
            [_connection_row(uuid.uuid4(), auth_reference=reference)],
            (1,),  # Vault secret delete
            _connection_row(uuid.uuid4(), auth_reference=None),  # disconnect
            (1,),  # attempt-scoped clear after the known failure
        ]
    )
    admin = FakeAdminClient()
    admin.delete_results.append(SupabaseAuthAdminRejectedError("rejected"))

    with pytest.raises(ExternalOperationFailedError):
        account_lifecycle.delete_account(_pool(conn), admin, profile_id=profile_id)

    # Account present (nothing cascade-deleted), credentials revoked, retry safe.
    assert admin.delete_calls == [profile_id]
    clear_sql, clear_params = conn.executed[5]
    assert "account_deletion_state = null" in clear_sql
    assert clear_params is not None
    assert clear_params[0] == profile_id


def test_explicit_retry_after_a_known_failure_succeeds() -> None:
    profile_id = uuid.uuid4()
    first = ScriptedConnection(
        [
            _profile_attempt_row(profile_id),
            (1,),  # claim
            [],  # zero Connections
            (1,),  # attempt-scoped clear after the known failure
        ]
    )
    admin = FakeAdminClient()
    admin.delete_results.append(SupabaseAuthAdminRejectedError("rejected"))

    with pytest.raises(ExternalOperationFailedError):
        account_lifecycle.delete_account(_pool(first), admin, profile_id=profile_id)
    assert len(admin.delete_calls) == 1

    admin.delete_results.append(None)  # the retry's delete resolves
    second = ScriptedConnection(
        [
            _profile_attempt_row(profile_id),  # cleared state: normal operation
            (1,),  # fresh attempt claim
            [],
        ]
    )
    outcome = account_lifecycle.delete_account(_pool(second), admin, profile_id=profile_id)

    assert outcome == AccountDeletionOutcome(profile_id=profile_id, result="deleted")
    assert len(admin.delete_calls) == 2


def test_unknown_delete_outcome_reconciles_absent_completes_as_deleted() -> None:
    profile_id = uuid.uuid4()
    conn = ScriptedConnection(
        [
            _profile_attempt_row(profile_id),
            (1,),
            [],
        ]
    )
    admin = FakeAdminClient()
    admin.delete_results.append(SupabaseAuthAdminOutcomeUnknownError("unknown"))
    admin.fetch_results.append(False)  # reconcile: the delete actually applied

    outcome = account_lifecycle.delete_account(_pool(conn), admin, profile_id=profile_id)

    assert outcome == AccountDeletionOutcome(profile_id=profile_id, result="already_absent")
    assert len(admin.delete_calls) == 1 and len(admin.fetch_calls) == 1


def test_unknown_delete_outcome_confirmed_present_is_a_known_not_applied_failure() -> None:
    profile_id = uuid.uuid4()
    conn = ScriptedConnection(
        [
            _profile_attempt_row(profile_id),
            (1,),
            [],
            (1,),  # attempt-scoped clear: normal use resumes
        ]
    )
    admin = FakeAdminClient()
    admin.delete_results.append(SupabaseAuthAdminOutcomeUnknownError("unknown"))
    admin.fetch_results.append(True)  # the delete provably did not apply

    with pytest.raises(ExternalOperationFailedError):
        account_lifecycle.delete_account(_pool(conn), admin, profile_id=profile_id)

    assert len(admin.fetch_calls) == 1 and len(admin.delete_calls) == 1
    clear_sql, _clear_params = conn.executed[3]
    assert "account_deletion_state = null" in clear_sql


def test_unknown_delete_outcome_that_cannot_be_reconciled_marks_the_attempt_uncertain() -> None:
    profile_id = uuid.uuid4()
    conn = ScriptedConnection(
        [
            _profile_attempt_row(profile_id),
            (1,),
            [],
            (1,),  # attempt-scoped 'active' -> 'uncertain'
        ]
    )
    admin = FakeAdminClient()
    admin.delete_results.append(SupabaseAuthAdminOutcomeUnknownError("unknown"))
    admin.fetch_results.append(SupabaseAuthAdminOutcomeUnknownError("lookup unknown"))

    with pytest.raises(ExternalOperationUncertainError):
        account_lifecycle.delete_account(_pool(conn), admin, profile_id=profile_id)

    # No replay inside the attempt: exactly one delete, one reconcile.
    assert len(admin.delete_calls) == 1 and len(admin.fetch_calls) == 1
    mark_sql, _mark_params = conn.executed[3]
    assert "account_deletion_state = 'uncertain'" in mark_sql


def test_credentials_cannot_be_reinstalled_in_the_post_commit_window() -> None:
    # The revocation barrier window: after the revocation commit and before
    # the Auth Admin call resolves, a concurrent authenticated request cannot
    # install a fresh Vault secret — the connection is disabled, so the
    # configure flow is denied before any Vault call and a successful Auth
    # deletion can no longer orphan a Vault secret.
    profile_id = uuid.uuid4()
    workspace_id = uuid.uuid4()
    reference = _reference()
    locked = _connection_row(workspace_id, auth_reference=reference)
    conn = ScriptedConnection(
        [
            _profile_attempt_row(profile_id),
            (1,),  # claim
            [locked],  # enumeration
            (1,),  # Vault secret delete
            _connection_row(workspace_id, auth_reference=None, enabled=False),  # disconnect
            # --- the post-commit window: a concurrent configure attempt ---
            (None, None, None),  # the account barrier read of the configure flow
            (uuid.uuid4(), profile_id, "platform", _OBSERVED, _OBSERVED, 5, ""),  # ws read
            _connection_row(
                workspace_id, auth_reference=None, enabled=False
            ),  # row-locked connection read: disabled by the revocation barrier
            # The configure flow is denied before any Vault call.
        ]
    )
    admin = FakeAdminClient()
    admin.delete_results.append(None)
    configure_denials: list[str] = []

    def on_delete() -> None:
        assert conn.transaction_depth == 0  # no transaction held across I/O
        with composed_transaction(_pool(conn)) as transaction_pool:
            require_account_operational(transaction_pool, profile_id=profile_id)
            require_profile_workspace(
                transaction_pool, profile_id=profile_id, workspace_id=workspace_id
            )
            connection = get_connection_for_update(transaction_pool, locked[0])
            if connection is None or connection.workspace_id != workspace_id:
                configure_denials.append("not-found")
            elif not connection.enabled:
                configure_denials.append("disabled")
            else:
                configure_denials.append("allowed")

    admin.on_delete = on_delete

    outcome = account_lifecycle.delete_account(_pool(conn), admin, profile_id=profile_id)

    assert outcome == AccountDeletionOutcome(profile_id=profile_id, result="deleted")
    # The concurrent configure attempt saw the disabled Connection (the
    # durable revocation barrier) and was denied.
    assert configure_denials == ["disabled"]
    # No fresh Vault secret was created in the window.
    window_sqls = [sql for sql, _ in conn.executed[6:]]
    assert all("vault.create_secret" not in sql for sql in window_sqls)


def test_the_account_barrier_fails_closed_on_any_attempt_state() -> None:
    profile_id = uuid.uuid4()

    operational = ScriptedConnection([(None, None, None)])
    require_account_operational(_pool(operational), profile_id=profile_id)  # passes
    assert len(operational.executed) == 1

    for state in ("active", "uncertain"):
        guarded = ScriptedConnection([(state, uuid.uuid4(), _OBSERVED)])
        with pytest.raises(ConflictError, match="account is being deleted"):
            require_account_operational(_pool(guarded), profile_id=profile_id)
        assert len(guarded.executed) == 1


def test_the_account_barrier_fails_closed_for_a_missing_profile() -> None:
    profile_id = uuid.uuid4()
    guarded = ScriptedConnection([None])  # the Profile row is absent

    with pytest.raises(NotFoundError):
        require_account_operational(_pool(guarded), profile_id=profile_id)


def test_the_barrier_read_uses_for_key_share_conflicting_with_the_deletion_lock() -> None:
    profile_id = uuid.uuid4()
    guarded = ScriptedConnection([(None, None, None)])

    require_account_operational(_pool(guarded), profile_id=profile_id)

    barrier_sql, barrier_params = guarded.executed[0]
    assert "for key share" in barrier_sql
    assert "openorc.profiles" in barrier_sql
    assert barrier_params == (profile_id,)
