"""Deterministic fake-seam tests for the refresh admission/status services (#375).

Proves the Owner-facing boundary truthfulness: the atomic admission
composition (Workspace/repository authorization, the #97 account-deletion
Owner-mutation barrier composed Profile-first in the SAME transaction as the
durable admission, uniform anti-probing not-found on a deletion race, the
typed conflict on an active overlapping scope), the three submission outcomes
(a 202-acknowledged established acceptance, the durable terminal ``failed``
on a known refusal — never a 202, and the conservative ``accepted`` retention
on an uncertain submission that is never a known non-delivery), and the
truthful status projection (verbatim stored facts; an expired accepted/running
record presents as ``unknown``; foreign records are the uniform not-found;
status reads compose no mutation barrier). No live GitHub, Valkey, or
Postgres.
"""

from __future__ import annotations

import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from typing import Any, ClassVar

import pytest

from openorc.domain.github_refreshes import (
    GitHubRefreshRecord,
    GitHubRefreshStatus,
    GitHubRefreshSummary,
    GitHubRefreshUnitError,
)
from openorc.services import github_refresh
from openorc.services.errors import (
    ConflictError,
    ExternalOperationFailedError,
    ExternalOperationUncertainError,
    InvalidCommandError,
    NotFoundError,
)

_WORKSPACE_ID = uuid.uuid4()
_REPOSITORY_ID = uuid.uuid4()
_PROFILE_ID = uuid.uuid4()
_NOW = datetime(2026, 11, 24, 12, 0, 0, tzinfo=UTC)

# The account-deletion Owner-mutation barrier (issue #97) composes as the
# FIRST database read of the admission; the REAL guard runs against the
# scripted connection, so the canned result row for an operational account
# carries the all-NULL attempt-state tuple, and an in-flight attempt carries
# its durable state (active or uncertain — both refuse).
_GUARD_OPERATIONAL_ROW = (None, None, None)
_GUARD_DELETION_ACTIVE_ROW = ("active", uuid.uuid4(), _NOW)
_GUARD_DELETION_UNCERTAIN_ROW = ("uncertain", uuid.uuid4(), _NOW)


class FakeCursor:
    def __init__(self, row: tuple[Any, ...] | None) -> None:
        self._row = row

    def fetchone(self) -> tuple[Any, ...] | None:
        return self._row


class ScriptedConnection:
    """Plays back canned statement results in order, recording executed SQL.

    Serves the REAL issue #97 account-deletion barrier read — the one SQL the
    admission path executes that the harness does not stub at the module
    boundary. An unexpected statement (or a consumed script) fails loudly.
    """

    def __init__(self, results: list[Any] | None = None) -> None:
        self.results: list[Any] = list(results or [])
        self.executed: list[tuple[str, tuple[Any, ...] | None]] = []

    def execute(self, sql: str, params: tuple[Any, ...] | None = None) -> FakeCursor:
        self.executed.append((sql, params))
        if not self.results:
            raise AssertionError(f"unexpected statement without a scripted result: {sql}")
        result = self.results.pop(0)
        if isinstance(result, Exception):
            raise result
        return FakeCursor(result)

    @contextmanager
    def transaction(self) -> Iterator[None]:
        # Supports composed_transaction's outer transaction entry and the
        # nested repository scopes under it.
        yield


class FakePool:
    def __init__(self, conn: ScriptedConnection) -> None:
        self._conn = conn

    def connection(self) -> Any:
        @contextmanager
        def managed() -> Iterator[ScriptedConnection]:
            yield self._conn

        return managed()

    def close(self) -> None:
        raise AssertionError("refresh service tests never close pools")


def _pool(conn: ScriptedConnection | None = None) -> Any:
    return FakePool(conn if conn is not None else ScriptedConnection([_GUARD_OPERATIONAL_ROW]))


class RecordingSubmission:
    """The F1-backed submission seam fake with one scripted outcome."""

    def __init__(self, outcome: github_refresh.RefreshSubmissionOutcome) -> None:
        self.outcome = outcome
        self.submitted: list[uuid.UUID] = []

    def submit(self, *, refresh_id: uuid.UUID) -> github_refresh.RefreshSubmissionOutcome:
        self.submitted.append(refresh_id)
        return self.outcome


def _record(
    *,
    status: GitHubRefreshStatus = GitHubRefreshStatus.ACCEPTED,
    repository_id: uuid.UUID | None = None,
    lease: datetime | None = _NOW + timedelta(minutes=10),
) -> GitHubRefreshRecord:
    return GitHubRefreshRecord(
        id=uuid.uuid4(),
        workspace_id=_WORKSPACE_ID,
        repository_id=repository_id,
        status=status,
        requested_at=_NOW,
        started_at=None,
        lease_expires_at=lease,
        completed_at=None,
        summary=None,
        errors=(),
    )


class _FrozenDatetime(datetime):
    """The controlled test clock for the lease-aware status projection.

    Production reads the wall clock through ``datetime.now(tz=UTC)`` when it
    demotes an expired accepted/running record to ``unknown``. The status
    tests freeze that exact seam to the fixture's ``_NOW`` instant, so lease
    freshness is deterministic relative to the fixture timestamps instead of
    rotting against the real calendar (the fresh record's lease is
    ``_NOW + 10 minutes`` — always valid at the frozen clock, an expired
    lease is authored as ``_NOW - 1 minute`` — always behind it).
    """

    frozen_at: ClassVar[datetime] = _NOW

    @classmethod
    def now(cls, tz: Any = None) -> datetime:
        return cls.frozen_at


@pytest.fixture
def admission_harness(monkeypatch: pytest.MonkeyPatch):
    """Stub the authorization guards and script the atomic admission outcome.

    The #97 account-deletion barrier is deliberately NOT stubbed: the real
    guard reads its canned ``profiles`` row through the scripted connection
    (the workspace-lifecycle seam), so the barrier-first composition and both
    refusal states are proven against the real guard while the ownership
    guards stay stubbed at the module boundary.
    """

    class Harness:
        def __init__(self, conn: ScriptedConnection) -> None:
            self.conn = conn
            self.pool: Any = FakePool(conn)
            self.admissions: list[dict[str, Any]] = []
            self.workspace_checked: list[uuid.UUID] = []
            self.repository_checked: list[uuid.UUID] = []
            self.admission_after_statements: list[int] = []
            self.admission_result: Any = _admission_success(_record())

        def admit(self, pool: Any, *, workspace_id: uuid.UUID, repository_id: uuid.UUID | None):
            self.admissions.append({"workspace_id": workspace_id, "repository_id": repository_id})
            # How many statements the REAL guard had executed when the durable
            # admission was reached: 1 proves the barrier composes first
            # inside the same composed transaction.
            self.admission_after_statements.append(len(self.conn.executed))
            result = self.admission_result
            if isinstance(result, Exception):
                raise result
            return result

        def require_ws(self, pool: Any, *, profile_id: uuid.UUID, workspace_id: uuid.UUID) -> Any:
            self.workspace_checked.append(workspace_id)
            return object()

        def require_repo(
            self,
            pool: Any,
            *,
            profile_id: uuid.UUID,
            workspace_id: uuid.UUID,
            repository_id: uuid.UUID,
        ) -> Any:
            self.repository_checked.append(repository_id)
            return object()

    harness = Harness(ScriptedConnection([_GUARD_OPERATIONAL_ROW]))
    monkeypatch.setattr(
        github_refresh,
        "admit_github_refresh",
        lambda pool, **kwargs: harness.admit(pool, **kwargs),
    )
    monkeypatch.setattr(github_refresh, "require_profile_workspace", harness.require_ws)
    monkeypatch.setattr(github_refresh, "require_workspace_repository", harness.require_repo)
    return harness


@pytest.fixture
def status_harness(monkeypatch: pytest.MonkeyPatch):
    """Stub the authorization guard and script the stored record read.

    The scripted connection stays scriptless: the status path must execute
    no SQL at all (the ownership guard is stubbed and no account-mutation
    barrier composes on a read), which the dedicated no-barrier test proves.
    The lease-aware projection's clock seam is frozen to the fixture's
    ``_NOW`` instant so the fresh/expired lease tests stay deterministic
    instead of rotting against the real calendar.
    """

    class Harness:
        def __init__(self) -> None:
            self.conn = ScriptedConnection()
            self.pool: Any = FakePool(self.conn)
            self.stored: GitHubRefreshRecord | None = None
            self.workspace_checked: list[uuid.UUID] = []

        def require_ws(self, pool: Any, *, profile_id: uuid.UUID, workspace_id: uuid.UUID) -> Any:
            self.workspace_checked.append(workspace_id)
            return object()

        def get(self, pool: Any, *, refresh_id: uuid.UUID) -> GitHubRefreshRecord | None:
            return self.stored

    harness = Harness()
    harness.stored = _record()
    monkeypatch.setattr(github_refresh, "require_profile_workspace", harness.require_ws)
    monkeypatch.setattr(github_refresh, "get_github_refresh", harness.get)
    monkeypatch.setattr(github_refresh, "datetime", _FrozenDatetime)
    return harness


def _admission_success(record: GitHubRefreshRecord) -> Any:
    class Success:
        def __init__(self) -> None:
            self.record: GitHubRefreshRecord | None = record
            self.conflict = False
            self.workspace_missing = False

    return Success()


def _admission_conflict() -> Any:
    class Conflict:
        def __init__(self) -> None:
            self.record = None
            self.conflict = True
            self.workspace_missing = False

    return Conflict()


def _admission_workspace_missing() -> Any:
    class Missing:
        def __init__(self) -> None:
            self.record = None
            self.conflict = False
            self.workspace_missing = True

    return Missing()


def test_the_workspace_admission_composes_authorization_and_submission(
    admission_harness,
) -> None:
    submission = RecordingSubmission(github_refresh.RefreshSubmissionOutcome.ACCEPTED)

    record = github_refresh.request_workspace_github_refresh_for_owner(
        _pool(), submission, profile_id=_PROFILE_ID, workspace_id=_WORKSPACE_ID
    )

    assert record.status is GitHubRefreshStatus.ACCEPTED
    assert admission_harness.workspace_checked == [_WORKSPACE_ID]
    assert admission_harness.admissions == [{"workspace_id": _WORKSPACE_ID, "repository_id": None}]
    assert submission.submitted == [record.id]


def test_the_repository_admission_authorizes_the_repository_scope(admission_harness) -> None:
    submission = RecordingSubmission(github_refresh.RefreshSubmissionOutcome.ACCEPTED)

    record = github_refresh.request_repository_github_refresh_for_owner(
        _pool(),
        submission,
        profile_id=_PROFILE_ID,
        workspace_id=_WORKSPACE_ID,
        repository_id=_REPOSITORY_ID,
    )

    assert record.workspace_id == _WORKSPACE_ID
    assert admission_harness.repository_checked == [_REPOSITORY_ID]
    assert admission_harness.admissions == [
        {"workspace_id": _WORKSPACE_ID, "repository_id": _REPOSITORY_ID}
    ]


def test_an_active_overlapping_scope_conflicts_the_admission(admission_harness) -> None:
    admission_harness.admission_result = _admission_conflict()
    submission = RecordingSubmission(github_refresh.RefreshSubmissionOutcome.ACCEPTED)

    with pytest.raises(ConflictError):
        github_refresh.request_workspace_github_refresh_for_owner(
            _pool(), submission, profile_id=_PROFILE_ID, workspace_id=_WORKSPACE_ID
        )

    # The refusal happened before any execution submission.
    assert submission.submitted == []


def test_a_deletion_race_is_the_uniform_not_found(admission_harness) -> None:
    admission_harness.admission_result = _admission_workspace_missing()
    submission = RecordingSubmission(github_refresh.RefreshSubmissionOutcome.ACCEPTED)

    with pytest.raises(NotFoundError):
        github_refresh.request_workspace_github_refresh_for_owner(
            _pool(), submission, profile_id=_PROFILE_ID, workspace_id=_WORKSPACE_ID
        )

    assert submission.submitted == []


def test_an_unresolved_account_deletion_attempt_refuses_admission_and_submission(
    admission_harness,
) -> None:
    submission = RecordingSubmission(github_refresh.RefreshSubmissionOutcome.ACCEPTED)

    for deletion_row in (_GUARD_DELETION_ACTIVE_ROW, _GUARD_DELETION_UNCERTAIN_ROW):
        admission_harness.conn.results = [deletion_row]
        with pytest.raises(ConflictError):
            github_refresh.request_workspace_github_refresh_for_owner(
                admission_harness.pool,
                submission,
                profile_id=_PROFILE_ID,
                workspace_id=_WORKSPACE_ID,
            )

    # The #97 barrier refused inside the admission composition: no durable
    # admission was attempted and no F1 submission ever happened — exactly
    # one real barrier read ran per attempt.
    assert admission_harness.admissions == []
    assert submission.submitted == []
    assert len(admission_harness.conn.executed) == 2
    assert all("openorc.profiles" in sql for sql, _ in admission_harness.conn.executed)


def test_the_account_barrier_is_the_first_lock_of_the_admission(admission_harness) -> None:
    submission = RecordingSubmission(github_refresh.RefreshSubmissionOutcome.ACCEPTED)

    record = github_refresh.request_workspace_github_refresh_for_owner(
        admission_harness.pool, submission, profile_id=_PROFILE_ID, workspace_id=_WORKSPACE_ID
    )

    assert record.status is GitHubRefreshStatus.ACCEPTED
    # The REAL #97 guard ran as the composition's first statement — the
    # Profile FOR KEY SHARE read — and the durable admission joined the same
    # composed transaction only after it; the F1 submission followed the
    # committed admission.
    guard_sql, guard_params = admission_harness.conn.executed[0]
    assert "openorc.profiles" in guard_sql
    assert "for key share" in guard_sql
    assert guard_params == (_PROFILE_ID,)
    assert admission_harness.admission_after_statements == [1]
    assert submission.submitted == [record.id]


def test_a_known_refusal_is_a_durable_failure_and_never_a_202(
    admission_harness, monkeypatch: pytest.MonkeyPatch
) -> None:
    admission_harness.admission_result = _admission_success(_record())
    recorded: dict[str, Any] = {}

    def fake_fail(pool: Any, *, refresh_id: uuid.UUID, errors: Any) -> GitHubRefreshRecord:
        recorded["refresh_id"] = refresh_id
        recorded["errors"] = errors
        return _record(status=GitHubRefreshStatus.FAILED)

    monkeypatch.setattr(github_refresh, "fail_unclaimed_github_refresh", fake_fail)
    submission = RecordingSubmission(github_refresh.RefreshSubmissionOutcome.KNOWN_REFUSED)

    with pytest.raises(ExternalOperationFailedError):
        github_refresh.request_workspace_github_refresh_for_owner(
            _pool(), submission, profile_id=_PROFILE_ID, workspace_id=_WORKSPACE_ID
        )

    assert recorded["errors"][0].code == "submission_refused"


def test_an_uncertain_submission_is_never_a_known_non_delivery(admission_harness) -> None:
    submission = RecordingSubmission(github_refresh.RefreshSubmissionOutcome.UNCERTAIN)

    with pytest.raises(ExternalOperationUncertainError):
        github_refresh.request_workspace_github_refresh_for_owner(
            _pool(), submission, profile_id=_PROFILE_ID, workspace_id=_WORKSPACE_ID
        )

    # The conservative retention: the record stays accepted (the work may
    # still be delivered and claimed), and its bounded lease owns recovery.
    assert admission_harness.admissions == [{"workspace_id": _WORKSPACE_ID, "repository_id": None}]


def test_the_status_projection_is_verbatim_for_a_fresh_record(status_harness) -> None:
    projection = github_refresh.get_workspace_github_refresh_for_owner(
        status_harness.pool,
        profile_id=_PROFILE_ID,
        workspace_id=_WORKSPACE_ID,
        refresh_id=status_harness.stored.id,
    )

    assert projection.status is GitHubRefreshStatus.ACCEPTED
    assert projection.completed_at is None
    assert projection.summary is None
    # A status read composes no account-mutation barrier: no SQL at all.
    assert status_harness.conn.executed == []


def test_an_expired_running_record_projects_as_unknown(status_harness) -> None:
    status_harness.stored = _record(
        status=GitHubRefreshStatus.RUNNING,
        # Authored relative to the frozen fixture clock (_NOW): always one
        # minute behind it, so the demotion is deterministic.
        lease=_NOW - timedelta(minutes=1),
    )

    projection = github_refresh.get_workspace_github_refresh_for_owner(
        status_harness.pool,
        profile_id=_PROFILE_ID,
        workspace_id=_WORKSPACE_ID,
        refresh_id=status_harness.stored.id,
    )

    # Lost completion evidence is never success.
    assert projection.status is GitHubRefreshStatus.UNKNOWN


def test_a_terminal_record_projects_verbatim(status_harness) -> None:
    status_harness.stored = GitHubRefreshRecord(
        id=uuid.uuid4(),
        workspace_id=_WORKSPACE_ID,
        repository_id=None,
        status=GitHubRefreshStatus.PARTIALLY_COMPLETED,
        requested_at=_NOW,
        started_at=_NOW,
        lease_expires_at=None,
        completed_at=_NOW,
        summary=GitHubRefreshSummary(repositories_checked=2, issues_discovered=1),
        errors=(GitHubRefreshUnitError(family="issue", code="failed_known", github_issue_id=503),),
    )

    projection = github_refresh.get_workspace_github_refresh_for_owner(
        status_harness.pool,
        profile_id=_PROFILE_ID,
        workspace_id=_WORKSPACE_ID,
        refresh_id=status_harness.stored.id,
    )

    assert projection.status is GitHubRefreshStatus.PARTIALLY_COMPLETED
    assert projection.summary is not None
    assert projection.summary.issues_discovered == 1
    assert projection.errors[0].family == "issue"


def test_a_foreign_refresh_record_is_the_uniform_not_found(status_harness) -> None:
    status_harness.stored = GitHubRefreshRecord(
        id=uuid.uuid4(),
        workspace_id=uuid.uuid4(),  # another Workspace
        repository_id=None,
        status=GitHubRefreshStatus.COMPLETED,
        requested_at=_NOW,
        started_at=_NOW,
        lease_expires_at=None,
        completed_at=_NOW,
        summary=None,
        errors=(),
    )

    with pytest.raises(NotFoundError):
        github_refresh.get_workspace_github_refresh_for_owner(
            status_harness.pool,
            profile_id=_PROFILE_ID,
            workspace_id=_WORKSPACE_ID,
            refresh_id=status_harness.stored.id,
        )


def test_malformed_commands_are_rejected_before_any_state(admission_harness) -> None:
    submission = RecordingSubmission(github_refresh.RefreshSubmissionOutcome.ACCEPTED)

    with pytest.raises(InvalidCommandError):
        github_refresh.request_workspace_github_refresh_for_owner(
            _pool(),
            submission,
            profile_id="not-a-uuid",  # type: ignore[arg-type]
            workspace_id=_WORKSPACE_ID,
        )
    with pytest.raises(InvalidCommandError):
        github_refresh.get_workspace_github_refresh_for_owner(
            _pool(),
            profile_id=_PROFILE_ID,
            workspace_id=_WORKSPACE_ID,
            refresh_id="not-a-uuid",  # type: ignore[arg-type]
        )
