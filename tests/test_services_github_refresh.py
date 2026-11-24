"""Deterministic fake-seam tests for the refresh admission/status services (#375).

Proves the Owner-facing boundary truthfulness: the atomic admission
composition (Workspace/repository authorization, uniform anti-probing
not-found on a deletion race, the typed conflict on an active overlapping
scope), the three submission outcomes (a 202-acknowledged established
acceptance, the durable terminal ``failed`` on a known refusal — never a
202, and the conservative ``accepted`` retention on an uncertain submission
that is never a known non-delivery), and the truthful status projection
(verbatim stored facts; an expired accepted/running record presents as
``unknown``; foreign records are the uniform not-found). No live GitHub,
Valkey, or Postgres.
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime, timedelta
from typing import Any

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


class _Pool:
    """Authorization is stubbed at the module boundary; no SQL ever runs."""

    def connection(self) -> Any:  # pragma: no cover
        raise AssertionError("refresh service tests never open connections")

    def close(self) -> None:  # pragma: no cover
        raise AssertionError("refresh service tests never close pools")


def _pool() -> Any:
    return _Pool()


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


@pytest.fixture
def admission_harness(monkeypatch: pytest.MonkeyPatch):
    """Stub the authorization guards and script the atomic admission outcome."""

    class Harness:
        def __init__(self) -> None:
            self.admissions: list[dict[str, Any]] = []
            self.workspace_checked: list[uuid.UUID] = []
            self.repository_checked: list[uuid.UUID] = []
            self.admission_result: Any = _admission_success(_record())

        def admit(self, pool: Any, *, workspace_id: uuid.UUID, repository_id: uuid.UUID | None):
            self.admissions.append({"workspace_id": workspace_id, "repository_id": repository_id})
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

    harness = Harness()
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
    """Stub the authorization guard and script the stored record read."""

    class Harness:
        def __init__(self) -> None:
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
        _pool(),
        profile_id=_PROFILE_ID,
        workspace_id=_WORKSPACE_ID,
        refresh_id=status_harness.stored.id,
    )

    assert projection.status is GitHubRefreshStatus.ACCEPTED
    assert projection.completed_at is None
    assert projection.summary is None


def test_an_expired_running_record_projects_as_unknown(status_harness) -> None:
    status_harness.stored = _record(
        status=GitHubRefreshStatus.RUNNING,
        lease=datetime.now(tz=UTC) - timedelta(minutes=1),
    )

    projection = github_refresh.get_workspace_github_refresh_for_owner(
        _pool(),
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
        _pool(),
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
            _pool(),
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
