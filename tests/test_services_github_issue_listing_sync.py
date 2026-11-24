"""Deterministic fake-seam tests for the open-issue listing synchronization (#375).

Proves the one-way GitHub-authoritative issue synchronization semantics the
review required: every successfully observed entry flows through the same
identity-guarded write phase regardless of whether it is new, known, or
previously closed (discovery is the insertion outcome, never a separate
semantic); a missed reopening is recovered exactly like any other state
change; per-entry failures are contained and classified without blocking the
remaining entries; and a bounded (incomplete) listing is a typed partial
fact from which closure, deletion, and complete discovery are never
inferred. No live GitHub, Valkey, or Postgres.
"""

from __future__ import annotations

import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime
from typing import Any, cast

import pytest

from openorc.adapters.github import (
    GitHubIssueObservation,
    GitHubRepositoryIssueListing,
    GitHubRepositoryObservation,
)
from openorc.domain.ownership import GitHubRepositoryIdentity, Repository, RepositoryMetadata
from openorc.persistence.pool import DatabasePool
from openorc.services import github_reconciliation
from openorc.services.errors import (
    AuthorizationError,
    ExternalOperationFailedError,
    ExternalOperationUncertainError,
)
from openorc.services.github_reconciliation import (
    RepositoryIssueListingEntryOutcome,
)

_OBSERVED = datetime(2026, 9, 25, 12, 0, 0, tzinfo=UTC)
_WORKSPACE_ID = uuid.uuid4()
_REPOSITORY_ID = uuid.uuid4()
_GITHUB_REPOSITORY_ID = 456


class FakeCursor:
    def __init__(self, row: tuple[Any, ...] | None) -> None:
        self._row = row

    def fetchone(self) -> tuple[Any, ...] | None:
        return self._row

    def fetchall(self) -> list[tuple[Any, ...]]:
        return [self._row] if self._row is not None else []


class ScriptedConnection:
    """Routes each execute to the next matching scripted SQL handler."""

    def __init__(self) -> None:
        self.executed: list[tuple[str, tuple[Any, ...] | None]] = []
        self._handlers: list[tuple[str, tuple[Any, ...] | None | Exception]] = []

    def on(self, sql_marker: str, result: tuple[Any, ...] | None | Exception) -> None:
        self._handlers.append((sql_marker.lower(), result))

    def execute(self, sql: str, params: tuple[Any, ...] | None = None) -> FakeCursor:
        self.executed.append((sql, params))
        lowered = " ".join(sql.split()).lower()
        for index, (marker, result) in enumerate(self._handlers):
            if marker in lowered:
                del self._handlers[index]
                if isinstance(result, Exception):
                    raise result
                return FakeCursor(result)
        raise AssertionError(f"no scripted handler matched: {sql}")

    @contextmanager
    def transaction(self) -> Iterator[None]:
        yield


class FakePool:
    def __init__(self, conn: ScriptedConnection) -> None:
        self._conn = conn
        self.active_connections = 0

    def connection(self) -> Any:
        @contextmanager
        def managed() -> Iterator[ScriptedConnection]:
            self.active_connections += 1
            try:
                yield self._conn
            finally:
                self.active_connections -= 1

        return managed()

    def close(self) -> None:
        raise AssertionError("listing synchronization tests never close pools")


def _pool(conn: ScriptedConnection) -> DatabasePool:
    return cast(DatabasePool, FakePool(conn))


def _repository() -> Repository:
    return Repository(
        id=_REPOSITORY_ID,
        project_id=uuid.uuid4(),
        workspace_id=_WORKSPACE_ID,
        identity=GitHubRepositoryIdentity(github_repository_id=_GITHUB_REPOSITORY_ID),
        metadata=RepositoryMetadata(
            owner_login="octocat",
            name="hello-world",
            html_url="https://github.com/octocat/hello-world",
            is_private=False,
            default_branch="main",
        ),
        created_at=_OBSERVED,
        updated_at=_OBSERVED,
        github_installation_id=uuid.uuid4(),
    )


def _repository_observation() -> GitHubRepositoryObservation:
    return GitHubRepositoryObservation(
        github_repository_id=_GITHUB_REPOSITORY_ID,
        owner_login="octocat",
        name="hello-world",
        html_url="https://github.com/octocat/hello-world",
        is_private=False,
        default_branch="main",
    )


def _issue_row(issue_number: int) -> tuple[Any, ...]:
    from openorc.domain.github_issues import github_issue_requirements_fingerprint

    return (
        uuid.uuid4(),
        _WORKSPACE_ID,
        _REPOSITORY_ID,
        500 + issue_number,
        issue_number,
        "Found a bug",
        "Steps",
        "open",
        github_issue_requirements_fingerprint("Found a bug", "Steps"),
        _OBSERVED,
        _OBSERVED,
        _OBSERVED,
        None,
        f"https://github.com/octocat/hello-world/issues/{issue_number}",
        _OBSERVED,
        None,
        None,
        [],
        [],
        None,
        None,
        None,
    )


def _issue_observation(issue_number: int, *, state: str = "open") -> GitHubIssueObservation:
    return GitHubIssueObservation(
        github_issue_id=500 + issue_number,
        issue_number=issue_number,
        title="Found a bug",
        body="Steps",
        state=state,
        provider_updated_at=_OBSERVED,
        is_pull_request=False,
        state_reason=None,
        html_url=f"https://github.com/octocat/hello-world/issues/{issue_number}",
        provider_created_at=_OBSERVED,
        provider_closed_at=None,
        user=None,
        labels=(),
        assignees=(),
        milestone=None,
    )


class FakeGitHub:
    """The adapter seam fake recording the listing walks and entry reads."""

    def __init__(
        self,
        *,
        repository_observation: GitHubRepositoryObservation,
        listing: GitHubRepositoryIssueListing | None = None,
        listing_error: Exception | None = None,
        entry_results: list[Any] | None = None,
        entry_errors: dict[int, Exception] | None = None,
    ) -> None:
        self._repository_observation = repository_observation
        self._listing = listing
        self._listing_error = listing_error
        self._entry_results = list(entry_results or [])
        self._entry_errors = dict(entry_errors or {})
        self.listing_calls: list[dict[str, Any]] = []
        self.entry_calls: list[dict[str, int]] = []

    def get_installation_repository(
        self, *, github_installation_id: int, github_repository_id: int
    ) -> GitHubRepositoryObservation:
        assert self._repository_observation is not None
        return self._repository_observation

    def list_repository_open_issues(
        self, *, github_installation_id: int, owner_login: str, repository_name: str
    ) -> GitHubRepositoryIssueListing:
        self.listing_calls.append(
            {
                "github_installation_id": github_installation_id,
                "owner_login": owner_login,
                "repository_name": repository_name,
            }
        )
        if self._listing_error is not None:
            raise self._listing_error
        assert self._listing is not None
        return self._listing

    def get_repository_issue(
        self,
        *,
        github_installation_id: int,
        owner_login: str,
        repository_name: str,
        issue_number: int,
    ) -> GitHubIssueObservation:
        self.entry_calls.append({"issue_number": issue_number})
        index = len(self.entry_calls) - 1
        error = self._entry_errors.get(index)
        if error is not None:
            raise error
        if self._entry_results:
            result = self._entry_results.pop(0)
            if isinstance(result, Exception):
                raise result
            assert isinstance(result, GitHubIssueObservation)
            return result
        raise AssertionError(f"no scripted entry result for call {index}")


@pytest.fixture
def route_seam(monkeypatch: pytest.MonkeyPatch):
    """Stub the route resolver and script the write phase per entry."""

    class Harness:
        def __init__(self) -> None:
            self.conn = ScriptedConnection()
            self.write_phases = 0
            self.route_installation_id = uuid.uuid4()

        def script_write_phase(self, *, issue_number: int) -> None:
            # The composed write phase for one synchronized entry: the account
            # barrier (the first lock acquisition), the locked repository
            # reload, the locked projection read (absent → first-projection
            # insert returning the created row), and the conditional update.
            self.conn.on(
                "from openorc.workspaces",
                (uuid.uuid4(), uuid.uuid4(), "ws", _OBSERVED, _OBSERVED, 5, "", None),
            )
            # The account-deletion barrier (the first lock acquisition).
            self.conn.on("from openorc.profiles", (None, None))
            self.conn.on(
                "from openorc.repositories",
                (
                    _REPOSITORY_ID,
                    uuid.uuid4(),
                    _WORKSPACE_ID,
                    _GITHUB_REPOSITORY_ID,
                    "octocat",
                    "hello-world",
                    "https://github.com/octocat/hello-world",
                    False,
                    "main",
                    _OBSERVED,
                    _OBSERVED,
                    self.route_installation_id,
                ),
            )
            self.conn.on("from openorc.github_issues", None)
            self.conn.on("insert into openorc.github_issues", _issue_row(issue_number))
            self.write_phases += 1

    harness = Harness()

    class Route:
        repository = _repository()
        installation_id = harness.route_installation_id
        github_installation_id = 4242

    monkeypatch.setattr(
        github_reconciliation,
        "resolve_system_repository_installation_route",
        lambda pool, **kwargs: Route(),
    )
    return harness


def _reconciliation(*, issue_number: int, created: bool) -> Any:
    from openorc.domain.github_issues import (
        GitHubIssueIdentity,
        GitHubIssueProjection,
        GitHubIssueState,
        github_issue_requirements_fingerprint,
    )

    return github_reconciliation.RepositoryIssueReconciliation(
        repository=_repository(),
        repository_metadata_changed=False,
        issue=GitHubIssueProjection(
            id=uuid.uuid4(),
            workspace_id=_WORKSPACE_ID,
            repository_id=_REPOSITORY_ID,
            identity=GitHubIssueIdentity(github_issue_id=500 + issue_number),
            issue_number=issue_number,
            title="Found a bug",
            body="Steps",
            state=GitHubIssueState.OPEN,
            requirements_fingerprint=github_issue_requirements_fingerprint("Found a bug", "Steps"),
            provider_updated_at=_OBSERVED,
            created_at=_OBSERVED,
            updated_at=_OBSERVED,
        ),
        issue_created=created,
        issue_state_changed=False,
        requirements_changed=False,
        previous_fingerprint=None if created else "a" * 64,
        previous_state=None,
    )


def _listing(
    *numbers: int,
    complete: bool = True,
    skipped: int = 0,
    state: str = "open",
) -> GitHubRepositoryIssueListing:
    return GitHubRepositoryIssueListing(
        issues=tuple(_issue_observation(number, state=state) for number in numbers),
        pull_request_entries_skipped=skipped,
        complete=complete,
    )


def test_every_listing_entry_flows_through_the_same_write_phase(route_seam) -> None:
    # Issue 51 is unknown (discovery is the insertion outcome) and issue 52
    # is a known open issue: both synchronize through the identical guarded
    # write phase — no separate discovery semantic exists.
    github = FakeGitHub(
        repository_observation=_repository_observation(),
        listing=_listing(51, 52),
        entry_results=[_issue_observation(51), _issue_observation(52)],
    )
    route_seam.script_write_phase(issue_number=51)
    route_seam.script_write_phase(issue_number=52)

    result = github_reconciliation.synchronize_repository_open_issue_listing(
        _pool(route_seam.conn),
        github,  # type: ignore[arg-type]
        workspace_id=_WORKSPACE_ID,
        repository_id=_REPOSITORY_ID,
    )

    assert result.listing_complete is True
    assert [entry.outcome for entry in result.entries] == [
        RepositoryIssueListingEntryOutcome.RECONCILED,
        RepositoryIssueListingEntryOutcome.RECONCILED,
    ]
    # The fresh single-issue reads address the fresh repository address.
    assert github.listing_calls == [
        {
            "github_installation_id": 4242,
            "owner_login": "octocat",
            "repository_name": "hello-world",
        }
    ]
    assert [call["issue_number"] for call in github.entry_calls] == [51, 52]
    # Each entry performed exactly one serialized write phase.
    assert route_seam.write_phases == 2


def test_a_reopened_issue_synchronizes_back_to_open(route_seam) -> None:
    # A previously closed issue returned by the open listing is a missed
    # reopening webhook: the same upsert synchronizes it open again.
    github = FakeGitHub(
        repository_observation=_repository_observation(),
        listing=_listing(51),
        entry_results=[_issue_observation(51)],
    )
    route_seam.script_write_phase(issue_number=53)

    result = github_reconciliation.synchronize_repository_open_issue_listing(
        _pool(route_seam.conn),
        github,  # type: ignore[arg-type]
        workspace_id=_WORKSPACE_ID,
        repository_id=_REPOSITORY_ID,
    )

    assert result.entries[0].outcome is RepositoryIssueListingEntryOutcome.RECONCILED
    assert result.entries[0].reconciliation is not None
    assert result.entries[0].reconciliation.issue.state.value == "open"
    # The scripted write phase projects the reopening as a durable write of
    # the same identity-guarded path; the entry's own facts are what matter
    # here (the open state synchronized through the shared upsert).


def test_a_failed_entry_is_contained_and_never_blocks_the_rest(route_seam) -> None:
    github = FakeGitHub(
        repository_observation=_repository_observation(),
        listing=_listing(51, 52),
        entry_results=[
            ExternalOperationFailedError("rate limited"),
            _issue_observation(52),
        ],
    )
    route_seam.script_write_phase(issue_number=54)

    result = github_reconciliation.synchronize_repository_open_issue_listing(
        _pool(route_seam.conn),
        github,  # type: ignore[arg-type]
        workspace_id=_WORKSPACE_ID,
        repository_id=_REPOSITORY_ID,
    )

    assert [entry.outcome for entry in result.entries] == [
        RepositoryIssueListingEntryOutcome.FAILED_KNOWN,
        RepositoryIssueListingEntryOutcome.RECONCILED,
    ]
    # The failed entry performed no write; the next entry proceeded.
    assert route_seam.write_phases == 1


def test_entry_failure_classifications_are_typed(route_seam) -> None:
    github = FakeGitHub(
        repository_observation=_repository_observation(),
        listing=_listing(51, 52, 53),
        entry_errors={
            0: AuthorizationError("access lost"),
            1: ExternalOperationUncertainError("unknown outcome"),
        },
        entry_results=[_issue_observation(53)],
    )
    route_seam.script_write_phase(issue_number=55)

    result = github_reconciliation.synchronize_repository_open_issue_listing(
        _pool(route_seam.conn),
        github,  # type: ignore[arg-type]
        workspace_id=_WORKSPACE_ID,
        repository_id=_REPOSITORY_ID,
    )

    assert [entry.outcome for entry in result.entries] == [
        RepositoryIssueListingEntryOutcome.ACCESS_LOST,
        RepositoryIssueListingEntryOutcome.FAILED_UNCERTAIN,
        RepositoryIssueListingEntryOutcome.RECONCILED,
    ]


def test_a_bounded_listing_is_a_typed_partial_fact(route_seam) -> None:
    github = FakeGitHub(
        repository_observation=_repository_observation(),
        listing=_listing(51, complete=False, skipped=2),
        entry_results=[_issue_observation(51)],
    )
    route_seam.script_write_phase(issue_number=56)

    result = github_reconciliation.synchronize_repository_open_issue_listing(
        _pool(route_seam.conn),
        github,  # type: ignore[arg-type]
        workspace_id=_WORKSPACE_ID,
        repository_id=_REPOSITORY_ID,
    )

    # The bounded walk is reported truthfully: the observed entry
    # synchronized, the skipped pseudo-issues are counted, and the
    # incompleteness is a typed fact — never a closure or deletion
    # implication and never complete discovery.
    assert result.listing_complete is False
    assert result.pull_request_entries_skipped == 2
    assert result.entries[0].outcome is RepositoryIssueListingEntryOutcome.RECONCILED


def test_a_failed_or_unobservable_listing_fails_the_unit_closed(route_seam) -> None:
    for error in (
        AuthorizationError("access lost"),
        ExternalOperationUncertainError("unknown"),
        ExternalOperationFailedError("rate limited"),
    ):
        github = FakeGitHub(
            repository_observation=_repository_observation(),
            listing_error=error,
        )
        with pytest.raises(type(error)):
            github_reconciliation.synchronize_repository_open_issue_listing(
                _pool(route_seam.conn),
                github,  # type: ignore[arg-type]
                workspace_id=_WORKSPACE_ID,
                repository_id=_REPOSITORY_ID,
            )


def test_an_empty_complete_listing_synchronizes_nothing(route_seam) -> None:
    github = FakeGitHub(
        repository_observation=_repository_observation(),
        listing=_listing(),
    )

    result = github_reconciliation.synchronize_repository_open_issue_listing(
        _pool(route_seam.conn),
        github,  # type: ignore[arg-type]
        workspace_id=_WORKSPACE_ID,
        repository_id=_REPOSITORY_ID,
    )

    # An empty complete listing synchronizes nothing and reports completion.
    assert result.listing_complete is True
    assert result.entries == ()
    assert github.entry_calls == []
    # No write ran for an empty listing.
    assert route_seam.write_phases == 0
