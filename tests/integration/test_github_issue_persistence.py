"""Integration-marked persistence tests for the GitHub issue projection
(issue #59).

These tests apply the committed Supabase migrations within the explicitly
supplied non-production Supabase branch database and prove the durable
issue-projection invariants directly: canonical per-(Repository, stable
GitHub issue ID) uniqueness, the durable issue-number address backstop, the
composite Workspace-consistency foreign key, round trips through the
serialized reconcile write, the true durable no-op (identical
re-reconciliation preserving ``updated_at``), CHECK vocabularies, and the
Repository-aggregate cascade. They are excluded from the ordinary
deterministic baseline by the repository pytest configuration.

The suite consumes the database it is given and never provisions one;
provisioning and teardown sit outside the test suite and outside agent
responsibility. No external GitHub call is involved anywhere: this leaf is
pure OpenOrc-database work.
"""

from __future__ import annotations

import uuid
from contextlib import contextmanager
from datetime import UTC, datetime
from typing import Any, cast

import pytest
from psycopg import Connection
from psycopg.errors import CheckViolation, ForeignKeyViolation, UniqueViolation

from openorc.adapters.github import (
    GitHubIssueObservation,
    GitHubRepositoryObservation,
)
from openorc.domain.github_issues import (
    GitHubIssueState,
    github_issue_requirements_fingerprint,
)
from openorc.persistence import deletion as deletion_repositories
from openorc.persistence import github_issues as issue_repositories
from openorc.persistence.pool import DatabasePool
from openorc.services import github_reconciliation as reconciliation_services
from openorc.services.errors import ConflictError, NotFoundError

pytestmark = pytest.mark.integration

_OBSERVED = datetime(2026, 9, 23, 12, 0, 0, tzinfo=UTC)
_EXTERNAL_INSTALLATION_ID = 12345678


def _insert_profile(conn: Connection[Any]) -> uuid.UUID:
    profile_id = uuid.uuid4()
    # profiles.id references auth.users (id) ON DELETE CASCADE — the single
    # sanctioned Supabase Auth boundary (issue #27): every Profile needs its
    # backing Auth user row. The inserts roll back with the test transaction.
    conn.execute("insert into auth.users (id) values (%s)", (profile_id,))
    conn.execute("insert into openorc.profiles (id) values (%s)", (profile_id,))
    return profile_id


def _insert_workspace(
    conn: Connection[Any], owner_profile_id: uuid.UUID, name: str = "workspace"
) -> uuid.UUID:
    workspace_id = uuid.uuid4()
    conn.execute(
        "insert into openorc.workspaces (id, owner_profile_id, name) values (%s, %s, %s)",
        (workspace_id, owner_profile_id, name),
    )
    return workspace_id


def _insert_project(conn: Connection[Any], workspace_id: uuid.UUID) -> uuid.UUID:
    project_id = uuid.uuid4()
    conn.execute(
        "insert into openorc.projects (id, workspace_id, name) values (%s, %s, %s)",
        (project_id, workspace_id, "project"),
    )
    return project_id


def _insert_repository(
    conn: Connection[Any],
    *,
    project_id: uuid.UUID,
    workspace_id: uuid.UUID,
    github_repository_id: int,
) -> uuid.UUID:
    repository_id = uuid.uuid4()
    conn.execute(
        "insert into openorc.repositories "
        "(id, project_id, workspace_id, github_repository_id, owner_login, name, "
        "html_url, is_private, default_branch) "
        "values (%s, %s, %s, %s, %s, %s, %s, %s, %s)",
        (
            repository_id,
            project_id,
            workspace_id,
            github_repository_id,
            "octocat",
            "hello-world",
            "https://github.com/octocat/hello-world",
            False,
            "main",
        ),
    )
    return repository_id


class IntegrationPool:
    def __init__(self, conn: Connection[Any]) -> None:
        self._conn = conn

    def connection(self) -> Any:
        @contextmanager
        def managed() -> Any:
            yield self._conn

        return managed()


def _pool(conn: Connection[Any]) -> DatabasePool:
    return cast(DatabasePool, IntegrationPool(conn))


def _reconcile_kwargs(
    *,
    workspace_id: uuid.UUID,
    repository_id: uuid.UUID,
    github_issue_id: int = 503,
    issue_number: int = 42,
    title: str = "Found a bug",
    body: str | None = "Requirements body",
    state: GitHubIssueState = GitHubIssueState.OPEN,
    fingerprint: str | None = None,
) -> dict[str, Any]:
    return {
        "workspace_id": workspace_id,
        "repository_id": repository_id,
        "github_issue_id": github_issue_id,
        "issue_number": issue_number,
        "title": title,
        "body": body,
        "state": state,
        "requirements_fingerprint": fingerprint
        or github_issue_requirements_fingerprint(title, body),
        "provider_updated_at": _OBSERVED,
    }


def _workspace_with_repository(conn: Connection[Any]) -> tuple[uuid.UUID, uuid.UUID]:
    profile_id = _insert_profile(conn)
    workspace_id = _insert_workspace(conn, profile_id)
    project_id = _insert_project(conn, workspace_id)
    repository_id = _insert_repository(
        conn, project_id=project_id, workspace_id=workspace_id, github_repository_id=987654321
    )
    return workspace_id, repository_id


def test_the_round_trip_through_the_serialized_reconcile_write(conn: Connection[Any]) -> None:
    workspace_id, repository_id = _workspace_with_repository(conn)

    inserted = issue_repositories.reconcile_github_issue(
        _pool(conn), **_reconcile_kwargs(workspace_id=workspace_id, repository_id=repository_id)
    )
    assert inserted.outcome is issue_repositories.GitHubIssueReconcileOutcome.INSERTED
    assert inserted.projection is not None
    assert inserted.projection.identity.github_issue_id == 503

    loaded = issue_repositories.find_github_issue(
        _pool(conn), repository_id=repository_id, github_issue_id=503
    )
    assert loaded is not None
    assert loaded.title == "Found a bug"
    assert loaded.body == "Requirements body"
    assert loaded.state is GitHubIssueState.OPEN
    by_number = issue_repositories.find_github_issue_by_number(
        _pool(conn), repository_id=repository_id, issue_number=42
    )
    assert by_number is not None and by_number.id == loaded.id

    # A changed observation durably updates in place and reports the true
    # serialized pre-image fingerprint.
    changed = issue_repositories.reconcile_github_issue(
        _pool(conn),
        **_reconcile_kwargs(
            workspace_id=workspace_id,
            repository_id=repository_id,
            title="Renamed requirements",
        ),
    )
    assert changed.outcome is issue_repositories.GitHubIssueReconcileOutcome.UPDATED
    assert changed.projection is not None
    assert changed.projection.title == "Renamed requirements"
    assert changed.previous_fingerprint == inserted.projection.requirements_fingerprint


def test_an_identical_re_reconciliation_preserves_updated_at(conn: Connection[Any]) -> None:
    workspace_id, repository_id = _workspace_with_repository(conn)
    first = issue_repositories.reconcile_github_issue(
        _pool(conn), **_reconcile_kwargs(workspace_id=workspace_id, repository_id=repository_id)
    )
    assert first.projection is not None
    original_updated_at = first.projection.updated_at

    second = issue_repositories.reconcile_github_issue(
        _pool(conn), **_reconcile_kwargs(workspace_id=workspace_id, repository_id=repository_id)
    )

    assert second.outcome is issue_repositories.GitHubIssueReconcileOutcome.UNCHANGED
    assert second.projection is not None
    assert second.projection.updated_at == original_updated_at
    assert second.previous_fingerprint == first.projection.requirements_fingerprint


def test_issue_number_reuse_and_address_coherence_are_classified_outcomes(
    conn: Connection[Any],
) -> None:
    workspace_id, repository_id = _workspace_with_repository(conn)
    first = issue_repositories.reconcile_github_issue(
        _pool(conn), **_reconcile_kwargs(workspace_id=workspace_id, repository_id=repository_id)
    )
    assert first.outcome is issue_repositories.GitHubIssueReconcileOutcome.INSERTED
    assert first.projection is not None

    # A different stable identity losing the race for the durably mapped
    # number: the reconciliation API absorbs the unique violation (its
    # savepoint-scoped insert rolls back), classifies from the re-read
    # durable mappings, and reports the durable NUMBER_CONFLICT — it never
    # propagates the driver exception and never rebinds identity.
    loser = issue_repositories.reconcile_github_issue(
        _pool(conn),
        **_reconcile_kwargs(
            workspace_id=workspace_id, repository_id=repository_id, github_issue_id=999
        ),
    )
    assert loser.outcome is issue_repositories.GitHubIssueReconcileOutcome.NUMBER_CONFLICT
    assert loser.projection is not None
    assert loser.projection.identity.github_issue_id == 503

    # The same stable identity reconciled under a different number is
    # detected by the initial locked identity read — before any insert —
    # and returns the classified IDENTITY_NUMBER_MISMATCH.
    mismatch = issue_repositories.reconcile_github_issue(
        _pool(conn),
        **_reconcile_kwargs(
            workspace_id=workspace_id, repository_id=repository_id, issue_number=43
        ),
    )
    assert (
        mismatch.outcome is issue_repositories.GitHubIssueReconcileOutcome.IDENTITY_NUMBER_MISMATCH
    )
    assert mismatch.projection is not None
    assert mismatch.projection.issue_number == 42

    # Both classified conflicts applied nothing: the canonical projection is
    # exactly the one the first reconciliation created.
    reloaded = issue_repositories.find_github_issue(
        _pool(conn), repository_id=repository_id, github_issue_id=503
    )
    assert reloaded is not None
    assert reloaded.id == first.projection.id
    assert reloaded.issue_number == 42


def test_the_durable_uniqueness_constraints_reject_conflicting_inserts(
    conn: Connection[Any],
) -> None:
    # The database constraints themselves are proven directly: the
    # reconciliation API deliberately absorbs and classifies these
    # violations, so the raw durable backstops are exercised with SQL here.
    workspace_id, repository_id = _workspace_with_repository(conn)
    inserted = issue_repositories.reconcile_github_issue(
        _pool(conn), **_reconcile_kwargs(workspace_id=workspace_id, repository_id=repository_id)
    )
    assert inserted.outcome is issue_repositories.GitHubIssueReconcileOutcome.INSERTED

    insert_sql = (
        "insert into openorc.github_issues "
        "(workspace_id, repository_id, github_issue_id, issue_number, title, body, "
        "state, requirements_fingerprint) "
        "values (%s, %s, %s, %s, %s, %s, %s, %s)"
    )
    base_params = (
        workspace_id,
        repository_id,
    )

    # A different stable identity under the durably mapped number violates
    # the issue-number backstop. Each expected violation runs inside its own
    # savepoint: the failed statement aborts only its savepoint, leaving the
    # test transaction usable for the next assertion.
    with pytest.raises(UniqueViolation), conn.transaction():
        conn.execute(
            insert_sql,
            (
                *base_params,
                999,
                42,
                "Another issue",
                None,
                "open",
                github_issue_requirements_fingerprint("Another issue", None),
            ),
        )

    # The same stable identity under a different number violates the
    # canonical identity uniqueness.
    with pytest.raises(UniqueViolation), conn.transaction():
        conn.execute(
            insert_sql,
            (
                *base_params,
                503,
                43,
                "Found a bug",
                "Requirements body",
                "open",
                github_issue_requirements_fingerprint("Found a bug", "Requirements body"),
            ),
        )


def test_cross_workspace_scope_disagreement_is_unrepresentable(conn: Connection[Any]) -> None:
    workspace_id, repository_id = _workspace_with_repository(conn)
    other_profile = _insert_profile(conn)
    other_workspace = _insert_workspace(conn, other_profile)

    # A projection row whose direct Workspace scope disagrees with the
    # owning Repository is rejected by the composite foreign key. The
    # expected violation runs inside its own savepoint so the test
    # transaction stays usable afterwards.
    with pytest.raises(ForeignKeyViolation), conn.transaction():
        conn.execute(
            "insert into openorc.github_issues "
            "(workspace_id, repository_id, github_issue_id, issue_number, title, body, "
            "state, requirements_fingerprint) "
            "values (%s, %s, %s, %s, %s, %s, %s, %s)",
            (
                other_workspace,
                repository_id,
                777,
                77,
                "Foreign scope",
                None,
                "open",
                github_issue_requirements_fingerprint("Foreign scope", None),
            ),
        )


def test_the_same_external_issue_is_independent_per_workspace(conn: Connection[Any]) -> None:
    workspace_id, repository_id = _workspace_with_repository(conn)
    other_profile = _insert_profile(conn)
    other_workspace = _insert_workspace(conn, other_profile)
    other_project = _insert_project(conn, other_workspace)
    other_repository = _insert_repository(
        conn,
        project_id=other_project,
        workspace_id=other_workspace,
        github_repository_id=987654321,
    )

    first = issue_repositories.reconcile_github_issue(
        _pool(conn), **_reconcile_kwargs(workspace_id=workspace_id, repository_id=repository_id)
    )
    second = issue_repositories.reconcile_github_issue(
        _pool(conn),
        **_reconcile_kwargs(workspace_id=other_workspace, repository_id=other_repository),
    )

    assert first.outcome is issue_repositories.GitHubIssueReconcileOutcome.INSERTED
    assert second.outcome is issue_repositories.GitHubIssueReconcileOutcome.INSERTED
    assert first.projection is not None and second.projection is not None
    assert first.projection.id != second.projection.id


def test_state_and_fingerprint_checks_are_durable(conn: Connection[Any]) -> None:
    workspace_id, repository_id = _workspace_with_repository(conn)

    with pytest.raises(CheckViolation), conn.transaction():
        conn.execute(
            "insert into openorc.github_issues "
            "(workspace_id, repository_id, github_issue_id, issue_number, title, body, "
            "state, requirements_fingerprint) "
            "values (%s, %s, %s, %s, %s, %s, %s, %s)",
            (
                workspace_id,
                repository_id,
                555,
                55,
                "Bad state",
                None,
                "all",
                github_issue_requirements_fingerprint("Bad state", None),
            ),
        )

    with pytest.raises(CheckViolation), conn.transaction():
        conn.execute(
            "insert into openorc.github_issues "
            "(workspace_id, repository_id, github_issue_id, issue_number, title, body, "
            "state, requirements_fingerprint) "
            "values (%s, %s, %s, %s, %s, %s, %s, %s)",
            (
                workspace_id,
                repository_id,
                556,
                56,
                "Bad fingerprint",
                None,
                "open",
                "not-a-digest",
            ),
        )


def test_repository_deletion_cascades_its_issue_projections(conn: Connection[Any]) -> None:
    workspace_id, repository_id = _workspace_with_repository(conn)
    inserted = issue_repositories.reconcile_github_issue(
        _pool(conn), **_reconcile_kwargs(workspace_id=workspace_id, repository_id=repository_id)
    )
    assert inserted.projection is not None

    deletion_repositories.delete_repository(_pool(conn), repository_id)

    assert (
        issue_repositories.find_github_issue(
            _pool(conn), repository_id=repository_id, github_issue_id=503
        )
        is None
    )


def test_the_service_boundary_fails_closed_on_an_unrouted_or_foreign_repository(
    conn: Connection[Any],
) -> None:
    workspace_id, repository_id = _workspace_with_repository(conn)
    other_profile = _insert_profile(conn)
    other_workspace = _insert_workspace(conn, other_profile)
    never_called = cast(Any, object())

    # A Phase 1 Repository with no configured installation route is valid
    # historical state, but reconciliation fails closed uniformly — the
    # GitHub adapter is never constructed or invoked on this path.
    with pytest.raises(NotFoundError):
        reconciliation_services.reconcile_repository_issue(
            _pool(conn),
            never_called,
            workspace_id=workspace_id,
            repository_id=repository_id,
            issue_number=42,
        )

    # A foreign-Workspace repository address is the uniform not-found too.
    with pytest.raises(NotFoundError):
        reconciliation_services.reconcile_repository_issue(
            _pool(conn),
            never_called,
            workspace_id=other_workspace,
            repository_id=repository_id,
            issue_number=42,
        )


class _FakeGitHubClient:
    """Minimal GitHubAppClient fake: typed observations, never the network."""

    def __init__(
        self,
        repository_observation: GitHubRepositoryObservation,
        issue_observation: GitHubIssueObservation,
    ) -> None:
        self._repository_observation = repository_observation
        self._issue_observation = issue_observation

    def get_installation_repository(
        self, *, github_installation_id: int, github_repository_id: int
    ) -> GitHubRepositoryObservation:
        assert github_installation_id == _EXTERNAL_INSTALLATION_ID
        assert github_repository_id == 987654321
        return self._repository_observation

    def get_repository_issue(
        self,
        *,
        github_installation_id: int,
        owner_login: str,
        repository_name: str,
        issue_number: int,
    ) -> GitHubIssueObservation:
        assert github_installation_id == _EXTERNAL_INSTALLATION_ID
        assert (owner_login, repository_name, issue_number) == ("octocat", "hello-world", 42)
        return self._issue_observation

    def validate_installation_repository_access(self, **_kwargs: Any) -> Any:
        raise AssertionError("the service must not compose raw #58 validation operations")

    def get_installation_capabilities(self, github_installation_id: int) -> Any:
        raise AssertionError("the service must not compose raw adapter operations")

    def get_issue_blocked_by(
        self,
        *,
        github_installation_id: int,
        owner_login: str,
        repository_name: str,
        issue_number: int,
    ) -> Any:
        raise AssertionError("this B3 reconciliation fake must not observe relationships")

    def get_issue_sub_issues(
        self,
        *,
        github_installation_id: int,
        owner_login: str,
        repository_name: str,
        issue_number: int,
    ) -> Any:
        raise AssertionError("this B3 reconciliation fake must not observe relationships")

    def get_issue_parent(
        self,
        *,
        github_installation_id: int,
        owner_login: str,
        repository_name: str,
        issue_number: int,
    ) -> Any:
        raise AssertionError("this B3 reconciliation fake must not observe relationships")

    def get_repository_by_address(
        self, *, github_installation_id: int, owner_login: str, repository_name: str
    ) -> int:
        raise AssertionError("this B3 reconciliation fake must not resolve addresses")


def _workspace_with_routed_repository(
    conn: Connection[Any],
) -> tuple[uuid.UUID, uuid.UUID]:
    """A Phase 1 aggregate plus the #57 installation route, all real rows."""
    workspace_id, repository_id = _workspace_with_repository(conn)
    installation_id = uuid.uuid4()
    conn.execute(
        "insert into openorc.github_installations "
        "(id, workspace_id, github_installation_id, github_account_id, account_login, "
        "account_type, suspended_at) "
        "values (%s, %s, %s, %s, %s, %s, %s)",
        (
            installation_id,
            workspace_id,
            _EXTERNAL_INSTALLATION_ID,
            501,
            "octocat",
            "Organization",
            None,
        ),
    )
    conn.execute(
        "update openorc.repositories set github_installation_id = %s where id = %s",
        (installation_id, repository_id),
    )
    return workspace_id, repository_id


def test_the_service_reconciles_an_issue_end_to_end_against_the_real_schema(
    conn: Connection[Any],
) -> None:
    workspace_id, repository_id = _workspace_with_routed_repository(conn)
    github = _FakeGitHubClient(
        GitHubRepositoryObservation(
            github_repository_id=987654321,
            owner_login="octocat",
            name="hello-world",
            html_url="https://github.com/octocat/hello-world",
            is_private=False,
            default_branch="main",
        ),
        GitHubIssueObservation(
            github_issue_id=503,
            issue_number=42,
            title="Found a bug",
            body="Requirements body",
            state="open",
            provider_updated_at=_OBSERVED,
            is_pull_request=False,
        ),
    )

    result = reconciliation_services.reconcile_repository_issue(
        _pool(conn),
        github,
        workspace_id=workspace_id,
        repository_id=repository_id,
        issue_number=42,
    )

    assert result.issue_created is True
    assert result.requirements_changed is False
    assert result.repository_metadata_changed is False
    assert result.previous_fingerprint is None
    durable = issue_repositories.find_github_issue(
        _pool(conn), repository_id=repository_id, github_issue_id=503
    )
    assert durable is not None
    assert durable.title == "Found a bug"
    assert durable.state is GitHubIssueState.OPEN


def test_the_service_fails_closed_on_number_reuse_without_rebinding_identity(
    conn: Connection[Any],
) -> None:
    workspace_id, repository_id = _workspace_with_routed_repository(conn)
    first = issue_repositories.reconcile_github_issue(
        _pool(conn), **_reconcile_kwargs(workspace_id=workspace_id, repository_id=repository_id)
    )
    assert first.projection is not None

    # The authoritative read reports a different stable issue for the
    # durably mapped number: the serialized classification reaches the
    # service boundary as a typed conflict, and the canonical projection is
    # never rebound.
    github = _FakeGitHubClient(
        GitHubRepositoryObservation(
            github_repository_id=987654321,
            owner_login="octocat",
            name="hello-world",
            html_url="https://github.com/octocat/hello-world",
            is_private=False,
            default_branch="main",
        ),
        GitHubIssueObservation(
            github_issue_id=999,
            issue_number=42,
            title="Found a bug",
            body="Requirements body",
            state="open",
            provider_updated_at=_OBSERVED,
            is_pull_request=False,
        ),
    )

    with pytest.raises(ConflictError):
        reconciliation_services.reconcile_repository_issue(
            _pool(conn),
            github,
            workspace_id=workspace_id,
            repository_id=repository_id,
            issue_number=42,
        )

    reloaded = issue_repositories.find_github_issue(
        _pool(conn), repository_id=repository_id, github_issue_id=503
    )
    assert reloaded is not None
    assert reloaded.id == first.projection.id
    assert reloaded.issue_number == 42
