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

from openorc.domain.github_issues import (
    GitHubIssueState,
    github_issue_requirements_fingerprint,
)
from openorc.persistence import deletion as deletion_repositories
from openorc.persistence import github_issues as issue_repositories
from openorc.persistence.pool import DatabasePool
from openorc.services import github_reconciliation as reconciliation_services
from openorc.services.errors import NotFoundError

pytestmark = pytest.mark.integration

_OBSERVED = datetime(2026, 9, 23, 12, 0, 0, tzinfo=UTC)


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


def test_issue_identity_uniqueness_and_the_number_backstop(conn: Connection[Any]) -> None:
    workspace_id, repository_id = _workspace_with_repository(conn)
    first = issue_repositories.reconcile_github_issue(
        _pool(conn), **_reconcile_kwargs(workspace_id=workspace_id, repository_id=repository_id)
    )
    assert first.outcome is issue_repositories.GitHubIssueReconcileOutcome.INSERTED

    # A different stable identity under the same durably mapped number
    # violates the number backstop.
    with pytest.raises(UniqueViolation):
        issue_repositories.reconcile_github_issue(
            _pool(conn),
            **_reconcile_kwargs(
                workspace_id=workspace_id, repository_id=repository_id, github_issue_id=999
            ),
        )

    # The same stable identity under a different number violates the
    # canonical identity uniqueness's address coherence.
    with pytest.raises(UniqueViolation):
        issue_repositories.reconcile_github_issue(
            _pool(conn),
            **_reconcile_kwargs(
                workspace_id=workspace_id, repository_id=repository_id, issue_number=43
            ),
        )


def test_cross_workspace_scope_disagreement_is_unrepresentable(conn: Connection[Any]) -> None:
    workspace_id, repository_id = _workspace_with_repository(conn)
    other_profile = _insert_profile(conn)
    other_workspace = _insert_workspace(conn, other_profile)

    # A projection row whose direct Workspace scope disagrees with the
    # owning Repository is rejected by the composite foreign key.
    with pytest.raises(ForeignKeyViolation):
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

    with pytest.raises(CheckViolation):
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

    with pytest.raises(CheckViolation):
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


def test_number_reuse_fails_closed_without_rebinding_identity(conn: Connection[Any]) -> None:
    workspace_id, repository_id = _workspace_with_repository(conn)
    first = issue_repositories.reconcile_github_issue(
        _pool(conn), **_reconcile_kwargs(workspace_id=workspace_id, repository_id=repository_id)
    )
    assert first.projection is not None

    # A different stable issue losing the race for the durably mapped
    # number: the bounded insert surfaces the durable violation the number
    # backstop exists to enforce; the existing row is untouched.
    with pytest.raises(UniqueViolation):
        issue_repositories.reconcile_github_issue(
            _pool(conn),
            **_reconcile_kwargs(
                workspace_id=workspace_id, repository_id=repository_id, github_issue_id=999
            ),
        )

    reloaded = issue_repositories.find_github_issue(
        _pool(conn), repository_id=repository_id, github_issue_id=503
    )
    assert reloaded is not None
    assert reloaded.id == first.projection.id
