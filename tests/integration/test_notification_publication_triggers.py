"""Integration test for the G4b notification publication triggers (#398).

The focused real-Postgres publication suite (issue #398): the committed
notification trigger machinery runs against the explicitly supplied
non-production branch database while a separate autocommit LISTEN session
collects the raw published payloads. Representative shared trigger families
are parameterized rather than exhaustive per-mutation tests:

- committed audit insertion publishes one workflow_event hint;
- non-audit GitHub issue metadata changes publish the issue hint plus the
  conservative workspace hint, and a genuine unchanged UPDATE publishes
  nothing;
- relationship-edge changes publish the anchored endpoint's issue hint plus
  the workspace hint, and an identical replacement is a no-op;
- Task child writes map to task hints, and retained cost checkpoint writes
  publish while the high-water no-op does not;
- refresh admission/claim/completion publish refresh + repository hints, and
  a workspace-scoped refresh publishes refresh + workspace hints;
- configuration writes publish their resource hints, with the identical
  whole-record role-binding write publishing nothing;
- establishing, lease-refreshing (nothing), and clearing the
  account-deletion barrier fans out workspace hints to the Profile's owned
  Workspaces only;
- GitHub user authorization changes fan out to the owned Workspaces;
- deletion publishes OLD identities through cascades, including the removed
  Workspace's own identity for stream invalidation;
- queue membership changes publish the workspace hint alongside the task and
  issue hints;
- a mutation in one Workspace never publishes another Workspace's hints;
- a rolled-back application mutation publishes nothing (one representative
  case; the full commit/savepoint matrix is G4a's established suite).

Every collected payload must decode through the strict contract decoder and
equal ``encode_notification_payload`` output byte for byte.

Run explicitly when a target has been made available:

    OPENORC_TEST_DATABASE_URL=<supplied non-production branch database URL> \\
      .venv/bin/python -m pytest -m integration \\
      tests/integration/test_notification_publication_triggers.py

The suite consumes the database it is given and never provisions one; it
skips cleanly when ``OPENORC_TEST_DATABASE_URL`` is absent. Unlike the
rollback-isolated shared fixtures, this suite commits (notifications are
visible only after the outer commit), so every test seeds its own freshly
generated identities and the ephemeral branch database is reset once per
session by the shared conftest bootstrap.
"""

from __future__ import annotations

from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any
from uuid import uuid4

import pytest
from psycopg import Connection, connect

from openorc.domain.connections import AdapterType, WorkflowRole
from openorc.domain.events import WorkflowEventActor, WorkflowEventType
from openorc.domain.github_issue_relations import RelatedIssueEndpoint
from openorc.domain.github_refreshes import GitHubRefreshStatus, GitHubRefreshSummary
from openorc.domain.tasks import TaskStatus
from openorc.notifications.contract import (
    DataChangedNotification,
    NotificationResourceType,
    WorkflowEventNotification,
    decode_notification_payload,
    encode_notification_payload,
)
from openorc.persistence import (
    connections as connection_repositories,
)
from openorc.persistence import (
    deletion as deletion_repositories,
)
from openorc.persistence import (
    events as event_repositories,
)
from openorc.persistence import (
    github_issue_relations as relation_repositories,
)
from openorc.persistence import (
    github_refreshes as refresh_repositories,
)
from openorc.persistence import (
    github_user_authorizations as authorization_repositories,
)
from openorc.persistence import (
    ownership as ownership_repositories,
)
from openorc.persistence import (
    planning as planning_repositories,
)
from openorc.persistence import (
    sessions as session_repositories,
)
from openorc.persistence import (
    tasks as task_repositories,
)
from openorc.persistence.github_issue_relations import GitHubRelationReplaceOutcome
from tests.integration.persistence_fixtures import (
    _insert_connection,
    _insert_installation,
    _insert_profile,
    _insert_ready_session,
    _insert_repository,
    _insert_task,
    _insert_workspace,
    _SingleConnectionPool,
)

pytestmark = pytest.mark.integration

_CHANNEL = "openorc_notifications"
_FINGERPRINT = "a" * 64


class NotificationCollector:
    """One autocommit LISTEN session collecting raw payloads with bounded waits."""

    def __init__(self, database_url: str) -> None:
        self._connection = connect(database_url, autocommit=True)
        self._connection.execute(f"listen {_CHANNEL}")

    def collect(self, *, count: int = 1, timeout: float = 10.0) -> list[str]:
        payloads: list[str] = []
        for notify in self._connection.notifies(timeout=timeout, stop_after=count):
            payloads.append(notify.payload)
        return payloads

    def quiesce(self, *, timeout: float = 1.0) -> list[str]:
        """Drain everything currently deliverable within a short window."""
        return self.collect(count=10_000, timeout=timeout)

    def close(self) -> None:
        self._connection.close()


@pytest.fixture
def writer(migrated_database: str) -> Iterator[Connection[Any]]:
    """One autocommit writer connection: every mutation commits on statement."""
    connection = connect(migrated_database, autocommit=True)
    yield connection
    connection.close()


@pytest.fixture
def pool(writer: Connection[Any]) -> _SingleConnectionPool:
    return _SingleConnectionPool(writer)


@pytest.fixture
def collector(migrated_database: str) -> Iterator[NotificationCollector]:
    collector = NotificationCollector(migrated_database)
    yield collector
    collector.close()


def _decoded(payloads: list[str]) -> list[Any]:
    return [decode_notification_payload(payload) for payload in payloads]


def _assert_canonical(payloads: list[str]) -> None:
    for payload in payloads:
        decoded = decode_notification_payload(payload)
        assert payload == encode_notification_payload(decoded), (
            "published payload is not byte-identical to the canonical encoder output"
        )


def _hints(collector: NotificationCollector, count: int, *, timeout: float = 10.0) -> list[Any]:
    payloads = collector.collect(count=count, timeout=timeout)
    _assert_canonical(payloads)
    return _decoded(payloads)


def _hint_identities(hints: list[Any]) -> set[tuple[Any, Any, Any]]:
    return {
        (hint.workspace_id, getattr(hint, "resource_type", None), hint.resource_id)
        for hint in hints
    }


def _data_changed(
    workspace_id: Any, resource_type: NotificationResourceType, resource_id: Any
) -> DataChangedNotification:
    return DataChangedNotification(
        workspace_id=workspace_id,
        resource_type=resource_type,
        resource_id=resource_id,
    )


def _insert_issue(
    conn: Connection[Any],
    *,
    workspace_id: Any,
    repository_id: Any,
    github_issue_id: int,
    title: str = "issue title",
) -> Any:
    issue_id = uuid4()
    conn.execute(
        "insert into openorc.github_issues "
        "(id, workspace_id, repository_id, github_issue_id, issue_number, title, body, "
        "state, requirements_fingerprint) "
        "values (%s, %s, %s, %s, %s, %s, null, 'open', %s)",
        (
            issue_id,
            workspace_id,
            repository_id,
            github_issue_id,
            github_issue_id,
            title,
            _FINGERPRINT,
        ),
    )
    return issue_id


def _seed_workspace_graph(conn: Connection[Any], *, owner_profile_id: Any) -> tuple[Any, Any, Any]:
    workspace_id = _insert_workspace(conn, owner_profile_id)
    installation_id = _insert_installation(conn, workspace_id=workspace_id)
    repository_id = _insert_repository(
        conn, workspace_id=workspace_id, github_installation_id=installation_id
    )
    return workspace_id, repository_id, installation_id


def test_committed_audit_insertion_publishes_one_workflow_event_hint(
    writer: Connection[Any], pool: _SingleConnectionPool, collector: NotificationCollector
) -> None:
    profile_id = _insert_profile(writer)
    workspace_id = _insert_workspace(writer, profile_id)
    collector.quiesce()

    event = event_repositories.record_workflow_event(
        pool,
        workspace_id=workspace_id,
        event_type=WorkflowEventType.TASK_CREATED,
        actor_type=WorkflowEventActor.OWNER,
    )

    hints = _hints(collector, count=1)
    assert hints == [WorkflowEventNotification(workspace_id, event.id)]
    assert collector.quiesce(timeout=1.0) == []


def test_issue_metadata_changes_publish_issue_and_workspace_hints(
    writer: Connection[Any], pool: _SingleConnectionPool, collector: NotificationCollector
) -> None:
    profile_id = _insert_profile(writer)
    workspace_id, repository_id, _ = _seed_workspace_graph(writer, owner_profile_id=profile_id)
    issue_id = _insert_issue(
        writer, workspace_id=workspace_id, repository_id=repository_id, github_issue_id=7701
    )
    collector.quiesce()

    writer.execute(
        "update openorc.github_issues set title = %s where id = %s",
        ("renamed issue", issue_id),
    )
    hints = _hints(collector, count=2)
    assert _hint_identities(hints) == {
        (workspace_id, NotificationResourceType.ISSUE, issue_id),
        (workspace_id, NotificationResourceType.WORKSPACE, workspace_id),
    }

    # A genuine unchanged UPDATE is suppressed whole-row.
    writer.execute(
        "update openorc.github_issues set title = %s where id = %s",
        ("renamed issue", issue_id),
    )
    assert collector.quiesce(timeout=1.0) == []

    writer.execute(
        "update openorc.github_issues set state = %s where id = %s",
        ("closed", issue_id),
    )
    hints = _hints(collector, count=2)
    assert _hint_identities(hints) == {
        (workspace_id, NotificationResourceType.ISSUE, issue_id),
        (workspace_id, NotificationResourceType.WORKSPACE, workspace_id),
    }


def test_relationship_edge_changes_publish_anchored_and_workspace_hints(
    writer: Connection[Any], pool: _SingleConnectionPool, collector: NotificationCollector
) -> None:
    profile_id = _insert_profile(writer)
    workspace_id, repository_id, _ = _seed_workspace_graph(writer, owner_profile_id=profile_id)
    anchor_issue_id = _insert_issue(
        writer, workspace_id=workspace_id, repository_id=repository_id, github_issue_id=7701
    )
    # A second Repository in the same Workspace with its own observed issue:
    # the cross-repository parent endpoint.
    second_repository_id = uuid4()
    second_project_id = uuid4()
    writer.execute(
        "insert into openorc.projects (id, workspace_id, name) values (%s, %s, %s)",
        (second_project_id, workspace_id, "second project"),
    )
    writer.execute(
        "insert into openorc.repositories "
        "(id, project_id, workspace_id, github_repository_id, owner_login, name, html_url, "
        "is_private, default_branch) "
        "values (%s, %s, %s, %s, %s, %s, %s, %s, %s)",
        (
            second_repository_id,
            second_project_id,
            workspace_id,
            77_000_006,
            "octocat",
            "second-repo",
            "https://github.com/octocat/second-repo",
            False,
            "main",
        ),
    )
    related_issue_id = _insert_issue(
        writer,
        workspace_id=workspace_id,
        repository_id=second_repository_id,
        github_issue_id=8801,
    )
    collector.quiesce()

    parent = relation_repositories.replace_github_issue_parent_edge(
        pool,
        workspace_id=workspace_id,
        repository_id=repository_id,
        github_issue_id=7701,
        parent=None,
    )

    assert parent == GitHubRelationReplaceOutcome.UNCHANGED  # no edge existed
    assert collector.quiesce(timeout=1.0) == []

    outcome = relation_repositories.replace_github_issue_parent_edge(
        pool,
        workspace_id=workspace_id,
        repository_id=repository_id,
        github_issue_id=7701,
        parent=RelatedIssueEndpoint(github_repository_id=77_000_006, github_issue_id=8801),
    )
    assert outcome == GitHubRelationReplaceOutcome.CHANGED
    hints = _hints(collector, count=2)
    # The anchored endpoint's issue hint plus the conservative workspace hint;
    # the cross-repository related endpoint is covered by the workspace hint.
    assert _hint_identities(hints) == {
        (workspace_id, NotificationResourceType.ISSUE, anchor_issue_id),
        (workspace_id, NotificationResourceType.WORKSPACE, workspace_id),
    }

    outcome = relation_repositories.replace_github_issue_parent_edge(
        pool,
        workspace_id=workspace_id,
        repository_id=repository_id,
        github_issue_id=7701,
        parent=RelatedIssueEndpoint(github_repository_id=77_000_006, github_issue_id=8801),
    )
    assert outcome == GitHubRelationReplaceOutcome.UNCHANGED
    assert collector.quiesce(timeout=1.0) == []

    assert related_issue_id  # used only to prove the related endpoint exists


def test_task_child_writes_map_to_task_hints(
    writer: Connection[Any], pool: _SingleConnectionPool, collector: NotificationCollector
) -> None:
    profile_id = _insert_profile(writer)
    workspace_id, repository_id, _ = _seed_workspace_graph(writer, owner_profile_id=profile_id)
    task_id = _insert_task(writer, workspace_id=workspace_id, repository_id=repository_id)
    collector.quiesce()

    revision = planning_repositories.create_plan_revision(
        pool,
        workspace_id=workspace_id,
        task_id=task_id,
        revision_number=1,
        content="the plan text",
        repository_base_sha="b" * 40,
    )

    hints = _hints(collector, count=1)
    assert hints == [_data_changed(workspace_id, NotificationResourceType.TASK, task_id)]
    assert revision.task_id == task_id
    assert collector.quiesce(timeout=1.0) == []


def test_retained_cost_checkpoints_publish_and_noops_do_not(
    writer: Connection[Any], pool: _SingleConnectionPool, collector: NotificationCollector
) -> None:
    profile_id = _insert_profile(writer)
    workspace_id, repository_id, _ = _seed_workspace_graph(writer, owner_profile_id=profile_id)
    task_id = _insert_task(writer, workspace_id=workspace_id, repository_id=repository_id)
    connection_id = _insert_connection(writer, workspace_id=workspace_id)
    session_id = _insert_ready_session(
        writer,
        workspace_id=workspace_id,
        task_id=task_id,
        connection_id=connection_id,
        role="producer",
        external_session_id=f"ext-{uuid4()}",
    )
    collector.quiesce()

    checkpointed = session_repositories.checkpoint_task_agent_session_cost(
        pool,
        workspace_id=workspace_id,
        task_id=task_id,
        task_agent_session_id=session_id,
        connection_id=connection_id,
        retained_cost_usd=Decimal("1.25"),
    )
    assert checkpointed is not None
    hints = _hints(collector, count=1)
    assert hints == [_data_changed(workspace_id, NotificationResourceType.TASK, task_id)]

    # The coalesced high-water path: an equal or lower observation matches no
    # row and publishes nothing.
    equal = session_repositories.checkpoint_task_agent_session_cost(
        pool,
        workspace_id=workspace_id,
        task_id=task_id,
        task_agent_session_id=session_id,
        connection_id=connection_id,
        retained_cost_usd=Decimal("1.25"),
    )
    assert equal is None
    lower = session_repositories.checkpoint_task_agent_session_cost(
        pool,
        workspace_id=workspace_id,
        task_id=task_id,
        task_agent_session_id=session_id,
        connection_id=connection_id,
        retained_cost_usd=Decimal("1.00"),
    )
    assert lower is None
    assert collector.quiesce(timeout=1.0) == []


def test_refresh_lifecycle_publishes_refresh_and_scope_hints(
    writer: Connection[Any], pool: _SingleConnectionPool, collector: NotificationCollector
) -> None:
    profile_id = _insert_profile(writer)
    workspace_id, repository_id, _ = _seed_workspace_graph(writer, owner_profile_id=profile_id)
    collector.quiesce()

    admitted = refresh_repositories.admit_github_refresh(
        pool, workspace_id=workspace_id, repository_id=repository_id
    )
    assert admitted.record is not None
    hints = _hints(collector, count=2)
    assert _hint_identities(hints) == {
        (workspace_id, NotificationResourceType.REFRESH, admitted.record.id),
        (workspace_id, NotificationResourceType.REPOSITORY, repository_id),
    }

    claimed = refresh_repositories.claim_github_refresh(pool, refresh_id=admitted.record.id)
    assert claimed is not None
    hints = _hints(collector, count=2)
    assert _hint_identities(hints) == {
        (workspace_id, NotificationResourceType.REFRESH, claimed.id),
        (workspace_id, NotificationResourceType.REPOSITORY, repository_id),
    }

    completed = refresh_repositories.complete_github_refresh(
        pool,
        refresh_id=claimed.id,
        status=GitHubRefreshStatus.COMPLETED,
        summary=GitHubRefreshSummary(repositories_checked=1),
        errors=(),
    )
    assert completed is not None
    hints = _hints(collector, count=2)
    assert _hint_identities(hints) == {
        (workspace_id, NotificationResourceType.REFRESH, completed.id),
        (workspace_id, NotificationResourceType.REPOSITORY, repository_id),
    }

    # A workspace-scoped refresh publishes the workspace hint instead.
    workspace_admitted = refresh_repositories.admit_github_refresh(
        pool, workspace_id=workspace_id, repository_id=None
    )
    assert workspace_admitted.record is not None
    hints = _hints(collector, count=2)
    assert _hint_identities(hints) == {
        (workspace_id, NotificationResourceType.REFRESH, workspace_admitted.record.id),
        (workspace_id, NotificationResourceType.WORKSPACE, workspace_id),
    }


def test_configuration_writes_publish_resource_hints(
    writer: Connection[Any], pool: _SingleConnectionPool, collector: NotificationCollector
) -> None:
    profile_id = _insert_profile(writer)
    collector.quiesce()

    # Workspace root creation, configuration update, and deletion (asserted in
    # the deletion suite below) all publish the row-id workspace hint.
    workspace_id = _insert_workspace(writer, profile_id)
    hints = _hints(collector, count=1)
    assert hints == [_data_changed(workspace_id, NotificationResourceType.WORKSPACE, workspace_id)]

    created = connection_repositories.create_connection(
        pool,
        workspace_id=workspace_id,
        adapter=AdapterType.CLINE,
        name="primary hub",
    )
    hints = _hints(collector, count=1)
    assert hints == [_data_changed(workspace_id, NotificationResourceType.CONNECTION, created.id)]

    updated = connection_repositories.update_connection(
        pool,
        created.id,
        name="renamed hub",
        safe_config={"repository_base": "/srv/runtime"},
        session_capacity=2,
        enabled=True,
        auth_reference=None,
    )
    assert updated is not None
    hints = _hints(collector, count=1)
    assert hints == [_data_changed(workspace_id, NotificationResourceType.CONNECTION, created.id)]

    binding, changed = connection_repositories.set_role_binding(
        pool,
        workspace_id=workspace_id,
        role=WorkflowRole.PRODUCER,
        connection_id=created.id,
        configured_provider="provider-a",
        configured_model="model-a",
        role_prompt_override=None,
    )
    assert changed
    hints = _hints(collector, count=1)
    assert hints == [_data_changed(workspace_id, NotificationResourceType.ROLE_BINDING, binding.id)]

    # The identical whole-record write is a locked no-op and publishes nothing.
    _, unchanged = connection_repositories.set_role_binding(
        pool,
        workspace_id=workspace_id,
        role=WorkflowRole.PRODUCER,
        connection_id=created.id,
        configured_provider="provider-a",
        configured_model="model-a",
        role_prompt_override=None,
    )
    assert not unchanged
    assert collector.quiesce(timeout=1.0) == []

    binding, changed = connection_repositories.set_role_binding(
        pool,
        workspace_id=workspace_id,
        role=WorkflowRole.PRODUCER,
        connection_id=created.id,
        configured_provider="provider-a",
        configured_model="model-b",
        role_prompt_override=None,
    )
    assert changed
    hints = _hints(collector, count=1)
    assert hints == [_data_changed(workspace_id, NotificationResourceType.ROLE_BINDING, binding.id)]

    # Project and Workspace configuration rows publish their own hints.
    project_id = uuid4()
    writer.execute(
        "insert into openorc.projects (id, workspace_id, name) values (%s, %s, %s)",
        (project_id, workspace_id, "project"),
    )
    collector.quiesce()
    writer.execute(
        "update openorc.projects set name = %s where id = %s",
        ("renamed project", project_id),
    )
    hints = _hints(collector, count=1)
    assert hints == [_data_changed(workspace_id, NotificationResourceType.PROJECT, project_id)]
    writer.execute(
        "update openorc.workspaces set name = %s where id = %s",
        ("renamed workspace", workspace_id),
    )
    hints = _hints(collector, count=1)
    assert hints == [_data_changed(workspace_id, NotificationResourceType.WORKSPACE, workspace_id)]


def test_account_deletion_barrier_fans_out_workspace_hints(
    writer: Connection[Any], pool: _SingleConnectionPool, collector: NotificationCollector
) -> None:
    owner_id = _insert_profile(writer)
    other_id = _insert_profile(writer)
    first_workspace = _insert_workspace(writer, owner_id)
    second_workspace = _insert_workspace(writer, owner_id)
    _insert_workspace(writer, other_id)
    collector.quiesce()

    attempt_id = uuid4()
    assert ownership_repositories.claim_account_deletion_attempt(
        pool, profile_id=owner_id, attempt_id=attempt_id
    )
    hints = _hints(collector, count=2)
    assert _hint_identities(hints) == {
        (first_workspace, NotificationResourceType.WORKSPACE, first_workspace),
        (second_workspace, NotificationResourceType.WORKSPACE, second_workspace),
    }

    # The lease-refresh write changes only the attempt id and instant while
    # the state stays 'active': the WHEN-scoped trigger publishes nothing.
    refreshed_attempt = uuid4()
    assert ownership_repositories.reclaim_expired_account_deletion_attempt(
        pool,
        profile_id=owner_id,
        expired_attempt_id=attempt_id,
        new_attempt_id=refreshed_attempt,
        lease_seconds=0.0,
    )
    assert collector.quiesce(timeout=1.0) == []

    assert ownership_repositories.clear_account_deletion_attempt(
        pool, profile_id=owner_id, attempt_id=refreshed_attempt
    )
    hints = _hints(collector, count=2)
    assert _hint_identities(hints) == {
        (first_workspace, NotificationResourceType.WORKSPACE, first_workspace),
        (second_workspace, NotificationResourceType.WORKSPACE, second_workspace),
    }


def test_github_user_authorization_changes_fan_out_to_owned_workspaces(
    writer: Connection[Any], pool: _SingleConnectionPool, collector: NotificationCollector
) -> None:
    profile_id = _insert_profile(writer)
    first_workspace = _insert_workspace(writer, profile_id)
    second_workspace = _insert_workspace(writer, profile_id)
    collector.quiesce()

    authorization_repositories.insert_active_github_user_authorization(
        pool,
        profile_id=profile_id,
        github_user_id=123_456,
        github_login="octocat",
        refresh_secret_reference="opaque-vault-reference",
        refresh_expires_at=datetime.now(UTC) + timedelta(days=30),
    )
    hints = _hints(collector, count=2)
    assert _hint_identities(hints) == {
        (first_workspace, NotificationResourceType.WORKSPACE, first_workspace),
        (second_workspace, NotificationResourceType.WORKSPACE, second_workspace),
    }

    revoked = authorization_repositories.revoke_github_user_authorization_row(
        pool, profile_id=profile_id
    )
    assert revoked is not None and revoked.status.value == "revoked"
    hints = _hints(collector, count=2)
    assert _hint_identities(hints) == {
        (first_workspace, NotificationResourceType.WORKSPACE, first_workspace),
        (second_workspace, NotificationResourceType.WORKSPACE, second_workspace),
    }


def test_deletion_publishes_old_identities_through_cascades(
    writer: Connection[Any], pool: _SingleConnectionPool, collector: NotificationCollector
) -> None:
    profile_id = _insert_profile(writer)
    workspace_id, repository_id, _ = _seed_workspace_graph(writer, owner_profile_id=profile_id)
    issue_id = _insert_issue(
        writer, workspace_id=workspace_id, repository_id=repository_id, github_issue_id=7701
    )
    task_id = _insert_task(writer, workspace_id=workspace_id, repository_id=repository_id)
    project_row = writer.execute(
        "select project_id from openorc.repositories where id = %s",
        (repository_id,),
    ).fetchone()
    assert project_row is not None
    project_id = project_row[0]
    collector.quiesce()

    deleted = deletion_repositories.delete_repository(pool, repository_id)
    assert deleted is not None
    # The task delete publishes its task hint; the cascaded issue-projection
    # delete publishes the issue and workspace hints; the repository delete
    # publishes the repository hint. The fixture task's issue identity is
    # unobserved, so exactly these four coalesced deliveries arrive.
    hints = _hints(collector, count=4)
    assert _hint_identities(hints) == {
        (workspace_id, NotificationResourceType.TASK, task_id),
        (workspace_id, NotificationResourceType.ISSUE, issue_id),
        (workspace_id, NotificationResourceType.WORKSPACE, workspace_id),
        (workspace_id, NotificationResourceType.REPOSITORY, repository_id),
    }

    # Workspace removal publishes its own identity for stream invalidation.
    # The repository deletion above leaves the Workspace's Project in place,
    # so the sanctioned workspace cascade also removes it: its project hint
    # publishes alongside the removed Workspace's own identity, and the
    # installation's conservative workspace hint coalesces into the same
    # workspace payload.
    deleted_workspace = deletion_repositories.delete_workspace(pool, workspace_id)
    assert deleted_workspace is not None
    hints = _hints(collector, count=2)
    assert _hint_identities(hints) == {
        (workspace_id, NotificationResourceType.WORKSPACE, workspace_id),
        (workspace_id, NotificationResourceType.PROJECT, project_id),
    }


def test_queue_membership_changes_publish_workspace_hints(
    writer: Connection[Any], pool: _SingleConnectionPool, collector: NotificationCollector
) -> None:
    profile_id = _insert_profile(writer)
    workspace_id, repository_id, _ = _seed_workspace_graph(writer, owner_profile_id=profile_id)
    issue_id = _insert_issue(
        writer, workspace_id=workspace_id, repository_id=repository_id, github_issue_id=7701
    )
    collector.quiesce()

    # Intake: READY_TO_PLAN with no frozen fingerprint (the intake contract).
    # queued_at is non-null exactly when the status is queued, and only the
    # guarded ready_to_plan -> queued transition populates it.
    intake = task_repositories.create_task(
        pool,
        workspace_id=workspace_id,
        repository_id=repository_id,
        github_issue_id=7701,
        github_issue_number=42,
        status=TaskStatus.READY_TO_PLAN,
    )
    hints = _hints(collector, count=2)
    assert _hint_identities(hints) == {
        (workspace_id, NotificationResourceType.TASK, intake.id),
        (workspace_id, NotificationResourceType.ISSUE, issue_id),
    }

    # Owner Start: the guarded transition freezes the requirements baseline
    # atomically with queue entry; the workspace hint covers other queued
    # Tasks' derived queue_position and the Workspace queue projection.
    queued = task_repositories.update_task_status(
        pool,
        intake.id,
        expected_state_token=intake.state_token,
        status=TaskStatus.QUEUED,
        capture_source_requirements_fingerprint=_FINGERPRINT,
    )
    assert queued is not None
    hints = _hints(collector, count=3)
    assert _hint_identities(hints) == {
        (workspace_id, NotificationResourceType.TASK, intake.id),
        (workspace_id, NotificationResourceType.ISSUE, issue_id),
        (workspace_id, NotificationResourceType.WORKSPACE, workspace_id),
    }

    # Queue exit: the queued -> planning claim clears queued_at.
    claimed = task_repositories.update_task_status(
        pool,
        intake.id,
        expected_state_token=queued.state_token,
        status=TaskStatus.PLANNING,
    )
    assert claimed is not None
    hints = _hints(collector, count=3)
    assert _hint_identities(hints) == {
        (workspace_id, NotificationResourceType.TASK, intake.id),
        (workspace_id, NotificationResourceType.ISSUE, issue_id),
        (workspace_id, NotificationResourceType.WORKSPACE, workspace_id),
    }

    # A non-queued intake publishes no workspace hint (a different observed
    # issue keeps the one-current-Task-per-issue invariant intact; still no
    # frozen fingerprint before an Owner Start).
    second_issue_id = _insert_issue(
        writer, workspace_id=workspace_id, repository_id=repository_id, github_issue_id=7702
    )
    collector.quiesce()
    idle = task_repositories.create_task(
        pool,
        workspace_id=workspace_id,
        repository_id=repository_id,
        github_issue_id=7702,
        github_issue_number=43,
        status=TaskStatus.READY_TO_PLAN,
    )
    hints = _hints(collector, count=2)
    assert _hint_identities(hints) == {
        (workspace_id, NotificationResourceType.TASK, idle.id),
        (workspace_id, NotificationResourceType.ISSUE, second_issue_id),
    }


def test_mutations_publish_only_their_own_workspace_hints(
    writer: Connection[Any], pool: _SingleConnectionPool, collector: NotificationCollector
) -> None:
    profile_id = _insert_profile(writer)
    other_profile_id = _insert_profile(writer)
    first_workspace, first_repository, _ = _seed_workspace_graph(
        writer, owner_profile_id=profile_id
    )
    second_workspace, second_repository, _ = _seed_workspace_graph(
        writer, owner_profile_id=other_profile_id
    )
    _insert_issue(
        writer, workspace_id=first_workspace, repository_id=first_repository, github_issue_id=7701
    )
    _insert_issue(
        writer,
        workspace_id=second_workspace,
        repository_id=second_repository,
        github_issue_id=7701,
    )
    first_task = _insert_task(writer, workspace_id=first_workspace, repository_id=first_repository)
    second_task = _insert_task(
        writer, workspace_id=second_workspace, repository_id=second_repository
    )
    collector.quiesce()

    # The same external issue identity exists independently in both
    # Workspaces; a mutation in the first Workspace never publishes the
    # second Workspace's identity.
    writer.execute(
        "update openorc.tasks set canonical_feature_branch = %s where id = %s",
        (f"feature/{uuid4().hex[:8]}", first_task),
    )
    hints = _hints(collector, count=1)
    assert hints == [_data_changed(first_workspace, NotificationResourceType.TASK, first_task)]
    writer.execute(
        "update openorc.tasks set canonical_feature_branch = %s where id = %s",
        (f"feature/{uuid4().hex[:8]}", second_task),
    )
    hints = _hints(collector, count=1)
    assert hints == [_data_changed(second_workspace, NotificationResourceType.TASK, second_task)]


def test_installation_changes_conservatively_invalidate_the_workspace(
    writer: Connection[Any], pool: _SingleConnectionPool, collector: NotificationCollector
) -> None:
    profile_id = _insert_profile(writer)
    workspace_id, _, installation_id = _seed_workspace_graph(writer, owner_profile_id=profile_id)
    collector.quiesce()

    writer.execute(
        "update openorc.github_installations set account_login = %s where id = %s",
        ("renamed-account", installation_id),
    )
    hints = _hints(collector, count=1)
    # The closed vocabulary has no installation resource: the conservative
    # workspace hint covers the installation and routed-repository reads.
    assert hints == [_data_changed(workspace_id, NotificationResourceType.WORKSPACE, workspace_id)]


def test_rolled_back_mutation_publishes_nothing(
    writer: Connection[Any], pool: _SingleConnectionPool, collector: NotificationCollector
) -> None:
    profile_id = _insert_profile(writer)
    workspace_id, repository_id, _ = _seed_workspace_graph(writer, owner_profile_id=profile_id)
    collector.quiesce()

    class _Rollback(Exception):
        """Control-flow exception rolling back the explicit transaction."""

    with pytest.raises(_Rollback), writer.transaction():
        event_repositories.record_workflow_event(
            pool,
            workspace_id=workspace_id,
            event_type=WorkflowEventType.TASK_CREATED,
            actor_type=WorkflowEventActor.OWNER,
        )
        task_repositories.create_task(
            pool,
            workspace_id=workspace_id,
            repository_id=repository_id,
            github_issue_id=7701,
            github_issue_number=42,
        )
        raise _Rollback

    # A rolled-back application mutation publishes nothing (one representative
    # case; G4a's cross-connection suite owns the full commit/savepoint matrix).
    assert collector.quiesce(timeout=1.5) == []
