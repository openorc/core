"""Integration coverage for the GitHub webhook delivery intake (issue #61).

Real-Postgres, integration-marked coverage for the durable invariants the
deterministic suite cannot execute: durable delivery-GUID uniqueness and
idempotent duplicate acknowledgement, concurrent same-GUID insert convergence
on two connections, the classification/routing CHECK consistency, the
Workspace/routing linkage consistency (composite cascade edges, fan-out,
cascade behavior on Workspace/Repository deletion), and the resolution
observations. Run explicitly by the Owner through the documented
``--testdb`` command; the suite consumes the supplied database and never
provisions one.
"""

from __future__ import annotations

import threading
import uuid
from contextlib import contextmanager
from typing import Any

import pytest
from psycopg import Connection
from psycopg.errors import CheckViolation, UniqueViolation

pytestmark = pytest.mark.integration


def _create_workspace_graph(conn: Connection) -> tuple[object, object, object]:
    profile_id = uuid.uuid4()
    conn.execute(
        "insert into auth.users (id) values (%s) on conflict do nothing",
        (profile_id,),
    )
    conn.execute("insert into openorc.profiles (id) values (%s)", (profile_id,))
    workspace_id = uuid.uuid4()
    conn.execute(
        "insert into openorc.workspaces (id, owner_profile_id, name) values (%s, %s, 'ws')",
        (workspace_id, profile_id),
    )
    project_id = uuid.uuid4()
    conn.execute(
        "insert into openorc.projects (id, workspace_id, name) values (%s, %s, 'p')",
        (project_id, workspace_id),
    )
    return workspace_id, project_id, profile_id


def _create_installation(
    conn: Connection, workspace_id: object, github_installation_id: int
) -> object:
    installation_pk = uuid.uuid4()
    conn.execute(
        "insert into openorc.github_installations "
        "(id, workspace_id, github_installation_id, github_account_id, "
        "account_login, account_type) "
        "values (%s, %s, %s, 501, 'octocat', 'Organization')",
        (installation_pk, workspace_id, github_installation_id),
    )
    return installation_pk


def _create_routed_repository(
    conn: Connection,
    workspace_id: object,
    project_id: object,
    installation_pk: object,
    github_repository_id: int,
) -> object:
    repository_id = uuid.uuid4()
    conn.execute(
        "insert into openorc.repositories "
        "(id, project_id, workspace_id, github_repository_id, owner_login, name, html_url, "
        "is_private, default_branch, github_installation_id) "
        "values (%s, %s, %s, %s, 'octocat', 'repo', 'https://github.com/octocat/repo', "
        "false, 'main', %s)",
        (repository_id, project_id, workspace_id, github_repository_id, installation_pk),
    )
    return repository_id


def _insert_delivery(conn: Connection, guid: str, **overrides: object) -> object:
    values: dict[str, object] = {
        "classification": "relevant",
        "routing_target": "issue_state",
        "routing_resolution": "resolved",
        "github_installation_id": 123,
        "github_repository_id": 456,
        "github_issue_number": 42,
        "github_pull_request_number": None,
    }
    values.update(overrides)
    row = conn.execute(
        "insert into openorc.github_webhook_deliveries "
        "(delivery_guid, event_name, action, classification, routing_target, "
        "routing_resolution, github_installation_id, github_repository_id, "
        "github_issue_number, github_pull_request_number) "
        "values (%s, 'issues', 'edited', %s, %s, %s, %s, %s, %s, %s) returning id",
        (
            guid,
            values["classification"],
            values["routing_target"],
            values["routing_resolution"],
            values["github_installation_id"],
            values["github_repository_id"],
            values["github_issue_number"],
            values["github_pull_request_number"],
        ),
    ).fetchone()
    assert row is not None
    return row[0]


def test_delivery_guid_is_durably_unique_and_duplicates_are_idempotent(conn: Connection) -> None:
    delivery_id = _insert_delivery(conn, "guid-dup")

    # A re-delivered GUID can never create a second accepted record.
    with pytest.raises(UniqueViolation), conn.transaction():
        _insert_delivery(conn, "guid-dup")

    count = conn.execute(
        "select count(*) from openorc.github_webhook_deliveries where delivery_guid = %s",
        ("guid-dup",),
    ).fetchone()
    assert count is not None and count[0] == 1
    assert delivery_id is not None


def test_concurrent_same_guid_inserts_converge_on_the_unique_rule(
    migrated_database: str,
) -> None:
    """Two independently committing connections race the same delivery GUID.

    Both contenders start behind a barrier on their own connection and run
    the PRODUCTION deduplication insert (``ON CONFLICT (delivery_guid) DO
    NOTHING RETURNING``, via the repository seam): exactly one acceptance
    and one idempotent duplicate, converging on the durable unique rule.
    The contention is real cross-connection unique-index arbitration: the
    losing insert waits on the winner's in-progress key and then observes
    the committed row. The race row is committed, so this test removes it in
    a finally block (the account-deletion suite is the commit precedent).
    """
    from psycopg import connect

    from openorc.domain.github_webhooks import (
        GitHubWebhookDeliveryClassification,
        GitHubWebhookDeliveryIntake,
        GitHubWebhookRoutingResolution,
        GitHubWebhookRoutingTarget,
    )
    from openorc.persistence.github_webhook_deliveries import record_github_webhook_delivery

    class _SingleConnectionPool:
        """Mirrors psycopg_pool connection-context commit/rollback semantics."""

        def __init__(self, connection: Connection) -> None:
            self._connection = connection

        def connection(self) -> Any:
            @contextmanager
            def managed() -> Any:
                try:
                    yield self._connection
                    self._connection.commit()
                except Exception:
                    self._connection.rollback()
                    raise

            return managed()

        def close(self) -> None:
            raise AssertionError("the race contenders own their connections")

    def _race_intake(guid: str) -> GitHubWebhookDeliveryIntake:
        return GitHubWebhookDeliveryIntake(
            delivery_guid=guid,
            event_name="issues",
            action="edited",
            classification=GitHubWebhookDeliveryClassification.RELEVANT,
            routing_target=GitHubWebhookRoutingTarget.ISSUE_STATE,
            routing_resolution=GitHubWebhookRoutingResolution.RESOLVED,
            github_installation_id=123,
            github_repository_id=456,
            github_issue_number=42,
            github_pull_request_number=None,
        )

    guid = f"guid-race-{uuid.uuid4()}"
    outcomes: list[str] = []
    barrier = threading.Barrier(2)

    def _contender() -> None:
        with connect(migrated_database) as connection:
            barrier.wait()
            outcome = record_github_webhook_delivery(
                _SingleConnectionPool(connection), _race_intake(guid)
            )
            outcomes.append("accepted" if outcome is not None else "duplicate")

    threads = [threading.Thread(target=_contender) for _ in range(2)]
    try:
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=60)
        assert not any(thread.is_alive() for thread in threads), (
            "the concurrent same-GUID race deadlocked"
        )
        assert sorted(outcomes) == ["accepted", "duplicate"]

        with connect(migrated_database) as verification:
            row = verification.execute(
                "select count(*) from openorc.github_webhook_deliveries where delivery_guid = %s",
                (guid,),
            ).fetchone()
            assert row is not None and row[0] == 1
    finally:
        with connect(migrated_database) as cleanup:
            cleanup.execute(
                "delete from openorc.github_webhook_deliveries where delivery_guid = %s",
                (guid,),
            )
            cleanup.commit()


@pytest.mark.parametrize(
    ("overrides", "expected_error"),
    [
        # A relevant delivery without a routing target/resolution violates
        # the classification consistency CHECKs.
        ({"routing_target": None}, CheckViolation),
        ({"routing_resolution": None}, CheckViolation),
        # A relevant delivery requires both stable identities.
        ({"github_installation_id": None}, CheckViolation),
        ({"github_repository_id": None}, CheckViolation),
        # Non-relevant classifications carry neither target nor resolution.
        (
            {
                "classification": "ignored",
                "routing_target": "issue_state",
                "routing_resolution": "resolved",
            },
            CheckViolation,
        ),
        # Bounded vocabularies.
        ({"classification": "mirrored"}, CheckViolation),
        ({"routing_target": "everything"}, CheckViolation),
        ({"routing_resolution": "guess"}, CheckViolation),
        # Positive-identity CHECKs.
        ({"github_installation_id": 0}, CheckViolation),
        ({"github_repository_id": -1}, CheckViolation),
        # Issue and PR numbers are mutually exclusive.
        ({"github_pull_request_number": 7}, CheckViolation),
    ],
)
def test_delivery_check_constraints(
    conn: Connection, overrides: dict[str, object], expected_error: type[Exception]
) -> None:
    with pytest.raises(expected_error), conn.transaction():
        _insert_delivery(conn, f"guid-{uuid.uuid4()}", **overrides)


def test_routing_linkage_requires_the_exact_workspace_route(conn: Connection) -> None:
    workspace_id, project_id, _ = _create_workspace_graph(conn)
    installation_pk = _create_installation(conn, workspace_id, 123)
    repository_id = _create_routed_repository(conn, workspace_id, project_id, installation_pk, 456)
    delivery_id = _insert_delivery(conn, "guid-route")

    conn.execute(
        "insert into openorc.github_webhook_delivery_routes "
        "(delivery_id, workspace_id, repository_id) values (%s, %s, %s)",
        (delivery_id, workspace_id, repository_id),
    )
    # A cross-Workspace linkage is unrepresentable (the composite route edge):
    # the exact driver error is the composite foreign-key violation.
    from psycopg.errors import ForeignKeyViolation

    other_workspace, _, _ = _create_workspace_graph(conn)
    with pytest.raises(ForeignKeyViolation), conn.transaction():
        conn.execute(
            "insert into openorc.github_webhook_delivery_routes "
            "(delivery_id, workspace_id, repository_id) values (%s, %s, %s)",
            (delivery_id, other_workspace, repository_id),
        )


def test_multi_workspace_fan_out_is_representable(conn: Connection) -> None:
    workspace_a, project_a, _ = _create_workspace_graph(conn)
    workspace_b, project_b, _ = _create_workspace_graph(conn)
    installation_a = _create_installation(conn, workspace_a, 123)
    installation_b = _create_installation(conn, workspace_b, 123)
    repository_a = _create_routed_repository(conn, workspace_a, project_a, installation_a, 456)
    repository_b = _create_routed_repository(conn, workspace_b, project_b, installation_b, 456)
    delivery_id = _insert_delivery(conn, "guid-fanout")

    conn.execute(
        "insert into openorc.github_webhook_delivery_routes "
        "(delivery_id, workspace_id, repository_id) values (%s, %s, %s), (%s, %s, %s)",
        (delivery_id, workspace_a, repository_a, delivery_id, workspace_b, repository_b),
    )
    rows = conn.execute(
        "select count(*) from openorc.github_webhook_delivery_routes where delivery_id = %s",
        (delivery_id,),
    ).fetchone()
    assert rows is not None and rows[0] == 2


def test_workspace_deletion_cascades_linkages_but_keeps_the_dedup_record(
    conn: Connection,
) -> None:
    workspace_id, project_id, profile_id = _create_workspace_graph(conn)
    installation_pk = _create_installation(conn, workspace_id, 123)
    repository_id = _create_routed_repository(conn, workspace_id, project_id, installation_pk, 456)
    delivery_id = _insert_delivery(conn, "guid-cascade")
    conn.execute(
        "insert into openorc.github_webhook_delivery_routes "
        "(delivery_id, workspace_id, repository_id) values (%s, %s, %s)",
        (delivery_id, workspace_id, repository_id),
    )

    # Deleting the account root cascades the Workspace (and its Repository),
    # removing the routing linkage while the provider-owned dedup record
    # survives.
    conn.execute("delete from auth.users where id = %s", (profile_id,))

    linkages = conn.execute(
        "select count(*) from openorc.github_webhook_delivery_routes where delivery_id = %s",
        (delivery_id,),
    ).fetchone()
    delivery = conn.execute(
        "select count(*) from openorc.github_webhook_deliveries where id = %s",
        (delivery_id,),
    ).fetchone()
    assert linkages is not None and linkages[0] == 0
    assert delivery is not None and delivery[0] == 1


def test_repository_deletion_cascades_its_linkages(conn: Connection) -> None:
    workspace_id, project_id, _ = _create_workspace_graph(conn)
    installation_pk = _create_installation(conn, workspace_id, 123)
    repository_id = _create_routed_repository(conn, workspace_id, project_id, installation_pk, 456)
    delivery_id = _insert_delivery(conn, "guid-repo-cascade")
    conn.execute(
        "insert into openorc.github_webhook_delivery_routes "
        "(delivery_id, workspace_id, repository_id) values (%s, %s, %s)",
        (delivery_id, workspace_id, repository_id),
    )

    conn.execute("delete from openorc.repositories where id = %s", (repository_id,))

    linkages = conn.execute(
        "select count(*) from openorc.github_webhook_delivery_routes where delivery_id = %s",
        (delivery_id,),
    ).fetchone()
    assert linkages is not None and linkages[0] == 0
