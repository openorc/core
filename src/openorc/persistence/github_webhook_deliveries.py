"""Repositories for durable GitHub webhook delivery intake (issue #61).

Explicit SQL repositories over the ``openorc`` schema for the GUID-keyed
delivery records and their resolved Workspace routing linkages. Rows map to
transport-independent domain objects from
:mod:`openorc.domain.github_webhooks`; instants returned from Postgres are
normalized to timezone-aware UTC at this boundary.

Deduplication semantics:

- ``record_github_webhook_delivery`` inserts one verified delivery record in
  a single statement (``INSERT ... ON CONFLICT (delivery_guid) DO NOTHING
  RETURNING``): the returned row is the accepted record; a missing row means
  the GUID was already durably accepted (a duplicate re-delivery observed,
  idempotently acknowledged, no second record created). Concurrent duplicate
  inserts converge deterministically on the durable unique constraint.
- ``record_webhook_delivery_routes`` persists the resolved Workspace routing
  facts of the accepted delivery in one set-based statement; the routes are
  the exact B1 route matches resolved within the same transaction.

Processing/recovery state (issue #120): the delivery record also carries the
smallest bounded processing/recovery metadata — a nullable ``processed_at``
instant written only when every routed reconciliation completed, so recovery
(#62) can identify accepted deliveries that still require processing. No
payload, error body, or queue state is ever persisted here.

Raw webhook request bodies, signature material, and customer content are
never written or read here. Violated database invariants surface as driver
exceptions (for example ``psycopg.errors.CheckViolation``); translating them
into typed application errors is a service-layer concern, not a persistence
one.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any
from uuid import UUID

from openorc.domain.github_webhooks import (
    GitHubWebhookDelivery,
    GitHubWebhookDeliveryClassification,
    GitHubWebhookDeliveryIntake,
    GitHubWebhookDeliveryRoute,
    GitHubWebhookResolvedRoute,
    GitHubWebhookRoutingResolution,
    GitHubWebhookRoutingTarget,
)
from openorc.persistence.pool import DatabasePool
from openorc.persistence.time import normalize_utc
from openorc.persistence.transactions import transaction

__all__ = [
    "get_github_webhook_delivery",
    "list_github_webhook_delivery_routes",
    "mark_github_webhook_delivery_processed",
    "record_github_webhook_delivery",
    "record_webhook_delivery_routes",
]

_DELIVERY_COLUMNS = (
    "id, delivery_guid, event_name, action, classification, routing_target, "
    "routing_resolution, github_installation_id, github_repository_id, "
    "github_issue_number, github_pull_request_number, received_at, processed_at"
)


def _delivery_from_row(row: Sequence[Any]) -> GitHubWebhookDelivery:
    return GitHubWebhookDelivery(
        id=row[0],
        delivery_guid=row[1],
        event_name=row[2],
        action=row[3],
        classification=GitHubWebhookDeliveryClassification(row[4]),
        routing_target=None if row[5] is None else GitHubWebhookRoutingTarget(row[5]),
        routing_resolution=(None if row[6] is None else GitHubWebhookRoutingResolution(row[6])),
        github_installation_id=row[7],
        github_repository_id=row[8],
        github_issue_number=row[9],
        github_pull_request_number=row[10],
        received_at=normalize_utc(row[11]),
        processed_at=None if row[12] is None else normalize_utc(row[12]),
    )


def record_github_webhook_delivery(
    pool: DatabasePool, intake: GitHubWebhookDeliveryIntake
) -> GitHubWebhookDelivery | None:
    """Insert one verified delivery record; ``None`` means the GUID was already accepted.

    A single statement performs both the insert and the deduplication
    arbitration: ``ON CONFLICT (delivery_guid) DO NOTHING`` converges
    concurrent duplicate inserts on the durable unique constraint, and the
    caller translates the missing row into the typed idempotent outcome.
    """
    with transaction(pool) as conn:
        row = conn.execute(
            "insert into openorc.github_webhook_deliveries "
            "(delivery_guid, event_name, action, classification, routing_target, "
            "routing_resolution, github_installation_id, github_repository_id, "
            "github_issue_number, github_pull_request_number) "
            "values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s) "
            "on conflict (delivery_guid) do nothing "
            f"returning {_DELIVERY_COLUMNS}",
            (
                intake.delivery_guid,
                intake.event_name,
                intake.action,
                intake.classification.value,
                None if intake.routing_target is None else intake.routing_target.value,
                (None if intake.routing_resolution is None else intake.routing_resolution.value),
                intake.github_installation_id,
                intake.github_repository_id,
                intake.github_issue_number,
                intake.github_pull_request_number,
            ),
        ).fetchone()
    return None if row is None else _delivery_from_row(row)


def record_webhook_delivery_routes(
    pool: DatabasePool,
    *,
    delivery_id: UUID,
    routes: Sequence[GitHubWebhookResolvedRoute],
) -> None:
    """Persist the resolved Workspace routing facts of one accepted delivery.

    One set-based statement; ``routes`` holds the exact B1 route matches
    resolved within the same transaction. An empty sequence persists nothing.
    """
    if not routes:
        return
    with transaction(pool) as conn:
        conn.execute(
            "insert into openorc.github_webhook_delivery_routes "
            "(delivery_id, workspace_id, repository_id) "
            "select %s, workspace_id, repository_id "
            "from unnest(%s::uuid[], %s::uuid[]) as resolved(workspace_id, repository_id)",
            (
                delivery_id,
                [route.workspace_id for route in routes],
                [route.repository_id for route in routes],
            ),
        )


def list_github_webhook_delivery_routes(
    pool: DatabasePool, *, delivery_id: UUID
) -> list[GitHubWebhookDeliveryRoute]:
    """Return the persisted routing linkages of one delivery, deterministically ordered."""
    with transaction(pool) as conn:
        rows = conn.execute(
            "select id, delivery_id, workspace_id, repository_id, created_at "
            "from openorc.github_webhook_delivery_routes "
            "where delivery_id = %s order by workspace_id, repository_id",
            (delivery_id,),
        ).fetchall()
    return [
        GitHubWebhookDeliveryRoute(
            id=row[0],
            delivery_id=row[1],
            workspace_id=row[2],
            repository_id=row[3],
            created_at=normalize_utc(row[4]),
        )
        for row in rows
    ]


def get_github_webhook_delivery(
    pool: DatabasePool, *, delivery_guid: str
) -> GitHubWebhookDelivery | None:
    """Return the durable delivery record for one provider delivery GUID, or ``None``.

    The GUID is the provider-owned deduplication identity; dispatch (#120)
    reloads durable state through this read — never queue payload content,
    never webhook payload content.
    """
    with transaction(pool) as conn:
        row = conn.execute(
            f"select {_DELIVERY_COLUMNS} from openorc.github_webhook_deliveries "
            "where delivery_guid = %s",
            (delivery_guid,),
        ).fetchone()
    return None if row is None else _delivery_from_row(row)


def mark_github_webhook_delivery_processed(pool: DatabasePool, *, delivery_id: UUID) -> bool:
    """Mark one delivery fully processed; ``False`` when already marked or absent.

    The idempotent conditional update (the ``processed_at is null`` guard)
    makes repeated dispatch/recovery convergences true no-ops: the delivery
    record gains only this bounded recovery instant, and nothing else is
    written.
    """
    with transaction(pool) as conn:
        row = conn.execute(
            "update openorc.github_webhook_deliveries "
            "set processed_at = now() "
            "where id = %s and processed_at is null "
            "returning id",
            (delivery_id,),
        ).fetchone()
    return row is not None
