"""Webhook-direction GitHub routing resolution over the B1 routing tables (issue #61).

The webhook-direction complement of the #57 configuration-direction routing:
an inbound delivery knows only provider-stable identifiers (the external
installation ID and the stable repository ID), and this module resolves them
against the explicit Workspace GitHubInstallation/Repository routing — never
from owner/login names or other mutable presentation data.

Resolution is exact-match and fails closed:

- The delivery's external installation ID must name an existing Workspace
  installation record; an unknown installation resolves nothing (the
  ``unmapped_installation`` observation).
- A routing linkage binds only when the Workspace's Repository record for the
  affected stable repository identity is explicitly routed to exactly this
  delivery's installation (the composite route foreign key keeps both sides
  in the same Workspace). An existing Repository record routed elsewhere —
  or unrouted — is the ``route_mismatch`` observation, never authority.
- A known installation whose Workspaces hold no Repository record for the
  affected stable repository identity is the ``unconfigured_repository``
  observation.
- Installation-scoped deliveries (issue #120: the ``installation`` and
  ``installation_repositories`` families carry no singular repository
  identity) resolve through ``resolve_github_webhook_installation_routes``:
  routes are every Workspace repository explicitly routed to the delivery's
  installation; a known installation with no routed repositories is the
  ``unconfigured_repository`` observation.

This is routing resolution only, never authorization: observed installation
state (``suspended_at``) is deliberately not consulted, consistent with the
#57 resolution boundary; dispatch (#120) re-validates current access before
any reconciliation effect.

Violated database invariants surface as driver exceptions; translating them
into typed application errors is a service-layer concern, not a persistence
one.
"""

from __future__ import annotations

from openorc.domain.github_webhooks import (
    GitHubWebhookResolvedRoute,
    GitHubWebhookRouteResolution,
    GitHubWebhookRoutingResolution,
)
from openorc.persistence.pool import DatabasePool
from openorc.persistence.transactions import transaction

__all__ = [
    "resolve_github_webhook_installation_routes",
    "resolve_github_webhook_routes",
]


def resolve_github_webhook_routes(
    pool: DatabasePool, *, github_installation_id: int, github_repository_id: int
) -> GitHubWebhookRouteResolution:
    """Resolve one delivery's stable installation/repository identity.

    Deterministic and exact-match: candidates are the Workspace installation
    records keyed by the stable external installation ID; a route binds only
    when the Workspace Repository record for the affected stable repository
    identity is explicitly routed to exactly this installation. Results are
    ordered by Workspace, so repeated resolutions converge on the same facts.
    """
    with transaction(pool) as conn:
        candidates = conn.execute(
            "select workspace_id from openorc.github_installations "
            "where github_installation_id = %s order by workspace_id",
            (github_installation_id,),
        ).fetchall()
        if not candidates:
            return GitHubWebhookRouteResolution(
                resolution=GitHubWebhookRoutingResolution.UNMAPPED_INSTALLATION, routes=()
            )
        routes = conn.execute(
            "select r.id, r.workspace_id "
            "from openorc.repositories r "
            "join openorc.github_installations gi "
            "on gi.id = r.github_installation_id and gi.workspace_id = r.workspace_id "
            "where r.github_repository_id = %s and gi.github_installation_id = %s "
            "order by r.workspace_id, r.id",
            (github_repository_id, github_installation_id),
        ).fetchall()
        if routes:
            return GitHubWebhookRouteResolution(
                resolution=GitHubWebhookRoutingResolution.RESOLVED,
                routes=tuple(
                    GitHubWebhookResolvedRoute(workspace_id=row[1], repository_id=row[0])
                    for row in routes
                ),
            )
        # No exact match: distinguish the fail-closed observations — the
        # affected stable repository identity is configured somewhere (the
        # route-mismatch case, failing closed) or nowhere (unconfigured).
        configured = conn.execute(
            "select 1 from openorc.repositories where github_repository_id = %s limit 1",
            (github_repository_id,),
        ).fetchone()
        resolution = (
            GitHubWebhookRoutingResolution.ROUTE_MISMATCH
            if configured is not None
            else GitHubWebhookRoutingResolution.UNCONFIGURED_REPOSITORY
        )
        return GitHubWebhookRouteResolution(resolution=resolution, routes=())


def resolve_github_webhook_installation_routes(
    pool: DatabasePool, *, github_installation_id: int
) -> GitHubWebhookRouteResolution:
    """Resolve one installation-scoped delivery's Workspace routing.

    The installation-scoped complement for repository-metadata deliveries
    that carry no singular repository identity (issue #120 — the
    ``installation``/``installation_repositories`` families): routes are
    every Workspace repository explicitly routed to this delivery's
    installation. Same fail-closed discipline: an unknown installation
    resolves nothing (the ``unmapped_installation`` observation); a known
    installation with no routed repositories is the
    ``unconfigured_repository`` observation. Exact-match only, never
    authorization, never owner/login heuristics.
    """
    with transaction(pool) as conn:
        candidates = conn.execute(
            "select workspace_id from openorc.github_installations "
            "where github_installation_id = %s order by workspace_id",
            (github_installation_id,),
        ).fetchall()
        if not candidates:
            return GitHubWebhookRouteResolution(
                resolution=GitHubWebhookRoutingResolution.UNMAPPED_INSTALLATION, routes=()
            )
        routes = conn.execute(
            "select r.id, r.workspace_id "
            "from openorc.repositories r "
            "join openorc.github_installations gi "
            "on gi.id = r.github_installation_id and gi.workspace_id = r.workspace_id "
            "where gi.github_installation_id = %s "
            "order by r.workspace_id, r.id",
            (github_installation_id,),
        ).fetchall()
        if routes:
            return GitHubWebhookRouteResolution(
                resolution=GitHubWebhookRoutingResolution.RESOLVED,
                routes=tuple(
                    GitHubWebhookResolvedRoute(workspace_id=row[1], repository_id=row[0])
                    for row in routes
                ),
            )
        return GitHubWebhookRouteResolution(
            resolution=GitHubWebhookRoutingResolution.UNCONFIGURED_REPOSITORY, routes=()
        )
