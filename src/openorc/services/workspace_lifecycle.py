"""Workspace lifecycle application service (issue #370).

The G1a Profile-facing Workspace discovery and creation use cases over the
existing ownership persistence (:mod:`openorc.persistence.ownership`): the
authenticated Profile lists the Workspaces it owns and creates a new
Workspace it owns. This module is deliberately narrow — creation and listing
only. The Workspace settings themselves stay in
:mod:`openorc.services.workspace_configuration` (issues #53/#240), the
ownership boundary stays in :mod:`openorc.services.workspace_authorization`
(issue #53), and destructive account/administrative lifecycle stays in its
owning services.

Canonical v1 ownership: a Workspace's owner is exactly the authenticated
Profile. The creation command carries no owner field and none is honored —
there is deliberately no path that creates a Workspace owned by anyone but
the caller. Creation is a guarded Owner mutation: the account-wide
account-deletion barrier (issue #97, :func:`require_account_operational`) is
the first lock acquisition inside one short ``composed_transaction``, so
Workspace creation fails closed while an account-deletion attempt is
unresolved — closing the orphaned-secret window the guard exists for.

The command-shape rule is service-owned (the durable ``name`` column carries
no constraint and the domain Workspace validates only the settings): a
Workspace name must be a non-blank string of bounded length, rejected before
any database work. The durable Workspace column defaults supply the
first-class settings (review-iteration limit 5, blank guidance, no default
Task budget); no creation-time audit event exists in the durable
``WorkflowEventType`` vocabulary, so none is invented here.
"""

from __future__ import annotations

from uuid import UUID

from openorc.domain.ownership import Workspace
from openorc.observability import annotate_span, application_span
from openorc.persistence.ownership import (
    create_workspace,
    list_workspaces_for_owner,
)
from openorc.persistence.pool import DatabasePool
from openorc.services.errors import InvalidCommandError
from openorc.services.profile_lifecycle_guard import require_account_operational
from openorc.services.transaction_composition import composed_transaction

__all__ = [
    "create_workspace_for_profile",
    "list_owned_workspaces",
]

# Application-service span boundaries (issues #108/#109): every public
# use-case operation of this module opens one span at the established service
# boundary. Only the safe attribute vocabulary is attachable.
_SERVICE_TRACER_SCOPE = "openorc.services.workspace_lifecycle"
_CREATE_SPAN_NAME = "workspace_lifecycle.create_workspace_for_profile"
_LIST_SPAN_NAME = "workspace_lifecycle.list_owned_workspaces"

# The Workspace ``name`` column is unconstrained durable data and the domain
# Workspace validates only the first-class settings, so the command-shape rule
# is service-owned: a non-blank, bounded Owner-authored name.
_MAX_WORKSPACE_NAME_LENGTH = 200


def _validate_workspace_name(name: str) -> None:
    """Reject an invalid Workspace name before any database work."""
    if not isinstance(name, str) or not name.strip() or len(name) > _MAX_WORKSPACE_NAME_LENGTH:
        raise InvalidCommandError(
            "Workspace name must be a non-blank string of at most "
            f"{_MAX_WORKSPACE_NAME_LENGTH} characters"
        )


def create_workspace_for_profile(pool: DatabasePool, *, profile_id: UUID, name: str) -> Workspace:
    """Create one Workspace owned by the authenticated Profile, failing closed.

    Canonical v1 ownership: the Workspace's owner is exactly the
    authenticated Profile — the caller's command carries no owner field and
    none is honored, so there is deliberately no path that creates a
    Workspace owned by anyone but the caller. The durable Workspace column
    defaults supply the first-class settings (review-iteration limit 5, blank
    guidance, no default Task budget). The account-wide Owner-mutation
    barrier (issue #97) is the first lock acquisition inside the one short
    composed transaction: creation fails closed while an account-deletion
    attempt is unresolved, closing the orphaned-secret window the guard
    exists for.
    """
    with application_span(_SERVICE_TRACER_SCOPE, _CREATE_SPAN_NAME) as span:
        annotate_span(span, operation=_CREATE_SPAN_NAME)
        # Command shape is validated before authorization, persistence
        # mutation, or any other boundary it could otherwise escape into.
        _validate_workspace_name(name)
        with composed_transaction(pool) as transaction_pool:
            # The account-wide Owner-mutation barrier first (issue #97): the
            # Profile FOR KEY SHARE read is the first lock acquisition and
            # fails closed while an account deletion attempt is unresolved.
            require_account_operational(transaction_pool, profile_id=profile_id)
            workspace = create_workspace(transaction_pool, owner_profile_id=profile_id, name=name)
        annotate_span(span, workspace_id=str(workspace.id))
        return workspace


def list_owned_workspaces(pool: DatabasePool, *, profile_id: UUID) -> list[Workspace]:
    """List the Workspaces owned by the authenticated Profile.

    The owner-scoped SQL filter IS the isolation boundary: rows are selected
    by ``owner_profile_id``, so no foreign Workspace can ever appear in the
    listing. Read-only — like the configuration reads, this composes no
    account-mutation barrier.
    """
    with application_span(_SERVICE_TRACER_SCOPE, _LIST_SPAN_NAME) as span:
        annotate_span(span, operation=_LIST_SPAN_NAME)
        return list_workspaces_for_owner(pool, owner_profile_id=profile_id)
