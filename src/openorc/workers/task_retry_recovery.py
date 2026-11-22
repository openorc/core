"""F12 bounded startup retry-recovery composition (issue #316).

The bounded post-restart pass that resubmits still-outstanding durably
authorized Level 2 retries through the executing process's F1 submission
seam. The candidates come from Postgres alone — the latest authorized
attempt of each non-escalated episode in a supported retry family whose
one-shot evidence still proves the authorization unexecuted — and every
candidate is re-proven from committed state at arrangement time: a
candidate whose Task/Workspace facts moved on submits nothing. Each
arrangement is exactly one typed retry invocation of the family's existing
registered operation kind carrying the original attempt identity; the
executing F1 worker/job performs the bounded ``not_before`` wait and the
existing F5c/F5d/F5f services remain authoritative for due time,
currentness, effect safety, and one-shot consumption.

Composition rules enforced by this module:

- This is scheduling only. It never clears, resets, or rewrites the retry
  ledger, never abandons or auto-blocks an outstanding authorization, never
  manufactures a new attempt, and never replays a consumed, escalated, or
  uncertain effect. Simultaneous original and recovered deliveries are
  expected and harmless: one-shot consumption admits the exact attempt
  once.
- The pass never routes retries into F11's ``QUEUED`` Task FIFO or its
  capacity-admission processor: these Tasks already own their reserved
  sessions, and the typed retry invocations go directly through the
  executing process's submitter.
- A crashed worker without a backend restart may leave an authorized retry
  pending until the next supported startup; that is the accepted bounded v1
  liveness limitation. This module is the supported trigger — the serving
  process's startup in both execution modes — and introduces no polling
  loop, timer, or scheduler of its own.
- Bounded forward progress: the enumeration itself scopes to the coarse
  family→status Task eligibility, and the pass pages past candidates it
  re-proves as moved-on (a full stale page advances the episode-id cursor
  within the bounded page budget) — obsolete candidates can never
  permanently starve a later eligible attempt across restarts. No
  scheduler, durable queue, or polling is introduced.
- A refused, uncertain, or failed arrangement never fails the pass: the
  committed authorization row owns recovery, and the next supported trigger
  revisits the rest.
"""

from __future__ import annotations

import logging
from uuid import UUID

from openorc.domain.operational_failures import OperationalFailureFamily
from openorc.persistence.operational_failures import (
    list_outstanding_authorized_retry_attempts,
)
from openorc.persistence.ownership import get_workspace
from openorc.persistence.pool import DatabasePool
from openorc.persistence.tasks import get_task
from openorc.services.task_authorized_retry_submission import (
    AUTHORIZED_RETRY_FAMILY_STATUSES,
    arrange_authorized_retry,
)
from openorc.workers.invocation import OperationSubmitter
from openorc.workers.task_workflow import F1AuthorizedRetrySubmission

__all__ = ["arrange_task_retry_recovery"]

logger = logging.getLogger(__name__)

# The bounded recovery pass arranges at most this many candidate handoffs per
# trigger, and examines at most this many full keyset pages: a page full of
# candidates the arrangement re-proved as moved-on advances the episode-id
# cursor to the next page instead of consuming the budget, so obsolete
# candidates can never starve a later eligible attempt. Both are paging
# bounds — never scheduling authority; the next supported trigger revisits
# the rest.
_RECOVERY_SUBJECT_BUDGET = 50
_RECOVERY_PAGE_BUDGET = 4


def arrange_task_retry_recovery(
    pool: DatabasePool,
    submitter: OperationSubmitter,
    *,
    subject_budget: int = _RECOVERY_SUBJECT_BUDGET,
    page_budget: int = _RECOVERY_PAGE_BUDGET,
) -> None:
    """One bounded post-restart authorized-retry recovery pass (F12, #316).

    The supported recovery trigger wired into the serving process's startup
    in both execution modes — beside the F11 Task-queue recovery — derive
    the candidates from the outstanding authorized-retry facts alone and
    submit the same typed retry invocation for each still-pending candidate
    through the executing process's submitter. Every candidate is re-proven
    from committed state at arrangement time; a candidate whose durable
    facts moved on submits nothing, and the pass advances its keyset cursor
    past it — a full page of moved-on candidates pages forward within the
    bounded page budget instead of consuming the arranged-handoff budget, so
    historical candidates can never permanently starve a later eligible
    attempt. Submissions are contained — a refused, uncertain, or lost
    arrangement never fails the recovery pass, never replays a consumed or
    uncertain effect, and the next supported trigger revisits the rest. The
    worker performs any remaining ``not_before`` wait.
    """
    bridge = F1AuthorizedRetrySubmission(lambda: submitter)
    arranged = 0
    after_episode_id: UUID | None = None
    for _page in range(max(page_budget, 0)):
        if arranged >= subject_budget:
            return
        candidates = list_outstanding_authorized_retry_attempts(
            pool, limit=subject_budget, after_episode_id=after_episode_id
        )
        if not candidates:
            return
        for candidate in candidates:
            if arranged >= subject_budget:
                return
            if _arrange_recovered_retry(
                bridge,
                pool,
                workspace_id=candidate.workspace_id,
                task_id=candidate.task_id,
                retry_attempt_id=candidate.attempt_id,
                expected_family=candidate.operation_family,
                origin="task_retry_recovery",
            ):
                arranged += 1
        if len(candidates) < subject_budget:
            return
        after_episode_id = candidates[-1].episode_id


def _arrange_recovered_retry(
    bridge: F1AuthorizedRetrySubmission,
    pool: DatabasePool,
    *,
    workspace_id: UUID,
    task_id: UUID,
    retry_attempt_id: UUID,
    expected_family: OperationalFailureFamily,
    origin: str,
) -> bool:
    """Arrange one recovered authorized-retry handoff, re-proven from state.

    The candidate's exact Task is re-read from committed state: the current
    coarse status must still match the family's executing-service
    expectation and the fresh token is the arrangement's currentness seed.
    The Workspace read resolves the accountable Owner identity; a missing
    Workspace or a moved-on Task submits nothing and reports ``False`` so
    the pass pages past the candidate. The submission is contained by the
    shared arranging seam; a re-proven candidate whose handoff reached the
    transport (any delivery outcome) reports ``True``.
    """
    task = get_task(pool, task_id=task_id)
    if task is None or task.workspace_id != workspace_id:
        return False
    expected_status = AUTHORIZED_RETRY_FAMILY_STATUSES.get(expected_family)
    if expected_status is None or task.status is not expected_status:
        return False
    workspace = get_workspace(pool, workspace_id=workspace_id)
    if workspace is None:  # pragma: no cover - the Task's scope proved it
        return False
    outcome = arrange_authorized_retry(
        bridge,
        workspace_id=workspace_id,
        task_id=task_id,
        profile_id=workspace.owner_profile_id,
        retry_attempt_id=retry_attempt_id,
        expected_state_token=task.state_token,
        family=expected_family,
        origin=origin,
    )
    return outcome is not None
