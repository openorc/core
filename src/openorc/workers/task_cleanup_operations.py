"""F10 terminal finalization operation composition (issues #256/#317).

The production handler for the one declared Task-scoped terminal
finalization operation kind, the F1-backed submission bridge the terminal
service seams arrange post-commit, and the production cleanup runtime
provider. The handler invokes the shared authoritative terminal finalization
service with process-local dependencies — the handler never decides Task
state, checkout identity, safety, or archival, and carries no workflow rules
of its own.

Composition rules enforced by this module:

- Finalization execution: the handler resolves the executing process's
  database pool and cleanup runtime composition and invokes the existing
  ``finalize_task_terminalization`` service with only the decoded scoped
  Workspace/Task identifiers. The service reloads the committed
  terminalization from durable state, re-derives the exact checkout and the
  original bound Producer session, rechecks every current safety fact
  before the one bounded cleanup attempt, settles the one-shot claim, and
  archives the Task atomically with its terminal audit and the ENDED
  transition of its remaining active session bindings; the invocation never
  carries or trusts paths, session identities, or outcomes. A late or
  duplicate invocation is the idempotent no-op; a concurrently unsettled
  cleanup claim defers the archival to its owning attempt.
- Submission arrangement: the bridge implements the finalization service's
  submission seam over a supplied process-local F1
  :class:`OperationSubmitter` — the serving process's bounded local
  executor in regular execution mode, the canonical queue submission in
  queue mode. Each arrangement becomes exactly one
  ``TASK_TERMINAL_FINALIZATION`` invocation carrying only the scoped
  identifiers, and the F1 submission outcome maps 1:1 onto the service's
  bounded outcome vocabulary. The bridge owns no transport mechanics of its
  own and no durable state: a known refusal or uncertain submission is
  returned as such — the arranging service owns the known-refusal
  archive-only fallback and treats uncertainty conservatively.
- This kind is not enrolled in the F5b Level 2 Task operational-failure
  triage or Level 3 gating: execution failures stay contained by the
  finalization service's own idempotent authority rechecks and F1's
  sanitized execution-error reporting. No retry, block, or disposition
  logic lives here.
"""

from __future__ import annotations

from typing import Any, cast
from uuid import UUID

from openorc.config import Settings
from openorc.services.cline_terminal_cleanup import ClineTerminalCleanupComposition
from openorc.services.task_terminal_cleanup import TerminalCheckoutCleanupRuntime
from openorc.services.task_terminal_finalization import (
    TerminalFinalizationSubmissionOutcome,
    finalize_task_terminalization,
)
from openorc.workers.invocation import (
    OperationDependencies,
    OperationHandler,
    OperationInvocation,
    OperationKind,
    OperationSubmitter,
    SubmissionOutcome,
    TerminalFinalizationPayload,
)

__all__ = [
    "F1TerminalFinalizationSubmission",
    "build_terminal_cleanup_operation_handlers",
    "default_terminal_cleanup_runtime_provider",
]


def _finalize_task_terminalization_handler(
    payload: Any, *, dependencies: OperationDependencies
) -> None:
    """Run one terminal finalization attempt (F10, issues #256/#317).

    The handler only decodes and delegates: the executing process resolves
    its database pool and cleanup runtime composition here, and the shared
    finalization service owns every durable-state recheck, the one-shot
    claim, the bounded optional cleanup, and the mandatory atomic archival
    with its session finalization. Handler failures are never swallowed into
    reported success — F1's sanitized execution-error containment owns them.
    """
    finalization_payload = cast(TerminalFinalizationPayload, payload)
    pool = dependencies.database_pool()
    finalize_task_terminalization(
        pool,
        dependencies.terminal_cleanup_runtime(),
        workspace_id=UUID(finalization_payload.workspace_id),
        task_id=UUID(finalization_payload.task_id),
    )


def build_terminal_cleanup_operation_handlers() -> dict[OperationKind, OperationHandler]:
    """The explicit F10 finalization kind-to-handler table (issues #256/#317).

    Merged into the production handler table next to the F5a Task-workflow
    and F9 Task-less GitHub families; deliberately not routed through the
    F5b-coordinated Task-workflow handler wrapper.
    """
    return {
        OperationKind.TASK_TERMINAL_FINALIZATION: _finalize_task_terminalization_handler,
    }


class F1TerminalFinalizationSubmission:
    """The F1-backed terminal finalization submission bridge (issues #256/#317).

    Implements the finalization service's submission seam over a supplied
    process-local F1 :class:`OperationSubmitter`: each post-commitment
    arrangement becomes exactly one ``TASK_TERMINAL_FINALIZATION``
    invocation carrying only the scoped Workspace/Task identifiers, and the
    F1 submission outcome maps 1:1 onto the service's bounded outcome
    vocabulary. A submission error is an unknown outcome — never a known
    non-delivery, never a replay — and acceptance is never completion. The
    arranging service owns the known-refusal archive-only fallback.
    """

    def __init__(self, submitter: OperationSubmitter) -> None:
        self._submitter = submitter

    def submit(self, *, workspace_id: UUID, task_id: UUID) -> TerminalFinalizationSubmissionOutcome:
        """Submit one terminal finalization through the supplied F1 submitter."""
        result = self._submitter.submit(
            OperationInvocation(
                kind=OperationKind.TASK_TERMINAL_FINALIZATION,
                payload=TerminalFinalizationPayload(
                    workspace_id=str(workspace_id),
                    task_id=str(task_id),
                ),
            )
        )
        if result.outcome is SubmissionOutcome.ACCEPTED_FOR_DELIVERY:
            return TerminalFinalizationSubmissionOutcome.ACCEPTED_FOR_DELIVERY
        if result.outcome is SubmissionOutcome.KNOWN_REFUSED:
            return TerminalFinalizationSubmissionOutcome.KNOWN_REFUSED
        return TerminalFinalizationSubmissionOutcome.UNCERTAIN


def default_terminal_cleanup_runtime_provider(
    settings: Settings,
) -> TerminalCheckoutCleanupRuntime:
    """The production Cline composition of the cleanup runtime seam."""
    return ClineTerminalCleanupComposition()
