"""F10 terminal checkout cleanup operation composition (issue #256).

The production handler for the one declared Task-scoped post-terminal
cleanup operation kind, the F1-backed submission bridge the terminal
service seams arrange post-commit, and the production cleanup runtime
provider. The handler invokes the existing shared cleanup application
service with process-local dependencies — the handler never decides Task
state, checkout identity, or safety, and carries no workflow rules of its
own.

Composition rules enforced by this module:

- Cleanup execution: the handler resolves the executing process's database
  pool and cleanup runtime composition and invokes the existing
  ``execute_terminal_checkout_cleanup`` service with only the decoded
  scoped Workspace/Task identifiers. The service reloads the archived
  terminal Task, re-derives the exact checkout and the original bound
  Producer session from durable state, and rechecks every current safety
  fact before any runtime effect; the invocation never carries or trusts
  paths, session identities, or outcomes.
- Submission arrangement: the bridge implements the cleanup service's
  submission seam over a supplied process-local F1
  :class:`OperationSubmitter` — the serving process's bounded local
  executor in regular execution mode, the canonical queue submission in
  queue mode. Each arrangement becomes exactly one
  ``TASK_TERMINAL_CHECKOUT_CLEANUP`` invocation carrying only the scoped
  identifiers, and the F1 submission outcome maps 1:1 onto the service's
  bounded outcome vocabulary. The bridge owns no transport mechanics of
  its own, no durable state, and no retry or replay: a known refusal or
  uncertain submission is returned as such — the stale checkout is
  accepted and never immediately re-submitted — and acceptance is never
  completion.
- This kind is not enrolled in the F5b Level 2 Task operational-failure
  triage or Level 3 gating: execution failures stay contained by the
  cleanup service's own best-effort containment and F1's sanitized
  execution-error reporting. No retry, block, or disposition logic lives
  here, and no cleanup state is ever recorded.
"""

from __future__ import annotations

from typing import Any, cast
from uuid import UUID

from openorc.config import Settings
from openorc.services.cline_terminal_cleanup import ClineTerminalCleanupComposition
from openorc.services.task_terminal_cleanup import (
    TerminalCheckoutCleanupRuntime,
    TerminalCheckoutCleanupSubmissionOutcome,
    execute_terminal_checkout_cleanup,
)
from openorc.workers.invocation import (
    OperationDependencies,
    OperationHandler,
    OperationInvocation,
    OperationKind,
    OperationSubmitter,
    SubmissionOutcome,
    TerminalCheckoutCleanupPayload,
)

__all__ = [
    "F1TerminalCheckoutCleanupSubmission",
    "build_terminal_cleanup_operation_handlers",
    "default_terminal_cleanup_runtime_provider",
]


def _execute_terminal_checkout_cleanup_handler(
    payload: Any, *, dependencies: OperationDependencies
) -> None:
    """Run one post-terminal cleanup attempt (F10, issue #256).

    The handler only decodes and delegates: the executing process resolves
    its database pool and cleanup runtime composition here, and the shared
    cleanup service owns every durable-state recheck, eligibility decision,
    safety guard, and contained realization. Handler failures are never
    swallowed into reported success — F1's sanitized execution-error
    containment owns them — and a contained cleanup failure accepts the
    stale checkout without any retry or disposition.
    """
    cleanup_payload = cast(TerminalCheckoutCleanupPayload, payload)
    pool = dependencies.database_pool()
    execute_terminal_checkout_cleanup(
        pool,
        dependencies.terminal_cleanup_runtime(),
        workspace_id=UUID(cleanup_payload.workspace_id),
        task_id=UUID(cleanup_payload.task_id),
    )


def build_terminal_cleanup_operation_handlers() -> dict[OperationKind, OperationHandler]:
    """The explicit F10 cleanup kind-to-handler table (issue #256).

    Merged into the production handler table next to the F5a Task-workflow
    and F9 Task-less GitHub families; deliberately not routed through the
    F5b-coordinated Task-workflow handler wrapper.
    """
    return {
        OperationKind.TASK_TERMINAL_CHECKOUT_CLEANUP: _execute_terminal_checkout_cleanup_handler,
    }


class F1TerminalCheckoutCleanupSubmission:
    """The F1-backed cleanup submission bridge (issue #256).

    Implements the cleanup service's submission seam over a supplied
    process-local F1 :class:`OperationSubmitter`: each post-archival
    arrangement becomes exactly one ``TASK_TERMINAL_CHECKOUT_CLEANUP``
    invocation carrying only the scoped Workspace/Task identifiers, and the
    F1 submission outcome maps 1:1 onto the service's bounded outcome
    vocabulary. A submission error is an unknown outcome — never a known
    non-delivery, never a replay — and acceptance is never completion.
    """

    def __init__(self, submitter: OperationSubmitter) -> None:
        self._submitter = submitter

    def submit(
        self, *, workspace_id: UUID, task_id: UUID
    ) -> TerminalCheckoutCleanupSubmissionOutcome:
        """Submit one cleanup attempt through the supplied F1 submitter."""
        result = self._submitter.submit(
            OperationInvocation(
                kind=OperationKind.TASK_TERMINAL_CHECKOUT_CLEANUP,
                payload=TerminalCheckoutCleanupPayload(
                    workspace_id=str(workspace_id),
                    task_id=str(task_id),
                ),
            )
        )
        if result.outcome is SubmissionOutcome.ACCEPTED_FOR_DELIVERY:
            return TerminalCheckoutCleanupSubmissionOutcome.ACCEPTED_FOR_DELIVERY
        if result.outcome is SubmissionOutcome.KNOWN_REFUSED:
            return TerminalCheckoutCleanupSubmissionOutcome.KNOWN_REFUSED
        return TerminalCheckoutCleanupSubmissionOutcome.UNCERTAIN


def default_terminal_cleanup_runtime_provider(
    settings: Settings,
) -> TerminalCheckoutCleanupRuntime:
    """The production Cline composition of the cleanup runtime seam."""
    return ClineTerminalCleanupComposition()
