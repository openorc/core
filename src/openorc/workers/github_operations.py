"""F9 GitHub operation composition (issue #255).

The production handlers for the two already-declared Task-less GitHub
operation kinds, the narrow F1-backed submission bridge for webhook
dispatch delivery, and the explicit mode-neutral reconciliation-sweep
submission seam. Every handler invokes the existing shared GitHub
application services with process-local dependencies — the handler never
decides Task state, routing, or authority, and carries no workflow rules
of its own.

Composition rules enforced by this module:

- Webhook dispatch delivery: the handler resolves the executing process's
  database pool and GitHub App client and invokes the existing
  ``dispatch_github_webhook_delivery`` service with only the decoded
  delivery GUID. The service reloads durable state by that GUID,
  re-derives every persisted Workspace/repository route, and derives all
  canonical effects from fresh authoritative GitHub reads; the invocation
  and this module never carry or trust webhook bytes.
- Reconciliation sweep: the handler invokes the existing
  ``run_github_reconciliation_sweep`` service with the decoded
  scheduling tick. The tick is a scheduling position only, never
  authority: every sweep unit re-derives its subject from durable
  Postgres and re-reads fresh authoritative GitHub state, so a replayed,
  delayed, or reordered invocation is harmless.
- These Task-less kinds are not enrolled in the F5b Level 2 Task
  operational-failure triage or Level 3 gating: execution failures stay
  contained by F1's sanitized execution-error reporting, and the
  services' own durable outstanding-work semantics own recovery. No
  retry, block, or disposition logic lives here.
- Submission acceptance is never effect completion. The dispatch bridge
  maps the F1 submission outcome onto the dispatch service's bounded
  outcome vocabulary without inventing completion, and a known refusal
  or uncertain submission is never retried or replayed here: the durable
  delivery stays recoverable through the existing sweep recovery path.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any, cast

from openorc.services.github_reconciliation_recovery import run_github_reconciliation_sweep
from openorc.services.github_webhook_dispatch import (
    EnqueueOutcome,
    WebhookDispatchSubmission,
    dispatch_github_webhook_delivery,
)
from openorc.workers.invocation import (
    OperationDependencies,
    OperationHandler,
    OperationInvocation,
    OperationKind,
    OperationSubmitter,
    ReconciliationSweepPayload,
    SubmissionOutcome,
    SubmissionResult,
    WebhookDeliveryDispatchPayload,
)
from openorc.workers.task_cleanup_operations import F1TerminalFinalizationSubmission

__all__ = [
    "F1WebhookDispatchSubmission",
    "build_github_operation_handlers",
    "submit_github_reconciliation_sweep",
]


def _dispatch_github_webhook_delivery_handler(
    payload: Any, *, dependencies: OperationDependencies
) -> None:
    """Dispatch one accepted webhook delivery (Task-less GitHub kind, F9).

    The handler only decodes and delegates: the executing process resolves
    its database pool and GitHub App client here, and the shared dispatch
    service owns every durable-state reload, routing derivation, and
    authoritative GitHub consequence. The F10 post-terminal cleanup
    submission bridge (issue #256) is resolved from the same process-local
    F1 submitter and forwarded unchanged; the dispatch service arranges it
    strictly post-commit for a newly applied terminal outcome only. Handler
    failures are never swallowed into reported success — F1's sanitized
    execution-error containment owns them, and the durable delivery stays
    recoverable.
    """
    dispatch_payload = cast(WebhookDeliveryDispatchPayload, payload)
    dispatch_github_webhook_delivery(
        dependencies.database_pool(),
        dependencies.github_client(),
        delivery_guid=dispatch_payload.delivery_guid,
        terminal_finalization_submission=F1TerminalFinalizationSubmission(
            dependencies.continuation_submitter()
        ),
    )


def _run_github_reconciliation_sweep_handler(
    payload: Any, *, dependencies: OperationDependencies
) -> None:
    """Run one reconciliation/recovery sweep (Task-less GitHub kind, F9).

    The handler only decodes and delegates: the executing process resolves
    its database pool, GitHub App client, and process settings here, and
    the shared sweep service owns the bounded work discovery, the fresh
    authoritative GitHub reads, and every canonical consequence. The tick
    is a scheduling position only — never authority — and is passed
    through unchanged. The F10 post-terminal cleanup submission bridge
    (issue #256) is resolved from the same process-local F1 submitter and
    forwarded unchanged; the sweep's terminal consequences arrange it
    strictly post-commit for a newly applied terminal outcome only.
    """
    sweep_payload = cast(ReconciliationSweepPayload, payload)
    run_github_reconciliation_sweep(
        dependencies.database_pool(),
        dependencies.github_client(),
        dependencies.settings,
        sweep_tick=sweep_payload.sweep_tick,
        terminal_finalization_submission=F1TerminalFinalizationSubmission(
            dependencies.continuation_submitter()
        ),
    )


def build_github_operation_handlers() -> Mapping[OperationKind, OperationHandler]:
    """The explicit F9 GitHub kind-to-handler table.

    Exactly the two already-declared Task-less kinds; no Task-workflow
    kind is ever registered here.
    """
    return {
        OperationKind.GITHUB_WEBHOOK_DELIVERY_DISPATCH: _dispatch_github_webhook_delivery_handler,
        OperationKind.GITHUB_RECONCILIATION_SWEEP: _run_github_reconciliation_sweep_handler,
    }


class F1WebhookDispatchSubmission:
    """The F1-backed webhook dispatch submission bridge (issue #255).

    Implements the dispatch service's existing submission seam over a
    supplied process-local F1 :class:`OperationSubmitter`: each delivery
    identity becomes exactly one ``GITHUB_WEBHOOK_DELIVERY_DISPATCH``
    invocation carrying only the GUID, and the F1 submission outcome maps
    1:1 onto the service's bounded outcome vocabulary. The bridge owns no
    transport mechanics of its own, no durable state, and no retry or
    replay: a known refusal or uncertain submission is returned as such —
    the delivery stays durably recoverable and is never immediately
    re-submitted — and acceptance is never completion.
    """

    def __init__(self, submitter: OperationSubmitter) -> None:
        self._submitter = submitter

    def enqueue(self, delivery_guid: str) -> EnqueueOutcome:
        """Submit one delivery identity through the supplied F1 submitter."""
        result = self._submitter.submit(
            OperationInvocation(
                kind=OperationKind.GITHUB_WEBHOOK_DELIVERY_DISPATCH,
                payload=WebhookDeliveryDispatchPayload(delivery_guid=delivery_guid),
            )
        )
        if result.outcome is SubmissionOutcome.ACCEPTED_FOR_DELIVERY:
            return EnqueueOutcome.ENQUEUED
        if result.outcome is SubmissionOutcome.KNOWN_REFUSED:
            return EnqueueOutcome.KNOWN_FAILED
        return EnqueueOutcome.UNCERTAIN


# Type-level proof that the bridge satisfies the dispatch service's
# submission seam: the service protocol and the F1-backed implementation
# stay structurally checked without the service ever importing this module.
_dispatch_submission_bridge: type[WebhookDispatchSubmission] = F1WebhookDispatchSubmission


def submit_github_reconciliation_sweep(
    submitter: OperationSubmitter, *, sweep_tick: int | None = None
) -> SubmissionResult:
    """Submit one explicit reconciliation/recovery sweep through F1 (issue #255).

    The trusted/internal, programmatically callable submission seam, mode
    neutral by construction: the caller supplies the active process-local
    :class:`OperationSubmitter` — the serving process's bounded local
    executor in regular execution mode, the canonical queue submission in
    queue mode. ``sweep_tick=None`` requests the explicitly selected full
    sweep; a validated nonnegative tick selects the existing bounded
    partition. The tick is a scheduling position only — never authority —
    and the sweep service re-derives every subject from durable state.

    The actual F1 :class:`SubmissionResult` is returned unchanged:
    acceptance is never completion, and a known refusal or uncertain
    outcome leaves recovery to the existing sweep path instead of
    triggering a replay. A malformed tick raises the typed invocation
    error before any submission is attempted. No recurring timer,
    polling, or Owner-facing command exists here; a future Owner-facing
    caller would add its own authentication and Workspace authorization
    before submitting.
    """
    return submitter.submit(
        OperationInvocation(
            kind=OperationKind.GITHUB_RECONCILIATION_SWEEP,
            payload=ReconciliationSweepPayload(sweep_tick=sweep_tick),
        )
    )
