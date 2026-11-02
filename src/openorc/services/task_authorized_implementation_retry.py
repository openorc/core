"""F5c: the one durably authorized implementation-retry execution (issue #263).

When F4 (#251) has durably authorized one bounded additional implementation
attempt for an implementation-execution failure episode, this seam executes
that single authorization exactly once. An authorized retry is **not** a
replay of the E6 one-shot operation: it is a distinct Execution, bound once
to its exact F4 ``OperationalFailureAttempt``, with every Task, Owner
approval, accepted Reviewer result, and original Producer-session subject
revalidated from committed state before any external effect.

Composition (authorization precedes every adapter acquisition; no database
transaction spans an attachment or any runtime call):

1. **Atomic admission and one-shot consumption** (one short Task-row-first
   composed transaction): the account-operational and Workspace-Owner scope,
   the exact current Task under the caller's ``expected_state_token``, a
   non-archived ``implementing`` Task with no current gate, and the unchanged
   exact approved implementation subjects (the E5-approved historical
   IMPLEMENTATION_AUTHORIZATION gate bound to the current plan revision, the
   CLOSED planning ReviewLoop whose latest finalized iteration for that
   revision is a Reviewer ACCEPTED, and the READY Producer binding) are all
   re-derived from durable state under the Task row lock. The F4 attempt and
   its episode are then proven: the episode is a non-terminally-disposed
   IMPLEMENTATION_EXECUTION episode rooted at an EXECUTION identity, the
   authorization is still unbound, is the episode's latest ordinal, has no
   unconsumed earlier sibling, and its predecessor effect is **positively
   known pre-effect failed** — the predecessor Execution row is
   ``failed_transient``/``failed_final`` on the same Task/Producer session
   and its ``execution_failed`` audit carries the safe machine pre-effect
   failure kind (F4's own corroboration primitive). No earlier unresolved or
   succeeded Execution may exist, and the persisted due instant must have
   arrived at actual admission. Only then does the transaction reserve
   exactly one new Execution with the next per-Task number, claim it
   ``running``, record the Producer-attributed ``execution_started`` audit,
   and bind the F4 attempt to **that** Execution set-once through F4's
   repository binding inside this same composition — never in a later
   independently committed transaction. A competing worker, duplicate
   delivery, premature wakeup, or stale Task cannot create a second
   Execution or reach the adapter: the Task lock serializes consumers, the
   conditional set-once binding and the per-Task execution-number uniqueness
   are the durable backstops, and any refusal rolls the whole composition
   back.
2. **The invocation's own attempt identity** (after the admission commit,
   before any external effect): the new retry Execution UUID is recorded
   into the caller's ``InvocationAttemptEvidence`` with
   ``family=IMPLEMENTATION_EXECUTION`` and ``original_attempt_kind=EXECUTION``
   — never the original episode Execution. F5b's existing durable binding
   lookup then resolves the episode's original identity and automatically
   constructs F4's ``PRIOR_AUTHORIZED`` report with the exact bound effect
   row on failure; this service duplicates none of that reporting logic.
3. **The shared E6 external mechanics** (no transaction open): process-local
   attachment to the same admitted Producer session, R1 revalidation, the
   native PLAN→ACT control on the same external session ID, the optional
   qualifying-activity subscription, R2 revalidation immediately before the
   one send carrying the canonical accepted Reviewer envelope, and the
   guarded one-shot finalize reusing E6's exact failure discipline. An
   uncertain ACT/send/finalize leaves the new Execution conservatively
   ``running``; only a positively proven pre-effect failure finalizes it
   truthfully. No automatic retries live in this service, and no F11
   wakeup, scheduler, polling loop, or additional recovery policy is built
   here: this seam is explicitly invocable for an already-due authorization
   in both execution modes, and the persisted due instant is re-proven at
   actual admission.

The E6 initial-implementation reservation stays strictly initial-only: this
module never calls it, and the two seams share one execution workflow rather
than a divergent copy.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from datetime import UTC, datetime
from typing import Final
from uuid import UUID

from openorc.domain.executions import ExecutionStatus
from openorc.domain.operational_failures import (
    OperationalFailureSourceKind,
    OriginalAttemptKind,
)
from openorc.domain.pending_interactions import PendingTaskRuntimeInteractionOperation
from openorc.observability import annotate_span, application_span
from openorc.persistence import executions as execution_records
from openorc.persistence import operational_failures as failure_records
from openorc.persistence import tasks as task_records
from openorc.persistence.pool import DatabasePool
from openorc.services import event_coordination
from openorc.services.errors import (
    ConflictError,
    NotFoundError,
    StaleOperationError,
)
from openorc.services.operation_attempt_evidence import InvocationAttemptEvidence
from openorc.services.profile_lifecycle_guard import require_account_operational
from openorc.services.review_result_envelope import (
    accepted_review_result_from_iteration,
    serialize_review_result,
)

# The narrowly scoped E6 shared mechanics (issue #81): the authorized
# subjects loader, the implementing-Task requirement, the reserved-attempt
# shape, and the one shared external execution workflow. Reusing them keeps
# the retry on exactly the established authority-revalidation, dispatch, and
# finalization semantics instead of a divergent copy.
from openorc.services.task_authorized_implementation_execution import (
    ActivitySubscriptionFactory,
    AuthorizedImplementationExecution,
    ProducerRuntimeAttachmentFactory,
    _execute_external_phase,
    _load_authorized_subjects,
    _require_activity_factory,
    _require_implementing_task,
    _require_producer_factory,
    _require_uuid_command,
    _ReservedAttempt,
)

# F4's own durable corroboration primitive (#251): the one definition of a
# positively proven pre-effect execution failure this seam must satisfy.
from openorc.services.task_operational_failure_triage import (
    _corroborate_pre_effect_execution_failure,
)
from openorc.services.task_subject_guards import require_current_task
from openorc.services.transaction_composition import composed_transaction
from openorc.services.workspace_authorization import require_workspace_task

__all__ = [
    "execute_authorized_implementation_retry",
]

logger = logging.getLogger(__name__)

_SERVICE_TRACER_SCOPE: Final[str] = "openorc.services.task_authorized_implementation_retry"
_EXECUTE_SPAN_NAME: Final[str] = "task_authorized_implementation_retry.execute"


def _consume_authorized_retry(
    transaction_pool: DatabasePool,
    *,
    profile_id: UUID,
    workspace_id: UUID,
    task_id: UUID,
    expected_state_token: UUID,
    retry_attempt_id: UUID,
    now: Callable[[], datetime],
) -> _ReservedAttempt:
    """Admission, one-shot consumption, and the durable reservation under the Task lock."""
    require_account_operational(transaction_pool, profile_id=profile_id)
    require_workspace_task(
        transaction_pool, profile_id=profile_id, workspace_id=workspace_id, task_id=task_id
    )
    locked = task_records.get_task_for_update(transaction_pool, task_id)
    if locked is None or locked.workspace_id != workspace_id:
        raise NotFoundError("the requested task is not available in this workspace")
    task = require_current_task(
        transaction_pool,
        workspace_id=workspace_id,
        task_id=task_id,
        expected_state_token=expected_state_token,
    )
    _require_implementing_task(task)

    # --- the F4-issued authorization and its episode, under the Task lock. ---
    attempt = failure_records.get_operational_failure_attempt(
        transaction_pool, attempt_id=retry_attempt_id
    )
    if attempt is None or attempt.workspace_id != workspace_id or attempt.task_id != task_id:
        raise NotFoundError("the requested retry attempt is not available for this task")
    episode = failure_records.get_operational_failure_episode(
        transaction_pool, episode_id=attempt.episode_id
    )
    if (
        episode is None
        or episode.workspace_id != workspace_id
        or episode.task_id != task_id
        or episode.operation_family
        is not PendingTaskRuntimeInteractionOperation.IMPLEMENTATION_EXECUTION
        or episode.original_attempt_kind is not OriginalAttemptKind.EXECUTION
    ):
        raise ConflictError(
            "the retry attempt's failure episode does not address this task's "
            "implementation execution"
        )
    if episode.escalated_task_block_id is not None:
        raise ConflictError(
            "the retry attempt's failure episode is terminally disposed; "
            "the authorization can no longer be executed"
        )
    if attempt.resulting_execution_id is not None:
        # F4's bound resulting_execution_id is the consumption identity: a
        # reinvocation after binding is a no-effect duplicate, whether the
        # bound Execution is failed, succeeded, or still running.
        raise StaleOperationError(
            "the retry authorization has already been consumed; the invocation is a duplicate"
        )
    attempts = failure_records.list_operational_failure_attempts(
        transaction_pool, episode_id=episode.id
    )
    if not attempts or attempts[-1].id != attempt.id:
        raise ConflictError(
            "the retry authorization is not the failure episode's latest authorized attempt"
        )
    if any(other.resulting_execution_id is None for other in attempts[:-1]):
        raise ConflictError(
            "the failure episode carries an unconsumed earlier authorization; "
            "the retry chain is inconsistent"
        )

    # --- the predecessor effect is positively known pre-effect failed. ---
    if attempt.source_kind is OperationalFailureSourceKind.ORIGINAL:
        if attempt.source_attempt_id != episode.original_attempt_id:
            raise ConflictError(
                "the retry authorization's source does not address the episode's original attempt"
            )
        predecessor_id = episode.original_attempt_id
    else:
        source_attempt = next(
            (other for other in attempts if other.id == attempt.source_attempt_id), None
        )
        if source_attempt is None or source_attempt.resulting_execution_id is None:
            raise ConflictError("the retry authorization's source authorization was never executed")
        predecessor_id = source_attempt.resulting_execution_id
    predecessor = execution_records.get_execution_for_update(
        transaction_pool, execution_id=predecessor_id
    )
    if (
        predecessor is None
        or predecessor.workspace_id != workspace_id
        or predecessor.task_id != task_id
    ):
        raise NotFoundError(
            "the retry authorization's predecessor execution is not available for this task"
        )
    if predecessor.review_iteration_id is not None:
        raise ConflictError(
            "the retry authorization addresses a remediation execution, not an "
            "implementation execution"
        )

    # --- the Task's implementation history admits no competing attempt. ---
    history = execution_records.list_task_executions(transaction_pool, task_id=task.id)
    for existing in history:
        if existing.status not in (ExecutionStatus.FAILED_TRANSIENT, ExecutionStatus.FAILED_FINAL):
            raise ConflictError(
                "the task carries an unresolved or successful implementation attempt; "
                "the authorized retry is not available"
            )

    # --- the exact unchanged approved subjects and the original session. ---
    gate, revision, loop, iteration, producer_session = _load_authorized_subjects(
        transaction_pool, task=task, require_ready_producer=True
    )
    if predecessor.producer_session_id != producer_session.id:
        raise ConflictError(
            "the predecessor execution does not belong to the task's current Producer session"
        )
    if predecessor.status not in (ExecutionStatus.FAILED_TRANSIENT, ExecutionStatus.FAILED_FINAL):
        raise ConflictError("the predecessor attempt is not positively known to have failed")
    if not _corroborate_pre_effect_execution_failure(
        transaction_pool, workspace_id=workspace_id, execution_id=predecessor.id
    ):
        raise ConflictError(
            "the predecessor attempt's failure is not positively proven to have "
            "occurred before any effect"
        )

    # --- the persisted due instant, enforced at actual admission. ---
    if attempt.not_before > now():
        raise StaleOperationError(
            "the retry authorization is not yet due; the wakeup has not arrived"
        )

    # --- reserve exactly one new Execution and bind the authorization. ---
    execution = execution_records.create_execution(
        transaction_pool,
        workspace_id=workspace_id,
        task_id=task.id,
        producer_session_id=producer_session.id,
        execution_number=max(existing.execution_number for existing in history) + 1,
    )
    claimed = execution_records.claim_execution_running(transaction_pool, execution_id=execution.id)
    if claimed is None:
        raise StaleOperationError(
            "the reserved retry attempt could not be claimed; the dispatch claim is stale"
        )
    event_coordination.record_execution_started_event(
        transaction_pool,
        workspace_id=workspace_id,
        task_id=task.id,
        execution_id=claimed.id,
        actor=event_coordination.producer_actor(producer_session.id),
    )
    bound = failure_records.bind_retry_attempt_execution(
        transaction_pool,
        attempt_id=attempt.id,
        task_id=task_id,
        workspace_id=workspace_id,
        resulting_execution_id=claimed.id,
    )
    if bound is None:
        # The set-once binding refused: the authorization was consumed
        # concurrently or is inconsistent. The whole composition rolls back,
        # so no second Execution survives.
        raise ConflictError(
            "the retry authorization could not be bound to the reserved execution; "
            "the authorization was consumed concurrently"
        )
    return _ReservedAttempt(
        task=task,
        owner_gate=gate,
        plan_revision=revision,
        review_loop=loop,
        review_iteration=iteration,
        accepted_review=accepted_review_result_from_iteration(iteration),
        producer_session=producer_session,
        execution=claimed,
    )


def execute_authorized_implementation_retry(
    pool: DatabasePool,
    *,
    profile_id: UUID,
    workspace_id: UUID,
    task_id: UUID,
    expected_state_token: UUID,
    retry_attempt_id: UUID,
    producer_runtime_factory: ProducerRuntimeAttachmentFactory,
    activity_subscription_factory: ActivitySubscriptionFactory | None = None,
    attempt_evidence: InvocationAttemptEvidence | None = None,
    clock: Callable[[], datetime] | None = None,
) -> AuthorizedImplementationExecution:
    """Execute the one F4-authorized additional implementation attempt.

    One Owner-accountable, exact-subject seam: the F4 authorization is
    atomically consumed for exactly one new Execution (reserved, claimed,
    audited, and bound set-once in the same short composition), the new
    Execution identity is recorded for the F5b failure handoff, and the
    shared E6 external mechanics dispatch the implementation on the original
    Producer session and finalize the new row truthfully. The caller cannot
    replay a consumed authorization, revive a terminal episode, run an
    undiscounted premature wakeup, or bypass any exact-subject revalidation.
    """
    _require_uuid_command(profile_id, "profile_id")
    _require_uuid_command(workspace_id, "workspace_id")
    _require_uuid_command(task_id, "task_id")
    _require_uuid_command(expected_state_token, "expected_state_token")
    _require_uuid_command(retry_attempt_id, "retry_attempt_id")
    producer_factory = _require_producer_factory(producer_runtime_factory)
    activity_factory = _require_activity_factory(activity_subscription_factory)
    now = clock if clock is not None else (lambda: datetime.now(UTC))
    with application_span(_SERVICE_TRACER_SCOPE, _EXECUTE_SPAN_NAME) as span:
        annotate_span(
            span,
            operation=_EXECUTE_SPAN_NAME,
            workspace_id=str(workspace_id),
            task_id=str(task_id),
        )
        # --- Phase 1: atomic admission, consumption, and reservation. ---
        with composed_transaction(pool) as transaction_pool:
            reserved = _consume_authorized_retry(
                transaction_pool,
                profile_id=profile_id,
                workspace_id=workspace_id,
                task_id=task_id,
                expected_state_token=expected_state_token,
                retry_attempt_id=retry_attempt_id,
                now=now,
            )
        if attempt_evidence is not None:
            # The F5b coordinator's handoff fact: this invocation's newly
            # produced retry Execution identity (#263) — never the original
            # episode Execution. F5b's durable binding lookup turns a later
            # failure into F4's PRIOR_AUTHORIZED report.
            attempt_evidence.record_attempt(
                family=PendingTaskRuntimeInteractionOperation.IMPLEMENTATION_EXECUTION,
                original_attempt_kind=OriginalAttemptKind.EXECUTION,
                original_attempt_id=reserved.execution.id,
                phase="authorized_implementation_retry",
            )
        message = serialize_review_result(reserved.accepted_review)
        # --- Phases 2 and 3: the shared authority-gated effects and finalize. ---
        return _execute_external_phase(
            pool,
            reserved=reserved,
            workspace_id=workspace_id,
            task_id=task_id,
            expected_state_token=expected_state_token,
            message=message,
            producer_factory=producer_factory,
            activity_factory=activity_factory,
        )
