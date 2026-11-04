"""F5f: the one durably authorized exact-head PR-authorization presentation
retry (issue #269).

When F4 (#251) has durably authorized one bounded additional presentation
attempt for an exact-head PR-authorization presentation failure episode
(the initial or authorized-retry composition's ``open_pr_authorization``
failed operationally at its B3/B7 GitHub reads after a successful E6), this
seam executes that single authorization exactly once. An authorized retry is
**not** a replay of the implementation: it re-derives every durable subject
from committed state, consumes the authorization set-once, and re-enters
ONLY the guarded E7 presentation — E6 is never rerun, no new Execution is
ever created, the one-shot canonical-branch bind and the one-current-gate
guards are unchanged, and the exact-head ``PR_AUTHORIZATION`` gate stays the
Owner's Approve/Cancel decision.

Composition (reads precede consumption; no database transaction ever spans
a GitHub call):

1. **Read-only durable pre-verification** (no writes): the account-operational
   barrier and the exact Profile's Workspace/Task Owner scope, the current
   pre-gate ``implementing`` Task under the caller's ``state_token`` with no
   current gate, the F4 attempt and its exact-head presentation episode
   (non-terminally disposed, rooted at the still-SUCCEEDED successful E6
   Execution UUID, the episode's latest ordinal with every earlier ordinal
   consumed), and the persisted due instant. Every refusal is a typed error
   raised before anything is consumed.
2. **Atomic consumption** (one short transaction): the one-shot consumption
   stamp (``consumed_at``) written through the repository conditional
   set-once update — a competing worker, duplicate delivery, or premature
   wakeup can never re-consume the authorization or reach the presentation.
3. **The guarded presentation**: ``open_pr_authorization`` re-derives every
   admission fact under its own transactions and performs the same B3/B7
   GitHub reads and the same final atomic bind + gate + transition as the
   ordinary presentation. Its operational failures propagate unchanged for
   the F5b coordinator's typed Level 2 handoff — the invocation's attempt
   evidence names the consumed attempt (its one-shot ``consumed_at`` stamp
   is the durable binding; the presentation family has no resulting
   Execution), addressed to the episode's original Execution identity with
   ``PRIOR_AUTHORIZED``. E7's own Phase-A admission recording is directed
   at a throwaway collector so it can never overwrite that consumed-attempt
   handoff identity: the latest recorded identity of an authorized
   re-presentation stays the consumed attempt for the whole invocation, and
   F4 therefore advances the existing episode's attempt accounting (or
   escalates when the budget is exhausted) instead of classifying the
   failure as the already-disposed original report. Its workflow-semantic
   refusals (a stale subject, a confirmed ``github_source_changed`` drift,
   a closed source issue) are Level 1 outcomes, never triaged: the consumed
   authorization remains forward-only consumed history, the workflow stays
   at its committed durable state, and nothing is replayed or re-consumed.

This seam never schedules wakeups (F11 #257 owns due-time submission), never
reruns the implementation, never mutates the successful E6 Execution, and
never applies a second presentation within one invocation.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Final
from uuid import UUID

from openorc.adapters.github import GitHubAppClient
from openorc.domain.executions import ExecutionStatus
from openorc.domain.operational_failures import (
    OperationalFailureAttempt,
    OperationalFailureEpisode,
    OriginalAttemptKind,
    PRAuthorizationPresentationOperation,
)
from openorc.domain.tasks import Task, TaskStatus
from openorc.observability import annotate_span, application_span
from openorc.persistence import executions as execution_records
from openorc.persistence import operational_failures as failure_records
from openorc.persistence.pool import DatabasePool
from openorc.services.errors import (
    ConflictError,
    InvalidCommandError,
    NotFoundError,
    StaleOperationError,
)
from openorc.services.operation_attempt_evidence import InvocationAttemptEvidence
from openorc.services.profile_lifecycle_guard import require_account_operational
from openorc.services.task_pr_authorization import PrAuthorizationGateOpened, open_pr_authorization
from openorc.services.task_subject_guards import require_current_task
from openorc.services.workspace_authorization import require_workspace_task

__all__ = [
    "execute_authorized_pr_authorization_retry",
]

_SERVICE_TRACER_SCOPE: Final[str] = "openorc.services.task_authorized_pr_authorization_retry"
_EXECUTE_SPAN_NAME: Final[str] = "task_authorized_pr_authorization_retry.execute"

# The safe machine phase label the invocation records as it progresses; the
# latest one is the phase a failing invocation reached (F5b handoff fact).
_PHASE_PRESENTATION: Final[str] = "pr_authorization_retry_presentation"


@dataclass(frozen=True, slots=True)
class _PresentationRetryAdmission:
    """The durably verified subjects one authorized presentation retry executes."""

    task: Task
    episode: OperationalFailureEpisode
    attempt: OperationalFailureAttempt


def _require_uuid_command(value: object, name: str) -> UUID:
    if not isinstance(value, UUID):
        raise InvalidCommandError(f"{name} must be a UUID")
    return value


def _load_presentation_retry_subjects(
    pool: DatabasePool,
    *,
    profile_id: UUID,
    workspace_id: UUID,
    task_id: UUID,
    retry_attempt_id: UUID,
    expected_state_token: UUID,
    now: Callable[[], datetime],
) -> _PresentationRetryAdmission:
    """Re-derive every retry subject from committed state (reads only).

    The exact currentness, the F4 authorization and its episode/budget
    state, the persisted due instant, and the still-SUCCEEDED episode-root
    Execution. Every refusal is a typed error raised before anything is
    consumed; the guarded presentation's own admission re-proves the same
    subjects under its Task-row transactions.
    """
    require_account_operational(pool, profile_id=profile_id)
    require_workspace_task(pool, profile_id=profile_id, workspace_id=workspace_id, task_id=task_id)
    current = require_current_task(
        pool,
        workspace_id=workspace_id,
        task_id=task_id,
        expected_state_token=expected_state_token,
    )
    if current.status is not TaskStatus.IMPLEMENTING or current.current_owner_gate_id is not None:
        raise StaleOperationError(
            "the task is not in the pre-gate implementing state; the presentation retry is stale"
        )

    # --- the F4-issued authorization and its episode. ---
    attempt = failure_records.get_operational_failure_attempt(pool, attempt_id=retry_attempt_id)
    if attempt is None or attempt.workspace_id != workspace_id or attempt.task_id != task_id:
        raise NotFoundError("the requested retry attempt is not available for this task")
    if attempt.consumed_at is not None:
        raise ConflictError(
            "the retry authorization has already been consumed; it is never re-consumed"
        )
    episode = failure_records.get_operational_failure_episode(pool, episode_id=attempt.episode_id)
    if (
        episode is None
        or episode.workspace_id != workspace_id
        or episode.task_id != task_id
        or episode.operation_family
        is not PRAuthorizationPresentationOperation.EXACT_HEAD_PR_AUTHORIZATION_PRESENTATION
        or episode.original_attempt_kind is not OriginalAttemptKind.EXECUTION
    ):
        raise ConflictError(
            "the retry attempt's failure episode does not address this task's "
            "exact-head pr authorization presentation"
        )
    if episode.escalated_task_block_id is not None:
        raise ConflictError(
            "the retry attempt's failure episode is terminally disposed; "
            "the authorization can no longer be executed"
        )

    # --- the episode's attempt budget: latest ordinal, coherent history. ---
    attempts = failure_records.list_operational_failure_attempts(pool, episode_id=episode.id)
    if not attempts or attempts[-1].id != attempt.id:
        raise ConflictError("the retry authorization is not the failure episode's latest attempt")
    for earlier in attempts[:-1]:
        if earlier.consumed_at is None:
            raise ConflictError(
                "an earlier authorized presentation attempt was never consumed; "
                "the episode's attempt history is incoherent"
            )

    # --- the persisted due instant, enforced at actual admission. ---
    if attempt.not_before > now():
        raise StaleOperationError(
            "the retry authorization is not yet due; the wakeup has not arrived"
        )

    # --- the episode root: the still-SUCCEEDED successful E6 Execution. ---
    execution = execution_records.get_execution(pool, execution_id=episode.original_attempt_id)
    if (
        execution is None
        or execution.workspace_id != workspace_id
        or execution.task_id != task_id
        or execution.status is not ExecutionStatus.SUCCEEDED
        or execution.review_iteration_id is not None
    ):
        raise ConflictError(
            "the presentation episode's root execution is no longer the task's "
            "successful implementation attempt"
        )
    return _PresentationRetryAdmission(task=current, episode=episode, attempt=attempt)


def execute_authorized_pr_authorization_retry(
    pool: DatabasePool,
    github: GitHubAppClient,
    *,
    profile_id: UUID,
    workspace_id: UUID,
    task_id: UUID,
    expected_state_token: UUID,
    retry_attempt_id: UUID,
    attempt_evidence: InvocationAttemptEvidence | None = None,
    clock: Callable[[], datetime] | None = None,
) -> PrAuthorizationGateOpened:
    """Execute the one F4-authorized exact-head presentation retry (F5f, issue #269).

    The durably verified subjects are consumed set-once and the guarded E7
    presentation is re-entered exactly once — never the implementation
    effect, never a second Execution, never a re-approval. See the module
    docstring for the full composition and failure semantics.
    """
    _require_uuid_command(profile_id, "profile_id")
    _require_uuid_command(workspace_id, "workspace_id")
    _require_uuid_command(task_id, "task_id")
    _require_uuid_command(expected_state_token, "expected_state_token")
    _require_uuid_command(retry_attempt_id, "retry_attempt_id")
    now = clock if clock is not None else (lambda: datetime.now(UTC))
    with application_span(_SERVICE_TRACER_SCOPE, _EXECUTE_SPAN_NAME) as span:
        annotate_span(
            span,
            operation=_EXECUTE_SPAN_NAME,
            workspace_id=str(workspace_id),
            task_id=str(task_id),
        )
        admission = _load_presentation_retry_subjects(
            pool,
            profile_id=profile_id,
            workspace_id=workspace_id,
            task_id=task_id,
            retry_attempt_id=retry_attempt_id,
            expected_state_token=expected_state_token,
            now=now,
        )
        # --- Stage 2: the one-shot consumption stamp. ---
        consumed = failure_records.consume_operational_failure_attempt(
            pool,
            attempt_id=admission.attempt.id,
            task_id=task_id,
            workspace_id=workspace_id,
            consumed_at=now(),
        )
        if consumed is None:
            raise ConflictError(
                "the presentation retry authorization could not be consumed; "
                "a competing execution consumed it first"
            )
        # The invocation's reporting identity stays the consumed attempt for
        # the whole authorized re-presentation: recording the post-consumption
        # currentness (the loader proved the exact payload token current; the
        # consumption never rotates it) and the consumed-attempt identity is
        # the invocation's own F5b handoff fact — a later operational failure
        # of this re-presentation must report PRIOR_AUTHORIZED against the
        # consumed attempt, never a fresh ORIGINAL claim over the episode's
        # root Execution (which F4 would classify as the already-disposed
        # original report and strand the consumed retry without a
        # disposition).
        if attempt_evidence is not None:
            attempt_evidence.record_task_token(admission.task.state_token)
            attempt_evidence.record_attempt(
                family=PRAuthorizationPresentationOperation.EXACT_HEAD_PR_AUTHORIZATION_PRESENTATION,
                original_attempt_kind=OriginalAttemptKind.EXECUTION,
                original_attempt_id=consumed.id,
                effect_attempt_id=consumed.id,
                phase=_PHASE_PRESENTATION,
            )
        # The guarded presentation's own Phase-A admission recording is
        # directed at a throwaway collector: E7's unconditional admission
        # identity (the root Execution UUID, no effect row) must never
        # overwrite the invocation's consumed-attempt handoff identity
        # above. The real handoff facts are already complete.
        presentation_evidence = InvocationAttemptEvidence()
        # --- Stage 3: the guarded E7-only re-presentation. ---
        return open_pr_authorization(
            pool,
            github,
            profile_id=profile_id,
            workspace_id=workspace_id,
            task_id=task_id,
            expected_state_token=expected_state_token,
            execution_id=admission.episode.original_attempt_id,
            attempt_evidence=presentation_evidence,
        )
