"""Task planning bootstrap: admitted-session readiness, checkout preparation,
and the prepared-for-planning handoff (issue #203, E1b).

The focused shared application-service continuation that finishes the
bootstrap of the two E1a-admitted Task/role sessions for a Task claimed into
the coarse ``planning`` state, then orchestrates the Producer-owned isolated
checkout preparation and the runtime-specific same-external-ID
planning-context transition, and exposes the prepared-for-planning outcome
the E2 planning capability (issue #77) consumes. ``planning`` here is the
coarse Task state claimed by the #76 start capability; it never implies a
PLAN control or a ``plan_result``: session readiness and workspace
preparation are separate gates, this leaf issues no PLAN control, creates no
PlanRevision, and mutates no Task status.

``prepare_task_for_planning`` is ONE logical bootstrap continuation composed
in phases, with no database transaction ever held across a runtime call and
every external runtime operation performed outside Postgres transactions:

1. entry/claim phase (one short transaction) — the account-operational
   barrier FIRST, the per-Workspace arbitration advisory lock, the exact
   Workspace/Task Owner scope (#53), the exact current Task
   (``require_current_task``, #54) still in coarse ``planning``, the
   one-shot planning-bootstrap claim fact, and both durable role bindings.
   The stage matrix classifies the durable facts and, for the one provably
   safe first establishment, writes the one-shot claim BEFORE any runtime
   effect;
2. establishment phase (external, no transaction open) — per role, branching
   on durable lifecycle BEFORE any lease acquisition: an already-READY
   binding is reused directly with its exact identity and immutable
   effective-configuration snapshot (no lease, no adapter, no runtime
   effect), and a CONNECTING binding acquires the injected fresh
   establishment lease and goes through the #130 establishment service with
   the lease's selected configuration, so fresh construction and the durable
   READY snapshot agree by construction;
3. pre-PREPARE recheck phase (one short transaction) — a fresh exact
   Task/token currentness check, the claim-token binding, and the exact
   READY-binding identity of both roles, followed by the durable recovery of
   the original selected Producer repository base (never the live
   ``Connection.safe_config``) and the authoritative repository address —
   because establishment involves long-running external operations and a
   Task cancelled or rotated during them must fail closed BEFORE the
   checkout-creating PREPARE effect;
4. PREPARE phase (external, no transaction open) — one typed
   ``PrepareWorkspaceInteraction`` (#181) rendered from the authoritative
   repository address, GitHub issue number, Task UUID, and the selected
   checkout path, realized on the exact bound Producer session through the
   injected interaction lease; only the shared-parser-validated
   ``WorkspaceResult`` reporting the requested operation as COMPLETED
   enables planning, and every other outcome stops progression with the
   partial facts retained;
5. post-PREPARE recheck (one short transaction) — the same exact
   Task/token/claim/binding-identity currentness check;
6. planning-context transition (external) — one runtime-neutral
   ``enter_planning_checkout`` dispatch for the exact existing Producer
   session at its selected Task checkout (production Cline: the #74/#190
   same-external-ID reconstruction into PLAN); no PLAN control is ever sent
   here;
7. final recheck (one short transaction) and the typed outcome.

The one-shot planning-bootstrap claim (issue #203): the durable
``tasks.planning_bootstrap_claim_token`` fact is written exactly once,
conditionally on the caller's exact ``state_token``, immediately BEFORE the
first runtime session-creation dispatch. Token rotation alone cannot
distinguish an unclaimed bootstrap from the residue of a previously
attempted one whose creation outcome is unknown, and a re-entering caller
can always refresh the current token; the claim fact is the discriminator.
Presence means a prior claim's establishment is unresolved (uncertain or
known-failed creation, recovery-required finalization, or a
claimed-but-never-dispatched continuation) and the continuation fails closed
with zero runtime effects for Owner/recovery handling; absence with both
bindings CONNECTING is the one provably safe first establishment. This is
Task-level bootstrap claim state, not #87/#88 dispatch identity: no outbox,
no job queue, no scheduler, no recovery engine, and no process-local
single-flight authority is implemented here, and uncertain external
operations are never retried, never downgraded, and never blindly replayed.
"""

from __future__ import annotations

import logging
import uuid
from abc import ABC, abstractmethod
from collections.abc import Mapping
from contextlib import AbstractContextManager
from dataclasses import dataclass
from enum import StrEnum
from typing import Final
from uuid import UUID

from openorc.adapters.agent_runtime.contract import AgentRuntimeAdapter
from openorc.adapters.agent_runtime.errors import (
    AgentRuntimeError,
    AgentRuntimeInterruptedTurnError,
    AgentRuntimeProtocolFailureError,
    AgentRuntimeTransportError,
    AgentRuntimeUncertainOutcomeError,
)
from openorc.domain.connections import WorkflowRole
from openorc.domain.operational_failures import OriginalAttemptKind
from openorc.domain.pending_interactions import PendingTaskRuntimeInteractionOperation
from openorc.domain.sessions import TaskAgentSession, TaskSessionLifecycleStatus
from openorc.domain.tasks import (
    Task,
    TaskPlanningBootstrapStage,
    TaskStatus,
)
from openorc.observability import annotate_span, application_span
from openorc.persistence.connections import get_connection
from openorc.persistence.detached_turns import (
    get_latest_session_initialization_receipt,
    get_outstanding_task_detached_turn_receipt,
)
from openorc.persistence.ownership import get_repository, get_workspace
from openorc.persistence.pool import DatabasePool
from openorc.persistence.sessions import get_task_agent_session
from openorc.persistence.tasks import (
    claim_task_planning_bootstrap,
    get_task,
    get_task_planning_bootstrap_claim,
    get_task_planning_bootstrap_evidence,
    lock_workspace_task_arbitration,
    record_planning_bootstrap_stage,
)
from openorc.protocol.interaction import PrepareWorkspaceInteraction
from openorc.protocol.models import (
    WORKSPACE_OPERATION_PREPARE,
    WORKSPACE_RESULT_FAMILY,
    WorkspaceResult,
)
from openorc.services.errors import (
    ConflictError,
    ExternalOperationFailedError,
    ExternalOperationUncertainError,
    InvalidCommandError,
    NotFoundError,
    StaleOperationError,
)
from openorc.services.operation_attempt_evidence import InvocationAttemptEvidence
from openorc.services.profile_lifecycle_guard import require_account_operational
from openorc.services.session_runtime import (
    EstablishmentObservationRegistrar,
    dispatch_task_agent_session_establishment,
    establish_task_agent_session,
)
from openorc.services.task_abort import settle_observed_task_interrupt
from openorc.services.task_completion_handoff import DetachedTurnCompletionHandoff
from openorc.services.task_subject_guards import require_current_task
from openorc.services.transaction_composition import composed_transaction
from openorc.services.workspace_authorization import require_workspace_task

__all__ = [
    "PlanningBootstrapTerminalConflictError",
    "PlanningBootstrapUnresolvedError",
    "ResumableBootstrapContinuation",
    "TaskBootstrapEstablishmentLease",
    "TaskBootstrapRuntimeComposition",
    "TaskPlanningBootstrapDispatchOutcome",
    "TaskPlanningPreparationOutcome",
    "WorkspacePreparationFailedError",
    "complete_resumed_bootstrap_prepare",
    "find_resumable_bootstrap_continuation",
    "prepare_task_for_planning",
]

_SERVICE_TRACER_SCOPE = "openorc.services.task_planning_bootstrap"
_PREPARE_SPAN_NAME = "task_planning_bootstrap.prepare_task_for_planning"

_WORKSPACE_STATUS_COMPLETED: Final = "COMPLETED"

logger = logging.getLogger(__name__)


class PlanningBootstrapTerminalConflictError(ConflictError):
    """An admitted Task/role session binding is a terminal historical session.

    A LOST or ENDED binding is lifecycle history on the same binding: it is
    never replaced, never reopened, and never re-established, so the
    bootstrap continuation cannot proceed for that Task.
    """


class PlanningBootstrapUnresolvedError(ConflictError):
    """The bootstrap continuation cannot be safely resumed or replayed.

    The one-shot planning-bootstrap claim fact exists, so a previous claim
    held the bootstrap: its session-creation outcome is unknown (the merged
    seams cannot prove that another creation is safe) or its continuation is
    already past establishment (whether workspace preparation ran is
    unprovable). The continuation stops with zero runtime effects; recovery
    is authoritative Owner/recovery handling, never a blind re-dispatch.
    """


class WorkspacePreparationFailedError(ExternalOperationFailedError):
    """The Producer reported the requested workspace preparation as failed."""


@dataclass(frozen=True, slots=True)
class TaskBootstrapEstablishmentLease:
    """One fresh-runtime establishment composition result.

    ``adapter`` is the runtime adapter the #130 establishment service drives,
    and ``selected_configuration`` is the one non-secret admitted-Connection
    configuration the composition selected for this fresh establishment: the
    invoking continuation passes it verbatim as
    ``establish_task_agent_session(..., effective_configuration=...)`` so the
    fresh construction and the durable READY snapshot agree by construction.
    """

    adapter: AgentRuntimeAdapter
    selected_configuration: Mapping[str, object]


class TaskBootstrapRuntimeComposition(ABC):
    """The injected runtime seam of the #203 planning bootstrap continuation.

    The workflow-neutral boundary between the bootstrap continuation and the
    concrete Agent Runtime: the continuation owns every authorization,
    currentness, stage, and workflow-consequence decision, and this seam owns
    only the runtime mechanics of three narrow operations. Implementations
    must surface failures as the normalized #67 adapter-error taxonomy (the
    continuation translates it at its own boundary); they never decide
    workflow meaning, never mutate durable OpenOrc persistence, and never
    expand the universal ``AgentRuntimeAdapter`` surface with runtime-native
    vocabulary — the planning transition is expressed as the runtime-neutral
    prepared-for-planning handoff, never as a runtime's native mode steps.
    """

    @abstractmethod
    def establishment_lease(
        self,
        pool: DatabasePool,
        *,
        workspace_id: UUID,
        task_id: UUID,
        role: WorkflowRole,
        working_directory: str | None,
    ) -> AbstractContextManager[TaskBootstrapEstablishmentLease]:
        """Acquire one fresh-establishment runtime lease for the exact role.

        ``working_directory`` is the caller-authorized phase-selected
        runtime-side path: ``None`` for the fresh Producer (whose bootstrap
        base the composition selects with its configuration) and the
        separately supplied existing neutral non-repository directory for the
        fresh Reviewer. The yielded lease carries the adapter and the one
        selected non-secret configuration the establishment pins into the
        durable READY snapshot. Routing authority is the durable admitted
        binding's Connection, never the mutable role-binding route.
        """

    @abstractmethod
    def interaction_lease(
        self,
        pool: DatabasePool,
        *,
        workspace_id: UUID,
        task_id: UUID,
        role: WorkflowRole,
        external_session_id: str,
    ) -> AbstractContextManager[AgentRuntimeAdapter]:
        """Acquire one exact-session interaction runtime lease.

        The yielded adapter addresses the exact durable READY binding's
        external session identity; the continuation realizes the
        already-authorized #181 workspace preparation control through it. The
        lease never sends initialization, never creates or reconstructs
        sessions, and never substitutes the exact session.
        """

    @abstractmethod
    def enter_planning_checkout(
        self,
        pool: DatabasePool,
        *,
        workspace_id: UUID,
        task_id: UUID,
        external_session_id: str,
        checkout_directory: str,
    ) -> None:
        """Enter the exact existing Producer session into its Task checkout.

        The runtime-neutral prepared-for-planning handoff: the exact external
        session identity and its raw conversation are preserved, no
        initialization or ``session_ready`` is resent, and no PLAN control is
        issued — ordinary planning begins separately in the E2 capability.
        """


@dataclass(frozen=True, slots=True)
class TaskPlanningPreparationOutcome:
    """The successful bootstrap outcome the E2 planning capability consumes.

    Safe, non-secret facts only: the current post-claim Task, the exact READY
    Producer and Reviewer bindings, and the selected Producer checkout
    directory. The Task carries the rotated post-claim token, and E2 alone
    issues the first actual PLAN control from here.
    """

    task: Task
    producer_session: TaskAgentSession
    reviewer_session: TaskAgentSession
    producer_checkout_directory: str


@dataclass(frozen=True, slots=True)
class TaskPlanningBootstrapDispatchOutcome:
    """The detached establishment's dispatched suspension (issue #324).

    Safe, non-secret facts only: the current post-claim Task (whose state
    token is the claim's rotation), the exactly-once claim identity, and that
    rotated token. Both initialization turns are durably enqueued (or one
    role already READY while the other's receipt is outstanding); the
    dispatching worker returns without waiting for ``session_ready``, and
    the observed completions settle readiness and resume the bootstrap
    through the post-commit continuation trigger. No runtime result, no
    bound external identity, and no PREPARE has happened.
    """

    task: Task
    claim_token: UUID
    post_claim_token: UUID


class _ClaimedBootstrapDisposition(StrEnum):
    """The claimed-bootstrap disposition matrix (issue #324).

    Read off the durable establishment receipts, binding lifecycle, and
    settled stage evidence alone:

    - ``resumable``: every establishment settled into READY with nothing
      outstanding and no PREPARE yet dispatched — the readiness-resumed
      continuation proceeds past establishment;
    - ``in_progress``: every non-READY binding carries an outstanding
      initialization receipt — the establishments are durably dispatched and
      their completions own the resumption; a replayed invocation
      dispatches nothing;
    - ``unresolved_connecting``: a CONNECTING binding with no outstanding
      initialization receipt — the prior creation/admission outcome is
      unknowable from durable state, or its dispatch was refused or
      positively failed; another creation can never be proven safe;
    - ``unresolved_past_establishment``: settled stage evidence past
      establishment — whether the outstanding bootstrap work ran is
      unprovable.

    Both unresolved variants fail closed with zero runtime effects for
    Owner/recovery handling.
    """

    RESUMABLE = "resumable"
    IN_PROGRESS = "in_progress"
    UNRESOLVED_CONNECTING = "unresolved_connecting"
    UNRESOLVED_PAST_ESTABLISHMENT = "unresolved_past_establishment"

    def message(
        self,
        *,
        producer: TaskAgentSession,
        reviewer: TaskAgentSession,
    ) -> str:
        """The typed fail-closed reason for the unresolved dispositions."""
        if self is _ClaimedBootstrapDisposition.UNRESOLVED_CONNECTING:
            return (
                "a previous planning-bootstrap claim exists and its session "
                "establishment is unresolved; another creation cannot be "
                "proven safe and is never re-dispatched"
            )
        return (
            "a previous planning-bootstrap claim exists and its continuation "
            "is already past establishment; whether the outstanding bootstrap "
            "work ran is unprovable and is never replayed"
        )


def _classify_claimed_bootstrap(
    transaction_pool: DatabasePool,
    *,
    producer: TaskAgentSession,
    reviewer: TaskAgentSession,
    stage: TaskPlanningBootstrapStage | None,
) -> _ClaimedBootstrapDisposition:
    """Classify one claimed bootstrap from the durable facts alone.

    The stage evidence past establishment is the existing fail-closed
    unresolved classification (whether the outstanding bootstrap work ran is
    unprovable). With no settled stage, every binding must be provably
    dispatched by the detached establishment machinery — CONNECTING with an
    outstanding ``session_initialization`` receipt, or READY with a settled
    one (its initialization receipt is the establishment provenance) —
    before the bootstrap can be trusted as in-progress or resumable. A READY
    binding with no initialization receipt was established by the blocking
    handshake (or predates the receipt vocabulary): its post-establishment
    bootstrap work stays honestly unprovable, exactly as before issue #324.
    """
    if stage is not None:
        return _ClaimedBootstrapDisposition.UNRESOLVED_PAST_ESTABLISHMENT
    producer_receipt = get_outstanding_task_detached_turn_receipt(
        transaction_pool, task_agent_session_id=producer.id
    )
    reviewer_receipt = get_outstanding_task_detached_turn_receipt(
        transaction_pool, task_agent_session_id=reviewer.id
    )
    producer_dispatched = producer_receipt is not None or (
        producer.lifecycle_status is TaskSessionLifecycleStatus.READY
        and get_latest_session_initialization_receipt(
            transaction_pool, task_agent_session_id=producer.id
        )
        is not None
    )
    reviewer_dispatched = reviewer_receipt is not None or (
        reviewer.lifecycle_status is TaskSessionLifecycleStatus.READY
        and get_latest_session_initialization_receipt(
            transaction_pool, task_agent_session_id=reviewer.id
        )
        is not None
    )
    if not producer_dispatched or not reviewer_dispatched:
        return _ClaimedBootstrapDisposition.UNRESOLVED_CONNECTING
    if (
        producer.lifecycle_status is TaskSessionLifecycleStatus.READY
        and reviewer.lifecycle_status is TaskSessionLifecycleStatus.READY
    ):
        return _ClaimedBootstrapDisposition.RESUMABLE
    return _ClaimedBootstrapDisposition.IN_PROGRESS


@dataclass(frozen=True, slots=True)
class ResumableBootstrapContinuation:
    """The post-readiness resumable bootstrap continuation (issue #324).

    Safe, non-secret facts only: the current Task carrying the claim's
    rotated post-claim token, and the Workspace Owner accountable for the
    resumed continuation's authorization. Presence means the durable state
    proves the resumable classification; the F1 boundary owns the submission
    (services never build RQ/invocation payloads).
    """

    task: Task
    owner_profile_id: UUID


def find_resumable_bootstrap_continuation(
    pool: DatabasePool,
    *,
    workspace_id: UUID,
    task_id: UUID,
) -> ResumableBootstrapContinuation | None:
    """Classify the post-readiness resumable bootstrap state, read-only.

    The post-commit classification the continuation trigger runs (issue
    #324): the Task must be current and coarse-``planning`` under its
    exactly-once claim with no settled stage, both role bindings READY with
    nothing outstanding, and the Workspace must resolve its accountable
    Owner. ``None`` means the durable state is not (or no longer) the
    resumable post-readiness state and the trigger submits nothing — the
    decision is re-derived from committed state on every trigger, never
    assumed. No transaction is held across the caller's submission.
    """
    _require_uuid_command(workspace_id, "workspace_id")
    _require_uuid_command(task_id, "task_id")
    with composed_transaction(pool) as transaction_pool:
        task = get_task(transaction_pool, task_id=task_id)
        if task is None or task.workspace_id != workspace_id:
            return None
        if task.status is not TaskStatus.PLANNING:
            return None
        evidence = get_task_planning_bootstrap_evidence(transaction_pool, task_id=task_id)
        if evidence is None or evidence.stage is not None:
            return None
        producer = get_task_agent_session(
            transaction_pool, task_id=task_id, role=WorkflowRole.PRODUCER
        )
        reviewer = get_task_agent_session(
            transaction_pool, task_id=task_id, role=WorkflowRole.REVIEWER
        )
        if (
            producer is None
            or reviewer is None
            or producer.lifecycle_status is not TaskSessionLifecycleStatus.READY
            or reviewer.lifecycle_status is not TaskSessionLifecycleStatus.READY
        ):
            return None
        if (
            get_outstanding_task_detached_turn_receipt(
                transaction_pool, task_agent_session_id=producer.id
            )
            is not None
            or get_outstanding_task_detached_turn_receipt(
                transaction_pool, task_agent_session_id=reviewer.id
            )
            is not None
        ):
            return None
        workspace = get_workspace(transaction_pool, workspace_id=workspace_id)
        if workspace is None:
            return None
        return ResumableBootstrapContinuation(
            task=task,
            owner_profile_id=workspace.owner_profile_id,
        )


def _require_uuid_command(value: object, name: str) -> UUID:
    """Reject a malformed command identifier before any read or effect."""
    if not isinstance(value, UUID):
        raise InvalidCommandError(f"{name} must be a UUID")
    return value


def _require_runtime_composition(value: object) -> TaskBootstrapRuntimeComposition:
    if not isinstance(value, TaskBootstrapRuntimeComposition):
        raise InvalidCommandError("runtime must be a TaskBootstrapRuntimeComposition")
    return value


def _canonical_absolute_path_failure(value: object) -> str | None:
    """Return a safe failure description for a non-canonical absolute path.

    The runtime-side repository base and the neutral Reviewer directory are
    existing runtime-side absolute paths; Core validates their shape only and
    never probes any filesystem. A canonical absolute path starts with ``/``
    and carries no empty, ``.``, or ``..`` segments.
    """
    if not isinstance(value, str) or not value.strip():
        return "must be a non-empty absolute runtime-side path"
    if not value.startswith("/"):
        return "must be an absolute runtime-side path"
    segments = value.split("/")[1:]
    if any(segment in ("", ".", "..") for segment in segments):
        return "must be canonical (no empty, '.', or '..' path segments)"
    return None


def _require_reviewer_neutral_directory(value: object) -> str:
    """Validate the separately supplied neutral Reviewer directory shape.

    The directory is an existing runtime-side non-repository path supplied
    separately from the Producer repository base; Core never probes the
    runtime-side filesystem, so this is shape validation only and the value
    is passed through verbatim, never invented or substituted.
    """
    if not isinstance(value, str):
        raise InvalidCommandError("reviewer_neutral_directory must be a string")
    failure = _canonical_absolute_path_failure(value)
    if failure is not None:
        raise InvalidCommandError(f"reviewer_neutral_directory {failure}")
    return value


def _require_scoped_binding(
    pool: DatabasePool, *, workspace_id: UUID, task_id: UUID, role: WorkflowRole
) -> TaskAgentSession:
    """Load one Task/role binding of the addressed Task or fail closed."""
    binding = get_task_agent_session(pool, task_id=task_id, role=role)
    if binding is None or binding.workspace_id != workspace_id or binding.task_id != task_id:
        raise NotFoundError("the requested agent session is not available in this workspace")
    return binding


def _require_ready_binding(
    pool: DatabasePool, *, workspace_id: UUID, task_id: UUID, role: WorkflowRole
) -> TaskAgentSession:
    """Reload one Task/role binding and require its exact READY state."""
    binding = _require_scoped_binding(pool, workspace_id=workspace_id, task_id=task_id, role=role)
    if binding.lifecycle_status is not TaskSessionLifecycleStatus.READY:
        raise ConflictError(
            "the task/role session binding is no longer an initialized ready session"
        )
    return binding


def _require_binding_identity(
    binding: TaskAgentSession, *, expected: TaskAgentSession, role: WorkflowRole
) -> None:
    """Prove the reloaded binding is still the exact identity the phase saw."""
    if (
        binding.connection_id != expected.connection_id
        or binding.external_session_id != expected.external_session_id
    ):
        raise ConflictError(
            f"the {role.value} session binding's identity moved on during the "
            "bootstrap continuation"
        )


def _require_claim_binding(claim: object, *, expected: UUID) -> None:
    """Prove the reloaded one-shot claim is still this continuation's claim.

    The claim fact is written exactly once and never rewritten, so a
    divergence is an invariant violation and never guessed around.
    """
    if claim != expected:
        raise ConflictError(
            "the planning-bootstrap claim fact moved on during the bootstrap continuation"
        )


def _producer_snapshot_repository_base(
    pool: DatabasePool, binding: TaskAgentSession, *, workspace_id: UUID
) -> str:
    """Recover the original selected Producer repository base, fail closed.

    The authoritative original base is the one pinned in the binding's own
    durable non-secret ``effective_config_snapshot`` at initialization — the
    composition-selected admitted-Connection configuration the establishment
    service assembled — never a possibly-since-edited live
    ``Connection.safe_config`` value, so a later Owner configuration edit can
    never relocate the Task. The snapshot must be consistent with the
    admitted Connection's adapter, and the base must be a canonical safe
    absolute runtime-side path (a parent directory, not a shared clone).
    """
    snapshot = binding.effective_config_snapshot
    if not isinstance(snapshot, Mapping):
        raise ConflictError(
            "the ready producer binding carries no effective configuration snapshot"
        )
    connection = get_connection(pool, binding.connection_id)
    if connection is None or connection.workspace_id != workspace_id:
        raise NotFoundError("the requested connection is not available in this workspace")
    configuration = snapshot.get("configuration")
    if snapshot.get("adapter") != connection.adapter.value or not isinstance(
        configuration, Mapping
    ):
        raise ConflictError(
            "the producer binding's configuration snapshot is not consistent with "
            "its admitted connection's original selected configuration"
        )
    base = configuration.get("repository_base")
    if not isinstance(base, str) or not base.strip():
        raise ConflictError("the producer binding's snapshot carries no selected repository base")
    failure = _canonical_absolute_path_failure(base)
    if failure is not None:
        raise ConflictError(f"the producer binding's selected repository base {failure}")
    return base


def _require_prepared_workspace_result(result: object) -> WorkspaceResult:
    """Accept only the validated workspace_result reporting PREPARE COMPLETED.

    The shared #65 funnel already validated the formal response shape; this
    is the calling service's operation/status check. The result summary is
    advisory and never persisted as workflow authority.
    """
    if not isinstance(result, WorkspaceResult):
        raise ConflictError(
            "the workspace preparation control did not produce a typed workspace result"
        )
    if result.operation != WORKSPACE_OPERATION_PREPARE:
        raise WorkspacePreparationFailedError(
            "the producer reported a workspace operation other than the requested "
            "workspace preparation"
        )
    if result.status != _WORKSPACE_STATUS_COMPLETED:
        raise WorkspacePreparationFailedError(
            "the producer reported the requested workspace preparation as not completed"
        )
    return result


def _recheck_bootstrap_subject(
    pool: DatabasePool,
    *,
    workspace_id: UUID,
    task_id: UUID,
    profile_id: UUID,
    claim_token: UUID,
    expected_state_token: UUID,
    expected_producer: TaskAgentSession,
    expected_reviewer: TaskAgentSession,
) -> Task:
    """One short durable recheck of the exact bootstrap subject (issue #203).

    Composed before and after every consequential external effect: the
    account-operational barrier FIRST, the exact current Task (still coarse
    ``planning`` and non-archived) against the continuation's own post-claim
    token, the one-shot claim fact still bound to this continuation's claim
    identity, and the exact READY-binding identity of both roles. A Task
    cancelled, archived, or rotated during the long-running establishment
    phase fails closed here — before the checkout-creating PREPARE effect,
    never after it.
    """
    with composed_transaction(pool) as transaction_pool:
        require_account_operational(transaction_pool, profile_id=profile_id)
        task = require_current_task(
            transaction_pool,
            workspace_id=workspace_id,
            task_id=task_id,
            expected_state_token=expected_state_token,
        )
        if task.status is not TaskStatus.PLANNING:
            raise ConflictError("the addressed task is not in its coarse planning state")
        _require_claim_binding(
            get_task_planning_bootstrap_claim(transaction_pool, task_id=task_id),
            expected=claim_token,
        )
        producer = _require_ready_binding(
            transaction_pool,
            workspace_id=workspace_id,
            task_id=task_id,
            role=WorkflowRole.PRODUCER,
        )
        reviewer = _require_ready_binding(
            transaction_pool,
            workspace_id=workspace_id,
            task_id=task_id,
            role=WorkflowRole.REVIEWER,
        )
        _require_binding_identity(producer, expected=expected_producer, role=WorkflowRole.PRODUCER)
        _require_binding_identity(reviewer, expected=expected_reviewer, role=WorkflowRole.REVIEWER)
    return task


def _record_bootstrap_stage_or_fail(
    pool: DatabasePool,
    *,
    task_id: UUID,
    claim_token: UUID,
    stage: TaskPlanningBootstrapStage,
) -> None:
    """Record one forward-only bootstrap stage settlement, or fail closed.

    The F2 stage evidence (issue #87) is observational, never authoritative
    workflow state: the write never rotates the Task ``state_token`` and
    composes no workflow transition. A rejected write means the Task moved
    on (archived, a foreign claim identity, or an earlier stage still
    unsettled), so the continuation fails closed truthfully — it never
    manufactures stage history. A process death before the write simply
    leaves the stage conservatively unsettled, which is exactly the
    outstanding-work fact the pending-interaction projection reads.
    """
    recorded = record_planning_bootstrap_stage(
        pool, task_id=task_id, claim_token=claim_token, stage=stage
    )
    if not recorded:
        raise StaleOperationError(
            "the planning bootstrap moved on during preparation; the stage evidence is stale"
        )


def prepare_task_for_planning(
    pool: DatabasePool,
    runtime: TaskBootstrapRuntimeComposition,
    *,
    workspace_id: UUID,
    task_id: UUID,
    profile_id: UUID,
    expected_state_token: UUID,
    reviewer_neutral_directory: str,
    attempt_evidence: InvocationAttemptEvidence | None = None,
    observation_registrar: EstablishmentObservationRegistrar | None = None,
    completion_handoff: DetachedTurnCompletionHandoff | None = None,
) -> TaskPlanningPreparationOutcome | TaskPlanningBootstrapDispatchOutcome:
    """Run the one logical planning-bootstrap continuation for the Task.

    The continuation consumes the exact E1a handoff context: a current,
    non-archived Task in coarse ``planning`` whose admission reserved both
    the Producer and the Reviewer roles, addressed under the Workspace/Task
    Owner scope with the exact current ``state_token``.

    Two outcomes are possible (issue #324):

    - :class:`TaskPlanningPreparationOutcome` — the established
      prepared-for-planning handoff: both role sessions READY, the typed
      PREPARE control's validated ``workspace_result`` settled, and the
      existing Producer session moved into its Task checkout. The blocking
      establishment path returns this directly; the detached path returns it
      from the readiness-resumed continuation.
    - :class:`TaskPlanningBootstrapDispatchOutcome` — the detached
      establishment's dispatched suspension: both initialization turns are
      durably enqueued (or one role already READY while the other is
      outstanding), the worker returns without waiting for ``session_ready``,
      and the observed completions settle readiness and resume the
      bootstrap through the post-commit continuation trigger.

    It never admits capacity a second time, never rebinds a route, never
    recreates a Task, never replaces a session, never clones in Core, and
    never sends a PLAN control. With an ``observation_registrar`` the
    initialization is dispatched through the detached establishment
    machinery (one promptless ``start`` plus one ``run.enqueue`` admission
    each); without one — the queue-mode interim until #328 — the existing
    blocking readiness handshake is used unchanged.
    """
    with application_span(_SERVICE_TRACER_SCOPE, _PREPARE_SPAN_NAME) as span:
        _require_uuid_command(workspace_id, "workspace_id")
        _require_uuid_command(task_id, "task_id")
        _require_uuid_command(profile_id, "profile_id")
        _require_uuid_command(expected_state_token, "expected_state_token")
        runtime_composition = _require_runtime_composition(runtime)
        reviewer_neutral_directory = _require_reviewer_neutral_directory(reviewer_neutral_directory)
        if observation_registrar is not None and not callable(
            getattr(observation_registrar, "register_establishment_session", None)
        ):
            raise InvalidCommandError(
                "the establishment observation registrar does not implement the "
                "establishment registration seam"
            )
        annotate_span(
            span,
            operation=_PREPARE_SPAN_NAME,
            workspace_id=str(workspace_id),
            task_id=str(task_id),
        )

        # --- Phase 1: entry/claim (one short transaction, database only).
        # The account-operational barrier is the FIRST lock acquisition; the
        # per-Workspace arbitration advisory lock serializes the
        # classification against the capacity-releasing lifecycle
        # transitions; the stage matrix then decides from the durable facts
        # alone. ---
        with composed_transaction(pool) as transaction_pool:
            require_account_operational(transaction_pool, profile_id=profile_id)
            lock_workspace_task_arbitration(transaction_pool, workspace_id=workspace_id)
            require_workspace_task(
                transaction_pool,
                profile_id=profile_id,
                workspace_id=workspace_id,
                task_id=task_id,
            )
            task = require_current_task(
                transaction_pool,
                workspace_id=workspace_id,
                task_id=task_id,
                expected_state_token=expected_state_token,
            )
            if task.status is not TaskStatus.PLANNING:
                raise ConflictError("the addressed task is not in its coarse planning state")
            existing_claim = get_task_planning_bootstrap_claim(transaction_pool, task_id=task_id)
            producer = _require_scoped_binding(
                transaction_pool,
                workspace_id=workspace_id,
                task_id=task_id,
                role=WorkflowRole.PRODUCER,
            )
            reviewer = _require_scoped_binding(
                transaction_pool,
                workspace_id=workspace_id,
                task_id=task_id,
                role=WorkflowRole.REVIEWER,
            )
            if producer.lifecycle_status in (
                TaskSessionLifecycleStatus.LOST,
                TaskSessionLifecycleStatus.ENDED,
            ) or reviewer.lifecycle_status in (
                TaskSessionLifecycleStatus.LOST,
                TaskSessionLifecycleStatus.ENDED,
            ):
                raise PlanningBootstrapTerminalConflictError(
                    "the task's admitted session binding is a terminal historical "
                    "session; the bootstrap continuation cannot proceed"
                )
            if existing_claim is not None:
                # A previous claim held the bootstrap. The claimed-bootstrap
                # disposition matrix (issue #324) classifies from the
                # durable establishment receipts and stage evidence alone;
                # no runtime effect is ever re-dispatched here.
                evidence = get_task_planning_bootstrap_evidence(transaction_pool, task_id=task_id)
                disposition = _classify_claimed_bootstrap(
                    transaction_pool,
                    producer=producer,
                    reviewer=reviewer,
                    stage=None if evidence is None else evidence.stage,
                )
                if disposition is _ClaimedBootstrapDisposition.IN_PROGRESS:
                    # Every non-READY binding carries an outstanding
                    # initialization receipt: the establishments are durably
                    # dispatched and their completions own the resumption.
                    # This replayed invocation dispatches nothing, never
                    # re-creates, and never re-enqueues.
                    if attempt_evidence is not None:
                        # The claimed bootstrap's original identity travels
                        # on the no-effect replay for the coordinator's
                        # records.
                        attempt_evidence.record_task_token(task.state_token)
                    return TaskPlanningBootstrapDispatchOutcome(
                        task=task,
                        claim_token=existing_claim,
                        post_claim_token=task.state_token,
                    )
                if disposition is _ClaimedBootstrapDisposition.RESUMABLE:
                    # Every establishment settled into READY with nothing
                    # outstanding and no PREPARE yet dispatched: the
                    # readiness-resumed continuation proceeds past
                    # establishment.
                    claim_token = existing_claim
                    post_claim_token = task.state_token
                else:
                    raise PlanningBootstrapUnresolvedError(
                        disposition.message(producer=producer, reviewer=reviewer)
                    )
            else:
                # The one provably safe first establishment: the one-shot
                # claim fact is absent, so no bootstrap claim ever succeeded
                # and no creation was ever dispatched for either binding.
                # The claim is written exactly once, conditionally on the
                # caller's exact token, BEFORE any runtime effect; the
                # continuation binds its later rechecks to its own claim
                # identity.
                claim_token = uuid.uuid4()
                post_claim_token = claim_task_planning_bootstrap(
                    transaction_pool,
                    task_id=task_id,
                    expected_state_token=expected_state_token,
                    claim_token=claim_token,
                )
                if post_claim_token is None:
                    # A concurrent continuation won the exactly-once claim:
                    # this invocation dispatches nothing and never retries.
                    raise StaleOperationError(
                        "the task state moved on during the planning-bootstrap claim; "
                        "the bootstrap is stale or already claimed"
                    )
                task = get_task(transaction_pool, task_id=task_id)
                if task is None:
                    raise NotFoundError("the requested task is not available in this workspace")

        if attempt_evidence is not None:
            # The F5b coordinator's handoff facts: the post-claim token this
            # invocation established (the claim rotates the Task token) and
            # the claimed bootstrap identity in its PREPARE phase (#262).
            attempt_evidence.record_task_token(post_claim_token)
            attempt_evidence.record_attempt(
                family=PendingTaskRuntimeInteractionOperation.PLANNING_BOOTSTRAP_PREPARE,
                original_attempt_kind=OriginalAttemptKind.PLANNING_BOOTSTRAP_CLAIM,
                original_attempt_id=claim_token,
                phase="planning_bootstrap_prepare",
            )

        bindings: dict[WorkflowRole, TaskAgentSession] = {
            WorkflowRole.PRODUCER: producer,
            WorkflowRole.REVIEWER: reviewer,
        }
        # --- Phase 2: establishment (external, no transaction open). Per
        # role, branch on durable lifecycle BEFORE any lease acquisition: an
        # already-READY binding is reused with no runtime effect. With an
        # observation registrar the CONNECTING binding's initialization is
        # dispatched through the detached establishment machinery (one
        # promptless ``start``, one observation registration, one
        # ``run.enqueue`` admission each — issue #324); without one, the
        # queue-mode interim, it is established exactly once through the
        # blocking #130 readiness handshake. ---
        dispatched_detached = False
        for role in (WorkflowRole.PRODUCER, WorkflowRole.REVIEWER):
            binding = bindings[role]
            if binding.lifecycle_status is TaskSessionLifecycleStatus.READY:
                continue
            working_directory = (
                None if role is WorkflowRole.PRODUCER else reviewer_neutral_directory
            )
            with runtime_composition.establishment_lease(
                pool,
                workspace_id=workspace_id,
                task_id=task_id,
                role=role,
                working_directory=working_directory,
            ) as lease:
                if observation_registrar is not None:
                    dispatched = dispatch_task_agent_session_establishment(
                        pool,
                        lease.adapter,
                        workspace_id=workspace_id,
                        task_id=task_id,
                        role=role,
                        effective_configuration=lease.selected_configuration,
                        original_attempt_kind=OriginalAttemptKind.PLANNING_BOOTSTRAP_CLAIM,
                        original_attempt_id=claim_token,
                        observation_registrar=observation_registrar,
                    )
                    bindings[role] = dispatched.task_agent_session
                    dispatched_detached = True
                else:
                    established = establish_task_agent_session(
                        pool,
                        lease.adapter,
                        workspace_id=workspace_id,
                        task_id=task_id,
                        role=role,
                        effective_configuration=lease.selected_configuration,
                    )
                    bindings[role] = established
        if dispatched_detached:
            # The detached path never waits for ``session_ready`` in the
            # dispatching worker. Both initialization turns are durably
            # enqueued (or already settled); the observed completions settle
            # readiness and resume the bootstrap. When every establishment
            # already settled within this dispatch, the ONE post-commit
            # completion handoff submits the readiness-resumed continuation
            # now; otherwise the later observed completion triggers it.
            if completion_handoff is not None:
                try:
                    completion_handoff.handoff_settled_completion(
                        workspace_id=workspace_id,
                        task_id=task_id,
                        operation=PendingTaskRuntimeInteractionOperation.SESSION_INITIALIZATION,
                    )
                except Exception as error:  # noqa: BLE001 - contained handoff
                    logger.warning(
                        "the detached-completion handoff failed after the "
                        "detached establishment dispatch (%s); the durable "
                        "resumable state owns recovery",
                        type(error).__name__,
                    )
            return TaskPlanningBootstrapDispatchOutcome(
                task=task,
                claim_token=claim_token,
                post_claim_token=post_claim_token,
            )

        # --- Phase 3: pre-PREPARE recheck (one short transaction): the exact
        # Task/token/claim currentness and the exact READY-binding identity,
        # then the durable recovery of the original selected Producer base
        # and the authoritative repository address. ---
        task = _recheck_bootstrap_subject(
            pool,
            workspace_id=workspace_id,
            task_id=task_id,
            profile_id=profile_id,
            claim_token=claim_token,
            expected_state_token=post_claim_token,
            expected_producer=bindings[WorkflowRole.PRODUCER],
            expected_reviewer=bindings[WorkflowRole.REVIEWER],
        )
        with composed_transaction(pool) as transaction_pool:
            producer = _require_ready_binding(
                transaction_pool,
                workspace_id=workspace_id,
                task_id=task_id,
                role=WorkflowRole.PRODUCER,
            )
            original_base = _producer_snapshot_repository_base(
                transaction_pool, producer, workspace_id=workspace_id
            )
            repository = get_repository(transaction_pool, task.repository_id)
            if repository is None or repository.workspace_id != task.workspace_id:
                raise NotFoundError("the requested repository is not available in this workspace")
        producer = bindings[WorkflowRole.PRODUCER]
        assert producer.external_session_id is not None
        checkout_directory = f"{original_base}/{task.id}"
        failure = _canonical_absolute_path_failure(checkout_directory)
        if failure is not None:
            raise ConflictError(f"the selected task checkout directory {failure}")

        # --- Phase 4: one typed PREPARE to the exact bound Producer session
        # (external, no transaction open). The durable PREPARE-dispatch
        # marker (issue #324) is committed strictly before the send: a
        # replayed or raced post-readiness continuation can never re-dispatch
        # a possibly-delivered control, and the send's outcome — whatever it
        # is — leaves the honest ``prepare_dispatched`` stage evidence for
        # the existing fail-closed recovery semantics. ---
        if not record_planning_bootstrap_stage(
            pool,
            task_id=task_id,
            claim_token=claim_token,
            stage=TaskPlanningBootstrapStage.PREPARE_DISPATCHED,
        ):
            raise ConflictError(
                "the planning-bootstrap stage did not advance to its PREPARE-dispatch "
                "marker; the PREPARE control is never re-dispatched against moved-on "
                "bootstrap state"
            )
        interaction = PrepareWorkspaceInteraction(
            repository_full_name=(f"{repository.metadata.owner_login}/{repository.metadata.name}"),
            issue_number=task.github_issue_number,
            task_uuid=task.id,
            target_directory=checkout_directory,
        )
        with runtime_composition.interaction_lease(
            pool,
            workspace_id=workspace_id,
            task_id=task_id,
            role=WorkflowRole.PRODUCER,
            external_session_id=producer.external_session_id,
        ) as adapter:
            try:
                result = adapter.realize_control(
                    producer.external_session_id,
                    interaction,
                    message=interaction.render(),
                    expected_family=WORKSPACE_RESULT_FAMILY,
                )
            except AgentRuntimeInterruptedTurnError as error:
                settle_observed_task_interrupt(
                    pool,
                    adapter,
                    task_id=task_id,
                    external_session_id=producer.external_session_id,
                )
                logger.warning(
                    "the workspace preparation control ended with the runtime "
                    "interrupting the send; the bootstrap stage stays unsettled"
                )
                raise ExternalOperationUncertainError(
                    "the workspace preparation control's send was interrupted by the "
                    "runtime; nothing is retried and the bootstrap stops with its "
                    "partial facts retained"
                ) from error
            except AgentRuntimeUncertainOutcomeError as error:
                logger.warning(
                    "the workspace preparation control ended with an uncertain "
                    "outcome; the requested preparation is never retried here"
                )
                raise ExternalOperationUncertainError(
                    "the outcome of the workspace preparation control is unknown; "
                    "nothing is retried and the bootstrap stops with its partial "
                    "facts retained"
                ) from error
            except (AgentRuntimeTransportError, AgentRuntimeProtocolFailureError) as error:
                logger.warning(
                    "the workspace preparation control ended with a post-send "
                    "runtime failure; the bootstrap stage stays unsettled"
                )
                raise ExternalOperationUncertainError(
                    "the workspace preparation control's send may have been "
                    "delivered but its response could not be interpreted; nothing "
                    "is retried and the bootstrap stops with its partial facts "
                    "retained"
                ) from error
            except AgentRuntimeError as error:
                raise ExternalOperationFailedError(
                    "the workspace preparation control failed before completion; "
                    "the bootstrap stops with its partial facts retained"
                ) from error
        _require_prepared_workspace_result(result)

        # --- The shared post-PREPARE tail: stage settlement, the
        # planning-context transition, and the prepared-for-planning
        # handoff (shared with the F8g resume continuation, issue #292). ---
        return _complete_prepared_bootstrap_tail(
            pool,
            runtime_composition,
            workspace_id=workspace_id,
            profile_id=profile_id,
            task_id=task_id,
            claim_token=claim_token,
            expected_state_token=post_claim_token,
            producer=bindings[WorkflowRole.PRODUCER],
            reviewer=bindings[WorkflowRole.REVIEWER],
            checkout_directory=checkout_directory,
            attempt_evidence=attempt_evidence,
        )


def _complete_prepared_bootstrap_tail(
    pool: DatabasePool,
    runtime_composition: TaskBootstrapRuntimeComposition,
    *,
    workspace_id: UUID,
    profile_id: UUID,
    task_id: UUID,
    claim_token: UUID,
    expected_state_token: UUID,
    producer: TaskAgentSession,
    reviewer: TaskAgentSession,
    checkout_directory: str,
    attempt_evidence: InvocationAttemptEvidence | None,
) -> TaskPlanningPreparationOutcome:
    """The shared post-PREPARE tail of the bootstrap continuation.

    One validated ``workspace_result`` in, the settled prepared-for-planning
    handoff out: the F2 ``prepared`` stage evidence, the post-PREPARE
    subject recheck, the one runtime-neutral planning-context transition
    (external, no transaction open; no PLAN control is ever sent here), the
    ``ready_for_planning`` settlement evidence, and the final recheck. Both
    the ordinary E1b continuation and the F8g resume continuation complete
    through this exact tail — never a second bootstrap composition.
    """
    _record_bootstrap_stage_or_fail(
        pool,
        task_id=task_id,
        claim_token=claim_token,
        stage=TaskPlanningBootstrapStage.PREPARED,
    )

    if attempt_evidence is not None:
        # The same claim identity advances to its transition phase: the
        # coordinator reports the phase the failing invocation reached
        # (issue #262).
        attempt_evidence.record_attempt(
            family=PendingTaskRuntimeInteractionOperation.PLANNING_BOOTSTRAP_TRANSITION,
            original_attempt_kind=OriginalAttemptKind.PLANNING_BOOTSTRAP_CLAIM,
            original_attempt_id=claim_token,
            phase="planning_bootstrap_transition",
        )

    _recheck_bootstrap_subject(
        pool,
        workspace_id=workspace_id,
        task_id=task_id,
        profile_id=profile_id,
        claim_token=claim_token,
        expected_state_token=expected_state_token,
        expected_producer=producer,
        expected_reviewer=reviewer,
    )

    if producer.external_session_id is None or not producer.external_session_id.strip():
        raise ConflictError("the producer session binding carries no external session identity")

    try:
        runtime_composition.enter_planning_checkout(
            pool,
            workspace_id=workspace_id,
            task_id=task_id,
            external_session_id=producer.external_session_id,
            checkout_directory=checkout_directory,
        )
    except AgentRuntimeUncertainOutcomeError as error:
        logger.warning(
            "the planning-context transition ended with an uncertain outcome; "
            "it is never retried or replayed here"
        )
        raise ExternalOperationUncertainError(
            "the outcome of the planning-context transition is unknown; "
            "nothing is retried and the bootstrap stops with its partial "
            "facts retained"
        ) from error
    except (AgentRuntimeTransportError, AgentRuntimeProtocolFailureError) as error:
        logger.warning(
            "the planning-context transition ended with a post-send runtime "
            "failure; the bootstrap stage stays unsettled"
        )
        raise ExternalOperationUncertainError(
            "the planning-context transition's send may have been delivered "
            "but its response could not be interpreted; nothing is retried "
            "and the bootstrap stops with its partial facts retained"
        ) from error
    except AgentRuntimeError as error:
        raise ExternalOperationFailedError(
            "the planning-context transition failed; the bootstrap stops "
            "with its partial facts retained"
        ) from error

    _record_bootstrap_stage_or_fail(
        pool,
        task_id=task_id,
        claim_token=claim_token,
        stage=TaskPlanningBootstrapStage.READY_FOR_PLANNING,
    )

    task = _recheck_bootstrap_subject(
        pool,
        workspace_id=workspace_id,
        task_id=task_id,
        profile_id=profile_id,
        claim_token=claim_token,
        expected_state_token=expected_state_token,
        expected_producer=producer,
        expected_reviewer=reviewer,
    )
    return TaskPlanningPreparationOutcome(
        task=task,
        producer_session=producer,
        reviewer_session=reviewer,
        producer_checkout_directory=checkout_directory,
    )


# __CHUNK_APPEND_MARKER__


def complete_resumed_bootstrap_prepare(
    pool: DatabasePool,
    runtime_composition: TaskBootstrapRuntimeComposition,
    *,
    workspace_id: UUID,
    profile_id: UUID,
    task_id: UUID,
    expected_state_token: UUID,
    result: WorkspaceResult,
) -> TaskPlanningPreparationOutcome:
    """Complete the resumed bootstrap PREPARE stage with its validated result.

    The Owner Resume routing's boundary-C composition for the interrupted
    E1b PREPARE control (issue #292): the ORIGINAL bootstrap claim identity
    and both READY bindings are re-derived from durable state — completed
    establishment effects are never repeated — and the resumed turn's
    validated ``workspace_result`` continues the SAME logical operation
    through the one shared post-PREPARE tail. An already-settled stage (a
    PREPARED-but-untransitioned bootstrap, whose outstanding fact is the
    planning-context reconstruction rather than an interrupted turn) fails
    closed.
    """
    evidence = get_task_planning_bootstrap_evidence(pool, task_id=task_id)
    if evidence is None:
        raise ConflictError("the task carries no planning bootstrap claim; the resume is stale")
    if evidence.stage is not None:
        raise ConflictError("the bootstrap PREPARE stage is already settled; the resume is stale")
    producer = _require_ready_binding(
        pool, workspace_id=workspace_id, task_id=task_id, role=WorkflowRole.PRODUCER
    )
    reviewer = _require_ready_binding(
        pool, workspace_id=workspace_id, task_id=task_id, role=WorkflowRole.REVIEWER
    )
    with composed_transaction(pool) as transaction_pool:
        original_base = _producer_snapshot_repository_base(
            transaction_pool, producer, workspace_id=workspace_id
        )
    checkout_directory = f"{original_base}/{task_id}"
    failure = _canonical_absolute_path_failure(checkout_directory)
    if failure is not None:
        raise ConflictError(f"the selected task checkout directory {failure}")
    _require_prepared_workspace_result(result)
    return _complete_prepared_bootstrap_tail(
        pool,
        runtime_composition,
        workspace_id=workspace_id,
        profile_id=profile_id,
        task_id=task_id,
        claim_token=evidence.claim_token,
        expected_state_token=expected_state_token,
        producer=producer,
        reviewer=reviewer,
        checkout_directory=checkout_directory,
        attempt_evidence=None,
    )


def producer_checkout_directory(pool: DatabasePool, *, workspace_id: UUID, task_id: UUID) -> str:
    """Derive the exact prepared Task checkout from the durable binding snapshot.

    The derivation rule this module owns (the original base's deterministic
    per-Task directory, fail-closed snapshot recovery) exposed for the F8g
    resume routing's Producer attachments: the workflow's Producer sessions
    address exactly the isolated checkout the bootstrap prepared, never a
    substituted path.
    """
    binding = get_task_agent_session(pool, task_id=task_id, role=WorkflowRole.PRODUCER)
    if binding is None or binding.workspace_id != workspace_id:
        raise ConflictError(
            "the requested producer session binding is not available in this workspace"
        )
    with composed_transaction(pool) as transaction_pool:
        base = _producer_snapshot_repository_base(
            transaction_pool, binding, workspace_id=workspace_id
        )
    return f"{base}/{task_id}"
