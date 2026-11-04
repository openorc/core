"""The F5b application Level 2 operational-failure handoff (issue #262).

One transport-neutral application coordinator around the F5a Task-workflow
Level 1 compositions. The coordinator invokes Level 1 unchanged, preserves
ordinary success and authorized continuations, and on an applicable
operational failure supplies F4's disposition service
(:func:`openorc.services.task_operational_failure_triage.
triage_operational_failure`) with stable typed classification and independently
verified durable attempt/effect evidence. It implements no recovery policy of
its own: F4's dispositions are authoritative, the durably authorized retry is
never consumed here (F5c owns that consumption, F11 the wakeups), and the
original exception always propagates unchanged so the F1 sanitized unchained
worker-error report is preserved.

Applicability and classification are strictly type-based:

- The normalized Agent Runtime taxonomy and the external-operation
  application errors (``ExternalOperationFailedError`` with its
  ``ExternalOperationRejectedError`` subtype, ``ExternalOperationUncertainError``)
  are applicable; the precise ``OperationalFailureCode`` comes from the typed
  ``AgentRuntimeError`` cause chain the Level 1 services deliberately chain
  (type-only — no exception-text parsing, no SDK/provider-message
  persistence), falling back to the coarse application type and the mandatory
  ``unknown`` fallback.
- Workflow-semantic typed errors (stale/not-found/conflict/command/
  authorization/authentication/integration-configuration) are Level 1
  outcomes, not operational failures: they propagate without triage. Any
  other unexpected exception is the ``unknown`` fallback and is triaged only
  when the invocation's recorded evidence verifies coherently.

Attribution is invocation-local: a failure is reported only against the
durable attempt identity the failing invocation's own Level 1 services
recorded through :mod:`openorc.services.operation_attempt_evidence`, and only
after that identity's committed row verifies coherently (same scope, still
outstanding, not settled/succeeded/finalized). A failing invocation that
produced no durable identity — including a duplicate invocation failing
before its own reservation while another invocation's attempt is durably
outstanding — fails closed without triaging another invocation's work and
without fabricating an episode. Expected currentness is the Task state token
Level 1 itself established for the invocation (the collector's), never a
post-failure Task snapshot.

Direct Owner Abort, cost-limit stops, Cancel, ordinary gate outcomes, the
E7/E8/E10a GitHub-authority consequences (the typed workflow-semantic
refusals and the ``github_source_changed`` block — never the presentation's
own operational failures, which F5f #269 reports through the
``EXACT_HEAD_PR_AUTHORIZATION_PRESENTATION`` family), and the legacy
direct-block writers stay outside Level 2 here; #252 (F6) owns the Level 1
retrofit.

The supported v1 family map this coordinator hands to F4 (F6's retrofit
inventory keys off this):

- PLANNING_BOOTSTRAP_PREPARE / PLANNING_BOOTSTRAP_TRANSITION — original
  identity: the E1b planning-bootstrap claim token (one claim identity, two
  phase families); effect evidence: the failing runtime class's own
  pre-effect contract (no committed claim/execution failure fact exists for
  these phases).
- INITIAL_PLAN_DISPATCH / PLANNING_REVISE_DISPATCH / PR_COMPOSE_DISPATCH —
  original identity: the F2 task_runtime_send_claims row; effect evidence:
  the claim's committed outcome (``known_nondelivery`` with the
  ``runtime_pre_effect`` label is positive pre-effect proof; a conservatively
  claimed row stays unclassified).
- REVIEWER_REVIEW — original identity: the E3a/E9a reserved ReviewIteration;
  effect evidence: conservatively unclassified (no committed failure-kind
  fact).
- IMPLEMENTATION_EXECUTION / PR_REMEDIATION_EXECUTION — original identity:
  the E6/E9 Execution row; effect evidence: the Execution's committed status
  plus its ``execution_failed`` audit fact (the runtime-pre-effect label is
  positive pre-effect proof; a running attempt stays unclassified). A
  durably authorized retry's Execution reports the episode's original
  identity with ``PRIOR_AUTHORIZED`` and the bound attempt/effect row.
- EXACT_HEAD_MERGE (F5d, issue #267) — original identity: the episode's
  approved ``MERGE_DECISION`` gate UUID; the reported invocation identity is
  the F5d retry execution's own consumed attempt (its one-shot ``consumed_at``
  stamp is the durable binding — the merge family has no resulting
  Execution); effect evidence: conservatively unclassified
  (``EFFECT_UNCLASSIFIED`` — an unknown merge-send outcome never proves
  non-delivery). Retry eligibility is additionally gated on the merge
  primitive's send-phase subclass and the recognized transient transport
  cause in its chain, verified by F4 under its Task lock; a plain uncertain
  error is a pre-send condition and can never produce the transport code.
  The reporting service propagates a send uncertainty only after a
  successful fresh authoritative post-send reconciliation grounded the
  open/unmerged same-head observation — freshness of that observation is the
  reporting caller's binding obligation; an unavailable post-send read or a
  reconciled changed head is settled in-service and never reported here.
- EXACT_HEAD_PR_AUTHORIZATION_PRESENTATION (F5f, issue #269) — original
  identity: the durably finalized SUCCESSFUL E6 Execution UUID (expected for
  this family, never the execution families' SUCCEEDED contradiction, which
  stays exactly intact); effect evidence: structurally proven
  ``PRE_EFFECT_NOT_APPLIED`` — the presentation commits exactly one durable
  effect (the guarded final composed transaction whose Task-token rotation
  is observable), so the Task still implementing with no current gate
  proves the effect never applied. The reported invocation identity of a
  durably authorized re-presentation is its own consumed attempt (the
  one-shot ``consumed_at`` stamp is the durable binding — the presentation
  family has no resulting Execution), addressed to the episode's original
  Execution identity with ``PRIOR_AUTHORIZED``. The retry-eligible code is
  ONLY the recognized transient GitHub transport subclass in the B3/B7 read
  error's bounded cause chain; every other uncertain outcome and every
  known provider failure keeps its escalating code.
"""

from __future__ import annotations

import logging
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from uuid import UUID

from openorc.adapters.agent_runtime.errors import (
    AgentRuntimeConfigurationRejectedError,
    AgentRuntimeDeliveryUncertainError,
    AgentRuntimeError,
    AgentRuntimeProtocolFailureError,
    AgentRuntimeTimeoutError,
    AgentRuntimeTransportError,
    AgentRuntimeUnavailableError,
    AgentSessionNotFoundError,
)
from openorc.adapters.github import (
    GitHubOutcomeUncertainError,
    GitHubTransportUncertainError,
)
from openorc.domain.executions import ExecutionStatus
from openorc.domain.operational_failures import (
    FailureEffectEvidence,
    GitHubMergeOperation,
    OperationalFailureCode,
    OperationalFailureFamily,
    OperationalFailureSourceKind,
    OriginalAttemptKind,
    PRAuthorizationPresentationOperation,
    canonical_failure_subject,
)
from openorc.domain.pending_interactions import PendingTaskRuntimeInteractionOperation
from openorc.domain.send_claims import TaskRuntimeSendClaimKind, TaskRuntimeSendClaimStatus
from openorc.domain.tasks import TaskPlanningBootstrapStage, TaskStatus
from openorc.observability import annotate_span, application_span
from openorc.persistence import executions as execution_records
from openorc.persistence import operational_failures as failure_records
from openorc.persistence import reviews as review_records
from openorc.persistence import send_claims as send_claim_records
from openorc.persistence import tasks as task_records
from openorc.persistence.pool import DatabasePool
from openorc.services.errors import (
    ApplicationError,
    ConflictError,
    ExternalOperationFailedError,
    ExternalOperationRejectedError,
    ExternalOperationUncertainError,
)
from openorc.services.event_coordination import owner_actor
from openorc.services.operation_attempt_evidence import (
    InvocationAttemptEvidence,
    ProducedAttemptIdentity,
)
from openorc.services.pull_request_merge import MergeRequestSendUncertainError
from openorc.services.task_operational_failure_triage import (
    OperationalFailureDisposition,
    OperationalFailureOperation,
    OperationalFailureReport,
    execution_failure_audit_kind,
    triage_operational_failure,
)

__all__ = ["coordinate_task_workflow_operation"]

logger = logging.getLogger(__name__)

_TRACER_SCOPE = "openorc.services.task_workflow_operation_coordinator"
_COORDINATION_SPAN_NAME = "task_workflow_operation_coordinator.report_failure"

# The sanitized machine failure-kind label the Level 1 seams record for a
# positively proven pre-effect runtime failure (F2 send claims and the E6/E9
# execution seams): the committed fact the pre-effect evidence class requires.
_RUNTIME_PRE_EFFECT_FAILURE_KIND = "runtime_pre_effect"

# The bounded walk over the failing error's typed cause chain. The Level 1
# services chain the normalized ``AgentRuntimeError`` as the explicit cause of
# the raised application error; the walk is type-only and bounded.
_MAX_CAUSE_CHAIN_NODES = 8

# The type classification of the normalized runtime taxonomy. Subtype checks
# run in declaration order, so the precise uncertain subtypes classify before
# any broader member.
_RUNTIME_ERROR_CODES: tuple[tuple[type[AgentRuntimeError], OperationalFailureCode], ...] = (
    (AgentRuntimeConfigurationRejectedError, OperationalFailureCode.RUNTIME_CONFIGURATION_REJECTED),
    (AgentRuntimeUnavailableError, OperationalFailureCode.RUNTIME_UNAVAILABLE),
    (AgentRuntimeTimeoutError, OperationalFailureCode.RUNTIME_TIMEOUT),
    (AgentRuntimeDeliveryUncertainError, OperationalFailureCode.RUNTIME_DELIVERY_UNCERTAIN),
    (AgentSessionNotFoundError, OperationalFailureCode.AGENT_SESSION_LOST),
    (AgentRuntimeTransportError, OperationalFailureCode.RUNTIME_TRANSPORT),
    (AgentRuntimeProtocolFailureError, OperationalFailureCode.RUNTIME_PROTOCOL),
)

# The runtime classes whose own contracts positively establish that the
# operation failed before any effect (the F2 proven pre-effect set): the
# bootstrap families' evidence class may rest on them because no committed
# claim/execution failure fact exists for those phases.
_PROVEN_PRE_EFFECT_RUNTIME_TYPES = (
    AgentRuntimeConfigurationRejectedError,
    AgentRuntimeUnavailableError,
    AgentSessionNotFoundError,
)

# The F2 send-claim kind each dispatch family's original identity resolves to
# (mirrors the triage service's own family/claim-kind pairing). The keys are
# runtime families only; the widened annotation is the Level 2 family union
# so the resolution reads against the closed union type.
_FAMILY_CLAIM_KINDS: Mapping[OperationalFailureFamily, TaskRuntimeSendClaimKind] = {
    PendingTaskRuntimeInteractionOperation.INITIAL_PLAN_DISPATCH: (
        TaskRuntimeSendClaimKind.INITIAL_PLAN_DISPATCH
    ),
    PendingTaskRuntimeInteractionOperation.PLANNING_REVISE_DISPATCH: (
        TaskRuntimeSendClaimKind.PLANNING_REVISE_DISPATCH
    ),
    PendingTaskRuntimeInteractionOperation.PR_COMPOSE_DISPATCH: (
        TaskRuntimeSendClaimKind.PR_COMPOSE_DISPATCH
    ),
}


def _runtime_error_in_chain(error: BaseException) -> AgentRuntimeError | None:
    """Return the nearest normalized runtime error in the typed cause chain.

    The walk follows explicit ``__cause__`` links before implicit
    ``__context__`` links, is bounded, and never inspects message text.
    """
    visited: set[int] = set()
    queue: list[tuple[BaseException, int]] = [(error, 0)]
    while queue:
        current, depth = queue.pop(0)
        if id(current) in visited or depth > _MAX_CAUSE_CHAIN_NODES:
            continue
        visited.add(id(current))
        if isinstance(current, AgentRuntimeError):
            return current
        cause = current.__cause__
        if cause is not None:
            queue.append((cause, depth + 1))
        context = current.__context__
        if context is not None:
            queue.append((context, depth + 1))
    return None


def _classify_failure_code(error: BaseException) -> OperationalFailureCode:
    """Classify the failing operation's stable error code by type only.

    The typed normalized runtime classification in the cause chain wins (it
    is the precise machine fact the Level 1 services deliberately chain); the
    caught application error type is the fallback, and anything else is the
    mandatory ``unknown`` member.
    """
    runtime_error = _runtime_error_in_chain(error)
    if runtime_error is not None:
        for runtime_type, code in _RUNTIME_ERROR_CODES:
            if isinstance(runtime_error, runtime_type):
                return code
    if isinstance(error, ExternalOperationRejectedError):
        return OperationalFailureCode.EXTERNAL_OPERATION_REJECTED
    if isinstance(error, ExternalOperationFailedError):
        return OperationalFailureCode.EXTERNAL_OPERATION_FAILED
    if isinstance(error, ExternalOperationUncertainError):
        return OperationalFailureCode.EXTERNAL_OPERATION_UNCERTAIN
    return OperationalFailureCode.UNKNOWN


def _is_applicable_operational_failure(error: BaseException) -> bool:
    """Whether the caught error class is triage-applicable at all.

    The normalized runtime taxonomy and the external-operation application
    errors are the operational-failure classes. Every other
    ``ApplicationError`` is a Level 1 workflow-semantic outcome (stale,
    not-found, conflict, command shape, authorization, integration
    configuration) that propagates without triage. Any other exception is the
    ``unknown`` fallback and is applicable only when the invocation's
    recorded evidence verifies coherently.
    """
    if isinstance(
        error,
        (AgentRuntimeError, ExternalOperationFailedError, ExternalOperationUncertainError),
    ):
        return True
    return not isinstance(error, ApplicationError)


def _canonical_operation_subject(identifiers: dict[str, object]) -> Mapping[str, object]:
    """Build one family subject through F4's own canonicalization contract.

    Every identifier is normalized to its canonical JSON representation (a
    UUID is never a JSON-representable value) and the mapping passes the
    domain canonicalizer the triage service validates against, so a subject
    that could not reach F4 fails here — inside the coordinator's
    best-effort handoff boundary — instead of inside the disposition
    service.
    """
    normalized = {key: str(value) for key, value in identifiers.items() if value is not None}
    return canonical_failure_subject(normalized)


def _diagnostic_context(
    identity: ProducedAttemptIdentity, error: BaseException
) -> dict[str, object]:
    """The bounded sanitized machine context passed with the report.

    Safe classification facts only — the error type name and the Level 1
    phase label. Never exception text, provider messages, or any customer
    content.
    """
    return {
        "error_type": type(error).__name__,
        "phase": identity.phase,
    }


@dataclass(frozen=True, slots=True)
class _ResolvedFailureReport:
    """The verified typed command pair handed to the triage service."""

    operation: OperationalFailureOperation
    report: OperationalFailureReport


def _bootstrap_effect_evidence(error: BaseException) -> FailureEffectEvidence:
    """Classify one bootstrap-phase failure's effect evidence.

    The bootstrap phases commit no claim/execution failure-kind fact of their
    own, so the evidence class rests on the failing runtime error's own
    contract: a positively proven pre-effect runtime class (rejected
    credentials, unreachable runtime, not-live addressed session) is the
    positive service-phase evidence; every other outcome stays conservatively
    effect-uncertain.
    """
    runtime_error = _runtime_error_in_chain(error)
    if runtime_error is not None and isinstance(runtime_error, _PROVEN_PRE_EFFECT_RUNTIME_TYPES):
        return FailureEffectEvidence.PRE_EFFECT_NOT_APPLIED
    return FailureEffectEvidence.EFFECT_UNCLASSIFIED


def _resolve_bootstrap_report(
    pool: DatabasePool,
    *,
    workspace_id: UUID,
    task_id: UUID,
    identity: ProducedAttemptIdentity,
    error: BaseException,
) -> _ResolvedFailureReport | None:
    """Verify and enrich a planning-bootstrap family report.

    The committed evidence is the Task's bootstrap claim/stage row: the
    reported claim token must be the Task's own claim, and the settled stage
    must still match the reported family (stage unset for PREPARE, PREPARED
    for the planning-context TRANSITION). A moved-past stage or a foreign
    claim identity fails closed without triaging.
    """
    evidence = task_records.get_task_planning_bootstrap_evidence(pool, task_id=task_id)
    if evidence is None or evidence.claim_token != identity.original_attempt_id:
        return None
    expected_stage = (
        None
        if identity.family is PendingTaskRuntimeInteractionOperation.PLANNING_BOOTSTRAP_PREPARE
        else TaskPlanningBootstrapStage.PREPARED
    )
    if evidence.stage != expected_stage:
        return None
    operation = OperationalFailureOperation(
        family=identity.family,
        original_attempt_kind=identity.original_attempt_kind,
        original_attempt_id=identity.original_attempt_id,
        subject=_canonical_operation_subject({"workspace_id": workspace_id, "task_id": task_id}),
    )
    return _ResolvedFailureReport(
        operation=operation,
        report=OperationalFailureReport(
            reporting_attempt_kind=OperationalFailureSourceKind.ORIGINAL,
            reporting_attempt_id=identity.original_attempt_id,
            error_code=_classify_failure_code(error),
            effect_evidence=_bootstrap_effect_evidence(error),
            diagnostic_context=_diagnostic_context(identity, error),
        ),
    )


def _resolve_send_claim_report(
    pool: DatabasePool,
    *,
    workspace_id: UUID,
    task_id: UUID,
    identity: ProducedAttemptIdentity,
    error: BaseException,
) -> _ResolvedFailureReport | None:
    """Verify and enrich one F2 send-claim family report.

    The committed evidence is the claim row itself: it must exist in the
    addressed scope with the family's exact claim kind. A settled claim
    contradicts any failure report (the operation completed) and fails
    closed. The claim's committed ``known_nondelivery`` outcome carrying the
    Level 1 runtime-pre-effect label is the positive durable fact for the
    pre-effect evidence class; a conservatively claimed row stays
    effect-uncertain.
    """
    claim = send_claim_records.get_task_runtime_send_claim(
        pool, claim_id=identity.original_attempt_id
    )
    if claim is None or claim.workspace_id != workspace_id or claim.task_id != task_id:
        return None
    expected_kind = _FAMILY_CLAIM_KINDS.get(identity.family)
    if expected_kind is None or claim.kind is not expected_kind:
        return None
    if claim.status is TaskRuntimeSendClaimStatus.SETTLED:
        return None
    if claim.status is TaskRuntimeSendClaimStatus.KNOWN_NON_DELIVERY:
        recorded_kind = claim.outcome.get("failure_kind")
        effect_evidence = (
            FailureEffectEvidence.PRE_EFFECT_NOT_APPLIED
            if recorded_kind == _RUNTIME_PRE_EFFECT_FAILURE_KIND
            else FailureEffectEvidence.KNOWN_FAILED
        )
    else:
        # Conservatively claimed: every uncertain or post-effect-capable
        # outcome leaves the claim claimed, so the effect stays unclassified.
        effect_evidence = FailureEffectEvidence.EFFECT_UNCLASSIFIED
    subject_identifiers: dict[str, object] = {"workspace_id": workspace_id, "task_id": task_id}
    if claim.review_iteration_id is not None:
        subject_identifiers["review_iteration_id"] = claim.review_iteration_id
    if claim.owner_gate_id is not None:
        subject_identifiers["owner_gate_id"] = claim.owner_gate_id
    if claim.subject_head_sha is not None:
        subject_identifiers["subject_head_sha"] = claim.subject_head_sha
    operation = OperationalFailureOperation(
        family=identity.family,
        original_attempt_kind=identity.original_attempt_kind,
        original_attempt_id=identity.original_attempt_id,
        subject=_canonical_operation_subject(subject_identifiers),
    )
    return _ResolvedFailureReport(
        operation=operation,
        report=OperationalFailureReport(
            reporting_attempt_kind=OperationalFailureSourceKind.ORIGINAL,
            reporting_attempt_id=identity.original_attempt_id,
            error_code=_classify_failure_code(error),
            effect_evidence=effect_evidence,
            diagnostic_context=_diagnostic_context(identity, error),
        ),
    )


def _resolve_review_iteration_report(
    pool: DatabasePool,
    *,
    workspace_id: UUID,
    task_id: UUID,
    identity: ProducedAttemptIdentity,
    error: BaseException,
) -> _ResolvedFailureReport | None:
    """Verify and enrich one reserved-ReviewIteration dispatch report.

    The committed evidence is the reserved iteration row: it must exist in
    the addressed scope and still be unfinalized — a finalized outcome
    contradicts the failure report (the round resolved). The v1 iteration
    families carry no committed failure-kind fact of their own, so the
    effect evidence stays conservatively unclassified.
    """
    iteration = review_records.get_review_iteration(
        pool, review_iteration_id=identity.original_attempt_id
    )
    if iteration is None or iteration.workspace_id != workspace_id or iteration.task_id != task_id:
        return None
    if iteration.outcome is not None:
        return None
    subject_identifiers: dict[str, object] = {
        "workspace_id": workspace_id,
        "task_id": task_id,
        "review_iteration_id": iteration.id,
    }
    if iteration.plan_revision_id is not None:
        subject_identifiers["plan_revision_id"] = iteration.plan_revision_id
    if iteration.task_pull_request_id is not None:
        subject_identifiers["task_pull_request_id"] = iteration.task_pull_request_id
    operation = OperationalFailureOperation(
        family=identity.family,
        original_attempt_kind=identity.original_attempt_kind,
        original_attempt_id=identity.original_attempt_id,
        subject=_canonical_operation_subject(subject_identifiers),
    )
    return _ResolvedFailureReport(
        operation=operation,
        report=OperationalFailureReport(
            reporting_attempt_kind=OperationalFailureSourceKind.ORIGINAL,
            reporting_attempt_id=identity.original_attempt_id,
            error_code=_classify_failure_code(error),
            effect_evidence=FailureEffectEvidence.EFFECT_UNCLASSIFIED,
            diagnostic_context=_diagnostic_context(identity, error),
        ),
    )


def _execution_effect_evidence(
    pool: DatabasePool, *, workspace_id: UUID, execution_id: UUID, status: ExecutionStatus
) -> FailureEffectEvidence:
    """Classify the committed outcome facts of one failed Execution attempt.

    The ``execution_failed`` audit event of the exact attempt carrying the
    Level 1 runtime-pre-effect label is the positive durable fact for the
    pre-effect class — found by scanning the attempt's bounded audit window
    (the shared ``execution_failure_audit_kind`` lookup), never by assuming
    the newest subject event is the failure; a conservatively running
    attempt stays effect-uncertain; any other finalized non-success is a
    known failure without positive pre-effect proof.
    """
    if status is ExecutionStatus.FAILED_TRANSIENT:
        kind = execution_failure_audit_kind(
            pool, workspace_id=workspace_id, execution_id=execution_id
        )
        if kind == _RUNTIME_PRE_EFFECT_FAILURE_KIND:
            return FailureEffectEvidence.PRE_EFFECT_NOT_APPLIED
        return FailureEffectEvidence.KNOWN_FAILED
    if status is ExecutionStatus.RUNNING:
        return FailureEffectEvidence.EFFECT_UNCLASSIFIED
    return FailureEffectEvidence.KNOWN_FAILED


def _resolve_execution_report(
    pool: DatabasePool,
    *,
    workspace_id: UUID,
    task_id: UUID,
    identity: ProducedAttemptIdentity,
    error: BaseException,
) -> _ResolvedFailureReport | None:
    """Verify and enrich one execution-family report.

    The committed evidence is the Execution row and its ``execution_failed``
    audit event. A succeeded attempt contradicts any failure report. When the
    recorded Execution is a durably authorized retry's resulting Execution
    (bound through the F4 attempt's set-once binding), the report retains the
    episode's original attempt identity and reports the authorized attempt as
    the reporting source with its exact bound effect row; otherwise the
    recorded Execution is the original attempt itself.
    """
    execution = execution_records.get_execution(pool, execution_id=identity.original_attempt_id)
    if execution is None or execution.workspace_id != workspace_id or execution.task_id != task_id:
        return None
    if execution.status is ExecutionStatus.SUCCEEDED:
        return None
    bound_attempt = failure_records.find_operational_failure_attempt_by_resulting_execution(
        pool, resulting_execution_id=execution.id
    )
    if bound_attempt is not None:
        if bound_attempt.workspace_id != workspace_id or bound_attempt.task_id != task_id:
            return None
        episode = failure_records.get_operational_failure_episode(
            pool, episode_id=bound_attempt.episode_id
        )
        if (
            episode is None
            or episode.workspace_id != workspace_id
            or episode.task_id != task_id
            or episode.operation_family is not identity.family
            or episode.original_attempt_kind is not identity.original_attempt_kind
        ):
            return None
        operation = OperationalFailureOperation(
            family=episode.operation_family,
            original_attempt_kind=episode.original_attempt_kind,
            original_attempt_id=episode.original_attempt_id,
            subject=dict(episode.subject),
        )
        reporting_kind = OperationalFailureSourceKind.PRIOR_AUTHORIZED
        reporting_attempt_id = bound_attempt.id
        effect_attempt_id: UUID | None = execution.id
    else:
        subject_identifiers: dict[str, object] = {
            "workspace_id": workspace_id,
            "task_id": task_id,
        }
        if execution.review_iteration_id is not None:
            subject_identifiers["review_iteration_id"] = execution.review_iteration_id
        operation = OperationalFailureOperation(
            family=identity.family,
            original_attempt_kind=identity.original_attempt_kind,
            original_attempt_id=identity.original_attempt_id,
            subject=_canonical_operation_subject(subject_identifiers),
        )
        reporting_kind = OperationalFailureSourceKind.ORIGINAL
        reporting_attempt_id = identity.original_attempt_id
        effect_attempt_id = None
    effect_evidence = _execution_effect_evidence(
        pool,
        workspace_id=workspace_id,
        execution_id=execution.id,
        status=execution.status,
    )
    return _ResolvedFailureReport(
        operation=operation,
        report=OperationalFailureReport(
            reporting_attempt_kind=reporting_kind,
            reporting_attempt_id=reporting_attempt_id,
            error_code=_classify_failure_code(error),
            effect_evidence=effect_evidence,
            effect_attempt_id=effect_attempt_id,
            diagnostic_context=_diagnostic_context(identity, error),
        ),
    )


def _merge_retry_failure_code(error: BaseException) -> OperationalFailureCode:
    """The merge family's send-phase error code, classified type-only (F5d).

    The recognized transient transport subtype in the send error's bounded
    cause chain is the retry-eligible code; every other uncertain send
    outcome is the unsupported-uncertainty code. Only the merge primitive's
    send-phase subclass can reach the transport classification: a plain
    uncertain error is a pre-send read/credential condition and is never
    merge-send evidence. Non-uncertain applicable failures (unexpected
    exceptions, the ``unknown`` fallback) keep the generic type-only
    classification.
    """
    if not isinstance(error, ExternalOperationUncertainError):
        return _classify_failure_code(error)
    current: BaseException | None = error
    for _ in range(_MAX_CAUSE_CHAIN_NODES + 1):
        if current is None:
            break
        if isinstance(current, GitHubTransportUncertainError):
            return OperationalFailureCode.EXTERNAL_TRANSPORT_UNAVAILABLE
        if isinstance(current, GitHubOutcomeUncertainError):
            return OperationalFailureCode.EXTERNAL_OPERATION_UNCERTAIN
        current = current.__cause__
    return OperationalFailureCode.EXTERNAL_OPERATION_UNCERTAIN


def _pr_authorization_failure_code(error: BaseException) -> OperationalFailureCode:
    """The presentation family's B3/B7 read-phase code, classified type-only (F5f).

    ONLY the GitHub adapter's narrow transient-transport subclass in the
    read error's bounded cause chain classifies as the retry-eligible
    ``EXTERNAL_TRANSPORT_UNAVAILABLE`` code — a connection-level transport
    loss provably applies no durable effect and is the one narrowly typed
    transient-network classification this family admits. Every other
    uncertain outcome (an interpretable-response ``GitHubOutcomeUncertainError``,
    e.g. a 5xx read) keeps the unsupported-uncertainty code, and every known
    provider failure keeps its coarse known-failure code: F4 escalates
    both. Non-uncertain applicable failures keep the generic type-only
    classification.
    """
    if not isinstance(error, ExternalOperationUncertainError):
        return _classify_failure_code(error)
    current: BaseException | None = error
    for _ in range(_MAX_CAUSE_CHAIN_NODES + 1):
        if current is None:
            break
        if isinstance(current, GitHubTransportUncertainError):
            return OperationalFailureCode.EXTERNAL_TRANSPORT_UNAVAILABLE
        current = current.__cause__
    return OperationalFailureCode.EXTERNAL_OPERATION_UNCERTAIN


def _resolve_merge_retry_report(
    pool: DatabasePool,
    *,
    workspace_id: UUID,
    task_id: UUID,
    identity: ProducedAttemptIdentity,
    error: BaseException,
) -> _ResolvedFailureReport | None:
    """Resolve the F5d merge-retry invocation's consumed-attempt identity.

    The recorded identity is the F5d retry execution's own consumed F4 merge
    attempt: its one-shot ``consumed_at`` stamp is the durable binding a
    report must prove (the merge family has no resulting Execution), and the
    recorded effect identity must be the attempt's own UUID. The reporting
    service reaches this handoff only after a successful fresh authoritative
    post-send reconciliation grounded the open/unmerged same-head observation
    (an unavailable read or a changed head is settled in-service, never
    reported) — so a retry-eligible code here stands on that fresh
    observation. The report addresses the episode's original identity — the
    approved ``MERGE_DECISION`` gate UUID — with ``PRIOR_AUTHORIZED``; the
    triage service re-proves the consumption, the episode's budget, and the
    merge subject's durable authority under its Task lock. Any incoherent
    recorded identity (foreign scope, wrong family/kind, unconsumed attempt,
    mismatched effect) fails closed without triaging.
    """
    if (
        identity.original_attempt_kind is not OriginalAttemptKind.MERGE_DECISION_GATE
        or identity.effect_attempt_id is None
        or identity.effect_attempt_id != identity.original_attempt_id
    ):
        return None
    attempt = failure_records.get_operational_failure_attempt(
        pool, attempt_id=identity.original_attempt_id
    )
    if attempt is None or attempt.workspace_id != workspace_id or attempt.task_id != task_id:
        return None
    if attempt.consumed_at is None:
        # Only the failure of an actually consumed attempt is reportable.
        return None
    episode = failure_records.get_operational_failure_episode(pool, episode_id=attempt.episode_id)
    if (
        episode is None
        or episode.workspace_id != workspace_id
        or episode.task_id != task_id
        or episode.operation_family is not GitHubMergeOperation.EXACT_HEAD_MERGE
        or episode.original_attempt_kind is not OriginalAttemptKind.MERGE_DECISION_GATE
    ):
        return None
    return _ResolvedFailureReport(
        operation=OperationalFailureOperation(
            family=episode.operation_family,
            original_attempt_kind=episode.original_attempt_kind,
            original_attempt_id=episode.original_attempt_id,
            subject=episode.subject,
        ),
        report=OperationalFailureReport(
            reporting_attempt_kind=OperationalFailureSourceKind.PRIOR_AUTHORIZED,
            reporting_attempt_id=attempt.id,
            error_code=_merge_retry_failure_code(error),
            effect_evidence=FailureEffectEvidence.EFFECT_UNCLASSIFIED,
            effect_attempt_id=attempt.id,
            diagnostic_context={
                "error_type": type(error).__name__,
                "phase": identity.phase,
                "merge_mutation_attempted": isinstance(error, MergeRequestSendUncertainError),
            },
        ),
    )


def _resolve_pr_authorization_presentation_report(
    pool: DatabasePool,
    *,
    workspace_id: UUID,
    task_id: UUID,
    identity: ProducedAttemptIdentity,
    error: BaseException,
) -> _ResolvedFailureReport | None:
    """Verify and enrich one exact-head presentation report (F5f, issue #269).

    The episode roots at the durably finalized SUCCESSFUL E6 Execution —
    expected for this family and never the execution families' SUCCEEDED
    contradiction, which stays exactly intact for them. Two report shapes:

    - ORIGINAL (the initial or authorized-retry composition's failing
      presentation): the recorded identity is the qualifying Execution; the
      committed evidence is that Execution row (same scope, SUCCEEDED,
      implementation-attempted) plus the Task still ``implementing`` with no
      current gate. That state structurally proves
      ``PRE_EFFECT_NOT_APPLIED``: the presentation commits exactly one
      durable effect — the guarded final composed transaction whose Task
      token rotates and whose gate becomes current — so the pre-gate state
      proves the effect never applied.
    - PRIOR_AUTHORIZED (a consumed F4-authorized re-presentation): the
      recorded identity is the consumed attempt (its one-shot
      ``consumed_at`` stamp is the durable binding — the presentation family
      has no resulting Execution); the report addresses the episode's
      original Execution identity with the consumed attempt as source, and
      the same structural pre-effect proof is re-derived.

    Any incoherent recorded identity (foreign scope, non-succeeded or
    remediation execution, moved-on Task, current gate, unconsumed attempt,
    mismatched effect) fails closed without triaging.
    """
    if identity.original_attempt_kind is not OriginalAttemptKind.EXECUTION:
        return None
    if (
        identity.effect_attempt_id is not None
        and identity.effect_attempt_id == identity.original_attempt_id
    ):
        # The consumed-attempt identity shape (mirrors the merge family):
        # the identity names the F4-authorized re-presentation attempt itself.
        return _resolve_pr_authorization_retry_report(
            pool, workspace_id=workspace_id, task_id=task_id, identity=identity, error=error
        )
    execution = execution_records.get_execution(pool, execution_id=identity.original_attempt_id)
    if execution is None or execution.workspace_id != workspace_id or execution.task_id != task_id:
        return None
    if execution.status is not ExecutionStatus.SUCCEEDED:
        # A non-succeeded execution is the execution families' subject; a
        # presentation report about it is incoherent and their SUCCEEDED
        # contradiction check stays untouched by this family.
        return None
    if execution.review_iteration_id is not None:
        # A PR-remediation execution never roots a presentation episode.
        return None
    task = task_records.get_task(pool, task_id=task_id)
    if task is None or task.workspace_id != workspace_id:
        return None
    if task.status is not TaskStatus.IMPLEMENTING or task.current_owner_gate_id is not None:
        # The presentation effect applied, or the workflow moved on: the
        # report no longer addresses outstanding pre-gate work.
        return None
    subject_identifiers: dict[str, object] = {
        "workspace_id": workspace_id,
        "task_id": task_id,
        "execution_id": execution.id,
    }
    return _ResolvedFailureReport(
        operation=OperationalFailureOperation(
            family=identity.family,
            original_attempt_kind=identity.original_attempt_kind,
            original_attempt_id=identity.original_attempt_id,
            subject=_canonical_operation_subject(subject_identifiers),
        ),
        report=OperationalFailureReport(
            reporting_attempt_kind=OperationalFailureSourceKind.ORIGINAL,
            reporting_attempt_id=identity.original_attempt_id,
            error_code=_pr_authorization_failure_code(error),
            effect_evidence=FailureEffectEvidence.PRE_EFFECT_NOT_APPLIED,
            effect_attempt_id=None,
            diagnostic_context=_diagnostic_context(identity, error),
        ),
    )


def _resolve_pr_authorization_retry_report(
    pool: DatabasePool,
    *,
    workspace_id: UUID,
    task_id: UUID,
    identity: ProducedAttemptIdentity,
    error: BaseException,
) -> _ResolvedFailureReport | None:
    """Resolve the F5f re-presentation invocation's consumed-attempt identity.

    The recorded identity is the F4-authorized presentation attempt: its
    one-shot ``consumed_at`` stamp is the durable binding a report must
    prove (the presentation family has no resulting Execution), and the
    recorded effect identity must be the attempt's own UUID. The episode
    must be this family rooted at a still-SUCCEEDED same-scope Execution,
    and the Task must still be in the pre-gate ``implementing`` state with
    no current gate — the structural pre-effect proof for the failed
    re-presentation. Any incoherence fails closed without triaging.
    """
    attempt = failure_records.get_operational_failure_attempt(
        pool, attempt_id=identity.original_attempt_id
    )
    if attempt is None or attempt.workspace_id != workspace_id or attempt.task_id != task_id:
        return None
    if attempt.consumed_at is None:
        # Only the failure of an actually consumed attempt is reportable.
        return None
    episode = failure_records.get_operational_failure_episode(pool, episode_id=attempt.episode_id)
    if (
        episode is None
        or episode.workspace_id != workspace_id
        or episode.task_id != task_id
        or episode.operation_family
        is not PRAuthorizationPresentationOperation.EXACT_HEAD_PR_AUTHORIZATION_PRESENTATION
        or episode.original_attempt_kind is not OriginalAttemptKind.EXECUTION
    ):
        return None
    execution = execution_records.get_execution(pool, execution_id=episode.original_attempt_id)
    if execution is None or execution.workspace_id != workspace_id or execution.task_id != task_id:
        return None
    if execution.status is not ExecutionStatus.SUCCEEDED:
        return None
    task = task_records.get_task(pool, task_id=task_id)
    if task is None or task.workspace_id != workspace_id:
        return None
    if task.status is not TaskStatus.IMPLEMENTING or task.current_owner_gate_id is not None:
        return None
    return _ResolvedFailureReport(
        operation=OperationalFailureOperation(
            family=episode.operation_family,
            original_attempt_kind=episode.original_attempt_kind,
            original_attempt_id=episode.original_attempt_id,
            subject=episode.subject,
        ),
        report=OperationalFailureReport(
            reporting_attempt_kind=OperationalFailureSourceKind.PRIOR_AUTHORIZED,
            reporting_attempt_id=attempt.id,
            error_code=_pr_authorization_failure_code(error),
            effect_evidence=FailureEffectEvidence.PRE_EFFECT_NOT_APPLIED,
            effect_attempt_id=attempt.id,
            diagnostic_context={
                "error_type": type(error).__name__,
                "phase": identity.phase,
                "presentation_retry_attempt_consumed": True,
            },
        ),
    )


def _resolve_failure_report(
    pool: DatabasePool,
    *,
    workspace_id: UUID,
    task_id: UUID,
    identity: ProducedAttemptIdentity,
    error: BaseException,
) -> _ResolvedFailureReport | None:
    """Dispatch to the recorded family's verification and enrichment."""
    family = identity.family
    if family in (
        PendingTaskRuntimeInteractionOperation.PLANNING_BOOTSTRAP_PREPARE,
        PendingTaskRuntimeInteractionOperation.PLANNING_BOOTSTRAP_TRANSITION,
    ):
        return _resolve_bootstrap_report(
            pool, workspace_id=workspace_id, task_id=task_id, identity=identity, error=error
        )
    if family in (
        PendingTaskRuntimeInteractionOperation.INITIAL_PLAN_DISPATCH,
        PendingTaskRuntimeInteractionOperation.PLANNING_REVISE_DISPATCH,
        PendingTaskRuntimeInteractionOperation.PR_COMPOSE_DISPATCH,
    ):
        return _resolve_send_claim_report(
            pool, workspace_id=workspace_id, task_id=task_id, identity=identity, error=error
        )
    if family is PendingTaskRuntimeInteractionOperation.REVIEWER_REVIEW:
        return _resolve_review_iteration_report(
            pool, workspace_id=workspace_id, task_id=task_id, identity=identity, error=error
        )
    if family in (
        PendingTaskRuntimeInteractionOperation.IMPLEMENTATION_EXECUTION,
        PendingTaskRuntimeInteractionOperation.PR_REMEDIATION_EXECUTION,
    ):
        return _resolve_execution_report(
            pool, workspace_id=workspace_id, task_id=task_id, identity=identity, error=error
        )
    if family is GitHubMergeOperation.EXACT_HEAD_MERGE:
        return _resolve_merge_retry_report(
            pool, workspace_id=workspace_id, task_id=task_id, identity=identity, error=error
        )
    if family is PRAuthorizationPresentationOperation.EXACT_HEAD_PR_AUTHORIZATION_PRESENTATION:
        return _resolve_pr_authorization_presentation_report(
            pool, workspace_id=workspace_id, task_id=task_id, identity=identity, error=error
        )
    # The recorded identity names no supported v1 family: fail closed.
    return None


def coordinate_task_workflow_operation(
    pool: DatabasePool,
    *,
    expected_status: TaskStatus,
    workspace_id: UUID,
    task_id: UUID,
    profile_id: UUID,
    expected_state_token: UUID,
    invoke_level_one: Callable[[InvocationAttemptEvidence], None],
    triage: Callable[..., OperationalFailureDisposition] | None = None,
) -> None:
    """Run one Level 1 Task-workflow operation with the typed failure handoff.

    On ordinary success this is a transparent passthrough. On an applicable
    operational failure the coordinator reports the invocation's own
    Level 1-produced attempt identity — verified against its committed
    durable row — to the F4 disposition service exactly once, then re-raises
    the original exception unchanged. A failing invocation that recorded no
    identity, an incoherent one, or a workflow-semantic error type fails
    closed with no triage call.

    ``expected_status`` is the coarse Task status the operation addressed;
    ``expected_state_token`` is the invocation payload's currentness seed,
    which Level 1's own authoritative rotations (recorded at their commit
    points) supersede in the collector. ``triage`` is injectable for
    fake-backed tests and defaults to the real F4 disposition service.
    """
    evidence = InvocationAttemptEvidence()
    evidence.record_task_token(expected_state_token)
    try:
        invoke_level_one(evidence)
        return
    except Exception as error:  # noqa: BLE001 - contained with safe classification
        if not _is_applicable_operational_failure(error):
            raise
        identity = evidence.latest_identity
        if identity is None:
            # The failing invocation produced no durable attempt identity of
            # its own: another invocation's outstanding work is never
            # attributed to it and no episode is fabricated.
            logger.warning(
                "task-workflow operation failed without a Level 1-produced "
                "attempt identity; the failure is not triaged (error type %s)",
                type(error).__name__,
            )
            raise
        current_token = evidence.current_task_token
        if current_token is None:
            # Unreachable: the coordinator seeds the token before Level 1
            # runs. Fail closed rather than report without currentness.
            raise ConflictError(
                "the failing invocation established no current Task token"
            ) from error
        triage_callable = triage if triage is not None else triage_operational_failure
        with application_span(_TRACER_SCOPE, _COORDINATION_SPAN_NAME) as span:
            annotate_span(
                span,
                operation=_COORDINATION_SPAN_NAME,
                workspace_id=str(workspace_id),
                task_id=str(task_id),
            )
            resolved: _ResolvedFailureReport | None = None
            try:
                resolved = _resolve_failure_report(
                    pool,
                    workspace_id=workspace_id,
                    task_id=task_id,
                    identity=identity,
                    error=error,
                )
            except Exception as resolution_error:  # noqa: BLE001 - best-effort handoff
                # Evidence resolution never replaces the original Level 1
                # failure: without verified evidence nothing is triaged, and
                # the original exception propagates unchanged below.
                logger.warning(
                    "the attempt-evidence resolution did not complete; "
                    "the failure is not triaged (error type %s)",
                    type(resolution_error).__name__,
                )
            else:
                if resolved is None:
                    # The recorded identity does not verify against its
                    # committed durable row (foreign scope, settled,
                    # succeeded, finalized, or moved past the reported
                    # family): fail closed without triaging.
                    logger.warning(
                        "task-workflow operation's recorded attempt identity did not "
                        "verify against durable state; the failure is not triaged "
                        "(family=%s, error type %s)",
                        identity.family.value,
                        type(error).__name__,
                    )
            if resolved is None:
                raise
            disposition: OperationalFailureDisposition | None = None
            try:
                disposition = triage_callable(
                    pool,
                    workspace_id=workspace_id,
                    task_id=task_id,
                    expected_state_token=current_token,
                    expected_status=expected_status,
                    expected_current_owner_gate_id=None,
                    actor=owner_actor(profile_id),
                    operation=resolved.operation,
                    report=resolved.report,
                )
            except Exception as triage_error:  # noqa: BLE001 - contained
                # A triage failure never masks the original error and never
                # converts it into success; the original propagates below.
                logger.warning(
                    "the operational-failure triage did not complete (error type %s)",
                    type(triage_error).__name__,
                )
            if disposition is not None:
                # Every disposition is authoritative and final here. The
                # authorized retry is recorded but never consumed, replayed,
                # or scheduled: F5c owns the execution attempt and F11 the
                # due wakeups. BLOCKED and every no-op already applied their
                # (or no) durable effect inside F4's own composition.
                logger.debug(
                    "operational-failure disposition applied (kind=%s)",
                    disposition.kind.value,
                )
            raise
