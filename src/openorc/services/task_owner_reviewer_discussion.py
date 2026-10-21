"""E4: Owner ↔ Reviewer discussion across Reviewer-related gates (issue #79).

One focused Owner-authenticated service routes free-form Owner ↔ Reviewer
discussion at every pending current OwnerGate of a non-archived
``waiting_for_owner`` Task through the Task's existing admitted READY
Reviewer session. At ordinary gates the exchange is strictly advisory. At
a pending current REVIEW_RESOLUTION gate, an explicitly recognizable
standalone, schema-valid Reviewer ``review_result`` reply is a formal
continuation of the same OPEN ReviewLoop and is dispatched — never applied
here — to the authoritative exact-subject settlement seam (E3c planning
today; E9 PR/head when that leaf lands). At a still-current plan-subject
IMPLEMENTATION_AUTHORIZATION gate, issue #219 makes exactly one reply
formal: a standalone schema-valid ``review_result`` with
``CHANGES_REQUESTED`` dispatches E3's implementation-gate change-request
handoff (a new iteration for the unchanged accepted revision, the gate
superseded, planning resumed); any ACCEPTED reply there — like ordinary
prose — stays advisory and the Owner may still approve the gate. E4 owns
message routing and recognition; it never mutates gates, loops, or
iterations directly. The runtime owns conversation context; OpenOrc
records no transcript.

Composition order (authorization precedes every adapter acquisition):

1. command-shape validation (no I/O);
2. one short transaction proving the account-operational barrier, exact
   Workspace/Task Owner authorization, the exact current Task under the
   caller's expected token, ``WAITING_FOR_OWNER``, the exact pending
   current gate addressed by ``owner_gate_id``, the gate's real subject
   form (exact current PlanRevision for plan-subject gates; the recorded
   pre-PR committed head for ``PR_AUTHORIZATION`` with no PR invented; the
   canonical TaskPullRequest plus its exact reconciled head for PR/head
   subjects), and the exact admitted READY Reviewer binding;
3. deferred acquisition of the exact Reviewer runtime attachment through
   the caller-supplied factory — production composition binds the #190
   ``attach_cline_runtime`` BOUND attachment to the Phase-1 external
   session identity, so no adapter is ever acquired before authorization;
4. one short revalidation of the same currentness facts (attachment takes
   wall-clock time; the send is refused when the state moved on);
5. the verbatim Owner message as ONE ordinary exact-session send
   (``expected_family=None``) with NO database transaction open;
6. post-send recheck and one short recheck transaction that FIRST takes the
   existing Task row ``FOR UPDATE`` — serializing the still-current decision
   and its effects against concurrent Task mutations (an Owner gate
   resolution stamps the gate and rotates the token under the same row
   lock), so an ordinary exchange can never audit itself against a gate
   that is no longer current: a still-current
   ordinary exchange records exactly two direction-only
   ``OWNER_REVIEWER_DISCUSSION_MESSAGE`` events (Owner message, Reviewer
   reply) bound to the addressed gate — direction-only context, never any
   text; a still-current formal result at a plan-subject REVIEW_RESOLUTION
   dispatches the E3c settlement, and a still-current formal
   CHANGES_REQUESTED at a plan-subject IMPLEMENTATION_AUTHORIZATION
   dispatches issue #219's E3 change-request handoff, each with
   ``record_discussion_message_events`` so the discussion facts commit
   atomically with the authoritative continuation and no partial success
   is representable; a recognized ACCEPTED at the implementation gate is
   advisory and records the ordinary exchange; a recognized result at a
   PR/head-subject REVIEW_RESOLUTION gate fails closed until E9 (#84)
   adds its settlement seam.

Recognition rule (deliberately stricter than the shared #65 parser's
one-object-amid-prose tolerance): the whole normalized reply text must be
exactly one standalone JSON object (unfenced) or exactly one fenced JSON
object with nothing outside the fence. Prose-wrapped, buried, quoted, or
example JSON, multiple objects, malformed JSON, and other-family or
unknown-version objects all stay ordinary displayed conversation. Only an
isolated candidate is validated through the existing #65
``parse_formal_response``; the family must classify as ``review_result``
and schema-validate, yielding the typed ``ReviewResult``. The parser matrix
is reused, never duplicated or extended. At the implementation gate
(issue #219) the recognized outcome additionally decides formality: only
``CHANGES_REQUESTED`` is a workflow trigger, and a schema-valid
``ACCEPTED`` stays ordinary displayed conversation.

Failure discipline: known/uncertain adapter failures, unavailable exact
sessions, stale/foreign subjects, and raced resolutions return typed safe
errors, record nothing, and grant no workflow authority. An uncertain send
may already have been delivered; it is never resent and never presented as
a successful exchange. No database transaction spans the runtime send or
any attachment acquisition.
"""

from __future__ import annotations

import json
import logging
import re
from collections.abc import Callable
from contextlib import AbstractContextManager
from dataclasses import dataclass
from enum import StrEnum
from typing import Any, cast
from uuid import UUID

from openorc.adapters.agent_runtime.contract import AgentRuntimeAdapter
from openorc.adapters.agent_runtime.errors import (
    AgentRuntimeError,
    AgentRuntimeUncertainOutcomeError,
)
from openorc.adapters.agent_runtime.results import AgentTextResponse
from openorc.adapters.github import GitHubAppClient
from openorc.domain.connections import WorkflowRole
from openorc.domain.events import WorkflowEventActor
from openorc.domain.gates import OwnerGate, OwnerGateType
from openorc.domain.sessions import TaskAgentSession, TaskSessionLifecycleStatus
from openorc.domain.tasks import Task, TaskStatus
from openorc.observability import annotate_span, application_span
from openorc.persistence import tasks as task_records
from openorc.persistence.pool import DatabasePool
from openorc.persistence.sessions import get_task_agent_session
from openorc.protocol.errors import ProtocolError
from openorc.protocol.models import REVIEW_OUTCOME_CHANGES_REQUESTED, ReviewResult
from openorc.protocol.parsing import parse_formal_response
from openorc.services import event_coordination
from openorc.services.errors import (
    ConflictError,
    ExternalOperationFailedError,
    ExternalOperationUncertainError,
    InvalidCommandError,
    NotFoundError,
    StaleOperationError,
)
from openorc.services.profile_lifecycle_guard import require_account_operational
from openorc.services.task_planning_review_resolution import (
    TaskPlanningGateChangeRequestOutcome,
    TaskPlanningReviewResolutionOutcome,
    settle_planning_review_resolution,
    supersede_implementation_authorization_with_changes,
)
from openorc.services.task_pr_review_decision import (
    TaskPrMergeGateChangeRequestOutcome,
    TaskPrReviewGateResolutionOutcome,
    retire_pr_merge_decision_with_changes,
    settle_pr_review_resolution,
)
from openorc.services.task_subject_guards import (
    require_current_owner_gate,
    require_current_plan_revision,
    require_current_task,
    require_task_pull_request_head,
)
from openorc.services.transaction_composition import composed_transaction
from openorc.services.workspace_authorization import require_workspace_task

__all__ = [
    "OwnerReviewerDiscussionExchange",
    "ProducerRuntimeAttachmentFactory",
    "ReviewerRuntimeAttachmentFactory",
    "TaskOwnerReviewerDiscussionOutcome",
    "send_owner_reviewer_discussion_message",
]

_SERVICE_TRACER_SCOPE = "openorc.services.task_owner_reviewer_discussion"
_DISCUSSION_SPAN_NAME = "task_owner_reviewer_discussion.send_owner_reviewer_discussion_message"

logger = logging.getLogger(__name__)

# The deferred attachment seams: the service decides WHEN each adapter is
# acquired; the caller owns HOW the production attachment is composed (the
# #190 BOUND ``attach_cline_runtime`` composition). No adapter is ever
# acquired before the Phase-1 authorization transaction has committed.
ReviewerRuntimeAttachmentFactory = Callable[
    [TaskAgentSession], AbstractContextManager[AgentRuntimeAdapter]
]
ProducerRuntimeAttachmentFactory = Callable[[], AbstractContextManager[AgentRuntimeAdapter]]

# Standalone fenced-object recognition: the whole normalized text must be
# exactly one fence whose inner content is one JSON object, with nothing
# outside the fence. The optional info string is runtime-neutral decoration.
_FENCED_OBJECT = re.compile(
    r"\A```[ \t]*[\w.+-]*[ \t]*\r?\n(?P<body>.*)\r?\n```[ \t]*\Z",
    re.DOTALL,
)


class OwnerReviewerDiscussionExchange(StrEnum):
    """The classified exchange outcome of one discussion service invocation."""

    ORDINARY_REPLY = "ordinary_reply"
    FORMAL_RESOLUTION_SETTLED = "formal_resolution_settled"
    FORMAL_PLANNING_CHANGE_REQUESTED = "formal_planning_change_requested"
    # Issue #84: the PR/head-subject formal continuations dispatch E9's
    # dedicated seams instead of the planning settlement.
    FORMAL_PR_RESOLUTION_SETTLED = "formal_pr_resolution_settled"
    FORMAL_PR_MERGE_GATE_CHANGE_REQUESTED = "formal_pr_merge_gate_change_requested"


@dataclass(frozen=True, slots=True)
class TaskOwnerReviewerDiscussionOutcome:
    """The safe durable result of one Owner ↔ Reviewer discussion exchange.

    ``exchange`` classifies the outcome. On ``ORDINARY_REPLY`` the reply
    text is returned unmodified for the Owner-facing caller; the exchange
    is audited as two direction-only events bound to the addressed gate.
    On ``FORMAL_RESOLUTION_SETTLED`` the recognized formal result was
    dispatched to the authoritative exact-subject settlement seam and
    ``resolution`` carries its outcome. On
    ``FORMAL_PLANNING_CHANGE_REQUESTED`` (issue #219) the recognized
    CHANGES_REQUESTED result at the implementation gate was dispatched to
    E3's change-request handoff and ``change_request`` carries its
    outcome. The reply text is not re-exposed for any formal exchange
    (the dispatched events and the runtime's own conversation context own
    the facts). ``task`` is the still-current Task observed by the
    post-send recheck. A failed exchange returns a typed error instead of
    an outcome, never a falsely successful one.
    """

    exchange: OwnerReviewerDiscussionExchange
    owner_gate: OwnerGate
    reviewer_session: TaskAgentSession
    task: Task
    reply_text: str | None
    resolution: TaskPlanningReviewResolutionOutcome | None
    change_request: TaskPlanningGateChangeRequestOutcome | None
    # Issue #84: the PR/head-subject formal continuations carry their E9
    # seam outcomes (the settled PR/head resolution — with its installed
    # merge decision gate or routed remediation round — or the retired
    # merge-gate change request).
    pr_resolution: TaskPrReviewGateResolutionOutcome | None = None
    pr_merge_gate_change_request: TaskPrMergeGateChangeRequestOutcome | None = None


@dataclass(frozen=True, slots=True)
class _DiscussionSubjects:
    """The exact current subjects one discussion exchange is admitted at."""

    task: Task
    gate: OwnerGate
    reviewer: TaskAgentSession


# --- command validation ---------------------------------------------------------


def _require_uuid_command(value: object, name: str) -> UUID:
    """Reject a malformed command identifier before any read or effect."""
    if not isinstance(value, UUID):
        raise InvalidCommandError(f"{name} must be a UUID")
    return value


def _require_message(value: object) -> str:
    """Require one nonblank Owner message; the text is sent verbatim."""
    if not isinstance(value, str) or not value.strip():
        raise InvalidCommandError("message must be a non-empty string")
    return value


def _require_reviewer_factory(value: object) -> ReviewerRuntimeAttachmentFactory:
    """Require the deferred Reviewer attachment factory dependency."""
    if not callable(value):
        raise InvalidCommandError(
            "reviewer_runtime_factory must be a callable returning one adapter "
            "attachment context manager for the exact resolved Reviewer binding"
        )
    return cast(ReviewerRuntimeAttachmentFactory, value)


def _require_producer_factory(value: object) -> ProducerRuntimeAttachmentFactory:
    """Require the deferred Producer attachment factory for a formal continuation."""
    if not callable(value):
        raise InvalidCommandError(
            "producer_runtime_factory must be a callable returning one adapter "
            "attachment context manager for the formal E2 continuation"
        )
    return cast(ProducerRuntimeAttachmentFactory, value)


# --- subject-form validation ------------------------------------------------------


def _require_pull_request_head_subject(pool: DatabasePool, *, task: Task, gate: OwnerGate) -> None:
    """Prove a PR-subject gate still binds the canonical PR and its exact head.

    The canonical TaskPullRequest is resolved from durable state (never a
    caller-supplied id), the gate's bound PR record must still be exactly
    that canonical record, and the gate's exact head SHA must equal the
    current reconciled head. Base-branch movement is deliberately never
    consulted; a changed head is a stale operation that applies nothing.
    """
    if gate.task_pull_request_id is None or gate.subject_head_sha is None:
        raise ConflictError(
            "the owner gate does not carry the canonical pull-request/head subject form"
        )
    pull_request = require_task_pull_request_head(
        pool, task=task, expected_head_sha=gate.subject_head_sha
    )
    if pull_request.id != gate.task_pull_request_id:
        raise StaleOperationError(
            "the owner gate no longer addresses the task's canonical pull request; "
            "the operation is stale"
        )


def _validate_gate_subject(pool: DatabasePool, *, task: Task, gate: OwnerGate) -> None:
    """Prove the addressed gate still binds its exact real subject form.

    Every pending gate type admits discussion only against its real
    subject: plan-subject gates need the exact current PlanRevision;
    ``PR_AUTHORIZATION`` needs its exact already-recorded committed head
    and no invented PR; ``MERGE_DECISION`` and the PR/head form of
    ``REVIEW_RESOLUTION`` need the canonical TaskPullRequest plus the
    exact reconciled current head. E4 never decides review, GitHub
    reconciliation, permission to implement, or permission to merge — it
    only refuses discussion at foreign, stale, or replaced subjects.
    """
    if gate.gate_type is OwnerGateType.IMPLEMENTATION_AUTHORIZATION:
        if gate.plan_revision_id is None:
            raise ConflictError(
                "the implementation authorization gate carries no plan revision subject"
            )
        require_current_plan_revision(pool, task=task, plan_revision_id=gate.plan_revision_id)
        return
    if gate.gate_type is OwnerGateType.PR_AUTHORIZATION:
        # The pre-PR gate's subject IS its already-recorded committed head;
        # the domain/DB CHECK guarantees the exact form. No TaskPullRequest
        # is resolved, compared, or invented here.
        if (
            gate.subject_head_sha is None
            or gate.plan_revision_id is not None
            or gate.task_pull_request_id is not None
        ):
            raise ConflictError(
                "the pr_authorization gate does not carry the exact pre-PR "
                "committed-head subject form"
            )
        return
    if gate.gate_type is OwnerGateType.MERGE_DECISION:
        _require_pull_request_head_subject(pool, task=task, gate=gate)
        return
    if gate.gate_type is OwnerGateType.REVIEW_RESOLUTION:
        if gate.plan_revision_id is not None:
            require_current_plan_revision(pool, task=task, plan_revision_id=gate.plan_revision_id)
            return
        _require_pull_request_head_subject(pool, task=task, gate=gate)
        return
    raise ConflictError("the owner gate type does not admit reviewer discussion")


# --- reviewer session resolution ----------------------------------------------------


def _require_ready_reviewer(
    pool: DatabasePool, *, workspace_id: UUID, task_id: UUID
) -> TaskAgentSession:
    """Resolve the exact admitted READY Reviewer role/session binding.

    Discussion reuses the original admitted Reviewer binding only: a
    missing, cross-Workspace, or cross-Task binding is the uniform
    not-found outcome; a CONNECTING/LOST/ENDED binding is a conflict. No
    rebinding, second session, or fresh initialization ever happens here —
    session loss is a blocking/recovery condition, never a silent
    replacement.
    """
    binding = get_task_agent_session(pool, task_id=task_id, role=WorkflowRole.REVIEWER)
    if binding is None or binding.workspace_id != workspace_id or binding.task_id != task_id:
        raise NotFoundError("the requested reviewer session binding is not available for this task")
    if binding.lifecycle_status is not TaskSessionLifecycleStatus.READY:
        raise ConflictError(
            "the reviewer session binding is not READY; the discussion uses the exact "
            "admitted session only and never recreates or reinitializes one"
        )
    if binding.external_session_id is None:
        raise ConflictError("the reviewer session binding has no external session identity")
    return binding


def _revalidate_currentness(
    pool: DatabasePool,
    *,
    workspace_id: UUID,
    task_id: UUID,
    owner_gate_id: UUID,
    expected_state_token: UUID,
    expected_reviewer_session_id: UUID | None,
) -> _DiscussionSubjects:
    """Reload and re-prove the exact current discussion subjects.

    The one shared currentness reloader: the exact current non-archived
    Task under the expected token, ``WAITING_FOR_OWNER``, the exact
    pending current gate, its real subject form, and the exact admitted
    READY Reviewer binding. Every phase proves the identical facts; when
    ``expected_reviewer_session_id`` is supplied (the pre-send revalidation
    and the post-send recheck), the durable binding must still be exactly
    the Phase-1 session — never a replacement.
    """
    task = require_current_task(
        pool,
        workspace_id=workspace_id,
        task_id=task_id,
        expected_state_token=expected_state_token,
    )
    if task.status is not TaskStatus.WAITING_FOR_OWNER:
        raise ConflictError("the addressed task is not waiting on its owner gate")
    gate = require_current_owner_gate(pool, task=task, owner_gate_id=owner_gate_id)
    _validate_gate_subject(pool, task=task, gate=gate)
    reviewer = _require_ready_reviewer(pool, workspace_id=workspace_id, task_id=task_id)
    if expected_reviewer_session_id is not None and reviewer.id != expected_reviewer_session_id:
        raise StaleOperationError(
            "the reviewer session binding has moved on since this exchange was "
            "prepared; the operation is stale"
        )
    return _DiscussionSubjects(task=task, gate=gate, reviewer=reviewer)


# --- the exact-session send ---------------------------------------------------------


def _send_discussion_message(
    runtime: AgentRuntimeAdapter, external_session_id: str, message: str
) -> AgentTextResponse:
    """Send the Owner message verbatim to the exact Reviewer session.

    ONE ordinary exact-session interaction (``expected_family=None``): the
    adapter performs no protocol parsing, so formal-looking JSON in the
    reply stays ordinary text at the adapter boundary. Known failure and
    uncertain outcome stay distinct; an uncertain send may already have
    been delivered and is never resent or presented as success. No
    database transaction is open across this call.
    """
    try:
        response = runtime.send(external_session_id, message, expected_family=None)
    except AgentRuntimeUncertainOutcomeError as error:
        logger.warning(
            "owner-reviewer discussion send ended with an uncertain runtime outcome; "
            "the exchange was not recorded and nothing was applied"
        )
        raise ExternalOperationUncertainError(
            "the outcome of the agent runtime discussion send is unknown; the exchange "
            "was not recorded and nothing was applied"
        ) from error
    except AgentRuntimeError as error:
        raise ExternalOperationFailedError(
            "the agent runtime discussion send failed; the exchange was not recorded "
            "and nothing was applied"
        ) from error
    if not isinstance(response, AgentTextResponse):
        # An ordinary send returns the typed ordinary reply; anything else
        # is a known non-answer and is never interpreted as one.
        raise ExternalOperationFailedError(
            "the agent runtime did not return an ordinary discussion reply; the "
            "exchange was not recorded and nothing was applied"
        )
    return response


# --- formal-result recognition -------------------------------------------------------


def _isolated_standalone_object(text: str) -> dict[str, Any] | None:
    """Isolate a standalone unfenced or single-fenced JSON object, or None.

    The whole normalized text must be exactly one JSON object, or exactly
    one fenced code block whose inner content is one JSON object with
    nothing outside the fence. Prose-wrapped, buried, quoted, or example
    JSON, multiple objects, and malformed spans are ordinary conversation,
    never extracted.
    """
    normalized = text.strip()
    if not normalized:
        return None
    try:
        direct = json.loads(normalized)
    except ValueError:
        direct = None
    if isinstance(direct, dict):
        return direct
    fenced = _FENCED_OBJECT.match(normalized)
    if fenced is None:
        return None
    try:
        inner = json.loads(fenced.group("body"))
    except ValueError:
        return None
    return inner if isinstance(inner, dict) else None


def _recognize_formal_review_result(text: str) -> ReviewResult | None:
    """Recognize one standalone schema-valid ``review_result``, or None.

    Only an explicitly-typed, schema-valid standalone ``review_result``
    becomes a formal Reviewer trigger. The existing #65 parser validates
    and types the already-isolated candidate; any protocol failure —
    unknown type, unsupported schema version, schema-invalid payload,
    malformed object — and any other response family leave the reply as
    ordinary displayed conversation. No parser behavior is duplicated,
    changed, or extended here.
    """
    payload = _isolated_standalone_object(text)
    if payload is None:
        return None
    try:
        formal = parse_formal_response(payload)
    except ProtocolError:
        return None
    if not isinstance(formal, ReviewResult):
        return None
    return formal


# --- audit helpers ---------------------------------------------------------------------


def _reviewer_actor(reviewer: TaskAgentSession) -> event_coordination.WorkflowActorContext:
    """The safe REVIEWER logical actor identity: the exact session UUID."""
    return event_coordination.WorkflowActorContext(WorkflowEventActor.REVIEWER, str(reviewer.id))


def _record_ordinary_exchange_events(
    transaction_pool: DatabasePool,
    *,
    workspace_id: UUID,
    task_id: UUID,
    gate: OwnerGate,
    reviewer: TaskAgentSession,
    profile_id: UUID,
) -> None:
    """Record the two direction-only message facts of one ordinary exchange.

    Called inside the same short transaction as the post-send recheck, so a
    still-current ordinary exchange commits its audit atomically and a
    raced resolution records nothing. Direction-only context; the mapping
    helper cannot carry text.
    """
    event_coordination.record_owner_reviewer_discussion_message_event(
        transaction_pool,
        workspace_id=workspace_id,
        task_id=task_id,
        owner_gate_id=gate.id,
        actor=event_coordination.owner_actor(profile_id),
        direction=event_coordination.OwnerReviewerDiscussionDirection.OWNER_TO_REVIEWER,
    )
    event_coordination.record_owner_reviewer_discussion_message_event(
        transaction_pool,
        workspace_id=workspace_id,
        task_id=task_id,
        owner_gate_id=gate.id,
        actor=_reviewer_actor(reviewer),
        direction=event_coordination.OwnerReviewerDiscussionDirection.REVIEWER_TO_OWNER,
    )


# --- the formal dispatch ----------------------------------------------------------------


def _dispatch_formal_resolution(
    pool: DatabasePool,
    *,
    profile_id: UUID,
    workspace_id: UUID,
    task_id: UUID,
    expected_state_token: UUID,
    subjects: _DiscussionSubjects,
    formal_result: ReviewResult,
    reviewer_runtime_factory: ReviewerRuntimeAttachmentFactory,
    producer_runtime_factory: ProducerRuntimeAttachmentFactory | None,
    github: GitHubAppClient | None,
) -> TaskOwnerReviewerDiscussionOutcome:
    """Hand one recognized formal result to the authoritative continuation seam.

    E4 never mutates gates, loops, or iterations: a plan-subject
    REVIEW_RESOLUTION dispatches the merged E3c settlement with the
    discussion-audit coordination flag, and a still-current plan-subject
    IMPLEMENTATION_AUTHORIZATION dispatches issue #219's E3 change-request
    handoff the same way — only a CHANGES_REQUESTED result ever reaches
    this dispatcher from that gate. A formal CHANGES_REQUESTED routes new
    findings to E2, so the Producer-side attachment is acquired lazily —
    only now, only for this path — around the seam call; a formal
    ACCEPTED at REVIEW_RESOLUTION needs no Producer attachment and no
    GitHub client. A PR/head-subject REVIEW_RESOLUTION belongs to E9
    (#84): until that seam lands, a recognized result there fails closed
    with no effect rather than being invented here. The dispatch target
    re-proves every exact subject in its own transaction, so a raced
    stale result applies nothing.
    """
    gate = subjects.gate
    if gate.plan_revision_id is None:
        # PR/head-subject form: E9's exact-subject PR-review seams (issue
        # #84). At a pending PR/head MERGE_DECISION gate only a standalone
        # valid CHANGES_REQUESTED is actionable — a formal ACCEPTED stays
        # advisory exactly like ordinary prose, and the Owner may still
        # approve the unchanged-head gate. At a PR/head REVIEW_RESOLUTION
        # gate any schema-valid formal result settles it through E9's
        # dedicated settlement. The dispatch target re-proves every exact
        # subject in its own transaction, so a raced stale result applies
        # nothing.
        if gate.gate_type is OwnerGateType.MERGE_DECISION:
            # Only a standalone valid CHANGES_REQUESTED reaches this branch:
            # E4's recognition keeps a formal ACCEPTED advisory, and the
            # retirement seam re-validates the outcome as its backstop.
            if producer_runtime_factory is None or github is None:
                raise InvalidCommandError(
                    "the actionable merge-gate continuation requires the deferred "
                    "producer runtime attachment factory and the GitHub client"
                )
            with producer_runtime_factory() as producer_runtime:
                pr_change_request = retire_pr_merge_decision_with_changes(
                    pool,
                    producer_runtime,
                    github,
                    reviewer_runtime_factory,
                    profile_id=profile_id,
                    workspace_id=workspace_id,
                    task_id=task_id,
                    expected_state_token=expected_state_token,
                    review_result=formal_result,
                    record_discussion_message_events=True,
                )
            return TaskOwnerReviewerDiscussionOutcome(
                exchange=OwnerReviewerDiscussionExchange.FORMAL_PR_MERGE_GATE_CHANGE_REQUESTED,
                owner_gate=subjects.gate,
                reviewer_session=subjects.reviewer,
                task=pr_change_request.task,
                reply_text=None,
                resolution=None,
                change_request=None,
                pr_resolution=None,
                pr_merge_gate_change_request=pr_change_request,
            )
        if formal_result.outcome == REVIEW_OUTCOME_CHANGES_REQUESTED:
            if producer_runtime_factory is None or github is None:
                raise InvalidCommandError(
                    "the formal continuation requires the deferred producer runtime "
                    "attachment factory and the GitHub client"
                )
            with producer_runtime_factory() as producer_runtime:
                pr_resolution = settle_pr_review_resolution(
                    pool,
                    producer_runtime,
                    github,
                    reviewer_runtime_factory,
                    profile_id=profile_id,
                    workspace_id=workspace_id,
                    task_id=task_id,
                    expected_state_token=expected_state_token,
                    review_result=formal_result,
                    record_discussion_message_events=True,
                )
        else:
            pr_resolution = settle_pr_review_resolution(
                pool,
                None,
                github,
                reviewer_runtime_factory,
                profile_id=profile_id,
                workspace_id=workspace_id,
                task_id=task_id,
                expected_state_token=expected_state_token,
                review_result=formal_result,
                record_discussion_message_events=True,
            )
        return TaskOwnerReviewerDiscussionOutcome(
            exchange=OwnerReviewerDiscussionExchange.FORMAL_PR_RESOLUTION_SETTLED,
            owner_gate=subjects.gate,
            reviewer_session=subjects.reviewer,
            task=pr_resolution.task,
            reply_text=None,
            resolution=None,
            change_request=None,
            pr_resolution=pr_resolution,
            pr_merge_gate_change_request=None,
        )
    if gate.gate_type is OwnerGateType.IMPLEMENTATION_AUTHORIZATION:
        # Issue #219: the Reviewer change request at the accepted-plan gate
        # hands planning back to the Producer through E3's exact-plan G1
        # handoff; only the CHANGES_REQUESTED outcome reaches this branch.
        if producer_runtime_factory is None:
            raise InvalidCommandError(
                "the formal continuation requires the deferred producer runtime attachment factory"
            )
        with producer_runtime_factory() as producer_runtime:
            change_request = supersede_implementation_authorization_with_changes(
                pool,
                producer_runtime,
                github,
                profile_id=profile_id,
                workspace_id=workspace_id,
                task_id=task_id,
                expected_state_token=expected_state_token,
                review_result=formal_result,
                record_discussion_message_events=True,
            )
        return TaskOwnerReviewerDiscussionOutcome(
            exchange=OwnerReviewerDiscussionExchange.FORMAL_PLANNING_CHANGE_REQUESTED,
            owner_gate=subjects.gate,
            reviewer_session=subjects.reviewer,
            task=change_request.task,
            reply_text=None,
            resolution=None,
            change_request=change_request,
        )
    if formal_result.outcome == REVIEW_OUTCOME_CHANGES_REQUESTED:
        if producer_runtime_factory is None:
            raise InvalidCommandError(
                "the formal continuation requires the deferred producer runtime attachment factory"
            )
        with producer_runtime_factory() as producer_runtime:
            resolution = settle_planning_review_resolution(
                pool,
                producer_runtime,
                github,
                profile_id=profile_id,
                workspace_id=workspace_id,
                task_id=task_id,
                expected_state_token=expected_state_token,
                review_result=formal_result,
                record_discussion_message_events=True,
            )
    else:
        resolution = settle_planning_review_resolution(
            pool,
            None,
            github,
            profile_id=profile_id,
            workspace_id=workspace_id,
            task_id=task_id,
            expected_state_token=expected_state_token,
            review_result=formal_result,
            record_discussion_message_events=True,
        )
    return TaskOwnerReviewerDiscussionOutcome(
        exchange=OwnerReviewerDiscussionExchange.FORMAL_RESOLUTION_SETTLED,
        owner_gate=subjects.gate,
        reviewer_session=subjects.reviewer,
        task=resolution.task,
        reply_text=None,
        resolution=resolution,
        change_request=None,
    )


def send_owner_reviewer_discussion_message(
    pool: DatabasePool,
    *,
    profile_id: UUID,
    workspace_id: UUID,
    task_id: UUID,
    owner_gate_id: UUID,
    expected_state_token: UUID,
    message: str,
    reviewer_runtime_factory: ReviewerRuntimeAttachmentFactory,
    producer_runtime_factory: ProducerRuntimeAttachmentFactory | None = None,
    github: GitHubAppClient | None = None,
) -> TaskOwnerReviewerDiscussionOutcome:
    """Route one Owner message to the exact admitted Reviewer session at one gate.

    The single E4 discussion operation. Every authorization and currentness
    check commits BEFORE any adapter acquisition; the exact Reviewer
    attachment is acquired through the deferred factory bound to the
    Phase-1 external session identity; the relevant currentness facts are
    revalidated after acquisition and again after the send; and the
    reply's classification decides only between ordinary audit and formal
    continuation dispatch (the E3c settlement, or issue #219's E3
    implementation-gate change-request handoff). No database transaction
    spans the attachment or the send, and no failure path ever applies
    workflow authority.
    """
    with application_span(_SERVICE_TRACER_SCOPE, _DISCUSSION_SPAN_NAME) as span:
        # --- Phase 0: command shape (no I/O). ---
        _require_uuid_command(profile_id, "profile_id")
        _require_uuid_command(workspace_id, "workspace_id")
        _require_uuid_command(task_id, "task_id")
        _require_uuid_command(owner_gate_id, "owner_gate_id")
        _require_uuid_command(expected_state_token, "expected_state_token")
        _require_message(message)
        reviewer_factory = _require_reviewer_factory(reviewer_runtime_factory)
        annotate_span(
            span,
            operation=_DISCUSSION_SPAN_NAME,
            workspace_id=str(workspace_id),
            task_id=str(task_id),
        )

        # --- Phase 1: authorization + currentness, BEFORE any acquisition. ---
        with composed_transaction(pool) as transaction_pool:
            require_account_operational(transaction_pool, profile_id=profile_id)
            require_workspace_task(
                transaction_pool,
                profile_id=profile_id,
                workspace_id=workspace_id,
                task_id=task_id,
            )
            subjects = _revalidate_currentness(
                transaction_pool,
                workspace_id=workspace_id,
                task_id=task_id,
                owner_gate_id=owner_gate_id,
                expected_state_token=expected_state_token,
                expected_reviewer_session_id=None,
            )
        prepared_reviewer_id = subjects.reviewer.id

        # --- Phase 2a: acquire the exact Reviewer attachment (no tx open). ---
        with reviewer_factory(subjects.reviewer) as reviewer_runtime:
            # --- Phase 2b: revalidate currentness after acquisition. ---
            with composed_transaction(pool) as transaction_pool:
                _revalidate_currentness(
                    transaction_pool,
                    workspace_id=workspace_id,
                    task_id=task_id,
                    owner_gate_id=owner_gate_id,
                    expected_state_token=expected_state_token,
                    expected_reviewer_session_id=prepared_reviewer_id,
                )
            # --- Phase 2c: the ONE verbatim send, no transaction open. ---
            reply = _send_discussion_message(
                reviewer_runtime,
                subjects.reviewer.external_session_id or "",
                message,
            )

        # Recognition is pure text work on the already-received reply; it
        # runs at REVIEW_RESOLUTION gates, at plan-subject
        # IMPLEMENTATION_AUTHORIZATION gates, and at PR/head MERGE_DECISION
        # gates, and decides which effects (if any) the still-current reply
        # may have.
        formal_result: ReviewResult | None = None
        if subjects.gate.gate_type is OwnerGateType.REVIEW_RESOLUTION:
            formal_result = _recognize_formal_review_result(reply.text)
        elif subjects.gate.gate_type is OwnerGateType.IMPLEMENTATION_AUTHORIZATION:
            recognized = _recognize_formal_review_result(reply.text)
            if recognized is not None and recognized.outcome == REVIEW_OUTCOME_CHANGES_REQUESTED:
                # Issue #219: at the implementation gate only a valid
                # CHANGES_REQUESTED reply is an actionable formal trigger;
                # a standalone ACCEPTED — like ordinary prose — stays
                # advisory and the Owner may still approve the gate.
                formal_result = recognized
        elif subjects.gate.gate_type is OwnerGateType.MERGE_DECISION:
            # Issue #84: at the pending PR/head merge gate only a standalone
            # valid CHANGES_REQUESTED reply is an actionable formal trigger;
            # ordinary discussion, malformed or embedded JSON, and a
            # standalone ACCEPTED all stay advisory while the head is
            # unchanged.
            recognized = _recognize_formal_review_result(reply.text)
            if recognized is not None and recognized.outcome == REVIEW_OUTCOME_CHANGES_REQUESTED:
                formal_result = recognized

        # --- Phase 3: post-send recheck + effects (one short transaction). ---
        with composed_transaction(pool) as transaction_pool:
            # Serialize against concurrent Task mutations FIRST (the same
            # Task-row lock every authoritative Task mutation takes, E3c's
            # established pattern): an Owner gate resolution stamps the gate
            # and rotates the token under this lock, so the still-current
            # decision below and the audit/dispatch it authorizes can never
            # interleave with a concurrent resolution. The lock lives only
            # inside this short transaction — never across the attachment
            # or the send, which happened before it.
            locked = task_records.get_task_for_update(transaction_pool, task_id)
            if locked is None or locked.workspace_id != workspace_id:
                raise NotFoundError("the requested task is not available in this workspace")
            current = _revalidate_currentness(
                transaction_pool,
                workspace_id=workspace_id,
                task_id=task_id,
                owner_gate_id=owner_gate_id,
                expected_state_token=expected_state_token,
                expected_reviewer_session_id=prepared_reviewer_id,
            )
            if formal_result is None:
                _record_ordinary_exchange_events(
                    transaction_pool,
                    workspace_id=workspace_id,
                    task_id=task_id,
                    gate=current.gate,
                    reviewer=current.reviewer,
                    profile_id=profile_id,
                )

        if formal_result is not None:
            return _dispatch_formal_resolution(
                pool,
                profile_id=profile_id,
                workspace_id=workspace_id,
                task_id=task_id,
                expected_state_token=expected_state_token,
                subjects=current,
                formal_result=formal_result,
                reviewer_runtime_factory=reviewer_runtime_factory,
                producer_runtime_factory=producer_runtime_factory,
                github=github,
            )

        return TaskOwnerReviewerDiscussionOutcome(
            exchange=OwnerReviewerDiscussionExchange.ORDINARY_REPLY,
            owner_gate=current.gate,
            reviewer_session=current.reviewer,
            task=current.task,
            reply_text=reply.text,
            resolution=None,
            change_request=None,
        )
