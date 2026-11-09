"""Shared logical-operation invocation and submission boundary (issue #86).

One small, typed logical-invocation and submission interface usable from
API/local and RQ worker processes. The execution mode determines only
where/how an operation is delivered; each operation handler invokes the
existing shared application service, and the service — never the delivery
mechanism — determines whether anything remains current or authorized.

Boundary rules enforced by this module:

- The operation-kind discriminator is closed (:class:`OperationKind`). New
  kinds are added explicitly by later leaves; there are no
  arbitrary module/function names, executable callables, pickled domain
  objects, unvalidated ``**kwargs``, or open-ended authority dictionaries
  in a wire payload.
- Payloads identify the existing durable unit of work and the exact
  subject/currentness inputs the owning service consumes. Nothing else is
  serialized: no credentials, SDK handles, live adapters/pools, customer
  content, prompts, runtime transcripts, endpoint/tunnel state, or mutable
  service-result DTOs. UUIDs and enums use stable JSON-safe representations.
- Strict decoding rejects unknown kinds, unexpected fields, malformed
  types, and missing required fields before any service/runtime effect.
- Dispatch fails closed when a kind has no registered handler. The
  production handler table registers the F5a Task-workflow handlers
  (``openorc.workers.task_workflow``), which invoke the existing E-series
  application services directly with process-local dependencies; the two
  GitHub delivery/reconciliation kinds remain deliberately unregistered
  until F9 (issue #255).
- Submission distinguishes accepted-for-delivery, known refusal/failure,
  and uncertain queue submission. Acceptance is never completion, external
  effect, or retry authorization; queue/job IDs are never operation
  authority; a timeout or transport loss is never converted into a known
  non-delivery. Durable claims and consequences stay with the caller and
  later leaves (F2/F4).
"""

from __future__ import annotations

import json
import logging
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from enum import StrEnum
from typing import TYPE_CHECKING, Any, Protocol
from uuid import UUID

import rq
from redis.exceptions import ConnectionError as RedisConnectionError
from redis.exceptions import RedisError
from redis.exceptions import TimeoutError as RedisTimeoutError

from openorc.adapters.github import GitHubAppClient, HttpGitHubAppClient
from openorc.config import ConfigurationError, ExecutionMode, Settings
from openorc.observability import annotate_span, application_span
from openorc.persistence.pool import DatabasePool, get_database_pool
from openorc.services.errors import InvalidCommandError
from openorc.workers.bootstrap import create_redis_client
from openorc.workers.queues import queue_name

if TYPE_CHECKING:
    from openorc.services.github_user_authorization import ProfileUserAccessTokenResolver
    from openorc.services.task_planning_bootstrap import TaskBootstrapRuntimeComposition

logger = logging.getLogger(__name__)

_TRACER_SCOPE = "openorc.workers.invocation"
_ENTRYPOINT_SPAN_NAME = "invocation.execute_operation_job"
# The RQ-importable path of the thin entrypoint every queue-mode submission
# enqueues; both transports decode the same wire form through this module.
_ENTRYPOINT_PATH = "openorc.workers.invocation.execute_operation_job"


class OperationExecutionError(Exception):
    """Raised when an operation handler failed, with safe content only.

    The message carries only the operation kind and the exception type name:
    arbitrary exception text may carry secret-bearing content and never
    travels through the raised transport error. A handler failure never
    turns into reported operation success.
    """


class InvalidInvocationError(InvalidCommandError):
    """Raised when a wire invocation fails strict envelope validation."""


class ExecutionModeDependencyError(Exception):
    """Raised when a transport seam is built without its required executor."""


class OperationKind(StrEnum):
    """The closed operation-kind discriminator.

    Every member names one durable-operation family with its own typed
    payload decoder. New kinds are added explicitly by later leaves; no
    other string is ever accepted on the wire.
    """

    GITHUB_WEBHOOK_DELIVERY_DISPATCH = "github_webhook_delivery_dispatch"
    GITHUB_RECONCILIATION_SWEEP = "github_reconciliation_sweep"

    # The F5a Task-workflow operation catalog (issue #253): the minimal
    # existing authorized workflow operations, each composed by
    # openorc.workers.task_workflow over the existing E-series services.
    # Tightly coupled in-process sequences stay within one logical
    # invocation; no speculative session operations, RuntimeRequest
    # framework kinds, or generalized orchestration graph members.
    TASK_PLANNING_BOOTSTRAP_AND_INITIAL_PLAN = "task_planning_bootstrap_and_initial_plan"
    TASK_PLANNING_REVIEW = "task_planning_review"
    TASK_AUTHORIZED_IMPLEMENTATION = "task_authorized_implementation"
    TASK_AUTHORIZED_IMPLEMENTATION_RETRY = "task_authorized_implementation_retry"
    # F5d (issue #267): the F4-authorized exact-head merge retry. The handler
    # invokes the shared merge-retry execution service; the due-time wakeup
    # scheduling belongs to F11 (#257), not to this catalog.
    TASK_AUTHORIZED_MERGE_RETRY = "task_authorized_merge_retry"
    # F5f (issue #269): the F4-authorized exact-head PR-authorization
    # presentation retry. The handler invokes the shared presentation-retry
    # execution seam, which re-enters ONLY the guarded E7 presentation; the
    # due-time wakeup scheduling belongs to F11 (#257), not to this catalog.
    TASK_PR_AUTHORIZATION_RETRY = "task_pr_authorization_retry"
    TASK_PR_COMPOSITION = "task_pr_composition"
    TASK_PR_REVIEW = "task_pr_review"
    # F8g (issue #292): the authorized same-session Resume continuation of
    # one interrupted workflow interaction, keyed to its durable one-shot
    # session-resume record identity.
    TASK_SESSION_RESUME = "task_session_resume"


class WorkflowRoleRef(StrEnum):
    """Stable JSON-safe workflow-role representation for payload components.

    Mirrors the v1 workflow-role vocabulary consuming runtime bindings; wire
    payloads carry the closed enum value, never free-form role strings.
    """

    PRODUCER = "producer"
    REVIEWER = "reviewer"


def _require_exact_fields(raw: object, allowed: frozenset[str]) -> dict[str, object]:
    """Require a mapping whose keys are exactly ``allowed``."""
    if not isinstance(raw, dict):
        raise InvalidInvocationError("the invocation payload must be a JSON object")
    unexpected = frozenset(raw) - allowed
    if unexpected:
        # Untrusted input field names are never echoed into errors: the
        # message is static so nothing payload-derived can reach reported
        # failures.
        raise InvalidInvocationError("the invocation payload has unexpected fields")
    missing = allowed - frozenset(raw)
    if missing:
        raise InvalidInvocationError(
            f"the invocation payload is missing required fields: {sorted(missing)}"
        )
    return raw


def _require_non_empty_string(raw: object, field: str) -> str:
    if not isinstance(raw, str) or not raw.strip():
        raise InvalidInvocationError(f"the invocation field {field!r} must be a non-empty string")
    return raw


def _require_canonical_uuid(raw: object, field: str) -> str:
    """Require a UUID-shaped string and return its canonical form."""
    if not isinstance(raw, str):
        raise InvalidInvocationError(f"the invocation field {field!r} must be a UUID string")
    try:
        return str(UUID(raw))
    except ValueError:
        # No chaining: malformed-invocation failures stay fully sanitized in
        # reported tracebacks.
        raise InvalidInvocationError(
            f"the invocation field {field!r} must be a UUID string"
        ) from None


@dataclass(frozen=True, slots=True)
class TaskScope:
    """Representative Task-scoped identity component (envelope machinery).

    The Task-scoped identity a Task-based operation payload carries: the
    existing durable Workspace and Task units of work plus the consuming
    workflow role. This component is the strictly validated, JSON-safe
    envelope machinery the F5a Task-workflow payloads (issue #253) compose
    into concrete Task-workflow operation kinds. Identifiers are canonical
    UUID strings and the role is a closed enum value — never credentials,
    live handles, or content.
    """

    workspace_id: str
    task_id: str
    role: WorkflowRoleRef

    def __post_init__(self) -> None:
        object.__setattr__(
            self, "workspace_id", _require_canonical_uuid(self.workspace_id, "workspace_id")
        )
        object.__setattr__(self, "task_id", _require_canonical_uuid(self.task_id, "task_id"))
        if not isinstance(self.role, WorkflowRoleRef):
            raise InvalidInvocationError("the invocation field 'role' must be a workflow role")

    def encode(self) -> dict[str, str]:
        """Return the stable JSON-safe representation of this component."""
        return {
            "workspace_id": self.workspace_id,
            "task_id": self.task_id,
            "role": self.role.value,
        }

    @classmethod
    def decode(cls, raw: object) -> TaskScope:
        """Strictly decode this component from its JSON representation."""
        fields = _require_exact_fields(raw, frozenset({"workspace_id", "task_id", "role"}))
        raw_role = fields["role"]
        if not isinstance(raw_role, str) or raw_role not in {
            member.value for member in WorkflowRoleRef
        }:
            raise InvalidInvocationError("the invocation field 'role' must be a workflow role")
        return cls(
            workspace_id=_require_canonical_uuid(fields["workspace_id"], "workspace_id"),
            task_id=_require_canonical_uuid(fields["task_id"], "task_id"),
            role=WorkflowRoleRef(raw_role),
        )


# --- F5a Task-workflow operation payloads (issue #253) ----------------------

# The per-kind consuming workflow role a Task-workflow payload must carry:
# the wire vocabulary stays self-describing, and a payload whose role does
# not match its kind is rejected before any effect.
_TASK_KIND_ROLES: Mapping[OperationKind, WorkflowRoleRef] = {
    OperationKind.TASK_PLANNING_BOOTSTRAP_AND_INITIAL_PLAN: WorkflowRoleRef.PRODUCER,
    OperationKind.TASK_PLANNING_REVIEW: WorkflowRoleRef.REVIEWER,
    OperationKind.TASK_AUTHORIZED_IMPLEMENTATION: WorkflowRoleRef.PRODUCER,
    OperationKind.TASK_AUTHORIZED_IMPLEMENTATION_RETRY: WorkflowRoleRef.PRODUCER,
    # F5d: the merge retry consumes no role session — the Owner-accountable
    # command's scope carries the review-context (REVIEWER) role as envelope
    # identity only; the executing service re-derives every subject from
    # committed state.
    OperationKind.TASK_AUTHORIZED_MERGE_RETRY: WorkflowRoleRef.REVIEWER,
    # F5f (issue #269): the presentation retry consumes no role session — the
    # authorized re-presentation continues the implementation phase's
    # authorized continuation, so the scope carries the implementation-phase
    # (PRODUCER) role as envelope identity only; the executing service
    # re-derives every subject from committed state.
    OperationKind.TASK_PR_AUTHORIZATION_RETRY: WorkflowRoleRef.PRODUCER,
    OperationKind.TASK_PR_COMPOSITION: WorkflowRoleRef.PRODUCER,
    OperationKind.TASK_PR_REVIEW: WorkflowRoleRef.REVIEWER,
}


def _decode_task_scope_for_kind(raw: object, kind: OperationKind) -> TaskScope:
    """Strictly decode the Task-scope component, pinned to the kind's role."""
    scope = TaskScope.decode(raw)
    if scope.role is not _TASK_KIND_ROLES[kind]:
        raise InvalidInvocationError(
            f"the invocation field 'role' does not match the operation kind {kind.value!r}"
        )
    return scope


@dataclass(frozen=True, slots=True)
class TaskPlanningBootstrapAndInitialPlanPayload:
    """The planning bootstrap + initial PLAN operation payload (F5a, issue #253).

    The exact durable Workspace/Task units with the consuming Producer role,
    the accountable Owner Profile identity, and the exact post-start
    ``state_token`` the bootstrap continuation re-proves. The tightly
    coupled E1b→E2 preparation→initial-PLAN sequence executes within this
    one logical invocation. No prompts, plan results, directories,
    credentials, SDK handles, or service-result DTOs are carried.
    """

    task: TaskScope
    profile_id: str
    expected_state_token: str

    def __post_init__(self) -> None:
        object.__setattr__(
            self, "profile_id", _require_canonical_uuid(self.profile_id, "profile_id")
        )
        object.__setattr__(
            self,
            "expected_state_token",
            _require_canonical_uuid(self.expected_state_token, "expected_state_token"),
        )


@dataclass(frozen=True, slots=True)
class TaskPlanningReviewPayload:
    """The planning REVIEW dispatch/decision operation payload (F5a).

    The exact durable Workspace/Task units with the consuming Reviewer
    role, the accountable Owner Profile identity, and the exact Task
    ``state_token`` authorizing this review round (the post-plan token for
    the first round, the post-revise token for a later round). The round's
    E3a dispatch→E3b decision sequence executes within this one logical
    invocation; the reviewed PlanRevision and reserved ReviewIteration are
    re-derived from durable state, never wire-carried.
    """

    task: TaskScope
    profile_id: str
    expected_state_token: str

    def __post_init__(self) -> None:
        object.__setattr__(
            self, "profile_id", _require_canonical_uuid(self.profile_id, "profile_id")
        )
        object.__setattr__(
            self,
            "expected_state_token",
            _require_canonical_uuid(self.expected_state_token, "expected_state_token"),
        )


@dataclass(frozen=True, slots=True)
class TaskAuthorizedImplementationPayload:
    """The E5-authorized initial implementation operation payload (F5a).

    The exact durable Workspace/Task units with the consuming Producer
    role, the accountable Owner Profile identity, and the exact post-E5
    approval ``state_token``. The approved IMPLEMENTATION_AUTHORIZATION
    gate and its exact-plan subject are re-derived from durable state by
    the executing service (never caller-supplied); the payload carries no
    gate resolution, plan text, review content, or runtime handles.
    """

    task: TaskScope
    profile_id: str
    expected_state_token: str

    def __post_init__(self) -> None:
        object.__setattr__(
            self, "profile_id", _require_canonical_uuid(self.profile_id, "profile_id")
        )
        object.__setattr__(
            self,
            "expected_state_token",
            _require_canonical_uuid(self.expected_state_token, "expected_state_token"),
        )


@dataclass(frozen=True, slots=True)
class TaskAuthorizedImplementationRetryPayload:
    """The F4-authorized implementation-retry operation payload (F5a/F5c).

    The exact durable Workspace/Task units with the consuming Producer role,
    the accountable Owner Profile identity the E6 discipline requires, the
    exact expected Task ``state_token``, and the existing F4 retry-attempt
    UUID whose authorization this invocation consumes. The F4 attempt, its
    episode, the predecessor proof, the approved gate/plan handoff, and the
    original Producer binding are all re-derived from committed state by the
    executing service (never caller-supplied); the payload carries no
    PlanRevision/result, message body, ``retryable`` flag, or runtime
    handle, and no new Owner approval or Agent session is involved.
    """

    task: TaskScope
    profile_id: str
    retry_attempt_id: str
    expected_state_token: str

    def __post_init__(self) -> None:
        object.__setattr__(
            self, "profile_id", _require_canonical_uuid(self.profile_id, "profile_id")
        )
        object.__setattr__(
            self,
            "retry_attempt_id",
            _require_canonical_uuid(self.retry_attempt_id, "retry_attempt_id"),
        )
        object.__setattr__(
            self,
            "expected_state_token",
            _require_canonical_uuid(self.expected_state_token, "expected_state_token"),
        )


@dataclass(frozen=True, slots=True)
class TaskAuthorizedMergeRetryPayload:
    """The F4-authorized exact-head merge-retry operation payload (F5d).

    The exact durable Workspace/Task units with the review-context scope
    role, the accountable Owner Profile identity the merge credential
    requires, the exact expected Task ``state_token``, and the existing F4
    retry-attempt UUID whose authorization this invocation consumes. The F4
    attempt, its episode, the approved merge-decision gate, the canonical PR,
    and the accepted head are all re-derived from committed state by the
    executing service (never caller-supplied); the payload carries no head
    SHA, PR number, ``retryable`` flag, or client handle, and no new Owner
    approval or Agent session is involved.
    """

    task: TaskScope
    profile_id: str
    retry_attempt_id: str
    expected_state_token: str

    def __post_init__(self) -> None:
        object.__setattr__(
            self, "profile_id", _require_canonical_uuid(self.profile_id, "profile_id")
        )
        object.__setattr__(
            self,
            "retry_attempt_id",
            _require_canonical_uuid(self.retry_attempt_id, "retry_attempt_id"),
        )
        object.__setattr__(
            self,
            "expected_state_token",
            _require_canonical_uuid(self.expected_state_token, "expected_state_token"),
        )


@dataclass(frozen=True, slots=True)
class TaskPrAuthorizationRetryPayload:
    """The F5f presentation-retry operation payload (issue #269).

    The exact durable Workspace/Task units with the consuming Producer
    role (the presentation continues the implementation phase's authorized
    continuation — no runtime session is consumed), the accountable Owner
    Profile identity, the exact F4-authorized retry attempt, and the exact
    post-E6 ``state_token`` the failed presentation proved current. No gate,
    head, or branch claim is carried; the guarded presentation re-derives
    every subject from durable state.
    """

    task: TaskScope
    profile_id: str
    retry_attempt_id: str
    expected_state_token: str

    def __post_init__(self) -> None:
        object.__setattr__(
            self, "profile_id", _require_canonical_uuid(self.profile_id, "profile_id")
        )
        object.__setattr__(
            self,
            "retry_attempt_id",
            _require_canonical_uuid(self.retry_attempt_id, "retry_attempt_id"),
        )
        object.__setattr__(
            self,
            "expected_state_token",
            _require_canonical_uuid(self.expected_state_token, "expected_state_token"),
        )


@dataclass(frozen=True, slots=True)
class TaskPrCompositionPayload:
    """The E8 PR composition/publish operation payload (F5a).

    The exact durable Workspace/Task units with the consuming Producer
    role, the accountable Owner Profile identity (the approving Profile
    whose user authorization publishes — never substituted), the exact
    current PR_AUTHORIZATION OwnerGate, and the exact post-E7 approval
    ``state_token``. No PR presentation, head SHA, or publication result
    is carried; the canonical PR is created from durable facts.
    """

    task: TaskScope
    profile_id: str
    owner_gate_id: str
    expected_state_token: str

    def __post_init__(self) -> None:
        object.__setattr__(
            self, "profile_id", _require_canonical_uuid(self.profile_id, "profile_id")
        )
        object.__setattr__(
            self, "owner_gate_id", _require_canonical_uuid(self.owner_gate_id, "owner_gate_id")
        )
        object.__setattr__(
            self,
            "expected_state_token",
            _require_canonical_uuid(self.expected_state_token, "expected_state_token"),
        )


@dataclass(frozen=True, slots=True)
class TaskPrReviewPayload:
    """The PR REVIEW dispatch/decision operation payload (F5a).

    The exact durable Workspace/Task units with the consuming Reviewer
    role, the accountable Owner Profile identity, the exact canonical
    ``TaskPullRequest`` record the dispatch addresses, and the exact
    post-publication ``state_token``. The reconciled head SHA is resolved
    authoritatively by the executing service; the remediation continuation
    stays within this one logical invocation.
    """

    task: TaskScope
    profile_id: str
    task_pull_request_id: str
    expected_state_token: str

    def __post_init__(self) -> None:
        object.__setattr__(
            self, "profile_id", _require_canonical_uuid(self.profile_id, "profile_id")
        )
        object.__setattr__(
            self,
            "task_pull_request_id",
            _require_canonical_uuid(self.task_pull_request_id, "task_pull_request_id"),
        )
        object.__setattr__(
            self,
            "expected_state_token",
            _require_canonical_uuid(self.expected_state_token, "expected_state_token"),
        )


@dataclass(frozen=True, slots=True)
class WebhookDeliveryDispatchPayload:
    """The GitHub webhook delivery-dispatch operation payload (Task-less).

    Exactly one safe stable identity — the provider delivery GUID — the
    owning dispatch service reloads durable state by. No Task, role, or
    Connection fields are invented for this kind.
    """

    delivery_guid: str

    def __post_init__(self) -> None:
        _require_non_empty_string(self.delivery_guid, "delivery_guid")


@dataclass(frozen=True, slots=True)
class ReconciliationSweepPayload:
    """The GitHub reconciliation/recovery sweep operation payload (Task-less).

    At most the scheduling position (``sweep_tick``, a validated
    non-negative integer, or omitted for the operator's full sweep). The
    tick is a scheduling position only, never workflow truth: every sweep
    unit re-derives its subject from durable state.
    """

    sweep_tick: int | None = None

    def __post_init__(self) -> None:
        if self.sweep_tick is None:
            return
        if isinstance(self.sweep_tick, bool) or not isinstance(self.sweep_tick, int):
            raise InvalidInvocationError(
                "the invocation field 'sweep_tick' must be a non-negative integer"
            )
        if self.sweep_tick < 0:
            raise InvalidInvocationError(
                "the invocation field 'sweep_tick' must be a non-negative integer"
            )


@dataclass(frozen=True, slots=True)
class TaskSessionResumePayload:
    """The F8g authorized session-resume continuation payload (issue #292).

    The exact durable Workspace/Task units, the accountable Owner Profile
    identity, the one-shot session-resume record identity, and the
    authorization-established restored Task status (the expected-currentness
    seed the coordinator supplies to the Level 2 triage) and authorization
    token. The record's routing context is re-derived from durable state by
    the continuation (never caller-supplied); the payload carries no gate
    resolution, routing decisions, or runtime handles.
    """

    task: TaskScope
    profile_id: str
    task_resume_intent_id: str
    restored_status: str
    expected_state_token: str

    def __post_init__(self) -> None:
        object.__setattr__(
            self, "profile_id", _require_canonical_uuid(self.profile_id, "profile_id")
        )
        object.__setattr__(
            self,
            "task_resume_intent_id",
            _require_canonical_uuid(self.task_resume_intent_id, "task_resume_intent_id"),
        )
        object.__setattr__(
            self,
            "expected_state_token",
            _require_canonical_uuid(self.expected_state_token, "expected_state_token"),
        )


OperationPayload = (
    WebhookDeliveryDispatchPayload
    | ReconciliationSweepPayload
    | TaskPlanningBootstrapAndInitialPlanPayload
    | TaskPlanningReviewPayload
    | TaskAuthorizedImplementationPayload
    | TaskAuthorizedImplementationRetryPayload
    | TaskAuthorizedMergeRetryPayload
    | TaskPrAuthorizationRetryPayload
    | TaskPrCompositionPayload
    | TaskPrReviewPayload
    | TaskSessionResumePayload
)


_ALLOWED_PAYLOAD_FIELDS: Mapping[OperationKind, frozenset[str]] = {
    OperationKind.GITHUB_WEBHOOK_DELIVERY_DISPATCH: frozenset({"delivery_guid"}),
    OperationKind.GITHUB_RECONCILIATION_SWEEP: frozenset({"sweep_tick"}),
    OperationKind.TASK_PLANNING_BOOTSTRAP_AND_INITIAL_PLAN: frozenset(
        {"task", "profile_id", "expected_state_token"}
    ),
    OperationKind.TASK_PLANNING_REVIEW: frozenset({"task", "profile_id", "expected_state_token"}),
    OperationKind.TASK_AUTHORIZED_IMPLEMENTATION: frozenset(
        {"task", "profile_id", "expected_state_token"}
    ),
    OperationKind.TASK_AUTHORIZED_IMPLEMENTATION_RETRY: frozenset(
        {"task", "profile_id", "retry_attempt_id", "expected_state_token"}
    ),
    OperationKind.TASK_AUTHORIZED_MERGE_RETRY: frozenset(
        {"task", "profile_id", "retry_attempt_id", "expected_state_token"}
    ),
    OperationKind.TASK_PR_AUTHORIZATION_RETRY: frozenset(
        {"task", "profile_id", "retry_attempt_id", "expected_state_token"}
    ),
    OperationKind.TASK_PR_COMPOSITION: frozenset(
        {"task", "profile_id", "owner_gate_id", "expected_state_token"}
    ),
    OperationKind.TASK_PR_REVIEW: frozenset(
        {"task", "profile_id", "task_pull_request_id", "expected_state_token"}
    ),
    OperationKind.TASK_SESSION_RESUME: frozenset(
        {"task", "profile_id", "task_resume_intent_id", "restored_status", "expected_state_token"}
    ),
}

# The typed payload each operation kind carries; construction-time
# correspondence keeps every mode's invocations equivalent and strictly
# validated (issue #86).
_PAYLOAD_TYPES: Mapping[OperationKind, type[OperationPayload]] = {
    OperationKind.GITHUB_WEBHOOK_DELIVERY_DISPATCH: WebhookDeliveryDispatchPayload,
    OperationKind.GITHUB_RECONCILIATION_SWEEP: ReconciliationSweepPayload,
    OperationKind.TASK_PLANNING_BOOTSTRAP_AND_INITIAL_PLAN: (
        TaskPlanningBootstrapAndInitialPlanPayload
    ),
    OperationKind.TASK_PLANNING_REVIEW: TaskPlanningReviewPayload,
    OperationKind.TASK_AUTHORIZED_IMPLEMENTATION: TaskAuthorizedImplementationPayload,
    OperationKind.TASK_AUTHORIZED_IMPLEMENTATION_RETRY: TaskAuthorizedImplementationRetryPayload,
    OperationKind.TASK_AUTHORIZED_MERGE_RETRY: TaskAuthorizedMergeRetryPayload,
    OperationKind.TASK_PR_AUTHORIZATION_RETRY: TaskPrAuthorizationRetryPayload,
    OperationKind.TASK_PR_COMPOSITION: TaskPrCompositionPayload,
    OperationKind.TASK_PR_REVIEW: TaskPrReviewPayload,
    OperationKind.TASK_SESSION_RESUME: TaskSessionResumePayload,
}


def _decode_webhook_delivery_dispatch_payload(raw: object) -> WebhookDeliveryDispatchPayload:
    fields = _require_exact_fields(
        raw, _ALLOWED_PAYLOAD_FIELDS[OperationKind.GITHUB_WEBHOOK_DELIVERY_DISPATCH]
    )
    return WebhookDeliveryDispatchPayload(
        delivery_guid=_require_non_empty_string(fields["delivery_guid"], "delivery_guid")
    )


def _decode_reconciliation_sweep_payload(raw: object) -> ReconciliationSweepPayload:
    fields = _require_exact_fields(
        raw, _ALLOWED_PAYLOAD_FIELDS[OperationKind.GITHUB_RECONCILIATION_SWEEP]
    )
    tick = fields["sweep_tick"]
    if tick is None:
        return ReconciliationSweepPayload(sweep_tick=None)
    if isinstance(tick, bool) or not isinstance(tick, int) or tick < 0:
        raise InvalidInvocationError(
            "the invocation field 'sweep_tick' must be a non-negative integer"
        )
    return ReconciliationSweepPayload(sweep_tick=tick)


def _decode_task_planning_bootstrap_payload(
    raw: object,
) -> TaskPlanningBootstrapAndInitialPlanPayload:
    kind = OperationKind.TASK_PLANNING_BOOTSTRAP_AND_INITIAL_PLAN
    fields = _require_exact_fields(raw, _ALLOWED_PAYLOAD_FIELDS[kind])
    return TaskPlanningBootstrapAndInitialPlanPayload(
        task=_decode_task_scope_for_kind(fields["task"], kind),
        profile_id=_require_canonical_uuid(fields["profile_id"], "profile_id"),
        expected_state_token=_require_canonical_uuid(
            fields["expected_state_token"], "expected_state_token"
        ),
    )


def _decode_task_planning_review_payload(raw: object) -> TaskPlanningReviewPayload:
    kind = OperationKind.TASK_PLANNING_REVIEW
    fields = _require_exact_fields(raw, _ALLOWED_PAYLOAD_FIELDS[kind])
    return TaskPlanningReviewPayload(
        task=_decode_task_scope_for_kind(fields["task"], kind),
        profile_id=_require_canonical_uuid(fields["profile_id"], "profile_id"),
        expected_state_token=_require_canonical_uuid(
            fields["expected_state_token"], "expected_state_token"
        ),
    )


def _decode_task_authorized_implementation_payload(
    raw: object,
) -> TaskAuthorizedImplementationPayload:
    kind = OperationKind.TASK_AUTHORIZED_IMPLEMENTATION
    fields = _require_exact_fields(raw, _ALLOWED_PAYLOAD_FIELDS[kind])
    return TaskAuthorizedImplementationPayload(
        task=_decode_task_scope_for_kind(fields["task"], kind),
        profile_id=_require_canonical_uuid(fields["profile_id"], "profile_id"),
        expected_state_token=_require_canonical_uuid(
            fields["expected_state_token"], "expected_state_token"
        ),
    )


def _decode_task_authorized_implementation_retry_payload(
    raw: object,
) -> TaskAuthorizedImplementationRetryPayload:
    kind = OperationKind.TASK_AUTHORIZED_IMPLEMENTATION_RETRY
    fields = _require_exact_fields(raw, _ALLOWED_PAYLOAD_FIELDS[kind])
    return TaskAuthorizedImplementationRetryPayload(
        task=_decode_task_scope_for_kind(fields["task"], kind),
        profile_id=_require_canonical_uuid(fields["profile_id"], "profile_id"),
        retry_attempt_id=_require_canonical_uuid(fields["retry_attempt_id"], "retry_attempt_id"),
        expected_state_token=_require_canonical_uuid(
            fields["expected_state_token"], "expected_state_token"
        ),
    )


def _decode_task_authorized_merge_retry_payload(
    raw: object,
) -> TaskAuthorizedMergeRetryPayload:
    kind = OperationKind.TASK_AUTHORIZED_MERGE_RETRY
    fields = _require_exact_fields(raw, _ALLOWED_PAYLOAD_FIELDS[kind])
    return TaskAuthorizedMergeRetryPayload(
        task=_decode_task_scope_for_kind(fields["task"], kind),
        profile_id=_require_canonical_uuid(fields["profile_id"], "profile_id"),
        retry_attempt_id=_require_canonical_uuid(fields["retry_attempt_id"], "retry_attempt_id"),
        expected_state_token=_require_canonical_uuid(
            fields["expected_state_token"], "expected_state_token"
        ),
    )


def _decode_task_pr_authorization_retry_payload(
    raw: object,
) -> TaskPrAuthorizationRetryPayload:
    kind = OperationKind.TASK_PR_AUTHORIZATION_RETRY
    fields = _require_exact_fields(raw, _ALLOWED_PAYLOAD_FIELDS[kind])
    return TaskPrAuthorizationRetryPayload(
        task=_decode_task_scope_for_kind(fields["task"], kind),
        profile_id=_require_canonical_uuid(fields["profile_id"], "profile_id"),
        retry_attempt_id=_require_canonical_uuid(fields["retry_attempt_id"], "retry_attempt_id"),
        expected_state_token=_require_canonical_uuid(
            fields["expected_state_token"], "expected_state_token"
        ),
    )


def _decode_task_pr_composition_payload(raw: object) -> TaskPrCompositionPayload:
    kind = OperationKind.TASK_PR_COMPOSITION
    fields = _require_exact_fields(raw, _ALLOWED_PAYLOAD_FIELDS[kind])
    return TaskPrCompositionPayload(
        task=_decode_task_scope_for_kind(fields["task"], kind),
        profile_id=_require_canonical_uuid(fields["profile_id"], "profile_id"),
        owner_gate_id=_require_canonical_uuid(fields["owner_gate_id"], "owner_gate_id"),
        expected_state_token=_require_canonical_uuid(
            fields["expected_state_token"], "expected_state_token"
        ),
    )


def _decode_task_pr_review_payload(raw: object) -> TaskPrReviewPayload:
    kind = OperationKind.TASK_PR_REVIEW
    fields = _require_exact_fields(raw, _ALLOWED_PAYLOAD_FIELDS[kind])
    return TaskPrReviewPayload(
        task=_decode_task_scope_for_kind(fields["task"], kind),
        profile_id=_require_canonical_uuid(fields["profile_id"], "profile_id"),
        task_pull_request_id=_require_canonical_uuid(
            fields["task_pull_request_id"], "task_pull_request_id"
        ),
        expected_state_token=_require_canonical_uuid(
            fields["expected_state_token"], "expected_state_token"
        ),
    )


def _decode_task_session_resume_payload(raw: object) -> TaskSessionResumePayload:
    kind = OperationKind.TASK_SESSION_RESUME
    fields = _require_exact_fields(raw, _ALLOWED_PAYLOAD_FIELDS[kind])
    return TaskSessionResumePayload(
        task=_decode_task_scope_for_kind(fields["task"], kind),
        profile_id=_require_canonical_uuid(fields["profile_id"], "profile_id"),
        task_resume_intent_id=_require_canonical_uuid(
            fields["task_resume_intent_id"], "task_resume_intent_id"
        ),
        restored_status=_require_non_empty_string(fields["restored_status"], "restored_status"),
        expected_state_token=_require_canonical_uuid(
            fields["expected_state_token"], "expected_state_token"
        ),
    )


_PAYLOAD_DECODERS: Mapping[OperationKind, Callable[[object], OperationPayload]] = {
    OperationKind.GITHUB_WEBHOOK_DELIVERY_DISPATCH: _decode_webhook_delivery_dispatch_payload,
    OperationKind.GITHUB_RECONCILIATION_SWEEP: _decode_reconciliation_sweep_payload,
    OperationKind.TASK_PLANNING_BOOTSTRAP_AND_INITIAL_PLAN: _decode_task_planning_bootstrap_payload,
    OperationKind.TASK_PLANNING_REVIEW: _decode_task_planning_review_payload,
    OperationKind.TASK_AUTHORIZED_IMPLEMENTATION: _decode_task_authorized_implementation_payload,
    OperationKind.TASK_AUTHORIZED_IMPLEMENTATION_RETRY: (
        _decode_task_authorized_implementation_retry_payload
    ),
    OperationKind.TASK_AUTHORIZED_MERGE_RETRY: _decode_task_authorized_merge_retry_payload,
    OperationKind.TASK_PR_AUTHORIZATION_RETRY: _decode_task_pr_authorization_retry_payload,
    OperationKind.TASK_PR_COMPOSITION: _decode_task_pr_composition_payload,
    OperationKind.TASK_PR_REVIEW: _decode_task_pr_review_payload,
    OperationKind.TASK_SESSION_RESUME: _decode_task_session_resume_payload,
}


def _encode_payload(kind: OperationKind, payload: OperationPayload) -> dict[str, object]:
    """Return the stable JSON-safe representation of one typed payload."""
    if isinstance(payload, WebhookDeliveryDispatchPayload) and (
        kind is OperationKind.GITHUB_WEBHOOK_DELIVERY_DISPATCH
    ):
        return {"delivery_guid": payload.delivery_guid}
    if isinstance(payload, ReconciliationSweepPayload) and (
        kind is OperationKind.GITHUB_RECONCILIATION_SWEEP
    ):
        return {"sweep_tick": payload.sweep_tick}
    if isinstance(payload, TaskPlanningBootstrapAndInitialPlanPayload) and (
        kind is OperationKind.TASK_PLANNING_BOOTSTRAP_AND_INITIAL_PLAN
    ):
        return {
            "task": payload.task.encode(),
            "profile_id": payload.profile_id,
            "expected_state_token": payload.expected_state_token,
        }
    if isinstance(payload, TaskPlanningReviewPayload) and (
        kind is OperationKind.TASK_PLANNING_REVIEW
    ):
        return {
            "task": payload.task.encode(),
            "profile_id": payload.profile_id,
            "expected_state_token": payload.expected_state_token,
        }
    if isinstance(payload, TaskAuthorizedImplementationPayload) and (
        kind is OperationKind.TASK_AUTHORIZED_IMPLEMENTATION
    ):
        return {
            "task": payload.task.encode(),
            "profile_id": payload.profile_id,
            "expected_state_token": payload.expected_state_token,
        }
    if isinstance(payload, TaskAuthorizedImplementationRetryPayload) and (
        kind is OperationKind.TASK_AUTHORIZED_IMPLEMENTATION_RETRY
    ):
        return {
            "task": payload.task.encode(),
            "profile_id": payload.profile_id,
            "retry_attempt_id": payload.retry_attempt_id,
            "expected_state_token": payload.expected_state_token,
        }
    if isinstance(payload, TaskAuthorizedMergeRetryPayload) and (
        kind is OperationKind.TASK_AUTHORIZED_MERGE_RETRY
    ):
        return {
            "task": payload.task.encode(),
            "profile_id": payload.profile_id,
            "retry_attempt_id": payload.retry_attempt_id,
            "expected_state_token": payload.expected_state_token,
        }
    if isinstance(payload, TaskPrAuthorizationRetryPayload) and (
        kind is OperationKind.TASK_PR_AUTHORIZATION_RETRY
    ):
        return {
            "task": payload.task.encode(),
            "profile_id": payload.profile_id,
            "retry_attempt_id": payload.retry_attempt_id,
            "expected_state_token": payload.expected_state_token,
        }
    if isinstance(payload, TaskPrCompositionPayload) and (
        kind is OperationKind.TASK_PR_COMPOSITION
    ):
        return {
            "task": payload.task.encode(),
            "profile_id": payload.profile_id,
            "owner_gate_id": payload.owner_gate_id,
            "expected_state_token": payload.expected_state_token,
        }
    if isinstance(payload, TaskPrReviewPayload) and kind is OperationKind.TASK_PR_REVIEW:
        return {
            "task": payload.task.encode(),
            "profile_id": payload.profile_id,
            "task_pull_request_id": payload.task_pull_request_id,
            "expected_state_token": payload.expected_state_token,
        }
    raise InvalidInvocationError(
        f"the payload type does not match the operation kind {kind.value!r}"
    )


@dataclass(frozen=True, slots=True)
class OperationInvocation:
    """One strictly validated, serializable logical-operation invocation.

    ``kind`` is the closed discriminator; ``payload`` is the kind-specific
    typed payload object produced by its strict decoder. Serialization is
    the stable JSON wire form; decoding is strict and happens before any
    service/runtime effect.
    """

    kind: OperationKind
    payload: OperationPayload

    def __post_init__(self) -> None:
        # Construction-time correspondence: an invocation declaring one kind
        # can never carry another kind's payload, so every execution mode
        # receives equivalent, strictly validated invocations.
        if not isinstance(self.kind, OperationKind):
            raise InvalidInvocationError("the invocation kind must be an operation kind")
        if not isinstance(self.payload, _PAYLOAD_TYPES[self.kind]):
            raise InvalidInvocationError(
                f"the payload type does not match the operation kind {self.kind.value!r}"
            )
        # Construction-time role correspondence (issue #253): a Task-workflow
        # payload whose consuming role does not match its kind is rejected at
        # construction — the same fail-closed validation the strict decoder
        # applies on the wire — so directly constructed regular-mode
        # invocations can never bypass it.
        expected_role = _TASK_KIND_ROLES.get(self.kind)
        if expected_role is not None:
            task_scope = getattr(self.payload, "task", None)
            if not isinstance(task_scope, TaskScope) or task_scope.role is not expected_role:
                raise InvalidInvocationError(
                    "the invocation field 'role' does not match the operation kind "
                    f"{self.kind.value!r}"
                )

    def encode(self) -> str:
        """Encode to the stable JSON wire form."""
        return json.dumps(
            {"kind": self.kind.value, "payload": _encode_payload(self.kind, self.payload)},
            sort_keys=True,
        )


def decode_operation_invocation(raw: str | bytes | Mapping[str, object]) -> OperationInvocation:
    """Strictly decode a wire invocation before any service/runtime effect.

    Accepts the encoded JSON string (the RQ wire form), its raw bytes, or an
    already-parsed mapping. Unknown kinds, unexpected top-level or payload
    fields, malformed types, and missing required fields are rejected with
    :class:`InvalidInvocationError`.
    """
    document: object
    if isinstance(raw, Mapping):
        document = raw
    else:
        try:
            document = json.loads(raw)
        except (TypeError, ValueError):
            # No chaining: malformed-invocation failures stay fully sanitized
            # in reported tracebacks.
            raise InvalidInvocationError("the invocation is not valid JSON") from None
    if not isinstance(document, dict):
        raise InvalidInvocationError("the invocation must be a JSON object")
    unexpected = frozenset(document) - {"kind", "payload"}
    if unexpected:
        # Untrusted input keys are never echoed into errors.
        raise InvalidInvocationError("the invocation has unexpected fields")
    missing = {"kind", "payload"} - frozenset(document)
    if missing:
        raise InvalidInvocationError(
            f"the invocation is missing required fields: {sorted(missing)}"
        )
    raw_kind = document["kind"]
    if not isinstance(raw_kind, str) or raw_kind not in {member.value for member in OperationKind}:
        raise InvalidInvocationError(
            "the invocation field 'kind' must be one of "
            f"{sorted(member.value for member in OperationKind)}"
        )
    kind = OperationKind(raw_kind)
    payload = _PAYLOAD_DECODERS[kind](document["payload"])
    return OperationInvocation(kind=kind, payload=payload)


DatabasePoolProvider = Callable[[Settings], DatabasePool]
GitHubClientProvider = Callable[[Settings], GitHubAppClient]
BootstrapRuntimeCompositionProvider = Callable[[Settings], "TaskBootstrapRuntimeComposition"]
UserTokenResolverProvider = Callable[[Settings], "ProfileUserAccessTokenResolver"]
ReviewerNeutralDirectoryProvider = Callable[[Settings], str]
ContinuationSubmitterProvider = Callable[[Settings], "OperationSubmitter"]


class LateBoundProcessSubmitter:
    """The late-bound regular-mode continuation-submitter provider.

    Regular-mode continuation submission admits follow-up work into the
    serving process's own bounded local executor. That executor is
    constructed with the very dependencies object that resolves the
    submitter, so the provider binds the executor explicitly after
    construction (the serving lifespan owns exactly one executor per
    process, issue #86). Until bound, resolution fails closed; the binding
    carries no credentials, live handles, or workflow authority.
    """

    def __init__(self) -> None:
        self._submitter: OperationSubmitter | None = None

    def bind(self, submitter: OperationSubmitter) -> None:
        """Bind the process's own bounded local executor."""
        self._submitter = submitter

    def __call__(self, settings: Settings) -> OperationSubmitter:
        if self._submitter is None:
            raise ExecutionModeDependencyError(
                "no process-local operation submitter is bound for continuation submission"
            )
        return self._submitter


@dataclass(frozen=True, slots=True)
class OperationDependencies:
    """Process-local dependency resolution for an executing process.

    Providers resolve lazily, inside the executing process, when a handler
    requests them: the database pool is the PID-scoped process-local pool
    boundary and the GitHub client is built from deployment configuration at
    the point of use. The F5a Task-workflow providers (issue #253) extend
    this object the same way: the bootstrap runtime composition, the
    Owner-accountable Profile user-token resolver, the deployment's
    reviewer-neutral directory, and the continuation submitter are all
    resolved only inside the executing process — never before an RQ fork
    and never in a submitting HTTP request. Tests inject recording fakes
    through the provider fields. Dependencies carry no credentials, live
    handles, or customer content, and no universal service locator exists:
    every provider is an explicitly named seam.
    """

    settings: Settings
    database_pool_provider: DatabasePoolProvider | None = None
    github_client_provider: GitHubClientProvider | None = None
    bootstrap_runtime_composition_provider: BootstrapRuntimeCompositionProvider | None = None
    user_token_resolver_provider: UserTokenResolverProvider | None = None
    reviewer_neutral_directory_provider: ReviewerNeutralDirectoryProvider | None = None
    continuation_submitter_provider: ContinuationSubmitterProvider | None = None

    def database_pool(self) -> DatabasePool:
        """Resolve the executing process's database pool."""
        if self.database_pool_provider is None:
            raise ExecutionModeDependencyError(
                "no database pool provider is configured for operation dependencies"
            )
        return self.database_pool_provider(self.settings)

    def github_client(self) -> GitHubAppClient:
        """Resolve the executing process's GitHub App client."""
        if self.github_client_provider is None:
            raise ExecutionModeDependencyError(
                "no GitHub client provider is configured for operation dependencies"
            )
        return self.github_client_provider(self.settings)

    def task_bootstrap_runtime_composition(self) -> TaskBootstrapRuntimeComposition:
        """Resolve the executing process's bootstrap runtime composition."""
        if self.bootstrap_runtime_composition_provider is None:
            raise ExecutionModeDependencyError(
                "no bootstrap runtime composition provider is configured for operation dependencies"
            )
        return self.bootstrap_runtime_composition_provider(self.settings)

    def user_token_resolver(self) -> ProfileUserAccessTokenResolver:
        """Resolve the executing process's Profile user-token resolver."""
        if self.user_token_resolver_provider is None:
            raise ExecutionModeDependencyError(
                "no user token resolver provider is configured for operation dependencies"
            )
        return self.user_token_resolver_provider(self.settings)

    def reviewer_neutral_directory(self) -> str:
        """Resolve the executing process's reviewer-neutral directory."""
        if self.reviewer_neutral_directory_provider is None:
            raise ExecutionModeDependencyError(
                "no reviewer-neutral directory provider is configured for operation dependencies"
            )
        return self.reviewer_neutral_directory_provider(self.settings)

    def continuation_submitter(self) -> OperationSubmitter:
        """Resolve the executing process's continuation-operation submitter."""
        if self.continuation_submitter_provider is None:
            raise ExecutionModeDependencyError(
                "no continuation submitter provider is configured for operation dependencies"
            )
        return self.continuation_submitter_provider(self.settings)

    def bind_regular_process_submitter(self, submitter: OperationSubmitter) -> None:
        """Bind the serving process's own executor as the continuation submitter.

        Regular-mode wiring only: the serving lifespan binds the one bounded
        local executor it constructed after the dependencies object exists.
        A provider that is not the late-bound process seam is a wiring error.
        """
        provider = self.continuation_submitter_provider
        if not isinstance(provider, LateBoundProcessSubmitter):
            raise ExecutionModeDependencyError(
                "operation dependencies are not wired with the late-bound "
                "regular-mode process submitter"
            )
        provider.bind(submitter)


def build_default_operation_dependencies(settings: Settings) -> OperationDependencies:
    """Wire the real process-local providers for an executing process.

    The Task-workflow providers come from the F5a composition module; the
    import is deferred because that module imports this one. In regular
    mode the continuation submitter is the late-bound process seam the
    serving lifespan binds to its executor; in queue mode each continuation
    enqueues through the canonical queue seam.
    """
    from openorc.workers.task_workflow import (
        default_bootstrap_runtime_composition_provider,
        default_reviewer_neutral_directory_provider,
        default_user_token_resolver_provider,
    )

    continuation_provider: ContinuationSubmitterProvider
    if settings.execution_mode is ExecutionMode.REGULAR:
        continuation_provider = LateBoundProcessSubmitter()
    else:
        continuation_provider = RQQueueOperationSubmission
    return OperationDependencies(
        settings=settings,
        database_pool_provider=get_database_pool,
        github_client_provider=HttpGitHubAppClient.from_settings,
        bootstrap_runtime_composition_provider=default_bootstrap_runtime_composition_provider,
        user_token_resolver_provider=default_user_token_resolver_provider,
        reviewer_neutral_directory_provider=default_reviewer_neutral_directory_provider,
        continuation_submitter_provider=continuation_provider,
    )


class OperationHandler(Protocol):
    """One operation-kind handler.

    The payload object type is fixed by the kind's typed payload decoder;
    the handler invokes the existing shared application service with the
    resolved process-local dependencies. The handler never decides
    Task-state, currentness, or authorization — that judgment belongs to
    the application service.
    """

    def __call__(self, payload: Any, *, dependencies: OperationDependencies) -> None: ...


def build_production_handlers() -> Mapping[OperationKind, OperationHandler]:
    """The explicit kind-to-handler table for production executing processes.

    F5a (issue #253) registers the Task-workflow handlers, which invoke the
    existing application services directly with process-local dependencies
    through the single composition seam in ``openorc.workers.task_workflow``.
    The two GitHub delivery/reconciliation kinds remain deliberately
    unregistered until F9 (issue #255), so dispatch still fails closed for
    them. The existing RQ job entrypoints are separate transports and
    remain unchanged.
    """
    from openorc.workers.task_workflow import build_task_workflow_handlers

    return build_task_workflow_handlers()


def invoke_operation(
    invocation: OperationInvocation,
    *,
    handlers: Mapping[OperationKind, OperationHandler],
    dependencies: OperationDependencies,
) -> None:
    """Dispatch one decoded invocation to its registered handler.

    The kind-to-handler table is explicit and small. Dispatch fails closed
    with :class:`InvalidInvocationError` before any effect when the kind has
    no registered handler. The executing process resolves process-local
    dependencies; tests inject both handlers and dependencies.
    """
    with application_span(
        _TRACER_SCOPE, f"invocation.invoke_operation.{invocation.kind.value}"
    ) as span:
        annotate_span(span, operation=invocation.kind.value)
        handler = handlers.get(invocation.kind)
        if handler is None:
            raise InvalidInvocationError(
                f"no handler is registered for operation kind {invocation.kind.value!r}"
            )
        handler(invocation.payload, dependencies=dependencies)


class SubmissionOutcome(StrEnum):
    """The explicit submission outcome of one logical operation.

    ``accepted_for_delivery`` — the transport took the invocation. This is
    never completion, successful external effect, or retry authorization.
    ``known_refused`` — a definitive refusal before acceptance with a typed
    reason. ``uncertain`` — the submission outcome is unknown (a timeout
    after sending is indistinguishable from a connect failure); never
    converted into a known non-delivery.
    """

    ACCEPTED_FOR_DELIVERY = "accepted_for_delivery"
    KNOWN_REFUSED = "known_refused"
    UNCERTAIN = "uncertain"


class SubmissionRefusalReason(StrEnum):
    """Typed reasons behind a known submission refusal."""

    NO_HANDLER_FOR_KIND = "no_handler_for_kind"
    EXECUTOR_SATURATED = "executor_saturated"
    EXECUTOR_SHUTTING_DOWN = "executor_shutting_down"
    QUEUE_UNAVAILABLE = "queue_unavailable"
    INVALID_INVOCATION = "invalid_invocation"


@dataclass(frozen=True, slots=True)
class SubmissionResult:
    """The typed result of one submission attempt."""

    outcome: SubmissionOutcome
    refusal_reason: SubmissionRefusalReason | None = None

    @classmethod
    def accepted(cls) -> SubmissionResult:
        """The transport accepted the invocation for delivery."""
        return cls(outcome=SubmissionOutcome.ACCEPTED_FOR_DELIVERY)

    @classmethod
    def refused(cls, reason: SubmissionRefusalReason) -> SubmissionResult:
        """A definitive refusal before acceptance, with its typed reason."""
        return cls(outcome=SubmissionOutcome.KNOWN_REFUSED, refusal_reason=reason)

    @classmethod
    def uncertain(cls) -> SubmissionResult:
        """The submission outcome is unknown."""
        return cls(outcome=SubmissionOutcome.UNCERTAIN)


class OperationSubmitter(Protocol):
    """The transport-facing submission seam for logical operations.

    Implemented by the bounded local executor in regular mode and by the
    RQ queue submission in queue mode. The seam owns transport mechanics
    and outcome classification only: no Task-state judgment, no
    authorization decision, no durable claims.
    """

    def submit(self, invocation: OperationInvocation) -> SubmissionResult: ...


class RQQueueOperationSubmission:
    """The queue-mode RQ submission seam (transport-owned queue mechanics).

    The connection is created per submission: a refused invocation never
    touches the queue backend at all. Timeout and connection errors are
    classified as UNCERTAIN — a timeout after sending is indistinguishable
    from a connect failure — while other Redis errors and a missing
    explicitly configured ``VALKEY_URL`` are definitive refusals. Queue and
    job IDs are never operation authority.
    """

    def __init__(self, settings: Settings) -> None:
        self._settings = settings

    def submit(self, invocation: OperationInvocation) -> SubmissionResult:
        try:
            encoded = invocation.encode()
        except (InvalidInvocationError, TypeError, ValueError) as error:
            logger.warning(
                "operation submission refused: the invocation cannot be encoded (error type %s)",
                type(error).__name__,
            )
            return SubmissionResult.refused(SubmissionRefusalReason.INVALID_INVOCATION)
        try:
            client = create_redis_client(self._settings)
            queue = rq.Queue(queue_name("default"), connection=client)
            queue.enqueue(_ENTRYPOINT_PATH, encoded)
        except ConfigurationError:
            return SubmissionResult.refused(SubmissionRefusalReason.QUEUE_UNAVAILABLE)
        except (RedisTimeoutError, RedisConnectionError):
            return SubmissionResult.uncertain()
        except RedisError:
            return SubmissionResult.refused(SubmissionRefusalReason.QUEUE_UNAVAILABLE)
        return SubmissionResult.accepted()


def build_operation_submitter(
    settings: Settings,
    *,
    local_executor: OperationSubmitter | None = None,
) -> OperationSubmitter:
    """Build the execution-mode-appropriate submission seam.

    Regular mode submits into the serving process's bounded local executor
    (required); queue mode enqueues the encoded wire invocation into the
    canonical default queue. The transport itself makes no Task-state or
    authorization decision.
    """
    if settings.execution_mode is ExecutionMode.REGULAR:
        if local_executor is None:
            raise ExecutionModeDependencyError(
                "regular execution mode requires the serving process's bounded local executor"
            )
        return local_executor
    return RQQueueOperationSubmission(settings)


def execute_operation_job(encoded_invocation: str) -> None:
    """Execute one logical operation (RQ job entrypoint, issue #86).

    The queue payload is exactly the encoded wire invocation. The
    entrypoint strictly decodes it before any effect (malformed envelopes
    raise the typed decode error unchanged), builds child-process-local
    dependencies after any fork, and calls the shared dispatcher — the same
    kind/handler mapping both modes use. Unregistered kinds fail closed.
    Every exception escaping the dispatcher is classified with the
    sanitized, unchained :class:`OperationExecutionError` — handler-raised
    typed service errors (``InvalidCommandError`` and its subclasses)
    included — so failures never turn into reported operation success and
    never leak arbitrary exception content through the raised error or its
    formatted traceback.
    """
    invocation = decode_operation_invocation(encoded_invocation)
    settings = Settings.from_env()
    with application_span(_TRACER_SCOPE, _ENTRYPOINT_SPAN_NAME) as span:
        job = rq.get_current_job()
        annotate_span(
            span,
            operation=invocation.kind.value,
            rq_job_id=job.id if job is not None else None,
        )
        try:
            invoke_operation(
                invocation,
                handlers=build_production_handlers(),
                dependencies=build_default_operation_dependencies(settings),
            )
        except Exception as error:  # noqa: BLE001 - contained with safe classification
            # Every exception escaping the dispatcher receives the sanitized,
            # unchained classification — application-service handler failures
            # included, InvalidCommandError and its subclasses included:
            # queue failure reporting formats full tracebacks, and a raw
            # typed error or any cause would carry the original (possibly
            # secret-bearing) message into reported failures. The sanitized
            # error and the type-only log line above are the entire safe
            # classification.
            logger.warning(
                "operation execution job did not complete (error type %s)",
                type(error).__name__,
            )
            raise OperationExecutionError(
                f"operation execution failed (kind={invocation.kind.value}, "
                f"error type {type(error).__name__})"
            ) from None
