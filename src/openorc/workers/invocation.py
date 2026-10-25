"""Shared logical-operation invocation and submission boundary (issue #86).

One small, typed logical-invocation and submission interface usable from
API/local and RQ worker processes. The execution mode determines only
where/how an operation is delivered; each operation handler invokes the
existing shared application service, and the service — never the delivery
mechanism — determines whether anything remains current or authorized.

Boundary rules enforced by this module:

- The operation-kind discriminator is closed (:class:`OperationKind`). New
  kinds are added explicitly by later leaves (F3a/F3b); there are no
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
- Dispatch fails closed when a kind has no registered handler. F1 registers
  no production handlers: F3a adds the typed Task-workflow handlers and F3b
  registers the GitHub delivery/reconciliation handlers that invoke the
  existing application services directly with process-local dependencies.
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
from typing import Any, Protocol
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
        raise InvalidInvocationError(
            f"the invocation payload has unexpected fields: {sorted(unexpected)}"
        )
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
    except ValueError as error:
        raise InvalidInvocationError(
            f"the invocation field {field!r} must be a UUID string"
        ) from error


@dataclass(frozen=True, slots=True)
class TaskScope:
    """Representative Task-scoped identity component (envelope machinery).

    The Task-scoped identity a Task-based operation payload carries: the
    existing durable Workspace and Task units of work plus the consuming
    workflow role. This component is the strictly validated, JSON-safe
    envelope machinery later leaves (F3a) compose into concrete Task-workflow
    operation kinds; F1 registers no Task-workflow kinds and wires no
    workflow behavior. Identifiers are canonical UUID strings and the role
    is a closed enum value — never credentials, live handles, or content.
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


OperationPayload = WebhookDeliveryDispatchPayload | ReconciliationSweepPayload

_ALLOWED_PAYLOAD_FIELDS: Mapping[OperationKind, frozenset[str]] = {
    OperationKind.GITHUB_WEBHOOK_DELIVERY_DISPATCH: frozenset({"delivery_guid"}),
    OperationKind.GITHUB_RECONCILIATION_SWEEP: frozenset({"sweep_tick"}),
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


_PAYLOAD_DECODERS: Mapping[OperationKind, Callable[[object], OperationPayload]] = {
    OperationKind.GITHUB_WEBHOOK_DELIVERY_DISPATCH: _decode_webhook_delivery_dispatch_payload,
    OperationKind.GITHUB_RECONCILIATION_SWEEP: _decode_reconciliation_sweep_payload,
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
        except (TypeError, ValueError) as error:
            raise InvalidInvocationError("the invocation is not valid JSON") from error
    if not isinstance(document, dict):
        raise InvalidInvocationError("the invocation must be a JSON object")
    unexpected = frozenset(document) - {"kind", "payload"}
    if unexpected:
        raise InvalidInvocationError(f"the invocation has unexpected fields: {sorted(unexpected)}")
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


@dataclass(frozen=True, slots=True)
class OperationDependencies:
    """Process-local dependency resolution for an executing process.

    Providers resolve lazily, inside the executing process, when a handler
    requests them: the database pool is the PID-scoped process-local pool
    boundary and the GitHub client is built from deployment configuration at
    the point of use. Later leaves extend this object (for example the
    admitted Cline connection attachment required by Task-workflow
    handlers); tests inject recording fakes. Dependencies carry no
    credentials, live handles, or customer content.
    """

    settings: Settings
    database_pool_provider: DatabasePoolProvider | None = None
    github_client_provider: GitHubClientProvider | None = None

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


def build_default_operation_dependencies(settings: Settings) -> OperationDependencies:
    """Wire the real process-local providers for an executing process."""
    return OperationDependencies(
        settings=settings,
        database_pool_provider=get_database_pool,
        github_client_provider=HttpGitHubAppClient.from_settings,
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

    F1 registers no production handlers: the table stays fail-closed until
    F3a adds the typed Task-workflow handlers and F3b registers the GitHub
    delivery/reconciliation handlers that invoke the existing application
    services directly with process-local dependencies. The existing RQ job
    entrypoints are separate transports and remain unchanged.
    """
    return {}


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
    entrypoint strictly decodes it, builds child-process-local dependencies
    after any fork, and calls the shared dispatcher — the same
    kind/handler mapping both modes use. Unregistered kinds fail closed;
    handler failures never turn into reported operation success and never
    leak arbitrary exception content through the raised error.
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
        except InvalidCommandError:
            raise
        except Exception as error:  # noqa: BLE001 - contained with safe classification
            logger.warning(
                "operation execution job did not complete (error type %s)",
                type(error).__name__,
            )
            raise OperationExecutionError(
                f"operation execution failed (kind={invocation.kind.value}, "
                f"error type {type(error).__name__})"
            ) from error
