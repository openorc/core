"""Connection-backed Cline SDK attachment and runtime composition (issue #190, D8b).

The narrow production composition entry point a trusted FastAPI
application-service or RQ worker invocation uses to acquire one
``ClineAdapter`` already attached to the exact admitted Workspace/Task/role
Connection through its current authenticated public SDK endpoint, wired to
D8a's durable construction-context resolver
(``services/cline_construction.DurableClineConstructionContextResolver``).

Boundary and discipline:

- The invoking application service remains responsible for workflow
  authorization and currentness, for the earlier #68 admission that reserved
  the binding, and for the phase-selected runtime-side ``working_directory``
  and effective mode; this composition never guesses them. D8b performs no
  session effect: it never sends initialization, never creates or
  reconstructs sessions, never settles uncertain operations, never derives
  workflow modes, and never mutates Task/session state. Fresh establishment
  remains the caller's use of ``establish_task_agent_session`` with the
  selected configuration this composition returns.
- Routing authority is the durable admitted binding's ``connection_id`` for
  both fresh and bound invocations — never the mutable role-binding route. A
  later ``WorkflowRoleBinding.connection_id`` change applies to future
  admissions only.
- Validation precedes every secret access and every dial: the exact
  Task/role's durable admitted binding and its Workspace/Task/role/lifecycle
  consistency are verified first; missing, terminal, mismatched, disabled,
  cross-Workspace, or non-Cline bindings and a mismatched caller-supplied
  external session ID fail closed before any runtime effect, and no
  cross-Workspace credential read or probing ever happens.
- One selection: the non-secret ``safe_config`` of the admitted Connection is
  read and selected once — through the #55 trusted credential resolution's
  own current read, so the dialed endpoint and the authentication credential
  are mutually current — and supplied verbatim as the fresh establishment's
  ``effective_configuration`` and as the fresh Producer's
  ``bootstrap_repository_base``. A between-reads Owner configuration edit is
  absorbed by the selection, never silently substituted.
- Attachment failures preserve the existing normalized taxonomy through the
  adapter's ``classify_backend_failure`` seam: an uncertain connect outcome
  is ``ExternalOperationUncertainError`` — never a reported known failure —
  while configuration rejection and runtime unavailability remain distinct
  typed causes of ``ExternalOperationFailedError``. Attachment-time failure
  never fabricates ``AGENT_SESSION_LOST``, never retries, and never replays.
- Bounded lifetimes: one ownership owns disposal from the moment the backend
  exists — exactly once on normal exit, known and unexpected attachment
  failures, and cancellation. Every explicit re-attachment is a new
  invocation with fresh Connection and credential reads — no in-memory secret
  survives rotation and the composition never switches Connections.
- Secrets and endpoint material never enter domain DTOs, snapshots, spans,
  messages, errors, or logs; ``ClineRemoteConfig`` is never stringified.
- No Cloud imports; no tunnel/VPN/provisioning; no application-level
  egress/SSRF policy or DNS/IP filtering (a deployment-operator obligation);
  no API routes or RQ jobs here.
"""

from __future__ import annotations

import logging
from collections.abc import Callable, Iterator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass
from enum import StrEnum
from typing import Final
from uuid import UUID

from openorc.adapters.agent_runtime.errors import AgentRuntimeUncertainOutcomeError
from openorc.adapters.cline.adapter import ClineAdapter
from openorc.adapters.cline.bridge_process import ClineBridgeProcessBackend
from openorc.adapters.cline.classification import classify_backend_failure
from openorc.adapters.cline.connection_endpoint import (
    ConnectionControlEndpointError,
    resolve_connection_control_endpoint,
)
from openorc.adapters.cline.contract import ClineSdkBackend
from openorc.adapters.cline.errors import ClineSdkBackendError
from openorc.adapters.cline.values import ClineRemoteConfig
from openorc.domain.connections import AdapterType, Connection, WorkflowRole
from openorc.domain.sessions import TaskAgentSession, TaskSessionLifecycleStatus
from openorc.observability import annotate_span, application_span
from openorc.persistence.connections import get_connection
from openorc.persistence.pool import DatabasePool
from openorc.persistence.sessions import get_task_agent_session
from openorc.services.cline_construction import DurableClineConstructionContextResolver
from openorc.services.connection_credentials import resolve_connection_runtime_credential
from openorc.services.errors import (
    ConflictError,
    ExternalOperationFailedError,
    ExternalOperationUncertainError,
    InvalidCommandError,
    NotFoundError,
)

__all__ = [
    "OPENORC_CLIENT_IDENTITY",
    "AttachedClineRuntime",
    "ClineAttachmentPhase",
    "attach_cline_runtime",
]

_SERVICE_TRACER_SCOPE = "openorc.services.cline_attachment"
_ATTACH_SPAN_NAME = "cline_attachment.attach_cline_runtime"

_CONNECT_OPERATION: Final = "connect"

# The fixed non-secret OpenOrc client identity presented at Hub registration:
# an attaching-client classification, never a credential and never per-tenant.
OPENORC_CLIENT_IDENTITY: Final = "openorc-core"

logger = logging.getLogger(__name__)


class ClineAttachmentPhase(StrEnum):
    """Which durable session lifecycle an attachment invocation addresses.

    ``FRESH`` is the establishment-side path: the exact admitted CONNECTING
    Task/role binding is about to be established. ``BOUND`` is the
    continuity-side path: the exact READY Task/role binding and its exact
    opaque external session identity already exist.
    """

    FRESH = "fresh"
    BOUND = "bound"


@dataclass(frozen=True)
class AttachedClineRuntime:
    """One acquired, already-attached Cline adapter lease result.

    ``adapter`` is the existing ``ClineAdapter`` consuming the durable
    construction-context resolver and the connected backend — ready for the
    caller's own session operations. ``selected_configuration`` is the one
    selected non-secret admitted-Connection ``safe_config`` copy on the fresh
    path: the invoking service passes it verbatim as
    ``establish_task_agent_session(..., effective_configuration=...)`` so the
    fresh construction and the durable READY snapshot agree by construction.
    It is ``None`` on the bound path, where the durable READY snapshot is
    already authoritative.
    """

    adapter: ClineAdapter
    selected_configuration: Mapping[str, object] | None


def _require_uuid_command(value: object, name: str) -> UUID:
    """Reject a malformed command identifier before any read or effect."""
    if not isinstance(value, UUID):
        raise InvalidCommandError(f"the attachment command {name} must be a UUID")
    return value


def _require_workflow_role(role: object) -> WorkflowRole:
    if not isinstance(role, WorkflowRole):
        raise InvalidCommandError("the attachment command role must be a WorkflowRole")
    return role


def _require_command_shape(
    *,
    workspace_id: UUID,
    task_id: UUID,
    role: WorkflowRole,
    phase: ClineAttachmentPhase,
    external_session_id: str | None,
    working_directory: str | None,
) -> None:
    """Validate the complete command shape before any durable read.

    A bound invocation establishes its exact opaque external session ID; a
    fresh invocation has no external identity yet and must not supply one.
    The working directory is the caller-authorized phase-selected runtime
    path, passed through verbatim and never invented here. Error messages
    name only the failing field — caller-supplied values are never echoed.
    """
    if not isinstance(phase, ClineAttachmentPhase):
        raise InvalidCommandError("the attachment command phase must be a ClineAttachmentPhase")
    if external_session_id is not None and (
        not isinstance(external_session_id, str) or not external_session_id.strip()
    ):
        raise InvalidCommandError(
            "the attachment command external_session_id must be None or a non-empty string"
        )
    if phase is ClineAttachmentPhase.FRESH:
        if external_session_id is not None:
            raise InvalidCommandError(
                "a fresh attachment invocation must not supply an external session identity"
            )
    elif external_session_id is None:
        raise InvalidCommandError(
            "a bound attachment invocation requires the exact external session identity"
        )
    if working_directory is not None and (
        not isinstance(working_directory, str) or not working_directory.strip()
    ):
        raise InvalidCommandError(
            "the attachment command working_directory must be None or a non-empty string"
        )
    _require_uuid_command(workspace_id, "workspace_id")
    _require_uuid_command(task_id, "task_id")
    _require_workflow_role(role)


def _require_admitted_binding(
    binding: TaskAgentSession | None,
    *,
    workspace_id: UUID,
    phase: ClineAttachmentPhase,
    external_session_id: str | None,
) -> TaskAgentSession:
    """Classify the durable binding against the addressed lifecycle.

    Missing or cross-Workspace bindings are the uniform not-found outcome.
    Lifecycle mismatches and a mismatched caller-supplied external session
    identity are conflicts: none of them is ever session-loss evidence, and
    none may reach a secret read or a dial.
    """
    if binding is None or binding.workspace_id != workspace_id:
        raise NotFoundError(
            "the requested task/role session binding is not available in this workspace"
        )
    if phase is ClineAttachmentPhase.FRESH:
        if binding.lifecycle_status is TaskSessionLifecycleStatus.CONNECTING:
            return binding
        if binding.lifecycle_status in (
            TaskSessionLifecycleStatus.LOST,
            TaskSessionLifecycleStatus.ENDED,
        ):
            raise ConflictError(
                "the existing task/role session binding is a terminal historical session"
            )
        raise ConflictError(
            "the addressed task/role session binding is already established; "
            "fresh construction is not applicable"
        )
    if binding.lifecycle_status is not TaskSessionLifecycleStatus.READY:
        raise ConflictError(
            "the addressed task/role session binding is not an initialized READY session"
        )
    if binding.external_session_id != external_session_id:
        raise ConflictError(
            "the supplied external session identity does not match the durable "
            "task/role session binding"
        )
    return binding


def _require_attachable_connection(
    connection: Connection | None, *, workspace_id: UUID
) -> Connection:
    """Fail closed on a missing, mis-scoped, disabled, or non-Cline Connection."""
    if connection is None or connection.workspace_id != workspace_id:
        raise NotFoundError("the requested connection is not available in this workspace")
    if connection.adapter is not AdapterType.CLINE or not connection.enabled:
        raise ConflictError(
            "the connection routed for this task/role is not an enabled Cline connection"
        )
    return connection


def _resolve_selected_endpoint(selection: Mapping[str, object]) -> str:
    """Resolve the validated control endpoint of the selected configuration.

    A missing endpoint is a stored-but-unattachable Connection, never a
    fabricated default; a malformed endpoint reuses the #188 parser's safe
    aspect-naming message. Both fail closed before any dial.
    """
    try:
        endpoint = resolve_connection_control_endpoint(selection)
    except ConnectionControlEndpointError as error:
        raise ConflictError(str(error)) from error
    if endpoint is None:
        raise ConflictError(
            "the connection has no configured control endpoint; it is stored but "
            "not attachable until the Owner configures one"
        )
    return endpoint


def _selected_producer_base(selection: Mapping[str, object]) -> str:
    """Return the selected original absolute repository base for fresh Producer."""
    base = selection.get("repository_base")
    if not isinstance(base, str) or not base.strip() or not base.startswith("/"):
        raise ConflictError(
            "the selected connection configuration carries no usable absolute "
            "runtime-side repository base for fresh producer construction"
        )
    return base


@contextmanager
def attach_cline_runtime(
    pool: DatabasePool,
    *,
    workspace_id: UUID,
    task_id: UUID,
    role: WorkflowRole,
    phase: ClineAttachmentPhase,
    external_session_id: str | None = None,
    working_directory: str | None = None,
    backend_factory: Callable[[], ClineSdkBackend] = ClineBridgeProcessBackend,
) -> Iterator[AttachedClineRuntime]:
    """Acquire one attached ``ClineAdapter`` for the exact admitted Connection.

    The trusted-caller composition boundary: resolve the durable admitted
    Task/role binding, validate its Workspace/Task/role/lifecycle
    consistency, resolve the admitted Connection's current control credential
    and validated control endpoint, establish the authenticated SDK bridge
    attachment, and yield the adapter wired to the durable construction-context
    resolver. Exit — normal, exceptional, or cancelled — disposes the backend
    exactly once.

    This composition performs no session effect and mutates no Task/session
    state: the caller owns workflow authorization, fresh establishment via
    ``establish_task_agent_session`` with the returned selection, and all
    D6b reconcile/settle/recover semantics after any uncertain operation.

    Raises the typed application vocabulary: ``InvalidCommandError`` for
    malformed command shape, uniform ``NotFoundError`` for missing or
    cross-Workspace subjects, ``ConflictError`` for lifecycle/routing/
    configuration refusals (all before any secret access or dial where the
    durable facts decide them), ``ExternalOperationFailedError`` with the
    normalized adapter-typed cause for known attachment failures, and
    ``ExternalOperationUncertainError`` when the attachment outcome itself is
    unknown. ``ExternalOperationUncertainError`` is deliberately not a known
    failure: it is never retried here and never classified as session loss.
    """
    with application_span(_SERVICE_TRACER_SCOPE, _ATTACH_SPAN_NAME) as span:
        # Command-shape validation runs INSIDE the use-case span and BEFORE
        # telemetry annotation (the #109 service-span contract): malformed
        # caller data is classified without exporting its values.
        _require_command_shape(
            workspace_id=workspace_id,
            task_id=task_id,
            role=role,
            phase=phase,
            external_session_id=external_session_id,
            working_directory=working_directory,
        )
        annotate_span(
            span,
            operation=_ATTACH_SPAN_NAME,
            workspace_id=str(workspace_id),
            task_id=str(task_id),
            workflow_role=role.value,
        )

        # --- Phase 1: exact durable subject validation (plain reads, no
        # transaction, no locks, no secret access, no external I/O). ---
        binding = _require_admitted_binding(
            get_task_agent_session(pool, task_id=task_id, role=role),
            workspace_id=workspace_id,
            phase=phase,
            external_session_id=external_session_id,
        )
        annotate_span(span, connection_id=str(binding.connection_id))
        # The mutable role-binding route is never consulted: the persisted
        # admitted Connection is the routing authority for both paths.
        _require_attachable_connection(
            get_connection(pool, binding.connection_id), workspace_id=workspace_id
        )

        # --- Phase 2: trusted credential resolution (the #55 boundary's own
        # current read) and the ONE non-secret configuration selection. The
        # endpoint and the credential come from the same durable read, so the
        # dialed attachment is mutually current. All Postgres/Vault reads
        # finish before any bridge/SDK network I/O and no transaction is
        # held across attachment. ---
        credential_connection, secret = resolve_connection_runtime_credential(
            pool, workspace_id=workspace_id, connection_id=binding.connection_id
        )
        if credential_connection.adapter is not AdapterType.CLINE:
            # Defensive durable-state invariant: the pre-read validated a
            # Cline Connection; a divergence is never guessed around.
            raise ConflictError(
                "the connection routed for this task/role is not an enabled Cline connection"
            )
        selection: dict[str, object] = dict(credential_connection.safe_config)
        endpoint = _resolve_selected_endpoint(selection)
        bootstrap_repository_base: str | None = None
        if phase is ClineAttachmentPhase.FRESH and role is WorkflowRole.PRODUCER:
            bootstrap_repository_base = _selected_producer_base(selection)

        # --- Phase 3: the authenticated SDK attachment, composition, and
        # bounded lease under ONE outer ownership: from the moment the
        # backend exists, its disposal is guaranteed exactly once — covering
        # remote-configuration construction, the connect call (typed
        # taxonomy failures AND unexpected exceptions or interruption),
        # adapter composition, and the yielded lease. The backend's own
        # dispose is idempotent and local-only: it never issues
        # stop/abort/delete and never replaces a session. ---
        backend = backend_factory()
        try:
            try:
                backend.connect(
                    ClineRemoteConfig(
                        endpoint=endpoint,
                        client_identity=OPENORC_CLIENT_IDENTITY,
                        auth_token=secret.secret_value(),
                    )
                )
            except ClineSdkBackendError as error:
                # Known taxonomy failures translate through the normalized
                # seam; unexpected exceptions and cancellation propagate
                # unchanged — both through the guaranteed disposal below.
                failure = classify_backend_failure(error, operation=_CONNECT_OPERATION)
                if isinstance(failure, AgentRuntimeUncertainOutcomeError):
                    logger.warning(
                        "cline runtime attachment ended with an uncertain outcome; "
                        "local bridge resources are released"
                    )
                    raise ExternalOperationUncertainError(
                        "the outcome of the cline runtime attachment is unknown; "
                        "no workflow effect was dispatched and nothing is retried here"
                    ) from failure
                raise ExternalOperationFailedError(
                    "the cline runtime attachment failed before any workflow effect"
                ) from failure

            resolver = DurableClineConstructionContextResolver(
                pool,
                bootstrap_repository_base=bootstrap_repository_base,
                working_directory=working_directory,
            )
            yield AttachedClineRuntime(
                adapter=ClineAdapter(backend, resolver),
                selected_configuration=selection if phase is ClineAttachmentPhase.FRESH else None,
            )
        finally:
            backend.dispose()
