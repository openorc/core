"""The process-owned Hub-run observation host (detached agent turns, issue #323).

The regular-mode application-lifecycle component that receives Hub run
completions without keeping any OpenOrc local worker slot or bridge RPC
blocked for the turn's lifetime. The observer is a process/application
-lifecycle concern owned by the serving lifespan; the shared settlement seam
(:mod:`openorc.services.task_hub_run_correlation`) remains the sole workflow
authority, and the owning Level 1 dispatch services (migrated in issue #325)
remain the only intended callers of the registration seam.

- One observer owns the serving process's Hub-run observation resources: a
  bounded registry of exact session subscriptions, each attached through the
  same current trusted Connection/credential resolution the runtime
  attachment composition uses. It is never a browser SSE surface, never an
  unauthenticated callback, and never a per-operation attachment.
- ``register_session`` is the positively-required pre-enqueue gate: the
  pinned SDK cannot replay an arbitrary missed terminal result, so a
  dispatch whose completion observation this observer owns must not enqueue
  without an actively subscribed exact session. Every failure to establish
  that observation — observer unavailable, capacity exhausted, attachment,
  attachment-configuration, or subscription failure, unresolvable binding —
  is a typed refusal the dispatch routes through the existing pre-effect
  failure machinery; a refusal never degrades into unobserved dispatch.
- Verified terminal observations route through the settlement seam within
  the observation's own authenticated Connection/Workspace scope; the Hub
  envelope's own session identity is authoritative and no identity is ever
  taken from payload contents, timestamps, or ``snapshot.prompt``.
  Duplicate/late delivery is deduplicated at the durable seam boundary, and
  ``run.started`` identities route through the read-only start
  corroboration.
- Post-settlement cleanup is coordinated with successor registration by one
  small per-session lifecycle lease: a successor's registration renews the
  session's observation lease, and a cleanup whose lease has since been
  renewed leaves the live observation in place — Run A's cleanup can never
  disconnect the observation Run B legitimately acquired after Run A's
  atomic settlement released the outstanding-run constraint. #322's
  single-outstanding-run invariant is relied upon unchanged.
- Reconnection is bounded and best-effort: when a connection's attachment
  proves unusable the next registration re-attaches through a fresh
  credential read, re-subscribes the registry's still-outstanding sessions,
  and consults the status-only ``run.list`` read for outstanding admitted
  receipts through the seam's classification — where status-only
  ``completed`` is never success evidence and an unusable record fails
  closed. There is no blanket polling, no idle-reset timer, no assumed
  gap-free observer lifetime, and no invention of missing formal results:
  an observer that died loses its in-memory registry by design, and
  previously outstanding receipts stay unresolved under the accepted v1
  fail-closed recovery bounds.
"""

from __future__ import annotations

import logging
import threading
from collections.abc import Callable
from enum import StrEnum
from typing import Final
from uuid import UUID

from openorc.adapters.cline import (
    ClineBridgeProcessBackend,
    ClineHubEvent,
    ClineHubRunStartObservation,
    ClineRemoteConfig,
    ClineSdkBackend,
    ClineSdkBackendError,
    Subscription,
    classify_backend_failure,
    project_hub_run_start,
    project_hub_run_statuses,
    project_terminal_hub_run,
    resolve_connection_control_endpoint,
)
from openorc.adapters.cline.connection_endpoint import ConnectionControlEndpointError
from openorc.domain.connections import AdapterType, Connection
from openorc.domain.hub_runs import (
    ObservedHubRunCompletion,
    ObservedHubRunStatus,
    TaskHubRunReceiptStatus,
)
from openorc.observability import annotate_span, application_span
from openorc.persistence.hub_runs import (
    find_outstanding_session_initialization_receipt,
    get_outstanding_task_hub_run_receipt,
)
from openorc.persistence.pool import DatabasePool
from openorc.persistence.sessions import find_task_agent_session_binding
from openorc.services.cline_attachment import OPENORC_CLIENT_IDENTITY
from openorc.services.connection_credentials import (
    ControlEndpointSecret,
    resolve_connection_runtime_credential,
)
from openorc.services.errors import (
    ConflictError,
    HubObservationRegistrationRefusedError,
    InvalidCommandError,
    NotFoundError,
)
from openorc.services.task_hub_run_correlation import (
    HubRunSettlementKind,
    HubRunStatusClassificationKind,
    check_hub_run_start_identity,
    classify_acknowledged_hub_run_status,
    settle_observed_hub_run_completion,
)

# The post-commit bootstrap continuation trigger of the process wiring
# (issue #324): invoked after an establishment settlement has committed a
# validated READY finalization, it owns the resumption classification and
# the continuation submission through the existing F1 boundary. Never
# invoked inside a database transaction; never a workflow authority.
BootstrapContinuationTrigger = Callable[..., None]

__all__ = [
    "BootstrapContinuationTrigger",
    "HubObservationRefusalReason",
    "HubRunObserver",
]

logger = logging.getLogger(__name__)

#: The trusted credential-resolution seam of one observation attachment: the
#: same current Workspace-scoped read the runtime attachment composition
#: uses, injected as an explicitly named seam for composition and tests.
RuntimeCredentialResolver = Callable[..., tuple[Connection, ControlEndpointSecret]]

_TRACER_SCOPE: Final[str] = "openorc.services.cline_hub_observation"
_REGISTER_SPAN_NAME: Final[str] = "hub_run_observation.register_session"
_REGISTER_ESTABLISHMENT_SPAN_NAME: Final[str] = "hub_run_observation.register_establishment_session"
_INGEST_SPAN_NAME: Final[str] = "hub_run_observation.ingest_hub_event"
_RECONNECT_SPAN_NAME: Final[str] = "hub_run_observation.reconnect_connection"
_CONNECT_OPERATION: Final[str] = "connect"
_SUBSCRIBE_OPERATION: Final[str] = "subscribe_hub"

# The defensive host bound beyond the structural one: subscriptions exist
# only for sessions with an outstanding receipt, themselves bounded by the
# session admission capacity. A registration beyond the cap is the typed
# refusal below — it blocks the enqueue rather than degrading observation.
_MAX_CONCURRENT_SESSION_OBSERVATIONS: Final[int] = 128

# The settlement classifications after which the session's observation has
# nothing further to wait for. Every other classification keeps observing:
# a staged early completion still awaits its acknowledgement, a rejected
# stale duplicate may precede the fresh completion, and an unresolved
# observation is retained rather than abandoned.
_CLEANUP_SETTLEMENT_KINDS: Final[frozenset[HubRunSettlementKind]] = frozenset(
    {
        HubRunSettlementKind.SETTLED_VALIDATED_RESULT,
        HubRunSettlementKind.SETTLED_OBSERVED_FAILURE,
        HubRunSettlementKind.SETTLED_RESULT_NOT_APPLIED,
        HubRunSettlementKind.NO_OUTSTANDING_RUN,
    }
)


class HubObservationRefusalReason(StrEnum):
    """The closed vocabulary of positive-observation registration refusals.

    The safe machine tokens a dispatch's pre-effect failure evidence may
    carry; no endpoint, credential, payload, or prompt material ever
    appears in a reason.
    """

    OBSERVER_UNAVAILABLE = "observer_unavailable"
    OBSERVER_AT_CAPACITY = "observer_at_capacity"
    SESSION_BINDING_UNRESOLVED = "session_binding_unresolved"
    OBSERVATION_ATTACHMENT_FAILED = "observation_attachment_failed"
    OBSERVATION_SUBSCRIPTION_FAILED = "observation_subscription_failed"
    # The detached establishment dispatch's typed carrier for a registration
    # failure that is not one of the observer's own classified refusals
    # (issue #324): the same proven pre-effect condition, one safe token.
    OBSERVATION_REGISTRATION_FAILED = "observation_registration_failed"


class _SessionObservation:
    """One registered exact-session observation and its lifecycle lease.

    ``lifecycle_token`` is the small successor guard: every registration
    that reuses the session's live observation renews it, and a cleanup
    whose lease has since been renewed leaves the observation in place.
    """

    __slots__ = (
        "connection_id",
        "external_session_id",
        "lifecycle_token",
        "subscription",
        "task_agent_session_id",
        "task_id",
        "workspace_id",
    )

    def __init__(
        self,
        *,
        workspace_id: UUID,
        task_id: UUID,
        task_agent_session_id: UUID,
        connection_id: UUID,
        external_session_id: str,
        subscription: Subscription | None,
    ) -> None:
        self.workspace_id = workspace_id
        self.task_id = task_id
        self.task_agent_session_id = task_agent_session_id
        self.connection_id = connection_id
        self.external_session_id = external_session_id
        self.subscription = subscription
        self.lifecycle_token = 0


class _ConnectionAttachment:
    """One attached backend shared by every observed session of a connection."""

    __slots__ = ("backend",)

    def __init__(self, backend: ClineSdkBackend) -> None:
        self.backend: ClineSdkBackend | None = backend


class HubRunObserver:
    """The serving process's bounded Hub-run observation host.

    Constructed and started exactly once by the regular-mode serving
    lifespan and disposed deterministically at its teardown; construction
    creates no attachment, subscription, or connection. Attachments are
    established lazily per Connection on first registration and shared by
    that connection's observed sessions.
    """

    def __init__(
        self,
        database_pool_provider: Callable[[], DatabasePool],
        *,
        backend_factory: Callable[[], ClineSdkBackend] = ClineBridgeProcessBackend,
        runtime_credential_resolver: RuntimeCredentialResolver = (
            resolve_connection_runtime_credential
        ),
        bootstrap_continuation_trigger: BootstrapContinuationTrigger | None = None,
    ) -> None:
        self._pool_provider = database_pool_provider
        self._backend_factory = backend_factory
        self._credential_resolver = runtime_credential_resolver
        self._bootstrap_continuation_trigger = bootstrap_continuation_trigger
        self._lock = threading.Lock()
        self._started = False
        self._disposed = False
        self._sessions: dict[UUID, _SessionObservation] = {}
        self._connections: dict[UUID, _ConnectionAttachment] = {}
        self._attach_locks: dict[UUID, threading.Lock] = {}

    @property
    def disposed(self) -> bool:
        """Whether this observer has been terminally disposed."""
        with self._lock:
            return self._disposed

    def start(self) -> None:
        """Mark the observer running; idempotent, creates nothing."""
        with self._lock:
            if self._disposed:
                raise ConflictError("the hub-run observer has been disposed and cannot restart")
            self._started = True

    def dispose(self) -> None:
        """Release every observation resource; idempotent and terminal.

        Stops admission first, then unsubscribes every session subscription
        and disposes every connection attachment. Disposal never deletes or
        replaces an external session and never implies any workflow
        decision; outstanding receipts simply stay unobserved under the
        accepted v1 fail-closed recovery bounds.
        """
        with self._lock:
            if self._disposed:
                return
            self._disposed = True
            self._started = False
            sessions = list(self._sessions.values())
            self._sessions.clear()
            connections = list(self._connections.values())
            self._connections.clear()
            self._attach_locks.clear()
        for entry in sessions:
            subscription = entry.subscription
            entry.subscription = None
            if subscription is not None:
                try:
                    subscription.unsubscribe()
                except Exception as error:  # noqa: BLE001 - best-effort teardown
                    logger.warning(
                        "hub-run observation unsubscribe failed during disposal (%s) "
                        "for task session %s",
                        type(error).__name__,
                        entry.task_agent_session_id,
                    )
        for attachment in connections:
            backend = attachment.backend
            attachment.backend = None
            if backend is not None:
                try:
                    backend.dispose()
                except Exception as error:  # noqa: BLE001 - best-effort teardown
                    logger.warning(
                        "hub-run observation attachment disposal failed during disposal (%s)",
                        type(error).__name__,
                    )

    def register_session(
        self,
        *,
        workspace_id: UUID,
        task_id: UUID,
        task_agent_session_id: UUID,
        connection_id: UUID,
        external_session_id: str,
    ) -> None:
        """Establish the positive Hub-run observation of one exact session.

        The pre-enqueue gate of the detached dispatch: returns only once the
        exact external session is actively subscribed through the
        connection's current trusted attachment. Every expected failure
        raises :class:`HubObservationRegistrationRefusedError` — the enqueue
        must not proceed without the observation, because the pinned SDK
        cannot replay an arbitrary missed terminal result; known
        attachment-configuration refusals (a missing, cross-Workspace,
        disabled, or non-Cline connection; an unusable credential reference;
        a missing or malformed control endpoint) are typed into the same
        refusal, and unexpected exceptions propagate unchanged for the
        caller's own failure machinery.

        The durable binding is proven within the authenticated
        Workspace/Connection scope before any secret access or dial; a
        registration whose identity tuple does not match the authoritative
        binding fails closed. A repeated registration of an already-observed
        session renews its lifecycle lease and reuses the live subscription
        (the successor guard), never double-subscribing.
        """
        for name, value in (
            ("workspace_id", workspace_id),
            ("task_id", task_id),
            ("task_agent_session_id", task_agent_session_id),
            ("connection_id", connection_id),
        ):
            if not isinstance(value, UUID):
                raise InvalidCommandError(f"{name} must be a UUID")
        if not isinstance(external_session_id, str) or not external_session_id.strip():
            raise InvalidCommandError("external_session_id must be a non-empty string")
        with application_span(_TRACER_SCOPE, _REGISTER_SPAN_NAME) as span:
            annotate_span(
                span,
                operation=_REGISTER_SPAN_NAME,
                workspace_id=str(workspace_id),
                task_id=str(task_id),
                connection_id=str(connection_id),
            )
            self._require_running()
            binding = find_task_agent_session_binding(
                self._pool_provider(),
                workspace_id=workspace_id,
                connection_id=connection_id,
                external_session_id=external_session_id,
            )
            if binding is None or binding.task_id != task_id or binding.id != task_agent_session_id:
                logger.warning(
                    "hub-run observation registration resolved no matching session "
                    "binding in its connection scope"
                )
                raise self._refusal(HubObservationRefusalReason.SESSION_BINDING_UNRESOLVED)
            self._register_proven_session(
                workspace_id=workspace_id,
                task_id=task_id,
                task_agent_session_id=task_agent_session_id,
                connection_id=connection_id,
                external_session_id=external_session_id,
            )

    def register_establishment_session(
        self,
        *,
        workspace_id: UUID,
        task_id: UUID,
        task_agent_session_id: UUID,
        connection_id: UUID,
        external_session_id: str,
    ) -> None:
        """Establish the positive observation of one initializing session.

        The detached session establishment's pre-enqueue gate (issue #324): a
        CONNECTING binding deliberately carries no bound external identity,
        so this registration proves the identity through the binding's
        outstanding ``session_initialization`` receipt instead — the receipt
        must be outstanding in this authenticated Workspace/Connection scope
        and carry exactly the provisional ``created_external_session_id``
        being registered. Every expected failure raises the same
        :class:`HubObservationRegistrationRefusedError` refusal taxonomy;
        the enqueue must not proceed without the observation. A repeated
        registration renews the lifecycle lease and reuses the live
        subscription exactly like the established-session gate.
        """
        for name, value in (
            ("workspace_id", workspace_id),
            ("task_id", task_id),
            ("task_agent_session_id", task_agent_session_id),
            ("connection_id", connection_id),
        ):
            if not isinstance(value, UUID):
                raise InvalidCommandError(f"{name} must be a UUID")
        if not isinstance(external_session_id, str) or not external_session_id.strip():
            raise InvalidCommandError("external_session_id must be a non-empty string")
        with application_span(_TRACER_SCOPE, _REGISTER_ESTABLISHMENT_SPAN_NAME) as span:
            annotate_span(
                span,
                operation=_REGISTER_ESTABLISHMENT_SPAN_NAME,
                workspace_id=str(workspace_id),
                task_id=str(task_id),
                connection_id=str(connection_id),
            )
            self._require_running()
            receipt = find_outstanding_session_initialization_receipt(
                self._pool_provider(),
                workspace_id=workspace_id,
                connection_id=connection_id,
                created_external_session_id=external_session_id,
            )
            if (
                receipt is None
                or receipt.task_id != task_id
                or receipt.task_agent_session_id != task_agent_session_id
            ):
                logger.warning(
                    "establishment observation registration resolved no outstanding "
                    "initialization receipt carrying the provisional identity in "
                    "its connection scope"
                )
                raise self._refusal(HubObservationRefusalReason.SESSION_BINDING_UNRESOLVED)
            self._register_proven_session(
                workspace_id=workspace_id,
                task_id=task_id,
                task_agent_session_id=task_agent_session_id,
                connection_id=connection_id,
                external_session_id=external_session_id,
            )

    def _register_proven_session(
        self,
        *,
        workspace_id: UUID,
        task_id: UUID,
        task_agent_session_id: UUID,
        connection_id: UUID,
        external_session_id: str,
    ) -> None:
        """Attach and subscribe a registration whose binding is proven."""
        with self._lock:
            if self._disposed:
                raise self._refusal(HubObservationRefusalReason.OBSERVER_UNAVAILABLE)
            existing = self._sessions.get(task_agent_session_id)
            if existing is not None and existing.subscription is not None:
                # Successor registration: renew the lease and reuse the live
                # observation; a settlement cleanup whose lease this renewal
                # supersedes leaves the observation in place.
                existing.lifecycle_token += 1
                return
            if existing is None and len(self._sessions) >= _MAX_CONCURRENT_SESSION_OBSERVATIONS:
                logger.warning(
                    "hub-run observation registration refused: the observer is at "
                    "its concurrent session bound"
                )
                raise self._refusal(HubObservationRefusalReason.OBSERVER_AT_CAPACITY)
        with self._connection_attach_lock(connection_id):
            with self._lock:
                if self._disposed:
                    raise self._refusal(HubObservationRefusalReason.OBSERVER_UNAVAILABLE)
                winner = self._sessions.get(task_agent_session_id)
                if winner is not None and winner.subscription is not None:
                    return  # a concurrent registration established the observation
            backend = self._ensure_connection_backend(
                workspace_id=workspace_id, connection_id=connection_id
            )
            try:
                subscription = backend.subscribe_hub(
                    external_session_id, self._make_listener(task_agent_session_id)
                )
            except ClineSdkBackendError as error:
                failure = classify_backend_failure(error, operation=_SUBSCRIBE_OPERATION)
                logger.warning(
                    "hub-run observation subscription failed (%s) for connection %s",
                    type(failure).__name__,
                    connection_id,
                )
                # The failed subscribe proves the current attachment unusable:
                # drop it so the next registration re-attaches through a fresh
                # credential read and re-establishes every outstanding session.
                self._drop_connection_backend(connection_id)
                raise self._refusal(
                    HubObservationRefusalReason.OBSERVATION_SUBSCRIPTION_FAILED
                ) from None
            self._install_subscription(
                task_agent_session_id=task_agent_session_id,
                previous=existing,
                workspace_id=workspace_id,
                task_id=task_id,
                connection_id=connection_id,
                external_session_id=external_session_id,
                subscription=subscription,
            )

    def _require_running(self) -> None:
        """Fail the registration closed when the observer is not running."""
        with self._lock:
            if not self._started or self._disposed:
                raise self._refusal(HubObservationRefusalReason.OBSERVER_UNAVAILABLE)

    def _refusal(
        self, reason: HubObservationRefusalReason
    ) -> HubObservationRegistrationRefusedError:
        return HubObservationRegistrationRefusedError(
            "positive hub-run observation could not be established for the exact session",
            reason=reason.value,
        )

    def _connection_attach_lock(self, connection_id: UUID) -> threading.Lock:
        """Return the per-connection attach lock, creating it once.

        Lock ordering: an attach lock is only ever acquired before the
        registry lock, never after it, so ingestion (registry lock only)
        can never be blocked behind an attachment attempt.
        """
        with self._lock:
            attach_lock = self._attach_locks.get(connection_id)
            if attach_lock is None:
                attach_lock = threading.Lock()
                self._attach_locks[connection_id] = attach_lock
            return attach_lock

    def _ensure_connection_backend(
        self, *, workspace_id: UUID, connection_id: UUID
    ) -> ClineSdkBackend:
        """Return the connection's live backend, freshly attaching when absent.

        Called under the connection's attach lock. A fresh attach resolves
        the connection's current endpoint and credential through the same
        trusted read the runtime attachment composition uses — never an
        in-memory secret — and runs the bounded reconnect reconciliation for
        the connection's still-registered sessions before returning.
        """
        with self._lock:
            attachment = self._connections.get(connection_id)
            backend = attachment.backend if attachment is not None else None
        if backend is not None:
            return backend
        backend = self._fresh_attach(workspace_id=workspace_id, connection_id=connection_id)
        with self._lock:
            reconnected = any(
                entry.connection_id == connection_id for entry in self._sessions.values()
            )
        if reconnected:
            self._reconcile_reconnected_connection(backend, connection_id=connection_id)
        return backend

    def _fresh_attach(self, *, workspace_id: UUID, connection_id: UUID) -> ClineSdkBackend:
        """Resolve current credentials, dial the Hub, and install the backend.

        Known, positively established pre-subscription attachment and
        configuration failures — a missing or cross-Workspace connection, a
        disabled connection, an unusable credential reference, a missing or
        malformed control endpoint — are typed into the registration refusal
        the dispatch gates the enqueue on: they are proven pre-effect
        conditions of the observation, never workflow conflicts escaping the
        observation host and never uncertain outcomes. Unexpected exceptions
        propagate unchanged.
        """
        try:
            credential_connection, secret = self._credential_resolver(
                self._pool_provider(), workspace_id=workspace_id, connection_id=connection_id
            )
            if credential_connection.adapter is not AdapterType.CLINE:
                # Defensive durable-state invariant: a non-Cline connection has
                # no Hub to observe and is never guessed around.
                raise ConflictError(
                    "the connection routed for this task/role is not an enabled Cline connection"
                )
            selection = dict(credential_connection.safe_config)
            try:
                endpoint = resolve_connection_control_endpoint(selection)
            except ConnectionControlEndpointError as error:
                raise ConflictError(str(error)) from error
            if endpoint is None:
                raise ConflictError(
                    "the connection has no configured control endpoint; it is stored but "
                    "not attachable until the Owner configures one"
                )
        except (NotFoundError, ConflictError) as error:
            logger.warning(
                "hub-run observation attachment refused (%s) for connection %s",
                type(error).__name__,
                connection_id,
            )
            raise self._refusal(HubObservationRefusalReason.OBSERVATION_ATTACHMENT_FAILED) from None
        backend = self._backend_factory()
        try:
            backend.connect(
                ClineRemoteConfig(
                    endpoint=endpoint,
                    client_identity=OPENORC_CLIENT_IDENTITY,
                    auth_token=secret.secret_value(),
                )
            )
        except ClineSdkBackendError as error:
            failure = classify_backend_failure(error, operation=_CONNECT_OPERATION)
            logger.warning(
                "hub-run observation attachment failed (%s) for connection %s",
                type(failure).__name__,
                connection_id,
            )
            try:
                backend.dispose()
            except Exception as dispose_error:  # noqa: BLE001 - best-effort teardown
                logger.warning(
                    "hub-run observation attachment disposal failed (%s)",
                    type(dispose_error).__name__,
                )
            raise self._refusal(HubObservationRefusalReason.OBSERVATION_ATTACHMENT_FAILED) from None
        with self._lock:
            if self._disposed:
                doomed = backend
            else:
                self._connections[connection_id] = _ConnectionAttachment(backend)
                doomed = None
        if doomed is not None:
            try:
                doomed.dispose()
            except Exception as error:  # noqa: BLE001 - best-effort teardown
                logger.warning(
                    "hub-run observation attachment disposal failed (%s)",
                    type(error).__name__,
                )
            raise self._refusal(HubObservationRefusalReason.OBSERVER_UNAVAILABLE)
        return backend

    def _drop_connection_backend(self, connection_id: UUID) -> None:
        """Discard an unusable backend and its dead session subscriptions.

        The next registration for this connection re-attaches through a
        fresh credential read and the bounded reconnect reconciliation
        re-establishes the still-outstanding sessions.
        """
        with self._lock:
            attachment = self._connections.pop(connection_id, None)
            backend = attachment.backend if attachment is not None else None
            for entry in self._sessions.values():
                if entry.connection_id == connection_id:
                    entry.subscription = None
        if backend is not None:
            try:
                backend.dispose()
            except Exception as error:  # noqa: BLE001 - best-effort teardown
                logger.warning(
                    "hub-run observation attachment disposal failed (%s)",
                    type(error).__name__,
                )

    def _install_subscription(
        self,
        *,
        task_agent_session_id: UUID,
        previous: _SessionObservation | None,
        workspace_id: UUID,
        task_id: UUID,
        connection_id: UUID,
        external_session_id: str,
        subscription: Subscription,
    ) -> None:
        """Install a freshly subscribed observation, or discard a losing race.

        Called under the connection's attach lock; the registry transition
        itself is guarded by the registry lock and never unsubscribes under
        it. A concurrent registration that installed its own live
        observation first, a disposal, or a late capacity breach discards
        the newcomer's subscription.
        """
        doomed = subscription
        doomed_reason: HubObservationRefusalReason | None = None
        with self._lock:
            if self._disposed:
                doomed_reason = HubObservationRefusalReason.OBSERVER_UNAVAILABLE
            else:
                current = self._sessions.get(task_agent_session_id)
                if current is not None and current.subscription is not None:
                    doomed_reason = None  # a concurrent registration owns the observation
                elif (
                    previous is None and len(self._sessions) >= _MAX_CONCURRENT_SESSION_OBSERVATIONS
                ):
                    doomed_reason = HubObservationRefusalReason.OBSERVER_AT_CAPACITY
                else:
                    if current is None:
                        current = _SessionObservation(
                            workspace_id=workspace_id,
                            task_id=task_id,
                            task_agent_session_id=task_agent_session_id,
                            connection_id=connection_id,
                            external_session_id=external_session_id,
                            subscription=subscription,
                        )
                        self._sessions[task_agent_session_id] = current
                    else:
                        current.subscription = subscription
                    current.lifecycle_token += 1
                    doomed = None
        if doomed is None:
            return
        try:
            doomed.unsubscribe()
        except Exception as error:  # noqa: BLE001 - best-effort teardown
            logger.warning(
                "hub-run observation unsubscribe of a losing registration failed (%s)",
                type(error).__name__,
            )
        if doomed_reason is None:
            return  # a concurrent registration owns the observation
        if doomed_reason is HubObservationRefusalReason.OBSERVER_AT_CAPACITY:
            logger.warning(
                "hub-run observation registration refused: the observer is at its "
                "concurrent session bound"
            )
        raise self._refusal(doomed_reason)

    def _make_listener(self, task_agent_session_id: UUID) -> Callable[[ClineHubEvent], None]:
        """Build the contained ingest listener of one session subscription."""

        def listener(event: ClineHubEvent) -> None:
            try:
                self._ingest_hub_event(task_agent_session_id, event)
            except Exception as error:  # noqa: BLE001 - the bridge reader is protected
                logger.warning(
                    "hub-run event ingestion ended unexpectedly (%s) for task session %s",
                    type(error).__name__,
                    task_agent_session_id,
                )

        return listener

    def _ingest_hub_event(self, task_agent_session_id: UUID, event: ClineHubEvent) -> None:
        """Route one delivered Hub envelope for its registered session.

        The envelope's own session identity is authoritative: a session-
        scoped subscription must only deliver the exact session's envelopes,
        and a mismatched identity is never routed or attributed. The
        delivery-time lifecycle lease is captured under the registry lock so
        the post-settlement cleanup can detect a successor registration.
        """
        with self._lock:
            entry = self._sessions.get(task_agent_session_id)
            if entry is None:
                return  # disposed, superseded, or already cleaned up
            lease = entry.lifecycle_token
        if event.session_id != entry.external_session_id:
            logger.warning(
                "a hub event envelope carried a foreign session identity for a "
                "subscribed task session; it is not routed"
            )
            return
        completion = project_terminal_hub_run(event)
        if completion is not None:
            self._route_terminal_observation(entry, lease, completion)
            return
        start = project_hub_run_start(event)
        if start is not None:
            self._route_start_observation(entry, start)

    def _route_terminal_observation(
        self,
        entry: _SessionObservation,
        lease: int,
        completion: ObservedHubRunCompletion,
    ) -> None:
        """Settle one verified terminal observation through the shared seam.

        The seam owns attribution, duplicate/stale rejection, staging for
        the admission race, formal validation, and every workflow
        consequence; this boundary only delivers the observation within its
        authenticated scope and contains every failure so the bridge's
        delivery thread is never disrupted. Cleanup after a terminal
        classification is guarded by the delivery-time lease: a successor
        registration that renewed the session's observation keeps it.
        """
        with application_span(_TRACER_SCOPE, _INGEST_SPAN_NAME) as span:
            annotate_span(
                span,
                operation=_INGEST_SPAN_NAME,
                workspace_id=str(entry.workspace_id),
                task_id=str(entry.task_id),
                connection_id=str(entry.connection_id),
            )
            try:
                outcome = settle_observed_hub_run_completion(
                    self._pool_provider(),
                    workspace_id=entry.workspace_id,
                    connection_id=entry.connection_id,
                    observation=completion,
                )
            except Exception as error:  # noqa: BLE001 - best-effort ingestion
                logger.warning(
                    "hub-run completion settlement failed (%s) for task session %s",
                    type(error).__name__,
                    entry.task_agent_session_id,
                )
                return
        if outcome.kind in _CLEANUP_SETTLEMENT_KINDS:
            self._cleanup_after_settlement(entry, lease)
        if (
            outcome.kind is HubRunSettlementKind.SETTLED_VALIDATED_RESULT
            and outcome.receipt is not None
            and outcome.receipt.operation.value == "session_initialization"
            and self._bootstrap_continuation_trigger is not None
        ):
            # The validated READY finalization of an initialization run
            # committed (issue #324): the post-commit bootstrap continuation
            # trigger owns the resumption classification and submission.
            # Strictly post-commit, contained, never a workflow authority;
            # a failed submission leaves the durable resumable state to the
            # existing recovery semantics.
            try:
                self._bootstrap_continuation_trigger(
                    workspace_id=entry.workspace_id, task_id=entry.task_id
                )
            except Exception as error:  # noqa: BLE001 - best-effort trigger
                logger.warning(
                    "the bootstrap continuation trigger failed (%s) after the "
                    "validated initialization settlement for task session %s",
                    type(error).__name__,
                    entry.task_agent_session_id,
                )

    def _route_start_observation(
        self, entry: _SessionObservation, start: ClineHubRunStartObservation
    ) -> None:
        """Corroborate one observed run-start identity through the seam.

        Read-only: the seam classifies corroboration, contradiction, and
        unattributable starts and records contradictions itself; this
        boundary delivers the identity exactly as observed and contains
        every failure.
        """
        try:
            check_hub_run_start_identity(
                self._pool_provider(),
                workspace_id=entry.workspace_id,
                connection_id=entry.connection_id,
                external_session_id=start.external_session_id,
                request_id=start.request_id,
            )
        except Exception as error:  # noqa: BLE001 - best-effort ingestion
            logger.warning(
                "hub-run start corroboration failed (%s) for task session %s",
                type(error).__name__,
                entry.task_agent_session_id,
            )

    def _cleanup_after_settlement(self, entry: _SessionObservation, lease: int) -> None:
        """Release the session's observation unless a successor renewed it.

        The lease captured at delivery time is compared under the registry
        lock: a registration that arrived between the settlement and this
        cleanup renewed the session's observation and the cleanup must not
        disconnect it — the runs never overlapped, but the live observation
        now belongs to the successor's authorized run.
        """
        subscription = self._detach_renewed_entry(entry, lease)
        if subscription is None:
            return
        try:
            subscription.unsubscribe()
        except Exception as error:  # noqa: BLE001 - best-effort teardown
            logger.warning(
                "hub-run observation unsubscribe failed (%s) for task session %s",
                type(error).__name__,
                entry.task_agent_session_id,
            )

    def _detach_renewed_entry(self, entry: _SessionObservation, lease: int) -> Subscription | None:
        """Detach the entry iff it is still the unrenewed observed session."""
        with self._lock:
            if self._sessions.get(entry.task_agent_session_id) is not entry:
                return None
            if entry.lifecycle_token != lease:
                return None  # a successor registration renewed the observation
            subscription = entry.subscription
            entry.subscription = None
            del self._sessions[entry.task_agent_session_id]
            return subscription

    def _reconcile_reconnected_connection(
        self, backend: ClineSdkBackend, *, connection_id: UUID
    ) -> None:
        """Re-establish observation after a fresh attachment replaced a dead one.

        Bounded by the registry's session entries; never a table scan and
        never a poll. Each still-registered session is re-subscribed on the
        fresh attachment, entries whose receipt is no longer outstanding are
        released, and each outstanding admitted receipt's status-only
        ``run.list`` record is consulted through the seam's classification —
        where ``completed`` is never success evidence and ambiguity stays
        unresolved.
        """
        with self._lock:
            entries = [
                entry for entry in self._sessions.values() if entry.connection_id == connection_id
            ]
        for entry in entries:
            if entry.subscription is not None:
                continue
            try:
                subscription = backend.subscribe_hub(
                    entry.external_session_id, self._make_listener(entry.task_agent_session_id)
                )
            except ClineSdkBackendError as error:
                failure = classify_backend_failure(error, operation=_SUBSCRIBE_OPERATION)
                logger.warning(
                    "hub-run reconnect re-subscription failed (%s) for task session %s",
                    type(failure).__name__,
                    entry.task_agent_session_id,
                )
                continue
            with self._lock:
                if self._disposed or self._sessions.get(entry.task_agent_session_id) is not entry:
                    doomed = subscription
                else:
                    entry.subscription = subscription
                    doomed = None
            if doomed is not None:
                try:
                    doomed.unsubscribe()
                except Exception as error:  # noqa: BLE001 - best-effort teardown
                    logger.warning(
                        "hub-run observation unsubscribe of a losing re-subscription failed (%s)",
                        type(error).__name__,
                    )
        for entry in entries:
            self._reconcile_session_receipt(backend, entry)

    def _reconcile_session_receipt(
        self, backend: ClineSdkBackend, entry: _SessionObservation
    ) -> None:
        """Recheck one entry's outstanding receipt and consult status-only reads."""
        try:
            receipt = get_outstanding_task_hub_run_receipt(
                self._pool_provider(), task_agent_session_id=entry.task_agent_session_id
            )
        except Exception as error:  # noqa: BLE001 - best-effort reconciliation
            logger.warning(
                "hub-run reconnect receipt recheck failed (%s) for task session %s",
                type(error).__name__,
                entry.task_agent_session_id,
            )
            return
        if receipt is None:
            # The run reached a terminal receipt while observation was down;
            # this entry observes nothing further.
            subscription = self._detach_entry(entry)
            if subscription is not None:
                try:
                    subscription.unsubscribe()
                except Exception as error:  # noqa: BLE001 - best-effort teardown
                    logger.warning(
                        "hub-run observation unsubscribe failed (%s) for task session %s",
                        type(error).__name__,
                        entry.task_agent_session_id,
                    )
            return
        if receipt.status is not TaskHubRunReceiptStatus.ADMITTED:
            return  # still claimed: the acknowledgement attach owns any staged completion
        try:
            records = backend.list_runs(entry.external_session_id)
            statuses = project_hub_run_statuses(records)
        except Exception as error:  # noqa: BLE001 - the consultation stays best-effort
            logger.warning(
                "hub-run reconnect run.list consultation failed (%s) for task session %s",
                type(error).__name__,
                entry.task_agent_session_id,
            )
            return
        self._classify_reconnected_receipt(entry, receipt.id, statuses)

    def _classify_reconnected_receipt(
        self,
        entry: _SessionObservation,
        receipt_id: UUID,
        statuses: list[ObservedHubRunStatus],
    ) -> None:
        """Hand one reconnected receipt's status-only observation to the seam."""
        with application_span(_TRACER_SCOPE, _RECONNECT_SPAN_NAME) as span:
            annotate_span(
                span,
                operation=_RECONNECT_SPAN_NAME,
                workspace_id=str(entry.workspace_id),
                task_id=str(entry.task_id),
                connection_id=str(entry.connection_id),
            )
            try:
                classification = classify_acknowledged_hub_run_status(
                    self._pool_provider(),
                    workspace_id=entry.workspace_id,
                    task_id=entry.task_id,
                    receipt_id=receipt_id,
                    observed_statuses=statuses,
                )
            except Exception as error:  # noqa: BLE001 - the consultation stays best-effort
                logger.warning(
                    "hub-run reconnect status classification failed (%s) for task session %s",
                    type(error).__name__,
                    entry.task_agent_session_id,
                )
                return
        if classification.kind is HubRunStatusClassificationKind.CLASSIFIED_FAILURE:
            logger.warning(
                "hub-run reconnect classified the outstanding run's status-only failed "
                "record for task session %s",
                entry.task_agent_session_id,
            )

    def _detach_entry(self, entry: _SessionObservation) -> Subscription | None:
        """Detach one entry atomically under the registry lock."""
        with self._lock:
            if self._sessions.get(entry.task_agent_session_id) is not entry:
                return None
            subscription = entry.subscription
            entry.subscription = None
            del self._sessions[entry.task_agent_session_id]
            return subscription
