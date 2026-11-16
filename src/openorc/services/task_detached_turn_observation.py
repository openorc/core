"""The process-owned detached-turn observation host (detached agent turns,
issues #323/#348).

The regular-mode application-lifecycle component that receives detached-turn
completions without keeping any OpenOrc local worker slot or bridge RPC
blocked for the turn's lifetime. The host is a process/application-lifecycle
concern owned by the serving lifespan; the shared settlement seam
(:mod:`openorc.services.task_detached_turn_correlation`) remains the sole
workflow authority, and the owning Level 1 dispatch services (migrated in
issue #325) remain the only intended callers of the registration seam.

The host is runtime-neutral: it owns the bounded registry of exact-session
observations, the registration gates and their typed refusals, the
per-session lifecycle lease, the delivery routing into the settlement seam,
the reconnection policy, and the #324 post-commit bootstrap continuation
trigger. Every runtime-native mechanic — attachment, subscription, native
evidence wrapping, and the classification of which native evidence proves or
counters an admitted turn — lives behind the injected
:class:`DetachedTurnObservationSource` port, implemented by the concrete
runtime integration. The host never imports a runtime integration and never
interprets native evidence.

- One host owns the serving process's observation resources: a bounded
  registry of exact session observations, each attached through the same
  current trusted Connection/credential resolution the runtime attachment
  composition uses. It is never a browser SSE surface, never an
  unauthenticated callback, and never a per-operation attachment.
- ``register_session`` is the positively-required pre-admission gate: the
  runtime integration cannot replay an arbitrary missed terminal result, so
  a dispatch whose completion observation this host owns must not admit a
  turn without an actively observed exact session. Every failure to
  establish that observation — host unavailable, capacity exhausted,
  attachment, attachment-configuration, or subscription failure,
  unresolvable binding — is a typed refusal the dispatch routes through the
  existing pre-effect failure machinery; a refusal never degrades into
  unobserved dispatch.
- Delivered terminal observations route through the settlement seam within
  the observation's own authenticated Connection/Workspace scope; the
  evidence envelope's own session identity is authoritative and no identity
  is ever taken from payload contents, timestamps, or snapshots. Duplicate
  and late delivery is deduplicated at the durable seam boundary, and
  turn-start observations route through the read-only start corroboration.
- Post-settlement cleanup is coordinated with successor registration by one
  small per-session lifecycle lease: a successor's registration renews the
  session's observation lease, and a cleanup whose lease has since been
  renewed leaves the live observation in place — turn A's cleanup can never
  disconnect the observation turn B legitimately acquired after turn A's
  atomic settlement released the outstanding-turn constraint. The
  single-outstanding-turn invariant is relied upon unchanged.
- Reconnection is bounded and best-effort: when a connection's attachment
  proves unusable the next registration re-attaches through a fresh trusted
  read, re-establishes the registry's still-outstanding session
  observations, and consults the integration's status-only evidence for
  outstanding admitted receipts through the seam's classification — where a
  completed status alone is never success evidence and an unusable record
  fails closed. There is no blanket polling, no idle-reset timer, no assumed
  gap-free host lifetime, and no invention of missing formal results: a host
  that died loses its in-memory registry by design, and previously
  outstanding receipts stay unresolved under the accepted v1 fail-closed
  recovery bounds.
"""

from __future__ import annotations

import logging
import threading
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from enum import StrEnum
from typing import Final, Protocol
from uuid import UUID

from openorc.domain.connections import AdapterType, Connection
from openorc.domain.detached_turns import (
    DetachedTurnObservationSourceError,
    NativeTurnEvidence,
    ObservationLease,
    ObservedTurnEvidence,
    TaskDetachedTurnReceiptStatus,
    TurnEvidenceFamily,
)
from openorc.observability import annotate_span, application_span
from openorc.persistence.detached_turns import (
    find_outstanding_session_initialization_receipt,
    get_outstanding_task_detached_turn_receipt,
)
from openorc.persistence.pool import DatabasePool
from openorc.persistence.sessions import find_task_agent_session_binding
from openorc.services.connection_credentials import (
    ControlEndpointSecret,
    resolve_connection_runtime_credential,
)
from openorc.services.errors import (
    ConflictError,
    InvalidCommandError,
    NotFoundError,
    ObservationRegistrationRefusedError,
)
from openorc.services.task_detached_turn_correlation import (
    TurnEvidenceClassifier,
    TurnSettlementKind,
    TurnStatusClassificationKind,
    check_turn_start_identity,
    classify_acknowledged_turn_status,
    settle_observed_turn_completion,
)

BootstrapContinuationTrigger = Callable[..., None]

__all__ = [
    "BootstrapContinuationTrigger",
    "DetachedTurnObservationSourceError",
    "DetachedTurnObservationHost",
    "DetachedTurnObservationSource",
    "DetachedTurnObservationSourceError",
    "ObservationAttachmentInputs",
    "ObservationRegistrationRefusalReason",
]

logger = logging.getLogger(__name__)

_TRACER_SCOPE: Final[str] = "openorc.services.task_detached_turn_observation"
_REGISTER_SPAN_NAME: Final[str] = "detached_turn_observation.register_session"
_REGISTER_ESTABLISHMENT_SPAN_NAME: Final[str] = (
    "detached_turn_observation.register_establishment_session"
)
_INGEST_SPAN_NAME: Final[str] = "detached_turn_observation.ingest_turn_evidence"
_RECONNECT_SPAN_NAME: Final[str] = "detached_turn_observation.reconnect_connection"

# The defensive host bound beyond the structural one: observations exist
# only for sessions with an outstanding receipt, themselves bounded by the
# session admission capacity. A registration beyond the cap is the typed
# refusal below — it blocks the admission rather than degrading observation.
_MAX_CONCURRENT_SESSION_OBSERVATIONS: Final[int] = 128

# The settlement classifications after which the session's observation has
# nothing further to wait for. Every other classification keeps observing:
# a staged early completion still awaits its acknowledgement, a rejected
# stale duplicate may precede the fresh completion, and an unresolved
# observation is retained rather than abandoned.
_CLEANUP_SETTLEMENT_KINDS: Final[frozenset[TurnSettlementKind]] = frozenset(
    {
        TurnSettlementKind.SETTLED_VALIDATED_RESULT,
        TurnSettlementKind.SETTLED_OBSERVED_FAILURE,
        TurnSettlementKind.SETTLED_RESULT_NOT_APPLIED,
        TurnSettlementKind.NO_OUTSTANDING_RUN,
    }
)


class ObservationRegistrationRefusalReason(StrEnum):
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
    # failure that is not one of the host's own classified refusals
    # (issue #324): the same proven pre-effect condition, one safe token.
    OBSERVATION_REGISTRATION_FAILED = "observation_registration_failed"


@dataclass(frozen=True, slots=True)
class ObservationAttachmentInputs:
    """The neutral attachment inputs one observation connection needs.

    ``endpoint`` is the runtime integration's own connection endpoint and
    ``auth_token`` the current attach token resolved through the trusted
    credential path. Sensitive: the token is excluded from ``repr`` and
    never allowed into errors, logging, events, or call diagnostics.
    """

    endpoint: str
    auth_token: str

    def __post_init__(self) -> None:
        if not isinstance(self.endpoint, str) or not self.endpoint.strip():
            raise ValueError("ObservationAttachmentInputs.endpoint must be a non-empty string")
        if not isinstance(self.auth_token, str) or not self.auth_token.strip():
            raise ValueError("ObservationAttachmentInputs.auth_token must be a non-empty string")

    def __repr__(self) -> str:
        return f"ObservationAttachmentInputs(endpoint={self.endpoint!r}, auth_token=<withheld>)"


#: The trusted credential-resolution seam the composition injects: the same
#: current Connection/credential read the runtime attachment composition
#: uses, never an in-memory secret.
RuntimeCredentialResolver = Callable[..., tuple[Connection, ControlEndpointSecret]]

#: The adapter-owned endpoint resolution seam the composition injects: it
#: reads the Connection's safe configuration and returns the configured
#: endpoint, or ``None`` when none is configured; a malformed configuration
#: raises a ``ValueError`` subclass.
EndpointResolver = Callable[[Mapping[str, object]], str | None]


class DetachedTurnObservationSource(TurnEvidenceClassifier, Protocol):
    """The runtime integration's observation source port.

    The concrete integration owns its per-connection attachment lifecycle,
    its session subscriptions, the wrapping of its native events into
    neutral evidence envelopes, and the native interpretation behind the
    inherited :class:`TurnEvidenceClassifier` seam (issue #348). Failures of
    its own bounded transport mechanics surface as the typed
    :class:`DetachedTurnObservationSourceError`; unexpected exceptions
    propagate unchanged for the caller's own failure machinery.
    """

    def attach_connection(
        self, *, connection_id: UUID, inputs: ObservationAttachmentInputs
    ) -> None:
        """Establish the connection's observation attachment."""
        ...

    def connection_attached(self, connection_id: UUID) -> bool:
        """Whether the connection currently has a live observation attachment."""
        ...

    def observe_session(
        self,
        *,
        connection_id: UUID,
        external_session_id: str,
        deliver: Callable[[ObservedTurnEvidence], None],
    ) -> ObservationLease:
        """Establish the exact session's observation, delivering neutral
        evidence envelopes to ``deliver``, and return its lease."""
        ...

    def release_observation(self, lease: ObservationLease) -> None:
        """Release exactly the observation the lease was returned for."""
        ...

    def consult_status_evidence(
        self, *, connection_id: UUID, external_session_id: str
    ) -> NativeTurnEvidence:
        """Pull one bounded status-only evidence consultation."""
        ...

    def drop_connection(self, connection_id: UUID) -> None:
        """Discard an unusable connection attachment and its dead observations."""
        ...

    def dispose(self) -> None:
        """Release every source resource; idempotent and terminal."""
        ...


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
        "observation_lease",
        "observed",
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
    ) -> None:
        self.workspace_id = workspace_id
        self.task_id = task_id
        self.task_agent_session_id = task_agent_session_id
        self.connection_id = connection_id
        self.external_session_id = external_session_id
        self.observed = False
        self.observation_lease: ObservationLease | None = None
        self.lifecycle_token = 0


class DetachedTurnObservationHost:
    """The serving process's bounded detached-turn observation host.

    Constructed and started exactly once by the regular-mode serving
    lifespan and disposed deterministically at its teardown; construction
    creates no attachment, observation, or connection. Attachments are
    established lazily per Connection on first registration and shared by
    that connection's observed sessions through the injected source.
    """

    def __init__(
        self,
        database_pool_provider: Callable[[], DatabasePool],
        *,
        observation_source: DetachedTurnObservationSource,
        runtime_credential_resolver: RuntimeCredentialResolver = (
            resolve_connection_runtime_credential
        ),
        control_endpoint_resolver: EndpointResolver,
        bootstrap_continuation_trigger: BootstrapContinuationTrigger | None = None,
    ) -> None:
        self._pool_provider = database_pool_provider
        self._source = observation_source
        self._credential_resolver = runtime_credential_resolver
        self._endpoint_resolver = control_endpoint_resolver
        self._bootstrap_continuation_trigger = bootstrap_continuation_trigger
        self._lock = threading.Lock()
        self._started = False
        self._disposed = False
        self._sessions: dict[UUID, _SessionObservation] = {}
        self._attach_locks: dict[UUID, threading.Lock] = {}

    @property
    def disposed(self) -> bool:
        """Whether this host has been terminally disposed."""
        with self._lock:
            return self._disposed

    def start(self) -> None:
        """Mark the host running; idempotent, creates nothing."""
        with self._lock:
            if self._disposed:
                raise ConflictError(
                    "the detached-turn observation host has been disposed and cannot restart"
                )
            self._started = True

    def dispose(self) -> None:
        """Release every observation resource; idempotent and terminal.

        Stops admission first, then releases every session observation and
        disposes the source's connection attachments. Disposal never deletes
        or replaces an external session and never implies any workflow
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
            self._attach_locks.clear()
        for entry in sessions:
            lease = entry.observation_lease
            entry.observation_lease = None
            if lease is not None:
                self._release_lease_quietly(lease)
        self._source.dispose()

    def register_session(
        self,
        *,
        workspace_id: UUID,
        task_id: UUID,
        task_agent_session_id: UUID,
        connection_id: UUID,
        external_session_id: str,
    ) -> None:
        """Establish the positive detached-turn observation of one exact session.

        The pre-admission gate of the detached dispatch: returns only once
        the exact external session is actively observed through the
        connection's current trusted attachment. Every expected failure
        raises :class:`ObservationRegistrationRefusedError` — the admission
        must not proceed without the observation, because the runtime
        integration cannot replay an arbitrary missed terminal result; known
        attachment-configuration refusals (a missing, cross-Workspace,
        disabled, or non-Cline connection; an unusable credential reference;
        a missing or malformed control endpoint) are typed into the same
        refusal, and unexpected exceptions propagate unchanged for the
        caller's own failure machinery.

        The durable binding is proven within the authenticated
        Workspace/Connection scope before any secret access or dial; a
        registration whose identity tuple does not match the authoritative
        binding fails closed. A repeated registration of an already-observed
        session renews its lifecycle lease and reuses the live observation
        (the successor guard), never double-observing.
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
                    "detached-turn observation registration resolved no matching session "
                    "binding in its connection scope"
                )
                raise self._refusal(ObservationRegistrationRefusalReason.SESSION_BINDING_UNRESOLVED)
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

        The detached session establishment's pre-admission gate (issue
        #324): a CONNECTING binding deliberately carries no bound external
        identity, so this registration proves the identity through the
        binding's outstanding ``session_initialization`` receipt instead —
        the receipt must be outstanding in this authenticated
        Workspace/Connection scope and carry exactly the provisional
        ``created_external_session_id`` being registered. Every expected
        failure raises the same
        :class:`ObservationRegistrationRefusedError` refusal taxonomy; the
        admission must not proceed without the observation. A repeated
        registration renews the lifecycle lease and reuses the live
        observation exactly like the established-session gate.
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
                raise self._refusal(ObservationRegistrationRefusalReason.SESSION_BINDING_UNRESOLVED)
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
        """Attach and observe a registration whose binding is proven."""
        with self._lock:
            if self._disposed:
                raise self._refusal(ObservationRegistrationRefusalReason.OBSERVER_UNAVAILABLE)
            existing = self._sessions.get(task_agent_session_id)
            if existing is not None and existing.observed:
                # Successor registration: renew the lease and reuse the live
                # observation; a settlement cleanup whose lease this renewal
                # supersedes leaves the observation in place.
                existing.lifecycle_token += 1
                return
            if existing is None and len(self._sessions) >= _MAX_CONCURRENT_SESSION_OBSERVATIONS:
                logger.warning(
                    "detached-turn observation registration refused: the host is at "
                    "its concurrent session bound"
                )
                raise self._refusal(ObservationRegistrationRefusalReason.OBSERVER_AT_CAPACITY)
        with self._connection_attach_lock(connection_id):
            with self._lock:
                if self._disposed:
                    raise self._refusal(ObservationRegistrationRefusalReason.OBSERVER_UNAVAILABLE)
                winner = self._sessions.get(task_agent_session_id)
                if winner is not None and winner.observed:
                    return  # a concurrent registration established the observation
            self._ensure_attachment(workspace_id=workspace_id, connection_id=connection_id)
            try:
                lease = self._source.observe_session(
                    connection_id=connection_id,
                    external_session_id=external_session_id,
                    deliver=self._make_listener(task_agent_session_id),
                )
            except DetachedTurnObservationSourceError as error:
                logger.warning(
                    "detached-turn observation subscription failed (%s) for connection %s",
                    error.reason,
                    connection_id,
                )
                # The failed observation proves the current attachment unusable:
                # drop it so the next registration re-attaches through a fresh
                # credential read and re-establishes every outstanding session.
                self._drop_attachment(connection_id)
                raise self._refusal(
                    ObservationRegistrationRefusalReason.OBSERVATION_SUBSCRIPTION_FAILED
                ) from None
            self._install_observation(
                lease=lease,
                task_agent_session_id=task_agent_session_id,
                previous=existing,
                workspace_id=workspace_id,
                task_id=task_id,
                connection_id=connection_id,
                external_session_id=external_session_id,
            )

    def _require_running(self) -> None:
        """Fail the registration closed when the host is not running."""
        with self._lock:
            if not self._started or self._disposed:
                raise self._refusal(ObservationRegistrationRefusalReason.OBSERVER_UNAVAILABLE)

    def _refusal(
        self, reason: ObservationRegistrationRefusalReason
    ) -> ObservationRegistrationRefusedError:
        return ObservationRegistrationRefusedError(
            "positive detached-turn observation could not be established for the exact session",
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

    def _attachment_inputs(
        self, *, workspace_id: UUID, connection_id: UUID
    ) -> ObservationAttachmentInputs:
        """Resolve the connection's current trusted attachment inputs.

        The same trusted read the runtime attachment composition uses —
        never an in-memory secret. Known, positively established
        pre-attachment failures — a missing or cross-Workspace connection, a
        disabled connection, an unusable credential reference, a missing or
        malformed control endpoint — are typed into the registration refusal
        the dispatch gates the admission on: they are proven pre-effect
        conditions of the observation, never workflow conflicts escaping the
        host and never uncertain outcomes. Unexpected exceptions propagate
        unchanged.
        """
        try:
            credential_connection, secret = self._credential_resolver(
                self._pool_provider(), workspace_id=workspace_id, connection_id=connection_id
            )
            if credential_connection.adapter is not AdapterType.CLINE:
                # Defensive durable-state invariant: a non-Cline connection has
                # nothing to observe and is never guessed around.
                raise ConflictError(
                    "the connection routed for this task/role is not an enabled Cline connection"
                )
            endpoint = self._endpoint_resolver(dict(credential_connection.safe_config))
            if endpoint is None:
                raise ConflictError(
                    "the connection has no configured control endpoint; it is stored but "
                    "not attachable until the Owner configures one"
                )
        except (NotFoundError, ConflictError, ValueError) as error:
            logger.warning(
                "detached-turn observation attachment refused (%s) for connection %s",
                type(error).__name__,
                connection_id,
            )
            raise self._refusal(
                ObservationRegistrationRefusalReason.OBSERVATION_ATTACHMENT_FAILED
            ) from None
        return ObservationAttachmentInputs(
            endpoint=endpoint,
            auth_token=secret.secret_value(),
        )

    def _ensure_attachment(self, *, workspace_id: UUID, connection_id: UUID) -> None:
        """Return with the connection's attachment live, freshly attaching when absent.

        Called under the connection's attach lock. A fresh attach resolves
        the connection's current attachment inputs through the trusted read
        and runs the bounded reconnect reconciliation for the connection's
        still-registered sessions before returning.
        """
        if self._source.connection_attached(connection_id):
            return
        inputs = self._attachment_inputs(workspace_id=workspace_id, connection_id=connection_id)
        try:
            self._source.attach_connection(connection_id=connection_id, inputs=inputs)
        except DetachedTurnObservationSourceError as error:
            logger.warning(
                "detached-turn observation attachment failed (%s) for connection %s",
                error.reason,
                connection_id,
            )
            refusal_reason = (
                ObservationRegistrationRefusalReason.OBSERVER_UNAVAILABLE
                if error.reason == DetachedTurnObservationSourceError.ATTACHMENT_UNAVAILABLE
                else ObservationRegistrationRefusalReason.OBSERVATION_ATTACHMENT_FAILED
            )
            raise self._refusal(refusal_reason) from None
        with self._lock:
            reconnected = any(
                entry.connection_id == connection_id for entry in self._sessions.values()
            )
        if reconnected:
            self._reconcile_reconnected_connection(connection_id=connection_id)

    def _drop_attachment(self, connection_id: UUID) -> None:
        """Discard an unusable attachment and its dead session observations.

        The next registration for this connection re-attaches through a
        fresh credential read and the bounded reconnect reconciliation
        re-establishes the still-outstanding sessions.
        """
        with self._lock:
            for entry in self._sessions.values():
                if entry.connection_id == connection_id:
                    entry.observed = False
        self._source.drop_connection(connection_id)

    def _install_observation(
        self,
        *,
        lease: ObservationLease,
        task_agent_session_id: UUID,
        previous: _SessionObservation | None,
        workspace_id: UUID,
        task_id: UUID,
        connection_id: UUID,
        external_session_id: str,
    ) -> None:
        """Install a freshly established observation, or discard a losing race.

        Called under the connection's attach lock; the registry transition
        itself is guarded by the registry lock and never releases an
        observation under it. A concurrent registration that installed its
        own live observation first, a disposal, or a late capacity breach
        discards the newcomer's observation.
        """
        doomed = False
        doomed_reason: ObservationRegistrationRefusalReason | None = None
        with self._lock:
            if self._disposed:
                doomed = True
                doomed_reason = ObservationRegistrationRefusalReason.OBSERVER_UNAVAILABLE
            else:
                current = self._sessions.get(task_agent_session_id)
                if current is not None and current.observed:
                    doomed = True  # a concurrent registration owns the observation
                elif (
                    previous is None and len(self._sessions) >= _MAX_CONCURRENT_SESSION_OBSERVATIONS
                ):
                    doomed = True
                    doomed_reason = ObservationRegistrationRefusalReason.OBSERVER_AT_CAPACITY
                else:
                    if current is None:
                        current = _SessionObservation(
                            workspace_id=workspace_id,
                            task_id=task_id,
                            task_agent_session_id=task_agent_session_id,
                            connection_id=connection_id,
                            external_session_id=external_session_id,
                        )
                        self._sessions[task_agent_session_id] = current
                    current.observed = True
                    current.observation_lease = lease
                    current.lifecycle_token += 1
        if not doomed:
            return
        self._release_lease_quietly(lease)
        if doomed_reason is None:
            return  # a concurrent registration owns the observation
        if doomed_reason is ObservationRegistrationRefusalReason.OBSERVER_AT_CAPACITY:
            logger.warning(
                "detached-turn observation registration refused: the host is at its "
                "concurrent session bound"
            )
        raise self._refusal(doomed_reason)

    def _release_lease_quietly(self, lease: ObservationLease) -> None:
        """Release one observation lease; failures never mask the caller."""
        try:
            self._source.release_observation(lease)
        except Exception as error:  # noqa: BLE001 - best-effort teardown
            logger.warning(
                "detached-turn observation release failed (%s)",
                type(error).__name__,
            )

    def _make_listener(self, task_agent_session_id: UUID) -> Callable[[ObservedTurnEvidence], None]:
        """Build the contained delivery listener of one session observation."""

        def listener(evidence: ObservedTurnEvidence) -> None:
            try:
                self._ingest_evidence(task_agent_session_id, evidence)
            except Exception as error:  # noqa: BLE001 - the source reader is protected
                logger.warning(
                    "detached-turn evidence ingestion ended unexpectedly (%s) for task session %s",
                    type(error).__name__,
                    task_agent_session_id,
                )

        return listener

    def _ingest_evidence(self, task_agent_session_id: UUID, evidence: ObservedTurnEvidence) -> None:
        """Route one delivered evidence envelope for its registered session.

        The envelope's own session identity is authoritative: a session-
        scoped observation must only deliver the exact session's evidence,
        and a mismatched identity is never routed or attributed. The
        delivery-time lifecycle lease is captured under the registry lock so
        the post-settlement cleanup can detect a successor registration.
        """
        with self._lock:
            entry = self._sessions.get(task_agent_session_id)
            if entry is None:
                return  # disposed, superseded, or already cleaned up
            lease = entry.lifecycle_token
        if evidence.external_session_id != entry.external_session_id:
            logger.warning(
                "a delivered evidence envelope carried a foreign session identity "
                "for a registered task session; it is not routed"
            )
            return
        if evidence.family is TurnEvidenceFamily.TURN_TERMINAL:
            self._route_terminal_observation(entry, lease, evidence)
            return
        self._route_start_observation(entry, evidence)

    def _route_terminal_observation(
        self,
        entry: _SessionObservation,
        lease: int,
        evidence: ObservedTurnEvidence,
    ) -> None:
        """Settle one delivered terminal observation through the shared seam.

        The seam owns attribution, duplicate/stale rejection, staging for
        the admission race, formal validation, and every workflow
        consequence; this boundary only delivers the observation within its
        authenticated scope and contains every failure so the source's
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
                outcome = settle_observed_turn_completion(
                    self._pool_provider(),
                    classifier=self._source,
                    workspace_id=entry.workspace_id,
                    connection_id=entry.connection_id,
                    evidence=evidence,
                )
            except Exception as error:  # noqa: BLE001 - best-effort ingestion
                logger.warning(
                    "detached-turn completion settlement failed (%s) for task session %s",
                    type(error).__name__,
                    entry.task_agent_session_id,
                )
                return
        if outcome.kind in _CLEANUP_SETTLEMENT_KINDS:
            self._cleanup_after_settlement(entry, lease)
        if (
            outcome.kind is TurnSettlementKind.SETTLED_VALIDATED_RESULT
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
        self, entry: _SessionObservation, evidence: ObservedTurnEvidence
    ) -> None:
        """Corroborate one delivered turn-start observation through the seam.

        Read-only: the seam resolves the durable correlation and records
        contradictions itself, on this integration's neutral attribution
        classification; this boundary delivers the evidence exactly as
        observed and contains every failure.
        """
        try:
            check_turn_start_identity(
                self._pool_provider(),
                classifier=self._source,
                workspace_id=entry.workspace_id,
                connection_id=entry.connection_id,
                evidence=evidence,
            )
        except Exception as error:  # noqa: BLE001 - best-effort ingestion
            logger.warning(
                "detached-turn start corroboration failed (%s) for task session %s",
                type(error).__name__,
                entry.task_agent_session_id,
            )

    def _cleanup_after_settlement(self, entry: _SessionObservation, lease: int) -> None:
        """Release the session's observation unless a successor renewed it.

        The lease captured at delivery time is compared under the registry
        lock: a registration that arrived between the settlement and this
        cleanup renewed the session's observation and the cleanup must not
        disconnect it — the turns never overlapped, but the live observation
        now belongs to the successor's authorized turn.
        """
        released = self._detach_renewed_entry(entry, lease)
        if released is None:
            return
        self._release_lease_quietly(released)

    def _detach_renewed_entry(
        self, entry: _SessionObservation, lease: int
    ) -> ObservationLease | None:
        """Detach the entry iff it is still the unrenewed observed session."""
        with self._lock:
            if self._sessions.get(entry.task_agent_session_id) is not entry:
                return None
            if entry.lifecycle_token != lease:
                return None  # a successor registration renewed the observation
            lease_handle = entry.observation_lease
            entry.observation_lease = None
            del self._sessions[entry.task_agent_session_id]
            return lease_handle

    def _reconcile_reconnected_connection(self, *, connection_id: UUID) -> None:
        """Re-establish observation after a fresh attachment replaced a dead one.

        Bounded by the registry's session entries; never a table scan and
        never a poll. Each still-registered session is re-observed on the
        fresh attachment, entries whose receipt is no longer outstanding are
        released, and each outstanding admitted receipt's status-only
        evidence is consulted through the seam's classification — where a
        completed status alone is never success evidence and ambiguity stays
        unresolved.
        """
        with self._lock:
            entries = [
                entry for entry in self._sessions.values() if entry.connection_id == connection_id
            ]
        for entry in entries:
            if entry.observed:
                continue
            try:
                lease = self._source.observe_session(
                    connection_id=connection_id,
                    external_session_id=entry.external_session_id,
                    deliver=self._make_listener(entry.task_agent_session_id),
                )
            except DetachedTurnObservationSourceError as error:
                logger.warning(
                    "detached-turn reconnect re-observation failed (%s) for task session %s",
                    error.reason,
                    entry.task_agent_session_id,
                )
                continue
            with self._lock:
                if self._disposed or self._sessions.get(entry.task_agent_session_id) is not entry:
                    doomed = lease
                else:
                    entry.observed = True
                    entry.observation_lease = lease
                    doomed = None
            if doomed is not None:
                self._release_lease_quietly(doomed)
        for entry in entries:
            self._reconcile_session_receipt(entry)

    def _reconcile_session_receipt(self, entry: _SessionObservation) -> None:
        """Recheck one entry's outstanding receipt and consult status evidence."""
        try:
            receipt = get_outstanding_task_detached_turn_receipt(
                self._pool_provider(), task_agent_session_id=entry.task_agent_session_id
            )
        except Exception as error:  # noqa: BLE001 - best-effort reconciliation
            logger.warning(
                "detached-turn reconnect receipt recheck failed (%s) for task session %s",
                type(error).__name__,
                entry.task_agent_session_id,
            )
            return
        if receipt is None:
            # The turn reached a terminal receipt while observation was down;
            # this entry observes nothing further.
            released = self._detach_entry(entry)
            if released is not None:
                self._release_lease_quietly(released)
            return
        if receipt.status is not TaskDetachedTurnReceiptStatus.ADMITTED:
            return  # still claimed: the acknowledgement attach owns any staged completion
        try:
            status_evidence = self._source.consult_status_evidence(
                connection_id=entry.connection_id,
                external_session_id=entry.external_session_id,
            )
        except Exception as error:  # noqa: BLE001 - the consultation stays best-effort
            logger.warning(
                "detached-turn reconnect status consultation failed (%s) for task session %s",
                type(error).__name__,
                entry.task_agent_session_id,
            )
            return
        self._classify_reconnected_receipt(entry, receipt.id, status_evidence)

    def _classify_reconnected_receipt(
        self,
        entry: _SessionObservation,
        receipt_id: UUID,
        status_evidence: NativeTurnEvidence,
    ) -> None:
        """Hand one reconnected receipt's status-only evidence to the seam."""
        with application_span(_TRACER_SCOPE, _RECONNECT_SPAN_NAME) as span:
            annotate_span(
                span,
                operation=_RECONNECT_SPAN_NAME,
                workspace_id=str(entry.workspace_id),
                task_id=str(entry.task_id),
                connection_id=str(entry.connection_id),
            )
            try:
                classification = classify_acknowledged_turn_status(
                    self._pool_provider(),
                    classifier=self._source,
                    workspace_id=entry.workspace_id,
                    task_id=entry.task_id,
                    receipt_id=receipt_id,
                    status_evidence=status_evidence,
                )
            except Exception as error:  # noqa: BLE001 - the consultation stays best-effort
                logger.warning(
                    "detached-turn reconnect status classification failed (%s) for task session %s",
                    type(error).__name__,
                    entry.task_agent_session_id,
                )
                return
        if classification.kind is TurnStatusClassificationKind.CLASSIFIED_FAILURE:
            logger.warning(
                "detached-turn reconnect classified the outstanding turn's status-only "
                "failed evidence for task session %s",
                entry.task_agent_session_id,
            )

    def _detach_entry(self, entry: _SessionObservation) -> ObservationLease | None:
        """Detach one entry atomically under the registry lock."""
        with self._lock:
            if self._sessions.get(entry.task_agent_session_id) is not entry:
                return None
            lease = entry.observation_lease
            entry.observation_lease = None
            del self._sessions[entry.task_agent_session_id]
            return lease
