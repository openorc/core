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
- Each observation carries the receipt-epoch attestation of its exact
  outstanding turn (issue #341): the epoch binds to the outstanding
  receipt's identity and advances — resetting its evidence — whenever a
  registration resolves a different outstanding receipt, including on the
  successor-guard path that reuses a live subscription. The epoch attests
  ``ACTIVE`` only when its receipt is autonomous (a formal family, never
  the discussion family), its start was positively attributed, and
  qualified model-backed activity or a usage signal was observed since
  that start; every other state is honestly ``UNATTESTED``. A usage
  signal additionally drives the shared best-effort cost-evaluation engine
  (:class:`openorc.services.cline_usage_observation.SessionCostEvaluationDriver`)
  over the neutral usage consultation and this attestation, so the
  existing cost policy enforces while the dispatch worker is gone — the
  composition-rooted post-commit Interrupt dispatcher is injected
  alongside the source consultation capability, and without either the
  usage signals attest but never trigger evaluations.
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
    OWNER_REVIEWER_DISCUSSION_FAMILY,
    DetachedTurnObservationSourceError,
    NativeTurnEvidence,
    ObservationLease,
    ObservedTurnEvidence,
    SessionUsageReading,
    TaskDetachedTurnReceiptStatus,
    TurnActivityAttestation,
    TurnEvidenceFamily,
)
from openorc.domain.pending_interactions import PendingTaskRuntimeInteractionOperation
from openorc.observability import annotate_span, application_span
from openorc.persistence.detached_turns import (
    find_outstanding_session_initialization_receipt,
    get_outstanding_task_detached_turn_receipt,
)
from openorc.persistence.pool import DatabasePool
from openorc.persistence.sessions import find_task_agent_session_binding
from openorc.services.cline_usage_observation import (
    InterruptDispatch,
    SessionCostEvaluationDriver,
)
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
from openorc.services.task_completion_handoff import DetachedTurnCompletionHandoff
from openorc.services.task_detached_turn_correlation import (
    TurnEvidenceClassifier,
    TurnSettlementKind,
    TurnStartCorroboration,
    TurnStatusClassificationKind,
    check_turn_start_identity,
    classify_acknowledged_turn_status,
    settle_observed_turn_completion,
)

__all__ = [
    "DetachedTurnObservationSourceError",
    "DetachedTurnObservationHost",
    "DetachedTurnObservationSource",
    "ObservationAttachmentInputs",
    "ObservationRegistrationRefusalReason",
    "SessionUsageConsultationSource",
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
        TurnSettlementKind.SETTLED_INTERRUPTED_TURN,
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


class SessionUsageConsultationSource(Protocol):
    """The optional neutral usage-consultation capability (issue #341).

    A structural capability beside :class:`DetachedTurnObservationSource`:
    a concrete integration MAY expose ``consult_session_usage`` to serve
    the observation host's detached-turn cost evaluations, exactly like
    the optional runtime capabilities beside ``AgentRuntimeAdapter``. The
    host duck-types the capability fail-closed: a source without it keeps
    every detached-turn observation behavior and simply never triggers a
    cost evaluation from its usage pulses. The returned reading is the
    neutral sanitized cumulative value — ``None`` is unknown truth.
    """

    def consult_session_usage(
        self, *, connection_id: UUID, external_session_id: str
    ) -> SessionUsageReading:
        """Read one bounded cumulative usage consultation."""
        ...


class _TurnAttestationEpoch:
    """The receipt-bound attestation epoch of one exact outstanding turn (issue #341).

    The epoch binds to the exact outstanding receipt's identity and carries
    only that turn's own evidence: whether the receipt is autonomous (a
    formal family — never the discussion family), whether its admitted
    turn's start was positively attributed, and whether qualified
    model-backed activity or a usage signal was observed for the exact
    session since that start. The flags live on the epoch, so a successor
    registration's epoch replacement — atomically swapping the whole object
    under the registry lock — can never inherit, erase, or mix predecessor
    evidence: turn A's activity can never attest turn B, and predecessor
    cleanup acts only on the epoch it captured. The epoch holds no content
    and is never persisted: absent evidence stays honestly unattested.
    """

    __slots__ = ("activity_observed", "autonomous_turn", "receipt_id", "start_claimed")

    def __init__(self, *, receipt_id: UUID | None, autonomous_turn: bool) -> None:
        self.receipt_id = receipt_id
        self.autonomous_turn = autonomous_turn
        self.start_claimed = False
        self.activity_observed = False

    def __repr__(self) -> str:
        return (
            f"_TurnAttestationEpoch(receipt_id={self.receipt_id!r}, "
            f"autonomous_turn={self.autonomous_turn!r}, "
            f"start_claimed={self.start_claimed!r}, "
            f"activity_observed={self.activity_observed!r})"
        )


class _SessionObservation:
    """One registered exact-session observation and its lifecycle lease.

    ``lifecycle_token`` is the small successor guard: every registration
    that reuses the session's live observation renews it, and a cleanup
    whose lease has since been renewed leaves the observation in place.
    ``attestation_epoch`` is the exact outstanding turn's receipt-bound
    evidence epoch (issue #341), replaced whenever a registration advances
    to a different outstanding receipt.
    """

    __slots__ = (
        "attestation_epoch",
        "connection_id",
        "cost_evaluation",
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
        self.attestation_epoch: _TurnAttestationEpoch | None = None
        self.cost_evaluation: SessionCostEvaluationDriver | None = None


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
        completion_handoff: DetachedTurnCompletionHandoff | None = None,
        cost_limit_interrupt_dispatch: InterruptDispatch | None = None,
    ) -> None:
        self._pool_provider = database_pool_provider
        self._source = observation_source
        self._credential_resolver = runtime_credential_resolver
        self._endpoint_resolver = control_endpoint_resolver
        self._completion_handoff = completion_handoff
        # The composition-rooted post-commit Interrupt dispatcher (issue
        # #341): the host drives the shared cost-evaluation engine with it
        # only when BOTH trusted seams exist — the source's neutral usage
        # consultation capability and this dispatcher. A source without the
        # consultation capability, or a composition without the dispatcher,
        # keeps every usage signal as attestation evidence but never
        # triggers a cost evaluation: a cost-limit block is never composed
        # without a dispatch path for its registered Interrupts.
        self._cost_limit_interrupt_dispatch = cost_limit_interrupt_dispatch
        self._usage_consultation = getattr(observation_source, "consult_session_usage", None)
        self._cost_evaluation_supported = (
            callable(self._usage_consultation) and cost_limit_interrupt_dispatch is not None
        )
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
            driver = entry.cost_evaluation
            entry.cost_evaluation = None
            if driver is not None:
                driver.dispose()
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
            # The registration's receipt-epoch resolution (issue #341): the
            # exact outstanding receipt this observation belongs to. The
            # dispatch flow commits the receipt companion strictly before
            # the registration gate, so the outstanding receipt exists here
            # under the single-outstanding invariant; a transiently missing
            # one binds a non-attesting epoch instead of guessing.
            try:
                receipt = get_outstanding_task_detached_turn_receipt(
                    self._pool_provider(), task_agent_session_id=task_agent_session_id
                )
            except Exception as error:  # noqa: BLE001 - the read stays best-effort
                logger.warning(
                    "detached-turn observation registration receipt read failed (%s) "
                    "for task session %s",
                    type(error).__name__,
                    task_agent_session_id,
                )
                receipt = None
            self._register_proven_session(
                workspace_id=workspace_id,
                task_id=task_id,
                task_agent_session_id=task_agent_session_id,
                connection_id=connection_id,
                external_session_id=external_session_id,
                receipt_id=None if receipt is None else receipt.id,
                autonomous_turn=(
                    False
                    if receipt is None
                    else receipt.expected_formal_family != OWNER_REVIEWER_DISCUSSION_FAMILY
                ),
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
                receipt_id=receipt.id,
                autonomous_turn=(
                    receipt.expected_formal_family != OWNER_REVIEWER_DISCUSSION_FAMILY
                ),
            )

    def _register_proven_session(
        self,
        *,
        workspace_id: UUID,
        task_id: UUID,
        task_agent_session_id: UUID,
        connection_id: UUID,
        external_session_id: str,
        receipt_id: UUID | None,
        autonomous_turn: bool,
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
                self._advance_attestation_epoch_locked(
                    existing, receipt_id=receipt_id, autonomous_turn=autonomous_turn
                )
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
                    # A concurrent registration established the observation:
                    # renew and advance this registration's own receipt epoch.
                    winner.lifecycle_token += 1
                    self._advance_attestation_epoch_locked(
                        winner, receipt_id=receipt_id, autonomous_turn=autonomous_turn
                    )
                    return
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
                receipt_id=receipt_id,
                autonomous_turn=autonomous_turn,
            )

    def _advance_attestation_epoch_locked(
        self, entry: _SessionObservation, *, receipt_id: UUID | None, autonomous_turn: bool
    ) -> None:
        """Advance the entry's receipt-epoch when registration resolves another receipt.

        Called under the registry lock. The same outstanding receipt keeps
        its in-progress evidence (a renewal of the same turn); a different
        receipt — including a first registration over reused evidence and a
        successor turn — replaces the whole epoch object, so predecessor
        evidence can never attest the successor's turn.
        """
        current = entry.attestation_epoch
        if current is not None and current.receipt_id == receipt_id:
            return
        entry.attestation_epoch = _TurnAttestationEpoch(
            receipt_id=receipt_id, autonomous_turn=autonomous_turn
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
        re-establishes the still-outstanding sessions. Each dropped
        observation's attestation evidence is invalidated (issue #341): an
        explicitly lost observation attests nothing, and a live turn is
        re-corroborated only by fresh qualifying activity on the
        re-established, actively observed session.
        """
        with self._lock:
            for entry in self._sessions.values():
                if entry.connection_id == connection_id:
                    entry.observed = False
                    epoch = entry.attestation_epoch
                    if epoch is not None:
                        epoch.start_claimed = False
                        epoch.activity_observed = False
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
        receipt_id: UUID | None,
        autonomous_turn: bool,
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
                    # The registration's own receipt-epoch (issue #341): the
                    # fresh observation binds the exact outstanding receipt
                    # this registration resolved, and — when the trusted
                    # composition provided both cost seams — the per-session
                    # evaluation driver the usage pulses request.
                    current.attestation_epoch = _TurnAttestationEpoch(
                        receipt_id=receipt_id, autonomous_turn=autonomous_turn
                    )
                    if self._cost_evaluation_supported:
                        current.cost_evaluation = self._make_cost_evaluation_driver(current)
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

    def _make_cost_evaluation_driver(
        self, entry: _SessionObservation
    ) -> SessionCostEvaluationDriver:
        """Compose the session's best-effort cost-evaluation driver (issue #341).

        The shared engine behind the lease-attached observer, parameterized
        by this process's trusted seams: the source's neutral cumulative
        usage consultation for the exact session, this host's receipt-epoch
        attestation sampled per evaluation, and the composition-rooted
        post-commit Interrupt dispatcher. Called only when both seams exist.
        """
        assert self._usage_consultation is not None  # guarded by _cost_evaluation_supported
        assert self._cost_limit_interrupt_dispatch is not None  # guarded likewise
        consultation = self._usage_consultation
        dispatch = self._cost_limit_interrupt_dispatch
        return SessionCostEvaluationDriver(
            self._pool_provider(),
            workspace_id=entry.workspace_id,
            task_id=entry.task_id,
            task_agent_session_id=entry.task_agent_session_id,
            connection_id=entry.connection_id,
            read_cumulative_usage=lambda: (
                consultation(
                    connection_id=entry.connection_id,
                    external_session_id=entry.external_session_id,
                ).aggregate_total_usd
            ),
            attest_autonomous_turn=lambda: (
                self.attest_session_activity(
                    workspace_id=entry.workspace_id,
                    task_id=entry.task_id,
                    task_agent_session_id=entry.task_agent_session_id,
                    connection_id=entry.connection_id,
                    external_session_id=entry.external_session_id,
                )
                is TurnActivityAttestation.ACTIVE
            ),
            dispatch_interrupt=dispatch,
        )

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
        delivery-time lifecycle lease and attestation epoch are captured
        under the registry lock so the post-settlement cleanup and a
        successor registration can detect what this delivery belongs to.
        """
        with self._lock:
            entry = self._sessions.get(task_agent_session_id)
            if entry is None:
                return  # disposed, superseded, or already cleaned up
            if evidence.external_session_id != entry.external_session_id:
                # The exact-session isolation guard (issue #341): a foreign
                # identity envelope is never routed and never mutates this
                # session's attestation epoch — this check precedes every
                # epoch mutation and evaluation trigger below, so foreign
                # activity can never attest and a foreign terminal can never
                # erase valid attestation.
                foreign = True
                epoch = None
                driver = None
            else:
                foreign = False
                lease = entry.lifecycle_token
                epoch = entry.attestation_epoch
                if evidence.family in (
                    TurnEvidenceFamily.TURN_ACTIVITY,
                    TurnEvidenceFamily.TURN_USAGE,
                ):
                    # The qualified activity/usage pulse (issue #341): the bare
                    # fact lands on the exact epoch captured here — a successor
                    # registration that replaced the epoch between the delivery
                    # and this ingest cannot inherit predecessor evidence.
                    if epoch is not None:
                        epoch.activity_observed = True
                    driver = entry.cost_evaluation
                elif evidence.family is TurnEvidenceFamily.TURN_TERMINAL and epoch is not None:
                    # The observed run ended: its epoch's in-progress evidence
                    # is cleared immediately, so even a settlement still in
                    # flight can never leave a finished turn attested.
                    epoch.start_claimed = False
                    epoch.activity_observed = False
                    driver = None
                else:
                    driver = None
        if foreign:
            logger.warning(
                "a delivered evidence envelope carried a foreign session identity "
                "for a registered task session; it is not routed"
            )
            return
        if driver is not None and evidence.family is TurnEvidenceFamily.TURN_USAGE:
            # One coalesced cumulative cost evaluation per usage signal
            # (issue #341), driven from the process-resident observation
            # lifetime while the dispatch worker is gone. Best-effort and
            # contained by the shared engine.
            driver.request_evaluation()
            return
        if evidence.family is TurnEvidenceFamily.TURN_TERMINAL:
            self._route_terminal_observation(entry, lease, evidence)
            return
        if evidence.family in (
            TurnEvidenceFamily.TURN_ACTIVITY,
            TurnEvidenceFamily.TURN_USAGE,
        ):
            return  # the pulse is recorded; nothing else routes
        self._route_start_observation(entry, epoch, evidence)

    def attest_session_activity(
        self,
        *,
        workspace_id: UUID,
        task_id: UUID,
        task_agent_session_id: UUID,
        connection_id: UUID,
        external_session_id: str,
    ) -> TurnActivityAttestation:
        """Return the exact session's autonomous-turn activity attestation (issue #341).

        The trusted read-only query behind the detached-turn cost path: the
        attestation is ``ACTIVE`` only when this host actively observes the
        exact identity tuple, the observation's receipt-bound epoch belongs
        to an outstanding AUTONOMOUS (formal-family) admitted turn, that
        turn's start was positively attributed to it, and qualified
        model-backed activity or a usage signal was observed for the exact
        session since that start. Every other state — an unobserved
        session, a discussion receipt, a start without activity, a turn
        whose terminal was delivered, a foreign identity tuple — is
        honestly ``UNATTESTED``, never ``ACTIVE``. The value is advisory
        attestation evidence for the application cost policy and is never a
        workflow decision.
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
        with self._lock:
            entry = self._sessions.get(task_agent_session_id)
            if (
                entry is None
                or not entry.observed
                or entry.workspace_id != workspace_id
                or entry.task_id != task_id
                or entry.connection_id != connection_id
                or entry.external_session_id != external_session_id
            ):
                # A lost observation (a dropped attachment, a disposed host,
                # an unregistered or foreign session) attests nothing
                # (issue #341): unavailable evidence is ``UNATTESTED``, and
                # reconnection must re-corroborate with fresh evidence.
                return TurnActivityAttestation.UNATTESTED
            epoch = entry.attestation_epoch
            if (
                epoch is None
                or not epoch.autonomous_turn
                or not epoch.start_claimed
                or not epoch.activity_observed
            ):
                return TurnActivityAttestation.UNATTESTED
            return TurnActivityAttestation.ACTIVE

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
            and self._completion_handoff is not None
        ):
            # A validated result settlement committed (issues #324/#333):
            # the ONE post-commit completion handoff owns the explicit
            # family routing and the resumption. Strictly post-commit,
            # contained, never a workflow authority; a failed routing
            # leaves the durable state to the existing recovery semantics.
            # The in-memory validated result is forwarded only for the
            # PR_COMPOSE family (issue #337): its presentation material is
            # deliberately not durable, so the same invocation's bounded
            # publication continuation receives it here; every other family
            # re-derives its continuation from committed durable state.
            try:
                self._completion_handoff.handoff_settled_completion(
                    workspace_id=entry.workspace_id,
                    task_id=entry.task_id,
                    operation=outcome.receipt.operation,
                    validated_result=(
                        outcome.formal_result
                        if outcome.receipt.operation
                        is PendingTaskRuntimeInteractionOperation.PR_COMPOSE_DISPATCH
                        else None
                    ),
                )
            except Exception as error:  # noqa: BLE001 - best-effort handoff
                logger.warning(
                    "the detached-completion handoff failed (%s) after the "
                    "validated settlement for task session %s",
                    type(error).__name__,
                    entry.task_agent_session_id,
                )
        if (
            outcome.kind is TurnSettlementKind.SETTLED_INTERRUPTED_TURN
            and outcome.receipt is not None
            and self._completion_handoff is not None
        ):
            # A settled interrupted-turn observation committed (issue #342):
            # the ONE post-commit completion handoff owns the Interrupt
            # settlement — the attached adapter's D6b guard release and the
            # annotated intent's outcome recording — exactly as it owns the
            # validated families' continuations, strictly after the
            # settlement composition committed and with no database
            # transaction across the adapter call. Contained best-effort
            # inside the handoff, never a workflow authority: a failed
            # settlement leaves the durable annotated intent exactly as
            # committed — the Owner-facing settlement branch remains the
            # authority.
            self._completion_handoff.handoff_settled_interrupt(
                workspace_id=entry.workspace_id,
                task_id=entry.task_id,
                task_agent_session_id=entry.task_agent_session_id,
                external_session_id=entry.external_session_id,
            )

    def _route_start_observation(
        self,
        entry: _SessionObservation,
        epoch: _TurnAttestationEpoch | None,
        evidence: ObservedTurnEvidence,
    ) -> None:
        """Corroborate one delivered turn-start observation through the seam.

        Read-only: the seam resolves the durable correlation and records
        contradictions itself, on this integration's neutral attribution
        classification; this boundary delivers the evidence exactly as
        observed and contains every failure. A positively corroborated
        start claims the captured receipt-epoch's ``start_claimed`` (issue
        #341) only when that same epoch still owns the session's
        observation — a successor registration that advanced the epoch in
        the meantime never inherits a predecessor's start claim.
        """
        try:
            corroboration = check_turn_start_identity(
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
            return
        if corroboration is not TurnStartCorroboration.CORROBORATED:
            return
        with self._lock:
            if (
                self._sessions.get(entry.task_agent_session_id) is entry
                and entry.attestation_epoch is epoch
                and epoch is not None
            ):
                # The attributed start opens this epoch's own evidence
                # window (issue #341): any qualifying activity observed
                # before it belonged to a predecessor turn or to the
                # queue-wait window and never attests this turn. The claim
                # resets the activity fact so only activity observed since
                # this start attests; a pulse racing the claim's write is
                # re-corroborated by the turn's next qualifying event.
                epoch.start_claimed = True
                epoch.activity_observed = False

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
            driver = entry.cost_evaluation
            entry.cost_evaluation = None
            del self._sessions[entry.task_agent_session_id]
        if driver is not None:
            driver.dispose()
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
            # The status-only reconciliation positively settled THIS
            # receipt's turn (issue #341): the matching epoch's stale
            # attestation evidence is invalidated — only the epoch bound to
            # the settled receipt id, never a successor's epoch.
            with self._lock:
                epoch = entry.attestation_epoch
                if (
                    self._sessions.get(entry.task_agent_session_id) is entry
                    and epoch is not None
                    and epoch.receipt_id == receipt_id
                ):
                    epoch.start_claimed = False
                    epoch.activity_observed = False

    def _detach_entry(self, entry: _SessionObservation) -> ObservationLease | None:
        """Detach one entry atomically under the registry lock."""
        with self._lock:
            if self._sessions.get(entry.task_agent_session_id) is not entry:
                return None
            lease = entry.observation_lease
            entry.observation_lease = None
            driver = entry.cost_evaluation
            entry.cost_evaluation = None
            del self._sessions[entry.task_agent_session_id]
        if driver is not None:
            driver.dispose()
        return lease
