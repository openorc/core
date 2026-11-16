"""The Cline detached-turn observation source (detached agent turns,
issues #323/#348).

The Cline integration's implementation of the runtime-neutral observation
source port (:class:`openorc.services.task_detached_turn_observation.DetachedTurnObservationSource`)
and of the neutral evidence-classification seam behind it. It owns the
per-connection Hub attachment lifecycle, the exact-session Hub
subscriptions, the wrapping of each delivered Hub envelope into an opaque
native evidence carrier, the bounded status-only ``run.list`` consultations,
and — through :mod:`openorc.adapters.cline.hub_projection` — the native
interpretation of which evidence proves or counters an admitted turn.
Shared application code consumes only the neutral evidence envelopes and
verdicts; no Hub payload, event name, status token, or identity comparison
crosses this boundary upward.

Failures of the source's own bounded transport mechanics surface as the
neutral :class:`DetachedTurnObservationSourceError` with a safe reason;
unexpected exceptions propagate unchanged.
"""

from __future__ import annotations

import logging
import threading
from collections.abc import Callable, Sequence
from typing import Final, Protocol
from uuid import UUID

from openorc.adapters.cline.bridge_process import ClineBridgeProcessBackend
from openorc.adapters.cline.classification import classify_backend_failure
from openorc.adapters.cline.contract import ClineSdkBackend
from openorc.adapters.cline.errors import ClineSdkBackendError
from openorc.adapters.cline.hub_projection import (
    classify_hub_run_start,
    classify_hub_run_statuses,
    classify_terminal_hub_run,
    hub_evidence_family,
)
from openorc.adapters.cline.values import (
    ClineHubEvent,
    ClineRemoteConfig,
    JsonObject,
    Subscription,
)
from openorc.domain.detached_turns import (
    DetachedTurnObservationSourceError,
    NativeTurnEvidence,
    ObservationLease,
    ObservedTurnEvidence,
    TurnAttribution,
    TurnStatusEvidenceVerdict,
    TurnTerminalClassification,
)

__all__ = [
    "ClineHubTurnObservationSource",
    "OPENORC_CLIENT_IDENTITY",
]

#: The attaching client's explicit identity on the managed Hub path.
OPENORC_CLIENT_IDENTITY: Final[str] = "openorc-core"

logger = logging.getLogger(__name__)

_CONNECT_OPERATION: Final[str] = "connect"
_SUBSCRIBE_OPERATION: Final[str] = "subscribe_hub"


class _AttachmentInputs(Protocol):
    """The structural view of the neutral attachment inputs the host resolves.

    ``auth_token`` is the current attach token: excluded from every
    representation, error, log, and diagnostic, and consumed only by the
    connect call.
    """

    @property
    def endpoint(self) -> str: ...

    @property
    def auth_token(self) -> str: ...


class _ClineHubEventEvidence(NativeTurnEvidence):
    """Opaque carrier of one delivered Hub event envelope.

    The envelope stays inside the Cline integration: shared application code
    routes the neutral evidence envelope and hands this carrier back to this
    integration's classifier, never inspecting or persisting its contents.
    """

    __slots__ = ("event",)

    def __init__(self, event: ClineHubEvent) -> None:
        self.event = event

    def __repr__(self) -> str:
        return "_ClineHubEventEvidence(<withheld>)"


class _ClineStatusRecordsEvidence(NativeTurnEvidence):
    """Opaque carrier of one status-only ``run.list`` record consultation.

    The records stay inside the Cline integration and are interpreted only
    by this integration's classifier; payload contents never enter
    representations, errors, or logs.
    """

    __slots__ = ("records",)

    def __init__(self, records: list[JsonObject]) -> None:
        self.records = records

    def __repr__(self) -> str:
        return f"_ClineStatusRecordsEvidence({len(self.records)} records)"


class _ClineObservationLease(ObservationLease):
    """The source's own handle of one established session observation."""

    __slots__ = ("connection_id", "external_session_id", "subscription")

    def __init__(
        self,
        *,
        connection_id: UUID,
        external_session_id: str,
        subscription: Subscription,
    ) -> None:
        self.connection_id = connection_id
        self.external_session_id = external_session_id
        self.subscription: Subscription | None = subscription


class ClineHubTurnObservationSource:
    """The Cline Hub attachment, subscription, and evidence-classification
    source behind the runtime-neutral observation port.

    Constructed once by the regular-mode composition root; construction
    creates no attachment or subscription. Attachments are established
    lazily per Connection on first registration through the neutral
    attachment inputs the host resolves, and shared by that connection's
    observed sessions.
    """

    def __init__(
        self,
        *,
        backend_factory: Callable[[], ClineSdkBackend] = ClineBridgeProcessBackend,
    ) -> None:
        self._backend_factory = backend_factory
        self._lock = threading.Lock()
        self._disposed = False
        self._connections: dict[UUID, ClineSdkBackend] = {}
        self._observations: dict[tuple[UUID, str], _ClineObservationLease] = {}

    # --- the attachment lifecycle -------------------------------------------------

    def attach_connection(
        self,
        *,
        connection_id: UUID,
        inputs: _AttachmentInputs,
    ) -> None:
        """Dial the Hub attachment for one connection through its inputs.

        ``inputs`` is the neutral attachment-inputs value the host resolved;
        only its endpoint and token cross here, and the token never enters
        any representation.
        """
        backend = self._backend_factory()
        try:
            backend.connect(
                ClineRemoteConfig(
                    endpoint=inputs.endpoint,
                    client_identity=OPENORC_CLIENT_IDENTITY,
                    auth_token=inputs.auth_token,
                )
            )
        except Exception as error:
            if not isinstance(error, ClineSdkBackendError):
                raise
            failure = classify_backend_failure(error, operation=_CONNECT_OPERATION)
            logger.warning(
                "detached-turn observation attachment failed (%s) for connection %s",
                type(failure).__name__,
                connection_id,
            )
            try:
                backend.dispose()
            except Exception as dispose_error:  # noqa: BLE001 - best-effort teardown
                logger.warning(
                    "detached-turn observation attachment disposal failed (%s)",
                    type(dispose_error).__name__,
                )
            raise DetachedTurnObservationSourceError(
                DetachedTurnObservationSourceError.ATTACHMENT_FAILED
            ) from None
        with self._lock:
            if self._disposed:
                doomed = backend
            else:
                self._connections[connection_id] = backend
                doomed = None
        if doomed is not None:
            try:
                doomed.dispose()
            except Exception as error:  # noqa: BLE001 - best-effort teardown
                logger.warning(
                    "detached-turn observation attachment disposal failed (%s)",
                    type(error).__name__,
                )
            raise DetachedTurnObservationSourceError(
                DetachedTurnObservationSourceError.ATTACHMENT_UNAVAILABLE
            )

    def connection_attached(self, connection_id: UUID) -> bool:
        """Whether the connection currently has a live Hub attachment."""
        with self._lock:
            return connection_id in self._connections

    def drop_connection(self, connection_id: UUID) -> None:
        """Discard an unusable Hub attachment and its dead observations.

        The next registration for this connection re-attaches through a
        fresh credential read; the disposed backend owns its dead
        subscriptions, so they are dropped without explicit unsubscribe.
        """
        with self._lock:
            backend = self._connections.pop(connection_id, None)
            for key in [key for key in self._observations if key[0] == connection_id]:
                lease = self._observations.pop(key)
                lease.subscription = None
        if backend is not None:
            try:
                backend.dispose()
            except Exception as error:  # noqa: BLE001 - best-effort teardown
                logger.warning(
                    "detached-turn observation attachment disposal failed (%s)",
                    type(error).__name__,
                )

    def dispose(self) -> None:
        """Release every attachment and observation; idempotent and terminal."""
        with self._lock:
            if self._disposed:
                return
            self._disposed = True
            connections = list(self._connections.values())
            self._connections.clear()
            leases = list(self._observations.values())
            self._observations.clear()
        for lease in leases:
            subscription = lease.subscription
            lease.subscription = None
            if subscription is not None:
                try:
                    subscription.unsubscribe()
                except Exception as error:  # noqa: BLE001 - best-effort teardown
                    logger.warning(
                        "detached-turn observation unsubscribe failed during disposal (%s)",
                        type(error).__name__,
                    )
        for backend in connections:
            try:
                backend.dispose()
            except Exception as error:  # noqa: BLE001 - best-effort teardown
                logger.warning(
                    "detached-turn observation attachment disposal failed during disposal (%s)",
                    type(error).__name__,
                )

    # --- the session observation lifecycle -----------------------------------------

    def observe_session(
        self,
        *,
        connection_id: UUID,
        external_session_id: str,
        deliver: Callable[[ObservedTurnEvidence], None],
    ) -> ObservationLease:
        """Subscribe the exact session's Hub events and return its lease.

        The delivered envelopes are this integration's own neutral evidence
        wrapping: only qualified turn observations with their own session
        identity are delivered, each carrying its opaque native carrier.
        """
        backend = self._connection_backend(connection_id)
        try:
            subscription = backend.subscribe_hub(
                external_session_id,
                self._make_delivery_listener(external_session_id, deliver),
            )
        except Exception as error:
            if not isinstance(error, ClineSdkBackendError):
                raise
            failure = classify_backend_failure(error, operation=_SUBSCRIBE_OPERATION)
            logger.warning(
                "detached-turn observation subscription failed (%s) for session %s",
                type(failure).__name__,
                external_session_id,
            )
            raise DetachedTurnObservationSourceError(
                DetachedTurnObservationSourceError.SUBSCRIPTION_FAILED
            ) from None
        lease = _ClineObservationLease(
            connection_id=connection_id,
            external_session_id=external_session_id,
            subscription=subscription,
        )
        with self._lock:
            if self._disposed:
                lease.subscription = None
                try:
                    subscription.unsubscribe()
                except Exception as error:  # noqa: BLE001 - best-effort teardown
                    logger.warning(
                        "detached-turn observation unsubscribe failed during disposal (%s)",
                        type(error).__name__,
                    )
                raise DetachedTurnObservationSourceError(
                    DetachedTurnObservationSourceError.ATTACHMENT_UNAVAILABLE
                )
            self._observations[(connection_id, external_session_id)] = lease
        return lease

    def release_observation(self, lease: ObservationLease) -> None:
        """Release exactly the observation the lease was returned for."""
        if not isinstance(lease, _ClineObservationLease):
            return  # a foreign or already-released lease; nothing to release
        with self._lock:
            stored = self._observations.pop((lease.connection_id, lease.external_session_id), None)
            if stored is not lease:
                stored = None
        subscription = lease.subscription
        lease.subscription = None
        if subscription is None:
            return  # the attachment drop already owned it
        try:
            subscription.unsubscribe()
        except Exception as error:  # noqa: BLE001 - best-effort teardown
            logger.warning(
                "detached-turn observation unsubscribe failed (%s) for session %s",
                type(error).__name__,
                lease.external_session_id,
            )

    def consult_status_evidence(
        self, *, connection_id: UUID, external_session_id: str
    ) -> NativeTurnEvidence:
        """Read one bounded status-only ``run.list`` consultation verbatim."""
        backend = self._connection_backend(connection_id)
        records = backend.list_runs(external_session_id)
        return _ClineStatusRecordsEvidence(records)

    # --- the neutral evidence classification seam ----------------------------------

    def classify_turn_start(
        self, evidence: NativeTurnEvidence, *, outstanding_turn_reference: str
    ) -> TurnAttribution:
        """Classify one delivered start envelope's attribution."""
        if not isinstance(evidence, _ClineHubEventEvidence):
            raise ValueError("start evidence must be this integration's carrier")
        return classify_hub_run_start(
            evidence.event, outstanding_turn_reference=outstanding_turn_reference
        )

    def classify_turn_terminal(
        self,
        evidence: NativeTurnEvidence,
        *,
        consumed_ordering_markers: Sequence[int],
    ) -> TurnTerminalClassification:
        """Classify one delivered terminal envelope's freshness and outcome."""
        if not isinstance(evidence, _ClineHubEventEvidence):
            raise ValueError("terminal evidence must be this integration's carrier")
        return classify_terminal_hub_run(
            evidence.event, consumed_ordering_markers=consumed_ordering_markers
        )

    def classify_turn_statuses(
        self, evidence: NativeTurnEvidence, *, outstanding_turn_reference: str
    ) -> TurnStatusEvidenceVerdict:
        """Classify one status-only record consultation for the outstanding ref."""
        if not isinstance(evidence, _ClineStatusRecordsEvidence):
            raise ValueError("status evidence must be this integration's carrier")
        return classify_hub_run_statuses(
            evidence.records, outstanding_turn_reference=outstanding_turn_reference
        )

    # --- internals ------------------------------------------------------------------

    def _connection_backend(self, connection_id: UUID) -> ClineSdkBackend:
        with self._lock:
            backend = self._connections.get(connection_id)
        if backend is None:
            raise DetachedTurnObservationSourceError(
                DetachedTurnObservationSourceError.ATTACHMENT_FAILED
            )
        return backend

    def _make_delivery_listener(
        self, external_session_id: str, deliver: Callable[[ObservedTurnEvidence], None]
    ) -> Callable[[ClineHubEvent], None]:
        """Wrap one session's Hub deliveries into neutral evidence envelopes."""

        def listener(event: ClineHubEvent) -> None:
            evidence = self._wrap_event(event)
            if evidence is not None:
                deliver(evidence)

        return listener

    def _wrap_event(self, event: ClineHubEvent) -> ObservedTurnEvidence | None:
        """Wrap one Hub envelope into its neutral evidence envelope, or ``None``
        for every event that is not a qualified turn observation."""
        family = hub_evidence_family(event)
        if family is None or event.session_id is None:
            return None
        return ObservedTurnEvidence(
            external_session_id=event.session_id,
            family=family,
            native_evidence=_ClineHubEventEvidence(event),
        )
