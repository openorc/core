"""The queue dispatch worker's remote detached-turn observation registrar
(issue #346).

The worker-side half of the private authenticated registration transport: the
queue dispatch worker's pre-enqueue gate for runtime-neutral detached
admission. It implements the exact duck-typed registration seam
(``register_session`` / ``register_establishment_session``) the Level 1
dispatch services already consume, but instead of hosting an observation it
performs the narrow internal handshake against the serving API/control-plane
process that hosts the one long-lived detached-turn observation host — and it
returns only when that host has positively acknowledged an installed or
renewed exact-session observation.

Boundary rules:

- The worker NEVER treats a sent request as registration success. Every
  non-acknowledged outcome — a typed refusal, an unexpected response, a
  transport failure, a timeout — raises the same typed
  :class:`ObservationRegistrationRefusedError` the local host raises, so the
  dispatch services' existing pre-effect refusal machinery routes it and the
  admission (the native Hub ``run.enqueue``) never happens. Registration is a
  pre-effect gate: there is no effect to be uncertain about, and no retry.
- The transport is deployment-internal only: the configured base URL plus the
  deployment-level shared secret (``OPENORC_QUEUE_OBSERVER_REGISTRATION_SECRET``)
  authenticate the handshake. The secret is never logged, never echoed in an
  error, and never stored anywhere but this process's configuration.
- The proxy is runtime-neutral: it carries only the existing exact
  Workspace/Connection/Task/session identities and never imports a concrete
  runtime integration. Only the injected observation source behind the host
  uses Hub APIs.
"""

from __future__ import annotations

import json
import logging
import urllib.error
import urllib.request
from collections.abc import Callable
from typing import Final
from uuid import UUID

from openorc.config import QUEUE_OBSERVER_REGISTRATION_PATH
from openorc.services.errors import ObservationRegistrationRefusedError
from openorc.services.task_detached_turn_observation import (
    ObservationRegistrationRefusalReason,
)

__all__ = ["QueueDetachedTurnObservationRegistrar", "RegistrationTransport"]

logger = logging.getLogger(__name__)

# The bounded handshake deadline: a registration must resolve quickly or the
# worker refuses and does not enqueue. There is no retry and no queue-side
# wait — the host either attests the observation or the dispatch fails closed.
_DEFAULT_TIMEOUT_SECONDS: Final[float] = 10.0

# The bounded response size: the handshake payload is tiny; a larger response
# is malformed by construction and is refused without being fully consumed.
_MAX_RESPONSE_BYTES: Final[int] = 64 * 1024

#: The injectable HTTP transport seam (tests substitute a fake): performs one
#: bounded request and returns the raw ``(status, body)`` exchange, raising
#: for transport-level failures. The default implementation is the standard
#: library client every other OpenOrc outbound integration uses.
RegistrationTransport = Callable[[urllib.request.Request, float], tuple[int, bytes]]

_REFUSAL_MESSAGE: Final[str] = (
    "positive detached-turn observation could not be attested through the "
    "queue registration transport"
)

_KNOWN_REFUSAL_REASONS: Final[frozenset[str]] = frozenset(
    member.value for member in ObservationRegistrationRefusalReason
)


def _default_transport(request: urllib.request.Request, timeout: float) -> tuple[int, bytes]:
    """Perform one bounded exchange on the standard-library client.

    A completed HTTP exchange — any status — returns its status and bounded
    body; transport-level failures (connection, timeout, TLS, DNS) raise.
    HTTPError responses are completed exchanges and are unwrapped so the
    registrar maps every status uniformly.
    """
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            return response.status, response.read(_MAX_RESPONSE_BYTES + 1)
    except urllib.error.HTTPError as error:
        with error:
            return error.code, error.read(_MAX_RESPONSE_BYTES + 1)


def _decode_json_object(body: bytes) -> dict[str, object] | None:
    """Decode a bounded response body as a JSON object, honestly unknown otherwise."""
    if len(body) > _MAX_RESPONSE_BYTES:
        return None
    try:
        document = json.loads(body.decode("utf-8"))
    except (UnicodeDecodeError, ValueError):
        return None
    return document if isinstance(document, dict) else None


def _closed_refusal_reason(document: dict[str, object] | None) -> str | None:
    """Extract a refusal reason only when it is one of the closed safe tokens."""
    if document is None:
        return None
    raw_reason = document.get("refusal_reason")
    if isinstance(raw_reason, str) and raw_reason in _KNOWN_REFUSAL_REASONS:
        return raw_reason
    return None


class QueueDetachedTurnObservationRegistrar:
    """The queue-mode remote registration proxy over the private handshake.

    Built by the queue-mode operation-dependency composition from the
    deployment's registration base URL and shared secret. Its registration
    methods are the exact seam the dispatch services gate detached admission
    on: they return only on a positively acknowledged live observation
    registration in the hosting API/control-plane process and raise the typed
    refusal for every other outcome.
    """

    def __init__(
        self,
        *,
        registration_url: str,
        registration_secret: str,
        timeout_seconds: float = _DEFAULT_TIMEOUT_SECONDS,
        transport: RegistrationTransport | None = None,
    ) -> None:
        base = registration_url.strip().rstrip("/")
        if not base:
            raise ValueError("the queue observer registration base URL must be a non-empty URL")
        self._registration_url = f"{base}{QUEUE_OBSERVER_REGISTRATION_PATH}"
        self._registration_secret = registration_secret
        self._timeout_seconds = timeout_seconds
        self._transport = transport if transport is not None else _default_transport

    def register_session(
        self,
        *,
        workspace_id: UUID,
        task_id: UUID,
        task_agent_session_id: UUID,
        connection_id: UUID,
        external_session_id: str,
    ) -> None:
        """Attest the observation of one established exact session (issue #346)."""
        self._register(
            "established_session",
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
        """Attest the observation of one initializing exact session (issue #346)."""
        self._register(
            "establishing_session",
            workspace_id=workspace_id,
            task_id=task_id,
            task_agent_session_id=task_agent_session_id,
            connection_id=connection_id,
            external_session_id=external_session_id,
        )

    def _register(
        self,
        kind: str,
        *,
        workspace_id: UUID,
        task_id: UUID,
        task_agent_session_id: UUID,
        connection_id: UUID,
        external_session_id: str,
    ) -> None:
        payload = {
            "kind": kind,
            "workspace_id": str(workspace_id),
            "task_id": str(task_id),
            "task_agent_session_id": str(task_agent_session_id),
            "connection_id": str(connection_id),
            "external_session_id": external_session_id,
        }
        request = urllib.request.Request(
            self._registration_url,
            data=json.dumps(payload).encode("utf-8"),
            headers={
                "Content-Type": "application/json",
                # The deployment-level shared credential; never logged and
                # never reflected in any error message.
                "Authorization": f"Bearer {self._registration_secret}",
            },
            method="POST",
        )
        try:
            status, body = self._transport(request, self._timeout_seconds)
        except Exception as error:  # noqa: BLE001 - transport failures are typed refusals
            logger.warning(
                "the queue-mode detached-turn observation registration transport "
                "failed (%s); the dispatch cannot attest a live observer "
                "registration and must not enqueue",
                type(error).__name__,
            )
            raise self._refusal(ObservationRegistrationRefusalReason.OBSERVER_UNAVAILABLE) from None
        document = _decode_json_object(body)
        if status == 200:
            if document is not None and document.get("acknowledged") is True:
                return  # the host positively attested the observation
            logger.warning(
                "the queue-mode detached-turn observation registration returned an "
                "unusable acknowledgement; the dispatch must not enqueue"
            )
            raise self._refusal(
                ObservationRegistrationRefusalReason.OBSERVATION_REGISTRATION_FAILED
            )
        reason = _closed_refusal_reason(document)
        if status == 409 and reason is not None:
            # The hosting observer's own typed refusal: the safe machine token
            # is carried through unchanged so the dispatch's pre-effect
            # evidence keeps the same closed vocabulary as a local host.
            raise self._refusal(ObservationRegistrationRefusalReason(reason))
        logger.warning(
            "the queue-mode detached-turn observation registration was not "
            "acknowledged (status %s); the dispatch cannot attest a live "
            "observer registration and must not enqueue",
            status,
        )
        raise self._refusal(ObservationRegistrationRefusalReason.OBSERVER_UNAVAILABLE)

    def _refusal(
        self, reason: ObservationRegistrationRefusalReason
    ) -> ObservationRegistrationRefusedError:
        return ObservationRegistrationRefusedError(_REFUSAL_MESSAGE, reason=reason.value)
