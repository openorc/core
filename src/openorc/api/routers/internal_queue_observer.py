"""The private queue-mode detached-turn observation registration transport
(issue #346).

The narrow internal, authenticated, deployment-only handshake a queue
dispatch worker presents to the serving API/control-plane process BEFORE it
may admit a detached turn: the worker proves the exact
Workspace/Connection/Task/session identities and the hosting process's
observation host installs or renews the exact-session observation, returning
a positive acknowledgement or a typed refusal with a safe reason token. The
worker never treats a sent request as registration success, and it must not
enqueue when it cannot attest a live observer registration — that judgment
lives entirely in the worker-side registrar proxy (issue #346) and the
dispatch services' existing pre-effect refusal machinery.

Boundary rules:

- The route is private/authenticated and nonpublic: it authenticates the
  deployment-level shared secret (``OPENORC_QUEUE_OBSERVER_REGISTRATION_SECRET``)
  with a constant-time comparison and rejects every authentication failure
  uniformly and detail-free (like the webhook secret boundary). The
  deployment's network isolation owns the nonpublic expectation; the route is
  never an application command, never an Owner-authority surface, and never
  routed through Supabase Auth.
- The route owns HTTP mechanics only: it strictly bounds and validates the
  registration shape, delegates the blocking registration — process-local
  pool acquisition and the trusted credential resolution included — off the
  event loop (the v1 persistence surface is synchronous), and maps only the
  typed registration outcomes onto HTTP. It contains no workflow logic: the
  observation host and the shared settlement seam own every semantic.
- An unavailable observation host (a regular-mode process, a host not yet
  started, or a host disposed during shutdown) refuses with the typed
  ``observer_unavailable`` reason rather than accepting an unobserved
  registration: the handshake fails closed.

Response mapping:

- 401 — missing, malformed, or invalid deployment credential (uniform,
  detail-free);
- 503 — the deployment registration secret is not configured for this
  process (the deployment fails closed rather than accepting unverified
  registrations);
- 422 — a malformed registration shape;
- 409 — a typed registration refusal carrying its safe reason token;
- 200 — the positive acknowledgement (the observation is installed or
  renewed).
"""

from __future__ import annotations

import hmac
from typing import Annotated, Literal

from fastapi import APIRouter, Header, Request
from fastapi.concurrency import run_in_threadpool
from pydantic import UUID4, BaseModel, ConfigDict
from starlette.responses import JSONResponse

from openorc.config import QUEUE_OBSERVER_REGISTRATION_PATH, Settings
from openorc.services.errors import (
    AuthenticationError,
    IntegrationNotConfiguredError,
    ObservationRegistrationRefusedError,
)
from openorc.services.task_detached_turn_observation import (
    DetachedTurnObservationRegistrar,
)

router = APIRouter(tags=["internal-queue-observer"])

_REGISTRATION_PATH = QUEUE_OBSERVER_REGISTRATION_PATH

_UNAUTHENTICATED_MESSAGE = (
    "authentication failed: the deployment registration credential is missing or invalid"
)


class DetachedTurnObservationRegistrationRequest(BaseModel):
    """The strictly-shaped registration handshake payload (issue #346).

    Only the exact existing identities the registration gates prove — no
    prompt, receipt, credential, or runtime-native material ever crosses
    this transport. Unknown fields are rejected so the shape stays closed.
    """

    model_config = ConfigDict(extra="forbid")

    kind: Literal["established_session", "establishing_session"]
    workspace_id: UUID4
    task_id: UUID4
    task_agent_session_id: UUID4
    connection_id: UUID4
    external_session_id: str


def _authenticate_deployment_registration(settings: Settings, authorization: str | None) -> None:
    """Authenticate the deployment-level shared credential, fail closed.

    A missing, malformed, blank, or mismatching bearer credential — and an
    unconfigured deployment secret — never accepts a registration. The
    unconfigured case is a loud deployment error (503) exactly like the
    webhook secret boundary; every supplied-but-invalid credential is the
    same uniform, detail-free 401. The comparison is constant-time and no
    credential material is ever reflected back.
    """
    secret = settings.queue_observer_registration_secret
    if secret is None:
        raise IntegrationNotConfiguredError(
            "the queue-mode observation registration secret is not configured for this process"
        )
    if authorization is None or not authorization.startswith("Bearer "):
        raise AuthenticationError(_UNAUTHENTICATED_MESSAGE)
    token = authorization[len("Bearer ") :].strip()
    if not token or not hmac.compare_digest(token, secret):
        raise AuthenticationError(_UNAUTHENTICATED_MESSAGE)


def _register_observation(
    registrar: DetachedTurnObservationRegistrar,
    registration: DetachedTurnObservationRegistrationRequest,
) -> None:
    """Run the blocking registration through the exact duck-typed gate.

    Executed in the worker thread: the host's registration resolves the
    trusted Connection/credential composition and may dial the runtime —
    blocking work that never runs on the event loop. The gate's typed
    refusals propagate unchanged for the route's own mapping.
    """
    kwargs = {
        "workspace_id": registration.workspace_id,
        "task_id": registration.task_id,
        "task_agent_session_id": registration.task_agent_session_id,
        "connection_id": registration.connection_id,
        "external_session_id": registration.external_session_id,
    }
    if registration.kind == "established_session":
        registrar.register_session(**kwargs)
    else:
        registrar.register_establishment_session(**kwargs)


@router.post(_REGISTRATION_PATH)
async def register_detached_turn_observation(
    request: Request,
    registration: DetachedTurnObservationRegistrationRequest,
    authorization: Annotated[str | None, Header()] = None,
) -> JSONResponse:
    """Serve one registration handshake against the hosting observer (issue #346).

    The lifespan owns exactly one observation host per queue-mode serving
    process; the route resolves it from application state and refuses
    positively when it cannot be served (a regular-mode process or a host
    disposed during teardown) so a worker can never mistake an unavailable
    host for an attested registration.
    """
    settings: Settings = request.app.state.settings
    _authenticate_deployment_registration(settings, authorization)
    observer = getattr(request.app.state, "detached_turn_observer", None)
    if observer is None:
        return JSONResponse(
            status_code=409,
            content={
                "acknowledged": False,
                "refusal_reason": "observer_unavailable",
            },
        )
    try:
        await run_in_threadpool(_register_observation, observer, registration)
    except ObservationRegistrationRefusedError as error:
        # The typed refusal is a positively proven pre-effect condition; its
        # reason is a safe machine token the worker's proxy re-raises.
        return JSONResponse(
            status_code=409,
            content={"acknowledged": False, "refusal_reason": error.reason},
        )
    return JSONResponse(status_code=200, content={"acknowledged": True})
