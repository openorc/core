"""Thin GitHub webhook transport for the OpenOrc API (issue #61).

This router owns HTTP concerns only: it obtains the exact raw request body
and the required GitHub delivery headers, delegates the (blocking,
database-bound) intake to the shared application service off the event loop
(the v1 persistence surface is synchronous), and maps the typed intake
outcomes/errors onto HTTP responses. It contains no GitHub reconciliation,
queue orchestration, or workflow logic — dispatch begins at #120.

Response mapping:

- 401 — missing, malformed, or invalid signature (uniform, detail-free);
- 400 — missing/malformed delivery GUID or event name;
- 204 — accepted, duplicate, ignored, or unusable deliveries (all are
  safely acknowledged; the delivery classification is durable state, not a
  client contract);
- 413 — the request body exceeds the bounded webhook size limit;
- 503 — the webhook secret is not configured for this process (the
  deployment fails closed rather than accepting unverified deliveries).
"""

from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Header, Request, Response
from fastapi.concurrency import run_in_threadpool
from starlette.status import HTTP_204_NO_CONTENT, HTTP_400_BAD_REQUEST

from openorc.config import Settings
from openorc.persistence.pool import get_database_pool
from openorc.services.errors import (
    AuthenticationError,
    IntegrationNotConfiguredError,
    InvalidCommandError,
)
from openorc.services.github_webhook_intake import intake_github_webhook

router = APIRouter(tags=["github-webhooks"])

# Bounded intake size: GitHub webhook payloads are capped far below this;
# the limit rejects oversized/unbounded request bodies before any work.
_MAX_WEBHOOK_BODY_BYTES = 25 * 1024 * 1024


@router.post("/api/github/webhooks", status_code=HTTP_204_NO_CONTENT)
async def receive_github_webhook(
    request: Request,
    x_hub_signature_256: Annotated[str | None, Header()] = None,
    x_github_event: Annotated[str | None, Header()] = None,
    x_github_delivery: Annotated[str | None, Header()] = None,
) -> Response:
    """Intake one authenticated GitHub webhook delivery (issue #61)."""
    settings: Settings = request.app.state.settings
    raw_body = await request.body()
    if len(raw_body) > _MAX_WEBHOOK_BODY_BYTES:
        return Response(status_code=413)
    if x_github_event is None or not x_github_event.strip():
        return Response(status_code=HTTP_400_BAD_REQUEST)
    if x_github_delivery is None or not x_github_delivery.strip():
        return Response(status_code=HTTP_400_BAD_REQUEST)
    try:
        # The v1 persistence surface is synchronous: the blocking intake call
        # runs off the event loop.
        await run_in_threadpool(
            intake_github_webhook,
            get_database_pool(settings),
            settings,
            raw_body=raw_body,
            signature_header=x_hub_signature_256,
            event_name=x_github_event,
            delivery_guid=x_github_delivery,
        )
    except AuthenticationError:
        # Uniform, detail-free rejection of missing/malformed/invalid
        # signatures: no probing detail leaves the boundary.
        return Response(status_code=401)
    except InvalidCommandError:
        return Response(status_code=HTTP_400_BAD_REQUEST)
    except IntegrationNotConfiguredError:
        # The deployment fails closed: no webhook is accepted while the
        # verification secret is unconfigured for this process.
        return Response(status_code=503)
    return Response(status_code=HTTP_204_NO_CONTENT)
