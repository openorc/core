"""Thin GitHub webhook transport for the OpenOrc API (issue #61).

This router owns HTTP concerns only: it bounds the request body WHILE
reading it (an oversized request is rejected without ever being fully
buffered), obtains the exact raw request bytes and the required GitHub
delivery headers, delegates the blocking, database-bound intake — pool
acquisition included — to the shared application service off the event loop
(the v1 persistence surface is synchronous), and maps the typed intake
outcomes/errors onto HTTP responses. It contains no GitHub reconciliation,
queue orchestration, or workflow logic — dispatch begins at #120.

Response mapping:

- 401 — missing, malformed, or invalid signature (uniform, detail-free);
- 400 — missing/malformed delivery GUID or event name;
- 204 — accepted, duplicate, ignored, or unusable deliveries (all are
  safely acknowledged; the delivery classification is durable state, not a
  client contract);
- 413 — the request body exceeds the bounded webhook size limit (enforced
  while reading; a matching Content-Length is only an additional fast
  rejection, never the sole bound);
- 503 — the webhook secret is not configured for this process (the
  deployment fails closed rather than accepting unverified deliveries).
"""

from __future__ import annotations

from collections.abc import AsyncIterator
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
from openorc.services.github_webhook_intake import GitHubWebhookIntake, intake_github_webhook

router = APIRouter(tags=["github-webhooks"])

# Bounded intake size: GitHub webhook payloads are capped far below this. The
# bound is enforced WHILE reading so an oversized/unbounded request body is
# never fully buffered before rejection.
_MAX_WEBHOOK_BODY_BYTES = 25 * 1024 * 1024


async def _read_bounded_body(body_stream: AsyncIterator[bytes]) -> bytes | None:
    """Read the exact raw request bytes, bounded while reading.

    Accumulates chunks until the configured limit is crossed and then stops:
    an oversized request is rejected without ever buffering beyond the limit
    (plus one in-flight chunk). Chunk concatenation is lossless, so the
    returned bytes are exactly what signature verification must see. Returns
    ``None`` when the bound was exceeded.
    """
    chunks: list[bytes] = []
    total = 0
    async for chunk in body_stream:
        total += len(chunk)
        if total > _MAX_WEBHOOK_BODY_BYTES:
            return None
        chunks.append(chunk)
    return b"".join(chunks)


def _declared_length_exceeds_limit(declared_length: str | None) -> bool:
    """Fast Content-Length precheck — never the sole bound.

    A clean integer declaration above the limit rejects immediately without
    reading; a missing, malformed, or lying declaration is still bounded by
    the streaming read.
    """
    if declared_length is None:
        return False
    try:
        length = int(declared_length)
    except ValueError:
        return False
    return length > _MAX_WEBHOOK_BODY_BYTES


def _intake_delivery(
    settings: Settings,
    raw_body: bytes,
    signature_header: str | None,
    event_name: str,
    delivery_guid: str,
) -> GitHubWebhookIntake:
    """Synchronous intake entry executed in the worker thread.

    The process-local pool is acquired HERE, off the event loop: first-use
    pool construction can block on the database and must never run inside an
    async request handler.
    """
    return intake_github_webhook(
        get_database_pool(settings),
        settings,
        raw_body=raw_body,
        signature_header=signature_header,
        event_name=event_name,
        delivery_guid=delivery_guid,
    )


@router.post("/api/github/webhooks", status_code=HTTP_204_NO_CONTENT)
async def receive_github_webhook(
    request: Request,
    x_hub_signature_256: Annotated[str | None, Header()] = None,
    x_github_event: Annotated[str | None, Header()] = None,
    x_github_delivery: Annotated[str | None, Header()] = None,
) -> Response:
    """Intake one authenticated GitHub webhook delivery (issue #61)."""
    settings: Settings = request.app.state.settings
    if _declared_length_exceeds_limit(request.headers.get("content-length")):
        return Response(status_code=413)
    if x_github_event is None or not x_github_event.strip():
        return Response(status_code=HTTP_400_BAD_REQUEST)
    if x_github_delivery is None or not x_github_delivery.strip():
        return Response(status_code=HTTP_400_BAD_REQUEST)
    raw_body = await _read_bounded_body(request.stream())
    if raw_body is None:
        return Response(status_code=413)
    try:
        # The blocking, database-bound intake — process-local pool
        # acquisition included — runs off the event loop in one worker step.
        await run_in_threadpool(
            _intake_delivery,
            settings,
            raw_body,
            x_hub_signature_256,
            x_github_event,
            x_github_delivery,
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
