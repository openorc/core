"""API request correlation transport (issue #108).

Every OpenOrc API request receives a safe opaque server-side ``request_id``
and exactly one canonical request-entry span, both owned by this middleware.

- OpenTelemetry creates spans only through explicit instrumentation, so the
  request-entry span is created here: every downstream middleware, route,
  and service executes inside its active context, and correlated
  application logs carry its trace/span context. Contrib framework
  instrumentation is deliberately not adopted in v1 (dependency policy); if
  it is ever adopted, it must be reconciled so exactly one request-entry
  span exists.
- The request ID is attached to the request span, enriched onto correlated
  log records through the observability boundary's request-context filter,
  and returned to the caller in a response header. It is observational
  only: never workflow authority, authentication/authorization context, an
  idempotency key, or a substitute for the OpenTelemetry trace ID, and it
  is never threaded through domain/service signatures as business data.
- The last-resort error path belongs to this middleware too: when an
  exception escapes the application, Starlette's outermost server-error
  layer would otherwise produce the final 500 outside this middleware
  (without the correlation header), so the middleware records the failure
  on the same request span and emits the final 500 itself with the same
  server-generated request ID. No second request span is created and no
  client-supplied ID is accepted. Once a response has started streaming,
  the exception re-raises for transport teardown.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING
from uuid import uuid4

from opentelemetry.trace import SpanKind, Status, StatusCode
from starlette.types import ASGIApp, Message, Receive, Scope, Send

from openorc.observability import (
    OPERATION as OPERATION_ATTRIBUTE,
)
from openorc.observability import (
    REQUEST_ID as REQUEST_ID_ATTRIBUTE,
)
from openorc.observability import (
    REQUEST_ID_CONTEXT,
    application_tracer,
)

if TYPE_CHECKING:
    from opentelemetry.trace import Span

REQUEST_ID_HEADER = "x-openorc-request-id"
REQUEST_SPAN_NAME = "openorc.api.request"

_TRACER_SCOPE = "openorc.api.request"
_REQUEST_ID_HEADER_BYTES = REQUEST_ID_HEADER.encode("ascii")
_LAST_RESORT_BODY = b'{"detail":"Internal Server Error"}'

logger = logging.getLogger(__name__)


def new_request_id() -> str:
    """Return a fresh opaque server-side request identifier."""
    return uuid4().hex


class RequestCorrelationMiddleware:
    """Pure ASGI middleware owning the canonical API request-entry span."""

    def __init__(self, app: ASGIApp) -> None:
        self._app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self._app(scope, receive, send)
            return
        tracer = application_tracer(_TRACER_SCOPE)
        request_id = new_request_id()
        header = (_REQUEST_ID_HEADER_BYTES, request_id.encode("ascii"))
        response_started = False

        async def send_with_request_id(message: Message) -> None:
            nonlocal response_started
            if message["type"] == "http.response.start":
                response_started = True
                headers = list(message.get("headers") or [])
                headers.append(header)
                message["headers"] = headers
            await send(message)

        with tracer.start_as_current_span(
            REQUEST_SPAN_NAME,
            kind=SpanKind.SERVER,
            attributes={
                OPERATION_ATTRIBUTE: REQUEST_SPAN_NAME,
                REQUEST_ID_ATTRIBUTE: request_id,
            },
        ) as span:
            token = REQUEST_ID_CONTEXT.set(request_id)
            try:
                await self._app(scope, receive, send_with_request_id)
            except Exception as exc:  # noqa: BLE001 - last-resort transport boundary
                if response_started:
                    # A replacement response cannot be sent once streaming
                    # has begun; re-raise for transport teardown.
                    raise
                _record_last_resort_failure(span, exc)
                logger.exception("Unhandled exception serving the API request")
                await _send_last_resort_error(send, header)
            finally:
                REQUEST_ID_CONTEXT.reset(token)


def _record_last_resort_failure(span: Span, exc: Exception) -> None:
    """Record an escaped application exception on the request-entry span.

    The exception is swallowed by the caller to prevent a second, headerless
    server-error response, so the context-manager exit never sees it and the
    failure is recorded explicitly here.
    """
    span.record_exception(exc)
    span.set_status(Status(StatusCode.ERROR, type(exc).__name__))


async def _send_last_resort_error(send: Send, header: tuple[bytes, bytes]) -> None:
    """Emit the final 500 response with the same server-generated request ID."""
    await send(
        {
            "type": "http.response.start",
            "status": 500,
            "headers": [
                (b"content-type", b"application/json"),
                header,
                (b"content-length", str(len(_LAST_RESORT_BODY)).encode("ascii")),
            ],
        }
    )
    await send({"type": "http.response.body", "body": _LAST_RESORT_BODY})
