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
- The only response path outside this middleware is a truly unhandled
  crash (Starlette's outermost server-error middleware), which never
  produces a response through the normal ASGI response path.
"""

from __future__ import annotations

from uuid import uuid4

from opentelemetry.trace import SpanKind
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

REQUEST_ID_HEADER = "x-openorc-request-id"
REQUEST_SPAN_NAME = "openorc.api.request"

_TRACER_SCOPE = "openorc.api.request"


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
        with tracer.start_as_current_span(
            REQUEST_SPAN_NAME,
            kind=SpanKind.SERVER,
            attributes={
                OPERATION_ATTRIBUTE: REQUEST_SPAN_NAME,
                REQUEST_ID_ATTRIBUTE: request_id,
            },
        ):
            token = REQUEST_ID_CONTEXT.set(request_id)
            try:
                await self._app(scope, receive, _send_with_request_id(send, request_id))
            finally:
                REQUEST_ID_CONTEXT.reset(token)


def _send_with_request_id(send: Send, request_id: str) -> Send:
    """Wrap ``send`` so the response carries the request ID header."""
    header = (REQUEST_ID_HEADER.encode("ascii"), request_id.encode("ascii"))

    async def send_with_request_id(message: Message) -> None:
        if message["type"] == "http.response.start":
            headers = list(message.get("headers") or [])
            headers.append(header)
            message["headers"] = headers
        await send(message)

    return send_with_request_id
