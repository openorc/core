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
- Unhandled exceptions keep the framework's server-error semantics: they
  propagate out of this middleware to Starlette's outermost
  ``ServerErrorMiddleware``, which owns the final 500 (registered handler,
  response-started handling, debug-mode precedence) and re-raises for
  server-side logging. This module provides the application-registered
  ``Exception`` handler (:func:`server_error_response`, wired in
  ``create_app``) so the framework's final response carries the same
  server-generated request ID, read from the request scope the correlation
  middleware wrote. The outermost error layer sits outside every user
  middleware — and therefore outside the request span — which is exactly
  why the identifier rides request context rather than the span. No second
  request span is created and no client-supplied ID is accepted. Failure
  telemetry is a safe classification only: the request span receives an
  ERROR status whose description is the exception type name; exception
  messages and stacktraces are never exported.
"""

from __future__ import annotations

from uuid import uuid4

from fastapi import Request
from opentelemetry.trace import SpanKind
from starlette.responses import JSONResponse
from starlette.types import ASGIApp, Message, Receive, Scope, Send

from openorc.observability import (
    OPERATION as OPERATION_ATTRIBUTE,
)
from openorc.observability import (
    REQUEST_ID as REQUEST_ID_ATTRIBUTE,
)
from openorc.observability import (
    REQUEST_ID_CONTEXT,
    application_span,
)

REQUEST_ID_HEADER = "x-openorc-request-id"
REQUEST_ID_SCOPE_KEY = "openorc_request_id"
REQUEST_SPAN_NAME = "openorc.api.request"

_TRACER_SCOPE = "openorc.api.request"
_REQUEST_ID_HEADER_BYTES = REQUEST_ID_HEADER.encode("ascii")


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
        request_id = new_request_id()
        header = (_REQUEST_ID_HEADER_BYTES, request_id.encode("ascii"))
        # The request ID rides request scope so the outermost framework
        # server-error layer — outside every user middleware — can attach it
        # to the final response; the contextvar carries it to correlated
        # application logs inside the request.
        scope.setdefault("state", {})[REQUEST_ID_SCOPE_KEY] = request_id

        async def send_with_request_id(message: Message) -> None:
            if message["type"] == "http.response.start":
                headers = list(message.get("headers") or [])
                headers.append(header)
                message["headers"] = headers
            await send(message)

        with application_span(
            _TRACER_SCOPE,
            REQUEST_SPAN_NAME,
            kind=SpanKind.SERVER,
            attributes={
                OPERATION_ATTRIBUTE: REQUEST_SPAN_NAME,
                REQUEST_ID_ATTRIBUTE: request_id,
            },
        ):
            token = REQUEST_ID_CONTEXT.set(request_id)
            try:
                await self._app(scope, receive, send_with_request_id)
            finally:
                REQUEST_ID_CONTEXT.reset(token)


async def server_error_response(request: Request, exc: Exception) -> JSONResponse:
    """Last-resort 500 handler registered on the application (issue #108).

    Registered as the application's ``Exception`` handler, so Starlette's
    outermost ``ServerErrorMiddleware`` remains the owning error layer: it
    keeps its re-raise-for-logging and response-started semantics, and
    debug-mode traceback responses keep taking precedence per Starlette's
    own contract. This handler only attaches the correlation header — the
    safe opaque request ID written into request scope by the correlation
    middleware — to the framework's final response. It never includes
    exception details: arbitrary exception text may carry secret-bearing
    content and must never reach the wire or telemetry.
    """
    request_id = request.scope.get("state", {}).get(REQUEST_ID_SCOPE_KEY)
    headers = {REQUEST_ID_HEADER: request_id} if isinstance(request_id, str) else None
    return JSONResponse(
        status_code=500,
        content={"detail": "Internal Server Error"},
        headers=headers,
    )
