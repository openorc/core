"""Application-error transport mapping for the OpenOrc API (issue #162).

Maps the typed application-error vocabulary (:mod:`openorc.services.errors`)
onto HTTP responses at the application level: routers stay thin callers and
application services raise typed errors. The mapping is a fixed table over
the demonstrated vocabulary. Error messages are authored safe by the
service-layer obligation ("service errors must carry only safe fields"), so
the response carries the typed message as the ``detail`` value; an unmapped
vocabulary member keeps the generic framework server-error response and
never leaks its text. Every mapped response carries the correlation
middleware's request ID header, like every other API response.
"""

from __future__ import annotations

from fastapi import Request
from starlette.responses import JSONResponse

from openorc.api.request_correlation import (
    REQUEST_ID_HEADER,
    REQUEST_ID_SCOPE_KEY,
    server_error_response,
)
from openorc.services.errors import (
    ApplicationError,
    AuthenticationError,
    AuthorizationError,
    ConflictError,
    ExternalOperationFailedError,
    ExternalOperationUncertainError,
    IntegrationNotConfiguredError,
    InvalidCommandError,
    NotFoundError,
    StaleOperationError,
)

__all__ = ["application_error_response"]

# Subclass → status mapping, checked in order: a subclass entry precedes its
# bases so the most specific mapping wins.
_STATUS_RULES: tuple[tuple[type[ApplicationError], int], ...] = (
    (AuthenticationError, 401),
    (AuthorizationError, 403),
    (NotFoundError, 404),
    (InvalidCommandError, 422),
    (IntegrationNotConfiguredError, 503),
    (ExternalOperationUncertainError, 503),
    (ExternalOperationFailedError, 503),
    (StaleOperationError, 409),
    (ConflictError, 409),
)


async def application_error_response(request: Request, error: ApplicationError) -> JSONResponse:
    """Map one typed application error onto its HTTP response."""
    for error_type, status in _STATUS_RULES:
        if isinstance(error, error_type):
            return _json_response(request, status, str(error))
    # An unmapped vocabulary member is an application gap the framework's
    # server-error layer owns; its text is never leaked.
    return await server_error_response(request, error)


def _json_response(request: Request, status: int, detail: str) -> JSONResponse:
    request_id = request.scope.get("state", {}).get(REQUEST_ID_SCOPE_KEY)
    headers = {REQUEST_ID_HEADER: request_id} if isinstance(request_id, str) else None
    return JSONResponse(status_code=status, content={"detail": detail}, headers=headers)
