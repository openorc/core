"""Application-error transport mapping for the OpenOrc API (issue #162).

Maps the typed application-error vocabulary (:mod:`openorc.services.errors`)
onto HTTP responses at the application level: routers stay thin callers and
application services raise typed errors. The mapping is a fixed table over
the demonstrated vocabulary. Error messages are authored safe by the
service-layer obligation ("service errors must carry only safe fields"), so
the response carries the typed message as the ``detail`` value; an unmapped
vocabulary member keeps the generic framework server-error response and
never leaks its text. Every mapped response carries the correlation
middleware's request ID header, like every other API response. The module
also publishes the representative OpenAPI response declarations of the same
fixed mapping for the public /api/v1 operations (issue #369): routers attach
them to document the existing status vocabulary without duplicating it.
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


# --- OpenAPI declarations of the same fixed mapping (issue #369) ----------------
#
# Representative documented responses for the public /api/v1 operations. Each
# documented body is the fixed {"detail": ...} envelope carrying the typed,
# safe, service-authored message; the examples below are representative of the
# mapping, not a per-route exhaustive matrix (the owning route tests prove the
# actual outcomes).

DOCUMENTED_AUTH_401 = {
    "description": (
        "Authentication failed: the Supabase session JWT bearer credential is "
        "missing, malformed, or invalid (uniform, detail-free)."
    ),
    "content": {
        "application/json": {
            "example": {
                "detail": "authentication failed: the access token is missing or malformed",
            }
        }
    },
}

DOCUMENTED_NOT_FOUND_404 = {
    "description": (
        "The addressed subject does not exist for the authenticated Profile: "
        "missing, foreign-Workspace, and non-owner subjects are uniformly "
        "this outcome, never a probing oracle."
    ),
    "content": {
        "application/json": {
            "example": {"detail": "the addressed subject does not exist in this workspace"}
        }
    },
}

DOCUMENTED_CONFLICT_409 = {
    "description": (
        "The operation conflicts with current durable state, or addresses a "
        "subject that has since moved on (a stale operation). A guarded "
        "Owner mutation fails closed this way while an account-deletion "
        "attempt is unresolved."
    ),
    "content": {
        "application/json": {
            "example": {"detail": "the operation conflicts with current durable state"}
        }
    },
}

DOCUMENTED_INVALID_COMMAND_422 = {
    "description": (
        "The command is invalid: the submitted value violates a semantic "
        "rule the application service owns (wire-shape failures are the "
        "framework's request-validation errors, also 422)."
    ),
    "content": {
        "application/json": {
            "example": {"detail": "the submitted command is not a valid value for this operation"}
        }
    },
}

DOCUMENTED_UNAVAILABLE_503 = {
    "description": (
        "A required integration is not configured for this process (fail "
        "closed), or the outcome of an external operation is unknown or was "
        "refused before delivery — never reported as invalid caller "
        "credentials."
    ),
    "content": {
        "application/json": {
            "example": {
                "detail": (
                    "the outcome of the external operation is unknown; the "
                    "durable projection remains the caller-visible truth"
                )
            }
        }
    },
}
