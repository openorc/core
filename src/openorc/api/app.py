"""FastAPI application factory for the OpenOrc API transport surface."""

from __future__ import annotations

from collections.abc import AsyncIterator, Callable
from contextlib import AbstractAsyncContextManager, asynccontextmanager

from fastapi import FastAPI

from openorc import __version__
from openorc.api.request_correlation import (
    RequestCorrelationMiddleware,
    server_error_response,
)
from openorc.api.routers.github_webhooks import router as github_webhooks_router
from openorc.api.routers.health import router as health_router
from openorc.config import Settings
from openorc.observability import ObservabilitySurface, initialize_observability

API_TITLE = "OpenOrc API"


def _observability_lifespan(
    settings: Settings,
) -> Callable[[FastAPI], AbstractAsyncContextManager[None]]:
    """Build the serving-process observability lifespan.

    Initialization happens inside the process that actually serves the
    application: uvicorn runs the lifespan once per serving process,
    including each reload subprocess, and never in a launcher parent.
    Teardown deliberately performs no process-global shutdown; the terminal
    flush is owned by the process-exit hook registered by initialization
    (issue #108).
    """

    @asynccontextmanager
    async def lifespan(_app: FastAPI) -> AsyncIterator[None]:
        initialize_observability(settings, ObservabilitySurface.API)
        yield

    return lifespan


def create_app(settings: Settings | None = None) -> FastAPI:
    """Build the OpenOrc API application.

    Routers are thin transports over shared application code. The
    health/readiness endpoint is deterministic and must not probe external
    systems (Supabase, Valkey, GitHub, Cline).

    Construction stays telemetry-free: no OpenTelemetry provider, exporter,
    processor, or handler is installed until the lifespan runs in a serving
    process, so repeated factory/test construction can never duplicate
    telemetry state (issue #108).
    """
    resolved = settings if settings is not None else Settings.from_env()
    app = FastAPI(
        title=API_TITLE,
        version=__version__,
        lifespan=_observability_lifespan(resolved),
        # The correlation module owns the application's registered Exception
        # handler so the framework's outermost server-error layer keeps its
        # semantics while the final response carries the request ID (issue
        # #108).
        exception_handlers={Exception: server_error_response},
    )
    app.add_middleware(RequestCorrelationMiddleware)
    app.include_router(health_router)
    app.include_router(github_webhooks_router)
    app.state.settings = resolved
    return app


app = create_app()
