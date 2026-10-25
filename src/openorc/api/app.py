"""FastAPI application factory for the OpenOrc API transport surface."""

from __future__ import annotations

from collections.abc import AsyncIterator, Callable
from contextlib import AbstractAsyncContextManager, asynccontextmanager

from fastapi import FastAPI

from openorc import __version__
from openorc.api.dependencies.application_errors import application_error_response
from openorc.api.request_correlation import (
    RequestCorrelationMiddleware,
    server_error_response,
)
from openorc.api.routers.github_webhooks import router as github_webhooks_router
from openorc.api.routers.health import router as health_router
from openorc.api.routers.role_bindings import router as role_bindings_router
from openorc.config import ExecutionMode, Settings
from openorc.observability import ObservabilitySurface, initialize_observability
from openorc.services.errors import ApplicationError
from openorc.workers.invocation import (
    build_default_operation_dependencies,
    build_operation_submitter,
    build_production_handlers,
)
from openorc.workers.local_executor import BoundedLocalExecutor

API_TITLE = "OpenOrc API"


def _serving_lifespan(
    settings: Settings,
) -> Callable[[FastAPI], AbstractAsyncContextManager[None]]:
    """Build the serving-process lifespan: observability and executor ownership.

    Initialization happens inside the process that actually serves the
    application: uvicorn runs the lifespan once per serving process,
    including each reload subprocess, and never in a launcher parent.

    In regular execution mode the lifespan starts and owns exactly one
    bounded local executor per serving process (issue #86); queue mode
    starts no executor and never touches the queue backend. No executor is
    created by module import, ``create_app()``, or a request. Observability
    teardown deliberately performs no process-global shutdown; the terminal
    flush is owned by the process-exit hook registered by initialization
    (issue #108). Executor teardown stops new admission and drains within
    the bounded default timeout, asserting nothing about any outstanding
    operation's outcome.
    """

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        initialize_observability(settings, ObservabilitySurface.API)
        executor: BoundedLocalExecutor | None = None
        if settings.execution_mode is ExecutionMode.REGULAR:
            executor = BoundedLocalExecutor(
                workers=settings.local_executor_workers,
                pending=settings.local_executor_pending,
                handlers=build_production_handlers(),
                dependencies=build_default_operation_dependencies(settings),
            )
        app.state.operation_executor = executor
        app.state.operation_submitter = build_operation_submitter(settings, local_executor=executor)
        try:
            yield
        finally:
            if executor is not None:
                executor.shutdown()

    return lifespan


def create_app(settings: Settings | None = None) -> FastAPI:
    """Build the OpenOrc API application.

    Routers are thin transports over shared application code. The
    health/readiness endpoint is deterministic and must not probe external
    systems (Supabase, Valkey, GitHub, Cline).

    Construction stays telemetry-free and executor-free: no OpenTelemetry
    provider, exporter, processor, or handler is installed and no bounded
    local executor is created until the lifespan runs in a serving process,
    so repeated factory/test construction can never duplicate telemetry
    state (issue #108) or executor ownership (issue #86).
    """
    resolved = settings if settings is not None else Settings.from_env()
    app = FastAPI(
        title=API_TITLE,
        version=__version__,
        lifespan=_serving_lifespan(resolved),
        # The correlation module owns the application's registered Exception
        # handler so the framework's outermost server-error layer keeps its
        # semantics while the final response carries the request ID (issue
        # #108).
        exception_handlers={
            Exception: server_error_response,
            ApplicationError: application_error_response,
        },
    )
    app.add_middleware(RequestCorrelationMiddleware)
    app.include_router(health_router)
    app.include_router(github_webhooks_router)
    app.include_router(role_bindings_router)
    app.state.settings = resolved
    return app


app = create_app()
