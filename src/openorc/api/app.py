"""FastAPI application factory for the OpenOrc API transport surface."""

from __future__ import annotations

from collections.abc import AsyncIterator, Callable
from contextlib import AbstractAsyncContextManager, asynccontextmanager

from fastapi import FastAPI

from openorc import __version__
from openorc.adapters.cline import (
    ClineBridgeProcessBackend,
    resolve_connection_control_endpoint,
)
from openorc.adapters.cline.hub_observation import ClineHubTurnObservationSource
from openorc.api.dependencies.application_errors import application_error_response
from openorc.api.request_correlation import (
    RequestCorrelationMiddleware,
    server_error_response,
)
from openorc.api.routers.github_webhooks import router as github_webhooks_router
from openorc.api.routers.health import router as health_router
from openorc.api.routers.role_bindings import router as role_bindings_router
from openorc.api.routers.tasks import router as tasks_router
from openorc.config import ExecutionMode, Settings
from openorc.observability import ObservabilitySurface, initialize_observability
from openorc.services.connection_credentials import resolve_connection_runtime_credential
from openorc.services.errors import ApplicationError
from openorc.services.task_detached_turn_observation import DetachedTurnObservationHost
from openorc.workers.invocation import (
    build_default_operation_dependencies,
    build_operation_submitter,
    build_production_handlers,
)
from openorc.workers.local_executor import BoundedLocalExecutor
from openorc.workers.task_workflow import build_detached_completion_handoff

API_TITLE = "OpenOrc API"


def _serving_lifespan(
    settings: Settings,
) -> Callable[[FastAPI], AbstractAsyncContextManager[None]]:
    """Build the serving-process lifespan: observability and executor ownership.

    Initialization happens inside the process that actually serves the
    application: uvicorn runs the lifespan once per serving process,
    including each reload subprocess, and never in a launcher parent.

    In regular execution mode the lifespan starts and owns exactly one
    bounded local executor per serving process (issue #86) and exactly one
    process-owned detached-turn observation host (issue #323) — started before the
    executor so observation exists before any dispatch, and disposed after
    the executor drained. Queue mode starts neither and never touches the
    queue backend. No executor or observer is created by module import,
    ``create_app()``, or a request. Observability teardown deliberately
    performs no process-global shutdown; the terminal flush is owned by the
    process-exit hook registered by initialization (issue #108). Executor
    teardown stops new admission and drains within the bounded default
    timeout, asserting nothing about any outstanding operation's outcome.
    """

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        initialize_observability(settings, ObservabilitySurface.API)
        executor: BoundedLocalExecutor | None = None
        observer: DetachedTurnObservationHost | None = None
        if settings.execution_mode is ExecutionMode.REGULAR:
            dependencies = build_default_operation_dependencies(settings)
            # The process-owned detached-turn observation host (issue #323):
            # started before any operation can dispatch and disposed after
            # the executor drained, so a registration from an in-flight
            # operation is still served during teardown. Its post-commit
            # completion handoff (issues #324/#333) routes each settled
            # validated completion by its explicit operation family — the
            # detached-established planning bootstrap resumption and the
            # initial-IMPLEMENT PR authorization presentation — resolving
            # every dependency lazily so the binding order of executor and
            # observer does not matter.
            observer = DetachedTurnObservationHost(
                dependencies.database_pool,
                observation_source=ClineHubTurnObservationSource(
                    backend_factory=ClineBridgeProcessBackend
                ),
                runtime_credential_resolver=resolve_connection_runtime_credential,
                control_endpoint_resolver=resolve_connection_control_endpoint,
                completion_handoff=build_detached_completion_handoff(dependencies),
            )
            observer.start()
            dependencies.bind_regular_process_detached_turn_observer(observer)
            executor = BoundedLocalExecutor(
                workers=settings.local_executor_workers,
                pending=settings.local_executor_pending,
                handlers=build_production_handlers(),
                dependencies=dependencies,
            )
            # The serving process's own executor is the regular-mode
            # continuation submitter: authorized follow-up operations are
            # admitted back into the same bounded executor (issue #253).
            dependencies.bind_regular_process_submitter(executor)
        app.state.operation_executor = executor
        app.state.detached_turn_observer = observer
        app.state.operation_submitter = build_operation_submitter(settings, local_executor=executor)
        try:
            yield
        finally:
            if executor is not None:
                executor.shutdown()
            if observer is not None:
                observer.dispose()

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
    app.include_router(tasks_router)
    app.state.settings = resolved
    return app


app = create_app()
