"""FastAPI application factory for the OpenOrc API transport surface."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncIterator, Callable
from contextlib import AbstractAsyncContextManager, asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from openorc import __version__
from openorc.adapters.cline import (
    ClineBridgeProcessBackend,
    resolve_connection_control_endpoint,
)
from openorc.adapters.cline.hub_observation import ClineHubTurnObservationSource
from openorc.api.dependencies.application_errors import application_error_response
from openorc.api.request_correlation import (
    REQUEST_ID_HEADER,
    RequestCorrelationMiddleware,
    server_error_response,
)
from openorc.api.routers.connections import router as connections_router
from openorc.api.routers.github_webhooks import router as github_webhooks_router
from openorc.api.routers.health import router as health_router
from openorc.api.routers.internal_queue_observer import (
    router as internal_queue_observer_router,
)
from openorc.api.routers.role_bindings import router as role_bindings_router
from openorc.api.routers.tasks import router as tasks_router
from openorc.api.routers.workspaces import router as workspaces_router
from openorc.config import ExecutionMode, Settings
from openorc.observability import ObservabilitySurface, initialize_observability
from openorc.services.cline_attachment import dispatch_cost_limit_interrupt
from openorc.services.connection_credentials import resolve_connection_runtime_credential
from openorc.services.errors import ApplicationError
from openorc.services.task_detached_turn_observation import DetachedTurnObservationHost
from openorc.workers.invocation import (
    OperationDependencies,
    build_default_operation_dependencies,
    build_operation_submitter,
    build_production_handlers,
)
from openorc.workers.local_executor import BoundedLocalExecutor
from openorc.workers.task_queue_operations import arrange_task_queue_recovery
from openorc.workers.task_retry_recovery import arrange_task_retry_recovery
from openorc.workers.task_workflow import (
    F1AuthorizedRetrySubmission,
    build_detached_completion_handoff,
)

API_TITLE = "OpenOrc API"

# The v1 reference is the FastAPI-generated documentation (issue #369): the
# public client contract lives under the URL-major ``/api/v1`` namespace while
# the API package ``version`` stays the package release version — the two are
# deliberately distinct. Health, provider webhook ingress, and private
# deployment infrastructure stay outside the versioned public namespace.
API_DESCRIPTION = (
    "OpenOrc control-plane HTTP transport. The public client contract is the "
    "versioned /api/v1 namespace: the Profile/Workspace discovery, creation "
    "and configuration routes (issue #370), the Workspace Connection "
    "administration and control-credential routes (issue #371), the "
    "role-binding configuration routes (issue #162), and the Task "
    "discussion/conversation routes (issue #339), authenticated by a "
    "Supabase Auth session JWT (GitHub-only sign-in) supplied as an HTTP "
    "Bearer credential — publishable and secret Supabase keys are not user "
    "JWTs. Deterministic health (/healthz), provider webhook ingress "
    "(/api/github/webhooks), and private deployment infrastructure are "
    "outside the versioned public namespace."
)

# OpenAPI tag metadata (issue #369): meaningful grouping/descriptions for the
# operations the published schema carries.
OPENAPI_TAG_METADATA = [
    {
        "name": "role-bindings",
        "description": (
            "Per-role runtime-session configuration of a Workspace "
            "(issue #162): the Owner-authorized read and full-record "
            "replacement."
        ),
    },
    {
        "name": "tasks",
        "description": (
            "Task-scoped discussion and conversation transport (issue #339): "
            "the asynchronous Owner ↔ Reviewer discussion admission, its "
            "durable receipt-status projection, and the transient "
            "Reviewer-conversation read."
        ),
    },
    {
        "name": "workspaces",
        "description": (
            "Profile/Workspace discovery, creation and configuration "
            "(issue #370): the authenticated Profile's owned-Workspace list "
            "and creation, the ownership-gated Workspace read model with its "
            "first-class settings, and the three Owner-authorized setting "
            "updates (review-iteration limit, guidance prose, default Task "
            "USD budget)."
        ),
    },
    {
        "name": "connections",
        "description": (
            "Workspace Agent Runtime Connection administration (issue #371): "
            "the Owner-authorized configured-inventory list/create/read/"
            "update with #188 safe-config validation, and the trusted "
            "control-credential setup/rotation over the issue #55 service. "
            "Exposed state is configured inventory only — never runtime Hub "
            "health, session READY, or active-run state."
        ),
    },
    {
        "name": "health",
        "description": (
            "Deterministic process health/readiness; an infrastructure "
            "endpoint outside the versioned public namespace."
        ),
    },
    {
        "name": "github-webhooks",
        "description": (
            "GitHub App webhook ingress authenticated by the provider's own "
            "signature material; provider-facing, outside the versioned "
            "public namespace."
        ),
    },
]

logger = logging.getLogger(__name__)


def _build_detached_turn_observer(
    dependencies: OperationDependencies,
) -> DetachedTurnObservationHost:
    """Build the serving process's one detached-turn observation host.

    The composition root (issue #348): only this concrete composition
    selects the Cline integration's observation source and its trusted
    credential/endpoint resolution, beside the post-commit completion
    handoff and the cost-limit Interrupt dispatcher. Both execution modes
    host through this one composition — the regular-mode serving process
    beside its bounded local executor, and the queue-mode API/control-plane
    process independently of any workhorse (issue #346).
    """
    return DetachedTurnObservationHost(
        dependencies.database_pool,
        observation_source=ClineHubTurnObservationSource(backend_factory=ClineBridgeProcessBackend),
        runtime_credential_resolver=resolve_connection_runtime_credential,
        control_endpoint_resolver=resolve_connection_control_endpoint,
        completion_handoff=build_detached_completion_handoff(dependencies),
        # The composition-rooted post-commit cost-limit Interrupt
        # dispatcher (issue #341): the host drives the shared cost
        # evaluation with it so a threshold crossing observed while
        # the dispatch worker is gone enforces through the existing
        # E10b machinery. Resolved through the process pool provider
        # per dispatch.
        cost_limit_interrupt_dispatch=lambda intent, external_session_id: (
            dispatch_cost_limit_interrupt(
                dependencies.database_pool(),
                intent=intent,
                external_session_id=external_session_id,
            )
        ),
        # The F12 (issue #316) post-settlement authorized-retry submission
        # seam: a settled observed failure's committed RETRY_AUTHORIZED
        # disposition arranges its one typed F1 retry invocation strictly
        # after the settlement committed, through this process's own
        # submission seam resolved at submit time. Never sleeping on the
        # settlement path — the executing F1 worker/job owns the due wait.
        retry_submission=F1AuthorizedRetrySubmission(dependencies.continuation_submitter),
    )


def _serving_lifespan(
    settings: Settings,
) -> Callable[[FastAPI], AbstractAsyncContextManager[None]]:
    """Build the serving-process lifespan: observability and executor ownership.

    Initialization happens inside the process that actually serves the
    application: uvicorn runs the lifespan once per serving process,
    including each reload subprocess, and never in a launcher parent.

    In regular execution mode the lifespan starts and owns exactly one
    bounded local executor per serving process (issue #86) and exactly one
    process-owned detached-turn observation host (issue #323) — started
    before the executor so observation exists before any dispatch, and
    disposed after the executor drained. In queue mode the serving
    API/control-plane process starts exactly one long-lived detached-turn
    observation host of its own (issue #346) — never in an RQ job, never
    inside a workhorse — performs its one bounded durable restart
    reconciliation off the event loop, and never creates an executor: queue
    operations are delivered by RQ and the private registration handshake
    routes to this host. No executor or observer is created by module
    import, ``create_app()``, or a request. Observability teardown
    deliberately performs no process-global shutdown; the terminal flush is
    owned by the process-exit hook registered by initialization (issue
    #108). Executor teardown stops new admission and drains within the
    bounded default timeout, asserting nothing about any outstanding
    operation's outcome.
    """

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        initialize_observability(settings, ObservabilitySurface.API)
        executor: BoundedLocalExecutor | None = None
        observer: DetachedTurnObservationHost | None = None
        dependencies = build_default_operation_dependencies(settings)
        if settings.execution_mode is ExecutionMode.REGULAR:
            # The process-owned detached-turn observation host (issue #323):
            # started before any operation can dispatch and disposed after
            # the executor drained, so a registration from an in-flight
            # operation is still served during teardown. Its post-commit
            # completion handoff (issues #324/#333) routes each settled
            # validated completion by its explicit operation family,
            # resolving every dependency lazily so the binding order of
            # executor and observer does not matter.
            observer = _build_detached_turn_observer(dependencies)
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
        else:
            # The queue-mode hosting (issue #346): the serving
            # API/control-plane process owns the ONE long-lived observation
            # host independently of every RQ workhorse process, then runs its
            # one bounded durable restart reconciliation off the event loop
            # so a host restart re-proves the durable outstanding receipts
            # through the existing registration gates. The v1 single-host
            # deployment assumption is explicit: multiple API instances must
            # not silently split session registrations.
            observer = _build_detached_turn_observer(dependencies)
            observer.start()
            await asyncio.to_thread(observer.reconcile_durable_outstanding_sessions)
        submitter = build_operation_submitter(settings, local_executor=executor)
        # The F11 bounded post-restart Task-queue recovery (issue #257), off
        # the event loop in BOTH execution modes: re-arrange lost capacity
        # wakes for the persisted queued Tasks and recover a lost F1
        # planning-bootstrap handoff only where the durable state positively
        # proves the bootstrap was never consumed. Regular mode delivers the
        # recovered arrangements through its own bounded local executor;
        # queue mode submits them to the canonical queue beside the
        # reconciliation above. Scheduling only — a failed pass is contained
        # and the next supported trigger revisits the rest; the durable
        # queued facts own recovery.
        try:
            await asyncio.to_thread(
                arrange_task_queue_recovery,
                dependencies.database_pool(),
                submitter,
            )
        except Exception as error:  # noqa: BLE001 - contained recovery pass
            logger.warning(
                "the bounded task-queue recovery pass failed (error type %s); "
                "the durable queued facts own recovery until the next trigger",
                type(error).__name__,
            )
        # The F12 bounded post-restart authorized-retry recovery (issue
        # #316), off the event loop in BOTH execution modes: resubmit the
        # same typed F1 retry invocation for each still-outstanding durably
        # authorized Level 2 retry — never into the F11 Task FIFO or its
        # capacity processor (these Tasks own their reserved sessions), and
        # never clearing, resetting, or rewriting the retry ledger. The
        # executing F1 worker/job performs any remaining ``not_before``
        # wait; duplicate deliveries are rejected by the F5 one-shot
        # consumption. Scheduling only — a failed pass is contained and the
        # next supported trigger revisits the rest.
        try:
            await asyncio.to_thread(
                arrange_task_retry_recovery,
                dependencies.database_pool(),
                submitter,
            )
        except Exception as error:  # noqa: BLE001 - contained recovery pass
            logger.warning(
                "the bounded authorized-retry recovery pass failed (error type %s); "
                "the committed authorization rows own recovery until the next trigger",
                type(error).__name__,
            )
        app.state.operation_executor = executor
        app.state.detached_turn_observer = observer
        app.state.operation_submitter = submitter
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
        description=API_DESCRIPTION,
        version=__version__,
        openapi_tags=OPENAPI_TAG_METADATA,
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
    # The public client contract is versioned (issue #369): the public
    # application routers mount under /api/v1 while health, provider webhook
    # ingress, and the private registration transport stay outside it.
    app.include_router(health_router)
    app.include_router(github_webhooks_router)
    app.include_router(workspaces_router, prefix="/api/v1")
    app.include_router(connections_router, prefix="/api/v1")
    app.include_router(role_bindings_router, prefix="/api/v1")
    app.include_router(tasks_router, prefix="/api/v1")
    # The private queue-mode detached-turn observation registration transport
    # (issue #346): deployment-only, authenticated, and nonpublic; it serves
    # the serving process's own observation host and is never an application
    # command or Owner-authority surface. Private infrastructure is also
    # excluded from the published OpenAPI schema (issue #369).
    app.include_router(internal_queue_observer_router)
    # Optional deployment-configured browser-CORS support (issue #369): HTTP
    # mechanism only, enabled solely by an explicit configured origin
    # allow-list — never a hardcoded Cloud/frontend origin, never a permissive
    # default, and simply left unset when an edge proxy already owns CORS.
    # Added after the correlation middleware so it is the outermost layer: a
    # CORS preflight short-circuits as browser mechanics before any
    # application request span exists, while every application request still
    # owns exactly one canonical correlation span, and the correlation
    # request-ID response header is exposed to allowed browser clients.
    if resolved.cors_allowed_origins:
        app.add_middleware(
            CORSMiddleware,
            allow_origins=list(resolved.cors_allowed_origins),
            allow_methods=["GET", "POST", "PUT"],
            allow_headers=["Authorization", "Content-Type"],
            expose_headers=[REQUEST_ID_HEADER],
        )
    app.state.settings = resolved
    return app


app = create_app()
