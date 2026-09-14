"""FastAPI application factory for the OpenOrc API transport surface."""

from __future__ import annotations

from fastapi import FastAPI

from openorc import __version__
from openorc.api.routers.health import router as health_router
from openorc.config import Settings

API_TITLE = "OpenOrc API"


def create_app(settings: Settings | None = None) -> FastAPI:
    """Build the OpenOrc API application.

    Routers are thin transports over shared application code. The
    health/readiness endpoint is deterministic and must not probe external
    systems (Supabase, Valkey, GitHub, Cline).
    """
    resolved = settings if settings is not None else Settings.from_env()
    app = FastAPI(title=API_TITLE, version=__version__)
    app.include_router(health_router)
    app.state.settings = resolved
    return app


app = create_app()
