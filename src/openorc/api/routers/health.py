"""Deterministic health/readiness router for the OpenOrc API."""

from __future__ import annotations

from fastapi import APIRouter

from openorc import __version__

router = APIRouter(tags=["health"])


@router.get("/healthz")
def read_health() -> dict[str, str]:
    """Report deterministic process health suitable for local smoke tests.

    This endpoint must not depend on Supabase, Valkey, GitHub, or Cline.
    Dependency-aware readiness probes arrive with persistence and queue
    integration work.
    """
    return {"status": "ok", "service": "openorc-api", "version": __version__}
