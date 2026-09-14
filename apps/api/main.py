"""Thin process bootstrap for the OpenOrc API process surface.

This launcher owns process mechanics only: it loads configuration from the
environment boundary and runs the package application under the ASGI server.
Application behavior lives in package code, never here.
"""

from __future__ import annotations

import sys

import uvicorn

from openorc.config import ConfigurationError, Settings


def main() -> None:
    """Run the API process with environment-resolved settings."""
    settings = Settings.from_env()
    uvicorn.run(
        "openorc.api.app:app",
        host=settings.api_host,
        port=settings.api_port,
        reload=settings.api_reload,
    )


if __name__ == "__main__":
    try:
        main()
    except ConfigurationError as exc:
        sys.exit(f"openorc-api: invalid configuration: {exc}")
