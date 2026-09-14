"""Thin process bootstrap for the OpenOrc RQ worker process surface.

This launcher owns process mechanics only. Queue transport behavior lives in
package code; the worker connects to its configured backend and runs until
stopped.
"""

from __future__ import annotations

import sys

from openorc.config import ConfigurationError, Settings
from openorc.workers.bootstrap import WorkerBootstrapError, run_worker


def main() -> None:
    """Run the worker process against its configured queue backend."""
    settings = Settings.from_env()
    run_worker(settings)


if __name__ == "__main__":
    try:
        main()
    except (ConfigurationError, WorkerBootstrapError) as exc:
        sys.exit(f"openorc-worker: {exc}")
