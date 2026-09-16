"""Shared devserver exception taxonomy.

Every devserver component may depend on this module; nothing here depends
on any other devserver component.
"""

from __future__ import annotations


class DevserverError(Exception):
    """Fatal devserver condition; the orchestrator fails closed."""


class UsageError(DevserverError):
    """Invalid command-line usage."""


class HelpRequested(Exception):
    """Raised when -h/--help is requested."""


class ShutdownRequested(Exception):
    """Raised in the main flow when a signal has requested shutdown.

    Signal handlers never clean up; they only record the request. The main
    flow converts the recorded request into this exception (or a foreground
    loop break) so the single finally-based cleanup path runs exactly once.
    """

    def __init__(self, signum: int) -> None:
        super().__init__(f"Shutdown requested by signal {signum}")
        self.signum = signum
