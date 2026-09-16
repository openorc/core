"""Local queue-backend ownership and guarded logical-DB reset.

Only explicitly localhost-shaped queue URLs may be flushed or managed;
the flush guard is based on the URL, never on server-process ownership.
"""

from __future__ import annotations

import os
import re
import shutil
from collections.abc import Callable
from pathlib import Path
from typing import Protocol

import redis

from openorc.devtools.devserver.errors import DevserverError
from openorc.devtools.devserver.logging import Logger, redact_url
from openorc.devtools.devserver.processes import (
    ChildProcess,
    CommandResolver,
    ProcessRunner,
    stop_child,
)

# OpenOrc queue naming is defined in src/openorc/workers/queues.py (openorc:
# prefix; canonical default queue openorc:default). The devserver local
# default is Redis DB index 2, the recommended local namespace (see
# .env.example); an exported VALKEY_URL always wins.
DEFAULT_VALKEY_URL = "redis://127.0.0.1:6379/2"

LOCAL_VALKEY_URL_RE = re.compile(r"^redis://(127\.0\.0\.1|localhost):([0-9]+)/([0-9]+)$")

VALKEY_READY_ATTEMPTS = 20
VALKEY_READY_SLEEP_SECONDS = 0.5
OWNERSHIP_GUARD_ATTEMPTS = 10
OWNERSHIP_GUARD_SLEEP_SECONDS = 0.1


class QueueClient(Protocol):
    """Minimal Redis-compatible client boundary (readiness + guarded flush)."""

    def ping(self) -> bool: ...

    def flushdb(self) -> None: ...


def is_local_valkey_url(url: str) -> bool:
    """True for the localhost-shaped queue URLs the logical-DB guard covers."""
    return LOCAL_VALKEY_URL_RE.match(url) is not None


class RedisQueueClient:
    """QueueClient adapter over the pinned redis client."""

    def __init__(self, url: str) -> None:
        self._client = redis.Redis.from_url(url, socket_connect_timeout=2.0, socket_timeout=2.0)

    def ping(self) -> bool:
        return bool(self._client.ping())

    def flushdb(self) -> None:
        self._client.flushdb()


def default_queue_client_factory(url: str) -> QueueClient:
    return RedisQueueClient(url)


# ---------------------------------------------------------------------------
# Local queue backend (Redis-compatible)
#
# Two independent safety questions:
#
# 1. Process ownership (may I stop this server?):
#    - a server already responding at the configured URL is externally owned;
#      it is used as-is and never stopped on cleanup;
#    - a server started by this orchestrator runs the redis-server binary
#      directly as an ephemeral child with persistence disabled and is
#      stopped on cleanup. NEVER `brew services start redis/valkey`: that
#      installs a persistent Homebrew LaunchAgent which outlives the
#      devserver. No Homebrew mutation is ever performed.
#
# 2. Logical DB ownership (may I reset this selected development DB?):
#    the explicitly localhost-shaped VALKEY_URL namespace (default
#    redis://127.0.0.1:6379/2) is flushed before the worker starts in both
#    externally-owned and devserver-owned cases. Non-local URLs are never
#    flushed and never managed. The flush guard is based on the URL, never
#    on server-process ownership.
# ---------------------------------------------------------------------------


class ValkeyManager:
    """Queue backend ownership plus guarded logical-DB reset."""

    def __init__(
        self,
        url: str,
        *,
        runner: ProcessRunner,
        log: Logger,
        sleep: Callable[[float], None],
        queue_client_factory: Callable[[str], QueueClient],
        tmp_root: Path,
        command_resolver: CommandResolver = shutil.which,
    ) -> None:
        self._url = url
        self._runner = runner
        self._log = log
        self._sleep = sleep
        self._queue_client_factory = queue_client_factory
        self._tmp_root = tmp_root
        self._command_resolver = command_resolver
        self._owned_child: ChildProcess | None = None
        self._owned_tmp_dir: Path | None = None

    @property
    def url(self) -> str:
        return self._url

    def _client(self) -> QueueClient:
        return self._queue_client_factory(self._url)

    def _responding(self) -> bool:
        try:
            self._client().ping()
        except Exception:  # any transport/timeout failure means not responding
            return False
        return True

    def ensure(self) -> None:
        """Reuse a responding server or start a devserver-owned one."""
        if not is_local_valkey_url(self._url):
            self._log.log(
                f"Using non-local queue backend from environment: {redact_url(self._url)}"
            )
            return
        if self._responding():
            self._log.log(
                f"Queue backend already responding at {self._url} "
                "(externally owned; devserver will not stop it on exit)"
            )
            return
        self._start_owned()

    def _start_owned(self) -> None:
        if self._command_resolver("redis-server") is None:
            raise DevserverError(
                f"No queue backend is responding at {self._url} and redis-server is not on "
                "PATH. Start a local Redis-compatible server or point VALKEY_URL at a "
                "running instance."
            )
        match = LOCAL_VALKEY_URL_RE.match(self._url)
        if match is None:  # unreachable: ensure() gates on the local URL shape
            raise DevserverError(
                f"Queue backend URL is not a supported local URL: {redact_url(self._url)}"
            )
        port = match.group(2)

        # Invocation-specific paths outside the repository keep concurrent
        # devserver runs independent and leave no repo residue.
        tmp_dir = self._tmp_root / f"openorc-redis-{os.getpid()}"
        log_path = self._tmp_root / f"openorc-redis-{os.getpid()}.log"
        tmp_dir.mkdir(parents=True, exist_ok=True)

        self._log.log(
            f"Starting devserver-owned queue backend (redis-server) on 127.0.0.1:{port}..."
        )
        self._log.log(f"Queue backend logs: {log_path}")
        child = self._runner.spawn(
            "queue-backend",
            [
                "redis-server",
                "--port",
                port,
                "--bind",
                "127.0.0.1",
                "--save",
                "",
                "--appendonly",
                "no",
                "--dir",
                str(tmp_dir),
            ],
            log_path=log_path,
        )
        # Record ownership immediately: if the ownership guard or the
        # readiness wait fails while the child is still alive, the canonical
        # cleanup path must still stop/reap this child and remove this temp
        # dir (startup failure after branch creation still invokes cleanup).
        self._owned_child = child
        self._owned_tmp_dir = tmp_dir

        # Ownership sanity guard: if a foreign server grabbed the port during
        # startup, our instance exits (bind conflict) within milliseconds
        # while the foreign instance answers the ping. Fail loudly instead of
        # silently adopting (and later killing) someone else's server.
        for _ in range(OWNERSHIP_GUARD_ATTEMPTS):
            if child.poll() is not None:
                raise DevserverError(
                    f"Queue backend answered at {self._url} but the devserver-started instance "
                    "exited; another server may already own the port. Devserver will not "
                    f"adopt it; see {log_path}"
                )
            self._sleep(OWNERSHIP_GUARD_SLEEP_SECONDS)

        for _ in range(VALKEY_READY_ATTEMPTS):
            if self._responding():
                self._log.log(
                    f"Queue backend started (devserver-owned PID {child.pid}; "
                    "stopped automatically on exit)"
                )
                return
            if child.poll() is not None:
                raise DevserverError(
                    f"devserver-started queue backend exited before becoming ready at "
                    f"{self._url}; see {log_path}"
                )
            self._sleep(VALKEY_READY_SLEEP_SECONDS)

        raise DevserverError(
            f"devserver-started queue backend did not become ready at {self._url}; see {log_path}"
        )

    def flush_local_db(self) -> None:
        """Flush the explicitly selected local development DB (guarded)."""
        if not is_local_valkey_url(self._url):
            return
        if not self._responding():
            raise DevserverError(
                f"Cannot flush local queue DB because the queue backend is not responding "
                f"at {self._url}."
            )
        self._log.log(f"Flushing local OpenOrc queue DB: {self._url}")
        self._client().flushdb()

    def stop_owned(self) -> None:
        """Stop only the server this invocation started; external servers untouched."""
        if self._owned_child is None:
            return
        child = self._owned_child
        self._owned_child = None
        stop_child(self._log, "queue-backend", child, sleep=self._sleep)

    def remove_temp_dir(self) -> None:
        """Remove this invocation's temp dir (only when devserver created it)."""
        tmp_dir = self._owned_tmp_dir
        if tmp_dir is None:
            return
        self._owned_tmp_dir = None
        if tmp_dir.parent != self._tmp_root or not tmp_dir.name.startswith("openorc-redis-"):
            self._log.warn(f"Refusing to remove unexpected queue temp dir: {tmp_dir}")
            return
        shutil.rmtree(tmp_dir, ignore_errors=True)
