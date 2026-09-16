"""Reusable subprocess mechanics: runner boundary, children, supervision.

Nothing here is specific to Supabase or Valkey; those managers compose
these primitives. Tests substitute the ProcessRunner/ChildProcess seams
at the Python level.
"""

from __future__ import annotations

import contextlib
import os
import signal
import subprocess
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol

from openorc.devtools.devserver.logging import Logger

STOP_TERM_TIMEOUT_SECONDS = 5.0
STOP_POLL_INTERVAL_SECONDS = 0.1


class ChildProcess(Protocol):
    """A long-lived child process owned by the orchestrator."""

    @property
    def pid(self) -> int: ...

    def poll(self) -> int | None: ...

    def wait(self, timeout: float | None = None) -> int: ...

    def signal(self, signum: int) -> None: ...


@dataclass(frozen=True)
class RunResult:
    """Result of a completed short-lived command."""

    returncode: int
    stdout: str
    stderr: str


class ProcessRunner(Protocol):
    """Boundary for all child-process interaction.

    ``run`` executes a short-lived command (Supabase CLI, the migration
    tooling); ``spawn`` starts a long-lived child (API, worker, app, ngrok,
    devserver-owned queue server). Tests substitute this boundary instead of
    constructing fake executable trees.
    """

    def run(
        self,
        argv: Sequence[str],
        *,
        env: Mapping[str, str] | None = None,
        cwd: Path | None = None,
        capture: bool = True,
    ) -> RunResult: ...

    def spawn(
        self,
        label: str,
        argv: Sequence[str],
        *,
        env: Mapping[str, str] | None = None,
        cwd: Path | None = None,
        log_path: Path | None = None,
        inherit_stdin: bool = False,
    ) -> ChildProcess: ...


# Resolves a command name to an executable path (or None). Real execution
# uses shutil.which; deterministic tests inject a fake so the suite never
# depends on host-installed developer tools (supabase, npm, redis-server,
# ngrok).
CommandResolver = Callable[[str], str | None]


# ---------------------------------------------------------------------------
# Real process runner and child process
#
# Children start in their own session (start_new_session=True), so the child
# PID is the process-group ID and a group-wide signal reaches the whole child
# tree (for example npm and its Vite child). Terminal Ctrl-C reaches only the
# orchestrator, which then performs the single controlled cleanup.
# ---------------------------------------------------------------------------


class PopenChildProcess:
    """ChildProcess implementation over subprocess.Popen."""

    def __init__(self, process: subprocess.Popen[bytes]) -> None:
        self._process = process

    @property
    def pid(self) -> int:
        return self._process.pid

    def poll(self) -> int | None:
        return self._process.poll()

    def wait(self, timeout: float | None = None) -> int:
        try:
            return self._process.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            returncode = self._process.returncode
            return returncode if returncode is not None else -1

    def signal(self, signum: int) -> None:
        # The process tree may already be gone; poll()/wait() reaps it.
        with contextlib.suppress(ProcessLookupError, PermissionError):
            os.killpg(self._process.pid, signum)


class SubprocessRunner:
    """Real ProcessRunner over the subprocess module."""

    def run(
        self,
        argv: Sequence[str],
        *,
        env: Mapping[str, str] | None = None,
        cwd: Path | None = None,
        capture: bool = True,
    ) -> RunResult:
        completed = subprocess.run(
            list(argv),
            env=dict(env) if env is not None else None,
            cwd=str(cwd) if cwd is not None else None,
            stdin=subprocess.DEVNULL,
            capture_output=capture,
            text=capture,
            check=False,
        )
        stdout = completed.stdout if isinstance(completed.stdout, str) else ""
        stderr = completed.stderr if isinstance(completed.stderr, str) else ""
        return RunResult(returncode=completed.returncode, stdout=stdout, stderr=stderr)

    def spawn(
        self,
        label: str,
        argv: Sequence[str],
        *,
        env: Mapping[str, str] | None = None,
        cwd: Path | None = None,
        log_path: Path | None = None,
        inherit_stdin: bool = False,
    ) -> ChildProcess:
        output_handle = None
        stdout: int | None = None
        stderr: int | None = None
        if log_path is not None:
            log_path.parent.mkdir(parents=True, exist_ok=True)
            output_handle = log_path.open("ab")
            stdout = output_handle.fileno()
            stderr = subprocess.STDOUT
        try:
            process = subprocess.Popen(
                list(argv),
                env=dict(env) if env is not None else None,
                cwd=str(cwd) if cwd is not None else None,
                stdin=None if inherit_stdin else subprocess.DEVNULL,
                stdout=stdout,
                stderr=stderr,
                start_new_session=True,
            )
        finally:
            if output_handle is not None:
                output_handle.close()
        return PopenChildProcess(process)


def stop_child(
    log: Logger,
    label: str,
    child: ChildProcess,
    *,
    sleep: Callable[[float], None],
    term_timeout_seconds: float = STOP_TERM_TIMEOUT_SECONDS,
    poll_interval_seconds: float = STOP_POLL_INTERVAL_SECONDS,
) -> None:
    """Stop one child: TERM, bounded wait, escalate to KILL; always reap."""
    if child.poll() is None:
        log.log(f"Stopping {label} (PID {child.pid})...")
        child.signal(signal.SIGTERM)
        for _ in range(int(term_timeout_seconds / poll_interval_seconds)):
            if child.poll() is not None:
                break
            sleep(poll_interval_seconds)
        if child.poll() is None:
            log.warn(f"{label} (PID {child.pid}) did not stop gracefully; forcing.")
            child.signal(signal.SIGKILL)
    # Reap the child so nothing lingers as a zombie.
    child.wait()


@dataclass
class _ManagedProcess:
    label: str
    child: ChildProcess


class ProcessSupervisor:
    """Registry of started children; stops them in reverse start order."""

    def __init__(
        self,
        log: Logger,
        *,
        sleep: Callable[[float], None],
        stop_term_timeout_seconds: float = STOP_TERM_TIMEOUT_SECONDS,
    ) -> None:
        self._log = log
        self._sleep = sleep
        self._stop_term_timeout_seconds = stop_term_timeout_seconds
        self._managed: list[_ManagedProcess] = []

    def start(
        self,
        label: str,
        argv: Sequence[str],
        *,
        runner: ProcessRunner,
        env: Mapping[str, str] | None = None,
        cwd: Path | None = None,
        log_path: Path | None = None,
        inherit_stdin: bool = False,
    ) -> ChildProcess:
        child = runner.spawn(
            label, argv, env=env, cwd=cwd, log_path=log_path, inherit_stdin=inherit_stdin
        )
        self._managed.append(_ManagedProcess(label, child))
        return child

    def stop_all_reverse(self) -> None:
        """Stop children newest-first so dependents die before dependencies.

        Registration order is queue-server (owned), API, worker, ngrok,
        foreground service; reverse order stops the foreground service first
        and the queue server last, with the worker fully reaped (RQ teardown)
        before the queue backend is ever touched.
        """
        for managed in reversed(self._managed):
            try:
                stop_child(
                    self._log,
                    managed.label,
                    managed.child,
                    sleep=self._sleep,
                    term_timeout_seconds=self._stop_term_timeout_seconds,
                )
            except Exception as error:
                # One stuck child must not block stopping the remaining ones.
                self._log.warn(f"Failed to stop {managed.label} (PID {managed.child.pid}): {error}")
        self._managed.clear()
