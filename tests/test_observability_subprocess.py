"""Subprocess tests for real process-global observability behaviors (issue #108).

The OpenTelemetry global providers are set-once and shutdown is terminal,
so these behaviors cannot run inside the ordinary pytest process without
contaminating other tests. Each case drives the deterministic harness in a
small subprocess with in-memory exporters — no network, no SDK private
globals, no hosted observability backend.
"""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path
from typing import Any

from openorc import __version__

_HARNESS_PATH = Path(__file__).with_name("observability_subprocess_harness.py")


def _run_mode(mode: str) -> dict[str, dict[str, Any]]:
    result = subprocess.run(  # noqa: S603 - fixed argv, no shell
        [sys.executable, str(_HARNESS_PATH), mode],
        capture_output=True,
        text=True,
        timeout=180,
        check=False,
    )
    assert result.returncode == 0, result.stderr
    payloads = [json.loads(line) for line in result.stdout.splitlines() if line.startswith("{")]
    assert payloads, result.stdout
    typed: dict[str, dict[str, Any]] = {}
    for payload in payloads:
        kind = payload.get("kind")
        assert isinstance(kind, str)
        typed[kind] = payload
    return typed


def test_configured_runtime_installs_once_and_shuts_down_terminally() -> None:
    payloads = _run_mode("identity")
    pre_exit = payloads["pre_exit"]
    post_exit = payloads["post_exit"]

    assert pre_exit["configured"] is True
    assert pre_exit["terminal"] is True
    assert pre_exit["service_name"] == "openorc-api"
    assert pre_exit["service_version"] == __version__
    assert pre_exit["environment"] == "production"
    assert pre_exit["duplicate_noop"] is True
    assert pre_exit["surface_conflict"] is True
    assert pre_exit["endpoint_conflict"] is True
    assert pre_exit["reinit_terminal_error"] is True
    # The explicit terminal shutdown flushed the ended span and log record
    # to the in-memory exporters.
    assert post_exit["spans"] == 1
    assert post_exit["logs"] == 1


def test_unconfigured_runtime_installs_nothing_and_stays_reinitializable() -> None:
    payloads = _run_mode("unconfigured")
    pre_exit = payloads["pre_exit"]

    assert pre_exit["configured"] is False
    assert pre_exit["terminal"] is False
    assert pre_exit["sdk_provider_installed"] is False
    assert pre_exit["root_level_info"] is True
    assert pre_exit["reinitialized"] is True


def test_worker_surface_uses_worker_identity_and_terminal_shutdown() -> None:
    payloads = _run_mode("worker")
    pre_exit = payloads["pre_exit"]
    post_exit = payloads["post_exit"]

    assert pre_exit["configured"] is True
    assert pre_exit["terminal"] is True
    assert pre_exit["service_name"] == "openorc-worker"
    assert pre_exit["reinit_terminal_error"] is True
    assert post_exit["spans"] == 0
    assert post_exit["logs"] == 0


def test_process_exit_hook_flushes_ended_spans_and_logs() -> None:
    payloads = _run_mode("atexit_flush")
    pre_exit = payloads["pre_exit"]
    post_exit = payloads["post_exit"]

    # The main flow ends without an explicit shutdown: the registered
    # process-exit hook is what flushes the batch processors at exit.
    assert pre_exit["configured"] is True
    assert pre_exit["terminal"] is False
    assert post_exit["spans"] == 1
    assert post_exit["logs"] == 1
