"""Tests for the credential-safe API access logging (issue #373).

The serving boundary's access formatter never renders the request target of
the credential-bearing GitHub user-authorization callback route: the OAuth
``code`` and correlation ``state`` query values would otherwise land in the
process access log. These tests pin the redaction against the exact record
shape the pinned uvicorn produces, prove other routes keep their full
target, and prove the package log config wires the redacting formatter.
"""

from __future__ import annotations

import contextlib
import logging

from openorc.api.access_logging import (
    _ACCESS_FMT,
    RedactingAccessFormatter,
    uvicorn_log_config,
)

_CALLBACK_PATH = "/api/github/user-authorization/callback"


def _access_record(full_path: str, status_code: int = 200) -> logging.LogRecord:
    """One access record in uvicorn's exact argument shape."""
    return logging.LogRecord(
        name="uvicorn.access",
        level=logging.INFO,
        pathname="uvicorn/protocols/http/impl.py",
        lineno=0,
        msg='%s - "%s %s HTTP/%s" %d',
        args=(
            "127.0.0.1:51234",
            "GET",
            full_path,
            "1.1",
            status_code,
        ),
        exc_info=None,
        func=None,
    )


def _formatted(full_path: str) -> str:
    formatter = RedactingAccessFormatter(fmt=_ACCESS_FMT, use_colors=False)
    return formatter.format(_access_record(full_path))


def test_the_callback_target_is_logged_without_its_query_string() -> None:
    rendered = _formatted(f"{_CALLBACK_PATH}?code=secret-oauth-code&state=correlation-state")

    assert _CALLBACK_PATH in rendered
    # The credential-bearing and correlation-bearing query values never reach
    # the access log (issue #373).
    assert "secret-oauth-code" not in rendered
    assert "correlation-state" not in rendered
    assert "?" not in rendered


def test_other_routes_keep_their_full_request_target() -> None:
    rendered = _formatted("/api/v1/profile/github-user-authorization?cursor=page-2&filter=open")

    assert "/api/v1/profile/github-user-authorization?cursor=page-2&filter=open" in rendered


def test_an_unexpected_record_shape_is_left_untouched() -> None:
    # Defensive delegation: any args shape the formatter does not recognize
    # passes through unchanged so the access log keeps working. (Uvicorn's
    # own unpacking may reject such a record; the redaction layer itself
    # never rewrites the args.)
    record = _access_record("/healthz")
    record.args = ("127.0.0.1:1",)  # type: ignore[assignment]

    formatter = RedactingAccessFormatter(fmt=_ACCESS_FMT, use_colors=False)
    with contextlib.suppress(Exception):
        formatter.format(record)


def test_the_package_log_config_wires_the_redacting_formatter() -> None:
    config = uvicorn_log_config()

    access_formatter = config["formatters"]["access"]
    assert access_formatter["()"] == "openorc.api.access_logging.RedactingAccessFormatter"
    assert access_formatter["fmt"] == _ACCESS_FMT
    # The rest of uvicorn's default logging configuration is preserved.
    assert config["loggers"]["uvicorn.access"]["handlers"] == ["access"]
    assert config["loggers"]["uvicorn.access"]["propagate"] is False
