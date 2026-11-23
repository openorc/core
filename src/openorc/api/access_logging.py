"""Credential-safe uvicorn access logging for the API serving boundary (issue #373).

The GitHub user-authorization provider ingress route receives the OAuth
``code`` and ``state`` as query parameters — credential-bearing and
correlation-bearing values that must never reach logs or telemetry (the
issue #373 non-disclosure contract). Uvicorn's default access formatter
renders the full request target (path plus query string), so this module
supplies the serving boundary's log configuration: the access formatter
rewrites the request target of the credential-bearing ingress paths to the
bare path (dropping the query string) before formatting, leaving every
other route's target untouched.

Application telemetry is already safe by construction (the correlation
middleware logs request IDs only; the observability rules forbid request
targets), so this boundary owns the framework's own access log. Reverse
proxies and hosting layers are deployment responsibilities documented in
``docs/development.md``: they must not persist (or must redact) the callback
URL's query string.

The formatter is deliberately defensive about the access record's argument
shape (uvicorn's documented ``(client_addr, method, full_path, http_version,
status_code)`` tuple): any unexpected shape delegates unchanged so the
access log keeps working, and the owning test proves the redaction against
the real record shape the pinned uvicorn produces.
"""

from __future__ import annotations

import copy
import logging
from typing import Any

from uvicorn.config import LOGGING_CONFIG
from uvicorn.logging import AccessFormatter

from openorc.api.routers.github_user_authorization_callback import (
    GITHUB_USER_AUTHORIZATION_CALLBACK_PATH,
)

__all__ = ["RedactingAccessFormatter", "uvicorn_log_config"]

# Paths whose request targets carry credential-bearing query parameters and
# must never be logged with their query string.
_CREDENTIAL_BEARING_PATHS = frozenset({GITHUB_USER_AUTHORIZATION_CALLBACK_PATH})

# Uvicorn's default access formatter template (the fmt is inherited
# unchanged; only the request target is rewritten).
_ACCESS_FMT = '%(levelprefix)s %(client_addr)s - "%(request_line)s" %(status_code)s'


def _redacted_access_args(args: object) -> object:
    """Drop the query string from credential-bearing ingress request targets.

    Uvicorn's access records carry ``(client_addr, method, full_path,
    http_version, status_code)``. Any unexpected shape is returned unchanged
    so the access log keeps working; the owning test pins the real shape.
    """
    if not isinstance(args, tuple) or len(args) != 5 or not isinstance(args[2], str):
        return args
    target = args[2]
    if target.split("?", 1)[0] not in _CREDENTIAL_BEARING_PATHS:
        return args
    bare_target = target.split("?", 1)[0]
    return (args[0], args[1], bare_target, args[3], args[4])


class RedactingAccessFormatter(AccessFormatter):
    """Uvicorn access formatter that never renders credential-bearing queries."""

    def formatMessage(self, record: logging.LogRecord) -> str:
        record.args = _redacted_access_args(record.args)  # type: ignore[assignment]
        return super().formatMessage(record)


def uvicorn_log_config() -> dict[str, Any]:
    """The serving boundary's uvicorn log configuration (issue #373).

    Mirrors uvicorn's default logging configuration with the access
    formatter replaced by :class:`RedactingAccessFormatter`. The formatter
    class is referenced by importable qualname, so the configuration works
    in uvicorn's reload subprocesses too. ``apps/api/main.py`` passes this
    to ``uvicorn.run`` so every repo-owned API serving surface redacts the
    credential-bearing callback target by construction.
    """
    config = copy.deepcopy(LOGGING_CONFIG)
    config["formatters"]["access"] = {
        "()": "openorc.api.access_logging.RedactingAccessFormatter",
        "fmt": _ACCESS_FMT,
    }
    return config
