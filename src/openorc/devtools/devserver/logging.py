"""Secret-safe logging and diagnostic redaction helpers.

Credentials and tokens are never logged; URLs are logged host-only.
"""

from __future__ import annotations

import re
import sys
from collections.abc import Mapping
from typing import Protocol, TextIO

LOG_PREFIX = "[devserver]"


class Logger(Protocol):
    """Logging boundary; implementations must never emit secret values."""

    def log(self, message: str) -> None: ...

    def warn(self, message: str) -> None: ...

    def error(self, message: str) -> None: ...


class StreamLogger:
    """Print-based logger writing prefixed lines to stdout/stderr."""

    def __init__(self, stdout: TextIO | None = None, stderr: TextIO | None = None) -> None:
        self._stdout = stdout if stdout is not None else sys.stdout
        self._stderr = stderr if stderr is not None else sys.stderr

    def log(self, message: str) -> None:
        print(f"{LOG_PREFIX} {message}", file=self._stdout, flush=True)

    def warn(self, message: str) -> None:
        print(f"{LOG_PREFIX} WARNING: {message}", file=self._stderr, flush=True)

    def error(self, message: str) -> None:
        print(f"{LOG_PREFIX} ERROR: {message}", file=self._stderr, flush=True)


# ---------------------------------------------------------------------------
# Redaction and URL helpers
#
# Credentials and tokens are never logged; URLs are logged host-only.
# ---------------------------------------------------------------------------

_URL_WITH_CREDENTIALS_RE = re.compile(r"((?:postgres(?:ql)?|https?)://[^:/@\s]+):[^@\s]+@")
_URL_SCHEMES = {"postgres", "postgresql", "redis", "rediss", "https", "http"}


def sanitize_cli_error(text: str, env: Mapping[str, str]) -> str:
    """Redact the access token and URL credentials from CLI diagnostics."""
    token = env.get("SUPABASE_ACCESS_TOKEN")
    if token:
        text = text.replace(token, "<redacted-token>")
    return _URL_WITH_CREDENTIALS_RE.sub(r"\1:<redacted>@", text)


def redact_url(url: str) -> str:
    """Return a credential-free, host-oriented form of a URL."""
    scheme, separator, rest = url.partition("://")
    if not separator or scheme not in _URL_SCHEMES:
        return "<unrecognized-url>"
    if "@" in rest:
        rest = rest.rpartition("@")[2]
    host = re.split(r"[/?:]", rest, maxsplit=1)[0]
    host = host.removeprefix("[").removesuffix("]")
    if not host:
        return "<unrecognized-url>"
    return f"{scheme}://{host}"
