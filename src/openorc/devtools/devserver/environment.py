"""Optional .env loading and typed environment parsing helpers."""

from __future__ import annotations

import re
from collections.abc import Mapping, MutableMapping
from pathlib import Path

from openorc.devtools.devserver.logging import Logger

# ---------------------------------------------------------------------------
# Environment file
#
# Optional local configuration file: .env at the repository root (git-ignored).
# Blank lines and # comments are skipped, keys must be valid shell identifiers,
# one surrounding quote pair is stripped from values, and variables already
# exported in the calling shell always win over .env.
# ---------------------------------------------------------------------------

_ENV_KEY_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _strip_quotes(value: str) -> str:
    if len(value) >= 2 and value[0] == value[-1] and value[0] in {'"', "'"}:
        return value[1:-1]
    return value


def load_env_file(env_file: Path, env: MutableMapping[str, str], log: Logger) -> None:
    """Load the optional root .env into env; exported values always win."""
    if not env_file.is_file():
        log.log("No .env file found; using exported environment and defaults.")
        return

    log.log(f"Loading environment file: {env_file}")
    for raw_line in env_file.read_text(encoding="utf-8").splitlines():
        line = raw_line.lstrip()
        if not line or line.startswith("#"):
            continue
        key, separator, value = line.partition("=")
        if not separator or not _ENV_KEY_RE.match(key):
            log.warn(f"Ignoring invalid .env line: {key}")
            continue
        if key not in env:
            env[key] = _strip_quotes(value)


def _env_int(env: Mapping[str, str], key: str, default: int, log: Logger) -> int:
    raw = env.get(key)
    if not raw:
        return default
    try:
        return int(raw)
    except ValueError:
        log.warn(f"Ignoring non-integer {key}: {raw}")
        return default


def _env_float(env: Mapping[str, str], key: str, default: float, log: Logger) -> float:
    raw = env.get(key)
    if not raw:
        return default
    try:
        return float(raw)
    except ValueError:
        log.warn(f"Ignoring non-numeric {key}: {raw}")
        return default
