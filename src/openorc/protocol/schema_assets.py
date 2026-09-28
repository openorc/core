"""Loading and rendering of the canonical v1 formal-response JSON Schema assets.

The five JSON Schema files shipped under ``src/openorc/protocol/schemas/`` are
the sole machine-contract source of truth for OpenOrc formal responses
(issue #65). This module is the single access path for those assets so no
handwritten schema copies appear in Markdown or Python constants, and so
installed wheels load the same canonical files the repository ships
(``tests/test_protocol_packaging.py`` proves the installed-artifact path).

Standard library imports only: the canonical assets must remain loadable from
a dependency-free installed package artifact.
"""

from __future__ import annotations

import json
from functools import cache
from importlib import resources
from typing import Any, Final

from openorc.protocol.models import FORMAL_RESPONSE_FAMILIES

_SCHEMA_DIRECTORY: Final = "schemas"


def _asset_name(family: str) -> str:
    if family not in FORMAL_RESPONSE_FAMILIES:
        raise ValueError(f"unknown OpenOrc formal response family: {family!r}")
    return f"{family}.json"


@cache
def schema_asset_text(family: str) -> str:
    """Return the canonical JSON Schema asset text for the response family."""
    asset = resources.files("openorc.protocol") / _SCHEMA_DIRECTORY / _asset_name(family)
    return asset.read_text(encoding="utf-8")


def load_schema(family: str) -> dict[str, Any]:
    """Return the canonical v1 JSON Schema for the response family, freshly parsed.

    A new dictionary is returned on every call so callers can never mutate a
    shared cache.
    """
    schema = json.loads(schema_asset_text(family))
    if not isinstance(schema, dict):  # defensive: canonical assets are schema objects
        raise ValueError(f"canonical schema asset for {family!r} is not a JSON object")
    return schema


def render_schema(family: str) -> str:
    """Return the canonical asset text verbatim, for controlled insertion points.

    Rendering is the shipped file itself and never a re-serialization, so
    interaction content produced for role initialization and workflow controls
    (#66/#129) cannot drift from the canonical machine contract.
    """
    return schema_asset_text(family)
