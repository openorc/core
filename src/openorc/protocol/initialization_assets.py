"""Loading, rendering, and Workspace-guidance composition of the canonical v1
role-initialization assets (issue #66).

The two Markdown files shipped under ``src/openorc/protocol/initialization/``
are settled OpenOrc product contracts. This module is the single access path
for them so installed wheels load the same canonical assets the repository
ships (``tests/test_protocol_packaging.py`` proves the installed-artifact
path), and so every controlled ``{{...}}`` schema insertion point in the
assets is rendered only from the canonical v1 JSON Schema files through
``openorc.protocol.schema_assets.render_schema`` — never a handwritten schema
copy. Asset-integrity problems (missing or unresolved insertion points) fail
closed instead of silently producing partial initialization content.

``compose_initialization`` is the one controlled composition boundary: the
rendered canonical initialization plus, only when the Workspace guidance is
nonblank, a mechanical delimiter/heading identifying the following text as
Owner-authored subordinate Workspace guidance, followed by that guidance
verbatim. Implementation code synthesizes no other prompt prose. There is no
prompt/template version, hash, history, or snapshot persistence, and
identical inputs always produce identical output.

Standard library imports only: the canonical assets must remain loadable from
a dependency-free installed package artifact.
"""

from __future__ import annotations

import re
from functools import cache
from importlib import resources
from typing import Final

from openorc.protocol import schema_assets
from openorc.protocol.models import (
    IMPLEMENTATION_RESULT_FAMILY,
    PLAN_RESULT_FAMILY,
    PR_RESULT_FAMILY,
    REVIEW_RESULT_FAMILY,
    SESSION_READY_FAMILY,
)

_INITIALIZATION_DIRECTORY: Final = "initialization"

PRODUCER_ROLE: Final = "producer"
REVIEWER_ROLE: Final = "reviewer"
INITIALIZATION_ROLES: Final[frozenset[str]] = frozenset({PRODUCER_ROLE, REVIEWER_ROLE})

# Role wiring for the controlled schema insertion points in the canonical
# assets. This is the composition wiring, never a schema definition.
_ROLE_SCHEMA_FAMILIES: Final[dict[str, tuple[str, ...]]] = {
    PRODUCER_ROLE: (
        PLAN_RESULT_FAMILY,
        IMPLEMENTATION_RESULT_FAMILY,
        PR_RESULT_FAMILY,
        SESSION_READY_FAMILY,
    ),
    REVIEWER_ROLE: (REVIEW_RESULT_FAMILY, SESSION_READY_FAMILY),
}

_UNRESOLVED_PLACEHOLDER_PATTERN: Final[re.Pattern[str]] = re.compile(r"\{\{[A-Z][A-Z0-9_]*\}\}")

# The only code-authored text beyond verbatim content: a mechanical delimiter
# identifying the following text as Owner-authored subordinate Workspace
# guidance. It carries no policy prose.
_GUIDANCE_DELIMITER: Final = "\n\n---\n\n## Workspace guidance (Owner-authored, subordinate)\n\n"


def _role_asset_name(role: str) -> str:
    if role not in INITIALIZATION_ROLES:
        raise ValueError(f"unknown OpenOrc initialization role: {role!r}")
    return f"{role}.md"


def _placeholder(family: str) -> str:
    return "{{" + family.upper() + "_SCHEMA}}"


@cache
def initialization_asset_text(role: str) -> str:
    """Return the canonical role-initialization Markdown asset text.

    Loaded from the installed OpenOrc package; never overridden by a
    Workspace, a Connection, a database table, or a prompt registry.
    """
    asset = resources.files("openorc.protocol") / _INITIALIZATION_DIRECTORY / _role_asset_name(role)
    return asset.read_text(encoding="utf-8")


def _render_asset_text(role: str, asset_text: str) -> str:
    """Substitute the controlled schema insertion points in an asset text.

    Fails closed when an expected insertion point is missing from the asset
    or when any placeholder-like token remains after substitution. Kept
    separate from the package-resource loader so tests can prove the
    fail-closed behavior with synthetic asset texts.
    """
    rendered = asset_text
    for family in _ROLE_SCHEMA_FAMILIES[role]:
        placeholder = _placeholder(family)
        if placeholder not in rendered:
            raise ValueError(
                f"canonical {role} initialization asset is missing the "
                f"controlled {family} schema insertion point"
            )
        rendered = rendered.replace(placeholder, schema_assets.render_schema(family))
    remaining = _UNRESOLVED_PLACEHOLDER_PATTERN.search(rendered)
    if remaining is not None:
        raise ValueError(
            f"canonical {role} initialization asset contains an unresolved "
            f"placeholder: {remaining.group(0)}"
        )
    return rendered


def render_initialization(role: str) -> str:
    """Return the canonical role initialization with every controlled schema
    insertion point rendered from the canonical v1 schema assets."""
    return _render_asset_text(role, initialization_asset_text(role))


def compose_initialization(role: str, guidance: str | None) -> str:
    """Return the initialization content for a role session.

    The rendered canonical role initialization is the whole content except
    for one mechanical addition: when ``guidance`` is nonblank, a delimiter
    heading identifying the following text as Owner-authored subordinate
    Workspace guidance is appended, followed by the guidance verbatim. Blank
    guidance (``None`` or whitespace-only) contributes nothing, and no other
    prose is ever synthesized. Guidance is never interpreted, parsed, or
    converted into configuration; Markdown and code fences inside it remain
    ordinary prose.
    """
    if guidance is not None and not isinstance(guidance, str):
        raise TypeError("Workspace guidance must be a string or None")
    rendered = render_initialization(role)
    if guidance is None or guidance.strip() == "":
        return rendered
    return rendered + _GUIDANCE_DELIMITER + guidance
