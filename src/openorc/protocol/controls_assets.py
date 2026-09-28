"""Loading and deterministic rendering of the canonical v1 workflow-control
prose assets (issue #129).

The only canonical v1 control Markdown assets are the two files shipped under
``src/openorc/protocol/controls/``: ``plan.md`` (rendered by PLAN) and
``pr_compose.md`` (rendered by PR_COMPOSE). They are settled OpenOrc product
contracts. This module is their single access path so installed wheels load
the same canonical assets the repository ships, and so REVIEW, REVISE, and
IMPLEMENT can never resolve to a hidden prompt asset: any control-asset name
other than these two fails closed.

Workflow semantics do not imply prompt prose. v1 defines no review, remediation,
or implementation Markdown asset, and no Python-synthesized competing prompt
templates exist anywhere. Rendering substitutes only the controlled
``{{repository_full_name}}`` and ``{{issue_number}}`` insertion points supplied
by the calling workflow service from authoritative OpenOrc state, and fails
closed when an asset is missing an expected placeholder or contains an
unresolved placeholder-like token after substitution.

There is no prompt/template version, hash, or history machinery, and identical
inputs always produce identical output.

Standard library imports only: the canonical assets must remain loadable from
a dependency-free installed package artifact.
"""

from __future__ import annotations

import re
from functools import cache
from importlib import resources
from typing import Final

_CONTROLS_DIRECTORY: Final = "controls"

PLAN_CONTROL_ASSET: Final = "plan"
PR_COMPOSE_CONTROL_ASSET: Final = "pr_compose"

# The closed set of renderable canonical v1 control assets. REVIEW, REVISE,
# IMPLEMENT, and every other name resolve to no asset, by construction.
CONTROL_ASSETS: Final[frozenset[str]] = frozenset({PLAN_CONTROL_ASSET, PR_COMPOSE_CONTROL_ASSET})

_REPOSITORY_FULL_NAME_PLACEHOLDER: Final = "{{repository_full_name}}"
_ISSUE_NUMBER_PLACEHOLDER: Final = "{{issue_number}}"

_UNRESOLVED_PLACEHOLDER_PATTERN: Final[re.Pattern[str]] = re.compile(r"\{\{[a-z0-9_]+\}\}")


def _asset_file_name(asset: str) -> str:
    if asset not in CONTROL_ASSETS:
        raise ValueError(
            f"unknown canonical v1 workflow-control asset: {asset!r}. "
            "Only plan and pr_compose have canonical v1 prose assets."
        )
    return f"{asset}.md"


@cache
def control_asset_text(asset: str) -> str:
    """Return the canonical workflow-control Markdown asset text.

    Loaded from the installed OpenOrc package; never overridden by a
    Workspace, a Connection, a database table, or a prompt registry. Any
    name outside the closed canonical set fails closed.
    """
    asset_file = resources.files("openorc.protocol") / _CONTROLS_DIRECTORY / _asset_file_name(asset)
    return asset_file.read_text(encoding="utf-8")


def render_control_asset(
    asset: str,
    *,
    repository_full_name: str,
    issue_number: int,
) -> str:
    """Return the canonical control asset with its controlled insertion
    points rendered from authoritative OpenOrc-supplied values.

    Deterministic: identical inputs always produce identical output.
    Rendering fails closed when the asset is missing an expected placeholder
    or contains an unresolved placeholder-like token after substitution.
    """
    text = control_asset_text(asset)
    if _REPOSITORY_FULL_NAME_PLACEHOLDER not in text:
        raise ValueError(
            f"canonical {asset} control asset is missing the controlled "
            "repository_full_name insertion point"
        )
    if _ISSUE_NUMBER_PLACEHOLDER not in text:
        raise ValueError(
            f"canonical {asset} control asset is missing the controlled "
            "issue_number insertion point"
        )
    rendered = text.replace(_REPOSITORY_FULL_NAME_PLACEHOLDER, repository_full_name)
    rendered = rendered.replace(_ISSUE_NUMBER_PLACEHOLDER, str(issue_number))
    remaining = _UNRESOLVED_PLACEHOLDER_PATTERN.search(rendered)
    if remaining is not None:
        raise ValueError(
            f"canonical {asset} control asset contains an unresolved "
            f"placeholder: {remaining.group(0)}"
        )
    return rendered
