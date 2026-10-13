"""Loading and deterministic rendering of the canonical v1 workflow-control
prose assets (issue #129; workspace lifecycle per issue #181).

The canonical v1 control Markdown assets are the four files shipped under
``src/openorc/protocol/controls/``: ``plan.md`` (rendered by PLAN),
``pr_compose.md`` (rendered by PR_COMPOSE), ``prepare_workspace.md`` (rendered
by PREPARE_WORKSPACE), and ``cleanup_workspace.md`` (rendered by
CLEANUP_WORKSPACE). They are settled OpenOrc product contracts. This module is
their single access path so installed wheels load the same canonical assets
the repository ships, and so REVIEW, REVISE, and IMPLEMENT can never resolve
to a hidden prompt asset: any control-asset name outside this closed set fails
closed.

Workflow semantics do not imply prompt prose. v1 defines no review, remediation,
or implementation Markdown asset, and no Python-synthesized competing prompt
templates exist anywhere. Rendering substitutes only the controlled
``{{repository_full_name}}``, ``{{issue_number}}``, ``{{task_uuid}}``, and
``{{target_directory}}`` insertion points supplied by the calling workflow
service from authoritative OpenOrc state, and fails closed when an asset is
missing an expected placeholder, when a value required by the asset is not
supplied, when a value is supplied for a placeholder the asset does not use,
or when an unresolved placeholder-like token remains after substitution.

There is no prompt/template version, hash, or history machinery, and identical
inputs always produce identical output.

Standard library imports only: the canonical assets must remain loadable from
a dependency-free installed package artifact.
"""

from __future__ import annotations

import re
import uuid
from functools import cache
from importlib import resources
from typing import Final

_CONTROLS_DIRECTORY: Final = "controls"

PLAN_CONTROL_ASSET: Final = "plan"
PR_COMPOSE_CONTROL_ASSET: Final = "pr_compose"
PREPARE_WORKSPACE_CONTROL_ASSET: Final = "prepare_workspace"
CLEANUP_WORKSPACE_CONTROL_ASSET: Final = "cleanup_workspace"

# The closed set of renderable canonical v1 control assets. REVIEW, REVISE,
# IMPLEMENT, and every other name resolve to no asset, by construction.
CONTROL_ASSETS: Final[frozenset[str]] = frozenset(
    {
        PLAN_CONTROL_ASSET,
        PR_COMPOSE_CONTROL_ASSET,
        PREPARE_WORKSPACE_CONTROL_ASSET,
        CLEANUP_WORKSPACE_CONTROL_ASSET,
    }
)

_REPOSITORY_FULL_NAME_PLACEHOLDER: Final = "{{repository_full_name}}"
_ISSUE_NUMBER_PLACEHOLDER: Final = "{{issue_number}}"
_TASK_UUID_PLACEHOLDER: Final = "{{task_uuid}}"
_TARGET_DIRECTORY_PLACEHOLDER: Final = "{{target_directory}}"

# Per-asset closed sets of controlled insertion points. An asset missing an
# expected placeholder, a caller omitting a value the asset requires, or a
# caller supplying a value the asset does not use all fail closed.
_ASSET_PLACEHOLDERS: Final[dict[str, frozenset[str]]] = {
    PLAN_CONTROL_ASSET: frozenset({_REPOSITORY_FULL_NAME_PLACEHOLDER, _ISSUE_NUMBER_PLACEHOLDER}),
    PR_COMPOSE_CONTROL_ASSET: frozenset(
        {_REPOSITORY_FULL_NAME_PLACEHOLDER, _ISSUE_NUMBER_PLACEHOLDER}
    ),
    PREPARE_WORKSPACE_CONTROL_ASSET: frozenset(
        {
            _REPOSITORY_FULL_NAME_PLACEHOLDER,
            _ISSUE_NUMBER_PLACEHOLDER,
            _TASK_UUID_PLACEHOLDER,
            _TARGET_DIRECTORY_PLACEHOLDER,
        }
    ),
    CLEANUP_WORKSPACE_CONTROL_ASSET: frozenset(
        {
            _REPOSITORY_FULL_NAME_PLACEHOLDER,
            _ISSUE_NUMBER_PLACEHOLDER,
            _TASK_UUID_PLACEHOLDER,
            _TARGET_DIRECTORY_PLACEHOLDER,
        }
    ),
}

_UNRESOLVED_PLACEHOLDER_PATTERN: Final[re.Pattern[str]] = re.compile(r"\{\{[a-z0-9_]+\}\}")


def _placeholder_name(placeholder: str) -> str:
    return placeholder[2:-2]


def _asset_file_name(asset: str) -> str:
    if asset not in CONTROL_ASSETS:
        raise ValueError(
            f"unknown canonical v1 workflow-control asset: {asset!r}. "
            "Only the closed set of canonical v1 control assets is renderable."
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
    task_uuid: uuid.UUID | None = None,
    target_directory: str | None = None,
) -> str:
    """Return the canonical control asset with its controlled insertion
    points rendered from authoritative OpenOrc-supplied values.

    Deterministic: identical inputs always produce identical output.
    Rendering fails closed when the asset is missing an expected placeholder,
    when a value the asset requires is not supplied, when a value is supplied
    for a placeholder the asset does not use, or when an unresolved
    placeholder-like token remains after substitution.
    """
    text = control_asset_text(asset)
    expected = _ASSET_PLACEHOLDERS[asset]
    supplied: dict[str, str | None] = {
        _REPOSITORY_FULL_NAME_PLACEHOLDER: repository_full_name,
        _ISSUE_NUMBER_PLACEHOLDER: str(issue_number),
        _TASK_UUID_PLACEHOLDER: None if task_uuid is None else str(task_uuid),
        _TARGET_DIRECTORY_PLACEHOLDER: target_directory,
    }
    values: dict[str, str] = {}
    for placeholder in sorted(expected):
        if placeholder not in text:
            raise ValueError(
                f"canonical {asset} control asset is missing the controlled "
                f"{_placeholder_name(placeholder)} insertion point"
            )
        value = supplied[placeholder]
        if not value:
            raise ValueError(
                f"canonical {asset} control asset requires a value for the "
                f"controlled {_placeholder_name(placeholder)} insertion point"
            )
        values[placeholder] = value
    unexpected = [
        _placeholder_name(placeholder)
        for placeholder, value in supplied.items()
        if value is not None and placeholder not in expected
    ]
    if unexpected:
        raise ValueError(
            f"canonical {asset} control asset does not use the controlled "
            f"{', '.join(unexpected)} insertion point(s)"
        )
    rendered = text
    for placeholder in sorted(expected):
        rendered = rendered.replace(placeholder, values[placeholder])
    remaining = _UNRESOLVED_PLACEHOLDER_PATTERN.search(rendered)
    if remaining is not None:
        raise ValueError(
            f"canonical {asset} control asset contains an unresolved "
            f"placeholder: {remaining.group(0)}"
        )
    return rendered
