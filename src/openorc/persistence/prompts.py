"""Repositories for PromptTemplateOverride persistence.

Explicit SQL repositories over the ``openorc`` schema for the
Workspace-owned prompt override slot (Phase 1, issue #26). Rows map to
transport-independent domain objects from :mod:`openorc.domain.prompts`;
instants returned from Postgres are normalized to timezone-aware UTC at
this boundary.

- Built-in prompt defaults remain application code and are never
  materialized here: an override row exists exactly while a Workspace
  has overridden the slot's instruction text, and row absence means the
  currently shipped built-in default applies. Reset is actual row
  deletion — never a tombstone, never a stored default copy.
- ``(workspace_id, template_key)`` is the override slot: the upsert
  preserves the existing row identity and ``created_at`` and advances
  ``updated_at`` — a configuration change, not historical replacement.
  The override carries configurable instruction text only and never
  protocol/authority/session/workflow semantics.
- This module does not write workflow events. Prompt changes are
  audited through WorkflowEvent by the later application/service layer,
  which coordinates the state mutation plus its audit event; persistence
  repositories remain focused on their own aggregate.

Violated database invariants (uniqueness, foreign keys, CHECK
constraints) surface as driver exceptions; translating driver exceptions
into typed application errors is a service-layer concern, not a
persistence one.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any
from uuid import UUID

from openorc.domain.prompts import (
    PromptTemplateOverride,
    PromptTemplateOverrideDomainError,
)
from openorc.persistence.pool import DatabasePool
from openorc.persistence.time import normalize_utc
from openorc.persistence.transactions import transaction

__all__ = [
    "get_prompt_template_override",
    "list_prompt_template_overrides",
    "reset_prompt_template_override",
    "set_prompt_template_override",
]

_PROMPT_TEMPLATE_OVERRIDE_COLUMNS = (
    "id, workspace_id, template_key, base_template_version, instruction_text, "
    "created_at, updated_at"
)


def _prompt_template_override_from_row(row: Sequence[Any]) -> PromptTemplateOverride:
    return PromptTemplateOverride(
        id=row[0],
        workspace_id=row[1],
        template_key=row[2],
        base_template_version=row[3],
        instruction_text=row[4],
        created_at=normalize_utc(row[5]),
        updated_at=normalize_utc(row[6]),
    )


def _require_uuid(value: object, name: str) -> None:
    if not isinstance(value, UUID):
        raise PromptTemplateOverrideDomainError(f"{name} must be a UUID")


def _require_nonblank_str(value: object, name: str) -> None:
    if not isinstance(value, str) or not value.strip():
        raise PromptTemplateOverrideDomainError(f"{name} must be a non-empty string")


def set_prompt_template_override(
    pool: DatabasePool,
    *,
    workspace_id: UUID,
    template_key: str,
    base_template_version: str,
    instruction_text: str,
) -> PromptTemplateOverride:
    """Create or update the Workspace's override of one prompt slot.

    ``(workspace_id, template_key)`` is the override slot: the upsert
    preserves the existing row ``id`` and ``created_at`` and updates the
    instruction text and base template version, advancing ``updated_at``.
    Built-in defaults are never written here — the override row is the
    only thing that exists, and removing it through
    :func:`reset_prompt_template_override` restores the built-in default
    by absence.
    """
    _require_uuid(workspace_id, "workspace_id")
    _require_nonblank_str(template_key, "template_key")
    _require_nonblank_str(base_template_version, "base_template_version")
    _require_nonblank_str(instruction_text, "instruction_text")
    with transaction(pool) as conn:
        row = conn.execute(
            "insert into openorc.prompt_template_overrides "
            "(workspace_id, template_key, base_template_version, instruction_text) "
            "values (%s, %s, %s, %s) "
            "on conflict (workspace_id, template_key) do update set "
            "base_template_version = excluded.base_template_version, "
            "instruction_text = excluded.instruction_text, "
            "updated_at = now() "
            f"returning {_PROMPT_TEMPLATE_OVERRIDE_COLUMNS}",
            (workspace_id, template_key, base_template_version, instruction_text),
        ).fetchone()
    assert row is not None
    return _prompt_template_override_from_row(row)


def get_prompt_template_override(
    pool: DatabasePool, *, workspace_id: UUID, template_key: str
) -> PromptTemplateOverride | None:
    """Return the Workspace's current override of one slot, or ``None``.

    ``None`` means the slot is not overridden: the currently shipped
    built-in default applies (defaults are never materialized here).
    """
    _require_uuid(workspace_id, "workspace_id")
    _require_nonblank_str(template_key, "template_key")
    with transaction(pool) as conn:
        row = conn.execute(
            f"select {_PROMPT_TEMPLATE_OVERRIDE_COLUMNS} "
            "from openorc.prompt_template_overrides "
            "where workspace_id = %s and template_key = %s",
            (workspace_id, template_key),
        ).fetchone()
    return None if row is None else _prompt_template_override_from_row(row)


def list_prompt_template_overrides(
    pool: DatabasePool, *, workspace_id: UUID
) -> list[PromptTemplateOverride]:
    """List a Workspace's current overrides in deterministic template_key order.

    Only actually-overridden slots appear: built-in defaults are
    application code and are never enumerated from persistence.
    """
    _require_uuid(workspace_id, "workspace_id")
    with transaction(pool) as conn:
        rows = conn.execute(
            f"select {_PROMPT_TEMPLATE_OVERRIDE_COLUMNS} "
            "from openorc.prompt_template_overrides "
            "where workspace_id = %s order by template_key",
            (workspace_id,),
        ).fetchall()
    return [_prompt_template_override_from_row(row) for row in rows]


def reset_prompt_template_override(
    pool: DatabasePool, *, workspace_id: UUID, template_key: str
) -> PromptTemplateOverride | None:
    """Delete the Workspace's override of one prompt slot.

    Reset means actual row deletion: row absence is "use the currently
    shipped built-in default", and no tombstone or built-in default copy
    is ever retained for audit purposes (override changes are audited by
    the service layer through WorkflowEvent). The ``DELETE ... RETURNING``
    yields the exact override that was removed — identity plus
    audit-relevant metadata — so a later application service recording a
    ``prompt_override_changed`` event has the deleted record without a
    racy read-before-delete sequence. ``None`` means there was nothing to
    reset (the slot already uses the built-in default).
    """
    _require_uuid(workspace_id, "workspace_id")
    _require_nonblank_str(template_key, "template_key")
    with transaction(pool) as conn:
        row = conn.execute(
            "delete from openorc.prompt_template_overrides "
            "where workspace_id = %s and template_key = %s "
            f"returning {_PROMPT_TEMPLATE_OVERRIDE_COLUMNS}",
            (workspace_id, template_key),
        ).fetchone()
    return None if row is None else _prompt_template_override_from_row(row)
