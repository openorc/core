"""PromptTemplateOverride domain models.

A PromptTemplateOverride is the Workspace-owned override of one built-in
prompt slot's configurable instruction text (Phase 1, issue #26).

- Built-in prompt defaults remain application code/versioned defaults and
  are never materialized into persistence: an override row exists exactly
  while a Workspace has overridden the slot, and row absence means the
  currently shipped built-in default applies. A reset is row absence,
  never a tombstone and never a stored copy of the default.
- An override changes configurable instruction text only. It can never
  replace or redefine OpenOrc's protocol envelope, structured response
  schema, review-subject identity, session semantics, authority rules,
  communication topology, or workflow transitions. ``template_key`` names
  an instruction slot; the record is not a complete prompt/protocol
  template, so the instruction-only boundary holds by construction.
- ``base_template_version`` is the built-in version the override was
  authored against. It preserves enough template/version identity for
  later audit reconstruction without copying built-in prompts into the
  database.
- ``(workspace_id, template_key)`` identifies the override slot: one
  current override per Workspace per template key. Changing an override
  updates that slot in place (identity and creation instant preserved);
  resetting deletes the row.

Prompt override persistence does not write workflow events: the later
application/service layer coordinates the state mutation plus its
``prompt_override_changed`` audit event. This module carries
transport-independent validation only — no prompt rendering, no
effective-prompt resolution, no workflow orchestration.
"""

from __future__ import annotations

from dataclasses import dataclass, fields
from datetime import datetime
from uuid import UUID

__all__ = [
    "PromptTemplateOverride",
    "PromptTemplateOverrideDomainError",
    "prompt_template_override_field_names",
]


class PromptTemplateOverrideDomainError(Exception):
    """Raised when a PromptTemplateOverride domain invariant is violated."""


def _require_uuid(value: object, name: str) -> None:
    if not isinstance(value, UUID):
        raise PromptTemplateOverrideDomainError(f"PromptTemplateOverride.{name} must be a UUID")


def _require_nonblank_str(value: object, name: str) -> None:
    if not isinstance(value, str) or not value.strip():
        raise PromptTemplateOverrideDomainError(
            f"PromptTemplateOverride.{name} must be a non-empty string"
        )


@dataclass(frozen=True, slots=True)
class PromptTemplateOverride:
    """One Workspace's current override of one built-in prompt slot.

    ``instruction_text`` is the overridden configurable instruction text
    and ``base_template_version`` is the built-in version the override
    was authored against. Built-in defaults are never stored: this record
    exists exactly while the slot is overridden, and resetting deletes
    it (row absence restores the built-in default).
    """

    id: UUID
    workspace_id: UUID
    template_key: str
    base_template_version: str
    instruction_text: str
    created_at: datetime
    updated_at: datetime

    def __post_init__(self) -> None:
        for name in ("id", "workspace_id"):
            _require_uuid(getattr(self, name), name)
        for name in ("template_key", "base_template_version", "instruction_text"):
            _require_nonblank_str(getattr(self, name), name)


def prompt_template_override_field_names() -> frozenset[str]:
    """Return the exact field set a PromptTemplateOverride exposes.

    Lets tests prove the record carries only the override slot's own
    facts — instruction text, the built-in version it was authored
    against, and slot identity: no protocol/authority/session/workflow
    semantics, no built-in default content, and no rendered-template
    materialization.
    """
    return frozenset(field.name for field in fields(PromptTemplateOverride))
