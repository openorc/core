"""Shared deterministic full Task repository-row builder for ordinary tests (issue #312).

``task_row`` builds the complete positional ``openorc.tasks`` row consumed by
the Task repository mapping in :mod:`openorc.persistence.tasks` (the
``_TASK_COLUMNS`` projection and the ``_task_from_row`` conversion). Ordinary
service and worker tests previously duplicated that 18-column positional
tuple layout in local helpers, so every Task-column addition forced a
mechanical edit across dozens of unrelated modules. Building rows here from
one shared ordered field list removes that fixture-shape coupling: per-test
differences are passed by named field overrides instead of positional
position.

Faithfulness and independence guarantees:

- Mirrors, but deliberately does not import, the production projection: the
  field order below must match ``_TASK_COLUMNS``/``_task_from_row`` in
  ``src/openorc/persistence/tasks.py``. Task persistence-mapping tests keep
  their own hand-written rows so column order, nullable handling, and the
  mapped ``Task`` attributes stay independently verified of this builder
  (issue #312).
- Coherent pre-Start defaults (issue #233): the default row is a
  never-started Task — ``ready_to_plan`` status with NO frozen
  ``source_requirements_fingerprint``. Owner Start freezes the freshly
  verified fingerprint atomically with the ``ready_to_plan`` transition, so
  pure default construction never simulates an incoherent subject. Started
  or later scenarios pass explicit ``status`` and
  ``source_requirements_fingerprint`` overrides.
- Strict override names: every current Task field is a named override and
  unknown names raise ``TypeError``, so a stale convenience name (for
  example ``canonical_branch`` instead of ``canonical_feature_branch``)
  fails clearly instead of being silently ignored.
- Test-only: this is not a production API, not a row-schema or
  fake-database framework, and carries no workflow semantics. It is
  deterministic and offline; identity defaults are fresh ``uuid4()`` values
  per call, exactly like the local helpers it replaces.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

_TASK_ROW_FIELDS: tuple[str, ...] = (
    "id",
    "workspace_id",
    "repository_id",
    "github_issue_id",
    "github_issue_number",
    "status",
    "archived_at",
    "canonical_feature_branch",
    "state_token",
    "current_plan_revision_id",
    "current_owner_gate_id",
    "source_requirements_fingerprint",
    "queued_at",
    "created_at",
    "updated_at",
    "terminal_cause",
    "authorized_task_budget_usd",
    "effective_cost_threshold_usd",
)

_DEFAULT_OBSERVED_AT = datetime(2026, 1, 1, 12, 0, 0, tzinfo=UTC)


def task_row(**overrides: Any) -> tuple[Any, ...]:
    """Build one full 18-column ``openorc.tasks`` repository row.

    The returned tuple is positionally aligned with the canonical Task
    projection (``_TASK_COLUMNS`` in ``openorc.persistence.tasks``), which
    is the row shape every ``select``/``returning`` on ``openorc.tasks``
    feeds into ``_task_from_row``. Defaults form a coherent pre-Start Task:
    ``ready_to_plan`` status with no frozen
    ``source_requirements_fingerprint`` (issue #233). Pass named overrides
    for every differing field; unknown override names raise ``TypeError``.

    Convenience names used by former local helpers (``gate_id``,
    ``canonical_branch``, ``state_fingerprint``, ...) are deliberately not
    accepted here; module-local wrappers translate them to the canonical
    field names so call sites stay unchanged.

    Task persistence-mapping tests must NOT build their rows with this
    builder: they keep hand-written rows so that column order, nullable
    values, and UTC normalization stay independently verified.
    """
    unknown = set(overrides) - set(_TASK_ROW_FIELDS)
    if unknown:
        raise TypeError(
            "unknown task_row override(s): "
            + ", ".join(sorted(unknown))
            + f"; valid fields: {', '.join(_TASK_ROW_FIELDS)}"
        )
    values: dict[str, Any] = {
        "id": uuid4(),
        "workspace_id": uuid4(),
        "repository_id": uuid4(),
        "github_issue_id": 9001,
        "github_issue_number": 42,
        "status": "ready_to_plan",
        "archived_at": None,
        "canonical_feature_branch": None,
        "state_token": uuid4(),
        "current_plan_revision_id": None,
        "current_owner_gate_id": None,
        "source_requirements_fingerprint": None,
        "queued_at": None,
        "created_at": _DEFAULT_OBSERVED_AT,
        "updated_at": _DEFAULT_OBSERVED_AT,
        "terminal_cause": None,
        "authorized_task_budget_usd": None,
        "effective_cost_threshold_usd": None,
    }
    values.update(overrides)
    return tuple(values[field] for field in _TASK_ROW_FIELDS)
