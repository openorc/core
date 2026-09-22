"""Convention tests for the WorkflowEvent vocabulary extension migration (#56).

The ordinary suite cannot execute Postgres. These tests assert the durable,
architectural properties of the committed additive migration that runtime
behavior does not naturally establish: it extends the workflow-events
``event_type`` CHECK with exactly the one demonstrated
``workspace_configuration_changed`` value so the database CHECK vocabulary
is exactly the live Python enum, and it is purely additive — no deletes, no
data migration, no grants, no dynamic constraint discovery.
"""

from __future__ import annotations

import re
from pathlib import Path

from openorc.domain.events import WorkflowEventType

REPO_ROOT = Path(__file__).resolve().parents[1]
MIGRATIONS_DIR = REPO_ROOT / "supabase" / "migrations"

EXTENSION_MIGRATION_SUFFIX = "_add_workspace_configuration_changed_event_type.sql"


def _migration_text_raw() -> str:
    matches = sorted(p.name for p in MIGRATIONS_DIR.glob(f"*{EXTENSION_MIGRATION_SUFFIX}"))
    assert len(matches) == 1, (
        f"expected exactly one vocabulary extension migration, found {matches}"
    )
    return (MIGRATIONS_DIR / matches[0]).read_text(encoding="utf-8")


def test_the_extended_check_matches_the_live_vocabulary_exactly() -> None:
    match = re.search(
        r"add constraint workflow_events_event_type_check check \(event_type in \((.*?)\)\)",
        _migration_text_raw(),
        re.DOTALL,
    )
    assert match is not None, "expected the extended event_type CHECK"
    values = re.findall(r"'([a-z_]+)'", match.group(1))
    # The live lock after the extension: the database CHECK vocabulary is
    # exactly the current Python enum, value for value, in enum order.
    assert values == [member.value for member in WorkflowEventType]
    assert "workspace_configuration_changed" in values
    assert "prompt_override_changed" not in values


def test_the_extension_replaces_the_check_by_its_deterministic_name() -> None:
    text = _migration_text_raw().lower()
    # The same deterministic drop-by-exact-name pattern the #100 corrective
    # migration established: no dynamic catalog discovery, no substring
    # constraint matching.
    assert "drop constraint workflow_events_event_type_check" in text
    assert "do $$" not in text
    assert "pg_constraint" not in text


def test_the_extension_is_purely_additive() -> None:
    text = _migration_text_raw().lower()
    # Only the CHECK is replaced: no obsolete audit rows are deleted (the
    # extended value never existed before), no tables are touched, and no
    # grants are introduced.
    assert "delete from" not in text
    assert "drop table" not in text
    assert "grant " not in text
    assert "create type" not in text
