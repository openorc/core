"""Convention tests for the WorkflowEvent vocabulary extension migrations.

The ordinary suite cannot execute Postgres. These tests assert the durable,
architectural properties of each committed additive vocabulary extension
migration: it extends the workflow-events ``event_type`` CHECK so the
database CHECK vocabulary is exactly the live Python enum, and it is purely
additive — no deletes, no data migration, no grants, no dynamic constraint
discovery.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

from openorc.domain.events import WorkflowEventType

REPO_ROOT = Path(__file__).resolve().parents[1]
MIGRATIONS_DIR = REPO_ROOT / "supabase" / "migrations"

EXTENSION_MIGRATION_SUFFIXES = (
    "_add_execution_cancelled_event_type.sql",
    "_add_task_started_event_type.sql",
)

# The vocabulary values later extension migrations appended after each one:
# an earlier extension's frozen CHECK is the live enum minus these values
# (the same live-lock-at-authoring-time rule the removal migration's test
# applies to the original historical vocabulary).
_LATER_EXTENSION_VALUES: dict[str, frozenset[str]] = {
    "_add_execution_cancelled_event_type.sql": frozenset({"task_started"}),
    "_add_task_started_event_type.sql": frozenset(),
}


def _migration_text_raw(extension_suffix: str) -> str:
    matches = sorted(p.name for p in MIGRATIONS_DIR.glob(f"*{extension_suffix}"))
    assert len(matches) == 1, (
        f"expected exactly one vocabulary extension migration, found {matches}"
    )
    return (MIGRATIONS_DIR / matches[0]).read_text(encoding="utf-8")


@pytest.mark.parametrize("extension_suffix", EXTENSION_MIGRATION_SUFFIXES)
def test_the_extended_check_matches_the_live_vocabulary_exactly(
    extension_suffix: str,
) -> None:
    match = re.search(
        r"add constraint workflow_events_event_type_check check \(event_type in \((.*?)\)\)",
        _migration_text_raw(extension_suffix),
        re.DOTALL,
    )
    assert match is not None, "expected the extended event_type CHECK"
    values = re.findall(r"'([a-z_]+)'", match.group(1))
    # Each extension is the live lock at its authoring time: its CHECK is
    # exactly the current Python enum minus the values later extensions
    # appended, value for value, in enum order. The latest extension's CHECK
    # is the live lock itself.
    later_extension_values = _LATER_EXTENSION_VALUES[extension_suffix]
    expected = [
        member.value for member in WorkflowEventType if member.value not in later_extension_values
    ]
    assert values == expected
    assert "workspace_configuration_changed" in values
    assert "prompt_override_changed" not in values


@pytest.mark.parametrize(
    ("extension_suffix", "extension_value"),
    [
        ("_add_execution_cancelled_event_type.sql", "execution_cancelled"),
        ("_add_task_started_event_type.sql", "task_started"),
    ],
)
def test_each_extension_carries_its_demonstrated_value(
    extension_suffix: str, extension_value: str
) -> None:
    match = re.search(
        r"add constraint workflow_events_event_type_check check \(event_type in \((.*?)\)\)",
        _migration_text_raw(extension_suffix),
        re.DOTALL,
    )
    assert match is not None
    values = re.findall(r"'([a-z_]+)'", match.group(1))
    # Each extension is the live lock at its authoring time, so the later
    # extensions' values legitimately follow; the earlier ones never claim
    # them. Each migration demonstrates exactly its own new value plus every
    # value that existed when it was authored.
    assert extension_value in values
    # The demonstrated value is appended among the lifecycle facts it
    # belongs to, never detached from the vocabulary it extends.
    assert values.index(extension_value) < len(values) - 1


@pytest.mark.parametrize("extension_suffix", EXTENSION_MIGRATION_SUFFIXES)
def test_the_extension_replaces_the_check_by_its_deterministic_name(
    extension_suffix: str,
) -> None:
    text = _migration_text_raw(extension_suffix).lower()
    # The same deterministic drop-by-exact-name pattern the #100 corrective
    # migration established: no dynamic catalog discovery, no substring
    # constraint matching.
    assert "drop constraint workflow_events_event_type_check" in text
    assert "do $$" not in text
    assert "pg_constraint" not in text


@pytest.mark.parametrize("extension_suffix", EXTENSION_MIGRATION_SUFFIXES)
def test_the_extension_is_purely_additive(extension_suffix: str) -> None:
    text = _migration_text_raw(extension_suffix).lower()
    # Only the CHECK is replaced: no obsolete audit rows are deleted (the
    # extended value never existed before), no tables are touched, and no
    # grants are introduced.
    assert "delete from" not in text
    assert "drop table" not in text
    assert "grant " not in text
    assert "create type" not in text
