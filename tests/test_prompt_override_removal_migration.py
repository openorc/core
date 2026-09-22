"""Convention tests for the prompt-override removal corrective migration (#100).

The ordinary suite cannot execute Postgres. These tests assert the durable,
architectural properties of the committed corrective migration that runtime
behavior does not naturally establish: the obsolete
``openorc.prompt_template_overrides`` table is dropped with no replacement
prompt/override persistence, legacy ``prompt_override_changed`` audit rows are
deliberately deleted before the narrowed workflow-events vocabulary CHECK is
installed (the surviving values locked exactly to the live Python enum), the
session ``initialization_protocol_version`` column is dropped with no
substring/dynamic constraint discovery anywhere, and the explicitly named
three-fact initialization-coherence CHECK replaces the old four-fact one.
These are the current post-correction schema expectations; the original
migrations remain covered by their own frozen historical convention tests.
Behavioral invariants — including the upgrade path from a pre-correction
schema containing a ``prompt_override_changed`` row — are proven against a
real database by the integration-marked staged test in ``tests/integration/``.
"""

from __future__ import annotations

import re
from pathlib import Path

from openorc.domain.events import WorkflowEventType

REPO_ROOT = Path(__file__).resolve().parents[1]
MIGRATIONS_DIR = REPO_ROOT / "supabase" / "migrations"

REMOVAL_MIGRATION_SUFFIX = "_remove_prompt_overrides_and_initialization_protocol_version.sql"


def _migration_text_raw() -> str:
    matches = sorted(p.name for p in MIGRATIONS_DIR.glob(f"*{REMOVAL_MIGRATION_SUFFIX}"))
    assert len(matches) == 1, f"expected exactly one corrective removal migration, found {matches}"
    return (MIGRATIONS_DIR / matches[0]).read_text(encoding="utf-8")


def _migration_text() -> str:
    return _migration_text_raw().lower()


def test_the_obsolete_prompt_template_overrides_table_is_dropped() -> None:
    text = _migration_text()
    assert "drop table openorc.prompt_template_overrides;" in text
    # No replacement prompt/override persistence is introduced by this issue;
    # Phase 2 owns the new first-class Workspace configuration field.
    assert "create table openorc.prompt" not in text


def test_legacy_prompt_override_events_are_deleted_before_the_narrowed_check() -> None:
    text = _migration_text()
    # Deliberate pre-v1 policy: the obsolete abstraction is removed rather
    # than preserved, so obsolete audit rows of the removed event type are
    # deleted first — the narrowed CHECK must never fail an upgrade on
    # pre-existing data, and the delete must precede the CHECK replacement.
    delete_pos = text.index(
        "delete from openorc.workflow_events where event_type = 'prompt_override_changed';"
    )
    drop_pos = text.index("drop constraint workflow_events_event_type_check")
    assert delete_pos < drop_pos


def test_constraint_removal_is_explicit_and_deterministic() -> None:
    text = _migration_text()
    # The committed #26 migration defined the event_type CHECK inline on the
    # column, so Postgres assigned the deterministic auto-generated name; the
    # corrective migration drops and re-adds it explicitly. No dynamic
    # catalog discovery and no substring constraint matching exist.
    assert "drop constraint workflow_events_event_type_check" in text
    assert "do $$" not in text
    assert "pg_constraint" not in text


def test_the_narrowed_event_type_check_matches_the_live_vocabulary_exactly() -> None:
    match = re.search(
        r"add constraint workflow_events_event_type_check check \(event_type in \((.*?)\)\)",
        _migration_text_raw(),
        re.DOTALL,
    )
    assert match is not None, "expected the narrowed event_type CHECK"
    values = re.findall(r"'([a-z_]+)'", match.group(1))
    # The lock at this migration's point in history: the database CHECK
    # vocabulary is exactly the current Python enum minus the values later
    # demonstrated additive migrations extended it with (#56's
    # workspace_configuration_changed), value for value, with the removed
    # type gone.
    later_extension_values = {"workspace_configuration_changed"}
    assert values == [
        member.value for member in WorkflowEventType if member.value not in later_extension_values
    ]
    assert len(values) == 32
    assert "prompt_override_changed" not in values
    assert "workspace_configuration_changed" not in values


def test_the_initialization_protocol_version_column_is_dropped() -> None:
    text = _migration_text()
    assert "alter table openorc.task_agent_sessions" in text
    assert "drop column initialization_protocol_version" in text


def test_the_named_three_fact_coherence_check_replaces_the_old_one() -> None:
    text = _migration_text()
    assert "add constraint task_agent_sessions_initialization_coherence check (" in text
    # The dropped column name appears nowhere in the migration's executable
    # SQL except its deliberate drop statement (the header prose documents
    # the removal and is not SQL).
    sql_lines = [line for line in text.splitlines() if not line.lstrip().startswith("--")]
    sql_text = "\n".join(sql_lines)
    assert sql_text.count("initialization_protocol_version") == 1
    assert "drop column initialization_protocol_version" in sql_text
    match = re.search(
        r"add constraint task_agent_sessions_initialization_coherence check \((.*?)\);",
        _migration_text_raw(),
        re.DOTALL,
    )
    assert match is not None
    block = re.sub(r"\s+", " ", match.group(1))
    # Three-fact coherence: CONNECTING requires all three NULL; READY and
    # LOST require all three non-NULL; ENDED permits either coherent form.
    for fragment in (
        "lifecycle_status = 'connecting'",
        "lifecycle_status in ('ready', 'lost')",
        "lifecycle_status = 'ended'",
        "external_session_id is null",
        "initialized_at is null",
        "effective_config_snapshot is null",
        "external_session_id is not null",
        "initialized_at is not null",
        "effective_config_snapshot is not null",
    ):
        assert fragment in block


def test_no_grants_and_no_native_enums() -> None:
    text = _migration_text()
    assert "grant " not in text
    assert "create type" not in text
