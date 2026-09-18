"""Convention tests for the prompt override / workflow event migration (#26).

The ordinary suite cannot execute Postgres. These tests assert only the
durable, architectural properties of the committed migration that runtime
behavior does not naturally establish: the instruction-only prompt
override slot (built-in defaults never materialized, reset = row
absence), the append-oriented audit stream (locked CHECK vocabularies
exactly matching the Python enums, direct Workspace scope with optional
agreeing Task scope, the pair-shaped foreign-key-free subject reference,
canonical-JSON-object context, no updated_at, no trigger), the exact
query-driven index set, and the deliberate absences (no grants, no
native enums, no speculative tables/columns). Behavioral invariants are
proven against a real database by the integration-marked suite in
``tests/integration/``.
"""

from __future__ import annotations

import re
from pathlib import Path

from openorc.domain.events import WorkflowEventActor, WorkflowEventType

REPO_ROOT = Path(__file__).resolve().parents[1]
MIGRATIONS_DIR = REPO_ROOT / "supabase" / "migrations"

PROMPT_OVERRIDES_TABLE = "openorc.prompt_template_overrides"
WORKFLOW_EVENTS_TABLE = "openorc.workflow_events"

EXCLUDED_EVENT_TYPES = (
    "task_archived",
    "canonical_branch_bound",
    "agent_session_ended",
    "task_pull_request_created",
    "task_pull_request_updated",
    "prompt_override_set",
    "prompt_override_reset",
)


def _migration_text_raw() -> str:
    matches = sorted(
        p.name
        for p in MIGRATIONS_DIR.glob("*_create_prompt_override_and_workflow_event_tables.sql")
    )
    assert len(matches) == 1, f"expected exactly one prompt/event migration, found {matches}"
    return (MIGRATIONS_DIR / matches[0]).read_text(encoding="utf-8")


def _migration_text() -> str:
    return _migration_text_raw().lower()


def _prose_text() -> str:
    """Lowercased migration prose with comment markers and wrapping removed."""
    stripped_lines = []
    for line in _migration_text_raw().splitlines():
        content = line.strip().lower()
        if content.startswith("--"):
            content = content[2:].strip()
        stripped_lines.append(content)
    return re.sub(r"\s+", " ", " ".join(stripped_lines))


def _table_block(table: str) -> str:
    """The create-table statement body for one openorc table (raw case)."""
    text = _migration_text_raw()
    match = re.search(rf"create table {re.escape(table)}\s*\((.*?)\);", text, re.DOTALL)
    assert match is not None, f"expected a create table statement for {table}"
    return match.group(1)


def _collapsed(block: str) -> str:
    return re.sub(r"\s+", " ", block)


def _check_values(block: str, column: str) -> list[str]:
    """Extract the quoted vocabulary values from a column's CHECK."""
    match = re.search(rf"{column} text not null check \({column} in \((.*?)\)\)", block, re.DOTALL)
    assert match is not None, f"expected a vocabulary CHECK for {column}"
    return re.findall(r"'([a-z_]+)'", match.group(1))


def test_migration_creates_both_tables() -> None:
    text = _migration_text()
    assert f"create table {PROMPT_OVERRIDES_TABLE}" in text
    assert f"create table {WORKFLOW_EVENTS_TABLE}" in text


def test_the_prompt_override_slot_stores_instruction_text_only() -> None:
    block = _collapsed(_table_block(PROMPT_OVERRIDES_TABLE))
    # The named override body is instruction text only — never a whole
    # prompt/protocol template column.
    assert "instruction_text text not null check (instruction_text ~ '\\S')" in block
    assert " template text" not in block
    assert " prompt_template text" not in block
    # The slot identity plus the built-in version the override was
    # authored against (audit reconstruction without copying defaults).
    assert "template_key text not null check (template_key ~ '\\S')" in block
    assert "base_template_version text not null check (base_template_version ~ '\\S')" in block
    assert "unique (workspace_id, template_key)" in block
    assert "created_at timestamptz not null default now()" in block
    assert "updated_at timestamptz not null default now()" in block


def test_built_in_defaults_are_never_materialized_into_the_table() -> None:
    prose = _prose_text()
    assert "built-in prompt defaults remain application code" in prose
    assert "row absence means the currently shipped built-in default applies" in prose
    assert "a reset is actual row deletion" in prose
    block = _collapsed(_table_block(PROMPT_OVERRIDES_TABLE))
    # No default-materialization columns exist: no is_default flag, no
    # built-in content column, no tombstone marker.
    assert "is_default" not in block
    assert "default_text" not in block
    assert "deleted_at" not in block
    # The deletion rationale is stated in the prose as the negation.
    assert "never a tombstone, never a stored default copy" in prose


def test_the_override_cannot_redefine_protocol_or_workflow_semantics() -> None:
    prose = _prose_text()
    assert "can never redefine protocol, authority, session, or workflow semantics" in prose


def test_event_type_check_matches_the_locked_domain_vocabulary_exactly() -> None:
    values = _check_values(_table_block(WORKFLOW_EVENTS_TABLE), "event_type")
    assert values == [member.value for member in WorkflowEventType]
    assert len(values) == 33
    # The excluded CRUD-ish names appear nowhere in the migration at all.
    raw_lower = _migration_text()
    for excluded in EXCLUDED_EVENT_TYPES:
        assert excluded not in raw_lower


def test_actor_check_is_the_locked_six_with_owner_and_without_human() -> None:
    values = _check_values(_table_block(WORKFLOW_EVENTS_TABLE), "actor_type")
    assert values == [member.value for member in WorkflowEventActor]
    assert values == ["owner", "openorc", "producer", "reviewer", "runtime", "github"]
    assert "'human'" not in _migration_text()
    prose = _prose_text()
    assert "owner is the human-authority actor terminology" in prose
    assert "human is not an openorc actor" in prose


def test_events_carry_direct_workspace_scope_with_optional_agreeing_task_scope() -> None:
    block = _collapsed(_table_block(WORKFLOW_EVENTS_TABLE))
    raw = re.sub(r"\s+", " ", _migration_text_raw())
    # workspace_id is NOT NULL; task_id is nullable (a Workspace-level
    # event legitimately has no Task).
    assert "workspace_id uuid not null references openorc.workspaces (id)" in block
    assert "task_id uuid," in block
    assert "task_id uuid not null" not in block
    # Task scope must agree with Workspace scope through the existing
    # tasks composite hook (added by the session migration).
    assert "foreign key (task_id, workspace_id) references openorc.tasks (id, workspace_id)" in raw


def test_the_subject_reference_is_a_pair_without_a_foreign_key() -> None:
    block = _collapsed(_table_block(WORKFLOW_EVENTS_TABLE))
    assert "subject_type text check (subject_type is null or subject_type ~ '\\S')" in block
    assert "subject_id uuid," in block
    # Present as a pair or absent as a pair.
    assert (
        "(subject_type is null and subject_id is null) "
        "or (subject_type is not null and subject_id is not null)" in block
    )
    # Exactly two foreign keys exist on the table: the Workspace reference
    # and the scope-agreement composite — the generic subject reference is
    # deliberately FK-free and there is no subject enum/type table.
    assert block.count("references openorc.") == 2
    raw_lower = _migration_text()
    assert "create type" not in raw_lower


def test_event_context_is_a_canonical_json_object() -> None:
    block = _collapsed(_table_block(WORKFLOW_EVENTS_TABLE))
    assert "context jsonb not null default '{}'::jsonb" in block
    assert "check (jsonb_typeof(context) = 'object')" in block
    prose = _prose_text()
    # Context is subordinate metadata, never a second home for domain state.
    assert "must not become the canonical home for modeled domain state" in prose
    assert "never a shadow copy of canonical domain records" in prose


def test_events_are_immutable_history_without_an_update_stamp_or_trigger() -> None:
    block = _collapsed(_table_block(WORKFLOW_EVENTS_TABLE))
    raw_lower = _migration_text()
    # Immutable rows: created_at is the only instant; no updated_at exists.
    assert "created_at timestamptz not null default now()" in block
    assert "updated_at" not in block
    # Append-only is owned by the persistence surface: no database
    # trigger, no correction/replacement machinery in the migration.
    assert "create trigger" not in raw_lower
    assert "for each row" not in raw_lower
    prose = _prose_text()
    assert "event rows are immutable after creation" in prose
    assert "not event sourcing and not a second source of workflow state" in prose
    assert "not runtime telemetry storage" in prose
    assert "insert-only public surface" in prose


def test_exactly_the_query_driven_indexes_exist() -> None:
    raw_lower = _migration_text()
    for index_name in (
        "workflow_events_workspace_activity_idx",
        "workflow_events_task_history_idx",
        "workflow_events_type_time_idx",
        "workflow_events_workspace_actor_idx",
        "workflow_events_subject_idx",
    ):
        assert index_name in raw_lower
    # Query-driven indexes only: exactly the five event read-path indexes,
    # none speculative on the prompt override slot (its unique constraint
    # is the lookup).
    assert raw_lower.count("create index") == 5
    assert "prompt_template_overrides" not in "".join(re.findall(r"create index[^;]*;", raw_lower))
    # The task-history index is partial on task scope, like the sibling
    # subject index.
    assert "on openorc.workflow_events (task_id, created_at)\n    where task_id is not null" in (
        _migration_text_raw()
    )


def test_no_grants_no_native_enums_and_no_speculative_tables() -> None:
    raw_lower = _migration_text()
    # The schema-level lockout carries isolation: no per-table grants.
    assert "grant " not in raw_lower
    # Vocabularies are text + CHECK, never native Postgres enums.
    assert "create type" not in raw_lower
    # The migration adds exactly the two settled tables and nothing else.
    tables = re.findall(r"create table (openorc\.[a-z_]+)", raw_lower)
    assert sorted(tables) == [PROMPT_OVERRIDES_TABLE, WORKFLOW_EVENTS_TABLE]
