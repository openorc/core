"""Convention tests for the Task agent session migration (issue #22).

The ordinary suite cannot execute Postgres. These tests assert only the
durable, architectural properties of the committed migration that runtime
behavior does not naturally establish: the one-binding-per-Task/role unique
constraint, the Connection-scoped external-session identity non-reuse index,
the initialization-coherence and ``ended_at`` CHECKs, the composite foreign
keys (and the tasks hook they need), the lifecycle and provenance vocabulary
shapes, the documented non-secret snapshot rule, the query-driven indexes,
and the deliberate absences (no replacement machinery, no runtime
health/telemetry, no conversational content, no native enums).
Behavioral invariants are proven against a real database by the
integration-marked suite in ``tests/integration/``.
"""

from __future__ import annotations

import re
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
MIGRATIONS_DIR = REPO_ROOT / "supabase" / "migrations"

SESSIONS_TABLE = "openorc.task_agent_sessions"


def _migration_text() -> str:
    return _migration_text_raw().lower()


def _prose_text() -> str:
    """Lowercased migration prose with comment markers and wrapping removed.

    Strips each line's leading whitespace and ``--`` marker, then collapses
    all whitespace, so prose assertions match across the migration's comment
    line wrapping.
    """
    stripped_lines = []
    for line in _migration_text_raw().splitlines():
        content = line.strip().lower()
        if content.startswith("--"):
            content = content[2:].strip()
        stripped_lines.append(content)
    return re.sub(r"\s+", " ", " ".join(stripped_lines))


def _migration_text_raw() -> str:
    matches = sorted(p.name for p in MIGRATIONS_DIR.glob("*_create_task_agent_sessions.sql"))
    assert len(matches) == 1, (
        f"expected exactly one create_task_agent_sessions migration, found {matches}"
    )
    return (MIGRATIONS_DIR / matches[0]).read_text(encoding="utf-8")


def _table_block(text: str, table: str) -> str:
    """Return the create-table statement body for one openorc table."""
    match = re.search(rf"create table {re.escape(table)}\s*\((.*?)\);", text, re.DOTALL)
    assert match is not None, f"expected a create table statement for {table}"
    return match.group(1)


def test_migration_creates_the_task_agent_sessions_table() -> None:
    text = _migration_text()
    assert f"create table {SESSIONS_TABLE}" in text


def test_role_check_vocabulary_is_producer_and_reviewer() -> None:
    block = _table_block(_migration_text(), SESSIONS_TABLE)
    assert "check (role in ('producer', 'reviewer'))" in block


def test_lifecycle_check_enumerates_exactly_the_four_settled_states() -> None:
    block = _table_block(_migration_text(), SESSIONS_TABLE)
    for value in ("connecting", "ready", "lost", "ended"):
        assert f"'{value}'" in block, f"lifecycle check is missing '{value}'"


def test_durable_checks_mirror_domain_validation() -> None:
    # Raw text: lowercasing would corrupt the '\S' regex literals. The checks
    # make it impossible to commit binding state the domain rejects
    # afterwards: NULL-or-nonblank external session identity, NULL-or-nonblank
    # protocol version, and canonical JSON-object snapshot.
    block = _table_block(_migration_text_raw(), SESSIONS_TABLE)
    assert "check (external_session_id is null or external_session_id ~ '\\S')" in block
    assert (
        "initialization_protocol_version is null or initialization_protocol_version ~ '\\S'"
        in block
    )
    assert "check (jsonb_typeof(effective_config_snapshot) = 'object')" in block


def test_external_session_identity_column_is_nullable_until_initialization() -> None:
    block = _table_block(_migration_text(), SESSIONS_TABLE)
    assert "external_session_id text" in block
    assert "external_session_id text not null" not in block


def test_one_binding_per_task_role() -> None:
    block = _table_block(_migration_text(), SESSIONS_TABLE)
    assert "unique (task_id, role)" in block


def test_external_session_identity_is_non_reusable_within_a_connection() -> None:
    text = _migration_text()
    assert "create unique index task_agent_sessions_connection_external_session_uniq" in text
    assert "on openorc.task_agent_sessions (connection_id, external_session_id)" in text
    # Only initialized bindings (non-NULL identity) are constrained:
    # uninitialized bindings are unrestricted.
    assert "where external_session_id is not null" in text


def test_initialization_coherence_is_enforced_in_both_directions() -> None:
    # CONNECTING requires both NULL; READY/LOST require both non-NULL; ENDED
    # permits either coherent form. Mixed forms (one NULL, one non-NULL) match
    # no branch and are rejected for every lifecycle status.
    block = _table_block(_migration_text_raw(), SESSIONS_TABLE)
    assert "lifecycle_status = 'connecting'" in block
    assert "and external_session_id is null" in block
    assert "and initialized_at is null" in block
    assert "lifecycle_status in ('ready', 'lost')" in block
    assert "and external_session_id is not null" in block
    assert "and initialized_at is not null" in block
    assert "lifecycle_status = 'ended'" in block
    # Both ENDED forms are explicitly represented.
    assert "(external_session_id is null and initialized_at is null)" in block
    assert "(external_session_id is not null and initialized_at is not null)" in block


def test_ended_at_is_the_semantic_ended_timestamp() -> None:
    block = _table_block(_migration_text_raw(), SESSIONS_TABLE)
    assert "ended_at timestamptz" in block
    assert "(lifecycle_status = 'ended' and ended_at is not null)" in block
    assert "(lifecycle_status <> 'ended' and ended_at is null)" in block


def test_instants_are_timestamptz() -> None:
    block = _table_block(_migration_text(), SESSIONS_TABLE)
    assert "initialized_at timestamptz" in block
    assert "created_at timestamptz not null default now()" in block
    assert "updated_at timestamptz not null default now()" in block


def test_reported_provenance_is_nullable_opaque_strings() -> None:
    block = _table_block(_migration_text(), SESSIONS_TABLE)
    for column in ("reported_provider", "reported_model", "reported_runtime_version"):
        assert f"{column} text," in block
        assert f"{column} text not null" not in block
        # They are observations, never enum-constrained configuration.
        assert f"{column} text check" not in block


def test_workspace_scope_composite_foreign_keys_with_tasks_hook() -> None:
    text = _migration_text()
    block = _table_block(text, SESSIONS_TABLE)
    # Direct Workspace scope must agree with the Task's Workspace and the
    # bound Connection's Workspace.
    assert "foreign key (task_id, workspace_id)" in block
    assert "references openorc.tasks (id, workspace_id)" in block
    assert "foreign key (connection_id, workspace_id)" in block
    assert "references openorc.connections (id, workspace_id)" in block
    # tasks gains the (id, workspace_id) hook this composite FK needs.
    assert "alter table openorc.tasks" in text
    assert "unique (id, workspace_id)" in text


def test_query_driven_indexes() -> None:
    text = _migration_text()
    # Connection capacity accounting: active occupancy per Connection.
    assert "create index task_agent_sessions_connection_active_idx" in text
    assert "on openorc.task_agent_sessions (connection_id)" in text
    # Active session queries across a Workspace.
    assert "create index task_agent_sessions_workspace_active_idx" in text
    assert "on openorc.task_agent_sessions (workspace_id)" in text
    # Both partial indexes only cover active (CONNECTING or READY) bindings,
    # and the (task_id, role) unique constraint doubles as Task-role lookup.
    assert text.count("where lifecycle_status in ('connecting', 'ready')") >= 2
    # No speculative capacity-integer index: admission compares the
    # Connection's configured capacity against occupied sessions.
    assert "session_capacity_idx" not in text


def test_non_secret_snapshot_rule_is_documented_in_the_migration() -> None:
    # The snapshot is caller-assembled non-secret configuration: never a
    # blind serialization of Connection configuration or authentication
    # material, and raw credentials/tokens must never enter the column.
    # (Whitespace-normalized so the assertions match across comment wrapping.)
    prose = _prose_text()
    assert "non-secret effective runtime/session" in prose
    assert (
        "never be populated by blindly serializing connection configuration or "
        "authentication material" in prose
    )
    assert "raw credentials/tokens must never enter this column" in prose


def test_deliberate_absences() -> None:
    text = _migration_text()
    # No replacement machinery: a successor/replacement pointer would defeat
    # the no-replacement invariant.
    for forbidden in ("superseded", "replaced_by", "replacement_session"):
        assert forbidden not in text
    # No runtime health/telemetry columns: reachability and Hub state are
    # separate telemetry, never durable workflow state.
    for forbidden in ("runtime_health", "hub_status", "last_seen", "is_reachable"):
        assert forbidden not in text
    # No conversational content columns: runtime context is never workflow
    # state.
    for forbidden in ("transcript", "messages", "conversation_history"):
        assert forbidden not in text
    # Workflow/type vocabularies use text + CHECK, never native enums.
    assert "as enum" not in text
    # No references to Supabase-managed schemas.
    assert "auth." not in text
    assert "storage." not in text


def test_migration_introduces_no_role_privileges() -> None:
    assert "grant" not in _migration_text()
