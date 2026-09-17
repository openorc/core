"""Convention tests for the Connection/role binding migration (issue #20).

The ordinary suite cannot execute Postgres. These tests assert only the
durable, architectural properties of the committed migration that runtime
behavior does not naturally establish. Behavioral invariants (ownership
foreign keys, one binding per role, Producer/Reviewer sharing/separating
Connections, capacity scoping, nullable reported observations) are proven
against a real database by the integration-marked suite in
``tests/integration/``.
"""

from __future__ import annotations

import re
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
MIGRATIONS_DIR = REPO_ROOT / "supabase" / "migrations"

CONNECTIONS_TABLE = "openorc.connections"
BINDINGS_TABLE = "openorc.workflow_role_bindings"


def _migration_text() -> str:
    return _migration_text_raw().lower()


def _migration_text_raw() -> str:
    matches = sorted(
        p.name for p in MIGRATIONS_DIR.glob("*_create_connection_and_role_bindings.sql")
    )
    assert len(matches) == 1, (
        f"expected exactly one create_connection_and_role_bindings migration, found {matches}"
    )
    return (MIGRATIONS_DIR / matches[0]).read_text(encoding="utf-8")


def _table_block(text: str, table: str) -> str:
    """Return the create-table statement body for one openorc table."""
    match = re.search(rf"create table {re.escape(table)}\s*\((.*?)\);", text, re.DOTALL)
    assert match is not None, f"expected a create table statement for {table}"
    return match.group(1)


def test_migration_creates_both_tables() -> None:
    text = _migration_text()
    assert f"create table {CONNECTIONS_TABLE}" in text
    assert f"create table {BINDINGS_TABLE}" in text


def test_session_capacity_defaults_to_one_with_owner_admission_check() -> None:
    # Concurrency must be explicitly enabled by the Owner, never assumed and
    # never discovered from the runtime.
    block = _table_block(_migration_text(), CONNECTIONS_TABLE)
    assert "session_capacity integer not null default 1" in block
    assert "check (session_capacity > 0)" in block


def test_enablement_is_a_boolean_eligibility_switch() -> None:
    # No active/disabled runtime-status vocabulary: Owner-controlled
    # eligibility is a plain boolean with a permitted default.
    text = _migration_text()
    block = _table_block(text, CONNECTIONS_TABLE)
    assert "enabled boolean not null default true" in block
    assert "auth_status" not in text
    assert "status text" not in block


def test_authentication_is_only_the_opaque_nullable_reference() -> None:
    block = _table_block(_migration_text(), CONNECTIONS_TABLE)
    assert "auth_reference text" in block
    assert "auth_reference text not null" not in block


def test_durable_checks_mirror_domain_validation() -> None:
    # Raw text: lowercasing would corrupt the '\S' regex literals. The checks
    # make it impossible to commit Connection state the domain rejects
    # afterwards: nonblank names, NULL-or-nonblank auth reference, and
    # canonical JSON-object safe_config.
    block = _table_block(_migration_text_raw(), CONNECTIONS_TABLE)
    assert "check (name ~ '\\S')" in block
    assert "check (auth_reference is null or auth_reference ~ '\\S')" in block
    assert "check (jsonb_typeof(safe_config) = 'object')" in block


def test_reported_provider_model_are_nullable_opaque_strings() -> None:
    block = _table_block(_migration_text(), CONNECTIONS_TABLE)
    assert "reported_provider text," in block
    assert "reported_model text," in block
    assert "reported_provider text not null" not in block
    assert "reported_model text not null" not in block
    # They are observations, never enum-constrained configuration.
    assert "reported_provider text check" not in block
    assert "reported_model text check" not in block


def test_binding_models_only_role_and_identity() -> None:
    # The v1 binding is honest: no per-role session configuration exists
    # (provider/model selection is intentionally unavailable; PLAN/ACT mode is
    # workflow-derived; initialization prompts are OpenOrc protocol behavior).
    block = _table_block(_migration_text(), BINDINGS_TABLE)
    for column in ("id", "workspace_id", "role", "connection_id", "created_at", "updated_at"):
        assert column in block
    for forbidden in (
        "jsonb",
        "safe_config",
        "session",
        "reported",
        "auth",
        "enabled",
        "provider",
        "model",
        "capacity",
    ):
        assert forbidden not in block


def test_binding_is_unique_per_workspace_role() -> None:
    block = _table_block(_migration_text(), BINDINGS_TABLE)
    assert "unique (workspace_id, role)" in block


def test_binding_scope_cannot_disagree_with_connection_workspace() -> None:
    text = _migration_text()
    binding_block = _table_block(text, BINDINGS_TABLE)
    connections_block = _table_block(text, CONNECTIONS_TABLE)
    assert "foreign key (connection_id, workspace_id)" in binding_block
    assert "references openorc.connections (id, workspace_id)" in binding_block
    assert "unique (id, workspace_id)" in connections_block


def test_role_check_vocabulary_is_producer_and_reviewer() -> None:
    block = _table_block(_migration_text(), BINDINGS_TABLE)
    assert "check (role in ('producer', 'reviewer'))" in block


def test_adapter_check_vocabulary_is_cline_only() -> None:
    block = _table_block(_migration_text(), CONNECTIONS_TABLE)
    assert "check (adapter_type = 'cline')" in block


def test_workspace_list_and_binding_lookup_indexes_exist() -> None:
    text = _migration_text()
    assert "connections_workspace_id_idx" in text
    assert "workflow_role_bindings_connection_id_idx" in text
    # No speculative capacity-integer index: capacity admission will compare
    # Connection capacity against occupied TaskAgentSessions, so an index on
    # the integer itself supports no demonstrated query path.
    assert "session_capacity_idx" not in text


def test_migration_never_references_supabase_managed_schemas() -> None:
    text = _migration_text()
    assert "auth." not in text
    assert "storage." not in text


def test_migration_introduces_no_role_privileges() -> None:
    assert "grant" not in _migration_text()
