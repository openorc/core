"""Convention tests for the role-binding runtime configuration migration (#162).

The ordinary suite cannot execute Postgres. These tests assert only the
durable, architectural properties of the committed additive migration that
runtime behavior does not naturally establish: three nullable configuration
columns with no invented defaults, the provider/model pair and nonblank
integrity constraints, and the absences the contract forbids (no provider
catalog/enum, no generic runtime-configuration JSON bag, no prompt
blankness rule, no privileges). Behavioral invariants are proven against a
real database by the integration-marked suite in ``tests/integration/``.
"""

from __future__ import annotations

from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
MIGRATIONS_DIR = REPO_ROOT / "supabase" / "migrations"
BINDINGS_TABLE = "openorc.workflow_role_bindings"


def _migration_text() -> str:
    matches = sorted(
        p.name for p in MIGRATIONS_DIR.glob("*_add_role_binding_runtime_configuration.sql")
    )
    assert len(matches) == 1, (
        f"expected exactly one add_role_binding_runtime_configuration migration, found {matches}"
    )
    return (MIGRATIONS_DIR / matches[0]).read_text(encoding="utf-8")


def test_migration_extends_the_role_bindings_table() -> None:
    text = _migration_text()
    assert f"alter table {BINDINGS_TABLE}" in text
    assert "add column configured_provider text" in text
    assert "add column configured_model text" in text
    assert "add column role_prompt_override text" in text


def test_new_columns_are_nullable_with_no_invented_defaults() -> None:
    text = _migration_text()
    for column in ("configured_provider", "configured_model", "role_prompt_override"):
        declaration = f"add column {column} text"
        assert declaration in text
        assert declaration + " not null" not in text
        assert declaration + " default" not in text
    # Exactly three additive columns: no other column is added, and no
    # existing column is altered with an invented default.
    assert text.count("add column") == 3
    assert "alter column" not in text
    assert "set default" not in text


def test_provider_model_pair_completeness_is_durable() -> None:
    # The pair is complete only when both values are present: a partial
    # one-value configuration cannot be committed.
    text = _migration_text()
    assert "workflow_role_bindings_provider_model_pair_check" in text
    assert "check ((configured_provider is null) = (configured_model is null))" in text


def test_configured_identifiers_must_be_nonblank_when_present() -> None:
    # Opaque Owner-supplied strings: NULL-or-nonblank, mirroring the
    # durable check pattern of the Connection's auth_reference.
    text = _migration_text()
    assert "workflow_role_bindings_provider_nonblank_check" in text
    assert "workflow_role_bindings_model_nonblank_check" in text
    assert "check (configured_provider is null or configured_provider ~ '\\S')" in text
    assert "check (configured_model is null or configured_model ~ '\\S')" in text


def test_role_prompt_override_carries_no_blankness_rule() -> None:
    # Any non-NULL string — including an empty one — is the Owner's explicit
    # verbatim Markdown override; Core never parses or classifies the prose,
    # so no blankness/normalization constraint exists on this column.
    text = _migration_text()
    assert "role_prompt_override check" not in text
    assert "role_prompt_override ~" not in text


def test_no_catalog_enum_or_generic_configuration_bag() -> None:
    text = _migration_text()
    # No provider/model enums, no foreign catalog tables, and no generic
    # runtime-configuration JSON bag.
    assert "create table" not in text
    assert "create type" not in text
    assert "jsonb" not in text
    assert "references" not in text


def test_migration_never_references_supabase_managed_schemas_or_grants_roles() -> None:
    text = _migration_text()
    assert "auth." not in text
    assert "storage." not in text
    assert "grant" not in text
