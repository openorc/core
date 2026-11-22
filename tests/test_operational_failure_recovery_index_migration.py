"""Convention tests for the F12 outstanding-retry index migration (issue #316).

The ordinary suite cannot execute Postgres. These tests assert only the
durable, architectural properties of the committed migration that runtime
behavior does not naturally establish: exactly one additive partial index
covering the bounded startup-recovery enumeration's filter, no second retry
queue/table, no ledger rewrite, and no new privilege grants. Index behavior
against a real database stays with the integration-marked suites.
"""

from __future__ import annotations

import re
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
MIGRATIONS_DIR = REPO_ROOT / "supabase" / "migrations"

_SUPPORTED_FAMILIES = (
    "implementation_execution",
    "exact_head_merge",
    "exact_head_pr_authorization_presentation",
)


def _migration_text_raw() -> str:
    matches = sorted(
        p.name for p in MIGRATIONS_DIR.glob("*_add_operational_failure_outstanding_retry_index.sql")
    )
    assert len(matches) == 1, (
        f"expected exactly one add_operational_failure_outstanding_retry_index "
        f"migration, found {matches}"
    )
    return (MIGRATIONS_DIR / matches[0]).read_text(encoding="utf-8")


def _migration_statements() -> list[str]:
    text = _migration_text_raw()
    statement_lines: list[str] = []
    current: list[str] = []
    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line or line.startswith("--"):
            continue
        current.append(line)
        if line.endswith(";"):
            statement_lines.append(" ".join(current))
            current = []
    assert not current, "every migration statement must be terminated with ';'"
    return statement_lines


def test_exactly_one_additive_partial_index_is_created() -> None:
    """The migration creates exactly the narrow recovery-lookup index."""
    statements = _migration_statements()
    create_statements = [s for s in statements if s.lower().startswith("create index")]
    assert len(create_statements) == 1
    index = create_statements[0]
    assert "operational_failure_episodes_outstanding_retry_idx" in index
    assert "on openorc.operational_failure_episodes" in index
    # The partial predicate matches exactly the recovery enumeration's
    # filter: non-escalated episodes in the three supported retry families.
    assert "where escalated_task_block_id is null" in index
    for family in _SUPPORTED_FAMILIES:
        assert f"'{family}'" in index


def test_no_second_retry_ledger_or_table_is_created() -> None:
    """No new table, no second queue, and no ledger mutation exists."""
    statements = _migration_statements()
    for statement in statements:
        lowered = statement.lower()
        assert not lowered.startswith(("create table", "alter table", "drop table"))
        assert not lowered.startswith(("update ", "insert into", "delete from"))
        assert "create unique index" not in lowered


def test_the_migration_grants_no_new_privileges() -> None:
    """The openorc schema lockout carries isolation; no grants are added."""
    text = _migration_text_raw()
    statements = _migration_statements()
    assert not [s for s in statements if re.match(r"(?i)^(grant|revoke)\b", s)]
    assert "grant" not in text.lower().replace("no new privilege grants", "")
