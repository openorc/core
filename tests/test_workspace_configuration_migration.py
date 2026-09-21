"""Convention tests for the Workspace configuration migration (issue #53).

The ordinary suite cannot execute Postgres. These tests assert only the
durable, architectural properties of the committed additive migration that
runtime behavior does not naturally establish: typed first-class Workspace
settings with the configured-boundary default, the durable positivity
constraint, and the absence of any settings-bag, prompt-history, or
membership/RBAC machinery. Behavioral invariants (defaults, constraint
enforcement, round-trips, ReviewLoop history retention) are proven against a
real database by the integration-marked suite.
"""

from __future__ import annotations

import re
from pathlib import Path

from openorc.domain.reviews import DEFAULT_REVIEW_LOOP_ITERATION_LIMIT

REPO_ROOT = Path(__file__).resolve().parents[1]
MIGRATIONS_DIR = REPO_ROOT / "supabase" / "migrations"


def _configuration_sql() -> str:
    """Return the migration's SQL (comments stripped, normalized) lowercased."""
    matches = sorted(
        p.name for p in MIGRATIONS_DIR.glob("*_add_workspace_configuration_settings.sql")
    )
    assert len(matches) == 1, (
        f"expected exactly one add_workspace_configuration_settings migration, found {matches}"
    )
    raw = (MIGRATIONS_DIR / matches[0]).read_text(encoding="utf-8")
    statements = "\n".join(line for line in raw.splitlines() if not line.lstrip().startswith("--"))
    return " ".join(statements.lower().split())


def test_workspace_configuration_migration_is_additive_and_typed() -> None:
    sql = _configuration_sql()

    # Two additive typed columns on the existing workspaces table; no new
    # table, no drops, no JSON settings bag, no prompt/template machinery.
    assert sql.count("alter table openorc.workspaces") == 2
    assert (
        "add column review_iteration_limit integer not null default 5 "
        "check (review_iteration_limit > 0)" in sql
    )
    assert "add column guidance text not null default ''" in sql
    for stray in ("create table", "drop ", "grant", "jsonb", "prompt", "membership", "invite"):
        assert stray not in sql


def test_review_iteration_limit_default_is_the_configured_boundary_value() -> None:
    sql = _configuration_sql()

    match = re.search(r"review_iteration_limit integer not null default (\d+)", sql)
    assert match is not None
    # The durable default IS the v1 configured boundary: the backfill value
    # for existing Workspaces and the value for every future insert that
    # omits the column.
    assert int(match.group(1)) == DEFAULT_REVIEW_LOOP_ITERATION_LIMIT == 5


def test_guidance_has_no_history_or_protocol_semantics() -> None:
    sql = _configuration_sql()

    # Guidance is one current text value: no version/hash/snapshot/revision
    # column and no template-key counterpart exists anywhere in the change.
    for stray in ("hash", "version", "snapshot", "revision", "template"):
        assert stray not in sql
