"""Convention tests for the OwnerGate settled-status migration (issue #211).

The ordinary suite cannot execute Postgres. These tests assert only the
durable, architectural properties of the committed migration that runtime
behavior does not naturally establish: the terminal ``settled`` status
joins the status vocabulary gated to ``review_resolution`` gates only, the
decided-at coherence keeps the terminal stamp non-NULL exactly when the
status is not pending (settled included, re-added under an explicit name
instead of an auto-generated one), the drop targets are derived from the
committed creating migration rather than mirrored from prose, and the
deliberate absences (no new columns, tables, indexes, policies, or
grants — an additive two-constraint replacement and nothing else).
Behavioral invariants are proven against a real database by the
integration-marked suite in ``tests/integration/``.
"""

from __future__ import annotations

import re
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
MIGRATIONS_DIR = REPO_ROOT / "supabase" / "migrations"

OWNER_GATES_TABLE = "openorc.owner_gates"


def _migration_text_raw() -> str:
    matches = sorted(
        p.name for p in MIGRATIONS_DIR.glob("*_allow_settled_review_resolution_gate_status.sql")
    )
    assert len(matches) == 1, (
        f"expected exactly one allow_settled_review_resolution_gate_status migration, "
        f"found {matches}"
    )
    return (MIGRATIONS_DIR / matches[0]).read_text(encoding="utf-8")


def _gate_table_block() -> str:
    """The committed owner_gates table body from its creating migration.

    Lets the constraint-name tests derive Postgres's auto-generated names
    from the committed source instead of mirroring this migration's prose.
    """
    matches = sorted(
        p.name for p in MIGRATIONS_DIR.glob("*_create_gate_execution_request_block_tables.sql")
    )
    assert len(matches) == 1, (
        f"expected exactly one create_gate_execution_request_block_tables migration, "
        f"found {matches}"
    )
    creating_text = (MIGRATIONS_DIR / matches[0]).read_text(encoding="utf-8")
    match = re.search(
        rf"create table {re.escape(OWNER_GATES_TABLE)}\s*\((.*?)\);",
        creating_text.lower(),
        re.DOTALL,
    )
    assert match is not None, "expected a create table statement for openorc.owner_gates"
    return match.group(1)


def _collapsed(text: str) -> str:
    return re.sub(r"\s+", " ", text)


def test_the_dropped_owner_gates_check_names_match_the_creating_migration() -> None:
    block = _collapsed(_gate_table_block())
    # The status vocabulary is exactly one unnamed single-column CHECK on
    # status, so Postgres auto-named it ``owner_gates_status_check``.
    assert block.count("check (status in") == 1
    # Exactly five unnamed CHECK constraints exist in the creating block:
    # three column-level CHECKs (gate_type, status, subject_head_sha —
    # auto-named from their columns) plus exactly two unnamed table-level
    # CHECKs in creation order — the per-type subject coherence first
    # (auto-named ``owner_gates_check``, already replaced by the settled
    # subject-coherence migration) and the decided_at coherence second
    # (auto-named ``owner_gates_check1``).
    assert block.count("check (") == 5
    assert block.count("check (gate_type in") == 1
    assert block.count("check (subject_head_sha is null") == 1
    subject_index = block.index("gate_type = 'implementation_authorization'")
    decided_index = block.index("status = 'pending' and decided_at is null")
    assert subject_index < decided_index
    # The decided-at coherence is the final unnamed CHECK in the block,
    # so it carries the second table-level auto name.
    assert block.rindex("check (") < decided_index
    raw = _collapsed(_migration_text_raw())
    assert "drop constraint owner_gates_status_check;" in raw
    assert "drop constraint owner_gates_check1;" in raw


def test_settled_joins_the_status_vocabulary_only_for_review_resolution() -> None:
    raw = _collapsed(_migration_text_raw())
    assert "add constraint owner_gates_status_check" in raw
    block = re.search(
        r"add constraint owner_gates_status_check\s*check \((.*?)\);",
        raw,
        re.DOTALL,
    )
    assert block is not None
    status_rule = _collapsed(block.group(1))
    # The four Owner-driven statuses stay free-standing.
    assert "status in ('pending', 'approved', 'rejected', 'cancelled')" in status_rule
    # ``settled`` is never free-floating: it appears exactly once, gated
    # to the review_resolution gate type.
    assert "or (status = 'settled' and gate_type = 'review_resolution')" in status_rule
    assert status_rule.count("settled") == 1


def test_the_decided_at_coherence_keeps_the_terminal_stamp_rule() -> None:
    raw = _collapsed(_migration_text_raw())
    assert "add constraint owner_gates_decided_at_coherence_check" in raw
    block = re.search(
        r"add constraint owner_gates_decided_at_coherence_check\s*check \((.*?)\);",
        raw,
        re.DOTALL,
    )
    assert block is not None
    coherence = _collapsed(block.group(1))
    # The terminal stamp is non-NULL exactly when the status is not
    # pending, settled included.
    assert "(status = 'pending' and decided_at is null)" in coherence
    assert (
        "status in ('approved', 'rejected', 'cancelled', 'settled') and decided_at is not null"
    ) in coherence


def test_the_migration_changes_nothing_else() -> None:
    raw = _collapsed(_migration_text_raw())
    lowered = raw.lower()
    # Additive constraint replacement only: no new columns, tables,
    # indexes, policies, or grants; no other table touched.
    assert "create table" not in lowered
    assert "create index" not in lowered
    assert "create policy" not in lowered
    assert "grant" not in lowered
    assert "add column" not in lowered
    assert raw.count("alter table openorc.owner_gates") == 4
    assert "openorc.tasks" not in lowered
    assert "openorc.workflow_events" not in lowered
