"""Convention tests for the blocked-task gate invariant migration (issue #282).

The ordinary suite cannot execute Postgres. These tests assert only the
durable, architectural properties of the committed migration that runtime
behavior does not naturally establish: no data conversion at all (the
Owner-approved scope — OpenOrc develops against one empty application
database, so there is no historical BLOCKED history to classify or backfill),
the universal at-most-one-unresolved-TaskBlock backstop, the shared committed-
state assertion with its deliberately scoped converse exemption and a
version-compatible sole-block retrieval, and the three DEFERRABLE INITIALLY
DEFERRED constraint triggers covering Task, gate, and block mutations.
Behavioral invariants are proven against a real database by the integration-
marked suite ``tests/integration/test_blocked_task_gate_invariant_persistence.py``.
"""

from __future__ import annotations

import re
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
MIGRATIONS_DIR = REPO_ROOT / "supabase" / "migrations"


def _migration_text_raw() -> str:
    matches = sorted(p.name for p in MIGRATIONS_DIR.glob("*_add_blocked_task_gate_invariant.sql"))
    assert len(matches) == 1, (
        f"expected exactly one add_blocked_task_gate_invariant migration, found {matches}"
    )
    return (MIGRATIONS_DIR / matches[0]).read_text(encoding="utf-8")


def _collapsed(text: str) -> str:
    return re.sub(r"\s+", " ", text)


def _assertion_function_body() -> str:
    raw = _migration_text_raw()
    function_block = re.search(
        r"create function openorc\.assert_blocked_task_gate_invariant \(p_task_id uuid\)"
        r"(.*?)\$assert\$;",
        raw,
        re.DOTALL,
    )
    assert function_block is not None, "expected the shared assertion function"
    return _collapsed(function_block.group(1))


def _trigger_block(name: str) -> str:
    raw = _collapsed(_migration_text_raw())
    match = re.search(
        rf"create constraint trigger {name} .*?execute function [^;]+;",
        raw,
        re.DOTALL,
    )
    assert match is not None, f"expected the {name} constraint trigger"
    return match.group(0)


def test_the_migration_performs_no_data_conversion() -> None:
    raw = _collapsed(_migration_text_raw())
    # The Owner-approved scope (issue #282): OpenOrc develops against one
    # empty application database — the cutover creates enforcement machinery
    # only, with no historical classification, backfill, or event insertion.
    assert "insert into" not in raw
    assert "update openorc" not in raw
    assert "delete from" not in raw
    assert "do $" not in raw
    # No legacy-context classification machinery exists: the block-reason
    # vocabulary and producer context keys appear nowhere in the file.
    for legacy_marker in (
        "github_source_changed",
        "stale_operation",
        "external_operation_uncertain",
        "owner_action_required",
        "cost_limit",
        "owner_abort",
    ):
        assert legacy_marker not in raw


def test_the_unresolved_block_backstop_forbids_only_multiples() -> None:
    raw = _collapsed(_migration_text_raw())
    assert (
        "create unique index task_blocks_task_unresolved_uniq "
        "on openorc.task_blocks (task_id) where resolved_at is null" in raw
    )


def test_the_assertion_retrieves_the_sole_block_in_a_version_compatible_way() -> None:
    body = _assertion_function_body()
    # min/max on UUID values is unavailable on the released PostgreSQL
    # versions Supabase runs: the sole unresolved block identity comes from a
    # deterministic array_agg instead.
    assert "min(" not in body
    assert "max(" not in body


def test_the_shared_assertion_scopes_the_gate_contract_to_current_blocked_tasks() -> None:
    body = _assertion_function_body()

    # At most one unresolved TaskBlock for every Task, checked first.
    assert body.index("v_unresolved_count > 1") < body.index(
        "v_archived_at is not null or v_status <> 'blocked'"
    ), "the unresolved-block backstop must be evaluated for every Task"
    # The converse exemption: archived and non-blocked Tasks leave their gate
    # history untouched — the blocked-scope guard precedes every gate check.
    blocked_guard = body.index("v_archived_at is not null or v_status <> 'blocked'")
    for gate_check in (
        "has no current OwnerGate pointer",
        "is not a pending exceptional gate",
        "not bound to its sole unresolved task block",
    ):
        assert blocked_guard < body.index(gate_check), (
            f"the {gate_check!r} check must be scoped to current blocked Tasks"
        )
    # The gate contract itself: non-NULL pointer, pending, exceptional type,
    # and bound to exactly the sole unresolved block.
    assert "v_current_gate is null" in body
    assert "v_gate_status is distinct from 'pending'" in body
    assert "block_resolution', 'github_issue_change', 'github_state_mismatch" in body
    assert "v_gate_block is distinct from v_sole_block" in body


def test_the_trigger_wrapper_and_the_three_deferred_constraint_triggers() -> None:
    raw = _collapsed(_migration_text_raw())
    # PostgreSQL trigger functions take no declared arguments and return
    # trigger: the wrapper derives the Task identity per table and runs the
    # shared assertion.
    assert "create function openorc.assert_blocked_task_gate_invariant_trigger ()" in raw
    assert "returns trigger" in raw
    assert "perform openorc.assert_blocked_task_gate_invariant (v_task_id)" in raw

    tasks_trigger = _trigger_block("tasks_blocked_gate_invariant")
    assert "after insert or update of status, current_owner_gate_id, archived_at" in (tasks_trigger)
    gates_trigger = _trigger_block("owner_gates_blocked_gate_invariant")
    assert "after insert or update of status, gate_type, task_block_id or delete" in (gates_trigger)
    blocks_trigger = _trigger_block("task_blocks_blocked_gate_invariant")
    assert "after insert or update of resolved_at or delete" in blocks_trigger

    for name in (
        "tasks_blocked_gate_invariant",
        "owner_gates_blocked_gate_invariant",
        "task_blocks_blocked_gate_invariant",
    ):
        block = _trigger_block(name)
        # Committed-state validation on every affected Task, gate, and block
        # mutation: deferred, so the existing atomic multitable compositions
        # commit and their final state is what is validated.
        assert "deferrable initially deferred" in block
        assert "for each row" in block
        assert "execute function openorc.assert_blocked_task_gate_invariant_trigger ()" in block


def test_the_migration_grants_nothing_and_creates_no_new_tables_or_columns() -> None:
    raw = _collapsed(_migration_text_raw())
    # Least-privilege: no grants, no policies, and the internal functions are
    # revoked from PUBLIC.
    assert not re.search(r"\bgrant\b", raw)
    assert "create policy" not in raw
    assert "revoke execute on function openorc.assert_blocked_task_gate_invariant" in raw
    assert "revoke execute on function openorc.assert_blocked_task_gate_invariant_trigger ()" in raw
    # No new tables or columns: the invariant composes the existing schema.
    assert "create table" not in raw
    assert "add column" not in raw
