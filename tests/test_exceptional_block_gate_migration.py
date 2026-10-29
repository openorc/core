"""Convention tests for the exceptional block-gate migration (issue #258).

The ordinary suite cannot execute Postgres. These tests assert only the
durable, architectural properties of the committed migration that runtime
behavior does not naturally establish: the seven-type gate vocabulary, the
TaskBlock-only subject form for the three exceptional types with the
ordinary exact-subject rules (and the Reviewer-only ``settled`` status)
unchanged, the same-Task/Workspace composite foreign key on a new unique
identity key of ``task_blocks``, and the deliberate absences (no global
``BLOCKED`` invariant, no uniqueness over unresolved blocks or pending
historical gates, no historical backfill, no new tables/indexes/policies/
grants). Behavioral invariants are proven against a real database by the
integration-marked suite in ``tests/integration/``.
"""

from __future__ import annotations

import re
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
MIGRATIONS_DIR = REPO_ROOT / "supabase" / "migrations"

OWNER_GATES_TABLE = "openorc.owner_gates"
TASK_BLOCKS_TABLE = "openorc.task_blocks"

_EXCEPTIONAL_TYPES = ("block_resolution", "github_issue_change", "github_state_mismatch")
_ORDINARY_TYPES = (
    "implementation_authorization",
    "pr_authorization",
    "merge_decision",
    "review_resolution",
)


def _migration_text_raw() -> str:
    matches = sorted(p.name for p in MIGRATIONS_DIR.glob("*_add_exceptional_block_gates.sql"))
    assert len(matches) == 1, (
        f"expected exactly one add_exceptional_block_gates migration, found {matches}"
    )
    return (MIGRATIONS_DIR / matches[0]).read_text(encoding="utf-8")


def _collapsed(text: str) -> str:
    return re.sub(r"\s+", " ", text)


def _constraint_block(constraint: str) -> str:
    raw = _collapsed(_migration_text_raw())
    block = re.search(rf"add constraint {constraint}\s*check \((.*?)\);", raw, re.DOTALL)
    assert block is not None, f"expected an add-constraint CHECK for {constraint}"
    return _collapsed(block.group(1))


def test_the_task_block_subject_column_is_added() -> None:
    raw = _collapsed(_migration_text_raw())
    assert "alter table openorc.owner_gates add column task_block_id uuid;" in raw
    # Exactly one new column, on the gate table only.
    assert raw.count("add column") == 1


def test_the_gate_type_vocabulary_is_exactly_the_seven_settled_types() -> None:
    raw = _collapsed(_migration_text_raw())
    # The single-column CHECK was auto-named at table creation; the
    # replacement re-uses that name (the settled-status migration pattern).
    assert "drop constraint owner_gates_gate_type_check;" in raw
    block = re.search(
        r"add constraint owner_gates_gate_type_check\s*check \(gate_type in \((.*?)\)\);",
        raw,
        re.DOTALL,
    )
    assert block is not None
    vocabulary = _collapsed(block.group(1))
    assert set(re.findall(r"'([a-z_]+)'", vocabulary)) == set(_ORDINARY_TYPES + _EXCEPTIONAL_TYPES)


def test_the_subject_coherence_keeps_the_ordinary_rules_and_adds_the_block_subject() -> None:
    raw = _collapsed(_migration_text_raw())
    assert "drop constraint owner_gates_subject_coherence_check;" in raw
    coherence = _constraint_block("owner_gates_subject_coherence_check")
    # The four ordinary branches keep their exact subject forms and gain
    # the explicit task_block_id-is-null requirement.
    assert (
        "gate_type = 'implementation_authorization' and plan_revision_id is not null "
        "and subject_head_sha is null and task_pull_request_id is null "
        "and task_block_id is null"
    ) in coherence
    assert (
        "gate_type = 'pr_authorization' and plan_revision_id is null "
        "and subject_head_sha is not null and task_pull_request_id is null "
        "and task_block_id is null"
    ) in coherence
    assert (
        "gate_type = 'merge_decision' and plan_revision_id is null "
        "and subject_head_sha is not null and task_pull_request_id is not null "
        "and task_block_id is null"
    ) in coherence
    review_resolution = re.search(
        r"gate_type = 'review_resolution' and task_block_id is null and \((.*?)\)",
        coherence,
        re.DOTALL,
    )
    assert review_resolution is not None
    # The review_resolution branch keeps exactly one subject form.
    assert (
        "gate_type = 'review_resolution' and task_block_id is null and "
        "( ( plan_revision_id is not null and subject_head_sha is null "
        "and task_pull_request_id is null ) or ( plan_revision_id is null "
        "and subject_head_sha is not null and task_pull_request_id is not null ) )"
    ) in coherence
    # The three exceptional branches bind exactly one TaskBlock subject.
    for gate_type in _EXCEPTIONAL_TYPES:
        branch = re.search(
            rf"gate_type = '{gate_type}' and plan_revision_id is null "
            r"and subject_head_sha is null and task_pull_request_id is null "
            r"and task_block_id is not null",
            coherence,
        )
        assert branch is not None, f"expected the {gate_type} TaskBlock-subject branch"


def test_the_settled_status_rule_is_untouched() -> None:
    # The Reviewer-only ``settled`` status stays exactly as the issue #211
    # migration established it: no drop or re-add of the status CHECK here.
    raw = _collapsed(_migration_text_raw())
    assert "owner_gates_status_check" not in raw
    assert "owner_gates_decided_at_coherence_check" not in raw
    assert "'settled'" not in raw


def test_the_task_block_subject_is_the_same_task_and_workspace() -> None:
    raw = _collapsed(_migration_text_raw())
    # The referenced-side identity key on task_blocks.
    assert "alter table openorc.task_blocks" in raw
    assert (
        "add constraint task_blocks_id_task_id_workspace_id_uniq "
        "unique (id, task_id, workspace_id);"
    ) in raw
    # The composite foreign key on the gate's TaskBlock subject, classified
    # as within-Task-aggregate linkage (deferrable, never a cascade).
    assert (
        "add constraint owner_gates_task_block_fk "
        "foreign key (task_block_id, task_id, workspace_id) "
        "references openorc.task_blocks (id, task_id, workspace_id) "
        "deferrable initially deferred;"
    ) in raw


def test_the_migration_declares_no_global_blocked_invariant_or_backfill() -> None:
    raw = _collapsed(_migration_text_raw())
    lowered = raw.lower()
    # The #250 cutover owns the universal invariant: no partial/uniqueness
    # enforcement over unresolved blocks or pending gates, and no
    # historical backfill of already-blocked Tasks.
    assert "create unique index" not in lowered
    assert "where resolved_at is null" not in lowered
    assert "where status = 'pending'" not in lowered
    assert "update openorc" not in lowered
    assert "insert into" not in lowered
    # Additive constraint replacement only: no new tables, indexes,
    # policies, or grants; no other table touched.
    assert "create table" not in lowered
    assert "create index" not in lowered
    assert "create policy" not in lowered
    # ``grant`` as a standalone SQL keyword only (comments may say "grants").
    assert re.search(r"\bgrant\b", lowered) is None
    touched = set(re.findall(r"alter table ([a-z_.]+)", lowered))
    assert touched == {OWNER_GATES_TABLE, TASK_BLOCKS_TABLE}
