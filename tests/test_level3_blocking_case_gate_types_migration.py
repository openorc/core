"""Convention tests for the Level 3 blocking-case gate types migration (#286).

The ordinary suite cannot execute Postgres. These tests assert only the
durable, architectural properties of the committed migration that runtime
behavior does not naturally establish: the widened gate-type and
subject-coherence CHECK vocabularies, the re-declared F7c blocked-task gate
invariant with the widened exceptional-type list and an otherwise identical
assertion body, and the absence of data conversion, grants, or new
tables/columns. Behavioral invariants are proven against a real database by
the integration-marked F3 and F7c persistence suites.
"""

from __future__ import annotations

import re
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
MIGRATIONS_DIR = REPO_ROOT / "supabase" / "migrations"

_F7C_TYPE_LIST = "'block_resolution', 'github_issue_change', 'github_state_mismatch'"
_F8A_TYPE_LIST = (
    "'block_resolution', 'github_issue_change', 'github_state_mismatch',\n"
    "            'cost_limit', 'owner_abort'"
)


def _migration_text_raw() -> str:
    matches = sorted(
        p.name for p in MIGRATIONS_DIR.glob("*_add_level3_blocking_case_gate_types.sql")
    )
    assert len(matches) == 1, (
        f"expected exactly one add_level3_blocking_case_gate_types migration, found {matches}"
    )
    return (MIGRATIONS_DIR / matches[0]).read_text(encoding="utf-8")


def _f7c_migration_text_raw() -> str:
    matches = sorted(p.name for p in MIGRATIONS_DIR.glob("*_add_blocked_task_gate_invariant.sql"))
    assert len(matches) == 1, (
        f"expected exactly one add_blocked_task_gate_invariant migration, found {matches}"
    )
    return (MIGRATIONS_DIR / matches[0]).read_text(encoding="utf-8")


def _collapsed(text: str) -> str:
    return re.sub(r"\s+", " ", text)


def _assertion_body(text: str, *, widened: bool) -> str:
    function_block = re.search(
        r"create (?:or replace )?function openorc\.assert_blocked_task_gate_invariant "
        r"\(p_task_id uuid\)(.*?)\$assert\$;",
        text,
        re.DOTALL,
    )
    assert function_block is not None, "expected the shared assertion function"
    body = _collapsed(function_block.group(1))
    if widened:
        # Normalize the widened F8a type list back to the F7c three-type
        # list so the two bodies compare identical modulo the widening.
        body = body.replace(
            _collapsed(_F8A_TYPE_LIST),
            _collapsed(_F7C_TYPE_LIST),
        )
    return body


def test_the_type_check_vocabulary_widens_to_the_nine_settled_types() -> None:
    raw = _collapsed(_migration_text_raw())
    assert "drop constraint owner_gates_gate_type_check;" in raw
    match = re.search(
        r"add constraint owner_gates_gate_type_check\s*check \(gate_type in \((.*?)\)\);",
        raw,
        re.DOTALL,
    )
    assert match is not None
    vocabulary = {value.strip().strip("'") for value in match.group(1).split(",")}
    assert vocabulary == {
        "implementation_authorization",
        "pr_authorization",
        "merge_decision",
        "review_resolution",
        "block_resolution",
        "github_issue_change",
        "github_state_mismatch",
        "cost_limit",
        "owner_abort",
    }


def test_the_subject_coherence_check_binds_the_new_types_to_exactly_the_task_block() -> None:
    raw = _collapsed(_migration_text_raw())
    assert "drop constraint owner_gates_subject_coherence_check;" in raw
    match = re.search(
        r"add constraint owner_gates_subject_coherence_check\s*check \((.*?)\);",
        raw,
        re.DOTALL,
    )
    assert match is not None
    coherence = match.group(1)
    # Each branch segment runs from its ``gate_type = '<type>'`` marker to
    # the next branch marker (or the constraint's end for the last branch).
    segments = re.split(r"gate_type = '", coherence)[1:]
    assert {segment.split("'")[0] for segment in segments} == {
        "implementation_authorization",
        "pr_authorization",
        "merge_decision",
        "review_resolution",
        "block_resolution",
        "github_issue_change",
        "github_state_mismatch",
        "cost_limit",
        "owner_abort",
    }
    # Each new exceptional branch requires exactly the TaskBlock subject:
    # every other subject form NULL, task_block_id NOT NULL.
    for gate_type in ("cost_limit", "owner_abort"):
        branch = next(segment for segment in segments if segment.split("'")[0] == gate_type)
        assert "plan_revision_id is null" in branch
        assert "subject_head_sha is null" in branch
        assert "task_pull_request_id is null" in branch
        assert "task_block_id is not null" in branch


def test_the_invariant_assertion_is_redeclared_with_the_widened_exceptional_list() -> None:
    raw = _collapsed(_migration_text_raw())
    # The F7c assertion is replaced in place, not duplicated or dropped.
    assert "create or replace function openorc.assert_blocked_task_gate_invariant" in raw
    raw_body = _assertion_body(_migration_text_raw(), widened=False)
    for type_value in (
        "block_resolution",
        "github_issue_change",
        "github_state_mismatch",
        "cost_limit",
        "owner_abort",
    ):
        assert f"'{type_value}'" in raw_body
    # The trigger functions invoke the assertion by name and are not
    # re-created by this migration.
    assert "assert_blocked_task_gate_invariant_trigger" not in raw


def test_the_assertion_body_is_otherwise_identical_to_the_f7c_declaration() -> None:
    f7c_body = _assertion_body(_f7c_migration_text_raw(), widened=False)
    f8a_body = _assertion_body(_migration_text_raw(), widened=True)
    assert f8a_body == f7c_body, (
        "the widened assertion must be the F7c body with only the exceptional-type list widened"
    )


def test_the_migration_performs_no_data_conversion_and_creates_no_new_objects() -> None:
    raw = _collapsed(_migration_text_raw())
    # The Owner-approved scope (issue #282) applies equally: no historical
    # rows exist to convert, and no backfill or event insertion happens.
    assert "insert into" not in raw
    assert "update openorc" not in raw
    assert "delete from" not in raw
    assert "do $" not in raw
    # Least-privilege: no grants or policies; the existing revocations and
    # constraint triggers persist across the function replacement.
    assert not re.search(r"\bgrant\b", raw)
    assert "create policy" not in raw
    assert "create table" not in raw
    assert "add column" not in raw
