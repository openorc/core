"""Convention tests for the planning and review migration (issue #23).

The ordinary suite cannot execute Postgres. These tests assert only the
durable, architectural properties of the committed migration that runtime
behavior does not naturally establish: the three tables, the settled
purpose/outcome/status vocabularies, the per-Task revision uniqueness, the
per-Loop iteration uniqueness, the composite foreign keys (including the
same-Task/Workspace current-plan pointer), the finalize-coherence and
Reviewer-result-coherence CHECKs, the findings JSON-array shape, the
query-driven indexes, and the deliberate absences (no wire-envelope fields,
no ``updated_at`` on immutable rows, no native enums, no speculative
indexes, no second PR-review history model, no rewrite of prior tables).
Behavioral invariants are proven against a real database by the
integration-marked suite in ``tests/integration/``.
"""

from __future__ import annotations

import re
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
MIGRATIONS_DIR = REPO_ROOT / "supabase" / "migrations"

PLAN_REVISIONS_TABLE = "openorc.plan_revisions"
REVIEW_LOOPS_TABLE = "openorc.review_loops"
REVIEW_ITERATIONS_TABLE = "openorc.review_iterations"


def _migration_text_raw() -> str:
    matches = sorted(p.name for p in MIGRATIONS_DIR.glob("*_create_plan_review_tables.sql"))
    assert len(matches) == 1, (
        f"expected exactly one create_plan_review_tables migration, found {matches}"
    )
    return (MIGRATIONS_DIR / matches[0]).read_text(encoding="utf-8")


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


def _table_block(text: str, table: str) -> str:
    """Return the create-table statement body for one openorc table."""
    match = re.search(rf"create table {re.escape(table)}\s*\((.*?)\);", text, re.DOTALL)
    assert match is not None, f"expected a create table statement for {table}"
    return match.group(1)


def _collapsed_raw_block(table: str) -> str:
    """The raw-case table body with all whitespace collapsed to single spaces."""
    return re.sub(r"\s+", " ", _table_block(_migration_text_raw(), table))


def test_migration_creates_the_three_planning_review_tables() -> None:
    text = _migration_text()
    for table in (PLAN_REVISIONS_TABLE, REVIEW_LOOPS_TABLE, REVIEW_ITERATIONS_TABLE):
        assert f"create table {table}" in text


def test_plan_revision_columns_are_immutable_artifact_facts() -> None:
    block = _table_block(_migration_text_raw(), PLAN_REVISIONS_TABLE)
    assert "revision_number integer not null check (revision_number > 0)" in block
    assert "content text not null check (content ~ '\\S')" in block
    assert "repository_base_sha text not null check (repository_base_sha ~ '\\S')" in block
    assert "unique (task_id, revision_number)" in block
    # An immutable artifact has no update path and therefore no updated_at.
    assert "updated_at" not in block


def test_review_loop_purpose_vocabulary_is_exactly_planning_and_pr_review() -> None:
    block = _table_block(_migration_text(), REVIEW_LOOPS_TABLE)
    assert "check (purpose in ('planning', 'pr_review'))" in block
    # v1 has no implementation-review loop and no speculative categories.
    assert "implementation_review" not in block


def test_review_loop_has_no_database_iteration_limit_default() -> None:
    block = _table_block(_migration_text_raw(), REVIEW_LOOPS_TABLE)
    assert "iteration_limit integer not null check (iteration_limit > 0)" in block
    # The effective limit is caller-supplied: the v1 default (5) and any
    # later Workspace-level configurability live at the configured setting
    # boundary, and the prose documents the historical-reconstruction rule.
    assert "the v1 default (5)" in _prose_text()
    assert "configured setting boundary" in _prose_text()
    assert "preserved for historical reconstruction" in _prose_text()


def test_review_loop_lifecycle_is_minimal_open_closed_with_closed_at_coherence() -> None:
    block = _table_block(_migration_text(), REVIEW_LOOPS_TABLE)
    assert "check (status in ('open', 'closed'))" in block
    assert "(status = 'closed' and closed_at is not null)" in block
    assert "(status = 'open' and closed_at is null)" in block


def test_iteration_result_vocabulary_and_findings_shape() -> None:
    block = _table_block(_migration_text_raw(), REVIEW_ITERATIONS_TABLE)
    assert "outcome text check (outcome in ('accepted', 'changes_requested'))" in block
    assert "summary text check (summary is null or summary ~ '\\S')" in block
    assert "check (jsonb_typeof(findings) = 'array')" in block
    # Reviewer outcomes only: provider/runtime/protocol failures are
    # structurally excluded from the outcome vocabulary.
    for excluded in ("'failed'", "'error'", "'timeout'"):
        assert excluded not in block


def test_iteration_finalize_coherence_moves_four_facts_atomically() -> None:
    collapsed = _collapsed_raw_block(REVIEW_ITERATIONS_TABLE)
    assert (
        "outcome is null and summary is null and findings is null and decided_at is null"
        in collapsed
    )
    assert (
        "outcome is not null and summary is not null and findings is not null "
        "and decided_at is not null" in collapsed
    )


def test_iteration_result_coherence_is_settled_in_both_directions() -> None:
    collapsed = _collapsed_raw_block(REVIEW_ITERATIONS_TABLE)
    assert "outcome = 'accepted' and jsonb_array_length(findings) = 0" in collapsed
    assert "outcome = 'changes_requested' and jsonb_array_length(findings) >= 1" in collapsed


def test_iteration_result_coherence_is_case_guarded_for_arbitrary_json() -> None:
    # The coherence CHECK never evaluates jsonb_array_length on a non-array:
    # the CASE expression yields false for any other JSON type, so a
    # non-array findings document fails cleanly as a plain CheckViolation
    # (together with the jsonb_typeof column CHECK) instead of raising an
    # evaluation error. Boolean-expression evaluation order is not relied
    # upon.
    collapsed = _collapsed_raw_block(REVIEW_ITERATIONS_TABLE)
    assert "outcome is null or case when jsonb_typeof(findings) = 'array' then" in collapsed
    assert "else false end" in collapsed


def test_iteration_numbering_is_unique_per_review_loop() -> None:
    block = _table_block(_migration_text(), REVIEW_ITERATIONS_TABLE)
    assert "unique (review_loop_id, iteration_number)" in block


def test_iteration_subject_binds_the_same_task_plan_revision() -> None:
    block = _table_block(_migration_text_raw(), REVIEW_ITERATIONS_TABLE)
    assert "foreign key (review_loop_id, task_id, workspace_id)" in block
    assert "references openorc.review_loops (id, task_id, workspace_id)" in block
    assert "foreign key (plan_revision_id, task_id, workspace_id)" in block
    assert "references openorc.plan_revisions (id, task_id, workspace_id)" in block
    assert "check (plan_revision_id is not null)" in block


def test_current_plan_pointer_fk_enforces_same_task_and_workspace() -> None:
    raw = _migration_text_raw()
    assert "foreign key (current_plan_revision_id, id, workspace_id)" in raw
    assert "references openorc.plan_revisions (id, task_id, workspace_id)" in raw
    # The pointer is a nullable current-object identifier: this migration
    # never recreates or rewrites the committed tasks table, and no plan
    # content or review-outcome columns are duplicated onto the Task row.
    assert "create table openorc.tasks" not in raw


def test_workspace_scope_composite_foreign_keys_keep_direct_scope_in_agreement() -> None:
    collapsed = re.sub(r"\s+", " ", _migration_text_raw())
    assert (
        collapsed.count(
            "foreign key (task_id, workspace_id) references openorc.tasks (id, workspace_id)"
        )
        == 3
    )
    assert "unique (id, task_id, workspace_id)" in collapsed
    # The hooks exist on the two referenced tables; iterations chain through
    # them instead of carrying their own hook.
    assert collapsed.count("unique (id, task_id, workspace_id)") == 2


def test_indexes_are_query_driven_with_no_speculative_additions() -> None:
    raw = _migration_text()
    assert "create index review_loops_task_status_idx" in raw
    assert "on openorc.review_loops (task_id, status)" in raw
    # The planning-history and iteration-lookup indexes are the unique
    # constraint pairs themselves; no speculative indexes exist.
    assert raw.count("create index") == 1


def test_no_updated_at_on_immutable_rows_and_no_native_enums() -> None:
    raw = _migration_text_raw()
    for table in (PLAN_REVISIONS_TABLE, REVIEW_LOOPS_TABLE, REVIEW_ITERATIONS_TABLE):
        block = _table_block(raw, table)
        assert "updated_at" not in block, f"{table} must carry no updated_at"
        assert "create type" not in raw.lower()


def test_migration_only_creates_and_extends_never_rewrites() -> None:
    raw = _migration_text_raw().lower()
    # Migrations are append-only: this migration creates the new tables and
    # extends tasks with one constraint; it contains no drop, no data
    # rewrite, and no mutation of committed history.
    assert "drop " not in raw
    assert "update openorc." not in raw
    assert "delete from" not in raw


def test_prose_documents_the_settled_rules() -> None:
    prose = _prose_text()
    assert "versioned, immutable producer artifacts" in prose
    assert "a changed plan is a fresh revision, never a mutation of an existing one" in prose
    assert (
        "later movement of the repository base does not by itself invalidate "
        "a previously accepted/authorized plan revision" in prose
    )
    assert "never authorizes rewriting one" in prose
    assert "v1 purposes are exactly planning and pr_review" in prose
    assert "no implementation-review loop" in prose
    assert "finalizes atomically" in prose
    assert "no rewrite path exists afterwards" in prose
    assert "revisions and results are not rewritten to manufacture a different past" in prose
    assert "provider/runtime/protocol failures are not reviewer judgments" in prose
    assert "accepted clears with zero findings" in prose
    assert "retains at least one finding of unresolved work" in prose
    assert "persisted verbatim as historical evidence" in prose
    assert "phase 2 protocol validation in ``openorc.protocol``" in prose
    assert "wire envelope" in prose
    assert "no second review-history model" in prose
    assert "iteration numbering is unique per reviewloop" in prose
    assert "one fact, one home" in prose
    assert "leaves every older revision intact" in prose
    assert "closure policy and max-iteration transitions belong to later orchestration" in prose
