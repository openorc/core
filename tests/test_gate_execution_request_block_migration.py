"""Convention tests for the gate/execution/request/block migration (#24).

The ordinary suite cannot execute Postgres. These tests assert only the
durable, architectural properties of the committed migration that runtime
behavior does not naturally establish: the four tables plus the
TaskAgentSession hook and the Task current-gate composite foreign key, the
settled vocabularies (four gate types, four gate statuses, eight execution
statuses, the single ``action_approval`` kind, four runtime-request
statuses, and the twelve TaskBlock reasons), the per-type exact-subject
coherence, the resolution/terminal coherence CHECKs, the full-history
runtime-request correlation uniqueness, the per-Task attempt identity, the
query-driven indexes, and the deliberate absences (no Git SHA field on
executions, no singular current-Execution pointer, no ReviewLoop-exhaustion
block reason, no ``updated_at`` on single-mutation rows, no native enums,
no speculative indexes, and no rewrite of prior tables). Behavioral
invariants are proven against a real database by the integration-marked
suite in ``tests/integration/``.
"""

from __future__ import annotations

import re
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
MIGRATIONS_DIR = REPO_ROOT / "supabase" / "migrations"

OWNER_GATES_TABLE = "openorc.owner_gates"
EXECUTIONS_TABLE = "openorc.executions"
RUNTIME_REQUESTS_TABLE = "openorc.runtime_requests"
TASK_BLOCKS_TABLE = "openorc.task_blocks"


def _migration_text_raw() -> str:
    matches = sorted(
        p.name for p in MIGRATIONS_DIR.glob("*_create_gate_execution_request_block_tables.sql")
    )
    assert len(matches) == 1, (
        f"expected exactly one gate/execution/request/block migration, found {matches}"
    )
    return (MIGRATIONS_DIR / matches[0]).read_text(encoding="utf-8")


def _migration_text() -> str:
    return _migration_text_raw().lower()


def _prose_text() -> str:
    """Lowercased migration prose with comment markers and wrapping removed."""
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


def _sql_block(table: str) -> str:
    """The create-table body with inline comment lines removed.

    The migration deliberately documents its absences in comments; the
    "no stray vocabulary" assertions below must inspect executable SQL, not
    prose.
    """
    body = _table_block(_migration_text_raw(), table)
    kept = [line for line in body.splitlines() if not line.strip().startswith("--")]
    return re.sub(r"\s+", " ", "\n".join(kept))


def test_migration_creates_the_four_control_tables_and_the_session_hook() -> None:
    text = _migration_text()
    for table in (
        OWNER_GATES_TABLE,
        EXECUTIONS_TABLE,
        RUNTIME_REQUESTS_TABLE,
        TASK_BLOCKS_TABLE,
    ):
        assert f"create table {table}" in text
    assert "alter table openorc.task_agent_sessions" in text
    assert "add constraint task_agent_sessions_id_task_id_workspace_id_uniq" in text


def test_owner_gate_vocabularies_are_exactly_settled() -> None:
    sql = _sql_block(OWNER_GATES_TABLE)
    assert (
        "gate_type text not null check (gate_type in ( 'implementation_authorization', "
        "'pr_authorization', 'merge_decision', 'review_resolution' ))"
    ) in sql
    assert (
        "status text not null check (status in ( 'pending', 'approved', 'rejected', 'cancelled' ))"
    ) in sql
    # No speculative gate types or statuses in the executable vocabulary.
    for stray in ("chat", "question", "deferred", "awaiting"):
        assert stray not in sql


def test_owner_gate_subject_coherence_is_durable() -> None:
    raw = _collapsed_raw_block(OWNER_GATES_TABLE)
    # implementation_authorization binds exactly the plan-revision subject.
    assert "gate_type = 'implementation_authorization' and plan_revision_id is not null" in raw
    # pr_authorization and merge_decision bind exactly the head-SHA subject.
    assert "gate_type = 'pr_authorization' and plan_revision_id is null" in raw
    assert "gate_type = 'merge_decision' and plan_revision_id is null" in raw
    # review_resolution binds exactly one subject (either form).
    assert "(plan_revision_id is not null and subject_head_sha is null)" in raw
    assert "(plan_revision_id is null and subject_head_sha is not null)" in raw


def test_owner_gate_resolution_coherence_is_durable() -> None:
    raw = _collapsed_raw_block(OWNER_GATES_TABLE)
    assert "(status = 'pending' and decided_at is null)" in raw
    assert "status in ('approved', 'rejected', 'cancelled') and decided_at is not null" in raw


def test_owner_gate_hook_supports_the_task_pointer() -> None:
    raw = _collapsed_raw_block(OWNER_GATES_TABLE)
    assert "unique (id, task_id, workspace_id)" in raw
    text = _migration_text()
    assert "add constraint tasks_current_owner_gate_fk" in text
    assert "foreign key (current_owner_gate_id, id, workspace_id)" in text
    assert "references openorc.owner_gates (id, task_id, workspace_id)" in text


def test_single_mutation_rows_carry_no_updated_at() -> None:
    # The gate's only mutation is the resolution (stamped decided_at), the
    # runtime request's only mutations are terminal transitions (stamped
    # decided_at), and the block's only mutation is resolution (stamped
    # resolved_at). Immutable single-mutation rows have no updated_at.
    for table in (OWNER_GATES_TABLE, RUNTIME_REQUESTS_TABLE, TASK_BLOCKS_TABLE):
        assert "updated_at" not in _table_block(_migration_text_raw(), table)


def test_the_migration_declares_no_native_enums() -> None:
    text = _migration_text()
    assert "create type" not in text
    assert "create domain" not in text


def test_the_migration_does_not_rewrite_prior_tables() -> None:
    text = _migration_text()
    assert "drop table" not in text
    assert "drop column" not in text
    # The only prior-table changes are the session hook and the Task FK.
    assert text.count("alter table openorc.tasks") == 1
    assert text.count("alter table openorc.task_agent_sessions") == 1


def test_the_migration_defines_no_browser_facing_grants() -> None:
    # The openorc schema lockout carries isolation: no grant statements in
    # the migration's executable SQL (the prose explains this).
    sql_only = "\n".join(
        line for line in _migration_text_raw().splitlines() if not line.strip().startswith("--")
    )
    assert "grant" not in sql_only.lower()
    assert "the ``openorc`` schema lockout" in _prose_text()


def test_execution_vocabularies_and_identity_are_settled() -> None:
    raw = _collapsed_raw_block(EXECUTIONS_TABLE)
    assert (
        "status text not null check (status in ( 'queued', 'running', 'paused', "
        "'paused_for_approval', 'succeeded', 'failed_transient', 'failed_final', "
        "'cancelled' ))"
    ) in raw
    assert "execution_number integer not null check (execution_number > 0)" in raw
    assert "unique (task_id, execution_number)" in raw


def test_execution_session_binding_is_exact() -> None:
    raw = _collapsed_raw_block(EXECUTIONS_TABLE)
    assert "producer_session_id uuid not null" in raw
    assert "foreign key (producer_session_id, task_id, workspace_id)" in raw
    assert "references openorc.task_agent_sessions (id, task_id, workspace_id)" in raw


def test_executions_carry_no_git_sha_field_or_branch_ownership() -> None:
    # Runtime sandboxes are runtime-private execution state; local Git HEAD
    # is not OpenOrc's canonical engineering truth. GitHub owns committed
    # repository truth, reconciled later through GitHub. No Git SHA field of
    # any kind — head, base, or result — belongs on an Execution, and the
    # canonical feature branch remains a Task-level ownership fact.
    sql = _sql_block(EXECUTIONS_TABLE)
    for stray in ("head_sha", "base_sha", "result_sha", "commit_sha", "_sha", "canonical"):
        assert stray not in sql


def test_executions_stamp_status_transitions_with_updated_at() -> None:
    block = _table_block(_migration_text_raw(), EXECUTIONS_TABLE)
    assert "updated_at timestamptz not null default now()" in block


def test_runtime_request_kind_is_exactly_action_approval() -> None:
    sql = _sql_block(RUNTIME_REQUESTS_TABLE)
    assert "kind text not null check (kind = 'action_approval')" in sql
    for stray in ("owner_decision", "question", "chat", "free_form"):
        assert stray not in sql


def test_runtime_request_status_vocabulary_is_settled() -> None:
    raw = _collapsed_raw_block(RUNTIME_REQUESTS_TABLE)
    assert (
        "status text not null check (status in ( 'pending', 'resolved', 'expired', 'cancelled' ))"
    ) in raw


def test_runtime_request_correlation_is_unique_across_history() -> None:
    block = _table_block(_migration_text_raw(), RUNTIME_REQUESTS_TABLE)
    assert "unique (producer_session_id, external_approval_id)" in block
    # Deliberately not a partial unique on pending: resolution never frees
    # the external identity for a second historical row in the session.
    assert "where status = 'pending'" not in block


def test_runtime_request_terminal_coherence_is_durable() -> None:
    raw = _collapsed_raw_block(RUNTIME_REQUESTS_TABLE)
    assert "status = 'pending' and resolution is null and decided_at is null" in raw
    assert "status = 'resolved' and resolution is not null" in raw
    assert "status in ('expired', 'cancelled') and resolution is null" in raw


def test_runtime_request_resolution_vocabulary_is_the_typed_control() -> None:
    raw = _collapsed_raw_block(RUNTIME_REQUESTS_TABLE)
    assert "resolution text check (resolution in ('approved', 'rejected'))" in raw


def test_task_block_reason_vocabulary_is_exactly_settled() -> None:
    raw = _collapsed_raw_block(TASK_BLOCKS_TABLE)
    for reason in (
        "review_failure",
        "runtime_failure",
        "agent_session_lost",
        "connection_unavailable",
        "invalid_credentials",
        "external_operation_uncertain",
        "stale_operation",
        "github_source_changed",
        "runtime_request_rejected",
        "pr_closed_unmerged",
        "owner_action_required",
        "unknown",
    ):
        assert f"'{reason}'" in raw


def test_review_loop_exhaustion_is_not_a_block_reason() -> None:
    sql = _sql_block(TASK_BLOCKS_TABLE)
    for stray in ("exhaust", "iteration_limit", "review_limit", "max_iteration"):
        assert stray not in sql
    prose = _prose_text()
    assert "never as a taskblock reason" in prose


def test_task_blocks_persist_canonical_json_context() -> None:
    raw = _collapsed_raw_block(TASK_BLOCKS_TABLE)
    assert "context jsonb check (jsonb_typeof(context) = 'object')" in raw
    assert "resolved_at timestamptz" in raw


def test_the_migration_declares_the_query_driven_indexes_only() -> None:
    text = _migration_text()
    assert "create index owner_gates_task_pending_idx" in text
    assert "create index executions_task_active_idx" in text
    assert "where status in ('queued', 'running', 'paused', 'paused_for_approval')" in text
    assert "create index runtime_requests_task_pending_idx" in text
    assert "create index task_blocks_task_current_idx" in text
    assert text.count("where resolved_at is null") == 1
    # No speculative indexes beyond the four query-driven ones.
    assert len(re.findall(r"create index", text)) == 4
