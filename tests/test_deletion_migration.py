"""Convention tests for the deletion-ownership migration (issue #27).

The ordinary suite cannot execute Postgres. These tests assert only the
durable, architectural properties of the committed deletion migration that
runtime behavior does not naturally establish: the single sanctioned
``auth.users`` account-root boundary, the explicit ON DELETE CASCADE
classification of true ownership edges, and the DEFERRABLE INITIALLY DEFERRED
restrictive classification of scope-consistency, cross-reference, and
within-aggregate linkage edges. Behavioral deletion semantics are proven
against a real database by ``tests/integration/test_deletion_persistence.py``.
"""

from __future__ import annotations

import re
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
MIGRATIONS_DIR = REPO_ROOT / "supabase" / "migrations"

DELETION_MIGRATION_SUFFIX = "_define_deletion_ownership_graph.sql"

# True ownership edges: the child exists solely as part of the referenced
# parent's owned aggregate, and the parent is the deletion authority. The
# first entry is the sanctioned Supabase Auth account root.
OWNERSHIP_CASCADE_CONSTRAINTS = (
    "profiles_id_auth_users_fk",
    "workspaces_owner_profile_id_fkey",
    "projects_workspace_id_fkey",
    "repositories_project_id_workspace_id_fkey",
    "tasks_repository_id_workspace_id_fkey",
    "task_agent_sessions_task_id_workspace_id_fkey",
    "plan_revisions_task_id_workspace_id_fkey",
    "review_loops_task_id_workspace_id_fkey",
    "review_iterations_task_id_workspace_id_fkey",
    "owner_gates_task_id_workspace_id_fkey",
    "executions_task_id_workspace_id_fkey",
    "runtime_requests_task_id_workspace_id_fkey",
    "task_blocks_task_id_workspace_id_fkey",
    "task_pull_requests_task_id_workspace_id_fkey",
    "task_pull_requests_task_id_repository_id_workspace_id_fkey",
    "workflow_events_task_id_workspace_id_fkey",
    "connections_workspace_id_fkey",
    "workflow_role_bindings_workspace_id_fkey",
    "prompt_template_overrides_workspace_id_fkey",
    "workflow_events_workspace_id_fkey",
)

# Restrictive edges: direct Workspace scope-consistency facts of Task-owned
# rows, Connection historical/config cross-references, within-Task-aggregate
# linkage, and the Task's upward current-object pointers. Never ownership.
DEFERRED_RESTRICTIVE_CONSTRAINTS = (
    "tasks_workspace_id_fkey",
    "task_agent_sessions_workspace_id_fkey",
    "plan_revisions_workspace_id_fkey",
    "review_loops_workspace_id_fkey",
    "review_iterations_workspace_id_fkey",
    "owner_gates_workspace_id_fkey",
    "executions_workspace_id_fkey",
    "runtime_requests_workspace_id_fkey",
    "task_blocks_workspace_id_fkey",
    "task_pull_requests_workspace_id_fkey",
    "task_agent_sessions_connection_id_workspace_id_fkey",
    "workflow_role_bindings_connection_id_workspace_id_fkey",
    "review_iterations_review_loop_id_task_id_workspace_id_fkey",
    "review_iterations_plan_revision_id_task_id_workspace_id_fkey",
    "review_iterations_task_pull_request_fk",
    "owner_gates_plan_revision_id_task_id_workspace_id_fkey",
    "owner_gates_task_pull_request_fk",
    "executions_producer_session_id_task_id_workspace_id_fkey",
    "runtime_requests_producer_session_id_task_id_workspace_id_fkey",
    "tasks_current_plan_revision_fk",
    "tasks_current_owner_gate_fk",
)

# The historical/config cross-references that must never cascade through
# history (issue #27 Connection semantics).
_CONNECTION_REFERENCE_CONSTRAINTS = (
    "task_agent_sessions_connection_id_workspace_id_fkey",
    "workflow_role_bindings_connection_id_workspace_id_fkey",
)


def _deletion_migration_text() -> str:
    matches = sorted(p.name for p in MIGRATIONS_DIR.glob(f"*{DELETION_MIGRATION_SUFFIX}"))
    assert len(matches) == 1, f"expected exactly one deletion-ownership migration, found {matches}"
    return (MIGRATIONS_DIR / matches[0]).read_text(encoding="utf-8").lower()


def _add_constraint_statement(text: str, constraint: str) -> str:
    pattern = re.compile(rf"\badd constraint {constraint}\b")
    statements = [s for s in text.split(";") if pattern.search(s)]
    assert len(statements) == 1, f"expected exactly one add constraint for {constraint}"
    return statements[0]


def test_exactly_one_sanctioned_auth_users_boundary_exists_across_migrations() -> None:
    total = 0
    for migration_path in sorted(MIGRATIONS_DIR.glob("*.sql")):
        total += migration_path.read_text(encoding="utf-8").lower().count("references auth.users")
    assert total == 1, (
        "profiles.id -> auth.users is the single sanctioned Supabase Auth "
        "boundary; additional managed-schema foreign keys need an explicit, "
        "reviewed decision"
    )


def test_the_auth_users_boundary_cascades_the_account_root() -> None:
    text = _deletion_migration_text()
    statement = _add_constraint_statement(text, "profiles_id_auth_users_fk")
    assert "references auth.users (id)" in statement
    assert "on delete cascade" in statement


def test_no_other_migration_references_supabase_managed_schemas() -> None:
    for migration_path in sorted(MIGRATIONS_DIR.glob("*.sql")):
        text = migration_path.read_text(encoding="utf-8").lower()
        if DELETION_MIGRATION_SUFFIX in migration_path.name:
            continue
        assert "references auth." not in text, migration_path.name
        assert "references storage." not in text, migration_path.name


def test_true_ownership_edges_cascade() -> None:
    text = _deletion_migration_text()
    for constraint in OWNERSHIP_CASCADE_CONSTRAINTS:
        statement = _add_constraint_statement(text, constraint)
        assert "on delete cascade" in statement, constraint
        assert "deferrable" not in statement, constraint


def test_restrictive_edges_are_deferred_and_never_cascade() -> None:
    text = _deletion_migration_text()
    for constraint in DEFERRED_RESTRICTIVE_CONSTRAINTS:
        statement = _add_constraint_statement(text, constraint)
        assert "deferrable initially deferred" in statement, constraint
        assert "on delete cascade" not in statement, constraint


def test_connection_reference_edges_never_cascade_through_history() -> None:
    text = _deletion_migration_text()
    for constraint in _CONNECTION_REFERENCE_CONSTRAINTS:
        statement = _add_constraint_statement(text, constraint)
        assert "deferrable initially deferred" in statement, constraint
        assert "on delete cascade" not in statement, constraint
        # The Connection-reference edges guard historical TaskAgentSession
        # and role-binding evidence; their classification must match this
        # module's declarative list.
        assert constraint in DEFERRED_RESTRICTIVE_CONSTRAINTS


def test_migration_classifies_every_relationship_explicitly() -> None:
    # Every FK constraint re-declared by the deletion migration carries an
    # explicit action: cascade or deferrable restriction — nothing implicit.
    text = _deletion_migration_text()
    adds = [s for s in text.split(";") if "add constraint" in s]
    foreign_key_adds = [s for s in adds if "foreign key" in s]
    assert len(foreign_key_adds) == len(OWNERSHIP_CASCADE_CONSTRAINTS) + len(
        DEFERRED_RESTRICTIVE_CONSTRAINTS
    )
    for statement in foreign_key_adds:
        assert "on delete cascade" in statement or "deferrable initially deferred" in statement, (
            statement
        )
