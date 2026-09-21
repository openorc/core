"""Integration-marked staged-upgrade test for the corrective removal migration (#100).

The ordinary suite cannot execute Postgres, and the shared session fixture
applies every committed migration in one pass. This module instead proves the
corrective migration's upgrade path against a PRE-correction schema: inside
one rolled-back transaction it resets the ``openorc`` schema, applies every
committed migration except the corrective removal migration, seeds a
pre-correction state (a ``prompt_override_changed`` workflow event row, a
prompt-template override row, and a four-fact-initialized TaskAgentSession
row), executes the committed corrective migration file, and asserts the
deliberate removal policy end to end:

- the obsolete ``openorc.prompt_template_overrides`` table is gone;
- the legacy ``prompt_override_changed`` event row is deleted by the
  migration (the pre-v1 abstraction is removed, not preserved), the narrowed
  event-type CHECK rejects any further ``prompt_override_changed`` insert,
  and every surviving Python enum value still inserts;
- the ``initialization_protocol_version`` column is gone while the remaining
  row data is preserved, and the three-fact initialization-coherence CHECK
  rejects incoherent rows in both directions.

The suite consumes the database it is given and never provisions one; it
skips cleanly when ``OPENORC_TEST_DATABASE_URL`` is absent (the same
Owner-controlled target the other integration suites use).
"""

from __future__ import annotations

import uuid
from collections.abc import Iterator
from pathlib import Path
from typing import Any, LiteralString, cast

import pytest
from psycopg import Connection, connect
from psycopg.errors import CheckViolation
from psycopg.types.json import Jsonb

from openorc.domain.events import WorkflowEventType

REPO_ROOT = Path(__file__).resolve().parents[2]
MIGRATIONS_DIR = REPO_ROOT / "supabase" / "migrations"

REMOVAL_MIGRATION_SUFFIX = "_remove_prompt_overrides_and_initialization_protocol_version.sql"

# Every test in this module requires the explicitly supplied non-production
# branch database. The marker excludes the module from ordinary DB-free runs
# (pyproject addopts "-m 'not integration'") and lets integration runs select
# it explicitly with "-m integration".
pytestmark = pytest.mark.integration


@pytest.fixture
def conn(database_url: str) -> Iterator[Connection[Any]]:
    """One connection per test; the staged upgrade runs in one rolled-back transaction.

    Deliberately keyed on the raw ``database_url`` (not the session
    ``migrated_database`` fixture): the staged test rebuilds the schema
    itself, so the shared once-per-session migration application is neither
    needed nor wanted here. The rollback restores the session fixture's
    committed schema state for any other module running in the same session.
    """
    with connect(database_url) as connection:
        yield connection
        connection.rollback()


def _corrective_migration_path() -> Path:
    matches = sorted(MIGRATIONS_DIR.glob(f"*{REMOVAL_MIGRATION_SUFFIX}"))
    assert len(matches) == 1, f"expected exactly one corrective removal migration, found {matches}"
    return matches[0]


def _apply(conn: Connection[Any], path: Path) -> None:
    # Committed migration files are trusted repository content applied
    # wholesale; LiteralString is the driver's injection-safe query contract,
    # satisfied here by repository-controlled file text.
    conn.execute(cast(LiteralString, path.read_text(encoding="utf-8")))


def _row_count(conn: Connection[Any], sql: str, params: tuple[Any, ...]) -> int:
    return int(conn.execute(sql, params).fetchone()[0])  # type: ignore[index]


def _table_exists(conn: Connection[Any], table_name: str) -> bool:
    row = conn.execute(
        "select count(*) from information_schema.tables "
        "where table_schema = 'openorc' and table_name = %s",
        (table_name,),
    ).fetchone()
    assert row is not None
    return int(row[0]) == 1


def _column_exists(conn: Connection[Any], table_name: str, column_name: str) -> bool:
    row = conn.execute(
        "select count(*) from information_schema.columns "
        "where table_schema = 'openorc' and table_name = %s and column_name = %s",
        (table_name, column_name),
    ).fetchone()
    assert row is not None
    return int(row[0]) == 1


def _seed_pre_correction_chain(conn: Connection[Any]) -> tuple[uuid.UUID, uuid.UUID, uuid.UUID]:
    """One Profile -> Workspace -> Repository -> Task -> Connection chain."""
    profile_id = uuid.uuid4()
    # profiles.id references auth.users (id) ON DELETE CASCADE — the single
    # sanctioned Supabase Auth boundary: every Profile needs its backing Auth
    # user row. The inserts roll back with the test transaction.
    conn.execute("insert into auth.users (id) values (%s)", (profile_id,))
    conn.execute("insert into openorc.profiles (id) values (%s)", (profile_id,))
    workspace_id = uuid.uuid4()
    conn.execute(
        "insert into openorc.workspaces (id, owner_profile_id, name) values (%s, %s, %s)",
        (workspace_id, profile_id, "workspace"),
    )
    project_id = uuid.uuid4()
    conn.execute(
        "insert into openorc.projects (id, workspace_id, name) values (%s, %s, %s)",
        (project_id, workspace_id, "project"),
    )
    repository_id = uuid.uuid4()
    conn.execute(
        "insert into openorc.repositories "
        "(id, project_id, workspace_id, github_repository_id, owner_login, name, "
        "html_url, is_private, default_branch) "
        "values (%s, %s, %s, %s, %s, %s, %s, %s, %s)",
        (
            repository_id,
            project_id,
            workspace_id,
            70_300_001,
            "octocat",
            "hello-world",
            "https://github.com/octocat/hello-world",
            False,
            "main",
        ),
    )
    task_id = uuid.uuid4()
    conn.execute(
        "insert into openorc.tasks "
        "(id, workspace_id, repository_id, github_issue_id, github_issue_number, status) "
        "values (%s, %s, %s, %s, %s, 'ready_to_plan')",
        (task_id, workspace_id, repository_id, 9701, 300),
    )
    connection_id = uuid.uuid4()
    conn.execute(
        "insert into openorc.connections (id, workspace_id, adapter_type, name) "
        "values (%s, %s, 'cline', 'staged-hub')",
        (connection_id, workspace_id),
    )
    return workspace_id, task_id, connection_id


def test_the_corrective_migration_upgrades_a_pre_correction_schema(
    conn: Connection[Any],
) -> None:
    corrective_path = _corrective_migration_path()

    # Stage 1: reset the schema and apply every committed migration except
    # the corrective removal migration — the pre-correction schema.
    conn.execute("drop schema if exists openorc cascade")
    for migration_path in sorted(MIGRATIONS_DIR.glob("*.sql")):
        if migration_path == corrective_path:
            continue
        _apply(conn, migration_path)

    # Stage 2: seed the pre-correction state the corrective migration must
    # deliberately upgrade — a prompt-template override row, a workflow event
    # of the removed type, and a fully initialized (four-fact) session row.
    workspace_id, task_id, connection_id = _seed_pre_correction_chain(conn)
    conn.execute(
        "insert into openorc.prompt_template_overrides "
        "(workspace_id, template_key, base_template_version, instruction_text) "
        "values (%s, %s, %s, %s)",
        (workspace_id, "producer.plan_instructions", "builtin-1.0.0", "Override."),
    )
    conn.execute(
        "insert into openorc.workflow_events (workspace_id, event_type, actor_type) "
        "values (%s, %s, %s)",
        (workspace_id, "prompt_override_changed", "owner"),
    )
    session_id = uuid.uuid4()
    conn.execute(
        "insert into openorc.task_agent_sessions "
        "(id, workspace_id, task_id, role, connection_id, external_session_id, "
        "lifecycle_status, initialization_protocol_version, "
        "effective_config_snapshot, initialized_at) "
        "values (%s, %s, %s, 'producer', %s, %s, 'ready', %s, %s, now())",
        (
            session_id,
            workspace_id,
            task_id,
            connection_id,
            "ext-session-staged",
            "1",
            Jsonb({"stage": "plan"}),
        ),
    )

    # Stage 3: flush pending deferred-constraint events, then apply the
    # committed corrective migration. The issue #27 foreign keys are
    # ``NO ACTION DEFERRABLE INITIALLY DEFERRED``, so the seeded rows carry
    # pending referential checks that would otherwise fire only at commit —
    # but a real migration runs against committed data with no pending
    # constraint events. ``SET CONSTRAINTS ALL IMMEDIATE`` forces every
    # seeded foreign key to be checked right now, emulating the committed-data
    # boundary preceding a real migration while the whole staged upgrade
    # stays inside this single rollback-only transaction.
    conn.execute("set constraints all immediate")
    _apply(conn, corrective_path)
    # Restore the transaction's original deferred-checking state immediately
    # after the migration: nothing downstream of the applied DDL needs
    # immediate checking, so the committed-boundary emulation stays local to
    # the flush above.
    conn.execute("set constraints all deferred")

    # Stage 4a: the obsolete prompt-override table is gone, with its legacy
    # row removed together with it.
    assert not _table_exists(conn, "prompt_template_overrides")

    # Stage 4b: the legacy prompt_override_changed event row is deliberately
    # removed by the migration, and the narrowed CHECK rejects any further
    # insert of the removed type.
    assert (
        _row_count(
            conn,
            "select count(*) from openorc.workflow_events "
            "where event_type = 'prompt_override_changed'",
            (),
        )
        == 0
    )
    with pytest.raises(CheckViolation), conn.transaction():
        conn.execute(
            "insert into openorc.workflow_events (workspace_id, event_type, actor_type) "
            "values (%s, %s, %s)",
            (workspace_id, "prompt_override_changed", "owner"),
        )
    # Every surviving Python enum value still inserts cleanly.
    for member in WorkflowEventType:
        conn.execute(
            "insert into openorc.workflow_events (workspace_id, event_type, actor_type) "
            "values (%s, %s, %s)",
            (workspace_id, member.value, "owner"),
        )
    assert _row_count(conn, "select count(*) from openorc.workflow_events", ()) == len(
        WorkflowEventType
    )

    # Stage 4c: the initialization-protocol-version column is gone while the
    # seeded session row's remaining data is preserved...
    assert not _column_exists(conn, "task_agent_sessions", "initialization_protocol_version")
    row = conn.execute(
        "select external_session_id, lifecycle_status, effective_config_snapshot, "
        "initialized_at from openorc.task_agent_sessions where id = %s",
        (session_id,),
    ).fetchone()
    assert row is not None
    assert row[0] == "ext-session-staged"
    assert row[1] == "ready"
    assert row[2] == {"stage": "plan"}
    assert row[3] is not None
    # ...and the three-fact coherence CHECK is enforced: a READY row missing
    # any initialization fact is rejected (the fully set coherent form was
    # just read back above).
    with pytest.raises(CheckViolation), conn.transaction():
        conn.execute(
            "insert into openorc.task_agent_sessions "
            "(id, workspace_id, task_id, role, connection_id, external_session_id, "
            "lifecycle_status) values (%s, %s, %s, 'reviewer', %s, %s, 'ready')",
            (uuid.uuid4(), workspace_id, task_id, connection_id, "ext-session-partial"),
        )
    # CONNECTING with one initialization fact set (mixed state: identity set,
    # instant and snapshot still NULL) matches no CHECK branch and is
    # rejected exactly like every other partially initialized status.
    with pytest.raises(CheckViolation), conn.transaction():
        conn.execute(
            "insert into openorc.task_agent_sessions "
            "(id, workspace_id, task_id, role, connection_id, external_session_id, "
            "lifecycle_status) values (%s, %s, %s, 'reviewer', %s, %s, 'connecting')",
            (uuid.uuid4(), workspace_id, task_id, connection_id, "ext-session-mixed"),
        )
    # The valid pre-initialization CONNECTING form — all three facts NULL —
    # still commits, proving the rejections above are selective enforcement
    # and not a broken CHECK.
    conn.execute(
        "insert into openorc.task_agent_sessions "
        "(id, workspace_id, task_id, role, connection_id, lifecycle_status) "
        "values (%s, %s, %s, 'reviewer', %s, 'connecting')",
        (uuid.uuid4(), workspace_id, task_id, connection_id),
    )
    assert (
        _row_count(
            conn,
            "select count(*) from openorc.task_agent_sessions where task_id = %s",
            (task_id,),
        )
        == 2
    )
