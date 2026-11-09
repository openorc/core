"""Convention tests for the session-resume record migration (F8g, issue #292).

The ordinary suite cannot execute Postgres. These tests assert only the
durable, architectural properties of the committed migration that runtime
behavior does not naturally establish: the forward-only status CHECK
vocabulary, the composite subject foreign keys binding every edge to the
exact Task/Workspace subjects, the one non-terminal-record-per-session
partial unique index, and the absence of grants or data conversion.
Behavioral invariants are proven against a real database by the
integration-marked intent-persistence suite.
"""

from __future__ import annotations

import re
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
MIGRATIONS_DIR = REPO_ROOT / "supabase" / "migrations"


def _migration_text_raw() -> str:
    matches = sorted(p.name for p in MIGRATIONS_DIR.glob("*_add_task_resume_intents.sql"))
    assert len(matches) == 1, matches
    return (MIGRATIONS_DIR / matches[0]).read_text()


def test_the_status_check_admits_exactly_the_forward_only_vocabulary() -> None:
    body = _migration_text_raw()
    assert "status in ('authorized', 'dispatched', 'settled', 'non_delivered', " in body
    assert "'interrupted', 'superseded')" in body


def test_the_partial_unique_index_admits_one_nonterminal_record_per_session() -> None:
    body = _migration_text_raw()
    assert (
        "create unique index task_resume_intents_open_session_uniq\n"
        "    on openorc.task_resume_intents (task_agent_session_id)\n"
        "    where status in ('authorized', 'dispatched');" in body
    )


def test_every_subject_edge_is_a_composite_cascade_foreign_key() -> None:
    body = _migration_text_raw()
    for table in (
        "openorc.workspaces (id)",
        "openorc.tasks (id, workspace_id)",
        "openorc.task_agent_sessions (id, task_id, workspace_id)",
        "openorc.task_blocks (id, task_id, workspace_id)",
        "openorc.profiles (id)",
    ):
        assert f"references {table}\n        on delete cascade" in body, table


def test_no_grant_statements_and_no_data_conversion_exist() -> None:
    body = _migration_text_raw()
    assert re.search(r"(?im)^\s*grant\s", body) is None
    assert re.search(r"(?im)^\s*update\s+openorc\.", body) is None
    assert re.search(r"(?im)^\s*insert\s+into\s", body) is None
