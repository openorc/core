"""Convention tests for the notification publication trigger migration (G4b, #398).

The ordinary suite cannot execute Postgres. These tests assert only the
durable, architectural properties of the committed migration that runtime
behavior does not naturally establish: the fixed transport contract is
reproduced byte-exactly in SQL (one channel literal, two canonical envelope
templates evaluated against the contract encoder), publication machinery is
small and static (no SECURITY DEFINER, no dynamic SQL, one pg_notify per
envelope kind, every function revoked from PUBLIC), the covered-table set and
its explicit omissions match the documented coverage map, unchanged UPDATEs
are suppressed whole-row, the account-deletion barrier trigger is WHEN-scoped
to exactly the account_deletion_state column, and the workflow_events trigger
is INSERT-only. Behavioral invariants are proven against a real database by
the integration-marked suite
``tests/integration/test_notification_publication_triggers.py``.
"""

from __future__ import annotations

import re
from pathlib import Path
from uuid import uuid4

from openorc.notifications.contract import (
    NOTIFICATION_CHANNEL,
    DataChangedNotification,
    NotificationResourceType,
    WorkflowEventNotification,
    encode_notification_payload,
)

REPO_ROOT = Path(__file__).resolve().parents[1]
MIGRATIONS_DIR = REPO_ROOT / "supabase" / "migrations"

_COVERED_TABLES = frozenset(
    {
        "workspaces",
        "projects",
        "repositories",
        "connections",
        "workflow_role_bindings",
        "github_installations",
        "workflow_events",
        "tasks",
        "task_agent_sessions",
        "executions",
        "plan_revisions",
        "review_loops",
        "review_iterations",
        "owner_gates",
        "task_blocks",
        "task_pull_requests",
        "runtime_requests",
        "task_interrupt_intents",
        "task_resume_intents",
        "task_pull_request_close_intents",
        "task_runtime_send_claims",
        "task_hub_run_receipts",
        "github_issues",
        "github_issue_hierarchy",
        "github_issue_dependencies",
        "github_issue_sub_issues",
        "github_refreshes",
        "github_user_authorizations",
        "profiles",
    }
)
_OMITTED_TABLES = frozenset(
    {
        "task_hub_run_early_completions",
        "github_user_authorization_flows",
        "github_webhook_deliveries",
        "github_webhook_delivery_routes",
        "operational_failure_episodes",
        "operational_failure_attempts",
    }
)


def _migration_text_raw() -> str:
    matches = sorted(
        p.name for p in MIGRATIONS_DIR.glob("*_add_notification_publication_triggers.sql")
    )
    assert len(matches) == 1, (
        f"expected exactly one notification publication trigger migration, found {matches}"
    )
    return (MIGRATIONS_DIR / matches[0]).read_text(encoding="utf-8")


def _collapsed(text: str) -> str:
    return re.sub(r"\s+", " ", text)


def _helper_body(function_name: str) -> str:
    raw = _migration_text_raw()
    match = re.search(
        rf"create function openorc\.{function_name} .*?\$notifier\$;",
        raw,
        re.DOTALL,
    )
    assert match is not None, f"expected the {function_name} function"
    return match.group(0)


def _quoted_literals(text: str) -> list[str]:
    return re.findall(r"'((?:[^']|'')*)'", text)


def _code_text() -> str:
    """The migration's executable SQL: comment lines stripped, collapsed."""
    lines = [
        line for line in _migration_text_raw().splitlines() if not line.lstrip().startswith("--")
    ]
    return _collapsed("\n".join(lines))


def test_the_channel_is_the_one_fixed_g4a_channel() -> None:
    code = _code_text()
    assert code.count("pg_notify") == 2, (
        "payload construction exists only in the two helpers, one pg_notify each"
    )
    channel_literals = [
        literal for literal in _quoted_literals(code) if literal == NOTIFICATION_CHANNEL
    ]
    assert len(channel_literals) == 2, (
        "the fixed channel literal appears exactly once in each payload helper"
    )


def test_the_data_changed_template_reproduces_the_canonical_bytes() -> None:
    body = _helper_body("notify_data_changed")
    # Literal order inside the helper: channel, prefix, resource-type middle,
    # resource-id middle, tail.
    literals = _quoted_literals(body)
    assert literals[0] == NOTIFICATION_CHANNEL
    prefix, type_middle, id_middle, tail = literals[1:5]
    workspace_id, resource_id = uuid4(), uuid4()
    canonical = encode_notification_payload(
        DataChangedNotification(
            workspace_id=workspace_id,
            resource_type=NotificationResourceType.TASK,
            resource_id=resource_id,
        )
    )
    # Evaluating the SQL concatenation with sample identities must equal the
    # contract encoder's output byte for byte.
    emitted = (
        prefix + str(workspace_id) + type_middle + "task" + id_middle + str(resource_id) + tail
    )
    assert emitted == canonical


def test_the_workflow_event_template_reproduces_the_canonical_bytes() -> None:
    body = _helper_body("notify_workflow_event")
    literals = _quoted_literals(body)
    assert literals[0] == NOTIFICATION_CHANNEL
    prefix, id_middle, tail = literals[1:4]
    workspace_id, workflow_event_id = uuid4(), uuid4()
    canonical = encode_notification_payload(
        WorkflowEventNotification(
            workspace_id=workspace_id,
            workflow_event_id=workflow_event_id,
        )
    )
    emitted = prefix + str(workspace_id) + id_middle + str(workflow_event_id) + tail
    assert emitted == canonical


def test_publication_machinery_is_small_and_static() -> None:
    code = _code_text()
    assert "security definer" not in code.lower()
    # No dynamic SQL constructs; the only EXECUTE is the trigger attachment
    # form `execute function openorc.<function>`.
    assert "format(" not in code
    assert "execute immediate" not in code
    for match in re.finditer(r"execute (?!function|on\b)", code):
        raise AssertionError(f"unexpected EXECUTE form: {code[match.start() : match.start() + 40]}")


def test_every_function_is_revoked_from_public() -> None:
    raw = _collapsed(_migration_text_raw())
    created = set(re.findall(r"create function openorc\.(\w+)", raw))
    revoked = set(re.findall(r"revoke execute on function openorc\.(\w+)", raw))
    assert created, "expected the publication functions"
    assert revoked == created
    assert all(
        f"revoke execute on function openorc.{name}" in raw
        and re.search(rf"revoke execute on function openorc\.{name}[^;]*from public;", raw)
        for name in revoked
    )


def test_the_covered_table_set_matches_the_documented_map() -> None:
    raw = _collapsed(_migration_text_raw())
    triggered = set(re.findall(r"create trigger \w+ .*? on openorc\.(\w+)", raw))
    assert triggered == _COVERED_TABLES
    for table in _OMITTED_TABLES:
        assert f"on openorc.{table} " not in raw + " ", f"{table} must carry no publication trigger"


def test_the_workflow_events_trigger_is_insert_only() -> None:
    blocks = {
        match.group(1): match.group(0)
        for match in re.finditer(
            r"(create trigger \w+ after [^;]*? on openorc\.workflow_events"
            r" .*?execute function [^;]+;)",
            _collapsed(_migration_text_raw()),
        )
    }
    assert blocks, "expected the workflow_events publication trigger"
    for block in blocks.values():
        assert "after insert on openorc.workflow_events" in block
        assert "update" not in block.replace("after insert on openorc.workflow_events", "")
        assert "delete" not in block.replace("after insert on openorc.workflow_events", "")


def test_the_account_barrier_trigger_is_when_scoped_to_the_state_column() -> None:
    raw = _collapsed(_migration_text_raw())
    match = re.search(
        r"create trigger notify_profiles_account_barrier_changed .*?execute function [^;]+;",
        raw,
    )
    assert match is not None, "expected the WHEN-scoped account barrier trigger"
    block = match.group(0)
    assert "after update on openorc.profiles" in block
    assert "insert" not in block and "delete" not in block
    assert "when (old.account_deletion_state is distinct from new.account_deletion_state)" in block


def test_unchanged_updates_are_suppressed_whole_row() -> None:
    code = _code_text()
    # Every dedicated trigger function guards UPDATE with the whole-row
    # comparison (dispatcher, task, issue, relation, refresh, and user
    # authorization); the account barrier relies on its WHEN scope. The
    # queue-membership workspace hint uses the null-safe comparison of the
    # queue-entry instants.
    assert code.count("old is not distinct from new") == 6
    assert "v_entry_queued_at is distinct from v_exit_queued_at" in code


def test_dispatcher_arguments_stay_in_the_closed_vocabulary() -> None:
    raw = _collapsed(_migration_text_raw())
    vocabulary = {member.value for member in NotificationResourceType}
    for match in re.finditer(
        r"execute function openorc\.notify_row_data_changed \('(\w+)', '(\w+)'\)", raw
    ):
        resource_type, identity_column = match.group(1), match.group(2)
        assert resource_type in vocabulary, (
            f"dispatcher resource_type {resource_type!r} is outside the fixed vocabulary"
        )
        assert identity_column in {"id", "task_id", "workspace_id"}
