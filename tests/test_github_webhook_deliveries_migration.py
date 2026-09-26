"""Convention tests for the GitHub webhook delivery migration (issue #61).

The ordinary suite cannot execute Postgres. These tests assert only the
durable, architectural properties of the committed migration that runtime
behavior does not naturally establish: the durable GUID uniqueness, the
bounded classification/routing-target/routing-resolution CHECK vocabularies,
the classification-consistency CHECKs, the true-ownership cascade edges, the
absence of raw-payload/customer-content/credential columns, and the absence
of grants. Behavioral invariants are proven against a real database by the
integration-marked suite.
"""

from __future__ import annotations

import re
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
MIGRATIONS_DIR = REPO_ROOT / "supabase" / "migrations"


def _migration_text() -> str:
    matches = sorted(p.name for p in MIGRATIONS_DIR.glob("*_add_github_webhook_deliveries.sql"))
    assert len(matches) == 1, (
        f"expected exactly one add_github_webhook_deliveries migration, found {matches}"
    )
    return (MIGRATIONS_DIR / matches[0]).read_text(encoding="utf-8")


def _table_block(text: str, table: str) -> str:
    match = re.search(rf"create table {re.escape(table)}\s*\((.*?)\n\);", text, re.DOTALL)
    assert match is not None, f"expected a create table statement for {table}"
    return match.group(1)


def test_the_delivery_guid_is_durably_unique() -> None:
    text = _migration_text()

    assert "unique (delivery_guid)" in _table_block(text, "openorc.github_webhook_deliveries")


def test_the_classification_vocabularies_are_bounded() -> None:
    text = _migration_text()
    block = _table_block(text, "openorc.github_webhook_deliveries")

    assert "classification in ('relevant', 'ignored', 'unusable')" in block
    for target in (
        "repository_metadata",
        "issue_state",
        "issue_relations",
        "task_branch_or_pull_request",
        "pull_request_state",
        "checks",
    ):
        assert f"'{target}'" in block
    for resolution in (
        "resolved",
        "unmapped_installation",
        "unconfigured_repository",
        "route_mismatch",
    ):
        assert f"'{resolution}'" in block


def test_the_classification_consistency_checks_are_declared() -> None:
    text = _migration_text()
    block = _table_block(text, "openorc.github_webhook_deliveries")

    assert "(classification = 'relevant') = (routing_target is not null)" in block
    assert "(classification = 'relevant') = (routing_resolution is not null)" in block
    assert "classification <> 'relevant' or github_installation_id is not null" in block
    assert "classification <> 'relevant' or github_repository_id is not null" in block
    assert "github_issue_number is null or github_pull_request_number is null" in block


def test_the_delivery_table_carries_no_workspace_foreign_key() -> None:
    text = _migration_text().lower()
    delivery_statements = [
        s
        for s in text.split(";")
        if "alter table openorc.github_webhook_deliveries " in s and "foreign key" in s
    ]

    assert delivery_statements == []


def test_the_routing_linkage_edges_are_true_ownership_cascades() -> None:
    text = _migration_text().lower()
    expected_edges = (
        (
            "github_webhook_delivery_routes_delivery_id_fkey",
            "references openorc.github_webhook_deliveries (id)",
        ),
        ("github_webhook_delivery_routes_workspace_id_fkey", "references openorc.workspaces (id)"),
        (
            "github_webhook_delivery_routes_repository_id_workspace_id_fkey",
            "references openorc.repositories (id, workspace_id)",
        ),
    )
    for constraint, reference in expected_edges:
        statement = [s for s in text.split(";") if f"add constraint {constraint}" in s]
        assert len(statement) == 1, constraint
        assert reference in statement[0], constraint
        assert "on delete cascade" in statement[0], constraint
        assert "deferrable" not in statement[0], constraint


def test_no_payload_customer_content_or_credential_column_exists() -> None:
    text = _migration_text()
    blocks = (
        _table_block(text, "openorc.github_webhook_deliveries")
        + " "
        + _table_block(text, "openorc.github_webhook_delivery_routes")
    ).lower()

    for forbidden in (
        "payload",
        "raw_body",
        "body ",
        "signature",
        "secret",
        "token",
        "credential",
        "title",
        "comment",
        "content",
    ):
        assert forbidden not in blocks, forbidden


def test_the_resolver_index_exists() -> None:
    text = _migration_text().lower()

    assert "create index repositories_github_repository_id_idx" in text


def test_the_migration_grants_no_privileges() -> None:
    text = _migration_text().lower()

    assert "grant " not in text
    assert "to anon" not in text
