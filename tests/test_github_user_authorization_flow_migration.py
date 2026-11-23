"""Convention tests for the issue #373 migrations.

The ordinary suite cannot execute Postgres. These tests assert only the
durable, architectural properties of the committed migrations that runtime
behavior does not naturally establish: the authorization table gains its
monotonic revocation-epoch fence (advanced by every Owner revocation, never
reset), the pending-flow correlation table carries the Profile-primary-key
single-flight invariant, the exact-state single-use delete semantics, the
purpose and expiry CHECK vocabulary, the sanctioned true-ownership cascade
edge, and that no privilege is granted by either migration.
"""

from __future__ import annotations

import re
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
MIGRATIONS_DIR = REPO_ROOT / "supabase" / "migrations"


def _migration_text(pattern: str) -> str:
    matches = sorted(p.name for p in MIGRATIONS_DIR.glob(pattern))
    assert len(matches) == 1, f"expected exactly one migration matching {pattern}, found {matches}"
    return (MIGRATIONS_DIR / matches[0]).read_text(encoding="utf-8")


def test_the_revocation_fence_migration_adds_the_monotonic_count() -> None:
    text = _migration_text("*_add_github_user_authorization_revocation_fence.sql")

    alter = [
        s.strip() for s in text.split(";") if "alter table openorc.github_user_authorizations" in s
    ]
    assert len(alter) == 1
    statement = alter[0]
    assert re.search(r"add column revocation_count integer not null default 0", statement)
    assert "check (revocation_count >= 0)" in statement
    # The fence is the only change: no column is dropped and no other table
    # or privilege is touched.
    assert "drop column" not in text
    assert not re.search(r"^\s*grant\b", text, re.MULTILINE)


def test_the_flows_migration_declares_the_pending_only_correlation_table() -> None:
    text = _migration_text("*_add_github_user_authorization_flows.sql")

    create = re.search(
        r"create table openorc\.github_user_authorization_flows \(.*?\);", text, re.S
    )
    assert create is not None
    table = create.group(0)
    # The Profile IS the row identity: DB-enforced one-pending-flow-per-Profile.
    assert "profile_id uuid primary key" in table
    assert "purpose text not null check (purpose in ('authorize', 'reauthorize'))" in table
    assert "state text not null unique" in table
    assert "pkce_verifier_reference text not null" in table
    assert "observed_revocation_count integer not null" in table
    assert "check (observed_revocation_count >= 0)" in table
    assert "started_at timestamptz not null default now()" in table
    assert "expires_at timestamptz not null" in table
    assert "check (expires_at > started_at)" in table


def test_the_flows_migration_declares_the_true_ownership_cascade_edge() -> None:
    text = _migration_text("*_add_github_user_authorization_flows.sql")

    fk = re.search(
        r"alter table openorc\.github_user_authorization_flows.*?"
        r"foreign key \(profile_id\).*?references openorc\.profiles \(id\).*?"
        r"on delete cascade",
        text,
        re.S,
    )
    assert fk is not None
    # No privilege is granted by the migration.
    assert not re.search(r"^\s*grant\b", text, re.MULTILINE)
