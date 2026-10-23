"""Convention tests for the Owner-Start fingerprint-freeze migration (issue #233).

The ordinary suite cannot execute Postgres. These tests assert only the
durable, architectural properties of the committed migration that runtime
behavior does not naturally establish: the column becomes NULLable for the
never-started state, the auto-named intake-era digest CHECK is replaced by an
explicitly named status-aware commitment CHECK (NULL only for
``ready_to_plan`` and ``cancelled``; every present value a lowercase hex
SHA-256 digest), the migration policy clears only current not-yet-started
``ready_to_plan`` rows (historical started fingerprints are never touched),
and no privilege is granted. Behavioral invariants are proven against a real
database by the integration-marked suite in ``tests/integration/``.
"""

from __future__ import annotations

from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
MIGRATIONS_DIR = REPO_ROOT / "supabase" / "migrations"


def _migration_text() -> str:
    matches = sorted(MIGRATIONS_DIR.glob("*_freeze_task_fingerprint_on_owner_start.sql"))
    assert len(matches) == 1, (
        f"expected exactly one freeze_task_fingerprint_on_owner_start migration, found {matches}"
    )
    text = matches[0].read_text(encoding="utf-8").lower()
    return " ".join(text.split())


def test_the_column_becomes_nullable_for_the_never_started_state() -> None:
    text = _migration_text()
    assert (
        "alter table openorc.tasks alter column source_requirements_fingerprint drop not null"
        in text
    )


def test_the_status_aware_commitment_check_replaces_the_intake_era_check() -> None:
    text = _migration_text()
    # The auto-named CHECK from the issue #60 migration is dropped by its
    # generated name (the same pattern the #211 settled-status migration
    # established) and replaced by an explicitly named status-aware one.
    assert "drop constraint tasks_source_requirements_fingerprint_check" in text
    assert "add constraint tasks_source_requirements_fingerprint_status_check" in text
    # The CHECK is the NULL-safe CASE form: a plain disjunction evaluates to
    # NULL (which Postgres CHECK constraints treat as PASS) for a NULL
    # fingerprint on a started status, silently admitting exactly the state
    # the contract forbids. The CASE always yields a definite boolean.
    assert "case" in text
    assert "when source_requirements_fingerprint is null" in text
    assert "then status in ('ready_to_plan', 'cancelled')" in text
    assert "else source_requirements_fingerprint ~ '^[0-9a-f]{64}$'" in text
    assert "end" in text


def test_the_policy_clears_only_current_not_yet_started_rows() -> None:
    text = _migration_text()
    assert "update openorc.tasks" in text
    assert "set source_requirements_fingerprint = null" in text
    assert "where status = 'ready_to_plan' and archived_at is null" in text
    # The historical record is never rewritten: no archived row and no
    # started row is updated by the policy statement.
    assert "archived_at is not null" not in text.split("update openorc.tasks")[1].split(";")[0]


def test_no_new_surface_and_no_grants() -> None:
    text = _migration_text()
    for forbidden in ("create table", "grant ", "create index"):
        assert forbidden not in text, f"the freeze migration must not add {forbidden!r}"
