"""Migration-text tests for the F5f presentation-retry extension (issue #269).

The additive evolution of the F4 (#251) operational-failure disposition
state in the exact form the F5d (#267) migration established: the exact-head
PR-authorization presentation operation family joins the episode family
CHECK vocabulary. Nothing else changes — the episode still roots at an
existing durable identity kind (the successful E6 Execution UUID under the
existing ``execution`` identity kind), the attempt error-code vocabulary
already admits the one retry-eligible transport code (F5d), and no new
table, column, or privilege grant exists.

Static migration-text checks; the constraint behavior is exercised by the
integration-marked real-Postgres suites.
"""

from __future__ import annotations

import re
from pathlib import Path

MIGRATIONS_DIR = Path(__file__).parent.parent / "supabase" / "migrations"

_FAMILIES = (
    "planning_bootstrap_prepare",
    "planning_bootstrap_transition",
    "initial_plan_dispatch",
    "planning_revise_dispatch",
    "reviewer_review",
    "implementation_execution",
    "pr_remediation_execution",
    "pr_compose_dispatch",
    "exact_head_merge",
    "exact_head_pr_authorization_presentation",
)


def _migration_text_raw() -> str:
    matches = sorted(
        p.name
        for p in MIGRATIONS_DIR.glob(
            "*_extend_operational_failures_for_pr_authorization_presentation.sql"
        )
    )
    assert len(matches) == 1, (
        "expected exactly one "
        "extend_operational_failures_for_pr_authorization_presentation migration, "
        f"found {matches}"
    )
    return (MIGRATIONS_DIR / matches[0]).read_text(encoding="utf-8")


def _collapsed(text: str) -> str:
    return re.sub(r"\s+", " ", text)


def test_the_migration_creates_no_new_tables_or_columns() -> None:
    raw = _collapsed(_migration_text_raw())
    assert "create table" not in raw
    assert "add column" not in raw


def test_the_episode_family_vocabulary_gains_only_the_presentation_member() -> None:
    raw = _collapsed(_migration_text_raw())
    assert (
        "constraint operational_failure_episodes_operation_family_check "
        "check (operation_family in ("
    ) in raw
    for family in _FAMILIES:
        assert f"'{family}'" in raw, family


def test_only_the_operation_family_constraint_is_evolved() -> None:
    """The F5f extension is deliberately minimal: exactly one CHECK is
    dropped and recreated; the identity-kind and error-code vocabularies
    already admit the presentation family's values."""
    raw = _migration_text_raw()
    collapsed = _collapsed(raw)
    assert collapsed.count("drop constraint") == 1
    assert collapsed.count("add constraint") == 1
    assert "original_attempt_kind_check" not in collapsed.split("add constraint")[-1]
    assert "error_code_check" not in raw


def test_the_migration_grants_no_new_privileges() -> None:
    text = _migration_text_raw().lower()
    assert "grant " not in text
    assert "create policy" not in text


def test_the_domain_vocabulary_stays_aligned_with_the_migration() -> None:
    # The closed vocabularies evolve together: the domain enum carries
    # exactly the migrated database value.
    from openorc.domain.operational_failures import (
        OriginalAttemptKind,
        PRAuthorizationPresentationOperation,
    )

    assert {family.value for family in PRAuthorizationPresentationOperation} == {
        "exact_head_pr_authorization_presentation"
    }
    # The episode root reuses the existing execution identity kind.
    assert OriginalAttemptKind.EXECUTION.value == "execution"
    for value in (member.value for member in PRAuthorizationPresentationOperation):
        assert f"'{value}'" in _collapsed(_migration_text_raw())
    for value in (member.value for member in OriginalAttemptKind):
        # Every identity kind is admitted by the (unchanged) episode CHECK.
        assert f"'{value}'" in _collapsed(
            (
                MIGRATIONS_DIR / "20261103000000_extend_operational_failures_for_merge_retries.sql"
            ).read_text(encoding="utf-8")
        )
