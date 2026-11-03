"""Migration-text tests for the F5d merge-retry extension (issue #267).

The additive evolution of the F4 (#251) operational-failure disposition
state: the exact-head merge operation family, the approved merge-decision
gate identity kind, the classified transient external-transport error code,
and the set-once consumed-merge-attempt evidence column. The existing
episode/attempt uniqueness backstops and the escalation/consumption linkage
are untouched by the extension.

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
)
_IDENTITY_KINDS = (
    "runtime_send_claim",
    "planning_bootstrap_claim",
    "review_iteration",
    "execution",
    "owner_control_intent",
    "merge_decision_gate",
)
_ERROR_CODES = (
    "runtime_unavailable",
    "runtime_timeout",
    "runtime_delivery_uncertain",
    "runtime_transport",
    "runtime_protocol",
    "agent_session_lost",
    "runtime_configuration_rejected",
    "external_operation_failed",
    "external_operation_uncertain",
    "external_operation_rejected",
    "external_transport_unavailable",
    "unknown",
)


def _migration_text_raw() -> str:
    matches = sorted(
        p.name for p in MIGRATIONS_DIR.glob("*_extend_operational_failures_for_merge_retries.sql")
    )
    assert len(matches) == 1, (
        f"expected exactly one extend_operational_failures_for_merge_retries migration, "
        f"found {matches}"
    )
    return (MIGRATIONS_DIR / matches[0]).read_text(encoding="utf-8")


def _collapsed(text: str) -> str:
    return re.sub(r"\s+", " ", text)


def test_the_migration_creates_no_new_tables() -> None:
    raw = _collapsed(_migration_text_raw())
    assert "create table" not in raw


def test_the_episode_family_vocabulary_gains_only_the_exact_head_merge_member() -> None:
    raw = _collapsed(_migration_text_raw())
    assert (
        "constraint operational_failure_episodes_operation_family_check "
        "check (operation_family in ("
    ) in raw
    for family in _FAMILIES:
        assert f"'{family}'" in raw, family
    assert "'exact_head_merge'" in raw


def test_the_identity_kind_vocabulary_gains_the_merge_decision_gate() -> None:
    raw = _collapsed(_migration_text_raw())
    for kind in _IDENTITY_KINDS:
        assert f"'{kind}'" in raw, kind
    assert "'merge_decision_gate'" in raw


def test_the_error_code_vocabulary_gains_the_transient_external_transport_code() -> None:
    raw = _collapsed(_migration_text_raw())
    for code in _ERROR_CODES:
        assert f"'{code}'" in raw, code
    assert "'external_transport_unavailable'" in raw


def test_the_consumed_merge_attempt_stamp_is_additive_and_nullable() -> None:
    raw = _collapsed(_migration_text_raw())
    assert (
        "alter table openorc.operational_failure_attempts add column consumed_at timestamptz"
    ) in raw
    # The stamp is deliberately NOT a not-null or unique column: one-shot
    # consumption is the repository's conditional set-once update inside the
    # consuming Task-lock transaction.
    assert "consumed_at timestamptz not null" not in raw
    assert "unique" not in raw.split("consumed_at timestamptz")[-1]


def test_the_migration_grants_no_new_privileges() -> None:
    text = _migration_text_raw().lower()
    assert "grant " not in text
    assert "create policy" not in text


def test_the_domain_vocabulary_stays_aligned_with_the_migration() -> None:
    # The closed vocabularies evolve together: the domain enums carry exactly
    # the migrated database values.
    from openorc.domain.operational_failures import (
        GitHubMergeOperation,
        OperationalFailureCode,
        OriginalAttemptKind,
    )

    assert {family.value for family in GitHubMergeOperation} == {"exact_head_merge"}
    assert OriginalAttemptKind.MERGE_DECISION_GATE.value == "merge_decision_gate"
    assert OperationalFailureCode.EXTERNAL_TRANSPORT_UNAVAILABLE.value == (
        "external_transport_unavailable"
    )
    for value in (member.value for member in GitHubMergeOperation):
        assert f"'{value}'" in _collapsed(_migration_text_raw())
    for value in (member.value for member in OriginalAttemptKind):
        assert f"'{value}'" in _collapsed(_migration_text_raw())
    for value in (member.value for member in OperationalFailureCode):
        assert f"'{value}'" in _collapsed(_migration_text_raw())
