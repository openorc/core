"""Migration-text tests for the detached terminal-cleanup receipts (issue #344).

The additive evolution of the shared detached Hub-run receipt seam: the
``terminal_cleanup`` receipt operation, its dedicated Task-scoped
``terminal_cleanup_claim`` original attempt kind, the pairing/family
identity coherence CHECK, and the shared episode identity-kind CHECK
extension. The receipt table's forward-only lifecycle, single-outstanding
backstop, and staged early-completion handoff are untouched by the
extension.

Static migration-text checks; the constraint behavior is exercised by the
integration-marked real-Postgres suites.
"""

from __future__ import annotations

from pathlib import Path

MIGRATIONS_DIR = Path(__file__).parent.parent / "supabase" / "migrations"


def _migration_text() -> str:
    matches = sorted(MIGRATIONS_DIR.glob("*_add_terminal_cleanup_receipts.sql"))
    assert len(matches) == 1, (
        f"expected exactly one terminal-cleanup receipt migration, got {matches}"
    )
    return (MIGRATIONS_DIR / matches[0]).read_text(encoding="utf-8")


def test_the_receipt_operation_vocab_admits_terminal_cleanup() -> None:
    text = _migration_text()
    # The vocabulary CHECK is dropped and re-added with every member: the
    # extension appends the cleanup operation without losing any predecessor.
    assert "drop constraint task_hub_run_receipts_operation_vocab" in text
    assert "'terminal_cleanup'" in text
    for predecessor in (
        "'planning_bootstrap_prepare'",
        "'session_initialization'",
        "'owner_reviewer_discussion'",
        "'session_resume'",
    ):
        assert predecessor in text


def test_the_receipt_attempt_kind_vocab_admits_the_task_scoped_claim() -> None:
    text = _migration_text()
    assert "drop constraint task_hub_run_receipts_original_attempt_kind_vocab" in text
    assert "'terminal_cleanup_claim'" in text
    for predecessor in (
        "'runtime_send_claim'",
        "'owner_control_intent'",
        "'owner_gate_discussion'",
    ):
        assert predecessor in text


def test_the_identity_coherence_check_binds_the_pairing_and_the_family() -> None:
    text = _migration_text()
    assert "task_hub_run_receipts_terminal_cleanup_identity_coherence" in text
    # The cleanup receipt pairs its operation with its dedicated Task-scoped
    # attempt kind and pins the canonical workspace_result family; every
    # other operation stays off the cleanup attempt kind. The family token
    # itself is NOT exclusive — the bootstrap PREPARE receipt legitimately
    # expects the same workspace_result family.
    assert "operation = 'terminal_cleanup'" in text
    assert "original_attempt_kind = 'terminal_cleanup_claim'" in text
    assert "expected_formal_family = 'workspace_result'" in text
    assert "expected_formal_family <> 'workspace_result'" not in text


def test_the_episode_identity_kind_check_admits_the_shared_member() -> None:
    text = _migration_text()
    assert "drop constraint operational_failure_episodes_original_attempt_kind_check" in text
    assert "add constraint operational_failure_episodes_original_attempt_kind_check" in text
    # The shared closed vocabulary admits every member even though the
    # cleanup family is deliberately NOT a Level 2 operational-failure
    # family (no cleanup episode is ever created).
    episode_check = text.split(
        "add constraint operational_failure_episodes_original_attempt_kind_check"
    )[-1]
    assert "'terminal_cleanup_claim'" in episode_check
