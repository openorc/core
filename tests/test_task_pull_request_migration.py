"""Convention tests for the TaskPullRequest migration (issue #25).

The ordinary suite cannot execute Postgres. These tests assert only the
durable, architectural properties of the committed migration that runtime
behavior does not naturally establish: the canonical TaskPullRequest table
with the Task composite-identity hook, the full-history one-PR-per-Task
uniqueness, the separation between stable GitHub PR identity and the
repository-local PR address, the per-Workspace GitHub-identity
canonicalization, the Task/Repository/Workspace agreement foreign keys, the
PR-subject ReviewIteration columns with exact-head subject coherence and
the composite subject foreign key, the corrected OwnerGate per-type subject
coherence (PR_AUTHORIZATION deliberately without a PR binding;
MERGE_DECISION requiring the TaskPullRequest plus the head SHA), the
merged-implies-closed coherence, and the deliberate absences (no richer
lifecycle vocabulary, no native enums, no speculative indexes, no grants,
and no rewrite beyond the two subject-coherence constraint replacements).
Behavioral invariants are proven against a real database by the
integration-marked suite in ``tests/integration/``.
"""

from __future__ import annotations

import re
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
MIGRATIONS_DIR = REPO_ROOT / "supabase" / "migrations"

TASK_PULL_REQUESTS_TABLE = "openorc.task_pull_requests"
REVIEW_ITERATIONS_TABLE = "openorc.review_iterations"
OWNER_GATES_TABLE = "openorc.owner_gates"


def _migration_text_raw() -> str:
    matches = sorted(p.name for p in MIGRATIONS_DIR.glob("*_create_task_pull_requests.sql"))
    assert len(matches) == 1, (
        f"expected exactly one create_task_pull_requests migration, found {matches}"
    )
    return (MIGRATIONS_DIR / matches[0]).read_text(encoding="utf-8")


def _migration_text() -> str:
    return _migration_text_raw().lower()


def _prose_text() -> str:
    """Lowercased migration prose with comment markers and wrapping removed."""
    stripped_lines = []
    for line in _migration_text_raw().splitlines():
        content = line.strip().lower()
        if content.startswith("--"):
            content = content[2:].strip()
        stripped_lines.append(content)
    return re.sub(r"\s+", " ", " ".join(stripped_lines))


def _table_block(text: str, table: str) -> str:
    """Return the create-table statement body for one openorc table."""
    match = re.search(rf"create table {re.escape(table)}\s*\((.*?)\);", text, re.DOTALL)
    assert match is not None, f"expected a create table statement for {table}"
    return match.group(1)


def _collapsed_raw_block(table: str) -> str:
    """The raw-case table body with all whitespace collapsed to single spaces."""
    return re.sub(r"\s+", " ", _table_block(_migration_text_raw(), table))


def _plan_review_iterations_table_block() -> str:
    """The committed review_iterations table body from the prior migration.

    Lets the constraint-name tests derive Postgres's auto-generated names
    from the committed source instead of mirroring this migration's prose.
    """
    matches = sorted(p.name for p in MIGRATIONS_DIR.glob("*_create_plan_review_tables.sql"))
    assert len(matches) == 1, (
        f"expected exactly one create_plan_review_tables migration, found {matches}"
    )
    prior_text = (MIGRATIONS_DIR / matches[0]).read_text(encoding="utf-8")
    return _table_block(prior_text, REVIEW_ITERATIONS_TABLE)


def test_migration_creates_the_task_pull_request_table() -> None:
    text = _migration_text()
    assert f"create table {TASK_PULL_REQUESTS_TABLE}" in text


def test_the_task_hook_and_workspace_scope_agreement_foreign_keys_exist() -> None:
    raw = re.sub(r"\s+", " ", _migration_text_raw())
    # Same-Repository Task identity hook: one composite foreign key from
    # the child records can then require Task/Repository/Workspace
    # agreement (mirrors the sibling-hook pattern).
    assert "add constraint tasks_id_repository_id_workspace_id_uniq" in raw
    assert "unique (id, repository_id, workspace_id)" in raw
    # Direct Workspace scope must agree with the Task's Workspace.
    assert "foreign key (task_id, workspace_id) references openorc.tasks (id, workspace_id)" in raw
    # The PR belongs to the Task's own Repository within the same
    # Workspace: no incompatible Task/Repository/Workspace attachment.
    assert (
        "foreign key (task_id, repository_id, workspace_id) "
        "references openorc.tasks (id, repository_id, workspace_id)" in raw
    )


def test_one_canonical_pull_request_per_task_is_full_history() -> None:
    block = _table_block(_migration_text_raw(), TASK_PULL_REQUESTS_TABLE)
    # Exactly one PR per Task for the whole v1 lifetime — no partial
    # predicate, so a closed-unmerged PR keeps blocking replacement rows.
    assert "unique (task_id)" in block
    assert "where state = 'open'" not in block
    assert "where state" not in block
    prose = _prose_text()
    assert "no replacement-pr rows and no speculative pr-replacement/adoption history" in prose
    assert "a closed-unmerged pr remains the task's canonical pr record" in prose


def test_github_identity_and_local_address_are_separate_concerns() -> None:
    raw = _collapsed_raw_block(TASK_PULL_REQUESTS_TABLE)
    # Stable GitHub PR identity (reconciliation key, per-Workspace
    # canonicalization) is persisted separately from the repository-local
    # address metadata captured at creation.
    assert "github_pr_id bigint not null check (github_pr_id > 0)" in raw
    assert "github_pr_number integer not null check (github_pr_number > 0)" in raw
    assert "unique (workspace_id, github_pr_id)" in raw
    # The local address is never an identity: no unique constraint on it.
    assert "unique (github_pr_number" not in raw
    assert "unique (workspace_id, github_pr_number)" not in raw


def test_the_task_hook_and_pr_hook_enable_composite_subject_foreign_keys() -> None:
    raw = re.sub(r"\s+", " ", _migration_text_raw())
    # The PR record exposes the composite hook the review/gate subject
    # foreign keys reference.
    assert "unique (id, task_id, workspace_id)" in raw
    assert (
        "foreign key (task_pull_request_id, task_id, workspace_id) "
        "references openorc.task_pull_requests (id, task_id, workspace_id)" in raw
    )
    # Both child tables carry the PR-subject binding, and each reference
    # requires same-Task/Workspace agreement.
    assert (
        raw.count(
            "foreign key (task_pull_request_id, task_id, workspace_id) "
            "references openorc.task_pull_requests (id, task_id, workspace_id)"
        )
        == 2
    )


def test_review_iterations_carry_the_exact_pr_review_subject() -> None:
    raw = re.sub(r"\s+", " ", _migration_text_raw())
    assert "add column task_pull_request_id uuid" in raw
    assert "add column reviewed_head_sha text" in raw
    assert "check (reviewed_head_sha is null or reviewed_head_sha ~ '\\S')" in raw
    # The settled XOR subject coherence replaces the prior bare not-NULL
    # check: exactly one complete subject form per iteration. The drop
    # target is the auto-generated name derived from the committed prior
    # migration (asserted exactly by the dedicated test below).
    assert "drop constraint review_iterations_plan_revision_id_check" in raw
    assert "drop constraint review_iterations_check;" not in raw
    assert "add constraint review_iterations_subject_form_check" in raw
    block = re.search(
        r"add constraint review_iterations_subject_form_check\s*check \((.*?)\);",
        raw,
        re.DOTALL,
    )
    assert block is not None
    coherence = re.sub(r"\s+", " ", block.group(1))
    # Planning form: the PlanRevision only — no PR identity, no head SHA.
    assert "plan_revision_id is not null and task_pull_request_id is null" in coherence
    assert "and reviewed_head_sha is null" in coherence
    # PR form: the TaskPullRequest plus the exact reviewed head SHA.
    assert "plan_revision_id is null and task_pull_request_id is not null" in coherence
    assert "and reviewed_head_sha is not null" in coherence
    prose = _prose_text()
    assert "movement of the pr target/base branch alone does not invalidate acceptance" in prose
    assert "the exact reviewed head shas are historical facts on the review records" in prose


def test_the_dropped_review_iterations_check_name_matches_the_prior_migration() -> None:
    # The old subject CHECK is unnamed and single-column, so Postgres
    # auto-named it ``<table>_<column>_check``. The drop target is derived
    # here from the committed plan-review migration — not mirrored from
    # this migration's prose — so a wrong drop name fails the convention
    # test instead of the real migration run.
    old_block = re.sub(r"\s+", " ", _plan_review_iterations_table_block())
    # The prior subject rule is exactly one unnamed single-column CHECK on
    # plan_revision_id, so its generated name has no numeric suffix.
    assert "check (plan_revision_id is not null)," in old_block
    assert old_block.count("plan_revision_id is not null") == 1
    raw = re.sub(r"\s+", " ", _migration_text_raw())
    assert "drop constraint review_iterations_plan_revision_id_check;" in raw
    # The multi-column owner_gates CHECK keeps its plain table name.
    assert "drop constraint owner_gates_check;" in raw


def test_owner_gates_bind_the_settled_subject_forms_per_type() -> None:
    raw = re.sub(r"\s+", " ", _migration_text_raw())
    assert "alter table openorc.owner_gates add column task_pull_request_id uuid" in raw
    # The prior auto-named per-type check is replaced by the settled one.
    assert "drop constraint owner_gates_check" in raw
    assert "add constraint owner_gates_subject_coherence_check" in raw
    block = re.search(
        r"add constraint owner_gates_subject_coherence_check\s*check \((.*?)\);",
        raw,
        re.DOTALL,
    )
    assert block is not None
    coherence = re.sub(r"\s+", " ", block.group(1))
    # IMPLEMENTATION_AUTHORIZATION: the exact PlanRevision, nothing else.
    assert (
        "gate_type = 'implementation_authorization' and plan_revision_id is not null "
        "and subject_head_sha is null and task_pull_request_id is null" in coherence
    )
    # PR_AUTHORIZATION: the pre-PR exact-head gate — the TaskPullRequest
    # binding is explicitly forbidden (it happens before the PR exists).
    assert (
        "gate_type = 'pr_authorization' and plan_revision_id is null "
        "and subject_head_sha is not null and task_pull_request_id is null" in coherence
    )
    # MERGE_DECISION: the canonical TaskPullRequest plus the exact head SHA.
    assert (
        "gate_type = 'merge_decision' and plan_revision_id is null "
        "and subject_head_sha is not null and task_pull_request_id is not null" in coherence
    )
    # REVIEW_RESOLUTION: exactly one subject form — the exhausted planning
    # PlanRevision, or the PR-review subject as PR plus exact head SHA.
    assert "plan_revision_id is not null and subject_head_sha is null" in coherence
    assert "plan_revision_id is null and subject_head_sha is not null" in coherence
    prose = _prose_text()
    assert "pr_authorization binds only the exact committed producer head sha" in prose
    assert "it happens before the canonical pr exists" in prose
    assert "a pr-subject gate is never representable as a bare head sha" in prose


def test_merged_implies_closed_is_durable() -> None:
    raw = _collapsed_raw_block(TASK_PULL_REQUESTS_TABLE)
    assert "check (merged_at is null or state = 'closed')" in raw
    prose = _prose_text()
    assert "a merged pr is a closed pr" in prose


def test_the_observed_lifecycle_is_a_settled_text_vocabulary() -> None:
    raw = _collapsed_raw_block(TASK_PULL_REQUESTS_TABLE)
    # Text/CHECK vocabulary rather than a native ENUM, with no lifecycle
    # default: reconciliation establishes the observed state explicitly.
    assert "state text not null check (state in ('open', 'closed'))" in raw
    assert "merged_at timestamptz" in raw
    prose = _prose_text()
    assert "no lifecycle default" in prose


def test_the_migration_declares_no_speculative_indexes() -> None:
    text = _migration_text()
    # The per-Task and per-identity lookups are the unique constraints
    # themselves; no speculative index exists in this migration.
    assert len(re.findall(r"create index", text)) == 0


def test_no_native_enums_and_no_grants() -> None:
    text = _migration_text()
    assert "create type" not in text
    assert "grant " not in text


def test_the_migration_creates_and_extends_and_never_rewrites() -> None:
    raw = _migration_text_raw().lower()
    # The only destructive statements are the two deliberate constraint
    # replacements (the old auto-named subject CHECKs); there is no table
    # drop, no column drop, no data rewrite, and no mutation beyond
    # creating/extending committed tables.
    assert raw.count("drop constraint") == 2
    assert "drop table" not in raw
    assert "drop column" not in raw
    assert "drop schema" not in raw
    assert "update openorc." not in raw
    assert "delete from" not in raw
    # Prior tables are never recreated.
    assert "create table openorc.tasks" not in raw
    assert "create table openorc.review_iterations" not in raw
    assert "create table openorc.owner_gates" not in raw


def test_github_api_orchestration_is_deliberately_out_of_persistence() -> None:
    prose = _prose_text()
    assert "github pr creation/reconciliation api calls are later service/adapter work" in prose
    assert "deliberately not in persistence" in prose


def test_prose_documents_the_settled_rules() -> None:
    prose = _prose_text()
    assert "one task has exactly one taskpullrequest for its whole v1 lifetime" in prose
    assert "``unique (task_id)`` is full-history with no partial predicate" in prose
    assert "the exact reviewed head shas are historical facts on the review records" in prose
    assert "changes across remediation rounds while the pr identity stays stable" in prose
    assert "a changed head invalidates prior acceptance" in prose
    assert (
        "reviewer acceptance identity is the taskpullrequest plus the exact reviewed head sha"
        in prose
    )
    assert "mutable observed reconciliation state" in prose
    assert "address metadata, never identity" in prose
