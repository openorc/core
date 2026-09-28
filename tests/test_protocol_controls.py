"""Workflow-control and interaction-model tests for the protocol package
(issue #129).

Deterministic protocol/control tests only: they verify OpenOrc-owned
structure, routing, rendering, and the closed interaction vocabulary. No
database, no network, and no evaluation of the meaning or wording of
LLM-authored free-text fields — no keyword matching, negative-word tests,
scoring, or classification of ``summary``, ``details``, ``plan``, ``changes``,
``validation``, or ``notes`` content. Canonical asset wording is never
duplicated in test code; assertions check structural identity and
authoritative-value substitution only.
"""

from __future__ import annotations

from pathlib import Path

import pytest

import openorc.protocol
from openorc.protocol.controls_assets import (
    CONTROL_ASSETS,
    PLAN_CONTROL_ASSET,
    PR_COMPOSE_CONTROL_ASSET,
    control_asset_text,
    render_control_asset,
)
from openorc.protocol.interaction import (
    INTERACTION_KINDS,
    ImplementInteraction,
    InteractionKind,
    PlanInteraction,
    PlanReviewSubject,
    PrComposeInteraction,
    PrReviewSubject,
    ReviewInteraction,
    ReviseInteraction,
)
from openorc.protocol.models import (
    IMPLEMENTATION_RESULT_FAMILY,
    PLAN_RESULT_FAMILY,
    PR_RESULT_FAMILY,
    REVIEW_RESULT_FAMILY,
    ReviewFinding,
    ReviewResult,
)

_REPO = "openorc/core"
_ISSUE = 129
_PR_NUMBER = 42
_HEAD_SHA = "0123456789abcdef0123456789abcdef01234567"

_PLAN_SUBJECT = PlanReviewSubject(
    plan_revision_id="plan-rev-1",
    repository_full_name=_REPO,
    issue_number=_ISSUE,
    plan="exact planning content supplied by OpenOrc",
)

_PR_SUBJECT = PrReviewSubject(
    repository_full_name=_REPO,
    pull_request_number=_PR_NUMBER,
    head_sha=_HEAD_SHA,
    issue_number=_ISSUE,
)

_CHANGES_REQUESTED = ReviewResult(
    outcome="CHANGES_REQUESTED",
    summary="opaque reviewer free text",
    findings=(ReviewFinding(summary="opaque", details="opaque"),),
)

_ACCEPTED = ReviewResult(outcome="ACCEPTED", summary="opaque", findings=())


def _repository_asset(asset: str) -> str:
    package_dir = Path(openorc.protocol.__file__).resolve().parent
    return (package_dir / "controls" / f"{asset}.md").read_text(encoding="utf-8")


# --- Closed v1 interaction vocabulary ---


def test_closed_interaction_vocabulary_exactly_the_five_kinds():
    assert frozenset({"PLAN", "REVIEW", "REVISE", "IMPLEMENT", "PR_COMPOSE"}) == INTERACTION_KINDS
    assert {kind.value for kind in InteractionKind} == set(INTERACTION_KINDS)


def test_owner_reviewer_advisory_discussion_is_not_a_workflow_control():
    assert "DISCUSS" not in INTERACTION_KINDS


# --- Canonical control assets: closed set and deterministic rendering ---


def test_only_plan_and_pr_compose_are_renderable_control_assets():
    assert frozenset({"plan", "pr_compose"}) == CONTROL_ASSETS


def test_control_assets_load_exactly_the_repository_markdown_files():
    for asset in sorted(CONTROL_ASSETS):
        assert control_asset_text(asset) == _repository_asset(asset)


def test_unknown_control_asset_names_fail_closed():
    for name in ("review", "remediate", "implement", "session", "discuss"):
        with pytest.raises(ValueError):
            control_asset_text(name)
        with pytest.raises(ValueError):
            render_control_asset(name, repository_full_name=_REPO, issue_number=_ISSUE)


def test_plan_and_pr_compose_render_the_exact_canonical_assets_with_authoritative_values():
    plan = PlanInteraction(repository_full_name=_REPO, issue_number=_ISSUE).render()
    assert plan == render_control_asset(
        PLAN_CONTROL_ASSET,
        repository_full_name=_REPO,
        issue_number=_ISSUE,
    )
    assert _REPO in plan
    assert f"#{_ISSUE}" in plan

    compose = PrComposeInteraction(repository_full_name=_REPO, issue_number=_ISSUE).render()
    assert compose == render_control_asset(
        PR_COMPOSE_CONTROL_ASSET,
        repository_full_name=_REPO,
        issue_number=_ISSUE,
    )
    assert _REPO in compose
    assert f"#{_ISSUE}" in compose
    assert f"Closes #{_ISSUE}" in compose


def test_rendering_is_deterministic():
    first = PlanInteraction(repository_full_name=_REPO, issue_number=_ISSUE).render()
    second = PlanInteraction(repository_full_name=_REPO, issue_number=_ISSUE).render()
    assert first == second

    compose_first = PrComposeInteraction(repository_full_name=_REPO, issue_number=_ISSUE).render()
    compose_second = PrComposeInteraction(repository_full_name=_REPO, issue_number=_ISSUE).render()
    assert compose_first == compose_second


def test_rendering_never_leaves_unresolved_placeholders():
    for rendered in (
        PlanInteraction(repository_full_name=_REPO, issue_number=_ISSUE).render(),
        PrComposeInteraction(repository_full_name=_REPO, issue_number=_ISSUE).render(),
    ):
        assert "{{" not in rendered
        assert "}}" not in rendered


# --- REVIEW: exact-subject binding ---


def test_planning_review_carries_the_exact_supplied_plan_subject_and_expects_review_result():
    interaction = ReviewInteraction(subject=_PLAN_SUBJECT)
    assert interaction.kind is InteractionKind.REVIEW
    assert interaction.subject is _PLAN_SUBJECT
    assert interaction.expected_response_family == REVIEW_RESULT_FAMILY


def test_pr_review_carries_the_exact_canonical_pr_identity_head_and_expects_review_result():
    interaction = ReviewInteraction(subject=_PR_SUBJECT)
    assert interaction.subject is _PR_SUBJECT
    assert isinstance(interaction.subject, PrReviewSubject)
    assert interaction.subject.head_sha == _HEAD_SHA
    assert interaction.subject.pull_request_number == _PR_NUMBER
    assert interaction.expected_response_family == REVIEW_RESULT_FAMILY


def test_review_requires_a_subject():
    with pytest.raises(ValueError):
        ReviewInteraction(subject=None)


def test_no_implementation_review_subject_variant_exists():
    # The closed REVIEW-subject model has exactly two variants.
    from openorc.protocol import interaction as interaction_module

    subject_names = {
        name
        for name in dir(interaction_module)
        if name.endswith("ReviewSubject") and name != "ReviewSubject"
    }
    assert subject_names == {"PlanReviewSubject", "PrReviewSubject"}


# --- REVISE: opaque routing of the validated review result ---


def test_planning_revise_routes_the_validated_result_opaquely_and_expects_plan_result():
    interaction = ReviseInteraction(subject=_PLAN_SUBJECT, review_result=_CHANGES_REQUESTED)
    assert interaction.kind is InteractionKind.REVISE
    # Structural identity: the very object is routed, not a copy or a rewrite.
    assert interaction.review_result is _CHANGES_REQUESTED
    assert interaction.expected_response_family == PLAN_RESULT_FAMILY


def test_pr_revise_routes_the_validated_result_opaquely_and_expects_implementation_result():
    interaction = ReviseInteraction(subject=_PR_SUBJECT, review_result=_CHANGES_REQUESTED)
    assert interaction.review_result is _CHANGES_REQUESTED
    assert interaction.expected_response_family == IMPLEMENTATION_RESULT_FAMILY


def test_revise_free_text_fields_pass_through_untouched():
    # Byte-identity of every field, including free text, proves no rewriting
    # or inspection without evaluating the wording of any of it.
    interaction = ReviseInteraction(subject=_PR_SUBJECT, review_result=_CHANGES_REQUESTED)
    routed = interaction.review_result
    assert routed is not None
    assert routed.outcome == _CHANGES_REQUESTED.outcome
    assert routed.summary == _CHANGES_REQUESTED.summary
    assert routed.findings == _CHANGES_REQUESTED.findings


def test_revise_rejects_non_changes_requested_results():
    with pytest.raises(ValueError):
        ReviseInteraction(subject=_PLAN_SUBJECT, review_result=_ACCEPTED)


def test_revise_requires_subject_and_result():
    with pytest.raises(ValueError):
        ReviseInteraction(subject=None, review_result=_CHANGES_REQUESTED)
    with pytest.raises(ValueError):
        ReviseInteraction(subject=_PLAN_SUBJECT, review_result=None)


def test_revise_expected_family_mismatch_fails_closed():
    with pytest.raises(ValueError):
        ReviseInteraction(
            subject=_PLAN_SUBJECT,
            review_result=_CHANGES_REQUESTED,
            expected_response_family=IMPLEMENTATION_RESULT_FAMILY,
        )


# --- IMPLEMENT: runtime-neutral semantic intent only ---


def test_implement_has_no_prose_rendering_and_only_semantic_intent():
    interaction = ImplementInteraction()
    assert interaction.kind is InteractionKind.IMPLEMENT
    assert not hasattr(interaction, "render")
    fields = {f.name for f in type(interaction).__dataclass_fields__.values()}
    assert fields == {"kind"}


# --- PR_COMPOSE: presentation-only boundary ---


def test_pr_compose_expects_pr_result_and_has_no_publication_authorization_surface():
    interaction = PrComposeInteraction(repository_full_name=_REPO, issue_number=_ISSUE)
    assert interaction.kind is InteractionKind.PR_COMPOSE
    assert interaction.expected_response_family == PR_RESULT_FAMILY
    fields = {f.name for f in type(interaction).__dataclass_fields__.values()}
    assert fields == {
        "kind",
        "repository_full_name",
        "issue_number",
        "guidance",
        "expected_response_family",
    }


# --- Runtime neutrality of the interaction models ---


def test_interaction_models_carry_no_provider_or_runtime_specific_fields():
    import inspect

    from openorc.protocol import interaction as interaction_module

    model_names = (
        "PlanInteraction",
        "ReviewInteraction",
        "ReviseInteraction",
        "ImplementInteraction",
        "PrComposeInteraction",
        "PlanReviewSubject",
        "PrReviewSubject",
    )
    for name in model_names:
        model = getattr(interaction_module, name)
        fields = {f.name for f in model.__dataclass_fields__.values()}
        for field in fields:
            lowered = field.lower()
            assert "cline" not in lowered, f"{name}.{field} names the Cline runtime"
            assert "hub" not in lowered, f"{name}.{field} names a Hub identifier"
            assert "provider" not in lowered, f"{name}.{field} is provider-specific"
        assert "SDK" not in inspect.getsource(model)


# --- Fail-closed interaction-kind and response-family coherence ---


def test_contradictory_interaction_kinds_are_rejected():
    with pytest.raises(ValueError):
        PlanInteraction(
            kind=InteractionKind.REVIEW, repository_full_name=_REPO, issue_number=_ISSUE
        )
    with pytest.raises(ValueError):
        ReviewInteraction(kind=InteractionKind.PLAN, subject=_PLAN_SUBJECT)
    with pytest.raises(ValueError):
        ReviseInteraction(
            kind=InteractionKind.IMPLEMENT, subject=_PR_SUBJECT, review_result=_CHANGES_REQUESTED
        )
    with pytest.raises(ValueError):
        ImplementInteraction(kind=InteractionKind.PLAN)
    with pytest.raises(ValueError):
        PrComposeInteraction(
            kind=InteractionKind.REVISE, repository_full_name=_REPO, issue_number=_ISSUE
        )


def test_review_wrong_expected_response_family_is_rejected():
    with pytest.raises(ValueError):
        ReviewInteraction(subject=_PLAN_SUBJECT, expected_response_family=PLAN_RESULT_FAMILY)


def test_pr_compose_wrong_expected_response_family_is_rejected():
    with pytest.raises(ValueError):
        PrComposeInteraction(
            repository_full_name=_REPO,
            issue_number=_ISSUE,
            expected_response_family=REVIEW_RESULT_FAMILY,
        )


def test_revise_wrong_expected_response_family_for_pr_subject_is_rejected():
    with pytest.raises(ValueError):
        ReviseInteraction(
            subject=_PR_SUBJECT,
            review_result=_CHANGES_REQUESTED,
            expected_response_family=PLAN_RESULT_FAMILY,
        )


# --- Fail-closed REVIEW/REVISE subject typing ---


def test_review_rejects_implementation_result_and_arbitrary_objects_as_subject():
    from openorc.protocol.models import ImplementationResult

    implementation = ImplementationResult(
        status="COMPLETED",
        branch="feat/some-branch",
        summary="opaque",
        changes=("opaque",),
        validation=("opaque",),
        notes=None,
    )
    with pytest.raises(ValueError):
        ReviewInteraction(subject=implementation)  # type: ignore[arg-type]
    with pytest.raises(ValueError):
        ReviewInteraction(subject="plan-rev-1")  # type: ignore[arg-type]
    with pytest.raises(ValueError):
        ReviseInteraction(subject=implementation, review_result=_CHANGES_REQUESTED)  # type: ignore[arg-type]


# --- Workspace-guidance composition through the #66 boundary ---


def test_plan_and_pr_compose_compose_guidance_through_the_shared_subordinate_boundary():
    from openorc.protocol.initialization_assets import append_subordinate_guidance

    guidance = "Owner-authored guidance prose."

    with_guidance = PlanInteraction(
        repository_full_name=_REPO, issue_number=_ISSUE, guidance=guidance
    ).render()
    base = PlanInteraction(repository_full_name=_REPO, issue_number=_ISSUE).render()
    assert with_guidance == append_subordinate_guidance(base, guidance)
    assert guidance in with_guidance
    assert base in with_guidance

    compose_base = PrComposeInteraction(repository_full_name=_REPO, issue_number=_ISSUE).render()
    compose_with = PrComposeInteraction(
        repository_full_name=_REPO, issue_number=_ISSUE, guidance=guidance
    ).render()
    assert compose_with == append_subordinate_guidance(compose_base, guidance)
    assert guidance in compose_with


def test_blank_guidance_contributes_nothing():
    base = PlanInteraction(repository_full_name=_REPO, issue_number=_ISSUE).render()
    assert (
        PlanInteraction(repository_full_name=_REPO, issue_number=_ISSUE, guidance=None).render()
        == base
    )
    assert (
        PlanInteraction(repository_full_name=_REPO, issue_number=_ISSUE, guidance="   ").render()
        == base
    )
    compose_base = PrComposeInteraction(repository_full_name=_REPO, issue_number=_ISSUE).render()
    assert (
        PrComposeInteraction(
            repository_full_name=_REPO, issue_number=_ISSUE, guidance=None
        ).render()
        == compose_base
    )
