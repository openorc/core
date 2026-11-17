"""The regular-mode detached-turn seam tests (issue #340).

The #340 closure proof for the ordinary model-turn migration (#325): one
compact fake-backed regular-mode walk spanning the key Producer↔Reviewer
handoffs through the ONE post-commit completion handoff, a targeted static
survey proving no ordinary model-call wait remains reachable from a
regular-mode worker, and representative stale/refusal seam behavior. Each
family's own tests keep owning their domain detail (dispatch services,
correlation, observation, settlement); this module proves only the
cross-family wiring: settled family → wired handoff route → next typed F1
submission or detached dispatch, re-derived from committed state, with no
recursive in-process chain and no blocking model call.

The documented blocking exceptions the survey names are the queue-mode
interim entry points (until #328), the F10 terminal cleanup (#327), the
bounded native ACT mode controls, and the currently-unsubmitted F5c
authorized-implementation-retry composition (nothing submits its operation
kind until F11 #257 owns the due-time wakeup).
"""

from __future__ import annotations

import ast
import types
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
from uuid import UUID, uuid4

import pytest

from openorc.domain.pending_interactions import PendingTaskRuntimeInteractionOperation
from openorc.domain.reviews import ReviewLoopPurpose, ReviewLoopStatus, ReviewOutcome
from openorc.domain.send_claims import TaskRuntimeSendClaimKind, TaskRuntimeSendClaimStatus
from openorc.domain.tasks import TaskPlanningBootstrapStage, TaskStatus
from openorc.protocol.models import PrResult
from openorc.services.errors import ConflictError
from openorc.services.operation_attempt_evidence import InvocationAttemptEvidence
from openorc.workers import task_workflow
from openorc.workers.invocation import (
    OperationKind,
    SubmissionRefusalReason,
    SubmissionResult,
    TaskPlanningReviewPayload,
    TaskPrReviewPayload,
)
from tests.test_workers_task_workflow import make_dependencies

_REPO_ROOT = Path(__file__).resolve().parents[1]

_WORKSPACE_ID = uuid4()
_TASK_ID = uuid4()
_PROFILE_ID = uuid4()


# --- the durable-state harness ------------------------------------------------------


@dataclass
class _DurableState:
    """The committed durable rows the handoff routes re-derive from, per stage."""

    task: Any = None
    workspace: Any = None
    loops: list[Any] = field(default_factory=list)
    iterations_by_loop: dict[UUID, list[Any]] = field(default_factory=dict)
    executions: list[Any] = field(default_factory=list)
    send_claims: list[Any] = field(default_factory=list)
    bootstrap_evidence: Any = None
    resumable: Any = None


def _patch_durable_reads(monkeypatch: pytest.MonkeyPatch, state: _DurableState) -> None:
    """Wire every handoff route's durable reads to the live stage state."""
    monkeypatch.setattr(
        task_workflow.task_records,
        "get_task",
        lambda _pool, *, task_id: (
            state.task if state.task is not None and state.task.id == task_id else None
        ),
    )
    monkeypatch.setattr(
        task_workflow.task_records,
        "get_task_planning_bootstrap_evidence",
        lambda _pool, *, task_id: state.bootstrap_evidence,
    )
    monkeypatch.setattr(task_workflow, "get_workspace", lambda _pool, workspace_id: state.workspace)
    monkeypatch.setattr(
        task_workflow,
        "find_resumable_bootstrap_continuation",
        lambda _pool, *, workspace_id, task_id: state.resumable,
    )
    monkeypatch.setattr(
        task_workflow.review_records,
        "list_task_review_loops",
        lambda _pool, *, task_id: state.loops,
    )
    monkeypatch.setattr(
        task_workflow.review_records,
        "list_review_loop_iterations",
        lambda _pool, *, review_loop_id: state.iterations_by_loop.get(review_loop_id, []),
    )
    monkeypatch.setattr(
        task_workflow.execution_records,
        "list_task_executions",
        lambda _pool, *, task_id: state.executions,
    )
    monkeypatch.setattr(
        task_workflow.send_claim_records,
        "list_task_runtime_send_claims",
        lambda _pool, *, task_id: state.send_claims,
    )


def _patch_recording_coordinator(
    monkeypatch: pytest.MonkeyPatch, coordinates: list[dict[str, Any]]
) -> None:
    """Record each coordinated continuation and run its Level 1 handoff once."""

    def recording_coordinate(_pool: Any, **kwargs: Any) -> Any:
        coordinates.append(dict(kwargs))
        kwargs["invoke_level_one"](InvocationAttemptEvidence())
        return None

    monkeypatch.setattr(task_workflow, "coordinate_task_workflow_operation", recording_coordinate)


def _task(**overrides: Any) -> Any:
    fields: dict[str, Any] = {
        "id": _TASK_ID,
        "workspace_id": _WORKSPACE_ID,
        "state_token": uuid4(),
        "status": TaskStatus.PLANNING,
        "current_plan_revision_id": None,
        "current_owner_gate_id": None,
        "canonical_feature_branch": None,
    }
    fields.update(overrides)
    return types.SimpleNamespace(**fields)


def _workspace() -> Any:
    return types.SimpleNamespace(id=_WORKSPACE_ID, owner_profile_id=_PROFILE_ID)


def _review_loop(purpose: ReviewLoopPurpose) -> Any:
    return types.SimpleNamespace(
        id=uuid4(), workspace_id=_WORKSPACE_ID, purpose=purpose, status=ReviewLoopStatus.OPEN
    )


def _iteration(**overrides: Any) -> Any:
    fields: dict[str, Any] = {
        "id": uuid4(),
        "outcome": None,
        "plan_revision_id": None,
        "task_pull_request_id": None,
    }
    fields.update(overrides)
    return types.SimpleNamespace(**fields)


def _recorder(calls: list[dict[str, Any]], *positional: str) -> Any:
    """A recording service fake for ``(pool[, github[, resolver]], **kwargs)`` seams."""

    def record(*args: Any, **kwargs: Any) -> Any:
        captured: dict[str, Any] = dict(kwargs)
        for name, value in zip(positional, args, strict=True):
            captured[name] = value
        calls.append(captured)
        return types.SimpleNamespace()

    return record


# --- the compact regular-mode Producer↔Reviewer handoff walk ------------------------


def test_the_planning_seam_walks_readiness_prepare_plan_and_the_review_revise_loop(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """One regular-mode walk over the planning family's handoffs (#340): the
    settled initialization readiness submits the resumed bootstrap, the settled
    PREPARE runs the same-session transition and submits the next bootstrap, a
    settled initial PLAN submits the typed REVIEW round, an in-budget
    CHANGES_REQUESTED dispatches the detached Producer REVISE, the settled
    REVISE submits the next REVIEW, and the accepted review presents the
    exact-plan IMPLEMENTATION_AUTHORIZATION. Every continuation is re-derived
    from committed state through the ONE handoff; no blocking model call runs."""
    dependencies, submitter = make_dependencies()
    handoff = task_workflow.build_detached_completion_handoff(dependencies)
    state = _DurableState()
    _patch_durable_reads(monkeypatch, state)
    coordinates: list[dict[str, Any]] = []
    _patch_recording_coordinator(monkeypatch, coordinates)
    revise_dispatches: list[dict[str, Any]] = []
    monkeypatch.setattr(
        task_workflow,
        "dispatch_planning_revise_detached",
        _recorder(revise_dispatches, "pool", "github"),
    )
    presentations: list[dict[str, Any]] = []
    monkeypatch.setattr(
        task_workflow,
        "open_implementation_authorization",
        _recorder(presentations, "pool", "github"),
    )

    def hand_off(operation: PendingTaskRuntimeInteractionOperation) -> None:
        handoff.handoff_settled_completion(
            workspace_id=_WORKSPACE_ID, task_id=_TASK_ID, operation=operation
        )

    # 1. The settled initialization readiness → the resumed bootstrap submission.
    task = _task()
    state.task = task
    state.workspace = _workspace()
    state.resumable = types.SimpleNamespace(task=task, owner_profile_id=_PROFILE_ID)
    hand_off(PendingTaskRuntimeInteractionOperation.SESSION_INITIALIZATION)
    assert [s.kind for s in submitter.submissions] == [
        OperationKind.TASK_PLANNING_BOOTSTRAP_AND_INITIAL_PLAN
    ]
    state.resumable = None

    # 2. The settled workspace PREPARE → the same-session transition continuation
    #    under the F5b coordinator, then the next typed bootstrap invocation.
    state.bootstrap_evidence = _task(stage=TaskPlanningBootstrapStage.PREPARED)
    transitions: list[dict[str, Any]] = []
    monkeypatch.setattr(
        task_workflow,
        "complete_settled_bootstrap_prepare_transition",
        _recorder(transitions, "pool", "runtime_composition"),
    )
    hand_off(PendingTaskRuntimeInteractionOperation.PLANNING_BOOTSTRAP_PREPARE)
    assert len(transitions) == 1
    assert coordinates[-1]["expected_status"] is TaskStatus.PLANNING
    assert submitter.submissions[-1].kind is OperationKind.TASK_PLANNING_BOOTSTRAP_AND_INITIAL_PLAN
    state.bootstrap_evidence = None

    # 3. The settled initial PLAN → the next typed planning REVIEW round.
    first_revision = uuid4()
    task.current_plan_revision_id = first_revision
    hand_off(PendingTaskRuntimeInteractionOperation.INITIAL_PLAN_DISPATCH)
    assert submitter.submissions[-1].kind is OperationKind.TASK_PLANNING_REVIEW
    planning_payload = submitter.submissions[-1].payload
    assert isinstance(planning_payload, TaskPlanningReviewPayload)
    assert planning_payload.task.task_id == str(_TASK_ID)
    assert planning_payload.profile_id == str(_PROFILE_ID)

    # 4. The settled REVIEW with an in-budget CHANGES_REQUESTED → the detached
    #    Producer REVISE dispatch on the persisted iteration.
    planning_loop = _review_loop(ReviewLoopPurpose.PLANNING)
    changes = _iteration(outcome=ReviewOutcome.CHANGES_REQUESTED, plan_revision_id=first_revision)
    state.loops = [planning_loop]
    state.iterations_by_loop = {planning_loop.id: [changes]}
    hand_off(PendingTaskRuntimeInteractionOperation.REVIEWER_REVIEW)
    assert len(revise_dispatches) == 1
    assert revise_dispatches[0]["review_iteration_id"] is changes.id
    assert revise_dispatches[0]["prior_plan_revision_id"] == first_revision
    assert revise_dispatches[0]["expected_state_token"] == task.state_token
    assert coordinates[-1]["expected_status"] is TaskStatus.PLANNING

    # 5. The settled REVISE → the next typed planning REVIEW round.
    second_revision = uuid4()
    task.current_plan_revision_id = second_revision
    hand_off(PendingTaskRuntimeInteractionOperation.PLANNING_REVISE_DISPATCH)
    assert submitter.submissions[-1].kind is OperationKind.TASK_PLANNING_REVIEW

    # 6. The settled REVIEW with an ACCEPTED outcome → the exact-plan
    #    IMPLEMENTATION_AUTHORIZATION presentation (never an approval).
    accepted = _iteration(outcome=ReviewOutcome.ACCEPTED, plan_revision_id=second_revision)
    state.iterations_by_loop = {planning_loop.id: [changes, accepted]}
    hand_off(PendingTaskRuntimeInteractionOperation.REVIEWER_REVIEW)
    assert len(presentations) == 1
    assert coordinates[-1]["expected_status"] is TaskStatus.PLANNING

    # The whole walk ran through the ONE handoff with no other submissions.
    assert [s.kind for s in submitter.submissions] == [
        OperationKind.TASK_PLANNING_BOOTSTRAP_AND_INITIAL_PLAN,
        OperationKind.TASK_PLANNING_BOOTSTRAP_AND_INITIAL_PLAN,
        OperationKind.TASK_PLANNING_REVIEW,
        OperationKind.TASK_PLANNING_REVIEW,
    ]


# --- the implementation → PR_AUTHORIZATION → PR_COMPOSE → PR_REVIEW chain -----------


def test_the_pr_seam_walks_implementation_authorization_composition_review_and_remediation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """One regular-mode walk over the implementation/PR family's handoffs (#340):
    the settled IMPLEMENTATION presents the exact-head PR_AUTHORIZATION gate,
    the settled PR_COMPOSE publishes the bounded composition and submits the
    typed PR REVIEW, the PR REVIEW's in-budget change request dispatches the
    detached E9 remediation, and the settled remediation's GitHub-confirmed new
    head submits the next PR REVIEW. No blocking model call and no recursive
    in-process chain: every continuation re-derives from committed state."""
    dependencies, submitter = make_dependencies()
    handoff = task_workflow.build_detached_completion_handoff(dependencies)
    state = _DurableState()
    _patch_durable_reads(monkeypatch, state)
    coordinates: list[dict[str, Any]] = []
    _patch_recording_coordinator(monkeypatch, coordinates)
    pr_presentations: list[dict[str, Any]] = []
    monkeypatch.setattr(
        task_workflow, "open_pr_authorization", _recorder(pr_presentations, "pool", "github")
    )
    publications: list[dict[str, Any]] = []
    remediation_dispatches: list[dict[str, Any]] = []
    monkeypatch.setattr(
        task_workflow, "dispatch_pr_remediation_detached", _recorder(remediation_dispatches, "pool")
    )

    def hand_off(
        operation: PendingTaskRuntimeInteractionOperation,
        validated_result: PrResult | None = None,
    ) -> None:
        handoff.handoff_settled_completion(
            workspace_id=_WORKSPACE_ID,
            task_id=_TASK_ID,
            operation=operation,
            validated_result=validated_result,
        )

    # 1. The settled IMPLEMENTATION → the exact-head PR_AUTHORIZATION presentation.
    task = _task(status=TaskStatus.IMPLEMENTING)
    state.task = task
    state.workspace = _workspace()
    hand_off(PendingTaskRuntimeInteractionOperation.IMPLEMENTATION_EXECUTION)
    assert len(pr_presentations) == 1
    assert coordinates[-1]["expected_status"] is TaskStatus.IMPLEMENTING

    # 2. The settled PR_COMPOSE → the bounded publication in the same completion
    #    processing, then the typed PR REVIEW submission from committed state.
    task.status = TaskStatus.WAITING_FOR_OWNER
    task.canonical_feature_branch = "feat/issue-340"
    claim = types.SimpleNamespace(
        kind=TaskRuntimeSendClaimKind.PR_COMPOSE_DISPATCH,
        status=TaskRuntimeSendClaimStatus.SETTLED,
        owner_gate_id=uuid4(),
    )
    state.send_claims = [claim]
    published = types.SimpleNamespace(
        approving_profile_id=_PROFILE_ID,
        pull_request=types.SimpleNamespace(id=uuid4()),
        task=types.SimpleNamespace(state_token=uuid4()),
    )

    def recording_publish(pool: Any, github: Any, resolver: Any, **kwargs: Any) -> Any:
        publications.append(dict(kwargs))
        return published

    monkeypatch.setattr(task_workflow, "publish_settled_task_pr_composition", recording_publish)
    validated = PrResult(title="T", body="B")
    hand_off(PendingTaskRuntimeInteractionOperation.PR_COMPOSE_DISPATCH, validated_result=validated)
    assert len(publications) == 1
    assert publications[0]["pr_result"] is validated
    assert publications[0]["owner_gate_id"] == claim.owner_gate_id
    assert submitter.submissions[-1].kind is OperationKind.TASK_PR_REVIEW
    compose_review_payload = submitter.submissions[-1].payload
    assert isinstance(compose_review_payload, TaskPrReviewPayload)
    assert compose_review_payload.task_pull_request_id == str(published.pull_request.id)
    assert compose_review_payload.profile_id == str(_PROFILE_ID)

    # 3. The settled PR REVIEW with an in-budget CHANGES_REQUESTED → the detached
    #    E9 remediation dispatch on the exact addressed iteration.
    task.status = TaskStatus.REVIEWING
    pr_loop = _review_loop(ReviewLoopPurpose.PR_REVIEW)
    changes = _iteration(outcome=ReviewOutcome.CHANGES_REQUESTED, task_pull_request_id=uuid4())
    state.loops = [pr_loop]
    state.iterations_by_loop = {pr_loop.id: [changes]}
    state.executions = []
    hand_off(PendingTaskRuntimeInteractionOperation.REVIEWER_REVIEW)
    assert len(remediation_dispatches) == 1
    assert remediation_dispatches[0]["addressed_iteration"] is changes
    assert remediation_dispatches[0]["expected_state_token"] == task.state_token
    assert coordinates[-1]["expected_status"] is TaskStatus.REVIEWING

    # 4. The settled remediation → the verified new head routes the next typed
    #    PR REVIEW through F1; a None verification owns no continuation.
    verification = types.SimpleNamespace(
        task=types.SimpleNamespace(workspace_id=_WORKSPACE_ID, id=_TASK_ID, state_token=uuid4()),
        pull_request=types.SimpleNamespace(id=changes.task_pull_request_id),
    )
    monkeypatch.setattr(
        task_workflow,
        "verify_settled_pr_remediation_push",
        lambda pool, github, **kwargs: verification,
    )
    hand_off(PendingTaskRuntimeInteractionOperation.PR_REMEDIATION_EXECUTION)
    assert submitter.submissions[-1].kind is OperationKind.TASK_PR_REVIEW
    remediation_review_payload = submitter.submissions[-1].payload
    assert isinstance(remediation_review_payload, TaskPrReviewPayload)
    assert remediation_review_payload.task_pull_request_id == str(changes.task_pull_request_id)

    # The whole PR walk made exactly two typed submissions, both PR REVIEWs.
    assert [s.kind for s in submitter.submissions] == [
        OperationKind.TASK_PR_REVIEW,
        OperationKind.TASK_PR_REVIEW,
    ]


# --- the discussion handoff, route coverage, and representative stale/refusal -------


def test_the_discussion_handoff_rederives_the_producer_continuation_from_committed_state(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The settled Owner↔Reviewer discussion routes through the same committed
    state derivations the ordinary review decisions use (#339/#340): a planning
    Task whose latest finalized iteration is an ACCEPTED review presents the
    exact-plan IMPLEMENTATION_AUTHORIZATION, while a Task still carrying a
    current OwnerGate is owned by that gate's own seam and sends no prompt."""
    dependencies, submitter = make_dependencies()
    handoff = task_workflow.build_detached_completion_handoff(dependencies)
    state = _DurableState()
    _patch_durable_reads(monkeypatch, state)
    coordinates: list[dict[str, Any]] = []
    _patch_recording_coordinator(monkeypatch, coordinates)
    presentations: list[dict[str, Any]] = []
    monkeypatch.setattr(
        task_workflow,
        "open_implementation_authorization",
        _recorder(presentations, "pool", "github"),
    )
    task = _task()
    state.task = task
    state.workspace = _workspace()
    revision = uuid4()
    task.current_plan_revision_id = revision
    planning_loop = _review_loop(ReviewLoopPurpose.PLANNING)
    accepted = _iteration(outcome=ReviewOutcome.ACCEPTED, plan_revision_id=revision)
    state.loops = [planning_loop]
    state.iterations_by_loop = {planning_loop.id: [accepted]}
    handoff.handoff_settled_completion(
        workspace_id=_WORKSPACE_ID,
        task_id=_TASK_ID,
        operation=PendingTaskRuntimeInteractionOperation.OWNER_REVIEWER_DISCUSSION,
    )
    assert len(presentations) == 1
    assert submitter.submissions == []

    # The gate-owned variant: the current OwnerGate owns the workflow, so the
    # same settled family hands nothing and sends no Producer prompt.
    presentations.clear()
    task.current_owner_gate_id = uuid4()
    handoff.handoff_settled_completion(
        workspace_id=_WORKSPACE_ID,
        task_id=_TASK_ID,
        operation=PendingTaskRuntimeInteractionOperation.OWNER_REVIEWER_DISCUSSION,
    )
    assert presentations == []
    assert submitter.submissions == []


def test_every_detached_settled_family_has_a_wired_handoff_route(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The closed handoff route coverage (#340): every detached settled family
    routes (its re-derivation simply finds nothing on absent state), while the
    two non-detached vocabulary members — the bootstrap transition phase and
    the F8g session resume — fail closed as unsettled-by-observation families."""
    dependencies, submitter = make_dependencies()
    handoff = task_workflow.build_detached_completion_handoff(dependencies)
    _patch_durable_reads(monkeypatch, _DurableState())
    routed = (
        PendingTaskRuntimeInteractionOperation.SESSION_INITIALIZATION,
        PendingTaskRuntimeInteractionOperation.IMPLEMENTATION_EXECUTION,
        PendingTaskRuntimeInteractionOperation.PLANNING_BOOTSTRAP_PREPARE,
        PendingTaskRuntimeInteractionOperation.INITIAL_PLAN_DISPATCH,
        PendingTaskRuntimeInteractionOperation.PLANNING_REVISE_DISPATCH,
        PendingTaskRuntimeInteractionOperation.REVIEWER_REVIEW,
        PendingTaskRuntimeInteractionOperation.PR_REMEDIATION_EXECUTION,
        PendingTaskRuntimeInteractionOperation.PR_COMPOSE_DISPATCH,
        PendingTaskRuntimeInteractionOperation.OWNER_REVIEWER_DISCUSSION,
    )
    for operation in routed:
        handoff.handoff_settled_completion(
            workspace_id=_WORKSPACE_ID,
            task_id=_TASK_ID,
            operation=operation,
            validated_result=PrResult(title="t", body="b")
            if operation is PendingTaskRuntimeInteractionOperation.PR_COMPOSE_DISPATCH
            else None,
        )
    assert submitter.submissions == []
    for operation in (
        PendingTaskRuntimeInteractionOperation.PLANNING_BOOTSTRAP_TRANSITION,
        PendingTaskRuntimeInteractionOperation.SESSION_RESUME,
    ):
        with pytest.raises(ConflictError):
            handoff.handoff_settled_completion(
                workspace_id=_WORKSPACE_ID, task_id=_TASK_ID, operation=operation
            )


def test_a_moved_on_subject_hands_no_continuation_and_no_prompt(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Representative stale seam behavior (#340): a moved-on subject at the
    handoff settles nothing further — no dispatch, no typed submission — and
    the durable state owns recovery, never a blind re-enqueue."""
    dependencies, submitter = make_dependencies()
    handoff = task_workflow.build_detached_completion_handoff(dependencies)
    state = _DurableState()
    _patch_durable_reads(monkeypatch, state)
    coordinates: list[dict[str, Any]] = []
    _patch_recording_coordinator(monkeypatch, coordinates)
    revise_dispatches: list[dict[str, Any]] = []
    monkeypatch.setattr(
        task_workflow,
        "dispatch_planning_revise_detached",
        _recorder(revise_dispatches, "pool", "github"),
    )
    task = _task()
    state.task = task
    state.workspace = _workspace()

    # A REVIEWER_REVIEW on a gate-owned Task is owned by the gate's own seam.
    task.current_owner_gate_id = uuid4()
    handoff.handoff_settled_completion(
        workspace_id=_WORKSPACE_ID,
        task_id=_TASK_ID,
        operation=PendingTaskRuntimeInteractionOperation.REVIEWER_REVIEW,
    )
    assert revise_dispatches == []
    assert submitter.submissions == []

    # A superseded iteration is never a continuation subject.
    task.current_owner_gate_id = None
    task.current_plan_revision_id = uuid4()
    planning_loop = _review_loop(ReviewLoopPurpose.PLANNING)
    stale = _iteration(outcome=ReviewOutcome.CHANGES_REQUESTED, plan_revision_id=uuid4())
    state.loops = [planning_loop]
    state.iterations_by_loop = {planning_loop.id: [stale]}
    handoff.handoff_settled_completion(
        workspace_id=_WORKSPACE_ID,
        task_id=_TASK_ID,
        operation=PendingTaskRuntimeInteractionOperation.REVIEWER_REVIEW,
    )
    assert revise_dispatches == []
    assert submitter.submissions == []

    # A plan-result settlement on a moved-on coarse state routes nothing.
    task.status = TaskStatus.IMPLEMENTING
    handoff.handoff_settled_completion(
        workspace_id=_WORKSPACE_ID,
        task_id=_TASK_ID,
        operation=PendingTaskRuntimeInteractionOperation.INITIAL_PLAN_DISPATCH,
    )
    assert revise_dispatches == []
    assert submitter.submissions == []


def test_the_pr_compose_route_fails_closed_without_the_settlement_presentation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The one deliberate in-memory `pr_result` parameter is fail-closed (#337):
    without the settlement's validated presentation the publication continuation
    is not available, and no title/body is ever invented or re-derived."""
    dependencies, _ = make_dependencies()
    handoff = task_workflow.build_detached_completion_handoff(dependencies)
    _patch_durable_reads(monkeypatch, _DurableState())
    publications: list[dict[str, Any]] = []
    monkeypatch.setattr(
        task_workflow,
        "publish_settled_task_pr_composition",
        _recorder(publications, "pool", "github", "user_token_resolver"),
    )
    with pytest.raises(ConflictError):
        handoff.handoff_settled_completion(
            workspace_id=_WORKSPACE_ID,
            task_id=_TASK_ID,
            operation=PendingTaskRuntimeInteractionOperation.PR_COMPOSE_DISPATCH,
        )
    with pytest.raises(ConflictError):
        handoff.handoff_settled_completion(
            workspace_id=_WORKSPACE_ID,
            task_id=_TASK_ID,
            operation=PendingTaskRuntimeInteractionOperation.IMPLEMENTATION_EXECUTION,
            validated_result=PrResult(title="t", body="b"),
        )
    assert publications == []


def test_a_refused_or_uncertain_continuation_submission_is_transport_information_only(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A failed or uncertain follow-on submission is not success and never
    permits a blind re-enqueue (#340): the handoff does not raise, submits
    exactly once per settled family, and leaves the workflow at its committed
    durable state — the durable state owns recovery."""
    dependencies, submitter = make_dependencies()
    submitter.outcomes = [
        SubmissionResult.refused(SubmissionRefusalReason.EXECUTOR_SATURATED),
        SubmissionResult.uncertain(),
    ]
    handoff = task_workflow.build_detached_completion_handoff(dependencies)
    state = _DurableState()
    _patch_durable_reads(monkeypatch, state)
    state.task = _task(current_plan_revision_id=uuid4())
    state.workspace = _workspace()
    for _ in range(2):
        handoff.handoff_settled_completion(
            workspace_id=_WORKSPACE_ID,
            task_id=_TASK_ID,
            operation=PendingTaskRuntimeInteractionOperation.INITIAL_PLAN_DISPATCH,
        )
    assert [s.kind for s in submitter.submissions] == [
        OperationKind.TASK_PLANNING_REVIEW,
        OperationKind.TASK_PLANNING_REVIEW,
    ]


# --- the targeted static survey: no ordinary model-call wait remains ----------------


def _function_calls(relative_path: str) -> dict[str, set[str]]:
    """Map every function/method name in one module to the names it calls."""
    tree = ast.parse((_REPO_ROOT / relative_path).read_text())
    calls: dict[str, set[str]] = {}
    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            names = calls.setdefault(node.name, set())
            for sub in ast.walk(node):
                if isinstance(sub, ast.Call):
                    if isinstance(sub.func, ast.Name):
                        names.add(sub.func.id)
                    elif isinstance(sub.func, ast.Attribute):
                        names.add(sub.func.attr)
    return calls


def _caller_roots(calls: dict[str, set[str]], target: str) -> set[str]:
    """The module-local functions that transitively reach ``target`` and are
    themselves reached by no other reaching function — the entry points."""
    reachers = {target}
    changed = True
    while changed:
        changed = False
        for name, called in calls.items():
            if name not in reachers and called & reachers:
                reachers.add(name)
                changed = True
    return {
        name
        for name in reachers
        if not any(other != name and name in calls.get(other, set()) for other in reachers)
    }


def test_blocking_model_turn_helpers_are_reachable_only_from_queue_mode_interims() -> None:
    """The targeted survey (#340): each owning service's blocking model-turn
    helper is reachable only from its documented queue-mode interim entry
    points (until #328) — no regular-mode path reaches a blocking send."""
    sanctioned = (
        (
            "src/openorc/services/task_plan_production.py",
            "_realize_plan_control",
            {"produce_initial_task_plan", "revise_task_plan"},
        ),
        (
            "src/openorc/services/task_planning_review_dispatch.py",
            "_realize_review_control",
            {"dispatch_planning_review"},
        ),
        (
            "src/openorc/services/task_pr_review_dispatch.py",
            "_realize_pr_review_control",
            {"dispatch_pr_review"},
        ),
        (
            "src/openorc/services/task_pr_composition.py",
            "_compose_pr_with_producer",
            {"compose_and_publish_task_pr"},
        ),
        (
            "src/openorc/services/task_pr_remediation.py",
            "_send_remediation_revision",
            {"route_pr_remediation"},
        ),
        (
            "src/openorc/services/task_authorized_implementation_execution.py",
            "_send_and_finalize",
            {"execute_authorized_implementation"},
        ),
    )
    for module, helper, expected_roots in sanctioned:
        actual_roots = _caller_roots(_function_calls(module), helper)
        assert actual_roots == expected_roots, (module, sorted(actual_roots))


def test_the_bootstrap_prepare_split_dispatches_detached_from_its_own_entry() -> None:
    """The workspace-PREPARE realize lives inline in the dual-mode bootstrap
    entry; the survey proves that entry's detached branch exists (#334/#340)."""
    calls = _function_calls("src/openorc/services/task_planning_bootstrap.py")
    assert "_dispatch_detached_bootstrap_prepare" in calls["prepare_task_for_planning"]


def test_the_f10_terminal_cleanup_realize_stays_inside_the_bounded_cleanup() -> None:
    """The F10 cleanup realization (#327, out of #340's scope) stays inside the
    bounded terminal-cleanup boundary and reaches no other module path."""
    calls = _function_calls("src/openorc/services/cline_terminal_cleanup.py")
    assert "realize_control" in calls["cleanup_checkout"]


def test_every_regular_mode_composition_splits_to_its_detached_dispatch() -> None:
    """Each worker composition that carries a blocking model-turn entry also
    calls its owning service's detached dispatch in the regular-mode branch
    (#340): the mode split exists in all five migrated compositions."""
    calls = _function_calls("src/openorc/workers/task_workflow.py")
    splits = (
        (
            "_compose_task_planning_bootstrap_and_initial_plan",
            "dispatch_initial_plan_detached",
            ("produce_initial_task_plan",),
        ),
        (
            "_compose_task_planning_review",
            "dispatch_planning_review_detached",
            ("dispatch_planning_review",),
        ),
        (
            "_compose_task_authorized_implementation",
            "dispatch_authorized_implementation_detached",
            ("execute_authorized_implementation",),
        ),
        (
            "_compose_task_pr_composition",
            "dispatch_pr_compose_detached",
            ("compose_and_publish_task_pr",),
        ),
        ("_compose_task_pr_review", "dispatch_pr_review_detached", ("dispatch_pr_review",)),
    )
    for composition, detached, blocking_entries in splits:
        assert composition in calls, composition
        assert detached in calls[composition], composition
        for entry in blocking_entries:
            assert entry in calls[composition], (composition, entry)


def test_the_f5c_retry_composition_is_the_documented_blocking_exception() -> None:
    """The F5c authorized-implementation retry keeps the blocking E6 external
    mechanics until its own detached migration: nothing submits its operation
    kind until F11 (#257) owns the due-time wakeup, so no regular-mode worker
    waits on it today — and the composition documents that boundary in place."""
    tree = ast.parse((_REPO_ROOT / "src/openorc/workers/task_workflow.py").read_text())
    retry = next(
        node
        for node in ast.walk(tree)
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
        and node.name == "_compose_task_authorized_implementation_retry"
    )
    docstring = ast.get_docstring(retry) or ""
    assert "F11" in docstring and "#257" in docstring
    calls = _function_calls("src/openorc/workers/task_workflow.py")
    assert (
        "execute_authorized_implementation_retry"
        in calls["_compose_task_authorized_implementation_retry"]
    )
    assert not any(
        name.endswith("_detached")
        for name in calls["_compose_task_authorized_implementation_retry"]
    )
