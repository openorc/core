"""Deterministic domain tests for the runtime-send claim models (issue #87).

The closed kind/status vocabularies, the per-kind source-subject form
coherence, the forward-only terminal coherence, and the canonical safe
outcome payload — the transport-independent validation the persistence
boundary and services rely on.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

import pytest

from openorc.domain.connections import WorkflowRole
from openorc.domain.pending_interactions import (
    PendingTaskRuntimeInteraction,
    PendingTaskRuntimeInteractionOperation,
)
from openorc.domain.send_claims import (
    TaskRuntimeSendClaim,
    TaskRuntimeSendClaimKind,
    TaskRuntimeSendClaimStatus,
    canonical_send_claim_outcome,
)

_CLAIM_AT = datetime(2026, 10, 27, 9, 0, 0, tzinfo=UTC)


def _claim(
    *,
    kind: TaskRuntimeSendClaimKind = TaskRuntimeSendClaimKind.INITIAL_PLAN_DISPATCH,
    status: TaskRuntimeSendClaimStatus = TaskRuntimeSendClaimStatus.CLAIMED,
    workspace_id: object = None,
    review_iteration_id: object = None,
    owner_gate_id: object = None,
    subject_head_sha: object = None,
    outcome: Any = None,
    settled_at: object = None,
) -> TaskRuntimeSendClaim:
    return TaskRuntimeSendClaim(
        id=uuid4(),
        workspace_id=workspace_id if workspace_id is not None else uuid4(),  # type: ignore[arg-type]
        task_id=uuid4(),
        kind=kind,
        task_agent_session_id=uuid4(),
        review_iteration_id=review_iteration_id,  # type: ignore[arg-type]
        owner_gate_id=owner_gate_id,  # type: ignore[arg-type]
        subject_head_sha=subject_head_sha,  # type: ignore[arg-type]
        status=status,
        outcome=outcome if outcome is not None else {},
        claimed_at=_CLAIM_AT,
        settled_at=settled_at,  # type: ignore[arg-type]
        updated_at=_CLAIM_AT,
    )


def test_initial_plan_claim_carries_no_source_subject_beyond_the_session() -> None:
    claim = _claim()
    assert claim.kind is TaskRuntimeSendClaimKind.INITIAL_PLAN_DISPATCH
    assert claim.review_iteration_id is None
    assert claim.owner_gate_id is None
    assert claim.subject_head_sha is None


def test_planning_revise_claim_addresses_exactly_one_review_iteration() -> None:
    claim = _claim(
        kind=TaskRuntimeSendClaimKind.PLANNING_REVISE_DISPATCH,
        review_iteration_id=uuid4(),
    )
    assert claim.owner_gate_id is None
    assert claim.subject_head_sha is None

    with pytest.raises(ValueError, match="planning_revise_dispatch"):
        _claim(kind=TaskRuntimeSendClaimKind.PLANNING_REVISE_DISPATCH)

    with pytest.raises(ValueError, match="planning_revise_dispatch"):
        _claim(
            kind=TaskRuntimeSendClaimKind.PLANNING_REVISE_DISPATCH,
            review_iteration_id=uuid4(),
            owner_gate_id=uuid4(),
        )


def test_pr_compose_claim_addresses_exactly_one_gate_and_head() -> None:
    claim = _claim(
        kind=TaskRuntimeSendClaimKind.PR_COMPOSE_DISPATCH,
        owner_gate_id=uuid4(),
        subject_head_sha="0" * 40,
    )
    assert claim.review_iteration_id is None

    with pytest.raises(ValueError, match="pr_compose_dispatch"):
        _claim(kind=TaskRuntimeSendClaimKind.PR_COMPOSE_DISPATCH)

    with pytest.raises(ValueError, match="subject_head_sha"):
        _claim(
            kind=TaskRuntimeSendClaimKind.PR_COMPOSE_DISPATCH,
            owner_gate_id=uuid4(),
            subject_head_sha="   ",
        )


def test_terminal_coherence_settled_at_is_written_with_the_terminal_status() -> None:
    settled = _claim(
        status=TaskRuntimeSendClaimStatus.SETTLED,
        outcome={"plan_revision_id": str(uuid4())},
        settled_at=_CLAIM_AT,
    )
    assert settled.status is TaskRuntimeSendClaimStatus.SETTLED

    known = _claim(status=TaskRuntimeSendClaimStatus.KNOWN_NON_DELIVERY, settled_at=_CLAIM_AT)
    assert known.status is TaskRuntimeSendClaimStatus.KNOWN_NON_DELIVERY

    # A claimed claim never carries a settlement stamp, and a terminal claim
    # never omits one — the database CHECK backstops the same invariant.
    with pytest.raises(ValueError, match="settlement instant"):
        _claim(settled_at=_CLAIM_AT)
    with pytest.raises(ValueError, match="settlement instant"):
        _claim(status=TaskRuntimeSendClaimStatus.SETTLED)


def test_outcome_is_canonicalized_and_rejects_non_json_payloads() -> None:
    canonical = canonical_send_claim_outcome({"failure_kind": "runtime_pre_effect"})
    assert dict(canonical) == {"failure_kind": "runtime_pre_effect"}
    # Tuples normalize to lists; the frozen view is exactly what jsonb stores.
    tuple_view = canonical_send_claim_outcome({"subjects": ("a", "b")})
    assert dict(tuple_view) == {"subjects": ["a", "b"]}

    with pytest.raises(ValueError, match="must be a mapping"):
        canonical_send_claim_outcome(["not", "a", "mapping"])
    with pytest.raises(ValueError, match="JSON-representable"):
        canonical_send_claim_outcome({"bad": object()})
    with pytest.raises(ValueError, match="NaN"):
        canonical_send_claim_outcome({"bad": float("nan")})


def test_non_uuid_identities_are_rejected() -> None:
    with pytest.raises(ValueError, match="workspace_id"):
        _claim(workspace_id="not-a-uuid")  # type: ignore[arg-type]


def test_pending_interaction_subject_is_canonical_and_frozen() -> None:
    interaction = PendingTaskRuntimeInteraction(
        operation=PendingTaskRuntimeInteractionOperation.REVIEWER_REVIEW,
        workspace_id=uuid4(),
        task_id=uuid4(),
        role=WorkflowRole.REVIEWER,
        task_agent_session_id=None,
        subject={"review_iteration_id": str(uuid4()), "iteration_number": 2},
        started_at=None,
    )
    # None started_at is the honest unknown (residue claimed before the
    # start-instant evidence existed), and the subject is a canonical frozen
    # mapping.
    assert interaction.started_at is None
    frozen_subject = interaction.subject
    with pytest.raises(TypeError):
        frozen_subject["review_iteration_id"] = "rewritten"  # type: ignore[index]


def test_pending_interaction_validates_operation_role_and_instants() -> None:
    with pytest.raises(ValueError, match="operation"):
        PendingTaskRuntimeInteraction(
            operation="planning_bootstrap_prepare",  # type: ignore[arg-type]
            workspace_id=uuid4(),
            task_id=uuid4(),
            role=WorkflowRole.PRODUCER,
            task_agent_session_id=None,
            subject={},
            started_at=None,
        )
    with pytest.raises(ValueError, match="role"):
        PendingTaskRuntimeInteraction(
            operation=PendingTaskRuntimeInteractionOperation.INITIAL_PLAN_DISPATCH,
            workspace_id=uuid4(),
            task_id=uuid4(),
            role="producer",  # type: ignore[arg-type]
            task_agent_session_id=None,
            subject={},
            started_at=None,
        )
    naive = datetime(2026, 10, 27, 9, 0, 0)
    with pytest.raises(ValueError, match="timezone-aware"):
        PendingTaskRuntimeInteraction(
            operation=PendingTaskRuntimeInteractionOperation.INITIAL_PLAN_DISPATCH,
            workspace_id=uuid4(),
            task_id=uuid4(),
            role=WorkflowRole.PRODUCER,
            task_agent_session_id=uuid4(),
            subject={},
            started_at=naive,
        )
