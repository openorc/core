"""Deterministic tests for the operational-failure domain models (issue #251).

The vocabulary and model invariants the triage service and persistence rely
on: the closed error-code/evidence/source/identity-kind vocabularies, the
episode's terminal-disposition field shape, the attempt's 1..3 ordinal and
timezone-aware instants, and the bounded canonical diagnostic context.

No database, runtime, or network access.
"""

from __future__ import annotations

from datetime import UTC, datetime
from types import MappingProxyType
from uuid import uuid4

import pytest

from openorc.domain.operational_failures import (
    FailureEffectEvidence,
    OperationalFailureAttempt,
    OperationalFailureCode,
    OperationalFailureDomainError,
    OperationalFailureEpisode,
    OperationalFailureSourceKind,
    OriginalAttemptKind,
    canonical_bounded_failure_context,
    operational_failure_attempt_field_names,
    operational_failure_episode_field_names,
)
from openorc.domain.pending_interactions import PendingTaskRuntimeInteractionOperation

_OBSERVED = datetime(2026, 10, 30, 12, 0, 0, tzinfo=UTC)


def _episode(**overrides: object) -> OperationalFailureEpisode:
    values: dict[str, object] = {
        "id": uuid4(),
        "workspace_id": uuid4(),
        "task_id": uuid4(),
        "operation_family": PendingTaskRuntimeInteractionOperation.IMPLEMENTATION_EXECUTION,
        "original_attempt_kind": OriginalAttemptKind.EXECUTION,
        "original_attempt_id": uuid4(),
        "subject": {"execution_id": "x"},
        "escalated_task_block_id": None,
        "created_at": _OBSERVED,
    }
    values.update(overrides)
    return OperationalFailureEpisode(**values)  # type: ignore[arg-type]


def _attempt(**overrides: object) -> OperationalFailureAttempt:
    values: dict[str, object] = {
        "id": uuid4(),
        "workspace_id": uuid4(),
        "task_id": uuid4(),
        "episode_id": uuid4(),
        "ordinal": 1,
        "source_kind": OperationalFailureSourceKind.ORIGINAL,
        "source_attempt_id": uuid4(),
        "error_code": OperationalFailureCode.RUNTIME_UNAVAILABLE,
        "not_before": _OBSERVED,
        "created_at": _OBSERVED,
    }
    values.update(overrides)
    return OperationalFailureAttempt(**values)  # type: ignore[arg-type]


def test_the_error_code_vocabulary_is_the_closed_taxonomy_set_with_fallback() -> None:
    assert {code.value for code in OperationalFailureCode} == {
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
        "unknown",
    }


def test_the_evidence_vocabulary_distinguishes_proven_pre_effect_from_uncertainty() -> None:
    assert {evidence.value for evidence in FailureEffectEvidence} == {
        "pre_effect_not_applied",
        "effect_unclassified",
        "known_failed",
    }


def test_the_episode_carries_exactly_its_own_facts() -> None:
    assert operational_failure_episode_field_names() == frozenset(
        {
            "id",
            "workspace_id",
            "task_id",
            "operation_family",
            "original_attempt_kind",
            "original_attempt_id",
            "subject",
            "escalated_task_block_id",
            "created_at",
        }
    )


def test_the_attempt_carries_exactly_its_own_facts() -> None:
    assert operational_failure_attempt_field_names() == frozenset(
        {
            "id",
            "workspace_id",
            "task_id",
            "episode_id",
            "ordinal",
            "source_kind",
            "source_attempt_id",
            "error_code",
            "not_before",
            "created_at",
        }
    )


def test_an_attempt_ordinal_must_be_between_one_and_three() -> None:
    for ordinal in (0, 4, -1):
        with pytest.raises(OperationalFailureDomainError):
            _attempt(ordinal=ordinal)


def test_attempt_instants_must_be_timezone_aware() -> None:
    with pytest.raises(OperationalFailureDomainError):
        _attempt(not_before=datetime(2026, 10, 30, 12, 0, 0))


def test_the_episode_subject_is_canonicalized_and_validated() -> None:
    episode = _episode(subject={"a": ("x", "y")})
    assert episode.subject == MappingProxyType({"a": ["x", "y"]})
    with pytest.raises(OperationalFailureDomainError):
        _episode(subject={"bad": {1: "non-string-key"}})


def test_the_bounded_context_canonicalizes_and_enforces_bounds() -> None:
    context = canonical_bounded_failure_context({"code": "runtime_unavailable"})
    assert context == MappingProxyType({"code": "runtime_unavailable"})
    with pytest.raises(OperationalFailureDomainError):
        canonical_bounded_failure_context(["not", "a", "mapping"])
    with pytest.raises(OperationalFailureDomainError):
        canonical_bounded_failure_context({"nan": float("nan")})
    with pytest.raises(OperationalFailureDomainError):
        canonical_bounded_failure_context({str(i): i for i in range(17)})
    deep: dict[str, object] = {"l1": {"l2": {"l3": {"l4": {"l5": "too deep"}}}}}
    with pytest.raises(OperationalFailureDomainError):
        canonical_bounded_failure_context(deep)
    with pytest.raises(OperationalFailureDomainError):
        canonical_bounded_failure_context({"big": "x" * 5000})
