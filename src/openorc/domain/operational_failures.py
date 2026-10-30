"""Operational-failure disposition domain models (F4, issue #251).

The stable typed vocabulary and durable facts of the Level 2
operational-failure disposition: one failure episode per original durable
logical operation/attempt identity, at most three durably authorized
automatic additional attempts with persisted exponential-backoff due times,
and the episode's terminal escalation disposition referencing the exact
exceptional TaskBlock (#258) that blocked the Task.

- ``OperationalFailureCode`` is the stable machine-readable error-code
  vocabulary the triage service classifies on. Its members mirror the
  existing concrete error taxonomies as inputs to a small explicit
  classification — the normalized Agent Runtime adapter taxonomy (#67) and
  the external-operation application errors — plus the mandatory ``unknown``
  fallback. It is deliberately not an error catalogue: no member is inferred
  from exception-message text, queue status, raw provider HTTP errors, or a
  caller-supplied retryable assertion.
- ``FailureEffectEvidence`` is the classified outcome/effect evidence class
  of one failure report: a positively proven pre-effect non-application is
  the only evidence that can ever support a safe automatic retry, and only
  together with an operation whose concrete semantics prove repeat safety;
  uncertainty is neither success nor known failure.
- ``OriginalAttemptKind`` names the existing durable identity family a
  failure episode roots at (an F2 runtime-send claim, the E1b planning
  bootstrap claim token, the E3a reserved ReviewIteration, the E6/E9
  Execution row, or an owner-control intent). A Task state token or mutable
  phase is never an episode root.
- Operation families reuse the F2 pending-interaction operation vocabulary
  (the closed set of the automated workflow's claimed consequential
  interactions); a dedicated parallel vocabulary is deliberately not
  introduced.
- ``OperationalFailureEpisode`` carries the episode's scope, family, original
  identity, canonical safe subject, and its terminal escalation disposition:
  ``escalated_task_block_id`` references the exact unresolved TaskBlock the
  F3 exceptional blocking service installed, is set exactly once, and is
  never cleared. A disposed episode is disposed history even after the
  block is later resolved.
- ``OperationalFailureAttempt`` is one durably authorized automatic
  additional attempt: its own UUID, the 1..3 ordinal within its episode,
  the exact failed attempt it succeeds, the classified error code, and the
  persisted UTC ``not_before`` instant. Attempts are forward-only history
  for replay prevention and later consumption; they are never rewritten.

This module carries transport-independent validation only. It performs no
disposition decisions, no retry policy, and no Task state transitions.
"""

from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import dataclass, fields
from datetime import datetime
from enum import StrEnum
from types import MappingProxyType
from uuid import UUID

from openorc.domain.intents import canonical_intent_outcome
from openorc.domain.pending_interactions import PendingTaskRuntimeInteractionOperation

__all__ = [
    "FailureEffectEvidence",
    "OperationalFailureAttempt",
    "OperationalFailureCode",
    "OperationalFailureDomainError",
    "OperationalFailureEpisode",
    "OperationalFailureSourceKind",
    "OriginalAttemptKind",
    "canonical_bounded_failure_context",
    "canonical_failure_subject",
    "operational_failure_attempt_field_names",
    "operational_failure_episode_field_names",
]


class OperationalFailureDomainError(Exception):
    """Raised when an operational-failure domain invariant is violated."""


class OperationalFailureCode(StrEnum):
    """The stable machine-readable failure-code vocabulary.

    Exactly the concrete taxonomy classes the Level 1 paths produce, plus
    the stable fallback. Every member escalates unless the triage service's
    explicit eligibility matrix says otherwise; the vocabulary itself decides
    nothing.
    """

    RUNTIME_UNAVAILABLE = "runtime_unavailable"
    RUNTIME_TIMEOUT = "runtime_timeout"
    RUNTIME_DELIVERY_UNCERTAIN = "runtime_delivery_uncertain"
    RUNTIME_TRANSPORT = "runtime_transport"
    RUNTIME_PROTOCOL = "runtime_protocol"
    AGENT_SESSION_LOST = "agent_session_lost"
    RUNTIME_CONFIGURATION_REJECTED = "runtime_configuration_rejected"
    EXTERNAL_OPERATION_FAILED = "external_operation_failed"
    EXTERNAL_OPERATION_UNCERTAIN = "external_operation_uncertain"
    EXTERNAL_OPERATION_REJECTED = "external_operation_rejected"
    UNKNOWN = "unknown"


class FailureEffectEvidence(StrEnum):
    """The classified outcome/effect evidence class of one failure report.

    ``PRE_EFFECT_NOT_APPLIED`` is positive proof the operation failed before
    any effect; it is the only evidence that can support a safe automatic
    retry, and only for an operation whose concrete semantics independently
    prove repeat safety. ``EFFECT_UNCLASSIFIED`` is uncertainty. A known
    non-success without positive pre-effect proof is ``KNOWN_FAILED`` —
    never a safe-retry classification by itself.
    """

    PRE_EFFECT_NOT_APPLIED = "pre_effect_not_applied"
    EFFECT_UNCLASSIFIED = "effect_unclassified"
    KNOWN_FAILED = "known_failed"


class OperationalFailureSourceKind(StrEnum):
    """Which attempt a failure report is about.

    ``ORIGINAL`` reports the episode's root logical attempt;
    ``PRIOR_AUTHORIZED`` reports one of the episode's durably authorized
    additional attempts after it was actually executed.
    """

    ORIGINAL = "original"
    PRIOR_AUTHORIZED = "prior_authorized"


class OriginalAttemptKind(StrEnum):
    """The closed vocabulary of durable identities a failure episode roots at.

    Every member is an existing durable claim/attempt identity with its own
    uniqueness; ``owner_control_intent`` is retained for the E10 intent
    identities although no v1 operation family pairs with it yet.
    """

    RUNTIME_SEND_CLAIM = "runtime_send_claim"
    PLANNING_BOOTSTRAP_CLAIM = "planning_bootstrap_claim"
    REVIEW_ITERATION = "review_iteration"
    EXECUTION = "execution"
    OWNER_CONTROL_INTENT = "owner_control_intent"


# The bounded diagnostic context: a canonical JSON object (string keys at
# every level, JSON-representable values only) with explicit size bounds so
# no caller can smuggle unbounded or raw provider content into a TaskBlock
# context or telemetry.
_MAX_CONTEXT_KEYS = 16
_MAX_CONTEXT_DEPTH = 3
_MAX_CONTEXT_JSON_CHARS = 4096


def _canonical_bounded_value(value: object, depth: int) -> object:
    if isinstance(value, Mapping):
        if depth > _MAX_CONTEXT_DEPTH:
            raise OperationalFailureDomainError(
                "operational-failure context exceeds the maximum nesting depth"
            )
        if len(value) > _MAX_CONTEXT_KEYS:
            raise OperationalFailureDomainError(
                "operational-failure context exceeds the maximum mapping size"
            )
        canonical: dict[str, object] = {}
        for key, item in value.items():
            if not isinstance(key, str):
                raise OperationalFailureDomainError(
                    "operational-failure context keys must be strings at every level"
                )
            canonical[key] = _canonical_bounded_value(item, depth + 1)
        return canonical
    if isinstance(value, (list, tuple)):
        if len(value) > _MAX_CONTEXT_KEYS:
            raise OperationalFailureDomainError(
                "operational-failure context exceeds the maximum sequence size"
            )
        return [_canonical_bounded_value(item, depth + 1) for item in value]
    if isinstance(value, (str, bool, int, float)) or value is None:
        if isinstance(value, float) and (value != value or value in (float("inf"), float("-inf"))):
            raise OperationalFailureDomainError(
                "operational-failure context must be canonical JSON; NaN and Infinity are not JSON"
            )
        return value
    raise OperationalFailureDomainError(
        "operational-failure context values must be JSON-representable "
        f"(mapping, sequence, string, number, boolean, or null); got {type(value).__name__}"
    )


def _plain(value: object) -> object:
    """Return the plain (non-proxy) form for JSON size measurement."""
    if isinstance(value, MappingProxyType):
        return {key: _plain(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_plain(item) for item in value]
    return value


def canonical_bounded_failure_context(value: object) -> Mapping[str, object]:
    """Canonicalize one bounded, sanitized diagnostic context object.

    The canonical JSON-object discipline of every durable context, plus
    explicit bounds on nesting depth, per-mapping/sequence size, and total
    serialized size. Raw provider messages, exception text, secrets, and
    prompt or transcript bodies have no place here; the bounds keep even
    well-intentioned diagnostic facts bounded.
    """
    if not isinstance(value, Mapping):
        raise OperationalFailureDomainError("operational-failure context must be a mapping")
    canonical = _canonical_bounded_value(value, 0)
    serialized = json.dumps(_plain(canonical), allow_nan=False, separators=(",", ":"))
    if len(serialized) > _MAX_CONTEXT_JSON_CHARS:
        raise OperationalFailureDomainError(
            "operational-failure context exceeds the maximum serialized size"
        )
    return MappingProxyType(canonical)  # type: ignore[arg-type]


def canonical_failure_subject(value: object) -> Mapping[str, object]:
    """Canonicalize one failure episode's safe subject mapping.

    The same canonical JSON-object semantics as the other domain JSON
    containers: string keys at every level, JSON-representable values only,
    sequences normalized to lists. The subject carries stable identifiers
    only — never prompts, plan or review content, outcomes, credentials, or
    raw diagnostic payloads.
    """
    try:
        return canonical_intent_outcome(value)
    except ValueError as error:
        raise OperationalFailureDomainError(str(error)) from error


def _require_uuid(value: object, name: str, *, nullable: bool = False) -> None:
    if value is None and nullable:
        return
    if not isinstance(value, UUID):
        raise OperationalFailureDomainError(f"operational-failure {name} must be a UUID")


def _require_instant(value: object, name: str) -> None:
    if not isinstance(value, datetime) or value.tzinfo is None:
        raise OperationalFailureDomainError(
            f"operational-failure {name} must be a timezone-aware datetime"
        )


@dataclass(frozen=True, slots=True)
class OperationalFailureEpisode:
    """One failure episode rooted at one original durable attempt identity.

    ``subject`` is the canonical safe-identifier mapping of the exact
    workflow subject the operation addresses. ``escalated_task_block_id`` is
    the terminal disposition: NULL while the episode has outstanding
    authorized attempts or awaits its first disposition, and the exact
    TaskBlock id once the episode escalated through the exceptional blocking
    service — set exactly once, never cleared.
    """

    id: UUID
    workspace_id: UUID
    task_id: UUID
    operation_family: PendingTaskRuntimeInteractionOperation
    original_attempt_kind: OriginalAttemptKind
    original_attempt_id: UUID
    subject: Mapping[str, object]
    escalated_task_block_id: UUID | None
    created_at: datetime

    def __post_init__(self) -> None:
        for name in ("id", "workspace_id", "task_id", "original_attempt_id"):
            _require_uuid(getattr(self, name), name)
        _require_uuid(self.escalated_task_block_id, "escalated_task_block_id", nullable=True)
        if not isinstance(self.operation_family, PendingTaskRuntimeInteractionOperation):
            raise OperationalFailureDomainError(
                "OperationalFailureEpisode.operation_family must be a "
                "PendingTaskRuntimeInteractionOperation"
            )
        if not isinstance(self.original_attempt_kind, OriginalAttemptKind):
            raise OperationalFailureDomainError(
                "OperationalFailureEpisode.original_attempt_kind must be an OriginalAttemptKind"
            )
        if not isinstance(self.subject, Mapping):
            raise OperationalFailureDomainError(
                "OperationalFailureEpisode.subject must be a mapping"
            )
        object.__setattr__(self, "subject", canonical_failure_subject(self.subject))
        _require_instant(self.created_at, "created_at")


@dataclass(frozen=True, slots=True)
class OperationalFailureAttempt:
    """One durably authorized automatic additional attempt.

    ``ordinal`` is the 1..3 position within the episode's three-attempt
    budget; ``not_before`` is the persisted exponential-backoff due instant
    computed at authorization. The ``source`` pair names the exact failed
    attempt this row succeeds — the episode's original identity for the
    first additional attempt, or a prior authorized attempt row afterwards.
    """

    id: UUID
    workspace_id: UUID
    task_id: UUID
    episode_id: UUID
    ordinal: int
    source_kind: OperationalFailureSourceKind
    source_attempt_id: UUID
    error_code: OperationalFailureCode
    not_before: datetime
    resulting_execution_id: UUID | None
    created_at: datetime

    def __post_init__(self) -> None:
        for name in ("id", "workspace_id", "task_id", "episode_id", "source_attempt_id"):
            _require_uuid(getattr(self, name), name)
        _require_uuid(self.resulting_execution_id, "resulting_execution_id", nullable=True)
        if (
            isinstance(self.ordinal, bool)
            or not isinstance(self.ordinal, int)
            or not (1 <= self.ordinal <= 3)
        ):
            raise OperationalFailureDomainError(
                "OperationalFailureAttempt.ordinal must be an integer between 1 and 3"
            )
        if not isinstance(self.source_kind, OperationalFailureSourceKind):
            raise OperationalFailureDomainError(
                "OperationalFailureAttempt.source_kind must be an OperationalFailureSourceKind"
            )
        if not isinstance(self.error_code, OperationalFailureCode):
            raise OperationalFailureDomainError(
                "OperationalFailureAttempt.error_code must be an OperationalFailureCode"
            )
        _require_instant(self.not_before, "not_before")
        _require_instant(self.created_at, "created_at")


def operational_failure_episode_field_names() -> frozenset[str]:
    """Return the exact field set an OperationalFailureEpisode exposes."""
    return frozenset(field.name for field in fields(OperationalFailureEpisode))


def operational_failure_attempt_field_names() -> frozenset[str]:
    """Return the exact field set an OperationalFailureAttempt exposes."""
    return frozenset(field.name for field in fields(OperationalFailureAttempt))
