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
  Execution row, an owner-control intent, or — F5d (issue #267) — the
  approved exact-head ``MERGE_DECISION`` OwnerGate, whose UUID is the
  original durable merge-operation identity). A Task state token, a mutable
  phase, or a bare PR number is never an episode root.
- Operation families reuse the F2 pending-interaction operation vocabulary
  (the closed set of the automated workflow's claimed consequential
  interactions) plus — from F5d (issue #267) — the one explicit exact-head
  GitHub merge family (:class:`GitHubMergeOperation`). A merge is never
  mislabeled as a pending agent-runtime interaction, and no further parallel
  vocabulary exists.
- ``OperationalFailureEpisode`` carries the episode's scope, family, original
  identity, canonical safe subject, and its terminal escalation disposition:
  ``escalated_task_block_id`` references the exact unresolved TaskBlock the
  F3 exceptional blocking service installed, is set exactly once, and is
  never cleared. A disposed episode is disposed history even after the
  block is later resolved.
- ``OperationalFailureAttempt`` is one durably authorized automatic
  additional attempt: its own UUID, the 1..3 ordinal within its episode,
  the exact failed attempt it succeeds, the classified error code, and the
  persisted UTC ``not_before`` instant. ``consumed_at`` (F5d) is the set-once
  consumption stamp an execution seam writes atomically before an outbound
  effect that is not an Execution — only the failure of a consumed attempt
  may be reported to its episode. Attempts are forward-only history for
  replay prevention and later consumption; they are never rewritten.

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
    "GitHubMergeOperation",
    "OperationalFailureAttempt",
    "OperationalFailureCode",
    "OperationalFailureDomainError",
    "OperationalFailureEpisode",
    "OperationalFailureFamily",
    "OperationalFailureSourceKind",
    "OriginalAttemptKind",
    "canonical_bounded_failure_context",
    "canonical_failure_subject",
    "operational_failure_attempt_field_names",
    "operational_failure_episode_field_names",
    "operational_failure_family_from_value",
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
    # F5d (issue #267): the classified transient network/transport failure of
    # an external mutation — derived type-only from the concrete adapter
    # taxonomy's recognized transient transport subtype, never from provider
    # prose, HTTP status guessing, or a caller-supplied retryable flag. It is
    # the one retry-eligible code of the exact-head merge family; every other
    # code escalates unless a family's explicit eligibility admits it.
    EXTERNAL_TRANSPORT_UNAVAILABLE = "external_transport_unavailable"
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
    # F5d (issue #267): the existing approved exact-head MERGE_DECISION
    # OwnerGate — its UUID is the original durable merge-operation identity
    # one merge episode roots at. The gate is resolved history at merge time;
    # the episode never re-approves, mutates, or replaces it.
    MERGE_DECISION_GATE = "merge_decision_gate"


class GitHubMergeOperation(StrEnum):
    """The closed vocabulary of exact-head GitHub merge operation families (F5d).

    A deliberate, single-member extension of the Level 2 operation families:
    the Owner-authorized exact-head merge request over one canonical
    TaskPullRequest. It is deliberately NOT a member of the F2
    pending-interaction runtime vocabulary — a merge is an Owner-accountable
    GitHub mutation, not an outstanding runtime interaction — and it carries
    its own original durable identity kind (the approved ``MERGE_DECISION``
    gate UUID). The Level 2 repeat-safety proof is structural: the merge
    request is guarded by GitHub's documented expected-head parameter and the
    canonical PR's single merged state, so a reconciled open/unmerged
    same-head observation plus a classified transient transport failure is
    the one retry-eligible classification this family admits.
    """

    EXACT_HEAD_MERGE = "exact_head_merge"


OperationalFailureFamily = PendingTaskRuntimeInteractionOperation | GitHubMergeOperation
"""The closed union of Level 2 operation-family vocabularies (F5d, issue #267)."""


def operational_failure_family_from_value(
    value: str,
) -> PendingTaskRuntimeInteractionOperation | GitHubMergeOperation:
    """Resolve one stored operation-family value to its closed-vocabulary member."""
    try:
        return PendingTaskRuntimeInteractionOperation(value)
    except ValueError:
        pass
    try:
        return GitHubMergeOperation(value)
    except ValueError as error:
        raise OperationalFailureDomainError(
            "the operation family value is not a member of the closed vocabulary"
        ) from error


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


def _require_instant(value: object, name: str, *, nullable: bool = False) -> None:
    if value is None and nullable:
        return
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
    operation_family: PendingTaskRuntimeInteractionOperation | GitHubMergeOperation
    original_attempt_kind: OriginalAttemptKind
    original_attempt_id: UUID
    subject: Mapping[str, object]
    escalated_task_block_id: UUID | None
    created_at: datetime

    def __post_init__(self) -> None:
        for name in ("id", "workspace_id", "task_id", "original_attempt_id"):
            _require_uuid(getattr(self, name), name)
        _require_uuid(self.escalated_task_block_id, "escalated_task_block_id", nullable=True)
        if not isinstance(
            self.operation_family,
            (PendingTaskRuntimeInteractionOperation, GitHubMergeOperation),
        ):
            raise OperationalFailureDomainError(
                "OperationalFailureEpisode.operation_family must be a "
                "PendingTaskRuntimeInteractionOperation or a GitHubMergeOperation"
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
    ``consumed_at`` (F5d) is the set-once instant an execution seam stamped
    atomically before the outbound effect it ran; ``NULL`` means the
    authorization has not been executed.
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
    consumed_at: datetime | None
    created_at: datetime

    def __post_init__(self) -> None:
        for name in ("id", "workspace_id", "task_id", "episode_id", "source_attempt_id"):
            _require_uuid(getattr(self, name), name)
        _require_uuid(self.resulting_execution_id, "resulting_execution_id", nullable=True)
        _require_instant(self.consumed_at, "consumed_at", nullable=True)
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
