"""Level 1-produced attempt-identity and currentness evidence (F5b, issue #262).

The explicitly threaded, process-local collector the F5b operation coordinator
reads when a Level 1 invocation fails: the durable attempt identities and the
authoritative Task state token that the failing invocation's own Level 1
services produced. Every fact here is trustworthy because it was returned by
the service call that durably committed it inside this invocation — never
re-derived from current durable state, which cannot distinguish this
invocation's attempt from another invocation's still-outstanding work.

- ``ProducedAttemptIdentity`` is one Level 1-produced attempt identity: the
  closed F4 operation family, the durable identity kind, the exact claim /
  iteration / execution UUID, the effect row when it differs (a durably
  authorized retry execution reports the episode's original identity while
  naming its own Execution as the effect row), and a safe machine phase label.
  A service re-records the same attempt with a refined family when the
  invocation reaches the next phase of the same durable identity (the
  planning bootstrap's single claim token roots both the PREPARE and the
  planning-context TRANSITION families); the latest record is authoritative.
- ``InvocationAttemptEvidence`` collects those identities plus the Task
  state token Level 1 established as current for the addressed subject —
  seeded by the caller with the invocation payload's exact token and updated
  at authoritative token rotations (the planning-bootstrap claim rotates the
  token before its failure-prone runtime phases, so the post-claim token is
  recorded at the claim commit itself). The coordinator reports this
  Level 1-established token to Level 2; it never derives expected currentness
  from an unrelated post-failure Task snapshot.

The collector is diagnostic handoff plumbing only: it is never workflow
authority, never durable state, never a session/queue identity, and never
read on the success path. Recording is optional everywhere (``None`` disables
it), so existing callers stay source-compatible and the coordinator fails
closed when a failing invocation produced no identity at all.
"""

from __future__ import annotations

from dataclasses import dataclass
from uuid import UUID

from openorc.domain.operational_failures import (
    GitHubMergeOperation,
    OperationalFailureFamily,
    OriginalAttemptKind,
)
from openorc.domain.pending_interactions import PendingTaskRuntimeInteractionOperation

__all__ = ["InvocationAttemptEvidence", "ProducedAttemptIdentity"]


@dataclass(frozen=True, slots=True)
class ProducedAttemptIdentity:
    """One Level 1-produced durable attempt identity of the failing invocation.

    ``family`` is the closed Level 2 operation family the attempt belongs to
    (the pending-interaction vocabulary plus, F5d #267, the exact-head GitHub
    merge family); ``original_attempt_kind``/``original_attempt_id`` name the
    exact durable claim/iteration/execution/attempt row the service
    committed; ``effect_attempt_id`` is the effect row when it differs from
    the original identity (a durably authorized retry's resulting Execution,
    or the merge retry's own consumed attempt); ``phase`` is a safe machine
    label of the service phase that produced it.
    """

    family: OperationalFailureFamily
    original_attempt_kind: OriginalAttemptKind
    original_attempt_id: UUID
    effect_attempt_id: UUID | None
    phase: str


class InvocationAttemptEvidence:
    """The failing invocation's own Level 1-produced handoff facts.

    Append-only for attempt identities (a later record refines an earlier
    one when the same durable identity advances to its next operation
    family) and set-once-per-update for the Level 1-established Task token.
    Construction and recording validate their inputs fail-closed: a malformed
    record is a programming error, never silently discarded.
    """

    def __init__(self) -> None:
        self._identities: list[ProducedAttemptIdentity] = []
        self._current_task_token: UUID | None = None

    def record_attempt(
        self,
        *,
        family: OperationalFailureFamily,
        original_attempt_kind: OriginalAttemptKind,
        original_attempt_id: UUID,
        effect_attempt_id: UUID | None = None,
        phase: str,
    ) -> None:
        """Record one Level 1-produced durable attempt identity."""
        if not isinstance(family, (PendingTaskRuntimeInteractionOperation, GitHubMergeOperation)):
            raise TypeError(
                "attempt family must be a PendingTaskRuntimeInteractionOperation "
                "or a GitHubMergeOperation"
            )
        if not isinstance(original_attempt_kind, OriginalAttemptKind):
            raise TypeError("attempt kind must be an OriginalAttemptKind")
        if not isinstance(original_attempt_id, UUID):
            raise TypeError("the original attempt id must be a UUID")
        if effect_attempt_id is not None and not isinstance(effect_attempt_id, UUID):
            raise TypeError("the effect attempt id must be a UUID or None")
        if not isinstance(phase, str) or not phase.strip():
            raise TypeError("the attempt phase must be a non-blank string")
        self._identities.append(
            ProducedAttemptIdentity(
                family=family,
                original_attempt_kind=original_attempt_kind,
                original_attempt_id=original_attempt_id,
                effect_attempt_id=effect_attempt_id,
                phase=phase,
            )
        )

    def record_task_token(self, token: UUID) -> None:
        """Record the Task state token Level 1 established as current."""
        if not isinstance(token, UUID):
            raise TypeError("the recorded task state token must be a UUID")
        self._current_task_token = token

    @property
    def latest_identity(self) -> ProducedAttemptIdentity | None:
        """The latest recorded attempt identity, or ``None`` when none exists.

        The latest record is the phase the failing invocation reached: a
        failure during or after a phase reports that phase's identity, and an
        invocation that produced no durable identity reports nothing.
        """
        return self._identities[-1] if self._identities else None

    @property
    def current_task_token(self) -> UUID | None:
        """The Task state token Level 1 established as current, if any."""
        return self._current_task_token
