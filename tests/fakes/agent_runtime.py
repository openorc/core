"""Deterministic fake Agent Runtime adapter for ordinary tests (issue #69).

``FakeAgentRuntimeAdapter`` implements the merged #67
``AgentRuntimeAdapter`` contract — exactly the runtime-neutral surface real
runtime adapters implement — while remaining deliberately dumb about OpenOrc
workflow state: it carries no Task, ReviewLoop, OwnerGate, Execution,
RuntimeRequest, or GitHub semantics, delivers initialization/message content
verbatim, never interprets Workspace-guidance or any other prompt prose, and
produces only explicitly scripted outcomes. It is test infrastructure, not a
second orchestration engine, and nothing here is a production API.

Faithfulness guarantees:

- Formal validation is never duplicated: every configured creation/readiness
  candidate and every formal send candidate travels through the inherited
  shared #67 funnel (``AgentRuntimeAdapter._parse_formal_candidate`` → the
  #65 ``openorc.protocol`` parser), so canonical validation and the
  normalized ``AgentRuntimeProtocolFailureError`` wrapping remain exactly the
  merged #67 → #65 path.
- Formal and non-formal sends stay distinct: a queued raw candidate is
  parsed only when the call carries an ``expected_family``; the same payload
  consumed by a non-formal send (``expected_family=None``) is returned as
  ordinary ``AgentTextResponse`` text, so formal-looking JSON stays ordinary
  text.
- Exact-session isolation: each successful creation establishes one fresh
  isolated fake context for the exact workspace/task/role, addressed by a
  stable opaque external session ID. IDs are deterministic per adapter
  instance (``fake-session-1``, ``fake-session-2``, ... in successful
  creation order), and invalid, known-failed, or uncertain
  creation/readiness never exposes one. Exact-session loss raises the
  normalized ``AgentSessionNotFoundError`` and never creates a replacement.
- Deterministic, offline, and in-process: no network, model inference,
  randomness, sleeps, or wall-clock behavior.

Scripting model: outcomes are queued FIFO and consumed exactly once per
interaction. A scripted outcome is either a raw runtime candidate (``str``
or ``dict``; formal-shaped or not) or one normalized #67 error instance to
raise as-is — the normalized taxonomy is the scripting surface, never
arbitrary provider/runtime-native exception objects. Scripted error messages
must remain safe by authoring, exactly like the taxonomy's own messages.
The fake's native control realization is record-only: it proves an
already-authorized semantic control reached the adapter, carries no workflow
authority, and never invents a formal result.

The optional ``lifecycle`` constructor argument is a narrow test seam for
coordinating creation timing with an unrelated scripted seam (for example a
scripted persistence connection): when supplied, each ``create_session``
appends ``"create_session"`` to the shared list.
"""

from __future__ import annotations

import json
from collections.abc import Sequence
from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any
from uuid import UUID

from openorc.adapters.agent_runtime.contract import AgentRuntimeAdapter
from openorc.adapters.agent_runtime.errors import AgentRuntimeError, AgentSessionNotFoundError
from openorc.adapters.agent_runtime.results import (
    AgentSessionCreated,
    AgentSessionCreationRequest,
    AgentTextResponse,
)
from openorc.domain.connections import WorkflowRole
from openorc.protocol.interaction import Interaction
from openorc.protocol.models import (
    REVIEW_OUTCOME_ACCEPTED,
    REVIEW_OUTCOME_CHANGES_REQUESTED,
    SESSION_READY_FAMILY,
    FormalResponse,
)

__all__ = [
    "ControlRealization",
    "ControlRecord",
    "FakeAgentRuntimeAdapter",
    "FakeSessionContext",
    "SendRecord",
    "accepted_review_result_candidate",
    "changes_requested_review_result_candidate",
    "implementation_result_candidate",
    "plan_result_candidate",
    "pr_result_candidate",
    "session_ready_candidate",
]

# A scripted outcome is a raw runtime candidate (formal-shaped or not) or a
# normalized #67 error instance raised as-is at the configured point.
ScriptedOutcome = str | dict[str, Any] | AgentRuntimeError


class ControlRealization(StrEnum):
    """How the fake realizes one already-authorized semantic control.

    ``PROSE_ONLY`` is the shared default realization: the inherited #67
    prose-only path through ``send``. ``NATIVE_ONLY`` records a fake-native
    action and performs no send. ``NATIVE_THEN_PROSE`` records the native
    action and then takes the shared prose path.
    """

    PROSE_ONLY = "prose_only"
    NATIVE_ONLY = "native_only"
    NATIVE_THEN_PROSE = "native_then_prose"


@dataclass(frozen=True)
class SendRecord:
    """One exact-session send the fake accepted: the delivered message and
    whether the call was a formal interaction carrying an expected family."""

    external_session_id: str
    message: str
    expected_family: str | None
    formal: bool


@dataclass(frozen=True)
class ControlRecord:
    """One realized semantic workflow control: the exact #67 inputs and
    whether the realization included the fake-native action."""

    external_session_id: str
    interaction: Interaction
    message: str | None
    expected_family: str | None
    native_action: bool


@dataclass(frozen=True)
class FakeSessionContext:
    """Bounded inspection view of one fake session context."""

    external_session_id: str
    role: WorkflowRole
    workspace_id: UUID
    task_id: UUID
    initialization: str
    lost: bool


@dataclass
class _FakeSession:
    """Fake runtime-local session state, isolated by creation."""

    role: WorkflowRole
    workspace_id: UUID
    task_id: UUID
    initialization: str
    lost: bool = False
    send_outcomes: list[ScriptedOutcome] = field(default_factory=list)
    realization_modes: list[ControlRealization] = field(default_factory=list)


def session_ready_candidate(status: str = "READY") -> dict[str, Any]:
    """Raw canonical v1 ``session_ready`` wire candidate; also the
    creation/readiness candidate for the fake's readiness handshake."""
    return {"type": "session_ready", "schema_version": 1, "status": status}


def plan_result_candidate(plan: str) -> dict[str, Any]:
    """Raw canonical v1 ``plan_result`` wire candidate."""
    return {"type": "plan_result", "schema_version": 1, "plan": plan}


def accepted_review_result_candidate(summary: str) -> dict[str, Any]:
    """Raw canonical v1 ACCEPTED ``review_result`` wire candidate."""
    return {
        "type": "review_result",
        "schema_version": 1,
        "outcome": REVIEW_OUTCOME_ACCEPTED,
        "summary": summary,
        "findings": [],
    }


def changes_requested_review_result_candidate(
    summary: str,
    findings: Sequence[tuple[str, str]],
) -> dict[str, Any]:
    """Raw canonical v1 CHANGES_REQUESTED ``review_result`` wire candidate."""
    return {
        "type": "review_result",
        "schema_version": 1,
        "outcome": REVIEW_OUTCOME_CHANGES_REQUESTED,
        "summary": summary,
        "findings": [
            {"summary": finding_summary, "details": details}
            for finding_summary, details in findings
        ],
    }


def implementation_result_candidate(
    *,
    branch: str,
    summary: str,
    changes: Sequence[str],
    validation: Sequence[str],
    status: str = "COMPLETED",
    notes: str | None = None,
) -> dict[str, Any]:
    """Raw canonical v1 ``implementation_result`` wire candidate."""
    payload: dict[str, Any] = {
        "type": "implementation_result",
        "schema_version": 1,
        "status": status,
        "branch": branch,
        "summary": summary,
        "changes": list(changes),
        "validation": list(validation),
    }
    if notes is not None:
        payload["notes"] = notes
    return payload


def pr_result_candidate(*, title: str, body: str) -> dict[str, Any]:
    """Raw canonical v1 ``pr_result`` wire candidate."""
    return {"type": "pr_result", "schema_version": 1, "title": title, "body": body}


class FakeAgentRuntimeAdapter(AgentRuntimeAdapter):
    """Deterministic in-process fake Agent Runtime for ordinary tests.

    Construct a fresh adapter per test: every instance starts with no
    sessions, no queued outcomes, and no recorded interactions, and instances
    never share state. There is deliberately no cross-test global session
    registry.
    """

    def __init__(
        self,
        *,
        reported_provider: str | None = None,
        reported_model: str | None = None,
        reported_runtime_version: str | None = None,
        lifecycle: list[str] | None = None,
    ) -> None:
        self._reported_provider = reported_provider
        self._reported_model = reported_model
        self._reported_runtime_version = reported_runtime_version
        self._lifecycle = lifecycle
        self._creation_outcomes: list[ScriptedOutcome] = []
        self._sessions: dict[str, _FakeSession] = {}
        self._session_number = 0
        self._creation_requests: list[AgentSessionCreationRequest] = []
        self._sends: list[SendRecord] = []
        self._controls: list[ControlRecord] = []

    # --- scripting ---

    def queue_creation(self, outcome: ScriptedOutcome) -> None:
        """Queue the next creation/readiness outcome: a raw ``session_ready``
        candidate (traveling through the shared #67 → #65 funnel) or a
        normalized #67 error instance."""
        self._creation_outcomes.append(outcome)

    def queue_send(self, external_session_id: str, outcome: ScriptedOutcome) -> None:
        """Queue the next send outcome for one created session: a raw
        candidate for any v1 family, a non-formal text reply, or a normalized
        #67 error instance. Formal versus non-formal is decided by the call's
        ``expected_family`` when the outcome is consumed."""
        self._tracked_session(external_session_id).send_outcomes.append(outcome)

    def script_realization(self, external_session_id: str, mode: ControlRealization) -> None:
        """Queue how the next semantic control on this session is realized.
        After queued modes are consumed, realization falls back to the shared
        PROSE_ONLY default."""
        self._tracked_session(external_session_id).realization_modes.append(mode)

    def lose_session(self, external_session_id: str) -> None:
        """Deterministically lose one created session: every later
        exact-session interaction raises the normalized
        ``AgentSessionNotFoundError`` and no replacement is ever created.
        Idempotent."""
        self._tracked_session(external_session_id).lost = True

    # --- bounded inspection ---

    def creation_requests(self) -> tuple[AgentSessionCreationRequest, ...]:
        """Every ``create_session`` attempt in order, exactly as received."""
        return tuple(self._creation_requests)

    def created_session_ids(self) -> tuple[str, ...]:
        """Stable opaque external session IDs of successful creations, in
        order."""
        return tuple(self._sessions)

    def session_context(self, external_session_id: str) -> FakeSessionContext:
        """Inspect one session's fake context, including the initialization
        delivered verbatim exactly as composed upstream."""
        session = self._sessions.get(external_session_id)
        if session is None:
            raise KeyError(external_session_id)
        return FakeSessionContext(
            external_session_id=external_session_id,
            role=session.role,
            workspace_id=session.workspace_id,
            task_id=session.task_id,
            initialization=session.initialization,
            lost=session.lost,
        )

    def submitted_sends(self, external_session_id: str | None = None) -> tuple[SendRecord, ...]:
        """Accepted sends in order, optionally filtered to one session."""
        return tuple(
            record
            for record in self._sends
            if external_session_id is None or record.external_session_id == external_session_id
        )

    def realized_controls(
        self, external_session_id: str | None = None
    ) -> tuple[ControlRecord, ...]:
        """Realized semantic controls in order, optionally filtered to one
        session."""
        return tuple(
            record
            for record in self._controls
            if external_session_id is None or record.external_session_id == external_session_id
        )

    # --- #67 contract implementation ---

    def create_session(self, request: AgentSessionCreationRequest) -> AgentSessionCreated:
        self._creation_requests.append(request)
        if self._lifecycle is not None:
            self._lifecycle.append("create_session")
        outcome = self._pop_outcome(self._creation_outcomes, "the creation/readiness handshake")
        if isinstance(outcome, AgentRuntimeError):
            raise outcome
        # The configured readiness candidate travels through the inherited
        # shared funnel exactly like a real adapter's candidate; only a valid
        # session_ready readiness yields a session.
        self._parse_formal_candidate(outcome, expected_family=SESSION_READY_FAMILY)
        self._session_number += 1
        external_session_id = f"fake-session-{self._session_number}"
        self._sessions[external_session_id] = _FakeSession(
            role=request.role,
            workspace_id=request.workspace_id,
            task_id=request.task_id,
            initialization=request.initialization,
        )
        return AgentSessionCreated(
            external_session_id=external_session_id,
            reported_provider=self._reported_provider,
            reported_model=self._reported_model,
            reported_runtime_version=self._reported_runtime_version,
        )

    def send(
        self,
        session_id: str,
        message: str,
        *,
        expected_family: str | None = None,
    ) -> FormalResponse | AgentTextResponse:
        self._require_live_session(session_id)
        self._sends.append(
            SendRecord(
                external_session_id=session_id,
                message=message,
                expected_family=expected_family,
                formal=expected_family is not None,
            )
        )
        session = self._sessions[session_id]
        outcome = self._pop_outcome(session.send_outcomes, f"a send to {session_id}")
        if isinstance(outcome, AgentRuntimeError):
            raise outcome
        if expected_family is None:
            # Non-formal exact-session interaction: no formal parsing is ever
            # triggered, so formal-looking JSON stays ordinary text.
            return AgentTextResponse(
                text=outcome if isinstance(outcome, str) else json.dumps(outcome)
            )
        return self._parse_formal_candidate(outcome, expected_family=expected_family)

    def realize_control(
        self,
        session_id: str,
        interaction: Interaction,
        *,
        message: str | None = None,
        expected_family: str | None = None,
    ) -> FormalResponse | None:
        self._require_live_session(session_id)
        session = self._sessions[session_id]
        mode = (
            session.realization_modes.pop(0)
            if session.realization_modes
            else ControlRealization.PROSE_ONLY
        )
        self._controls.append(
            ControlRecord(
                external_session_id=session_id,
                interaction=interaction,
                message=message,
                expected_family=expected_family,
                native_action=mode is not ControlRealization.PROSE_ONLY,
            )
        )
        if mode is ControlRealization.NATIVE_ONLY:
            # The fake-native action is record-only and has no hidden
            # authority; no formal result is invented.
            return None
        return super().realize_control(
            session_id,
            interaction,
            message=message,
            expected_family=expected_family,
        )

    # --- internal helpers ---

    def _tracked_session(self, external_session_id: str) -> _FakeSession:
        session = self._sessions.get(external_session_id)
        if session is None:
            raise ValueError(
                f"no fake session context exists for {external_session_id!r}; "
                "queue against a session this adapter successfully created"
            )
        return session

    def _require_live_session(self, external_session_id: str) -> _FakeSession:
        session = self._sessions.get(external_session_id)
        if session is None or session.lost:
            raise AgentSessionNotFoundError(
                "the exact addressed session is not live on this fake runtime"
            )
        return session

    @staticmethod
    def _pop_outcome(outcomes: list[ScriptedOutcome], description: str) -> ScriptedOutcome:
        if not outcomes:
            raise AssertionError(
                f"no scripted outcome remains for {description}; queue outcomes before interacting"
            )
        return outcomes.pop(0)
