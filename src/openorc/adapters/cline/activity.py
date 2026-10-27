"""Typed activity/usage observations and the pure event projection of the
Cline adapter (issue #164, D6c).

D6c exposes a Cline-specific, exact-Task-session, safe and typed
observational activity/usage surface on
:class:`openorc.adapters.cline.adapter.ClineAdapter`: ``subscribe_activity``
delivers a live projection of supported SDK session events to a consumer
listener, and ``read_activity_snapshot`` reports the usage the runtime
currently accounts for the session. This module defines the immutable typed
observations, the conservative pinned-SDK event mapping, and the pure
projection functions; the adapter owns binding resolution, the backend
subscription, and delivery, and nothing above the D6c projection ever
receives the opaque SDK event payload.

Authority and meaning stay above this boundary. The projection is an
allowlist, never a sanitization of a raw SDK object: every observation is
constructed fresh from explicitly permitted fields, and everything else —
reasoning content, prompts and guidance, command strings and output,
tool/MCP inputs and results, source and diffs, raw error text, snapshot
contents, and arbitrary payloads — never appears in projected activity.
Progress commentary is deliberately content-bearing agent-reported text and
may reach the authorized Task-facing product projection only; it never
enters OpenTelemetry spans/logs/attributes, PostHog, ordinary application
error strings, exception chains, persistence, audit ``WorkflowEvent``, or
broadly visible diagnostics, and the representations here withhold it.
Recognized formal OpenOrc response envelopes are suppressed from
commentary — as bare JSON or inside one complete surrounding Markdown code
fence, the same presentation tolerance the shared protocol funnel applies
to formal responses — so formal protocol output is never relabelled as
progress.

The E6 qualifying-activity pulse (issue #246) is the smallest content-free,
runtime-neutral projection on top of this vocabulary:
``project_qualifying_pulse`` classifies an observation as genuine
implementation activity (thinking indicators, commentary, tool lifecycle,
iteration progress) versus passive runtime bookkeeping, and
``ClineAdapter.subscribe_qualifying_activity`` delivers only a
zero-argument pulse to its consumer — no kind, detail, session, or SDK
content crosses that seam. The pulse is advisory runtime observation and
never workflow authority.

All observation is advisory. A completed turn, tool, or SDK ``done`` event
never completes an OpenOrc Execution/Task, validates a review, grants Owner
authority, selects the current workflow phase, settles an uncertain
dispatch/abort, or classifies genuine session loss; ``status``/``ended``
observations are runtime bookkeeping, never OpenOrc Task/Execution state.
Usage observations preserve the pinned shapes' per-turn versus accumulated
meaning and their aggregation scopes: ``getAccumulatedUsage`` returns a
``SessionUsageSummary`` whose ``usage`` covers the root/lead-agent session
and whose ``aggregateUsage`` additionally covers teammates and subagents —
the scopes are preserved distinctly, never substituted for one another, and
authoritative only as runtime bookkeeping within the current Cline Hub
lifetime (a Hub restart may reset them even when the conversation survives,
nothing is summed across observations, and usage is never evidence of
delivery, continuity, success, or Task progress). Reported usage values are
finite and non-negative: missing, mistyped, or negative readings stay
unreported rather than becoming invented numbers.

Pinned-SDK mapping limits (0.0.90): only demonstrated supported
``CoreSessionEvent``/``AgentEvent`` shapes are projected. ``chunk`` is not
a reliably typed assistant-text/reasoning feed and is never projected;
media content, hook/team/pending events, and unknown or malformed events
produce no observation. No built-in file-tool input schema is documented at
the pinned baseline, so the optional repository-relative tool filename is
deliberately not projected and tool ``input`` is never inspected. Missing,
malformed, or unsupported observations stay explicitly non-authoritative
rather than being invented.
"""

from __future__ import annotations

import json
import math
import re
from dataclasses import dataclass
from enum import StrEnum
from typing import Final

from openorc.adapters.cline.values import ClineSessionEvent, JsonObject
from openorc.protocol.models import FORMAL_RESPONSE_FAMILIES

__all__ = [
    "ClineAccumulatedUsage",
    "ClineActivityDetail",
    "ClineActivityKind",
    "ClineActivityObservation",
    "ClineCommentaryActivity",
    "ClineFinishActivity",
    "ClineIterationActivity",
    "ClineNoticeActivity",
    "ClineNoticeCategory",
    "ClineRuntimeEndedActivity",
    "ClineRuntimeErrorActivity",
    "ClineRuntimeStatusActivity",
    "ClineToolActivity",
    "ClineToolCategory",
    "ClineTurnFinishReason",
    "ClineUsageActivity",
    "project_qualifying_pulse",
    "project_session_event",
    "project_usage_summary",
]


class ClineActivityKind(StrEnum):
    """The safe activity vocabulary of the D6c projection.

    One bounded, content-safe classification per supported observation;
    missing, malformed, and unsupported SDK events project nothing instead
    of an invented entry.
    """

    THINKING_STARTED = "thinking_started"
    THINKING_ENDED = "thinking_ended"
    COMMENTARY = "commentary"
    TOOL_STARTED = "tool_started"
    TOOL_PROGRESS = "tool_progress"
    TOOL_COMPLETED = "tool_completed"
    TOOL_FAILED = "tool_failed"
    ITERATION_STARTED = "iteration_started"
    ITERATION_ENDED = "iteration_ended"
    NOTICE = "notice"
    TURN_FINISHED = "turn_finished"
    RUNTIME_ERROR = "runtime_error"
    RUNTIME_STATUS = "runtime_status"
    RUNTIME_ENDED = "runtime_ended"
    USAGE_OBSERVED = "usage_observed"


class ClineToolCategory(StrEnum):
    """Conservative live-feed activity classification of a tool name.

    Classification only — never tool permission, tool-identity authority,
    or semantic success verification. Unknown or unqualified names stay
    generic (:attr:`OTHER`).
    """

    COMMAND = "command"
    FILE = "file"
    MCP = "mcp"
    OTHER = "other"


class ClineNoticeCategory(StrEnum):
    """The pinned ``AgentNoticeEvent`` notice categories."""

    RECOVERY = "recovery"
    STOP = "stop"
    STATUS = "status"


class ClineTurnFinishReason(StrEnum):
    """Observational classification of a turn's ``done`` reason.

    The documented pinned ``AgentFinishReason`` values plus ``UNKNOWN`` for
    an unrecognized reported reason: the observation stays, the
    classification stays honest, and a finished turn never becomes workflow
    authority.
    """

    COMPLETED = "completed"
    MAX_ITERATIONS = "max_iterations"
    ABORTED = "aborted"
    MISTAKE_LIMIT = "mistake_limit"
    ERROR = "error"
    UNKNOWN = "unknown"


# Progress commentary is bounded product content: longer completed text is
# truncated explicitly instead of summarized or forwarded wholesale.
_MAX_COMMENTARY_CHARS: Final = 2000

# Safe bounded machine identifiers (tool names, tool-call correlation IDs,
# runtime status/reason/error-class strings). Arbitrary provider text never
# passes.
_MAX_IDENTIFIER_LENGTH: Final = 128
_IDENTIFIER_PATTERN: Final = re.compile(rf"[A-Za-z0-9_.:\-]{{1,{_MAX_IDENTIFIER_LENGTH}}}")

# Documented built-in tool names at the pinned baseline, classified
# conservatively for the live feed only. MCP tools follow the documented
# ``server__tool`` naming shape; every other name stays generic.
_COMMAND_TOOL_NAMES: Final[frozenset[str]] = frozenset({"run_commands"})
_FILE_TOOL_NAMES: Final[frozenset[str]] = frozenset({"read_files", "apply_patch", "editor"})
_MCP_NAME_SEPARATOR: Final = "__"

# The documented pinned notice reasons; anything else stays unreported.
_NOTICE_REASONS: Final[frozenset[str]] = frozenset(
    {
        "api_error",
        "invalid_tool_call",
        "completion_without_submit",
        "tool_execution_failed",
        "mistake_limit",
        "auto_compaction",
        "manual_compaction",
        "compaction_budget_emergency",
    }
)


def _finite_number(value: object) -> int | float | None:
    """Return ``value`` when it is a finite JSON number, else ``None``.

    Booleans are never numbers here and non-finite floats stay unreported:
    missing or unusable fields never become invented values.
    """
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    if isinstance(value, float) and not math.isfinite(value):
        return None
    return value


def _non_negative_number(value: object) -> int | float | None:
    number = _finite_number(value)
    if number is None or number < 0:
        return None
    return number


def _non_negative_int(value: object) -> int | None:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        return None
    return value


def _positive_int(value: object) -> int | None:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        return None
    return value


def _machine_identifier(value: object) -> str | None:
    """Return ``value`` when it is a bounded safe machine identifier."""
    if not isinstance(value, str) or _IDENTIFIER_PATTERN.fullmatch(value) is None:
        return None
    return value


@dataclass(frozen=True)
class ClineCommentaryActivity:
    """Bounded non-reasoning assistant progress commentary.

    The completed non-reasoning assistant text of one turn, deliberately
    content-bearing and labelled as agent-reported: it is never a verified
    machine fact, never a formal OpenOrc response (recognized formal
    response envelopes are suppressed, not relabelled), and never
    summarized by another model. Longer text is truncated explicitly.
    Streaming chunks and reasoning never reach this field. The
    representation withholds the text: commentary may reach the authorized
    Task-facing product projection only, never broadly visible diagnostics.
    """

    text: str
    truncated: bool

    def __post_init__(self) -> None:
        if not isinstance(self.text, str) or not self.text.strip():
            raise ValueError("ClineCommentaryActivity.text must be a non-empty string")
        if not isinstance(self.truncated, bool):
            raise ValueError("ClineCommentaryActivity.truncated must be a boolean")

    def __repr__(self) -> str:
        return (
            f"ClineCommentaryActivity(text=<{len(self.text)} characters>, "
            f"truncated={self.truncated!r})"
        )


@dataclass(frozen=True)
class ClineToolActivity:
    """Safe tool lifecycle observation for one tool event.

    ``category`` is live-feed activity classification only — never tool
    permission or semantic success verification. ``tool_name`` and
    ``tool_call_id`` are bounded safe machine identifiers when the SDK
    reported a well-formed value, and ``duration_ms`` is the
    runtime-reported tool duration when present. Tool input, progress
    updates, output, error text, command strings, and MCP
    arguments/results are never carried here.
    """

    category: ClineToolCategory
    tool_name: str | None = None
    tool_call_id: str | None = None
    duration_ms: int | float | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.category, ClineToolCategory):
            raise ValueError("ClineToolActivity.category must be a ClineToolCategory")
        for name in ("tool_name", "tool_call_id"):
            value = getattr(self, name)
            if value is not None and _machine_identifier(value) != value:
                raise ValueError(
                    f"ClineToolActivity.{name} must be None or a bounded machine identifier"
                )
        if (
            self.duration_ms is not None
            and _non_negative_number(self.duration_ms) != self.duration_ms
        ):
            raise ValueError(
                "ClineToolActivity.duration_ms must be None or a non-negative finite number"
            )


@dataclass(frozen=True)
class ClineIterationActivity:
    """Observational iteration progress of the running turn."""

    iteration: int
    had_tool_calls: bool | None = None
    tool_call_count: int | None = None

    def __post_init__(self) -> None:
        if not _positive_int(self.iteration):
            raise ValueError("ClineIterationActivity.iteration must be a positive integer")
        if self.had_tool_calls is not None and not isinstance(self.had_tool_calls, bool):
            raise ValueError("ClineIterationActivity.had_tool_calls must be None or a boolean")
        if (
            self.tool_call_count is not None
            and _non_negative_int(self.tool_call_count) != self.tool_call_count
        ):
            raise ValueError(
                "ClineIterationActivity.tool_call_count must be None or a non-negative integer"
            )


@dataclass(frozen=True)
class ClineNoticeActivity:
    """Safe notice-category observation.

    The pinned notice category and, when the runtime reported a documented
    reason, that safe machine identifier. The raw notice message and
    metadata never ride here.
    """

    category: ClineNoticeCategory
    reason: str | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.category, ClineNoticeCategory):
            raise ValueError("ClineNoticeActivity.category must be a ClineNoticeCategory")
        if self.reason is not None and _machine_identifier(self.reason) != self.reason:
            raise ValueError(
                "ClineNoticeActivity.reason must be None or a bounded machine identifier"
            )


@dataclass(frozen=True)
class ClineFinishActivity:
    """Observational turn-finish classification.

    The safe finish-reason classification and the runtime-reported
    iteration count. The final assistant response text and any usage
    payload never ride here, and a finished turn is never workflow
    authority.
    """

    reason: ClineTurnFinishReason
    iterations: int | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.reason, ClineTurnFinishReason):
            raise ValueError("ClineFinishActivity.reason must be a ClineTurnFinishReason")
        if self.iterations is not None and _non_negative_int(self.iterations) != self.iterations:
            raise ValueError(
                "ClineFinishActivity.iterations must be None or a non-negative integer"
            )


@dataclass(frozen=True)
class ClineRuntimeErrorActivity:
    """Observational runtime error classification.

    The safe machine error class when the SDK reported one, the
    runtime-reported recoverability flag, and the iteration when present.
    Raw error text, stack traces, and exception objects never ride here.
    """

    error_class: str | None = None
    recoverable: bool | None = None
    iteration: int | None = None

    def __post_init__(self) -> None:
        if (
            self.error_class is not None
            and _machine_identifier(self.error_class) != self.error_class
        ):
            raise ValueError(
                "ClineRuntimeErrorActivity.error_class must be None or a bounded machine identifier"
            )
        if self.recoverable is not None and not isinstance(self.recoverable, bool):
            raise ValueError("ClineRuntimeErrorActivity.recoverable must be None or a boolean")
        if self.iteration is not None and _non_negative_int(self.iteration) != self.iteration:
            raise ValueError(
                "ClineRuntimeErrorActivity.iteration must be None or a non-negative integer"
            )


@dataclass(frozen=True)
class ClineRuntimeStatusActivity:
    """Observational runtime status bookkeeping.

    The runtime's reported session status as a bounded safe machine
    identifier. This is runtime bookkeeping only — never OpenOrc
    Task/Execution state, workflow authority, or session-loss evidence.
    """

    status: str

    def __post_init__(self) -> None:
        if not isinstance(self.status, str) or _machine_identifier(self.status) != self.status:
            raise ValueError(
                "ClineRuntimeStatusActivity.status must be a bounded machine identifier"
            )


@dataclass(frozen=True)
class ClineRuntimeEndedActivity:
    """Observational runtime-ended bookkeeping.

    The runtime reported the session ended; the reason is carried only when
    it validates as a bounded safe machine identifier and stays ``None``
    otherwise. This is runtime bookkeeping only — never OpenOrc
    Task/Execution state, workflow authority, or session-loss evidence.
    """

    reason: str | None = None

    def __post_init__(self) -> None:
        if self.reason is not None and _machine_identifier(self.reason) != self.reason:
            raise ValueError(
                "ClineRuntimeEndedActivity.reason must be None or a bounded machine identifier"
            )


@dataclass(frozen=True)
class ClineUsageActivity:
    """Token/cost usage reported by the live usage event.

    Per-turn figures and the runtime's accumulated totals are preserved as
    the pinned event reports them — ``None`` means not reported, never
    zero, and nothing is summed or invented here. Values are finite and
    non-negative: a negative or mistyped reading stays unreported. Usage is
    runtime bookkeeping within the current Cline Hub lifetime, never
    evidence of delivery, continuity, success, or Task progress.
    """

    input_tokens: int | float | None = None
    output_tokens: int | float | None = None
    cache_read_tokens: int | float | None = None
    cache_write_tokens: int | float | None = None
    cost: int | float | None = None
    reasoning_token_count: int | float | None = None
    total_input_tokens: int | float | None = None
    total_output_tokens: int | float | None = None
    total_cache_read_tokens: int | float | None = None
    total_cache_write_tokens: int | float | None = None
    total_cost: int | float | None = None

    def __post_init__(self) -> None:
        for name in (
            "input_tokens",
            "output_tokens",
            "cache_read_tokens",
            "cache_write_tokens",
            "cost",
            "reasoning_token_count",
            "total_input_tokens",
            "total_output_tokens",
            "total_cache_read_tokens",
            "total_cache_write_tokens",
            "total_cost",
        ):
            value = getattr(self, name)
            if value is not None and _non_negative_number(value) != value:
                raise ValueError(
                    f"ClineUsageActivity.{name} must be None or a non-negative finite number"
                )


@dataclass(frozen=True)
class ClineAccumulatedUsage:
    """One aggregation scope of the runtime's accumulated usage summary.

    Maps the pinned ``SessionAccumulatedUsage`` shape: ``None`` fields are
    not reported values, never zeros, and nothing is summed or invented
    here. Values are finite and non-negative: a negative or mistyped
    reading stays unreported. The scope a value belongs to is carried by
    the surrounding snapshot, never implied.
    """

    input_tokens: int | float | None = None
    output_tokens: int | float | None = None
    cache_read_tokens: int | float | None = None
    cache_write_tokens: int | float | None = None
    total_cost: int | float | None = None

    def __post_init__(self) -> None:
        for name in (
            "input_tokens",
            "output_tokens",
            "cache_read_tokens",
            "cache_write_tokens",
            "total_cost",
        ):
            value = getattr(self, name)
            if value is not None and _non_negative_number(value) != value:
                raise ValueError(
                    f"ClineAccumulatedUsage.{name} must be None or a non-negative finite number"
                )


@dataclass(frozen=True)
class ClineActivitySnapshot:
    """The current observational usage snapshot of one bound session.

    ``usage`` is the runtime's root/lead-agent session totals and
    ``aggregate_usage`` the totals including teammates and subagents, as
    the pinned ``SessionUsageSummary`` reports them. The scopes are
    preserved distinctly and never substituted for one another: an absent
    or unusable scope is ``None`` (unknown), never filled from the other
    scope, and a root-only total is never presented as the Task-session
    aggregate. Both are runtime-observed bookkeeping within the current
    Cline Hub lifetime: a Hub restart may reset them even when the
    conversation survives, successive reads are independent observations
    rather than a lifetime ledger, and neither is evidence of delivery,
    continuity, success, or Task progress.
    """

    session_id: str
    usage: ClineAccumulatedUsage | None = None
    aggregate_usage: ClineAccumulatedUsage | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.session_id, str) or not self.session_id.strip():
            raise ValueError("ClineActivitySnapshot.session_id must be a non-empty string")
        if self.usage is not None and not isinstance(self.usage, ClineAccumulatedUsage):
            raise ValueError("ClineActivitySnapshot.usage must be None or a ClineAccumulatedUsage")
        if self.aggregate_usage is not None and not isinstance(
            self.aggregate_usage, ClineAccumulatedUsage
        ):
            raise ValueError(
                "ClineActivitySnapshot.aggregate_usage must be None or a ClineAccumulatedUsage"
            )


type ClineActivityDetail = (
    ClineCommentaryActivity
    | ClineToolActivity
    | ClineIterationActivity
    | ClineNoticeActivity
    | ClineFinishActivity
    | ClineRuntimeErrorActivity
    | ClineRuntimeStatusActivity
    | ClineRuntimeEndedActivity
    | ClineUsageActivity
)

# The detail type each activity kind carries; the thinking indicators are
# deliberately content-free.
_NO_DETAIL_KINDS: Final[frozenset[ClineActivityKind]] = frozenset(
    {ClineActivityKind.THINKING_STARTED, ClineActivityKind.THINKING_ENDED}
)

_ALLOWED_DETAIL_KINDS: Final[dict[type, frozenset[ClineActivityKind]]] = {
    ClineCommentaryActivity: frozenset({ClineActivityKind.COMMENTARY}),
    ClineToolActivity: frozenset(
        {
            ClineActivityKind.TOOL_STARTED,
            ClineActivityKind.TOOL_PROGRESS,
            ClineActivityKind.TOOL_COMPLETED,
            ClineActivityKind.TOOL_FAILED,
        }
    ),
    ClineIterationActivity: frozenset(
        {ClineActivityKind.ITERATION_STARTED, ClineActivityKind.ITERATION_ENDED}
    ),
    ClineNoticeActivity: frozenset({ClineActivityKind.NOTICE}),
    ClineFinishActivity: frozenset({ClineActivityKind.TURN_FINISHED}),
    ClineRuntimeErrorActivity: frozenset({ClineActivityKind.RUNTIME_ERROR}),
    ClineRuntimeStatusActivity: frozenset({ClineActivityKind.RUNTIME_STATUS}),
    ClineRuntimeEndedActivity: frozenset({ClineActivityKind.RUNTIME_ENDED}),
    ClineUsageActivity: frozenset({ClineActivityKind.USAGE_OBSERVED}),
}


@dataclass(frozen=True)
class ClineActivityObservation:
    """One safe typed activity observation of an exact bound session.

    The only shape a D6c consumer receives: immutable, OpenOrc-owned, and
    never the opaque SDK event payload. ``kind`` selects the vocabulary
    entry and ``detail`` carries its typed content (``None`` for the
    content-free thinking indicators).
    """

    session_id: str
    kind: ClineActivityKind
    detail: ClineActivityDetail | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.session_id, str) or not self.session_id.strip():
            raise ValueError("ClineActivityObservation.session_id must be a non-empty string")
        if not isinstance(self.kind, ClineActivityKind):
            raise ValueError("ClineActivityObservation.kind must be a ClineActivityKind")
        if self.detail is None:
            if self.kind not in _NO_DETAIL_KINDS:
                raise ValueError(f"a {self.kind.value} observation carries a typed detail")
        else:
            allowed = _ALLOWED_DETAIL_KINDS.get(type(self.detail))
            if allowed is None or self.kind not in allowed:
                raise ValueError(
                    f"a {self.kind.value} observation does not carry {type(self.detail).__name__}"
                )


def _observation(
    session_id: str,
    kind: ClineActivityKind,
    detail: ClineActivityDetail | None = None,
) -> ClineActivityObservation:
    return ClineActivityObservation(session_id=session_id, kind=kind, detail=detail)


def project_session_event(
    event: ClineSessionEvent,
    *,
    expected_session_id: str,
) -> list[ClineActivityObservation]:
    """Project one backend event into safe typed activity observations.

    The event must already be addressed to the subscription's session: an
    event whose own ``session_id`` differs from ``expected_session_id`` is
    dropped here as well, independently of the backend's session filter, so
    different Producer/Reviewer sessions, Tasks, and Connections can never
    cross-feed. Unknown, unsupported, and malformed events — including a
    missing or malformed nested agent event — yield no observation rather
    than an invented one, and this function never raises on event content.
    """
    if not isinstance(event, ClineSessionEvent) or event.session_id != expected_session_id:
        return []
    if event.kind == "agent_event":
        nested = event.payload.get("event")
        if not isinstance(nested, dict) or any(not isinstance(key, str) for key in nested):
            return []
        return _project_agent_event(nested, event.session_id)
    if event.kind == "status":
        status = _machine_identifier(event.payload.get("status"))
        if status is None:
            return []
        return [
            _observation(
                event.session_id,
                ClineActivityKind.RUNTIME_STATUS,
                ClineRuntimeStatusActivity(status=status),
            )
        ]
    if event.kind == "ended":
        return [
            _observation(
                event.session_id,
                ClineActivityKind.RUNTIME_ENDED,
                ClineRuntimeEndedActivity(reason=_machine_identifier(event.payload.get("reason"))),
            )
        ]
    if event.kind == "session_snapshot":
        snapshot = event.payload.get("snapshot")
        if not isinstance(snapshot, dict):
            return []
        status = _machine_identifier(snapshot.get("status"))
        if status is None:
            return []
        return [
            _observation(
                event.session_id,
                ClineActivityKind.RUNTIME_STATUS,
                ClineRuntimeStatusActivity(status=status),
            )
        ]
    # ``chunk`` is not a reliably typed assistant-text/reasoning feed, and
    # hook/team/pending events are not part of the supported observation
    # vocabulary; unknown kinds are likewise deliberately unprojected.
    return []


def _project_agent_event(agent: JsonObject, session_id: str) -> list[ClineActivityObservation]:
    """Project one nested ``AgentEvent`` object into its safe observations."""
    event_type = agent.get("type")
    if event_type == "content_start":
        return _project_content_start(agent, session_id)
    if event_type == "content_update":
        return _project_tool_progress(agent, session_id)
    if event_type == "content_end":
        return _project_content_end(agent, session_id)
    if event_type == "iteration_start":
        iteration = _positive_int(agent.get("iteration"))
        if iteration is None:
            return []
        return [
            _observation(
                session_id,
                ClineActivityKind.ITERATION_STARTED,
                ClineIterationActivity(iteration=iteration),
            )
        ]
    if event_type == "iteration_end":
        iteration = _positive_int(agent.get("iteration"))
        if iteration is None:
            return []
        had_tool_calls = agent.get("hadToolCalls")
        return [
            _observation(
                session_id,
                ClineActivityKind.ITERATION_ENDED,
                ClineIterationActivity(
                    iteration=iteration,
                    had_tool_calls=had_tool_calls if isinstance(had_tool_calls, bool) else None,
                    tool_call_count=_non_negative_int(agent.get("toolCallCount")),
                ),
            )
        ]
    if event_type == "notice":
        return _project_notice(agent, session_id)
    if event_type == "usage":
        return _project_usage_event(agent, session_id)
    if event_type == "done":
        return _project_done(agent, session_id)
    if event_type == "error":
        return _project_error(agent, session_id)
    # Unknown agent event types stay unprojected.
    return []


def _project_content_start(agent: JsonObject, session_id: str) -> list[ClineActivityObservation]:
    content_type = agent.get("contentType")
    if content_type == "reasoning":
        # Thinking began: an indicator only — the reasoning text, redacted
        # reasoning blocks, and accumulated text never ride along.
        return [_observation(session_id, ClineActivityKind.THINKING_STARTED)]
    if content_type == "tool":
        return [
            _observation(
                session_id,
                ClineActivityKind.TOOL_STARTED,
                _tool_detail(agent, duration_ms=None),
            )
        ]
    # Text content starts are streaming mechanics: only the completed text
    # end becomes commentary. Media and unknown content types are not
    # projected.
    return []


def _project_tool_progress(agent: JsonObject, session_id: str) -> list[ClineActivityObservation]:
    if agent.get("contentType") != "tool":
        return []
    return [
        _observation(
            session_id,
            ClineActivityKind.TOOL_PROGRESS,
            _tool_detail(agent, duration_ms=None),
        )
    ]


def _project_content_end(agent: JsonObject, session_id: str) -> list[ClineActivityObservation]:
    content_type = agent.get("contentType")
    if content_type == "reasoning":
        return [_observation(session_id, ClineActivityKind.THINKING_ENDED)]
    if content_type == "tool":
        error = agent.get("error")
        failed = isinstance(error, str) and bool(error.strip())
        kind = ClineActivityKind.TOOL_FAILED if failed else ClineActivityKind.TOOL_COMPLETED
        return [
            _observation(
                session_id,
                kind,
                _tool_detail(agent, duration_ms=_non_negative_number(agent.get("durationMs"))),
            )
        ]
    if content_type == "text":
        commentary = _commentary(agent.get("text"))
        if commentary is None:
            return []
        return [_observation(session_id, ClineActivityKind.COMMENTARY, commentary)]
    return []


def _tool_detail(agent: JsonObject, *, duration_ms: int | float | None) -> ClineToolActivity:
    """Build the safe tool identity of one tool event, conservatively.

    The name classifies into the bounded live-feed categories — documented
    command and file names, the documented ``server__tool`` MCP shape, and
    generic ``OTHER`` for everything else — and only well-formed bounded
    machine identifiers survive. Tool input, updates, output, and error
    text are never read here.
    """
    name = _machine_identifier(agent.get("toolName"))
    if name is not None and name in _COMMAND_TOOL_NAMES:
        category = ClineToolCategory.COMMAND
    elif name is not None and name in _FILE_TOOL_NAMES:
        category = ClineToolCategory.FILE
    elif name is not None and _MCP_NAME_SEPARATOR in name:
        category = ClineToolCategory.MCP
    else:
        category = ClineToolCategory.OTHER
    return ClineToolActivity(
        category=category,
        tool_name=name,
        tool_call_id=_machine_identifier(agent.get("toolCallId")),
        duration_ms=duration_ms,
    )


def _commentary(text: object) -> ClineCommentaryActivity | None:
    """Bound one completed non-reasoning assistant text into commentary.

    Recognized formal OpenOrc response envelopes are suppressed — they have
    separate canonical protocol/authority paths and are never relabelled as
    progress. Suppression covers the pinned presentation tolerance for
    formal responses: the whole text as bare JSON, or one complete Markdown
    code fence around it — the same tolerance the shared protocol funnel
    applies when it parses a formal response. Prose around a fence,
    partial fences, and multiple fenced blocks stay ordinary commentary.
    Longer text is truncated explicitly, never summarized, and the
    projection never infers workflow transitions from it.
    """
    if not isinstance(text, str) or not text.strip():
        return None
    if _is_formal_response_text(text):
        return None
    if len(text) <= _MAX_COMMENTARY_CHARS:
        return ClineCommentaryActivity(text=text, truncated=False)
    return ClineCommentaryActivity(text=text[:_MAX_COMMENTARY_CHARS], truncated=True)


def _is_formal_response_text(text: str) -> bool:
    """Whether the whole text is one recognized formal OpenOrc response.

    The text is a formal envelope when it parses — bare, or after
    unwrapping one complete surrounding Markdown code fence — into a JSON
    object whose ``type`` names a canonical formal response family. The
    family check is deliberately recognition-only: a malformed formal body
    is still formal protocol output and never becomes progress commentary.
    """
    fenced = _unfenced_text(text)
    try:
        parsed = json.loads(fenced if fenced is not None else text)
    except ValueError:
        return False
    return isinstance(parsed, dict) and parsed.get("type") in FORMAL_RESPONSE_FAMILIES


def _unfenced_text(text: str) -> str | None:
    """Return the content of one complete surrounding Markdown code fence.

    Recognizes the whole text as a single fenced block: an opening fence
    line (optionally carrying a backtick-free info string such as
    ``json``) and a closing fence alone on the last line. Prose around a
    fence, multiple fences, unterminated fences, and nested fences are not
    recognized — the caller then treats the text as ordinary content.
    """
    lines = text.strip().splitlines()
    if len(lines) < 3 or lines[-1].strip() != "```":
        return None
    opener = lines[0]
    if not opener.startswith("```") or "`" in opener[3:]:
        return None
    return "\n".join(lines[1:-1])


def _project_notice(agent: JsonObject, session_id: str) -> list[ClineActivityObservation]:
    notice_type = agent.get("noticeType")
    if not isinstance(notice_type, str):
        return []
    try:
        category = ClineNoticeCategory(notice_type)
    except ValueError:
        return []
    reason = agent.get("reason")
    return [
        _observation(
            session_id,
            ClineActivityKind.NOTICE,
            ClineNoticeActivity(
                category=category,
                reason=reason if isinstance(reason, str) and reason in _NOTICE_REASONS else None,
            ),
        )
    ]


def _project_usage_event(agent: JsonObject, session_id: str) -> list[ClineActivityObservation]:
    return [
        _observation(
            session_id,
            ClineActivityKind.USAGE_OBSERVED,
            ClineUsageActivity(
                input_tokens=_non_negative_number(agent.get("inputTokens")),
                output_tokens=_non_negative_number(agent.get("outputTokens")),
                cache_read_tokens=_non_negative_number(agent.get("cacheReadTokens")),
                cache_write_tokens=_non_negative_number(agent.get("cacheWriteTokens")),
                cost=_non_negative_number(agent.get("cost")),
                reasoning_token_count=_non_negative_number(agent.get("reasoningTokenCount")),
                total_input_tokens=_non_negative_number(agent.get("totalInputTokens")),
                total_output_tokens=_non_negative_number(agent.get("totalOutputTokens")),
                total_cache_read_tokens=_non_negative_number(agent.get("totalCacheReadTokens")),
                total_cache_write_tokens=_non_negative_number(agent.get("totalCacheWriteTokens")),
                total_cost=_non_negative_number(agent.get("totalCost")),
            ),
        )
    ]


def _project_done(agent: JsonObject, session_id: str) -> list[ClineActivityObservation]:
    raw_reason = agent.get("reason")
    reason = (
        _FINISH_REASONS.get(raw_reason, ClineTurnFinishReason.UNKNOWN)
        if isinstance(raw_reason, str)
        else ClineTurnFinishReason.UNKNOWN
    )
    return [
        _observation(
            session_id,
            ClineActivityKind.TURN_FINISHED,
            ClineFinishActivity(
                reason=reason, iterations=_non_negative_int(agent.get("iterations"))
            ),
        )
    ]


def _project_error(agent: JsonObject, session_id: str) -> list[ClineActivityObservation]:
    recoverable = agent.get("recoverable")
    return [
        _observation(
            session_id,
            ClineActivityKind.RUNTIME_ERROR,
            ClineRuntimeErrorActivity(
                error_class=_machine_identifier(agent.get("errorClass")),
                recoverable=recoverable if isinstance(recoverable, bool) else None,
                iteration=_non_negative_int(agent.get("iteration")),
            ),
        )
    ]


_FINISH_REASONS: Final[dict[str, ClineTurnFinishReason]] = {
    reason.value: reason
    for reason in (
        ClineTurnFinishReason.COMPLETED,
        ClineTurnFinishReason.MAX_ITERATIONS,
        ClineTurnFinishReason.ABORTED,
        ClineTurnFinishReason.MISTAKE_LIMIT,
        ClineTurnFinishReason.ERROR,
    )
}


def project_usage_summary(
    raw: JsonObject | None,
) -> tuple[ClineAccumulatedUsage | None, ClineAccumulatedUsage | None]:
    """Parse the pinned ``SessionUsageSummary | undefined`` wrapper.

    Returns the two aggregation scopes the runtime reports — the
    root/lead-agent ``usage`` and the session-wide ``aggregateUsage`` that
    additionally includes teammates and subagents — preserved distinctly
    and never substituted for one another. ``None`` input, a non-object
    wrapper, or a non-object scope means that scope is unknown, and a
    scope without any reported finite non-negative number is likewise
    unknown rather than an all-unreported placeholder.
    """
    if not isinstance(raw, dict):
        return (None, None)
    return (_accumulated_usage(raw.get("usage")), _accumulated_usage(raw.get("aggregateUsage")))


def _accumulated_usage(scope: object) -> ClineAccumulatedUsage | None:
    if not isinstance(scope, dict):
        return None
    usage = ClineAccumulatedUsage(
        input_tokens=_non_negative_number(scope.get("inputTokens")),
        output_tokens=_non_negative_number(scope.get("outputTokens")),
        cache_read_tokens=_non_negative_number(scope.get("cacheReadTokens")),
        cache_write_tokens=_non_negative_number(scope.get("cacheWriteTokens")),
        total_cost=_non_negative_number(scope.get("totalCost")),
    )
    if all(
        getattr(usage, name) is None
        for name in (
            "input_tokens",
            "output_tokens",
            "cache_read_tokens",
            "cache_write_tokens",
            "total_cost",
        )
    ):
        return None
    return usage


# The runtime-neutral qualifying-activity vocabulary (issue #246): the D6c
# kinds that represent genuine implementation activity and may advance the
# authorized-implementation service's advisory Execution marker through the
# zero-argument qualifying pulse. Passive runtime status/idle/ended/error/
# turn-finish notices and usage observations never qualify, and the pulse
# carries none of this vocabulary — only the fact that qualifying activity
# was observed for the exact bound session.
_QUALIFYING_PULSE_KINDS: Final[frozenset[ClineActivityKind]] = frozenset(
    {
        ClineActivityKind.THINKING_STARTED,
        ClineActivityKind.THINKING_ENDED,
        ClineActivityKind.COMMENTARY,
        ClineActivityKind.TOOL_STARTED,
        ClineActivityKind.TOOL_PROGRESS,
        ClineActivityKind.TOOL_COMPLETED,
        ClineActivityKind.TOOL_FAILED,
        ClineActivityKind.ITERATION_STARTED,
        ClineActivityKind.ITERATION_ENDED,
    }
)


def project_qualifying_pulse(observation: ClineActivityObservation) -> bool:
    """Classify one D6c observation as a qualifying activity pulse (issue #246).

    The vendor-local half of the E6 qualifying-activity seam: the shared
    authorized-implementation service never receives Cline vocabulary, only
    a zero-argument pulse callback, so this projection is where the
    qualifying classification lives. Genuine exact-session implementation
    activity — thinking indicators, agent commentary, tool lifecycle, and
    iteration progress — qualifies; passive status/idle/ended notices,
    runtime errors, turn-finish reports, and usage observations never do.
    Session identity is enforced by the subscribing adapter before the
    pulse fires, never by the consumer.
    """
    return observation.kind in _QUALIFYING_PULSE_KINDS
