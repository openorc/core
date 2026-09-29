"""Shared deterministic test fakes for OpenOrc test suites.

Reusable fakes live here instead of inside individual test modules. Each
fake is ordinary test infrastructure: never a production API, never owner
of OpenOrc workflow semantics, and always deterministic and offline.
"""

from .agent_runtime import (
    ControlRealization,
    ControlRecord,
    FakeAgentRuntimeAdapter,
    FakeSessionContext,
    ScriptedOutcome,
    SendRecord,
    accepted_review_result_candidate,
    changes_requested_review_result_candidate,
    implementation_result_candidate,
    plan_result_candidate,
    pr_result_candidate,
    session_ready_candidate,
)

__all__ = [
    "ControlRealization",
    "ControlRecord",
    "FakeAgentRuntimeAdapter",
    "FakeSessionContext",
    "ScriptedOutcome",
    "SendRecord",
    "accepted_review_result_candidate",
    "changes_requested_review_result_candidate",
    "implementation_result_candidate",
    "plan_result_candidate",
    "pr_result_candidate",
    "session_ready_candidate",
]
