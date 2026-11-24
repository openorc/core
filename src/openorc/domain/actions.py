"""Backend-generated action-token vocabulary (issue #376).

The read surface's action projection exposes these tokens to authorized
clients; G3 (issue #93) exposes the command routes that consume them. The
vocabulary is a fixed domain contract — never a per-Workspace or
database-configured registry, never an action engine, and never stored.
Offering an action is neither permission nor a promise of success: every
invocation independently revalidates the authenticated Profile, the current
Task state token, the exact gate/subject, and any external authority at the
command boundary.

- Task-level tokens address the Task aggregate: ``start`` (an eligible
  ``ready_to_plan`` attempt), ``abort`` (the Owner Abort control over
  established in-flight runtime activity), ``cancel_task`` (the unified
  Owner Cancel from a safely inactive, fully accounted state), and
  ``purge`` (the archived-attempt administrative deletion).
- Gate-scoped tokens address the exact current OwnerGate subject: the
  ordinary ``approve``/``discuss`` decisions of the v1 gate types and the
  exceptional menus' ``ignore``/``continue``/``resume``/``cancel_task``
  decisions. ``cancel_task`` is shared: it is never offered twice for the
  same Task (a gate offering it suppresses the Task-level token).
"""

from __future__ import annotations

from enum import StrEnum

__all__ = [
    "OwnerGateActionToken",
    "TaskActionToken",
]


class TaskActionToken(StrEnum):
    """The Task-level command tokens the read surface may offer (issue #376)."""

    START = "start"
    ABORT = "abort"
    CANCEL_TASK = "cancel_task"
    PURGE = "purge"


class OwnerGateActionToken(StrEnum):
    """The gate-scoped command tokens the read surface may offer (issue #376)."""

    APPROVE = "approve"
    DISCUSS = "discuss"
    IGNORE = "ignore"
    CONTINUE = "continue"
    RESUME = "resume"
    CANCEL_TASK = "cancel_task"
