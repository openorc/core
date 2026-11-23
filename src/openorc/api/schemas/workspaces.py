"""Wire DTOs for the Workspace discovery, creation and configuration surface (issue #370).

Thin transport models only: the application services own every semantic rule
(canonical Profile-derived ownership, the account-deletion mutation barrier,
setting validity, same-value no-op and audit-on-change) and these models never
duplicate service/domain validation — FastAPI parses the declared shapes and
the router delegates.
"""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import Annotated
from uuid import UUID

from pydantic import BaseModel, StrictInt, WithJsonSchema

__all__ = [
    "DefaultTaskBudgetChange",
    "DefaultTaskBudgetSetting",
    "GuidanceChange",
    "GuidanceSetting",
    "ReviewIterationLimitChange",
    "ReviewIterationLimitSetting",
    "WorkspaceCreate",
    "WorkspaceRead",
]

# The exact-decimal transport for the Owner-authorized default USD Task
# budget (issue #240): pydantic serializes Decimal as canonical decimal TEXT
# in JSON — the exactness-preserving form (never a binary float) — so the
# published schema deliberately declares a string, while requests may submit
# either a JSON number or that same decimal text; both parse to the exact
# Decimal the service validates.
_ExactUsdAmount = Annotated[
    Decimal,
    WithJsonSchema(
        {
            "type": "string",
            "description": (
                "Exact decimal USD amount as canonical decimal text (never a "
                "binary float); null means spending policy is disabled."
            ),
        }
    ),
]


class WorkspaceCreate(BaseModel):
    """The creation command: one non-blank, bounded Workspace name.

    No owner field exists: the Workspace's owner is always the authenticated
    Profile (canonical v1 ownership) and no other creation input is honored.
    The name's non-blank/length rule is the service's, not a wire duplicate.
    """

    name: str


class WorkspaceRead(BaseModel):
    """One Workspace's durable identity and current first-class settings."""

    id: UUID
    owner_profile_id: UUID
    name: str
    created_at: datetime
    updated_at: datetime
    review_iteration_limit: int
    guidance: str
    # The Owner-authorized default USD Task budget (issue #240), carried as
    # canonical decimal text (never a binary float). Explicit null means
    # spending policy is disabled — absence is the ordinary unconfigured
    # case, never zero.
    default_task_budget_usd: _ExactUsdAmount | None


class ReviewIterationLimitSetting(BaseModel):
    """The desired review-loop iteration limit value.

    A wire-precise JSON integer (a JSON boolean is not an integer); the
    positivity and transport-neutral bool rules stay the service's.
    """

    review_iteration_limit: StrictInt


class GuidanceSetting(BaseModel):
    """The desired guidance prose value (blank means no Workspace guidance)."""

    guidance: str


class DefaultTaskBudgetSetting(BaseModel):
    """The desired default Task USD budget; explicit null disables spending policy."""

    default_task_budget_usd: _ExactUsdAmount | None


class ReviewIterationLimitChange(BaseModel):
    """The settled result of one review-limit update."""

    workspace_id: UUID
    changed: bool
    previous_review_iteration_limit: int
    new_review_iteration_limit: int


class GuidanceChange(BaseModel):
    """The settled result of one guidance update: whether the value changed.

    The change result identifies only the setting change — Owner-authored
    prose is never echoed back through it; the Workspace read model is the
    supported read surface for the current prose.
    """

    workspace_id: UUID
    changed: bool


class DefaultTaskBudgetChange(BaseModel):
    """The settled result of one default-budget update."""

    workspace_id: UUID
    changed: bool
    previous_default_task_budget_usd: _ExactUsdAmount | None
    new_default_task_budget_usd: _ExactUsdAmount | None
