"""Typed construction inputs and the injected read-only construction-context
resolver of the Cline adapter (issue #74, D5a).

The universal ``AgentSessionCreationRequest`` deliberately carries no
provider/model/working-directory fields, and the universal contract never
grows them. The Cline adapter instead consumes a small **injected, read-only
construction-context resolver** supplied at the adapter wiring boundary. The
resolver is the authoritative seam for both construction inputs and
session-identity continuity:

- ``resolve_construction_context`` is keyed by the exact Task/role and
  supplies the construction inputs for a FRESH session: the current #162
  ``WorkflowRoleBinding`` configuration (opaque provider/model pair plus the
  nullable role-prompt Markdown override), the workflow-selected existing
  runtime-side working directory for the authorized construction phase
  (bootstrap repository base, Task checkout, cleanup base, or the Reviewer's
  neutral directory), and enough admitted-Connection/Task/role identity to
  prevent misrouting.
- ``resolve_bound_session_context`` is keyed by the exact bound external
  session ID and supplies the same typed context for an EXISTING durable
  Task/role session — the authority that lets any adapter instance (including
  a fresh instance after a process restart) operate on and reconstruct the
  exact bound session without repeating initialization. A ``None`` answer is
  an identity-establishment miss, never evidence of session loss.

The resolver implementation lives at the caller/wiring boundary (it may read
durable bindings there); this module defines only the contract and the typed
values, and the adapter itself imports no services, persistence, or Cloud.
The caller/wiring — never the adapter — determines and preserves the
authoritative repository-base/Task-checkout paths across worker restarts and
configuration changes; the adapter never derives paths from Task IDs and
never reads arbitrary agent-reported paths.

Absent construction inputs are representable on purpose: a #162 role binding
may be entirely unconfigured, and a workflow phase may not have supplied a
working directory yet. The adapter fails closed on any absent input BEFORE
any runtime effect; the typed context merely carries what the wiring
resolved.

Role Markdown resolution is adapter-owned: ``role_prompt_override is None``
loads the current packaged ``openorc.protocol`` ``role_prompts`` asset for
the role; any non-``None`` override — including an empty non-NULL string —
passes verbatim. These are role behavior inputs for the Cline ``rules``
construction field, never initialization or workflow-control prompts.
"""

from __future__ import annotations

from dataclasses import dataclass
from functools import cache
from importlib import resources
from uuid import UUID

from openorc.adapters.agent_runtime.errors import AgentRuntimeConfigurationRejectedError
from openorc.domain.connections import WorkflowRole

__all__ = [
    "ClineConstructionContext",
    "ClineConstructionContextResolver",
    "ClineSessionIdentity",
    "resolve_role_markdown",
]


def _require_non_empty_string(owner: str, name: str, value: object) -> None:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{owner}.{name} must be a non-empty string when present")


@dataclass(frozen=True)
class ClineSessionIdentity:
    """The exact admitted Task/role/Connection identity of one bound session.

    The narrow identity facts that prevent misrouting: every role-sensitive
    adapter operation proves the addressed external session is bound to the
    exact expected Task/role/Connection before any runtime effect. Diagnostics
    may carry these identifiers and nothing else about the session.
    """

    workspace_id: UUID
    task_id: UUID
    role: WorkflowRole
    connection_id: UUID

    def __post_init__(self) -> None:
        for name in ("workspace_id", "task_id", "connection_id"):
            if not isinstance(getattr(self, name), UUID):
                raise ValueError(f"ClineSessionIdentity.{name} must be a UUID")
        if not isinstance(self.role, WorkflowRole):
            raise ValueError("ClineSessionIdentity.role must be a WorkflowRole")


@dataclass(frozen=True)
class ClineConstructionContext:
    """The typed construction inputs the wiring resolved for one construction.

    ``configured_provider``/``configured_model`` mirror the #162 binding
    configuration: opaque Owner-supplied identifiers, complete only when both
    are present (a partial pair is not representable). ``role_prompt_override``
    is ``None`` (use the current packaged default for the role) or the Owner's
    verbatim Markdown — including an empty non-NULL string. ``cwd`` and
    ``workspace_root`` are the workflow-selected existing runtime-side working
    directory for this authorized phase; ``None`` means the phase has not
    supplied one yet, which the adapter fails closed on before any runtime
    effect. The adapter never invents, provisions, or checks any of these
    values against its own filesystem.
    """

    identity: ClineSessionIdentity
    configured_provider: str | None
    configured_model: str | None
    role_prompt_override: str | None
    cwd: str | None
    workspace_root: str | None

    def __post_init__(self) -> None:
        if not isinstance(self.identity, ClineSessionIdentity):
            raise ValueError("ClineConstructionContext.identity must be a ClineSessionIdentity")
        if (self.configured_provider is None) != (self.configured_model is None):
            raise ValueError(
                "ClineConstructionContext provider/model configuration is complete "
                "only when both values are present; a partial configuration cannot "
                "be represented"
            )
        for name in ("configured_provider", "configured_model"):
            value = getattr(self, name)
            if value is not None:
                _require_non_empty_string("ClineConstructionContext", name, value)
        if self.role_prompt_override is not None and not isinstance(self.role_prompt_override, str):
            raise ValueError(
                "ClineConstructionContext.role_prompt_override must be None (use "
                "the current packaged default for the role) or the Owner's "
                "verbatim Markdown override"
            )
        for name in ("cwd", "workspace_root"):
            value = getattr(self, name)
            if value is not None:
                _require_non_empty_string("ClineConstructionContext", name, value)


class ClineConstructionContextResolver:
    """The read-only construction-context seam injected at adapter wiring.

    Implementations resolve the then-current typed construction inputs from
    the durable Task/role/Connection binding authority at the caller/wiring
    boundary. They are read-only queries: they never mutate durable
    persistence, never select or provision working directories, and never
    decide workflow authorization. ``resolve_bound_session_context`` returning
    ``None`` means the binding context could not be established for that
    exact external session ID — an establishment miss, never session loss.
    """

    def resolve_construction_context(
        self, workspace_id: UUID, task_id: UUID, role: WorkflowRole
    ) -> ClineConstructionContext:
        """Resolve the fresh-construction context for the exact Task/role."""
        raise NotImplementedError

    def resolve_bound_session_context(self, session_id: str) -> ClineConstructionContext | None:
        """Resolve the authoritative context of one existing bound session.

        Keyed by the exact opaque external session ID of the durable
        Task/role binding. ``None`` when no binding context can be
        established for the addressed session — never interpreted as
        exact-session absence.
        """
        raise NotImplementedError


@cache
def _packaged_role_markdown(role: str) -> str:
    """Load the packaged role-prompt Markdown asset for one role.

    Loaded from the installed OpenOrc package; never overridden by a
    Workspace, a Connection, a database table, or a prompt registry.
    """
    asset = resources.files("openorc.protocol") / "role_prompts" / f"{role}.md"
    return asset.read_text(encoding="utf-8")


def resolve_role_markdown(role: WorkflowRole, role_prompt_override: str | None) -> str:
    """Resolve the role behavior Markdown for one construction.

    ``None`` loads the current packaged ``role_prompts`` asset for the role;
    any non-``None`` override passes verbatim, including an empty non-NULL
    string. A missing packaged asset fails closed as a normalized
    configuration rejection before any runtime effect.
    """
    if role_prompt_override is not None:
        if not isinstance(role_prompt_override, str):
            raise ValueError("the role-prompt override must be None or a string when resolved")
        return role_prompt_override
    try:
        return _packaged_role_markdown(role.value)
    except (OSError, UnicodeError):
        pass
    raise AgentRuntimeConfigurationRejectedError(
        "the packaged role prompt asset for the workflow role could not be loaded"
    ) from None
