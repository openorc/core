"""Durable Cline construction-context resolution (issue #189, D8a).

The production implementation of the #74 injected read-only
``ClineConstructionContextResolver`` seam: the missing bridge between the
durable Task/role/Connection facts and ``ClineAdapter`` construction and
exact-session continuity. The adapter consumes only the typed contexts this
resolver supplies — fresh construction inputs keyed by the exact
Task/role, and the authoritative context of an existing bound session keyed
by its exact opaque external session ID — and the adapter itself never
imports services, persistence, or workflow authority.

Boundary and discipline:

- The resolver is a read-only query at the caller/wiring boundary. It never
  mutates durable persistence, never decides workflow authorization, never
  selects or provisions working directories beyond validating the
  composition-selected paths, and emits no WorkflowEvents. No Cloud imports,
  no network I/O, no SSH, and no Cline Hub access: resolution is over the
  local durable Postgres facts only.
- ``resolve_construction_context`` (fresh path) is the establishment-side
  resolution: the exact durable binding must exist for the addressed
  Task/role in its addressed Workspace and be CONNECTING, the ADMITTED
  Connection (never the mutable role-binding route) must be an available,
  enabled Cline Connection of that Workspace, and the current #162 role
  configuration supplies the opaque provider/model pair and prompt
  override. The fresh Producer's working directory is the
  composition-selected bootstrap repository base and the fresh Reviewer's
  is the composition-supplied neutral non-repository directory — both are
  validated, never invented, and never substituted.
- ``resolve_bound_session_context`` (bound path) is the continuity-side
  resolution: the exact external session identity must resolve to exactly
  one durable READY binding, whose admitted Connection is an available,
  enabled Cline Connection and whose current role configuration is
  established. The Producer's original repository base is recovered from
  the binding's own durable ``effective_config_snapshot`` — a later
  Connection configuration edit never relocates the Task — and a supplied
  working directory is accepted only as that original base or the
  deterministic ``<original base>/<task id>`` checkout. A Reviewer session
  never requires a repository base: it resolves with its supplied neutral
  working directory verbatim.
- Failure language follows the answering boundary: fresh-path failures are
  normalized adapter-boundary configuration rejections
  (:class:`AgentRuntimeConfigurationRejectedError`) so the adapter's
  ``create_session`` outcome stays inside its normalized taxonomy;
  bound-path unestablishability is ``None`` — an identity-establishment
  miss that the adapter fails closed on, never evidence of exact-session
  loss. Malformed caller command shape is a ``ValueError`` (the adapter
  validates its own inputs first; a ValueError here is a program error).
- The one-authoritative-selection contract: fresh construction uses the
  ``bootstrap_repository_base`` the composition selected once from the
  admitted Connection before establishment, and establishment pins the
  same selected configuration into the durable READY snapshot. This
  resolver never sources construction paths from the current
  ``Connection.safe_config.repository_base``, so an Owner configuration
  edit between the selection and the runtime call can never make the
  constructed working directory and the pinned snapshot disagree.
"""

from __future__ import annotations

from collections.abc import Mapping
from uuid import UUID

from openorc.adapters.agent_runtime.errors import AgentRuntimeConfigurationRejectedError
from openorc.adapters.cline.construction import (
    ClineConstructionContext,
    ClineConstructionContextResolver,
    ClineSessionIdentity,
)
from openorc.domain.connections import AdapterType, WorkflowRole
from openorc.domain.sessions import TaskAgentSession, TaskSessionLifecycleStatus
from openorc.persistence.connections import get_connection, get_role_binding
from openorc.persistence.pool import DatabasePool
from openorc.persistence.sessions import (
    get_task_agent_session,
    list_task_agent_sessions_by_external_session_id,
)

__all__ = ["DurableClineConstructionContextResolver"]


def _require_uuid(value: object, name: str) -> UUID:
    """Reject a malformed request identifier as a program error."""
    if not isinstance(value, UUID):
        raise ValueError(f"the construction-context request {name} must be a UUID")
    return value


def _producer_snapshot_repository_base(binding: TaskAgentSession) -> str | None:
    """Recover the original selected repository base of one Producer binding.

    The authoritative original base is the one pinned in the binding's own
    durable non-secret ``effective_config_snapshot`` at initialization, never
    a possibly-since-edited Connection setting. A missing, non-canonical, or
    incomplete snapshot fails closed as ``None`` for the bound resolution.
    """
    snapshot = binding.effective_config_snapshot
    if not isinstance(snapshot, Mapping):
        return None
    if snapshot.get("adapter") != AdapterType.CLINE.value:
        return None
    configuration = snapshot.get("configuration")
    if not isinstance(configuration, Mapping):
        return None
    base = configuration.get("repository_base")
    if not isinstance(base, str) or not base.strip():
        return None
    return base


class DurableClineConstructionContextResolver(ClineConstructionContextResolver):
    """Resolve Cline construction inputs from durable Task/role/Connection facts.

    Constructed at the process invocation/composition boundary with the
    durable pool plus the composition-selected construction facts:

    - ``bootstrap_repository_base`` — the existing runtime-side repository
      base selected ONCE from the admitted Producer Connection before
      establishment and shared with the establishment service, so fresh
      construction and the durable READY snapshot record one authoritative
      selection. Required only for fresh Producer construction; a fresh
      Reviewer never uses it.
    - ``working_directory`` — the composition-supplied existing runtime-side
      working directory for the authorized construction phase(s) this
      instance serves (``None`` means the phase has not supplied one yet,
      which passes through so identity-only adapter operations still resolve
      while reconstruction fails closed adapter-side).
    """

    def __init__(
        self,
        pool: DatabasePool,
        *,
        bootstrap_repository_base: str | None = None,
        working_directory: str | None = None,
    ) -> None:
        if bootstrap_repository_base is not None and (
            not isinstance(bootstrap_repository_base, str)
            or not bootstrap_repository_base.strip()
            or not bootstrap_repository_base.startswith("/")
        ):
            raise ValueError(
                "the composition-selected bootstrap repository base must be a "
                "non-empty absolute runtime-side path"
            )
        if working_directory is not None and (
            not isinstance(working_directory, str) or not working_directory.strip()
        ):
            raise ValueError(
                "the composition-supplied working directory must be None or a "
                "non-empty runtime-side path"
            )
        self._pool = pool
        self._bootstrap_repository_base = bootstrap_repository_base
        self._working_directory = working_directory

    def resolve_construction_context(
        self, workspace_id: UUID, task_id: UUID, role: WorkflowRole
    ) -> ClineConstructionContext:
        """Resolve the fresh-construction context for the exact Task/role."""
        _require_uuid(workspace_id, "workspace_id")
        _require_uuid(task_id, "task_id")
        if not isinstance(role, WorkflowRole):
            raise ValueError("the construction-context request role must be a WorkflowRole")
        binding = get_task_agent_session(self._pool, task_id=task_id, role=role)
        if binding is None or binding.workspace_id != workspace_id or binding.task_id != task_id:
            raise AgentRuntimeConfigurationRejectedError(
                "the admitted Task/role session binding for the requested fresh "
                "construction could not be established"
            )
        if binding.lifecycle_status is not TaskSessionLifecycleStatus.CONNECTING:
            # Fresh construction is definitionally the establishment of an
            # admitted CONNECTING binding; READY/LOST/ENDED bindings are
            # never freshly constructed.
            raise AgentRuntimeConfigurationRejectedError(
                "fresh construction requires the exact admitted CONNECTING Task/role binding"
            )
        connection = get_connection(self._pool, binding.connection_id)
        if (
            connection is None
            or connection.workspace_id != workspace_id
            or connection.adapter is not AdapterType.CLINE
        ):
            raise AgentRuntimeConfigurationRejectedError(
                "the admitted Connection of the requested fresh construction is "
                "not an available Cline Connection of the addressed Workspace"
            )
        if not connection.enabled:
            raise AgentRuntimeConfigurationRejectedError(
                "the admitted Connection bound to this Task/role is currently disabled"
            )
        route = get_role_binding(self._pool, workspace_id=workspace_id, role=role)
        if route is None or route.workspace_id != workspace_id:
            raise AgentRuntimeConfigurationRejectedError(
                "the current workflow role configuration for the requested fresh "
                "construction could not be established"
            )
        if role is WorkflowRole.PRODUCER:
            if self._bootstrap_repository_base is None:
                raise AgentRuntimeConfigurationRejectedError(
                    "no composition-selected bootstrap repository base was supplied "
                    "for the fresh Producer construction phase"
                )
            cwd = workspace_root = self._bootstrap_repository_base
        else:
            if self._working_directory is None:
                raise AgentRuntimeConfigurationRejectedError(
                    "no composition-supplied neutral working directory was supplied "
                    "for the fresh Reviewer construction phase"
                )
            cwd = workspace_root = self._working_directory
        return ClineConstructionContext(
            identity=ClineSessionIdentity(
                workspace_id=workspace_id,
                task_id=task_id,
                role=role,
                connection_id=binding.connection_id,
            ),
            configured_provider=route.configured_provider,
            configured_model=route.configured_model,
            role_prompt_override=route.role_prompt_override,
            cwd=cwd,
            workspace_root=workspace_root,
        )

    def resolve_bound_session_context(self, session_id: str) -> ClineConstructionContext | None:
        """Resolve the authoritative context of one existing bound session.

        Keyed by the exact opaque external session ID of the durable
        Task/role binding. ``None`` when no binding context can be
        established for the addressed session — never interpreted as
        exact-session absence.
        """
        if not isinstance(session_id, str) or not session_id.strip():
            return None
        bindings = list_task_agent_sessions_by_external_session_id(
            self._pool, external_session_id=session_id
        )
        if len(bindings) != 1:
            # Zero matches is an establishment miss. More than one match is
            # possible only as an ambiguous cross-Connection duplicate the
            # healthy schema cannot admit: neither is ever guessed, and
            # neither is session-loss evidence.
            return None
        binding = bindings[0]
        if binding.lifecycle_status is not TaskSessionLifecycleStatus.READY:
            # Only a READY binding is an initialized, operable bound session:
            # LOST/ENDED lifecycle records are terminal for runtime effect.
            return None
        connection = get_connection(self._pool, binding.connection_id)
        if (
            connection is None
            or connection.workspace_id != binding.workspace_id
            or connection.adapter is not AdapterType.CLINE
            or not connection.enabled
        ):
            return None
        route = get_role_binding(self._pool, workspace_id=binding.workspace_id, role=binding.role)
        if route is None or route.workspace_id != binding.workspace_id:
            return None
        if binding.role is WorkflowRole.PRODUCER:
            original_base = _producer_snapshot_repository_base(binding)
            if original_base is None:
                return None
            if self._working_directory is None:
                # The authorized phase has not selected a working directory
                # yet: identity-only operations still resolve, and the
                # adapter fails closed on reconstruction without a supplied
                # context. No path is invented or substituted here.
                cwd = workspace_root = None
            elif self._working_directory in (
                original_base,
                f"{original_base}/{binding.task_id}",
            ):
                # The E1 original-base rules: an authorized bound Producer
                # construction runs at the original base or its deterministic
                # isolated checkout, never at a substituted path.
                cwd = workspace_root = self._working_directory
            else:
                return None
        else:
            # A Reviewer is constructed at its separately supplied neutral
            # non-repository directory and never requires a repository base.
            cwd = workspace_root = self._working_directory
        return ClineConstructionContext(
            identity=ClineSessionIdentity(
                workspace_id=binding.workspace_id,
                task_id=binding.task_id,
                role=binding.role,
                connection_id=binding.connection_id,
            ),
            configured_provider=route.configured_provider,
            configured_model=route.configured_model,
            role_prompt_override=route.role_prompt_override,
            cwd=cwd,
            workspace_root=workspace_root,
        )
