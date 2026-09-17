"""Connection and workflow role binding domain models.

A Connection is one configured agent-runtime route within one Workspace. A
WorkflowRoleBinding is the mutable per-role runtime configuration that points
a workflow role (Producer or Reviewer) at a Connection.

- Raw OpenOrc-owned credentials never appear in these models. OpenOrc-owned
  authentication is represented only through the nullable, opaque
  ``auth_reference`` boundary: ``None`` means no OpenOrc-owned auth reference
  is currently configured; a non-empty string means one has been supplied.
  Validation, expiry, revocation, health, and authentication lifecycle state
  belong to later functionality that can actually determine those facts, so
  no speculative auth status vocabulary exists.
- ``enabled`` is the Owner-controlled eligibility switch: OpenOrc is
  permitted to use the Connection. It says nothing about runtime reachability,
  health, initialization, READY, or WORKING state.
- ``session_capacity`` is Owner-configured OpenOrc admission control, scoped
  to the Connection and never discovered from the runtime. The default of 1
  is deliberate: concurrency must be explicitly enabled by the Owner.
- ``reported_provider``/``reported_model`` are nullable opaque runtime-reported
  observation strings — never configuration authority, never enums. A future
  OpenOrc-side selection capability would add separate ``configured_*``
  concepts rather than repurposing these fields.
- ``safe_config`` is the non-secret Owner configuration container. It is
  validated only for structural JSON-serializability; concrete adapter
  configuration schemas enforce allowed fields once those configurations
  exist. Secret material never belongs here.
- Exactly one binding exists per ``(Workspace, role)`` in v1 (no runtime
  pools, no failover). Producer and Reviewer bindings are independent and may
  reference the same Connection or separate Connections. The binding carries
  only workspace/role/connection identity and timestamps; per-role session
  configuration is added later only when a real configurable property exists.
"""

from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import dataclass, fields
from datetime import datetime
from enum import StrEnum
from types import MappingProxyType
from uuid import UUID

__all__ = [
    "AdapterType",
    "Connection",
    "ConnectionDomainError",
    "WorkflowRole",
    "WorkflowRoleBinding",
]


class ConnectionDomainError(Exception):
    """Raised when a Connection or WorkflowRoleBinding invariant is violated."""


class WorkflowRole(StrEnum):
    """The two workflow roles that consume runtime bindings in v1."""

    PRODUCER = "producer"
    REVIEWER = "reviewer"


class AdapterType(StrEnum):
    """The agent-runtime adapter a Connection routes to. v1: Cline only."""

    CLINE = "cline"


@dataclass(frozen=True, slots=True)
class Connection:
    """One configured agent-runtime route within one Workspace.

    Configuration and routing metadata only: this object carries no runtime
    credentials and no authentication lifecycle state. OpenOrc-owned auth is
    represented solely by the opaque nullable ``auth_reference``.
    """

    id: UUID
    workspace_id: UUID
    adapter: AdapterType
    name: str
    safe_config: Mapping[str, object]
    session_capacity: int
    enabled: bool
    auth_reference: str | None
    reported_provider: str | None
    reported_model: str | None
    created_at: datetime
    updated_at: datetime

    def __post_init__(self) -> None:
        if not isinstance(self.name, str) or not self.name.strip():
            raise ConnectionDomainError("Connection.name must be a non-empty string")
        if isinstance(self.session_capacity, bool) or not isinstance(self.session_capacity, int):
            raise ConnectionDomainError(
                "Connection.session_capacity must be a positive integer "
                "(Owner-configured admission control)"
            )
        if self.session_capacity <= 0:
            raise ConnectionDomainError(
                f"Connection.session_capacity must be positive, got {self.session_capacity}"
            )
        if not isinstance(self.enabled, bool):
            raise ConnectionDomainError("Connection.enabled must be a boolean")
        if self.auth_reference is not None and (
            not isinstance(self.auth_reference, str) or not self.auth_reference.strip()
        ):
            raise ConnectionDomainError(
                "Connection.auth_reference must be None or a non-empty opaque reference "
                "(NULL means no OpenOrc-owned auth reference is configured)"
            )
        for field_name in ("reported_provider", "reported_model"):
            value = getattr(self, field_name)
            # Opaque runtime-reported observations: any string, or None. No
            # vocabulary, no enum, no normalization — these are never
            # configuration authority.
            if value is not None and not isinstance(value, str):
                raise ConnectionDomainError(f"Connection.{field_name} must be None or a string")
        object.__setattr__(self, "safe_config", self._validated_safe_config(self.safe_config))

    @staticmethod
    def _validated_safe_config(value: object) -> Mapping[str, object]:
        """Validate the non-secret configuration container and freeze its view.

        Only structural rules apply: it must be a string-keyed mapping whose
        content survives strict JSON round-tripping (jsonb storage). No
        credential-shaped key rejection happens here — the contract is that
        secret material does not belong in this container at all, and concrete
        adapter configuration schemas enforce allowed fields when they exist.
        """
        if not isinstance(value, Mapping):
            raise ConnectionDomainError("Connection.safe_config must be a mapping")
        try:
            json.dumps(dict(value), allow_nan=False)
        except (TypeError, ValueError) as exc:
            raise ConnectionDomainError(
                f"Connection.safe_config must be JSON-serializable for jsonb storage: {exc}"
            ) from exc
        return MappingProxyType(dict(value))


@dataclass(frozen=True, slots=True)
class WorkflowRoleBinding:
    """Mutable per-role runtime configuration within one Workspace.

    Exactly one binding per ``(workspace_id, role)`` in v1. The binding is
    honest about its shape: workspace/role/connection identity and timestamps
    only.
    """

    id: UUID
    workspace_id: UUID
    role: WorkflowRole
    connection_id: UUID
    created_at: datetime
    updated_at: datetime

    def __post_init__(self) -> None:
        if not isinstance(self.role, WorkflowRole):
            raise ConnectionDomainError(
                "WorkflowRoleBinding.role must be a WorkflowRole (producer or reviewer)"
            )


def connection_field_names() -> frozenset[str]:
    """Return the exact field set a Connection exposes.

    Lets tests prove ordinary persistence objects expose no raw-secret field:
    OpenOrc-owned authentication appears only through the opaque nullable
    ``auth_reference`` boundary.
    """
    return frozenset(field.name for field in fields(Connection))


def workflow_role_binding_field_names() -> frozenset[str]:
    """Return the exact field set a WorkflowRoleBinding exposes."""
    return frozenset(field.name for field in fields(WorkflowRoleBinding))
