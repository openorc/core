"""Typed normalized results of the universal Agent Runtime session contract
(issue #67).

Only safe normalized facts cross this boundary upward: the opaque external
session identity of a ready session, nullable opaque runtime-reported
provenance observations, and the normalized agent reply text for sends that
expect no formal response. Runtime credentials, provider tokens, Hub URLs
carrying sensitive identifiers, initialization bodies, transcripts, raw
runtime response objects, and SDK-native objects never appear as result
fields.

The creation request carries the exact Task/role identity facts plus the
composed canonical #66 initialization content supplied through the creation
boundary. The adapter treats the initialization content as opaque delivery
payload: it is delivered verbatim, never authored, selected, or interpreted
by the adapter, and never echoed back in results or diagnostics.
"""

from __future__ import annotations

from dataclasses import dataclass
from uuid import UUID

from openorc.domain.connections import WorkflowRole

__all__ = [
    "AgentSessionCreated",
    "AgentSessionCreationRequest",
    "AgentTextResponse",
]


@dataclass(frozen=True)
class AgentSessionCreationRequest:
    """The exact Task/role context and initialization content for one
    ``create_session`` readiness handshake.

    ``workspace_id``/``task_id`` are the narrow identity facts a runtime
    needs to establish an isolated context for the exact Task/role, and the
    only safe identifiers diagnostics may carry. ``initialization`` is the
    fully composed canonical initialization content supplied through the
    creation boundary: the adapter delivers it verbatim and never authors,
    selects, or re-renders prompt content.
    """

    workspace_id: UUID
    task_id: UUID
    role: WorkflowRole
    initialization: str

    def __post_init__(self) -> None:
        if not isinstance(self.workspace_id, UUID):
            raise ValueError("AgentSessionCreationRequest.workspace_id must be a UUID")
        if not isinstance(self.task_id, UUID):
            raise ValueError("AgentSessionCreationRequest.task_id must be a UUID")
        if not isinstance(self.role, WorkflowRole):
            raise ValueError("AgentSessionCreationRequest.role must be a WorkflowRole")
        if not isinstance(self.initialization, str) or not self.initialization.strip():
            raise ValueError(
                "AgentSessionCreationRequest.initialization must be a non-empty string"
            )


@dataclass(frozen=True)
class AgentSessionCreated:
    """One successfully established ready external Task/role session.

    Success is itself the evidence that the canonical initialization
    interaction completed with a valid ``session_ready`` formal response:
    callers never need a second ordinary send merely to establish readiness.
    ``external_session_id`` addresses exactly this ready context and is
    opaque to OpenOrc. The reported provenance observations are nullable
    opaque runtime-reported facts — never configuration authority.
    """

    external_session_id: str
    reported_provider: str | None = None
    reported_model: str | None = None
    reported_runtime_version: str | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.external_session_id, str) or not self.external_session_id.strip():
            raise ValueError("AgentSessionCreated.external_session_id must be a non-empty string")
        for name in ("reported_provider", "reported_model", "reported_runtime_version"):
            value = getattr(self, name)
            if value is not None and not (isinstance(value, str) and value.strip()):
                raise ValueError(f"AgentSessionCreated.{name} must be None or a non-empty string")


@dataclass(frozen=True)
class AgentTextResponse:
    """The normalized agent reply for a send that expected no formal
    response — for example advisory Owner ↔ Reviewer discussion routed
    through the existing Reviewer session.

    ``text`` is the runtime-envelope-stripped candidate reply exactly as the
    adapter normalized it, with no added meaning. It is never parsed against
    the formal protocol: a reply that resembles or contains a formal JSON
    object stays ordinary text, because the caller's expected response
    family is the only trigger for shared #65 parsing.
    """

    text: str

    def __post_init__(self) -> None:
        if not isinstance(self.text, str):
            raise ValueError("AgentTextResponse.text must be a string")
