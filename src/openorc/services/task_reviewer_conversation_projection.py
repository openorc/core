"""The narrow authorized Reviewer-conversation read projection (issue #339).

The Owner-facing read surface of the asynchronous discussion seam: the
authorized client reads the current Reviewer conversation through the
runtime-native transcript/query surface via the optional
``SessionConversationReader`` capability — one pure observational adapter
read, projected transiently. The projection owns authorization and exact
session identity only: the account-operational barrier, the exact
authenticated Owner Workspace/Task scope, and the Task's exact existing
admitted READY Reviewer binding (the original live session, never a
reconstruction or replacement).

The read takes no external effect, records nothing durable, and never
becomes a transcript store: the runtime owns the conversation, OpenOrc
projects it to the authorized caller at read time, and no workflow event,
receipt outcome, or OpenOrc table ever carries it. The projection is
deliberately narrow — no general chat service, no replay, no search, no
cross-session history.
"""

from __future__ import annotations

from collections.abc import Callable, Iterator
from contextlib import AbstractContextManager, contextmanager
from dataclasses import dataclass
from typing import Final, Protocol, cast
from uuid import UUID

from openorc.adapters.agent_runtime.contract import AgentRuntimeAdapter
from openorc.domain.connections import WorkflowRole
from openorc.domain.sessions import TaskAgentSession, TaskSessionLifecycleStatus
from openorc.observability import annotate_span, application_span
from openorc.persistence.pool import DatabasePool
from openorc.persistence.sessions import get_task_agent_session
from openorc.services.cline_attachment import ClineAttachmentPhase, attach_cline_runtime
from openorc.services.errors import (
    ConflictError,
    ExternalOperationFailedError,
    InvalidCommandError,
    NotFoundError,
)
from openorc.services.profile_lifecycle_guard import require_account_operational
from openorc.services.transaction_composition import composed_transaction
from openorc.services.workspace_authorization import require_workspace_task

__all__ = [
    "ReviewerConversationMessage",
    "ReviewerConversationProjection",
    "ReviewerRuntimeAttachmentFactory",
    "build_cline_reviewer_conversation_factory",
    "project_task_reviewer_conversation",
]

_TRACER_SCOPE: Final = "openorc.services.task_reviewer_conversation_projection"
_PROJECTION_SPAN_NAME = "task_reviewer_conversation_projection.project_task_reviewer_conversation"

ReviewerRuntimeAttachmentFactory = Callable[
    [TaskAgentSession], AbstractContextManager[AgentRuntimeAdapter]
]


class ConversationReaderRuntime(Protocol):
    """The duck-typed conversation-read capability of the attached runtime."""

    def read_conversation(self, session_id: str) -> list[object]:
        """Project the session's current conversation records, in order."""
        ...


@dataclass(frozen=True, slots=True)
class ReviewerConversationMessage:
    """One projected conversation message (the runtime's own record shape).

    ``index`` is the zero-based conversation position; ``kind`` is the
    runtime message's own category token when present; ``text`` is its
    textual content when present. A message shaped beyond the projection
    carries ``None`` fields — honestly unknown, never guessed.
    """

    index: int
    kind: str | None
    text: str | None


@dataclass(frozen=True, slots=True)
class ReviewerConversationProjection:
    """The transient projection of one Reviewer session's conversation.

    No message content is persisted anywhere by this service; the returned
    projection exists only for the authorized response.
    """

    task_id: UUID
    reviewer_session_id: UUID
    external_session_id: str
    messages: tuple[ReviewerConversationMessage, ...]


def build_cline_reviewer_conversation_factory(
    pool: DatabasePool,
    *,
    workspace_id: UUID,
    task_id: UUID,
    reviewer_working_directory: str,
) -> ReviewerRuntimeAttachmentFactory:
    """Compose the production per-session Reviewer read attachment factory.

    The deferred-attachment seam the projection consumes: the service
    decides WHEN the adapter is acquired (strictly after its authorization),
    and this composition owns HOW — the exact durable READY Reviewer binding
    at the deployment's reviewer-neutral directory. Construction composes no
    attachment and touches no runtime.
    """

    def factory(session: TaskAgentSession) -> AbstractContextManager[AgentRuntimeAdapter]:
        external_session_id = session.external_session_id
        if external_session_id is None or not external_session_id.strip():
            raise ConflictError("the reviewer session binding carries no external session identity")

        @contextmanager
        def lease() -> Iterator[AgentRuntimeAdapter]:
            with attach_cline_runtime(
                pool,
                workspace_id=workspace_id,
                task_id=task_id,
                role=WorkflowRole.REVIEWER,
                phase=ClineAttachmentPhase.BOUND,
                external_session_id=external_session_id,
                working_directory=reviewer_working_directory,
            ) as attached:
                yield attached.adapter

        return lease()

    return factory


def _require_uuid_command(value: object, name: str) -> UUID:
    """Reject a malformed command identifier before any read or effect."""
    if not isinstance(value, UUID):
        raise InvalidCommandError(f"{name} must be a UUID")
    return value


def _require_factory(value: object) -> ReviewerRuntimeAttachmentFactory:
    """Require the deferred Reviewer attachment factory dependency."""
    if not callable(value):
        raise InvalidCommandError(
            "reviewer_runtime_factory must be a callable returning one adapter "
            "attachment context manager for the exact resolved Reviewer binding"
        )
    return cast(ReviewerRuntimeAttachmentFactory, value)


def project_task_reviewer_conversation(
    pool: DatabasePool,
    *,
    profile_id: UUID,
    workspace_id: UUID,
    task_id: UUID,
    reviewer_runtime_factory: ReviewerRuntimeAttachmentFactory,
) -> ReviewerConversationProjection:
    """Read the Task's Reviewer conversation for its authorized Owner.

    One short authorization transaction (the account-operational barrier and
    the exact authenticated Owner Workspace/Task scope for the claimed
    ``profile_id``) closes before any adapter acquisition; the exact existing
    admitted READY Reviewer binding is the only session the projection ever
    reads — the original live session identity, never a reconstruction or a
    replacement. The runtime's conversation-read capability is duck-typed
    fail-closed, and the read is one observational adapter call whose
    records are projected into the neutral shape. No database transaction is
    open across the read, and nothing is persisted.
    """
    with application_span(_TRACER_SCOPE, _PROJECTION_SPAN_NAME) as span:
        # --- Phase 0: command shape (no I/O). ---
        _require_uuid_command(profile_id, "profile_id")
        _require_uuid_command(workspace_id, "workspace_id")
        _require_uuid_command(task_id, "task_id")
        factory = _require_factory(reviewer_runtime_factory)
        annotate_span(
            span,
            operation=_PROJECTION_SPAN_NAME,
            workspace_id=str(workspace_id),
            task_id=str(task_id),
        )

        # --- Phase 1: authorization (one short transaction). ---
        with composed_transaction(pool) as transaction_pool:
            require_account_operational(transaction_pool, profile_id=profile_id)
            require_workspace_task(
                transaction_pool,
                profile_id=profile_id,
                workspace_id=workspace_id,
                task_id=task_id,
            )

        # --- Phase 2: the exact existing READY Reviewer binding. ---
        binding = get_task_agent_session(pool, task_id=task_id, role=WorkflowRole.REVIEWER)
        if binding is None or binding.workspace_id != workspace_id or binding.task_id != task_id:
            raise NotFoundError(
                "the requested reviewer session binding is not available for this task"
            )
        if binding.lifecycle_status is not TaskSessionLifecycleStatus.READY:
            raise ConflictError(
                "the reviewer session binding is not READY; the conversation read "
                "uses the exact admitted session only"
            )
        external_session_id = binding.external_session_id
        if external_session_id is None or not external_session_id.strip():
            raise ConflictError("the reviewer session binding carries no external session identity")

        # --- Phase 3: the observational read (no transaction open). ---
        with factory(binding) as runtime:
            read_conversation = getattr(runtime, "read_conversation", None)
            if not callable(read_conversation):
                raise ExternalOperationFailedError(
                    "the reviewer runtime does not implement the conversation read "
                    "capability; the projection is unavailable"
                )
            reader = cast(ConversationReaderRuntime, runtime)
            try:
                messages = reader.read_conversation(external_session_id)
            except Exception as error:
                raise ExternalOperationFailedError(
                    "the reviewer conversation read failed; the projection is "
                    "unavailable and nothing was recorded"
                ) from error
        projected: list[ReviewerConversationMessage] = []
        for message in messages:
            if isinstance(message, ReviewerConversationMessage):
                projected.append(message)
            elif isinstance(message, dict):
                # Tolerate plain mapping records from duck-typed fakes with
                # the same defensive extraction the adapter performs.
                kind = message.get("kind")
                text = message.get("text")
                index = message.get("index")
                projected.append(
                    ReviewerConversationMessage(
                        index=index if isinstance(index, int) and index >= 0 else len(projected),
                        kind=kind if isinstance(kind, str) else None,
                        text=text if isinstance(text, str) else None,
                    )
                )
            else:
                raise ExternalOperationFailedError(
                    "the reviewer runtime returned an unreadable conversation record; "
                    "the projection is unavailable"
                )
        return ReviewerConversationProjection(
            task_id=task_id,
            reviewer_session_id=binding.id,
            external_session_id=external_session_id,
            messages=tuple(projected),
        )
