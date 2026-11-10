"""Production Cline runtime composition for the F10 checkout cleanup (issue #256).

The production implementation of the runtime-neutral
``services.task_terminal_cleanup.TerminalCheckoutCleanupRuntime`` seam: one
narrow composition over the merged #190 attachment lease
(``attach_cline_runtime``), the #74 same-external-ID reconstruction seam
(``ClineAdapter.reconstruct_session``), and the #181 canonical
``CLEANUP_WORKSPACE`` control realization. The cleanup service owns every
identity, eligibility, and safety decision and derives the exact
interaction; this composition owns only the Cline attachment mechanics and
the caller-authorized ACT construction change, and it never decides
workflow meaning, never mutates durable OpenOrc persistence, never settles
an uncertain operation, and never creates or replaces a session.

- ``cleanup_checkout`` attaches the exact durable READY Producer binding
  BOUND at the caller-derived original repository base (outside the
  checkout being removed — the resolver admits only the original base or
  its deterministic Task checkout, so the working directory is never
  substituted), reconstructs the SAME external session identity into
  native Cline ACT preserving the raw conversation, and realizes the
  already-authorized cleanup control through it, returning the validated
  ``workspace_result``. No initialization is resent and no PLAN control is
  issued; the ``ClineConstructionMode`` vocabulary stays inside this
  Cline-side composition, and the universal ``AgentRuntimeAdapter``
  surface is never expanded.

Failures surface as the normalized #67 adapter-error taxonomy through the
attachment composition's existing classification (the runtime-neutral
cleanup service contains them at its own boundary); routing authority is
the durable admitted binding's ``connection_id``, never the mutable
role-binding route. No Cloud imports; no API routes or RQ jobs here.
"""

from __future__ import annotations

from collections.abc import Callable
from uuid import UUID

from openorc.adapters.cline.bridge_process import ClineBridgeProcessBackend
from openorc.adapters.cline.values import ClineConstructionMode
from openorc.domain.connections import WorkflowRole
from openorc.persistence.pool import DatabasePool
from openorc.protocol.interaction import CleanupWorkspaceInteraction
from openorc.protocol.models import WORKSPACE_RESULT_FAMILY, WorkspaceResult
from openorc.services.cline_attachment import ClineAttachmentPhase, attach_cline_runtime
from openorc.services.errors import ConflictError
from openorc.services.task_terminal_cleanup import TerminalCheckoutCleanupRuntime

__all__ = ["ClineTerminalCleanupComposition"]


class ClineTerminalCleanupComposition(TerminalCheckoutCleanupRuntime):
    """The production Cline composition of the F10 cleanup runtime seam."""

    def __init__(
        self,
        *,
        backend_factory: Callable[[], ClineBridgeProcessBackend] = ClineBridgeProcessBackend,
    ) -> None:
        self._backend_factory = backend_factory

    def cleanup_checkout(
        self,
        pool: DatabasePool,
        *,
        workspace_id: UUID,
        task_id: UUID,
        external_session_id: str,
        working_directory: str,
        interaction: CleanupWorkspaceInteraction,
    ) -> WorkspaceResult:
        # The authorized ACT/base -> ACT/original-base cleanup transition is
        # definitionally the Producer session's; the resolver accepts only
        # the original base or its deterministic Task checkout, so the
        # supplied original-base working directory is never substituted.
        with attach_cline_runtime(
            pool,
            workspace_id=workspace_id,
            task_id=task_id,
            role=WorkflowRole.PRODUCER,
            phase=ClineAttachmentPhase.BOUND,
            external_session_id=external_session_id,
            working_directory=working_directory,
            backend_factory=self._backend_factory,
        ) as attached:
            attached.adapter.reconstruct_session(
                external_session_id,
                target_mode=ClineConstructionMode.ACT,
            )
            result = attached.adapter.realize_control(
                external_session_id,
                interaction,
                message=interaction.render(),
                expected_family=WORKSPACE_RESULT_FAMILY,
            )
        if not isinstance(result, WorkspaceResult):
            # Defensive composition invariant: the #67 funnel returns the
            # validated formal response for the expected family. Never
            # guessed around.
            raise ConflictError("the cleanup realization returned no validated workspace result")
        return result
