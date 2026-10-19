"""Production Cline runtime composition for the #203 planning bootstrap.

The production implementation of the runtime-neutral
``services.task_planning_bootstrap.TaskBootstrapRuntimeComposition`` seam:
three narrow compositions over the merged #190 attachment lease
(``attach_cline_runtime``) and the #74 same-external-ID reconstruction seam
(``ClineAdapter.reconstruct_session``). The bootstrap continuation owns
every authorization, currentness, stage, and workflow-consequence decision;
this composition owns only the Cline attachment mechanics and the
caller-authorized construction change, and it never decides workflow
meaning, never mutates durable OpenOrc persistence, and never settles an
uncertain operation.

- ``establishment_lease`` wraps the #190 FRESH attachment lease for the
  exact admitted Task/role/Connection: the fresh Producer's bootstrap
  repository base is the composition-selected admitted-Connection
  configuration itself (the caller supplies no working directory), and the
  fresh Reviewer resolves the separately supplied existing neutral
  non-repository directory. The lease yields the attached adapter and the
  one selected non-secret configuration the establishment pins into its
  durable READY snapshot.
- ``interaction_lease`` wraps the #190 BOUND attachment lease for the exact
  durable READY binding's external session identity, yielding the adapter
  the continuation realizes the already-authorized #181 workspace
  preparation control through. The BOUND lease's construction context is
  identity-only: a prose control realization needs no working directory,
  and none is invented here.
- ``enter_planning_checkout`` is the authorized same-external-ID planning
  transition: one BOUND attachment at the exact selected Task checkout
  directory and one ``reconstruct_session`` into native Cline PLAN,
  preserving the exact external session identity and the raw conversation,
  resending no initialization, and issuing no PLAN control. The
  ``ClineConstructionMode`` vocabulary stays inside this Cline-side
  composition; the runtime-neutral bootstrap continuation never sees it,
  and the universal ``AgentRuntimeAdapter`` surface is never expanded.

Failures surface as the normalized #67 adapter-error taxonomy through the
attachment composition's existing classification; routing authority is the
durable admitted binding's ``connection_id``, never the mutable role-binding
route. No Cloud imports; no API routes or RQ jobs here.
"""

from __future__ import annotations

from collections.abc import Callable
from contextlib import contextmanager
from uuid import UUID

from openorc.adapters.cline.bridge_process import ClineBridgeProcessBackend
from openorc.adapters.cline.values import ClineConstructionMode
from openorc.domain.connections import WorkflowRole
from openorc.persistence.pool import DatabasePool
from openorc.services.cline_attachment import (
    AttachedClineRuntime,
    ClineAttachmentPhase,
    attach_cline_runtime,
)
from openorc.services.errors import ConflictError
from openorc.services.task_planning_bootstrap import (
    TaskBootstrapEstablishmentLease,
    TaskBootstrapRuntimeComposition,
)

__all__ = ["ClineBootstrapRuntimeComposition"]


class ClineBootstrapRuntimeComposition(TaskBootstrapRuntimeComposition):
    """The production Cline composition of the #203 bootstrap runtime seam."""

    def __init__(
        self,
        *,
        backend_factory: Callable[[], ClineBridgeProcessBackend] = ClineBridgeProcessBackend,
    ) -> None:
        self._backend_factory = backend_factory

    @contextmanager
    def establishment_lease(
        self,
        pool: DatabasePool,
        *,
        workspace_id: UUID,
        task_id: UUID,
        role: WorkflowRole,
        working_directory: str | None,
    ):
        with attach_cline_runtime(
            pool,
            workspace_id=workspace_id,
            task_id=task_id,
            role=role,
            phase=ClineAttachmentPhase.FRESH,
            working_directory=working_directory,
            backend_factory=self._backend_factory,
        ) as attached:
            yield self._establishment_lease(attached)

    @staticmethod
    def _establishment_lease(
        attached: AttachedClineRuntime,
    ) -> TaskBootstrapEstablishmentLease:
        if attached.selected_configuration is None:
            # Defensive composition invariant: the fresh lease always
            # carries the one selected non-secret configuration.
            raise ConflictError("the fresh attachment lease carried no selected configuration")
        return TaskBootstrapEstablishmentLease(
            adapter=attached.adapter,
            selected_configuration=attached.selected_configuration,
        )

    @contextmanager
    def interaction_lease(
        self,
        pool: DatabasePool,
        *,
        workspace_id: UUID,
        task_id: UUID,
        role: WorkflowRole,
        external_session_id: str,
    ):
        with attach_cline_runtime(
            pool,
            workspace_id=workspace_id,
            task_id=task_id,
            role=role,
            phase=ClineAttachmentPhase.BOUND,
            external_session_id=external_session_id,
            backend_factory=self._backend_factory,
        ) as attached:
            yield attached.adapter

    def enter_planning_checkout(
        self,
        pool: DatabasePool,
        *,
        workspace_id: UUID,
        task_id: UUID,
        external_session_id: str,
        checkout_directory: str,
    ) -> None:
        # The authorized ACT/base -> PLAN/checkout planning transition is
        # definitionally the Producer session's; the resolver accepts only
        # the original base or its deterministic Task checkout, so the
        # working directory is never substituted.
        with attach_cline_runtime(
            pool,
            workspace_id=workspace_id,
            task_id=task_id,
            role=WorkflowRole.PRODUCER,
            phase=ClineAttachmentPhase.BOUND,
            external_session_id=external_session_id,
            working_directory=checkout_directory,
            backend_factory=self._backend_factory,
        ) as attached:
            attached.adapter.reconstruct_session(
                external_session_id,
                target_mode=ClineConstructionMode.PLAN,
            )
