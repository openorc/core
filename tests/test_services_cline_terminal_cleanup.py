"""Deterministic tests for the production Cline cleanup composition (issue #344).

The composition is a narrow pass-through over proven seams (the #190
attachment lease, the #74 same-ID reconstruction, the #348 detached
capability): these tests prove the NEW composition wiring only — the
serialized same-external-session reconstruction ends in the adapter's
detached admission with the canonical rendered instruction verbatim, and
the adapter's normalized classification propagates untouched. The
attachment and adapter behaviors themselves are owned by
``test_services_cline_attachment.py`` and ``test_adapters_cline_adapter.py``.
"""

from __future__ import annotations

from contextlib import contextmanager
from typing import Any
from uuid import UUID

import pytest

from openorc.adapters.agent_runtime import AgentTurnAdmission
from openorc.adapters.agent_runtime.errors import AgentRuntimeUncertainOutcomeError
from openorc.adapters.cline.values import ClineConstructionMode
from openorc.domain.connections import WorkflowRole
from openorc.protocol.interaction import CleanupWorkspaceInteraction
from openorc.services.cline_attachment import ClineAttachmentPhase
from openorc.services.cline_terminal_cleanup import ClineTerminalCleanupComposition

_WORKSPACE_ID = UUID("0f1c2d3e-4a5b-6c7d-8e9f-000000000001")
_TASK_ID = UUID("0f1c2d3e-4a5b-6c7d-8e9f-000000000002")
_CONNECTION_ID = UUID("0f1c2d3e-4a5b-6c7d-8e9f-000000000003")
_EXTERNAL_SESSION_ID = "producer-external-session"
_ORIGINAL_BASE = "/srv/runtime/repositories"


class _RecordingAdapter:
    def __init__(self, *, uncertain: bool = False) -> None:
        self._uncertain = uncertain
        self.reconstructions: list[tuple[str, ClineConstructionMode]] = []
        self.admissions: list[tuple[str, str]] = []

    def reconstruct_session(self, session_id: str, *, target_mode: ClineConstructionMode) -> None:
        self.reconstructions.append((session_id, target_mode))

    def admit_detached_turn(self, session_id: str, content: str) -> AgentTurnAdmission:
        self.admissions.append((session_id, content))
        if self._uncertain:
            raise AgentRuntimeUncertainOutcomeError("unknown admission outcome")
        return AgentTurnAdmission(turn_reference="run-344")


class _RecordingAttachment:
    def __init__(self, adapter: _RecordingAdapter) -> None:
        self.adapter = adapter
        self.calls: list[dict[str, Any]] = []


def _wire_attachment(monkeypatch: pytest.MonkeyPatch, attached: _RecordingAttachment) -> None:
    from openorc.services import cline_terminal_cleanup as composition_module

    @contextmanager
    def _fake_attach(pool: Any, **kwargs: Any):
        attached.calls.append(dict(kwargs))
        yield attached

    monkeypatch.setattr(composition_module, "attach_cline_runtime", _fake_attach)


def _interaction() -> CleanupWorkspaceInteraction:
    return CleanupWorkspaceInteraction(
        repository_full_name="octocat/admitted-repo",
        issue_number=4311,
        task_uuid=_TASK_ID,
        target_directory=f"{_ORIGINAL_BASE}/{_TASK_ID}",
    )


def test_the_cleanup_admission_reconstructs_act_and_admits_the_canonical_instruction(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The serialized same-external-session reconstruction (issue #344): the
    exact READY Producer binding is attached BOUND at the original base,
    reconstructed into native ACT, and the canonical rendered cleanup
    instruction is admitted verbatim as ONE detached turn."""
    adapter = _RecordingAdapter()
    attached = _RecordingAttachment(adapter)
    _wire_attachment(monkeypatch, attached)
    composition = ClineTerminalCleanupComposition()
    interaction = _interaction()

    admitted = composition.admit_cleanup_turn(
        None,  # type: ignore[arg-type]
        workspace_id=_WORKSPACE_ID,
        task_id=_TASK_ID,
        external_session_id=_EXTERNAL_SESSION_ID,
        working_directory=_ORIGINAL_BASE,
        interaction=interaction,
    )

    assert admitted.turn_reference == "run-344"
    assert len(attached.calls) == 1
    attach_call = attached.calls[0]
    assert attach_call["workspace_id"] == _WORKSPACE_ID
    assert attach_call["task_id"] == _TASK_ID
    assert attach_call["role"] is WorkflowRole.PRODUCER
    assert attach_call["phase"] is ClineAttachmentPhase.BOUND
    assert attach_call["external_session_id"] == _EXTERNAL_SESSION_ID
    assert attach_call["working_directory"] == _ORIGINAL_BASE
    assert adapter.reconstructions == [(_EXTERNAL_SESSION_ID, ClineConstructionMode.ACT)]
    assert adapter.admissions == [(_EXTERNAL_SESSION_ID, interaction.render())]


def test_the_cleanup_admission_propagates_the_adapter_classification_untouched(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The composition never catches the normalized adapter taxonomy: an
    uncertain admission stays the typed uncertainty the finalizer contains —
    never a resend, never a guessed outcome."""
    adapter = _RecordingAdapter(uncertain=True)
    attached = _RecordingAttachment(adapter)
    _wire_attachment(monkeypatch, attached)
    composition = ClineTerminalCleanupComposition()

    with pytest.raises(AgentRuntimeUncertainOutcomeError):
        composition.admit_cleanup_turn(
            None,  # type: ignore[arg-type]
            workspace_id=_WORKSPACE_ID,
            task_id=_TASK_ID,
            external_session_id=_EXTERNAL_SESSION_ID,
            working_directory=_ORIGINAL_BASE,
            interaction=_interaction(),
        )

    assert len(adapter.admissions) == 1
