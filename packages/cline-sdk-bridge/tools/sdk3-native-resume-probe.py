"""SDK3 native-reconstruction live qualification probe (core issue #408).

Bounded, sanitized, database-independent evidence for the native
``session.resume`` reconstruction through the PRODUCTION Python path —
``ClineBridgeProcessBackend`` over the built bridge and ``ClineAdapter`` with
explicit per-role construction contexts, observed through
``ClineHubTurnObservationSource`` — against the SDK2a/SDK2c-qualified 0.2.1
baseline (CLI 3.0.75 / core 0.2.1) on the isolated SDK1 reference ClineBox
through the operator SSH loopback forward. It is a one-off qualification
tool, not a test and not part of CI; ordinary suites stay offline.

Required verdicts (a wrong or missing verdict fails the probe with a
non-zero exit):

- production attach and promptless ACT allocation at a disposable
  repository base; a baseline free-pair turn (attributable completed fresh
  terminal over independently observed evidence);
- the ordinary native reconstruction ACT/base -> PLAN/checkout through
  ``reconstruct_session``: the exact session identity, a private verbatim
  saved-history comparison, and the honest usage observation;
- EFFECTIVE PLAN construction: a write request under PLAN must NOT produce
  the marker file on the Box (verified through the Owner-approved operator
  box-check hook — never inferred from a reply or the same ID);
- EFFECTIVE ACT construction: the same write request under ACT at the
  checkout MUST produce the marker file on the Box, then ACT back at the
  original base (verbatim comparison each transition);
- the native RESIDENT refusal, exercised through the native resume operation
  DIRECTLY (``backend.resume_session`` on the live incarnation — never via
  ``reconstruct_session``, which by design stops the incarnation first):
  the classified ``session_already_exists`` refusal with the live
  incarnation undisturbed;
- the authorized stopped recovery (``recover_session``): positive surviving
  history, positively absent incarnation, native restore of the exact
  identity, verbatim saved conversation, and a usable continuation turn;
- the Reviewer PLAN session in its separate neutral context where affected:
  same-ID native recovery in PLAN with a verbatim conversation;
- aggregate usage before/after reconstruction and recovery recorded honestly
  with the aggregate scope distinguished from a root-scope reading (a real
  zero stays zero, absence stays unknown, a reset is recorded as such);
- bounded cleanup: sessions registered at allocation, stopped before
  attachment disposal, quiescence verified through the status-only
  ``run.list`` surface, Box scratch directories removed.

A controlled Hub restart is deliberately NOT part of this probe: the single
final Owner-run ordered D7 campaign is the normal restart evidence source
(issue #408), and a separate targeted restart requires a documented distinct
gap plus explicit Owner authorization.

Inputs (environment, never logged):

- ``OPENORC_SDK3_TOKEN_FILE`` — path of a mode-600 file carrying the CURRENT
  Hub attach credential (resolved by the trusted operator supplier over SSH;
  the caller deletes the file afterwards).
- ``OPENORC_SDK3_ENDPOINT`` — the admitted Hub WebSocket endpoint (the
  loopback forward, e.g. ``ws://127.0.0.1:25463/hub``).
- ``OPENORC_SDK3_ENTRYPOINT`` / ``OPENORC_SDK3_NODE`` — optional overrides.
- ``OPENORC_SDK3_BOX_CHECK_COMMAND`` — the Owner-approved operator hook for
  Box-side verification; a command template whose ``{path}`` placeholder
  receives the addressed Box-side path and whose exit code answers
  existence (0 = exists).
- ``OPENORC_SDK3_SSH_COMMAND`` — the Owner-approved SSH command template
  (``{command}`` placeholder) used ONLY to provision and remove the probe's
  own disposable scratch directories on the Box.

Output: one JSON line per phase with sanitized fields only (counts, shapes,
verdicts, event-kind names, status tokens). Credentials, prompts,
transcripts, tool contents, reply text, and formal response bodies are never
printed; private transient comparisons stay in-process and emit only
verdicts/counts.

Inference: the explicitly configured SDK1/SDK2a/SDK2b-verified free pair
(provider ``cline``, model ``cline-free/nemotron-3.5-super-vl``) with
explicit per-role configuration; native account auth; no provider secret
injection and no paid fallback. The provider/model construction input is
therefore exercised as the same free pair carried verbatim through the
native resume — an actual override pair would require purchased inference
and is recorded as not-established rather than purchased.
"""

from __future__ import annotations

import json
import os
import queue
import subprocess
import sys
import threading
import time
import uuid
from decimal import Decimal
from pathlib import Path
from typing import Any

# The probe imports the production integration from the repository checkout
# it lives in (tools/ -> package root -> repo root).
_REPO_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(_REPO_ROOT / "src"))

from openorc.adapters.agent_runtime.errors import AgentSessionNotFoundError  # noqa: E402
from openorc.adapters.agent_runtime.results import AgentSessionCreationRequest  # noqa: E402
from openorc.adapters.cline import (  # noqa: E402
    ClineAdapter,
    ClineConstructionContext,
    ClineConstructionContextResolver,
    ClineConstructionMode,
    ClineRemoteConfig,
    ClineSdkOperationRejectedError,
    ClineSessionIdentity,
)
from openorc.adapters.cline.bridge_process import ClineBridgeProcessBackend  # noqa: E402
from openorc.adapters.cline.hub_observation import ClineHubTurnObservationSource  # noqa: E402
from openorc.adapters.cline.values import ClineResumeRequest  # noqa: E402
from openorc.domain.connections import WorkflowRole  # noqa: E402
from openorc.domain.detached_turns import (  # noqa: E402
    TurnAttribution,
    TurnEvidenceFamily,
    TurnFreshness,
)
from openorc.protocol.initialization_assets import compose_initialization  # noqa: E402

TOKEN_FILE = os.environ.get("OPENORC_SDK3_TOKEN_FILE", "")
ENDPOINT = os.environ.get("OPENORC_SDK3_ENDPOINT", "")
ENTRYPOINT = os.environ.get("OPENORC_SDK3_ENTRYPOINT", "")
NODE = os.environ.get("OPENORC_SDK3_NODE", "")
BOX_CHECK_COMMAND = os.environ.get("OPENORC_SDK3_BOX_CHECK_COMMAND", "")
SSH_COMMAND = os.environ.get("OPENORC_SDK3_SSH_COMMAND", "")

PROVIDER = "cline"
MODEL = "cline-free/nemotron-3.5-super-vl"
CLIENT_IDENTITY = "openorc-core/sdk3-live"
PROBE_DIR = "/home/openorc"
WINDOW_S = {"turn": 240.0}
_ACTIVE_RUN_STATES = frozenset({"running", "queued", "pending"})


def emit(phase: str, data: dict[str, Any]) -> None:
    print(json.dumps({"phase": phase, **data}, sort_keys=True), flush=True)


def class_name(error: BaseException) -> str:
    return type(error).__name__


def box_check(path: str) -> bool:
    """Owner-approved Box-side existence check (True exists / False absent).

    The hook is required exactly like the D7 module's operator hook; its
    exit code answers existence. Hook breakage surfaces through the
    surrounding verdicts (an ACT write whose check reports False fails the
    probe), never as silently swallowed evidence.
    """
    if not BOX_CHECK_COMMAND:
        raise RuntimeError("OPENORC_SDK3_BOX_CHECK_COMMAND is required")
    completed = subprocess.run(  # noqa: S603 - the Owner-approved hook template
        BOX_CHECK_COMMAND.format(path=path),
        shell=True,  # noqa: S602 - the Owner-approved hook is a shell template
        capture_output=True,
        timeout=60,
    )
    return completed.returncode == 0


def box_run(command: str) -> None:
    """Run one Owner-approved SSH command on the Box (scratch management)."""
    if not SSH_COMMAND:
        raise RuntimeError("OPENORC_SDK3_SSH_COMMAND is required for scratch management")
    completed = subprocess.run(  # noqa: S603 - the Owner-approved hook template
        SSH_COMMAND.format(command=command),
        shell=True,  # noqa: S602 - the Owner-approved hook is a shell template
        capture_output=True,
        timeout=60,
    )
    if completed.returncode != 0:
        raise RuntimeError("the Owner-approved SSH command failed")


class Cleanup:
    """Bounded best-effort release of everything the probe acquired.

    Phase 0 stops the exact disposable runtime sessions the probe allocated
    (registered at allocation), phase 1 releases observations and
    subscriptions, phase 2 disposes the observation source, phase 3 disposes
    the bridge backend, and phase 4 removes the probe's Box scratch
    directories. Session stops always run first so a disposed attachment can
    never strand a running probe session, and every stop outcome is emitted
    for the qualification record.
    """

    _OBSERVATIONS = 1
    _SOURCE = 2
    _BACKEND = 3
    _SCRATCH = 4

    def __init__(self, backend: Any) -> None:
        self._backend = backend
        self._sessions: list[str] = []
        self._stop_outcomes: dict[str, str] = {}
        self._jobs: list[tuple[int, str, Any]] = []
        self._scratch_dirs: list[str] = []

    def add_session(self, session_id: str) -> None:
        """Register one allocated disposable session for bounded stop."""
        self._sessions.append(session_id)

    def add_scratch_dir(self, path: str) -> None:
        self._scratch_dirs.append(path)

    def add(self, phase: int, label: str, job: Any) -> None:
        self._jobs.append((phase, label, job))

    def stop_sessions(self) -> bool:
        """Best-effort stop of every registered session; outcomes emitted."""
        all_stopped = True
        for index, session_id in enumerate(self._sessions):
            if session_id in self._stop_outcomes:
                continue
            try:
                self._backend.stop(session_id)
                outcome = "stopped"
            except AgentSessionNotFoundError:
                # Positively identified absence of a live incarnation: the
                # session was already stopped earlier in the probe.
                outcome = "already_stopped"
            except Exception as error:  # noqa: BLE001 - cleanup is best-effort
                outcome = class_name(error)
                all_stopped = False
            self._stop_outcomes[session_id] = outcome
            emit("cleanup_session", {"session_index": index, "outcome": outcome})
        return all_stopped

    def run(self) -> bool:
        stopped = self.stop_sessions()
        for _phase, label, job in sorted(self._jobs, key=lambda item: (item[0], item[1])):
            try:
                job()
            except Exception as error:  # noqa: BLE001 - cleanup is best-effort
                emit("cleanup_error", {"label": label, "error_class": class_name(error)})
        for scratch in self._scratch_dirs:
            try:
                box_run(f"rm -rf {scratch}")
            except Exception as error:  # noqa: BLE001 - cleanup is best-effort
                emit("cleanup_scratch", {"path_prefix": True, "error_class": class_name(error)})
        return stopped


class ProbeResolver(ClineConstructionContextResolver):
    """Explicit per-role construction contexts for the disposable probe sessions.

    The probe replaces the durable wiring resolver with explicit safe
    contexts: the qualified free pair for both roles, the packaged role
    Markdown (no override), and per-phase working contexts bound explicitly
    (the Producer's base/checkout phases; the Reviewer's neutral directory).
    The roles stay independently configured — separate identities, separate
    construction inputs, the Reviewer pinned to PLAN by the adapter's own
    enforcement.
    """

    def __init__(self, *, producer_base: str) -> None:
        self._workspace_id = uuid.uuid4()
        self._task_id = uuid.uuid4()
        self._connection_id = uuid.uuid4()
        self._bound: dict[str, ClineConstructionContext] = {}
        self._contexts: dict[WorkflowRole, ClineConstructionContext] = {
            role: ClineConstructionContext(
                identity=ClineSessionIdentity(
                    workspace_id=self._workspace_id,
                    task_id=self._task_id,
                    role=role,
                    connection_id=self._connection_id,
                ),
                configured_provider=PROVIDER,
                configured_model=MODEL,
                role_prompt_override=None,
                cwd=producer_base if role is WorkflowRole.PRODUCER else PROBE_DIR,
                workspace_root=(producer_base if role is WorkflowRole.PRODUCER else PROBE_DIR),
            )
            for role in (WorkflowRole.PRODUCER, WorkflowRole.REVIEWER)
        }

    @property
    def workspace_id(self) -> uuid.UUID:
        return self._workspace_id

    @property
    def task_id(self) -> uuid.UUID:
        return self._task_id

    @property
    def connection_id(self) -> uuid.UUID:
        return self._connection_id

    def bind(self, session_id: str, context: ClineConstructionContext) -> None:
        self._bound[session_id] = context

    def resolve_construction_context(
        self, workspace_id: uuid.UUID, task_id: uuid.UUID, role: WorkflowRole
    ) -> ClineConstructionContext:
        context = self._contexts[role]
        if context.identity.workspace_id != workspace_id or context.identity.task_id != task_id:
            raise ValueError("the probe resolver does not address that Task/role")
        return context

    def resolve_bound_session_context(self, session_id: str) -> ClineConstructionContext | None:
        # The probe binds by its own registry; any other identifier fails
        # closed (an establishment miss, never session-loss evidence).
        return self._bound.get(session_id)


class EvidenceCollector:
    """Queue-backed collector of one session's neutral evidence deliveries."""

    def __init__(self) -> None:
        self._queue: queue.Queue[Any] = queue.Queue()
        self.usage_pulses = 0
        self._lock = threading.Lock()

    def deliver(self, evidence: Any) -> None:
        self._queue.put(evidence)

    def wait_for(self, families: tuple[Any, ...], deadline_s: float) -> list[Any] | None:
        """Collect the requested families in order, or None on deadline."""
        wanted = list(families)
        found: list[Any] = []
        deadline = time.monotonic() + deadline_s
        while wanted:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return None
            try:
                evidence = self._queue.get(timeout=min(remaining, 1.0))
            except queue.Empty:
                continue
            if evidence.family in wanted:
                found.append(evidence)
                wanted.remove(evidence.family)
            elif evidence.family is TurnEvidenceFamily.TURN_USAGE:
                with self._lock:
                    self.usage_pulses += 1
        return found


class _Inputs:
    """Minimal neutral attachment-inputs value for the observation source."""

    def __init__(self, *, endpoint: str, auth_token: str) -> None:
        self._endpoint = endpoint
        self._auth_token = auth_token

    @property
    def endpoint(self) -> str:
        return self._endpoint

    @property
    def auth_token(self) -> str:
        return self._auth_token


def classify_terminal(source: Any, evidence: Any, consumed: list[int]) -> dict[str, Any]:
    classification = source.classify_turn_terminal(
        evidence.native_evidence, consumed_ordering_markers=consumed
    )
    outcome = classification.outcome
    marker = classification.ordering_marker
    fresh_consumed = list(consumed)
    if marker is not None and classification.freshness is TurnFreshness.FRESH:
        fresh_consumed.append(marker)
    return {
        "outcome_kind": outcome.kind.value,
        "freshness": classification.freshness.value,
        "interrupted": outcome.interrupted,
        "result_candidate_present": outcome.result_candidate is not None,
        "result_candidate_chars": (
            len(outcome.result_candidate) if outcome.result_candidate is not None else 0
        ),
        "_consumed": fresh_consumed,
    }


def _is_zero_cost(cost: Any) -> bool:
    """A genuinely observed zero (Decimal or numeric) — real evidence."""
    if isinstance(cost, Decimal):
        return cost == 0
    return isinstance(cost, (int, float)) and not isinstance(cost, bool) and cost == 0


def usage_scopes(snapshot: Any) -> dict[str, str]:
    """Honest per-scope rendering: aggregate absence is distinct from a root
    reading; a genuinely observed zero stays zero; unknown stays unknown."""
    states: dict[str, str] = {}
    for name in ("usage", "aggregate_usage"):
        scope = getattr(snapshot, name, None)
        if scope is None:
            states[name] = "absent"
            continue
        cost = getattr(scope, "total_cost", None)
        if cost is None:
            states[name] = "cost_unknown"
        elif _is_zero_cost(cost):
            states[name] = "cost_zero"
        else:
            states[name] = "cost_nonzero"
    return states


def fail_and_cleanup(phase: str, error: BaseException, cleanup: Cleanup) -> None:
    emit(phase, {"ok": False, "error_class": class_name(error)})
    cleanup.run()


def fail(phase: str, reason: str, cleanup: Cleanup) -> int:
    """Fail the probe on a wrong or missing required verdict."""
    emit(phase, {"ok": False, "reason": reason})
    cleanup.run()
    return 3


def verify_quiescence(backend: Any, session_ids: list[str]) -> dict[str, Any]:
    """Verify the probe's own sessions are quiescent via status-only records."""
    sessions: dict[str, Any] = {}
    all_quiet = True
    for index, session_id in enumerate(session_ids):
        states = [
            str(record.get("state"))
            for record in backend.list_runs(session_id)
            if isinstance(record, dict) and isinstance(record.get("state"), str)
        ]
        active = [state for state in states if state in _ACTIVE_RUN_STATES]
        all_quiet = all_quiet and not active
        sessions[f"session_{index}"] = {
            "active_runs": len(active),
            "states": sorted(set(states)),
        }
    return {"ok": all_quiet, "sessions": sessions}


def _run_turn(
    *,
    phase: str,
    adapter: Any,
    source: Any,
    collector: EvidenceCollector,
    cleanup: Cleanup,
    session_id: str,
    prompt: str,
    consumed: list[int],
) -> tuple[dict[str, Any], list[int]] | None:
    """Admit one detached turn and classify its start and terminal evidence.

    Returns (terminal, chained consumed markers) on an attributable, fresh,
    completed, non-interrupted terminal; None (after emitting the failure)
    otherwise.
    """
    try:
        admission = adapter.admit_detached_turn(session_id, prompt)
    except Exception as error:  # noqa: BLE001
        fail_and_cleanup(phase, error, cleanup)
        return None
    reference = admission.turn_reference
    found = collector.wait_for(
        (TurnEvidenceFamily.TURN_START, TurnEvidenceFamily.TURN_TERMINAL),
        WINDOW_S["turn"],
    )
    if found is None:
        fail(phase, "evidence window timed out", cleanup)
        return None
    start_evidence, terminal_evidence = found
    attribution = source.classify_turn_start(
        start_evidence.native_evidence, outstanding_turn_reference=reference
    )
    terminal = classify_terminal(source, terminal_evidence, consumed=consumed)
    emit(
        phase,
        {
            "ok": True,
            "start_attribution": attribution.value,
            "outcome_kind": terminal["outcome_kind"],
            "freshness": terminal["freshness"],
            "interrupted": terminal["interrupted"],
            "result_candidate_chars": terminal["result_candidate_chars"],
        },
    )
    if attribution is not TurnAttribution.ATTRIBUTED:
        fail(phase, f"start attribution was {attribution.value}", cleanup)
        return None
    if terminal["outcome_kind"] != "completed" or terminal["interrupted"]:
        fail(
            phase,
            f"terminal was {terminal['outcome_kind']} interrupted={terminal['interrupted']}",
            cleanup,
        )
        return None
    if terminal["freshness"] != TurnFreshness.FRESH.value:
        fail(phase, f"terminal freshness was {terminal['freshness']}", cleanup)
        return None
    return terminal, terminal["_consumed"]


def _reconstruct(
    *,
    phase: str,
    adapter: Any,
    backend: Any,
    cleanup: Cleanup,
    session_id: str,
    target_mode: ClineConstructionMode,
) -> list[Any] | None:
    """Run the native same-ID reconstruction and privately compare histories.

    The pre-reconstruction read happens immediately before the reconstruction
    inside this phase, so the comparison isolates the reconstruction's own
    effect: any conversation growth from an intervening turn is captured in
    the baseline rather than counted against the reconstruction.
    """
    try:
        before = backend.read_messages(session_id)
        adapter.reconstruct_session(session_id, target_mode=target_mode)
        after = backend.read_messages(session_id)
    except Exception as error:  # noqa: BLE001
        fail_and_cleanup(phase, error, cleanup)
        return None
    verbatim = bool(after and after == before)
    emit(
        phase,
        {
            "ok": True,
            "target_mode": target_mode.value,
            "history_entries_before": len(before),
            "history_entries_after": len(after),
            "verbatim": verbatim,
        },
    )
    if not verbatim:
        fail(phase, "the saved conversation was not preserved verbatim", cleanup)
        return None
    return after


def main() -> int:
    for name, value in (
        ("OPENORC_SDK3_TOKEN_FILE", TOKEN_FILE),
        ("OPENORC_SDK3_ENDPOINT", ENDPOINT),
        ("OPENORC_SDK3_BOX_CHECK_COMMAND", BOX_CHECK_COMMAND),
        ("OPENORC_SDK3_SSH_COMMAND", SSH_COMMAND),
    ):
        if not value:
            emit("preflight", {"ok": False, "missing": name})
            return 2
    entrypoint = ENTRYPOINT or str(_REPO_ROOT / "packages" / "cline-sdk-bridge" / "dist" / "cli.js")
    if not Path(entrypoint).is_file():
        emit("preflight", {"ok": False, "missing": "bridge dist entrypoint"})
        return 2
    try:
        token = Path(TOKEN_FILE).read_text(encoding="utf-8").strip()
    except OSError as error:
        emit("preflight", {"ok": False, "error_class": class_name(error)})
        return 2
    if not token:
        emit("preflight", {"ok": False, "missing": "credential file content"})
        return 2
    emit("preflight", {"ok": True})

    backend = (
        ClineBridgeProcessBackend(command=[NODE, entrypoint])
        if NODE
        else ClineBridgeProcessBackend()
    )
    cleanup = Cleanup(backend)
    cleanup.add(Cleanup._BACKEND, "backend.dispose", backend.dispose)
    remote = ClineRemoteConfig(endpoint=ENDPOINT, client_identity=CLIENT_IDENTITY, auth_token=token)
    source = ClineHubTurnObservationSource(backend_factory=lambda: backend)
    cleanup.add(Cleanup._SOURCE, "source.dispose", source.dispose)

    # Disposable Box scratch directories: the Producer's original repository
    # base and its deterministic Task checkout, provisioned through the
    # Owner-approved SSH hook and removed at cleanup.
    base = f"{PROBE_DIR}/sdk3-probe-base-{uuid.uuid4().hex[:8]}"
    checkout = f"{base}/checkout"
    try:
        box_run(f"mkdir -p {checkout}")
    except Exception as error:  # noqa: BLE001
        fail_and_cleanup("scratch_provision", error, cleanup)
        return 3
    cleanup.add_scratch_dir(base)
    emit("scratch_provision", {"ok": True})

    resolver = ProbeResolver(producer_base=base)
    adapter = ClineAdapter(backend, resolver)

    try:
        backend.connect(remote)
        history = backend.list_history()
        runs = backend.list_runs(None)
        emit(
            "attach",
            {
                "ok": True,
                "history_entries": len(history) if isinstance(history, list) else None,
                "existing_runs": len(runs) if isinstance(runs, list) else None,
            },
        )
    except Exception as error:  # noqa: BLE001
        fail_and_cleanup("attach", error, cleanup)
        return 3

    return _qualification(
        adapter=adapter,
        backend=backend,
        source=source,
        resolver=resolver,
        cleanup=cleanup,
        token=token,
        base=base,
        checkout=checkout,
    )


def _qualification(
    *,
    adapter: Any,
    backend: Any,
    source: Any,
    resolver: ProbeResolver,
    cleanup: Cleanup,
    token: str,
    base: str,
    checkout: str,
) -> int:
    """The required verdict sequence, ending in bounded cleanup."""
    marker_base = f"{checkout}/sdk3-marker.txt"
    producer_context = ClineConstructionContext(
        identity=ClineSessionIdentity(
            workspace_id=resolver.workspace_id,
            task_id=resolver.task_id,
            role=WorkflowRole.PRODUCER,
            connection_id=resolver.connection_id,
        ),
        configured_provider=PROVIDER,
        configured_model=MODEL,
        role_prompt_override=None,
        cwd=base,
        workspace_root=base,
    )
    try:
        allocation = adapter.allocate_detached_session(
            AgentSessionCreationRequest(
                workspace_id=resolver.workspace_id,
                task_id=resolver.task_id,
                role=WorkflowRole.PRODUCER,
                initialization=compose_initialization("producer", None),
            )
        )
    except Exception as error:  # noqa: BLE001
        fail_and_cleanup("allocate_producer", error, cleanup)
        return 3
    producer_id = allocation.external_session_id
    resolver.bind(producer_id, producer_context)
    cleanup.add_session(producer_id)
    emit("allocate_producer", {"ok": True, "session_allocated": True})

    collector = EvidenceCollector()
    try:
        source.attach_connection(
            connection_id=resolver.connection_id,
            inputs=_Inputs(endpoint=ENDPOINT, auth_token=token),
        )
        lease = source.observe_session(
            connection_id=resolver.connection_id,
            external_session_id=producer_id,
            deliver=collector.deliver,
        )
    except Exception as error:  # noqa: BLE001
        fail_and_cleanup("observe_producer", error, cleanup)
        return 3
    cleanup.add(Cleanup._OBSERVATIONS, "lease.release", lambda: source.release_observation(lease))
    emit("observe_producer", {"ok": True, "subscribed_before_enqueue": True})

    # Baseline ACT turn at the original base.
    outcome = _run_turn(
        phase="baseline_turn",
        adapter=adapter,
        source=source,
        collector=collector,
        cleanup=cleanup,
        session_id=producer_id,
        prompt="SDK3 probe: reply with the single word ok.",
        consumed=[],
    )
    if outcome is None:
        return 3
    _terminal, consumed = outcome
    usage_before = adapter.read_activity_snapshot(producer_id)
    history_base = backend.read_messages(producer_id)
    if not history_base:
        return fail("baseline_turn", "no saved history after the baseline turn", cleanup)
    emit(
        "usage_before_reconstruction",
        {"ok": True, **usage_scopes(usage_before), "history_entries": len(history_base)},
    )

    # Ordinary native reconstruction: ACT/base -> PLAN/checkout (issue #408).
    resolver.bind(
        producer_id,
        ClineConstructionContext(
            identity=producer_context.identity,
            configured_provider=PROVIDER,
            configured_model=MODEL,
            role_prompt_override=None,
            cwd=checkout,
            workspace_root=checkout,
        ),
    )
    history_plan = _reconstruct(
        phase="reconstruct_plan",
        adapter=adapter,
        backend=backend,
        cleanup=cleanup,
        session_id=producer_id,
        target_mode=ClineConstructionMode.PLAN,
    )
    if history_plan is None:
        return 3
    emit(
        "usage_after_reconstruction",
        {"ok": True, **usage_scopes(adapter.read_activity_snapshot(producer_id))},
    )

    # EFFECTIVE PLAN construction: the write must NOT land on the Box.
    outcome = _run_turn(
        phase="effective_plan_turn",
        adapter=adapter,
        source=source,
        collector=collector,
        cleanup=cleanup,
        session_id=producer_id,
        prompt=(
            f"SDK3 probe: create the file {marker_base} containing the single"
            " word written. Use the write tool now."
        ),
        consumed=consumed,
    )
    if outcome is None:
        return 3
    _terminal, consumed = outcome
    marker_written = box_check(marker_base)
    emit(
        "effective_plan_construction",
        {"ok": not marker_written, "marker_written": marker_written},
    )
    if marker_written:
        return fail(
            "effective_plan_construction",
            "the restored PLAN construction allowed a mutating write",
            cleanup,
        )

    # EFFECTIVE ACT construction: the write MUST land on the Box.
    history_act = _reconstruct(
        phase="reconstruct_act",
        adapter=adapter,
        backend=backend,
        cleanup=cleanup,
        session_id=producer_id,
        target_mode=ClineConstructionMode.ACT,
    )
    if history_act is None:
        return 3
    outcome = _run_turn(
        phase="effective_act_turn",
        adapter=adapter,
        source=source,
        collector=collector,
        cleanup=cleanup,
        session_id=producer_id,
        prompt=(
            f"SDK3 probe: create the file {marker_base} containing the single"
            " word written. Use the write tool now."
        ),
        consumed=consumed,
    )
    if outcome is None:
        return 3
    _terminal, consumed = outcome
    marker_written = box_check(marker_base)
    emit("effective_act_construction", {"ok": marker_written, "marker_written": marker_written})
    if not marker_written:
        return fail(
            "effective_act_construction",
            "the restored ACT construction did not perform the authorized write",
            cleanup,
        )

    # ACT/checkout -> ACT/original base (the authorized cleanup transition).
    resolver.bind(producer_id, producer_context)
    history_back = _reconstruct(
        phase="reconstruct_act_base",
        adapter=adapter,
        backend=backend,
        cleanup=cleanup,
        session_id=producer_id,
        target_mode=ClineConstructionMode.ACT,
    )
    if history_back is None:
        return 3

    # The RESIDENT native resume, exercised through the native resume
    # operation DIRECTLY on the live idle incarnation (never via
    # reconstruct_session, which by design stops the incarnation first).
    # The QUALIFIED baseline's observed behavior is recorded as the contract:
    # whether the Hub refuses or accepts idempotently, the exact identity,
    # the conversation, and the incarnation's service must survive, and no
    # stop is ever issued here.
    resume_request = ClineResumeRequest(
        session_id=producer_id,
        provider_id=PROVIDER,
        model_id=MODEL,
        mode=ClineConstructionMode.ACT,
        rules="",
        system_prompt="",
        cwd=base,
        workspace_root=base,
        enable_tools=True,
        interactive=True,
        tool_policies={"*": {"autoApprove": True}, "ask_question": {"enabled": False}},
    )
    history_pre_resume = backend.read_messages(producer_id)
    refused_code: str | None
    try:
        backend.resume_session(resume_request)
    except ClineSdkOperationRejectedError as refusal:
        refused_code = refusal.code
    except Exception as error:  # noqa: BLE001
        fail_and_cleanup("resident_resume", error, cleanup)
        return 3
    else:
        refused_code = None
    history_post_resume = backend.read_messages(producer_id)
    resident_verbatim = bool(history_post_resume and history_post_resume == history_pre_resume)
    observed = "refused" if refused_code is not None else "accepted_idempotent"
    emit(
        "resident_resume",
        {
            "ok": resident_verbatim,
            "observed": observed,
            "refusal_code": refused_code,
            "history_entries_before": len(history_pre_resume),
            "history_entries_after": len(history_post_resume),
            "verbatim": resident_verbatim,
        },
    )
    if not resident_verbatim:
        return fail(
            "resident_resume",
            "the resident native resume did not preserve the conversation",
            cleanup,
        )
    # The live incarnation must still serve: the next admitted turn works.
    still_live = _run_turn(
        phase="resident_still_live",
        adapter=adapter,
        source=source,
        collector=collector,
        cleanup=cleanup,
        session_id=producer_id,
        prompt="SDK3 probe: reply with the single word alive.",
        consumed=consumed,
    )
    if still_live is None:
        return 3
    _terminal, consumed = still_live

    # Authorized stopped recovery: positive surviving history, positively
    # absent incarnation, native restore of the exact identity.
    pre_stop_history = backend.read_messages(producer_id)
    try:
        backend.stop(producer_id)
        record_present = backend.get(producer_id) is not None
    except Exception as error:  # noqa: BLE001
        fail_and_cleanup("stop_for_recovery", error, cleanup)
        return 3
    try:
        adapter.recover_session(producer_id, target_mode=ClineConstructionMode.ACT)
        history_recovered = backend.read_messages(producer_id)
    except Exception as error:  # noqa: BLE001
        fail_and_cleanup("stopped_recovery", error, cleanup)
        return 3
    recovered_verbatim = bool(history_recovered and history_recovered == pre_stop_history)
    emit(
        "stopped_recovery",
        {
            "ok": True,
            "record_present_while_stopped": record_present,
            "history_entries": len(history_recovered),
            "verbatim": recovered_verbatim,
        },
    )
    if not recovered_verbatim or not record_present:
        return fail(
            "stopped_recovery",
            "the native recovery did not restore the exact saved conversation",
            cleanup,
        )
    emit(
        "usage_after_recovery",
        {"ok": True, **usage_scopes(adapter.read_activity_snapshot(producer_id))},
    )

    # Usable continuation on the restored incarnation.
    outcome = _run_turn(
        phase="recovery_continuation",
        adapter=adapter,
        source=source,
        collector=collector,
        cleanup=cleanup,
        session_id=producer_id,
        prompt="SDK3 probe: reply with the single word recovered.",
        consumed=consumed,
    )
    if outcome is None:
        return 3
    _terminal, consumed = outcome

    # --- the Reviewer PLAN session in its separate neutral context ---------
    reviewer_context = ClineConstructionContext(
        identity=ClineSessionIdentity(
            workspace_id=resolver.workspace_id,
            task_id=resolver.task_id,
            role=WorkflowRole.REVIEWER,
            connection_id=resolver.connection_id,
        ),
        configured_provider=PROVIDER,
        configured_model=MODEL,
        role_prompt_override=None,
        cwd=PROBE_DIR,
        workspace_root=PROBE_DIR,
    )
    try:
        reviewer_allocation = adapter.allocate_detached_session(
            AgentSessionCreationRequest(
                workspace_id=resolver.workspace_id,
                task_id=resolver.task_id,
                role=WorkflowRole.REVIEWER,
                initialization=compose_initialization("reviewer", None),
            )
        )
    except Exception as error:  # noqa: BLE001
        fail_and_cleanup("allocate_reviewer", error, cleanup)
        return 3
    reviewer_id = reviewer_allocation.external_session_id
    resolver.bind(reviewer_id, reviewer_context)
    cleanup.add_session(reviewer_id)
    reviewer_collector = EvidenceCollector()
    try:
        reviewer_lease = source.observe_session(
            connection_id=resolver.connection_id,
            external_session_id=reviewer_id,
            deliver=reviewer_collector.deliver,
        )
    except Exception as error:  # noqa: BLE001
        fail_and_cleanup("observe_reviewer", error, cleanup)
        return 3
    cleanup.add(
        Cleanup._OBSERVATIONS,
        "reviewer_lease.release",
        lambda: source.release_observation(reviewer_lease),
    )
    emit("allocate_reviewer", {"ok": True, "session_allocated": True})

    outcome = _run_turn(
        phase="reviewer_baseline_turn",
        adapter=adapter,
        source=source,
        collector=reviewer_collector,
        cleanup=cleanup,
        session_id=reviewer_id,
        prompt="SDK3 probe: reply with the single word ok.",
        consumed=[],
    )
    if outcome is None:
        return 3
    _terminal, reviewer_consumed = outcome
    reviewer_history = backend.read_messages(reviewer_id)
    if not reviewer_history:
        return fail("reviewer_baseline_turn", "no saved history", cleanup)
    try:
        backend.stop(reviewer_id)
        reviewer_record_present = backend.get(reviewer_id) is not None
        adapter.recover_session(reviewer_id, target_mode=ClineConstructionMode.PLAN)
        reviewer_recovered = backend.read_messages(reviewer_id)
    except Exception as error:  # noqa: BLE001
        fail_and_cleanup("reviewer_plan_recovery", error, cleanup)
        return 3
    reviewer_verbatim = bool(reviewer_recovered and reviewer_recovered == reviewer_history)
    emit(
        "reviewer_plan_recovery",
        {
            "ok": True,
            "record_present_while_stopped": reviewer_record_present,
            "history_entries": len(reviewer_recovered),
            "verbatim": reviewer_verbatim,
        },
    )
    if not reviewer_verbatim or not reviewer_record_present:
        return fail(
            "reviewer_plan_recovery",
            "the Reviewer PLAN recovery did not restore the exact conversation",
            cleanup,
        )

    # --- bounded cleanup and quiescence verification -----------------------
    if not cleanup.stop_sessions():
        return fail("cleanup", "one or more probe sessions could not be stopped", cleanup)
    try:
        quiescence = verify_quiescence(backend, [producer_id, reviewer_id])
    except Exception as error:  # noqa: BLE001
        fail_and_cleanup("cleanup_quiescence", error, cleanup)
        return 3
    emit("cleanup_quiescence", quiescence)
    if not quiescence["ok"]:
        return fail("cleanup_quiescence", "a probe session still has an active run", cleanup)
    cleanup.run()
    emit("probe_complete", {"ok": True})
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
