"""SDK2c adapter-level live qualification probe (core issue #413).

Bounded, sanitized, database-independent evidence for the existing
``ClineAdapter`` / detached-turn observation integration against the
SDK2a-qualified 0.2.1 bridge and the SDK1 reference ClineBox (CLI 3.0.75 /
core 0.2.1) through the operator SSH loopback forward. The probe drives the
PRODUCTION Python path — ``ClineBridgeProcessBackend`` over the built bridge,
``ClineAdapter`` with explicit per-role construction contexts, and
``ClineHubTurnObservationSource`` — never a fake backend and never a
database. It is a one-off qualification tool, not a test and not part of CI;
ordinary suites stay offline.

Discipline:

- Every REQUIRED verdict (attribution, ordering freshness, terminal outcome,
  formal validation, settlement, transcript preservation, cleanup
  quiescence) determines qualification success: a wrong or missing verdict
  fails the probe with a non-zero exit instead of merely being printed.
- The interruption/resume cycle exercises the production detached-turn
  mechanics only: both the interrupted turn and the authorized continuation
  are enqueued through ``admit_detached_turn`` (the continuation content is
  the shared #343 fixed ``RESUMPTION_INSTRUCTION``), and every admitted
  reference is classified against independently observed start and terminal
  evidence with the session's consumed markers chained across turns.
- Every allocated disposable session is registered for bounded best-effort
  cleanup at allocation; cleanup stops the sessions before releasing
  observations and disposing attachments, reports each stop outcome, and the
  success path additionally verifies quiescence through the status-only
  ``run.list`` surface before attachment disposal.

Inputs (environment, never logged):

- ``OPENORC_SDK2C_TOKEN_FILE`` — path of a mode-600 file carrying the
  CURRENT Hub attach credential (resolved by the trusted operator supplier
  over SSH; the caller deletes the file afterwards).
- ``OPENORC_SDK2C_ENDPOINT`` — the admitted Hub WebSocket endpoint (the
  loopback forward, e.g. ``ws://127.0.0.1:25463/hub``).
- ``OPENORC_SDK2C_ENTRYPOINT`` / ``OPENORC_SDK2C_NODE`` — optional overrides.

Output: one JSON line per phase with sanitized fields only (counts, shapes,
verdicts, event-kind names, status tokens). Prompts are the fixed harmless
bounded strings below. Credentials, prompts, transcripts, payload contents,
and formal response bodies are never printed; the observed formal candidate
is validated through the shared protocol funnel and only the verdict is
emitted.

Inference: the explicitly configured SDK1/SDK2a-verified free pair
(provider ``cline``, model ``cline-free/nemotron-3.5-super-vl``) with
explicit per-role configuration; native account auth; no provider secret
injection and no paid fallback.
"""

from __future__ import annotations

import json
import os
import queue
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

from openorc.adapters.agent_runtime.errors import (  # noqa: E402
    AgentRuntimeError,
    AgentSessionNotFoundError,
)
from openorc.adapters.agent_runtime.instructions import RESUMPTION_INSTRUCTION  # noqa: E402
from openorc.adapters.agent_runtime.results import AgentSessionCreationRequest  # noqa: E402
from openorc.adapters.cline import (  # noqa: E402
    ClineAdapter,
    ClineConstructionContext,
    ClineConstructionContextResolver,
    ClineConstructionMode,
    ClineSessionIdentity,
)
from openorc.adapters.cline.bridge_process import ClineBridgeProcessBackend  # noqa: E402
from openorc.adapters.cline.hub_observation import ClineHubTurnObservationSource  # noqa: E402
from openorc.adapters.cline.values import ClineRemoteConfig  # noqa: E402
from openorc.domain.connections import WorkflowRole  # noqa: E402
from openorc.domain.detached_turns import (  # noqa: E402
    TurnAttribution,
    TurnEvidenceFamily,
    TurnFreshness,
    TurnStatusEvidenceVerdict,
)
from openorc.protocol.initialization_assets import compose_initialization  # noqa: E402
from openorc.protocol.models import SESSION_READY_FAMILY  # noqa: E402
from openorc.protocol.parsing import parse_formal_response  # noqa: E402

TOKEN_FILE = os.environ.get("OPENORC_SDK2C_TOKEN_FILE", "")
ENDPOINT = os.environ.get("OPENORC_SDK2C_ENDPOINT", "")
ENTRYPOINT = os.environ.get("OPENORC_SDK2C_ENTRYPOINT", "")
NODE = os.environ.get("OPENORC_SDK2C_NODE", "")

PROVIDER = "cline"
MODEL = "cline-free/nemotron-3.5-super-vl"
CLIENT_IDENTITY = "openorc-core/sdk2c-live"
PROBE_DIR = "/home/openorc"
REVIEWER_OK_PROMPT = "SDK2c probe: reply with the single word ok."
PRODUCER_COUNT_PROMPT = "SDK2c probe: count from 1 to 60, one number per line, and nothing else."
WINDOW_S = {
    "init_turn": 240.0,
    "second_turn": 180.0,
    "interrupt_turn": 240.0,
    "resume_turn": 240.0,
}
INTERRUPT_ATTEMPTS = 2
_ACTIVE_RUN_STATES = frozenset({"running", "queued", "pending"})


def emit(phase: str, data: dict[str, Any]) -> None:
    print(json.dumps({"phase": phase, **data}, sort_keys=True), flush=True)


def class_name(error: BaseException) -> str:
    return type(error).__name__


class Cleanup:
    """Bounded best-effort release of everything the probe acquired.

    Phase 0 stops the exact disposable runtime sessions the probe allocated
    (registered at allocation), phase 1 releases observations and
    subscriptions, phase 2 disposes the observation source, and phase 3
    disposes the bridge backend. Session stops always run first so a disposed
    attachment can never strand a running probe session, and every stop
    outcome is emitted for the qualification record.
    """

    _OBSERVATIONS = 1
    _SOURCE = 2
    _BACKEND = 3

    def __init__(self, backend: Any) -> None:
        self._backend = backend
        self._sessions: list[str] = []
        self._stop_outcomes: dict[str, str] = {}
        self._jobs: list[tuple[int, str, Any]] = []

    def add_session(self, session_id: str) -> None:
        """Register one allocated disposable session for bounded stop."""
        self._sessions.append(session_id)

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
        return stopped


class ProbeResolver(ClineConstructionContextResolver):
    """Explicit per-role construction contexts for the disposable probe sessions.

    The probe replaces the durable wiring resolver with explicit safe
    contexts: the qualified free pair for both roles, the packaged role
    Markdown (no override), and the same neutral non-repository working
    directory on the Box. The roles stay independently configured — separate
    identities, separate construction inputs, Reviewer pinned to PLAN.
    """

    def __init__(self) -> None:
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
                cwd=PROBE_DIR,
                workspace_root=PROBE_DIR,
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

    def bind(self, session_id: str, role: WorkflowRole) -> None:
        self._bound[session_id] = self._contexts[role]

    def resolve_construction_context(
        self, workspace_id: uuid.UUID, task_id: uuid.UUID, role: WorkflowRole
    ) -> ClineConstructionContext:
        context = self._contexts[role]
        if context.identity.workspace_id != workspace_id or context.identity.task_id != task_id:
            raise ValueError("the probe resolver does not address that Task/role")
        return context

    def resolve_bound_session_context(self, session_id: str) -> ClineConstructionContext | None:
        # The probe binds by its own allocation registry; any other identifier
        # fails closed (an establishment miss, never session-loss evidence).
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
        "ordering_marker_present": marker is not None,
        "_consumed": fresh_consumed,
    }


def usage_state(value: Any) -> str:
    if value is None:
        return "unknown"
    if isinstance(value, Decimal) and value == 0:
        return "zero"
    if isinstance(value, (int, float)) and not isinstance(value, bool) and value == 0:
        return "zero"
    return "nonzero"


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


def main() -> int:
    for name, value in (
        ("OPENORC_SDK2C_TOKEN_FILE", TOKEN_FILE),
        ("OPENORC_SDK2C_ENDPOINT", ENDPOINT),
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

    resolver = ProbeResolver()
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
    adapter = ClineAdapter(backend, resolver)

    try:
        try:
            backend.connect(remote)
        except Exception as error:  # noqa: BLE001
            fail_and_cleanup("attach", error, cleanup)
            return 3
        try:
            history = backend.list_history()
            runs = backend.list_runs(None)
            run_count = len(runs) if isinstance(runs, list) else None
            emit(
                "attach",
                {
                    "ok": True,
                    "history_entries": len(history) if isinstance(history, list) else None,
                    "existing_runs": run_count,
                },
            )
        except Exception as error:  # noqa: BLE001
            fail_and_cleanup("attach", error, cleanup)
            return 3

        # --- Reviewer: promptless allocation, observation before enqueue ----
        reviewer_request = AgentSessionCreationRequest(
            workspace_id=resolver.workspace_id,
            task_id=resolver.task_id,
            role=WorkflowRole.REVIEWER,
            initialization=compose_initialization("reviewer", None),
        )
        try:
            allocation = adapter.allocate_detached_session(reviewer_request)
        except Exception as error:  # noqa: BLE001
            fail_and_cleanup("allocate_reviewer", error, cleanup)
            return 3
        reviewer_id = allocation.external_session_id
        resolver.bind(reviewer_id, WorkflowRole.REVIEWER)
        cleanup.add_session(reviewer_id)
        emit("allocate_reviewer", {"ok": True, "session_allocated": True})

        reviewer_collector = EvidenceCollector()
        try:
            source.attach_connection(
                connection_id=resolver.connection_id,
                inputs=_Inputs(endpoint=ENDPOINT, auth_token=token),
            )
            lease = source.observe_session(
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
            lambda: source.release_observation(lease),
        )
        emit("observe_reviewer", {"ok": True, "subscribed_before_enqueue": True})

        try:
            admission = adapter.admit_detached_turn(
                reviewer_id, compose_initialization("reviewer", None)
            )
        except Exception as error:  # noqa: BLE001
            fail_and_cleanup("enqueue_initialization", error, cleanup)
            return 3
        init_ref = admission.turn_reference
        emit("enqueue_initialization", {"ok": True, "has_turn_reference": bool(init_ref)})
        return _continue_after_enqueue(
            adapter=adapter,
            backend=backend,
            source=source,
            resolver=resolver,
            cleanup=cleanup,
            reviewer_id=reviewer_id,
            reviewer_collector=reviewer_collector,
            lease=lease,
            init_ref=init_ref,
            token=token,
        )
    finally:
        cleanup.run()


def _continue_after_enqueue(
    *,
    adapter: Any,
    backend: Any,
    source: Any,
    resolver: ProbeResolver,
    cleanup: Cleanup,
    reviewer_id: str,
    reviewer_collector: EvidenceCollector,
    lease: Any,
    init_ref: str,
    token: str,
) -> int:
    """The initialization turn and everything after it, through cleanup."""
    found = reviewer_collector.wait_for(
        (TurnEvidenceFamily.TURN_START, TurnEvidenceFamily.TURN_TERMINAL),
        WINDOW_S["init_turn"],
    )
    if found is None:
        return fail("initialization_turn", "evidence window timed out", cleanup)
    start_evidence, terminal_evidence = found
    attribution = source.classify_turn_start(
        start_evidence.native_evidence, outstanding_turn_reference=init_ref
    )
    terminal = classify_terminal(source, terminal_evidence, consumed=[])
    session_ready_validated = False
    if terminal["result_candidate_present"]:
        candidate = source.classify_turn_terminal(
            terminal_evidence.native_evidence, consumed_ordering_markers=[]
        ).outcome.result_candidate
        try:
            parsed = parse_formal_response(candidate, expected_family=SESSION_READY_FAMILY)
            session_ready_validated = getattr(parsed, "status", None) == "READY"
        except Exception:  # noqa: BLE001 - a failed validation is recorded, not raised
            session_ready_validated = False
    emit(
        "initialization_turn",
        {
            "ok": True,
            "start_attribution": attribution.value,
            "outcome_kind": terminal["outcome_kind"],
            "freshness": terminal["freshness"],
            "interrupted": terminal["interrupted"],
            "session_ready_validated": session_ready_validated,
        },
    )
    if attribution is not TurnAttribution.ATTRIBUTED:
        return fail("initialization_turn", f"start attribution was {attribution.value}", cleanup)
    if terminal["outcome_kind"] != "completed" or terminal["interrupted"]:
        return fail(
            "initialization_turn",
            (
                f"terminal outcome was {terminal['outcome_kind']} "
                f"interrupted={terminal['interrupted']}"
            ),
            cleanup,
        )
    if terminal["freshness"] != TurnFreshness.FRESH.value:
        return fail("initialization_turn", f"freshness was {terminal['freshness']}", cleanup)
    if not session_ready_validated:
        return fail("initialization_turn", "session_ready was not validated", cleanup)
    consumed = terminal["_consumed"]

    # --- status-only consultation after the validated terminal --------------
    try:
        status_evidence = source.consult_status_evidence(
            connection_id=resolver.connection_id,
            external_session_id=reviewer_id,
        )
        verdict = source.classify_turn_statuses(
            status_evidence, outstanding_turn_reference=init_ref
        )
        emit("status_consultation", {"verdict": verdict.value})
    except Exception as error:  # noqa: BLE001
        fail_and_cleanup("status_consultation", error, cleanup)
        return 3
    if verdict is not TurnStatusEvidenceVerdict.COMPLETION_WITHOUT_RESULT_EVIDENCE:
        return fail(
            "status_consultation",
            f"a completed run's status-only record classified as {verdict.value}",
            cleanup,
        )

    # --- idle-separated continuity: a second admitted turn -------------------
    try:
        second_admission = adapter.admit_detached_turn(reviewer_id, REVIEWER_OK_PROMPT)
    except Exception as error:  # noqa: BLE001
        fail_and_cleanup("second_turn", error, cleanup)
        return 3
    second_ref = second_admission.turn_reference
    found = reviewer_collector.wait_for(
        (TurnEvidenceFamily.TURN_START, TurnEvidenceFamily.TURN_TERMINAL),
        WINDOW_S["second_turn"],
    )
    if found is None:
        return fail("second_turn", "evidence window timed out", cleanup)
    second_start, second_terminal = found
    second_attribution = source.classify_turn_start(
        second_start.native_evidence, outstanding_turn_reference=second_ref
    )
    second = classify_terminal(source, second_terminal, consumed=consumed)
    emit(
        "second_turn",
        {
            "ok": True,
            "start_attribution": second_attribution.value,
            "outcome_kind": second["outcome_kind"],
            "freshness": second["freshness"],
            "result_candidate_present": second["result_candidate_present"],
        },
    )
    if second_attribution is not TurnAttribution.ATTRIBUTED:
        return fail("second_turn", f"start attribution was {second_attribution.value}", cleanup)
    if second["outcome_kind"] != "completed" or second["freshness"] != TurnFreshness.FRESH.value:
        return fail(
            "second_turn",
            f"terminal was {second['outcome_kind']} freshness {second['freshness']}",
            cleanup,
        )
    source.release_observation(lease)
    reviewer_stopped = True
    try:
        backend.stop(reviewer_id)
    except Exception as error:  # noqa: BLE001
        reviewer_stopped = False
        emit("reviewer_stop", {"ok": False, "error_class": class_name(error)})
    record_present = None
    try:
        record_present = backend.get(reviewer_id) is not None
    except Exception as error:  # noqa: BLE001
        emit("reviewer_record", {"ok": False, "error_class": class_name(error)})
    emit(
        "reviewer_stopped",
        {"stopped": reviewer_stopped, "record_present": record_present},
    )
    if not reviewer_stopped:
        return fail("reviewer_stopped", "the reviewer session could not be stopped", cleanup)
    return _continue_with_producer(
        adapter=adapter,
        backend=backend,
        source=source,
        resolver=resolver,
        cleanup=cleanup,
        reviewer_id=reviewer_id,
        token=token,
    )


def _continue_with_producer(
    *,
    adapter: Any,
    backend: Any,
    source: Any,
    resolver: ProbeResolver,
    cleanup: Cleanup,
    reviewer_id: str,
    token: str,
) -> int:
    """Producer allocation, interruption/resume, usage, reconstruction, recovery.

    Both the interrupted turn and the authorized continuation exercise the
    production detached-turn mechanics: each is enqueued through
    ``admit_detached_turn``, each admitted reference is classified against
    independently observed start evidence, and each terminal is classified
    against the session's chained consumed markers.
    """
    producer_request = AgentSessionCreationRequest(
        workspace_id=resolver.workspace_id,
        task_id=resolver.task_id,
        role=WorkflowRole.PRODUCER,
        initialization=compose_initialization("producer", None),
    )
    try:
        producer_allocation = adapter.allocate_detached_session(producer_request)
    except Exception as error:  # noqa: BLE001
        fail_and_cleanup("allocate_producer", error, cleanup)
        return 3
    producer_id = producer_allocation.external_session_id
    resolver.bind(producer_id, WorkflowRole.PRODUCER)
    cleanup.add_session(producer_id)
    emit("allocate_producer", {"ok": True, "session_allocated": True})

    producer_collector = EvidenceCollector()
    producer_lease = source.observe_session(
        connection_id=resolver.connection_id,
        external_session_id=producer_id,
        deliver=producer_collector.deliver,
    )
    cleanup.add(
        Cleanup._OBSERVATIONS,
        "producer_lease.release",
        lambda: source.release_observation(producer_lease),
    )
    pulses = {"count": 0}
    subscription = adapter.subscribe_qualifying_activity(
        producer_id, lambda: pulses.__setitem__("count", pulses["count"] + 1)
    )
    cleanup.add(
        Cleanup._OBSERVATIONS, "qualifying_subscription.unsubscribe", subscription.unsubscribe
    )

    interrupted_verdict: bool | None = None
    producer_consumed: list[int] = []
    for attempt in range(1, INTERRUPT_ATTEMPTS + 1):
        phase = f"interrupt_turn_{attempt}"
        try:
            interrupt_admission = adapter.admit_detached_turn(producer_id, PRODUCER_COUNT_PROMPT)
        except Exception as error:  # noqa: BLE001
            fail_and_cleanup(phase, error, cleanup)
            return 3
        interrupt_ref = interrupt_admission.turn_reference
        found = producer_collector.wait_for(
            (TurnEvidenceFamily.TURN_START,), WINDOW_S["interrupt_turn"]
        )
        if found is None:
            return fail(phase, "no start observation", cleanup)
        attribution = source.classify_turn_start(
            found[0].native_evidence, outstanding_turn_reference=interrupt_ref
        )
        if attribution is not TurnAttribution.ATTRIBUTED:
            return fail(phase, f"start attribution was {attribution.value}", cleanup)
        try:
            adapter.abort_activity(producer_id)
        except AgentRuntimeError as abort_failure:
            # A positively pre-effect abort failure (for example a turn that
            # already completed) is recorded and settled honestly.
            emit(phase, {"abort_class": class_name(abort_failure)})
        found = producer_collector.wait_for(
            (TurnEvidenceFamily.TURN_TERMINAL,), WINDOW_S["interrupt_turn"]
        )
        if found is None:
            return fail(phase, "no terminal observation", cleanup)
        terminal = classify_terminal(source, found[0], consumed=producer_consumed)
        interrupted_verdict = terminal["interrupted"]
        emit(
            phase,
            {
                "ok": True,
                "start_attribution": attribution.value,
                "outcome_kind": terminal["outcome_kind"],
                "freshness": terminal["freshness"],
                "interrupted": interrupted_verdict,
            },
        )
        if terminal["freshness"] != TurnFreshness.FRESH.value:
            return fail(phase, f"terminal freshness was {terminal['freshness']}", cleanup)
        producer_consumed = terminal["_consumed"]
        if interrupted_verdict:
            break
        # An un-interrupted attempt leaves an acknowledged abort guard when
        # the abort landed; settle it with the observed terminal evidence
        # before any retry, exactly as the D6b seam requires.
        try:
            adapter.settle_abort(
                producer_id, runtime_work_settled=True, operation_accounted_for=True
            )
        except Exception as error:  # noqa: BLE001
            emit(phase, {"settle_error": class_name(error)})
    if interrupted_verdict is not True:
        return fail("interruption", "no positively interrupted terminal", cleanup)

    try:
        outcome = adapter.settle_abort(
            producer_id, runtime_work_settled=True, operation_accounted_for=True
        )
        emit("settle_abort", {"settled": outcome.settled})
    except Exception as error:  # noqa: BLE001
        fail_and_cleanup("settle_abort", error, cleanup)
        return 3
    if not outcome.settled:
        return fail("settle_abort", "the interrupted-turn settlement did not commit", cleanup)

    # --- the authorized continuation: the production detached Resume ---------
    # The #343 fixed instruction is enqueued through the detached admission
    # seam — the same instruction the blocking path would send — and the
    # admitted reference is verified against the independently observed start
    # and terminal evidence on the same session.
    try:
        resume_admission = adapter.admit_detached_turn(producer_id, RESUMPTION_INSTRUCTION)
    except Exception as error:  # noqa: BLE001
        fail_and_cleanup("resume_admission", error, cleanup)
        return 3
    resume_ref = resume_admission.turn_reference
    found = producer_collector.wait_for(
        (TurnEvidenceFamily.TURN_START, TurnEvidenceFamily.TURN_TERMINAL),
        WINDOW_S["resume_turn"],
    )
    if found is None:
        return fail("resume_activity", "evidence window timed out", cleanup)
    resume_start, resume_terminal = found
    resume_attribution = source.classify_turn_start(
        resume_start.native_evidence, outstanding_turn_reference=resume_ref
    )
    resume = classify_terminal(source, resume_terminal, consumed=producer_consumed)
    emit(
        "resume_activity",
        {
            "ok": True,
            "mechanism": "detached_admission",
            "start_attribution": resume_attribution.value,
            "outcome_kind": resume["outcome_kind"],
            "freshness": resume["freshness"],
            "result_candidate_chars": resume["result_candidate_chars"],
        },
    )
    if resume_attribution is not TurnAttribution.ATTRIBUTED:
        return fail("resume_activity", f"start attribution was {resume_attribution.value}", cleanup)
    if (
        resume["outcome_kind"] != "completed"
        or resume["freshness"] != TurnFreshness.FRESH.value
        or resume["interrupted"]
    ):
        return fail(
            "resume_activity",
            (
                f"the resumed turn was {resume['outcome_kind']} freshness "
                f"{resume['freshness']} interrupted={resume['interrupted']}"
            ),
            cleanup,
        )
    producer_consumed = resume["_consumed"]

    # --- aggregate usage and content-free activity attestation --------------
    try:
        reading = source.consult_session_usage(
            connection_id=resolver.connection_id, external_session_id=producer_id
        )
        snapshot = adapter.read_activity_snapshot(producer_id)
        emit(
            "usage_attestation",
            {
                "aggregate_total_usd": (
                    "unknown"
                    if reading.aggregate_total_usd is None
                    else str(reading.aggregate_total_usd)
                ),
                "root_scope": usage_state(
                    snapshot.usage.total_cost if snapshot.usage is not None else None
                ),
                "aggregate_scope": usage_state(
                    snapshot.aggregate_usage.total_cost
                    if snapshot.aggregate_usage is not None
                    else None
                ),
                "qualifying_activity_pulses": pulses["count"],
                "usage_pulses": producer_collector.usage_pulses,
            },
        )
    except Exception as error:  # noqa: BLE001
        fail_and_cleanup("usage_attestation", error, cleanup)
        return 3
    if pulses["count"] <= 0:
        return fail(
            "usage_attestation",
            "no qualified model-backed activity was observed for the exact session",
            cleanup,
        )
    return _continue_with_reconstruction(
        adapter=adapter,
        backend=backend,
        resolver=resolver,
        cleanup=cleanup,
        producer_id=producer_id,
        reviewer_id=reviewer_id,
    )


def _continue_with_reconstruction(
    *,
    adapter: Any,
    backend: Any,
    resolver: ProbeResolver,
    cleanup: Cleanup,
    producer_id: str,
    reviewer_id: str,
) -> int:  # noqa: ANN201 - the probe keeps its own typing local and simple
    """Same-ID reconstruction and exact stopped-session recovery, then cleanup.

    Transcript preservation is established by comparing the actual raw
    transcript contents privately before and after each same-ID start (never
    logged); the effective construction behavior of a mode transition is
    attributed to the Owner-run D7 suite, which owns the full workflow
    scenarios.
    """
    try:
        before = backend.read_messages(producer_id)
        adapter.reconstruct_session(producer_id, target_mode=ClineConstructionMode.PLAN)
        after = backend.read_messages(producer_id)
    except Exception as error:  # noqa: BLE001
        fail_and_cleanup("reconstruction", error, cleanup)
        return 3
    verbatim = bool(before and after == before)
    emit(
        "reconstruction",
        {
            "ok": True,
            "target_mode": "plan",
            "transcript_messages": len(before),
            "transcript_verbatim": verbatim,
        },
    )
    if not verbatim:
        return fail("reconstruction", "the transcript was not preserved verbatim", cleanup)

    # --- exact stopped-session recovery --------------------------------------
    try:
        backend.stop(producer_id)
        record_present = backend.get(producer_id) is not None
        adapter.recover_session(producer_id, target_mode=ClineConstructionMode.ACT)
        recovered = backend.read_messages(producer_id)
    except Exception as error:  # noqa: BLE001
        fail_and_cleanup("stopped_recovery", error, cleanup)
        return 3
    recovered_verbatim = bool(recovered and recovered == after)
    emit(
        "stopped_recovery",
        {
            "ok": True,
            "record_present_while_stopped": record_present,
            "transcript_messages_after_recovery": len(recovered),
            "transcript_verbatim": recovered_verbatim,
        },
    )
    if not recovered_verbatim:
        return fail(
            "stopped_recovery", "the recovered transcript was not preserved verbatim", cleanup
        )

    # --- bounded cleanup and quiescence verification --------------------------
    # Every allocated session is registered for stop at allocation; the stops
    # run before any attachment disposal, each outcome is emitted, and the
    # probe's own sessions are verified quiescent through the status-only
    # run.list surface before the attachments are disposed.
    if not cleanup.stop_sessions():
        return fail("cleanup", "one or more probe sessions could not be stopped", cleanup)
    try:
        quiescence = verify_quiescence(backend, [reviewer_id, producer_id])
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
