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
injection and no paid fallback. Sessions are disposable; every acquired
resource is released in the bounded cleanup path.
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

from openorc.adapters.agent_runtime.errors import AgentRuntimeError  # noqa: E402
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
    TurnEvidenceFamily,
    TurnFreshness,
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
WINDOW_S = {"init_turn": 240.0, "second_turn": 180.0, "interrupt_turn": 240.0}
INTERRUPT_ATTEMPTS = 2


def emit(phase: str, data: dict[str, Any]) -> None:
    print(json.dumps({"phase": phase, **data}, sort_keys=True), flush=True)


def class_name(error: BaseException) -> str:
    return type(error).__name__


class Cleanup:
    """Bounded best-effort release of everything the probe acquired."""

    def __init__(self) -> None:
        self._jobs: list[tuple[str, Any]] = []

    def add(self, label: str, job: Any) -> None:
        self._jobs.append((label, job))

    def run(self) -> None:
        for label, job in self._jobs:
            try:
                job()
            except Exception as error:  # noqa: BLE001 - cleanup is best-effort
                emit("cleanup_error", {"label": label, "error_class": class_name(error)})


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

    cleanup = Cleanup()
    resolver = ProbeResolver()
    backend = (
        ClineBridgeProcessBackend(command=[NODE, entrypoint])
        if NODE
        else ClineBridgeProcessBackend()
    )
    cleanup.add("backend.dispose", backend.dispose)
    remote = ClineRemoteConfig(endpoint=ENDPOINT, client_identity=CLIENT_IDENTITY, auth_token=token)
    source = ClineHubTurnObservationSource(backend_factory=lambda: backend)
    cleanup.add("source.dispose", source.dispose)
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
        cleanup.add("reviewer_lease.release", lambda: source.release_observation(lease))
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
        emit("initialization_turn", {"ok": False, "reason": "evidence window timed out"})
        cleanup.run()
        return 3
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
        emit("status_consultation", {"ok": False, "error_class": class_name(error)})

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
        emit("second_turn", {"ok": False, "reason": "evidence window timed out"})
        cleanup.run()
        return 3
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
    source.release_observation(lease)
    try:
        backend.stop(reviewer_id)
        reviewer_stopped = True
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
    return _continue_with_producer(
        adapter=adapter,
        backend=backend,
        source=source,
        resolver=resolver,
        cleanup=cleanup,
        token=token,
    )


def _continue_with_producer(
    *,
    adapter: Any,
    backend: Any,
    source: Any,
    resolver: ProbeResolver,
    cleanup: Cleanup,
    token: str,
) -> int:
    """Producer allocation, interruption/resume, usage, reconstruction, recovery."""
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
    emit("allocate_producer", {"ok": True, "session_allocated": True})

    producer_collector = EvidenceCollector()
    producer_lease = source.observe_session(
        connection_id=resolver.connection_id,
        external_session_id=producer_id,
        deliver=producer_collector.deliver,
    )
    cleanup.add("producer_lease.release", lambda: source.release_observation(producer_lease))
    pulses = {"count": 0}
    subscription = adapter.subscribe_qualifying_activity(
        producer_id, lambda: pulses.__setitem__("count", pulses["count"] + 1)
    )
    cleanup.add("qualifying_subscription.unsubscribe", subscription.unsubscribe)

    interrupted_verdict: bool | None = None
    for attempt in range(1, INTERRUPT_ATTEMPTS + 1):
        phase = f"interrupt_turn_{attempt}"
        try:
            adapter.admit_detached_turn(producer_id, PRODUCER_COUNT_PROMPT)
        except Exception as error:  # noqa: BLE001
            fail_and_cleanup(phase, error, cleanup)
            return 3
        found = producer_collector.wait_for(
            (TurnEvidenceFamily.TURN_START,), WINDOW_S["interrupt_turn"]
        )
        if found is None:
            emit(phase, {"ok": False, "reason": "no start observation"})
            cleanup.run()
            return 3
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
            emit(phase, {"ok": False, "reason": "no terminal observation"})
            cleanup.run()
            return 3
        terminal = classify_terminal(source, found[0], consumed=[])
        interrupted_verdict = terminal["interrupted"]
        emit(
            phase,
            {
                "ok": True,
                "outcome_kind": terminal["outcome_kind"],
                "interrupted": interrupted_verdict,
            },
        )
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
        emit("interruption", {"ok": False, "reason": "no positively interrupted terminal"})
        cleanup.run()
        return 3

    try:
        outcome = adapter.settle_abort(
            producer_id, runtime_work_settled=True, operation_accounted_for=True
        )
        emit("settle_abort", {"settled": outcome.settled})
    except Exception as error:  # noqa: BLE001
        fail_and_cleanup("settle_abort", error, cleanup)
        return 3

    try:
        response = adapter.resume_activity(producer_id, expected_family=None)
        emit("resume_activity", {"ok": True, "response_chars": len(getattr(response, "text", ""))})
    except Exception as error:  # noqa: BLE001
        fail_and_cleanup("resume_activity", error, cleanup)
        return 3

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
    return _continue_with_reconstruction(
        adapter=adapter,
        backend=backend,
        resolver=resolver,
        cleanup=cleanup,
        producer_id=producer_id,
    )


def _continue_with_reconstruction(
    *,
    adapter: Any,
    backend: Any,
    resolver: ProbeResolver,
    cleanup: Cleanup,
    producer_id: str,
) -> int:  # noqa: ANN201 - the probe keeps its own typing local and simple
    """Same-ID reconstruction and exact stopped-session recovery, then cleanup."""
    try:
        before = len(backend.read_messages(producer_id))
        adapter.reconstruct_session(producer_id, target_mode=ClineConstructionMode.PLAN)
        after = len(backend.read_messages(producer_id))
        emit(
            "reconstruction",
            {
                "ok": True,
                "target_mode": "plan",
                "transcript_messages_before": before,
                "transcript_messages_after": after,
                "retained": bool(before > 0 and after == before),
            },
        )
    except Exception as error:  # noqa: BLE001
        fail_and_cleanup("reconstruction", error, cleanup)
        return 3

    # --- exact stopped-session recovery --------------------------------------
    try:
        backend.stop(producer_id)
        record_present = backend.get(producer_id) is not None
        adapter.recover_session(producer_id, target_mode=ClineConstructionMode.ACT)
        after = len(backend.read_messages(producer_id))
        emit(
            "stopped_recovery",
            {
                "ok": True,
                "record_present_while_stopped": record_present,
                "transcript_messages_after_recovery": after,
            },
        )
    except Exception as error:  # noqa: BLE001
        fail_and_cleanup("stopped_recovery", error, cleanup)
        return 3

    emit("probe_complete", {"ok": True})
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
