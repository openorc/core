"""The official Cline Agent Runtime adapter (issue #74, D5a).

``ClineAdapter`` implements the universal runtime-neutral #67
``AgentRuntimeAdapter`` contract on top of the language-neutral #71
``ClineSdkBackend`` seam (the real #73 bridge-process backend or the
deterministic test fake). It owns exactly the runtime-native mechanics D5a
assigns to it: fresh Task/role session construction with the canonical
first-send ``session_ready`` readiness handshake, verbatim exact-session
interactions with shared #65 formal-response validation, realization of
already-authorized #129/#181 semantic controls — including the native Cline
PLAN→ACT realization of IMPLEMENT — and one serialized same-external-ID
reconstruction seam reused later by D6b recovery.

Authority and meaning stay above this boundary. The adapter never decides
workflow authorization, never mutates durable OpenOrc persistence, never
authors or re-renders initialization/control prose, never interprets a
received ``WorkspaceResult``, and never substitutes, redirects, or replaces
an exact external session. Fresh Producer construction is native Cline ACT
at the caller-selected existing repository base; the Reviewer is always
native Cline PLAN at a neutral non-repository directory; bootstrap ACT is
workspace-preparation context, never implementation authority.

Construction inputs (provider/model pair, role Markdown, phase-selected
working directory) arrive only through the injected read-only
``ClineConstructionContextResolver`` — the authoritative wiring seam for
both fresh construction and continued operation on existing bound sessions.
The adapter's memory of sessions it established itself is an optimization
only: its absence is never session-loss evidence, and exact-session absence
is claimed only from positively established backend evidence.

Failure discipline: every backend failure translates into exactly one
normalized #67 error with a documented safe classification grounded in the
pinned result shape and positive code/phase evidence — a machine code the
pinned bridge merely forwards without establishing effect never becomes a
known pre-effect failure for an effectful operation; unproven effectful
outcomes stay conservatively uncertain and latch reconstruction. Native
backend exceptions are handled and discarded so
``__cause__``/``__context__`` stay clean; the one deliberate chain is the
shared safe #65 ``ProtocolError`` inside ``AgentRuntimeProtocolFailureError``.
Uncertain outcomes are never retried, never downgraded to known failures,
and latch the session against same-ID reconstruction until the caller's
reconciliation work re-establishes safety.

The D6a activity-control operation (``abort_activity``) is the narrow
Cline-specific interrupt behind the authorized Owner-facing interrupt
command and terminal Task cancellation: one public ``ClineCore.abort``
dispatch for the exact bound Producer or Reviewer session ID, dispatched
without the per-session send/reconstruction lock so it reaches the runtime
while a send is still in flight. A short-lived per-session coordination
gate orders abort against the serialized same-ID reconstruction section:
a reconstruction that already entered its incarnation-change section makes
the abort decline locally before any dispatch, and a reconstruction
refuses — before its first ``read_messages``/``stop`` effect — while any
abort of that session is outstanding, acknowledged-but-unreconciled, or
outcome-uncertain. Outstanding aborts are counted per session, so one
abort's known pre-effect failure can never release a concurrent abort's
unresolved intent, and an acknowledged abort never clears the guard by
itself: that reconciliation belongs to D6b, not to this adapter.

The D6b interruption-reconciliation and recovery seam (issue #148) is the
small Cline-local, caller-invoked surface beside these operations:
``reconcile_session`` inspects the exact bound session through qualified
public reads and returns a typed, conservative continuity classification
plus bounded checkpoint-derived observations; ``settle_uncertain_send`` and
``settle_abort`` release exactly one accounted-for uncertainty kind with
positive caller-attested evidence under the existing lock ordering; and
``recover_session`` performs the authorized Hub-restart recovery through
the same single same-ID reconstruction seam. All three consume the
backend's already-established attachment — the adapter never reconnects,
replaces, or manages transport state — and never decide workflow meaning.

The D6c activity/usage observation seam (issue #164) is the Cline-local
observational surface beside these operations: ``subscribe_activity``
delivers a safe typed projection of the attached backend's session events
for the exact bound session, and ``read_activity_snapshot`` reports the
usage the runtime currently accounts for it. Both resolve the durable
Task/role binding first and fail closed before any backend effect, take no
send/reconstruction lock, and never reconnect, replace, or manage transport
state. Consumers receive only
:mod:`openorc.adapters.cline.activity` observations — never the opaque SDK
event payload — and every observation stays advisory runtime bookkeeping
that never decides workflow meaning.
"""

from __future__ import annotations

import enum
import threading
from collections.abc import Callable
from dataclasses import dataclass, replace
from typing import Final, TypeVar

from openorc.adapters.agent_runtime.contract import AgentRuntimeAdapter
from openorc.adapters.agent_runtime.errors import (
    AgentRuntimeConfigurationRejectedError,
    AgentRuntimeError,
    AgentRuntimeProtocolFailureError,
    AgentRuntimeTransportError,
    AgentRuntimeUnavailableError,
    AgentRuntimeUncertainOutcomeError,
    AgentSessionNotFoundError,
)
from openorc.adapters.agent_runtime.results import (
    AgentSessionCreated,
    AgentSessionCreationRequest,
    AgentTextResponse,
)
from openorc.adapters.cline.activity import (
    ClineActivityObservation,
    ClineActivitySnapshot,
    project_session_event,
    project_usage_summary,
)
from openorc.adapters.cline.classification import (
    EFFECTFUL_OPERATIONS,
    classify_backend_failure,
)
from openorc.adapters.cline.construction import (
    ClineConstructionContext,
    ClineConstructionContextResolver,
    ClineSessionIdentity,
    resolve_role_markdown,
)
from openorc.adapters.cline.contract import ClineSdkBackend
from openorc.adapters.cline.errors import ClineSdkBackendError
from openorc.adapters.cline.reconciliation import (
    ClineDispatchCheckpoint,
    ClineReconciliationResult,
    ClineSessionEvidence,
    ClineSettlementOutcome,
    ClineSettlementRefusal,
    classify_session_evidence,
)
from openorc.adapters.cline.review_subject import (
    UNREADABLE_EVIDENCE,
    ClineReviewSubjectUnverifiedError,
    verify_pr_review_turn,
)
from openorc.adapters.cline.values import (
    ClineConstructionMode,
    ClineSessionEvent,
    ClineStartRequest,
    JsonObject,
    Subscription,
)
from openorc.domain.connections import WorkflowRole
from openorc.observability import annotate_span, application_span
from openorc.protocol.interaction import (
    Interaction,
    InteractionKind,
    PrReviewSubject,
    ReviewInteraction,
)
from openorc.protocol.models import (
    REVIEW_RESULT_FAMILY,
    SESSION_READY_FAMILY,
    FormalResponse,
    SessionReady,
)

__all__ = ["ClineAdapter"]

_T = TypeVar("_T")

_TRACER_SCOPE: Final = "openorc.adapters.cline"

# The pinned SDK-native ``AgentResult`` completion marker: the one finish
# reason that carries a usable reply candidate (qualified R2b/R3 baseline).
_SUCCESSFUL_FINISH: Final = "completed"


def _extract_reply_candidate(result: JsonObject | None, *, phase: str) -> str:
    """Extract the reply candidate from the pinned SDK-native ``AgentResult``.

    The qualified 0.0.90 shape carries ``finishReason`` and, on successful
    completion, the reply ``text`` (R2b/R3). Validation is envelope-first:
    a legitimate ``None`` SDK response, a non-object result, a missing or
    non-string ``finishReason``, or a missing reply text is an
    answered-but-unusable native response — a normalized transport failure,
    never a formal-protocol failure and never a heuristic extraction. An
    SDK-reported non-success finish is a known non-success outcome, never a
    successful response: during the readiness construction it is a
    construction failure (the R3-qualified first-turn provider/configuration
    rejection shape); on later turns it is a transport-class failure. The
    SDK-native ``error`` payload is never read into any message.
    """
    if result is None:
        raise AgentRuntimeTransportError(
            f"the cline runtime answered the {phase} turn without a usable response"
        )
    if not isinstance(result, dict):
        raise AgentRuntimeTransportError(
            f"the cline runtime {phase} response was not a usable result object"
        )
    finish_reason = result.get("finishReason")
    if not isinstance(finish_reason, str):
        raise AgentRuntimeTransportError(
            "the cline runtime response completion envelope could not be validated"
        )
    if finish_reason != _SUCCESSFUL_FINISH:
        if phase == "readiness":
            raise AgentRuntimeConfigurationRejectedError(
                "the cline session readiness turn did not complete successfully"
            )
        raise AgentRuntimeTransportError(
            "the cline runtime turn ended without a successful response"
        )
    text = result.get("text")
    if not isinstance(text, str):
        raise AgentRuntimeTransportError("the cline runtime response carried no usable reply text")
    return text


def _require_construction_inputs(context: ClineConstructionContext) -> None:
    """Fail closed on absent construction inputs before any runtime effect.

    The provider/model pair and the workflow-selected working directory are
    the adapter's complete construction inputs. Absent values are known
    pre-effect construction failures — never defaults, never substitutions,
    and never a reason to probe the local filesystem for runtime-side paths.
    """
    if context.configured_provider is None or context.configured_model is None:
        raise AgentRuntimeConfigurationRejectedError(
            "the workflow role binding has no complete configured provider/model "
            "pair for cline session construction"
        )
    if context.cwd is None or context.workspace_root is None:
        raise AgentRuntimeConfigurationRejectedError(
            "no workflow-selected existing runtime-side working directory was "
            "supplied for this authorized construction phase"
        )
    if context.cwd != context.workspace_root:
        raise AgentRuntimeConfigurationRejectedError(
            "the workflow-selected working directory for this authorized "
            "construction phase must identify the same runtime-side path as "
            "both the cwd and the workspace root"
        )


def _build_start_request(
    context: ClineConstructionContext,
    *,
    mode: ClineConstructionMode,
    session_id: str | None = None,
    initial_messages: list[JsonObject] | None = None,
) -> ClineStartRequest:
    """Build the complete v1 construction bundle for one fresh or same-ID start.

    The role Markdown is the resolved role behavior input (``rules``); the
    system prompt is the required explicit empty string; the deliberate v1
    session tool policy auto-approves enabled tools and disables
    ``ask_question``. Nothing else is synthesized: no spawn/team flags, no
    approval executors, no credentials, no catalogs, no custom harnesses.
    """
    return ClineStartRequest(
        provider_id=context.configured_provider or "",
        model_id=context.configured_model or "",
        mode=mode,
        rules=resolve_role_markdown(context.identity.role, context.role_prompt_override),
        system_prompt="",
        cwd=context.cwd or "",
        workspace_root=context.workspace_root or "",
        enable_tools=True,
        interactive=True,
        tool_policies={"*": {"autoApprove": True}, "ask_question": {"enabled": False}},
        session_id=session_id,
        initial_messages=initial_messages,
    )


def _history_presence(history: list[JsonObject], session_id: str) -> bool | None:
    """Disambiguate exact-session presence from one history enumeration.

    D6b uses the public ``list_history`` observation only to disambiguate
    whether a history enumeration that answered successfully positively
    identifies the exact session ID. Returns True when an entry positively
    matches the exact ID, False when the enumeration demonstrably carries
    session identity (at least one entry with a recognizable session-ID
    field) and no entry matches, and None when identity cannot be
    established from the entries (inconclusive — never evidence of
    absence). Record semantics are never interpreted here.

    A False answer observes that this enumeration is readable, never that
    it exhaustively covers every persisted session: the #71 backend
    contract guarantees no such exhaustiveness, so enumeration absence is
    never terminal-loss proof (see
    :func:`openorc.adapters.cline.reconciliation.classify_session_evidence`).
    """
    identifiable = False
    for entry in history:
        if not isinstance(entry, dict):
            continue
        matched = False
        has_identity = False
        for field in ("sessionId", "session_id", "id"):
            value = entry.get(field)
            if isinstance(value, str) and value.strip():
                has_identity = True
                if value == session_id:
                    matched = True
        if matched:
            return True
        if has_identity:
            identifiable = True
    return False if identifiable else None


class _AbortState(enum.Enum):
    """Local coordination state of one session's activity-abort intent (D6a).

    ``PENDING`` marks a registered abort whose backend outcome has not
    settled; ``ACKNOWLEDGED`` an acknowledged dispatch whose interruption is
    deliberately left locally unresolved for #148's reconciliation;
    ``UNCERTAIN`` an abort whose effect could not be classified. Guard
    states only strengthen: ``ACKNOWLEDGED`` and ``UNCERTAIN`` never
    downgrade, and only a ``PENDING`` intent with no other outstanding
    abort for the session may return to ``NONE`` after a known pre-effect
    failure or positive session-not-found outcome.
    """

    NONE = "none"
    PENDING = "pending"
    ACKNOWLEDGED = "acknowledged"
    UNCERTAIN = "uncertain"


@dataclass
class _AbortGuard:
    """One session's outstanding-abort accounting and guard state (D6a).

    ``outstanding`` counts registered-but-unsettled ``abort_activity``
    invocations for the session, so one abort's known pre-effect failure
    can never release the reconstruction guard while a concurrent abort
    for the same session is still in flight.
    """

    outstanding: int = 0
    state: _AbortState = _AbortState.NONE


class ClineAdapter(AgentRuntimeAdapter):
    """The official Cline Agent Runtime adapter over the #71 SDK backend seam.

    The injected backend is an already-connected ``ClineSdkBackend`` for the
    admitted Connection — the real bridge-process backend or the test fake.
    Current Hub endpoint/attach-token resolution and backend lifecycle belong
    to the existing runtime/Cloud seam; transport discovery, token refresh,
    and native Cline settings access are never implemented here.
    """

    def __init__(
        self,
        backend: ClineSdkBackend,
        construction_contexts: ClineConstructionContextResolver,
    ) -> None:
        self._backend = backend
        self._construction_contexts = construction_contexts
        # Per-session serialization of runtime-incarnation changes against
        # this adapter's own same-session sends/controls.
        self._session_locks: dict[str, threading.Lock] = {}
        # Optimization-only memory of sessions THIS instance created or
        # reconstituted. It is never the identity authority: the injected
        # resolver's durable binding resolution is. A registry miss is an
        # optimization miss, never exact-session-loss evidence.
        self._known_identity: dict[str, ClineSessionIdentity] = {}
        # Unresolved uncertain runtime effects, per session and per effect
        # kind (the EFFECTFUL_OPERATIONS vocabulary; D6b): a pure
        # observational read can never take an external effect, so only an
        # effectful operation's unproven outcome is recorded here.
        # Same-ID reconstruction is refused while any kind remains
        # unresolved; the D6b settlement seam releases exactly one
        # accounted-for kind with positive caller-attested evidence. No
        # automatic clearing exists.
        self._uncertain_effects: dict[str, set[str]] = {}
        # D6a activity-abort coordination: per-session outstanding-abort
        # accounting plus the reconstruction-active set, ordered against the
        # send/reconstruction locks through one short-lived coordination lock
        # (send lock -> coordination lock; the abort path takes only the
        # coordination lock and never waits on the send lock).
        self._abort_coordination = threading.Lock()
        self._abort_guards: dict[str, _AbortGuard] = {}
        self._reconstruction_active: set[str] = set()

    def _lock_for(self, session_id: str) -> threading.Lock:
        return self._session_locks.setdefault(session_id, threading.Lock())

    def _backend_operation(
        self,
        operation: str,
        invoke: Callable[[], _T],
        *,
        session_id: str | None = None,
    ) -> _T:
        """Run one backend operation under the normalized failure discipline.

        The backend taxonomy is the only native boundary, so only
        ``ClineSdkBackendError`` is translated; the backend error object is
        discarded and the normalized error is raised after the failing
        handler exits, keeping ``__cause__`` and ``__context__`` empty. An
        uncertain outcome on a session-addressed *effectful* operation
        (``EFFECTFUL_OPERATIONS``) records that operation kind as an
        unresolved uncertain effect for the session (D6b): same-ID
        reconstruction is refused while any kind remains. A pure
        observational read can never take an external effect, so even an
        uncertain-classified transport failure on a read never latches an
        otherwise safe session.
        """
        failure: AgentRuntimeError | None = None
        try:
            return invoke()
        except ClineSdkBackendError as error:
            failure = classify_backend_failure(error, operation=operation)
            if (
                session_id is not None
                and operation in EFFECTFUL_OPERATIONS
                and isinstance(failure, AgentRuntimeUncertainOutcomeError)
            ):
                self._uncertain_effects.setdefault(session_id, set()).add(operation)
        assert failure is not None
        raise failure from None

    def _resolve_bound_context(self, session_id: str) -> ClineConstructionContext:
        """Resolve the full construction context of an existing bound session.

        Reconstruction and role-sensitive enforcement both resolve through
        this method: the wiring-resolved durable binding is the sole
        authority, and this instance's memory of sessions it established is
        only a consistency check that must agree with the resolver — never a
        substitute for it. An establishment miss fails closed before any
        runtime effect without claiming session loss.
        """
        known = self._known_identity.get(session_id)
        resolved = self._construction_contexts.resolve_bound_session_context(session_id)
        if resolved is None:
            raise AgentRuntimeConfigurationRejectedError(
                "the bound Task/role construction context for the addressed "
                "external session could not be established"
            )
        if known is not None and known != resolved.identity:
            raise ValueError(
                "the resolved binding context disagrees with the session "
                "identity this adapter instance established"
            )
        return resolved

    def _require_producer_identity(self, session_id: str, *, control: str) -> None:
        """Enforce the exact bound Producer context of a Producer-only control.

        Workspace preparation/cleanup and IMPLEMENT are never dispatched from
        an unrelated Reviewer session or from a session whose bound context
        cannot be established; both fail closed before any runtime effect.
        """
        identity = self._resolve_bound_context(session_id).identity
        if identity.role is not WorkflowRole.PRODUCER:
            raise ValueError(
                f"the {control} control is Producer-only; the addressed session "
                "is bound to another workflow role"
            )

    def create_session(self, request: AgentSessionCreationRequest) -> AgentSessionCreated:
        """Establish one ready isolated external Cline session for the Task/role.

        One logical readiness handshake: allocate the fresh external context
        with the complete construction bundle (fresh Producer = native ACT at
        the authorized existing repository base; Reviewer = native PLAN at a
        neutral non-repository directory), deliver the composed canonical
        initialization verbatim as the first model-backed send, validate the
        reply as a canonical ``session_ready`` through the shared funnel, and
        return the exact allocated external session ID. Readiness is never
        implied workspace preparation, and an allocated-but-unconfirmed ID is
        never a successful result.
        """
        with application_span(_TRACER_SCOPE, "cline_adapter.create_session") as span:
            annotate_span(
                span,
                operation="cline_adapter.create_session",
                workspace_id=str(request.workspace_id),
                task_id=str(request.task_id),
                workflow_role=request.role.value,
            )
            context = self._construction_contexts.resolve_construction_context(
                request.workspace_id, request.task_id, request.role
            )
            identity = context.identity
            if (
                identity.workspace_id != request.workspace_id
                or identity.task_id != request.task_id
                or identity.role is not request.role
            ):
                raise AgentRuntimeConfigurationRejectedError(
                    "the resolved construction context does not address the "
                    "requested exact Task/role"
                )
            _require_construction_inputs(context)
            mode = (
                ClineConstructionMode.ACT
                if request.role is WorkflowRole.PRODUCER
                else ClineConstructionMode.PLAN
            )
            start_request = _build_start_request(context, mode=mode)
            allocated = self._backend_operation("start", lambda: self._backend.start(start_request))
            session_id = allocated.session_id
            raw = self._backend_operation(
                "send",
                lambda: self._backend.send(session_id, request.initialization),
                session_id=session_id,
            )
            candidate = _extract_reply_candidate(raw, phase="readiness")
            ready = self._parse_formal_candidate(candidate, expected_family=SESSION_READY_FAMILY)
            if not isinstance(ready, SessionReady) or ready.status != "READY":
                raise AgentRuntimeProtocolFailureError(
                    "formal response failed the shared canonical v1 protocol contract"
                )
            self._session_locks.setdefault(session_id, threading.Lock())
            self._known_identity[session_id] = identity
            return AgentSessionCreated(external_session_id=session_id)

    def send(
        self,
        session_id: str,
        message: str,
        *,
        expected_family: str | None = None,
    ) -> FormalResponse | AgentTextResponse:
        """Send one composed message verbatim to the exact external session.

        The message is forwarded unchanged; sessions are never replaced,
        redirected, merged, or selected from history. With an expected
        family, the extracted runtime-native candidate is validated through
        the shared #65 funnel; without one, the reply stays ordinary text.
        """
        with application_span(_TRACER_SCOPE, "cline_adapter.send") as span:
            annotate_span(span, operation="cline_adapter.send")
            if not isinstance(session_id, str) or not session_id.strip():
                raise ValueError("session_id must be a non-empty string")
            if not isinstance(message, str) or not message.strip():
                raise ValueError("message must be a non-empty string")
            with self._lock_for(session_id):
                raw = self._backend_operation(
                    "send",
                    lambda: self._backend.send(session_id, message),
                    session_id=session_id,
                )
            candidate = _extract_reply_candidate(raw, phase="send")
            if expected_family is None:
                return AgentTextResponse(text=candidate)
            return self._parse_formal_candidate(candidate, expected_family=expected_family)

    def realize_control(
        self,
        session_id: str,
        interaction: Interaction,
        *,
        message: str | None = None,
        expected_family: str | None = None,
    ) -> FormalResponse | None:
        """Realize one already-authorized semantic #129/#181 control.

        ``ImplementInteraction`` is realized natively: the authorized same-ID
        PLAN→ACT reconstruction at the Task checkout, returning ``None`` and
        issuing no implementation prompt — the actual implementation
        interaction is separately composed and sent by workflow code. A
        ``ReviewInteraction`` carrying a ``PrReviewSubject`` takes the D5b
        verified PR-review path (:meth:`_send_verified_pr_review`): the
        caller-composed review message is sent and the parsed
        ``review_result`` is released only after the current-turn
        ``read_messages`` tool evidence establishes access to the exact
        assigned PR subject. All other controls, including planning reviews
        and the #181 workspace preparation and cleanup controls, take the
        shared prose-only path with the caller-composed message and expected
        family; the adapter never renders control prose, never decides
        authorization, and never changes construction context from a
        received ``WorkspaceResult``.
        """
        with application_span(_TRACER_SCOPE, "cline_adapter.realize_control") as span:
            annotate_span(span, operation="cline_adapter.realize_control")
            if not isinstance(interaction, Interaction):
                raise ValueError("interaction must be a protocol semantic Interaction")
            if interaction.kind is InteractionKind.IMPLEMENT:
                self._require_producer_identity(session_id, control="IMPLEMENT")
                self.reconstruct_session(session_id, target_mode=ClineConstructionMode.ACT)
                return None
            if interaction.kind in (
                InteractionKind.PREPARE_WORKSPACE,
                InteractionKind.CLEANUP_WORKSPACE,
            ):
                self._require_producer_identity(session_id, control=interaction.kind.value)
            if isinstance(interaction, ReviewInteraction) and isinstance(
                interaction.subject, PrReviewSubject
            ):
                return self._send_verified_pr_review(
                    session_id,
                    interaction.subject,
                    message=message,
                    expected_family=expected_family,
                )
            return super().realize_control(
                session_id,
                interaction,
                message=message,
                expected_family=expected_family,
            )

    def _send_verified_pr_review(
        self,
        session_id: str,
        subject: PrReviewSubject,
        *,
        message: str | None,
        expected_family: str | None,
    ) -> FormalResponse:
        """Send the composed PR-review message and verify its subject (D5b).

        The caller-composed message is forwarded verbatim to the existing
        persistent PLAN-mode Reviewer session at its neutral non-repository
        working directory — the adapter renders no review prose and changes
        no construction context. The caller's message must name the assigned
        repository, PR number, and exact head SHA and direct the Reviewer to
        inspect that PR through its native GitHub MCP, reading code at the
        assigned SHA with commit-addressed ``get_file_contents`` reads; the
        adapter cannot render, repair, or infer that message, and it never
        derives the authoritative subject from message text.

        Verification: under this adapter's per-session serialization, one
        pre-send transcript snapshot is read through the public
        ``read_messages`` surface, the formal send completes, and one
        post-send read establishes the appended current-turn suffix. The
        shared #65 funnel parses the reply exactly as an ordinary formal
        send would. The typed ``review_result`` is released only when the
        suffix's tool evidence — paired ``tool_use``/``tool_result`` blocks
        for the two qualified native GitHub MCP shapes — establishes access
        to the assigned PR and at least one commit-addressed code read at
        the assigned SHA; otherwise the parsed result is discarded and the
        interaction fails with the safe
        :class:`openorc.adapters.cline.review_subject.ClineReviewSubjectUnverifiedError`
        without retry. A failed pre-send read means the boundary cannot even
        be attempted: the normalized backend error surfaces before any send,
        so no unverifiable review is ever delivered. A failed post-send
        evidence read also withholds the result — as the same safe unverified
        failure, never as a pre-effect classification of the already-completed
        send, so no caller can blind-replay the interaction. No new session,
        no reconstruction, no GitHub/workflow authority is introduced here;
        the
        assigned SHA stays OpenOrc/GitHub-owned and the workflow owns every
        subsequent current-head check.
        """
        if message is None:
            raise ValueError(
                "the verified PR-review realization requires the composed "
                "interaction message; composing it is the application layer's "
                "obligation, never adapter prose"
            )
        if not message.strip():
            raise ValueError("the composed control message must be a non-empty string")
        if expected_family != REVIEW_RESULT_FAMILY:
            raise ValueError(
                "the verified PR-review realization expects the review_result response family"
            )
        with self._lock_for(session_id):
            pre_turn = self._backend_operation(
                "read_messages",
                lambda: self._backend.read_messages(session_id),
                session_id=session_id,
            )
            raw = self._backend_operation(
                "send",
                lambda: self._backend.send(session_id, message),
                session_id=session_id,
            )
            candidate = _extract_reply_candidate(raw, phase="send")
            result = self._parse_formal_candidate(candidate, expected_family=REVIEW_RESULT_FAMILY)
            evidence_read_failed = False
            try:
                post_turn = self._backend_operation(
                    "read_messages",
                    lambda: self._backend.read_messages(session_id),
                    session_id=session_id,
                )
            except AgentRuntimeError:
                # The review send already completed: failing to retrieve the
                # verification transcript withholds the review result without
                # implying the send never occurred — no pre-effect
                # unavailability/non-delivery classification may escape and
                # invite a blind replay of the completed interaction. The
                # normalized error is discarded here and the safe unverified
                # failure is raised after this handler exits so the exception
                # chain stays clean.
                evidence_read_failed = True
                post_turn = None
        if evidence_read_failed or post_turn is None:
            raise ClineReviewSubjectUnverifiedError(UNREADABLE_EVIDENCE)
        verify_pr_review_turn(subject, pre_turn, post_turn)
        return result

    def abort_activity(self, session_id: str) -> None:
        """Interrupt the active Cline activity of the exact bound session (D6a).

        The narrow Cline-specific activity control behind the authorized
        Owner-facing interrupt command and, separately, terminal Task
        cancellation: dispatch exactly one public ``ClineCore.abort`` for the
        exact external session ID of a bound Task/role session — Producer or
        Reviewer alike. It is not OpenOrc Task cancellation, session
        deletion, a resume command, or ``stop(sessionId)``: the same
        ``TaskAgentSession``, external session ID, and retained conversation
        are preserved, no replacement session or new conversational prompt is
        created, and no required abort reason exists (the backend's optional
        default is used; Owner prose or Task content is never passed to the
        runtime or exported as telemetry).

        Concurrency: the dispatch deliberately does not take this adapter's
        per-session send/reconstruction lock — an abort must reach the
        backend while an ordinary or verified-PR-review ``send`` is still in
        flight, with other sessions unaffected. A short-lived per-session
        coordination gate orders the abort against the serialized
        ``read_messages`` → ``stop`` → ``start`` reconstruction section: a
        reconstruction that already entered its incarnation-change section
        makes the abort decline locally with a safe known pre-dispatch
        unavailability (never a queued interruption of a possibly new
        incarnation), and a reconstruction refuses — before its first
        ``read_messages``/``stop`` effect — while any abort of the session
        is outstanding, acknowledged-but-unreconciled, or outcome-uncertain.
        Outstanding aborts are counted per session, so one abort's known
        pre-effect failure can never release another concurrent abort's
        unresolved intent.

        A normal return means the abort request was acknowledged — not that
        the active turn is fully quiescent, that an in-flight ``send``
        definitively failed, or that a later workflow result cannot arrive.
        An acknowledged or uncertain interruption stays locally unresolved
        for #148's reconciliation through public runtime state; no
        ``resume()`` operation exists here, and any subsequent authorized
        interaction uses the existing exact-session send path. Caller
        authorization and Task/Execution consequences stay entirely with the
        application layer.
        """
        with application_span(_TRACER_SCOPE, "cline_adapter.abort_activity") as span:
            annotate_span(span, operation="cline_adapter.abort_activity")
            if not isinstance(session_id, str) or not session_id.strip():
                raise ValueError("session_id must be a non-empty string")
            self._resolve_bound_context(session_id)
            with self._abort_coordination:
                if session_id in self._reconstruction_active:
                    raise AgentRuntimeUnavailableError(
                        "the session's runtime incarnation is changing; the "
                        "activity abort is declined before dispatch"
                    )
                guard = self._abort_guards.get(session_id)
                if guard is None:
                    guard = _AbortGuard()
                    self._abort_guards[session_id] = guard
                guard.outstanding += 1
                if guard.state is _AbortState.NONE:
                    guard.state = _AbortState.PENDING
            try:
                self._backend_operation(
                    "abort",
                    lambda: self._backend.abort(session_id),
                    session_id=session_id,
                )
            except AgentRuntimeError as failure:
                self._settle_abort(session_id, failure)
                raise
            self._settle_abort(session_id, None)

    def _settle_abort(self, session_id: str, failure: AgentRuntimeError | None) -> None:
        """Settle one outstanding abort against the per-session guard (D6a).

        Acknowledgement and uncertainty are sticky: an acknowledged abort
        stays locally unresolved until D6b's settlement, and an uncertain
        abort keeps both its guard state and the abort-kind uncertain-effect
        entry recorded by ``_backend_operation``. D6b's ``settle_abort``
        clears both atomically with positive caller-attested evidence. Only
        a ``PENDING``
        intent with no other outstanding abort may return to ``NONE`` after
        a known pre-effect failure or positive session-not-found outcome —
        the operation verifiably never reached the runtime, so the D5a
        baseline reconstruction safety applies again. No clearing or retry
        procedure exists beyond this.
        """
        with self._abort_coordination:
            guard = self._abort_guards.get(session_id)
            if guard is None:  # pragma: no cover - registration precedes settlement
                return
            guard.outstanding -= 1
            if failure is None:
                if guard.state is not _AbortState.UNCERTAIN:
                    guard.state = _AbortState.ACKNOWLEDGED
            elif isinstance(failure, AgentRuntimeUncertainOutcomeError):
                guard.state = _AbortState.UNCERTAIN
            elif guard.outstanding == 0 and guard.state is _AbortState.PENDING:
                guard.state = _AbortState.NONE

    def reconstruct_session(
        self,
        session_id: str,
        *,
        target_mode: ClineConstructionMode,
    ) -> None:
        """Reconstruct the exact existing bound external session (D5a seam).

        The Cline-local same-ID reconstruction seam, reusable by D6b recovery:
        rebuild the addressed external session into the caller-authorized
        target construction mode with the resolver-resolved current
        construction context and phase-selected working directory, preserving
        the exact external session identity and the full raw transcript. The
        serialized sequence on the pinned backend is ``read_messages`` →
        ``stop`` (incarnation release, not abort) → ``start`` carrying the
        identical session ID and the raw persisted message array verbatim;
        the start must return the identical session ID or the reconstruction
        fails closed. No initialization resend, no synthesized continuation or
        mode prose, no summarization or projection, no reliance on stale
        native ``metadata.mode``, and no session substitution. The requested
        mode is an input of this invocation, never adapter-persisted state:
        reapplying the same construction parameters is valid.

        Serialization and safety: runtime-incarnation changes serialize
        against this adapter's own same-session sends/controls; a session
        whose last interaction ended with an uncertain outcome is refused
        (fail closed) — a concurrent in-flight send is awaited by the
        serialization, an uncertain one is never silently displaced, and an
        unresolved D6a activity interruption (an outstanding,
        acknowledged-but-unreconciled, or outcome-uncertain abort) is
        likewise refused before the first ``read_messages``/``stop`` effect,
        so reconstruction never displaces the possibly active work an abort
        was requested to interrupt. The
        bound Task/role/Connection context and the construction inputs come
        from the injected resolver's durable binding resolution; identity
        disagreement with this instance's memory fails closed. The caller
        owns the authorization to reconstruct and the workflow consequences.
        """
        with application_span(_TRACER_SCOPE, "cline_adapter.reconstruct_session") as span:
            annotate_span(span, operation="cline_adapter.reconstruct_session")
            if not isinstance(session_id, str) or not session_id.strip():
                raise ValueError("session_id must be a non-empty string")
            if not isinstance(target_mode, ClineConstructionMode):
                raise ValueError("target_mode must be a ClineConstructionMode")
            context = self._resolve_bound_context(session_id)
            identity = context.identity
            reviewer_forbidden = (
                identity.role is WorkflowRole.REVIEWER
                and target_mode is not ClineConstructionMode.PLAN
            )
            if reviewer_forbidden:
                raise ValueError(
                    "the reviewer session remains native PLAN for the Task "
                    "lifetime; reconstruction into ACT is refused"
                )
            _require_construction_inputs(context)
            self._run_same_id_reconstruction(
                session_id,
                context,
                target_mode,
                tolerate_positive_stop_absence=False,
            )

    # --- D6b: interruption reconciliation and recovery (issue #148) ---

    def reconcile_session(
        self,
        session_id: str,
        *,
        checkpoint: ClineDispatchCheckpoint | None = None,
    ) -> ClineReconciliationResult:
        """Reconcile the exact bound session through public state (D6b).

        The caller-invoked Cline-local interruption-reconciliation
        operation: after a local bridge/client/transport interruption, a
        Cline activity abort, or a managed Hub restart — once the
        caller/wiring layer has independently restored the D4 backend
        attachment — this operation inspects the addressed exact external
        session through the qualified public reads (``get``,
        ``read_messages``, and, when the transcript read positively
        reports absence, one ``list_history`` enumeration to disambiguate
        presence) and returns a typed, conservative continuity
        classification with bounded, content-free observations
        (:mod:`openorc.adapters.cline.reconciliation`).

        Preconditions and discipline:

        - The injected backend is consumed as-is: this operation performs
          no ``connect`` and never replaces, restarts, or manages transport
          state. An absent or not-ready attachment fails closed through
          the existing normalized unavailability classification — never a
          session-loss or replay claim.
        - The exact Task/role/Connection binding is resolved through the
          injected resolver before any read; a missing, mismatched, or
          otherwise unestablishable binding fails closed as a
          configuration rejection and is never evidence that the Cline
          transcript is gone. This instance's ``_known_identity`` memory
          is never authority.
        - The reads are pure observations: they take no per-session
          send/reconstruction lock (they may observe in-flight work) and
          can never latch an uncertain-effect guard — a read cannot take
          an external effect. Attachment/transport failures raise the
          existing normalized errors; evidence-shaped answers (including
          positive session-not-found) classify into the typed result.
        - A caller-supplied :class:`ClineDispatchCheckpoint` derives only
          bounded, clearly identified new-turn observations. Message
          presence beyond it never proves that an interrupted logical
          send completed; no formal output is ever reconstituted from
          record status or agent prose, and the delivery/currentness
          decision stays with the caller's durable dispatch identity.
        - Raw transcripts stay transient inside this call and never enter
          the typed result, errors, logs, spans, or telemetry. The span
          carries only the safe fixed vocabulary (operation plus the
          resolved binding's stable IDs).
        """
        with application_span(_TRACER_SCOPE, "cline_adapter.reconcile_session") as span:
            annotate_span(span, operation="cline_adapter.reconcile_session")
            if not isinstance(session_id, str) or not session_id.strip():
                raise ValueError("session_id must be a non-empty string")
            if checkpoint is not None and not isinstance(checkpoint, ClineDispatchCheckpoint):
                raise ValueError("checkpoint must be a ClineDispatchCheckpoint when supplied")
            identity = self._resolve_bound_context(session_id).identity
            annotate_span(
                span,
                workspace_id=str(identity.workspace_id),
                task_id=str(identity.task_id),
                workflow_role=identity.role.value,
            )
            record_present: bool | None
            try:
                record = self._backend_operation(
                    "get",
                    lambda: self._backend.get(session_id),
                    session_id=session_id,
                )
            except AgentSessionNotFoundError:
                record_present = False
            else:
                record_present = record is not None
            transcript: list[JsonObject] | None
            transcript_absent = False
            try:
                transcript = self._backend_operation(
                    "read_messages",
                    lambda: self._backend.read_messages(session_id),
                    session_id=session_id,
                )
            except AgentSessionNotFoundError:
                transcript = None
                transcript_absent = True
            history_presence: bool | None = None
            if transcript_absent:
                history = self._backend_operation(
                    "list_history",
                    lambda: self._backend.list_history(),
                )
                history_presence = _history_presence(history, session_id)
            usage_observed: bool | None = None
            if transcript is not None:
                try:
                    usage = self._backend_operation(
                        "get_accumulated_usage",
                        lambda: self._backend.get_accumulated_usage(session_id),
                        session_id=session_id,
                    )
                except AgentRuntimeError:
                    # Optional supporting observation only: a failed usage
                    # read never fails the reconciliation and never
                    # establishes or denies anything.
                    usage_observed = None
                else:
                    usage_observed = usage is not None
            result = classify_session_evidence(
                ClineSessionEvidence(
                    record_present=record_present,
                    transcript=transcript,
                    transcript_absent=transcript_absent,
                    history_presence=history_presence,
                    # The pinned #71 backend contract makes no exhaustive
                    # public-history guarantee, so enumeration absence is
                    # never terminal-loss proof today; a future documented
                    # contract would assert this deliberately.
                    history_exhaustive=False,
                ),
                checkpoint=checkpoint,
            )
            return replace(result, usage_observed=usage_observed)

    def settle_uncertain_send(
        self,
        session_id: str,
        *,
        runtime_work_settled: bool,
        operation_accounted_for: bool,
    ) -> ClineSettlementOutcome:
        """Release the session's send-kind uncertainty latch (D6b).

        The explicit, caller-driven settlement of D5a's uncertain-send
        protection: the only way this adapter clears a session's
        ``send``-kind unresolved uncertain effect. Nothing else does — not
        a reconnect, not a succeeding read, not an acknowledged abort, and
        never the absence of a process-local latch after a worker or
        bridge restart, which is not settlement evidence.

        Caller preconditions (documented, fail closed): the caller must
        hold positive, durable evidence that the interrupted runtime work
        has settled or cannot still execute (``runtime_work_settled``) and
        that the applicable logical dispatch has been accounted for by its
        owning caller (``operation_accounted_for``) — the durable
        pending-operation/currentness facts belong to the later dispatch
        reconciliation layer, never to this adapter. An unsatisfied
        attestation raises before any effect. The attestation covers every
        outstanding interrupted send of this session's ``send`` kind: the
        per-kind latch does not distinguish individual sends, so settling
        with a still-unaccounted interrupted send outstanding is a caller
        authority error this seam cannot detect.

        Concurrency: settlement serializes against this adapter's own
        same-session sends/reconstructions (send/reconstruction lock
        first), then takes the short coordination lock for the final
        recheck — any outstanding abort, unresolved acknowledged or
        uncertain abort, or active reconstruction is a veto that keeps
        every guard latched (typed refusal). Only the ``send`` kind is
        released; unrelated unresolved kinds stay latched. Independent
        sessions are unaffected.
        """
        with application_span(_TRACER_SCOPE, "cline_adapter.settle_uncertain_send") as span:
            annotate_span(span, operation="cline_adapter.settle_uncertain_send")
            if not isinstance(session_id, str) or not session_id.strip():
                raise ValueError("session_id must be a non-empty string")
            if not runtime_work_settled or not operation_accounted_for:
                raise ValueError(
                    "settlement requires positive caller evidence that the "
                    "interrupted runtime work has settled or cannot still "
                    "execute and that the logical dispatch is accounted for"
                )
            identity = self._resolve_bound_context(session_id).identity
            annotate_span(
                span,
                workspace_id=str(identity.workspace_id),
                task_id=str(identity.task_id),
                workflow_role=identity.role.value,
            )
            with self._lock_for(session_id), self._abort_coordination:
                guard = self._abort_guards.get(session_id)
                if guard is not None and guard.outstanding > 0:
                    return ClineSettlementOutcome(
                        settled=False,
                        refusal=ClineSettlementRefusal.OUTSTANDING_ABORT,
                    )
                if guard is not None and guard.state is not _AbortState.NONE:
                    return ClineSettlementOutcome(
                        settled=False,
                        refusal=ClineSettlementRefusal.UNRESOLVED_ABORT,
                    )
                if session_id in self._reconstruction_active:
                    return ClineSettlementOutcome(
                        settled=False,
                        refusal=ClineSettlementRefusal.RECONSTRUCTION_ACTIVE,
                    )
                effects = self._uncertain_effects.get(session_id)
                if effects is None or "send" not in effects:
                    return ClineSettlementOutcome(
                        settled=False,
                        refusal=ClineSettlementRefusal.NOTHING_TO_SETTLE,
                    )
                effects.discard("send")
                if not effects:
                    self._uncertain_effects.pop(session_id, None)
            return ClineSettlementOutcome(settled=True)

    def settle_abort(
        self,
        session_id: str,
        *,
        runtime_work_settled: bool,
        operation_accounted_for: bool,
    ) -> ClineSettlementOutcome:
        """Settle the session's activity-interruption guard (D6b).

        The explicit, caller-driven settlement of D6a's sticky
        ``ACKNOWLEDGED``/``UNCERTAIN`` abort guards and of the abort-kind
        uncertain effect an uncertain abort records: the only way this
        adapter clears a session's activity-interruption protection, and
        both are cleared atomically in one settlement. Nothing else does —
        not a reconnect, not the abort acknowledgement itself, not a
        succeeding read, and never the absence of a process-local latch
        after a worker or bridge restart, which is not settlement
        evidence.

        Caller preconditions (documented, fail closed): the caller must
        hold positive, durable evidence that the interrupted runtime work
        has settled or cannot still execute (``runtime_work_settled``)
        and that the applicable logical control has been accounted for by
        its owning caller (``operation_accounted_for``) — the durable
        pending-operation/currentness facts belong to the later dispatch
        reconciliation layer, never to this adapter. An unsatisfied
        attestation raises before any effect.

        Concurrency: settlement serializes against this adapter's own
        same-session sends/reconstructions (send/reconstruction lock
        first), then takes the short coordination lock for the final
        recheck — any outstanding abort or active reconstruction is a veto
        that keeps every guard latched (typed refusal); an unresolved
        concurrent abort can never be settled away. Only the
        abort-kind protection is released; unrelated unresolved kinds
        (for example an uncertain send) stay latched. Independent
        sessions are unaffected.
        """
        with application_span(_TRACER_SCOPE, "cline_adapter.settle_abort") as span:
            annotate_span(span, operation="cline_adapter.settle_abort")
            if not isinstance(session_id, str) or not session_id.strip():
                raise ValueError("session_id must be a non-empty string")
            if not runtime_work_settled or not operation_accounted_for:
                raise ValueError(
                    "settlement requires positive caller evidence that the "
                    "interrupted runtime work has settled or cannot still "
                    "execute and that the logical control is accounted for"
                )
            identity = self._resolve_bound_context(session_id).identity
            annotate_span(
                span,
                workspace_id=str(identity.workspace_id),
                task_id=str(identity.task_id),
                workflow_role=identity.role.value,
            )
            with self._lock_for(session_id), self._abort_coordination:
                guard = self._abort_guards.get(session_id)
                if guard is not None and guard.outstanding > 0:
                    return ClineSettlementOutcome(
                        settled=False,
                        refusal=ClineSettlementRefusal.OUTSTANDING_ABORT,
                    )
                if session_id in self._reconstruction_active:
                    return ClineSettlementOutcome(
                        settled=False,
                        refusal=ClineSettlementRefusal.RECONSTRUCTION_ACTIVE,
                    )
                effects = self._uncertain_effects.get(session_id)
                guard_unresolved = guard is not None and guard.state is not _AbortState.NONE
                if (effects is None or "abort" not in effects) and not guard_unresolved:
                    return ClineSettlementOutcome(
                        settled=False,
                        refusal=ClineSettlementRefusal.NOTHING_TO_SETTLE,
                    )
                if effects is not None and "abort" in effects:
                    effects.discard("abort")
                    if not effects:
                        self._uncertain_effects.pop(session_id, None)
                if guard_unresolved and guard is not None:
                    guard.state = _AbortState.NONE
            return ClineSettlementOutcome(settled=True)

    def recover_session(
        self,
        session_id: str,
        *,
        target_mode: ClineConstructionMode,
    ) -> None:
        """Recover the exact bound session after a qualified restart (D6b).

        The authorized Hub-restart recovery: rebuild the addressed exact
        external session into the caller-selected effective mode through
        D5a's single same-ID reconstruction seam, preserving the exact
        external session identity and the full raw transcript. The
        workflow selects the authorized effective mode (the Reviewer is
        always PLAN; the Producer's ACT/PLAN phase selection is the
        caller's authority) and the resolver's durable binding supplies
        the current construction context — including the original
        repository base and Task checkout for Producer recovery, never a
        newly edited live Connection default. No initialization resend,
        no ``session_ready`` re-dispatch, no synthesized continuation
        notice, no replacement session, and no second reconstruction
        mechanism exists.

        Preconditions (documented, fail closed):

        - The caller/wiring layer has already restored the D4 backend
          attachment with current attach credentials; this operation
          performs no ``connect`` and no transport management.
        - Any latched uncertainty has been settled by the caller with its
          durable facts (``settle_uncertain_send``/``settle_abort``): an
          unresolved uncertain effect or activity-interruption guard
          refuses the recovery before the first effect, so recovery never
          displaces possibly active work.
        - The exact persisted transcript must be positively established
          inside the serialized section before any ``stop``/``start``: an
          empty or absent read on an established bound session refuses
          the recovery (inadequate evidence, never permission to
          reinitialize).

        The narrow qualified-restart adjustment stays inside the same
        reconstruction seam: when the serialized ``stop`` fails with a
        positively identified session absence after the transcript was
        just read, the recovery proceeds directly to the same-ID
        ``start`` — an ambiguous, uncertain, or unavailable ``stop``
        failure never permits a start. A wrong returned start ID always
        fails closed. Genuine exact-context loss is surfaced to the
        existing session-lifecycle/blocking authority through the
        normalized taxonomy; this adapter never mutates Task or
        ``TaskAgentSession`` state.
        """
        with application_span(_TRACER_SCOPE, "cline_adapter.recover_session") as span:
            annotate_span(span, operation="cline_adapter.recover_session")
            if not isinstance(session_id, str) or not session_id.strip():
                raise ValueError("session_id must be a non-empty string")
            if not isinstance(target_mode, ClineConstructionMode):
                raise ValueError("target_mode must be a ClineConstructionMode")
            context = self._resolve_bound_context(session_id)
            identity = context.identity
            annotate_span(
                span,
                workspace_id=str(identity.workspace_id),
                task_id=str(identity.task_id),
                workflow_role=identity.role.value,
            )
            self._check_reconstruction_mode(context, target_mode)
            self._run_same_id_reconstruction(
                session_id,
                context,
                target_mode,
                tolerate_positive_stop_absence=True,
            )

    # --- D6c: activity and usage observation (issue #164) ---

    def subscribe_activity(
        self,
        session_id: str,
        listener: Callable[[ClineActivityObservation], None],
    ) -> Subscription:
        """Subscribe to safe typed activity observations of the exact bound session (D6c).

        The narrow Cline-local observational subscription behind later
        authorized product projections: the attached backend's session
        events for the exact external session ID of a bound Task/role
        session — Producer or Reviewer alike — are projected into the
        immutable observations of :mod:`openorc.adapters.cline.activity`
        and delivered to the consumer listener. The consumer never receives
        the opaque SDK event payload.

        Preconditions and discipline:

        - The exact Task/role/Connection binding is resolved through the
          injected resolver before any backend effect; a missing or
          mismatched binding fails closed as a configuration rejection,
          and a fresh adapter instance can observe a durably bound session.
          This instance's ``_known_identity`` memory is never authority.
        - The already-attached D4 backend is consumed as-is: no
          ``connect``, no credential handling, no session allocation or
          reconstruction, no prompts, tools, aborts, or mode changes.
        - The subscription and its delivery take no per-session
          send/reconstruction lock: observations flow while a send is in
          flight or an activity abort is outstanding.
        - The subscription is addressed to the exact session, and every
          event's own ``session_id`` is verified again before projection,
          so different Producer/Reviewer sessions, Tasks, and Connections
          cannot cross-feed.
        - Delivery rides the backend's existing bounded event dispatch —
          no additional queue, thread, buffer, or replay. The projection is
          best-effort live observation: no historical replay, no
          exactly-once guarantee, and no recovery of a turn's lost
          mid-flight events. A disposed or replaced backend attachment
          invalidates its subscriptions; a later caller may explicitly
          subscribe again on an independently restored attachment.
        - Unsubscribe is explicit and idempotent through the returned
          handle; disposal never implies a workflow decision.

        Every observation is advisory runtime bookkeeping: no event,
        completion, or usage reading completes an OpenOrc Execution/Task,
        validates a review, grants Owner authority, settles an uncertain
        dispatch/abort, or classifies session loss. Missing telemetry
        proves nothing about session health; unknown stays a valid
        downstream representation.
        """
        with application_span(_TRACER_SCOPE, "cline_adapter.subscribe_activity") as span:
            annotate_span(span, operation="cline_adapter.subscribe_activity")
            if not isinstance(session_id, str) or not session_id.strip():
                raise ValueError("session_id must be a non-empty string")
            if not callable(listener):
                raise ValueError("listener must be callable")
            identity = self._resolve_bound_context(session_id).identity
            annotate_span(
                span,
                workspace_id=str(identity.workspace_id),
                task_id=str(identity.task_id),
                workflow_role=identity.role.value,
            )

            def _deliver(event: ClineSessionEvent) -> None:
                for observation in project_session_event(event, expected_session_id=session_id):
                    listener(observation)

            return self._backend_operation(
                "subscribe",
                lambda: self._backend.subscribe(session_id, _deliver),
                session_id=session_id,
            )

    def read_activity_snapshot(self, session_id: str) -> ClineActivitySnapshot:
        """Read the usage the runtime currently reports for the exact bound session (D6c).

        One public ``get_accumulated_usage`` read for the exact external
        session ID of a bound Task/role session, projected into the typed
        snapshot of :mod:`openorc.adapters.cline.activity`: the pinned
        ``SessionUsageSummary`` wrapper's ``usage`` (root/lead-agent
        totals) and ``aggregateUsage`` (totals including teammates and
        subagents) scopes, preserved distinctly and never substituted for
        one another. An absent or unusable scope is unknown (``None``),
        never an invented number, and a root-only total is never presented
        as the Task-session aggregate.

        The read is a pure observation on the already-attached backend: it
        resolves the durable binding first and fails closed before any
        backend effect, takes no per-session send/reconstruction lock,
        never latches an uncertain-effect guard, and never reconnects,
        replaces, or manages transport state. Attachment and transport
        failures raise the existing normalized errors; positive
        exact-session absence raises ``AgentSessionNotFoundError``.

        The result is runtime-observed bookkeeping within the current
        Cline Hub lifetime: a Hub restart may reset it even when the
        conversation survives, successive reads are independent
        observations rather than a lifetime ledger, and usage is never
        evidence of delivery, continuity, success, or Task progress.
        """
        with application_span(_TRACER_SCOPE, "cline_adapter.read_activity_snapshot") as span:
            annotate_span(span, operation="cline_adapter.read_activity_snapshot")
            if not isinstance(session_id, str) or not session_id.strip():
                raise ValueError("session_id must be a non-empty string")
            identity = self._resolve_bound_context(session_id).identity
            annotate_span(
                span,
                workspace_id=str(identity.workspace_id),
                task_id=str(identity.task_id),
                workflow_role=identity.role.value,
            )
            raw = self._backend_operation(
                "get_accumulated_usage",
                lambda: self._backend.get_accumulated_usage(session_id),
                session_id=session_id,
            )
            usage, aggregate_usage = project_usage_summary(raw)
            return ClineActivitySnapshot(
                session_id=session_id,
                usage=usage,
                aggregate_usage=aggregate_usage,
            )

    def _check_reconstruction_mode(
        self,
        context: ClineConstructionContext,
        target_mode: ClineConstructionMode,
    ) -> None:
        """Fail closed on a role-forbidden mode or absent construction inputs.

        Shared by ``reconstruct_session`` and D6b's ``recover_session``:
        the Reviewer session remains native PLAN for the Task lifetime,
        and the resolver-resolved construction inputs must be complete
        before any runtime effect.
        """
        identity = context.identity
        reviewer_forbidden = (
            identity.role is WorkflowRole.REVIEWER and target_mode is not ClineConstructionMode.PLAN
        )
        if reviewer_forbidden:
            raise ValueError(
                "the reviewer session remains native PLAN for the Task "
                "lifetime; reconstruction into ACT is refused"
            )
        _require_construction_inputs(context)

    def _run_same_id_reconstruction(
        self,
        session_id: str,
        context: ClineConstructionContext,
        target_mode: ClineConstructionMode,
        *,
        tolerate_positive_stop_absence: bool,
    ) -> None:
        """Run the serialized same-ID reconstruction section (D5a seam).

        The single same-external-ID reconstruction mechanism, shared by
        ``reconstruct_session`` and D6b's ``recover_session``: under the
        per-session send/reconstruction lock, refuse while any unresolved
        uncertain effect kind or activity-interruption guard remains,
        register the reconstruction against concurrent aborts, then
        execute the qualified ``read_messages`` → ``stop`` (incarnation
        release, not abort) → ``start`` sequence with the identical
        session ID and the raw persisted message array verbatim. The
        start must return the identical session ID or the reconstruction
        fails closed; no initialization resend, no synthesized
        continuation or mode prose, and no session substitution.

        ``tolerate_positive_stop_absence`` is D6b's narrow qualified-
        restart adjustment, staying inside this one seam: only the
        recovery entrypoint sets it, and it changes exactly one thing —
        a ``stop`` that fails with a positively identified session
        absence (``AgentSessionNotFoundError``, a known pre-effect
        outcome proving the session is not live on this runtime) is
        accepted as the absent-incarnation evidence after the surviving
        transcript was just read in this same section, so the authorized
        recovery proceeds directly to the same-ID ``start``. Any
        ambiguous, uncertain, or unavailable ``stop`` failure never
        permits a start, with or without the flag.
        """
        with self._lock_for(session_id):
            if self._uncertain_effects.get(session_id):
                raise AgentRuntimeUncertainOutcomeError(
                    "an earlier interaction on this session ended with an "
                    "uncertain outcome; same-ID reconstruction safety cannot "
                    "be established"
                )
            try:
                with self._abort_coordination:
                    self._reconstruction_active.add(session_id)
                    guard = self._abort_guards.get(session_id)
                    if guard is not None and guard.state is not _AbortState.NONE:
                        raise AgentRuntimeUncertainOutcomeError(
                            "an activity interruption of this session is "
                            "unresolved; same-ID reconstruction safety cannot "
                            "be established"
                        )
                messages = self._backend_operation(
                    "read_messages",
                    lambda: self._backend.read_messages(session_id),
                    session_id=session_id,
                )
                if tolerate_positive_stop_absence and not messages:
                    # Recovery precondition: the exact persisted transcript
                    # must be positively established before a same-ID start.
                    # An empty read on an established bound session is
                    # inadequate evidence, never permission to reinitialize.
                    raise AgentRuntimeUncertainOutcomeError(
                        "the persisted transcript of the addressed session "
                        "could not be positively established; same-ID "
                        "recovery is refused"
                    )
                try:
                    self._backend_operation(
                        "stop",
                        lambda: self._backend.stop(session_id),
                        session_id=session_id,
                    )
                except AgentSessionNotFoundError:
                    if not tolerate_positive_stop_absence:
                        raise
                    # Positive absence: no live incarnation addresses this
                    # session on the attached runtime. The surviving
                    # transcript was just read in this section and no
                    # unresolved work remains (checked under the locks
                    # above), so the authorized recovery proceeds directly
                    # to the same-ID start. An ambiguous failed stop never
                    # reaches this path.
                start_request = _build_start_request(
                    context,
                    mode=target_mode,
                    session_id=session_id,
                    initial_messages=messages,
                )
                result = self._backend_operation(
                    "start",
                    lambda: self._backend.start(start_request),
                    session_id=session_id,
                )
                if result.session_id != session_id:
                    raise AgentRuntimeConfigurationRejectedError(
                        "the same-ID reconstruction did not rebind the "
                        "requested exact external session identity"
                    )
            finally:
                with self._abort_coordination:
                    self._reconstruction_active.discard(session_id)
            self._known_identity[session_id] = context.identity
