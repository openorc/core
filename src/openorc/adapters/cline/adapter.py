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
"""

from __future__ import annotations

import enum
import threading
from collections.abc import Callable
from dataclasses import dataclass
from typing import Final, TypeVar

from openorc.adapters.agent_runtime.contract import AgentRuntimeAdapter
from openorc.adapters.agent_runtime.errors import (
    AgentRuntimeConfigurationRejectedError,
    AgentRuntimeDeliveryUncertainError,
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
from openorc.adapters.cline.construction import (
    ClineConstructionContext,
    ClineConstructionContextResolver,
    ClineSessionIdentity,
    resolve_role_markdown,
)
from openorc.adapters.cline.contract import ClineSdkBackend
from openorc.adapters.cline.errors import (
    ClineBackendUncertainOutcomeError,
    ClineBridgeProtocolError,
    ClineRemoteAttachmentRejectedError,
    ClineRemoteAttachmentUnavailableError,
    ClineSdkBackendError,
    ClineSdkOperationRejectedError,
    ClineSessionNotFoundError,
)
from openorc.adapters.cline.review_subject import (
    UNREADABLE_EVIDENCE,
    ClineReviewSubjectUnverifiedError,
    verify_pr_review_turn,
)
from openorc.adapters.cline.values import ClineConstructionMode, ClineStartRequest, JsonObject
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

# Safe machine classifications of the qualified baseline's connection-level
# SDK rejection codes (R2b/R4). They classify transport state only — never
# credentials, never exact-session loss.
_HUB_CONNECT_FAILED: Final = "hub_connect_failed"
_HUB_CONNECTION_CLOSED: Final = "hub_connection_closed"

# D3's bridge wire-protocol rejection for operations attempted before a
# successful attachment: positively pre-effect unavailability.
_NOT_ATTACHED: Final = "not_attached"

# Operations whose effect is an external session allocation, an incarnation
# change, or an activity interruption: a connection-level breakage during
# them leaves the effect unknown.
_EFFECTFUL_OPERATIONS: Final[frozenset[str]] = frozenset({"start", "send", "stop", "abort"})


def _classify_backend_failure(error: ClineSdkBackendError, *, operation: str) -> AgentRuntimeError:
    """Translate one #71 backend failure into the normalized #67 taxonomy.

    Documented safe classification grounded in the D2 taxonomy and the
    qualified baseline (R2b/R3/R4). The backend error object itself is
    discarded by the caller after this classification; only these safe
    machine-identifier messages survive. Known pre-effect classifications
    are preserved only where positive code/phase evidence supports them:
    the two qualified hub transport codes classify attachment state, and a
    pure read can never take an external effect. Any other SDK machine code
    is forwarded by the pinned bridge without establishing whether the
    operation took effect, so an unproven effectful ``start``/``send``/
    ``stop`` outcome is conservatively uncertain — never a known
    pre-effect failure, never a safe retry. Deliberate non-inferences: bad
    credentials are never inferred from ``hub_connect_failed`` alone,
    exact-session loss is never inferred from connection loss, and an
    uncertain ``start``/``send``/``stop`` is never downgraded to a safe
    retry. The backend taxonomy currently carries no positive timeout
    marker, so every uncertain backend outcome maps to delivery uncertainty.
    """
    if isinstance(error, ClineSessionNotFoundError):
        return AgentSessionNotFoundError(
            "the exact addressed external session is not live on this runtime"
        )
    if isinstance(error, ClineBackendUncertainOutcomeError):
        return AgentRuntimeDeliveryUncertainError(
            f"the {operation} outcome is unknown whether it took effect"
        )
    if isinstance(error, ClineRemoteAttachmentRejectedError):
        return AgentRuntimeConfigurationRejectedError(
            f"the runtime rejected the {operation} attachment configuration"
        )
    if isinstance(error, ClineRemoteAttachmentUnavailableError):
        return AgentRuntimeUnavailableError(
            f"the runtime was unreachable for the {operation} before any known effect"
        )
    if isinstance(error, ClineBridgeProtocolError):
        if error.reason == _NOT_ATTACHED:
            return AgentRuntimeUnavailableError(
                f"the bridge attachment was not established for the {operation}"
            )
        return AgentRuntimeConfigurationRejectedError(
            f"the bridge wire protocol rejected the {operation} ({error.reason!r})"
        )
    if isinstance(error, ClineSdkOperationRejectedError):
        if error.code == _HUB_CONNECT_FAILED:
            return AgentRuntimeUnavailableError(
                f"the remote hub could not be reached for the {operation} ({error.code!r})"
            )
        if error.code == _HUB_CONNECTION_CLOSED:
            if operation in _EFFECTFUL_OPERATIONS:
                return AgentRuntimeUncertainOutcomeError(
                    f"the connection closed around the {operation}; its effect cannot be classified"
                )
            return AgentRuntimeUnavailableError(
                f"the connection closed before the {operation} could be served ({error.code!r})"
            )
        # Any other code is forwarded by the pinned bridge without
        # establishing whether the operation took effect. For effectful
        # operations there is no positive evidence of non-effect, so the
        # outcome stays conservatively uncertain (and latches same-ID
        # reconstruction); a pure read can never take an external effect,
        # so its answered-but-unusable response is a transport-class failure.
        if operation in _EFFECTFUL_OPERATIONS:
            return AgentRuntimeUncertainOutcomeError(
                f"the {operation} was rejected with an unproven effect ({error.code!r})"
            )
        return AgentRuntimeTransportError(
            f"the {operation} read was answered without a usable response ({error.code!r})"
        )
    raise AssertionError(  # pragma: no cover - the D2 taxonomy is closed
        f"unclassified cline backend failure: {type(error).__name__}"
    )


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
        # Sessions whose last interaction ended with an uncertain outcome:
        # same-ID reconstruction is refused until the caller's reconciliation
        # work re-establishes safety. No automatic clearing exists.
        self._uncertain_sessions: set[str] = set()
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
        uncertain outcome on a session-addressed operation latches that
        session against reconstruction.
        """
        failure: AgentRuntimeError | None = None
        try:
            return invoke()
        except ClineSdkBackendError as error:
            failure = _classify_backend_failure(error, operation=operation)
            if session_id is not None and isinstance(failure, AgentRuntimeUncertainOutcomeError):
                self._uncertain_sessions.add(session_id)
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
        stays locally unresolved until #148's reconciliation, and an
        uncertain abort keeps both its guard state and the D5a uncertain
        latch already applied by ``_backend_operation``. Only a ``PENDING``
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
            with self._lock_for(session_id):
                if session_id in self._uncertain_sessions:
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
                    self._backend_operation(
                        "stop",
                        lambda: self._backend.stop(session_id),
                        session_id=session_id,
                    )
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
                self._known_identity[session_id] = identity
