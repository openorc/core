"""OAuth start-to-callback correlation service for the Profile-scoped GitHub
user authorization (issue #373).

The secure browser/API flow layer the #142 establishment service deliberately
excluded: the authenticated Owner starts an authorization (or reauthorization)
flow, GitHub redirects the provider's browser answer to OpenOrc's unauthenticated
provider ingress, and the correlation is completed against the same Profile the
authenticated start bound it to — through the existing same-human establishment
proof. Supabase Auth remains the canonical sign-in/session boundary; the
Supabase/GitHub sign-in provider token is never consumed, copied, persisted, or
reused here.

Documented callback authentication strategy: the provider ingress route is an
unauthenticated browser-redirect transport. Its completion authority comes
exclusively from:

1. correlation state created only by the authenticated start operation and
   durably bound to the initiating Profile and the intended flow purpose;
2. atomic single-use consumption (the durable delete IS the replay barrier —
   a replayed, expired, or unknown state consumes nothing and completes
   nothing);
3. bounded expiry (an expired flow can never be consumed);
4. the #142 same-human binding proof — the GitHub account resolved
   authoritatively under the exchanged token must be the stable identity
   backing the bound Profile's trusted Supabase sign-in; and
5. the durable revocation fence — the flow's captured ``revocation_count``
   must still equal the durable count under the authorization row lock at
   the final installation, so any Owner revocation committed after the flow
   started (including an idempotent revoke of an already-revoked row)
   permanently defeats it.

Owner identity is never taken from a browser-supplied actor ID (none exists
in this design) and never from the state alone: the state only selects the
subject; the provider-side identity match is what permits completion. Wrong
human, replay, expiry, and revocation all fail closed with nothing usable
stored. No credential material — verifier, client secret, tokens, or
references — ever enters the authorize URL beyond the documented public
query parameters (client ID, state, S256 code challenge), the callback
response, logs, or telemetry.

Transaction discipline: the start composition (barrier, supersession, guard,
flow + verifier insert) and the consume are each ONE short transaction; the
completion's external GitHub/Auth calls run with NO database transaction
open; the final installation composes its own short transaction inside #142.
"""

from __future__ import annotations

import base64
import hashlib
import logging
import secrets as _secrets
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timedelta
from uuid import UUID

from openorc.adapters.github import (
    GitHubUserTokenClient,
    compose_user_authorization_url,
)
from openorc.adapters.supabase import SupabaseAuthAdminClient
from openorc.domain.github_user_authorization import (
    GitHubUserAuthorization,
    GitHubUserAuthorizationStatus,
)
from openorc.domain.github_user_authorization_flows import (
    GitHubUserAuthorizationFlowPurpose,
)
from openorc.observability import annotate_span, application_span
from openorc.persistence import (
    github_user_authorization_flows,
    github_user_authorizations,
)
from openorc.persistence.github_user_authorization_flows import (
    GitHubUserFlowVerifierReferenceError,
)
from openorc.persistence.pool import DatabasePool
from openorc.persistence.time import utc_now
from openorc.services.errors import (
    ApplicationError,
    ConflictError,
    IntegrationNotConfiguredError,
    InvalidCommandError,
    NotFoundError,
)
from openorc.services.github_user_authorization import (
    CONDITION_MISSING,
    GitHubUserAuthorizationUnavailableError,
    establish_github_user_authorization,
)
from openorc.services.github_user_authorization import (
    revoke_github_user_authorization as revoke_github_user_authorization_service,
)
from openorc.services.profile_lifecycle_guard import require_account_operational
from openorc.services.transaction_composition import composed_transaction

__all__ = [
    "CONDITION_UNAVAILABLE",
    "DEFAULT_FLOW_LIFETIME_SECONDS",
    "GitHubUserAuthorizationFlowStart",
    "GitHubUserAuthorizationFlowUnavailableError",
    "GitHubUserAuthorizationPendingFlow",
    "GitHubUserAuthorizationStatusRead",
    "abort_pending_github_user_authorization_flow",
    "complete_github_user_authorization_flow",
    "read_github_user_authorization_status",
    "revoke_profile_github_user_authorization",
    "start_github_user_authorization_flow",
]

logger = logging.getLogger(__name__)

_TRACER_SCOPE = "openorc.services.github_user_authorization_flows"
_START_SPAN_NAME = "github_user_authorization_flow.start"
_COMPLETE_SPAN_NAME = "github_user_authorization_flow.complete"

# --- typed conditions and projections ----------------------------------------

# The bounded usability window of one pending flow: long enough for the
# Owner to complete GitHub's consent step, short enough that a stolen
# correlation value expires quickly.
DEFAULT_FLOW_LIFETIME_SECONDS = 600

# PKCE code-verifier shape (RFC 7636): 43-128 characters from the unreserved
# set. OpenOrc generates 43-character token_urlsafe values; the shape check
# is the fail-closed self-check on the generator seam.
_PKCE_VERIFIER_MIN_CHARS = 43
_PKCE_VERIFIER_MAX_CHARS = 128
_PKCE_UNRESERVED_CHARS = frozenset(
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-._~"
)
# The correlation state is OpenOrc's own token_urlsafe value with the same
# charset discipline and a bounded length.
_STATE_MAX_CHARS = 128

# Callback input bounds (the transport additionally bounds them): GitHub
# authorization codes and correlation states are far smaller.
_CALLBACK_INPUT_MAX_CHARS = 2048

# Typed unavailable-flow condition carried by
# GitHubUserAuthorizationFlowUnavailableError. Safe vocabulary only — never a
# distinguishing oracle for unknown/expired/replayed states.
CONDITION_UNAVAILABLE = "unavailable"


class GitHubUserAuthorizationFlowUnavailableError(ApplicationError):
    """The pending GitHub authorization flow cannot be completed (issue #373).

    The correlation state is unknown, expired, or already consumed — one
    uniform, detail-free condition. The pending flow is single-use: the
    first consume deletes it durably, so any later presentation of the same
    state is this condition.
    """

    def __init__(self, *, condition: str, message: str) -> None:
        super().__init__(message)
        self.condition = condition


@dataclass(frozen=True, slots=True)
class GitHubUserAuthorizationFlowStart:
    """The safe projection of one started authorization flow.

    ``authorize_url`` is the documented GitHub authorize URL: the only place
    the correlation state and the S256 code challenge ever appear. The
    PKCE verifier, client secret, and every credential have no field here.
    """

    authorize_url: str
    purpose: GitHubUserAuthorizationFlowPurpose
    expires_at: datetime


@dataclass(frozen=True, slots=True)
class GitHubUserAuthorizationPendingFlow:
    """The safe projection of the Profile's pending flow."""

    purpose: GitHubUserAuthorizationFlowPurpose
    started_at: datetime
    expires_at: datetime


@dataclass(frozen=True, slots=True)
class GitHubUserAuthorizationStatusRead:
    """The safe status projection of the Profile's GitHub authorization.

    ``missing`` (never authorized), ``active``, and ``revoked`` are the
    durable currentness states — deliberately distinguishable. Presentation
    metadata (``github_login``) is never identity authority. The refresh
    credential's reference, the verifier, and the correlation state have no
    field here.
    """

    status: str
    github_user_id: int | None
    github_login: str | None
    authorized_at: datetime | None
    refresh_expires_at: datetime | None
    pending_flow: GitHubUserAuthorizationPendingFlow | None


# --- input validation helpers ------------------------------------------------


def _default_randomness() -> str:
    """Cryptographic randomness for the verifier and the correlation state."""
    return _secrets.token_urlsafe(32)


def _s256_code_challenge(verifier: str) -> str:
    """Derive the S256 PKCE code challenge from the verifier (RFC 7636)."""
    digest = hashlib.sha256(verifier.encode("ascii")).digest()
    return base64.urlsafe_b64encode(digest).rstrip(b"=").decode("ascii")


def _require_pkce_verifier(verifier: object) -> str:
    """Validate the generated verifier's RFC 7636 shape before any effect."""
    if (
        not isinstance(verifier, str)
        or not (_PKCE_VERIFIER_MIN_CHARS <= len(verifier) <= _PKCE_VERIFIER_MAX_CHARS)
        or not set(verifier) <= _PKCE_UNRESERVED_CHARS
    ):
        raise InvalidCommandError(
            "the generated PKCE code verifier does not have the required shape"
        )
    return verifier


def _require_generated_state(state: object) -> str:
    """Validate the generated correlation state before any durable effect."""
    if (
        not isinstance(state, str)
        or not state
        or len(state) > _STATE_MAX_CHARS
        or not set(state) <= _PKCE_UNRESERVED_CHARS
    ):
        raise InvalidCommandError(
            "the generated OAuth correlation state does not have the required shape"
        )
    return state


def _require_callback_state(state: object) -> str:
    """Validate one callback correlation-state input before any durable read.

    The transport bounds the raw values further; this is the service's own
    fail-closed shape check before the consume transaction.
    """
    if not isinstance(state, str) or not state or len(state) > _CALLBACK_INPUT_MAX_CHARS:
        raise InvalidCommandError(
            "the GitHub authorization flow callback state is not a usable correlation value"
        )
    return state


def _require_callback_code(code: object) -> str:
    """Validate one callback authorization-code input before any durable read."""
    if not isinstance(code, str) or not code or len(code) > _CALLBACK_INPUT_MAX_CHARS:
        raise InvalidCommandError(
            "the GitHub authorization code is not a usable value for this operation"
        )
    return code


# --- start -------------------------------------------------------------------


def start_github_user_authorization_flow(
    pool: DatabasePool,
    *,
    profile_id: UUID,
    purpose: GitHubUserAuthorizationFlowPurpose,
    user_flow_client_id: str,
    randomness: Callable[[], str] | None = None,
    clock: Callable[[], datetime] | None = None,
    flow_lifetime_seconds: int = DEFAULT_FLOW_LIFETIME_SECONDS,
) -> GitHubUserAuthorizationFlowStart:
    """Start one pending OAuth authorization (or reauthorization) flow.

    One short transaction: compose the account-operational barrier, supersede
    any prior pending flow (row + Vault verifier secret deleted together —
    at most one pending flow per Profile, so concurrent starts beyond the
    first resolve by the database's per-Profile constraint), apply the
    purpose's authority guard against the durable authorization row, capture
    the current ``revocation_count`` fence evidence (0 when no authorization
    exists), and install the new pending flow with its PKCE verifier in
    Vault. Concurrent starts beyond the first surface the per-Profile
    constraint as a typed conflict.

    No external call happens at start, so the external user-flow boundaries
    are deliberately absent from this signature — the transport composes and
    fail-closes them (the completion needs them; a deployment without the
    credentials refuses at the composition boundary). The guards are intent
    discrimination on the Owner's own durable state: ``authorize`` conflicts
    with an already-ACTIVE authorization (the supported action is
    reauthorization) and ``reauthorize`` conflicts with no authorization row
    (the supported action is authorization). A revoked row permits both
    purposes — re-establishment is the recovery path. The captured fence
    evidence, not these guards, is what fences the eventual completion
    against intervening revocations.

    Fail closed before any durable effect when the user-flow client ID is
    not configured, when the generated verifier/state does not have the
    required shape, or when the flow lifetime is not a positive integer.
    The returned projection carries the documented authorize URL — the only
    place the correlation state and the S256 code challenge ever appear.
    """
    with application_span(_TRACER_SCOPE, _START_SPAN_NAME) as span:
        annotate_span(span, operation=_START_SPAN_NAME)
        if not isinstance(user_flow_client_id, str) or not user_flow_client_id:
            raise IntegrationNotConfiguredError(
                "the GitHub App user-flow client ID is not configured for this process"
            )
        if (
            isinstance(flow_lifetime_seconds, bool)
            or not isinstance(flow_lifetime_seconds, int)
            or flow_lifetime_seconds <= 0
        ):
            raise InvalidCommandError("the authorization flow lifetime must be a positive integer")
        randomness = randomness if randomness is not None else _default_randomness
        now = clock if clock is not None else utc_now
        # Generation order is part of the seam contract: the first call is
        # the PKCE verifier, the second the correlation state.
        verifier = _require_pkce_verifier(randomness())
        state = _require_generated_state(randomness())
        challenge = _s256_code_challenge(verifier)
        expires_at = now() + timedelta(seconds=flow_lifetime_seconds)

        with composed_transaction(pool) as transaction_pool:
            require_account_operational(transaction_pool, profile_id=profile_id)
            try:
                github_user_authorization_flows.delete_pending_flow_with_verifier_secret(
                    transaction_pool, profile_id=profile_id
                )
            except GitHubUserFlowVerifierReferenceError as exc:
                logger.warning(
                    "github authorization flow start rejected an unrecognized "
                    "pending-flow verifier reference"
                )
                raise ConflictError(
                    "the pending authorization flow's credential reference could not be reconciled"
                ) from exc
            existing = github_user_authorizations.get_github_user_authorization(
                transaction_pool, profile_id=profile_id
            )
            if (
                purpose is GitHubUserAuthorizationFlowPurpose.AUTHORIZE
                and existing is not None
                and existing.status is GitHubUserAuthorizationStatus.ACTIVE
            ):
                raise ConflictError(
                    "an active GitHub authorization already exists for this "
                    "Profile; reauthorization is the supported action"
                )
            if purpose is GitHubUserAuthorizationFlowPurpose.REAUTHORIZE and existing is None:
                raise ConflictError(
                    "no GitHub authorization exists for this Profile to "
                    "reauthorize; authorization is the supported action"
                )
            observed_revocation_count = 0 if existing is None else existing.revocation_count
            flow = github_user_authorization_flows.create_pending_flow(
                transaction_pool,
                profile_id=profile_id,
                purpose=purpose,
                state=state,
                verifier=verifier,
                observed_revocation_count=observed_revocation_count,
                expires_at=expires_at,
            )
        authorize_url = compose_user_authorization_url(
            client_id=user_flow_client_id,
            state=flow.state,
            code_challenge=challenge,
        )
        return GitHubUserAuthorizationFlowStart(
            authorize_url=authorize_url,
            purpose=flow.purpose,
            expires_at=flow.expires_at,
        )


# --- completion --------------------------------------------------------------


def complete_github_user_authorization_flow(
    pool: DatabasePool,
    *,
    state: str,
    code: str,
    admin_client: SupabaseAuthAdminClient,
    token_client: GitHubUserTokenClient,
) -> GitHubUserAuthorization:
    """Complete one pending flow through the #142 establishment proof.

    Transaction 1: atomically consume the pending flow by its exact state
    (unknown/expired/replayed states are one uniform typed condition with no
    distinguishing oracle; a successful consume durably deletes the row and
    decrypts its Vault verifier in the same transaction). Then, with NO
    database transaction open, run the full #142 establishment against the
    bound Profile: the trusted Supabase GitHub-identity lookup, the code
    exchange forwarding the correlation's PKCE verifier, the authoritative
    GitHub current-user lookup, and the same-human binding comparison.

    The captured ``observed_revocation_count`` fences the final installation
    under the authorization row lock: any Owner revocation committed after
    the flow started — including an idempotent revoke of an already-revoked
    row — permanently defeats the completion with nothing stored, while a
    genuinely new post-revocation flow remains eligible.

    Every establishment outcome (definitive rejection, identity mismatch,
    uncertainty, or a fenced conflict) leaves the flow consumed: the
    correlation is spent, the caller's user retries from a fresh
    authenticated start. The typed establishment errors propagate unchanged.
    """
    with application_span(_TRACER_SCOPE, _COMPLETE_SPAN_NAME) as span:
        annotate_span(span, operation=_COMPLETE_SPAN_NAME)
        state = _require_callback_state(state)
        code = _require_callback_code(code)
        consumed = github_user_authorization_flows.consume_pending_flow(pool, state=state)
        if consumed is None:
            logger.warning(
                "a github user authorization flow completion was refused: the "
                "correlation state is unknown, expired, or already consumed"
            )
            raise GitHubUserAuthorizationFlowUnavailableError(
                condition=CONDITION_UNAVAILABLE,
                message=(
                    "the GitHub authorization flow cannot be completed: it is "
                    "unknown, expired, or already used"
                ),
            )
        flow, verifier = consumed
        return establish_github_user_authorization(
            pool,
            profile_id=flow.profile_id,
            code=code,
            admin_client=admin_client,
            token_client=token_client,
            code_verifier=verifier,
            expected_revocation_count=flow.observed_revocation_count,
        )


def abort_pending_github_user_authorization_flow(pool: DatabasePool, *, state: str) -> None:
    """Consume one pending flow the provider has already terminated.

    The callback arrived with a GitHub ``error`` answer (for example, the
    Owner denied access at the consent step): the correlation is deleted so
    it can never be completed later. No verifier resolution is needed; an
    unknown state is already the desired end state. This is a deliberate
    invalidation, not a completion — the caller reports a uniform failure
    outcome either way.
    """
    state = _require_callback_state(state)
    github_user_authorization_flows.discard_pending_flow(pool, state=state)


# --- status and revoke seams -------------------------------------------------


def read_github_user_authorization_status(
    pool: DatabasePool, *, profile_id: UUID
) -> GitHubUserAuthorizationStatusRead:
    """Project the Profile's GitHub authorization status safely.

    A durable read only: the currentness state (missing/active/revoked are
    deliberately distinguishable), the stable proven GitHub user ID, the
    presentation-only login, the current authorization/refresh-expiry
    instants, and the pending flow's purpose/window when one is outstanding.
    The refresh credential reference, the PKCE verifier, and the correlation
    state have no field in this projection.
    """
    authorization = github_user_authorizations.get_github_user_authorization(
        pool, profile_id=profile_id
    )
    pending = github_user_authorization_flows.get_pending_flow(pool, profile_id=profile_id)
    pending_flow = (
        None
        if pending is None
        else GitHubUserAuthorizationPendingFlow(
            purpose=pending.purpose,
            started_at=pending.started_at,
            expires_at=pending.expires_at,
        )
    )
    if authorization is None:
        return GitHubUserAuthorizationStatusRead(
            status="missing",
            github_user_id=None,
            github_login=None,
            authorized_at=None,
            refresh_expires_at=None,
            pending_flow=pending_flow,
        )
    return GitHubUserAuthorizationStatusRead(
        status=authorization.status.value,
        github_user_id=authorization.github_user_id,
        github_login=authorization.github_login,
        authorized_at=authorization.authorized_at,
        refresh_expires_at=authorization.refresh_expires_at,
        pending_flow=pending_flow,
    )


def revoke_profile_github_user_authorization(
    pool: DatabasePool, *, profile_id: UUID
) -> GitHubUserAuthorization:
    """Revoke the Profile's GitHub authorization through the #142 boundary.

    Thin composition over the existing revocation service: the transaction
    already invalidates the Profile's pending flow correlation, advances the
    revocation fence on every successful revoke (including the externally
    idempotent already-revoked case), and deletes the refresh secret for an
    active authorization. A missing authorization translates into the typed
    not-found vocabulary for this Owner-facing seam.
    """
    try:
        return revoke_github_user_authorization_service(pool, profile_id=profile_id)
    except GitHubUserAuthorizationUnavailableError as exc:
        if exc.condition == CONDITION_MISSING:
            raise NotFoundError("this Profile has no GitHub authorization to revoke") from exc
        raise
