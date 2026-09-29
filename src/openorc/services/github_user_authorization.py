"""Profile-scoped GitHub user authorization application service (issue #142).

The secure lifecycle boundary for OpenOrc acting on GitHub on behalf of the
same human Owner who signed in to OpenOrc. Supabase Auth remains the
canonical sign-in/session boundary; the Supabase/GitHub sign-in provider
token is identity-establishment material only and is never consumed, copied,
persisted, refreshed, or reused here — repository credentials come exclusively
from the separately authorized OpenOrc GitHub App user-to-server token.

Same-human binding proof: the verified JWT principal carries only the
Supabase Auth UUID, so establishment first recovers the stable GitHub
provider identity backing that exact authenticated Profile through the
trusted server-side Auth Admin boundary, then exchanges the (post-correlation)
authorization code, resolves the authorized GitHub account authoritatively
through ``GET /user``, and compares the stable numeric GitHub user IDs after
strict normalization. Zero, multiple, or malformed trusted GitHub identities,
and any ID mismatch, fail closed with nothing usable stored. Mutable login/
email/``user_metadata`` are never identity authority and the verified JWT
principal is never broadened with mutable provider presentation data.

Credential model: OpenOrc persists a refresh credential — never a long-lived
user access token. A definitive token answer without the expiring-token
capability fails closed as unsupported/misconfigured GitHub App behavior.
Raw tokens never enter ordinary ``openorc.*`` columns, DTOs, events, logs,
errors, or telemetry: the refresh credential is persisted only through the
#141 Vault boundary behind the purpose-specific opaque reference, and the
user access token lives only in the bounded in-memory cache below.

Refresh rotation and stale-write safety: GitHub refresh credentials rotate,
so persistence is guarded by the durable ``refresh_generation`` — read the
current generation, perform exactly one GitHub refresh exchange with NO
database transaction open, then in one short transaction compose the
account-operational barrier, lock/reload the authorization, and install the
rotated credential (Vault in-place update + expiry/generation increment)
only when the durable generation still matches and the row is still active.
A stale process can never overwrite a newer rotated credential: it discards
its returned token pair and reloads. A definitive refresh rejection fails
that resolution closed without durable mutation (durable state is reloaded
first so an already-committed concurrent rotation is observed); an
uncertain outcome is never replayed and never mutates durable state. There
is no installation-token fallback.

Every authorization mutation composes the account-wide Owner-mutation
barrier (``require_account_operational``) FIRST, before the authorization
row lock, so once account deletion claims the Profile no authorization
mutation can create or rotate a secret outside the account-deletion
cleanup set.
"""

from __future__ import annotations

import logging
import threading
from collections import OrderedDict
from collections.abc import Callable
from datetime import datetime
from typing import Protocol
from uuid import UUID

from openorc.adapters.github import (
    GitHubAuthenticationRejectedError,
    GitHubAuthorizationRejectedError,
    GitHubCurrentUser,
    GitHubOutcomeUncertainError,
    GitHubRequestRejectedError,
    GitHubUserAccessToken,
    GitHubUserRefreshSecret,
    GitHubUserTokenClient,
    GitHubUserTokenGrant,
    GitHubUserTokenRefreshCapabilityMissingError,
    GitHubUserTokenRejectedError,
)
from openorc.adapters.supabase import (
    SupabaseAuthAdminClient,
    SupabaseAuthAdminOutcomeUnknownError,
    SupabaseAuthAdminRejectedError,
    SupabaseAuthAdminUserAbsentError,
)
from openorc.domain.github_user_authorization import (
    GitHubUserAuthorization,
    GitHubUserAuthorizationStatus,
)
from openorc.observability import annotate_span, application_span
from openorc.persistence import github_user_authorizations, github_user_refresh_secrets
from openorc.persistence.github_user_refresh_secrets import (
    GitHubUserRefreshSecretReferenceError,
)
from openorc.persistence.pool import DatabasePool
from openorc.persistence.time import utc_now
from openorc.services.errors import (
    ApplicationError,
    ConflictError,
    ExternalOperationFailedError,
    ExternalOperationUncertainError,
    InvalidCommandError,
    StaleOperationError,
)
from openorc.services.profile_lifecycle_guard import require_account_operational
from openorc.services.transaction_composition import composed_transaction

__all__ = [
    "CONDITION_EXPIRED_UNREFRESHABLE",
    "CONDITION_MISSING",
    "CONDITION_REFRESH_CAPABILITY_UNAVAILABLE",
    "CONDITION_REFRESH_REJECTED",
    "CONDITION_REVOKED",
    "GitHubUserAccessTokenResolver",
    "GitHubUserAuthorizationUnavailableError",
    "GitHubUserIdentityMismatchError",
    "ProfileUserAccessTokenResolver",
    "SupabaseGitHubIdentityStateError",
    "establish_github_user_authorization",
    "normalize_strict_github_user_id",
    "revoke_github_user_authorization",
]

# Typed unusable-authorization conditions carried by
# GitHubUserAuthorizationUnavailableError. The condition is safe vocabulary:
# it never carries credential material or provider response content.
CONDITION_MISSING = "missing"
CONDITION_REVOKED = "revoked"
CONDITION_EXPIRED_UNREFRESHABLE = "expired_unrefreshable"
CONDITION_REFRESH_REJECTED = "refresh_rejected"
CONDITION_REFRESH_CAPABILITY_UNAVAILABLE = "refresh_capability_unavailable"

# Typed trusted-identity-state conditions carried by
# SupabaseGitHubIdentityStateError: the exact Supabase Auth user's GitHub
# identity backing could not be proven usable.
IDENTITY_CONDITION_MISSING = "github_identity_missing"
IDENTITY_CONDITION_AMBIGUOUS = "github_identity_ambiguous"
IDENTITY_CONDITION_MALFORMED = "github_identity_malformed"
IDENTITY_CONDITION_ACCOUNT_ABSENT = "account_absent"

# Strict normalization of the trusted Supabase GitHub provider_id fact: the
# canonical decimal rendering of a positive GitHub numeric user ID. Anything
# else (leading zeros, signs, whitespace, non-digits) is malformed trusted
# identity state and fails closed.
_STRICT_DECIMAL = frozenset("123456789")

# Application-service span boundaries (issues #108/#109). Only the safe
# attribute vocabulary is attachable: Profile identity and the operation
# name — never codes, tokens, references, or provider responses.
_TRACER_SCOPE = "openorc.services.github_user_authorization"
_ESTABLISH_SPAN_NAME = "github_user_authorization.establish"
_REVOKE_SPAN_NAME = "github_user_authorization.revoke"
_RESOLVE_SPAN_NAME = "github_user_authorization.resolve_access_token"

# Bounded reclassification passes for the refresh-resolution loop: a stale
# compare-and-swap reloads from the newer durable state and retries the
# resolution a bounded number of times; pathological persistent contention
# fails closed rather than looping or replaying.
_MAX_RESOLUTION_PASSES = 3

# A cached access token is treated as unusable this many seconds before its
# parsed expiry, so a live request never presents a token that expires
# mid-flight. The cache is process-memory only.
_ACCESS_TOKEN_EXPIRY_SAFETY_MARGIN_SECONDS = 60.0

# Bounded access-token cache cardinality: one entry per (Profile, generation)
# that actually resolved, pruned opportunistically and LRU-bounded.
_ACCESS_TOKEN_CACHE_MAX_ENTRIES = 32

logger = logging.getLogger(__name__)


class ProfileUserAccessTokenResolver(Protocol):
    """The resolver seam Owner-accountable GitHub writes depend on (issue #143).

    Structural contract so the shared Owner-write boundary and the write
    services never depend on the concrete resolver: the exact Profile's
    fail-closed user-token resolution (the typed unavailable-authorization
    conditions) and the single bounded-recovery eviction seam. The concrete
    :class:`GitHubUserAccessTokenResolver` satisfies it structurally; tests
    substitute structural fakes.
    """

    def resolve(self, pool: DatabasePool, *, profile_id: UUID) -> GitHubUserAccessToken:
        """Return a usable user access token for the Profile's authorization."""
        ...

    def evict_cached_access_token(self, profile_id: UUID) -> None:
        """Drop the Profile's cached access token (the bounded 401-recovery seam)."""
        ...


class GitHubUserAuthorizationUnavailableError(ApplicationError):
    """The Profile's GitHub user authorization is unusable (issue #142).

    Missing (never established), explicitly revoked, expired-and-
    unrefreshable, refresh-rejected, or refresh-capability-unavailable. An
    explicit typed authorization/integration condition — never a trigger for
    an installation-token fallback. ``condition`` carries one of the module
    ``CONDITION_*`` constants; messages never carry credential material.
    """

    def __init__(self, *, condition: str, message: str) -> None:
        super().__init__(message)
        self.condition = condition


class GitHubUserIdentityMismatchError(ApplicationError):
    """The authorized GitHub account is not the Profile's sign-in identity.

    The same-human binding proof failed: the GitHub account resolved
    authoritatively under the fresh user access token is not the stable
    GitHub identity backing the exact authenticated Profile's Supabase
    sign-in. Fails closed with nothing stored.
    """


class SupabaseGitHubIdentityStateError(ApplicationError):
    """The trusted Supabase GitHub identity state is not single and usable.

    Zero, multiple, or malformed GitHub identity entries back the exact
    authenticated Profile (or the account itself is absent at the identity
    provider). Never guessed; ``condition`` carries one of the module
    ``IDENTITY_CONDITION_*`` constants.
    """

    def __init__(self, *, condition: str, message: str) -> None:
        super().__init__(message)
        self.condition = condition


def _require_code(code: object) -> str:
    """Validate the callback command input before any durable read or external call."""
    if not isinstance(code, str) or code == "":
        raise InvalidCommandError("a GitHub authorization code must be a non-empty string")
    return code


def _require_optional_verifier(code_verifier: object) -> None:
    """Validate the optional PKCE verifier callback input before any external call.

    The verifier is forwarded verbatim to the token exchange when the
    correlated authorization request used PKCE; when present it must be a
    non-empty string (challenge/verifier lifecycle itself is the callback
    layer's responsibility).
    """
    if code_verifier is None:
        return
    if not isinstance(code_verifier, str) or code_verifier == "":
        raise InvalidCommandError("a GitHub PKCE code verifier must be a non-empty string")


def normalize_strict_github_user_id(provider_id: object) -> int:
    """Strictly normalize one trusted provider_id fact to the GitHub user ID.

    The stable provider-side fact must be the canonical decimal rendering of
    a positive integer. Anything else — missing, non-string, empty, signed,
    whitespace-padded, zero-padded, non-digit — is malformed trusted identity
    state.
    """
    if (
        not isinstance(provider_id, str)
        or not provider_id
        or provider_id[0] not in _STRICT_DECIMAL
        or not provider_id.isdigit()
    ):
        raise SupabaseGitHubIdentityStateError(
            condition=IDENTITY_CONDITION_MALFORMED,
            message="the trusted GitHub identity fact is not a usable stable provider ID",
        )
    return int(provider_id)


def _resolve_trusted_github_identity(
    admin_client: SupabaseAuthAdminClient, *, profile_id: UUID
) -> int:
    """Prove the stable GitHub identity backing the exact authenticated Profile.

    One trusted server-side Admin read of the exact Supabase Auth user. The
    adapter returns the multiplicity-preserving collection of GitHub
    provider facts; the authorization decision is made here: exactly one
    entry whose provider_id strictly normalizes to a stable numeric GitHub
    user ID. Mutable login/email/user_metadata are never read into the
    proof. No database transaction is open across this call.
    """
    try:
        provider_ids = admin_client.fetch_user_github_provider_ids(profile_id)
    except SupabaseAuthAdminUserAbsentError as exc:
        raise SupabaseGitHubIdentityStateError(
            condition=IDENTITY_CONDITION_ACCOUNT_ABSENT,
            message="the authenticated account is absent at the identity provider",
        ) from exc
    except SupabaseAuthAdminRejectedError as exc:
        raise ExternalOperationFailedError(
            "the GitHub identity of the authenticated account could not be "
            "resolved: the identity provider rejected the administrative lookup"
        ) from exc
    except SupabaseAuthAdminOutcomeUnknownError as exc:
        raise ExternalOperationUncertainError(
            "the GitHub identity of the authenticated account could not be "
            "resolved: the identity provider lookup outcome is unknown"
        ) from exc
    if len(provider_ids) == 0:
        raise SupabaseGitHubIdentityStateError(
            condition=IDENTITY_CONDITION_MISSING,
            message="the authenticated account has no GitHub sign-in identity to bind",
        )
    if len(provider_ids) > 1:
        raise SupabaseGitHubIdentityStateError(
            condition=IDENTITY_CONDITION_AMBIGUOUS,
            message="the authenticated account's GitHub sign-in identity is ambiguous",
        )
    return normalize_strict_github_user_id(provider_ids[0])


def _exchange_authorization_code(
    token_client: GitHubUserTokenClient,
    *,
    code: str,
    redirect_uri: str | None,
    code_verifier: str | None,
) -> GitHubUserTokenGrant:
    """Exchange the post-correlation code for the normalized token grant."""
    try:
        grant = token_client.exchange_authorization_code(
            code, redirect_uri=redirect_uri, code_verifier=code_verifier
        )
    except GitHubUserTokenRejectedError as exc:
        raise ExternalOperationFailedError(
            "the GitHub user authorization request was rejected by the token endpoint"
        ) from exc
    except GitHubUserTokenRefreshCapabilityMissingError as exc:
        logger.warning(
            "github user authorization rejected: the GitHub App does not provide "
            "the required expiring user-token capability"
        )
        raise GitHubUserAuthorizationUnavailableError(
            condition=CONDITION_REFRESH_CAPABILITY_UNAVAILABLE,
            message="the GitHub App does not provide the expiring user-token "
            "capability OpenOrc requires; fix the GitHub App configuration",
        ) from exc
    except GitHubOutcomeUncertainError as exc:
        raise ExternalOperationUncertainError(
            "the GitHub user authorization request outcome is unknown"
        ) from exc
    return grant


def establish_github_user_authorization(
    pool: DatabasePool,
    *,
    profile_id: UUID,
    code: str,
    admin_client: SupabaseAuthAdminClient,
    token_client: GitHubUserTokenClient,
    redirect_uri: str | None = None,
    code_verifier: str | None = None,
) -> GitHubUserAuthorization:
    """Establish (or re-establish) the Profile's GitHub user authorization.

    The full same-human binding proof, then the durable establishment:

    1. Prove the trusted GitHub identity of the exact authenticated Profile
       through the Auth Admin boundary (zero/multiple/malformed fail closed).
    2. Exchange the post-correlation authorization code and require the
       expiring-token capability (a capability-less answer fails closed —
       never a silently persisted long-lived token). ``code_verifier`` is
       forwarded when the correlated authorization request used PKCE.
    3. Resolve the authorized GitHub account authoritatively and compare the
       stable numeric GitHub user IDs; a mismatch fails closed storing
       nothing.
    4. In ONE short transaction: compose the account-operational barrier,
       lock/read the authorization row, delete the superseded Vault secret
       of an ACTIVE authorization (fail closed on malformed/dangling
       references), install the new refresh secret + reference, and update
       the row in place — inserting it at generation 1 when absent, or
       reactivating a revoked row (which carries no superseded credential)
       / replacing an active one with its generation advanced monotonically
       otherwise. The row is never delete-and-reinserted, so no earlier
       access-token cache key can resurface. Reauthorization is the
       recovery path after revocation, including after an account-deletion
       attempt whose external Auth deletion was definitively rejected.

    External calls run with no database transaction open. The caller
    validates OAuth ``state``/session correlation before invoking this
    service; the code is never treated as proof of Profile identity here.
    """
    with application_span(_TRACER_SCOPE, _ESTABLISH_SPAN_NAME) as span:
        annotate_span(span, operation=_ESTABLISH_SPAN_NAME)
        _require_code(code)
        _require_optional_verifier(code_verifier)
        # No database transaction is open across any of the three external
        # proof/exchange calls.
        proven_github_user_id = _resolve_trusted_github_identity(
            admin_client, profile_id=profile_id
        )
        grant = _exchange_authorization_code(
            token_client, code=code, redirect_uri=redirect_uri, code_verifier=code_verifier
        )
        current_user = _resolve_authorized_github_user(
            token_client, access_token=grant.access_token
        )
        if current_user.github_user_id != proven_github_user_id:
            logger.warning(
                "github user authorization rejected: the authorized account does not "
                "match the authenticated Profile's sign-in identity"
            )
            raise GitHubUserIdentityMismatchError(
                "the authorized GitHub account is not the account that signed in to this Profile"
            )
        return _install_established_authorization(
            pool,
            profile_id=profile_id,
            proven_github_user_id=proven_github_user_id,
            github_login=current_user.login,
            refresh_secret=grant.refresh_token,
        )


def _resolve_authorized_github_user(
    token_client: GitHubUserTokenClient, *, access_token: GitHubUserAccessToken
) -> GitHubCurrentUser:
    """Resolve the authorized GitHub account authoritatively under the token."""
    try:
        return token_client.fetch_authenticated_user(access_token)
    except (
        GitHubAuthenticationRejectedError,
        GitHubAuthorizationRejectedError,
        GitHubRequestRejectedError,
    ) as exc:
        raise ExternalOperationFailedError(
            "the authorized GitHub account could not be resolved: GitHub rejected "
            "the authoritative lookup"
        ) from exc
    except GitHubOutcomeUncertainError as exc:
        raise ExternalOperationUncertainError(
            "the authorized GitHub account could not be resolved: the lookup outcome is unknown"
        ) from exc


def _install_established_authorization(
    pool: DatabasePool,
    *,
    profile_id: UUID,
    proven_github_user_id: int,
    github_login: str | None,
    refresh_secret: GitHubUserRefreshSecret,
) -> GitHubUserAuthorization:
    """Durably install one proven authorization (insert or in-place replace)."""
    with composed_transaction(pool) as transaction_pool:
        # The account-wide Owner-mutation barrier FIRST, before the row lock.
        require_account_operational(transaction_pool, profile_id=profile_id)
        existing = github_user_authorizations.get_github_user_authorization_for_update(
            transaction_pool, profile_id=profile_id
        )
        if existing is not None:
            if existing.github_user_id != proven_github_user_id:
                # Durable-state inconsistency: every write path proves the
                # same-human binding, so a stored different identity is an
                # invariant failure, never a silently replaced binding.
                raise ConflictError(
                    "the durable GitHub authorization identity is inconsistent "
                    "with the proven sign-in identity"
                )
            if existing.status is GitHubUserAuthorizationStatus.ACTIVE:
                # Only an ACTIVE authorization carries a superseded secret to
                # delete; a revoked row is CHECK-required to carry none, and
                # reactivating it in place is the reauthorization recovery
                # path (including after a failed account-deletion attempt).
                _delete_superseded_refresh_secret(transaction_pool, existing=existing)
        secret_id = github_user_refresh_secrets.create_github_user_refresh_secret(
            transaction_pool, secret=refresh_secret.secret_value(), profile_id=profile_id
        )
        reference = github_user_refresh_secrets.encode_github_user_refresh_reference(secret_id)
        if existing is None:
            return github_user_authorizations.insert_active_github_user_authorization(
                transaction_pool,
                profile_id=profile_id,
                github_user_id=proven_github_user_id,
                github_login=github_login,
                refresh_secret_reference=reference,
                refresh_expires_at=refresh_secret.expires_at,
            )
        updated = github_user_authorizations.reauthorize_github_user_authorization(
            transaction_pool,
            profile_id=profile_id,
            github_user_id=proven_github_user_id,
            github_login=github_login,
            refresh_secret_reference=reference,
            refresh_expires_at=refresh_secret.expires_at,
        )
        if updated is None:  # pragma: no cover - unreachable under the row lock
            raise ConflictError("the durable GitHub authorization vanished while being replaced")
        return updated


def _delete_superseded_refresh_secret(
    transaction_pool: DatabasePool, *, existing: GitHubUserAuthorization
) -> None:
    """Delete the superseded refresh secret of an existing authorization.

    Fail closed on a malformed or dangling reference: only this service
    writes references, so either shape is a durable-state inconsistency that
    must roll the whole establishment back rather than orphan a secret.
    """
    if existing.refresh_secret_reference is None:
        # Unreachable for a durable row (CHECK-consistent); fail closed anyway.
        raise ConflictError("the existing authorization carries no credential reference")
    try:
        secret_id = github_user_refresh_secrets.parse_github_user_refresh_reference(
            existing.refresh_secret_reference
        )
    except GitHubUserRefreshSecretReferenceError as exc:
        logger.warning("github authorization replacement rejected an unrecognized reference")
        raise ConflictError(
            "the existing authorization's credential reference is not a recognized v1 reference"
        ) from exc
    if not github_user_refresh_secrets.delete_github_user_refresh_secret(
        transaction_pool, secret_id=secret_id
    ):
        logger.warning("github authorization replacement rejected a dangling reference")
        raise ConflictError(
            "the existing authorization's credential reference does not point at an existing secret"
        )


def revoke_github_user_authorization(
    pool: DatabasePool, *, profile_id: UUID
) -> GitHubUserAuthorization:
    """Explicitly revoke the Profile's GitHub user authorization (Owner action).

    In one short transaction: compose the account-operational barrier, lock
    the authorization row, delete the referenced Vault refresh secret
    (fail closed on malformed/dangling references), and mark the row revoked
    with its generation advanced. Revoking an already-revoked authorization
    is an idempotent no-op returning its durable row; a missing
    authorization is the typed MISSING condition, never a fallback.
    """
    with application_span(_TRACER_SCOPE, _REVOKE_SPAN_NAME) as span:
        annotate_span(span, operation=_REVOKE_SPAN_NAME)
        with composed_transaction(pool) as transaction_pool:
            require_account_operational(transaction_pool, profile_id=profile_id)
            existing = github_user_authorizations.get_github_user_authorization_for_update(
                transaction_pool, profile_id=profile_id
            )
            if existing is None:
                raise GitHubUserAuthorizationUnavailableError(
                    condition=CONDITION_MISSING,
                    message="this Profile has no GitHub authorization to revoke",
                )
            if existing.status is GitHubUserAuthorizationStatus.REVOKED:
                return existing
            if existing.refresh_secret_reference is None:
                # Unreachable for an active row (CHECK-consistent); fail closed.
                raise ConflictError("the authorization carries no credential reference")
            try:
                secret_id = github_user_refresh_secrets.parse_github_user_refresh_reference(
                    existing.refresh_secret_reference
                )
            except GitHubUserRefreshSecretReferenceError as exc:
                logger.warning("github authorization revocation rejected an unrecognized reference")
                raise ConflictError(
                    "the authorization's credential reference is not a recognized v1 reference"
                ) from exc
            if not github_user_refresh_secrets.delete_github_user_refresh_secret(
                transaction_pool, secret_id=secret_id
            ):
                logger.warning("github authorization revocation rejected a dangling reference")
                raise ConflictError(
                    "the authorization's credential reference does not point at an existing secret"
                )
            revoked = github_user_authorizations.revoke_github_user_authorization_row(
                transaction_pool, profile_id=profile_id
            )
            if revoked is None:  # pragma: no cover - unreachable under the row lock
                raise ConflictError("the durable GitHub authorization vanished while being revoked")
            return revoked


class GitHubUserAccessTokenResolver:
    """Restart-safe resolver for the Profile's GitHub user access token.

    Resolution is durable-state first: the bounded in-memory cache is keyed
    by the (Profile, authorization generation) pair, and every lifecycle
    transition advances that generation atomically with its durable effect,
    so no earlier cache key can ever serve again. A fresh process (or an
    evicted entry) resolves from durable authorization metadata plus the
    Vault refresh reference:

    1. read the current authorization (missing/revoked are typed conditions);
    2. resolve its exact refresh secret through the Vault boundary;
    3. perform exactly ONE GitHub refresh exchange with no database
       transaction open;
    4. on known success, install the rotation through the generation
       compare-and-swap (barrier first, lock/reload, Vault in-place update,
       expiry/generation increment) — a stale process discards its returned
       token pair and reloads from the newer durable state;
    5. a definitive refresh rejection fails the resolution closed with no
       durable mutation (durable state is reloaded first so an
       already-committed concurrent rotation is reported as moved-on); an
       uncertain outcome is never replayed and never mutates durable state.

    There is no installation-token fallback anywhere on this path.
    """

    def __init__(
        self,
        *,
        token_client: GitHubUserTokenClient,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        self._token_client = token_client
        self._clock = clock if clock is not None else utc_now
        self._cache: OrderedDict[tuple[UUID, int], GitHubUserAccessToken] = OrderedDict()
        self._cache_lock = threading.Lock()

    def resolve(self, pool: DatabasePool, *, profile_id: UUID) -> GitHubUserAccessToken:
        """Return a usable user access token for the Profile's authorization."""
        with application_span(_TRACER_SCOPE, _RESOLVE_SPAN_NAME) as span:
            annotate_span(span, operation=_RESOLVE_SPAN_NAME)
            for _pass in range(_MAX_RESOLUTION_PASSES):
                durable = github_user_authorizations.get_github_user_authorization(
                    pool, profile_id=profile_id
                )
                if durable is None:
                    raise GitHubUserAuthorizationUnavailableError(
                        condition=CONDITION_MISSING,
                        message="this Profile has no GitHub authorization",
                    )
                if durable.status is GitHubUserAuthorizationStatus.REVOKED:
                    raise GitHubUserAuthorizationUnavailableError(
                        condition=CONDITION_REVOKED,
                        message="the Profile's GitHub authorization is revoked",
                    )
                now = self._clock()
                cached = self._cache_get(profile_id, durable.refresh_generation, now)
                if cached is not None:
                    return cached
                if durable.refresh_expires_at is not None and durable.refresh_expires_at <= now:
                    raise GitHubUserAuthorizationUnavailableError(
                        condition=CONDITION_EXPIRED_UNREFRESHABLE,
                        message="the Profile's GitHub refresh credential is durably expired",
                    )
                durable_refresh_expires_at = durable.refresh_expires_at
                if durable_refresh_expires_at is None:
                    # Unreachable for an active row (CHECK-consistent); fail
                    # closed rather than resolve without the expiry fact.
                    raise ConflictError("the authorization carries no refresh expiry")
                refresh_credential = self._resolve_refresh_secret(
                    pool,
                    reference=durable.refresh_secret_reference,
                    refresh_expires_at=durable_refresh_expires_at,
                )
                grant = self._refresh_with_classification(
                    pool,
                    profile_id=profile_id,
                    refresh_token=refresh_credential,
                    observed_generation=durable.refresh_generation,
                )
                installed = self._install_refreshed_credential(
                    pool,
                    profile_id=profile_id,
                    observed_generation=durable.refresh_generation,
                    grant=grant,
                )
                if installed is not None:
                    return installed
                # The compare-and-swap found newer durable state: this
                # process's returned token pair is discarded and the
                # resolution reloads from the newer durable state.
            raise ConflictError(
                "the GitHub authorization refresh state kept moving; the resolution "
                "was not completed"
            )

    def evict_cached_access_token(self, profile_id: UUID) -> None:
        """Drop the Profile's cached access token (the bounded 401-recovery seam).

        The only sanctioned trigger is a DEFINITIVE authentication rejection
        (GitHub answered a 401-class failure) from a GitHub call made under
        this Profile's resolved user token — the #142 lifecycle's allowed
        bounded recovery: the next :meth:`resolve` re-reads durable state
        and, finding no cache entry, performs exactly one refresh exchange
        through the generation compare-and-swap. Never a replay mechanism:
        uncertain outcomes, known policy rejections, and stale operations
        must never trigger an eviction, and a recovered credential never
        widens the authorization it had. Evicting an unknown Profile's
        (absent) cache entry is a harmless no-op.
        """
        if not isinstance(profile_id, UUID):
            raise InvalidCommandError("profile_id must be a UUID")
        with self._cache_lock:
            for key in [key for key in self._cache if key[0] == profile_id]:
                del self._cache[key]

    def _resolve_refresh_secret(
        self, pool: DatabasePool, *, reference: object, refresh_expires_at: datetime
    ) -> GitHubUserRefreshSecret:
        """Resolve the Vault refresh credential behind the durable reference.

        The decrypted value crosses the persistence boundary only here, only
        on explicit resolution, and is wrapped IMMEDIATELY in the redacted
        secret-bearing carrier — it is never held as a plain string beyond
        this wrapping, never copied into ordinary structures, and reaches a
        raw representation only at the adapter's final request-construction
        boundary.
        """
        if not isinstance(reference, str) or not reference:
            # Unreachable for an active row (CHECK-consistent); fail closed anyway.
            logger.warning("github token resolution rejected a missing reference")
            raise ConflictError("the authorization carries no credential reference")
        try:
            secret_id = github_user_refresh_secrets.parse_github_user_refresh_reference(reference)
        except GitHubUserRefreshSecretReferenceError as exc:
            # Invariant: only this service writes references, so an
            # unrecognized shape is a durable-state inconsistency.
            logger.warning("github token resolution rejected an unrecognized reference")
            raise ConflictError(
                "the authorization's credential reference is not a recognized v1 reference"
            ) from exc
        secret_value = github_user_refresh_secrets.read_github_user_refresh_secret(
            pool, secret_id=secret_id
        )
        if secret_value is None:
            logger.warning("github token resolution rejected a dangling reference")
            raise ConflictError(
                "the authorization's credential reference does not point at an existing secret"
            )
        return GitHubUserRefreshSecret(value=secret_value, expires_at=refresh_expires_at)

    def _refresh_with_classification(
        self,
        pool: DatabasePool,
        *,
        profile_id: UUID,
        refresh_token: GitHubUserRefreshSecret,
        observed_generation: int,
    ) -> GitHubUserTokenGrant:
        """Perform exactly one refresh exchange, classified fail-closed.

        The secret-bearing carrier is forwarded as-is; no database
        transaction is open across the external call. A definitive
        rejection never mutates durable authorization state: it reloads
        durable state first, so an already-committed concurrent rotation is
        reported as moved-on rather than as a dead credential.
        """
        try:
            return self._token_client.refresh_user_token(refresh_token)
        except GitHubUserTokenRefreshCapabilityMissingError as exc:
            # The deployment's GitHub App no longer provides the expiring
            # user-token capability: a known misconfiguration condition,
            # surfaced as the typed application condition with NO durable
            # mutation (the durable credential is untouched).
            logger.warning(
                "github token refresh failed closed: the GitHub App does not "
                "provide the required expiring user-token capability"
            )
            raise GitHubUserAuthorizationUnavailableError(
                condition=CONDITION_REFRESH_CAPABILITY_UNAVAILABLE,
                message="the GitHub App does not provide the expiring user-token "
                "capability OpenOrc requires; fix the GitHub App configuration",
            ) from exc
        except GitHubUserTokenRejectedError as exc:
            newer = github_user_authorizations.get_github_user_authorization(
                pool, profile_id=profile_id
            )
            if newer is not None and newer.refresh_generation > observed_generation:
                logger.warning(
                    "github token refresh was rejected after a concurrent rotation "
                    "advanced the authorization; the resolution failed closed"
                )
                raise StaleOperationError(
                    "the GitHub refresh credential was rejected, but a concurrent "
                    "rotation has already advanced the authorization; re-resolve "
                    "from current durable state"
                ) from exc
            logger.warning("github token refresh was definitively rejected by GitHub")
            raise GitHubUserAuthorizationUnavailableError(
                condition=CONDITION_REFRESH_REJECTED,
                message="the Profile's GitHub refresh credential was rejected; "
                "reauthorization is the recovery path",
            ) from exc
        except GitHubOutcomeUncertainError as exc:
            # The refresh token GitHub may have consumed is not safe to
            # reuse: fail closed, never replay, never mutate durable state.
            logger.warning(
                "the outcome of the github token refresh is unknown; the resolution "
                "failed closed without durable mutation"
            )
            raise ExternalOperationUncertainError(
                "the outcome of the GitHub user token refresh is unknown"
            ) from exc

    def _install_refreshed_credential(
        self,
        pool: DatabasePool,
        *,
        profile_id: UUID,
        observed_generation: int,
        grant: GitHubUserTokenGrant,
    ) -> GitHubUserAccessToken | None:
        """Install one successful rotation through the generation compare-and-swap.

        One short transaction: the account-operational barrier first, then
        the row lock; the rotated Vault value and the row's expiry/generation
        install only when the durable generation still matches and the row
        is still active. Returns the fresh access token on success, or None
        when the durable state moved on — the caller's returned token pair
        is discarded and never written.
        """
        with composed_transaction(pool) as transaction_pool:
            require_account_operational(transaction_pool, profile_id=profile_id)
            row = github_user_authorizations.get_github_user_authorization_for_update(
                transaction_pool, profile_id=profile_id
            )
            if row is None:
                raise ConflictError(
                    "the durable GitHub authorization vanished while its refresh "
                    "was being installed"
                )
            if row.status is not GitHubUserAuthorizationStatus.ACTIVE:
                # Revocation raced the install: the returned token pair is
                # discarded and nothing durable is written.
                raise GitHubUserAuthorizationUnavailableError(
                    condition=CONDITION_REVOKED,
                    message="the Profile's GitHub authorization was revoked during the refresh",
                )
            if row.refresh_generation != observed_generation:
                return None
            if row.refresh_secret_reference is None:
                # Unreachable under the active-row check; fail closed anyway.
                raise ConflictError("the authorization carries no credential reference")
            try:
                secret_id = github_user_refresh_secrets.parse_github_user_refresh_reference(
                    row.refresh_secret_reference
                )
            except GitHubUserRefreshSecretReferenceError as exc:
                logger.warning("github refresh install rejected an unrecognized reference")
                raise ConflictError(
                    "the authorization's credential reference is not a recognized v1 reference"
                ) from exc
            if not github_user_refresh_secrets.update_github_user_refresh_secret(
                transaction_pool,
                secret_id=secret_id,
                secret=grant.refresh_token.secret_value(),
            ):
                logger.warning("github refresh install rejected a dangling reference")
                raise ConflictError(
                    "the authorization's credential reference does not point at an existing secret"
                )
            installed = github_user_authorizations.try_install_rotated_refresh(
                transaction_pool,
                profile_id=profile_id,
                expected_generation=observed_generation,
                refresh_expires_at=grant.refresh_token.expires_at,
            )
            if installed is None:  # pragma: no cover - guarded by the locked check
                raise ConflictError("the GitHub authorization generation moved under the row lock")
        self._cache_put(profile_id, installed.refresh_generation, grant.access_token)
        return grant.access_token

    def _cache_get(
        self, profile_id: UUID, generation: int, now: datetime
    ) -> GitHubUserAccessToken | None:
        """Return the generation-bound cached token if still safely usable."""
        with self._cache_lock:
            token = self._cache.get((profile_id, generation))
            if token is None:
                return None
            usable_margin = _ACCESS_TOKEN_EXPIRY_SAFETY_MARGIN_SECONDS
            if (token.expires_at - now).total_seconds() <= usable_margin:
                del self._cache[(profile_id, generation)]
                return None
            self._cache.move_to_end((profile_id, generation))
            return token

    def _cache_put(self, profile_id: UUID, generation: int, token: GitHubUserAccessToken) -> None:
        """Bind the token to its exact (Profile, generation) key, bounded LRU.

        Entries for any other generation of the same Profile are dropped:
        every lifecycle transition advances the generation, so an older
        cache key can never serve again.
        """
        with self._cache_lock:
            for key in [
                key for key in self._cache if key[0] == profile_id and key[1] != generation
            ]:
                del self._cache[key]
            self._cache[(profile_id, generation)] = token
            self._cache.move_to_end((profile_id, generation))
            while len(self._cache) > _ACCESS_TOKEN_CACHE_MAX_ENTRIES:
                self._cache.popitem(last=False)
