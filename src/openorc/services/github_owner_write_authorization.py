"""Owner-accountable GitHub write authorization boundary (issue #143).

The shared composed boundary every Owner-accountable GitHub write (canonical
PR publication, exact-head merge request, and later engineering-record
mutations) passes through before any credential resolution or external
GitHub I/O, and the trusted credential path those writes resolve:

1. The accountable Profile is explicit at the service boundary: every
   Owner-accountable write command carries the exact ``profile_id`` whose
   GitHub user authorization will externally represent the write. The
   identity comes from the trusted workflow/Owner authorization context —
   the GitHub adapter never guesses a Profile from Workspace ownership,
   mutable GitHub metadata, repository membership, or current-user
   heuristics.
2. Authorization is re-established for that exact human BEFORE the user
   credential is resolved or any external GitHub I/O happens: the
   account-wide deletion-attempt barrier (``require_account_operational``)
   composes FIRST in one short database-only transaction, then the existing
   Workspace Owner authorization boundary (``require_workspace_task``)
   proves the exact Profile owns the addressed Workspace/Task — an unowned
   Workspace, a foreign Task, or a missing Profile is the uniform not-found
   outcome. A caller-supplied or stale ``profile_id`` is never sufficient
   authority by itself; the subject/currentness validation (state token,
   canonical branch, exact PR head) stays with each calling service.
3. Credential resolution is Profile-scoped and fail-closed: the #142
   resolver supplies the exact Profile's user access token (missing,
   revoked, expired, refresh-rejected, or capability-unavailable
   authorizations are the typed unavailable conditions — there is no
   installation-token fallback), and the adapter proves the effective
   user × App installation × repository intersection for the exact routed
   identities before any write. A user token that cannot reach the routed
   repository through the routed installation is the typed
   intersection-failure condition: never another installation, another
   Profile, an installation-token write, or a PAT.
4. Bounded recovery: a DEFINITIVE 401-style rejection under the user token
   performs only the single bounded recovery the #142 lifecycle allows —
   evict the cached token, re-resolve through the durable refresh
   lifecycle, and retry the failed step exactly once. Uncertain outcomes
   are never refreshed, retried, or replayed.

The module owns no workflow meaning: it classifies authorization and
integration conditions into the typed application vocabulary and returns
the credential; the calling services keep their exact-head/state-token/
reconciliation semantics untouched.
"""

from __future__ import annotations

from uuid import UUID

from openorc.adapters.github import (
    GitHubAppClient,
    GitHubAuthenticationRejectedError,
    GitHubAuthorizationRejectedError,
    GitHubOutcomeUncertainError,
    GitHubProfileUserAccessToken,
    GitHubRateLimitedError,
    GitHubRequestRejectedError,
)
from openorc.domain.tasks import Task
from openorc.observability import annotate_span, application_span
from openorc.persistence.pool import DatabasePool
from openorc.services.errors import (
    ApplicationError,
    ExternalOperationFailedError,
    ExternalOperationUncertainError,
    InvalidCommandError,
)
from openorc.services.github_installation_route import (
    ResolvedRepositoryInstallationRoute,
)
from openorc.services.github_user_authorization import ProfileUserAccessTokenResolver
from openorc.services.profile_lifecycle_guard import require_account_operational
from openorc.services.transaction_composition import composed_transaction
from openorc.services.workspace_authorization import require_workspace_task

__all__ = [
    "CONDITION_USER_INSTALLATION_REPOSITORY_ACCESS_MISSING",
    "GitHubUserRepositoryAccessError",
    "require_owner_write_authorization",
    "resolve_owner_write_credential",
    "resolve_owner_write_credential_after_rejection",
    "translate_owner_write_failure",
]

# Typed user×installation×repository intersection-failure condition carried by
# GitHubUserRepositoryAccessError. The condition is safe vocabulary: it never
# carries credential material or provider response content.
CONDITION_USER_INSTALLATION_REPOSITORY_ACCESS_MISSING = (
    "user_installation_repository_access_missing"
)

# Application-service span boundary (issues #108/#109). Only the safe
# attribute vocabulary is attachable: the operation name and Workspace
# identity — never codes, tokens, references, or provider responses.
_SERVICE_TRACER_SCOPE = "openorc.services.github_owner_write_authorization"
_AUTHORIZE_SPAN_NAME = "github_owner_write_authorization.require_owner_write_authorization"
_RESOLVE_SPAN_NAME = "github_owner_write_authorization.resolve_owner_write_credential"


class GitHubUserRepositoryAccessError(ApplicationError):
    """The accountable Profile's user authorization lacks the routed access (issue #143).

    The effective user × App installation × repository intersection failed:
    the exact Profile-bound GitHub App user authorization cannot act through
    the routed installation or cannot reach the routed stable repository
    identity through it. An explicit typed authorization/integration
    condition — never a trigger for another installation, another Profile,
    an installation-token write, or a PAT. ``condition`` carries the module
    ``CONDITION_*`` constant; messages never carry credential material.
    """

    def __init__(self, *, condition: str, message: str) -> None:
        super().__init__(message)
        self.condition = condition


def _require_uuid_command(value: object, name: str) -> None:
    """Reject a malformed UUID command argument before any state is touched."""
    if not isinstance(value, UUID):
        raise InvalidCommandError(f"{name} must be a UUID")


def require_owner_write_authorization(
    pool: DatabasePool, *, profile_id: UUID, workspace_id: UUID, task_id: UUID
) -> Task:
    """Re-establish the accountable Profile's Owner authority for one write.

    One short database-only composed transaction (the issue #143 required
    sequence): the account-operational barrier for the supplied
    ``profile_id`` composes FIRST — the Profile ``FOR KEY SHARE`` read is
    the first lock acquisition and fails closed while an account deletion
    attempt is unresolved (#97) — then the existing Workspace Owner
    authorization boundary resolves and authorizes the exact Workspace/Task
    under that exact Profile. An unowned Workspace, a foreign Task, or a
    missing Profile is the uniform not-found outcome, so probing internal
    UUIDs is indistinguishable from addressing something absent. Returns
    the authorized Task for the caller's subject/currentness validation;
    the authorization is re-proven here before any credential resolution or
    external GitHub I/O, never assumed from caller-supplied facts.
    """
    _require_uuid_command(profile_id, "profile_id")
    _require_uuid_command(workspace_id, "workspace_id")
    _require_uuid_command(task_id, "task_id")
    with application_span(_SERVICE_TRACER_SCOPE, _AUTHORIZE_SPAN_NAME) as span:
        # Attach only after the caller-supplied identifiers proved valid: a
        # malformed command is classified without exporting its values.
        annotate_span(span, operation=_AUTHORIZE_SPAN_NAME, workspace_id=str(workspace_id))
        with composed_transaction(pool) as transaction_pool:
            # The account-wide Owner-mutation barrier first (issue #97).
            require_account_operational(transaction_pool, profile_id=profile_id)
            # The existing Workspace Owner authorization boundary: exact
            # ownership of the addressed Workspace and its Task.
            return require_workspace_task(
                transaction_pool,
                profile_id=profile_id,
                workspace_id=workspace_id,
                task_id=task_id,
            )


def _resolve_profile_credential(
    user_token_resolver: ProfileUserAccessTokenResolver,
    pool: DatabasePool,
    *,
    profile_id: UUID,
) -> GitHubProfileUserAccessToken:
    """Resolve and bundle the exact Profile's user credential (fail closed).

    The #142 resolver's typed unavailable conditions (missing, revoked,
    expired-unrefreshable, refresh-rejected, capability-unavailable)
    propagate unchanged — there is no installation-token fallback anywhere
    on this path, and a valid credential belonging to some other Profile is
    never reusable here.
    """
    return GitHubProfileUserAccessToken(
        profile_id=profile_id,
        access_token=user_token_resolver.resolve(pool, profile_id=profile_id),
    )


def _validate_user_repository_intersection(
    github: GitHubAppClient,
    *,
    credential: GitHubProfileUserAccessToken,
    route: ResolvedRepositoryInstallationRoute,
) -> None:
    """Prove the user × installation × repository intersection (external I/O)."""
    try:
        github.validate_user_installation_repository_access(
            credential=credential,
            github_installation_id=route.github_installation_id,
            github_repository_id=route.repository.identity.github_repository_id,
        )
    except GitHubAuthorizationRejectedError as error:
        # The user cannot act on the routed repository through the routed
        # installation: the typed intersection condition — never another
        # installation, another Profile, or an installation-token write.
        raise GitHubUserRepositoryAccessError(
            condition=CONDITION_USER_INSTALLATION_REPOSITORY_ACCESS_MISSING,
            message=(
                "the accountable Profile's GitHub user authorization does not "
                "reach the routed repository through the routed installation"
            ),
        ) from error
    except GitHubRateLimitedError as error:
        # A rate limit is a known provider condition, deliberately NOT an
        # authorization absence.
        raise ExternalOperationFailedError(
            "the GitHub user installation access validation was rate limited"
        ) from error
    except GitHubAuthenticationRejectedError:
        # Propagates unchanged: the caller (resolve_owner_write_credential)
        # owns the single bounded user-token recovery pass it may trigger.
        raise
    except GitHubRequestRejectedError as error:
        raise ExternalOperationFailedError(
            "the GitHub user installation access validation failed as a known provider condition"
        ) from error
    except GitHubOutcomeUncertainError as error:
        raise ExternalOperationUncertainError(
            "the outcome of the GitHub user installation access validation is unknown"
        ) from error


def _resolve_and_validate_once(
    pool: DatabasePool,
    user_token_resolver: ProfileUserAccessTokenResolver,
    github: GitHubAppClient,
    *,
    profile_id: UUID,
    route: ResolvedRepositoryInstallationRoute,
) -> GitHubProfileUserAccessToken:
    """One credential resolution + one intersection proof (no recovery).

    The typed unavailable-authorization conditions propagate unchanged; the
    intersection proof's classified outcomes translate as usual EXCEPT a
    definitive :class:`GitHubAuthenticationRejectedError`, which propagates
    unchanged for the caller's bounded-recovery decision — never recovered
    inside this helper.
    """
    credential = _resolve_profile_credential(user_token_resolver, pool, profile_id=profile_id)
    _validate_user_repository_intersection(github, credential=credential, route=route)
    return credential


def resolve_owner_write_credential(
    pool: DatabasePool,
    user_token_resolver: ProfileUserAccessTokenResolver,
    github: GitHubAppClient,
    *,
    profile_id: UUID,
    route: ResolvedRepositoryInstallationRoute,
) -> GitHubProfileUserAccessToken:
    """Resolve the accountable Profile's user credential and prove the intersection.

    Runs only AFTER the caller has re-established the account-operational +
    Workspace Owner authorization and validated its exact subject/route
    authority (the issue #143 required sequence). The resolution is
    fail-closed: the typed unavailable-authorization conditions propagate
    unchanged, and the adapter's App-scoped intersection proof must succeed
    for the exact routed identities. A definitive 401-style rejection from
    the intersection validation performs the ONE bounded recovery the #142
    lifecycle allows (evict the cached token, re-resolve through the durable
    refresh lifecycle) and retries the validation exactly once; a second
    rejection fails closed. Uncertain outcomes are never refreshed, retried,
    or replayed.
    """
    _require_uuid_command(profile_id, "profile_id")
    with application_span(_SERVICE_TRACER_SCOPE, _RESOLVE_SPAN_NAME) as span:
        annotate_span(span, operation=_RESOLVE_SPAN_NAME)
        try:
            return _resolve_and_validate_once(
                pool, user_token_resolver, github, profile_id=profile_id, route=route
            )
        except GitHubAuthenticationRejectedError:
            # Definitive 401-style non-delivery under the user token during
            # the initial resolution: the single bounded recovery pass the
            # #142 lifecycle allows. The fresh credential re-proves the
            # intersection; a second rejection fails closed.
            user_token_resolver.evict_cached_access_token(profile_id)
            try:
                return _resolve_and_validate_once(
                    pool, user_token_resolver, github, profile_id=profile_id, route=route
                )
            except GitHubAuthenticationRejectedError as error:
                raise ExternalOperationFailedError(
                    "GitHub rejected the accountable Profile's user credential after "
                    "the bounded recovery; the resolution fails closed"
                ) from error


def resolve_owner_write_credential_after_rejection(
    pool: DatabasePool,
    user_token_resolver: ProfileUserAccessTokenResolver,
    github: GitHubAppClient,
    *,
    profile_id: UUID,
    route: ResolvedRepositoryInstallationRoute,
) -> GitHubProfileUserAccessToken:
    """The bounded user-token recovery after a definitive write rejection.

    Called ONLY after a definitive 401-style authentication rejection from an
    Owner-accountable write already attempted under a resolved credential.
    Performs EXACTLY ONE bounded recovery pass: evict the cached token,
    re-resolve through the #142 durable refresh lifecycle (exactly one
    refresh exchange), and re-prove the user × installation × repository
    intersection once with the fresh credential before the caller retries
    its write exactly once. Any definitive 401-style rejection during that
    re-proof fails closed — never another eviction, never another refresh,
    and the caller's write retry never runs. Uncertain outcomes are never
    refreshed, retried, or replayed, and known policy/state rejections or
    stale operations never trigger this path at all.
    """
    _require_uuid_command(profile_id, "profile_id")
    user_token_resolver.evict_cached_access_token(profile_id)
    try:
        return _resolve_and_validate_once(
            pool, user_token_resolver, github, profile_id=profile_id, route=route
        )
    except GitHubAuthenticationRejectedError as error:
        # The recovery pass is spent: the re-resolved credential was
        # rejected while re-proving the intersection. Fail closed — no
        # second eviction/refresh and no write retry.
        raise ExternalOperationFailedError(
            "GitHub rejected the re-resolved user credential while re-proving the "
            "user installation access after the bounded recovery; the recovery "
            "fails closed"
        ) from error


def translate_owner_write_failure(
    error: GitHubRequestRejectedError, *, operation: str
) -> ApplicationError:
    """Translate a definitive adapter rejection of an Owner-accountable write.

    The user-authenticated write's failure text is honest about the
    credential path (issue #143): it never describes a user-authenticated
    write failure as a routed-installation credential failure. A rate limit
    stays a known provider condition, deliberately not an authorization
    absence; uncertain outcomes are handled by the caller and stay
    uncertain.
    """
    if isinstance(error, GitHubRateLimitedError):
        return ExternalOperationFailedError(
            f"the Owner-accountable GitHub {operation} was rate limited"
        )
    if isinstance(error, GitHubAuthenticationRejectedError):
        return ExternalOperationFailedError(
            f"GitHub rejected the accountable Profile's user credential for the {operation}"
        )
    if isinstance(error, GitHubAuthorizationRejectedError):
        return ExternalOperationFailedError(
            f"GitHub rejected the Owner-accountable {operation} under the accountable "
            "Profile's user authorization"
        )
    return ExternalOperationFailedError(
        f"the Owner-accountable GitHub {operation} failed as a known provider condition"
    )
