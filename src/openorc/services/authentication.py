"""Authentication application service.

The Phase 2A backend authentication boundary (issue #52): verify a Supabase
Auth access token and resolve it to the canonical OpenOrc
:class:`~openorc.domain.ownership.Profile`. This is the transport-neutral
operation later API/worker surfaces call directly — no FastAPI/RQ objects
cross this boundary.

Verification happens before any Profile work: the possible JWKS retrieval is
external I/O and runs with no database transaction open. Profile resolution
is one short persistence transaction. Error translation is service-owned:
adapter token rejections and the database's fail-closed account-identity FK
surface as :class:`AuthenticationError`; JWKS retrieval failures surface as
the external-operation vocabulary, never as invalid caller credentials. Error
messages are safe by construction: no bearer token, signature, or key
material ever enters an exception, return value, or log.
"""

from __future__ import annotations

from dataclasses import dataclass

from psycopg import errors as psycopg_errors

from openorc.adapters.supabase import (
    SupabaseAccessTokenRejectedError,
    SupabaseAccessTokenVerifier,
    SupabaseJwksOutcomeUnknownError,
    SupabaseJwksUnavailableError,
)
from openorc.domain.identity import AuthenticatedPrincipal
from openorc.domain.ownership import Profile
from openorc.observability import annotate_span, application_span
from openorc.persistence.ownership import ensure_profile
from openorc.persistence.pool import DatabasePool
from openorc.services.errors import (
    AuthenticationError,
    ExternalOperationFailedError,
    ExternalOperationUncertainError,
)

__all__ = ["AuthenticatedUser", "authenticate"]

# Application-service span boundary (issues #108/#109): the Phase 2A
# authentication use case participates in OpenTelemetry traces. Only the safe
# attribute vocabulary is attachable, so the bearer token has no supported
# path into telemetry.
_AUTHENTICATION_TRACER_SCOPE = "openorc.services.authentication"
_AUTHENTICATE_SPAN_NAME = "authentication.authenticate"


@dataclass(frozen=True, slots=True)
class AuthenticatedUser:
    """One successful authentication: the verified principal plus its
    canonical OpenOrc Profile (idempotently bootstrapped on first use)."""

    principal: AuthenticatedPrincipal
    profile: Profile


def authenticate(
    pool: DatabasePool,
    verifier: SupabaseAccessTokenVerifier,
    *,
    token: str,
) -> AuthenticatedUser:
    """Authenticate a raw bearer token and resolve its canonical Profile.

    The verifier establishes the verified ``sub`` identity first; only then is
    the Profile resolved or idempotently ensured for that exact UUID. The
    Profile is never created without the corresponding ``auth.users`` row: the
    sanctioned ``openorc.profiles.id -> auth.users (id)`` foreign key fails
    closed for a permanently deleted account (for example, a still-valid JWT
    whose Auth user was removed), and that case is translated to a typed
    authentication failure rather than leaked as a database error.
    """
    with application_span(_AUTHENTICATION_TRACER_SCOPE, _AUTHENTICATE_SPAN_NAME) as span:
        annotate_span(span, operation=_AUTHENTICATE_SPAN_NAME)
        try:
            principal = verifier.verify(token)
        except SupabaseAccessTokenRejectedError as exc:
            raise AuthenticationError(
                "authentication failed: the access token is not valid"
            ) from exc
        except SupabaseJwksOutcomeUnknownError as exc:
            # JWKS retrieval outcome unknown (timeout/connection loss): an
            # external-operation uncertainty, never invalid caller credentials.
            raise ExternalOperationUncertainError(
                "authentication is temporarily unavailable: the identity provider "
                "signing-key source could not be reached"
            ) from exc
        except SupabaseJwksUnavailableError as exc:
            # Known JWKS retrieval failure: verification infrastructure failed,
            # not the caller's credential.
            raise ExternalOperationFailedError(
                "authentication is temporarily unavailable: the identity provider "
                "signing-key source rejected the lookup"
            ) from exc

        try:
            profile = ensure_profile(pool, profile_id=principal.user_id)
        except psycopg_errors.ForeignKeyViolation as exc:
            # Fail closed at the account-identity boundary: never re-create an
            # application identity whose Supabase Auth user is gone.
            raise AuthenticationError(
                "authentication failed: the account backing this session no longer exists"
            ) from exc

    return AuthenticatedUser(principal=principal, profile=profile)
