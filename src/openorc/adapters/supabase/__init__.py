"""Supabase adapter: Supabase Auth verification and Auth Admin mechanics.

Owns the transport-level mechanics of verifying Supabase Auth access tokens
against the project's asymmetric signing keys (JWKS) and the server-side
Auth Admin boundary for permanent account deletion. OpenOrc workflow and
identity semantics belong to the services above these boundaries.
"""

from openorc.adapters.supabase.admin import (
    AUTH_ADMIN_USERS_PATH,
    DEFAULT_ADMIN_REQUEST_TIMEOUT_SECONDS,
    HttpSupabaseAuthAdminClient,
    SupabaseAuthAdminClient,
    SupabaseAuthAdminOutcomeUnknownError,
    SupabaseAuthAdminRejectedError,
    SupabaseAuthAdminUserAbsentError,
)
from openorc.adapters.supabase.auth import (
    EXPECTED_TOKEN_ROLE,
    GITHUB_PROVIDER,
    SUPPORTED_ASYMMETRIC_ALGORITHMS,
    HttpJwksClient,
    JwksClient,
    SupabaseAccessTokenRejectedError,
    SupabaseAccessTokenVerifier,
    SupabaseJwksOutcomeUnknownError,
    SupabaseJwksUnavailableError,
)

__all__ = [
    "AUTH_ADMIN_USERS_PATH",
    "DEFAULT_ADMIN_REQUEST_TIMEOUT_SECONDS",
    "EXPECTED_TOKEN_ROLE",
    "GITHUB_PROVIDER",
    "SUPPORTED_ASYMMETRIC_ALGORITHMS",
    "HttpJwksClient",
    "HttpSupabaseAuthAdminClient",
    "JwksClient",
    "SupabaseAuthAdminClient",
    "SupabaseAuthAdminOutcomeUnknownError",
    "SupabaseAuthAdminRejectedError",
    "SupabaseAuthAdminUserAbsentError",
    "SupabaseAccessTokenRejectedError",
    "SupabaseAccessTokenVerifier",
    "SupabaseJwksOutcomeUnknownError",
    "SupabaseJwksUnavailableError",
]
