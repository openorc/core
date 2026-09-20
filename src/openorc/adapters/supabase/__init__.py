"""Supabase adapter: Supabase Auth JWT verification mechanics.

Owns the transport-level mechanics of verifying Supabase Auth access tokens
against the project's asymmetric signing keys (JWKS). OpenOrc workflow and
identity semantics belong to the services above this boundary.
"""

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
    "EXPECTED_TOKEN_ROLE",
    "GITHUB_PROVIDER",
    "SUPPORTED_ASYMMETRIC_ALGORITHMS",
    "HttpJwksClient",
    "JwksClient",
    "SupabaseAccessTokenRejectedError",
    "SupabaseAccessTokenVerifier",
    "SupabaseJwksOutcomeUnknownError",
    "SupabaseJwksUnavailableError",
]
