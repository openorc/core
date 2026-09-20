# Supabase Adapter Agent Context

## Boundary

This adapter owns Supabase Auth JWT verification mechanics: JWKS fetching/caching, signature verification against the project's asymmetric signing keys, and token-claim normalization into `AuthenticatedPrincipal`. Application services decide authentication meaning (identity resolution, Profile bootstrap, typed error translation).

## Rules

- Dependency direction: this package never imports `openorc.services`. Adapter-local normalized errors (`SupabaseAccessTokenRejectedError`, `SupabaseJwksUnavailableError`, `SupabaseJwksOutcomeUnknownError`) carry only safe, non-token content; the authentication service translates them into the typed application vocabulary.
- Token rejection and JWKS retrieval failure are distinct outcomes. An invalid credential is never conflated with an inability to verify (JWKS outage); known retrieval failure and unknown retrieval outcome are also distinct (timeout/connection loss is never reclassified).
- Verification is constrained to the explicit asymmetric algorithm allowlist (ES256/RS256). No symmetric/HS256 shared-secret path exists; legacy symmetric-secret projects fail closed.
- Identity authority is the verified JWT `sub` UUID only. GitHub username, email, and `user_metadata` are never identity or authorization inputs.
- The GitHub-only v1 sign-in rule is enforced against trusted `app_metadata` as defense-in-depth account-origin validation; the real deployment guarantee is the Supabase Auth configuration (GitHub as the only enabled sign-in provider).
- This adapter uses only the public JWKS source. The Supabase secret/service key belongs to the later administrative lifecycle boundary, never here.
- Raw tokens never appear in errors, logs, returned objects, or persistence.