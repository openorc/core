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

## Auth Admin boundary

- The Auth Admin client owns the transport mechanics of the two supported server-side Admin operations the account-deletion lifecycle needs — permanent user deletion and the read surface used to reconcile uncertain outcomes — and nothing else. Workflow meaning, reconciliation policy, and typed error translation belong to the account-lifecycle service; this adapter never imports `openorc.services`.
- The deployment's secret API key is not a JWT: it travels on the `apikey` request header only — never as `Authorization: Bearer`, never in a URL or query parameter, and on no other header.
- Privileged-transport hardening: the project URL must be HTTPS (the administrative credential is never sent over plaintext HTTP; a narrowly-constrained local development exception permits http only for loopback hosts), and redirects are never followed — urllib copies non-content request headers into redirected requests, so a followed redirect could forward the credential to another host. A not-followed redirect surfaces as an unclassified 3xx and is classified as an unknown outcome, never success and never a safe replay.
- Outcome classification is the service boundary: any 2xx is success; 404 is the confirmed-absent classified end state (`SupabaseAuthAdminUserAbsentError` — for a delete it is the desired end state already true, never a failure); other 4xx answers are definitive rejections (`SupabaseAuthAdminRejectedError`, a known failure); 5xx, timeout, connection loss, and uninterpretable transport responses are `SupabaseAuthAdminOutcomeUnknownError` — deliberately uncertain for a destructive write, whose effect an intermediary can mask. Error messages never contain the key, the project URL (it carries the project reference), or user-record contents.
- Key hygiene is structural: construction fails fast on a missing/blank key (the component that owns account deletion must possess the credential), the key is held in a private field with a redacted `repr`/`str`, and no log record, span attribute, exception, or returned object can carry it. Only the operation name is attached to the representative external-adapter spans.
- `request_timeout_seconds` is the bounded request timeout the account-deletion service derives its durable active-attempt lease from; the client must keep every request bounded.