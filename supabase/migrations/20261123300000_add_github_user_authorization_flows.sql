-- Pending GitHub App user authorization OAuth start-to-callback correlation
-- (Phase 2G, issue #373).
--
--   Profile -> GitHubUserAuthorizationFlow (one PENDING flow per Profile)
--
-- This table holds PENDING correlation rows only; terminal rows are deleted,
-- never retained. The row is deleted at exactly three moments:
--
-- - the provider callback's atomic consume, conditional on the exact
--   ``state`` and an unexpired row — the DELETE is the single-use
--   enforcement, so a replayed, expired, or unknown state consumes nothing;
-- - the start operation's supersession of the Profile's prior pending flow
--   (at most one pending flow per Profile, enforced by the primary key, so
--   concurrent starts resolve by the database constraint);
-- - Owner revocation / account-deletion invalidation, which deletes the row
--   and its Vault verifier secret in the same short transaction.
--
-- Security properties carried by this migration:
--
-- - ``state`` is the unpredictable, expiring, single-use correlation value
--   the authenticated start operation created and the provider callback
--   presents. It selects the flow's subject and intended purpose; it is
--   never identity authority — completion is fenced by the captured
--   revocation evidence (``observed_revocation_count``) and the #142
--   same-human binding proof.
-- - ``pkce_verifier_reference`` is the opaque Vault pointer to the flow's
--   PKCE code verifier — never the verifier value. The verifier is
--   secret-bearing exchange material and lives exclusively behind the Vault
--   boundary until the callback consumes the flow.
-- - ``observed_revocation_count`` is the durable revocation-epoch evidence
--   captured from the Profile's authorization row when the authenticated
--   start ran (0 when no authorization existed). The final credential
--   installation proceeds only when the durable count still equals this
--   captured value, so any Owner revocation committed after the flow
--   started permanently invalidates it.
-- - ``purpose`` is the intended flow the authenticated start declared:
--   'authorize' for a first authorization over an absent/revoked
--   authorization, 'reauthorize' for a new authorization over an existing
--   row.
-- - ``expires_at`` bounds the flow's usability and is CHECK-required to lie
--   strictly after ``started_at``; expired flows can never be consumed.
--
-- FOREIGN-KEY CLASSIFICATION (the deletion-ownership vocabulary, issue #27):
-- - github_user_authorization_flows.profile_id -> profiles: TRUE OWNERSHIP
--   edge, ON DELETE CASCADE — a pending flow is transient Profile-owned
--   state within the owning Profile's account aggregate, so the sanctioned
--   auth.users -> profiles account-root cascade removes it (the Vault
--   verifier secret cleanup for a still-pending flow happens first through
--   the account-deletion revocation transaction, mirroring the #142
--   refresh-secret cleanup).
--
-- No privileges are granted here: the openorc schema lockout established by
-- create_openorc_schema carries isolation, and runtime access is
-- backend-only.

create table openorc.github_user_authorization_flows (
    profile_id uuid primary key,
    purpose text not null check (purpose in ('authorize', 'reauthorize')),
    state text not null unique,
    pkce_verifier_reference text not null
        check (char_length(trim(pkce_verifier_reference)) > 0),
    observed_revocation_count integer not null
        check (observed_revocation_count >= 0),
    started_at timestamptz not null default now(),
    expires_at timestamptz not null,

    -- A pending flow is always bounded: its expiry strictly follows its
    -- start instant.
    check (expires_at > started_at)
);

-- The Profile ownership edge, declared with its explicit deletion action:
-- true ownership — the pending flow exists solely within its owning
-- Profile's account aggregate.
alter table openorc.github_user_authorization_flows
    add constraint github_user_authorization_flows_profile_id_fkey
    foreign key (profile_id)
    references openorc.profiles (id)
    on delete cascade;