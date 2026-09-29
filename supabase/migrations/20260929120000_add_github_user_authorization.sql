-- Durable Profile-scoped GitHub App user authorization (Phase 2B, issue #142).
--
-- Establishes the account-boundary credential state for OpenOrc acting on
-- GitHub on behalf of the same human Owner who signed in to OpenOrc:
--
--   Profile -> GitHubUserAuthorization (one current authorization per Profile)
--
-- Durable properties carried by this migration:
--
-- - The row is keyed by the Profile itself (profile_id is the primary key):
--   exactly one current authorization per Profile in v1. The FK to
--   openorc.profiles is the explicit ownership edge; successful account
--   deletion removes the row with the Profile graph through the sanctioned
--   auth.users -> profiles cascade, while the Vault secret cleanup happens
--   first through the account lifecycle service (issue #97 revocation
--   transaction).
-- - Only non-secret facts are stored. The refresh credential itself lives
--   exclusively in Supabase Vault, addressed by the opaque
--   ``refresh_secret_reference`` (never a token value); access and refresh
--   tokens have no column and no path into this table.
-- - ``status`` is the durable currentness vocabulary: 'active' (a usable
--   authorization with a live refresh credential) and 'revoked' (explicitly
--   unusable — missing access, revoked upstream, or administratively
--   revoked). Revocation is durable state, deliberately distinct from
--   never-authorized (no row), and removes the usable refresh reference
--   while the row persists.
-- - CHECK-consistent lifecycle tuples: an active authorization carries
--   exactly one Vault refresh reference and its durable expiry instant; a
--   revoked authorization carries neither and always carries ``revoked_at``.
-- - ``refresh_generation`` is monotonic for the row's whole Profile lifetime
--   (starting at 1; every credential lifecycle transition — refresh rotation,
--   reauthorization, revocation — advances it atomically with the durable
--   effect). It is the cross-process compare-and-swap guard that prevents a
--   stale process from overwriting a newer rotated credential, and the
--   binding key of the in-memory access-token cache, so a stale
--   (Profile, generation) cache key can never become apparently current
--   again. The row is never delete-and-reinserted, so the generation never
--   resets.
-- - ``github_user_id`` is the stable numeric GitHub identity the
--   authorization is proven against (the same-human binding proof); ``login``
--   is mutable observed presentation metadata only.
-- - ``refresh_expires_at`` is the durable expiry instant of the current
--   refresh credential parsed from GitHub's token responses. Authorization
--   establishment fails closed unless GitHub returned the expiring-token
--   capability, so an active row always carries it.
-- - ``authorized_at`` is when the current active authorization was
--   established; ``created_at``/``updated_at`` are the row's durable
--   lifecycle instants.
--
-- FOREIGN-KEY CLASSIFICATION (the deletion-ownership vocabulary, issue #27):
-- - github_user_authorizations.profile_id -> profiles: TRUE OWNERSHIP edge,
--   ON DELETE CASCADE — the authorization exists solely within its owning
--   Profile's account aggregate.
--
-- No privileges are granted here: the openorc schema lockout established by
-- create_openorc_schema carries isolation, and runtime access is
-- backend-only.

create table openorc.github_user_authorizations (
    profile_id uuid primary key,
    github_user_id bigint not null check (github_user_id > 0),
    github_login text check (github_login is null or char_length(trim(github_login)) > 0),
    status text not null check (status in ('active', 'revoked')),
    refresh_secret_reference text check (
        refresh_secret_reference is null or char_length(trim(refresh_secret_reference)) > 0
    ),
    refresh_expires_at timestamptz,
    refresh_generation integer not null check (refresh_generation >= 1),
    authorized_at timestamptz not null default now(),
    revoked_at timestamptz,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),

    -- CHECK-consistent lifecycle: an active authorization carries exactly
    -- the usable refresh credential facts; a revoked one carries none of
    -- them and always carries its revocation instant.
    check ((status = 'active') = (refresh_secret_reference is not null)),
    check ((status = 'active') = (refresh_expires_at is not null)),
    check ((status = 'revoked') = (revoked_at is not null))
);

-- The Profile ownership edge, declared with its explicit deletion action:
-- true ownership — the authorization exists solely within its owning
-- Profile's account aggregate.
alter table openorc.github_user_authorizations
    add constraint github_user_authorizations_profile_id_fkey
    foreign key (profile_id)
    references openorc.profiles (id)
    on delete cascade;
