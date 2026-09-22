-- Durable account-deletion attempt state (Phase 2A, issue #97).
--
-- The permanent account-deletion lifecycle needs a database-backed,
-- Profile-level write barrier that survives the deliberate no-transaction
-- window around the external Supabase Auth Admin call: while an attempt is
-- unresolved, ordinary Owner mutations must fail closed so no fresh
-- OpenOrc-owned Vault secret can be created for an account whose OpenOrc
-- graph is about to be cascade-deleted (a Vault secret is not a foreign-keyed
-- row and would otherwise survive the cascade as an orphan).
--
-- Three nullable columns on openorc.profiles carry the attempt state:
--
--   account_deletion_state        'active'   — one deletion attempt is in
--                                            flight (durable single-flight
--                                            marker for concurrent
--                                            invocations);
--                                 'uncertain' — a prior Auth Admin delete
--                                             outcome is unreconciled; the
--                                             next explicit invocation must
--                                             reconcile through the Admin
--                                             read surface before any
--                                             replay.
--   account_deletion_attempt_id   the UUID of the attempt that owns the
--                                 state; every transition and clear is
--                                 conditional on this exact identity, so one
--                                 invocation can never clear or move another
--                                 invocation's protection.
--   account_deletion_started_at   database-clock establishment time; the
--                                 active-attempt lease basis (an 'active'
--                                 row older than the documented lease is an
--                                 abandoned attempt and is recovered through
--                                 reconciliation, not replay).
--
-- A COMPOSITE CHECK encodes the state machine at the database boundary:
-- either all three columns are NULL (normal operation) or the state is
-- 'active'/'uncertain' with both the attempt UUID and the establishment time
-- non-NULL. Impossible safety tuples are unrepresentable even if a future
-- code path bypasses the persistence helpers.
--
-- Lifecycle: the state is claimed in the same short transaction as the
-- OpenOrc-owned credential revocation (before the external Auth Admin
-- call), cleared (attempt-scoped) on a confirmed known-not-applied outcome
-- so normal account use can resume, retained under unreconciled uncertainty,
-- and removed together with the Profile by the existing
-- auth.users -> profiles ON DELETE CASCADE on success. No privileges are
-- changed here: the openorc schema lockout established by
-- create_openorc_schema carries isolation, and runtime access is
-- backend-only.

alter table openorc.profiles
    add column account_deletion_state text,
    add column account_deletion_attempt_id uuid,
    add column account_deletion_started_at timestamptz;

alter table openorc.profiles
    add constraint profiles_account_deletion_state_tuple_check check (
        (
            account_deletion_state is null
            and account_deletion_attempt_id is null
            and account_deletion_started_at is null
        )
        or (
            account_deletion_state in ('active', 'uncertain')
            and account_deletion_attempt_id is not null
            and account_deletion_started_at is not null
        )
    );