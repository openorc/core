-- Freeze the Task requirements fingerprint on Owner Start, not issue
-- discovery (remediation for issues #60/#76/#120, tracked as issue #233).
--
-- Issue discovery is a UI/read-model concern: a GitHub `issues` webhook
-- reconciles and refreshes projections but never creates a Task, and explicit
-- Owner intake creates the one current `ready_to_plan` Task WITHOUT an
-- immutable requirements baseline. Owner `Start Task` is the commitment
-- point: a successful start decision freshly re-reads authoritative GitHub
-- state and freezes the exact then-current requirements fingerprint on the
-- Task, atomically with the transition from `ready_to_plan` to `planning` or
-- `queued`. A capacity-queued Task has started for authority/baseline
-- purposes even though agents have not yet run. A failed, rejected, or stale
-- start leaves the Task `ready_to_plan` with no baseline; there is no silent
-- re-baselining after a successful start.
--
-- Durable properties carried by this migration:
--
-- - `openorc.tasks.source_requirements_fingerprint` becomes NULLable. NULL
--   means the Task has never successfully started: no baseline exists yet.
--   The only reachable NULL-bearing statuses are `ready_to_plan` (the
--   pre-start attempt) and `cancelled` (an attempt cancelled directly from
--   `ready_to_plan`, before any start committed).
-- - The original inline lowercase hex SHA-256 CHECK (auto-named
--   `tasks_source_requirements_fingerprint_check` by the issue #60 migration)
--   is replaced by an explicitly named status-aware CHECK: every present
--   value must still be a valid lowercase hex SHA-256 digest, and `queued`,
--   `planning`, and every subsequent nonterminal/terminal post-start status
--   require a present frozen fingerprint. The CHECK passes on NULL only for
--   `ready_to_plan` and `cancelled`. The atomic Start capture (the fingerprint
--   is set in the same guarded statement that transitions the status) keeps
--   the NULL-bearing state exactly the never-started state; no path erases a
--   frozen baseline.
-- - Migration policy for existing rows: intake-time fingerprints captured on
--   current, not-yet-started `ready_to_plan` rows are NOT baselines under the
--   corrected contract, so they are cleared to NULL; their first Owner Start
--   then freezes the freshly verified fingerprint and cannot be rejected by a
--   stale intake capture. Historical archived rows are left untouched:
--   started fingerprints are never corrupted or erased, and an archived
--   row's inert captured value has no remaining consumer.
--
-- No privileges are granted here: the openorc schema lockout established by
-- the create_openorc_schema migration carries isolation, and runtime access
-- is backend-only.

-- 1. The baseline no longer exists before Start: allow NULL.
alter table openorc.tasks
    alter column source_requirements_fingerprint drop not null;

-- 2. Pre-change captured fingerprints on not-yet-started current Tasks are
--    not baselines; clear them so the first Owner Start freezes the fresh
--    fingerprint. Started and archived rows are never touched.
update openorc.tasks
    set source_requirements_fingerprint = null
    where status = 'ready_to_plan' and archived_at is null;

-- 3. Replace the plain digest CHECK with the status-aware commitment CHECK.
--    The CASE form is deliberately NULL-safe: a plain `digest ~ pattern or
--    (fingerprint is null and status in ...)` disjunction evaluates to NULL
--    (not FALSE) for a NULL fingerprint on a started status, and Postgres
--    CHECK constraints reject only FALSE — the NULL would silently pass.
--    The CASE always yields a definite boolean: NULL fingerprints are
--    admitted exactly on the never-started statuses and rejected everywhere
--    else.
alter table openorc.tasks
    drop constraint tasks_source_requirements_fingerprint_check;

alter table openorc.tasks
    add constraint tasks_source_requirements_fingerprint_status_check
    check (
        case
            when source_requirements_fingerprint is null
                then status in ('ready_to_plan', 'cancelled')
            else source_requirements_fingerprint ~ '^[0-9a-f]{64}$'
        end
    );
