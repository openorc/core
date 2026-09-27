-- GitHub webhook delivery processing state and installation-scoped routing
-- identity (Phase 2B, issue #120).
--
-- Dispatch (#120) extends the #61 delivery record with two durable facts:
--
-- 1. `processed_at` — the smallest bounded processing/recovery metadata.
--    Dispatch marks an accepted delivery processed exactly when every routed
--    reconciliation completed (or typedly declined with nothing
--    outstanding); the column stays null while processing is unresolved, so
--    the #62 repeatable-reconciliation path can identify deliveries that
--    still require recovery. It is operational recovery metadata, never
--    workflow authority and never GitHub truth. No payload, error body, or
--    queue state is ever persisted.
-- 2. The repository-identity requirement relaxes for repository-metadata
--    deliveries: the `installation` and `installation_repositories` families
--    (default-delivered to every GitHub App, not manually subscribable)
--    carry no singular repository identity — they affect repository sets or
--    the whole installation. Their relevant deliveries are
--    installation-scoped (`github_repository_id` null) and dispatch fans out
--    over the installation's explicitly routed repositories; every other
--    relevant target still requires both stable identities. The replacement
--    constraint is explicitly named so later migrations never depend on
--    Postgres's auto-generated check numbering; the old inline check is
--    located by its exact normalized definition (exactly one match, else the
--    migration fails hard) rather than by a guessed auto-generated name.
--
-- No foreign keys, deletion-ownership edges, or grants change: the delivery
-- table keeps no Workspace foreign key (the delivery GUID is provider-owned
-- identity that survives Workspace lifecycle), and the openorc schema
-- lockout established by the create_openorc_schema migration carries
-- isolation with backend-only runtime access.

alter table openorc.github_webhook_deliveries
    add column processed_at timestamptz;

do $$
declare
    matched_name text;
    match_count int;
begin
    select coalesce(max(conname::text), ''), count(*)
    into matched_name, match_count
    from pg_constraint
    where conrelid = 'openorc.github_webhook_deliveries'::regclass
      and contype = 'c'
      and pg_get_constraintdef(oid) like '%github_repository_id IS NOT NULL%';
    if match_count <> 1 then
        raise exception
            'expected exactly one repository-identity check constraint on '
            'openorc.github_webhook_deliveries, found %',
            match_count;
    end if;
    execute format(
        'alter table openorc.github_webhook_deliveries drop constraint %I',
        matched_name
    );
end $$;

alter table openorc.github_webhook_deliveries
    add constraint github_webhook_deliveries_relevant_repository_identity_check
    check (
        classification <> 'relevant'
        or github_repository_id is not null
        or routing_target = 'repository_metadata'
    );