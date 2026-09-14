# Supabase Agent Context

## Boundary

Supabase/Postgres schema, migrations, persistence integration, and Supabase Auth integration are supported core product code. Concrete production Supabase project operations belong to `openorc/cloud`.

## Migration rules

- Repository migrations are the authoritative source for schema evolution. Schema changes are authored in `supabase/migrations/` first, reviewed with the code change, and then applied to deployed Supabase projects through the supported deployment path.
- Do not mutate the live production Supabase schema first and backfill a migration afterward. Production is a deployment target, not an agent development workbench.
- Agents must not use Supabase MCP, SQL consoles, or equivalent privileged tooling to make ad hoc production DDL changes as the normal development workflow.
- Develop and validate schema changes against local Supabase or another explicitly designated non-production environment when available. The production project must be reproducible from committed migrations rather than from undocumented live edits.
- Repository migration files and applied DDL must remain aligned. If emergency/manual production repair is ever explicitly authorized, reconcile it back into the migration history immediately and document the exceptional path.
- The supported repository-native developer commands for authoring and applying migrations are documented in `docs/supabase-migrations.md`. Use those commands rather than ad hoc live-schema mutation; the Supabase CLI version is pinned exactly in `supabase/cli-version` and enforced by the tooling.
- Public-schema changes explicitly define least-privilege grants; do not rely on broad defaults.
- Never hardcode generated environment/project identifiers in product migrations.
- Project identity comes from actual environment/repository configuration, never from values inherited from any prior project.
- Schema changes require corresponding persistence/domain tests and affected generated types/contracts if such generation is adopted.

## Security / isolation

- Workspace isolation is a security boundary.
- RLS/authorization must preserve explicit user/Workspace ownership.
- Privileged server credentials remain backend/worker-only and never reach the browser.
- Browser clients use Supabase for authentication, not as a bypass around the OpenOrc application API for workflow data.
- Raw OpenOrc-owned secrets do not belong in ordinary tables.

Read persistence, domain/services, API, and frontend guidance when schema/auth/type changes cross those boundaries.
