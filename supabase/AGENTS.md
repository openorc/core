# Supabase Agent Context

## Boundary

Supabase/Postgres schema, migrations, persistence integration, and Supabase Auth integration are supported core product code. Concrete production Supabase project operations belong to `openorc/cloud`.

## Migration rules

- Repository migration files and applied DDL must remain aligned.
- Public-schema changes explicitly define least-privilege grants; do not rely on broad defaults.
- Never hardcode generated environment/project identifiers in product migrations.
- Project identity comes from actual environment/repository configuration, never from Babelbeez values.
- Schema changes require corresponding persistence/domain tests and affected generated types/contracts if such generation is adopted.

## Security / isolation

- Workspace isolation is a security boundary.
- RLS/authorization must preserve explicit user/Workspace ownership.
- Privileged server credentials remain backend/worker-only and never reach the browser.
- Browser clients use Supabase for authentication, not as a bypass around the OpenOrc application API for workflow data.
- Raw OpenOrc-owned secrets do not belong in ordinary tables.

Read persistence, domain/services, API, and frontend guidance when schema/auth/type changes cross those boundaries.
