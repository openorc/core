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

## Deletion ownership and the sanctioned Auth boundary (issue #27)

- The deletion-ownership migration re-declares every foreign key with an explicit, reviewed action: `ON DELETE CASCADE` for true ownership edges, `NO ACTION DEFERRABLE INITIALLY DEFERRED` for scope-consistency/cross-reference/linkage edges and the Task's upward current-object pointers. The classification is enforced by `tests/test_deletion_migration.py`; keep it reviewable rather than implicit.
- `openorc.profiles.id → auth.users (id) ON DELETE CASCADE` is the single deliberate, sanctioned exception to managed-schema isolation: the canonical account identity. Direct Supabase-administrative deletion of an auth user must remove the complete OpenOrc-owned graph (no orphaned Profile/Workspace rows from administrative deletion outside the OpenOrc application). Supabase Auth owns its own Auth/OAuth/session cleanup; OpenOrc application code invokes no Supabase Auth admin API in Phase 1, and the future Delete Account service (Phase 2) converges on the same database behavior.
- The Auth-root cascade fires only when the `auth.users` row is actually removed. Supabase soft deletion (`should_soft_delete=True`) preserves the Auth row and therefore never triggers the cascade or removes any OpenOrc data. The future OpenOrc Delete Account operation must, after its required external cleanup/revocation, ultimately perform a permanent/hard Auth-user deletion — never Supabase soft deletion — or OpenOrc-owned account data is stranded.
- OpenOrc deletion must never delete or mutate GitHub engineering artifacts or runtime-owned credentials/configuration/filesystem state; the schema grants no deletion path into any external system.

## Supabase Vault (issue #55)

- The supported Supabase Vault extension is enabled by the `enable_supabase_vault` migration (`create extension if not exists supabase_vault cascade`). It is the concrete v1 store for OpenOrc-owned Agent Runtime control-endpoint credentials; raw secrets never live in ordinary `openorc.*` tables.
- The vault schema is deliberately locked to the direct backend Postgres path: the migration revokes all access from PUBLIC, anon, authenticated, and service_role (schema, `vault.secrets`, `vault.decrypted_secrets`, and the secret-management functions). The migrating (owning) role needs no grants. This is a Vault-specific least-privilege restriction: OpenOrc still uses the privileged Supabase service-role/secret credential for other Supabase-owned capabilities (Auth, administrative lifecycle), and nothing here disables, removes, or broadly reduces service_role outside `vault`.
- The effective privilege posture is proven against a real Supabase branch by integration assertions (`has_schema_privilege`/`has_table_privilege`/`has_function_privilege` in `tests/integration/test_connection_credential_services.py`), not inferred from migration text.

## Security / isolation

- Workspace isolation is a security boundary.
- RLS/authorization must preserve explicit user/Workspace ownership.
- Privileged server credentials remain backend/worker-only and never reach the browser.
- Browser clients use Supabase for authentication, not as a bypass around the OpenOrc application API for workflow data.
- Raw OpenOrc-owned secrets do not belong in ordinary tables.

Read persistence, domain/services, API, and frontend guidance when schema/auth/type changes cross those boundaries.
