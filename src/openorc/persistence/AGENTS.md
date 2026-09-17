# Persistence Agent Context

## Boundary

Persistence owns durable representation and repository/data-access mechanics for OpenOrc control-plane state. It does not define product workflow semantics merely because a table or constraint exists.

## Rules

- Postgres is the durable source for OpenOrc workflow/control state.
- Preserve Workspace scope explicitly on Workspace-owned operational data where practical.
- Enforce durable uniqueness/identity constraints that protect domain invariants, including at most one current/non-archived Task per repository issue.
- Store stable external identifiers and exact SHAs needed for reconciliation; mutable display fields must not be sole identity.
- Do not store runtime conversational context or chain-of-thought as workflow state.
- Raw secrets do not belong in ordinary application tables.
- Schema changes must remain aligned with `supabase/migrations/` and tests.
- Database shape must not become an excuse to bypass domain/service rules.

## v1 driver and connection conventions

These are durable Phase 1 conventions. Later issues inherit them; do not re-litigate them issue-by-issue. Changing one is a deliberate architecture decision, not an implementation detail.

- Postgres is accessed through synchronous psycopg 3 with `psycopg_pool.ConnectionPool`. No async psycopg API and no parallel sync/async persistence implementations exist at this boundary. The synchronous foundation serves API/service code and RQ workers alike.
- Pool ownership is strictly process-local. A pool belongs to exactly one OS process, and the `ConnectionPool` object must be created in the OS process that uses it, after any fork that process has gone through. Never construct a pool anywhere it could be inherited across an RQ fork boundary (the default RQ worker forks a child process per job): an RQ parent must not establish database pool/connection state before forking its work horse. Process surfaces own the lifespan/ownership wiring that satisfies this invariant within their process.
- Pool access is process-aware: the accessor tracks the owning PID and fails closed on PID change. A changed process identity can never reuse, replace, or close the previous pool; a stale fork-inherited pool is abandoned untouched. A process creates a pool through the explicit fresh-pool path only when its parent established no pool before the fork.
- Pool size bounds are per process, not deployment-wide. Deployment sizing must account for process count × pool size.
- Connections are pooler-neutral by default: `prepare_threshold=None`, no dependence on `search_path`, and no session-local state (no session `SET`/GUCs). Pooler mode (direct/session/transaction) is a deployment concern and must never leak into persistence code or domain meaning.
- Transactions are thin and composable: yield one connection and delegate commit-on-success, rollback-on-error, and connection-return to psycopg/psycopg_pool connection-context semantics. OpenOrc does not duplicate transaction machinery, and repositories do not establish auto-commit behavior.
- No external call (GitHub, Cline, model inference, or any other external system) may occur while a database transaction is open.
- Row locking is narrow and deliberate: repositories author semantic `SELECT ... FOR UPDATE` operations inside short transactions where the domain requires them. There is no generic lock-row or query-builder locking abstraction.
- `READ COMMITTED` is the normal isolation level; the persistence foundation does not customize isolation.

## SQL and data conventions

- Repositories are explicit SQL repositories. Table references use fully qualified `openorc.*` names, values are always parameterized, and `psycopg.sql` composition is used only where dynamic identifiers are genuinely required. No f-string SQL.
- OpenOrc-owned application tables live in the dedicated `openorc` schema. Migration authority, grant posture, and the append-only migration rule are owned by `supabase/AGENTS.md`; persistence code never mutates committed migration history.
- Internal domain records use UUID primary keys unless an explicit 1:1 infrastructure identity deliberately reuses another system's UUID (e.g. `Profile` mapping to the Supabase Auth user UUID).
- Workflow/type vocabularies are Python enums/value objects enforced in the database as text columns plus CHECK constraints, never native Postgres ENUMs.
- Instants are stored as `TIMESTAMPTZ`. Python datetimes crossing the persistence boundary are timezone-aware and normalized to UTC; naive datetimes are rejected. `utc_now()`-style helpers are low-level utilities, not workflow-clock semantics.

## Ownership and repository identity conventions (Phase 1)

- The ownership tables (`profiles`, `workspaces`, `projects`, `repositories`) live in the `openorc` schema and map to `openorc.domain.ownership` objects. Instants read from Postgres are normalized to UTC at the mapping boundary.
- `profiles.id` is the caller-supplied Supabase Auth user UUID (1:1 by value). OpenOrc migrations never create foreign keys into Supabase-managed schemas (`auth`, `storage`, ...); domain references point at `openorc.profiles`.
- Workspace-owned child rows carry a direct `workspace_id`. Where the parent also carries `workspace_id`, a composite foreign key `(parent_id, workspace_id) → parent (id, workspace_id)` enforces that the direct scope agrees with parent ownership. This is the durable pattern for later Workspace-owned tables.
- External identities that must be canonical per Workspace use `UNIQUE (workspace_id, <stable external identity>)` (for example the GitHub repository ID). Mutable observed metadata changes through explicit repository UPDATE statements that also advance `updated_at`; identity columns are never part of such updates.
- Violated durable invariants surface as driver exceptions (`UniqueViolation`, `ForeignKeyViolation`, ...). Translating them into typed application errors is a service-layer concern, not a persistence one.

## Connection and workflow role binding conventions (Phase 1)

These conventions are established by the Connection/role-binding persistence (issue #20); later issues inherit them.

- `connections` and `workflow_role_bindings` map to `openorc.domain.connections` objects. Both carry a direct `workspace_id`; the composite foreign key `(connection_id, workspace_id) → connections (id, workspace_id)` keeps a binding's direct scope in agreement with the bound Connection's Workspace.
- Raw OpenOrc-owned secrets do not live in ordinary application tables. OpenOrc-owned authentication is represented only through the nullable opaque `connections.auth_reference` boundary: NULL means no OpenOrc-owned auth reference is currently configured, non-NULL means one was supplied. Validation, expiry, revocation, and authentication lifecycle state are later functionality that can actually determine those facts — do not persist speculative auth status vocabulary. Runtime-owned provider/MCP/tool credentials remain runtime-owned and are never modeled as OpenOrc credential records.
- `connections.safe_config` is the non-secret Owner configuration container (jsonb, defaults empty). Persistence applies no credential key filtering; the contract is simply that secret material does not belong there, and concrete adapter configuration schemas enforce allowed fields when they exist.
- `connections.enabled` is the Owner-controlled eligibility switch (default true): OpenOrc is permitted to use the Connection. It never encodes runtime reachability, health, initialization, READY, or WORKING state — no runtime status vocabulary is persisted.
- `connections.session_capacity` is Owner-configured OpenOrc admission control scoped to the Connection (default 1: concurrency is explicitly enabled by the Owner, never assumed). It is never discovered from the runtime. Capacity admission later compares this against occupied TaskAgentSessions; do not add speculative capacity indexes.
- `reported_provider`/`reported_model` are nullable opaque runtime-reported observation strings — never configuration authority, never enums. A future OpenOrc-side selection capability would add separate `configured_*` concepts rather than repurposing these fields.
- A workflow role binding is mutable per-role configuration: `unique (workspace_id, role)` gives exactly one binding per workflow role in v1 (no runtime pools, no failover). Producer and Reviewer bindings are independent rows that may reference the same Connection or separate Connections. The binding carries only workspace/role/connection identity and timestamps; per-role session configuration is added only when a real configurable property exists. Repository upserts (`set_role_binding`) preserve binding identity and `created_at` — a configuration change, not historical replacement.

Read `supabase/AGENTS.md` for migration/auth/RLS concerns and domain/services guidance for semantic changes.
